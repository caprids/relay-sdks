#!/usr/bin/env python3
"""Opt-in live smoke test against a real Relay. Excluded from ``mise run python:check``.

Publishes and subscribes on the same random namespace over the SDK's default transport (native QUIC
on aioquic), then asserts the list in ``spec/behavior.md``:

1. A publisher connection and a subscriber connection both open with the supplied token.
2. Once the publisher's ``announced`` settles, subscriptions to two tracks on the namespace are
   accepted, retrying the fresh-namespace rejection while the announce propagates; the report
   records the retries.
3. Binary payloads, an empty payload, and a text payload all arrive intact, each written once after
   a warm-up marker has arrived on its track; the report records the warm-up writes.
4. Closing one track writer does not affect delivery on another track.
5. A late subscriber decodes LARGEST_OBJECT and receives the next live object.
6. The subscriber's discovery on the namespace's parent prefix reports the namespace ``announced``,
   then ``unannounced`` after the publisher's clean ``close()``; the report records how long that took.
7. All connections close cleanly once the test closes them.

Environment (shared by every SDK's tooling): ``RELAY_TOKEN`` (or a hidden prompt), ``RELAY_URL``
(https, without the token), ``RELAY_SUB_URL`` (the subscribers' relay for a cross-relay run, defaulting
to ``RELAY_URL``), ``RELAY_NAMESPACE_PREFIX`` (a random field is appended). The token is
never written to disk and is redacted from output. For anonymous local relays, set
``RELAY_ANONYMOUS=1``; ``RELAY_CA_FILE`` supplies a local certificate trust root.
See ``spec/relay-interop.md`` for the local moq-relay 0.17.0 reproduction.

    # After the README's endpoint/namespace setup; enter the token at the hidden prompt.
    RELAY_URL="$RELAY_URL" RELAY_NAMESPACE_PREFIX="$RELAY_NAMESPACE" mise run python:test:relay
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import uuid
from getpass import getpass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from moq_core import RelativeGroupFilter
from mux_relay import (
    DEFAULT_ENDPOINT,
    CancellationToken,
    Closed,
    MessageParameters,
    MoqtObject,
    RelayAwaitable,
    RelayError,
    RelayPublisher,
    RelaySubscriber,
    RelaySubscriptionHandlers,
    connect_relay_publisher,
    connect_relay_subscriber,
)

# The announce-race retry is the examples' policy, shared with this test; see spec/behavior.md.
EXAMPLES = str(Path(__file__).resolve().parents[1] / "examples")

if EXAMPLES not in sys.path:
    sys.path.insert(0, EXAMPLES)

from shared.announce_race import subscribe_with_retry  # noqa: E402

RUN_DEADLINE = 45.0
RECEIVE_DEADLINE = 10.0
WARM_UP = b"relay-sdk-smoke warm-up"
WARM_UP_INTERVAL = 0.25


def read_token() -> str | None:
    if os.environ.get("RELAY_ANONYMOUS") == "1":
        return os.environ.get("RELAY_TOKEN") or None

    token = os.environ.get("RELAY_TOKEN")

    if not token and sys.stdin.isatty():
        token = getpass("Relay token (input hidden): ").strip()
    elif not token:
        token = sys.stdin.readline().strip()

    if not token:
        raise SystemExit("A Relay token is required (RELAY_TOKEN or the prompt)")

    return token


def redact(value: str, token: str | None) -> str:
    return value.replace(token, "[redacted]") if token else value


def read_endpoint(name: str, default: str) -> str:
    endpoint = os.environ.get(name, default)
    parts = urlsplit(endpoint)

    if parts.scheme != "https":
        raise SystemExit(f"{name} must be an https URL")

    if "jwt=" in parts.query:
        raise SystemExit(f"Supply the token via stdin or RELAY_TOKEN, not {name}")

    return endpoint


async def run(endpoint: str, sub_endpoint: str, token: str | None, namespace: list[str], report: Any) -> dict[str, Any]:
    from aioquic.quic.configuration import QuicConfiguration

    from mux_relay.quic import connect_quic

    cafile = os.environ.get("RELAY_CA_FILE")

    def dial(url: str, protocols: Any) -> Any:
        return connect_quic(url, protocols, configuration=QuicConfiguration(is_client=True, cafile=cafile))

    # Without a trust root, the run takes the SDK's own default transport.
    create_transport = dial if cafile else None

    lifetime = CancellationToken()
    loop = asyncio.get_running_loop()
    deadline = loop.call_later(
        RUN_DEADLINE, lambda: lifetime.cancel(TimeoutError(f"Live relay test exceeded {RUN_DEADLINE:.0f} seconds"))
    )
    received: dict[str, list[bytes]] = {"events": [], "status": [], "lateStatus": []}
    errors: list[str] = []
    expected_events = [bytes([0, 255, 128, 1]), b"hello from Relay SDK", b""]
    closing = False
    publisher: RelayPublisher | None = None
    subscriber: RelaySubscriber | None = None
    late_subscriber: RelaySubscriber | None = None
    late_largest_object = None
    discovered: list[str] = []
    discovery_task: asyncio.Task[None] | None = None
    closed_on_purpose: set[str] = set()

    def observe_closed(label: str, closed: RelayAwaitable[Closed]) -> None:
        def done(future: asyncio.Future[Closed]) -> None:
            if not closing and label not in closed_on_purpose:
                errors.append(f"{label} closed: {future.result().error or 'peer closure'}")

        closed.add_done_callback(done)

    async def wait_for(predicate: Any, label: str) -> None:
        until = time.monotonic() + RECEIVE_DEADLINE

        while not predicate():
            if errors:
                raise RuntimeError("; ".join(errors))

            lifetime.throw_if_cancelled()

            if time.monotonic() >= until:
                raise TimeoutError(f"Timed out waiting for {label}")

            await asyncio.sleep(0.02)

    async def wait_for_count(track: str, count: int) -> None:
        await wait_for(lambda: len(received[track]) >= count, f"{track}: {count} objects")

    warmed: set[str] = set()

    async def warm_up(track: str, writer: Any) -> int:
        # SUBSCRIBE_OK says the subscriber's relay took the request, not that the path to the publisher
        # is ready. A relay on another hop first subscribes only to learn the track's info and cancels
        # that within about 50 ms (spec/relay-interop.md), so the writes it takes are lost. Repeat a
        # marker the handlers drop until one arrives; the assertion payloads are then written once.
        until = time.monotonic() + RECEIVE_DEADLINE
        writes = 0

        while track not in warmed:
            if errors:
                raise RuntimeError("; ".join(errors))

            if time.monotonic() >= until:
                raise TimeoutError(f"Timed out waiting for {track}: warm-up object")

            try:
                await writer.write(WARM_UP)
            except RelayError as error:
                if error.code != "SUBSCRIPTION_CHANGED":
                    raise

            writes += 1
            sent = time.monotonic()

            while track not in warmed and time.monotonic() - sent < WARM_UP_INTERVAL:
                lifetime.throw_if_cancelled()
                await asyncio.sleep(0.02)

        return writes

    try:
        publisher = await connect_relay_publisher(
            endpoint,
            namespace=namespace,
            get_token=lambda: token,
            cancellation=lifetime,
            create_transport=create_transport,
        )
        observe_closed("publisher", publisher.closed)
        report({"phase": "publisher connected", "namespace": namespace})
        events = publisher.register_track("events", write_timeout=20)
        status = publisher.register_track("status", write_timeout=20)
        subscriber = await connect_relay_subscriber(
            sub_endpoint, get_token=lambda: token, cancellation=lifetime, create_transport=create_transport
        )
        observe_closed("subscriber", subscriber.closed)
        report({"phase": "subscriber connected"})

        # Discovery: the parent prefix may hold other runs' namespaces, so only this run's counts.
        prefix = namespace[:-1] if len(namespace) > 1 else namespace
        discovery = subscriber.namespace_events(prefix, cancellation=lifetime)

        async def discover() -> None:
            try:
                async with discovery:
                    async for event in discovery:
                        if list(event.namespace) == namespace:
                            discovered.append(event.type)
                if not closing:
                    errors.append("discovery ended early")
            except Exception as error:
                if not closing:
                    errors.append(f"discovery: {error!r}")

        discovery_task = asyncio.create_task(discover())

        # Assertion 2. The announce left with the first registered track; the relay routes subscribes to
        # the namespace about a round trip later, so the subscribe retries the rejection it may still answer.
        await publisher.announced
        report({"phase": "announced"})
        await wait_for(lambda: "announced" in discovered, "discovery: announced")
        report({"phase": "discovered"})
        retries = {"events": 0, "status": 0}

        for track in ("events", "status"):

            def on_object(obj: MoqtObject, track: str = track) -> None:
                if obj.status != "normal":
                    return
                if obj.payload == WARM_UP:
                    warmed.add(track)
                else:
                    received[track].append(obj.payload)

            def on_retry(attempt: int, delay: float, track: str = track) -> None:
                retries[track] += 1
                report({"phase": "subscribe retried", "track": track, "attempt": attempt, "delay": delay})

            await subscribe_with_retry(
                subscriber,
                namespace,
                track,
                RelaySubscriptionHandlers(
                    on_subscribed=lambda ok, track=track: report({"phase": "subscribed", "track": track}),
                    on_object=on_object,
                    on_error=lambda error, track=track: errors.append(f"{track}: {error.error_code} {error.reason}"),
                    on_subgroup_reset=lambda reset, track=track: errors.append(f"{track} reset: {reset.error}"),
                ),
                on_retry=on_retry,
            )

        if errors:
            raise RuntimeError("; ".join(errors))

        warm_up_writes = {"events": await warm_up("events", events), "status": await warm_up("status", status)}
        report({"phase": "warmed up", "writes": warm_up_writes})

        for payload in expected_events:
            await events.write(payload)

        await status.write(bytes([42]))
        await wait_for_count("events", 3)
        await wait_for_count("status", 1)
        report(
            {"phase": "received initial payloads", "events": len(received["events"]), "status": len(received["status"])}
        )
        # Make the relay send LARGEST_OBJECT by joining after content has arrived.
        late_subscriber = await connect_relay_subscriber(
            sub_endpoint, get_token=lambda: token, cancellation=lifetime, create_transport=create_transport
        )
        observe_closed("late subscriber", late_subscriber.closed)

        def on_late_ok(ok: Any) -> None:
            nonlocal late_largest_object
            late_largest_object = ok.parameters.largest_object
            if late_largest_object is None:
                errors.append("late SUBSCRIBE_OK omitted LARGEST_OBJECT")

        def on_late_object(obj: MoqtObject) -> None:
            if obj.status == "normal" and obj.payload != WARM_UP:
                received["lateStatus"].append(obj.payload)

        # Start at the next group, the next live object. Not Next Object: moq-relay 0.17.0 frames
        # LOCATION_FILTER the draft-20 way on moqt-22 too, and closes the session on draft-22's Next
        # Object spelling (spec/relay-interop.md, "Draft-22 LOCATION_FILTER framing"). A relative start
        # of 0 is the same bytes in both drafts.
        late_subscriber.subscribe(
            namespace,
            "status",
            RelaySubscriptionHandlers(
                on_subscribed=on_late_ok,
                on_object=on_late_object,
                on_error=lambda error: errors.append(f"late status: {error.error_code} {error.reason}"),
            ),
            parameters=MessageParameters(location_filter=RelativeGroupFilter(groups_before_next=0)),
        )
        await wait_for(lambda: late_largest_object is not None, "late SUBSCRIBE_OK with LARGEST_OBJECT")
        events.close()
        await status.write(bytes([43]))
        await wait_for_count("status", 2)
        await wait_for_count("lateStatus", 1)
        if received["lateStatus"] != [bytes([43])]:
            raise RuntimeError("Late subscriber payload mismatch")

        if sorted(received["events"]) != sorted(expected_events) or received["status"] != [bytes([42]), bytes([43])]:
            raise RuntimeError(f"Payload mismatch: {received!r}")

        if errors:
            raise RuntimeError("; ".join(errors))

        # A clean publisher close withdraws the announce; the subscriber's discovery hears it.
        closed_at = time.monotonic()
        closed_on_purpose.add("publisher")
        await publisher.close()
        await wait_for(lambda: "unannounced" in discovered, "discovery: unannounced")
        withdrawn_after_ms = round((time.monotonic() - closed_at) * 1000)
        report({"phase": "undiscovered", "afterMs": withdrawn_after_ms})
        if discovered != ["announced", "unannounced"]:
            raise RuntimeError(f"Discovery mismatch: {discovered!r}")

        return {
            "endpoint": endpoint,
            "subscriberEndpoint": sub_endpoint,
            "namespace": namespace,
            "events": len(received["events"]),
            "status": len(received["status"]),
            "announceRaceRetries": retries,
            "warmUpWrites": warm_up_writes,
            "binaryAndEmptyPayloads": "passed",
            "independentTrackClose": "passed",
            "lateSubscriberLargestObject": {
                "group": late_largest_object.group,
                "object": late_largest_object.object,
            }
            if late_largest_object is not None
            else None,
            "discovery": {"announced": "passed", "withdrawnAfterMs": withdrawn_after_ms},
        }
    finally:
        closing = True
        deadline.cancel()
        if discovery_task is not None:
            discovery_task.cancel()

        try:
            if publisher is not None:
                await publisher.close()
        finally:
            if subscriber is not None:
                subscriber.close()
            if late_subscriber is not None:
                late_subscriber.close()


def main() -> int:
    endpoint = read_endpoint("RELAY_URL", DEFAULT_ENDPOINT)
    sub_endpoint = read_endpoint("RELAY_SUB_URL", endpoint)
    token = read_token()
    prefix = [field for field in os.environ.get("RELAY_NAMESPACE_PREFIX", "relay-sdk-smoke").split("/") if field]
    namespace = [*prefix, str(uuid.uuid4())]

    def report(value: Any) -> None:
        print(redact(json.dumps(value) if not isinstance(value, str) else value, token), flush=True)

    try:
        result = asyncio.run(run(endpoint, sub_endpoint, token, namespace, report))
    except BaseException as error:
        report({"result": "FAIL", "error": redact(str(error) or type(error).__name__, token)})
        return 1

    report({"result": "PASS", **result, "connections": "closed"})
    return 0


if __name__ == "__main__":
    sys.exit(main())
