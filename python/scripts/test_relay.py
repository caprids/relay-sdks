#!/usr/bin/env python3
"""Opt-in live smoke test against a real Relay. Excluded from ``mise run python:check``.

Publishes and subscribes on the same random namespace over the SDK's default transport (native QUIC
on aioquic), then asserts the list in ``spec/behavior.md``:

1. A publisher connection and a subscriber connection both open with the supplied token.
2. Once the publisher's ``announced`` settles, subscriptions to two tracks on the namespace are
   accepted, retrying the fresh-namespace rejection while the announce propagates; the report
   records the retries.
3. Binary payloads, an empty payload, and a text payload all arrive intact.
4. Closing one track writer does not affect delivery on another track.
5. A late subscriber decodes LARGEST_OBJECT and receives the next live object.
6. All connections close cleanly once the test closes them.

Environment (shared by every SDK's tooling): ``RELAY_TOKEN`` (or a hidden prompt), ``RELAY_URL``
(https, without the token), ``RELAY_NAMESPACE_PREFIX`` (a random field is appended). The token is
never written to disk and is redacted from output. For anonymous local relays, set
``RELAY_ANONYMOUS=1``; ``RELAY_CA_FILE`` supplies a local certificate trust root. ``RELAY_FRAMING=webtransport``
dials a WebTransport session (``mux_relay.webtransport``) instead, until #232 retires it.
See ``spec/relay-interop.md`` for the local moq-relay 0.15.8 reproduction.

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

from moq_core import NextObjectFilter
from mux_relay import (
    DEFAULT_ENDPOINT,
    CancellationToken,
    Closed,
    MessageParameters,
    MoqtObject,
    RelayAwaitable,
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


async def run(endpoint: str, token: str | None, namespace: list[str], report: Any) -> dict[str, Any]:
    from aioquic.quic.configuration import QuicConfiguration

    from mux_relay.quic import connect_quic
    from mux_relay.webtransport import connect_webtransport

    framing = os.environ.get("RELAY_FRAMING", "native-quic")

    if framing not in ("native-quic", "webtransport"):
        raise SystemExit("RELAY_FRAMING must be native-quic or webtransport")

    connect = connect_quic if framing == "native-quic" else connect_webtransport
    cafile = os.environ.get("RELAY_CA_FILE")

    def dial(url: str, protocols: Any) -> Any:
        return connect(url, protocols, configuration=QuicConfiguration(is_client=True, cafile=cafile))

    # Without a trust root or a framing to swap, the run takes the SDK's own default transport.
    create_transport = dial if cafile or framing != "native-quic" else None

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

    def observe_closed(label: str, closed: RelayAwaitable[Closed]) -> None:
        def done(future: asyncio.Future[Closed]) -> None:
            if not closing:
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
            endpoint, get_token=lambda: token, cancellation=lifetime, create_transport=create_transport
        )
        observe_closed("subscriber", subscriber.closed)
        report({"phase": "subscriber connected"})

        # Assertion 2. The announce left with the first registered track; the relay routes subscribes to
        # the namespace about a round trip later, so the subscribe retries the rejection it may still answer.
        await publisher.announced
        report({"phase": "announced"})
        retries = {"events": 0, "status": 0}

        for track in ("events", "status"):

            def on_object(obj: MoqtObject, track: str = track) -> None:
                if obj.status == "normal":
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
            endpoint, get_token=lambda: token, cancellation=lifetime, create_transport=create_transport
        )
        observe_closed("late subscriber", late_subscriber.closed)

        def on_late_ok(ok: Any) -> None:
            nonlocal late_largest_object
            late_largest_object = ok.parameters.largest_object
            if late_largest_object is None:
                errors.append("late SUBSCRIBE_OK omitted LARGEST_OBJECT")

        def on_late_object(obj: MoqtObject) -> None:
            if obj.status == "normal":
                received["lateStatus"].append(obj.payload)

        late_subscriber.subscribe(
            namespace,
            "status",
            RelaySubscriptionHandlers(
                on_subscribed=on_late_ok,
                on_object=on_late_object,
                on_error=lambda error: errors.append(f"late status: {error.error_code} {error.reason}"),
            ),
            parameters=MessageParameters(location_filter=NextObjectFilter()),
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

        return {
            "endpoint": endpoint,
            "namespace": namespace,
            "events": len(received["events"]),
            "status": len(received["status"]),
            "announceRaceRetries": retries,
            "binaryAndEmptyPayloads": "passed",
            "independentTrackClose": "passed",
            "lateSubscriberLargestObject": {
                "group": late_largest_object.group,
                "object": late_largest_object.object,
            }
            if late_largest_object is not None
            else None,
        }
    finally:
        closing = True
        deadline.cancel()

        try:
            if publisher is not None:
                await publisher.close()
        finally:
            if subscriber is not None:
                subscriber.close()
            if late_subscriber is not None:
                late_subscriber.close()


def main() -> int:
    endpoint = os.environ.get("RELAY_URL", DEFAULT_ENDPOINT)
    parts = urlsplit(endpoint)

    if parts.scheme != "https":
        raise SystemExit("RELAY_URL must be an https URL")

    if "jwt=" in parts.query:
        raise SystemExit("Supply the token via stdin or RELAY_TOKEN, not RELAY_URL")

    token = read_token()
    prefix = [field for field in os.environ.get("RELAY_NAMESPACE_PREFIX", "relay-sdk-smoke").split("/") if field]
    namespace = [*prefix, str(uuid.uuid4())]

    def report(value: Any) -> None:
        print(redact(json.dumps(value) if not isinstance(value, str) else value, token), flush=True)

    try:
        result = asyncio.run(run(endpoint, token, namespace, report))
    except BaseException as error:
        report({"result": "FAIL", "error": redact(str(error) or type(error).__name__, token)})
        return 1

    report({"result": "PASS", **result, "connections": "closed"})
    return 0


if __name__ == "__main__":
    sys.exit(main())
