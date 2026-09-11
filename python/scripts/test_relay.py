#!/usr/bin/env python3
"""Opt-in live smoke test against a real Relay. Excluded from ``mise run python:check``.

Publishes and subscribes on the same random namespace over the aioquic WebTransport transport, then
asserts the list in ``spec/behavior.md``:

1. A publisher connection and a subscriber connection both open with the supplied token.
2. Subscriptions to two tracks on the namespace are accepted.
3. Binary payloads, an empty payload, and a text payload all arrive intact.
4. Closing one track writer does not affect delivery on another track.
5. Both connections close cleanly once the test closes them.

Environment (shared by every SDK's tooling): ``RELAY_TOKEN`` (or a hidden prompt), ``RELAY_URL``
(https, without the token), ``RELAY_NAMESPACE_PREFIX`` (a random field is appended). The token is
never written to disk and is redacted from output.

    RELAY_NAMESPACE_PREFIX=mattypoo/relay-sdk-smoke mise run python:test:relay
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import uuid
from getpass import getpass
from typing import Any
from urllib.parse import urlsplit

from mux_relay import (
    DEFAULT_ENDPOINT,
    CancellationToken,
    Closed,
    MoqtObject,
    RelayConnection,
    RelayPublisher,
    RelayTimeoutError,
    SubscriptionHandlers,
    connect_relay,
    connect_relay_publisher,
)

RUN_DEADLINE = 45.0
RECEIVE_DEADLINE = 10.0


def read_token() -> str:
    token = os.environ.get("RELAY_TOKEN")

    if not token and sys.stdin.isatty():
        token = getpass("Relay token (input hidden): ").strip()
    elif not token:
        token = sys.stdin.readline().strip()

    if not token:
        raise SystemExit("A Relay token is required (RELAY_TOKEN or the prompt)")

    return token


def redact(value: str, token: str) -> str:
    return value.replace(token, "[redacted]")


async def run(endpoint: str, token: str, namespace: list[str], report: Any) -> dict[str, Any]:
    lifetime = CancellationToken()
    loop = asyncio.get_running_loop()
    deadline = loop.call_later(
        RUN_DEADLINE, lambda: lifetime.cancel(RelayTimeoutError(f"Live relay test exceeded {RUN_DEADLINE:.0f} seconds"))
    )
    received: dict[str, list[bytes]] = {"events": [], "status": []}
    errors: list[str] = []
    expected_events = [bytes([0, 255, 128, 1]), b"hello from Relay SDK", b""]
    closing = False
    publisher: RelayPublisher | None = None
    subscriber: RelayConnection | None = None

    def observe_closed(label: str, closed: asyncio.Future[Closed]) -> None:
        def done(future: asyncio.Future[Closed]) -> None:
            if not closing:
                errors.append(f"{label} closed: {future.result().error or 'peer closure'}")

        closed.add_done_callback(done)

    async def wait_for_count(track: str, count: int) -> None:
        until = time.monotonic() + RECEIVE_DEADLINE

        while len(received[track]) < count:
            if errors:
                raise RuntimeError("; ".join(errors))

            lifetime.throw_if_cancelled()

            if time.monotonic() >= until:
                raise TimeoutError(f"Timed out receiving {track}: {len(received[track])}/{count}")

            await asyncio.sleep(0.02)

    try:
        publisher = await connect_relay_publisher(
            endpoint, namespace=namespace, get_token=lambda: token, cancellation=lifetime
        )
        observe_closed("publisher", publisher.closed)
        report({"phase": "publisher connected", "namespace": namespace})
        events = publisher.publish("events", write_timeout=20)
        status = publisher.publish("status", write_timeout=20)
        subscriber = await connect_relay(endpoint, get_token=lambda: token, cancellation=lifetime)
        observe_closed("subscriber", subscriber.closed)
        report({"phase": "subscriber connected"})

        for track in ("events", "status"):

            def on_object(obj: MoqtObject, track: str = track) -> None:
                if obj.status == "normal":
                    received[track].append(obj.payload)

            subscriber.subscribe(
                namespace,
                track,
                SubscriptionHandlers(
                    on_ok=lambda ok, track=track: report({"phase": "subscribed", "track": track}),
                    on_object=on_object,
                    on_error=lambda error, track=track: errors.append(f"{track}: {error.error_code} {error.reason}"),
                    on_subgroup_reset=lambda reset, track=track: errors.append(f"{track} reset: {reset.error}"),
                ),
            )

        for payload in expected_events:
            await events.write(payload)

        await status.write(bytes([42]))
        await wait_for_count("events", 3)
        await wait_for_count("status", 1)
        report(
            {"phase": "received initial payloads", "events": len(received["events"]), "status": len(received["status"])}
        )
        events.close()
        await status.write(bytes([43]))
        await wait_for_count("status", 2)

        if sorted(received["events"]) != sorted(expected_events) or received["status"] != [bytes([42]), bytes([43])]:
            raise RuntimeError(f"Payload mismatch: {received!r}")

        if errors:
            raise RuntimeError("; ".join(errors))

        return {
            "endpoint": endpoint,
            "namespace": namespace,
            "events": len(received["events"]),
            "status": len(received["status"]),
            "binaryAndEmptyPayloads": "passed",
            "independentTrackClose": "passed",
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
