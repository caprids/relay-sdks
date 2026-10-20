#!/usr/bin/env python3
"""Publish JSON ticks to a Relay namespace and subscribe to them again.

The SDK usage lives in :func:`run_demo`. It connects a publisher with ``connect_relay_publisher``,
registers a track, connects a supervised subscriber with ``connect_supervised_subscriber`` and wants
the same track, publishes JSON objects one second apart, and prints each object as it comes back
through the relay. It decodes the JSON itself; the SDK only delivers bytes. Unlike the TypeScript
example, no browser is involved: the SDK's native QUIC transport runs on aioquic.

    # After the README's endpoint/namespace setup; enter the token at the hidden prompt.
    mise run python:example:mux-global -- --url "$RELAY_URL" --namespace "$RELAY_NAMESPACE"
"""

from __future__ import annotations

import argparse
import asyncio
import datetime
import json
import os
import sys
import uuid
from collections.abc import Callable, Coroutine
from contextlib import AsyncExitStack
from dataclasses import dataclass
from getpass import getpass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from mux_relay import (
    DEFAULT_ENDPOINT,
    Closed,
    MoqtObject,
    RelayAwaitable,
    RelayError,
    RelayPublisher,
    RelayTrackWriter,
    SubscriptionRequestError,
    WantHandlers,
    connect_relay_publisher,
    connect_supervised_subscriber,
)

# The policies the examples share live one directory up; a script run by path cannot see them otherwise.
EXAMPLES = str(Path(__file__).resolve().parents[1])

if EXAMPLES not in sys.path:
    sys.path.insert(0, EXAMPLES)

from shared.demand_timeout import write_with_retry  # noqa: E402

Log = Callable[[dict[str, Any]], None]


@dataclass(frozen=True, slots=True, kw_only=True)
class Config:
    url: str
    token: str
    namespace: list[str]
    track: str
    mode: str
    count: int
    interval: float
    timeout: float


async def run_demo(config: Config, log: Log) -> dict[str, int]:
    problems: list[str] = []
    broken = asyncio.Event()
    received = 0
    closing = False

    # A failure seen in a callback or background task: recorded, and it wakes a write waiting for demand.
    def report(problem: str) -> None:
        problems.append(problem)
        broken.set()

    # Both connection kinds expose `closed`; a connection ending before we close it is a failure.
    def watch(label: str, closed: RelayAwaitable[Closed]) -> None:
        def done(future: asyncio.Future[Closed]) -> None:
            if not closing:
                report(f"{label} connection closed: {describe(future.result().error)}")

        closed.add_done_callback(done)

    def fail_if_broken() -> None:
        if problems:
            raise RuntimeError("; ".join(problems))

    # The SDK can wait for demand for a whole write deadline. A failure recorded meanwhile (a rejected
    # subscribe, a closed connection) cancels that wait and becomes the run's error, rather than
    # surfacing a deadline later or hiding behind the run's own timeout.
    async def unless_broken(write: Coroutine[Any, Any, None]) -> None:
        writing = asyncio.ensure_future(write)
        interrupted = asyncio.ensure_future(broken.wait())

        try:
            await asyncio.wait({writing, interrupted}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            interrupted.cancel()
            writing.cancel()
            await asyncio.gather(writing, interrupted, return_exceptions=True)

        if writing.cancelled():
            fail_if_broken()

        writing.result()

    # The standard timeout cancels pending work; context managers close every acquired connection.
    deadline = asyncio.timeout(config.timeout)
    try:
        async with deadline, AsyncExitStack() as connections:
            try:
                writer: RelayTrackWriter | None = None
                publisher: RelayPublisher | None = None

                if config.mode != "subscribe":
                    # One publisher connection serves one namespace; register as many tracks on it as needed.
                    try:
                        publisher = await connect_relay_publisher(
                            config.url, namespace=config.namespace, get_token=lambda: config.token
                        )
                    except Exception as error:
                        raise RuntimeError(
                            f"publisher failed to connect (check the token and endpoint): {describe(error)}"
                        ) from error

                    await connections.enter_async_context(publisher)
                    watch("publisher", publisher.closed)
                    log({"event": "publisher connected", "url": config.url, "namespace": "/".join(config.namespace)})
                    writer = publisher.register_track(config.track, write_timeout=15)

                    # The announce leaves with the first registered track; the relay routes subscribes
                    # about a round trip later. Logged so a two-process run shows the publisher's side.
                    def on_announced(future: asyncio.Future[None]) -> None:
                        if not future.cancelled() and future.exception() is None:
                            log({"event": "announced", "namespace": "/".join(config.namespace)})

                    publisher.announced.add_done_callback(on_announced)

                if config.mode != "publish":
                    # The supervised subscriber reconnects on its own, so a lost connection is logged, not a failure.
                    def on_status(status: str, error: BaseException | None) -> None:
                        if status == "reconnecting":
                            log({"event": "subscriber reconnecting", "detail": describe(error)})

                    try:
                        subscriber = await connect_supervised_subscriber(
                            config.url, get_token=lambda: config.token, on_status=on_status
                        )
                    except Exception as error:
                        raise RuntimeError(
                            f"subscriber failed to connect (check the token and endpoint): {describe(error)}"
                        ) from error

                    await connections.enter_async_context(subscriber)
                    watch("subscriber", subscriber.closed)
                    log({"event": "subscriber connected", "url": config.url})

                    def on_object(obj: MoqtObject) -> None:
                        nonlocal received

                        # Status-only objects (end-of-group and end-of-track markers) carry no payload.
                        if obj.status != "normal":
                            return

                        received += 1
                        log(
                            {
                                "event": "received",
                                "group": obj.group_id,
                                "bytes": len(obj.payload),
                                **inspect(obj.payload),
                            }
                        )

                    # The want keeps the track subscribed until the run ends: it retries the announce race (a
                    # subscribe that reaches the relay before the publisher's announce), resubscribes after a
                    # publisher restart, and moves to a new connection. Only a rejection no wait fixes reaches
                    # on_error, and ends the run.
                    subscriber.want(
                        "ticks",
                        config.namespace,
                        config.track,
                        WantHandlers(
                            on_subscribed=lambda ok, namespace: log(
                                {
                                    "event": "subscribed",
                                    "namespace": "/".join(namespace),
                                    "track": config.track,
                                    "alias": ok.track_alias,
                                }
                            ),
                            on_object=on_object,
                            on_error=lambda error: report(f"subscribe rejected: {rejection(error)}"),
                            on_subgroup_reset=lambda reset: report(
                                f"group {reset.group_id} reset: {describe(reset.error)}"
                            ),
                            on_publisher_done=lambda done: log(
                                {"event": "publisher done", "status": done.status_code, "reason": done.reason}
                            ),
                            on_restarted=lambda reason, _namespace: log({"event": "resubscribing", "reason": reason}),
                        ),
                    )

                if writer is not None:
                    # The SDK waits for demand inside write(), up to the track's write deadline. Only a
                    # timeout in that wait proves nothing was sent, so only it writes the same object again.
                    def on_demand_timeout(attempt: int, error: RelayError) -> None:
                        fail_if_broken()
                        log(
                            {
                                "event": "waiting",
                                "detail": "no relay demand within the write deadline; nothing was sent, writing again",
                            }
                        )

                    def on_uncertain_write(error: RelayError) -> None:
                        log(
                            {
                                "event": "write failed",
                                "code": error.code,
                                "phase": error.phase,
                                "detail": "sending had started; the relay may hold the object, so it is not resent",
                            }
                        )

                    for seq in range(1, config.count + 1):
                        payload = json.dumps(
                            {
                                "seq": seq,
                                "sentAt": datetime.datetime.now(datetime.UTC).isoformat(),
                                "message": f"tick {seq} from the mux-relay example",
                            }
                        ).encode("utf-8")
                        fail_if_broken()

                        if not writer.has_demand:
                            log({"event": "waiting", "detail": "no relay demand yet; write() waits for a subscriber"})

                        await unless_broken(
                            write_with_retry(
                                writer, payload, on_retry=on_demand_timeout, on_uncertain=on_uncertain_write
                            )
                        )
                        log({"event": "published", "seq": seq, "bytes": len(payload)})

                        if seq < config.count:
                            await asyncio.sleep(config.interval)

                while config.mode != "publish" and received < config.count:
                    fail_if_broken()
                    await asyncio.sleep(0.1)

                fail_if_broken()
                return {"published": config.count if writer is not None else 0, "received": received}
            finally:
                closing = True
    except TimeoutError as error:
        if deadline.expired():
            raise TimeoutError(f"Run exceeded {config.timeout:g} s") from error
        raise


def inspect(payload: bytes) -> dict[str, Any]:
    """The SDK delivers raw bytes. This expects the JSON written above and shows anything else as text."""
    text = payload.decode("utf-8", errors="replace")

    try:
        parsed = json.loads(text)
        sent_at = parsed.get("sentAt")
        age_ms = None

        if sent_at:
            sent = datetime.datetime.fromisoformat(sent_at)
            age_ms = max(0, int((datetime.datetime.now(datetime.UTC) - sent).total_seconds() * 1000))

        # Only a real one-way latency when the same process published it; across machines this includes clock skew.
        return {"seq": parsed.get("seq"), "ageMs": age_ms}
    except (ValueError, AttributeError):
        return {"text": text[:60]}


def rejection(error: BaseException) -> str:
    """A want's surfaced failure: the relay's code and reason for a rejection, the SDK's code otherwise."""
    if isinstance(error, SubscriptionRequestError):
        return f"code {error.request_error.error_code} {error.request_error.reason}"

    return f"{getattr(error, 'code', type(error).__name__)} {error}"


def describe(error: BaseException | None) -> str:
    if error is None:
        return "no error"

    name = type(error).__name__
    return f"{name}: {error}" if str(error) else name


def read_token() -> str:
    token = os.environ.get("RELAY_TOKEN")

    if not token and sys.stdin.isatty():
        token = getpass("Relay token (input hidden): ").strip()
    elif not token:
        token = sys.stdin.readline().strip()

    return token or ""


def parse_args(argv: list[str]) -> Config:
    parser = argparse.ArgumentParser(description="Publish JSON ticks to a Relay namespace and subscribe to them again.")
    parser.add_argument("--url", default=os.environ.get("RELAY_URL", DEFAULT_ENDPOINT), help="Relay endpoint")
    parser.add_argument(
        "--namespace",
        help="Namespace, slash-separated, relative to the --url path (with no path, include the token's root prefix) "
        "(default RELAY_NAMESPACE_PREFIX or relay-sdk-example, plus a random field)",
    )
    parser.add_argument("--track", default="ticker", help="Track name (default ticker)")
    parser.add_argument("--mode", choices=["both", "publish", "subscribe"], default="both")
    parser.add_argument("--count", type=int, default=5, help="Objects to publish and/or wait for (default 5)")
    parser.add_argument(
        "--interval", type=int, default=1000, help="Delay between published objects in ms (default 1000)"
    )
    parser.add_argument("--timeout", type=int, default=60, help="Deadline for the whole run in seconds (default 60)")
    args = parser.parse_args(argv)

    parts = urlsplit(args.url)
    if parts.scheme != "https":
        parser.error("--url must be an https URL")
    if "jwt=" in parts.query:
        parser.error("Supply the token via RELAY_TOKEN or the prompt, not inside --url")

    for name in ("count", "interval", "timeout"):
        if getattr(args, name) <= 0:
            parser.error(f"--{name} must be a positive integer")

    namespace_text = (
        args.namespace or f"{os.environ.get('RELAY_NAMESPACE_PREFIX', 'relay-sdk-example')}/{uuid.uuid4().hex[:8]}"
    )
    namespace = [field for field in namespace_text.split("/") if field]
    if not namespace:
        parser.error("--namespace must contain at least one field")
    if not args.track:
        parser.error("--track must not be empty")

    token = read_token()
    if not token:
        parser.error("A Relay token is required (RELAY_TOKEN or the prompt)")

    return Config(
        url=args.url,
        token=token,
        namespace=namespace,
        track=args.track,
        mode=args.mode,
        count=args.count,
        interval=args.interval / 1000,
        timeout=float(args.timeout),
    )


def main(argv: list[str] | None = None) -> int:
    config = parse_args(sys.argv[1:] if argv is None else argv)

    def redact(text: str) -> str:
        return text.replace(config.token, "[redacted]")

    def log(entry: dict[str, Any]) -> None:
        event = str(entry.pop("event"))
        fields = " ".join(
            f"{key}={value if isinstance(value, str) else json.dumps(value)}"
            for key, value in entry.items()
            if value is not None
        )
        print(redact(f"{event:<20} {fields}".rstrip()), flush=True)

    log(
        {
            "event": "starting",
            "mode": config.mode,
            "namespace": "/".join(config.namespace),
            "track": config.track,
            "count": config.count,
        }
    )

    try:
        result = asyncio.run(run_demo(config, log))
    except KeyboardInterrupt:
        print(redact("interrupted"), file=sys.stderr)
        return 130
    except BaseException as error:
        print(redact(f"{'failed':<20} error={describe(error)}"), file=sys.stderr)
        return 1

    log({"event": "finished", **result})
    return 0


if __name__ == "__main__":
    sys.exit(main())
