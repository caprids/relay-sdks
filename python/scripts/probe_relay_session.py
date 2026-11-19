#!/usr/bin/env python3
"""Opt-in live check of ``connect_relay``: one connection per peer that publishes several namespaces and subscribes.

The Python twin of ``ts/scripts/probe-relay-session.mjs``. Excluded from ``mise run python:check``'s tests.

Two peers, A and B, each open ONE connection. Each discovers the root, then publishes two namespaces,
``<root>/<name>`` (track ``ticks``) and ``<root>/<name>-host`` (track ``control``), writing one object per
group every 200 ms on each while the relay has demand. Each subscribes to both of the other's tracks, and A
also to its own ``ticks`` (loopback, which the relay refuses). After the run the writes in flight finish
before the connections close, and the summary gives, per subscription, the objects received against the
writes that completed, and the status objects (a track's end) apart from them.

Environment: ``RELAY_URL`` and ``RELAY_TOKEN`` (both required), ``RELAY_NAMESPACE_PREFIX`` (default
``relay-sdk-probe``), ``PROBE_RUN_S`` (default 6, so up to 30 objects per track), ``PROBE_PAYLOADS=1`` to
print every payload received.

    RELAY_URL=https://ord.relay.mux.global/<root> RELAY_TOKEN=... mise run python:probe:relay-session
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from caprid_relay import RelayError, RelayPublication, RelaySession, RelaySubscriptionHandlers, connect_relay
from moq_core import MoqtObject, RequestError

TICK = 0.2


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"{name} is required")
    return value


URL = _required("RELAY_URL")
TOKEN = _required("RELAY_TOKEN")
RUN = float(os.environ.get("PROBE_RUN_S", "6"))
ROOT = [
    *(field for field in os.environ.get("RELAY_NAMESPACE_PREFIX", "relay-sdk-probe").split("/") if field),
    f"session-{uuid.uuid4()}",
]
START = time.monotonic()


def at() -> int:
    return round((time.monotonic() - START) * 1000)


@dataclass
class Peer:
    name: str
    session: RelaySession
    discovery: list[str] = field(default_factory=list)
    announced_at: list[asyncio.Future[int]] = field(default_factory=list)
    sent: dict[str, int] = field(default_factory=dict)
    """Objects whose write completed, by namespace suffix."""
    failed: list[str] = field(default_factory=list)
    """Writes that raised: the object, the error's code and phase."""
    tasks: list[asyncio.Task[Any]] = field(default_factory=list)
    """Discovery and the tickers, which stop before the connection closes."""
    writes: set[asyncio.Task[None]] = field(default_factory=set)


@dataclass
class Result:
    label: str
    sender: Peer
    suffix: str
    objects: int = 0
    """Objects with a payload, which the publisher wrote; a track's end arrives as a status object, not one of them."""
    statuses: list[str] = field(default_factory=list)
    first: str | None = None
    error: str | None = None
    payloads: list[str] = field(default_factory=list)


async def open_peer(name: str) -> Peer:
    session = await connect_relay(URL, get_token=lambda: TOKEN)
    peer = Peer(name, session)

    async def discover() -> None:
        with contextlib.suppress(Exception):
            async with session.namespace_events(ROOT) as events:
                async for event in events:
                    if event.type != "loaded" and event.namespace is not None:
                        peer.discovery.append(f"{event.type} {'/'.join(event.namespace[len(ROOT) :])}")

    peer.tasks.append(asyncio.ensure_future(discover()))
    return peer


def publish(peer: Peer, suffix: str, track: str) -> RelayPublication:
    publication = peer.session.publish([*ROOT, suffix])
    writer = publication.register_track(track)

    async def announced() -> int:
        await publication.announced
        return at()

    peer.announced_at.append(asyncio.ensure_future(announced()))

    peer.sent[suffix] = 0

    async def write(n: int) -> None:
        try:
            await writer.write(f"{suffix}:{n}".encode())
        except RelayError as error:
            peer.failed.append(f"{suffix}:{n} {error.code} in {error.phase} at {at()} ms")
        else:
            peer.sent[suffix] += 1

    async def tick() -> None:
        n = 0
        while True:
            await asyncio.sleep(TICK)
            if writer.has_demand:
                writing = asyncio.ensure_future(write(n))
                peer.writes.add(writing)
                writing.add_done_callback(peer.writes.discard)
                n += 1

    peer.tasks.append(asyncio.ensure_future(tick()))
    return publication


def subscribe(peer: Peer, sender: Peer, suffix: str, track: str, label: str) -> Result:
    result = Result(label, sender, suffix)

    def on_object(obj: MoqtObject) -> None:
        if obj.status != "normal":
            result.statuses.append(f"{obj.status}@g{obj.group_id}")
            return
        result.objects += 1
        result.payloads.append(f"{obj.payload.decode()}@g{obj.group_id}")
        if result.first is None:
            result.first = obj.payload.decode()

    def on_error(error: RequestError) -> None:
        result.error = f"request error {error.error_code}"

    peer.session.subscribe([*ROOT, suffix], track, RelaySubscriptionHandlers(on_object=on_object, on_error=on_error))
    return result


async def main() -> None:
    print(f"root {'/'.join(ROOT)} on {URL}")
    a, b = await asyncio.gather(open_peer("a"), open_peer("b"))
    await asyncio.sleep(0.3)

    for peer in (a, b):
        publish(peer, peer.name, "ticks")
        publish(peer, f"{peer.name}-host", "control")

    announced_at = await asyncio.gather(*a.announced_at, *b.announced_at)
    print(f"all four namespaces announced by {max(announced_at)} ms")

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and not (len(a.discovery) >= 2 and len(b.discovery) >= 2):
        await asyncio.sleep(0.025)

    results = [
        subscribe(a, b, "b", "ticks", "a <- b/ticks"),
        subscribe(a, b, "b-host", "control", "a <- b-host/control"),
        subscribe(b, a, "a", "ticks", "b <- a/ticks"),
        subscribe(b, a, "a-host", "control", "b <- a-host/control"),
        subscribe(a, a, "a", "ticks", "a <- a/ticks (loopback)"),
    ]
    await asyncio.sleep(RUN)

    # Stop writing and let the writes in flight finish, so every object counted as sent had its chance to arrive.
    for peer in (a, b):
        for task in peer.tasks:
            task.cancel()
    await asyncio.gather(*a.writes, *b.writes, return_exceptions=True)
    await asyncio.sleep(0.5)
    await asyncio.gather(a.session.close(), b.session.close())

    print("\n=== summary ===")
    for peer in (a, b):
        print(f"[{peer.name}] discovery saw: {', '.join(peer.discovery) or 'nothing'}")
        print(f"[{peer.name}] failed writes: {', '.join(peer.failed) or 'none'}")
    for r in results:
        sent = r.sender.sent[r.suffix]
        statuses = ", ".join(r.statuses) or "none"
        print(
            f"{r.label}: objects {r.objects} of {sent} sent, first {r.first!r}, status objects {statuses},"
            f" error {r.error or 'none'}"
        )
        repeated = sorted({payload for payload in r.payloads if r.payloads.count(payload) > 1})
        if repeated or os.environ.get("PROBE_PAYLOADS"):
            print(f"  repeated: {repeated}; payloads: {r.payloads}")


if __name__ == "__main__":
    asyncio.run(main())
