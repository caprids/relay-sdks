#!/usr/bin/env python3
"""Opt-in live check of ``connect_supervised_relay``: one supervised connection per peer that publishes two roots
under one generation, follows the other peer's roots, and moves everything together on a rotation.

The Python twin of ``ts/scripts/probe-supervised-relay.mjs``. Excluded from ``mise run python:check``'s tests.

Two peers, A and B. Each publishes ``<root>/p/<name>`` (track ``ticks``) and ``<root>/host/<name>`` (track
``control``), writing one object per group every 200 ms on each while the relay has demand, follows
``<root>/p`` with ``publishing_roots``, and wants both of the other's tracks with ``generations=True``.
Halfway through, A rotates. The check: each peer holds one connection per generation, both of A's roots carry
the same generation before and after, B keeps receiving A's tracks across the rotation and moves to A's new
generation, and neither peer's roster lists itself.

Environment: ``RELAY_URL`` and ``RELAY_TOKEN`` (both required), ``RELAY_NAMESPACE_PREFIX`` (default
``relay-sdk-probe``), ``PROBE_RUN_S`` (default 8).

    RELAY_URL=https://ord.relay.mux.global/<root> RELAY_TOKEN=... mise run python:probe:supervised-relay
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from caprid_relay import (
    PublishingRootsChange,
    PublishingRootsHandlers,
    SupervisedPublication,
    SupervisedRelay,
    WantHandlers,
    WantSwitchedInfo,
    connect_supervised_relay,
)

TICK = 0.2


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"{name} is required")
    return value


URL = _required("RELAY_URL")
TOKEN = _required("RELAY_TOKEN")
RUN = float(os.environ.get("PROBE_RUN_S", "8"))
ROOT = [
    *(field for field in os.environ.get("RELAY_NAMESPACE_PREFIX", "relay-sdk-probe").split("/") if field),
    f"supervised-{uuid.uuid4()}",
]
START = time.monotonic()


def at() -> int:
    return round((time.monotonic() - START) * 1000)


@dataclass
class Received:
    objects: int = 0
    namespaces: set[str] = field(default_factory=set)
    """The generation fields the want delivered from."""
    switched: int = 0
    restarted: list[str] = field(default_factory=list)
    error: str | None = None


@dataclass
class Peer:
    name: str
    connects: int = 0
    generations: list[dict[str, Any]] = field(default_factory=list)
    roster_ever: set[str] = field(default_factory=set)
    publications: dict[str, SupervisedPublication] = field(default_factory=dict)
    received: dict[str, Received] = field(default_factory=dict)
    tickers: list[asyncio.Task[None]] = field(default_factory=list)
    relay: SupervisedRelay | None = None

    @property
    def supervised(self) -> SupervisedRelay:
        assert self.relay is not None
        return self.relay


async def open_peer(name: str) -> Peer:
    peer = Peer(name)

    def token() -> str:
        peer.connects += 1
        return TOKEN

    peer.relay = await connect_supervised_relay(
        URL,
        get_token=token,
        on_generation=lambda generation: peer.generations.append({"generation": generation, "at": at()}),
    )
    for prefix, track in (("p", "ticks"), ("host", "control")):
        publication = peer.supervised.publish([*ROOT, prefix, name])
        writer = publication.register_track(track)
        peer.publications[prefix] = publication

        async def tick(writer: Any = writer) -> None:
            n = 0
            while True:
                await asyncio.sleep(TICK)
                if writer.has_demand:
                    with contextlib.suppress(Exception):
                        await writer.write_text(f"{name}:{n}")
                    n += 1

        peer.tickers.append(asyncio.ensure_future(tick()))

    def on_changed(change: PublishingRootsChange) -> None:
        peer.roster_ever.update(root.id for root in change.present)

    peer.supervised.publishing_roots([*ROOT, "p"], PublishingRootsHandlers(on_changed=on_changed))
    return peer


def want(peer: Peer, other: str, prefix: str, track: str) -> None:
    result = Received()
    peer.received[f"{prefix}/{other}"] = result

    def on_switched(info: WantSwitchedInfo) -> None:
        result.switched += 1
        result.namespaces.add(info.to_namespace[-1])

    def on_object(_object: object) -> None:
        result.objects += 1

    def on_error(error: BaseException) -> None:
        result.error = repr(error)

    peer.supervised.want(
        f"{prefix}/{other}",
        [*ROOT, prefix, other],
        track,
        WantHandlers(
            on_subscribed=lambda _ok, namespace: result.namespaces.add(namespace[-1]),
            on_object=on_object,
            on_switched=on_switched,
            on_restarted=lambda reason, _namespace: result.restarted.append(reason),
            on_error=on_error,
        ),
        generations=True,
    )


def generation_of(publication: SupervisedPublication) -> str | None:
    namespace = publication.namespace
    return None if namespace is None else namespace[-1]


async def main() -> int:
    print(f"root {'/'.join(ROOT)} on {URL}")
    a, b = await asyncio.gather(open_peer("a"), open_peer("b"))
    for peer, other in ((a, b), (b, a)):
        want(peer, other.name, "p", "ticks")
        want(peer, other.name, "host", "control")
    before = {prefix: generation_of(publication) for prefix, publication in a.publications.items()}
    await asyncio.sleep(RUN / 2)
    at_rotation = {key: value.objects for key, value in b.received.items()}
    a.supervised.rotate()
    await asyncio.sleep(RUN / 2)
    after = {prefix: generation_of(publication) for prefix, publication in a.publications.items()}

    for peer in (a, b):
        for ticker in peer.tickers:
            ticker.cancel()
    await asyncio.gather(a.supervised.close(), b.supervised.close())

    report = {
        "a_generations": a.generations,
        "a_roots_before": before,
        "a_roots_after": after,
        "a_connects": a.connects,
        "b_connects": b.connects,
        "a_roster": sorted(a.roster_ever),
        "b_roster": sorted(b.roster_ever),
        "b_received": {
            key: {
                "at_rotation": at_rotation[key],
                "total": value.objects,
                "namespaces": sorted(value.namespaces),
                "switched": value.switched,
                "restarted": value.restarted,
                "error": value.error,
            }
            for key, value in b.received.items()
        },
        "a_received": {key: {"total": value.objects, "error": value.error} for key, value in a.received.items()},
    }
    print(json.dumps(report, indent=2))

    checks = {
        "A rotated to a new generation": len(a.generations) == 2 and before["p"] != after["p"],
        "both of A's roots share the generation, before and after": before["p"] == before["host"]
        and after["p"] == after["host"],
        "one connection per generation (A: 2, B: 1)": a.connects == 2 and b.connects == 1,
        "B followed A's ticks to the new generation": after["p"] in b.received["p/a"].namespaces,
        "B kept receiving after the rotation": all(
            value.objects > at_rotation[key] + 5 for key, value in b.received.items()
        ),
        "A kept receiving B": all(value.objects > 10 for value in a.received.values()),
        "no roster lists its own root": "a" not in a.roster_ever and "b" not in b.roster_ever,
        "each roster lists the other": "b" in a.roster_ever and "a" in b.roster_ever,
    }
    for check, ok in checks.items():
        print(f"{'PASS' if ok else 'FAIL'} {check}")
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
