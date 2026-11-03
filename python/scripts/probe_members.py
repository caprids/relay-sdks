#!/usr/bin/env python3
"""Opt-in live probe of member discovery (issue #422). Excluded from ``mise run python:check``'s tests.

The Python twin of ``ts/scripts/probe-members.mjs``: the same flags, environment and JSON lines, so a
lane script compares a TypeScript run and a Python run line for line. Who is present under a namespace
root, from the subscriber's side, across publisher rotations, crashes, clean departures and the
subscriber's own rotations and reconnects.

Each member p0..p{N-1} is a child process (this script with ``--publisher-child``) running a supervised
publisher on ``[*root, id]`` with one track, ``beat``, written every 100 ms. The subscriber runs here, in
one of two views:

- ``--view members``: ``SupervisedSubscriber.members(root, ...)``: joins and leaves as the SDK reports them.
- ``--view raw``: ``connect_relay_subscriber`` and ``namespace_events`` on the root, unsupervised; the probe
  folds the generation namespaces into members itself (a member joins on its first namespace and leaves
  on its last). Works without the ``loaded`` event, which it waits for only briefly.

Every event is one JSON line on stdout with ``t``, ms since the probe started; the last line is
``{"summary": {...}}``. ``duplicates`` counts the joins and leaves the ground truth does not explain: a
join for a member already present, a second join (no publisher here ever restarts), a leave for a member
not present or whose publisher is still up, and any event for an id with no publisher. The exit code is 0
unless the probe itself fails.

Flags (durations in ms, as in the TypeScript probe)::

    --view members|raw (default members)    --members N (default 5)     --root a/b (default <uuid>/p)
    --subscriber-after                       start the subscriber once every publisher is up (default: before)
    --rotate-publishers K                    rotate every publisher K times, one at a time
    --rotate-subscriber K                    SupervisedSubscriber.rotate() K times (members view)
    --reconnect-subscriber                   close the subscriber's QUIC transport and refuse dials for --outage-ms
    --outage-ms MS (default 3000)            --depart-during-outage M   M publishers close during the outage
    --kill-publisher M                       SIGKILL M publishers        --close-publisher M   M publishers close
    --want-generations                       want [*root, 'p0'] track `beat` with generations=True
    --latency --trials T (default 5)         T new publishers: generation-up to joined (members) or announced
                                             (raw), and a fresh subscriber's connect-to-loaded per trial
    --settle-ms MS (default 15000)           how long to wait for each expected join or leave
    --end close|keep (default close)         close every remaining publisher and wait for its leave, or not
    --out PATH                               also write {argv, events, summary} as one JSON file

Environment: ``RELAY_URL`` (a room-rooted endpoint), ``RELAY_SUB_URL`` (default ``RELAY_URL``),
``RELAY_TOKEN`` or ``RELAY_ANONYMOUS=1``. A local relay's trust root is ``RELAY_CA_FILE`` (a PEM file), as
for the other Python scripts: aioquic cannot pin ``RELAY_CERTIFICATE_HASH``.

    RELAY_URL=https://ord.relay.mux.global/mattypoo mise run python:probe:members -- --members 5 --rotate-publishers 3
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import math
import os
import signal
import sys
import time
import traceback
import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from caprid_relay import (
    ConnectingTransport,
    PublisherGeneration,
    SupervisedStatus,
    SupervisedSubscriber,
    SupervisionBackoff,
    WantHandlers,
    WantSwitchedInfo,
    connect_relay_subscriber,
    connect_supervised_publisher,
    connect_supervised_subscriber,
)
from moq_core import SubscribeOk

ROTATE_SETTLE = 1.0
RAW_LOADED_WAIT_MS = 3000
ROTATION_WAIT = 15.0
OUTAGE_RETRY = 0.25
BEAT = 0.1
PUBLISHER_UP_WAIT_MS = 30_000

Event = dict[str, Any]


def parse_flags(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Live probe of member discovery", allow_abbrev=False)
    parser.add_argument("--publisher-child")
    parser.add_argument("--view", default="members")
    parser.add_argument("--members", default="5")
    parser.add_argument("--root")
    parser.add_argument("--subscriber-after", action="store_true")
    parser.add_argument("--rotate-publishers", default="0")
    parser.add_argument("--rotate-subscriber", default="0")
    parser.add_argument("--reconnect-subscriber", action="store_true")
    parser.add_argument("--outage-ms", default="3000")
    parser.add_argument("--depart-during-outage", default="0")
    parser.add_argument("--kill-publisher", default="0")
    parser.add_argument("--close-publisher", default="0")
    parser.add_argument("--want-generations", action="store_true")
    parser.add_argument("--latency", action="store_true")
    parser.add_argument("--trials", default="5")
    parser.add_argument("--settle-ms", default="15000")
    parser.add_argument("--end", default="close")
    parser.add_argument("--out")
    return parser.parse_args(argv)


@dataclass(frozen=True, slots=True)
class Relay:
    """Where and how every connection in the run dials."""

    url: str
    sub_url: str
    token: str | None
    ca_file: str | None
    root: tuple[str, ...]

    def get_token(self) -> str | None:
        return self.token

    def dial(self, url: str, protocols: Sequence[str]) -> ConnectingTransport:
        from aioquic.quic.configuration import QuicConfiguration

        from caprid_relay.quic import connect_quic

        configuration = QuicConfiguration(is_client=True, cafile=self.ca_file) if self.ca_file else None
        return connect_quic(url, protocols, configuration=configuration)

    def describe(self, error: BaseException) -> str:
        code = getattr(error, "code", None)
        text = f"{code}: {error}" if isinstance(code, str) else str(error) or type(error).__name__
        return text.replace(self.token, "[redacted]") if self.token else text


def read_relay(flags: argparse.Namespace) -> Relay:
    url = os.environ.get("RELAY_URL")
    if not url:
        raise SystemExit("RELAY_URL is required")
    token = os.environ.get("RELAY_TOKEN") or None
    if not token and os.environ.get("RELAY_ANONYMOUS") != "1":
        raise SystemExit("Set RELAY_TOKEN, or RELAY_ANONYMOUS=1 for a local relay")
    if os.environ.get("RELAY_CERTIFICATE_HASH") and not os.environ.get("RELAY_CA_FILE"):
        raise SystemExit("aioquic cannot pin RELAY_CERTIFICATE_HASH; set RELAY_CA_FILE to the relay's certificate")
    root = tuple(field for field in flags.root.split("/") if field) if flags.root else (str(uuid.uuid4()), "p")
    return Relay(url, os.environ.get("RELAY_SUB_URL") or url, token, os.environ.get("RELAY_CA_FILE"), root)


def now_ms() -> float:
    # CLOCK_MONOTONIC is system-wide, so a child's timestamps share the probe's clock.
    return time.monotonic() * 1000


async def run_publisher(relay: Relay, member: str) -> None:
    def send(message: dict[str, Any]) -> None:
        sys.stdout.write(json.dumps(message) + "\n")
        sys.stdout.flush()

    def on_generation(generation: PublisherGeneration) -> None:
        send({"type": "generation-up", "generation": generation.generation, "at": now_ms()})

    def on_status(status: SupervisedStatus, error: BaseException | None) -> None:
        send({"type": "status", "status": status, **({"error": relay.describe(error)} if error else {})})

    publisher = await connect_supervised_publisher(
        relay.url,
        namespace=(*relay.root, member),
        get_token=relay.get_token,
        create_transport=relay.dial,
        on_generation=on_generation,
        on_status=on_status,
    )
    beat = publisher.register_track("beat")
    closing = False

    async def follow_commands() -> None:
        nonlocal closing
        loop = asyncio.get_running_loop()
        reader = asyncio.StreamReader()
        await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), sys.stdin)
        while line := await reader.readline():
            command = json.loads(line)["cmd"]
            if command == "rotate":
                publisher.rotate()
            elif command == "close":
                closing = True
                await publisher.close()
                send({"type": "closed", "at": now_ms()})
                os._exit(0)
        os._exit(0)

    commands = asyncio.ensure_future(follow_commands())
    n = 0
    while not closing and not commands.done():
        if beat.has_demand:
            with contextlib.suppress(Exception):
                await beat.write_text(str(n))
        await asyncio.sleep(BEAT)
        n += 1
    await commands


@dataclass(slots=True)
class Departure:
    kind: str
    from_: int
    event: Event


@dataclass(slots=True, eq=False)
class Publisher:
    id: str
    process: asyncio.subprocess.Process
    generations: list[str] = field(default_factory=list)
    departure: Departure | None = None
    exited: bool = False

    def send(self, cmd: str) -> None:
        assert self.process.stdin is not None
        self.process.stdin.write((json.dumps({"cmd": cmd}) + "\n").encode())


@dataclass(slots=True)
class Sink:
    """Where a view reports; the main view reports everything, a trial's fresh view only its errors."""

    loaded: Callable[[list[str]], object] | None = None
    joined: Callable[[str, dict[str, Any]], object] | None = None
    left: Callable[[str], object] | None = None
    raw: Callable[[str, str, str | None], object] | None = None
    error: Callable[[BaseException], object] | None = None


@dataclass(frozen=True, slots=True)
class Loaded:
    ids: list[str]
    connect_to_loaded_ms: float


@dataclass(slots=True, eq=False)
class View:
    transports: list[ConnectingTransport]
    loaded: asyncio.Future[Loaded]
    close: Callable[[], None]
    supervised: SupervisedSubscriber | None = None


def count(flags: argparse.Namespace, name: str) -> int:
    value = getattr(flags, name.replace("-", "_"))
    if not value.isdigit():
        raise ValueError(f"--{name} must be a whole number")
    return int(value)


async def within(awaitable: Awaitable[Any], ms: float) -> Any:
    """The awaitable's result, or ``None`` once ``ms`` pass; never cancels what it waits for."""
    try:
        return await asyncio.wait_for(asyncio.shield(asyncio.ensure_future(awaitable)), ms / 1000)
    except TimeoutError:
        return None


class Probe:
    def __init__(self, relay: Relay, flags: argparse.Namespace, argv: Sequence[str]) -> None:
        self.relay = relay
        self.flags = flags
        self.argv = list(argv)
        self.view: str = flags.view
        self.start = now_ms()
        self.events: list[Event] = []
        self.waiters: set[Callable[[Event], None]] = set()
        self.publishers: dict[str, Publisher] = {}
        self.next_index = 0
        self.tasks: set[asyncio.Future[Any]] = set()
        self.present: set[str] = set()
        self.ever_joined: set[str] = set()
        self.counts = {"joined": 0, "left": 0}
        self.duplicates: list[Event] = []
        self.outage_until = 0.0
        self.settle_ms = 0

    def since_start(self, ms: float) -> float:
        return round(ms - self.start, 1)

    def emit(self, type_: str, fields: dict[str, Any] | None = None, t: float | None = None) -> Event:
        event = {"t": self.since_start(now_ms()) if t is None else t, "type": type_, **(fields or {})}
        self.events.append(event)
        sys.stdout.write(json.dumps(event) + "\n")
        sys.stdout.flush()
        for waiter in list(self.waiters):
            waiter(event)
        return event

    async def wait_for(self, match: Callable[[Event], bool], timeout_ms: float, from_: int = 0) -> Event | None:
        past = next((event for event in self.events[from_:] if match(event)), None)
        if past is not None:
            return past
        found: asyncio.Future[Event] = asyncio.get_running_loop().create_future()

        def waiter(event: Event) -> None:
            if match(event) and not found.done():
                found.set_result(event)

        self.waiters.add(waiter)
        try:
            return await within(found, timeout_ms)
        finally:
            self.waiters.discard(waiter)

    async def expect(
        self, what: str, match: Callable[[Event], bool], timeout_ms: float, from_: int = 0
    ) -> Event | None:
        event = await self.wait_for(match, timeout_ms, from_)
        if event is None:
            self.emit("timeout", {"waitingFor": what, "afterMs": timeout_ms})
        return event

    def spawn(self, coroutine: Awaitable[Any]) -> None:
        task = asyncio.ensure_future(coroutine)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    def kill_children(self) -> None:
        for publisher in self.publishers.values():
            if not publisher.exited:
                with contextlib.suppress(ProcessLookupError):
                    publisher.process.kill()

    async def start_publisher(self) -> Publisher:
        member = f"p{self.next_index}"
        self.next_index += 1
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            __file__,
            "--publisher-child",
            member,
            "--root",
            "/".join(self.relay.root),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
        )
        publisher = Publisher(member, process)
        self.publishers[member] = publisher
        self.spawn(self.follow_publisher(publisher))
        return publisher

    async def follow_publisher(self, publisher: Publisher) -> None:
        member = publisher.id
        assert publisher.process.stdout is not None
        async for line in publisher.process.stdout:
            message = json.loads(line)
            if message["type"] == "generation-up":
                publisher.generations.append(message["generation"])
                fields = {"id": member, "generation": message["generation"], "first": len(publisher.generations) == 1}
                self.emit("publisher-up", fields, self.since_start(message["at"]))
            elif message["type"] == "status":
                fields = {"id": member, "status": message["status"]}
                self.emit("publisher-status", fields | ({"error": message["error"]} if "error" in message else {}))
            elif message["type"] == "closed":
                self.emit("publisher-closed", {"id": member}, self.since_start(message["at"]))
        code = await publisher.process.wait()
        publisher.exited = True
        if publisher.departure is None:
            exit_code, exit_signal = (None, signal.Signals(-code).name) if code < 0 else (code, None)
            self.emit("publisher-exit", {"id": member, "code": exit_code, "signal": exit_signal})

    def alive(self) -> list[Publisher]:
        return [publisher for publisher in self.publishers.values() if publisher.departure is None]

    def pick_departures(self, n: int) -> list[Publisher]:
        return list(reversed(self.alive()))[:n]

    def depart(self, publisher: Publisher, kind: str) -> None:
        from_ = len(self.events)
        event = self.emit("kill" if kind == "kill" else "close", {"id": publisher.id, "kind": kind})
        publisher.departure = Departure(kind, from_, event)
        if kind == "kill":
            publisher.process.kill()
        else:
            publisher.send("close")

    async def await_left(self, departed: list[Publisher]) -> None:
        def waits(publisher: Publisher) -> Awaitable[Event | None]:
            assert publisher.departure is not None
            return self.expect(
                f"left {publisher.id}",
                lambda e: e["type"] == "left" and e.get("id") == publisher.id,
                self.settle_ms,
                publisher.departure.from_,
            )

        await asyncio.gather(*(waits(publisher) for publisher in departed))

    def observe(self, type_: str, member: str, fields: dict[str, Any] | None = None) -> None:
        publisher = self.publishers.get(member)
        unexplained = None
        if publisher is None:
            unexplained = "no such publisher"
        elif type_ == "joined" and member in self.present:
            unexplained = "already present"
        elif type_ == "joined" and member in self.ever_joined:
            unexplained = "joined again"
        elif type_ == "left" and member not in self.present:
            unexplained = "not present"
        elif type_ == "left" and publisher.departure is None:
            unexplained = "publisher still up"
        self.counts[type_] += 1
        if type_ == "joined":
            self.present.add(member)
            self.ever_joined.add(member)
        else:
            self.present.discard(member)
        extra = {"unexplained": unexplained} if unexplained else {}
        event = self.emit(type_, {"id": member, "view": self.view, **(fields or {}), **extra})
        if unexplained:
            self.duplicates.append(event)

    async def open_view(self, sink: Sink, *, primary: bool = False) -> View:
        relay = self.relay
        transports: list[ConnectingTransport] = []

        def create_transport(url: str, protocols: Sequence[str]) -> ConnectingTransport:
            if primary and now_ms() < self.outage_until:
                self.emit("subscriber-dial", {"refused": True})
                raise ConnectionRefusedError("probe-members outage: dial refused")
            opened = relay.dial(url, protocols)
            transports.append(opened)
            if primary:
                self.emit("subscriber-dial", {"connection": len(transports)})
            return opened

        connect_at = now_ms()
        loaded: asyncio.Future[Loaded] = asyncio.get_running_loop().create_future()

        def resolve_loaded(ids: list[str]) -> None:
            if not loaded.done():
                loaded.set_result(Loaded(ids, round(now_ms() - connect_at, 1)))

        if self.view == "members":
            return await self.open_members_view(sink, primary, create_transport, transports, loaded, resolve_loaded)

        subscriber = await connect_relay_subscriber(
            relay.sub_url, get_token=relay.get_token, create_transport=create_transport
        )
        namespaces = subscriber.namespace_events(relay.root)
        generations: dict[str, set[str]] = {}

        async def fold() -> None:
            try:
                async for event in namespaces:
                    if event.type == "loaded" or event.namespace is None:
                        ids = list(generations)
                        resolve_loaded(ids)
                        if sink.loaded:
                            sink.loaded(ids)
                        continue
                    rest = event.namespace[len(relay.root) :]
                    if not rest or len(rest) > 2:
                        continue
                    member = rest[0]
                    generation = rest[1] if len(rest) == 2 else None
                    if sink.raw:
                        sink.raw(event.type, member, generation)
                    held = generations.get(member, set())
                    if event.type == "announced":
                        held.add(generation or "")
                        generations[member] = held
                        if len(held) == 1 and sink.joined:
                            sink.joined(member, {})
                    elif (generation or "") in held:
                        held.discard(generation or "")
                        if not held:
                            del generations[member]
                            if sink.left:
                                sink.left(member)
            except Exception as error:
                if sink.error:
                    sink.error(error)

        self.spawn(fold())

        def close() -> None:
            namespaces.close()
            subscriber.close()

        return View(transports, loaded, close)

    async def open_members_view(
        self,
        sink: Sink,
        primary: bool,
        create_transport: Callable[[str, Sequence[str]], ConnectingTransport],
        transports: list[ConnectingTransport],
        loaded: asyncio.Future[Loaded],
        resolve_loaded: Callable[[list[str]], None],
    ) -> View:
        from caprid_relay import MembersChange, MembersHandlers

        relay = self.relay

        def on_status(status: SupervisedStatus, error: BaseException | None) -> None:
            if primary:
                described = {"error": relay.describe(error)} if error else {}
                self.emit("subscriber-status", {"status": status, **described})

        reconnect = self.flags.reconnect_subscriber
        supervised = await connect_supervised_subscriber(
            relay.sub_url,
            get_token=relay.get_token,
            create_transport=create_transport,
            reconnect=SupervisionBackoff(initial_delay=OUTAGE_RETRY, max_delay=OUTAGE_RETRY)
            if primary and reconnect
            else None,
            on_status=on_status,
        )

        def on_changed(change: MembersChange) -> None:
            if change.initial:
                ids = [member.id for member in change.joined]
                resolve_loaded(ids)
                if sink.loaded:
                    sink.loaded(ids)
            for member in change.joined:
                if sink.joined:
                    sink.joined(member.id, {"initial": True} if change.initial else {})
            for member in change.left:
                if sink.left:
                    sink.left(member.id)

        def on_error(error: BaseException) -> None:
            if sink.error:
                sink.error(error)

        members = supervised.members(relay.root, MembersHandlers(on_changed=on_changed, on_error=on_error))

        def close() -> None:
            members.close()
            supervised.close()

        return View(transports, loaded, close, supervised)

    async def run(self) -> None:
        flags = self.flags
        view = self.view
        if view not in ("members", "raw"):
            raise ValueError("--view must be members or raw")
        if flags.end not in ("close", "keep"):
            raise ValueError("--end must be close or keep")
        members = count(flags, "members")
        rotate_publishers = count(flags, "rotate-publishers")
        rotate_subscriber = count(flags, "rotate-subscriber")
        outage_ms = count(flags, "outage-ms")
        depart_during_outage = count(flags, "depart-during-outage")
        kill_count = count(flags, "kill-publisher")
        close_count = count(flags, "close-publisher")
        trials = count(flags, "trials") if flags.latency else 0
        self.settle_ms = settle_ms = count(flags, "settle-ms")
        loaded_wait_ms = RAW_LOADED_WAIT_MS if view == "raw" else settle_ms
        if view == "raw" and (rotate_subscriber or flags.reconnect_subscriber):
            raise ValueError(
                "--rotate-subscriber and --reconnect-subscriber need --view members: the raw view is unsupervised"
            )
        if depart_during_outage and not flags.reconnect_subscriber:
            raise ValueError("--depart-during-outage needs --reconnect-subscriber")
        if depart_during_outage + kill_count + close_count > members:
            raise ValueError("More departures than --members")
        if flags.want_generations and members < 1:
            raise ValueError("--want-generations needs a member p0")

        def main_joined(member: str, fields: dict[str, Any]) -> None:
            self.observe("joined", member, fields)

        def main_raw(type_: str, member: str, generation: str | None) -> None:
            self.emit(type_, {"id": member} | ({"generation": generation} if generation is not None else {}))

        main_sink = Sink(
            loaded=lambda ids: self.emit("loaded", {"view": view, "members": ids}),
            joined=main_joined,
            left=lambda member: self.observe("left", member),
            raw=main_raw,
            error=lambda error: self.emit("subscriber-error", {"error": self.relay.describe(error)}),
        )
        main: View | None = None
        connect_to_loaded_ms: float | None = None
        loaded_members: list[str] | None = None

        async def start_subscriber() -> View:
            nonlocal connect_to_loaded_ms, loaded_members
            opened = await self.open_view(main_sink, primary=True)
            loaded = await within(opened.loaded, loaded_wait_ms)
            if loaded is None:
                self.emit("loaded-missing", {"view": view, "afterMs": loaded_wait_ms})
            else:
                loaded_members, connect_to_loaded_ms = loaded.ids, loaded.connect_to_loaded_ms
            return opened

        async def start_members(n: int) -> list[tuple[Publisher, int]]:
            started = []
            for _ in range(n):
                from_ = len(self.events)
                started.append((await self.start_publisher(), from_))

            def up(publisher: Publisher, from_: int) -> Awaitable[Event | None]:
                return self.expect(
                    f"publisher-up {publisher.id}",
                    lambda e: e["type"] == "publisher-up" and e.get("id") == publisher.id,
                    PUBLISHER_UP_WAIT_MS,
                    from_,
                )

            await asyncio.gather(*(up(publisher, from_) for publisher, from_ in started))
            return started

        async def await_joined(started: list[tuple[Publisher, int]]) -> None:
            def joined(publisher: Publisher, from_: int) -> Awaitable[Event | None]:
                return self.expect(
                    f"joined {publisher.id}",
                    lambda e: e["type"] == "joined" and e.get("id") == publisher.id,
                    settle_ms,
                    from_,
                )

            await asyncio.gather(*(joined(publisher, from_) for publisher, from_ in started))

        if not flags.subscriber_after:
            main = await start_subscriber()
        initial = await start_members(members)
        if flags.subscriber_after:
            main = await start_subscriber()
        assert main is not None
        await await_joined(initial)

        want = {"objects": 0, "subscribed": 0, "switched": 0, "restarted": 0, "errors": 0}
        want_subscriber: SupervisedSubscriber | None = None
        if flags.want_generations:
            want_subscriber = main.supervised or await connect_supervised_subscriber(
                self.relay.sub_url, get_token=self.relay.get_token, create_transport=self.relay.dial
            )

            def on_object(_: object) -> None:
                want["objects"] += 1

            def on_subscribed(_: SubscribeOk, namespace: tuple[str, ...]) -> None:
                want["subscribed"] += 1
                self.emit("want-subscribed", {"generation": namespace[-1], "objects": want["objects"]})

            def on_switched(info: WantSwitchedInfo) -> None:
                want["switched"] += 1
                fields = {"from": info.from_namespace[-1], "to": info.to_namespace[-1], "objects": want["objects"]}
                self.emit("want-switched", fields)

            def on_restarted(reason: str, namespace: tuple[str, ...]) -> None:
                want["restarted"] += 1
                fields = {"reason": reason, "generation": namespace[-1], "objects": want["objects"]}
                self.emit("want-restarted", fields)

            def on_want_error(error: BaseException) -> None:
                want["errors"] += 1
                self.emit("want-error", {"error": self.relay.describe(error)})

            want_subscriber.want(
                "beat",
                (*self.relay.root, "p0"),
                "beat",
                WantHandlers(
                    on_object=on_object,
                    on_subscribed=on_subscribed,
                    on_switched=on_switched,
                    on_restarted=on_restarted,
                    on_error=on_want_error,
                ),
                generations=True,
            )
            await self.expect("want-subscribed", lambda e: e["type"] == "want-subscribed", settle_ms)

        publisher_rotations = 0
        for round_ in range(1, rotate_publishers + 1):
            for publisher in self.alive():
                from_ = len(self.events)
                rotate = self.emit("rotate", {"id": publisher.id, "round": round_})
                publisher.send("rotate")
                up = await self.expect(
                    f"rotation of {publisher.id}",
                    lambda e, member=publisher.id: e["type"] == "publisher-up" and e.get("id") == member,
                    ROTATION_WAIT * 1000,
                    from_,
                )
                if up is not None:
                    publisher_rotations += 1
                    self.emit(
                        "rotated", {"id": publisher.id, "round": round_, "rotateMs": round(up["t"] - rotate["t"], 1)}
                    )
                await asyncio.sleep(ROTATE_SETTLE)

        subscriber_rotations = []
        for n in range(1, rotate_subscriber + 1):
            assert main.supervised is not None
            old = main.transports[-1]
            from_ = len(self.events)
            rotate = self.emit("subscriber-rotate", {"n": n})
            main.supervised.rotate()
            dialed = await self.expect(
                "subscriber rotation dial", lambda e: e["type"] == "subscriber-dial", ROTATION_WAIT * 1000, from_
            )

            async def settle(awaitable: Awaitable[Any], rotate: Event = rotate) -> float | None:
                if await within(_settled(awaitable), ROTATION_WAIT * 1000) is None:
                    return None
                return round(now_ms() - self.start - rotate["t"], 1)

            new_up_ms = await settle(main.transports[-1].ready) if dialed is not None else None
            old_closed_ms = await settle(old.transport.wait_closed())
            subscriber_rotations.append({"n": n, "newUpMs": new_up_ms, "oldClosedMs": old_closed_ms})
            self.emit("subscriber-rotated", {"n": n, "newUpMs": new_up_ms, "oldClosedMs": old_closed_ms})
            await asyncio.sleep(ROTATE_SETTLE)

        reconnect = None
        if flags.reconnect_subscriber:
            from_ = len(self.events)
            self.outage_until = now_ms() + outage_ms
            cut = self.emit("subscriber-reconnect", {"phase": "cut", "outageMs": outage_ms})
            main.transports[-1].transport.close(0, "probe-members outage")
            departed = self.pick_departures(depart_during_outage)
            for publisher in departed:
                self.depart(publisher, "outage-close")
            live = await self.expect(
                "subscriber live after outage",
                lambda e: e["type"] == "subscriber-status" and e.get("status") == "live",
                outage_ms + settle_ms,
                from_,
            )
            reconnect = {"outageMs": round(live["t"] - cut["t"], 1) if live is not None else None}
            self.emit("subscriber-reconnect", {"phase": "live", **reconnect})
            await self.await_left(departed)

        killed = self.pick_departures(kill_count)
        for publisher in killed:
            self.depart(publisher, "kill")
        await self.await_left(killed)
        closed = self.pick_departures(close_count)
        for publisher in closed:
            self.depart(publisher, "close")
        await self.await_left(closed)

        latency_trials: list[dict[str, Any]] = []
        for trial in range(1, trials + 1):
            [(publisher, from_)] = await start_members(1)
            member = publisher.id
            up = next((e for e in self.events[from_:] if e["type"] == "publisher-up" and e.get("id") == member), None)
            seen_type = "joined" if view == "members" else "announced"
            seen = await self.expect(
                f"{seen_type} {member}",
                lambda e, seen_type=seen_type, member=member: e["type"] == seen_type and e.get("id") == member,
                settle_ms,
                from_,
            )
            fresh = await self.open_view(
                Sink(error=lambda error: self.emit("trial-subscriber-error", {"error": self.relay.describe(error)}))
            )
            loaded = await within(fresh.loaded, loaded_wait_ms)
            fresh.close()
            result = {
                "trial": trial,
                "id": member,
                "joinMs": round(seen["t"] - up["t"], 1) if up is not None and seen is not None else None,
                "connectToLoadedMs": loaded.connect_to_loaded_ms if loaded is not None else None,
                "loadedHasAll": all(p.id in loaded.ids for p in self.alive()) if loaded is not None else None,
            }
            latency_trials.append(result)
            self.emit("latency-trial", result)

        if flags.end == "close":
            ended = self.alive()
            for publisher in ended:
                self.depart(publisher, "end-close")
            await self.await_left(ended)

        def departure_row(publisher: Publisher) -> dict[str, Any]:
            assert publisher.departure is not None
            departure = publisher.departure
            left = next(
                (e for e in self.events[departure.from_ :] if e["type"] == "left" and e.get("id") == publisher.id), None
            )
            return {
                "id": publisher.id,
                "kind": departure.kind,
                "departToLeftMs": round(left["t"] - departure.event["t"], 1) if left is not None else None,
            }

        departed_publishers = sorted(
            (p for p in self.publishers.values() if p.departure is not None),
            key=lambda p: p.departure.event["t"] if p.departure is not None else 0,
        )
        departures = [departure_row(publisher) for publisher in departed_publishers]

        def stats(values: list[float | None]) -> dict[str, Any]:
            ranked = sorted(value for value in values if value is not None)

            def rank(p: float) -> float | None:
                if not ranked:
                    return None
                return ranked[max(0, min(len(ranked) - 1, math.ceil(p * len(ranked)) - 1))]

            return {"n": len(ranked), "median": rank(0.5), "p95": rank(0.95)}

        summary: dict[str, Any] = {
            "view": view,
            "root": list(self.relay.root),
            "joined": self.counts["joined"],
            "left": self.counts["left"],
            "duplicates": len(self.duplicates),
            "duplicateEvents": self.duplicates,
            "loadedMembers": loaded_members,
            "connectToLoadedMs": connect_to_loaded_ms,
            "departToLeftMs": [row["departToLeftMs"] for row in departures],
            "departures": departures,
            "missingLeft": [row["id"] for row in departures if row["departToLeftMs"] is None],
            "presentAtEnd": list(self.present),
            "expectedAtEnd": [publisher.id for publisher in self.alive()],
            "publisherRotations": publisher_rotations,
            "subscriberRotations": subscriber_rotations,
            "reconnect": reconnect,
        }
        if flags.want_generations:
            summary["want"] = want
        if view == "raw":
            summary["raw"] = {
                "announced": sum(1 for e in self.events if e["type"] == "announced"),
                "unannounced": sum(1 for e in self.events if e["type"] == "unannounced"),
                "loadedSeen": any(e["type"] == "loaded" for e in self.events),
            }
        if flags.latency:
            summary["latency"] = {
                "trials": latency_trials,
                "joinMs": stats([trial["joinMs"] for trial in latency_trials]),
                "connectToLoadedMs": stats([trial["connectToLoadedMs"] for trial in latency_trials]),
            }
        summary["errors"] = sum(
            1
            for e in self.events
            if any(word in e["type"] for word in ("error", "timeout", "missing")) or e["type"].endswith("exit")
        )

        if want_subscriber is not None and want_subscriber is not main.supervised:
            want_subscriber.close()
        main.close()
        self.kill_children()
        sys.stdout.write(json.dumps({"summary": summary}) + "\n")
        sys.stdout.flush()
        if flags.out:
            record = json.dumps({"argv": self.argv, "events": self.events, "summary": summary}, indent=2) + "\n"
            await asyncio.to_thread(write_out, Path(flags.out), record)


def write_out(out: Path, record: str) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(record)


async def _settled(awaitable: Awaitable[Any]) -> bool:
    """Wait for ``awaitable`` to finish either way."""
    with contextlib.suppress(Exception):
        await awaitable
    return True


async def run_probe(relay: Relay, flags: argparse.Namespace, argv: Sequence[str]) -> None:
    probe = Probe(relay, flags, argv)
    loop = asyncio.get_running_loop()

    def interrupted() -> None:
        probe.kill_children()
        os._exit(130)

    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, interrupted)
    try:
        await probe.run()
    except BaseException as error:
        sys.stderr.write(json.dumps({"error": relay.describe(error), "stack": traceback.format_exc()}) + "\n")
        probe.kill_children()
        os._exit(1)
    # Exit without waiting for closing connections to finish.
    os._exit(0)


def main() -> None:
    argv = sys.argv[1:]
    flags = parse_flags(argv)
    relay = read_relay(flags)
    if flags.publisher_child is not None:
        asyncio.run(run_publisher(relay, flags.publisher_child))
    else:
        asyncio.run(run_probe(relay, flags, argv))


if __name__ == "__main__":
    main()
