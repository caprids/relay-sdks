"""The supervised relay over scripted connections that publish and subscribe.

Mirrors ``ts/tests/supervised-relay.test.ts``: each case drives what the relay would do to one connection,
and the review-fix cases keep the regressions the TypeScript review passes on #529 found.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest
from core_helpers.raw_peer import raw_subscribe, solicit_namespace
from core_helpers.transport_pair import PairTransport, create_transport_pair
from core_helpers.waiting import settle, wait_for
from relay_helpers.supervision_fakes import FakePublisher, FakeSession, FakeSubscriber, until
from test_relay_session import scripted_relay

from caprid_relay import (
    CancellationToken,
    ConnectingTransport,
    PublishingRootsHandlers,
    RelayError,
    SupervisedRelay,
    SupervisedSubscriber,
    SupervisionBackoff,
    WantHandlers,
    connect_relay,
)
from caprid_relay.supervised_publisher import _supervise_relay_publisher
from caprid_relay.supervised_relay import _supervise_relay
from caprid_relay.supervised_subscriber import _supervise_relay_subscriber
from moq_core import Closed
from moq_core.control_messages import Namespace, encode_goaway

FAST = SupervisionBackoff(initial_delay=0, max_delay=0)
SLOW = SupervisionBackoff(initial_delay=10, max_delay=10)


@pytest.fixture
async def cleanup() -> AsyncIterator[list[Callable[[], object]]]:
    closers: list[Callable[[], object]] = []
    yield closers
    for close in closers:
        result = close()
        if isinstance(result, Awaitable):
            await result


class Sessions:
    def __init__(self) -> None:
        self.connections: list[FakeSession] = []
        self.statuses: list[tuple[str, BaseException | None]] = []
        self.generations: list[str] = []

    async def connect(self, _attempt: CancellationToken) -> Any:
        session = FakeSession()
        self.connections.append(session)
        return session


async def start(cleanup: list[Callable[[], object]]) -> tuple[SupervisedRelay, Sessions]:
    sessions = Sessions()
    relay = await _supervise_relay(
        sessions.connect,
        reconnect=FAST,
        on_status=lambda status, error: sessions.statuses.append((status, error)),
        on_generation=sessions.generations.append,
    )
    cleanup.append(relay.close)
    return relay, sessions


def namespaces(session: FakeSession) -> list[str]:
    return sorted(session.publications)


def lost(code: Any = "TRANSPORT_ERROR", reason: str = "gone") -> RelayError:
    return RelayError(reason, code=code, phase="connection")


async def cancelled(attempt: CancellationToken) -> Any:
    """A connection attempt that never connects: it fails once the supervisor cancels it."""
    gave_up: asyncio.Future[BaseException] = asyncio.get_running_loop().create_future()
    attempt.add_callback(gave_up.set_result)
    raise await gave_up


# ------------------------------------------------------------------ connect_supervised_relay


async def test_serves_every_root_under_the_connection_generation_and_subscribes_on_the_same_connection(
    cleanup: list[Callable[[], object]],
) -> None:
    relay, s = await start(cleanup)
    generation = relay.generation
    assert generation is not None
    assert s.generations == [generation]
    me = relay.publish(["p", "alice"])
    host = relay.publish(["host"])
    assert me.namespace == ("p", "alice", generation)
    assert host.namespace == ("host", generation)
    assert namespaces(s.connections[0]) == [f"host/{generation}", f"p/alice/{generation}"]

    video = me.register_track("video")
    track = s.connections[0].publications[f"p/alice/{generation}"].tracks["video"]
    track.set_demand(True)
    await video.write(b"\x01")
    assert track.writes == [b"\x01"]

    relay.want("bob", ["p", "bob", "g1"], "video", retry=FAST)
    await until(lambda: len(s.connections[0].subscriptions) == 1, "subscribe on the one connection")
    assert len(s.connections) == 1


async def test_brings_every_root_track_and_want_back_together_as_the_next_generation_after_a_reconnect(
    cleanup: list[Callable[[], object]],
) -> None:
    relay, s = await start(cleanup)
    me = relay.publish(["p", "alice"])
    relay.publish(["host"])
    writer = me.register_track("video")
    relay.want("bob", ["p", "bob", "g1"], "video", retry=FAST)
    await until(lambda: len(s.connections[0].subscriptions) == 1, "subscribe")
    s.connections[0].subscriptions[0].accept()

    error = lost()
    s.connections[0].end(error)
    await until(lambda: len(s.connections) == 2 and relay.status == "live", "second connection")
    assert ("reconnecting", error) in s.statuses
    first, second = s.generations
    assert second > first
    assert namespaces(s.connections[1]) == [f"host/{second}", f"p/alice/{second}"]
    await until(lambda: len(s.connections[1].subscriptions) == 1, "want resubscribed on the new connection")

    track = s.connections[1].publications[f"p/alice/{second}"].tracks["video"]
    track.set_demand(True)
    await writer.write(b"\x02")
    assert track.writes == [b"\x02"]


async def test_fails_a_write_between_connections_with_reconnecting(cleanup: list[Callable[[], object]]) -> None:
    gate = asyncio.Event()
    connections: list[FakeSession] = []

    async def connect(_attempt: CancellationToken) -> Any:
        if connections:
            await gate.wait()
        session = FakeSession()
        connections.append(session)
        return session

    relay = await _supervise_relay(connect, reconnect=FAST)
    cleanup.append(relay.close)
    writer = relay.publish(["p", "alice"]).register_track("video")
    connections[0].end(lost())
    await until(lambda: relay.status == "reconnecting", "reconnecting")
    with pytest.raises(RelayError) as failure:
        await writer.write(b"\x01")
    assert failure.value.code == "RECONNECTING"
    gate.set()
    await until(lambda: relay.status == "live", "live again")


async def test_replaces_the_whole_connection_when_the_relay_stops_soliciting_one_root(
    cleanup: list[Callable[[], object]],
) -> None:
    relay, s = await start(cleanup)
    relay.publish(["p", "alice"])
    relay.publish(["host"])
    first = s.generations[0]
    withdrawn = lost("NAMESPACE_ENDED", "Relay withdrew the namespace")
    s.connections[0].publications[f"host/{first}"].end(withdrawn)
    await until(lambda: len(s.connections) == 2 and relay.status == "live", "replacement connection")
    assert ("reconnecting", withdrawn) in s.statuses
    # The old connection is closed, not left serving the other root.
    await until(lambda: s.connections[0].closed.done(), "old connection closed")
    second = s.generations[1]
    assert namespaces(s.connections[1]) == [f"host/{second}", f"p/alice/{second}"]


async def test_rotates_make_before_break_the_tracks_and_the_wants_move_then_the_old_connection_closes(
    cleanup: list[Callable[[], object]],
) -> None:
    relay, s = await start(cleanup)
    me = relay.publish(["p", "alice"])
    writer = me.register_track("video")
    objects: list[int] = []
    relay.want(
        "bob", ["p", "bob", "g1"], "video", WantHandlers(on_object=lambda o: objects.append(o.group_id)), retry=FAST
    )
    await until(lambda: len(s.connections[0].subscriptions) == 1, "subscribe")
    s.connections[0].subscriptions[0].accept()
    s.connections[0].subscriptions[0].send_object(1)
    first_track = s.connections[0].publications[f"p/alice/{s.generations[0]}"].tracks["video"]
    first_track.set_demand(True)

    relay.rotate()
    await until(lambda: len(s.connections) == 2 and len(s.generations) == 2, "replacement up")
    second = s.generations[1]
    assert relay.generation == second
    assert me.namespace == ("p", "alice", second)
    # The old connection keeps serving until the track has demand on the new one.
    await until(lambda: len(s.connections[1].subscriptions) == 1, "want moved")
    s.connections[1].subscriptions[0].accept()
    s.connections[1].subscriptions[0].send_object(2)
    next_track = s.connections[1].publications[f"p/alice/{second}"].tracks["video"]
    next_track.set_demand(True)
    await writer.write(b"\x09")
    assert next_track.writes == [b"\x09"]
    await until(lambda: first_track.closed, "old connection closed")
    assert objects == [1, 2]
    assert relay.status == "live"


async def test_publishes_a_root_added_while_live_keeps_roots_unique_and_closes_one_publication_alone(
    cleanup: list[Callable[[], object]],
) -> None:
    relay, s = await start(cleanup)
    generation = relay.generation
    me = relay.publish(["p", "alice"])
    with pytest.raises(ValueError):
        relay.publish(["p", "alice"])
    host = relay.publish(["host"])
    host.register_track("chat")
    await host.close()
    assert s.connections[0].publications[f"host/{generation}"].close_calls == 1
    with pytest.raises(RelayError):
        host.register_track("other")
    with pytest.raises(ValueError):
        relay.publish(["host"])
    # The connection and the other publication carry on.
    assert relay.status == "live"
    assert me.namespace == ("p", "alice", generation)
    assert s.connections[0].publications[f"p/alice/{generation}"].close_calls == 0


async def test_checks_a_root_at_once_and_refuses_calls_after_close(cleanup: list[Callable[[], object]]) -> None:
    relay, s = await start(cleanup)
    with pytest.raises(TypeError):
        relay.publish([])
    with pytest.raises(ValueError):
        relay.publish([f"f{i}" for i in range(32)])
    await relay.close()
    assert s.statuses[-1] == ("closed", None)
    assert await relay.closed == Closed()
    with pytest.raises(RelayError):
        relay.publish(["p"])
    with pytest.raises(RelayError):
        relay.want("k", ["p"], "t")


async def test_ends_the_writers_and_closes_the_connection_on_close(cleanup: list[Callable[[], object]]) -> None:
    relay, s = await start(cleanup)
    writer = relay.publish(["p", "alice"]).register_track("video")
    await relay.close()
    assert writer.closed
    assert len(s.connections[0].publications) == 1
    assert next(iter(s.connections[0].publications.values())).tracks["video"].closed
    assert relay.generation is None


# ------------------------------------------------------------------ publishing roots


async def test_never_reports_its_own_roots_also_from_the_connection_a_rotation_is_replacing(
    cleanup: list[Callable[[], object]],
) -> None:
    relay, s = await start(cleanup)
    relay.publish(["room", "p", "alice"])
    changes: list[list[str]] = []
    relay.publishing_roots(
        ["room", "p"],
        PublishingRootsHandlers(on_changed=lambda change: changes.append(sorted(root.id for root in change.present))),
    )
    await until(lambda: len(s.connections[0].discoveries) == 1, "discovery")
    discovery = s.connections[0].discoveries[0]
    # What the old connection would see during a rotation: its own next generation, announced by another session.
    discovery.push("announced", ["room", "p", "alice", "g2"])
    discovery.push("announced", ["room", "p", "bob", "g1"])
    discovery.push("loaded")
    await until(lambda: bool(changes) and "bob" in changes[-1], "bob listed")
    assert "alice" not in [root for change in changes for root in change]


# ------------------------------------------------------------------ the review passes on #529


async def test_close_closes_a_session_faulted_while_its_replacement_is_connecting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old = FakeSession()
    closes = 0
    close = old.close

    async def counted() -> None:
        nonlocal closes
        closes += 1
        await close()

    monkeypatch.setattr(old, "close", counted)
    attempts = 0

    async def connect(attempt: CancellationToken) -> Any:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return old
        return await cancelled(attempt)

    relay = await _supervise_relay(connect, reconnect=FAST)
    relay.publish(["alice"])
    relay.publish(["host"])
    relay.rotate()
    await until(lambda: attempts == 2)
    next(iter(old.publications.values())).end(lost("NAMESPACE_ENDED", "withdrawn"))
    await until(lambda: relay.status == "reconnecting")
    await relay.close()
    try:
        assert closes == 1
    finally:
        await close()


async def test_a_publication_close_waits_for_its_groups_on_an_already_draining_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old, nxt = FakeSession(), FakeSession()
    drain = asyncio.Event()
    draining = drained = False
    close = old.close

    async def slow() -> None:
        nonlocal draining, drained
        draining = True
        for publication in old.publications.values():
            for track in publication.tracks.values():
                track.writer.close()
        await drain.wait()
        drained = True
        await close()

    monkeypatch.setattr(old, "close", slow)
    attempts = 0

    async def connect(_attempt: CancellationToken) -> Any:
        nonlocal attempts
        attempts += 1
        return old if attempts == 1 else nxt

    relay = await _supervise_relay(connect, reconnect=FAST)
    publication = relay.publish(["alice"])
    writer = publication.register_track("video")
    group = await writer.open_group(b"\x01")
    await group.close()
    relay.rotate()
    await until(lambda: draining)
    closing = asyncio.ensure_future(publication.close())
    await asyncio.sleep(0.02)
    try:
        assert not drained
        assert not closing.done()
    finally:
        drain.set()
        await closing
        await relay.close()


async def test_removes_a_previously_discovered_root_when_it_becomes_an_own_root() -> None:
    session = FakeSession()

    async def connect(_attempt: CancellationToken) -> Any:
        return session

    relay = await _supervise_relay(connect, reconnect=FAST)
    view = relay.publishing_roots(["room"], PublishingRootsHandlers(on_changed=lambda _change: None))
    await until(lambda: len(session.discoveries) == 1)
    discovery = session.discoveries[0]
    discovery.push("announced", ["room", "alice", "old-generation"])
    discovery.push("loaded")
    await until(lambda: view.loaded and any(root.id == "alice" for root in view.present))
    relay.publish(["room", "alice"])
    discovery.push("unannounced", ["room", "alice", "old-generation"])
    await asyncio.sleep(0.15)
    try:
        assert not any(root.id == "alice" for root in view.present)
    finally:
        await relay.close()


def raising_on_closed(failure: BaseException) -> Callable[[str, BaseException | None], None]:
    def on_status(status: str, _error: BaseException | None) -> None:
        if status == "closed":
            raise failure

    return on_status


async def test_a_publisher_close_raises_when_awaited_after_its_closed_callback_failed_during_a_reconnect() -> None:
    old = FakePublisher(["alice", "generation"])
    failure = RuntimeError("status callback failed")

    async def connect(_namespace: tuple[str, ...], _attempt: CancellationToken) -> Any:
        return old

    publisher = await _supervise_relay_publisher(
        connect, ["alice"], reconnect=SLOW, on_status=raising_on_closed(failure)
    )
    old.end(lost())
    await until(lambda: publisher.status == "reconnecting")
    closing = publisher.close()
    with pytest.raises(RuntimeError) as raised:
        await closing
    assert raised.value is failure
    assert publisher.closed.done()


async def test_a_publication_closed_after_the_relay_waits_for_the_drain_stop_started(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = FakeSession()
    drain = asyncio.Event()
    drained = False
    close = session.close

    async def slow() -> None:
        nonlocal drained
        await drain.wait()
        drained = True
        await close()

    monkeypatch.setattr(session, "close", slow)

    async def connect(_attempt: CancellationToken) -> Any:
        return session

    relay = await _supervise_relay(connect, reconnect=FAST)
    publication = relay.publish(["alice"])
    stopping = asyncio.ensure_future(relay.close())
    await asyncio.sleep(0)
    closing = asyncio.ensure_future(publication.close())
    await asyncio.sleep(0.02)
    try:
        assert not drained
        assert not closing.done()
    finally:
        drain.set()
        await closing
        await stopping


async def test_wants_leave_a_connection_given_up_for_a_lost_root_without_waiting_for_its_drain(
    monkeypatch: pytest.MonkeyPatch, cleanup: list[Callable[[], object]]
) -> None:
    old = FakeSession()
    drain = asyncio.Event()
    close = old.close

    async def slow() -> None:
        await drain.wait()
        await close()

    monkeypatch.setattr(old, "close", slow)
    connections: list[FakeSession] = []

    async def connect(_attempt: CancellationToken) -> Any:
        session = FakeSession() if connections else old
        connections.append(session)
        return session

    relay = await _supervise_relay(connect, reconnect=FAST)
    cleanup.append(relay.close)
    relay.publish(["alice"])
    relay.want("bob", ["bob"], "video", retry=FAST)
    await until(lambda: len(old.subscriptions) == 1, "subscribe")
    old.subscriptions[0].accept()
    next(iter(old.publications.values())).end(lost("NAMESPACE_ENDED", "withdrawn"))
    try:
        await until(
            lambda: len(connections) == 2 and len(connections[1].subscriptions) == 1,
            "want moved before the drain ends",
        )
    finally:
        drain.set()


async def test_every_close_raises_after_on_status_raised_reporting_closed() -> None:
    old = FakePublisher(["alice", "generation"])
    failure = RuntimeError("status callback failed")

    async def connect(_namespace: tuple[str, ...], _attempt: CancellationToken) -> Any:
        return old

    publisher = await _supervise_relay_publisher(
        connect, ["alice"], reconnect=SLOW, on_status=raising_on_closed(failure)
    )
    old.end(lost())
    await until(lambda: publisher.status == "reconnecting")
    for _ in range(2):
        with pytest.raises(RuntimeError) as raised:
            await publisher.close()
        assert raised.value is failure


async def test_a_close_from_on_status_reporting_closed_keeps_the_cancellation_reason() -> None:
    cancellation = CancellationToken()
    reason = RuntimeError("cancelled")
    subscriber: SupervisedSubscriber | None = None

    def on_status(status: str, _error: BaseException | None) -> None:
        if status == "closed" and subscriber is not None:
            subscriber.close()

    async def connect(_attempt: CancellationToken) -> Any:
        return FakeSubscriber()

    subscriber = await _supervise_relay_subscriber(connect, cancellation=cancellation, on_status=on_status)
    cancellation.cancel(reason)
    assert await subscriber.closed == Closed(reason)


# ------------------------------------------------------------------ over connect_relay


def real_connections() -> tuple[list[PairTransport], Callable[[CancellationToken], Awaitable[Any]]]:
    """``connect_relay`` over in-memory transports, each to a scripted relay: the relay ends of every connection
    made, and the connect function."""
    servers: list[PairTransport] = []

    async def connect(attempt: CancellationToken) -> Any:
        pair = create_transport_pair()
        scripted_relay(pair.server)
        servers.append(pair.server)
        ready: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        ready.set_result(None)
        return await connect_relay(
            "https://relay.example.com/room",
            cancellation=attempt,
            create_transport=lambda url, protocols: ConnectingTransport(transport=pair.client, ready=ready),
        )

    return servers, connect


async def test_over_real_connections_a_lost_root_replaces_the_connection_and_every_root_comes_back() -> None:
    servers, connect = real_connections()
    statuses: list[tuple[str, BaseException | None]] = []
    relay = await _supervise_relay(connect, reconnect=FAST, on_status=lambda status, e: statuses.append((status, e)))
    try:
        own = relay.publish(["room", "p", "alice"])
        host = relay.publish(["room", "host"])
        own.register_track("video")
        host.register_track("control")
        first = relay.generation
        await solicit_namespace(servers[0], ["room", "p"], 1)
        hosts = await solicit_namespace(servers[0], ["room", "host"], 3)
        await wait_for(lambda: hosts.kinds() == ["request-ok", "namespace"])

        hosts.reset()
        await wait_for(lambda: len(servers) == 2 and relay.status == "live")
        error = next(error for status, error in statuses if status == "reconnecting")
        assert isinstance(error, RelayError)
        assert error.code == "NAMESPACE_ENDED"
        # The old connection is closed, not left serving the other root.
        await asyncio.wait_for(servers[0].wait_closed(), 1)

        second = relay.generation
        assert second is not None
        assert second != first
        solicitation = await solicit_namespace(servers[1], ["room"], 1)
        await wait_for(lambda: len([m for m in solicitation.received if isinstance(m, Namespace)]) == 2)
        announced = sorted(
            "/".join(m.track_namespace_suffix) for m in solicitation.received if isinstance(m, Namespace)
        )
        assert announced == [f"host/{second}", f"p/alice/{second}"]
    finally:
        await relay.close()


async def test_over_real_connections_a_close_after_a_goaway_waits_for_the_ended_connection_to_drain() -> None:
    servers, connect = real_connections()
    relay = await _supervise_relay(connect, reconnect=SLOW)
    try:
        publication = relay.publish(["room", "p", "alice"])
        publication.register_track("video")
        namespace = publication.namespace
        assert namespace is not None
        solicitation = await solicit_namespace(servers[0], ["room"], 1)
        await wait_for(lambda: solicitation.kinds() == ["request-ok", "namespace"])
        upstream = await raw_subscribe(servers[0], namespace, "video", 3)
        await wait_for(lambda: upstream.kinds() == ["subscribe-ok"])

        await solicitation.send(encode_goaway(1000))
        await wait_for(lambda: relay.status == "reconnecting")
        closing = asyncio.ensure_future(relay.close())
        await settle()
        # The session ended at the GOAWAY, but its publisher still waits for the relay to read the track's end.
        assert not servers[0].ended
        assert not closing.done()

        await upstream.fin()
        await asyncio.wait_for(closing, 1)
        assert servers[0].ended
    finally:
        await relay.close()
