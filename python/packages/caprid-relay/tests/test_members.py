"""The supervised subscriber's member view.

Mirrors ``ts/tests/members.test.ts``: the view over scripted connections, a member leaving, the view
over the relay wire, discovery shared with wants, and the pure reducer and transition.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest
from core_helpers.fake_transport import FakeTransport, create_fake_transport
from relay_helpers.supervision_fakes import FakeDiscovery, FakeSubscriber, FakeSubscription, until

from caprid_relay import (
    CancellationToken,
    ConnectingTransport,
    Member,
    Members,
    MembersChange,
    MembersHandlers,
    RelayError,
    RequestError,
    SubscriptionConnectionClosedError,
    SubscriptionRequestError,
    SupervisedSubscriber,
    SupervisionBackoff,
    WantHandlers,
    connect_relay_subscriber,
)
from caprid_relay._discovery import DiscoveryIndex
from caprid_relay._supervision import new_generation
from caprid_relay.members import (
    _apply_namespace,
    _AwaitConnection,
    _CancelLeave,
    _Close,
    _Closed,
    _Connected,
    _Emit,
    _Ended,
    _Error,
    _LeaveDue,
    _Live,
    _Loaded,
    _Loading,
    _member_field,
    _Namespace,
    _Open,
    _Phase,
    _Presence,
    _Release,
    _Replacing,
    _Retiring,
    _Retry,
    _ScheduleLeave,
    _Source,
    _transition,
)
from caprid_relay.supervised_subscriber import _supervise_relay_subscriber
from moq_core.control_messages import encode_namespace, encode_namespace_done, encode_request_ok

FAST = SupervisionBackoff(initial_delay=0, max_delay=0)
ROOT = ("room", "p")


def at(*fields: str) -> tuple[str, ...]:
    return (*ROOT, *fields)


async def pause(seconds: float = 0.01) -> None:
    await asyncio.sleep(seconds)


@pytest.fixture
async def cleanup() -> AsyncIterator[list[Callable[[], object]]]:
    closers: list[Callable[[], object]] = []
    yield closers
    for close in closers:
        result = close()
        if isinstance(result, Awaitable):
            await result


class Relay:
    """Scripted subscriber connections, the n-th expiring at ``expiries[n]``."""

    def __init__(self, expiries: Sequence[datetime | None]) -> None:
        self.expiries = expiries
        self.connections: list[FakeSubscriber] = []

    async def connect(self, _attempt: CancellationToken) -> Any:
        index = len(self.connections)
        fake = FakeSubscriber(self.expiries[index] if index < len(self.expiries) else None)
        self.connections.append(fake)
        return fake

    async def discovery(self, n: int = 0, count: int = 1) -> FakeDiscovery:
        def opened() -> bool:
            return len(self.connections) > n and len(self.connections[n].discoveries) >= count

        await until(opened, f"discovery {count} on connection {n}")
        return self.connections[n].discoveries[count - 1]


async def start(
    cleanup: list[Callable[[], object]], expiries: Sequence[datetime | None] = ()
) -> tuple[SupervisedSubscriber, Relay]:
    relay = Relay(expiries)
    subscriber = await _supervise_relay_subscriber(relay.connect, reconnect=FAST, rotate_before=0.15)
    cleanup.append(subscriber.close)
    return subscriber, relay


class Recorder:
    def __init__(self) -> None:
        self.changes: list[MembersChange] = []
        self.errors: list[BaseException] = []
        self.handlers = MembersHandlers(on_changed=self.changes.append, on_error=self.errors.append)


def quiet() -> MembersHandlers:
    return MembersHandlers(on_changed=lambda _change: None)


def ids(members: Sequence[Member]) -> list[str]:
    return [member.id for member in members]


def summary(change: MembersChange) -> dict[str, object]:
    return {
        "joined": ids(change.joined),
        "left": ids(change.left),
        "present": ids(change.present),
        "initial": change.initial,
    }


def change(joined: list[str], left: list[str], present: list[str], initial: bool = False) -> dict[str, object]:
    return {"joined": joined, "left": left, "present": present, "initial": initial}


def request_error(code: int, reason: str) -> SubscriptionRequestError:
    return SubscriptionRequestError(RequestError(error_code=code, retry_interval=0, reason=reason))


def connection_lost() -> RelayError:
    return RelayError("idle", code="TRANSPORT_ERROR", phase="connection")


# ------------------------------------------------------------------ SupervisedSubscriber.members


async def test_loads_with_every_snapshot_member_in_joined_and_calls_nothing_before(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, relay = await start(cleanup)
    r = Recorder()
    view = subscriber.members(ROOT, r.handlers)
    assert view.loaded is False
    assert view.present == ()
    d = await relay.discovery()
    assert d.prefix == ROOT
    d.push("announced", at("alice", new_generation()))
    d.push("announced", at("bob"))
    await pause()
    assert r.changes == []
    assert view.loaded is False
    d.push("loaded")
    await until(lambda: len(r.changes) == 1, "load")
    assert summary(r.changes[0]) == change(["alice", "bob"], [], ["alice", "bob"], initial=True)
    assert r.changes[0].joined[0].namespace == at("alice")
    assert view.loaded is True
    assert view.present is r.changes[0].present


async def test_loads_an_empty_root_once_with_nobody(cleanup: list[Callable[[], object]]) -> None:
    subscriber, relay = await start(cleanup)
    r = Recorder()
    view = subscriber.members(ROOT, r.handlers)
    (await relay.discovery()).push("loaded")
    await until(lambda: len(r.changes) == 1, "load")
    assert summary(r.changes[0]) == change([], [], [], initial=True)
    assert view.loaded is True
    assert view.present == ()


async def test_reports_one_joined_and_one_left_for_a_live_announce_and_withdrawal(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, relay = await start(cleanup)
    r = Recorder()
    view = subscriber.members(ROOT, r.handlers)
    d = await relay.discovery()
    d.push("loaded")
    generation = new_generation()
    d.push("announced", at("carol", generation))
    await until(lambda: len(r.changes) == 2, "join")
    assert summary(r.changes[1]) == change(["carol"], [], ["carol"])
    assert view.present is r.changes[1].present
    d.push("unannounced", at("carol", generation))
    await until(lambda: len(r.changes) == 3, "leave")
    assert summary(r.changes[2]) == change([], ["carol"], [])


async def test_reports_nothing_for_a_publisher_rotation_a_relay_repeat_or_a_namespace_below_a_generation(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, relay = await start(cleanup)
    r = Recorder()
    subscriber.members(ROOT, r.handlers)
    d = await relay.discovery()
    old = new_generation(1_000)
    d.push("announced", at("alice", old))
    d.push("loaded")
    await until(lambda: len(r.changes) == 1, "load")
    present = r.changes[0].present
    replacement = new_generation(2_000)
    d.push("announced", at("alice", replacement))
    d.push("unannounced", at("alice", old))
    d.push("announced", at("alice", replacement))
    d.push("announced", at("alice", replacement, "deeper"))
    d.push("announced", at("mallory", new_generation(), "deeper"))
    d.push("announced", at("zed"))
    await until(lambda: len(r.changes) == 2, "the marker join")
    assert summary(r.changes[1]) == change(["zed"], [], ["alice", "zed"])
    assert r.changes[1].present is not present


async def test_follows_an_unsupervised_publisher_one_field_below_the_root_in_and_out(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, relay = await start(cleanup)
    r = Recorder()
    subscriber.members(ROOT, r.handlers)
    d = await relay.discovery()
    d.push("loaded")
    d.push("announced", at("dave"))
    await until(lambda: len(r.changes) == 2, "join")
    assert summary(r.changes[1]) == change(["dave"], [], ["dave"])
    d.push("unannounced", at("dave"))
    await until(lambda: len(r.changes) == 3, "leave")
    assert summary(r.changes[2]) == change([], ["dave"], [])


async def test_reports_nothing_for_rotate(cleanup: list[Callable[[], object]]) -> None:
    subscriber, relay = await start(cleanup)
    r = Recorder()
    view = subscriber.members(ROOT, r.handlers)
    generation = new_generation()
    first = await relay.discovery()
    first.push("announced", at("alice", generation))
    first.push("loaded")
    await until(lambda: len(r.changes) == 1, "load")
    subscriber.rotate()
    nxt = await relay.discovery(1)
    assert nxt.prefix == ROOT
    assert "alice" in ids(view.present)
    nxt.push("announced", at("alice", generation))
    nxt.push("loaded")
    nxt.push("announced", at("zed"))
    await until(lambda: len(r.changes) == 2, "the marker join on the new connection")
    assert summary(r.changes[1]) == change(["zed"], [], ["alice", "zed"])


async def test_follows_the_old_discovery_through_rotate_until_the_new_one_loads_with_no_want_holding_the_connection(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, relay = await start(cleanup)
    r = Recorder()
    subscriber.members(ROOT, r.handlers)
    first = await relay.discovery()
    first.push("announced", at("alice"))
    first.push("loaded")
    await until(lambda: len(r.changes) == 1, "load")
    subscriber.rotate()
    nxt = await relay.discovery(1)
    await pause()
    assert not relay.connections[0].closed.done()
    first.push("announced", at("bob"))
    await until(lambda: len(r.changes) == 2, "a join on the old discovery while the new one loads")
    assert summary(r.changes[1]) == change(["bob"], [], ["alice", "bob"])
    nxt.push("announced", at("alice"))
    nxt.push("announced", at("bob"))
    nxt.push("loaded")
    await until(relay.connections[0].closed.done, "the old connection closes once the new discovery has loaded")
    assert len(r.changes) == 2


async def test_reports_nothing_for_a_rotation_before_the_credential_expires(
    cleanup: list[Callable[[], object]],
) -> None:
    now = datetime.now(UTC)
    subscriber, relay = await start(cleanup, [now + timedelta(seconds=0.6), now + timedelta(hours=1)])
    r = Recorder()
    subscriber.members(ROOT, r.handlers)
    first = await relay.discovery()
    first.push("announced", at("alice"))
    first.push("loaded")
    await until(lambda: len(r.changes) == 1, "load")
    nxt = await relay.discovery(1)
    nxt.push("announced", at("alice"))
    nxt.push("loaded")
    await until(lambda: relay.connections[0].discoveries[0].closed, "the old connection released")
    nxt.push("announced", at("zed"))
    await until(lambda: len(r.changes) == 2, "the marker join on the new connection")
    assert summary(r.changes[1]) == change(["zed"], [], ["alice", "zed"])


async def test_holds_the_view_through_a_reconnect_and_reports_nothing_when_the_set_is_unchanged(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, relay = await start(cleanup)
    r = Recorder()
    view = subscriber.members(ROOT, r.handlers)
    first = await relay.discovery()
    first.push("announced", at("alice"))
    first.push("announced", at("bob"))
    first.push("loaded")
    await until(lambda: len(r.changes) == 1, "load")
    relay.connections[0].end(connection_lost())
    nxt = await relay.discovery(1)
    assert view.loaded is True
    assert ids(view.present) == ["alice", "bob"]
    nxt.push("announced", at("bob"))
    nxt.push("announced", at("alice"))
    nxt.push("loaded")
    nxt.push("announced", at("zed"))
    await until(lambda: len(r.changes) == 2, "the marker join")
    assert summary(r.changes[1]) == change(["zed"], [], ["alice", "bob", "zed"])


async def test_reports_exactly_the_member_who_left_during_an_outage_once_the_new_discovery_loads(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, relay = await start(cleanup)
    r = Recorder()
    subscriber.members(ROOT, r.handlers)
    first = await relay.discovery()
    first.push("announced", at("alice"))
    first.push("announced", at("bob"))
    first.push("loaded")
    await until(lambda: len(r.changes) == 1, "load")
    relay.connections[0].end(connection_lost())
    nxt = await relay.discovery(1)
    nxt.push("announced", at("alice"))
    await pause()
    assert len(r.changes) == 1
    nxt.push("loaded")
    await until(lambda: len(r.changes) == 2, "the difference")
    assert summary(r.changes[1]) == change([], ["bob"], ["alice"])
    await pause()
    assert len(r.changes) == 2


async def test_holds_the_view_when_discovery_ends_with_a_retryable_error_and_reloads(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, relay = await start(cleanup)
    r = Recorder()
    view = subscriber.members(ROOT, r.handlers)
    first = await relay.discovery()
    first.push("announced", at("alice"))
    first.push("loaded")
    await until(lambda: len(r.changes) == 1, "load")
    first.fail(request_error(0x0, "internal error"))
    nxt = await relay.discovery(0, 2)
    assert ids(view.present) == ["alice"]
    nxt.push("announced", at("alice"))
    nxt.push("announced", at("bob"))
    nxt.push("loaded")
    await until(lambda: len(r.changes) == 2, "the difference")
    assert summary(r.changes[1]) == change(["bob"], [], ["alice", "bob"])
    assert r.errors == []


async def test_calls_on_error_and_closes_for_a_refusal_no_wait_fixes(cleanup: list[Callable[[], object]]) -> None:
    subscriber, relay = await start(cleanup)
    r = Recorder()
    view = subscriber.members(ROOT, r.handlers)
    first = await relay.discovery()
    first.push("loaded")
    await until(lambda: len(r.changes) == 1, "load")
    refusal = request_error(0x1, "unauthorized")
    first.fail(refusal)
    await until(lambda: len(r.errors) == 1, "on_error")
    assert r.errors[0] is refusal
    await pause(0.3)
    assert len(relay.connections[0].discoveries) == 1
    assert len(r.changes) == 1
    assert view.loaded is True


async def test_is_safe_to_close_from_on_changed_nothing_is_delivered_after_it(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, relay = await start(cleanup)
    changes: list[MembersChange] = []
    views: list[Members] = []

    def on_changed(change: MembersChange) -> None:
        changes.append(change)
        views[0].close()
        views[0].close()

    views.append(subscriber.members(ROOT, MembersHandlers(on_changed=on_changed)))
    d = await relay.discovery()
    d.push("announced", at("alice"))
    d.push("loaded")
    await until(lambda: len(changes) == 1, "load")
    d.push("announced", at("bob"))
    await pause()
    assert len(changes) == 1
    assert ids(views[0].present) == ["alice"]


async def test_loads_a_second_view_of_a_root_already_loaded_on_the_connection_never_inside_members(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, relay = await start(cleanup)
    first = Recorder()
    subscriber.members(ROOT, first.handlers)
    d = await relay.discovery()
    d.push("announced", at("alice"))
    d.push("loaded")
    await until(lambda: len(first.changes) == 1, "first load")
    second = Recorder()
    view = subscriber.members(ROOT, second.handlers)
    assert second.changes == []
    assert view.loaded is False
    await until(lambda: len(second.changes) == 1, "second load")
    assert summary(second.changes[0]) == change(["alice"], [], ["alice"], initial=True)
    assert len(relay.connections[0].discoveries) == 1


async def test_ends_silently_when_the_supervisor_closes(cleanup: list[Callable[[], object]]) -> None:
    subscriber, relay = await start(cleanup)
    r = Recorder()
    view = subscriber.members(ROOT, r.handlers)
    d = await relay.discovery()
    d.push("announced", at("alice"))
    d.push("loaded")
    await until(lambda: len(r.changes) == 1, "load")
    subscriber.close()
    d.push("announced", at("bob"))
    await pause()
    assert len(r.changes) == 1
    assert r.errors == []
    assert view.loaded is True
    assert ids(view.present) == ["alice"]
    with pytest.raises(RelayError):
        subscriber.members(ROOT, quiet())


async def test_checks_its_arguments_at_once(cleanup: list[Callable[[], object]]) -> None:
    subscriber, _relay = await start(cleanup)
    with pytest.raises(TypeError):
        subscriber.members(ROOT, None)  # ty: ignore[invalid-argument-type]
    with pytest.raises(TypeError):
        subscriber.members([], quiet())
    with pytest.raises(TypeError):
        subscriber.members("room", quiet())
    with pytest.raises(ValueError):
        subscriber.members(["f"] * 31, quiet())
    with pytest.raises(TypeError):
        MembersHandlers()  # ty: ignore[missing-argument]

    async def on_changed(_change: MembersChange) -> None:
        pass

    with pytest.raises(TypeError):
        subscriber.members(ROOT, MembersHandlers(on_changed=on_changed))  # ty: ignore[invalid-argument-type]
    subscriber.members(["f"] * 30, quiet()).close()


# ------------------------------------------------------------------ a member leaving


async def test_reports_nothing_when_a_rotation_withdraws_the_only_generation_just_before_announcing_the_next(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, relay = await start(cleanup)
    r = Recorder()
    view = subscriber.members(ROOT, r.handlers)
    d = await relay.discovery()
    old = new_generation(1_000)
    d.push("announced", at("alice", old))
    d.push("loaded")
    await until(lambda: len(r.changes) == 1, "load")
    d.push("unannounced", at("alice", old))
    await pause(0.02)
    assert "alice" in ids(view.present)
    d.push("announced", at("alice", new_generation(2_000)))
    await pause(0.15)
    d.push("announced", at("zed"))
    await until(lambda: len(r.changes) == 2, "the marker join")
    assert summary(r.changes[1]) == change(["zed"], [], ["alice", "zed"])


async def test_reports_a_withdrawal_with_nothing_after_it_once_after_the_grace(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, relay = await start(cleanup)
    r = Recorder()
    view = subscriber.members(ROOT, r.handlers)
    d = await relay.discovery()
    generation = new_generation()
    d.push("announced", at("alice", generation))
    d.push("loaded")
    await until(lambda: len(r.changes) == 1, "load")
    loop = asyncio.get_running_loop()
    withdrawn = loop.time()
    d.push("unannounced", at("alice", generation))
    await pause()
    assert len(r.changes) == 1
    assert view.present is r.changes[0].present
    await until(lambda: len(r.changes) == 2, "leave")
    assert loop.time() - withdrawn >= 0.09
    assert summary(r.changes[1]) == change([], ["alice"], [])
    await pause(0.15)
    assert len(r.changes) == 2


async def test_reports_a_reconnects_joins_and_departures_in_one_change_with_no_grace(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, relay = await start(cleanup)
    r = Recorder()
    subscriber.members(ROOT, r.handlers)
    first = await relay.discovery()
    for member_id in ("alice", "bob", "carol"):
        first.push("announced", at(member_id))
    first.push("loaded")
    await until(lambda: len(r.changes) == 1, "load")
    relay.connections[0].end(connection_lost())
    nxt = await relay.discovery(1)
    nxt.push("announced", at("carol"))
    nxt.push("announced", at("dave"))
    nxt.push("loaded")
    await until(lambda: len(r.changes) == 2, "the difference")
    assert summary(r.changes[1]) == change(["dave"], ["alice", "bob"], ["carol", "dave"])
    await pause(0.15)
    assert len(r.changes) == 2


async def test_reports_nothing_for_a_member_the_old_discovery_still_shows_when_the_new_snapshot_misses_its_announce(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, relay = await start(cleanup)
    r = Recorder()
    subscriber.members(ROOT, r.handlers)
    first = await relay.discovery()
    first.push("announced", at("alice"))
    first.push("loaded")
    await until(lambda: len(r.changes) == 1, "load")
    subscriber.rotate()
    nxt = await relay.discovery(1)
    first.push("announced", at("bob"))
    await until(lambda: len(r.changes) == 2, "bob joins on the old discovery")
    nxt.push("announced", at("alice"))
    nxt.push("loaded")
    await pause(0.005)
    nxt.push("announced", at("bob"))
    await pause(0.15)
    nxt.push("announced", at("zed"))
    await until(lambda: len(r.changes) == 3, "the marker join")
    assert summary(r.changes[2]) == change(["zed"], [], ["alice", "bob", "zed"])


async def test_reports_a_member_the_old_discovery_still_shows_as_left_after_the_grace_when_the_new_one_never_does(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, relay = await start(cleanup)
    r = Recorder()
    subscriber.members(ROOT, r.handlers)
    first = await relay.discovery()
    first.push("announced", at("alice"))
    first.push("announced", at("bob"))
    first.push("loaded")
    await until(lambda: len(r.changes) == 1, "load")
    subscriber.rotate()
    nxt = await relay.discovery(1)
    nxt.push("announced", at("alice"))
    nxt.push("announced", at("carol"))
    loop = asyncio.get_running_loop()
    loaded = loop.time()
    nxt.push("loaded")
    await until(lambda: len(r.changes) == 2, "the join at the load")
    assert summary(r.changes[1]) == change(["carol"], [], ["alice", "bob", "carol"])
    await until(lambda: len(r.changes) == 3, "the leave")
    assert loop.time() - loaded >= 0.09
    assert summary(r.changes[2]) == change([], ["bob"], ["alice", "carol"])


async def test_reports_nothing_when_a_reconnects_snapshot_falls_between_a_rotations_withdrawal_and_its_announce(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, relay = await start(cleanup)
    r = Recorder()
    subscriber.members(ROOT, r.handlers)
    first = await relay.discovery()
    old = new_generation(1_000)
    first.push("announced", at("alice", old))
    first.push("loaded")
    await until(lambda: len(r.changes) == 1, "load")
    first.push("unannounced", at("alice", old))
    await pause(0.02)
    relay.connections[0].end(connection_lost())
    nxt = await relay.discovery(1)
    nxt.push("loaded")
    await pause(0.005)
    nxt.push("announced", at("alice", new_generation(2_000)))
    await pause(0.15)
    nxt.push("announced", at("zed"))
    await until(lambda: len(r.changes) == 2, "the marker join")
    assert summary(r.changes[1]) == change(["zed"], [], ["alice", "zed"])


async def test_holds_a_leave_whose_grace_runs_out_during_an_outage_for_the_new_load_to_settle(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, relay = await start(cleanup)
    r = Recorder()
    subscriber.members(ROOT, r.handlers)
    first = await relay.discovery()
    old = new_generation(1_000)
    first.push("announced", at("alice", old))
    first.push("loaded")
    await until(lambda: len(r.changes) == 1, "load")
    first.push("unannounced", at("alice", old))
    await pause(0.02)
    relay.connections[0].end(connection_lost())
    nxt = await relay.discovery(1)
    await pause(0.15)
    nxt.push("announced", at("alice", new_generation(2_000)))
    nxt.push("loaded")
    nxt.push("announced", at("zed"))
    await until(lambda: len(r.changes) == 2, "the marker join")
    assert summary(r.changes[1]) == change(["zed"], [], ["alice", "zed"])


async def test_holds_a_pending_leave_while_a_covering_request_replaces_the_discovery_until_it_loads(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, relay = await start(cleanup)
    r = Recorder()
    subscriber.members(ROOT, r.handlers)
    narrow = await relay.discovery()
    old = new_generation(1_000)
    narrow.push("announced", at("alice", old))
    narrow.push("loaded")
    await until(lambda: len(r.changes) == 1, "load")
    narrow.push("unannounced", at("alice", old))
    await pause(0.02)
    subscriber.members(["room"], quiet())
    covering = await relay.discovery(0, 2)
    assert covering.prefix == ("room",)
    await pause(0.15)
    covering.push("announced", at("alice", new_generation(2_000)))
    covering.push("loaded")
    covering.push("announced", at("zed"))
    await until(lambda: len(r.changes) == 2, "the marker join")
    assert summary(r.changes[1]) == change(["zed"], [], ["alice", "zed"])


async def test_starts_a_pending_leave_again_once_the_covering_request_loads_without_the_member(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, relay = await start(cleanup)
    r = Recorder()
    subscriber.members(ROOT, r.handlers)
    narrow = await relay.discovery()
    generation = new_generation()
    narrow.push("announced", at("alice", generation))
    narrow.push("loaded")
    await until(lambda: len(r.changes) == 1, "load")
    narrow.push("unannounced", at("alice", generation))
    await pause(0.02)
    subscriber.members(["room"], quiet())
    covering = await relay.discovery(0, 2)
    await pause(0.15)
    assert len(r.changes) == 1
    covering.push("loaded")
    await until(lambda: len(r.changes) == 2, "the leave")
    assert summary(r.changes[1]) == change([], ["alice"], [])


async def test_cancels_a_pending_leave_without_reporting_it_when_the_view_closes(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, relay = await start(cleanup)
    r = Recorder()
    view = subscriber.members(ROOT, r.handlers)
    d = await relay.discovery()
    d.push("announced", at("alice"))
    d.push("loaded")
    await until(lambda: len(r.changes) == 1, "load")
    d.push("unannounced", at("alice"))
    await pause()
    view.close()
    await pause(0.15)
    assert len(r.changes) == 1
    assert ids(view.present) == ["alice"]


# ------------------------------------------------------------------ over the relay wire


def _factory(fake: FakeTransport) -> Callable[[str, Sequence[str]], ConnectingTransport]:
    ready: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    ready.set_result(None)
    return lambda url, protocols: ConnectingTransport(transport=fake, ready=ready)


async def start_over_wire(cleanup: list[Callable[[], object]]) -> tuple[list[FakeTransport], Recorder]:
    fakes: list[FakeTransport] = []

    async def connect(attempt: CancellationToken) -> Any:
        fake = create_fake_transport()
        fakes.append(fake)
        connecting = asyncio.ensure_future(
            connect_relay_subscriber(
                "https://relay.example.com/moq", create_transport=_factory(fake), cancellation=attempt
            )
        )
        fake.send_server_setup()
        return await connecting

    subscriber = await _supervise_relay_subscriber(connect, reconnect=FAST)
    cleanup.append(subscriber.close)
    r = Recorder()
    subscriber.members(ROOT, r.handlers)
    return fakes, r


async def reconnect_unchanged(cleanup: list[Callable[[], object]], *, reset_first: bool) -> list[dict[str, object]]:
    fakes, r = await start_over_wire(cleanup)
    first = await fakes[0].next_request_stream()
    await first.first_message
    await first.send(encode_request_ok())
    await first.send(encode_namespace(["alice", "g1"]))
    await first.send(encode_namespace(["bob", "g1"]))
    await until(lambda: len(r.changes) == 1, "load")
    if reset_first:
        first.reset(Exception("connection lost"))
        await pause(0.001)
    fakes[0].drop_transport()
    await until(lambda: len(fakes) == 2, "reconnect", 3.0)
    second = await fakes[1].next_request_stream()
    await second.first_message
    await second.send(encode_request_ok())
    await second.send(encode_namespace(["alice", "g1"]))
    await second.send(encode_namespace(["bob", "g1"]))
    await pause(0.3)
    return [summary(c) for c in r.changes]


LOADED_ALICE_AND_BOB = change(["alice", "bob"], [], ["alice", "bob"], initial=True)
LOADED_ALICE = change(["alice"], [], ["alice"], initial=True)


async def test_reports_nothing_for_an_unchanged_room_after_the_transport_drops(
    cleanup: list[Callable[[], object]],
) -> None:
    assert await reconnect_unchanged(cleanup, reset_first=False) == [LOADED_ALICE_AND_BOB]


async def test_reports_nothing_for_an_unchanged_room_when_the_request_stream_fails_before_the_transport_drops(
    cleanup: list[Callable[[], object]],
) -> None:
    assert await reconnect_unchanged(cleanup, reset_first=True) == [LOADED_ALICE_AND_BOB]


async def test_reports_nothing_for_an_unchanged_room_when_the_request_stream_resets_with_the_connection_up(
    cleanup: list[Callable[[], object]],
) -> None:
    fakes, r = await start_over_wire(cleanup)
    first = await fakes[0].next_request_stream()
    await first.first_message
    await first.send(encode_request_ok())
    await first.send(encode_namespace(["alice", "g1"]))
    await until(lambda: len(r.changes) == 1, "load")
    first.reset(Exception("relay reset the request stream"))
    second = await fakes[0].next_request_stream()
    await second.first_message
    await second.send(encode_request_ok())
    await second.send(encode_namespace(["alice", "g1"]))
    await pause(0.3)
    assert [summary(c) for c in r.changes] == [LOADED_ALICE]


async def test_holds_a_pending_leave_once_the_request_stream_fails_through_the_retry(
    cleanup: list[Callable[[], object]],
) -> None:
    fakes, r = await start_over_wire(cleanup)
    first = await fakes[0].next_request_stream()
    await first.first_message
    await first.send(encode_request_ok())
    await first.send(encode_namespace(["alice", "g1"]))
    await until(lambda: len(r.changes) == 1, "load")
    await first.send(encode_namespace_done(["alice", "g1"]))
    await pause(0.02)
    first.reset(Exception("relay reset the request stream"))
    second = await fakes[0].next_request_stream()
    await second.first_message
    await second.send(encode_request_ok())
    await second.send(encode_namespace(["alice", "g2"]))
    await pause(0.3)
    assert [summary(c) for c in r.changes] == [LOADED_ALICE]


async def test_reports_nothing_for_an_unchanged_room_when_the_relay_finishes_the_request_with_the_connection_up(
    cleanup: list[Callable[[], object]],
) -> None:
    fakes, r = await start_over_wire(cleanup)
    first = await fakes[0].next_request_stream()
    await first.first_message
    await first.send(encode_request_ok())
    await first.send(encode_namespace(["alice", "g1"]))
    await until(lambda: len(r.changes) == 1, "load")
    await first.fin()
    second = await fakes[0].next_request_stream()
    await second.first_message
    await second.send(encode_request_ok())
    await second.send(encode_namespace(["alice", "g1"]))
    await pause(0.3)
    assert [summary(c) for c in r.changes] == [LOADED_ALICE]


# ------------------------------------------------------------------ one discovery shared by wants and member views


class WantRecorder:
    def __init__(self) -> None:
        self.seen: list[str] = []
        self.handlers = WantHandlers(
            on_subscribed=lambda _info, namespace: self.seen.append(f"subscribed {'/'.join(namespace)}"),
            on_object=lambda obj: self.seen.append(f"object {obj.group_id}"),
            on_restarted=lambda reason, _namespace: self.seen.append(f"restarted {reason}"),
            on_error=lambda error: self.seen.append(f"error {error}"),
        )

    def failures(self) -> list[str]:
        return [seen for seen in self.seen if seen.startswith(("error", "restarted"))]


def unauthorized() -> SubscriptionRequestError:
    return request_error(0x1, "token does not allow the room")


async def test_serves_a_want_under_a_members_root_from_the_same_request_without_waiting_for_the_load(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, relay = await start(cleanup)
    view = subscriber.members(ROOT, quiet())
    d = await relay.discovery()
    generation = new_generation()
    d.push("announced", at("alice", generation))
    await pause()
    subscriber.want("alice", at("alice"), "audio", generations=True)
    await until(lambda: len(relay.connections[0].subscriptions) == 1, "subscribe")
    assert relay.connections[0].subscriptions[0].namespace == at("alice", generation)
    assert len(relay.connections[0].discoveries) == 1
    assert view.loaded is False


@dataclass
class WantThenMembers:
    subscriber: SupervisedSubscriber
    relay: Relay
    want: WantRecorder
    members: Recorder
    covering: FakeDiscovery
    subscription: FakeSubscription
    generation: str


async def want_then_members(cleanup: list[Callable[[], object]]) -> WantThenMembers:
    subscriber, relay = await start(cleanup)
    want = WantRecorder()
    subscriber.want("alice", at("alice"), "audio", want.handlers, generations=True, retry=FAST)
    narrow = await relay.discovery()
    assert narrow.prefix == at("alice")
    generation = new_generation()
    narrow.push("announced", at("alice", generation))
    await until(lambda: len(relay.connections[0].subscriptions) == 1, "subscribe")
    subscription = relay.connections[0].subscriptions[0]
    subscription.accept()
    subscription.send_object(0)
    members = Recorder()
    subscriber.members(ROOT, members.handlers)
    covering = await relay.discovery(0, 2)
    assert covering.prefix == ROOT
    assert narrow.closed
    subscription.send_object(1)
    await pause()
    assert not subscription.cancelled
    return WantThenMembers(subscriber, relay, want, members, covering, subscription, generation)


async def test_a_covering_request_replaces_a_wants_the_want_staying_subscribed_until_the_load_withdraws_it(
    cleanup: list[Callable[[], object]],
) -> None:
    w = await want_then_members(cleanup)
    w.covering.push("loaded")
    await until(lambda: "restarted generation" in w.want.seen, "the withdrawal reported at the load")
    assert w.want.seen == [
        f"subscribed {'/'.join(at('alice', w.generation))}",
        "object 0",
        "object 1",
        "restarted generation",
    ]
    assert w.subscription.cancelled
    assert len(w.relay.connections[0].discoveries) == 2
    await until(lambda: len(w.members.changes) == 1, "members load")
    assert summary(w.members.changes[0]) == change([], [], [], initial=True)


async def test_keeps_a_want_under_the_root_delivering_when_the_covering_members_request_is_refused(
    cleanup: list[Callable[[], object]],
) -> None:
    w = await want_then_members(cleanup)
    w.covering.fail(unauthorized())
    await until(lambda: len(w.members.errors) == 1, "members on_error")
    await pause(0.1)
    w.subscription.send_object(2)
    await pause()
    assert w.want.failures() == []
    assert w.want.seen[-1] == "object 2"
    assert not w.subscription.cancelled


async def test_keeps_a_want_served_from_a_members_request_delivering_when_that_request_is_refused(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, relay = await start(cleanup)
    members = Recorder()
    subscriber.members(ROOT, members.handlers)
    wide = await relay.discovery()
    wide.push("announced", at("alice", new_generation()))
    await pause()
    want = WantRecorder()
    subscriber.want("alice", at("alice"), "audio", want.handlers, generations=True, retry=FAST)
    await until(lambda: len(relay.connections[0].subscriptions) == 1, "subscribe")
    subscription = relay.connections[0].subscriptions[0]
    subscription.accept()
    subscription.send_object(0)
    await pause()
    wide.fail(unauthorized())
    await until(lambda: len(members.errors) == 1, "members on_error")
    await pause(0.1)
    subscription.send_object(1)
    await pause()
    assert want.failures() == []
    assert want.seen[-1] == "object 1"
    assert not subscription.cancelled
    assert [d.prefix for d in relay.connections[0].discoveries] == [ROOT, at("alice")]


async def test_never_restarts_a_want_under_the_root_while_the_members_request_is_refused_again_and_again(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, relay = await start(cleanup)
    want = WantRecorder()
    subscriber.want("alice", at("alice"), "audio", want.handlers, generations=True, retry=FAST)
    generation = new_generation()
    served: set[int] = set()
    refusals = 0

    async def serve() -> None:
        nonlocal refusals
        while True:
            for d in relay.connections[0].discoveries:
                if d.closed or id(d) in served:
                    continue
                served.add(id(d))
                if len(d.prefix) == len(ROOT):
                    refusals += 1
                    d.fail(request_error(0x9, "excessive load"))
                else:
                    d.push("announced", at("alice", generation))
                    d.push("loaded")
            for s in relay.connections[0].subscriptions:
                if id(s) in served:
                    continue
                served.add(id(s))
                s.accept()
                s.send_object(0)
            await asyncio.sleep(0.001)

    server = asyncio.ensure_future(serve())
    cleanup.append(server.cancel)
    await until(lambda: len(relay.connections[0].subscriptions) == 1, "subscribe")
    subscriber.members(ROOT, quiet())
    await until(lambda: refusals >= 3, "three refusals", 5.0)
    await pause()
    assert want.failures() == []
    assert len(relay.connections[0].subscriptions) == 1


async def test_reports_nothing_for_a_held_namespace_the_covering_request_confirms(
    cleanup: list[Callable[[], object]],
) -> None:
    w = await want_then_members(cleanup)
    w.covering.push("announced", at("alice", w.generation))
    w.covering.push("loaded")
    await until(lambda: len(w.members.changes) == 1, "members load")
    assert summary(w.members.changes[0]) == change(["alice"], [], ["alice"], initial=True)
    await pause()
    assert w.want.seen == [f"subscribed {'/'.join(at('alice', w.generation))}", "object 0", "object 1"]
    assert not w.subscription.cancelled
    assert len(w.relay.connections[0].subscriptions) == 1


# ------------------------------------------------------------------ the reducer


def test_maps_a_namespace_to_a_member_and_the_field_it_adds() -> None:
    assert _member_field(ROOT, at("alice")) == ("alice", "")
    assert _member_field(ROOT, at("alice", "g1")) == ("alice", "g1")
    assert _member_field(ROOT, ROOT) is None
    assert _member_field(ROOT, at("alice", "g1", "x")) is None


def test_joins_a_member_on_its_first_field_and_leaves_it_on_its_last() -> None:
    presence: _Presence = {}
    assert _apply_namespace(presence, ROOT, "announced", at("alice", "g1")) == ("joined", "alice")
    assert _apply_namespace(presence, ROOT, "announced", at("alice", "g2")) is None
    assert _apply_namespace(presence, ROOT, "unannounced", at("alice", "g1")) is None
    assert _apply_namespace(presence, ROOT, "unannounced", at("alice", "g1")) is None
    assert _apply_namespace(presence, ROOT, "unannounced", at("alice", "g2")) == ("left", "alice")
    assert presence == {}


# ------------------------------------------------------------------ the transition


class StubConnection:
    def __init__(self) -> None:
        self.discovery = cast(DiscoveryIndex, None)
        self.ended = CancellationToken()
        self.retiring = CancellationToken()

    def hold(self) -> Callable[[], None]:
        return lambda: None


def opened_source(effects: Sequence[object]) -> _Source:
    opened = effects[-1]
    assert isinstance(opened, _Open)
    return opened.source


def emitted(effect: object) -> MembersChange:
    assert isinstance(effect, _Emit)
    return effect.change


def loaded(*members: str) -> tuple[StubConnection, _Source, _Phase, tuple[object, ...]]:
    on = StubConnection()
    phase, effects = _transition(_Loading(), _Connected(on), ROOT)
    source = opened_source(effects)
    for member_id in members:
        phase = _transition(phase, _Namespace(source, "announced", at(member_id)), ROOT).phase
    step = _transition(phase, _Loaded(source), ROOT)
    return on, source, step.phase, step.effects


def following(_source: _Source) -> bool:
    return True


def test_opens_on_a_connection_and_emits_the_load_once() -> None:
    _on, source, phase, effects = loaded("alice")
    assert isinstance(phase, _Live)
    assert len(effects) == 1
    load = emitted(effects[0])
    assert load.initial
    assert ids(load.joined) == ["alice"]
    assert _transition(phase, _Loaded(source), ROOT).effects == ()


def test_ignores_an_input_about_a_source_the_phase_no_longer_holds() -> None:
    _on, _source, phase, _effects = loaded("alice")
    stranger = _Source(StubConnection())
    step = _transition(phase, _Namespace(stranger, "announced", at("bob")), ROOT)
    assert step.phase is phase
    assert step.effects == ()


def test_follows_a_rotation_make_before_break_and_a_member_the_new_load_misses_leaves_after_its_grace() -> None:
    on, source, phase, _effects = loaded("alice", "bob")
    retired = _transition(phase, _Retiring(on, StubConnection()), ROOT)
    assert isinstance(retired.phase, _Replacing)
    candidate = opened_source(retired.effects)
    assert retired.effects == (_Open(candidate),)
    replacing = _transition(retired.phase, _Namespace(candidate, "announced", at("alice")), ROOT).phase
    done = _transition(replacing, _Loaded(candidate), ROOT)
    assert isinstance(done.phase, _Live)
    assert done.phase.source is candidate
    assert done.phase.leaving == ("bob",)
    assert done.effects == (_Release(source), _ScheduleLeave("bob"))
    due = _transition(done.phase, _LeaveDue("bob", following), ROOT)
    assert len(due.effects) == 1
    left = emitted(due.effects[0])
    assert (ids(left.joined), ids(left.left)) == ([], ["bob"])


def test_settles_every_pending_leave_by_a_new_loads_snapshot_one_it_shows_stays_one_it_does_not_keeps_its_grace() -> (
    None
):
    on, source, phase, _effects = loaded("alice", "bob", "carol")
    for member_id in ("alice", "bob"):
        phase = _transition(phase, _Namespace(source, "unannounced", at(member_id)), ROOT).phase
    retired = _transition(phase, _Retiring(on, StubConnection()), ROOT)
    candidate = opened_source(retired.effects)
    replacing = retired.phase
    for member_id in ("alice", "carol"):
        replacing = _transition(replacing, _Namespace(candidate, "announced", at(member_id)), ROOT).phase
    done = _transition(replacing, _Loaded(candidate), ROOT)
    assert isinstance(done.phase, _Live)
    assert done.phase.leaving == ("bob",)
    assert done.effects == (_Release(source), _CancelLeave("alice"), _ScheduleLeave("bob"))
    due = _transition(done.phase, _LeaveDue("bob", following), ROOT)
    left = emitted(due.effects[-1])
    assert ids(left.left) == ["bob"]
    assert ids(left.present) == ["alice", "carol"]


def test_a_reconnect_reports_a_member_the_old_discovery_showed_as_left_at_once_and_holds_one_in_its_grace() -> None:
    _on, source, phase, _effects = loaded("alice", "bob")
    withdrawn = _transition(phase, _Namespace(source, "unannounced", at("bob")), ROOT).phase
    ended = _transition(withdrawn, _Ended(source, SubscriptionConnectionClosedError(None)), ROOT)
    assert isinstance(ended.phase, _Replacing)
    assert ended.phase.source is None
    reopened = _transition(ended.phase, _Connected(StubConnection()), ROOT)
    candidate = opened_source(reopened.effects)
    done = _transition(reopened.phase, _Loaded(candidate), ROOT)
    assert isinstance(done.phase, _Live)
    assert done.phase.leaving == ("bob",)
    assert done.effects[0] == _ScheduleLeave("bob")
    assert len(done.effects) == 2
    left = emitted(done.effects[1])
    assert (ids(left.joined), ids(left.left)) == ([], ["alice"])


def test_holds_a_member_whose_last_namespace_went_until_its_grace_runs_out_and_cancels_it_if_one_comes_back() -> None:
    _on, source, phase, _effects = loaded("alice")
    gone = _transition(phase, _Namespace(source, "unannounced", at("alice")), ROOT)
    assert gone.effects == (_ScheduleLeave("alice"),)
    back = _transition(gone.phase, _Namespace(source, "announced", at("alice", "g2")), ROOT)
    assert back.effects == (_CancelLeave("alice"),)
    assert _transition(back.phase, _LeaveDue("alice", following), ROOT).effects == ()
    closed = _transition(gone.phase, _Close(), ROOT)
    assert closed.effects == (_Release(source), _CancelLeave("alice"))


def test_waits_for_a_connection_retries_with_backoff_or_fails_by_how_discovery_ended() -> None:
    on, source, phase, _effects = loaded("alice")

    def ended(error: BaseException | None) -> tuple[_Phase, tuple[object, ...]]:
        return _transition(phase, _Ended(source, error), ROOT)

    _phase, effects = ended(SubscriptionConnectionClosedError(None))
    assert effects == (_Release(source), _AwaitConnection(on))
    retried, effects = ended(None)
    assert isinstance(retried, _Replacing)
    assert retried.failures == 1
    assert effects == (_Release(source), _Retry(0))
    refusal = request_error(0x1, "unauthorized")
    failed, effects = ended(refusal)
    assert isinstance(failed, _Closed)
    assert effects[-1] == _Error(refusal)


def test_does_nothing_once_closed() -> None:
    _on, source, phase, _effects = loaded("alice")
    closed = _transition(phase, _Close(), ROOT)
    assert closed.effects == (_Release(source),)
    assert _transition(closed.phase, _Connected(StubConnection()), ROOT).effects == ()
    assert _transition(closed.phase, _Close(), ROOT).effects == ()
