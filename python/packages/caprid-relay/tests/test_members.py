"""The supervised subscriber's member view.

Mirrors ``ts/tests/members.test.ts``: the pure reducer and transition.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import cast

from caprid_relay import CancellationToken, RequestError, SubscriptionConnectionClosedError, SubscriptionRequestError
from caprid_relay._discovery import DiscoveryIndex
from caprid_relay.members import (
    Member,
    MembersChange,
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

ROOT = ("room", "p")


def at(*fields: str) -> tuple[str, ...]:
    return (*ROOT, *fields)


def ids(members: Sequence[Member]) -> list[str]:
    return [member.id for member in members]


def request_error(code: int, reason: str) -> SubscriptionRequestError:
    return SubscriptionRequestError(RequestError(error_code=code, retry_interval=0, reason=reason))


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
