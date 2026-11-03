"""Who is present under a root, as a supervised subscriber's member view reports it.

Each namespace one field below the root is a member, and one field further is that member's
generation. A pure transition over the view's phase (loading, live, replacing, closed) decides what
to report and what to do; :class:`Members` feeds it discovery's events through an inbox and runs
its effects. See ``spec/behavior.md``, "Members". The counterpart of ``ts/src/members.ts``.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Literal, NamedTuple, Protocol, TypeAlias

from ._discovery import DiscoveryIndex, DiscoveryWatch, DiscoveryWatchHandlers, discovery_ending, refused_for_good
from ._subscription import SubscriptionConnectionClosedError
from ._supervision import RESUBSCRIBE_BACKOFF, backoff_delay, wait_any
from ._write_slot import _observe
from .cancellation import CancellationToken

_log = logging.getLogger("caprid_relay")


@dataclass(frozen=True, slots=True)
class Member:
    """One participant under a root: a supervised publisher's root, or an unsupervised publisher's namespace."""

    id: str
    """The field one below the root, as the participant's publisher chose it."""
    namespace: tuple[str, ...]
    """The root followed by ``id``. Want it with ``generations=True`` to follow the member's supervised publisher."""


@dataclass(frozen=True, slots=True)
class MembersChange:
    """What ``on_changed`` receives: who joined and who left since the last call, and who is present now."""

    joined: tuple[Member, ...]
    """Members who arrived. On the first call (``initial``), everyone present when the view loaded, possibly no one."""
    left: tuple[Member, ...]
    """Members who went. Empty on the first call."""
    present: tuple[Member, ...]
    """Everyone present after this change, in the order the view first saw them: the same tuple as
    :attr:`Members.present`."""
    initial: bool
    """True on the first call only: the view has just loaded. A UI that announces joins skips this one."""


@dataclass(slots=True, kw_only=True)
class MembersHandlers:
    """The callbacks a members view delivers to. Like :class:`WantHandlers`, they must be synchronous."""

    on_changed: Callable[[MembersChange], None]
    """The view loaded, or members joined or left. The first call is the load and runs once, even for
    an empty root; every later call names at least one member in ``joined`` or ``left``. A member
    whose last namespace is withdrawn leaves once it has had none for 0.1 s, so a rotation whose
    withdrawal arrives just before the new generation's announce calls nothing, and neither do a
    member's generations changing or the relay repeating an announce. After a reconnect, rotation or
    retry, one call carries the difference the new discovery shows, if there is any, joins and
    departures together. The exception is a member the
    new discovery misses but the old one still showed, or was giving its 0.1 s: it leaves in a later
    call, once the new discovery has shown it nothing for 0.1 s."""
    on_error: Callable[[BaseException], None] | None = None
    """A discovery failure no wait fixes, such as the token not allowing the root. The view is closed."""


_Presence: TypeAlias = dict[str, set[str]]
"""Each member's fields under the root: ``''`` for an unsupervised publisher's own namespace, else a generation."""


def _member_field(root: Sequence[str], namespace: Sequence[str]) -> tuple[str, str] | None:
    if len(namespace) == len(root) + 1:
        return namespace[len(root)], ""
    if len(namespace) == len(root) + 2:
        return namespace[len(root)], namespace[len(root) + 1]
    return None


def _apply_namespace(
    presence: _Presence,
    root: Sequence[str],
    kind: Literal["announced", "unannounced"],
    namespace: Sequence[str],
) -> tuple[Literal["joined", "left"], str] | None:
    at = _member_field(root, namespace)
    if at is None:
        return None
    member_id, member_field = at
    fields = presence.get(member_id)
    if kind == "announced":
        if fields is not None:
            fields.add(member_field)
            return None
        presence[member_id] = {member_field}
        return "joined", member_id
    if fields is None or member_field not in fields:
        return None
    fields.discard(member_field)
    if fields:
        return None
    del presence[member_id]
    return "left", member_id


def _member(root: Sequence[str], member_id: str) -> Member:
    return Member(member_id, (*root, member_id))


@dataclass(frozen=True, slots=True, eq=False)
class _Settled:
    """What the view last reported; ``present`` is handed out unchanged so callers can compare it by identity."""

    present: tuple[Member, ...]
    by_id: Mapping[str, Member]


def _settle(members: Iterable[Member]) -> _Settled:
    present = tuple(members)
    return _Settled(present, {member.id: member for member in present})


def _diff_members(
    settled: _Settled, loaded: _Presence, root: Sequence[str]
) -> tuple[tuple[Member, ...], tuple[Member, ...]]:
    joined = tuple(_member(root, member_id) for member_id in loaded if member_id not in settled.by_id)
    left = tuple(known for known in settled.present if known.id not in loaded)
    return joined, left


class _MembersConnection(Protocol):
    discovery: DiscoveryIndex
    ended: CancellationToken
    retiring: CancellationToken

    def hold(self) -> Callable[[], None]: ...


@dataclass(slots=True, eq=False)
class _Source:
    """One discovery the view follows, and the presence it has shown."""

    connection: _MembersConnection
    presence: _Presence = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class _Loading:
    candidate: _Source | None = None
    failures: int = 0


@dataclass(frozen=True, slots=True)
class _Live:
    settled: _Settled
    leaving: tuple[str, ...]
    source: _Source


@dataclass(frozen=True, slots=True)
class _Replacing:
    settled: _Settled
    leaving: tuple[str, ...]
    source: _Source | None
    candidate: _Source | None
    failures: int = 0


@dataclass(frozen=True, slots=True)
class _Closed:
    settled: _Settled | None


_Phase: TypeAlias = _Loading | _Live | _Replacing | _Closed


@dataclass(frozen=True, slots=True)
class _Connected:
    connection: _MembersConnection


@dataclass(frozen=True, slots=True)
class _Retiring:
    connection: _MembersConnection
    to: _MembersConnection | None


@dataclass(frozen=True, slots=True)
class _Namespace:
    source: _Source
    kind: Literal["announced", "unannounced"]
    namespace: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _Loaded:
    source: _Source


@dataclass(frozen=True, slots=True)
class _Ended:
    source: _Source
    error: BaseException | None


@dataclass(frozen=True, slots=True)
class _LeaveDue:
    id: str
    following: Callable[[_Source], bool]


@dataclass(frozen=True, slots=True)
class _Close:
    pass


_Input: TypeAlias = _Connected | _Retiring | _Namespace | _Loaded | _Ended | _LeaveDue | _Close


@dataclass(frozen=True, slots=True)
class _Open:
    source: _Source


@dataclass(frozen=True, slots=True)
class _Release:
    source: _Source


@dataclass(frozen=True, slots=True)
class _Emit:
    change: MembersChange


@dataclass(frozen=True, slots=True)
class _ScheduleLeave:
    id: str


@dataclass(frozen=True, slots=True)
class _CancelLeave:
    id: str


@dataclass(frozen=True, slots=True)
class _AwaitConnection:
    after: _MembersConnection


@dataclass(frozen=True, slots=True)
class _Retry:
    attempt: int


@dataclass(frozen=True, slots=True)
class _Error:
    error: BaseException


_Effect: TypeAlias = _Open | _Release | _Emit | _ScheduleLeave | _CancelLeave | _AwaitConnection | _Retry | _Error


class _Step(NamedTuple):
    phase: _Phase
    effects: tuple[_Effect, ...] = ()


def _emit(joined: Iterable[Member], left: Iterable[Member], settled: _Settled, *, initial: bool = False) -> _Emit:
    return _Emit(MembersChange(tuple(joined), tuple(left), settled.present, initial))


def _without(leaving: tuple[str, ...], member_id: str) -> tuple[str, ...]:
    return tuple(other for other in leaving if other != member_id)


def _settled_of(phase: _Phase) -> _Settled | None:
    return None if isinstance(phase, _Loading) else phase.settled


def _sources_of(phase: _Phase) -> list[_Source]:
    if isinstance(phase, _Loading):
        return [] if phase.candidate is None else [phase.candidate]
    if isinstance(phase, _Live):
        return [phase.source]
    if isinstance(phase, _Replacing):
        return [source for source in (phase.source, phase.candidate) if source is not None]
    return []


def _abandon(phase: _Phase) -> tuple[_Effect, ...]:
    leaving = phase.leaving if isinstance(phase, _Live | _Replacing) else ()
    return (*map(_Release, _sources_of(phase)), *map(_CancelLeave, leaving))


def _retired_to(source: _Source, event: _Retiring) -> _MembersConnection | None:
    if source.connection is not event.connection or event.to is event.connection:
        return None
    return event.to


def _follow(phase: _Live | _Replacing, source: _Source, event: _Namespace, root: Sequence[str]) -> _Step:
    change = _apply_namespace(source.presence, root, event.kind, event.namespace)
    if change is None:
        return _Step(phase)
    kind, member_id = change
    if kind == "left":
        leaving = phase.leaving if member_id in phase.leaving else (*phase.leaving, member_id)
        return _Step(replace(phase, leaving=leaving), (_ScheduleLeave(member_id),))
    if member_id in phase.leaving:
        return _Step(replace(phase, leaving=_without(phase.leaving, member_id)), (_CancelLeave(member_id),))
    joined = _member(root, member_id)
    settled = _settle((*phase.settled.present, joined))
    return _Step(replace(phase, settled=settled), (_emit((joined,), (), settled),))


def _leave_due(phase: _Live | _Replacing, member_id: str, followed: _Presence) -> _Step:
    gone = phase.settled.by_id.get(member_id)
    if gone is None or member_id not in phase.leaving or member_id in followed:
        return _Step(phase)
    settled = _settle(member for member in phase.settled.present if member.id != member_id)
    leaving = _without(phase.leaving, member_id)
    return _Step(replace(phase, settled=settled, leaving=leaving), (_emit((), (gone,), settled),))


def _resume(phase: _Live | _Replacing) -> _Step:
    return _Step(phase, tuple(map(_ScheduleLeave, phase.leaving)))


def _after_ended(phase: _Loading | _Replacing, ended: _Ended, effects: tuple[_Effect, ...]) -> _Step:
    ending = discovery_ending(ended.error)
    if ending == "connection":
        return _Step(phase, (*effects, _AwaitConnection(ended.source.connection)))
    if ending == "retry":
        return _Step(replace(phase, failures=phase.failures + 1), (*effects, _Retry(phase.failures)))
    # discovery_ending calls a clean end a retry, so a fatal ending always carries its error.
    assert ended.error is not None
    return _Step(_Closed(_settled_of(phase)), (*effects, *_abandon(phase), _Error(ended.error)))


def _from_loading(phase: _Loading, event: _Input, root: Sequence[str]) -> _Step:
    candidate = phase.candidate
    if isinstance(event, _Connected):
        if candidate is not None:
            return _Step(phase)
        opened = _Source(event.connection)
        return _Step(replace(phase, candidate=opened), (_Open(opened),))
    if isinstance(event, _Retiring):
        to = None if candidate is None else _retired_to(candidate, event)
        if candidate is None or to is None:
            return _Step(phase)
        opened = _Source(to)
        return _Step(replace(phase, candidate=opened), (_Release(candidate), _Open(opened)))
    if isinstance(event, _Namespace):
        if candidate is not None and event.source is candidate:
            _apply_namespace(candidate.presence, root, event.kind, event.namespace)
        return _Step(phase)
    if isinstance(event, _Loaded):
        if candidate is None or event.source is not candidate:
            return _Step(phase)
        settled = _settle(_member(root, member_id) for member_id in candidate.presence)
        return _Step(_Live(settled, (), candidate), (_emit(settled.present, (), settled, initial=True),))
    if isinstance(event, _Ended):
        if candidate is None or event.source is not candidate:
            return _Step(phase)
        return _after_ended(replace(phase, candidate=None), event, (_Release(candidate),))
    return _Step(phase)


def _from_live(phase: _Live, event: _Input, root: Sequence[str]) -> _Step:
    source = phase.source
    if isinstance(event, _Namespace):
        return _follow(phase, source, event, root) if event.source is source else _Step(phase)
    if isinstance(event, _LeaveDue):
        return _leave_due(phase, event.id, source.presence) if event.following(source) else _Step(phase)
    if isinstance(event, _Loaded):
        return _resume(phase) if event.source is source else _Step(phase)
    if isinstance(event, _Retiring):
        to = _retired_to(source, event)
        if to is None:
            return _Step(phase)
        candidate = _Source(to)
        return _Step(_Replacing(phase.settled, phase.leaving, source, candidate), (_Open(candidate),))
    if isinstance(event, _Ended):
        if event.source is not source:
            return _Step(phase)
        return _after_ended(_Replacing(phase.settled, phase.leaving, None, None), event, (_Release(source),))
    return _Step(phase)


def _adopt(phase: _Replacing, candidate: _Source, root: Sequence[str]) -> _Step:
    """A member the new snapshot misses leaves at once only when nothing else shows it.

    One the old discovery still shows, or one already in its grace, may have announced just after
    the snapshot was taken, so it gets the grace against the new discovery instead.
    """
    joined, missing = _diff_members(phase.settled, candidate.presence, root)

    def doubtful(member_id: str) -> bool:
        return member_id in phase.leaving or (phase.source is not None and member_id in phase.source.presence)

    left = tuple(gone for gone in missing if not doubtful(gone.id))
    leaving = tuple(gone.id for gone in missing if doubtful(gone.id))
    settled = phase.settled
    if joined or left:
        gone_ids = {gone.id for gone in left}
        settled = _settle((*(member for member in settled.present if member.id not in gone_ids), *joined))
    return _Step(
        _Live(settled, leaving, candidate),
        (
            *(() if phase.source is None else (_Release(phase.source),)),
            *(_CancelLeave(member_id) for member_id in phase.leaving if member_id not in leaving),
            *map(_ScheduleLeave, leaving),
            *(() if settled is phase.settled else (_emit(joined, left, settled),)),
        ),
    )


def _from_replacing(phase: _Replacing, event: _Input, root: Sequence[str]) -> _Step:
    source, candidate = phase.source, phase.candidate
    if isinstance(event, _Connected):
        if candidate is not None:
            return _Step(phase)
        opened = _Source(event.connection)
        return _Step(replace(phase, candidate=opened), (_Open(opened),))
    if isinstance(event, _Retiring):
        retiring = candidate if candidate is not None else source
        to = None if retiring is None else _retired_to(retiring, event)
        if to is None:
            return _Step(phase)
        opened = _Source(to)
        return _Step(
            replace(phase, candidate=opened),
            (*(() if candidate is None else (_Release(candidate),)), _Open(opened)),
        )
    if isinstance(event, _Namespace):
        if candidate is not None and event.source is candidate:
            _apply_namespace(candidate.presence, root, event.kind, event.namespace)
            return _Step(phase)
        if source is not None and event.source is source:
            return _follow(phase, source, event, root)
        return _Step(phase)
    if isinstance(event, _LeaveDue):
        if source is not None and event.following(source):
            return _leave_due(phase, event.id, source.presence)
        return _Step(phase)
    if isinstance(event, _Loaded):
        if candidate is not None and event.source is candidate:
            return _adopt(phase, candidate, root)
        return _resume(phase) if source is not None and event.source is source else _Step(phase)
    if isinstance(event, _Ended):
        if candidate is not None and event.source is candidate:
            return _after_ended(replace(phase, candidate=None), event, (_Release(candidate),))
        if source is None or event.source is not source:
            return _Step(phase)
        if candidate is not None:
            return _Step(replace(phase, source=None), (_Release(source),))
        return _after_ended(replace(phase, source=None), event, (_Release(source),))
    return _Step(phase)


def _transition(phase: _Phase, event: _Input, root: Sequence[str]) -> _Step:
    if isinstance(phase, _Closed):
        return _Step(phase)
    if isinstance(event, _Close):
        return _Step(_Closed(_settled_of(phase)), _abandon(phase))
    if isinstance(phase, _Loading):
        return _from_loading(phase, event, root)
    if isinstance(phase, _Live):
        return _from_live(phase, event, root)
    return _from_replacing(phase, event, root)


_LEAVE_GRACE = 0.1
"""Seconds a member stays after its last namespace is withdrawn live before it is reported as left; a
newly loaded discovery's snapshot needs none. They count only while the followed discovery delivers:
a grace that runs out while the request is replaced or its stream has failed waits for the next load,
which settles it or starts it again. The relay orders nothing across two sessions: with no demand on
the rotating publisher, the old generation's withdrawal arrived before the new generation's announce
in 24 of 75 rotations measured on ord and fra (2026-11-03), by at most 0.2 ms."""

_AWAIT_CONNECTION_LIMIT = 1.0
"""Seconds a view whose discovery lost its connection waits for that connection's end to be known
before it asks for the live connection again."""


def _deliver(name: str, call: Callable[[], None]) -> None:
    try:
        call()
    except Exception:
        _log.exception("%s raised; the members view carries on", name)


@dataclass(slots=True)
class _Opened:
    watch: DiscoveryWatch
    detach: Callable[[], None]


class Members:
    """A live view of who is present under a root. Create one with :meth:`SupervisedSubscriber.members`."""

    __slots__ = (
        "_current",
        "_draining",
        "_inbox",
        "_leaves",
        "_live_connection",
        "_on_changed",
        "_on_closed",
        "_on_error",
        "_opened",
        "_phase",
        "_root",
        "_scope",
    )

    def __init__(
        self,
        root: tuple[str, ...],
        on_changed: Callable[[MembersChange], None],
        on_error: Callable[[BaseException], None] | None,
        *,
        live_connection: Callable[[CancellationToken], Awaitable[_MembersConnection | None]],
        current: Callable[[], _MembersConnection | None],
        on_closed: Callable[[], None],
    ) -> None:
        self._root = root
        self._on_changed = on_changed
        self._on_error = on_error
        self._live_connection = live_connection
        self._current = current
        self._on_closed = on_closed
        self._phase: _Phase = _Loading()
        self._inbox: deque[_Input] = deque()
        self._draining = False
        self._scope = CancellationToken()
        self._opened: dict[_Source, _Opened] = {}
        self._leaves: dict[str, asyncio.TimerHandle] = {}
        self._connect_after(limit=0)

    @property
    def loaded(self) -> bool:
        """False until the root's first discovery has reported everything announced under it; then
        ``present`` is empty only when nobody is there. Stays true across reconnects, while the last
        settled view is held."""
        return _settled_of(self._phase) is not None

    @property
    def present(self) -> tuple[Member, ...]:
        """Who is present, in the order the view first saw them: empty until loaded, and the last
        settled view while a reconnect's discovery loads. A member whose last namespace went stays
        until it is reported as left. The same tuple until the next ``on_changed``, so a UI can
        compare it by identity."""
        settled = _settled_of(self._phase)
        return () if settled is None else settled.present

    def close(self) -> None:
        """Stop following the root. Idempotent; nothing is delivered after it, even when called from ``on_changed``."""
        self._feed(_Close(), first=True)

    def _feed(self, event: _Input, *, first: bool = False) -> None:
        if first:
            self._inbox.appendleft(event)
        else:
            self._inbox.append(event)
        if self._draining:
            return
        self._draining = True
        try:
            while self._inbox:
                was = self._phase
                self._phase, effects = _transition(was, self._inbox.popleft(), self._root)
                if isinstance(self._phase, _Closed) and not isinstance(was, _Closed):
                    self._scope.cancel()
                    self._on_closed()
                for effect in effects:
                    self._run(effect)
        finally:
            self._draining = False

    def _connect_after(self, tokens: Sequence[CancellationToken] = (), *, limit: float | None = None) -> None:
        async def connect() -> None:
            await wait_any([self._scope, *tokens], limit=limit)
            connection = await self._live_connection(self._scope)
            if connection is not None:
                self._feed(_Connected(connection))

        _observe(asyncio.ensure_future(connect()))

    def _following(self, source: _Source) -> bool:
        opened = self._opened.get(source)
        return opened is not None and opened.watch.following

    def _leave_due(self, member_id: str) -> None:
        self._leaves.pop(member_id, None)
        self._feed(_LeaveDue(member_id, self._following))

    def _cancel_leave(self, member_id: str) -> None:
        timer = self._leaves.pop(member_id, None)
        if timer is not None:
            timer.cancel()

    def _watch(self, source: _Source) -> None:
        connection = source.connection
        try:
            watch = connection.discovery.watch(
                self._root,
                DiscoveryWatchHandlers(
                    on_announced=lambda namespace: self._feed(_Namespace(source, "announced", namespace)),
                    on_unannounced=lambda namespace: self._feed(_Namespace(source, "unannounced", namespace)),
                    on_loaded=lambda: self._feed(_Loaded(source)),
                    on_ended=lambda error: self._feed(_Ended(source, error)),
                ),
            )
        except Exception as error:
            ended = error if refused_for_good(error) else SubscriptionConnectionClosedError(error)
            self._feed(_Ended(source, ended))
            return
        # A source holds its connection, so a rotation keeps the old discovery until the new one has loaded.
        release = connection.hold()

        def retiring(_reason: BaseException | None = None) -> None:
            self._feed(_Retiring(connection, self._current()))

        connection.retiring.add_callback(retiring)

        def detach() -> None:
            connection.retiring.remove_callback(retiring)
            release()

        self._opened[source] = _Opened(watch, detach)
        for namespace in watch.namespaces():
            self._feed(_Namespace(source, "announced", namespace))
        if watch.loaded:
            self._feed(_Loaded(source))
        if connection.retiring.cancelled:
            retiring()

    def _run(self, effect: _Effect) -> None:
        if isinstance(effect, _Open):
            self._watch(effect.source)
        elif isinstance(effect, _Release):
            opened = self._opened.pop(effect.source, None)
            if opened is not None:
                opened.watch.release()
                opened.detach()
        elif isinstance(effect, _ScheduleLeave):
            self._cancel_leave(effect.id)
            loop = asyncio.get_running_loop()
            self._leaves[effect.id] = loop.call_later(_LEAVE_GRACE, self._leave_due, effect.id)
        elif isinstance(effect, _CancelLeave):
            self._cancel_leave(effect.id)
        elif isinstance(effect, _Emit):
            change = effect.change
            _deliver("on_changed", lambda: self._on_changed(change))
        elif isinstance(effect, _Error):
            on_error, error = self._on_error, effect.error
            if on_error is not None:
                _deliver("on_error", lambda: on_error(error))
        elif isinstance(effect, _AwaitConnection):
            self._connect_after((effect.after.ended,), limit=_AWAIT_CONNECTION_LIMIT)
        else:
            self._connect_after(limit=backoff_delay(effect.attempt, RESUBSCRIBE_BACKOFF))


__all__ = ["Member", "Members", "MembersChange", "MembersHandlers"]
