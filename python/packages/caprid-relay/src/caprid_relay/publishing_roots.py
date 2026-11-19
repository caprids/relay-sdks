"""The roots publishing under a prefix, as a supervised subscriber's publishing-roots view reports them.

Each namespace one field below the prefix is a root, and one field further is that root's
generation. A pure transition over the view's phase (loading, live, replacing, closed) decides what
to report and what to do; :class:`PublishingRoots` feeds it discovery's events through an inbox and
runs its effects. See ``spec/behavior.md``, "Publishing roots". The counterpart of
``ts/src/publishing-roots.ts``.
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
class PublishingRoot:
    """A root under the prefix: the namespace one field below it, publishing while a namespace at it or one
    field below it is announced. A supervised publisher announces its generations one field below its root;
    an unsupervised publisher announces the root itself (depth 1)."""

    id: str
    """The field one below the prefix, as the root's publisher chose it."""
    namespace: tuple[str, ...]
    """The prefix followed by ``id``. Want it with ``generations=True`` to follow the root's supervised publisher."""


@dataclass(frozen=True, slots=True)
class PublishingRootsChange:
    """What ``on_changed`` receives: which roots were added and removed since the last call, and which are
    publishing now."""

    added: tuple[PublishingRoot, ...]
    """Roots that started publishing. On the first call (``initial``), every root publishing when the view
    loaded, possibly none."""
    removed: tuple[PublishingRoot, ...]
    """Roots that stopped publishing. Empty on the first call."""
    present: tuple[PublishingRoot, ...]
    """Every root publishing after this change, in the order the view first saw them: the same tuple as
    :attr:`PublishingRoots.present`."""
    initial: bool
    """True on the first call only: the view has just loaded. A UI that announces arrivals skips this one."""


@dataclass(slots=True, kw_only=True)
class PublishingRootsHandlers:
    """The callbacks a publishing-roots view delivers to. Like :class:`WantHandlers`, they must be synchronous."""

    on_changed: Callable[[PublishingRootsChange], None]
    """The view loaded, or roots were added or removed. The first call is the load and runs once, even for
    an empty prefix; every later call names at least one root in ``added`` or ``removed``. A root whose
    last namespace is withdrawn is removed once it has had none for 0.1 s, so a rotation whose withdrawal
    arrives just before the new generation's announce calls nothing, and neither do a root's generations
    changing or the relay repeating an announce. After a reconnect, rotation or retry, one call carries
    the roots the new discovery adds, if there are any. A root the new discovery misses is removed in a
    later call, once the new discovery has shown it nothing for 0.1 s."""
    on_error: Callable[[BaseException], None] | None = None
    """A discovery failure no wait fixes: the relay refusing the request with a REQUEST_ERROR that does not
    invite a retry, such as NOT_SUPPORTED from a relay without namespace discovery. The view is closed. A
    prefix outside the token's grant is not one: moq-relay accepts the request and sends nothing, so the
    view loads empty and this is never called. An empty view with no error can therefore mean the token
    does not grant the prefix."""


_Presence: TypeAlias = dict[str, set[str]]
"""Each root's fields under the prefix: ``''`` for an unsupervised publisher's own namespace, else a generation."""


def _root_field(prefix: Sequence[str], namespace: Sequence[str]) -> tuple[str, str] | None:
    if len(namespace) == len(prefix) + 1:
        return namespace[len(prefix)], ""
    if len(namespace) == len(prefix) + 2:
        return namespace[len(prefix)], namespace[len(prefix) + 1]
    return None


def _apply_namespace(
    presence: _Presence,
    prefix: Sequence[str],
    kind: Literal["announced", "unannounced"],
    namespace: Sequence[str],
) -> tuple[Literal["added", "removed"], str] | None:
    at = _root_field(prefix, namespace)
    if at is None:
        return None
    root_id, root_field = at
    fields = presence.get(root_id)
    if kind == "announced":
        if fields is not None:
            fields.add(root_field)
            return None
        presence[root_id] = {root_field}
        return "added", root_id
    if fields is None or root_field not in fields:
        return None
    fields.discard(root_field)
    if fields:
        return None
    del presence[root_id]
    return "removed", root_id


def _publishing_root(prefix: Sequence[str], root_id: str) -> PublishingRoot:
    return PublishingRoot(root_id, (*prefix, root_id))


@dataclass(frozen=True, slots=True, eq=False)
class _Settled:
    """What the view last reported; ``present`` is handed out unchanged so callers can compare it by identity."""

    present: tuple[PublishingRoot, ...]
    by_id: Mapping[str, PublishingRoot]


def _settle(roots: Iterable[PublishingRoot]) -> _Settled:
    present = tuple(roots)
    return _Settled(present, {root.id: root for root in present})


def _diff_roots(
    settled: _Settled, loaded: _Presence, prefix: Sequence[str]
) -> tuple[tuple[PublishingRoot, ...], tuple[PublishingRoot, ...]]:
    added = tuple(_publishing_root(prefix, root_id) for root_id in loaded if root_id not in settled.by_id)
    removed = tuple(known for known in settled.present if known.id not in loaded)
    return added, removed


class _PublishingRootsConnection(Protocol):
    discovery: DiscoveryIndex
    ended: CancellationToken
    retiring: CancellationToken

    def hold(self) -> Callable[[], None]: ...


@dataclass(slots=True, eq=False)
class _Source:
    """One discovery the view follows, and the presence it has shown."""

    connection: _PublishingRootsConnection
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
    connection: _PublishingRootsConnection


@dataclass(frozen=True, slots=True)
class _Retiring:
    connection: _PublishingRootsConnection
    to: _PublishingRootsConnection | None


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
    change: PublishingRootsChange


@dataclass(frozen=True, slots=True)
class _ScheduleLeave:
    id: str


@dataclass(frozen=True, slots=True)
class _CancelLeave:
    id: str


@dataclass(frozen=True, slots=True)
class _AwaitConnection:
    after: _PublishingRootsConnection


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


def _emit(
    added: Iterable[PublishingRoot], removed: Iterable[PublishingRoot], settled: _Settled, *, initial: bool = False
) -> _Emit:
    return _Emit(PublishingRootsChange(tuple(added), tuple(removed), settled.present, initial))


def _without(leaving: tuple[str, ...], root_id: str) -> tuple[str, ...]:
    return tuple(other for other in leaving if other != root_id)


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


def _retired_to(source: _Source, event: _Retiring) -> _PublishingRootsConnection | None:
    if source.connection is not event.connection or event.to is event.connection:
        return None
    return event.to


def _follow(phase: _Live | _Replacing, source: _Source, event: _Namespace, prefix: Sequence[str]) -> _Step:
    change = _apply_namespace(source.presence, prefix, event.kind, event.namespace)
    if change is None:
        return _Step(phase)
    kind, root_id = change
    if kind == "removed":
        leaving = phase.leaving if root_id in phase.leaving else (*phase.leaving, root_id)
        return _Step(replace(phase, leaving=leaving), (_ScheduleLeave(root_id),))
    if root_id in phase.leaving:
        return _Step(replace(phase, leaving=_without(phase.leaving, root_id)), (_CancelLeave(root_id),))
    added = _publishing_root(prefix, root_id)
    settled = _settle((*phase.settled.present, added))
    return _Step(replace(phase, settled=settled), (_emit((added,), (), settled),))


def _leave_due(phase: _Live | _Replacing, root_id: str) -> _Step:
    gone = phase.settled.by_id.get(root_id)
    if gone is None or root_id not in phase.leaving or any(root_id in s.presence for s in _sources_of(phase)):
        return _Step(phase)
    settled = _settle(root for root in phase.settled.present if root.id != root_id)
    leaving = _without(phase.leaving, root_id)
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


def _from_loading(phase: _Loading, event: _Input, prefix: Sequence[str]) -> _Step:
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
            _apply_namespace(candidate.presence, prefix, event.kind, event.namespace)
        return _Step(phase)
    if isinstance(event, _Loaded):
        if candidate is None or event.source is not candidate:
            return _Step(phase)
        settled = _settle(_publishing_root(prefix, root_id) for root_id in candidate.presence)
        return _Step(_Live(settled, (), candidate), (_emit(settled.present, (), settled, initial=True),))
    if isinstance(event, _Ended):
        if candidate is None or event.source is not candidate:
            return _Step(phase)
        return _after_ended(replace(phase, candidate=None), event, (_Release(candidate),))
    return _Step(phase)


def _from_live(phase: _Live, event: _Input, prefix: Sequence[str]) -> _Step:
    source = phase.source
    if isinstance(event, _Namespace):
        return _follow(phase, source, event, prefix) if event.source is source else _Step(phase)
    if isinstance(event, _LeaveDue):
        return _leave_due(phase, event.id) if event.following(source) else _Step(phase)
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


def _adopt(phase: _Replacing, candidate: _Source, prefix: Sequence[str]) -> _Step:
    """A root the new snapshot misses is never removed at the load. It gets the grace against the new discovery.

    The relay may have taken the snapshot inside a publisher rotation's gap, or just before the
    root's announce reached it.
    """
    added, missing = _diff_roots(phase.settled, candidate.presence, prefix)
    leaving = tuple(gone.id for gone in missing)
    settled = _settle((*phase.settled.present, *added)) if added else phase.settled
    return _Step(
        _Live(settled, leaving, candidate),
        (
            *(() if phase.source is None else (_Release(phase.source),)),
            *(_CancelLeave(root_id) for root_id in phase.leaving if root_id not in leaving),
            *map(_ScheduleLeave, leaving),
            *(() if settled is phase.settled else (_emit(added, (), settled),)),
        ),
    )


def _from_replacing(phase: _Replacing, event: _Input, prefix: Sequence[str]) -> _Step:
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
            _apply_namespace(candidate.presence, prefix, event.kind, event.namespace)
            return _Step(phase)
        if source is not None and event.source is source:
            return _follow(phase, source, event, prefix)
        return _Step(phase)
    if isinstance(event, _LeaveDue):
        if source is not None and event.following(source):
            return _leave_due(phase, event.id)
        return _Step(phase)
    if isinstance(event, _Loaded):
        if candidate is not None and event.source is candidate:
            return _adopt(phase, candidate, prefix)
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


def _transition(phase: _Phase, event: _Input, prefix: Sequence[str]) -> _Step:
    if isinstance(phase, _Closed):
        return _Step(phase)
    if isinstance(event, _Close):
        return _Step(_Closed(_settled_of(phase)), _abandon(phase))
    if isinstance(phase, _Loading):
        return _from_loading(phase, event, prefix)
    if isinstance(phase, _Live):
        return _from_live(phase, event, prefix)
    return _from_replacing(phase, event, prefix)


_LEAVE_GRACE = 0.1
"""Seconds a root stays before it is reported as removed, after its last namespace is withdrawn live or
after a newly loaded discovery's snapshot misses it. They count only while the followed discovery delivers:
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
        _log.exception("%s raised; the publishing-roots view carries on", name)


@dataclass(slots=True)
class _Opened:
    watch: DiscoveryWatch
    detach: Callable[[], None]


class PublishingRoots:
    """A live view of the roots publishing under a prefix. Create one with
    :meth:`SupervisedSubscriber.publishing_roots`."""

    __slots__ = (
        "_current",
        "_draining",
        "_hides",
        "_inbox",
        "_leaves",
        "_live_connection",
        "_on_changed",
        "_on_closed",
        "_on_error",
        "_opened",
        "_phase",
        "_prefix",
        "_scope",
    )

    def __init__(
        self,
        prefix: tuple[str, ...],
        on_changed: Callable[[PublishingRootsChange], None],
        on_error: Callable[[BaseException], None] | None,
        *,
        live_connection: Callable[[CancellationToken], Awaitable[_PublishingRootsConnection | None]],
        current: Callable[[], _PublishingRootsConnection | None],
        on_closed: Callable[[], None],
        hides: Callable[[tuple[str, ...]], bool],
    ) -> None:
        self._prefix = prefix
        self._on_changed = on_changed
        self._on_error = on_error
        self._live_connection = live_connection
        self._current = current
        self._on_closed = on_closed
        # Whether a namespace is the supervisor's own publishing: its announcement is never reported. The relay
        # hides a connection's own announcements from it, but not those of the connection a rotation is replacing
        # or bringing up. Withdrawals still count, so a root seen before the supervisor published it leaves when it
        # is withdrawn.
        self._hides = hides
        self._phase: _Phase = _Loading()
        self._inbox: deque[_Input] = deque()
        self._draining = False
        self._scope = CancellationToken()
        self._opened: dict[_Source, _Opened] = {}
        self._leaves: dict[str, asyncio.TimerHandle] = {}
        self._connect_after(limit=0)

    @property
    def loaded(self) -> bool:
        """False until the prefix's first discovery has reported everything announced under it; then
        ``present`` is empty only when no root is publishing. Stays true across reconnects, while the last
        settled view is held."""
        return _settled_of(self._phase) is not None

    @property
    def present(self) -> tuple[PublishingRoot, ...]:
        """The roots publishing, in the order the view first saw them: empty until loaded, and the last
        settled view while a reconnect's discovery loads. A root whose last namespace went stays
        until it is reported as removed. The same tuple until the next ``on_changed``, so a UI can
        compare it by identity."""
        settled = _settled_of(self._phase)
        return () if settled is None else settled.present

    def close(self) -> None:
        """Stop following the prefix. Idempotent; nothing is delivered after it, even when called from
        ``on_changed``."""
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
                self._phase, effects = _transition(was, self._inbox.popleft(), self._prefix)
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

    def _leave_due(self, root_id: str) -> None:
        self._leaves.pop(root_id, None)
        self._feed(_LeaveDue(root_id, self._following))

    def _cancel_leave(self, root_id: str) -> None:
        timer = self._leaves.pop(root_id, None)
        if timer is not None:
            timer.cancel()

    def _watch(self, source: _Source) -> None:
        connection = source.connection
        try:
            watch = connection.discovery.watch(
                self._prefix,
                DiscoveryWatchHandlers(
                    on_announced=lambda namespace: self._announced(source, namespace),
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
            self._announced(source, namespace)
        if watch.loaded:
            self._feed(_Loaded(source))
        if connection.retiring.cancelled:
            retiring()

    def _announced(self, source: _Source, namespace: tuple[str, ...]) -> None:
        if not self._hides(namespace):
            self._feed(_Namespace(source, "announced", namespace))

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


__all__ = ["PublishingRoot", "PublishingRoots", "PublishingRootsChange", "PublishingRootsHandlers"]
