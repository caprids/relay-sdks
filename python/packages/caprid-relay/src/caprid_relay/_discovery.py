"""One connection's namespace discovery, shared by every want and member view on it.

Draft-22 §9.15 allows one SUBSCRIBE_NAMESPACE per prefix tree on a session, so the index keeps open
requests whose prefixes never overlap. A watch whose prefix an open request covers is served by
filtering that request. A watch that covers open requests replaces them with one request for its own
prefix, break before make: the watches they served move to it with what they saw (held) and their
``loaded`` flag, and at the new request's ``loaded`` each held namespace it did not confirm is
withdrawn. A request outlives its last watch by :data:`DISCOVERY_LINGER` and is never narrowed. The
counterpart of ``ts/src/discovery.ts``.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import Literal, TypeAlias

from moq_core.errors import is_moqt_protocol_error

from ._subscription import SubscriptionConnectionClosedError, SubscriptionOverflowError, SubscriptionRequestError
from ._supervision import is_permanent_request_error
from ._write_slot import _observe
from .errors import RelayError
from .namespace_events import NamespaceEvent, NamespaceEvents, _is_stopped, _is_withdrawn_by_end

_log = logging.getLogger("caprid_relay")

DISCOVERY_CAPACITY = sys.maxsize
"""Events one request may queue: no bound, since a snapshot can list any number of namespaces in one read."""

DISCOVERY_LINGER = 5.0
"""Seconds a request stays open after its last watch is released, so one asked for again at once reuses it."""

Namespace: TypeAlias = tuple[str, ...]

DiscoveryEnding: TypeAlias = Literal["connection", "retry", "fatal"]
"""What a watch's ending means for its owner: wait for the next connection, ask again after a backoff,
or surface the error."""


@dataclass(slots=True, kw_only=True)
class DiscoveryWatchHandlers:
    """What one watch hears, synchronously. A handler that raises is logged; discovery carries on."""

    on_announced: Callable[[Namespace], None] | None = None
    on_unannounced: Callable[[Namespace], None] | None = None
    on_loaded: Callable[[], None] | None = None
    """The request serving the watch has loaded: first, and again after each move to a new request."""
    on_ended: Callable[[BaseException | None], None] | None = None
    """The watch ended with its request's ending (``None`` when the relay finished the request)."""


@dataclass(frozen=True, slots=True)
class Ended:
    """How a watch ended; :func:`discovery_ending` classifies ``error``."""

    error: BaseException | None


@dataclass(frozen=True, slots=True)
class _Served:
    request: _Request


@dataclass(frozen=True, slots=True)
class _Released:
    pass


_RELEASED = _Released()

_WatchState: TypeAlias = _Served | Ended | _Released


@dataclass(slots=True, eq=False)
class _Record:
    prefix: Namespace
    handlers: DiscoveryWatchHandlers
    state: _WatchState = _RELEASED
    loaded: bool = False


@dataclass(slots=True, eq=False)
class _Request:
    prefix: Namespace
    events: NamespaceEvents
    held: dict[Namespace, None]
    """What the watches moved here had seen, until this request's load confirms or withdraws it."""
    loaded: bool = False
    announced: dict[Namespace, None] = field(default_factory=dict)
    watches: dict[Namespace, dict[_Record, None]] = field(default_factory=dict)
    """The watches it serves, by prefix."""
    linger: asyncio.TimerHandle | None = None
    reader: asyncio.Future[None] | None = None


@dataclass(frozen=True, slots=True)
class _Covered:
    by: _Request


@dataclass(frozen=True, slots=True)
class _Opens:
    replacing: list[_Request]


def covers(prefix: Sequence[str], namespace: Sequence[str]) -> bool:
    """Whether ``namespace`` is ``prefix`` or falls under it, field by field."""
    return tuple(namespace[: len(prefix)]) == tuple(prefix)


def _place_watch(open_requests: Iterable[_Request], prefix: Namespace) -> _Covered | _Opens:
    replacing: list[_Request] = []
    for request in open_requests:
        if covers(request.prefix, prefix):
            return _Covered(request)
        if covers(prefix, request.prefix):
            replacing.append(request)
    return _Opens(replacing)


def discovery_ending(error: BaseException | None) -> DiscoveryEnding:
    """What a watch's ending means for a want or a member view."""
    if isinstance(error, SubscriptionConnectionClosedError):
        return "connection"
    if error is None or isinstance(error, SubscriptionOverflowError):
        return "retry"
    if isinstance(error, SubscriptionRequestError) and not is_permanent_request_error(error.request_error):
        return "retry"
    return "fatal"


def refused_for_good(error: BaseException) -> bool:
    """Whether a request refused at the call is the caller's own fault, which no connection fixes.

    An argument, or a request the wire cannot carry. Anything else raised there (``CLOSED``, the
    connection's ``TRANSPORT_ERROR`` or ``GOAWAY``) is the connection ending before the supervisor
    has heard of it.
    """
    return isinstance(error, TypeError | ValueError) or is_moqt_protocol_error(error)


def _call(callback: Callable[..., None] | None, *args: object) -> None:
    if callback is None:
        return
    try:
        callback(*args)
    except Exception:
        _log.exception("a discovery handler raised; discovery carries on")


def _serves(request: _Request, record: _Record) -> bool:
    return isinstance(record.state, _Served) and record.state.request is request


def _records(request: _Request) -> list[_Record]:
    return [record for watches in request.watches.values() for record in watches]


def _attach(request: _Request, record: _Record) -> None:
    record.state = _Served(request)
    request.watches.setdefault(record.prefix, {})[record] = None


def _stop_linger(request: _Request) -> None:
    if request.linger is not None:
        request.linger.cancel()
        request.linger = None


def _lost_to(error: BaseException) -> SubscriptionConnectionClosedError:
    return error if isinstance(error, SubscriptionConnectionClosedError) else SubscriptionConnectionClosedError(error)


def _announce_ends(records: Iterable[_Record]) -> None:
    for record in records:
        if isinstance(record.state, Ended):
            _call(record.handlers.on_ended, record.state.error)


def _end(ending: list[_Record], error: BaseException | None) -> None:
    ended = Ended(error)
    for record in ending:
        record.state = ended
    _announce_ends(ending)


class DiscoveryWatch:
    """One owner's view of the namespaces under its prefix, from whichever request serves it."""

    __slots__ = ("_index", "_record")

    def __init__(self, index: DiscoveryIndex, record: _Record) -> None:
        self._index = index
        self._record = record

    @property
    def loaded(self) -> bool:
        """A request serving the watch has loaded: stays true through a move."""
        return self._record.loaded

    @property
    def following(self) -> bool:
        """The serving request has loaded and still delivers: false while a move's request loads or a stream failed."""
        state = self._record.state
        return isinstance(state, _Served) and state.request.loaded and not _is_stopped(state.request.events)

    @property
    def ended(self) -> Ended | None:
        """How the watch ended, once it has; ``None`` while served and after :meth:`release`."""
        state = self._record.state
        return state if isinstance(state, Ended) else None

    def namespaces(self) -> list[Namespace]:
        """The namespaces under the prefix announced now, held ones included."""
        state = self._record.state
        if not isinstance(state, _Served):
            return []
        shown = (*state.request.announced, *state.request.held)
        return [namespace for namespace in shown if covers(self._record.prefix, namespace)]

    def release(self) -> None:
        """Stop watching; nothing more is heard. Idempotent."""
        self._index._release(self._record)


class DiscoveryIndex:
    """One connection's namespace requests, pairwise non-overlapping, shared by every watch under them."""

    __slots__ = ("_closed", "_open", "_requests")

    def __init__(self, open_events: Callable[[Namespace], NamespaceEvents]) -> None:
        self._open = open_events
        self._requests: dict[Namespace, _Request] = {}
        self._closed = False

    def watch(self, prefix: Sequence[str], handlers: DiscoveryWatchHandlers) -> DiscoveryWatch:
        """Watch the namespaces under ``prefix``; raises what opening its request raises, or ``CLOSED``."""
        if self._closed:
            raise RelayError("Relay connection is closed", code="CLOSED")
        record = _Record(tuple(prefix), handlers)
        self._place(record)
        return DiscoveryWatch(self, record)

    def close(self) -> None:
        """The connection ended: end every watch with :class:`SubscriptionConnectionClosedError`. Idempotent."""
        if self._closed:
            return
        self._closed = True
        ending = list(self._requests.values())
        self._requests.clear()
        for request in ending:
            _stop_linger(request)
            request.events.close()
        _end([record for request in ending for record in _records(request)], SubscriptionConnectionClosedError(None))

    def _notify(self, request: _Request, namespace: Namespace, *, announced: bool) -> None:
        for length in range(len(request.prefix), len(namespace) + 1):
            for record in list(request.watches.get(namespace[:length], ())):
                if not _serves(request, record):
                    continue
                handlers = record.handlers
                _call(handlers.on_announced if announced else handlers.on_unannounced, namespace)

    def _dispatch(self, request: _Request, event: NamespaceEvent) -> None:
        if event.type == "loaded":
            if request.loaded:
                return
            request.loaded = True
            gone = list(request.held)
            request.held.clear()
            for namespace in gone:
                self._notify(request, namespace, announced=False)
            for record in _records(request):
                if not _serves(request, record):
                    continue
                record.loaded = True
                _call(record.handlers.on_loaded)
            return
        # The end that follows moves the narrower watches with what the request still shows.
        if _is_withdrawn_by_end(request.events, event):
            return
        namespace = event.namespace
        assert namespace is not None
        if event.type == "announced":
            if namespace in request.held:
                del request.held[namespace]
                request.announced[namespace] = None
                return
            if namespace in request.announced:
                return
            request.announced[namespace] = None
            self._notify(request, namespace, announced=True)
            return
        if namespace in request.announced:
            del request.announced[namespace]
            self._notify(request, namespace, announced=False)

    def _place(self, record: _Record, ended: _Request | None = None) -> None:
        placed = _place_watch(self._requests.values(), record.prefix)
        if isinstance(placed, _Covered):
            request = placed.by
            _stop_linger(request)
            record.loaded = record.loaded or request.loaded
            _attach(request, record)
            return
        # Break before make: §9.15 forbids the overlap, and the namespace request refuses it before sending.
        held: dict[Namespace, None] = {}
        moved: list[_Record] = []
        for replaced in placed.replacing:
            del self._requests[replaced.prefix]
            _stop_linger(replaced)
            moved.extend(_records(replaced))
            replaced.events.close()
        for shown in [*placed.replacing, *([ended] if ended is not None else [])]:
            for namespace in (*shown.announced, *shown.held):
                if covers(record.prefix, namespace):
                    held[namespace] = None
        try:
            events = self._open(record.prefix)
        except Exception as error:
            lost = Ended(_lost_to(error))
            for moved_record in moved:
                moved_record.state = lost
            asyncio.get_running_loop().call_soon(_announce_ends, moved)
            raise
        request = _Request(record.prefix, events, held)
        for moved_record in moved:
            _attach(request, moved_record)
        _attach(request, record)
        self._requests[request.prefix] = request
        request.reader = asyncio.ensure_future(self._consume(request))
        _observe(request.reader)

    async def _consume(self, request: _Request) -> None:
        error: BaseException | None = None
        try:
            async for event in request.events:
                if self._requests.get(request.prefix) is not request:
                    return
                self._dispatch(request, event)
        except Exception as thrown:
            error = thrown
        if self._requests.get(request.prefix) is not request:
            return
        del self._requests[request.prefix]
        _stop_linger(request)
        if isinstance(error, SubscriptionConnectionClosedError):
            _end(_records(request), error)
            return
        rehomed = sorted(
            (record for record in _records(request) if record.prefix != request.prefix),
            key=lambda record: len(record.prefix),
        )
        for record in rehomed:
            try:
                self._place(record, request)
            except Exception as thrown:
                record.state = Ended(_lost_to(thrown))
        _end(list(request.watches.get(request.prefix, ())), error)
        _announce_ends(rehomed)

    def _release(self, record: _Record) -> None:
        state = record.state
        record.state = _RELEASED
        if not isinstance(state, _Served):
            return
        request = state.request
        watches = request.watches.get(record.prefix)
        if watches is not None:
            watches.pop(record, None)
            if not watches:
                del request.watches[record.prefix]
        if request.watches or self._requests.get(request.prefix) is not request:
            return
        request.linger = asyncio.get_running_loop().call_later(DISCOVERY_LINGER, self._linger_over, request)

    def _linger_over(self, request: _Request) -> None:
        if request.watches or self._requests.get(request.prefix) is not request:
            return
        del self._requests[request.prefix]
        request.events.close()


__all__ = [
    "DISCOVERY_CAPACITY",
    "DISCOVERY_LINGER",
    "DiscoveryEnding",
    "DiscoveryIndex",
    "DiscoveryWatch",
    "DiscoveryWatchHandlers",
    "Ended",
    "covers",
    "discovery_ending",
    "refused_for_good",
]
