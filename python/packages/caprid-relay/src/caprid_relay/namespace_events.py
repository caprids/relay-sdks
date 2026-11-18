"""Namespace discovery: one SUBSCRIBE_NAMESPACE request from creation to its end, read through the
bounded events iterator ``subscribe_events`` uses. The relay answers with the namespaces under the
prefix that are announced now, then each later announce and withdrawal; this module turns the
wire's prefix-relative entries into full namespaces, keeps the events edge-triggered, and maps the
request's ending onto the subscription error classes.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Literal, TypeAlias

from moq_core import (
    REQUEST_ERROR_CODE,
    MoqtSubscribeNamespaceOptions,
    NamespaceSubscription,
    NamespaceSubscriptionHandlers,
    RequestError,
)

from ._bounded_events import BoundedEvents, BoundedEventSink
from ._subscription import SubscriptionConnectionClosedError, SubscriptionRequestError
from .cancellation import CancellationToken
from .errors import RelayError

_log = logging.getLogger("caprid_relay")


@dataclass(frozen=True, slots=True)
class NamespaceEvent:
    """One change in which namespaces under the prefix are announced, or the end of the relay's initial snapshot."""

    type: Literal["announced", "unannounced", "loaded"]
    """``announced``: a publisher announced the namespace, or it was already announced when the
    request was accepted. ``unannounced``: its announce was withdrawn. Each namespace alternates
    between the two; the relay's repeats are not passed on.

    ``loaded``: every namespace announced when the relay accepted the request has been yielded, so an
    application that tracks the announced set now holds the relay's view, and an empty set means
    nothing is announced. Yielded once, before any later change, and never once the request has
    ended. With MoQ Active Count negotiated it follows the NAMESPACE messages the relay counted (at
    once on a count of 0). Otherwise it is the SDK's estimate: the relay accepted the request, then
    :data:`SNAPSHOT_QUIET` passed without an entry, and never later than :data:`SNAPSHOT_LIMIT` after
    the relay accepted it."""
    namespace: tuple[str, ...] | None = None
    """The full namespace: the prefix's fields followed by the rest, without splitting or joining.
    ``None`` only for ``loaded``."""


SNAPSHOT_QUIET = 0.05
"""Without MoQ Active Count, how long, in seconds, the request's stream stays quiet after the relay
accepts it before the relay's snapshot counts as loaded. Each entry restarts it, so an empty prefix
loads this long after acceptance. Provisional: moq-relay 0.17.x wrote 60 entries within 0.94 ms of
REQUEST_OK, cross-relay, a 50x margin (2026-11-03,
https://github.com/muxinc/relay-sdks/issues/422#issuecomment-6599550554)."""

SNAPSHOT_LIMIT = 1.0
"""Without MoQ Active Count, the latest ``loaded`` comes this many seconds after the relay accepts,
so a busy prefix still loads."""


@dataclass(slots=True)
class _Quiet:
    quiet: asyncio.TimerHandle
    limit: asyncio.TimerHandle


_Snapshot: TypeAlias = Literal["unanswered", "counted", "loaded"] | _Quiet
"""Where a request stands on its relay's initial snapshot. ``counted``: MoQ Active Count will say
when it ends. ``_Quiet``: the SDK estimates it from the stream's quiet window and its cap."""


@dataclass(slots=True)
class _RequestMarks:
    """What the supervised subscriber's discovery reads of one request beyond its public events."""

    transport_ended: Callable[[], bool]
    """The connection's own check, true before the session has reported the closure to the request."""
    stopped: bool = False
    withdrawn_by_end: dict[int, NamespaceEvent] = field(default_factory=dict)
    """The withdrawals a clean end yields, by identity; holding them keeps their ids unique."""


def _is_withdrawn_by_end(events: NamespaceEvents, event: NamespaceEvent) -> bool:
    """Whether ``event`` is one of the withdrawals a clean end yields, not one the relay sent.

    The supervised subscriber's discovery reads them as the request's end, so a narrower root it
    moves keeps what it saw.
    """
    return events._marks.withdrawn_by_end.get(id(event)) is event


def _is_stopped(events: NamespaceEvents) -> bool:
    """Whether the request has failed or ended, though its reader may not have seen it yet: nothing more arrives.

    The supervised subscriber's discovery reads it so a publishing root's leave grace does not run out on a
    stream that is gone.
    """
    marks = events._marks
    return marks.stopped or marks.transport_ended()


@dataclass(frozen=True, slots=True)
class NamespaceRequestDependencies:
    """What a namespace request borrows from the connection."""

    subscribe_namespace: Callable[[MoqtSubscribeNamespaceOptions, NamespaceSubscriptionHandlers], NamespaceSubscription]
    """The core session's SUBSCRIBE_NAMESPACE."""
    observe_closed: Callable[[Callable[[BaseException | None], None]], Callable[[], None]]
    """Observe the connection's closure in the turn it happens; returns the removal."""
    transport_ended: Callable[[], bool]
    """Whether the transport has already ended, before the session has reported the closure."""
    prefixes: set[tuple[str, ...]]
    """The connection's prefixes with a namespace request open; see :func:`_claim_prefix`."""


def _claim_prefix(prefixes: set[tuple[str, ...]], prefix: tuple[str, ...]) -> Callable[[], None]:
    """Hold ``prefix`` for one request, or refuse it when it overlaps one already held.

    Draft-22 §9.15 has a relay answer an overlapping SUBSCRIBE_NAMESPACE on the same session with
    PREFIX_OVERLAP. moq-relay 0.17.0 does not, so the SDK refuses it itself, before anything is sent,
    and behaves the same against every relay. Returns the release.
    """
    for held in prefixes:
        shorter, longer = (held, prefix) if len(held) <= len(prefix) else (prefix, held)
        if longer[: len(shorter)] == shorter:
            raise RelayError(
                f"This connection already discovers {list(held)!r}, which overlaps {list(prefix)!r}; "
                "one request per prefix tree",
                code="PREFIX_OVERLAP",
                phase="subscription",
            )
    prefixes.add(prefix)
    return lambda: prefixes.discard(prefix)


class _NamespaceRequest:
    """The request one iterator owns: the core handle, the closure observation and the announced set."""

    __slots__ = (
        "_announced",
        "_core",
        "_ended",
        "_marks",
        "_prefix",
        "_release_prefix",
        "_remove_closed",
        "_sink",
        "_snapshot",
        "_transport_ended",
    )

    def __init__(
        self,
        dependencies: NamespaceRequestDependencies,
        prefix: tuple[str, ...],
        sink: BoundedEventSink[NamespaceEvent],
        marks: _RequestMarks,
    ) -> None:
        self._prefix = prefix
        self._sink = sink
        self._marks = marks
        self._ended = False
        self._core: NamespaceSubscription | None = None
        # A dict for its order: a clean end withdraws in the order the namespaces were announced.
        self._announced: dict[tuple[str, ...], None] = {}
        self._snapshot: _Snapshot = "unanswered"
        self._transport_ended = dependencies.transport_ended
        self._remove_closed: Callable[[], None] = lambda: None
        self._release_prefix = _claim_prefix(dependencies.prefixes, prefix)

        # A connection that has already ended tells the observer at once, ending this before any send.
        self._remove_closed = dependencies.observe_closed(
            lambda error: self._finish(SubscriptionConnectionClosedError(error))
        )
        if self._ended:
            return

        try:
            self._core = dependencies.subscribe_namespace(
                MoqtSubscribeNamespaceOptions(track_namespace_prefix=list(prefix)),
                NamespaceSubscriptionHandlers(
                    on_ok=self._on_ok,
                    on_synced=self._load,
                    on_namespace=self._on_namespace,
                    on_namespace_done=self._on_namespace_done,
                    on_error=self._fail,
                    on_end=self._on_end,
                ),
            )
        except BaseException:
            # Encoding can refuse the request (a field too long for the wire); leave nothing observing.
            self._end(None)
            raise
        if self._ended:
            self._core.cancel()

    def cancel(self, reason: object = None) -> None:
        """The iterator ended (close, cancellation, overflow): drop the request without reporting back."""
        self._end(reason)

    def _end(self, reason: object) -> bool:
        if self._ended:
            return False
        self._ended = True
        # Set before the iterator's reader resumes: a leave timer due in the same loop turn must see the end.
        self._marks.stopped = True
        self._release_prefix()
        self._remove_closed()
        self._stop_timers()
        if self._core is not None:
            self._core.cancel(reason)
        return True

    def _finish(self, error: BaseException | None) -> None:
        if not self._end(error):
            return
        # A clean end keeps what is queued: the withdrawals this request yields for it come just before.
        if error is None:
            self._sink.complete()
        else:
            self._sink.ended(error)

    def _on_ok(self, active_count: int | None) -> None:
        if active_count is not None:
            self._snapshot = "counted"
            return
        loop = asyncio.get_running_loop()
        self._snapshot = _Quiet(
            loop.call_later(SNAPSHOT_QUIET, self._load), loop.call_later(SNAPSHOT_LIMIT, self._load)
        )

    def _on_namespace(self, suffix: list[str]) -> None:
        self._change("announced", tuple(suffix))
        self._heard()

    def _on_namespace_done(self, suffix: list[str], request_ended: bool) -> None:
        # The withdrawals the core synthesizes for an ending: a clean end yields this iterator's own, an error none.
        if request_ended:
            return
        self._change("unannounced", tuple(suffix))
        self._heard()

    def _load(self) -> None:
        if self._ended or self._snapshot == "loaded":
            return
        self._stop_timers()
        self._snapshot = "loaded"
        # Once per request, so it takes no capacity: a snapshot that fills the queue still loads.
        self._sink.event(NamespaceEvent("loaded"), counted=False)

    def _heard(self) -> None:
        if self._ended:
            return
        snapshot = self._snapshot
        if isinstance(snapshot, _Quiet):
            snapshot.quiet.cancel()
            snapshot.quiet = asyncio.get_running_loop().call_later(SNAPSHOT_QUIET, self._load)

    def _stop_timers(self) -> None:
        snapshot = self._snapshot
        if isinstance(snapshot, _Quiet):
            snapshot.quiet.cancel()
            snapshot.limit.cancel()

    def _change(self, kind: Literal["announced", "unannounced"], key: tuple[str, ...], *, by_end: bool = False) -> None:
        if self._ended:
            return
        if kind == "announced":
            if key in self._announced:
                return
            self._announced[key] = None
        else:
            if key not in self._announced:
                return
            del self._announced[key]
        event = NamespaceEvent(kind, (*self._prefix, *key))
        if by_end:
            self._marks.withdrawn_by_end[id(event)] = event
        self._sink.event(event)

    def _fail(self, error: RequestError) -> None:
        """The same rule a subscription follows.

        A transport that has already ended fails its streams before the session reports the closure,
        so leave the ending to the closure observer. Otherwise hold the failure until the turn ends,
        so a closure reported in the same turn is the ending this request describes.
        """
        if self._ended:
            return
        self._marks.stopped = True
        if self._transport_ended():
            return

        def reject() -> None:
            try:
                self._finish(SubscriptionRequestError(error))
            except Exception:
                _log.debug("namespace events failed while recording a rejection", exc_info=True)

        asyncio.get_running_loop().call_soon(reject)

    def _on_end(self, error: BaseException | None) -> None:
        self._stop_timers()
        # A finished stream is the relay ending the request on purpose; a failed one may be a lost connection.
        if error is None:
            for key in list(self._announced):
                self._change("unannounced", key, by_end=True)
            self._finish(None)
            return
        reason = str(error) or "namespace request stream failed"
        self._fail(RequestError(error_code=REQUEST_ERROR_CODE.INTERNAL_ERROR, retry_interval=0, reason=reason))


class NamespaceEvents(BoundedEvents[NamespaceEvent]):
    """Single-consumer iterator owning one namespace request, never the shared connection.

    Create one with :meth:`caprid_relay.RelaySubscriber.namespace_events` and use ``async with`` around
    ``async for``. At most ``capacity`` events are retained, not counting the one ``loaded``; overflow
    discards the queue, cancels the request and raises :class:`SubscriptionOverflowError` on reads. A
    relay rejection raises :class:`SubscriptionRequestError`, the connection ending
    :class:`SubscriptionConnectionClosedError`; a relay that finishes the request, ``close()``, or
    ``break`` ends iteration cleanly.
    """

    def __init__(
        self,
        dependencies: NamespaceRequestDependencies,
        prefix: tuple[str, ...],
        *,
        capacity: int = 64,
        cancellation: CancellationToken | None = None,
    ) -> None:
        self._marks = _RequestMarks(dependencies.transport_ended)
        super().__init__(
            lambda sink: _NamespaceRequest(dependencies, prefix, sink, self._marks),
            capacity=capacity,
            cancellation=cancellation,
        )


__all__ = ["SNAPSHOT_LIMIT", "SNAPSHOT_QUIET", "NamespaceEvent", "NamespaceEvents"]
