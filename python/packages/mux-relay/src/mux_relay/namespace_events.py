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
from dataclasses import dataclass
from typing import Literal

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

_log = logging.getLogger("mux_relay")


@dataclass(frozen=True, slots=True)
class NamespaceEvent:
    """One change in which namespaces under the prefix are announced."""

    type: Literal["announced", "unannounced"]
    """``announced``: a publisher announced the namespace, or it was already announced when the
    request was accepted. ``unannounced``: its announce was withdrawn. Each namespace alternates
    between the two; the relay's repeats are not passed on."""
    namespace: tuple[str, ...]
    """The full namespace: the prefix's fields followed by the rest, without splitting or joining."""


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

    Draft-20 §10.19 has a relay answer an overlapping SUBSCRIBE_NAMESPACE on the same session with
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
        "_prefix",
        "_release_prefix",
        "_remove_closed",
        "_sink",
        "_transport_ended",
    )

    def __init__(
        self,
        dependencies: NamespaceRequestDependencies,
        prefix: tuple[str, ...],
        sink: BoundedEventSink[NamespaceEvent],
    ) -> None:
        self._prefix = prefix
        self._sink = sink
        self._ended = False
        self._core: NamespaceSubscription | None = None
        self._announced: set[tuple[str, ...]] = set()
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
                    on_namespace=lambda suffix: self._change("announced", suffix),
                    on_namespace_done=lambda suffix: self._change("unannounced", suffix),
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
        self._release_prefix()
        self._remove_closed()
        if self._core is not None:
            self._core.cancel(reason)
        return True

    def _finish(self, error: BaseException | None) -> None:
        if not self._end(error):
            return
        # A clean end keeps what is queued: the withdrawals the core reports for it come just before.
        if error is None:
            self._sink.complete()
        else:
            self._sink.ended(error)

    def _change(self, kind: Literal["announced", "unannounced"], suffix: list[str]) -> None:
        if self._ended:
            return
        key = tuple(suffix)
        if kind == "announced":
            if key in self._announced:
                return
            self._announced.add(key)
        else:
            if key not in self._announced:
                return
            self._announced.discard(key)
        self._sink.event(NamespaceEvent(kind, (*self._prefix, *key)))

    def _fail(self, error: RequestError) -> None:
        """The same rule a subscription follows.

        A transport that has already ended fails its streams before the session reports the closure,
        so leave the ending to the closure observer. Otherwise hold the failure until the turn ends,
        so a closure reported in the same turn is the ending this request describes.
        """
        if self._ended or self._transport_ended():
            return

        def reject() -> None:
            try:
                self._finish(SubscriptionRequestError(error))
            except Exception:
                _log.debug("namespace events failed while recording a rejection", exc_info=True)

        asyncio.get_running_loop().call_soon(reject)

    def _on_end(self, error: BaseException | None) -> None:
        # A finished stream is the relay ending the request on purpose; a failed one may be a lost connection.
        if error is None:
            self._finish(None)
            return
        reason = str(error) or "namespace request stream failed"
        self._fail(RequestError(error_code=REQUEST_ERROR_CODE.INTERNAL_ERROR, retry_interval=0, reason=reason))


class NamespaceEvents(BoundedEvents[NamespaceEvent]):
    """Single-consumer iterator owning one namespace request, never the shared connection.

    Create one with :meth:`mux_relay.RelaySubscriber.namespace_events` and use ``async with`` around
    ``async for``. At most ``capacity`` events are retained; overflow discards the queue, cancels the
    request and raises :class:`SubscriptionOverflowError` on reads. A relay rejection raises
    :class:`SubscriptionRequestError`, the connection ending :class:`SubscriptionConnectionClosedError`;
    a relay that finishes the request, ``close()``, or ``break`` ends iteration cleanly.
    """

    def __init__(
        self,
        dependencies: NamespaceRequestDependencies,
        prefix: tuple[str, ...],
        *,
        capacity: int = 64,
        cancellation: CancellationToken | None = None,
    ) -> None:
        super().__init__(
            lambda sink: _NamespaceRequest(dependencies, prefix, sink), capacity=capacity, cancellation=cancellation
        )


__all__ = ["NamespaceEvent", "NamespaceEvents"]
