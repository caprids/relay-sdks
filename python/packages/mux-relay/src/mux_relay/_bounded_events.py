"""The bounded events iterator both events forms share: one owned request, a queue bound and a
cancellation token. ``subscription_events.py`` and ``namespace_events.py`` each supply the request
and the events; this module owns the queue, the single pending read and the ending.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable
from types import TracebackType
from typing import Generic, Protocol, Self, TypeVar

from ._subscription import SubscriptionOverflowError
from .cancellation import CancellationToken

E = TypeVar("E")


class OwnedRequest(Protocol):
    """The request an iterator owns: all it needs is the cancel."""

    def cancel(self, reason: object = None) -> None:
        """Tear the request down; its echo through the sink is ignored."""


class BoundedEventSink(Generic[E]):
    """Where a producer delivers into a bounded iterator."""

    __slots__ = ("_events",)

    def __init__(self, events: BoundedEvents[E]) -> None:
        self._events = events

    def event(self, event: E) -> None:
        """One event; past the capacity the iterator ends with ``SubscriptionOverflowError``."""
        self._events._push(event)

    def ended(self, error: BaseException | None = None) -> None:
        """The producer ended: ``None`` finishes the iterator cleanly, anything else is raised from reads."""
        # An ending the iterator started (close, cancellation, overflow) echoes back here.
        self._events._finish(error)


class BoundedEvents(Generic[E]):
    """Single-consumer iterator over one owned request. Use ``async with`` for deterministic cleanup.

    At most ``capacity`` events are retained. Overflow discards the queue, cancels only this request
    and raises on reads. No transport backpressure is implied. Explicit close produces EOF; failures
    raise.
    """

    def __init__(
        self,
        start: Callable[[BoundedEventSink[E]], OwnedRequest],
        *,
        capacity: int = 64,
        cancellation: CancellationToken | None = None,
    ) -> None:
        if isinstance(capacity, bool) or not isinstance(capacity, int | float):
            raise TypeError("capacity must be a number")
        if not isinstance(capacity, int) or capacity <= 0:
            raise ValueError("capacity must be a positive integer")
        self._capacity = capacity
        self._queue: deque[E] = deque()
        # A one-element tuple, so an event that is itself ``None`` stays distinguishable from a wake.
        self._pending: asyncio.Future[tuple[E] | None] | None = None
        self._ended = False
        self._failure: BaseException | None = None
        self._request: OwnedRequest | None = None
        self._cancellation = cancellation
        if cancellation is not None and cancellation.cancelled:
            self._finish(cancellation.reason)
            return
        if cancellation is not None:
            cancellation.add_callback(self._finish)
        try:
            self._request = start(BoundedEventSink(self))
        except BaseException:
            self.close()
            raise
        # Events can overflow the queue before start() returns. The ending recorded then had no
        # request to cancel, so cancel it now; the echo of that cancel is ignored.
        if self._ended:
            self._request.cancel(self._failure)

    def _push(self, event: E) -> None:
        if self._ended:
            return
        if self._pending is not None and not self._pending.done():
            self._pending.set_result((event,))
            return
        if len(self._queue) == self._capacity:
            self._finish(SubscriptionOverflowError("Subscription event buffer overflow"))
            return
        self._queue.append(event)

    def _finish(self, error: BaseException | None = None) -> None:
        if self._ended:
            return
        self._ended = True
        self._failure = error
        self._queue.clear()
        if self._cancellation is not None:
            self._cancellation.remove_callback(self._finish)
        if self._request is not None:
            self._request.cancel(error)
        # Wake by value, not exception: abandoned reads cannot leak an unretrieved exception.
        if self._pending is not None and not self._pending.done():
            self._pending.set_result(None)

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> E:
        if self._pending is not None:
            raise RuntimeError("Only one pending subscription read is supported")
        try:
            if not self._ended and not self._queue:
                self._pending = asyncio.get_running_loop().create_future()
                handed = await self._pending
                # A handoff already completed before teardown belongs to the consumer, like TS.
                if handed is not None:
                    return handed[0]
            if self._ended:
                if self._failure is not None:
                    raise self._failure
                raise StopAsyncIteration
            return self._queue.popleft()
        except asyncio.CancelledError:
            self.close()
            raise
        finally:
            self._pending = None

    def close(self) -> None:
        """Discard queued events and settle pending reads as EOF. Idempotent."""
        self._finish()

    async def aclose(self) -> None:
        """:meth:`close` as a coroutine, for ``contextlib.aclosing`` and other async-iterator cleanup."""
        self.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        self.close()


__all__ = ["BoundedEventSink", "BoundedEvents", "OwnedRequest"]
