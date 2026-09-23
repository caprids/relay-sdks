"""The bounded events iterator: one consumer of a subscription (see ``_subscription.py``) that queues
product events with a capacity and turns the subscription's ending into the three subscription
error classes. It owns its subscription, never the connection.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable
from types import TracebackType
from typing import Self

from moq_core import RequestError

from ._subscription import (
    GoawayEvent,
    ObjectEvent,
    PublisherDoneEvent,
    RelaySubscription,
    StateNotifyEvent,
    SubgroupEndEvent,
    SubgroupResetEvent,
    SubscribedEvent,
    SubscriptionConnectionClosedError,
    SubscriptionConsumer,
    SubscriptionEnd,
    SubscriptionEvent,
    SubscriptionOverflowError,
    SubscriptionRequestError,
    UpdateOkEvent,
)
from .cancellation import CancellationToken

__all__ = [
    "GoawayEvent",
    "ObjectEvent",
    "PublisherDoneEvent",
    "StateNotifyEvent",
    "SubgroupEndEvent",
    "SubgroupResetEvent",
    "SubscribedEvent",
    "SubscriptionConnectionClosedError",
    "SubscriptionEvent",
    "SubscriptionEvents",
    "SubscriptionOverflowError",
    "SubscriptionRequestError",
    "UpdateOkEvent",
]


class _Consumer:
    """The iterator's face towards its subscription."""

    __slots__ = ("_events",)

    def __init__(self, events: SubscriptionEvents) -> None:
        self._events = events

    def event(self, event: SubscriptionEvent) -> None:
        self._events._push(event)

    def ended(self, end: SubscriptionEnd) -> None:
        self._events._ended_by_subscription(end)


class SubscriptionEvents:
    """Single-consumer iterator. Use ``async with`` for deterministic cleanup on break/errors.

    Create one with :meth:`mux_relay.RelaySubscriber.subscribe_events`. At most ``capacity`` events are
    retained, including lifecycle events. Overflow discards the queue, cancels only this request and
    raises on reads. No transport backpressure is implied. Publisher completion is an event, never
    EOF. Explicit close produces EOF; failures raise.
    """

    def __init__(
        self,
        subscribe: Callable[[SubscriptionConsumer], RelaySubscription],
        *,
        capacity: int = 64,
        cancellation: CancellationToken | None = None,
    ) -> None:
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity <= 0:
            raise ValueError("capacity must be a positive integer")
        self._capacity = capacity
        self._queue: deque[SubscriptionEvent] = deque()
        self._pending: asyncio.Future[SubscriptionEvent | None] | None = None
        self._ended = False
        self._failure: BaseException | None = None
        self._subscription: RelaySubscription | None = None
        self._cancellation = cancellation
        if cancellation is not None and cancellation.cancelled:
            self._finish(cancellation.reason)
            return
        if cancellation is not None:
            cancellation.add_callback(self._finish)
        try:
            self._subscription = subscribe(_Consumer(self))
        except BaseException:
            self.close()
            raise
        # Events can overflow the queue before subscribe() returns. The ending recorded then had no
        # subscription to cancel, so cancel it now; the echo of that cancel is ignored above.
        if self._ended:
            self._subscription.cancel(self._failure)

    def _push(self, event: SubscriptionEvent) -> None:
        if self._ended:
            return
        if self._pending is not None and not self._pending.done():
            self._pending.set_result(event)
            return
        if len(self._queue) == self._capacity:
            self._finish(SubscriptionOverflowError("Subscription event buffer overflow"))
            return
        self._queue.append(event)

    def _ended_by_subscription(self, end: SubscriptionEnd) -> None:
        # An ending this iterator started (close, cancellation, overflow) echoes back here.
        if self._ended:
            return
        if end.cause == "request-error" and isinstance(end.error, RequestError):
            self._finish(SubscriptionRequestError(end.error))
        elif end.cause == "connection-closed":
            self._finish(SubscriptionConnectionClosedError(end.error if isinstance(end.error, BaseException) else None))
        else:
            self._finish(end.error if isinstance(end.error, BaseException) else None)

    def _finish(self, error: BaseException | None = None) -> None:
        if self._ended:
            return
        self._ended = True
        self._failure = error
        self._queue.clear()
        if self._cancellation is not None:
            self._cancellation.remove_callback(self._finish)
        if self._subscription is not None:
            self._subscription.cancel(error)
        # Wake by value, not exception: abandoned reads cannot leak an unretrieved exception.
        if self._pending is not None and not self._pending.done():
            self._pending.set_result(None)

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> SubscriptionEvent:
        if self._pending is not None:
            raise RuntimeError("Only one pending subscription read is supported")
        try:
            if not self._ended and not self._queue:
                self._pending = asyncio.get_running_loop().create_future()
                event = await self._pending
                # A handoff already completed before teardown belongs to the consumer, like TS.
                if event is not None:
                    return event
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
        self.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        self.close()
