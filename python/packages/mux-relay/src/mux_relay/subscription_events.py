"""The bounded events iterator: one consumer of a subscription (see ``_subscription.py``) that queues
product events with a capacity and turns the subscription's ending into the three subscription
error classes. It owns its subscription, never the connection.
"""

from __future__ import annotations

from collections.abc import Callable

from moq_core import RequestError

from ._bounded_events import BoundedEvents, BoundedEventSink
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
    """The iterator's face towards its subscription: maps the subscription's ending onto the error classes."""

    __slots__ = ("_sink",)

    def __init__(self, sink: BoundedEventSink[SubscriptionEvent]) -> None:
        self._sink = sink

    def event(self, event: SubscriptionEvent) -> None:
        self._sink.event(event)

    def ended(self, end: SubscriptionEnd) -> None:
        if end.cause == "request-error" and isinstance(end.error, RequestError):
            self._sink.ended(SubscriptionRequestError(end.error))
        elif end.cause == "connection-closed":
            self._sink.ended(
                SubscriptionConnectionClosedError(end.error if isinstance(end.error, BaseException) else None)
            )
        else:
            self._sink.ended(end.error if isinstance(end.error, BaseException) else None)


class SubscriptionEvents(BoundedEvents[SubscriptionEvent]):
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
        super().__init__(lambda sink: subscribe(_Consumer(sink)), capacity=capacity, cancellation=cancellation)
