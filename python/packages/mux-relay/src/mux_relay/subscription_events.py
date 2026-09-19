"""Bounded subscription events. The iterator owns its request, never the connection."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from types import TracebackType
from typing import Literal, Self, TypeAlias

from moq_core import (
    Closed,
    Goaway,
    MessageParameters,
    MoqtObject,
    PublishDone,
    RequestError,
    SubgroupEnd,
    SubgroupReset,
    SubscribeOk,
    Subscription,
    SubscriptionHandlers,
)

from .cancellation import CancellationToken
from .errors import RelayError


@dataclass(frozen=True, slots=True)
class SubscribedEvent:
    """Subscription acceptance."""

    value: SubscribeOk
    type: Literal["subscribed"] = field(default="subscribed", init=False)


@dataclass(frozen=True, slots=True)
class ObjectEvent:
    """An object, with its payload and protocol metadata."""

    value: MoqtObject
    type: Literal["object"] = field(default="object", init=False)


@dataclass(frozen=True, slots=True)
class PublisherDoneEvent:
    """Publisher completion; trailing objects may still arrive."""

    value: PublishDone
    type: Literal["publisher_done"] = field(default="publisher_done", init=False)


@dataclass(frozen=True, slots=True)
class SubgroupEndEvent:
    """Clean FIN of one subgroup, not the whole subscription."""

    value: SubgroupEnd
    type: Literal["subgroup_end"] = field(default="subgroup_end", init=False)


@dataclass(frozen=True, slots=True)
class SubgroupResetEvent:
    """A subgroup reset; objects may be missing."""

    value: SubgroupReset
    type: Literal["subgroup_reset"] = field(default="subgroup_reset", init=False)


@dataclass(frozen=True, slots=True)
class StateNotifyEvent:
    """Publisher-initiated subscription state changes."""

    value: MessageParameters
    type: Literal["state_notify"] = field(default="state_notify", init=False)


@dataclass(frozen=True, slots=True)
class GoawayEvent:
    """Request relocation details; no automatic retry."""

    value: Goaway
    type: Literal["goaway"] = field(default="goaway", init=False)


@dataclass(frozen=True, slots=True)
class UpdateOkEvent:
    """Acknowledgement of a subscription update."""

    value: None = None
    type: Literal["update_ok"] = field(default="update_ok", init=False)


SubscriptionEvent: TypeAlias = (
    SubscribedEvent
    | ObjectEvent
    | PublisherDoneEvent
    | SubgroupEndEvent
    | SubgroupResetEvent
    | StateNotifyEvent
    | GoawayEvent
    | UpdateOkEvent
)
"""Typed callback arrivals; no ordering across streams or drain guarantee is implied."""


class SubscriptionOverflowError(RelayError):
    """A stalled consumer exceeded the event capacity; the subscription was cancelled."""


class SubscriptionRequestError(RelayError):
    """Request rejection, timeout, or request failure, with the protocol details preserved."""

    def __init__(self, request_error: RequestError) -> None:
        super().__init__(request_error.reason)
        self.request_error = request_error


class SubscriptionConnectionClosedError(RelayError):
    """The connection ended, without proving that subscription delivery was drained."""

    def __init__(self, cause: BaseException | None) -> None:
        super().__init__("Connection closed; subscription delivery ended without a drain guarantee")
        self.__cause__ = cause


class SubscriptionEvents:
    """Single-consumer iterator. Use ``async with`` for deterministic cleanup on break/errors.

    At most ``capacity`` events are retained, including lifecycle events. Overflow discards the
    queue, cancels only this request and raises on reads. No transport backpressure is implied.
    Publisher completion is an event, never EOF. Explicit close produces EOF; failures raise.
    """

    def __init__(
        self,
        subscribe: Callable[[SubscriptionHandlers], Subscription],
        closed: asyncio.Future[Closed],
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
        self._subscription: Subscription | None = None
        self._closed = closed
        self._cancellation = cancellation
        if cancellation is not None and cancellation.cancelled:
            self._finish(cancellation.reason)
            return
        closed.add_done_callback(self._connection_closed)
        if cancellation is not None:
            cancellation.add_callback(self._finish)
        try:
            self._subscription = subscribe(
                SubscriptionHandlers(
                    on_ok=lambda value: self._push(SubscribedEvent(value)),
                    on_object=lambda value: self._push(ObjectEvent(value)),
                    on_done=lambda value: self._push(PublisherDoneEvent(value)),
                    on_subgroup_end=lambda value: self._push(SubgroupEndEvent(value)),
                    on_subgroup_reset=lambda value: self._push(SubgroupResetEvent(value)),
                    on_state_notify=lambda value: self._push(StateNotifyEvent(value)),
                    on_goaway=lambda value: self._push(GoawayEvent(value)),
                    on_update_ok=lambda: self._push(UpdateOkEvent()),
                    on_error=lambda error: self._finish(SubscriptionRequestError(error)),
                )
            )
            if self._ended:
                self._subscription.cancel(self._failure)
        except BaseException:
            self.close()
            raise

    def _connection_closed(self, closed: asyncio.Future[Closed]) -> None:
        self._finish(SubscriptionConnectionClosedError(closed.result().error))

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

    def _finish(self, error: BaseException | None = None) -> None:
        if self._ended:
            return
        self._ended = True
        self._failure = error
        self._queue.clear()
        self._closed.remove_done_callback(self._connection_closed)
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
