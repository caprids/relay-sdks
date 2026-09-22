"""The application's subscription (see ``CONTEXT.md``): one SUBSCRIBE request from creation to its
terminal state, as distinct from demand, the relay's own subscription to a publisher.

The module owns the core handle, the connection's closure observation and the terminal state, and
translates the core's callbacks into product events once. Its consumers are the callback form
(:meth:`mux_relay.RelaySubscriber.subscribe` with handlers) and the bounded events iterator; both
attach through :class:`SubscriptionConsumer` and see the same events and the same ending. Nothing
here is exported from the package: the public handle keeps the core ``Subscription`` type, and
readiness and termination (issue #68) and the pre-acceptance ``update()`` queue (issue #69) land
here later.
"""

from __future__ import annotations

import asyncio
import inspect
import warnings
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Literal, Protocol, TypeAlias

from moq_core import (
    Closed,
    Goaway,
    MessageParameters,
    MoqtObject,
    PublishDone,
    SubgroupEnd,
    SubgroupReset,
    SubscribeOk,
    Subscription,
    SubscriptionHandlers,
)

from .errors import RelayClosedError

# ------------------------------------------------------------------ product events


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


# ------------------------------------------------------------------ lifecycle


@dataclass(frozen=True, slots=True)
class SubscriptionEnd:
    """How a subscription ended.

    ``accepted`` says whether SUBSCRIBE_OK had arrived first, which is what separates a rejection from
    a termination. A REQUEST_ERROR can arrive after acceptance: the core answers a refused ``update()``
    that way and ends the request (issue #94). ``error`` is the :class:`moq_core.RequestError` for a
    request error, the connection's failure (``None`` after a clean shutdown) for closure, and the
    caller's reason for a cancel.
    """

    cause: Literal["request-error", "connection-closed", "cancelled"]
    accepted: bool
    error: object


class SubscriptionConsumer(Protocol):
    """One consumer of a subscription: the handler object or an events iterator."""

    def event(self, event: SubscriptionEvent) -> None:
        """One product event, delivered synchronously as the core delivers it."""

    def ended(self, end: SubscriptionEnd) -> None:
        """The subscription ended; no event follows. Delivered once."""


@dataclass(frozen=True, slots=True)
class SubscriptionDependencies:
    """What the subscription borrows from the connection."""

    subscribe: Callable[[SubscriptionHandlers], Subscription]
    """The core session's subscribe, already bound to the track options."""
    closed: asyncio.Future[Closed]
    """The connection's closure."""


def synchronous_callback(name: str, callback: Callable[..., object]) -> Callable[..., None]:
    """Snapshot one product callback and hold it to the synchronous callback policy.

    An async function is refused before any request is sent, and a returned awaitable is closed or
    cancelled, observed, and diagnosed with a ``RuntimeWarning``, never awaited by delivery. Mirrors
    the core's own guard, which now only sees this module's callbacks.
    """
    if inspect.iscoroutinefunction(callback) or inspect.iscoroutinefunction(callback.__call__):
        raise TypeError(f"{name} must be synchronous; use Relay subscribe_events() for async consumption")

    def invoke(*args: object) -> None:
        result = callback(*args)
        if inspect.isawaitable(result):
            if inspect.iscoroutine(result):
                result.close()
            elif isinstance(result, asyncio.Future):
                result.cancel()

                # Observe an already-failed task as well as a pending task's eventual failure.
                def consume(future: asyncio.Future[object]) -> None:
                    if not future.cancelled():
                        future.exception()

                result.add_done_callback(consume)
            warnings.warn(
                f"{name} returned an awaitable; callbacks must be synchronous; use Relay subscribe_events()",
                RuntimeWarning,
                stacklevel=2,
            )

    return invoke


class ProductSubscription(Subscription):
    """The product subscription: the core handle plus the lifecycle every consumer shares.

    It subclasses the core handle so :meth:`mux_relay.RelaySubscriber.subscribe` keeps returning a
    ``Subscription``; every inherited member is overridden and the core's own slots are never set.
    """

    __slots__ = ("_accepted", "_closed", "_consumers", "_core", "_end")

    def __init__(self, dependencies: SubscriptionDependencies, consumers: Iterable[SubscriptionConsumer] = ()) -> None:
        # Deliberately no super().__init__(): the base's record and session stay unset.
        self._consumers: list[SubscriptionConsumer] = list(consumers)
        self._accepted = False
        self._end: SubscriptionEnd | None = None
        self._core: Subscription | None = None
        self._closed = dependencies.closed

        if self._closed.done():
            self._finish(SubscriptionEnd("connection-closed", False, self._closed.result().error))
            return

        self._closed.add_done_callback(self._connection_closed)
        try:
            self._core = dependencies.subscribe(
                SubscriptionHandlers(
                    on_ok=lambda value: self._deliver(SubscribedEvent(value)),
                    on_object=lambda value: self._deliver(ObjectEvent(value)),
                    on_done=lambda value: self._deliver(PublisherDoneEvent(value)),
                    on_subgroup_end=lambda value: self._deliver(SubgroupEndEvent(value)),
                    on_subgroup_reset=lambda value: self._deliver(SubgroupResetEvent(value)),
                    on_state_notify=lambda value: self._deliver(StateNotifyEvent(value)),
                    on_goaway=lambda value: self._deliver(GoawayEvent(value)),
                    on_update_ok=lambda: self._deliver(UpdateOkEvent()),
                    on_error=lambda error: self._finish(SubscriptionEnd("request-error", self._accepted, error)),
                )
            )
        except BaseException as error:
            self._finish(SubscriptionEnd("cancelled", False, error))
            raise

        # A source may deliver callbacks synchronously during subscribe(); the handle did not exist
        # when the ending was recorded, so cancel it now.
        if self._end is not None:
            self._core.cancel(self._end.error)

    # ------------------------------------------------------------------ the core handle

    @property
    def request_id(self) -> int:
        return self._handle().request_id

    def update(self, parameters: MessageParameters) -> None:
        self._handle().update(parameters)

    def cancel(self, reason: object = None) -> None:
        self._finish(SubscriptionEnd("cancelled", self._accepted, reason))

    # ------------------------------------------------------------------ the lifecycle

    @property
    def accepted(self) -> bool:
        """Whether SUBSCRIBE_OK has arrived."""
        return self._accepted

    @property
    def end(self) -> SubscriptionEnd | None:
        """How the subscription ended, once it has."""
        return self._end

    def attach(self, consumer: SubscriptionConsumer) -> Callable[[], None]:
        """Attach a consumer; one attached after the end is told the end at once. Returns the detach."""
        if self._end is not None:
            consumer.ended(self._end)
            return lambda: None

        self._consumers.append(consumer)

        def detach() -> None:
            if consumer in self._consumers:
                self._consumers.remove(consumer)

        return detach

    def _handle(self) -> Subscription:
        if self._core is not None:
            return self._core
        error = self._end.error if self._end is not None else None
        if isinstance(error, BaseException):
            raise error
        raise RelayClosedError("Subscription ended before its request was sent", phase="subscription")

    def _connection_closed(self, done: asyncio.Future[Closed]) -> None:
        self._finish(SubscriptionEnd("connection-closed", self._accepted, done.result().error))

    def _deliver(self, event: SubscriptionEvent) -> None:
        if self._end is not None:
            return
        if isinstance(event, SubscribedEvent):
            self._accepted = True
        for consumer in list(self._consumers):
            consumer.event(event)

    def _finish(self, end: SubscriptionEnd) -> None:
        if self._end is not None:
            return
        self._end = end
        self._closed.remove_done_callback(self._connection_closed)
        # Every ending cancels the core handle: after a REQUEST_ERROR or closure the core has already
        # dropped the request, and cancelling then is harmless; after a local cancel it is the point.
        if self._core is not None:
            self._core.cancel(end.error)
        consumers = list(self._consumers)
        self._consumers.clear()
        for consumer in consumers:
            consumer.ended(end)


__all__ = [
    "GoawayEvent",
    "ObjectEvent",
    "ProductSubscription",
    "PublisherDoneEvent",
    "StateNotifyEvent",
    "SubgroupEndEvent",
    "SubgroupResetEvent",
    "SubscribedEvent",
    "SubscriptionConsumer",
    "SubscriptionDependencies",
    "SubscriptionEnd",
    "SubscriptionEvent",
    "UpdateOkEvent",
    "synchronous_callback",
]
