"""The application's subscription (see ``CONTEXT.md``): one SUBSCRIBE request from creation to its
terminal state, as distinct from demand, the relay's own subscription to a publisher.

The module owns the core handle, the connection's closure observation and the terminal state, and
translates the core's callbacks into product events once. Its consumers are the callback form
(:meth:`mux_relay.RelaySubscriber.subscribe` with handlers) and the bounded events iterator; both
attach through :class:`SubscriptionConsumer` and see the same events and the same ending. Its
public face is :class:`RelaySubscription`: the core handle plus ``subscribed`` and ``ended``, the
readiness and termination awaitables. The pre-acceptance ``update()`` queue lives here too.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import warnings
from collections.abc import Callable, Iterable
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, TypeAlias

from moq_core import (
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
from moq_core.control_messages import encode_request_update

from ._awaitable import RelayAwaitable
from .errors import RelayError

_log = logging.getLogger("mux_relay")

UPDATE_QUEUE_BOUND = 8
"""How many ``update()`` calls may wait for SUBSCRIBE_OK.

REQUEST_UPDATE is legal only on an accepted subscription (draft-ietf-moq-transport-20 §5.1), so
earlier updates queue in order.
"""

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

SubscriptionEndKind: TypeAlias = Literal["rejected", "terminated", "cancelled"]
"""The lifecycle name for how a subscription ended.

``rejected``: the relay answered the request with REQUEST_ERROR before accepting it (including the
request timeout). ``terminated``: an accepted subscription can no longer deliver (the connection
ended, or a REQUEST_ERROR arrived after acceptance, which is how a refused ``update()`` is answered,
issue #94), or the connection ended before any answer. ``cancelled``: the application cancelled it.
"""


@dataclass(frozen=True, slots=True)
class SubscriptionEnd:
    """How a subscription ended.

    ``kind`` is the lifecycle name; ``cause`` is what happened on the wire or locally; ``accepted``
    says whether SUBSCRIBE_OK had arrived; ``error`` is the :class:`moq_core.RequestError` for a
    request error, the connection's failure (``None`` after a clean shutdown) for closure, and the
    caller's reason for a cancel.
    """

    kind: SubscriptionEndKind
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
    observe_closed: Callable[[Callable[[BaseException | None], None]], Callable[[], None]]
    """Observe the connection's closure in the turn it happens, ahead of the request failures the
    core synthesizes while tearing down. The returned function removes the observer."""
    transport_ended: Callable[[], bool]
    """Whether the transport has already ended, before the session has reported the closure. A
    transport that cannot say answers ``False``."""


class SubscriptionOverflowError(RelayError):
    """A stalled consumer exceeded the event capacity; the subscription was cancelled."""

    def __init__(self, message: str = "Subscription event buffer overflow") -> None:
        super().__init__(message, code="SUBSCRIPTION_OVERFLOW", phase="subscription")


class SubscriptionRequestError(RelayError):
    """Request rejection, timeout, or request failure, with the protocol details preserved."""

    def __init__(self, request_error: RequestError) -> None:
        super().__init__(request_error.reason, code="SUBSCRIPTION_REQUEST_FAILED", phase="subscription")
        self.request_error = request_error


class SubscriptionConnectionClosedError(RelayError):
    """The connection ended, without proving that subscription delivery was drained."""

    def __init__(self, cause: BaseException | None) -> None:
        super().__init__("Connection closed; subscription delivery ended without a drain guarantee", code="CLOSED")
        self.__cause__ = cause


def _end_error(end: SubscriptionEnd) -> BaseException:
    """The exception ``subscribed`` raises for an ending that came before acceptance."""
    if end.cause == "request-error" and isinstance(end.error, RequestError):
        return SubscriptionRequestError(end.error)
    if end.cause == "connection-closed":
        return SubscriptionConnectionClosedError(end.error if isinstance(end.error, BaseException) else None)
    if isinstance(end.error, BaseException):
        return end.error
    return RelayError("Subscription was cancelled before the relay answered", code="CLOSED", phase="subscription")


def _observe(future: asyncio.Future[Any]) -> None:
    """Mark a future's eventual exception as handled without acting on it."""

    def finished(done: asyncio.Future[Any]) -> None:
        if not done.cancelled():
            done.exception()

    future.add_done_callback(finished)


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


class RelaySubscription(Subscription):
    """The handle :meth:`mux_relay.RelaySubscriber.subscribe` returns.

    The core handle (``request_id``, ``update()``, ``cancel()``) plus two awaitables. ``subscribed``
    settles when the relay answers: it resolves with the acceptance info ``on_subscribed`` receives
    and raises :class:`SubscriptionRequestError` when the relay rejected the request or it timed out,
    :class:`SubscriptionConnectionClosedError` when the connection ended first, or the reason given to
    ``cancel()`` (a ``CLOSED`` :class:`RelayError` when none was given). ``ended`` settles once,
    when the subscription can no longer deliver, with a :class:`SubscriptionEnd`; it never raises.
    Publisher completion settles neither, and neither implies exactly-once or gap-free delivery.

    It subclasses the core handle so ``subscribe()`` keeps returning a ``Subscription``; every
    inherited member is overridden and the core's own slots are never set.
    """

    __slots__ = (
        "_accepted",
        "_consumers",
        "_core",
        "_end",
        "_ended",
        "_ended_awaitable",
        "_queued_updates",
        "_remove_closed",
        "_subscribed",
        "_subscribed_awaitable",
        "_transport_ended",
    )

    def __init__(self, dependencies: SubscriptionDependencies, consumers: Iterable[SubscriptionConsumer] = ()) -> None:
        # Deliberately no super().__init__(): the base's record and session stay unset.
        loop = asyncio.get_running_loop()
        self._consumers: list[SubscriptionConsumer] = list(consumers)
        self._queued_updates: list[MessageParameters] = []
        self._accepted = False
        self._end: SubscriptionEnd | None = None
        self._core: Subscription | None = None
        self._remove_closed: Callable[[], None] = lambda: None
        self._transport_ended = dependencies.transport_ended
        self._subscribed: asyncio.Future[SubscribeOk] = loop.create_future()
        # Nobody may await readiness; a failure must not be reported as an unretrieved exception.
        _observe(self._subscribed)
        self._ended: asyncio.Future[SubscriptionEnd] = loop.create_future()
        self._subscribed_awaitable = RelayAwaitable(self._subscribed)
        self._ended_awaitable = RelayAwaitable(self._ended)

        # A connection that has already ended tells the observer at once, ending this before any send.
        self._remove_closed = dependencies.observe_closed(self._connection_ended)
        if self._end is not None:
            return

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
                    on_error=self._fail_request,
                )
            )
        except BaseException as error:
            self._cancel_with(error)
            raise

        # A source may end the subscription synchronously during subscribe(), through a closure
        # observer it invokes at once; the handle did not exist when the ending was recorded, so
        # cancel it now. A request failure delivered that early is held until the turn ends and
        # cancels the handle then.
        if self._end is not None:
            self._core.cancel(self._end.error)

    # ------------------------------------------------------------------ the public handle

    @property
    def subscribed(self) -> RelayAwaitable[SubscribeOk]:
        """Settles when the relay answers. See the class docstring for what it raises.

        Every waiter is isolated: cancelling one, as ``asyncio.wait_for`` does on timeout, leaves
        the subscription's readiness intact for every other.
        """
        return self._subscribed_awaitable

    @property
    def ended(self) -> RelayAwaitable[SubscriptionEnd]:
        """Settles once, when the subscription can no longer deliver, with how it ended. Never raises.

        Every waiter is isolated, as for ``subscribed``.
        """
        return self._ended_awaitable

    @property
    def request_id(self) -> int:
        """The Request ID the SUBSCRIBE carried.

        Raises when the subscription ended before its request was sent (the connection had already
        ended): the error it ended with, or a ``CLOSED`` :class:`RelayError`.
        """
        return self._handle().request_id

    def update(self, parameters: MessageParameters) -> None:
        """Send REQUEST_UPDATE, or queue it in order until the relay accepts the subscription."""
        if self._end is not None:
            raise RelayError(
                "Subscription has ended; open a new one to change what you receive", code="CLOSED", phase="subscription"
            )
        if self._accepted:
            self._handle().update(parameters)
            return
        if len(self._queued_updates) >= UPDATE_QUEUE_BOUND:
            raise RelayError(
                f"More than {UPDATE_QUEUE_BOUND} updates are waiting for the subscription to be accepted; "
                "await on_subscribed before updating again",
                code="UPDATE_QUEUE_FULL",
                phase="subscription",
            )
        # A malformed or oversize update must fail here, where the caller can see it, not during the
        # flush on SUBSCRIBE_OK, so it is encoded exactly as the flush will encode it. The queue
        # holds a snapshot, so later mutation of the caller's object cannot change what is sent, as
        # an immediate send would have serialized it at once.
        encode_request_update(self._handle().request_id, parameters)
        self._queued_updates.append(deepcopy(parameters))

    def cancel(self, reason: object = None) -> None:
        self._cancel_with(reason)

    # ------------------------------------------------------------------ consumers (package-private)

    def _attach(self, consumer: SubscriptionConsumer) -> Callable[[], None]:
        """Attach a consumer; one attached after the end is told the end at once. Returns the detach."""
        if self._end is not None:
            consumer.ended(self._end)
            return lambda: None

        self._consumers.append(consumer)

        def detach() -> None:
            if consumer in self._consumers:
                self._consumers.remove(consumer)

        return detach

    # ------------------------------------------------------------------ the lifecycle

    def _handle(self) -> Subscription:
        if self._core is not None:
            return self._core
        error = self._end.error if self._end is not None else None
        if isinstance(error, BaseException):
            raise error
        raise RelayError("Subscription ended before its request was sent", code="CLOSED", phase="subscription")

    def _fail_request(self, error: RequestError) -> None:
        if self._accepted:
            self._finish(SubscriptionEnd(kind="terminated", cause="request-error", accepted=True, error=error))
            return

        # A transport that has already ended fails its streams before the session learns of the
        # closure, so this failure is the core noticing what the closure observer will report with
        # the connection error a few turns from now. Leave the ending to it.
        if self._transport_ended():
            return

        # The core fails a pending request before it reports the closure that killed it, in the same
        # turn. Hold the failure until the turn ends, so a closure that follows is the ending this
        # subscription describes; a genuine rejection is recorded one turn later.
        def reject() -> None:
            if self._end is not None:
                return
            try:
                self._finish(SubscriptionEnd(kind="rejected", cause="request-error", accepted=False, error=error))
            except Exception:
                # Inside the core's request reader a raising callback fails that task, which is logged
                # at debug level and dropped. This runs outside it, so contain the raise the same way
                # rather than hand it to the loop's exception handler.
                _log.debug("subscription callback failed while recording a rejection", exc_info=True)

        asyncio.get_running_loop().call_soon(reject)

    def _connection_ended(self, error: BaseException | None) -> None:
        self._finish(
            SubscriptionEnd(kind="terminated", cause="connection-closed", accepted=self._accepted, error=error)
        )

    def _cancel_with(self, error: object) -> None:
        self._finish(SubscriptionEnd(kind="cancelled", cause="cancelled", accepted=self._accepted, error=error))

    def _deliver(self, event: SubscriptionEvent) -> None:
        if self._end is not None:
            return
        if isinstance(event, SubscribedEvent):
            self._accepted = True
            if not self._subscribed.done():
                self._subscribed.set_result(event.value)
            # Now Established: the updates that waited go out in order, before consumers hear of it.
            queued, self._queued_updates = self._queued_updates, []
            try:
                if self._core is not None:
                    for parameters in queued:
                        self._core.update(parameters)
            except Exception as error:
                # Enqueue-time validation encoded each update with this request's id, so a raise here
                # is a core fault. Letting it escape into the core's request reader would disable the
                # subscription silently; end it loudly instead. Readiness settled first, so a caller
                # awaiting it sees the acceptance and then this ending.
                self._cancel_with(error)
                return
        for consumer in list(self._consumers):
            consumer.event(event)

    def _finish(self, end: SubscriptionEnd) -> None:
        if self._end is not None:
            return
        self._end = end
        self._queued_updates.clear()
        self._remove_closed()
        if not self._subscribed.done():
            self._subscribed.set_exception(_end_error(end))
        if not self._ended.done():
            self._ended.set_result(end)
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
    "PublisherDoneEvent",
    "RelaySubscription",
    "StateNotifyEvent",
    "SubgroupEndEvent",
    "SubgroupResetEvent",
    "SubscribedEvent",
    "SubscriptionConnectionClosedError",
    "SubscriptionConsumer",
    "SubscriptionDependencies",
    "SubscriptionEnd",
    "SubscriptionEndKind",
    "SubscriptionEvent",
    "SubscriptionOverflowError",
    "SubscriptionRequestError",
    "UpdateOkEvent",
    "synchronous_callback",
]
