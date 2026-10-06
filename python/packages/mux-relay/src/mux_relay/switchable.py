"""The switchable subscription: one subscription at a time delivered to one set of handlers, moved to
another track (or the same track with other parameters) make-before-break (issue #264).

Draft-22 REQUEST_UPDATE names no track, and moq-relay ends a subscription on any update, so changing
what a viewer receives means a new SUBSCRIBE. :meth:`SwitchableSubscription.switch_to` opens it while
the active subscription keeps delivering, holds it pending until one of its objects passes ``until``,
then cancels the old one and hands the handlers the new one. Each subscription underneath is an
ordinary product subscription (``_subscription.py``); this module only routes their events and owns
the switchable's own ending.
"""

from __future__ import annotations

import asyncio
import inspect
import math
from collections.abc import Callable
from dataclasses import dataclass, fields
from typing import Literal

from moq_core import (
    Goaway,
    MessageParameters,
    MoqtObject,
    PublishDone,
    RequestError,
    SubgroupEnd,
    SubgroupReset,
    SubscribeOk,
)

from ._awaitable import RelayAwaitable
from ._subscription import (
    ObjectEvent,
    RelaySubscription,
    SubgroupEndEvent,
    SubgroupResetEvent,
    SubscribedEvent,
    SubscriptionConsumer,
    SubscriptionEnd,
    SubscriptionEvent,
    _end_error,
    _observe,
    synchronous_callback,
)
from ._validation import track_name
from .cancellation import CancellationToken
from .connect_session import MAX_TIMEOUT
from .errors import RelayError


@dataclass(frozen=True, slots=True)
class SwitchedInfo:
    """What ``on_switched`` receives and :meth:`SwitchableSubscription.switch_to` resolves with."""

    from_track: str
    """The track that was active before the cutover."""
    to_track: str
    """The track now active; the same as ``from_track`` when only the parameters changed."""
    subscribed: SubscribeOk
    """The new subscription's acceptance info."""


@dataclass(slots=True)
class SwitchableHandlers:
    """Callbacks for a switchable subscription.

    Each runs synchronously, for the active subscription's events only, and receives the active
    track's name as its last argument. A pending subscription reaches no handler before its cutover.
    """

    on_subscribed: Callable[[SubscribeOk, str], None] | None = None
    """The first subscription accepted (SUBSCRIBE_OK). A later subscription's acceptance arrives with
    its cutover, in ``on_switched``."""
    on_error: Callable[[RequestError, str], None] | None = None
    """The active subscription ended with REQUEST_ERROR: the first one was rejected, or an accepted
    one was refused later. The switchable has ended. A pending subscription's rejection fails its
    ``switch_to`` instead."""
    on_object: Callable[[MoqtObject, str], None] | None = None
    """One object from the active subscription. Payloads are uninterpreted bytes."""
    on_subgroup_end: Callable[[SubgroupEnd, str], None] | None = None
    """One of the active subscription's subgroup streams finished cleanly (FIN)."""
    on_subgroup_reset: Callable[[SubgroupReset, str], None] | None = None
    """One of the active subscription's subgroup streams was reset; objects from it may be missing."""
    on_publisher_done: Callable[[PublishDone, str], None] | None = None
    """The active track's publisher finished with it (PUBLISH_DONE). Not an ending, as on
    ``RelaySubscriptionHandlers.on_publisher_done``; a pending switch carries on."""
    on_state_notify: Callable[[MessageParameters, str], None] | None = None
    """The publisher changed the active subscription's state (PUBLISH_STATE_NOTIFY)."""
    on_goaway: Callable[[Goaway, str], None] | None = None
    """GOAWAY on the active subscription's request stream. Informational; nothing is retried."""
    on_switched: Callable[[SwitchedInfo], None] | None = None
    """A cutover: ``to_track`` is now active and ``from_track`` has been cancelled (or is draining).
    Runs before the first object of ``to_track``, which is the one that passed ``until``. Decoders
    drop state that assumed continuity here."""


_EVENT_HANDLERS: dict[str, str | None] = {
    "subscribed": "on_subscribed",
    "object": "on_object",
    "publisher_done": "on_publisher_done",
    "subgroup_end": "on_subgroup_end",
    "subgroup_reset": "on_subgroup_reset",
    "state_notify": "on_state_notify",
    "goaway": "on_goaway",
    # A switchable has no update().
    "update_ok": None,
}
"""Which handler each product event reaches."""

Until = Literal["first-object"] | Callable[[MoqtObject], bool]
"""When to cut over: ``"first-object"``, or a synchronous predicate on the new subscription's objects."""


def snapshot_switchable_handlers(handlers: SwitchableHandlers | None) -> dict[str, Callable[..., None]]:
    """Check and snapshot the handlers, before connection state is read."""
    if handlers is None:
        return {}
    if not isinstance(handlers, SwitchableHandlers):
        raise TypeError("handlers must be SwitchableHandlers")
    snapshot: dict[str, Callable[..., None]] = {}
    for handler_field in fields(handlers):
        callback = getattr(handlers, handler_field.name)
        if callback is None:
            continue
        if not callable(callback):
            raise TypeError(f"{handler_field.name} must be callable")
        snapshot[handler_field.name] = synchronous_callback(handler_field.name, callback)
    return snapshot


def _until_predicate(until: Until) -> Callable[[MoqtObject], bool]:
    if until == "first-object":
        return lambda _object: True
    if isinstance(until, str) or not callable(until):
        raise TypeError("until must be 'first-object' or a callable")
    if inspect.iscoroutinefunction(until) or inspect.iscoroutinefunction(type(until).__call__):
        raise TypeError("until must be synchronous")

    def predicate(obj: MoqtObject) -> bool:
        passed = until(obj)
        if not isinstance(passed, bool):
            # An awaitable is never awaited: close or cancel it, and observe its failure.
            if inspect.iscoroutine(passed):
                passed.close()
            elif isinstance(passed, asyncio.Future):
                passed.cancel()
                _observe(passed)
            raise TypeError("until must return a bool")
        return passed

    return predicate


_HeldEvent = ObjectEvent | SubgroupEndEvent | SubgroupResetEvent
"""A group-scoped event of a pending subscription that did not cut over: an object ``until``
refused, or a subgroup's end."""

_HELD_EVENTS = 256
"""How many of a pending subscription's events are held for its cutover; the oldest are dropped first."""


def _hold(held: list[_HeldEvent], event: _HeldEvent) -> None:
    """Hold one more of a pending subscription's events, dropping the oldest beyond the bound."""
    held.append(event)
    if len(held) > _HELD_EVENTS:
        del held[0]


def _held_after(held: list[_HeldEvent], cutover: MoqtObject) -> list[_HeldEvent]:
    """The held events that come after the cutover object in its own subscription, in arrival order.

    Those of later groups, and later objects of its group. Groups arrive out of order across streams,
    so a later group can reach a pending subscription before the object that cuts over; discarding it
    would leave a gap, while the earlier groups are the old subscription's to deliver. Group IDs are
    compared only within the one subscription.
    """
    return [
        event
        for event in held
        if event.value.group_id > cutover.group_id
        or (
            isinstance(event, ObjectEvent)
            and event.value.group_id == cutover.group_id
            and event.value.object_id > cutover.object_id
        )
    ]


def _check_drain(drain: object) -> None:
    """Check a drain duration in seconds; shared with the supervised want."""
    if isinstance(drain, bool) or not isinstance(drain, (int, float)):
        raise TypeError("drain must be a number of seconds")
    # The same bound as every SDK deadline, which the TypeScript SDK's timers impose.
    if not math.isfinite(drain) or not 0 <= drain <= MAX_TIMEOUT:
        raise ValueError(f"drain must be between 0 and {MAX_TIMEOUT} seconds")


class _Slot:
    """One subscription the switchable holds: active, pending or draining."""

    __slots__ = ("accepted", "drain_through", "subscription", "track")

    def __init__(self, track: str) -> None:
        self.track = track
        self.subscription: RelaySubscription | None = None
        self.accepted: SubscribeOk | None = None
        # While draining after a cutover: the largest group it had delivered, through which its
        # events still reach the handlers.
        self.drain_through: int | None = None


class _PendingSwitch(_Slot):
    """A pending switch: its slot plus the future ``switch_to`` returned."""

    __slots__ = ("drain", "future", "held", "remove_cancellation", "until")

    def __init__(self, track: str, until: Callable[[MoqtObject], bool], drain: float) -> None:
        super().__init__(track)
        self.held: list[_HeldEvent] = []
        """Its events held until the cutover."""
        self.until = until
        self.drain = drain
        self.future: asyncio.Future[SwitchedInfo] = asyncio.get_running_loop().create_future()
        _observe(self.future)
        self.remove_cancellation: Callable[[], None] = lambda: None


class _SlotConsumer:
    """The consumer for one slot: active events go to the handlers, pending ones to the cutover check."""

    __slots__ = ("_slot", "_switchable")

    def __init__(self, switchable: SwitchableSubscription, slot: _Slot) -> None:
        self._switchable = switchable
        self._slot = slot

    def event(self, event: SubscriptionEvent) -> None:
        self._switchable._event(self._slot, event)

    def ended(self, end: SubscriptionEnd) -> None:
        self._switchable._slot_ended(self._slot, end)


OpenSubscription = Callable[[str, MessageParameters | None, SubscriptionConsumer], RelaySubscription]
"""Open one product subscription to a track in the switchable's namespace with the consumer
attached. Checks its arguments and the connection first, and raises for either."""


class SwitchableSubscription:
    """The handle :meth:`mux_relay.RelaySubscriber.subscribe_switchable` returns.

    Handlers see one subscription at a time, the active one. :meth:`switch_to` moves them to another
    track in the same namespace, or the same track with other parameters, make-before-break.
    """

    __slots__ = (
        "_active",
        "_active_largest_group",
        "_callbacks",
        "_draining",
        "_end",
        "_ended",
        "_ended_awaitable",
        "_open",
        "_pending",
        "_subscribed",
        "_subscribed_awaitable",
    )

    def __init__(
        self,
        open_subscription: OpenSubscription,
        track: str,
        parameters: MessageParameters | None,
        callbacks: dict[str, Callable[..., None]],
    ) -> None:
        loop = asyncio.get_running_loop()
        self._open = open_subscription
        self._callbacks = callbacks
        self._pending: _PendingSwitch | None = None
        # Old subscriptions draining after a cutover, each with the timer that cancels it.
        self._draining: dict[_Slot, asyncio.TimerHandle] = {}
        self._end: SubscriptionEnd | None = None
        # The largest group ID among the objects the active subscription delivered.
        self._active_largest_group: int | None = None
        self._subscribed: asyncio.Future[SubscribeOk] = loop.create_future()
        _observe(self._subscribed)
        self._ended: asyncio.Future[SubscriptionEnd] = loop.create_future()
        self._subscribed_awaitable = RelayAwaitable(self._subscribed)
        self._ended_awaitable = RelayAwaitable(self._ended)
        initial = _Slot(track)
        self._active = initial
        initial.subscription = open_subscription(track, parameters, _SlotConsumer(self, initial))
        # An ending recorded while the subscription was being opened found no subscription yet.
        if initial.subscription._end is not None:
            self._finish(initial.subscription._end)

    # ------------------------------------------------------------------ the public handle

    @property
    def track(self) -> str:
        """The active subscription's track name."""
        return self._active.track

    @property
    def pending(self) -> str | None:
        """The pending subscription's track name, while a switch is pending."""
        return self._pending.track if self._pending is not None else None

    @property
    def subscribed(self) -> RelayAwaitable[SubscribeOk]:
        """Settles with the acceptance info of the first subscription to be active and accepted.

        That is the first one, or the one a switch made before the first one was answered. Raises as
        ``RelaySubscription.subscribed`` does if the switchable ends before that.
        """
        return self._subscribed_awaitable

    @property
    def ended(self) -> RelayAwaitable[SubscriptionEnd]:
        """Settles once with the active subscription's ending, when it ends other than by a cutover,
        or when :meth:`cancel` runs. Any pending switch is cancelled with it. Never raises."""
        return self._ended_awaitable

    def switch_to(
        self,
        track: str,
        *,
        parameters: MessageParameters | None = None,
        until: Until = "first-object",
        drain: float = 0.0,
        cancellation: CancellationToken | None = None,
    ) -> RelayAwaitable[SwitchedInfo]:
        """Subscribe to ``track`` in the same namespace and cut over once one of its objects passes ``until``.

        The active subscription keeps delivering until then, and is cancelled at the cutover. The
        track may be the active one, to change parameters. ``parameters`` are not inherited from the
        subscription being replaced: omitted means none.

        ``until`` is ``"first-object"`` (the default), which promotes on the new subscription's first
        object, or a synchronous predicate that promotes on the first object it accepts. Objects
        before the cutover are discarded, except those of groups later than the one that cuts over
        (arrived early, out of order), which follow it. Compare the object's own timeline (a timestamp property,
        say) with what the active subscription delivered, so the cutover repeats media rather than
        skipping it: group IDs are no such timeline, since each track numbers its own groups. A new
        subscription can also start with a stale cached group, or lose its first object through
        another relay (issue #243); a timeline predicate absorbs both.

        ``drain`` keeps the old subscription open that many seconds after the cutover, from 0 to
        ``MAX_TIMEOUT`` (2147483.647, as for every SDK deadline). Groups arrive
        out of order across streams, most of all through another relay, so an older group of the old
        track can still be in flight at the cutover. While it drains, its objects, subgroup ends and
        resets from groups no later than the largest it had delivered still reach the handlers, with
        the old track's name, after ``on_switched``; everything else from it is discarded.

        ``cancellation`` abandons the switch while it is pending: the new subscription is cancelled
        and the active one stays. A newer ``switch_to`` also cancels a pending one, which raises
        ``CLOSED``. The returned awaitable raises as ``RelaySubscription.subscribed`` does if the new
        subscription ends before its cutover, the token's reason when it is cancelled, or what
        ``until`` raised; the active subscription carries on. Raises at once for an invalid track or
        options, a closed connection, or an ended switchable.
        """
        # Arguments before the switchable's or the connection's state (spec/behavior.md, Arguments).
        track_name(track)
        predicate = _until_predicate(until)
        _check_drain(drain)
        if cancellation is not None and not isinstance(cancellation, CancellationToken):
            raise TypeError("cancellation must be a CancellationToken")
        if self._end is not None:
            raise RelayError(
                "The switchable subscription has ended; subscribe again", code="CLOSED", phase="subscription"
            )
        if cancellation is not None:
            cancellation.throw_if_cancelled()
        switching = _PendingSwitch(track, predicate, float(drain))
        # Open first: an invalid track or a closed connection raises here and leaves any pending switch alone.
        subscription = self._open(track, parameters, _SlotConsumer(self, switching))
        switching.subscription = subscription
        previous = self._pending
        self._pending = switching
        if previous is not None:
            superseded = RelayError(f"Superseded by a switch to {track}", code="CLOSED", phase="subscription")
            self._abandon(previous, superseded)
        if subscription._end is not None:
            self._abandon(switching, _end_error(subscription._end))
            return RelayAwaitable(switching.future)
        if cancellation is not None:
            token = cancellation

            def on_cancel(reason: BaseException) -> None:
                if self._pending is switching:
                    self._abandon(switching, reason)

            token.add_callback(on_cancel)
            switching.remove_cancellation = lambda: token.remove_callback(on_cancel)
        return RelayAwaitable(switching.future)

    def cancel(self, reason: object = None) -> None:
        """Cancel the active subscription and any pending one. Idempotent."""
        if self._end is not None:
            return
        if self._pending is not None:
            error = (
                reason
                if isinstance(reason, BaseException)
                else RelayError("The switchable subscription was cancelled", code="CLOSED", phase="subscription")
            )
            self._abandon(self._pending, error)
        assert self._active.subscription is not None
        self._active.subscription.cancel(reason)

    # ------------------------------------------------------------------ routing

    def _call(self, name: str, *args: object) -> None:
        callback = self._callbacks.get(name)
        if callback is not None:
            callback(*args)

    def _mark_subscribed(self, info: SubscribeOk) -> None:
        if not self._subscribed.done():
            self._subscribed.set_result(info)

    def _event(self, slot: _Slot, event: SubscriptionEvent) -> None:
        if self._end is not None:
            return
        if isinstance(event, SubscribedEvent):
            slot.accepted = event.value
        if slot is self._active:
            if isinstance(event, SubscribedEvent):
                if self._subscribed.done():
                    return
                self._mark_subscribed(event.value)
            if isinstance(event, ObjectEvent) and (
                self._active_largest_group is None or event.value.group_id > self._active_largest_group
            ):
                self._active_largest_group = event.value.group_id
            name = _EVENT_HANDLERS[event.type]
            if name is not None:
                self._call(name, event.value, slot.track)
            return
        if slot in self._draining:
            assert slot.drain_through is not None
            if isinstance(event, (ObjectEvent, SubgroupEndEvent, SubgroupResetEvent)) and (
                event.value.group_id <= slot.drain_through
            ):
                self._call(_EVENT_HANDLERS[event.type] or "", event.value, slot.track)
            return
        pending = self._pending
        if slot is not pending:
            return
        if isinstance(event, (SubgroupEndEvent, SubgroupResetEvent)):
            _hold(pending.held, event)
        if not isinstance(event, ObjectEvent):
            return
        try:
            passed = pending.until(event.value)
        except (Exception, asyncio.CancelledError) as error:
            # CancelledError is a BaseException; left uncaught it would escape into the stream reader.
            if self._end is None and self._pending is pending:
                self._abandon(pending, error)
            return
        # The predicate is application code: it may have switched again or cancelled.
        if not passed:
            _hold(pending.held, event)
        elif self._end is None and self._pending is pending:
            self._cutover(pending, event.value)

    def _slot_ended(self, slot: _Slot, end: SubscriptionEnd) -> None:
        if slot is self._active:
            self._finish(end)
        elif slot in self._draining:
            self._stop_drain(slot)
        elif slot is self._pending:
            self._abandon(self._pending, _end_error(end))

    def _cutover(self, switching: _PendingSwitch, obj: MoqtObject) -> None:
        previous = self._active
        self._pending = None
        switching.remove_cancellation()
        self._active = switching
        # The old subscription's consumer checks the active slot first, so its events from here on
        # reach the handlers only while it drains, and its ending reaches nobody.
        assert previous.subscription is not None
        if switching.drain > 0 and self._active_largest_group is not None:
            previous.drain_through = self._active_largest_group
            loop = asyncio.get_running_loop()
            self._draining[previous] = loop.call_later(switching.drain, self._stop_drain, previous)
        else:
            previous.subscription.cancel()
        self._active_largest_group = obj.group_id
        # A subscription that delivered an object was accepted.
        assert switching.accepted is not None
        self._mark_subscribed(switching.accepted)
        switched = SwitchedInfo(from_track=previous.track, to_track=switching.track, subscribed=switching.accepted)
        if not switching.future.done():
            switching.future.set_result(switched)
        self._call("on_switched", switched)
        # A handler may have switched again or cancelled; the object still belongs to this cutover.
        if self._end is None and self._active is switching:
            self._call("on_object", obj, switching.track)
        for held in _held_after(switching.held, obj):
            if self._end is not None or self._active is not switching:
                break
            if isinstance(held, ObjectEvent) and held.value.group_id > self._active_largest_group:
                self._active_largest_group = held.value.group_id
            self._call(_EVENT_HANDLERS[held.type] or "", held.value, switching.track)
        switching.held = []

    # ------------------------------------------------------------------ the lifecycle

    def _stop_drain(self, slot: _Slot) -> None:
        timer = self._draining.pop(slot, None)
        if timer is not None:
            timer.cancel()
        if slot.subscription is not None:
            slot.subscription.cancel()

    def _abandon(self, switching: _PendingSwitch, error: BaseException) -> None:
        """Settle a pending switch that did not cut over, cancelling its subscription if it still runs."""
        if self._pending is switching:
            self._pending = None
        switching.remove_cancellation()
        if not switching.future.done():
            switching.future.set_exception(error)
        if switching.subscription is not None:
            switching.subscription.cancel(error)

    def _finish(self, end: SubscriptionEnd) -> None:
        if self._end is not None:
            return
        self._end = end
        if not self._subscribed.done():
            self._subscribed.set_exception(_end_error(end))
        if self._pending is not None:
            # A closed connection ends both subscriptions; report it the same way on the pending switch.
            error = (
                _end_error(end)
                if end.cause == "connection-closed"
                else RelayError(
                    "The switchable subscription ended before the switch", code="CLOSED", phase="subscription"
                )
            )
            self._abandon(self._pending, error)
        for slot in list(self._draining):
            self._stop_drain(slot)
        if not self._ended.done():
            self._ended.set_result(end)
        if end.cause == "request-error":
            self._call("on_error", end.error, self._active.track)


__all__ = ["SwitchableHandlers", "SwitchableSubscription", "SwitchedInfo", "Until"]
