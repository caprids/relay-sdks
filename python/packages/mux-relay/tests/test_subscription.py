"""The subscription through its own interface: a fake core subscribe and the connection's closure
future, with recording consumers. Relay behaviour over a real connection stays in test_connect.py
and test_subscription_events.py.
"""

import asyncio
import warnings
from typing import Any, cast
from unittest.mock import Mock

import pytest

from moq_core import Closed, MessageParameters, RequestError, SubscribeOk, Subscription, SubscriptionHandlers
from mux_relay import RelayClosedError, RelayError
from mux_relay._subscription import (
    UPDATE_QUEUE_BOUND,
    ProductSubscription,
    SubscribedEvent,
    SubscriptionDependencies,
    SubscriptionEnd,
    SubscriptionEvent,
    synchronous_callback,
)

REQUEST_ERROR = RequestError(error_code=404, retry_interval=0, reason="missing")
OK = cast(SubscribeOk, Mock(name="subscribe_ok"))


class Recorder:
    def __init__(self) -> None:
        self.events: list[SubscriptionEvent] = []
        self.endings: list[SubscriptionEnd] = []

    def event(self, event: SubscriptionEvent) -> None:
        self.events.append(event)

    def ended(self, end: SubscriptionEnd) -> None:
        self.endings.append(end)


class Harness:
    def __init__(self, *, closed_already: bool = False, closed_error: BaseException | None = None) -> None:
        self.handlers: SubscriptionHandlers | None = None
        self.core = Mock(spec=Subscription)
        self.core.request_id = 7
        self.closed: asyncio.Future[Closed] = asyncio.get_running_loop().create_future()
        if closed_already:
            self.closed.set_result(Closed(closed_error))
        self.subscribe = Mock(side_effect=self._subscribe)
        self.deps = SubscriptionDependencies(subscribe=self.subscribe, closed=self.closed)

    def _subscribe(self, handlers: SubscriptionHandlers) -> Subscription:
        self.handlers = handlers
        return cast(Subscription, self.core)

    def core_handlers(self) -> SubscriptionHandlers:
        assert self.handlers is not None, "core subscribe was not called"
        return self.handlers

    async def close_connection(self, error: BaseException | None = None) -> None:
        self.closed.set_result(Closed(error))
        await asyncio.sleep(0)


def types(events: list[SubscriptionEvent]) -> list[str]:
    return [event.type for event in events]


class TestTranslatingTheCoreOnce:
    async def test_delivers_each_core_callback_as_one_product_event_to_every_consumer_and_tracks_acceptance(
        self,
    ) -> None:
        h = Harness()
        first = Recorder()
        second = Recorder()
        subscription = ProductSubscription(h.deps, [first])
        subscription.attach(second)
        assert subscription.request_id == 7
        assert not subscription.accepted
        core = h.core_handlers()
        assert core.on_ok and core.on_object and core.on_done and core.on_subgroup_end and core.on_subgroup_reset
        assert core.on_state_notify and core.on_goaway and core.on_update_ok
        core.on_ok(OK)
        assert subscription.accepted
        core.on_object(cast(Any, Mock(name="object")))
        core.on_done(cast(Any, Mock(name="done")))
        core.on_subgroup_end(cast(Any, Mock(name="end")))
        core.on_subgroup_reset(cast(Any, Mock(name="reset")))
        core.on_state_notify(cast(Any, Mock(name="notify")))
        core.on_goaway(cast(Any, Mock(name="goaway")))
        core.on_update_ok()
        expected = [
            "subscribed",
            "object",
            "publisher_done",
            "subgroup_end",
            "subgroup_reset",
            "state_notify",
            "goaway",
            "update_ok",
        ]
        assert types(first.events) == expected
        assert types(second.events) == expected
        assert first.endings == []
        assert subscription.end is None

    async def test_forwards_update_to_the_core_handle_once_the_subscription_is_accepted(self) -> None:
        h = Harness()
        subscription = ProductSubscription(h.deps)
        on_ok = h.core_handlers().on_ok
        assert on_ok is not None
        on_ok(OK)
        parameters = MessageParameters(forward=0)
        subscription.update(parameters)
        h.core.update.assert_called_once_with(parameters)

    async def test_stops_delivering_to_a_detached_consumer(self) -> None:
        h = Harness()
        first = Recorder()
        subscription = ProductSubscription(h.deps)
        detach = subscription.attach(first)
        update_ok = h.core_handlers().on_update_ok
        assert update_ok is not None
        update_ok()
        detach()
        detach()
        update_ok()
        assert len(first.events) == 1

    async def test_is_a_core_subscription_for_callers_that_check(self) -> None:
        h = Harness()
        assert isinstance(ProductSubscription(h.deps), Subscription)


class TestEndings:
    @pytest.mark.parametrize("accepted", [False, True])
    async def test_records_a_request_error_cancels_the_core_releases_the_closure_observer_and_drops_later_events(
        self, accepted: bool
    ) -> None:
        h = Harness()
        r = Recorder()
        subscription = ProductSubscription(h.deps, [r])
        core = h.core_handlers()
        assert core.on_ok and core.on_error and core.on_update_ok
        if accepted:
            core.on_ok(OK)
        core.on_error(REQUEST_ERROR)
        assert r.endings == [SubscriptionEnd("request-error", accepted, REQUEST_ERROR)]
        assert subscription.end == r.endings[0]
        h.core.cancel.assert_called_once_with(REQUEST_ERROR)
        assert not h.closed._callbacks
        core.on_update_ok()
        core.on_error(REQUEST_ERROR)
        assert types(r.events) == (["subscribed"] if accepted else [])
        assert len(r.endings) == 1

    @pytest.mark.parametrize("accepted", [False, True])
    async def test_records_connection_closure_with_the_connection_error_and_cancels_the_core_with_it(
        self, accepted: bool
    ) -> None:
        h = Harness()
        r = Recorder()
        ProductSubscription(h.deps, [r])
        if accepted:
            on_ok = h.core_handlers().on_ok
            assert on_ok is not None
            on_ok(OK)
        error = RuntimeError("lost")
        await h.close_connection(error)
        assert r.endings == [SubscriptionEnd("connection-closed", accepted, error)]
        h.core.cancel.assert_called_once_with(error)

    async def test_records_a_local_cancel_once_idempotently_and_tells_consumers_attached_afterwards(self) -> None:
        h = Harness()
        r = Recorder()
        late = Recorder()
        subscription = ProductSubscription(h.deps, [r])
        on_ok = h.core_handlers().on_ok
        assert on_ok is not None
        on_ok(OK)
        reason = RuntimeError("done with it")
        subscription.cancel(reason)
        subscription.cancel(RuntimeError("again"))
        assert r.endings == [SubscriptionEnd("cancelled", True, reason)]
        h.core.cancel.assert_called_once_with(reason)
        assert not h.closed._callbacks
        subscription.attach(late)
        assert late.endings == r.endings

    @pytest.mark.parametrize("error", [RuntimeError("gone"), None])
    async def test_ends_before_sending_when_the_connection_had_already_closed_and_refuses_the_handle(
        self, error: BaseException | None
    ) -> None:
        h = Harness(closed_already=True, closed_error=error)
        r = Recorder()
        subscription = ProductSubscription(h.deps, [r])
        h.subscribe.assert_not_called()
        assert r.endings == [SubscriptionEnd("connection-closed", False, error)]
        with pytest.raises(RuntimeError if error is not None else RelayClosedError):
            _ = subscription.request_id

    async def test_releases_the_closure_observer_tells_consumers_and_reraises_when_the_core_subscribe_raises(
        self,
    ) -> None:
        h = Harness()
        failure = ValueError("bad options")
        h.subscribe.side_effect = failure
        r = Recorder()
        with pytest.raises(ValueError, match="bad options"):
            ProductSubscription(h.deps, [r])
        assert not h.closed._callbacks
        assert r.endings == [SubscriptionEnd("cancelled", False, failure)]

    async def test_cancels_the_core_handle_when_the_source_ends_the_subscription_synchronously_during_subscribe(
        self,
    ) -> None:
        core = Mock(spec=Subscription)
        core.request_id = 1

        def subscribe(handlers: SubscriptionHandlers) -> Subscription:
            assert handlers.on_error is not None
            handlers.on_error(REQUEST_ERROR)
            return cast(Subscription, core)

        closed: asyncio.Future[Closed] = asyncio.get_running_loop().create_future()
        r = Recorder()
        subscription = ProductSubscription(SubscriptionDependencies(subscribe=subscribe, closed=closed), [r])
        assert r.endings == [SubscriptionEnd("request-error", False, REQUEST_ERROR)]
        core.cancel.assert_called_once_with(REQUEST_ERROR)
        assert subscription.request_id == 1
        assert not closed._callbacks


class TestUpdatesBeforeAcceptance:
    async def test_queues_updates_issued_before_subscribe_ok_sends_them_in_order_once_accepted_then_forwards(
        self,
    ) -> None:
        h = Harness()
        subscription = ProductSubscription(h.deps)
        first = MessageParameters(forward=0)
        second = MessageParameters(forward=1)
        subscription.update(first)
        subscription.update(second)
        h.core.update.assert_not_called()
        on_ok = h.core_handlers().on_ok
        assert on_ok is not None
        on_ok(OK)
        assert [call.args[0] for call in h.core.update.call_args_list] == [first, second]
        subscription.update(MessageParameters(subscriber_priority=3))
        assert h.core.update.call_count == 3

    async def test_refuses_a_further_queued_update_with_update_queue_full_once_the_bound_is_reached(self) -> None:
        h = Harness()
        subscription = ProductSubscription(h.deps)
        for priority in range(UPDATE_QUEUE_BOUND):
            subscription.update(MessageParameters(subscriber_priority=priority))
        with pytest.raises(RelayError) as failure:
            subscription.update(MessageParameters(forward=0))
        assert (failure.value.code, failure.value.phase) == ("UPDATE_QUEUE_FULL", "subscription")
        on_ok = h.core_handlers().on_ok
        assert on_ok is not None
        on_ok(OK)
        assert h.core.update.call_count == UPDATE_QUEUE_BOUND

    async def test_discards_queued_updates_when_the_subscription_ends_before_acceptance(self) -> None:
        h = Harness()
        subscription = ProductSubscription(h.deps)
        subscription.update(MessageParameters(forward=0))
        on_error = h.core_handlers().on_error
        assert on_error is not None
        on_error(REQUEST_ERROR)
        h.core.update.assert_not_called()

    async def test_raises_closed_for_an_update_after_the_subscription_ended(self) -> None:
        h = Harness()
        subscription = ProductSubscription(h.deps)
        on_ok = h.core_handlers().on_ok
        assert on_ok is not None
        on_ok(OK)
        subscription.cancel(RuntimeError("done"))
        with pytest.raises(RelayClosedError) as failure:
            subscription.update(MessageParameters(forward=0))
        assert failure.value.phase == "subscription"
        h.core.update.assert_not_called()


class TestSynchronousCallbacks:
    def test_refuses_an_async_function_before_anything_is_sent(self) -> None:
        async def on_object(value: object) -> None:
            pass

        with pytest.raises(TypeError, match="synchronous"):
            synchronous_callback("on_object", on_object)

    async def test_closes_a_returned_coroutine_and_diagnoses_it_once_per_call(self) -> None:
        calls: list[int] = []

        async def work() -> None:
            calls.append(1)

        callback = synchronous_callback("on_object", lambda value: work())
        with pytest.warns(RuntimeWarning, match="returned an awaitable"):
            callback(1)
        await asyncio.sleep(0)
        assert calls == []

    @pytest.mark.parametrize("failed", [False, True])
    async def test_cancels_or_observes_a_returned_future(self, failed: bool) -> None:
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        if failed:
            future.set_exception(RuntimeError("async failure"))
        callback = synchronous_callback("on_object", lambda value: future)
        with pytest.warns(RuntimeWarning, match="returned an awaitable"):
            callback(1)
        await asyncio.sleep(0)
        if failed:
            assert not future._log_traceback
        else:
            assert future.cancelled()

    def test_passes_a_plain_return_through_silently(self) -> None:
        seen: list[object] = []
        callback = synchronous_callback("on_subscribed", seen.append)
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            callback(OK)
        assert seen == [OK]
        assert isinstance(SubscribedEvent(OK), SubscribedEvent)
