"""The subscription through its own interface: a fake core subscribe and a fake closure observer,
with recording consumers. Relay behaviour over a real connection stays in test_connect.py and
test_subscription_events.py.
"""

import asyncio
import warnings
from collections.abc import Callable
from typing import Any, cast
from unittest.mock import Mock

import pytest

from moq_core import MessageParameters, RequestError, SubscribeOk, Subscription, SubscriptionHandlers
from moq_core.control_messages import encode_request_update
from mux_relay import (
    RelayClosedError,
    RelayError,
    RelaySubscription,
    SubscriptionConnectionClosedError,
    SubscriptionEnd,
    SubscriptionRequestError,
)
from mux_relay._subscription import (
    UPDATE_QUEUE_BOUND,
    SubscribedEvent,
    SubscriptionDependencies,
    SubscriptionEvent,
    synchronous_callback,
)

REQUEST_ERROR = RequestError(error_code=404, retry_interval=0, reason="missing")
OK = cast(SubscribeOk, Mock(name="subscribe_ok"))

ClosureObserver = Callable[[BaseException | None], None]


class Recorder:
    def __init__(self) -> None:
        self.events: list[SubscriptionEvent] = []
        self.endings: list[SubscriptionEnd] = []

    def event(self, event: SubscriptionEvent) -> None:
        self.events.append(event)

    def ended(self, end: SubscriptionEnd) -> None:
        self.endings.append(end)


class Harness:
    def __init__(
        self, *, closed_already: bool = False, closed_error: BaseException | None = None, request_id: int = 7
    ) -> None:
        self.handlers: SubscriptionHandlers | None = None
        self.core = Mock(spec=Subscription)
        self.core.request_id = request_id
        self.closed_already = closed_already
        self.closed_error = closed_error
        self.observers: list[ClosureObserver] = []
        self.subscribe = Mock(side_effect=self._subscribe)
        self.deps = SubscriptionDependencies(subscribe=self.subscribe, observe_closed=self.observe_closed)

    def _subscribe(self, handlers: SubscriptionHandlers) -> Subscription:
        self.handlers = handlers
        return cast(Subscription, self.core)

    def observe_closed(self, observer: ClosureObserver) -> Callable[[], None]:
        if self.closed_already:
            observer(self.closed_error)
            return lambda: None
        self.observers.append(observer)

        def remove() -> None:
            if observer in self.observers:
                self.observers.remove(observer)

        return remove

    def core_handlers(self) -> SubscriptionHandlers:
        assert self.handlers is not None, "core subscribe was not called"
        return self.handlers

    def accept(self) -> None:
        on_ok = self.core_handlers().on_ok
        assert on_ok is not None
        on_ok(OK)

    def reject(self, error: RequestError = REQUEST_ERROR) -> None:
        on_error = self.core_handlers().on_error
        assert on_error is not None
        on_error(error)

    def close_connection(self, error: BaseException | None = None) -> None:
        """The lifetime tells its observers in the turn the connection ends."""
        observers, self.observers = list(self.observers), []
        for observer in observers:
            observer(error)


async def turn_ends() -> None:
    """A pending request failure is recorded when the turn that delivered it ends."""
    await asyncio.sleep(0)


def types(events: list[SubscriptionEvent]) -> list[str]:
    return [event.type for event in events]


def rejected(error: RequestError = REQUEST_ERROR) -> SubscriptionEnd:
    return SubscriptionEnd(kind="rejected", cause="request-error", accepted=False, error=error)


class TestTranslatingTheCoreOnce:
    async def test_delivers_each_core_callback_as_one_product_event_to_every_consumer_and_tracks_acceptance(
        self,
    ) -> None:
        h = Harness()
        first = Recorder()
        second = Recorder()
        subscription = RelaySubscription(h.deps, [first])
        subscription._attach(second)
        assert subscription.request_id == 7
        assert not subscription._accepted
        core = h.core_handlers()
        assert core.on_ok and core.on_object and core.on_done and core.on_subgroup_end and core.on_subgroup_reset
        assert core.on_state_notify and core.on_goaway and core.on_update_ok
        core.on_ok(OK)
        assert subscription._accepted
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
        assert subscription._end is None

    async def test_forwards_update_to_the_core_handle_once_the_subscription_is_accepted(self) -> None:
        h = Harness()
        subscription = RelaySubscription(h.deps)
        h.accept()
        parameters = MessageParameters(forward=0)
        subscription.update(parameters)
        h.core.update.assert_called_once_with(parameters)

    async def test_stops_delivering_to_a_detached_consumer(self) -> None:
        h = Harness()
        first = Recorder()
        subscription = RelaySubscription(h.deps)
        detach = subscription._attach(first)
        update_ok = h.core_handlers().on_update_ok
        assert update_ok is not None
        update_ok()
        detach()
        detach()
        update_ok()
        assert len(first.events) == 1

    async def test_is_a_core_subscription_for_callers_that_check(self) -> None:
        h = Harness()
        assert isinstance(RelaySubscription(h.deps), Subscription)


class TestEndings:
    async def test_records_a_request_error_after_acceptance_at_once_cancels_the_core_and_drops_later_events(
        self,
    ) -> None:
        h = Harness()
        r = Recorder()
        subscription = RelaySubscription(h.deps, [r])
        core = h.core_handlers()
        assert core.on_update_ok
        h.accept()
        h.reject()
        assert r.endings == [
            SubscriptionEnd(kind="terminated", cause="request-error", accepted=True, error=REQUEST_ERROR)
        ]
        assert subscription._end == r.endings[0]
        h.core.cancel.assert_called_once_with(REQUEST_ERROR)
        assert h.observers == []
        core.on_update_ok()
        h.reject()
        assert types(r.events) == ["subscribed"]
        assert len(r.endings) == 1

    async def test_records_a_request_error_before_acceptance_when_the_turn_ends_as_rejected(self) -> None:
        h = Harness()
        r = Recorder()
        subscription = RelaySubscription(h.deps, [r])
        core = h.core_handlers()
        assert core.on_update_ok
        h.reject()
        assert r.endings == []
        await turn_ends()
        assert r.endings == [rejected()]
        assert subscription._end == r.endings[0]
        h.core.cancel.assert_called_once_with(REQUEST_ERROR)
        assert h.observers == []
        core.on_update_ok()
        h.reject()
        await turn_ends()
        assert r.events == []
        assert len(r.endings) == 1

    async def test_lets_a_closure_reported_in_the_same_turn_as_a_pending_request_failure_win(self) -> None:
        h = Harness()
        r = Recorder()
        subscription = RelaySubscription(h.deps, [r])
        lost = RuntimeError("transport dropped")
        h.reject(RequestError(error_code=0, retry_interval=0, reason="session closed before the request was answered"))
        h.close_connection(lost)
        assert r.endings == [SubscriptionEnd(kind="terminated", cause="connection-closed", accepted=False, error=lost)]
        await turn_ends()
        assert len(r.endings) == 1
        h.core.cancel.assert_called_once_with(lost)
        with pytest.raises(SubscriptionConnectionClosedError) as failure:
            await subscription.subscribed
        assert failure.value.__cause__ is lost
        assert (await subscription.ended).kind == "terminated"

    @pytest.mark.parametrize("accepted", [False, True])
    async def test_records_connection_closure_with_the_connection_error_and_cancels_the_core_with_it(
        self, accepted: bool
    ) -> None:
        h = Harness()
        r = Recorder()
        RelaySubscription(h.deps, [r])
        if accepted:
            h.accept()
        error = RuntimeError("lost")
        h.close_connection(error)
        assert r.endings == [
            SubscriptionEnd(kind="terminated", cause="connection-closed", accepted=accepted, error=error)
        ]
        h.core.cancel.assert_called_once_with(error)

    async def test_records_a_local_cancel_once_idempotently_and_tells_consumers_attached_afterwards(self) -> None:
        h = Harness()
        r = Recorder()
        late = Recorder()
        subscription = RelaySubscription(h.deps, [r])
        h.accept()
        reason = RuntimeError("done with it")
        subscription.cancel(reason)
        subscription.cancel(RuntimeError("again"))
        assert r.endings == [SubscriptionEnd(kind="cancelled", cause="cancelled", accepted=True, error=reason)]
        h.core.cancel.assert_called_once_with(reason)
        assert h.observers == []
        subscription._attach(late)
        assert late.endings == r.endings

    @pytest.mark.parametrize("error", [RuntimeError("gone"), None])
    async def test_ends_before_sending_when_the_connection_had_already_closed_and_refuses_the_handle(
        self, error: BaseException | None
    ) -> None:
        h = Harness(closed_already=True, closed_error=error)
        r = Recorder()
        subscription = RelaySubscription(h.deps, [r])
        h.subscribe.assert_not_called()
        assert r.endings == [SubscriptionEnd(kind="terminated", cause="connection-closed", accepted=False, error=error)]
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
            RelaySubscription(h.deps, [r])
        assert h.observers == []
        assert r.endings == [SubscriptionEnd(kind="cancelled", cause="cancelled", accepted=False, error=failure)]

    async def test_cancels_the_core_handle_when_the_source_fails_the_request_during_subscribe_once_the_turn_ends(
        self,
    ) -> None:
        core = Mock(spec=Subscription)
        core.request_id = 1

        def subscribe(handlers: SubscriptionHandlers) -> Subscription:
            assert handlers.on_error is not None
            handlers.on_error(REQUEST_ERROR)
            return cast(Subscription, core)

        r = Recorder()
        subscription = RelaySubscription(
            SubscriptionDependencies(subscribe=subscribe, observe_closed=lambda observer: lambda: None), [r]
        )
        assert subscription.request_id == 1
        await turn_ends()
        assert r.endings == [rejected()]
        core.cancel.assert_called_once_with(REQUEST_ERROR)


class TestReadinessAndTermination:
    async def test_subscribed_settles_on_subscribe_ok_and_ended_stays_pending_through_publisher_completion(
        self,
    ) -> None:
        h = Harness()
        subscription = RelaySubscription(h.deps)
        h.accept()
        assert await subscription.subscribed is OK
        on_done = h.core_handlers().on_done
        assert on_done is not None
        on_done(cast(Any, Mock(name="done")))
        await asyncio.sleep(0)
        assert not subscription.ended.done()
        subscription.cancel(RuntimeError("done"))
        end = await subscription.ended
        assert (end.kind, end.cause, end.accepted) == ("cancelled", "cancelled", True)

    async def test_subscribed_raises_subscription_request_error_and_ended_is_rejected_on_a_request_error(
        self,
    ) -> None:
        h = Harness()
        subscription = RelaySubscription(h.deps)
        h.reject()
        with pytest.raises(SubscriptionRequestError) as failure:
            await subscription.subscribed
        assert failure.value.request_error is REQUEST_ERROR
        assert await subscription.ended == rejected()

    async def test_ended_is_terminated_on_a_request_error_after_acceptance_with_subscribed_already_settled(
        self,
    ) -> None:
        h = Harness()
        subscription = RelaySubscription(h.deps)
        h.accept()
        h.reject()
        assert await subscription.subscribed is OK
        end = await subscription.ended
        assert (end.kind, end.cause, end.accepted) == ("terminated", "request-error", True)

    @pytest.mark.parametrize("accepted", [False, True])
    async def test_ended_is_terminated_when_the_connection_closes_and_subscribed_raises_only_before_acceptance(
        self, accepted: bool
    ) -> None:
        h = Harness()
        subscription = RelaySubscription(h.deps)
        if accepted:
            h.accept()
        error = RuntimeError("lost")
        h.close_connection(error)
        if accepted:
            assert await subscription.subscribed is OK
        else:
            with pytest.raises(SubscriptionConnectionClosedError) as failure:
                await subscription.subscribed
            assert failure.value.__cause__ is error
        assert await subscription.ended == SubscriptionEnd(
            kind="terminated", cause="connection-closed", accepted=accepted, error=error
        )

    async def test_subscribed_raises_the_cancel_reason_or_closed_when_none_was_given(self) -> None:
        with_reason = RelaySubscription(Harness().deps)
        reason = RuntimeError("changed my mind")
        with_reason.cancel(reason)
        with pytest.raises(RuntimeError) as failure:
            await with_reason.subscribed
        assert failure.value is reason
        assert await with_reason.ended == SubscriptionEnd(
            kind="cancelled", cause="cancelled", accepted=False, error=reason
        )
        bare = RelaySubscription(Harness().deps)
        bare.cancel()
        with pytest.raises(RelayClosedError) as closed:
            await bare.subscribed
        assert closed.value.phase == "subscription"

    async def test_settles_each_awaitable_once_and_observes_a_readiness_failure_nobody_awaits(self) -> None:
        h = Harness()
        subscription = RelaySubscription(h.deps)
        h.reject()
        # No await on `subscribed` here: an unretrieved exception would be reported at teardown.
        await turn_ends()
        subscription.cancel(RuntimeError("too late"))
        assert (await subscription.ended).kind == "rejected"
        assert isinstance(subscription.subscribed.exception(), SubscriptionRequestError)

    async def test_settles_subscribed_before_flushing_queued_updates_so_a_flush_failure_ends_loudly(self) -> None:
        h = Harness()
        r = Recorder()
        subscription = RelaySubscription(h.deps, [r])
        subscription.update(MessageParameters(forward=0))
        fault = RuntimeError("core refused the update")
        h.core.update.side_effect = fault
        h.accept()
        assert await subscription.subscribed is OK
        assert await subscription.ended == SubscriptionEnd(
            kind="cancelled", cause="cancelled", accepted=True, error=fault
        )
        assert r.events == []
        h.core.cancel.assert_called_once_with(fault)


class TestUpdatesBeforeAcceptance:
    async def test_queues_updates_issued_before_subscribe_ok_sends_them_in_order_once_accepted_then_forwards(
        self,
    ) -> None:
        h = Harness()
        subscription = RelaySubscription(h.deps)
        first = MessageParameters(forward=0)
        second = MessageParameters(forward=1)
        subscription.update(first)
        subscription.update(second)
        h.core.update.assert_not_called()
        h.accept()
        assert [call.args[0] for call in h.core.update.call_args_list] == [first, second]
        subscription.update(MessageParameters(subscriber_priority=3))
        assert h.core.update.call_count == 3

    async def test_refuses_a_further_queued_update_with_update_queue_full_once_the_bound_is_reached(self) -> None:
        h = Harness()
        subscription = RelaySubscription(h.deps)
        for priority in range(UPDATE_QUEUE_BOUND):
            subscription.update(MessageParameters(subscriber_priority=priority))
        with pytest.raises(RelayError) as failure:
            subscription.update(MessageParameters(forward=0))
        assert (failure.value.code, failure.value.phase) == ("UPDATE_QUEUE_FULL", "subscription")
        h.accept()
        assert h.core.update.call_count == UPDATE_QUEUE_BOUND

    async def test_rejects_a_malformed_update_synchronously_and_leaves_the_subscription_and_its_queue_intact(
        self,
    ) -> None:
        h = Harness()
        r = Recorder()
        subscription = RelaySubscription(h.deps, [r])
        subscription.update(MessageParameters(forward=0))
        with pytest.raises(ValueError, match="out of range"):
            subscription.update(MessageParameters(expires=-1))
        h.accept()
        assert [call.args[0] for call in h.core.update.call_args_list] == [MessageParameters(forward=0)]
        assert types(r.events) == ["subscribed"]
        assert subscription._end is None

    async def test_validates_a_queued_update_with_its_own_request_id_so_an_unframeable_update_is_refused_now(
        self,
    ) -> None:
        def fits(request_id: int, parameters: MessageParameters) -> bool:
            try:
                encode_request_update(request_id, parameters)
            except Exception:
                return False
            return True

        # A two-byte request id can push a message over the control-message frame limit.
        parameters: MessageParameters | None = None
        for length in range(0xFFFF, 0xFFFF - 64, -1):
            candidate = MessageParameters(authorization_tokens=[bytes(length)])
            if fits(7, candidate) and not fits(128, candidate):
                parameters = candidate
                break
        assert parameters is not None, "no token length sits on the frame boundary"
        wide = RelaySubscription(Harness(request_id=128).deps)
        with pytest.raises(Exception, match="control message exceeds"):
            wide.update(parameters)
        narrow = RelaySubscription(Harness(request_id=7).deps)
        narrow.update(parameters)

    async def test_queues_a_snapshot_so_mutating_the_caller_object_afterwards_does_not_change_what_is_sent(
        self,
    ) -> None:
        h = Harness()
        subscription = RelaySubscription(h.deps)
        parameters = MessageParameters(forward=0, authorization_tokens=[b"\x01"])
        subscription.update(parameters)
        parameters.forward = 1
        parameters.authorization_tokens = [b"\x09"]
        h.accept()
        h.core.update.assert_called_once_with(MessageParameters(forward=0, authorization_tokens=[b"\x01"]))

    async def test_discards_queued_updates_when_the_subscription_ends_before_acceptance(self) -> None:
        h = Harness()
        subscription = RelaySubscription(h.deps)
        subscription.update(MessageParameters(forward=0))
        h.reject()
        await turn_ends()
        assert subscription._end == rejected()
        h.core.update.assert_not_called()

    async def test_raises_closed_for_an_update_after_the_subscription_ended(self) -> None:
        h = Harness()
        subscription = RelaySubscription(h.deps)
        h.accept()
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
