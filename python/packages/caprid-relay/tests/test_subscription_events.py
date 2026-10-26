"""Bounded iteration and callback policy over the actual session/transport path."""

import asyncio
from collections.abc import AsyncIterator
from typing import Any, assert_type, cast
from unittest.mock import Mock

import pytest
from core_helpers.fake_transport import FakeTransport, create_fake_transport, encode_subgroup
from core_helpers.streams import StreamClosedError
from core_helpers.waiting import settle, wait_for

from caprid_relay import (
    CancellationToken,
    ConnectingTransport,
    RelayError,
    RelaySubscriber,
    RelaySubscriptionHandlers,
    SubscriptionConnectionClosedError,
    SubscriptionEvents,
    SubscriptionOverflowError,
    SubscriptionRequestError,
    connect_relay_subscriber,
)
from caprid_relay._subscription import RelaySubscription, SubscriptionConsumer, SubscriptionEnd, UpdateOkEvent
from moq_core import MessageParameters, MoqtObject
from moq_core.control_messages import (
    RequestUpdate,
    encode_publish_done,
    encode_request_error,
    encode_subscribe_ok,
)


@pytest.fixture
async def connected() -> AsyncIterator[tuple[FakeTransport, RelaySubscriber]]:
    fake = create_fake_transport()
    ready = asyncio.get_running_loop().create_future()
    ready.set_result(None)
    connecting = asyncio.create_task(
        connect_relay_subscriber(
            "https://relay.example.com/moq",
            request_timeout=0.1,
            create_transport=lambda url, protocols: ConnectingTransport(transport=fake, ready=ready),
        )
    )
    fake.send_server_setup()
    connection = await connecting
    yield fake, connection
    connection.close()
    await settle()


async def test_done_trailing_objects_and_cross_stream_arrival_order(
    connected: tuple[FakeTransport, RelaySubscriber],
) -> None:
    fake, connection = connected
    async with connection.subscribe_events(["test"], "events") as events:
        request = await fake.next_request_stream()
        await request.send(encode_subscribe_ok(11))
        assert (await anext(events)).type == "subscribed"
        await request.send(encode_publish_done(0, 2))
        assert (await anext(events)).type == "publisher_done"
        for group_id in (9, 2):
            fake.open_data_stream(encode_subgroup(11, group_id, bytes([group_id])))
            event = await anext(events)
            assert event.type == "object"
            assert_type(event.value, MoqtObject)
            assert event.value.group_id == group_id
            assert (await anext(events)).type == "subgroup_end"
        pending = asyncio.create_task(anext(events))
        await settle()
        events.close()
        with pytest.raises(StopAsyncIteration):
            await pending
    assert not connection.closed.done()


async def test_subgroup_reset_is_gap_and_not_eof(connected: tuple[FakeTransport, RelaySubscriber]) -> None:
    fake, connection = connected
    async with connection.subscribe_events(["test"], "events") as events:
        request = await fake.next_request_stream()
        await request.send(encode_subscribe_ok(11))
        await anext(events)
        stream = fake.open_controlled_data_stream()
        await stream.write(encode_subgroup(11, 4, b"first"))
        assert (await anext(events)).type == "object"
        stream.reset(RuntimeError("gap"))
        assert (await anext(events)).type == "subgroup_reset"
        fake.open_data_stream(encode_subgroup(11, 5, b"next"))
        assert (await anext(events)).type == "object"


@pytest.mark.parametrize("mode", ["reject", "timeout", "drop", "close", "drop-pending", "close-pending"])
async def test_terminal_reads(connected: tuple[FakeTransport, RelaySubscriber], mode: str) -> None:
    fake, connection = connected
    events = connection.subscribe_events(["test"], "events")
    request = await fake.next_request_stream()
    if mode in ("drop", "close"):
        await request.send(encode_subscribe_ok(11))
        await anext(events)
    pending = asyncio.create_task(anext(events))
    if mode == "reject":
        await request.send(encode_request_error(1, "denied"))
    if mode in ("drop", "drop-pending"):
        fake.drop_transport()
    if mode in ("close", "close-pending"):
        connection.close()
    # A transport drop before SUBSCRIBE_OK is a closure, not a rejection, although the core fails the
    # pending request first.
    error = SubscriptionRequestError if mode in ("reject", "timeout") else SubscriptionConnectionClosedError
    with pytest.raises(error):
        await pending
    with pytest.raises(error):
        await anext(events)


@pytest.mark.parametrize("throws", [False, True])
async def test_context_cleans_up_break_and_consumer_errors(
    connected: tuple[FakeTransport, RelaySubscriber],
    throws: bool,
) -> None:
    fake, connection = connected
    events = connection.subscribe_events(["test"], "events")
    request = await fake.next_request_stream()
    await request.send(encode_subscribe_ok(11))

    async def consume() -> None:
        async with events:
            async for _event in events:
                if throws:
                    raise ValueError("consumer")
                break

    if throws:
        with pytest.raises(ValueError, match="consumer"):
            await consume()
    else:
        await consume()
    with pytest.raises(StopAsyncIteration):
        await anext(events)
    answers: list[Any] = []
    connection.subscribe(["test"], "other", RelaySubscriptionHandlers(on_subscribed=answers.append))
    await (await fake.next_request_stream()).send(encode_subscribe_ok(12))
    await wait_for(lambda: len(answers) == 1)
    assert not connection.closed.done()


async def test_cancel_pending_read_detaches_without_cancelling_connection(
    connected: tuple[FakeTransport, RelaySubscriber],
) -> None:
    _, connection = connected
    events = connection.subscribe_events(["test"], "events")
    pending = asyncio.create_task(anext(events))
    await settle()
    with pytest.raises(RuntimeError, match="one pending"):
        await anext(events)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    with pytest.raises(StopAsyncIteration):
        await anext(events)
    assert not connection.closed.done()


@pytest.mark.parametrize("already", [False, True])
async def test_cancellation_token(connected: tuple[FakeTransport, RelaySubscriber], already: bool) -> None:
    _, connection = connected
    token = CancellationToken()
    reason = ValueError("stop")
    if already:
        token.cancel(reason)
    events = connection.subscribe_events(["test"], "events", cancellation=token)
    token.cancel(reason)
    with pytest.raises(ValueError) as caught:
        await anext(events)
    assert caught.value is reason
    assert not token._callbacks
    assert not connection.closed.done()


class FakeSubscription:
    """A subscription that records its consumer, so a test can drive events into the iterator directly."""

    def __init__(self) -> None:
        self.consumer: SubscriptionConsumer | None = None
        self.cancel = Mock(side_effect=self._cancel)

    def _cancel(self, reason: object = None) -> None:
        assert self.consumer is not None
        self.consumer.ended(SubscriptionEnd(kind="cancelled", cause="cancelled", accepted=False, error=reason))

    def subscribe(self, consumer: SubscriptionConsumer) -> RelaySubscription:
        self.consumer = consumer
        return cast(RelaySubscription, self)

    def update_ok(self) -> None:
        assert self.consumer is not None
        self.consumer.event(UpdateOkEvent())


def harness(capacity: int = 2) -> tuple[SubscriptionEvents, FakeSubscription]:
    fake = FakeSubscription()
    return SubscriptionEvents(fake.subscribe, capacity=capacity), fake


async def test_slow_consumer_and_overflow_bound_all_events_and_cancel_the_subscription_once() -> None:
    events, fake = harness()
    fake.update_ok()
    fake.update_ok()
    assert (await anext(events)).type == "update_ok"
    fake.update_ok()
    for _ in range(1000):
        fake.update_ok()
    with pytest.raises(SubscriptionOverflowError):
        await anext(events)
    assert not events._queue
    fake.cancel.assert_called_once()


async def test_overflow_during_construction_still_cancels_the_subscription() -> None:
    # An eager transport can deliver events before subscribe() returns. The ending recorded then had
    # no subscription to cancel, so the iterator must cancel it once it holds it.
    fake = FakeSubscription()

    def subscribe(consumer: SubscriptionConsumer) -> RelaySubscription:
        subscription = fake.subscribe(consumer)
        for _ in range(3):
            fake.update_ok()
        return subscription

    events = SubscriptionEvents(subscribe, capacity=1)
    fake.cancel.assert_called_once()
    assert isinstance(fake.cancel.call_args.args[0], SubscriptionOverflowError)
    with pytest.raises(SubscriptionOverflowError):
        await anext(events)


@pytest.mark.parametrize("capacity", [0, -1, 1.2, float("inf"), float("nan")])
async def test_invalid_capacity(capacity: Any) -> None:
    with pytest.raises(ValueError, match="capacity must be a positive integer"):
        harness(capacity)


@pytest.mark.parametrize("capacity", ["64", None, True])
async def test_capacity_that_is_not_a_number_is_a_type_error(capacity: Any) -> None:
    with pytest.raises(TypeError, match="capacity must be a number"):
        harness(capacity)


async def test_async_handler_misuse(connected: tuple[FakeTransport, RelaySubscriber]) -> None:
    fake, connection = connected
    calls: list[int] = []

    async def on_object(obj: Any) -> None:
        calls.append(1)
        raise RuntimeError("async failure")

    with pytest.raises(TypeError, match="synchronous"):
        connection.subscribe(["test"], "events", RelaySubscriptionHandlers(on_object=cast(Any, on_object)))
    subscription = connection.subscribe(
        ["test"],
        "events",
        RelaySubscriptionHandlers(on_object=cast(Any, lambda obj: on_object(obj))),
    )
    assert subscription.request_id == 0
    request = await fake.next_request_stream()
    await request.send(encode_subscribe_ok(11))
    with pytest.warns(RuntimeWarning, match="returned an awaitable"):
        fake.open_data_stream(encode_subgroup(11, 1, b"hello"))
        await settle()
    assert calls == []


async def test_close_cancels_idle_subgroup_reader(connected: tuple[FakeTransport, RelaySubscriber]) -> None:
    fake, connection = connected
    events = connection.subscribe_events(["test"], "events")
    request = await fake.next_request_stream()
    await request.send(encode_subscribe_ok(11))
    await anext(events)
    stream = fake.open_controlled_data_stream()
    await stream.write(encode_subgroup(11, 4, b"first"))
    await anext(events)
    events.close()
    with pytest.raises(StreamClosedError, match="reader cancelled"):
        await stream.write(b"\0\0\0")
    with pytest.raises(StopAsyncIteration):
        await anext(events)
    assert not connection.closed.done()


@pytest.mark.parametrize("failed", [False, True])
async def test_returned_future_is_cancelled_or_observed(
    connected: tuple[FakeTransport, RelaySubscriber],
    failed: bool,
) -> None:
    fake, connection = connected
    future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    if failed:
        future.set_exception(RuntimeError("async failure"))
    connection.subscribe(["test"], "events", RelaySubscriptionHandlers(on_object=cast(Any, lambda obj: future)))
    request = await fake.next_request_stream()
    await request.send(encode_subscribe_ok(11))
    with pytest.warns(RuntimeWarning, match="returned an awaitable"):
        fake.open_data_stream(encode_subgroup(11, 1, b"hello"))
        await settle()
    if failed:
        assert not future._log_traceback
    else:
        assert future.cancelled()


async def test_subscribe_failure_detaches_the_cancellation_token() -> None:
    token = CancellationToken()

    def subscribe(consumer: SubscriptionConsumer) -> RelaySubscription:
        raise ValueError("bad options")

    with pytest.raises(ValueError, match="bad options"):
        SubscriptionEvents(subscribe, cancellation=token)
    assert not token._callbacks


async def test_rejection_releases_request_stream(connected: tuple[FakeTransport, RelaySubscriber]) -> None:
    fake, connection = connected
    events = connection.subscribe_events(["test"], "events")
    request = await fake.next_request_stream()
    # A later frame already buffered in the same chunk must not resurrect alias routing.
    await request.send(encode_request_error(1, "denied") + encode_subscribe_ok(11))
    with pytest.raises(SubscriptionRequestError):
        await anext(events)
    with pytest.raises(StreamClosedError, match="reader cancelled"):
        await request.send(encode_subscribe_ok(11))
    async with connection.subscribe_events(["test"], "other") as other:
        await (await fake.next_request_stream()).send(encode_subscribe_ok(11))
        assert (await anext(other)).type == "subscribed"
    assert not connection.closed.done()


@pytest.mark.parametrize("timeout_style", ["context", "wait_for"])
async def test_read_timeout_ends_subscription(
    connected: tuple[FakeTransport, RelaySubscriber],
    timeout_style: str,
) -> None:
    fake, connection = connected
    async with connection.subscribe_events(["test"], "events") as events:
        request = await fake.next_request_stream()
        await request.send(encode_subscribe_ok(11))
        await anext(events)
        with pytest.raises(TimeoutError):
            if timeout_style == "context":
                async with asyncio.timeout(0):
                    await anext(events)
            else:
                await asyncio.wait_for(anext(events), 0.001)
        with pytest.raises(StopAsyncIteration):
            await anext(events)
        with pytest.raises(StreamClosedError, match="reader cancelled"):
            await request.send(encode_publish_done(0, 0))
    assert not connection.closed.done()


@pytest.mark.parametrize("overflow", [False, True])
async def test_direct_handoff_is_outside_queue_capacity_and_survives_teardown(overflow: bool) -> None:
    events, fake = harness(capacity=1)
    pending = asyncio.create_task(anext(events))
    await settle()
    fake.update_ok()  # direct handoff
    fake.update_ok()  # exactly one queued event
    assert len(events._queue) == 1
    fake.cancel.assert_not_called()
    if overflow:
        fake.update_ok()
    else:
        events.close()
    assert (await pending).type == "update_ok"
    with pytest.raises(SubscriptionOverflowError if overflow else StopAsyncIteration):
        await anext(events)


async def test_sends_an_update_issued_before_acceptance_once_subscribe_ok_arrives(
    connected: tuple[FakeTransport, RelaySubscriber],
) -> None:
    fake, connection = connected
    subscription = connection.subscribe(["test"], "events")
    subscription.update(MessageParameters(forward=0))
    request = await fake.next_request_stream()
    await request.first_message
    assert len(request.messages) == 1
    await request.send(encode_subscribe_ok(11))
    await wait_for(lambda: len(request.messages) == 2)
    update = request.messages[1]
    assert isinstance(update, RequestUpdate)
    assert update.parameters.forward == 0
    assert update.request_id != 0


async def test_subscribed_settles_on_subscribe_ok_and_ended_on_connection_close_over_the_wire(
    connected: tuple[FakeTransport, RelaySubscriber],
) -> None:
    fake, connection = connected
    subscription = connection.subscribe(["test"], "events")
    request = await fake.next_request_stream()
    await request.send(encode_subscribe_ok(11))
    assert (await subscription.subscribed).track_alias == 11
    connection.close()
    end = await subscription.ended
    assert (end.kind, end.cause, end.accepted) == ("terminated", "connection-closed", True)


async def test_subscribed_raises_the_relay_rejection_and_ended_is_rejected_over_the_wire(
    connected: tuple[FakeTransport, RelaySubscriber],
) -> None:
    fake, connection = connected
    subscription = connection.subscribe(["test"], "events")
    request = await fake.next_request_stream()
    await request.send(encode_request_error(1, "denied"))
    with pytest.raises(SubscriptionRequestError) as failure:
        await subscription.subscribed
    assert (failure.value.request_error.error_code, failure.value.request_error.reason) == (1, "denied")
    end = await subscription.ended
    assert (end.kind, end.cause, end.accepted) == ("rejected", "request-error", False)
    assert not connection.closed.done()


async def test_handlers_are_snapshotted(connected: tuple[FakeTransport, RelaySubscriber]) -> None:
    fake, connection = connected
    original = Mock()
    replacement = Mock()
    handlers = RelaySubscriptionHandlers(on_subscribed=original)
    connection.subscribe(["test"], "events", handlers)
    handlers.on_subscribed = replacement
    await (await fake.next_request_stream()).send(encode_subscribe_ok(11))
    await wait_for(lambda: original.call_count == 1)
    replacement.assert_not_called()


@pytest.mark.parametrize("name", ["on_subscribed", "on_publisher_done"])
async def test_rejects_async_product_handlers_before_sending(
    connected: tuple[FakeTransport, RelaySubscriber],
    name: str,
) -> None:
    _, connection = connected

    async def callback(value: object) -> None:
        pass

    handlers = RelaySubscriptionHandlers()
    setattr(handlers, name, callback)
    with pytest.raises(TypeError, match="synchronous"):
        connection.subscribe(["test"], "events", handlers)
    assert connection.subscribe(["test"], "events").request_id == 0


def test_iterator_errors_have_stable_relay_codes_and_phases() -> None:
    from moq_core import RequestError

    request = RequestError(error_code=404, retry_interval=0, reason="missing")
    cause = RuntimeError("lost")
    overflow = SubscriptionOverflowError()
    rejected = SubscriptionRequestError(request)
    closed = SubscriptionConnectionClosedError(cause)
    for error, code, phase in (
        (overflow, "SUBSCRIPTION_OVERFLOW", "subscription"),
        (rejected, "SUBSCRIPTION_REQUEST_FAILED", "subscription"),
        (closed, "CLOSED", "connection"),
    ):
        assert isinstance(error, RelayError)
        assert (error.code, error.phase) == (code, phase)
    assert rejected.request_error is request
    assert closed.__cause__ is cause
