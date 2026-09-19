"""Bounded iteration and callback policy over the actual session/transport path."""

import asyncio
from collections.abc import AsyncIterator
from typing import Any, assert_type, cast
from unittest.mock import Mock

import pytest
from core_helpers.fake_transport import FakeTransport, create_fake_transport, encode_subgroup
from core_helpers.streams import StreamClosedError
from core_helpers.waiting import settle, wait_for

from moq_core import Closed, MoqtObject, Subscription, SubscriptionHandlers
from moq_core.control_messages import encode_publish_done, encode_request_error, encode_subscribe_ok
from mux_relay import (
    CancellationToken,
    ConnectingTransport,
    RelayConnection,
    SubscriptionConnectionClosedError,
    SubscriptionEvents,
    SubscriptionOverflowError,
    SubscriptionRequestError,
    connect_relay,
)


@pytest.fixture
async def connected() -> AsyncIterator[tuple[FakeTransport, RelayConnection]]:
    fake = create_fake_transport()
    ready = asyncio.get_running_loop().create_future()
    ready.set_result(None)
    connecting = asyncio.create_task(
        connect_relay(
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
    connected: tuple[FakeTransport, RelayConnection],
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


async def test_subgroup_reset_is_gap_and_not_eof(connected: tuple[FakeTransport, RelayConnection]) -> None:
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
async def test_terminal_reads(connected: tuple[FakeTransport, RelayConnection], mode: str) -> None:
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
    error = (
        SubscriptionRequestError if mode in ("reject", "timeout", "drop-pending") else SubscriptionConnectionClosedError
    )
    with pytest.raises(error):
        await pending
    with pytest.raises(error):
        await anext(events)


@pytest.mark.parametrize("throws", [False, True])
async def test_context_cleans_up_break_and_consumer_errors(
    connected: tuple[FakeTransport, RelayConnection],
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
    connection.subscribe(["test"], "other", SubscriptionHandlers(on_ok=answers.append))
    await (await fake.next_request_stream()).send(encode_subscribe_ok(12))
    await wait_for(lambda: len(answers) == 1)
    assert not connection.closed.done()


async def test_cancel_pending_read_detaches_without_cancelling_connection(
    connected: tuple[FakeTransport, RelayConnection],
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
async def test_cancellation_token(connected: tuple[FakeTransport, RelayConnection], already: bool) -> None:
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


def harness(capacity: int = 2) -> tuple[SubscriptionEvents, SubscriptionHandlers, Mock, asyncio.Future[Closed]]:
    handlers = SubscriptionHandlers()
    subscription = Mock(spec=Subscription)
    closed: asyncio.Future[Closed] = asyncio.get_running_loop().create_future()

    def subscribe(value: SubscriptionHandlers) -> Subscription:
        nonlocal handlers
        handlers = value
        return cast(Subscription, subscription)

    events = SubscriptionEvents(subscribe, closed, capacity=capacity)
    return events, handlers, subscription, closed


async def test_slow_consumer_and_overflow_bound_all_events_and_remove_observers() -> None:
    events, handlers, subscription, closed = harness()
    assert handlers.on_update_ok is not None
    handlers.on_update_ok()
    handlers.on_update_ok()
    assert (await anext(events)).type == "update_ok"
    handlers.on_update_ok()
    for _ in range(1000):
        handlers.on_update_ok()
    with pytest.raises(SubscriptionOverflowError):
        await anext(events)
    assert not events._queue
    subscription.cancel.assert_called_once()
    assert not closed._callbacks
    closed.set_result(Closed())
    await settle()


@pytest.mark.parametrize("capacity", [0, -1, 1.2, True, float("inf")])
async def test_invalid_capacity(capacity: Any) -> None:
    with pytest.raises(ValueError, match="capacity"):
        harness(capacity)


async def test_async_handler_misuse(connected: tuple[FakeTransport, RelayConnection]) -> None:
    fake, connection = connected
    calls: list[int] = []

    async def on_object(obj: Any) -> None:
        calls.append(1)
        raise RuntimeError("async failure")

    with pytest.raises(TypeError, match="synchronous"):
        connection.subscribe(["test"], "events", SubscriptionHandlers(on_object=cast(Any, on_object)))
    subscription = connection.subscribe(
        ["test"],
        "events",
        SubscriptionHandlers(on_object=cast(Any, lambda obj: on_object(obj))),
    )
    assert subscription.request_id == 0
    request = await fake.next_request_stream()
    await request.send(encode_subscribe_ok(11))
    with pytest.warns(RuntimeWarning, match="returned an awaitable"):
        fake.open_data_stream(encode_subgroup(11, 1, b"hello"))
        await settle()
    assert calls == []


async def test_close_cancels_idle_subgroup_reader(connected: tuple[FakeTransport, RelayConnection]) -> None:
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
    connected: tuple[FakeTransport, RelayConnection],
    failed: bool,
) -> None:
    fake, connection = connected
    future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    if failed:
        future.set_exception(RuntimeError("async failure"))
    connection.subscribe(["test"], "events", SubscriptionHandlers(on_object=cast(Any, lambda obj: future)))
    request = await fake.next_request_stream()
    await request.send(encode_subscribe_ok(11))
    with pytest.warns(RuntimeWarning, match="returned an awaitable"):
        fake.open_data_stream(encode_subgroup(11, 1, b"hello"))
        await settle()
    if failed:
        assert not future._log_traceback
    else:
        assert future.cancelled()


async def test_subscribe_failure_detaches_observers() -> None:
    closed: asyncio.Future[Closed] = asyncio.get_running_loop().create_future()
    token = CancellationToken()

    def subscribe(handlers: SubscriptionHandlers) -> Subscription:
        raise ValueError("bad options")

    with pytest.raises(ValueError, match="bad options"):
        SubscriptionEvents(subscribe, closed, cancellation=token)
    assert not closed._callbacks
    assert not token._callbacks


async def test_rejection_releases_request_stream(connected: tuple[FakeTransport, RelayConnection]) -> None:
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
    connected: tuple[FakeTransport, RelayConnection],
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
    events, handlers, subscription, _ = harness(capacity=1)
    pending = asyncio.create_task(anext(events))
    await settle()
    assert handlers.on_update_ok is not None
    handlers.on_update_ok()  # direct handoff
    handlers.on_update_ok()  # exactly one queued event
    assert len(events._queue) == 1
    subscription.cancel.assert_not_called()
    if overflow:
        handlers.on_update_ok()
    else:
        events.close()
    assert (await pending).type == "update_ok"
    with pytest.raises(SubscriptionOverflowError if overflow else StopAsyncIteration):
        await anext(events)


async def test_handlers_are_snapshotted(connected: tuple[FakeTransport, RelayConnection]) -> None:
    fake, connection = connected
    original = Mock()
    replacement = Mock()
    handlers = SubscriptionHandlers(on_ok=original)
    connection.subscribe(["test"], "events", handlers)
    handlers.on_ok = replacement
    await (await fake.next_request_stream()).send(encode_subscribe_ok(11))
    await wait_for(lambda: original.call_count == 1)
    replacement.assert_not_called()
