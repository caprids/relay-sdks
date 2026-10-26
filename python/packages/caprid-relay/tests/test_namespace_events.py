"""Namespace discovery through ``RelaySubscriber.namespace_events``, over the actual session path.

Mirrors ``ts/tests/namespace-events.test.ts``.
"""

import asyncio
from collections.abc import AsyncIterator, Callable, Sequence

import pytest
from core_helpers.fake_transport import FakeTransport, create_fake_transport
from core_helpers.waiting import settle

from caprid_relay import (
    CancellationToken,
    ConnectingTransport,
    NamespaceEvent,
    RelayError,
    RelaySubscriber,
    SubscriptionConnectionClosedError,
    SubscriptionOverflowError,
    SubscriptionRequestError,
    connect_relay_publisher,
    connect_relay_subscriber,
)
from moq_core.bytes import StreamReader
from moq_core.control_messages import (
    REQUEST_ERROR_CODE,
    SETUP_OPTION,
    ControlMessageFrame,
    KeyValuePair,
    Setup,
    SubscribeNamespace,
    decode_control_message,
    encode_namespace,
    encode_namespace_done,
    encode_request_error,
    encode_request_ok,
)


def _factory(fake: FakeTransport) -> Callable[[str, Sequence[str]], ConnectingTransport]:
    ready: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    ready.set_result(None)
    return lambda url, protocols: ConnectingTransport(transport=fake, ready=ready)


async def _client_setup_options(fake: FakeTransport) -> list[KeyValuePair]:
    reader: StreamReader = await fake.client_control_stream()
    stream_type = await reader.read_varint()
    high = await reader.read_uint8()
    low = await reader.read_uint8()
    message = decode_control_message(ControlMessageFrame(stream_type, await reader.read_bytes(high * 256 + low)))
    assert isinstance(message, Setup)
    return list(message.options)


@pytest.fixture
async def connected() -> AsyncIterator[tuple[FakeTransport, RelaySubscriber]]:
    fake = create_fake_transport()
    connecting = asyncio.create_task(
        connect_relay_subscriber("https://relay.example.com/moq", request_timeout=0.1, create_transport=_factory(fake))
    )
    fake.send_server_setup()
    connection = await connecting
    yield fake, connection
    connection.close()
    await settle()


async def test_asks_the_relay_to_advertise_namespaces_only_when_solicited(
    connected: tuple[FakeTransport, RelaySubscriber],
) -> None:
    fake, _ = connected
    options = await asyncio.wait_for(_client_setup_options(fake), 1)
    assert [option for option in options if option.type == SETUP_OPTION.SOLICIT] == [
        KeyValuePair(SETUP_OPTION.SOLICIT, 1)
    ]


async def test_leaves_the_publisher_setup_without_solicit() -> None:
    fake = create_fake_transport()
    owner = CancellationToken()
    connecting = asyncio.ensure_future(
        connect_relay_publisher(
            "https://relay.example.com/moq", namespace=["room"], cancellation=owner, create_transport=_factory(fake)
        )
    )
    try:
        options = await asyncio.wait_for(_client_setup_options(fake), 1)
    finally:
        owner.cancel()
        await asyncio.wait({connecting})
    assert not any(option.type == SETUP_OPTION.SOLICIT for option in options)


async def test_sends_subscribe_namespace_for_the_prefix_and_yields_full_namespaces_edge_triggered(
    connected: tuple[FakeTransport, RelaySubscriber],
) -> None:
    fake, connection = connected
    async with connection.namespace_events(["room", "a/b"]) as events:
        request = await fake.next_request_stream()
        message = await request.first_message
        assert isinstance(message, SubscribeNamespace)
        assert message.track_namespace_prefix == ["room", "a/b"]
        await request.send(encode_request_ok())
        await request.send(encode_namespace(["alice"]))
        await request.send(encode_namespace(["alice"]))  # a relay repeat (route change) is not passed on
        await request.send(encode_namespace([]))
        await request.send(encode_namespace_done(["alice"]))
        await request.send(encode_namespace(["alice"]))

        seen = [await anext(events) for _ in range(4)]
        assert seen == [
            NamespaceEvent("announced", ("room", "a/b", "alice")),
            NamespaceEvent("announced", ("room", "a/b")),
            NamespaceEvent("unannounced", ("room", "a/b", "alice")),
            NamespaceEvent("announced", ("room", "a/b", "alice")),
        ]
        events.close()
        with pytest.raises(StopAsyncIteration):
            await anext(events)


async def test_checks_the_prefix_before_connection_state_and_before_sending_anything(
    connected: tuple[FakeTransport, RelaySubscriber],
) -> None:
    fake, connection = connected
    with pytest.raises(TypeError):
        connection.namespace_events([])
    with pytest.raises(TypeError):
        connection.namespace_events(["ok", ""])
    with pytest.raises(TypeError):
        connection.namespace_events("room")
    with pytest.raises(ValueError, match="positive integer"):
        connection.namespace_events(["room"], capacity=0)
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(fake.next_request_stream(), 0.05)
    connection.close()
    with pytest.raises(TypeError):
        connection.namespace_events([])
    with pytest.raises(RelayError) as closed:
        connection.namespace_events(["room"])
    assert closed.value.code == "CLOSED"


async def test_ends_with_subscription_request_error_when_the_relay_rejects_the_request(
    connected: tuple[FakeTransport, RelaySubscriber],
) -> None:
    fake, connection = connected
    async with connection.namespace_events(["room"]) as events:
        request = await fake.next_request_stream()
        await request.first_message
        await request.send(encode_request_error(REQUEST_ERROR_CODE.UNAUTHORIZED, "not yours"))
        with pytest.raises(SubscriptionRequestError) as failure:
            await anext(events)
    error = failure.value.request_error
    assert (error.error_code, error.reason) == (REQUEST_ERROR_CODE.UNAUTHORIZED, "not yours")


async def test_ends_with_subscription_request_error_when_the_stream_resets_after_acceptance(
    connected: tuple[FakeTransport, RelaySubscriber],
) -> None:
    fake, connection = connected
    async with connection.namespace_events(["room"]) as events:
        request = await fake.next_request_stream()
        await request.first_message
        await request.send(encode_request_ok())
        request.reset(RuntimeError("gone"))
        with pytest.raises(SubscriptionRequestError) as failure:
            await anext(events)
    assert failure.value.request_error.error_code == REQUEST_ERROR_CODE.INTERNAL_ERROR
    assert failure.value.request_error.retry_interval == 0


async def test_withdraws_what_is_still_announced_then_ends_cleanly_when_the_relay_finishes_the_request(
    connected: tuple[FakeTransport, RelaySubscriber],
) -> None:
    fake, connection = connected
    async with connection.namespace_events(["room"]) as events:
        request = await fake.next_request_stream()
        await request.first_message
        await request.send(encode_request_ok())
        await request.send(encode_namespace(["alice"]))
        await request.fin()
        # Nothing was read before the end: the queued events survive it.
        await settle()
        assert await anext(events) == NamespaceEvent("announced", ("room", "alice"))
        assert await anext(events) == NamespaceEvent("unannounced", ("room", "alice"))
        with pytest.raises(StopAsyncIteration):
            await anext(events)
        assert await asyncio.wait_for(request.client_end, 1) == "fin"


async def test_ends_with_subscription_connection_closed_error_when_the_connection_drops(
    connected: tuple[FakeTransport, RelaySubscriber],
) -> None:
    fake, connection = connected
    async with connection.namespace_events(["room"]) as events:
        request = await fake.next_request_stream()
        await request.first_message
        await request.send(encode_request_ok())
        read = asyncio.ensure_future(anext(events))
        await settle()
        fake.drop_transport()
        with pytest.raises(SubscriptionConnectionClosedError):
            await read


async def test_ends_with_subscription_overflow_error_past_its_capacity_and_cancels_the_request(
    connected: tuple[FakeTransport, RelaySubscriber],
) -> None:
    fake, connection = connected
    async with connection.namespace_events(["room"], capacity=1) as events:
        request = await fake.next_request_stream()
        await request.first_message
        await request.send(encode_request_ok())
        await request.send(encode_namespace(["a"]))
        await request.send(encode_namespace(["b"]))
        assert await asyncio.wait_for(request.client_end, 1) == "reset"
        with pytest.raises(SubscriptionOverflowError):
            await anext(events)


async def test_cancels_the_request_on_cancellation_and_on_leaving_async_with(
    connected: tuple[FakeTransport, RelaySubscriber],
) -> None:
    fake, connection = connected
    owner = CancellationToken()
    events = connection.namespace_events(["room"], cancellation=owner)
    request = await fake.next_request_stream()
    await request.first_message
    owner.cancel(RuntimeError("done here"))
    with pytest.raises(RuntimeError, match="done here"):
        await anext(events)
    assert await asyncio.wait_for(request.client_end, 1) == "reset"

    async with connection.namespace_events(["room"]) as second:
        second_request = await fake.next_request_stream()
        await second_request.first_message
        await second_request.send(encode_request_ok())
        await second_request.send(encode_namespace(["alice"]))
        async for event in second:
            assert event.namespace == ("room", "alice")
            break
    with pytest.raises(StopAsyncIteration):
        await anext(second)
    assert await asyncio.wait_for(second_request.client_end, 1) == "reset"


async def test_an_already_cancelled_token_sends_nothing(connected: tuple[FakeTransport, RelaySubscriber]) -> None:
    fake, connection = connected
    owner = CancellationToken()
    owner.cancel(RuntimeError("never"))
    events = connection.namespace_events(["room"], cancellation=owner)
    with pytest.raises(RuntimeError, match="never"):
        await anext(events)
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(fake.next_request_stream(), 0.05)


async def test_raises_when_the_core_cannot_encode_the_prefix_and_leaves_the_connection_usable(
    connected: tuple[FakeTransport, RelaySubscriber],
) -> None:
    fake, connection = connected
    with pytest.raises(Exception):  # noqa: B017 - whichever encoding error the core raises
        connection.namespace_events(["x" * 70_000])
    async with connection.namespace_events(["room"]):
        request = await fake.next_request_stream()
        message = await request.first_message
        assert isinstance(message, SubscribeNamespace)
        assert message.track_namespace_prefix == ["room"]


async def test_refuses_a_prefix_overlapping_one_this_connection_is_discovering_until_that_one_ends(
    connected: tuple[FakeTransport, RelaySubscriber],
) -> None:
    fake, connection = connected
    first = connection.namespace_events(["room", "a"])
    await fake.next_request_stream()
    for prefix in (["room"], ["room", "a"], ["room", "a", "b"]):
        with pytest.raises(RelayError) as refused:
            connection.namespace_events(prefix)
        assert (refused.value.code, refused.value.phase) == ("PREFIX_OVERLAP", "subscription")
    # A sibling is no overlap: fields match whole.
    sibling = connection.namespace_events(["room", "ab"])
    first.close()
    sibling.close()
    again = connection.namespace_events(["room"])
    again.close()


async def test_releases_the_prefix_when_the_request_ends_or_the_connection_closes(
    connected: tuple[FakeTransport, RelaySubscriber],
) -> None:
    fake, connection = connected
    events = connection.namespace_events(["room"])
    request = await fake.next_request_stream()
    await request.first_message
    await request.send(encode_request_error(REQUEST_ERROR_CODE.UNAUTHORIZED, "no"))
    with pytest.raises(SubscriptionRequestError):
        await anext(events)
    # The ended request no longer holds the prefix.
    held = connection.namespace_events(["room"])
    await fake.next_request_stream()
    fake.drop_transport()
    with pytest.raises(SubscriptionConnectionClosedError):
        await anext(held)
    assert connection._prefixes == set()


async def test_close_after_a_clean_end_discards_what_is_queued(
    connected: tuple[FakeTransport, RelaySubscriber],
) -> None:
    fake, connection = connected
    events = connection.namespace_events(["room"])
    request = await fake.next_request_stream()
    await request.first_message
    await request.send(encode_request_ok())
    await request.send(encode_namespace(["alice"]))
    await request.fin()
    await settle()
    events.close()
    with pytest.raises(StopAsyncIteration):
        await anext(events)
