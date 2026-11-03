"""Namespace discovery through ``RelaySubscriber.namespace_events``, over the actual session path.

Mirrors ``ts/tests/namespace-events.test.ts``.
"""

import asyncio
from collections.abc import AsyncIterator, Callable, Sequence

import pytest
from core_helpers.fake_transport import FakeTransport, create_fake_transport
from core_helpers.waiting import settle

from caprid_relay import (
    SNAPSHOT_LIMIT,
    SNAPSHOT_QUIET,
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
    namespace_events,
)
from caprid_relay.namespace_events import _is_stopped, _is_withdrawn_by_end
from moq_core.bytes import StreamReader
from moq_core.control_messages import (
    REQUEST_ERROR_CODE,
    SETUP_OPTION,
    ControlMessageFrame,
    KeyValuePair,
    MessageParameters,
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


async def _connect(
    *, request_timeout: float = 0.1, server_setup: Sequence[KeyValuePair] = ()
) -> tuple[FakeTransport, RelaySubscriber]:
    fake = create_fake_transport()
    connecting = asyncio.create_task(
        connect_relay_subscriber(
            "https://relay.example.com/moq", request_timeout=request_timeout, create_transport=_factory(fake)
        )
    )
    fake.send_server_setup(server_setup)
    return fake, await connecting


@pytest.fixture
async def connected() -> AsyncIterator[tuple[FakeTransport, RelaySubscriber]]:
    fake, connection = await _connect()
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


async def test_withdraws_nothing_when_the_request_stream_fails_the_error_ends_it(
    connected: tuple[FakeTransport, RelaySubscriber],
) -> None:
    fake, connection = connected
    async with connection.namespace_events(["room"]) as events:
        request = await fake.next_request_stream()
        await request.first_message
        await request.send(encode_request_ok())
        await request.send(encode_namespace(["alice"]))
        assert await anext(events) == NamespaceEvent("announced", ("room", "alice"))
        request.reset(RuntimeError("gone"))
        with pytest.raises(SubscriptionRequestError):
            await anext(events)


LOADED = NamespaceEvent("loaded")
# asyncio runs a timer up to one clock resolution early.
EARLY = 0.005


def _announced(*namespace: str) -> NamespaceEvent:
    return NamespaceEvent("announced", namespace)


class TestLoadedWithoutActiveCount:
    async def test_yields_loaded_after_the_snapshot_once_the_stream_stays_quiet_and_once_only(
        self, connected: tuple[FakeTransport, RelaySubscriber]
    ) -> None:
        fake, connection = connected
        loop = asyncio.get_running_loop()
        async with connection.namespace_events(["room"]) as events:
            request = await fake.next_request_stream()
            await request.first_message
            await request.send(encode_request_ok())
            await request.send(encode_namespace(["alice"]))
            await request.send(encode_namespace(["bob"]))
            last_entry = loop.time()
            assert [await anext(events), await anext(events)] == [
                _announced("room", "alice"),
                _announced("room", "bob"),
            ]
            assert await anext(events) == LOADED
            assert loop.time() - last_entry >= SNAPSHOT_QUIET - EARLY

            await request.send(encode_namespace(["carol"]))
            assert await anext(events) == _announced("room", "carol")
            later = asyncio.ensure_future(anext(events))
            await asyncio.sleep(SNAPSHOT_LIMIT)
            assert not later.done()
            later.cancel()

    async def test_yields_loaded_alone_on_an_empty_prefix(
        self, connected: tuple[FakeTransport, RelaySubscriber]
    ) -> None:
        fake, connection = connected
        loop = asyncio.get_running_loop()
        async with connection.namespace_events(["room"]) as events:
            request = await fake.next_request_stream()
            await request.first_message
            await request.send(encode_request_ok())
            accepted = loop.time()
            assert await anext(events) == LOADED
            assert loop.time() - accepted >= SNAPSHOT_QUIET - EARLY

    async def test_yields_no_loaded_before_request_ok(self) -> None:
        fake, connection = await _connect(request_timeout=10 * SNAPSHOT_LIMIT)
        async with connection.namespace_events(["room"]) as events:
            request = await fake.next_request_stream()
            await request.first_message
            first = asyncio.ensure_future(anext(events))
            await asyncio.sleep(3 * SNAPSHOT_QUIET)
            assert not first.done()
            await request.send(encode_request_ok())
            assert await first == LOADED
        connection.close()

    async def test_restarts_the_quiet_window_on_each_entry_inside_it(
        self, connected: tuple[FakeTransport, RelaySubscriber]
    ) -> None:
        fake, connection = connected
        loop = asyncio.get_running_loop()
        async with connection.namespace_events(["room"]) as events:
            request = await fake.next_request_stream()
            await request.first_message
            await request.send(encode_request_ok())
            await asyncio.sleep(SNAPSHOT_QUIET - 0.02)
            await request.send(encode_namespace(["alice"]))
            entry = loop.time()
            assert await anext(events) == _announced("room", "alice")
            assert await anext(events) == LOADED
            assert loop.time() - entry >= SNAPSHOT_QUIET - EARLY

    async def test_yields_loaded_no_later_than_snapshot_limit_after_request_ok_under_steady_churn(
        self, connected: tuple[FakeTransport, RelaySubscriber], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Widened so a slow test host cannot open a quiet gap between churn steps.
        quiet, limit, step = 0.2, 0.5, 0.05
        monkeypatch.setattr(namespace_events, "SNAPSHOT_QUIET", quiet)
        monkeypatch.setattr(namespace_events, "SNAPSHOT_LIMIT", limit)
        fake, connection = connected
        loop = asyncio.get_running_loop()
        async with connection.namespace_events(["room"], capacity=1_000) as events:
            request = await fake.next_request_stream()
            await request.first_message
            await request.send(encode_request_ok())
            accepted = loop.time()
            seen: list[NamespaceEvent] = []

            async def churn() -> None:
                announce = True
                while True:
                    await request.send(encode_namespace(["alice"]) if announce else encode_namespace_done(["alice"]))
                    announce = not announce
                    await asyncio.sleep(step)

            churning = asyncio.ensure_future(churn())
            try:
                while LOADED not in seen:
                    seen.append(await anext(events))
            finally:
                churning.cancel()
            loaded_after = loop.time() - accepted
            assert len(seen) > 2
            assert limit - EARLY <= loaded_after < limit + step

    async def test_yields_no_loaded_once_the_stream_fails_inside_the_quiet_window(
        self, connected: tuple[FakeTransport, RelaySubscriber]
    ) -> None:
        fake, connection = connected
        async with connection.namespace_events(["room"]) as events:
            request = await fake.next_request_stream()
            await request.first_message
            await request.send(encode_request_ok())
            await request.send(encode_namespace(["alice"]))
            assert await anext(events) == _announced("room", "alice")
            request.reset(RuntimeError("gone"))
            with pytest.raises(SubscriptionRequestError):
                await anext(events)


ACTIVE_COUNT = KeyValuePair(SETUP_OPTION.ACTIVE_COUNT, 1)


class TestLoadedWithActiveCount:
    async def test_yields_loaded_right_after_the_counted_namespace_a_repeat_counted_never_from_a_window(
        self,
    ) -> None:
        fake, connection = await _connect(server_setup=[ACTIVE_COUNT])
        async with connection.namespace_events(["room"]) as events:
            request = await fake.next_request_stream()
            await request.first_message
            await request.send(encode_request_ok(MessageParameters(active_count=3)))
            await request.send(encode_namespace(["alice"]))
            await request.send(encode_namespace(["alice"]))
            assert await anext(events) == _announced("room", "alice")
            pending = asyncio.ensure_future(anext(events))
            await asyncio.sleep(SNAPSHOT_LIMIT + SNAPSHOT_QUIET)
            assert not pending.done()

            await request.send(encode_namespace(["bob"]))
            await request.send(encode_namespace(["carol"]))
            assert [await pending, await anext(events), await anext(events)] == [
                _announced("room", "bob"),
                LOADED,
                _announced("room", "carol"),
            ]
        connection.close()

    async def test_yields_loaded_at_once_on_a_count_of_0(self) -> None:
        fake, connection = await _connect(server_setup=[ACTIVE_COUNT])
        loop = asyncio.get_running_loop()
        async with connection.namespace_events(["room"]) as events:
            request = await fake.next_request_stream()
            await request.first_message
            await request.send(encode_request_ok(MessageParameters(active_count=0)))
            accepted = loop.time()
            assert await anext(events) == LOADED
            assert loop.time() - accepted < SNAPSHOT_QUIET
        connection.close()


class TestDiscoveryMarks:
    async def test_marks_only_the_withdrawals_a_clean_end_yields(
        self, connected: tuple[FakeTransport, RelaySubscriber]
    ) -> None:
        fake, connection = connected
        async with connection.namespace_events(["room"]) as events:
            request = await fake.next_request_stream()
            await request.first_message
            await request.send(encode_request_ok())
            await request.send(encode_namespace(["alice"]))
            await request.send(encode_namespace(["bob"]))
            await request.send(encode_namespace_done(["bob"]))
            await request.send(encode_namespace(["bob"]))
            await request.fin()
            await settle()
            seen = [event async for event in events]
        assert seen == [
            _announced("room", "alice"),
            _announced("room", "bob"),
            NamespaceEvent("unannounced", ("room", "bob")),
            _announced("room", "bob"),
            NamespaceEvent("unannounced", ("room", "alice")),
            NamespaceEvent("unannounced", ("room", "bob")),
        ]
        assert [_is_withdrawn_by_end(events, event) for event in seen] == [False, False, False, False, True, True]
        assert not _is_withdrawn_by_end(events, NamespaceEvent("unannounced", ("room", "alice")))

    async def test_marks_a_request_whose_stream_failed_or_that_ended_as_stopped_and_a_live_one_not(
        self, connected: tuple[FakeTransport, RelaySubscriber]
    ) -> None:
        fake, connection = connected
        events = connection.namespace_events(["room"])
        request = await fake.next_request_stream()
        await request.first_message
        await request.send(encode_request_ok())
        await request.send(encode_namespace(["alice"]))
        assert await anext(events) == _announced("room", "alice")
        assert not _is_stopped(events)
        request.reset(RuntimeError("gone"))
        with pytest.raises(SubscriptionRequestError):
            await anext(events)
        assert _is_stopped(events)

        closed = connection.namespace_events(["room"])
        await fake.next_request_stream()
        closed.close()
        assert _is_stopped(closed)

    async def test_marks_a_request_stopped_in_the_turn_its_connection_ends_before_its_reader_resumes(
        self, connected: tuple[FakeTransport, RelaySubscriber]
    ) -> None:
        fake, connection = connected
        events = connection.namespace_events(["room"])
        request = await fake.next_request_stream()
        await request.first_message
        await request.send(encode_request_ok())
        await request.send(encode_namespace(["alice"]))
        assert await anext(events) == _announced("room", "alice")
        fake.drop_transport()
        assert _is_stopped(events)
        await settle()
        assert _is_stopped(events)
