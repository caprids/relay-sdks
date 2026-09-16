"""``connect_relay`` behavior over a scripted fake transport, ported from the TypeScript SDK tests."""

import asyncio
import math
from collections.abc import AsyncIterator, Sequence
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest
from core_helpers.fake_transport import FakeTransport, create_fake_transport, encode_subgroup
from core_helpers.waiting import settle, wait_for

from moq_core import REQUEST_ERROR_CODE, Closed, MoqtObject, MoqtSubscribeOptions, SubscriptionHandlers
from moq_core.control_messages import Subscribe, encode_request_error, encode_subscribe_ok
from mux_relay import (
    CancellationToken,
    ConnectingTransport,
    RelayClosedError,
    RelayConnection,
    RelayTimeoutError,
    connect_relay,
)

ENDPOINT = "https://relay.example.com/moq"


def completed() -> asyncio.Future[None]:
    future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    future.set_result(None)
    return future


class Factory:
    """A ``create_transport`` that records its calls and hands out one fake transport."""

    def __init__(self, fake: FakeTransport, ready: asyncio.Future[None] | None = None) -> None:
        self.fake = fake
        self.ready = ready
        self.calls: list[tuple[str, list[str]]] = []

    def __call__(self, url: str, protocols: Sequence[str]) -> ConnectingTransport:
        self.calls.append((url, list(protocols)))
        return ConnectingTransport(transport=self.fake, ready=self.ready if self.ready is not None else completed())


@pytest.fixture
async def connections() -> AsyncIterator[list[RelayConnection]]:
    opened: list[RelayConnection] = []
    yield opened

    for connection in opened:
        connection.close()


async def connect_fake(
    connections: list[RelayConnection], url: str = ENDPOINT, **options: Any
) -> tuple[FakeTransport, RelayConnection, Factory]:
    fake = create_fake_transport()
    factory = Factory(fake)
    connecting = asyncio.ensure_future(connect_relay(url, create_transport=factory, **options))
    fake.send_server_setup()
    connection = await connecting
    connections.append(connection)
    return fake, connection, factory


class TestConnectRelay:
    @pytest.mark.parametrize("namespace", [[], ["live", ""], ["live", 7], "live", None])
    async def test_rejects_invalid_namespace_before_sending(
        self, connections: list[RelayConnection], namespace: Any
    ) -> None:
        fake, connection, _ = await connect_fake(connections)
        with pytest.raises(TypeError, match="non-empty string fields"):
            connection.subscribe(namespace, "events")
        with pytest.raises(TypeError, match="non-empty string fields"):
            connection.tracks.subscribe(MoqtSubscribeOptions(track_namespace=namespace, track_name="events"))
        connection.subscribe(["live", "room/42"], "events")
        request = await fake.next_request_stream()
        assert await request.first_message == Subscribe(
            request_id=0, track_namespace=["live", "room/42"], track_name="events"
        )

    async def test_namespace_limit_rejects_before_request_id_and_accepts_32_fields(
        self, connections: list[RelayConnection]
    ) -> None:
        fake, connection, _ = await connect_fake(connections)
        namespace = [f"field-{index}" for index in range(32)]
        with pytest.raises(ValueError, match="at most 32"):
            connection.subscribe([*namespace, "extra"], "events")
        with pytest.raises(ValueError, match="at most 32"):
            connection.tracks.subscribe(
                MoqtSubscribeOptions(track_namespace=[*namespace, "extra"], track_name="events")
            )
        connection.subscribe(namespace, "events")
        request = await fake.next_request_stream()
        assert await request.first_message == Subscribe(request_id=0, track_namespace=namespace, track_name="events")

    async def test_subscribes_to_arbitrary_binary_tracks_without_a_catalog_or_media_timestamps(
        self, connections: list[RelayConnection]
    ) -> None:
        fake, connection, _ = await connect_fake(connections)
        objects: list[MoqtObject] = []

        connection.subscribe(["sensors", "room/42"], "readings", SubscriptionHandlers(on_object=objects.append))
        request = await fake.next_request_stream()

        assert await request.first_message == Subscribe(
            request_id=0, track_namespace=["sensors", "room/42"], track_name="readings"
        )
        await request.send(encode_subscribe_ok(11))
        payload = bytes([0xFF, 0, 0x80, 1])

        fake.open_data_stream(encode_subgroup(11, 8, payload))
        await wait_for(lambda: len(objects) == 1)
        assert (objects[0].group_id, objects[0].object_id, objects[0].properties, objects[0].payload) == (
            8,
            0,
            [],
            payload,
        )

    async def test_keeps_quiet_data_tracks_alive_after_their_subscription_is_accepted(
        self, connections: list[RelayConnection]
    ) -> None:
        fake, connection, _ = await connect_fake(connections, request_timeout=0.05)
        errors: list[object] = []
        objects: list[MoqtObject] = []

        connection.subscribe(
            ["scores"], "current", SubscriptionHandlers(on_error=errors.append, on_object=objects.append)
        )
        request = await fake.next_request_stream()

        await request.first_message
        await request.send(encode_subscribe_ok(11))
        # Far longer than the request timeout: an accepted subscription may stay quiet indefinitely.
        await asyncio.sleep(0.15)
        assert errors == []

        payload = bytes([1, 0])

        fake.open_data_stream(encode_subgroup(11, 0, payload))
        await wait_for(lambda: len(objects) == 1)
        assert objects[0].payload == payload

    async def test_cancels_one_subscription_while_another_consumer_keeps_receiving(
        self, connections: list[RelayConnection]
    ) -> None:
        fake, connection, factory = await connect_fake(connections)
        first = connection.subscribe(["room"], "one")
        first_request = await fake.next_request_stream()

        await first_request.first_message
        await first_request.send(encode_subscribe_ok(11))

        objects: list[MoqtObject] = []
        second = connection.tracks.subscribe(
            MoqtSubscribeOptions(track_namespace=["room"], track_name="two"),
            SubscriptionHandlers(on_object=objects.append),
        )
        second_request = await fake.next_request_stream()

        await second_request.first_message
        await second_request.send(encode_subscribe_ok(12))
        first.cancel()
        first.cancel()
        fake.open_data_stream(encode_subgroup(12, 0, bytes([42])))

        await wait_for(lambda: len(objects) == 1)
        assert fake.close_calls == 0
        assert first.request_id != second.request_id
        assert len(factory.calls) == 1

    async def test_provides_a_borrowed_generic_track_interface_without_connection_shutdown(
        self, connections: list[RelayConnection]
    ) -> None:
        _, connection, _ = await connect_fake(connections)

        assert not hasattr(connection.tracks, "close")
        assert not hasattr(connection.tracks, "destroy")
        assert connection.tracks.closed is connection.closed
        connection.close()
        assert await connection.closed == Closed()

        with pytest.raises(RelayClosedError, match="closed"):
            connection.tracks.subscribe(MoqtSubscribeOptions(track_namespace=["room"], track_name="events"))

    async def test_preserves_request_error_codes_without_closing_unrelated_consumers(
        self, connections: list[RelayConnection]
    ) -> None:
        fake, connection, _ = await connect_fake(connections)
        errors: list[Any] = []

        connection.subscribe(["room"], "missing", SubscriptionHandlers(on_error=errors.append))
        request = await fake.next_request_stream()

        await request.first_message
        await request.send(encode_request_error(REQUEST_ERROR_CODE.DOES_NOT_EXIST, "missing track"))
        await wait_for(lambda: len(errors) == 1)
        assert errors[0].error_code == REQUEST_ERROR_CODE.DOES_NOT_EXIST
        assert errors[0].reason == "missing track"
        assert fake.close_calls == 0

    async def test_adds_relay_authentication_to_the_url_and_negotiates_the_pinned_protocol(
        self, connections: list[RelayConnection]
    ) -> None:
        calls = 0

        async def get_token() -> str:
            nonlocal calls
            calls += 1
            return "token with/&?"

        _, _, factory = await connect_fake(connections, url=f"{ENDPOINT}?keep=1", get_token=get_token)
        url, protocols = factory.calls[0]
        query = parse_qs(urlsplit(url).query, keep_blank_values=True)

        assert urlsplit(url).path == "/moq"
        assert query["keep"] == ["1"]
        assert query["jwt"] == ["token with/&?"]
        assert protocols == ["moqt-20"]
        assert calls == 1

    async def test_does_not_mint_a_token_when_the_endpoint_already_contains_an_explicit_jwt(
        self, connections: list[RelayConnection]
    ) -> None:
        def get_token() -> str:
            raise AssertionError("must not run")

        _, _, factory = await connect_fake(connections, url=f"{ENDPOINT}?jwt=explicit", get_token=get_token)

        assert factory.calls == [(f"{ENDPOINT}?jwt=explicit", ["moqt-20"])]

    async def test_closes_an_established_connection_when_the_owner_cancels_and_closes_the_transport_once(
        self, connections: list[RelayConnection]
    ) -> None:
        owner = CancellationToken()
        fake, connection, _ = await connect_fake(connections, cancellation=owner)
        error = RuntimeError("owner finished")

        owner.cancel(error)
        assert await connection.closed == Closed(error)
        connection.close()
        assert fake.close_calls == 1

    async def test_observes_peer_closure_and_prevents_new_requests_on_the_dead_connection(
        self, connections: list[RelayConnection]
    ) -> None:
        fake, connection, _ = await connect_fake(connections)

        fake.drop_transport()
        await connection.closed

        with pytest.raises(RelayClosedError, match="closed"):
            connection.subscribe(["room"], "events")

    async def test_cancels_while_a_token_provider_is_pending_without_opening_a_late_transport(self) -> None:
        owner = CancellationToken()
        token: asyncio.Future[str] = asyncio.get_running_loop().create_future()
        factory = Factory(create_fake_transport())
        connecting = asyncio.ensure_future(
            connect_relay(ENDPOINT, cancellation=owner, get_token=lambda: token, create_transport=factory)
        )
        await settle()

        owner.cancel(RuntimeError("cancelled"))

        with pytest.raises(RuntimeError, match="cancelled"):
            await connecting

        token.set_result("late token")
        await settle()
        assert factory.calls == []

    async def test_closes_a_pending_transport_immediately_on_cancel_without_closing_it_again_when_ready_settles(
        self,
    ) -> None:
        owner = CancellationToken()
        fake = create_fake_transport()
        ready: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        connecting = asyncio.ensure_future(
            connect_relay(ENDPOINT, cancellation=owner, create_transport=Factory(fake, ready))
        )
        await settle()

        owner.cancel(RuntimeError("cancelled"))

        with pytest.raises(RuntimeError, match="cancelled"):
            await connecting

        assert fake.close_calls == 1
        ready.set_result(None)
        await settle()
        assert fake.close_calls == 1

    async def test_closes_a_transport_returned_by_a_factory_that_cancels_synchronously(self) -> None:
        owner = CancellationToken()
        fake = create_fake_transport()

        def create_transport(url: str, protocols: Sequence[str]) -> ConnectingTransport:
            owner.cancel(RuntimeError("factory cancelled"))
            return ConnectingTransport(transport=fake, ready=completed())

        with pytest.raises(RuntimeError, match="factory cancelled"):
            await connect_relay(ENDPOINT, cancellation=owner, create_transport=create_transport)

        assert fake.close_calls == 1

    async def test_times_out_a_transport_that_never_completes_the_handshake(self) -> None:
        fake = create_fake_transport()
        never: asyncio.Future[None] = asyncio.get_running_loop().create_future()

        with pytest.raises(RelayTimeoutError, match="timed out"):
            await connect_relay(ENDPOINT, connect_timeout=0.02, create_transport=Factory(fake, never))

        assert fake.close_calls == 1

    async def test_times_out_a_peer_that_connects_but_never_sends_protocol_setup(self) -> None:
        fake = create_fake_transport()

        with pytest.raises(RelayTimeoutError, match="timed out"):
            await connect_relay(ENDPOINT, connect_timeout=0.02, create_transport=Factory(fake))

        assert fake.close_calls == 1

    async def test_closes_a_transport_whose_handshake_fails(self) -> None:
        fake = create_fake_transport()
        failed: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        failed.set_exception(ConnectionError("handshake failed"))

        with pytest.raises(ConnectionError, match="handshake failed"):
            await connect_relay(ENDPOINT, create_transport=Factory(fake, failed))

        assert fake.close_calls == 1

    async def test_rejects_an_already_cancelled_owner_before_minting_tokens_or_creating_a_transport(self) -> None:
        owner = CancellationToken()
        token_calls: list[int] = []
        factory = Factory(create_fake_transport())

        owner.cancel(RuntimeError("already cancelled"))

        with pytest.raises(RuntimeError, match="already cancelled"):
            await connect_relay(
                ENDPOINT, cancellation=owner, get_token=lambda: token_calls.append(1), create_transport=factory
            )

        assert token_calls == []
        assert factory.calls == []

    async def test_uses_the_aioquic_webtransport_client_by_default(self) -> None:
        # Nothing listens here, so the default transport's handshake never completes and the
        # connection deadline is what ends the attempt.
        with pytest.raises(RelayTimeoutError, match="timed out"):
            await connect_relay("https://127.0.0.1:9/moq", connect_timeout=0.3)

    @pytest.mark.parametrize("connect_timeout", [0, -1, math.nan, math.inf])
    async def test_rejects_invalid_connection_deadlines(self, connect_timeout: float) -> None:
        with pytest.raises(ValueError, match="positive finite"):
            await connect_relay(ENDPOINT, connect_timeout=connect_timeout)

    @pytest.mark.parametrize(
        "url",
        ["http://relay.example.com/moq", "https://relay.example.com/moq#frag", "https://user@relay.example.com/moq"],
    )
    async def test_rejects_endpoints_that_are_not_plain_https(self, url: str) -> None:
        with pytest.raises(TypeError, match="Relay endpoints"):
            await connect_relay(url)

    async def test_is_an_async_context_manager_that_closes_on_exit(self, connections: list[RelayConnection]) -> None:
        _, connection, _ = await connect_fake(connections)

        async with connection:
            pass

        assert await connection.closed == Closed()
