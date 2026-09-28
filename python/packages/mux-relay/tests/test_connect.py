"""``connect_relay_subscriber`` behavior over a scripted fake transport, ported from the TypeScript SDK tests."""

import asyncio
import math
from collections.abc import AsyncIterator, Sequence
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest
from core_helpers.fake_transport import FakeTransport, create_fake_transport, encode_subgroup
from core_helpers.waiting import settle, wait_for

from moq_core import (
    PUBLISH_DONE_STATUS,
    REQUEST_ERROR_CODE,
    Closed,
    MoqtObject,
    MoqtSubscribeOptions,
    PublishDone,
    SubscribeOk,
    SubscriptionHandlers,
)
from moq_core.control_messages import (
    Subscribe,
    encode_goaway,
    encode_publish_done,
    encode_request_error,
    encode_setup,
    encode_subscribe_ok,
)
from mux_relay import (
    CancellationToken,
    ConnectingTransport,
    RelayError,
    RelaySubscriber,
    RelaySubscriptionHandlers,
    SubscriptionConnectionClosedError,
    SubscriptionEnd,
    SubscriptionRequestError,
    connect_relay_subscriber,
)
from mux_relay.webtransport import WebTransportError

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
async def connections() -> AsyncIterator[list[RelaySubscriber]]:
    opened: list[RelaySubscriber] = []
    yield opened

    for connection in opened:
        connection.close()


async def connect_fake(
    connections: list[RelaySubscriber], url: str = ENDPOINT, **options: Any
) -> tuple[FakeTransport, RelaySubscriber, Factory]:
    fake = create_fake_transport()
    factory = Factory(fake)
    connecting = asyncio.ensure_future(connect_relay_subscriber(url, create_transport=factory, **options))
    fake.send_server_setup()
    connection = await connecting
    connections.append(connection)
    return fake, connection, factory


class TestConnectRelay:
    async def test_task_cancellation_wins_over_simultaneous_owner_cancellation(self) -> None:
        owner = CancellationToken()
        ready: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        connecting = asyncio.create_task(
            connect_relay_subscriber(
                ENDPOINT, cancellation=owner, create_transport=Factory(create_fake_transport(), ready)
            )
        )
        await asyncio.sleep(0)
        connecting.cancel("caller cancelled task")
        owner.cancel()
        with pytest.raises(asyncio.CancelledError, match="caller cancelled task"):
            await connecting
        assert connecting.cancelled()

    async def test_shares_a_wrapped_transport_failure_with_rejected_operations(
        self, connections: list[RelaySubscriber]
    ) -> None:
        fake, connection, _ = await connect_fake(connections)
        cause = ConnectionError("transport failed")
        fake._closed.set_exception(cause)
        closed = await asyncio.wait_for(connection.closed, 1)
        assert isinstance(closed.error, RelayError)
        assert closed.error.code == "TRANSPORT_ERROR"
        assert closed.error.phase == "connection"
        assert closed.error.__cause__ is cause
        with pytest.raises(RelayError) as failure:
            connection.subscribe(["room"], "events")
        assert failure.value is closed.error

    async def test_preserves_token_provider_exceptions(self) -> None:
        cause = TypeError("token provider failed")

        def get_token() -> str:
            raise cause

        with pytest.raises(TypeError) as failure:
            await connect_relay_subscriber(ENDPOINT, get_token=get_token)
        assert failure.value is cause

    @pytest.mark.parametrize("namespace", [[], ["live", ""], ["live", 7], "live", None])
    async def test_rejects_invalid_namespace_before_sending(
        self, connections: list[RelaySubscriber], namespace: Any
    ) -> None:
        fake, connection, _ = await connect_fake(connections)
        with pytest.raises(TypeError, match="non-empty string fields"):
            connection.subscribe(namespace, "events")
        with pytest.raises(TypeError, match="non-empty string fields"):
            connection.track_source.subscribe(MoqtSubscribeOptions(track_namespace=namespace, track_name="events"))
        connection.subscribe(["live", "room/42"], "events")
        request = await fake.next_request_stream()
        assert await request.first_message == Subscribe(
            request_id=0, track_namespace=["live", "room/42"], track_name="events"
        )

    async def test_namespace_limit_rejects_before_request_id_and_accepts_32_fields(
        self, connections: list[RelaySubscriber]
    ) -> None:
        fake, connection, _ = await connect_fake(connections)
        namespace = [f"field-{index}" for index in range(32)]
        with pytest.raises(ValueError, match="at most 32"):
            connection.subscribe([*namespace, "extra"], "events")
        with pytest.raises(ValueError, match="at most 32"):
            connection.track_source.subscribe(
                MoqtSubscribeOptions(track_namespace=[*namespace, "extra"], track_name="events")
            )
        connection.subscribe(namespace, "events")
        request = await fake.next_request_stream()
        assert await request.first_message == Subscribe(request_id=0, track_namespace=namespace, track_name="events")

    async def test_subscribes_to_arbitrary_binary_tracks_without_a_catalog_or_media_timestamps(
        self, connections: list[RelaySubscriber]
    ) -> None:
        fake, connection, _ = await connect_fake(connections)
        objects: list[MoqtObject] = []

        connection.subscribe(["sensors", "room/42"], "readings", RelaySubscriptionHandlers(on_object=objects.append))
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

    async def test_ends_pending_accepted_and_cancelled_subscriptions_with_a_goaway_as_a_closure_never_a_request_error(
        self, connections: list[RelaySubscriber]
    ) -> None:
        fake = create_fake_transport()
        control = fake.open_controlled_data_stream()
        connecting = asyncio.create_task(connect_relay_subscriber(ENDPOINT, create_transport=Factory(fake)))
        await control.write(encode_setup([]))
        connection = await connecting
        connections.append(connection)
        pending_errors: list[Any] = []
        accepted_errors: list[Any] = []
        cancelled_errors: list[Any] = []
        answers: list[Any] = []

        pending = connection.subscribe(
            ["live"], "pending", RelaySubscriptionHandlers(on_error=pending_errors.append, on_subscribed=answers.append)
        )
        await (await fake.next_request_stream()).first_message
        accepted_subscription = connection.subscribe(
            ["live"], "accepted", RelaySubscriptionHandlers(on_error=accepted_errors.append)
        )
        accepted = await fake.next_request_stream()
        await accepted.first_message
        await accepted.send(encode_subscribe_ok(11))
        await accepted_subscription.subscribed
        cancelled = connection.subscribe(
            ["live"], "cancelled", RelaySubscriptionHandlers(on_error=cancelled_errors.append)
        )
        await (await fake.next_request_stream()).first_message
        cancelled.cancel()
        await control.write(encode_goaway(1000))
        closed = await connection.closed
        assert isinstance(closed.error, RelayError)
        assert closed.error.code == "GOAWAY"
        assert closed.error.phase == "connection"
        assert fake.close_info is not None
        assert fake.close_info.close_code == 0
        assert fake.close_info.reason == ""
        connection.close()
        await settle()
        with pytest.raises(SubscriptionConnectionClosedError) as failure:
            await pending.subscribed
        assert failure.value.__cause__ is closed.error
        assert await pending.ended == SubscriptionEnd(
            kind="terminated", cause="connection-closed", accepted=False, error=closed.error
        )
        accepted_end = await accepted_subscription.ended
        assert (accepted_end.kind, accepted_end.accepted, accepted_end.error) == ("terminated", True, closed.error)
        assert (await cancelled.ended).kind == "cancelled"
        assert pending_errors == answers == accepted_errors == cancelled_errors == []

    async def test_reports_a_transport_drop_before_subscribe_ok_as_a_closure_not_a_rejection(
        self, connections: list[RelaySubscriber]
    ) -> None:
        fake = create_fake_transport()
        connecting = asyncio.create_task(connect_relay_subscriber(ENDPOINT, create_transport=Factory(fake)))
        fake.send_server_setup()
        connection = await connecting
        connections.append(connection)
        errors: list[Any] = []
        subscription = connection.subscribe(["room"], "pending", RelaySubscriptionHandlers(on_error=errors.append))
        await (await fake.next_request_stream()).first_message
        fake.drop_transport()
        closed = await connection.closed
        # The fake drops without a transport error, so the closure carries none; the core still fails
        # the pending request first, and that failure must not become the subscription's ending.
        assert closed.error is None
        with pytest.raises(SubscriptionConnectionClosedError) as failure:
            await subscription.subscribed
        assert failure.value.__cause__ is None
        assert await subscription.ended == SubscriptionEnd(
            kind="terminated", cause="connection-closed", accepted=False, error=None
        )
        assert errors == []

    async def test_reports_a_transport_drop_as_a_closure_even_when_the_session_learns_of_it_turns_later(
        self, connections: list[RelaySubscriber]
    ) -> None:
        fake = create_fake_transport()
        # Like the native transport: streams fail at once, wait_closed returns after the socket is
        # released, so the core fails the pending request turns before it reports the closure.
        fake.release_after_turns = 5
        connecting = asyncio.create_task(connect_relay_subscriber(ENDPOINT, create_transport=Factory(fake)))
        fake.send_server_setup()
        connection = await connecting
        connections.append(connection)
        # Drain the client's SETUP so the session's start-up write completes, as it does at once over
        # a real transport; left pending, its failure would make the fake report closure early.
        await wait_for(lambda: len(fake.client_uni_streams) > 0)
        await fake.client_uni_streams[0].read()
        errors: list[Any] = []
        subscription = connection.subscribe(["room"], "pending", RelaySubscriptionHandlers(on_error=errors.append))
        await (await fake.next_request_stream()).first_message
        fake.drop_transport()
        for _ in range(3):
            await asyncio.sleep(0)
        # The request stream has failed by now, the session has not reported closure yet.
        assert not connection.closed.done()
        assert not subscription.ended.done(), subscription.ended.result()
        closed = await connection.closed
        assert await subscription.ended == SubscriptionEnd(
            kind="terminated", cause="connection-closed", accepted=False, error=closed.error
        )
        with pytest.raises(SubscriptionConnectionClosedError):
            await subscription.subscribed
        assert errors == []

    async def test_contains_an_on_error_that_raises_on_rejection_and_keeps_the_connection_usable(
        self, connections: list[RelaySubscriber]
    ) -> None:
        fake = create_fake_transport()
        connecting = asyncio.create_task(connect_relay_subscriber(ENDPOINT, create_transport=Factory(fake)))
        fake.send_server_setup()
        connection = await connecting
        connections.append(connection)
        calls: list[Any] = []

        def on_error(error: Any) -> None:
            calls.append(error)
            raise RuntimeError("application callback failed")

        subscription = connection.subscribe(["room"], "missing", RelaySubscriptionHandlers(on_error=on_error))
        request = await fake.next_request_stream()
        await request.first_message
        await request.send(encode_request_error(REQUEST_ERROR_CODE.DOES_NOT_EXIST, "missing track"))
        with pytest.raises(SubscriptionRequestError):
            await subscription.subscribed
        assert (await subscription.ended).kind == "rejected"
        assert len(calls) == 1
        other = connection.subscribe(["room"], "present")
        await (await fake.next_request_stream()).send(encode_subscribe_ok(11))
        assert (await other.subscribed).track_alias == 11

    async def test_keeps_quiet_data_tracks_alive_after_their_subscription_is_accepted(
        self, connections: list[RelaySubscriber]
    ) -> None:
        fake, connection, _ = await connect_fake(connections, request_timeout=0.05)
        errors: list[object] = []
        objects: list[MoqtObject] = []

        connection.subscribe(
            ["scores"], "current", RelaySubscriptionHandlers(on_error=errors.append, on_object=objects.append)
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
        self, connections: list[RelaySubscriber]
    ) -> None:
        fake, connection, factory = await connect_fake(connections)
        first = connection.subscribe(["room"], "one")
        first_request = await fake.next_request_stream()

        await first_request.first_message
        await first_request.send(encode_subscribe_ok(11))

        objects: list[MoqtObject] = []
        second = connection.track_source.subscribe(
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

    async def test_maps_product_handler_names_onto_the_core_and_keeps_protocol_names_on_track_source(
        self, connections: list[RelaySubscriber]
    ) -> None:
        fake, connection, _ = await connect_fake(connections)
        accepted: list[SubscribeOk] = []
        done: list[PublishDone] = []
        connection.subscribe(
            ["live"], "events", RelaySubscriptionHandlers(on_subscribed=accepted.append, on_publisher_done=done.append)
        )
        request = await fake.next_request_stream()
        await request.first_message
        await request.send(encode_subscribe_ok(11))
        await request.send(encode_publish_done(PUBLISH_DONE_STATUS.TRACK_ENDED, 0, "track ended"))
        await wait_for(lambda: len(accepted) == 1 and len(done) == 1)
        assert accepted[0].track_alias == 11
        assert (done[0].status_code, done[0].reason) == (PUBLISH_DONE_STATUS.TRACK_ENDED, "track ended")

        core_accepted: list[SubscribeOk] = []
        core_done: list[PublishDone] = []
        connection.track_source.subscribe(
            MoqtSubscribeOptions(track_namespace=["live"], track_name="raw"),
            SubscriptionHandlers(on_ok=core_accepted.append, on_done=core_done.append),
        )
        second = await fake.next_request_stream()
        await second.first_message
        await second.send(encode_subscribe_ok(12))
        await second.send(encode_publish_done(PUBLISH_DONE_STATUS.TRACK_ENDED, 0, "track ended"))
        await wait_for(lambda: len(core_accepted) == 1 and len(core_done) == 1)
        assert core_accepted[0].track_alias == 12

    async def test_provides_a_borrowed_generic_track_interface_without_connection_shutdown(
        self, connections: list[RelaySubscriber]
    ) -> None:
        _, connection, _ = await connect_fake(connections)

        assert not hasattr(connection.track_source, "close")
        assert not hasattr(connection.track_source, "destroy")
        # The borrowed source keeps the core's `Future` contract; the product handle isolates its waiters.
        # Both report the same closure.
        assert not connection.track_source.closed.done()
        assert not connection.closed.done()
        connection.close()
        assert await connection.closed == Closed()
        assert connection.track_source.closed.result() == Closed()

        with pytest.raises(RelayError, match="closed") as failure:
            connection.track_source.subscribe(MoqtSubscribeOptions(track_namespace=["room"], track_name="events"))
        assert failure.value.code == "CLOSED"

    async def test_preserves_request_error_codes_without_closing_unrelated_consumers(
        self, connections: list[RelaySubscriber]
    ) -> None:
        fake, connection, _ = await connect_fake(connections)
        errors: list[Any] = []

        connection.subscribe(["room"], "missing", RelaySubscriptionHandlers(on_error=errors.append))
        request = await fake.next_request_stream()

        await request.first_message
        await request.send(encode_request_error(REQUEST_ERROR_CODE.DOES_NOT_EXIST, "missing track"))
        await wait_for(lambda: len(errors) == 1)
        assert errors[0].error_code == REQUEST_ERROR_CODE.DOES_NOT_EXIST
        assert errors[0].reason == "missing track"
        assert fake.close_calls == 0

    async def test_adds_relay_authentication_to_the_url_and_negotiates_the_pinned_protocol(
        self, connections: list[RelaySubscriber]
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
        self, connections: list[RelaySubscriber]
    ) -> None:
        def get_token() -> str:
            raise AssertionError("must not run")

        _, _, factory = await connect_fake(connections, url=f"{ENDPOINT}?jwt=explicit", get_token=get_token)

        assert factory.calls == [(f"{ENDPOINT}?jwt=explicit", ["moqt-20"])]

    @pytest.mark.parametrize("query", ["?jwt=", "?jwt", "?keep=1&jwt=", "?jwt=&jwt=explicit", "?jwt=explicit&jwt="])
    async def test_rejects_an_empty_jwt_on_the_endpoint_before_the_token_provider_or_transport(
        self, query: str
    ) -> None:
        def forbidden(*args: Any) -> Any:
            pytest.fail("an empty jwt must fail before the token provider or transport")

        with pytest.raises(ValueError, match=r"empty jwt query parameter.*supply get_token"):
            await connect_relay_subscriber(f"{ENDPOINT}{query}", get_token=forbidden, create_transport=forbidden)

    async def test_closes_an_established_connection_when_the_owner_cancels_and_closes_the_transport_once(
        self, connections: list[RelaySubscriber]
    ) -> None:
        owner = CancellationToken()
        fake, connection, _ = await connect_fake(connections, cancellation=owner)
        error = RuntimeError("owner finished")

        owner.cancel(error)
        assert await connection.closed == Closed(error)
        connection.close()
        assert fake.close_calls == 1

    async def test_observes_peer_closure_and_prevents_new_requests_on_the_dead_connection(
        self, connections: list[RelaySubscriber]
    ) -> None:
        fake, connection, _ = await connect_fake(connections)

        fake.drop_transport()
        await connection.closed

        with pytest.raises(RelayError, match="closed") as failure:
            connection.subscribe(["room"], "events")
        assert failure.value.code == "CLOSED"

    async def test_cancels_while_a_token_provider_is_pending_without_opening_a_late_transport(self) -> None:
        owner = CancellationToken()
        token: asyncio.Future[str] = asyncio.get_running_loop().create_future()
        factory = Factory(create_fake_transport())
        connecting = asyncio.ensure_future(
            connect_relay_subscriber(ENDPOINT, cancellation=owner, get_token=lambda: token, create_transport=factory)
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
            connect_relay_subscriber(ENDPOINT, cancellation=owner, create_transport=Factory(fake, ready))
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
            await connect_relay_subscriber(ENDPOINT, cancellation=owner, create_transport=create_transport)

        assert fake.close_calls == 1

    async def test_times_out_a_transport_that_never_completes_the_handshake(self) -> None:
        fake = create_fake_transport()
        never: asyncio.Future[None] = asyncio.get_running_loop().create_future()

        with pytest.raises(RelayError) as failure:
            await connect_relay_subscriber(ENDPOINT, connect_timeout=0.02, create_transport=Factory(fake, never))
        assert failure.value.code == "CONNECTION_TIMEOUT"
        assert failure.value.phase == "connecting"

        assert fake.close_calls == 1

    async def test_times_out_a_peer_that_connects_but_never_sends_protocol_setup(self) -> None:
        fake = create_fake_transport()

        with pytest.raises(RelayError, match="timed out") as failure:
            await connect_relay_subscriber(ENDPOINT, connect_timeout=0.02, create_transport=Factory(fake))
        assert (failure.value.code, failure.value.phase) == ("CONNECTION_TIMEOUT", "connecting")

        assert fake.close_calls == 1

    async def test_closes_a_transport_whose_handshake_fails(self) -> None:
        fake = create_fake_transport()
        failed: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        cause = WebTransportError("handshake failed", code=42, reason="peer detail")
        failed.set_exception(cause)

        with pytest.raises(RelayError, match="handshake failed") as failure:
            await connect_relay_subscriber(ENDPOINT, create_transport=Factory(fake, failed))
        assert failure.value.code == "TRANSPORT_ERROR"
        assert failure.value.phase == "connecting"
        assert failure.value.__cause__ is cause
        assert cause.code == 42
        assert cause.reason == "peer detail"

        assert fake.close_calls == 1

    @pytest.mark.parametrize(
        ("close_code", "reason"), [(0x2, "unauthorized"), (0x16, "malformed token"), (0x18, "expired token")]
    )
    async def test_reports_a_session_the_relay_closed_for_its_credentials_as_unauthorized(
        self, close_code: int, reason: str
    ) -> None:
        fake = create_fake_transport()
        failed: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        cause = WebTransportError("peer closed the session", code=close_code, reason=reason, peer_closed=True)
        failed.set_exception(cause)

        with pytest.raises(RelayError) as failure:
            await connect_relay_subscriber(ENDPOINT, create_transport=Factory(fake, failed))
        assert (failure.value.code, failure.value.phase) == ("UNAUTHORIZED", "connecting")
        assert str(failure.value) == (
            f"Relay refused the connection's credentials: {reason} (session closed with code 0x{close_code:x})"
        )
        assert failure.value.__cause__ is cause

    @pytest.mark.parametrize(
        "cause",
        [
            # QUIC CONNECTION_REFUSED is 0x2 too, but it is not the peer's session close code.
            WebTransportError("QUIC connection terminated", code=0x2, reason="refused"),
            WebTransportError("peer closed the session", code=0x3, reason="protocol violation", peer_closed=True),
        ],
    )
    async def test_other_closures_stay_transport_errors(self, cause: WebTransportError) -> None:
        fake = create_fake_transport()
        failed: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        failed.set_exception(cause)

        with pytest.raises(RelayError) as failure:
            await connect_relay_subscriber(ENDPOINT, create_transport=Factory(fake, failed))
        assert failure.value.code == "TRANSPORT_ERROR"
        assert failure.value.__cause__ is cause

    async def test_rejects_an_already_cancelled_owner_before_minting_tokens_or_creating_a_transport(self) -> None:
        owner = CancellationToken()
        token_calls: list[int] = []
        factory = Factory(create_fake_transport())

        owner.cancel(RuntimeError("already cancelled"))

        with pytest.raises(RuntimeError, match="already cancelled"):
            await connect_relay_subscriber(
                ENDPOINT, cancellation=owner, get_token=lambda: token_calls.append(1), create_transport=factory
            )

        assert token_calls == []
        assert factory.calls == []

    async def test_uses_the_aioquic_webtransport_client_by_default(self) -> None:
        # Nothing listens here, so the default transport's handshake never completes and the
        # connection deadline is what ends the attempt.
        with pytest.raises(RelayError, match="timed out") as failure:
            await connect_relay_subscriber("https://127.0.0.1:9/moq", connect_timeout=0.3)
        assert failure.value.code == "CONNECTION_TIMEOUT"

    @pytest.mark.parametrize("connect_timeout", [0, -1, math.nan, math.inf, 2_147_483.648])
    async def test_rejects_invalid_connection_deadlines(self, connect_timeout: float) -> None:
        with pytest.raises(
            ValueError, match=r"connect_timeout must be between 0 \(exclusive\) and 2147483.647 seconds"
        ):
            await connect_relay_subscriber(ENDPOINT, connect_timeout=connect_timeout)

    @pytest.mark.parametrize("request_timeout", [0, -1, math.nan, math.inf, 2_147_483.648])
    async def test_rejects_invalid_request_deadlines(self, request_timeout: float) -> None:
        with pytest.raises(
            ValueError, match=r"request_timeout must be between 0 \(exclusive\) and 2147483.647 seconds"
        ):
            await connect_relay_subscriber(ENDPOINT, request_timeout=request_timeout)

    @pytest.mark.parametrize("name", ["connect_timeout", "request_timeout"])
    @pytest.mark.parametrize("value", ["10", None, True])
    async def test_rejects_a_deadline_that_is_not_a_number_as_a_type_error(self, name: str, value: Any) -> None:
        def forbidden(*args: Any) -> Any:
            pytest.fail("a bad deadline must fail before the transport")

        with pytest.raises(TypeError, match=f"{name} must be a number of seconds"):
            await connect_relay_subscriber(ENDPOINT, create_transport=forbidden, **{name: value})

    async def test_accepts_the_largest_deadline_the_typescript_timers_can_hold(
        self, connections: list[RelaySubscriber]
    ) -> None:
        await connect_fake(connections, connect_timeout=2_147_483.647, request_timeout=2_147_483.647)

    @pytest.mark.parametrize(
        "url",
        [
            "http://relay.example.com/moq",
            "wss://relay.example.com/moq",
            "https://relay.example.com/moq#frag",
            "https://user@relay.example.com/moq",
            "https://user:pass@relay.example.com/moq",
        ],
    )
    async def test_rejects_endpoints_relay_does_not_accept_as_a_value_error(self, url: str) -> None:
        def forbidden(*args: Any) -> Any:
            pytest.fail("a bad endpoint must fail before the token provider or transport")

        with pytest.raises(ValueError, match="Relay endpoints"):
            await connect_relay_subscriber(url, get_token=forbidden, create_transport=forbidden)

    @pytest.mark.parametrize("url", ["relay.example.com/moq", "https://", "https:///moq", "https://[::1/moq", 7, b"x"])
    async def test_rejects_endpoints_that_are_not_url_strings_as_a_type_error(self, url: Any) -> None:
        def forbidden(*args: Any) -> Any:
            pytest.fail("a bad endpoint must fail before the transport")

        with pytest.raises(TypeError, match="Relay endpoints"):
            await connect_relay_subscriber(url, create_transport=forbidden)

    async def test_is_an_async_context_manager_that_closes_on_exit(self, connections: list[RelaySubscriber]) -> None:
        _, connection, _ = await connect_fake(connections)

        async with connection:
            pass

        assert await connection.closed == Closed()
