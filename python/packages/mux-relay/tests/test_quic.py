"""The native QUIC dialing end (``mux_relay.quic``) over a real QUIC loopback, test by test after its TA rule.

The accepting end is the conformance peer in its native QUIC mode (``relay_helpers.conformance_peer``), in
process: it records the dial, selects a protocol as told, resets, stops, closes at either layer, withholds
acknowledgement and limits stream credit. The whole SDK runs against the in-memory relay hub served over
native QUIC. The setup options themselves (TA-6) are covered against the vectors in ``test_setup_options.py``;
here they are read off a real connection's CLIENT_SETUP.
"""

from __future__ import annotations

import asyncio
import socket
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from functools import partial
from typing import Any, cast

import pytest
from aioquic.asyncio import QuicConnectionProtocol, serve
from aioquic.quic import events
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.connection import QuicConnection
from aioquic.quic.packet import QuicErrorCode, QuicFrameType
from core_helpers.waiting import wait_for
from cryptography.hazmat.primitives import serialization
from relay_helpers.conformance_peer import (
    IDLE_TIMEOUT,
    ConformancePeer,
    NativeQuicConformanceProtocol,
    generate_leaf_certificate,
)
from relay_helpers.native_quic import NativeQuicSession
from relay_helpers.relay_hub import RelayHub, create_relay_hub
from relay_helpers.wt_server import SERVER_NAME
from test_setup_options import option_value, read_client_setup

from moq_core import MoqtObject
from moq_core.bytes import StreamReader
from moq_core.control_messages import SETUP_OPTION
from moq_core.transport import ReceiveStream
from mux_relay import (
    Closed,
    ConnectingTransport,
    RelayError,
    RelaySubscriptionHandlers,
    connect_relay_publisher,
    connect_relay_subscriber,
)
from mux_relay.quic import connect_quic
from mux_relay.quic._client import QuicSession
from mux_relay.webtransport import WebTransportError
from mux_relay.webtransport._errors import StreamStopped

MOQT = "moqt-20"
NAMESPACE = ["loopback", "quic"]


@dataclass
class Peer:
    """The conformance peer, listening, and how to dial it."""

    peer: ConformancePeer
    port: int
    certificate_pem: bytes

    def url(self, path: str = "/moq?jwt=secret") -> str:
        return f"https://{SERVER_NAME}:{self.port}{path}"

    def configuration(self, **options: Any) -> QuicConfiguration:
        return QuicConfiguration(is_client=True, cadata=self.certificate_pem, **options)

    def dial(self, protocols: Sequence[str] = (MOQT,), **options: Any) -> tuple[QuicSession, ConnectingTransport]:
        created = connect_quic(self.url(), protocols, configuration=self.configuration(**options))
        assert isinstance(created.transport, QuicSession)
        return created.transport, created

    def factory(self, created: list[QuicSession] | None = None):
        def create_transport(url: str, protocols: Sequence[str]) -> ConnectingTransport:
            connecting = connect_quic(url, protocols, configuration=self.configuration())
            if created is not None:
                created.append(cast(QuicSession, connecting.transport))
            return connecting

        return create_transport


@pytest.fixture
async def peer() -> AsyncIterator[Peer]:
    conformance = ConformancePeer("native-quic")
    certificate, key, _ = generate_leaf_certificate()
    configuration = QuicConfiguration(is_client=False, idle_timeout=IDLE_TIMEOUT)
    configuration.certificate = certificate
    configuration.private_key = key
    # `::` takes both families, so `localhost` reaches it whichever address it resolves to.
    server = await serve(
        "::", 0, configuration=configuration, create_protocol=partial(NativeQuicConformanceProtocol, peer=conformance)
    )
    transport = server._transport
    assert transport is not None
    pem = certificate.public_bytes(serialization.Encoding.PEM)

    try:
        yield Peer(conformance, transport.get_extra_info("sockname")[1], pem)
    finally:
        await conformance.dispose()
        server.close()
        await asyncio.sleep(0.05)


async def ready(session: QuicSession, created: ConnectingTransport) -> QuicSession:
    await asyncio.wait_for(asyncio.ensure_future(created.ready), 5)
    return session


async def read_all(stream: ReceiveStream) -> bytes:
    out = bytearray()
    while (chunk := await stream.read()) is not None:
        out += chunk
    return bytes(out)


class TestDial:
    async def test_ta3_dials_the_host_and_port_with_the_host_as_the_server_name(self, peer: Peer) -> None:
        session = await ready(*peer.dial())

        assert peer.peer.observed is not None
        assert peer.peer.observed["serverName"] == SERVER_NAME
        session.close()
        await asyncio.wait_for(session.wait_closed(), 5)

    async def test_ta3_never_reads_the_path_or_query(self, peer: Peer) -> None:
        # A path and a query no URL parser would want to validate: the adapter must not look at them.
        created = connect_quic(peer.url("/%zz/../x?jwt=%%%&&=="), [MOQT], configuration=peer.configuration())
        session = await ready(cast(QuicSession, created.transport), created)

        assert session.protocol_id == MOQT
        session.close()

    async def test_ta5_offers_exactly_the_protocols_as_the_alpn_list_in_order(self, peer: Peer) -> None:
        session = await ready(*peer.dial([MOQT, "moqt-21"]))

        assert peer.peer.observed is not None
        assert peer.peer.observed["protocols"] == [MOQT, "moqt-21"]  # no `h3`
        session.close()

    async def test_ta7_a_peer_that_selects_no_protocol_fails_the_dial(self, peer: Peer) -> None:
        await peer.peer.prepare(select="none")
        session, created = peer.dial()

        with pytest.raises(WebTransportError, match="selected no application protocol"):
            await asyncio.wait_for(asyncio.ensure_future(created.ready), 5)

        with pytest.raises(WebTransportError):
            await asyncio.wait_for(session.wait_closed(), 5)

    async def test_ta7_a_peer_that_selects_an_unoffered_protocol_fails_the_dial(self, peer: Peer) -> None:
        await peer.peer.prepare(select="unoffered")
        session, created = peer.dial()

        with pytest.raises(WebTransportError, match="unoffered application protocol 'moqt-unoffered'"):
            await asyncio.wait_for(asyncio.ensure_future(created.ready), 5)

        assert session.protocol_id is None

    async def test_ta8_protocol_id_is_the_negotiated_alpn(self, peer: Peer) -> None:
        session, created = peer.dial(["moqt-21", MOQT])
        assert session.protocol_id is None
        await ready(session, created)

        assert session.protocol_id == "moqt-21"  # the peer takes the first offered
        session.close()

    async def test_the_factory_declares_native_quic_framing(self, peer: Peer) -> None:
        session, created = peer.dial()

        assert created.framing == "native-quic"
        await ready(session, created)
        session.close()


class TestLifetime:
    async def test_ta9_a_peer_application_close_ends_the_session_cleanly_with_its_code_and_reason(
        self, peer: Peer
    ) -> None:
        session = await ready(*peer.dial())
        stream = await session.create_unidirectional_stream()

        await peer.peer.close(7, "goodbye")
        await asyncio.wait_for(session.wait_closed(), 5)  # returns: a clean end, whatever the code

        assert session.close_info == (7, "goodbye")
        # TA-24: an operation that fails because of the peer's close carries its code and reason.
        with pytest.raises(WebTransportError) as failure:
            await stream.write(b"late")
        assert (failure.value.peer_closed, failure.value.code, failure.value.reason) == (True, 7, "goodbye")

        with pytest.raises(WebTransportError):
            await session.create_unidirectional_stream()

    async def test_ta9_a_quic_layer_close_raises(self, peer: Peer) -> None:
        session = await ready(*peer.dial())

        await peer.peer.quicClose()

        with pytest.raises(WebTransportError) as failure:
            await asyncio.wait_for(session.wait_closed(), 5)
        assert not failure.value.peer_closed
        assert session.close_info is None

    async def test_ta9_a_quic_layer_close_with_no_error_still_raises(self, peer: Peer) -> None:
        session = await ready(*peer.dial())
        protocol = peer.peer._require_protocol()

        # Code 0, but frame type 0x1c: the QUIC layer's close, not the application's.
        protocol._quic.close(error_code=QuicErrorCode.NO_ERROR, frame_type=QuicFrameType.PADDING, reason_phrase="bye")
        protocol.transmit()

        with pytest.raises(WebTransportError):
            await asyncio.wait_for(session.wait_closed(), 5)
        assert session.close_info is None

    async def test_ta9_connection_loss_raises_at_the_idle_timeout(self, peer: Peer) -> None:
        session = await ready(*peer.dial(idle_timeout=0.5))

        await peer.peer.loseConnection()

        with pytest.raises(WebTransportError, match="Idle timeout"):
            await asyncio.wait_for(session.wait_closed(), 5)

    async def test_ta10_close_ends_the_connection_with_an_application_close_carrying_code_and_reason(
        self, peer: Peer
    ) -> None:
        session = await ready(*peer.dial())

        session.close(0x10, "done here")
        session.close(0x11, "again")  # idempotent: the first close wins

        assert await asyncio.wait_for(peer.peer.closed(), 5) == {"closeCode": 0x10, "reason": "done here"}
        await asyncio.wait_for(session.wait_closed(), 5)
        session.close()  # and never raises after the end

    async def test_ta10_close_delivers_what_was_written_before_it_209(self, peer: Peer) -> None:
        session = await ready(*peer.dial())
        stream = await session.create_unidirectional_stream()
        payload = bytes(range(256)) * 400
        await stream.write(payload)
        await stream.close()

        session.close()
        stream_id = await peer.peer.acceptUni()
        read = await asyncio.wait_for(peer.peer.read(stream_id, discard=True), 5)

        assert (read["byteLength"], read["fin"]) == (len(payload), True)
        await asyncio.wait_for(session.wait_closed(), 10)

    async def test_ta11_close_before_ready_cancels_the_dial(self, peer: Peer) -> None:
        await peer.peer.prepare(handshake="hold")
        session, created = peer.dial()
        await wait_for(lambda: peer.peer._protocol is not None, timeout=5)

        session.close()

        with pytest.raises(WebTransportError, match="closed before the session was established"):
            await asyncio.wait_for(asyncio.ensure_future(created.ready), 5)
        with pytest.raises(WebTransportError):
            await asyncio.wait_for(session.wait_closed(), 5)

        await peer.peer.releaseHandshake()
        await asyncio.sleep(0.2)
        assert peer.peer._session is None  # no session was established afterwards

    async def test_a_connection_keeps_itself_alive_158(self, peer: Peer) -> None:
        session = await ready(*peer.dial())
        protocol = session._protocol
        assert protocol is not None

        assert protocol._keepalive is not None
        session.close()
        await asyncio.wait_for(session.wait_closed(), 5)
        assert protocol._keepalive is None

    async def test_a_closed_port_fails_the_dial_at_once_163(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]

        created = connect_quic(f"https://127.0.0.1:{port}/moq", [MOQT])
        loop = asyncio.get_running_loop()
        started = loop.time()

        with pytest.raises(WebTransportError) as failure:
            await asyncio.wait_for(asyncio.ensure_future(created.ready), 5)

        assert isinstance(failure.value.__cause__, ConnectionRefusedError)
        assert loop.time() - started < 2


class TestStreams:
    async def test_ta13_a_peer_stream_arrives_with_its_bytes_in_order_then_the_fin(self, peer: Peer) -> None:
        session = await ready(*peer.dial())
        stream_id = await peer.peer.openUni()
        await peer.peer.write(stream_id, [1, 2, 3])
        await peer.peer.write(stream_id, [4, 5])
        await peer.peer.finish(stream_id)

        incoming = await asyncio.wait_for(anext(aiter(session.incoming_unidirectional_streams())), 5)

        assert await asyncio.wait_for(read_all(incoming), 5) == bytes([1, 2, 3, 4, 5])
        session.close()

    async def test_ta13_a_bidirectional_stream_carries_both_ways(self, peer: Peer) -> None:
        session = await ready(*peer.dial())
        stream = await session.create_bidirectional_stream()
        await stream.writable.write(b"ping")
        stream_id = await peer.peer.acceptBi()

        assert (await peer.peer.read(stream_id, count=4))["bytes"] == list(b"ping")
        await peer.peer.write(stream_id, list(b"pong"))
        await peer.peer.finish(stream_id)
        assert await asyncio.wait_for(read_all(stream.readable), 5) == b"pong"
        session.close()

    async def test_ta15_close_is_acknowledged_only_once_the_peer_has_the_whole_stream(self, peer: Peer) -> None:
        session = await ready(*peer.dial())
        await peer.peer.pause()
        stream = cast(Any, await session.create_unidirectional_stream())
        await stream.write(b"data")
        await stream.close()  # the FIN is handed over
        acknowledged = asyncio.ensure_future(stream.wait_acknowledged())
        await asyncio.sleep(0.3)

        assert not acknowledged.done()  # the peer has acknowledged nothing
        await peer.peer.resume()
        await asyncio.wait_for(acknowledged, 5)
        session.close()

    async def test_ta16_ta17_abort_resets_with_the_reasons_code_after_the_fin_went_out(self, peer: Peer) -> None:
        session = await ready(*peer.dial())
        stream = cast(Any, await session.create_unidirectional_stream())
        await stream.write(b"x")
        stream_id = await peer.peer.acceptUni()
        await peer.peer.pause()
        await stream.close()
        acknowledged = asyncio.ensure_future(stream.wait_acknowledged())

        reason = type("Reason", (Exception,), {"stream_error_code": 0x12})()
        stream.abort(reason)
        await peer.peer.resume()

        with pytest.raises(Exception) as failure:
            await asyncio.wait_for(acknowledged, 5)
        assert failure.value is reason
        read = await asyncio.wait_for(peer.peer.read(stream_id), 5)
        assert (read["fin"], read["resetCode"]) == (False, 0x12)
        session.close()

    async def test_ta17_a_code_out_of_range_resets_with_0(self, peer: Peer) -> None:
        session = await ready(*peer.dial())
        stream = await session.create_unidirectional_stream()
        await stream.write(b"x")
        stream_id = await peer.peer.acceptUni()

        stream.abort(type("Reason", (Exception,), {"stream_error_code": 2**32})())

        assert (await asyncio.wait_for(peer.peer.read(stream_id), 5))["resetCode"] == 0
        session.close()

    async def test_ta18_cancel_sends_stop_sending_with_the_reasons_code(self, peer: Peer) -> None:
        session = await ready(*peer.dial())
        stream_id = await peer.peer.openUni()
        await peer.peer.write(stream_id, [1])
        incoming = await asyncio.wait_for(anext(aiter(session.incoming_unidirectional_streams())), 5)

        incoming.cancel(9)

        assert await asyncio.wait_for(peer.peer.stopped(stream_id), 5) == 9
        assert await incoming.read() is None
        session.close()

    async def test_ta19_a_peer_reset_fails_the_receive_stream_with_its_code(self, peer: Peer) -> None:
        session = await ready(*peer.dial())
        stream_id = await peer.peer.openUni()
        await peer.peer.write(stream_id, [1])
        incoming = await asyncio.wait_for(anext(aiter(session.incoming_unidirectional_streams())), 5)
        assert await incoming.read() == b"\x01"

        await peer.peer.reset(stream_id, 0xABCD)

        with pytest.raises(WebTransportError) as failure:
            await asyncio.wait_for(incoming.read(), 5)
        assert failure.value.code == 0xABCD  # as it was on the wire: no HTTP/3 mapping
        session.close()

    async def test_ta13_ta19_a_peer_stream_that_opens_with_a_reset_still_arrives(self, peer: Peer) -> None:
        session = await ready(*peer.dial())
        stream_id = await peer.peer.openBi()  # nothing is sent on it yet

        await peer.peer.reset(stream_id, 5)

        incoming = await asyncio.wait_for(anext(aiter(session.incoming_bidirectional_streams())), 5)
        with pytest.raises(WebTransportError) as failure:
            await asyncio.wait_for(incoming.readable.read(), 5)
        assert failure.value.code == 5
        # The other half is still the client's to use.
        await incoming.writable.write(b"still here")
        await incoming.writable.close()
        assert (await asyncio.wait_for(peer.peer.read(stream_id), 5))["bytes"] == list(b"still here")
        session.close()

    async def test_ta13_ta20_a_peer_stream_that_opens_with_a_stop_still_arrives(self, peer: Peer) -> None:
        session = await ready(*peer.dial())
        stream_id = await peer.peer.openBi()

        await peer.peer.stop(stream_id, 6)

        incoming = await asyncio.wait_for(anext(aiter(session.incoming_bidirectional_streams())), 5)
        with pytest.raises(StreamStopped) as failure:
            await incoming.writable.write(b"x")
        assert failure.value.code == 6
        session.close()

    async def test_ta20_a_peer_stop_fails_the_next_write_with_its_code(self, peer: Peer) -> None:
        session = await ready(*peer.dial())
        stream = await session.create_unidirectional_stream()
        await stream.write(b"x")
        stream_id = await peer.peer.acceptUni()

        await peer.peer.stop(stream_id, 6)
        await asyncio.sleep(0.2)

        with pytest.raises(StreamStopped) as failure:
            await stream.write(b"y")
        assert failure.value.code == 6
        session.close()

    async def test_ta21_creates_reject_at_once_without_credit_and_succeed_once_it_returns(self, peer: Peer) -> None:
        await peer.peer.prepare(maxStreams=2)
        session = await ready(*peer.dial())
        first = await session.create_unidirectional_stream()
        await session.create_unidirectional_stream()

        with pytest.raises(WebTransportError, match="unidirectional stream limit"):
            await asyncio.wait_for(session.create_unidirectional_stream(), 1)
        await asyncio.wait_for(session.create_bidirectional_stream(), 1)  # the other kind has its own credit

        await first.write(b"x")
        await first.close()
        first_id = await peer.peer.acceptUni()
        await asyncio.wait_for(peer.peer.read(first_id), 5)  # read to its FIN: one stream of credit returns

        await wait_for(lambda: session._protocol is not None and session._protocol._quic._remote_max_streams_uni == 3)
        await asyncio.wait_for(session.create_unidirectional_stream(), 1)
        session.close()


class TestRelayConnection:
    async def test_ta6_client_setup_carries_the_endpoint_path_and_authority(self, peer: Peer) -> None:
        connecting = asyncio.ensure_future(
            connect_relay_subscriber(
                f"https://{SERVER_NAME}:{peer.port}/moq", get_token=lambda: "a token", create_transport=peer.factory()
            )
        )
        control = await asyncio.wait_for(peer.peer.acceptUni(), 5)
        options = await asyncio.wait_for(read_client_setup(StreamReader(peer.peer._stream(control).receive)), 5)

        assert option_value(options, SETUP_OPTION.PATH) == "/moq?jwt=a+token"
        assert option_value(options, SETUP_OPTION.AUTHORITY) == f"{SERVER_NAME}:{peer.port}"
        connecting.cancel()
        await asyncio.wait({connecting})

    @pytest.mark.parametrize("role", ["subscriber", "publisher"])
    async def test_ta24_a_peer_close_with_unauthorized_reports_unauthorized(self, peer: Peer, role: str) -> None:
        async def refuse() -> None:
            # Like moq-relay: read the client's SETUP, then close the connection with UNAUTHORIZED (0x2).
            await peer.peer.acceptUni()
            await peer.peer.close(0x2, "unauthorized")

        refusing = asyncio.ensure_future(refuse())
        url = peer.url("/moq")

        with pytest.raises(RelayError) as failure:
            if role == "subscriber":
                await connect_relay_subscriber(url, create_transport=peer.factory(), connect_timeout=5)
            else:
                await connect_relay_publisher(
                    url, namespace=NAMESPACE, create_transport=peer.factory(), connect_timeout=5
                )

        await refusing
        error = failure.value
        assert (error.code, error.phase) == ("UNAUTHORIZED", "connecting")
        assert str(error) == "Relay refused the connection's credentials: unauthorized (session closed with code 0x2)"

    async def test_a_peer_close_with_another_code_before_setup_is_a_transport_error(self, peer: Peer) -> None:
        async def close() -> None:
            await peer.peer.acceptUni()
            await peer.peer.close(0x3, "protocol violation")

        closing = asyncio.ensure_future(close())

        with pytest.raises(RelayError) as failure:
            await connect_relay_subscriber(peer.url("/moq"), create_transport=peer.factory(), connect_timeout=5)

        await closing
        assert (failure.value.code, failure.value.phase) == ("TRANSPORT_ERROR", "connecting")


# ---------------------------------------------------------------------- the relay hub over native QUIC


class _HubProtocol(QuicConnectionProtocol):
    """One accepted native QUIC connection, served by the relay hub in one role."""

    def __init__(
        self,
        quic: QuicConnection,
        stream_handler: Any = None,
        *,
        hub: RelayHub,
        role: str,
        sessions: list[NativeQuicSession],
    ) -> None:
        super().__init__(quic, stream_handler)
        self.session = NativeQuicSession(self)
        self._hub = hub
        self._role = role
        self._sessions = sessions

    def quic_event_received(self, event: events.QuicEvent) -> None:
        if isinstance(event, events.HandshakeCompleted):
            self._sessions.append(self.session)
            if self._role == "publisher":
                self._hub.serve_publisher(cast(Any, self.session))
            else:
                self._hub.serve_subscriber(cast(Any, self.session))
        self.session.on_event(event)


@dataclass
class HubServer:
    port: int
    certificate_pem: bytes
    sessions: list[NativeQuicSession]
    server: Any

    def factory(self):
        def create_transport(url: str, protocols: Sequence[str]) -> ConnectingTransport:
            return connect_quic(url, protocols, configuration=QuicConfiguration(cadata=self.certificate_pem))

        return create_transport

    def url(self) -> str:
        return f"https://{SERVER_NAME}:{self.port}/moq"


async def start_hub_server(hub: RelayHub, role: str) -> HubServer:
    certificate, key, _ = generate_leaf_certificate()
    configuration = QuicConfiguration(is_client=False, alpn_protocols=[MOQT], idle_timeout=IDLE_TIMEOUT)
    configuration.certificate = certificate
    configuration.private_key = key
    sessions: list[NativeQuicSession] = []
    server = await serve(
        "::",
        0,
        configuration=configuration,
        create_protocol=partial(_HubProtocol, hub=hub, role=role, sessions=sessions),
    )
    transport = server._transport
    assert transport is not None
    pem = certificate.public_bytes(serialization.Encoding.PEM)
    return HubServer(transport.get_extra_info("sockname")[1], pem, sessions, server)


@pytest.fixture
async def hub() -> AsyncIterator[tuple[RelayHub, HubServer, HubServer]]:
    relay = create_relay_hub()
    publishers = await start_hub_server(relay, "publisher")
    subscribers = await start_hub_server(relay, "subscriber")

    try:
        yield relay, publishers, subscribers
    finally:
        relay.destroy()
        publishers.server.close()
        subscribers.server.close()
        await asyncio.sleep(0.05)


class TestOverTheRelayHub:
    async def test_publishes_and_subscribes_one_track_over_native_quic(
        self, hub: tuple[RelayHub, HubServer, HubServer]
    ) -> None:
        _, publishers, subscribers = hub
        publisher = await asyncio.wait_for(
            connect_relay_publisher(publishers.url(), namespace=NAMESPACE, create_transport=publishers.factory()), 10
        )
        subscriber = await asyncio.wait_for(
            connect_relay_subscriber(subscribers.url(), create_transport=subscribers.factory()), 10
        )
        received: list[bytes] = []

        def on_object(obj: MoqtObject) -> None:
            if obj.status == "normal":
                received.append(obj.payload)

        accepted: list[str] = []
        subscriber.subscribe(
            NAMESPACE,
            "track",
            RelaySubscriptionHandlers(on_subscribed=lambda ok: accepted.append("track"), on_object=on_object),
        )
        await wait_for(lambda: accepted == ["track"], timeout=10)
        track = publisher.register_track("track", write_timeout=5)

        for payload in (b"object 0", b"", bytes([0, 255])):
            await track.write(payload)

        await wait_for(lambda: len(received) == 3, timeout=10)
        assert sorted(received) == sorted([b"object 0", b"", bytes([0, 255])])

        await asyncio.wait_for(publisher.close(), 10)
        subscriber.close()
        assert await publisher.closed == Closed()
        assert await subscriber.closed == Closed()

    async def test_ta24_a_peer_close_with_unauthorized_after_setup_reports_unauthorized(
        self, hub: tuple[RelayHub, HubServer, HubServer]
    ) -> None:
        _, _, subscribers = hub
        subscriber = await asyncio.wait_for(
            connect_relay_subscriber(subscribers.url(), create_transport=subscribers.factory()), 10
        )

        subscribers.sessions[0].close(0x2, "unauthorized")
        closed: Closed = await asyncio.wait_for(subscriber.closed, 5)

        assert isinstance(closed.error, RelayError)
        assert (closed.error.code, closed.error.phase) == ("UNAUTHORIZED", "connection")
