"""The aioquic WebTransport adapter: wire helpers, and the whole SDK over a real QUIC loopback.

The loopback test is the offline counterpart of the live smoke test in ``scripts/test_relay.py``: it
runs the same assertion list from ``spec/behavior.md`` against the in-memory relay hub served over
real WebTransport sessions on 127.0.0.1, so the transport adapter is exercised end to end.
"""

import asyncio
from collections.abc import AsyncIterator, Sequence
from typing import Any

import pytest
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.h3.connection import H3Connection, H3Stream
from aioquic.h3.events import HeadersReceived, WebTransportStreamDataReceived
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.connection import QuicConnection
from aioquic.quic.packet_builder import QuicDeliveryState
from aioquic.quic.rangeset import RangeSet
from aioquic.quic.stream import QuicStream, QuicStreamReceiver, QuicStreamSender
from core_helpers.waiting import wait_for
from relay_helpers.relay_hub import RelayHub, create_relay_hub
from relay_helpers.wt_server import (
    LoopbackWebTransportServer,
    WebTransportServerSession,
    start_webtransport_server,
)

from moq_core import MoqtObject, SubscriptionHandlers
from moq_core.transport import ReceiveStream, SendStream
from mux_relay import Closed, ConnectingTransport, connect_relay, connect_relay_publisher
from mux_relay.webtransport import (
    CAPSULE_CLOSE_WEBTRANSPORT_SESSION,
    WebTransportError,
    WebTransportSession,
    WebTransportSessionBase,
    _open_socket,
    _parse_url,
    connect_webtransport,
    encode_close_session_capsule,
    h3_to_webtransport_error_code,
    parse_sf_list_of_strings,
    parse_sf_string,
    sf_list_of_strings,
    webtransport_to_h3_error_code,
)

NAMESPACE = ["loopback", "data"]


class TestWireHelpers:
    def test_error_codes_round_trip_through_the_reserved_h3_range(self) -> None:
        for code in (0, 1, 0x1D, 0x1E, 0x1F, 12345, 0xFFFFFFFF):
            mapped = webtransport_to_h3_error_code(code)
            assert h3_to_webtransport_error_code(mapped) == code

        # The draft's worked example: application code 0 sits at the start of the range.
        assert webtransport_to_h3_error_code(0) == 0x52E4A40FA8DB
        assert h3_to_webtransport_error_code(0x100) is None
        # Every 31st value in the range is skipped (a GREASE code point), so it maps to nothing.
        assert h3_to_webtransport_error_code(0x52E4A40FA8DB + 0x1E) is None

    def test_structured_field_lists_of_strings(self) -> None:
        assert sf_list_of_strings(["moqt-20"]) == b'"moqt-20"'
        assert sf_list_of_strings(["a", 'b"c']) == b'"a", "b\\"c"'
        assert parse_sf_list_of_strings(b'"moqt-20", "moqt-19"') == ["moqt-20", "moqt-19"]
        assert parse_sf_string(b'"moqt-20"') == "moqt-20"
        assert parse_sf_string(b"moqt-20") == "moqt-20"

    def test_parses_endpoints_into_connect_headers(self) -> None:
        endpoint = _parse_url("https://sjc.relay.mux.global")
        assert (endpoint.host, endpoint.port, endpoint.authority, endpoint.path) == (
            "sjc.relay.mux.global",
            443,
            "sjc.relay.mux.global",
            "/",
        )

        endpoint = _parse_url("https://127.0.0.1:4443/moq?jwt=abc")
        assert (endpoint.authority, endpoint.path) == ("127.0.0.1:4443", "/moq?jwt=abc")

        with pytest.raises(ValueError, match="HTTPS"):
            _parse_url("http://relay.example.com")

    async def test_close_capsule_from_the_peer_ends_the_session_cleanly(self) -> None:
        session = WebTransportSessionBase()
        capsule = encode_close_session_capsule(7, "bye")

        assert capsule[:2] == bytes([0x68, 0x43])  # 0x2843 as a two-byte QUIC varint
        # Split across two DATA frames: the capsule parser buffers partial input.
        session._on_connect_data(capsule[:3], False)
        assert not session._closed.done()
        session._on_connect_data(capsule[3:], False)
        assert session.close_info == (7, "bye")
        await session.wait_closed()

    def test_the_close_capsule_type_is_the_registered_value(self) -> None:
        assert CAPSULE_CLOSE_WEBTRANSPORT_SESSION == 0x2843

    def test_socket_fallback_picks_an_ipv4_address_when_ipv6_sockets_are_unavailable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import socket as socket_module

        real_socket = socket_module.socket

        def no_ipv6(family: int = socket_module.AF_INET, *args: Any, **kwargs: Any) -> Any:
            if family == socket_module.AF_INET6:
                raise OSError("IPv6 is unavailable")

            return real_socket(family, *args, **kwargs)

        monkeypatch.setattr(socket_module, "socket", no_ipv6)
        # The resolver put an IPv6 address first; the IPv4 socket must not be pointed at it.
        sock, address = _open_socket([("2001:db8::1", 443, 0, 0), ("192.0.2.1", 443)])

        try:
            assert sock.family == socket_module.AF_INET
            assert address == ("192.0.2.1", 443)
        finally:
            sock.close()

        with pytest.raises(WebTransportError, match="only to IPv6"):
            _open_socket([("2001:db8::1", 443, 0, 0)])

    def test_dual_stack_socket_maps_ipv4_addresses(self) -> None:
        try:
            sock, address = _open_socket([("192.0.2.1", 443)])
        except WebTransportError:
            pytest.skip("no IPv6 socket support on this host")

        try:
            assert address == ("::ffff:192.0.2.1", 443, 0, 0)
        finally:
            sock.close()


@pytest.fixture
async def loopback() -> AsyncIterator[tuple[LoopbackWebTransportServer, RelayHub, list[WebTransportServerSession]]]:
    hub = create_relay_hub()
    sessions: list[WebTransportServerSession] = []

    def on_session(session: WebTransportServerSession) -> None:
        sessions.append(session)

        if session.path.startswith("/publish"):
            hub.serve_publisher(session)
        else:
            hub.serve_subscriber(session)

    server = await start_webtransport_server(on_session)

    try:
        yield server, hub, sessions
    finally:
        hub.destroy()
        server.close()
        await asyncio.sleep(0.05)


def _hub_on_session(hub: RelayHub):
    def on_session(session: WebTransportServerSession) -> None:
        if session.path.startswith("/publish"):
            hub.serve_publisher(session)
        else:
            hub.serve_subscriber(session)

    return on_session


def factory_for(server: LoopbackWebTransportServer):
    def create_transport(url: str, protocols: Sequence[str]) -> ConnectingTransport:
        return connect_webtransport(url, protocols, configuration=server.client_configuration())

    return create_transport


class TestLoopback:
    async def test_negotiates_the_moqt_protocol_over_a_real_webtransport_session(
        self, loopback: tuple[LoopbackWebTransportServer, RelayHub, list[WebTransportServerSession]]
    ) -> None:
        server, _, sessions = loopback
        created = connect_webtransport(
            server.url_for("/subscribe?jwt=test"), ["moqt-20"], configuration=server.client_configuration()
        )
        await asyncio.wait_for(created.ready, 5)
        transport: Any = created.transport

        assert transport.protocol_id == "moqt-20"
        await wait_for(lambda: len(sessions) == 1)
        assert sessions[0].path == "/subscribe?jwt=test"
        assert sessions[0].protocol_id == "moqt-20"

        transport.close()
        await asyncio.wait_for(transport.wait_closed(), 5)
        # The peer observed the CLOSE_WEBTRANSPORT_SESSION capsule as a clean end.
        await asyncio.wait_for(sessions[0].wait_closed(), 5)
        assert sessions[0].close_info == (0, "")

    async def test_runs_the_smoke_test_assertion_list_over_quic(
        self, loopback: tuple[LoopbackWebTransportServer, RelayHub, list[WebTransportServerSession]]
    ) -> None:
        server, hub, _ = loopback
        create_transport = factory_for(server)

        # 1. A publisher connection and a subscriber connection both open.
        publisher = await asyncio.wait_for(
            connect_relay_publisher(server.url_for("/publish"), namespace=NAMESPACE, create_transport=create_transport),
            10,
        )
        subscriber = await asyncio.wait_for(
            connect_relay(server.url_for("/subscribe"), create_transport=create_transport), 10
        )
        closing = False
        problems: list[str] = []

        def observe(label: str, closed: asyncio.Future[Closed]) -> None:
            def done(future: asyncio.Future[Closed]) -> None:
                if not closing:
                    problems.append(f"{label} closed: {future.result().error!r}")

            closed.add_done_callback(done)

        observe("publisher", publisher.closed)
        observe("subscriber", subscriber.closed)

        try:
            events = publisher.publish("events", write_timeout=5)
            status = publisher.publish("status", write_timeout=5)
            received: dict[str, list[bytes]] = {"events": [], "status": []}
            accepted: list[str] = []

            def subscribe(track: str) -> None:
                def on_object(obj: MoqtObject) -> None:
                    if obj.status == "normal":
                        received[track].append(obj.payload)

                subscriber.subscribe(
                    NAMESPACE,
                    track,
                    SubscriptionHandlers(
                        on_ok=lambda ok: accepted.append(track),
                        on_object=on_object,
                        on_error=lambda error: problems.append(f"{track}: {error.error_code} {error.reason}"),
                        on_subgroup_reset=lambda reset: problems.append(f"{track} reset: {reset.error!r}"),
                    ),
                )

            subscribe("events")
            subscribe("status")
            # 2. Subscriptions to two tracks on the namespace are accepted.
            await wait_for(lambda: sorted(accepted) == ["events", "status"], timeout=10)

            # 3. Binary, empty, and text payloads all arrive intact.
            expected_events = [bytes([0, 255, 128, 1]), b"hello from Relay SDK", b""]

            for payload in expected_events:
                await events.write(payload)

            await status.write(bytes([42]))
            await wait_for(lambda: len(received["events"]) == 3 and len(received["status"]) == 1, timeout=10)

            # 4. Closing one track writer does not affect delivery on another track.
            events.close()
            await status.write(bytes([43]))
            await wait_for(lambda: len(received["status"]) == 2, timeout=10)

            assert sorted(received["events"]) == sorted(expected_events)
            assert received["status"] == [bytes([42]), bytes([43])]
            assert problems == []
            assert hub.subscribes.count("events") >= 1
        finally:
            closing = True
            # 5. Both connections close cleanly once the test closes them.
            await asyncio.wait_for(publisher.close(), 10)
            subscriber.close()

        assert await publisher.closed == Closed()
        assert await subscriber.closed == Closed()

    async def test_closing_a_session_whose_connect_is_unanswered_fails_ready_at_once(self) -> None:
        requests: list[dict[bytes, bytes]] = []
        server = await start_webtransport_server(
            lambda session: None, respond_to_connect=False, on_request=requests.append
        )

        try:
            created = connect_webtransport(
                server.url_for("/hung"), ["moqt-20"], configuration=server.client_configuration()
            )
            await wait_for(lambda: len(requests) == 1, timeout=5)  # the CONNECT reached the server; no answer is coming

            created.transport.close()

            with pytest.raises(WebTransportError, match="closed before the session was established"):
                await asyncio.wait_for(created.ready, 2)

            await asyncio.wait_for(created.transport.wait_closed(), 2)
        finally:
            server.close()

    async def test_closing_an_unanswered_connect_rejects_ready_before_the_drain(self) -> None:
        requests: list[dict[bytes, bytes]] = []
        server = await start_webtransport_server(
            lambda session: None, respond_to_connect=False, on_request=requests.append
        )

        try:
            created = connect_webtransport(
                server.url_for("/hung"), ["moqt-20"], configuration=server.client_configuration()
            )
            await wait_for(lambda: len(requests) == 1, timeout=5)  # the CONNECT reached the server; no answer yet

            session = created.transport
            assert isinstance(session, WebTransportSession)
            session.close()

            # In the same turn as close(), not once the connection drains: a peer that stopped
            # acknowledging would otherwise hold the handshake for the whole drain bound.
            assert session.ready.done()

            with pytest.raises(WebTransportError, match="closed before the session was established"):
                session.ready.result()

            # A 2xx that crossed with the close must not revive a session that already rejects writes.
            session._on_connect_headers(
                HeadersReceived(
                    headers=[(b":status", b"200"), (b"sec-webtransport-http3-draft", b"draft02")],
                    stream_id=session.session_id,
                    stream_ended=False,
                )
            )

            with pytest.raises(WebTransportError, match="closed before the session was established"):
                session.ready.result()

            with pytest.raises(WebTransportError, match="WebTransport session closed"):
                await session.create_unidirectional_stream()

            await asyncio.wait_for(session.wait_closed(), 5)
        finally:
            server.close()

    async def test_a_connection_dropped_before_the_connect_answer_fails_ready(self) -> None:
        requests: list[dict[bytes, bytes]] = []
        server = await start_webtransport_server(
            lambda session: None, respond_to_connect=False, on_request=requests.append
        )
        created = connect_webtransport(
            server.url_for("/hung"), ["moqt-20"], configuration=server.client_configuration()
        )
        await wait_for(lambda: len(requests) == 1, timeout=5)

        # The server goes away: the QUIC connection terminates with an error, and so must `ready`.
        server.close()

        with pytest.raises(WebTransportError):
            await asyncio.wait_for(created.ready, 5)

    async def test_reports_a_rejected_connect(self) -> None:
        # A plain HTTPS/QUIC server that is not there: the handshake never completes.
        created = connect_webtransport("https://127.0.0.1:9/", ["moqt-20"])

        with pytest.raises(WebTransportError, match="closed before the session was established"):
            created.transport.close()
            await created.ready


async def _drain_uni_streams(session: Any, into: list[int]) -> None:
    """Read every incoming unidirectional stream to its end, recording the byte count of each."""
    readers: list[asyncio.Task[None]] = []

    async def read_all(stream: ReceiveStream) -> None:
        total = 0
        while (chunk := await stream.read()) is not None:
            total += len(chunk)
        into.append(total)

    async for stream in session.incoming_unidirectional_streams():
        readers.append(asyncio.ensure_future(read_all(stream)))


class TestSendBackpressure:
    """The send half applies real transport backpressure: #3, #30, #5, #4.

    Each test drives the real aioquic stream code over a loopback QUIC connection, the offline
    counterpart of the live checks in the issues.
    """

    async def _connect(self, server: LoopbackWebTransportServer) -> Any:
        created = connect_webtransport(
            server.url_for("/sink"), ["moqt-20"], configuration=server.client_configuration()
        )
        await asyncio.wait_for(created.ready, 5)
        return created.transport

    async def test_write_suspends_and_the_deadline_fires_when_the_link_is_slow(self) -> None:
        # Behind a slow uplink the send half must block (the congestion window fills and the write
        # cannot drain), so a deadline around the write fires instead of it returning at once (#3, #30).
        # (aioquic's own receiver keeps growing the flow-control window as data arrives whether or not
        # the application reads it, so a slow *link*, not a slow reader, is what models backpressure.)
        received: list[int] = []
        sessions: list[Any] = []
        drainers: list[asyncio.Task[None]] = []

        def on_session(session: Any) -> None:
            sessions.append(session)
            drainers.append(asyncio.ensure_future(_drain_uni_streams(session, received)))

        server = await start_webtransport_server(on_session)
        transport = await self._connect(server)
        await wait_for(lambda: len(sessions) == 1, timeout=5)
        sessions[0].impair(rate=64_000)  # 64 kB/s: about 15 s per MiB, far past the deadline below

        try:
            stream: SendStream = await transport.create_unidirectional_stream()

            with pytest.raises(TimeoutError):
                for _ in range(8):
                    await asyncio.wait_for(stream.write(b"\0" * (1024 * 1024)), 0.3)

            # The bytes are still queued in the client: nothing was lost, the write is simply not done.
            quic = transport._protocol._quic
            assert sum(len(s.sender._buffer) for s in quic._streams.values()) > 0
        finally:
            transport.close()
            await asyncio.wait_for(transport.wait_closed(), 5)
            server.close()
            await asyncio.sleep(0.05)

    async def test_a_write_to_a_reading_peer_completes_promptly(self) -> None:
        # The positive control: when the peer reads, the same write settles quickly.
        received: list[int] = []
        drainers: list[asyncio.Task[None]] = []

        def on_session(session: Any) -> None:
            drainers.append(asyncio.ensure_future(_drain_uni_streams(session, received)))

        server = await start_webtransport_server(on_session)
        transport = await self._connect(server)

        try:
            stream = await transport.create_unidirectional_stream()
            await asyncio.wait_for(stream.write(b"\0" * (1024 * 1024)), 5)
            await asyncio.wait_for(stream.close(), 5)
            await wait_for(lambda: received == [1024 * 1024], timeout=5)
        finally:
            transport.close()
            await asyncio.wait_for(transport.wait_closed(), 5)
            server.close()
            await asyncio.sleep(0.05)

    async def test_close_drains_queued_data_before_the_connection_goes(self) -> None:
        # A slow uplink: before the fix, write() returned at once and close() tore the connection down
        # with the bytes still queued, so the peer saw a reset. Now write() paces to the link and
        # close() drains what it accepted, so every byte reaches the peer (#30).
        received: list[int] = []
        sessions: list[Any] = []
        drainers: list[asyncio.Task[None]] = []

        def on_session(session: Any) -> None:
            sessions.append(session)
            drainers.append(asyncio.ensure_future(_drain_uni_streams(session, received)))

        server = await start_webtransport_server(on_session)
        transport = await self._connect(server)
        await wait_for(lambda: len(sessions) == 1, timeout=5)
        sessions[0].impair(rate=200_000)  # 200 kB/s: 512 KiB takes ~2.5 s, past the old drain window

        payload = 512 * 1024
        stream = await transport.create_unidirectional_stream()
        await asyncio.wait_for(stream.write(b"\7" * payload), 20)
        await asyncio.wait_for(stream.close(), 20)
        transport.close()
        await asyncio.wait_for(transport.wait_closed(), 20)
        await wait_for(lambda: received == [payload], timeout=20)
        server.close()
        await asyncio.sleep(0.05)

    async def test_per_stream_state_is_pruned_as_streams_finish(self) -> None:
        # Every single-object write is one stream; finished streams must leave no permanent per-stream
        # state in the adapter's maps or in aioquic's live stream table (#4).
        hub = create_relay_hub()
        server = await start_webtransport_server(_hub_on_session(hub))
        create_transport = factory_for(server)
        publisher = await connect_relay_publisher(
            server.url_for("/publish"), namespace=NAMESPACE, create_transport=create_transport
        )
        subscriber = await connect_relay(server.url_for("/subscribe"), create_transport=create_transport)

        try:
            track = publisher.publish("events", write_timeout=10)
            received = 0

            def on_object(_: MoqtObject) -> None:
                nonlocal received
                received += 1

            subscriber.subscribe(NAMESPACE, "events", SubscriptionHandlers(on_object=on_object))
            await wait_for(lambda: track.bound, timeout=10)

            session: Any = publisher._lifetime._transport
            protocol = session._protocol
            baseline_sends = len(session._sends)
            baseline_owners = len(protocol.stream_owners)
            baseline_quic = len(protocol._quic._streams)
            baseline_h3 = len(protocol.h3._stream)

            count = 200

            for _ in range(count):
                await track.write(b"x" * 4096)

            await wait_for(lambda: received == count, timeout=30)

            # A handful of streams may still be settling their FIN acks after the peer has read
            # everything, and pruning only follows the acknowledgement; the point is the maps do not
            # grow with the number of groups ever sent, so they stay near the pre-write baseline.
            def maps_are_back_at_baseline() -> bool:
                return (
                    len(session._sends) <= baseline_sends + 3
                    and len(protocol.stream_owners) <= baseline_owners + 3
                    and len(protocol._quic._streams) <= baseline_quic + 3
                    and len(protocol.h3._stream) <= baseline_h3 + 3
                )

            await wait_for(maps_are_back_at_baseline, timeout=10)
        finally:
            await asyncio.wait_for(publisher.close(), 10)
            subscriber.close()
            hub.destroy()
            server.close()
            await asyncio.sleep(0.05)

    async def test_many_concurrent_slow_streams_drain_after_the_link_heals(self) -> None:
        # Hundreds of blocked single-object streams at once used to strand some forever; when the link
        # heals the whole queue must drain (#5).
        received: list[int] = []
        sessions: list[Any] = []
        drainers: list[asyncio.Task[None]] = []

        def on_session(session: Any) -> None:
            sessions.append(session)
            drainers.append(asyncio.ensure_future(_drain_uni_streams(session, received)))

        server = await start_webtransport_server(on_session)
        transport = await self._connect(server)
        await wait_for(lambda: len(sessions) == 1, timeout=5)
        impairment = sessions[0].impair(delay=0.05, jitter=0.02)
        quic = transport._protocol._quic

        async def one(index: int) -> None:
            stream = await transport.create_unidirectional_stream()
            await stream.write(index.to_bytes(4, "big") + b"\0" * 4092)
            await stream.close()

        count = 80
        writes = [asyncio.ensure_future(one(index)) for index in range(count)]
        await asyncio.sleep(0.5)  # let many streams pile up behind the delay
        impairment.delay = 0.0
        impairment.jitter = 0.0
        await asyncio.wait_for(asyncio.gather(*writes), 20)
        await wait_for(lambda: len(received) == count, timeout=20)
        # Nothing is stranded in the client's send buffers once the link is clean. aioquic holds a
        # stream's bytes until they are acknowledged, so this lags the peer having read them all.
        await wait_for(lambda: sum(len(s.sender._buffer) for s in quic._streams.values()) == 0, timeout=5)

        transport.close()
        await asyncio.wait_for(transport.wait_closed(), 5)
        server.close()
        await asyncio.sleep(0.05)

    async def test_a_retransmitted_fin_on_a_bidirectional_stream_is_not_a_new_stream(self) -> None:
        # The peer opens a bidirectional stream and FINs its half while our half still has a write
        # queued. aioquic reports a retransmitted FIN as another end-of-stream event; that must not
        # queue a second incoming stream or replace the send half whose write is still pending.
        sessions: list[Any] = []
        echoed: list[int] = []
        tasks: list[asyncio.Task[None]] = []

        def on_session(session: Any) -> None:
            sessions.append(session)

            async def open_fin_then_read_the_reply() -> None:
                pair = await session.create_bidirectional_stream()
                await pair.writable.write(b"hello")
                await pair.writable.close()
                total = 0
                while (chunk := await pair.readable.read()) is not None:
                    total += len(chunk)
                echoed.append(total)

            tasks.append(asyncio.ensure_future(open_fin_then_read_the_reply()))

        server = await start_webtransport_server(on_session)
        transport = await self._connect(server)
        pair = await asyncio.wait_for(anext(transport.incoming_bidirectional_streams()), 5)
        assert await pair.readable.read() == b"hello"
        assert await pair.readable.read() is None  # the peer's FIN
        stream_id: int = pair.writable._stream_id
        await wait_for(lambda: len(sessions) == 1, timeout=5)

        sessions[0].impair(paused=True)  # our bytes stop leaving, so the write below stays pending
        pending = asyncio.ensure_future(pair.writable.write(b"\5" * (256 * 1024)))
        await asyncio.sleep(0.1)
        assert not pending.done()

        # Exactly what aioquic emits for a retransmitted FIN: its receiver reports end-of-stream again.
        session: Any = transport
        session._on_stream_data(
            WebTransportStreamDataReceived(
                stream_id=stream_id, data=b"", session_id=session.session_id, stream_ended=True
            )
        )

        assert len(transport.incoming_bidirectional_streams()._items) == 0  # no phantom second stream
        assert session._sends[stream_id] is pair.writable  # the open send half was not replaced
        assert not pending.done()

        sessions[0].impair()
        await asyncio.wait_for(pending, 20)  # the pending write still settles
        await asyncio.wait_for(pair.writable.close(), 20)
        await wait_for(lambda: echoed == [256 * 1024], timeout=20)
        transport.close()
        await asyncio.wait_for(transport.wait_closed(), 5)
        server.close()
        await asyncio.sleep(0.05)

    async def test_a_cancelled_write_does_not_poison_the_stream_for_other_writers(self) -> None:
        # Giving up on a blocked write (a deadline, a cancelled task) must not cancel the transmit
        # future that concurrent and later writers on the stream share: the second writer keeps
        # waiting, and close() still completes once the link heals. The abandoned bytes stay queued,
        # the documented "a failed write may have delivered partial data".
        received: list[int] = []
        sessions: list[Any] = []
        drainers: list[asyncio.Task[None]] = []

        def on_session(session: Any) -> None:
            sessions.append(session)
            drainers.append(asyncio.ensure_future(_drain_uni_streams(session, received)))

        server = await start_webtransport_server(on_session)
        transport = await self._connect(server)
        await wait_for(lambda: len(sessions) == 1, timeout=5)
        sessions[0].impair(paused=True)

        stream = await transport.create_unidirectional_stream()
        victim = asyncio.ensure_future(asyncio.wait_for(stream.write(b"\1" * (256 * 1024)), 0.05))
        await asyncio.sleep(0)
        second = asyncio.ensure_future(stream.write(b"tail"))

        with pytest.raises(TimeoutError):
            await victim

        await asyncio.sleep(0.05)
        assert not second.done()  # neither cancelled nor failed by the victim's deadline

        sessions[0].impair()
        await asyncio.wait_for(second, 20)
        await asyncio.wait_for(stream.close(), 20)  # a following close on the same stream completes
        await wait_for(lambda: received == [256 * 1024 + 4], timeout=20)
        transport.close()
        await asyncio.wait_for(transport.wait_closed(), 5)
        server.close()
        await asyncio.sleep(0.05)


class TestAioquicInternals:
    """Pins the aioquic internals the adapter reaches into.

    The send half's backpressure, the close drain, stream pruning and the FIN retry depend on state
    aioquic's public API does not expose. A rename in a later aioquic 1.x would otherwise fail
    silently and only under load (a write that never settles, a FIN never re-armed), so a dependency
    bump has to fail here first. Behaviour is asserted alongside the names, since the adapter relies
    on both.
    """

    async def test_the_private_aioquic_members_the_adapter_relies_on_still_exist(self) -> None:
        # Verified with `uv run --with "aioquic==<version>" pytest <this file> -k aioquic` on:
        #   1.3.0 (the locked version): passes.
        #   1.2.0 (the only other 1.2.x release on PyPI): every QuicStreamSender / QuicStreamReceiver /
        #     QuicConnection member below exists, but `H3Connection._get_or_create_stream` is a plain method
        #     there and only became a context manager in 1.3.0; `create_bidirectional_stream` relied on that
        #     before this test existed, so the adapter's real floor is 1.3.0 despite the `>=1.2` pin.
        # Revisit this list, and the members below, with any aioquic bump.

        # QuicStreamSender: what write() queues, what a transmit consumes (settling a write), what an
        # ACK finishes (pruning the stream), and how a reset shows.
        sender = QuicStreamSender(stream_id=2, writable=True)
        assert isinstance(sender._pending, RangeSet)
        assert len(sender._pending) == 0  # `len`, never truthiness: RangeSet.__bool__ is not implemented
        assert isinstance(sender._buffer, bytearray)
        assert sender._pending_eof is False
        assert sender._acked_fin is False
        assert sender._buffer_fin is None
        assert sender._reset_error_code is None
        assert sender.buffer_is_empty is True
        assert sender.is_finished is False

        sender.write(b"abc", end_stream=True)
        assert len(sender._pending) == 1
        assert sender._pending_eof is True
        assert sender._buffer_fin == 3
        assert sender.buffer_is_empty is False

        frame = sender.get_frame(1024)
        assert frame is not None and frame.fin and frame.data == b"abc"
        assert len(sender._pending) == 0 and sender._pending_eof is False  # transmitted

        sender.on_data_delivery(QuicDeliveryState.ACKED, 0, 3, True)
        assert sender._acked_fin is True
        assert len(sender._buffer) == 0
        assert sender.is_finished is True

        # The FIN retry re-arms exactly these two after aioquic consumed a FIN it could not send.
        rearmed = QuicStreamSender(stream_id=6, writable=True)
        rearmed.write(b"", end_stream=True)
        rearmed._pending_eof = False
        rearmed.buffer_is_empty = True
        rearmed.reset(0)
        assert rearmed._reset_error_code == 0

        # QuicStreamReceiver: the half `_finish_receiving_half` marks done on a send-only stream.
        receiver = QuicStreamReceiver(stream_id=2, readable=False)
        assert receiver.is_finished is False  # aioquic ignores `readable` here; the adapter sets it
        receiver.is_finished = True

        # QuicConnection / QuicStream: the live stream table the settle and drain paths read.
        connection = QuicConnection(configuration=QuicConfiguration(is_client=True))
        h3 = H3Connection(connection, enable_webtransport=True)
        stream_id = connection.get_next_available_stream_id(is_unidirectional=True)
        connection.send_stream_data(stream_id, b"x")
        assert isinstance(connection._streams, dict)
        stream = connection._streams[stream_id]
        assert isinstance(stream, QuicStream)
        assert isinstance(stream.sender, QuicStreamSender)
        assert isinstance(stream.receiver, QuicStreamReceiver)

        # H3Connection: the per-stream record the adapter drops once both halves are done.
        assert isinstance(h3._stream, dict)
        with h3._get_or_create_stream(stream_id) as h3_stream:
            assert isinstance(h3_stream, H3Stream)
        assert stream_id in h3._stream

        # QuicConnectionProtocol: the asyncio glue the adapter drives transmits through.
        protocol = QuicConnectionProtocol(connection)
        assert protocol._quic is connection
        assert callable(protocol._transmit_soon)
        assert protocol._transport is None
        assert protocol._loop is asyncio.get_running_loop()
