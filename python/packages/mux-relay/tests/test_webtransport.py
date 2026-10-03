"""The aioquic WebTransport adapter over a real QUIC loopback: the whole SDK, backpressure, pruning, teardown.

The loopback test is the offline counterpart of the live smoke test in ``scripts/test_relay.py``: it
runs the same assertion list from ``spec/behavior.md`` against the in-memory relay hub served over
real WebTransport sessions on 127.0.0.1, so the transport adapter is exercised end to end. The wire
codecs are covered in ``test_webtransport_wire.py`` and the aioquic internals the adapter relies on in
``test_webtransport_aioquic.py``.
"""

import asyncio
import errno
import gc
import select
import socket
import sys
import warnings
from collections.abc import AsyncIterator, Sequence
from typing import Any

import pytest
from aioquic.h3.connection import ErrorCode
from aioquic.h3.events import HeadersReceived, WebTransportStreamDataReceived
from aioquic.quic import events as quic_events
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.connection import QuicConnection
from core_helpers.waiting import wait_for
from relay_helpers.relay_hub import RelayHub, create_relay_hub
from relay_helpers.wt_server import (
    LoopbackWebTransportServer,
    WebTransportServerSession,
    start_webtransport_server,
)

from moq_core import MoqtObject
from moq_core.transport import ReceiveStream, SendStream
from mux_relay import (
    Closed,
    ConnectingTransport,
    RelayAwaitable,
    RelayError,
    RelaySubscriptionHandlers,
    connect_relay_publisher,
    connect_relay_subscriber,
)
from mux_relay.webtransport import WebTransportError, connect_webtransport
from mux_relay.webtransport._client import WebTransportSession
from mux_relay.webtransport._protocol import WebTransportProtocol
from mux_relay.webtransport._udp import reports_refusals
from mux_relay.webtransport._wire import encode_close_session_capsule

NAMESPACE = ["loopback", "data"]


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
            connect_relay_subscriber(server.url_for("/subscribe"), create_transport=create_transport), 10
        )
        closing = False
        problems: list[str] = []

        def observe(label: str, closed: RelayAwaitable[Closed]) -> None:
            def done(future: asyncio.Future[Closed]) -> None:
                if not closing:
                    problems.append(f"{label} closed: {future.result().error!r}")

            closed.add_done_callback(done)

        observe("publisher", publisher.closed)
        observe("subscriber", subscriber.closed)

        try:
            events = publisher.register_track("events", write_timeout=5)
            status = publisher.register_track("status", write_timeout=5)
            received: dict[str, list[bytes]] = {"events": [], "status": []}
            accepted: list[str] = []

            def subscribe(track: str) -> None:
                def on_object(obj: MoqtObject) -> None:
                    if obj.status == "normal":
                        received[track].append(obj.payload)

                subscriber.subscribe(
                    NAMESPACE,
                    track,
                    RelaySubscriptionHandlers(
                        on_subscribed=lambda ok: accepted.append(track),
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


async def _drain_uni_streams(session: Any, into: list[int | BaseException]) -> None:
    """Read every incoming unidirectional stream to its end, recording the byte count of each.

    A stream that fails records its error instead, so a test sees why rather than waiting out its deadline.
    """
    readers: list[asyncio.Task[None]] = []

    async def read_all(stream: ReceiveStream) -> None:
        total = 0

        try:
            while (chunk := await stream.read()) is not None:
                total += len(chunk)
        except Exception as error:
            into.append(error)
            return

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
        received: list[int | BaseException] = []
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
            # Abandon them, so the close below does not spend its drain bound on a link that cannot drain.
            stream.abort()
        finally:
            transport.close()
            await asyncio.wait_for(transport.wait_closed(), 5)
            server.close()
            await asyncio.sleep(0.05)

    async def test_a_write_to_a_reading_peer_completes_promptly(self) -> None:
        # The positive control: when the peer reads, the same write settles quickly.
        received: list[int | BaseException] = []
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
        received: list[int | BaseException] = []
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
        await wait_for(lambda: len(received) == 1, timeout=20)
        assert received == [payload]
        server.close()
        await asyncio.sleep(0.05)

    async def test_close_sends_the_session_close_only_after_the_streams_are_acknowledged(self) -> None:
        # The peer ends the session as soon as it reads CLOSE_WEBTRANSPORT_SESSION, resetting every
        # stream of it, so a FIN still being retransmitted when the capsule arrives was lost: the flake
        # of #209, a tail drop on the rate-capped link. Here a blackout makes both wait for the same
        # retransmission, and the peer reads the capsule first; the capsule must not be sent until the
        # stream is acknowledged, so the peer reads the stream to its end inside the session.
        sessions: list[Any] = []
        readers: list[asyncio.Task[None]] = []
        ended_at_fin: list[bool] = []

        async def read_while_open(session: Any) -> None:
            async for stream in session.incoming_unidirectional_streams():
                while await stream.read() is not None:
                    pass

                ended_at_fin.append(session.ended)

        def on_session(session: Any) -> None:
            sessions.append(session)
            readers.append(asyncio.ensure_future(read_while_open(session)))

        server = await start_webtransport_server(on_session)
        transport = await self._connect(server)
        await wait_for(lambda: len(sessions) == 1, timeout=5)

        try:
            sessions[0].impair(paused=True)
            stream = await transport.create_unidirectional_stream()
            await asyncio.wait_for(stream.write(b"\1" * 1000), 5)
            await asyncio.wait_for(stream.close(), 5)
            transport.close()
            await asyncio.sleep(0.3)
            sessions[0].impair()
            await asyncio.wait_for(transport.wait_closed(), 10)
            await wait_for(lambda: sessions[0].ended, timeout=5)
            assert ended_at_fin == [False]
            assert sessions[0].close_info == (0, "")
        finally:
            server.close()
            await asyncio.sleep(0.05)

    async def test_wait_acknowledged_settles_on_the_peers_acknowledgement_not_on_transmission(self) -> None:
        # A settling group (#160, #161): the FIN has been handed to the wire, so close() returns, but
        # the acknowledgement arrives only once the uplink is back. A reset stream never settles cleanly.
        received: list[int | BaseException] = []
        sessions: list[Any] = []
        drainers: list[asyncio.Task[None]] = []

        def on_session(session: Any) -> None:
            sessions.append(session)
            drainers.append(asyncio.ensure_future(_drain_uni_streams(session, received)))

        server = await start_webtransport_server(on_session)
        transport = await self._connect(server)
        await wait_for(lambda: len(sessions) == 1, timeout=5)

        try:
            sessions[0].impair(paused=True)
            stream = await transport.create_unidirectional_stream()
            await asyncio.wait_for(stream.write(b"\1" * 1000), 5)
            await asyncio.wait_for(stream.close(), 5)
            acknowledged = asyncio.ensure_future(stream.wait_acknowledged())
            await asyncio.sleep(0.3)
            assert not acknowledged.done()

            sessions[0].impair()
            await asyncio.wait_for(acknowledged, 5)
            await wait_for(lambda: received == [1000], timeout=5)
            await stream.wait_acknowledged()  # settled for later waiters too

            aborted = await transport.create_unidirectional_stream()
            await aborted.write(b"\2")
            aborted.abort()
            with pytest.raises(WebTransportError):
                await asyncio.wait_for(aborted.wait_acknowledged(), 5)
        finally:
            transport.close()
            await asyncio.wait_for(transport.wait_closed(), 5)
            server.close()
            await asyncio.sleep(0.05)

    async def test_an_abort_after_an_unacknowledged_fin_resets_with_the_code_and_fails_the_acknowledgement(
        self,
    ) -> None:
        # TA-16 after the FIN went out (#167): a settling group's SUBGROUP_DELIVERY_TIMEOUT resets a stream
        # whose FIN is in flight. The blackout drops the FIN; once the link is back the peer sees only
        # RESET_STREAM with DELIVERY_TIMEOUT (0x2), and the acknowledgement wait fails instead of settling.
        outcomes: list[tuple[str, int]] = []
        sessions: list[Any] = []
        readers: list[asyncio.Task[None]] = []

        async def read(stream: ReceiveStream) -> None:
            total = 0
            try:
                while (chunk := await stream.read()) is not None:
                    total += len(chunk)
            except WebTransportError as error:
                outcomes.append(("reset", error.code if error.code is not None else -1))
                return
            outcomes.append(("fin", total))

        async def accept(session: Any) -> None:
            async for stream in session.incoming_unidirectional_streams():
                readers.append(asyncio.ensure_future(read(stream)))

        def on_session(session: Any) -> None:
            sessions.append(session)
            readers.append(asyncio.ensure_future(accept(session)))

        server = await start_webtransport_server(on_session)
        transport = await self._connect(server)
        await wait_for(lambda: len(sessions) == 1, timeout=5)

        try:
            stream = await transport.create_unidirectional_stream()
            await asyncio.wait_for(stream.write(b"\1" * 1000), 5)
            await asyncio.sleep(0.2)  # the data is acknowledged; only the FIN will be in flight
            sessions[0].impair(paused=True)
            await asyncio.wait_for(stream.close(), 5)  # the FIN went out, into the blackout
            acknowledged = asyncio.ensure_future(stream.wait_acknowledged())
            await asyncio.sleep(0.2)
            assert not acknowledged.done()

            stream.abort(WebTransportError("delivery timeout", code=2))

            sessions[0].impair()
            with pytest.raises(WebTransportError):
                await asyncio.wait_for(acknowledged, 5)
            await wait_for(lambda: len(outcomes) == 1, timeout=5)
            await asyncio.sleep(0.3)  # time for a retransmitted FIN, if one were still coming
            assert outcomes == [("reset", 2)]

            # Once a FIN is acknowledged there is nothing to reset: abort() sends nothing.
            done = await transport.create_unidirectional_stream()
            await done.write(b"\2")
            await done.close()
            await asyncio.wait_for(done.wait_acknowledged(), 5)
            done.abort(WebTransportError("too late", code=2))
            await wait_for(lambda: len(outcomes) == 2, timeout=5)
            await asyncio.sleep(0.2)
            assert outcomes == [("reset", 2), ("fin", 1)]
        finally:
            transport.close()
            await asyncio.wait_for(transport.wait_closed(), 5)
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
        subscriber = await connect_relay_subscriber(server.url_for("/subscribe"), create_transport=create_transport)

        try:
            track = publisher.register_track("events", write_timeout=10)
            received = 0

            def on_object(_: MoqtObject) -> None:
                nonlocal received
                received += 1

            subscriber.subscribe(NAMESPACE, "events", RelaySubscriptionHandlers(on_object=on_object))
            await wait_for(lambda: track.has_demand, timeout=10)

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
        received: list[int | BaseException] = []
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
        assert received == [4096] * count
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
        stream_id: int = pair.writable.stream_id
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
        received: list[int | BaseException] = []
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


class TestCancelledHandshake:
    async def test_cancelled_handshake_observes_aioquic_teardown_error(self) -> None:
        loop = asyncio.get_running_loop()
        errors: list[dict[str, Any]] = []
        previous_handler = loop.get_exception_handler()
        loop.set_exception_handler(lambda _loop, context: errors.append(context))
        try:
            connection = QuicConnection(configuration=QuicConfiguration(is_client=True))
            protocol = WebTransportProtocol(connection)
            waiting = asyncio.create_task(protocol.wait_connected())
            await asyncio.sleep(0)
            assert protocol._connected_waiter is not None
            waiting.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiting

            # No network: drive the real aioquic event path that fails its shielded waiter
            # after connect_timeout or CancellationToken already cancelled the calling task.
            connection._events.append(quic_events.ConnectionTerminated(error_code=0, frame_type=None, reason_phrase=""))
            protocol._process_events()
            assert protocol._connected_waiter is None
            await asyncio.sleep(0)
            del waiting, protocol, connection
            gc.collect()
            await asyncio.sleep(0)
            assert errors == []
        finally:
            loop.set_exception_handler(previous_handler)


class TestKeepalive:
    """A client connection sends a QUIC PING after ``_KEEPALIVE_INTERVAL`` without sending anything (#158)."""

    INTERVAL = 0.3

    @pytest.fixture(autouse=True)
    def _short_interval(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from mux_relay.webtransport import _quic_protocol as _protocol

        assert _protocol._KEEPALIVE_INTERVAL == 5.0
        monkeypatch.setattr(_protocol, "_KEEPALIVE_INTERVAL", self.INTERVAL)

    async def _connect(self, server: LoopbackWebTransportServer) -> tuple[Any, list[float]]:
        created = connect_webtransport(
            server.url_for("/idle"), ["moqt-20"], configuration=server.client_configuration()
        )
        await asyncio.wait_for(created.ready, 5)
        transport: Any = created.transport
        protocol = transport._protocol
        loop = asyncio.get_running_loop()
        pings: list[float] = []
        send_ping = protocol._quic.send_ping

        def record(uid: int) -> None:
            pings.append(loop.time() - protocol._last_sent_at)
            send_ping(uid)

        protocol._quic.send_ping = record
        return transport, pings

    async def test_an_idle_connection_pings_after_the_interval_without_sending(self) -> None:
        sessions: list[Any] = []
        server = await start_webtransport_server(sessions.append)
        transport, pings = await self._connect(server)
        protocol = transport._protocol

        try:
            await asyncio.sleep(self.INTERVAL * 3.5)
            # Nothing flows either way but the PINGs and their ACKs: one per interval of send silence.
            assert 2 <= len(pings) <= 4
            assert all(self.INTERVAL - 0.02 <= silence <= self.INTERVAL * 1.5 for silence in pings)
            assert protocol.terminated is None
        finally:
            transport.close()
            await transport.wait_closed()
            server.close()

    async def test_a_connection_that_keeps_sending_sends_no_ping(self) -> None:
        sessions: list[Any] = []
        server = await start_webtransport_server(sessions.append)
        transport, pings = await self._connect(server)

        try:
            stream = await transport.create_unidirectional_stream()

            for _ in range(16):
                await stream.write(b"x" * 32)
                await asyncio.sleep(self.INTERVAL / 4)

            assert pings == []
        finally:
            transport.close()
            await transport.wait_closed()
            server.close()

    async def test_close_leaves_no_keepalive_timer_or_resource_warning(self) -> None:
        sessions: list[Any] = []
        server = await start_webtransport_server(sessions.append)
        transport, pings = await self._connect(server)
        protocol = transport._protocol
        assert protocol._keepalive is not None

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            transport.close()
            await transport.wait_closed()
            assert protocol._keepalive is None
            await asyncio.sleep(self.INTERVAL * 2)
            server.close()
            await asyncio.sleep(0.05)
            del transport
            gc.collect()

        assert protocol._keepalive is None
        assert pings == []
        assert [w for w in caught if issubclass(w.category, ResourceWarning)] == []

    async def test_a_server_connection_does_not_ping(self) -> None:
        sessions: list[Any] = []
        server = await start_webtransport_server(sessions.append)
        transport, _ = await self._connect(server)
        await wait_for(lambda: len(sessions) == 1, timeout=5)

        try:
            assert sessions[0]._protocol._keepalive is None
        finally:
            transport.close()
            await transport.wait_closed()
            server.close()


def _closed_udp_port() -> int:
    """A loopback UDP port nothing listens on (free a moment ago; the kernel does not hand it out again at once)."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def _cause_chain(error: BaseException) -> list[BaseException]:
    chain: list[BaseException] = []
    current: BaseException | None = error

    while current is not None:
        chain.append(current)
        current = current.__cause__

    return chain


def _polls_errored(sock: Any) -> bool:
    """Whether the socket polls as errored, which would wake the event loop's reader again and again."""
    poller = select.poll()
    poller.register(sock.fileno(), 0)  # POLLERR is reported whatever is registered
    return any(events & select.POLLERR for _, events in poller.poll(0))


class TestClosedPort:
    """A connect to a UDP port nobody listens on fails at once, as ``TRANSPORT_ERROR`` / ``connecting`` (#163)."""

    @pytest.mark.skipif(not sys.platform.startswith("linux"), reason="only Linux reports ICMP to an unconnected socket")
    @pytest.mark.parametrize(
        "connect", [connect_relay_publisher, connect_relay_subscriber], ids=["publisher", "subscriber"]
    )
    @pytest.mark.parametrize("ipv6_sockets", [True, False], ids=["dual-stack", "ipv4-fallback"])
    async def test_a_refused_handshake_fails_fast_with_the_os_error_as_cause(
        self, connect: Any, ipv6_sockets: bool, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        if not ipv6_sockets:
            real_socket = socket.socket

            def no_ipv6(family: int = socket.AF_INET, *args: Any, **kwargs: Any) -> Any:
                if family == socket.AF_INET6:
                    raise OSError("IPv6 is unavailable")

                return real_socket(family, *args, **kwargs)

            monkeypatch.setattr(socket, "socket", no_ipv6)

        loop = asyncio.get_running_loop()
        started = loop.time()
        kwargs: dict[str, Any] = {"namespace": NAMESPACE} if connect is connect_relay_publisher else {}

        with pytest.raises(RelayError) as failure:
            await connect(f"https://127.0.0.1:{_closed_udp_port()}", connect_timeout=2, **kwargs)

        assert loop.time() - started < 1
        assert (failure.value.code, failure.value.phase) == ("TRANSPORT_ERROR", "connecting")
        assert any(isinstance(cause, ConnectionRefusedError) for cause in _cause_chain(failure.value))

    async def test_an_address_that_never_answers_still_times_out(self) -> None:
        # A blackhole: the datagrams arrive and nothing comes back, not even a refusal.
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as blackhole:
            blackhole.bind(("127.0.0.1", 0))
            port = blackhole.getsockname()[1]
            loop = asyncio.get_running_loop()
            started = loop.time()

            with pytest.raises(RelayError) as failure:
                await connect_relay_subscriber(f"https://127.0.0.1:{port}", connect_timeout=0.5)

            assert loop.time() - started >= 0.5
            assert (failure.value.code, failure.value.phase) == ("CONNECTION_TIMEOUT", "connecting")

    async def test_the_socket_stays_unconnected_and_stops_reporting_after_the_handshake(self) -> None:
        sessions: list[Any] = []
        server = await start_webtransport_server(sessions.append)
        created = connect_webtransport(
            server.url_for("/sink"), ["moqt-20"], configuration=server.client_configuration()
        )
        await asyncio.wait_for(created.ready, 5)
        transport: Any = created.transport
        sock = transport._protocol._transport.get_extra_info("socket")

        try:
            # No peer association: a change of the client's own address still lets the relay migrate.
            with pytest.raises(OSError):
                sock.getpeername()

            assert not reports_refusals(sock)
        finally:
            transport.close()
            await transport.wait_closed()
            server.close()

    @pytest.mark.skipif(not sys.platform.startswith("linux"), reason="only Linux reports ICMP to an unconnected socket")
    async def test_a_refused_connect_leaves_no_error_report_to_spin_on(self) -> None:
        created = connect_webtransport(f"https://127.0.0.1:{_closed_udp_port()}", ["moqt-20"])

        with pytest.raises(WebTransportError):
            await asyncio.wait_for(created.ready, 2)

        transport: Any = created.transport
        protocol = transport._transport_protocol()
        sock = protocol._transport.get_extra_info("socket")
        # The teardown's CONNECTION_CLOSE draws another port unreachable; with reporting off it queues nothing.
        await asyncio.sleep(0.1)

        try:
            assert protocol._transport is not None and not protocol._transport.is_closing()
            assert not reports_refusals(sock)
            assert not _polls_errored(sock)
        finally:
            await asyncio.wait_for(protocol.transport_closed.wait(), 5)

    @pytest.mark.skipif(not sys.platform.startswith("linux"), reason="only Linux reports ICMP to an unconnected socket")
    async def test_an_error_ignored_during_the_handshake_is_drained(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as blackhole:
            blackhole.bind(("127.0.0.1", 0))
            created = connect_webtransport(f"https://127.0.0.1:{blackhole.getsockname()[1]}", ["moqt-20"])
            transport: Any = created.transport
            await wait_for(lambda: transport._transport_protocol() is not None, timeout=2)
            protocol = transport._transport_protocol()
            sock = protocol._transport.get_extra_info("socket")

            # Queue a real ICMP report on the client socket without letting the loop read it, then hand
            # the protocol an error it ignores (as a host unreachable would be).
            closed = ("127.0.0.1", _closed_udp_port())
            if sock.family == socket.AF_INET6:
                closed = ("::ffff:127.0.0.1", closed[1], 0, 0)

            with socket.fromfd(sock.fileno(), sock.family, socket.SOCK_DGRAM) as same_socket:
                same_socket.sendto(b"x", closed)

            poller = select.poll()
            poller.register(sock.fileno(), 0)
            assert poller.poll(500), "the port unreachable should have been queued"  # blocks the loop on purpose
            protocol.error_received(OSError(errno.EHOSTUNREACH, "No route to host"))

            try:
                assert not _polls_errored(sock)
                assert reports_refusals(sock)  # still in the handshake: a real refusal must still be heard
                assert protocol.refused is None and not transport.ready.done()
            finally:
                transport.close()
                with pytest.raises(WebTransportError):
                    await created.ready
                assert not reports_refusals(sock)  # closing a pending handshake turns reporting off
                await asyncio.wait_for(protocol.transport_closed.wait(), 5)

    async def test_a_refusal_after_the_handshake_changes_nothing(self) -> None:
        sessions: list[Any] = []
        server = await start_webtransport_server(sessions.append)
        created = connect_webtransport(
            server.url_for("/sink"), ["moqt-20"], configuration=server.client_configuration()
        )
        await asyncio.wait_for(created.ready, 5)
        transport: Any = created.transport
        protocol = transport._protocol

        try:
            # A transient ICMP error on an established connection: QUIC's own recovery decides, not the socket.
            protocol.error_received(ConnectionRefusedError(111, "Connection refused"))
            assert protocol.refused is None
            stream = await transport.create_unidirectional_stream()
            await stream.write(b"still here")
            await stream.close()
            await wait_for(lambda: len(sessions) == 1, timeout=5)
            assert protocol.terminated is None and not transport.ended
        finally:
            transport.close()
            await transport.wait_closed()
            server.close()


class TestReceivePruning:
    """Per-stream state for peer-initiated streams is dropped once aioquic discards the stream."""

    async def _connect(self, server: LoopbackWebTransportServer) -> Any:
        created = connect_webtransport(
            server.url_for("/sink"), ["moqt-20"], configuration=server.client_configuration()
        )
        await asyncio.wait_for(created.ready, 5)
        return created.transport

    @staticmethod
    def _maps(transport: Any) -> tuple[int, ...]:
        protocol = transport._protocol
        return (
            len(transport._receives),
            len(protocol.stream_owners),
            len(protocol.h3._stream),
            len(protocol._quic._streams),
            len(protocol._settling),
        )

    async def test_received_streams_leave_no_per_stream_state_behind(self) -> None:
        # Hundreds of received groups must not grow the adapter's maps, the settle map or the FIN
        # sweep: a peer-initiated unidirectional stream has no send half of ours to finish.
        sessions: list[Any] = []
        server = await start_webtransport_server(sessions.append)
        transport = await self._connect(server)
        await wait_for(lambda: len(sessions) == 1, timeout=5)
        protocol = transport._protocol
        baseline = self._maps(transport)
        received: list[int] = []

        async def read_all() -> None:
            async for stream in transport.incoming_unidirectional_streams():
                total = 0

                while (chunk := await stream.read()) is not None:
                    total += len(chunk)

                received.append(total)

        reader = asyncio.ensure_future(read_all())

        try:
            count = 300

            for _ in range(count):
                sender = await sessions[0].create_unidirectional_stream()
                await sender.write(b"\1" * 64)
                await sender.close()

            await wait_for(lambda: len(received) == count, timeout=30)
            # Pruning follows aioquic's own discard, which follows the acknowledgement.
            await wait_for(lambda: self._maps(transport) == baseline, timeout=10)
            await wait_for(lambda: protocol.fin_retry.idle, timeout=5)
        finally:
            reader.cancel()
            transport.close()
            await asyncio.wait_for(transport.wait_closed(), 5)
            server.close()
            await asyncio.sleep(0.05)

    async def test_a_cancelled_receive_half_is_pruned_when_the_peer_ends_the_stream(self) -> None:
        # The application stops reading (STOP_SENDING) and the peer's FIN crosses with it. The half
        # ignores the FIN, but the stream is over as far as QUIC is concerned, so its entries must go.
        sessions: list[Any] = []
        server = await start_webtransport_server(sessions.append)
        transport = await self._connect(server)
        await wait_for(lambda: len(sessions) == 1, timeout=5)
        protocol = transport._protocol

        try:
            sender = await sessions[0].create_unidirectional_stream()
            await sender.write(b"head")
            incoming = await asyncio.wait_for(anext(transport.incoming_unidirectional_streams()), 5)
            assert await incoming.read() == b"head"
            stream_id: int = incoming.stream_id

            # Our STOP_SENDING never reaches the peer, so its FIN is already on the way: the half is
            # cancelled when the end of stream arrives.
            sessions[0].impair(paused=True)
            incoming.cancel()
            await asyncio.wait_for(sender.close(), 10)
            await wait_for(lambda: incoming.cancelled and not incoming.ended, timeout=5)
            sessions[0].impair()

            await wait_for(
                lambda: (
                    stream_id not in transport._receives
                    and stream_id not in protocol.stream_owners
                    and stream_id not in protocol.h3._stream
                    and stream_id not in protocol._quic._streams
                    and stream_id not in protocol._settling
                ),
                timeout=10,
            )
        finally:
            transport.close()
            await asyncio.wait_for(transport.wait_closed(), 5)
            server.close()
            await asyncio.sleep(0.05)


class TestReceiveBatching:
    """Datagrams already waiting on the client's socket are taken in together, with one transmit (#162)."""

    async def test_waiting_datagrams_share_one_transmit_and_are_still_acknowledged(self) -> None:
        sessions: list[Any] = []
        server = await start_webtransport_server(sessions.append)
        created = connect_webtransport(
            server.url_for("/sink"), ["moqt-20"], configuration=server.client_configuration()
        )
        await asyncio.wait_for(created.ready, 5)
        transport: Any = created.transport
        await wait_for(lambda: len(sessions) == 1, timeout=5)
        protocol: WebTransportProtocol = transport._protocol
        peer: WebTransportProtocol = sessions[0]._protocol
        udp: Any = protocol._transport  # a selector datagram transport, which can pause its reads
        assert udp is not None
        received: list[bytes] = []

        async def read_all() -> None:
            async for stream in transport.incoming_unidirectional_streams():
                while (chunk := await stream.read()) is not None:
                    received.append(chunk)

        reader = asyncio.ensure_future(read_all())
        calls: list[tuple[int, int]] = []
        """(datagrams taken in, transmits) per ``datagram_received`` call."""
        counts = [0, 0]
        receive_datagram = protocol._quic.receive_datagram
        transmit = protocol.transmit
        datagram_received = protocol.datagram_received

        def counting_receive(*args: Any, **kwargs: Any) -> None:
            counts[0] += 1
            receive_datagram(*args, **kwargs)

        def counting_transmit() -> None:
            counts[1] += 1
            transmit()

        def recording_datagram_received(data: bytes | str, addr: Any) -> None:
            counts[:] = [0, 0]
            datagram_received(data, addr)
            calls.append((counts[0], counts[1]))

        try:
            # Hold the client's reads so the peer's packets pile up on its socket, as they do under load.
            udp.pause_reading()
            sender = await sessions[0].create_unidirectional_stream()
            await sender.write(b"\1" * 8000)  # several full-size packets, well inside the initial window
            await asyncio.sleep(0.1)
            assert peer._quic._loss.bytes_in_flight > 0

            protocol._quic.receive_datagram = counting_receive  # ty: ignore[invalid-assignment]
            protocol.transmit = counting_transmit  # ty: ignore[invalid-assignment]
            protocol.datagram_received = recording_datagram_received  # ty: ignore[invalid-assignment]
            udp.resume_reading()

            await wait_for(lambda: sum(len(chunk) for chunk in received) == 8000, timeout=5)
            taken, transmits = calls[0]
            assert taken > 1
            assert transmits == 1
            # The one transmit (or the ACK timer it armed) still acknowledges everything.
            await wait_for(lambda: peer._quic._loss.bytes_in_flight == 0, timeout=5)
        finally:
            reader.cancel()
            transport.close()
            await asyncio.wait_for(transport.wait_closed(), 5)
            server.close()
            await asyncio.sleep(0.05)


class TestPeerRefusal:
    """The peer refuses a session the way moq-relay 0.15.8 refuses credentials (#14).

    After ``:status 200`` and the client's SETUP, the relay sends CLOSE_WEBTRANSPORT_SESSION (code
    0x2, "unauthorized") with FIN on the CONNECT stream, FIN on its HTTP/3 control stream, and
    STOP_SENDING on the client's streams, all in one flight. aioquic on its own turns the control-stream
    FIN into a 0x104 connection close and drops the capsule; the adapter must surface the capsule.
    """

    @staticmethod
    async def _refuse(session: WebTransportServerSession, order: str) -> None:
        # Like the relay, answer only once the client's control stream (its SETUP) has arrived.
        async for _control in session.incoming_unidirectional_streams():
            break

        protocol = session._protocol
        assert protocol is not None
        quic = protocol._quic
        control_stream_id = protocol.h3._local_control_stream_id
        assert control_stream_id is not None

        def send_capsule() -> None:
            protocol.h3.send_data(session.session_id, encode_close_session_capsule(2, "unauthorized"), end_stream=True)

        def end_control_stream() -> None:
            quic.send_stream_data(control_stream_id, b"", end_stream=True)

        def stop_client_streams() -> None:
            for stream_id in list(session._receives):
                quic.stop_stream(stream_id, 0)

        if order == "control stream first":  # two datagrams, in this order
            end_control_stream()
            stop_client_streams()
            protocol.transmit()
            send_capsule()
            protocol.transmit()
        elif order == "capsule first":  # two datagrams, in this order
            send_capsule()
            protocol.transmit()
            stop_client_streams()
            end_control_stream()
            protocol.transmit()
        elif order == "one datagram":
            send_capsule()
            stop_client_streams()
            end_control_stream()
            protocol.transmit()
        elif order == "control stream only":  # a genuine control-stream closure: no capsule follows
            end_control_stream()
            protocol.transmit()
        else:
            raise AssertionError(order)

    async def _serve_refusing(self, order: str) -> tuple[LoopbackWebTransportServer, list[WebTransportSession]]:
        refusals: list[asyncio.Task[None]] = []
        server = await start_webtransport_server(
            lambda session: refusals.append(asyncio.ensure_future(self._refuse(session, order)))
        )
        self._refusals = refusals  # keep the tasks referenced
        return server, []

    @staticmethod
    def _capturing_factory(server: LoopbackWebTransportServer, created: list[WebTransportSession]):
        def create_transport(url: str, protocols: Sequence[str]) -> ConnectingTransport:
            connecting = connect_webtransport(url, protocols, configuration=server.client_configuration())
            assert isinstance(connecting.transport, WebTransportSession)
            created.append(connecting.transport)
            return connecting

        return create_transport

    @pytest.mark.parametrize("order", ["control stream first", "capsule first", "one datagram"])
    async def test_a_refused_subscriber_reports_unauthorized(self, order: str) -> None:
        server, created = await self._serve_refusing(order)

        try:
            with pytest.raises(RelayError) as failure:
                await connect_relay_subscriber(
                    server.url_for("/subscribe"),
                    create_transport=self._capturing_factory(server, created),
                    connect_timeout=5,
                )

            error = failure.value
            assert (error.code, error.phase) == ("UNAUTHORIZED", "connecting")
            assert (
                str(error) == "Relay refused the connection's credentials: unauthorized (session closed with code 0x2)"
            )
            cause = error.__cause__
            assert isinstance(cause, WebTransportError)
            assert (cause.peer_closed, cause.code, cause.reason) == (True, 2, "unauthorized")

            session = created[0]
            assert session.close_info == (2, "unauthorized")
            protocol = session._protocol
            assert protocol is not None

            if order == "control stream first":
                # The FIN really was held for the capsule, and the capsule settled it.
                assert protocol._control_stream_fin is not None
                assert protocol._control_stream_fin.cancelled()

            # The client closed the connection itself, cleanly: the held FIN never became a 0x104 close.
            await asyncio.wait_for(protocol.terminated_event.wait(), 5)
            assert protocol.terminated is not None
            assert protocol.terminated.error_code == ErrorCode.H3_NO_ERROR
        finally:
            server.close()

    async def test_a_refused_publisher_reports_unauthorized(self) -> None:
        server, created = await self._serve_refusing("control stream first")

        try:
            with pytest.raises(RelayError) as failure:
                await connect_relay_publisher(
                    server.url_for("/publish"),
                    namespace=NAMESPACE,
                    create_transport=self._capturing_factory(server, created),
                    connect_timeout=5,
                )

            assert (failure.value.code, failure.value.phase) == ("UNAUTHORIZED", "connecting")
        finally:
            server.close()

    async def test_a_control_stream_closed_without_a_capsule_still_fails_the_connection(self) -> None:
        server, created = await self._serve_refusing("control stream only")

        try:
            with pytest.raises(RelayError) as failure:
                await connect_relay_subscriber(
                    server.url_for("/subscribe"),
                    create_transport=self._capturing_factory(server, created),
                    connect_timeout=5,
                )

            error = failure.value
            assert (error.code, error.phase) == ("TRANSPORT_ERROR", "connecting")
            assert "Closing control stream is not allowed (code 0x104)" in str(error)
            cause = error.__cause__
            assert isinstance(cause, WebTransportError)
            assert not cause.peer_closed
            assert cause.code == ErrorCode.H3_CLOSED_CRITICAL_STREAM
        finally:
            server.close()

    async def test_a_session_closed_with_code_0x2_after_setup_reports_unauthorized(
        self, loopback: tuple[LoopbackWebTransportServer, RelayHub, list[WebTransportServerSession]]
    ) -> None:
        server, _, sessions = loopback
        subscriber = await connect_relay_subscriber(
            server.url_for("/subscribe"), create_transport=factory_for(server), connect_timeout=5
        )
        await wait_for(lambda: len(sessions) == 1)

        sessions[0].close(2, "unauthorized")
        closed = await asyncio.wait_for(subscriber.closed, 5)

        assert isinstance(closed.error, RelayError)
        assert (closed.error.code, closed.error.phase) == ("UNAUTHORIZED", "connection")

    async def test_a_session_closed_with_another_code_is_a_transport_error(
        self, loopback: tuple[LoopbackWebTransportServer, RelayHub, list[WebTransportServerSession]]
    ) -> None:
        server, _, sessions = loopback
        subscriber = await connect_relay_subscriber(
            server.url_for("/subscribe"), create_transport=factory_for(server), connect_timeout=5
        )
        await wait_for(lambda: len(sessions) == 1)

        sessions[0].close(0x3, "protocol violation")
        closed = await asyncio.wait_for(subscriber.closed, 5)

        assert isinstance(closed.error, RelayError)
        assert (closed.error.code, closed.error.phase) == ("TRANSPORT_ERROR", "connection")
        assert str(closed.error) == "peer closed the WebTransport session: protocol violation (code 0x3)"
