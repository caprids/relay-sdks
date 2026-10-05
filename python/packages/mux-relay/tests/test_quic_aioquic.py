"""Pins the aioquic internals the adapter reaches into, so a dependency bump fails here first.

``TestAioquicInternals`` asserts the private members by name and behaviour; ``TestAioquicFunctions`` checks
each function of ``mux_relay.quic._aioquic``, the one module that names those members, against
real aioquic objects one state transition at a time.
"""

import asyncio

from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.connection import QuicConnection
from aioquic.quic.packet_builder import QuicDeliveryState, QuicSentPacket
from aioquic.quic.rangeset import RangeSet
from aioquic.quic.stream import QuicStream, QuicStreamReceiver, QuicStreamSender
from relay_helpers.quic_pair import QuicPair

from mux_relay.quic._aioquic import (
    acknowledged,
    fin_unsent,
    finish_receiving_half,
    fins_in_flight,
    live_stream,
    packets_sent,
    rearm_fin,
    reset_pending,
    serve_resets_first,
    stream_credit_left,
    transmitted,
)
from mux_relay.quic._protocol import QuicProtocol


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
        #     QuicConnection member below exists. The floor of 1.3.0 came from the HTTP/3 layer the package no
        #     longer carries (#232); the test suite's WebTransport accepting end still needs it.
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

        # The dropped-FIN retry (`_fin_retry`, #47) re-arms exactly these two after aioquic consumed a FIN it
        # could not send; delete this block with it.
        rearmed = QuicStreamSender(stream_id=6, writable=True)
        rearmed.write(b"", end_stream=True)
        rearmed._pending_eof = False
        rearmed.buffer_is_empty = True
        rearmed.reset(0)
        assert rearmed._reset_error_code == 0

        # Resets served first (#160) read this flag and the receiver's `stop_pending`.
        assert rearmed.reset_pending is True

        # QuicStreamReceiver: the half `_finish_receiving_half` marks done on a send-only stream.
        receiver = QuicStreamReceiver(stream_id=2, readable=False)
        assert receiver.is_finished is False  # aioquic ignores `readable` here; the adapter sets it
        assert receiver.stop_pending is False
        receiver.is_finished = True

        # The mirror image, which the adapter relies on and does not set: the unused send half of a
        # peer-initiated unidirectional stream is finished from the start, so the FIN alone lets
        # aioquic discard the stream and the settle path prune a received group's entries.
        assert QuicStreamSender(stream_id=3, writable=False).is_finished is True

        # QuicConnection / QuicStream: the live stream table the settle and drain paths read.
        connection = QuicConnection(configuration=QuicConfiguration(is_client=True))
        stream_id = connection.get_next_available_stream_id(is_unidirectional=True)
        connection.send_stream_data(stream_id, b"x")
        assert isinstance(connection._streams, dict)
        stream = connection._streams[stream_id]
        assert isinstance(stream, QuicStream)
        assert isinstance(stream.sender, QuicStreamSender)
        assert isinstance(stream.receiver, QuicStreamReceiver)
        # The per-packet stream service order, and the sent packets whose delivery handlers show a FIN in flight.
        assert stream in connection._streams_queue
        assert isinstance(connection._loss.spaces, list)
        pair = QuicPair()
        [fin_stream] = pair.open_streams(1, 10, fin=True)
        pair.send_unpaced()
        packets = [packet for space in pair.client._loss.spaces for packet in space.sent_packets.values()]
        handlers = [(handler, args) for packet in packets for handler, args in packet.delivery_handlers]
        sender = pair.client._streams[fin_stream].sender
        assert (sender.on_data_delivery, (0, 10, True)) in handlers
        assert isinstance(packets[0], QuicSentPacket)

        # QuicConnection._packet_number: the keepalive's send clock, one per packet built.
        assert connection._packet_number == 0

        # QuicConnection._remote_max_streams_uni / _bidi: the peer's MAX_STREAMS, which the session's
        # creates check before opening a stream aioquic would otherwise hold blocked (TA-21).
        assert connection._remote_max_streams_uni == 0
        assert connection._remote_max_streams_bidi == 0

        # QuicConnectionProtocol: the asyncio glue the adapter drives transmits through.
        protocol = QuicConnectionProtocol(connection)
        assert protocol._quic is connection
        assert callable(protocol._transmit_soon)
        assert callable(protocol._process_events)  # the batched receive feeds datagrams without its transmit
        assert protocol._connected_waiter is None
        assert protocol._transport is None
        assert protocol._loop is asyncio.get_running_loop()


class TestAioquicFunctions:
    """Each named question ``_aioquic`` answers, over real aioquic objects."""

    @staticmethod
    def _connection() -> QuicConnection:
        return QuicConnection(configuration=QuicConfiguration(is_client=True))

    def _stream_with(self, quic: QuicConnection, data: bytes, *, fin: bool = False) -> QuicStream:
        stream_id = quic.get_next_available_stream_id(is_unidirectional=True)
        quic.send_stream_data(stream_id, data, end_stream=fin)
        stream = live_stream(quic, stream_id)
        assert stream is not None
        return stream

    def test_live_stream_is_none_for_a_stream_aioquic_does_not_hold(self) -> None:
        quic = self._connection()
        assert live_stream(quic, 998) is None
        stream = self._stream_with(quic, b"x")
        stream_id = stream.stream_id
        assert stream_id is not None
        assert live_stream(quic, stream_id) is stream

    def test_transmitted_once_nothing_queued_remains(self) -> None:
        stream = self._stream_with(self._connection(), b"abc")
        assert not transmitted(stream)

        assert stream.sender.get_frame(1024) is not None

        assert transmitted(stream)
        assert not acknowledged(stream)  # on the wire, not yet acknowledged

    def test_a_queued_fin_counts_as_untransmitted(self) -> None:
        stream = self._stream_with(self._connection(), b"abc")
        assert stream.sender.get_frame(1024) is not None
        assert transmitted(stream)

        stream.sender.write(b"", end_stream=True)

        assert not transmitted(stream)
        assert stream.sender.get_frame(1024) is not None
        assert transmitted(stream)

    def test_acknowledged_once_data_and_fin_are_acked(self) -> None:
        stream = self._stream_with(self._connection(), b"abc", fin=True)
        frame = stream.sender.get_frame(1024)
        assert frame is not None and frame.fin
        assert not acknowledged(stream)

        stream.sender.on_data_delivery(QuicDeliveryState.ACKED, 0, 3, True)

        assert acknowledged(stream)
        assert stream.sender.is_finished

    def test_a_stream_without_a_fin_is_acknowledged_once_its_bytes_are(self) -> None:
        stream = self._stream_with(self._connection(), b"abc")
        assert stream.sender.get_frame(1024) is not None

        stream.sender.on_data_delivery(QuicDeliveryState.ACKED, 0, 3, False)

        assert acknowledged(stream)
        assert not stream.sender.is_finished  # more can still be written

    def test_reset_pending_until_the_reset_is_acknowledged(self) -> None:
        stream = self._stream_with(self._connection(), b"abc")
        assert not reset_pending(stream)

        stream.sender.reset(5)

        assert reset_pending(stream)
        assert not acknowledged(stream)

        stream.sender.on_reset_delivery(QuicDeliveryState.ACKED)

        assert not reset_pending(stream)
        assert acknowledged(stream)

    def test_fin_unsent_names_a_consumed_unacknowledged_fin_and_rearm_requeues_it(self) -> None:
        stream = self._stream_with(self._connection(), b"", fin=True)
        assert not fin_unsent(stream, set())  # queued, not yet consumed

        frame = stream.sender.get_frame(1024)
        assert frame is not None and frame.fin
        assert fin_unsent(stream, set())

        rearm_fin(stream)

        assert not fin_unsent(stream, set())
        assert stream.sender._pending_eof is True
        assert stream.sender.buffer_is_empty is False
        again = stream.sender.get_frame(1024)
        assert again is not None and again.fin  # the next packet carries it

        stream.sender.on_data_delivery(QuicDeliveryState.ACKED, 0, 0, True)

        assert not fin_unsent(stream, set())

    def test_fin_unsent_is_false_once_the_stream_is_reset(self) -> None:
        stream = self._stream_with(self._connection(), b"", fin=True)
        assert stream.sender.get_frame(1024) is not None
        assert fin_unsent(stream, set())

        stream.sender.reset(0)

        assert not fin_unsent(stream, set())

    def test_fins_in_flight_names_a_sent_fin_until_it_is_acknowledged(self) -> None:
        pair = QuicPair()
        with_fin = pair.open_streams(2, 100, fin=True)
        [without_fin] = pair.open_streams(1, 100)
        assert fins_in_flight(pair.client) == set()  # queued, not yet in a packet

        pair.send_unpaced()

        streams = [pair.client._streams[stream_id] for stream_id in with_fin]
        assert fins_in_flight(pair.client) == {stream.sender for stream in streams}
        assert pair.client._streams[without_fin].sender not in fins_in_flight(pair.client)
        assert not fin_unsent(streams[0], fins_in_flight(pair.client))  # sent, so not a dropped FIN

        pair.deliver()  # the server acknowledges them

        assert fins_in_flight(pair.client) == set()

    def test_a_reset_behind_queued_data_holds_a_transmit_to_one_packet_until_served_first(self) -> None:
        # What aioquic 1.3 does (#160): the RESET_STREAM's `start_frame` raises QuicPacketBuilderStop in a
        # full packet, which ends the whole transmit. If this starts sending more, aioquic fixed it.
        stalled = QuicPair()
        stream_ids = stalled.open_streams(40, 1000)
        stalled.client.reset_stream(stream_ids[-1], 0)

        assert stalled.send_unpaced() == 1

        # Served first, the same transmit fills the congestion window.
        served = QuicPair()
        stream_ids = served.open_streams(40, 1000)
        served.client.reset_stream(stream_ids[-1], 0)

        serve_resets_first(served.client)

        assert served.client._streams_queue[0].stream_id == stream_ids[-1]
        assert served.send_unpaced() > 5
        assert served.client._loss.congestion_window - served.client._loss.bytes_in_flight < 1200

    def test_a_stop_sending_is_served_first_too(self) -> None:
        pair = QuicPair()
        pair.open_streams(40, 1000)
        stream_id = pair.client.get_next_available_stream_id()
        pair.client.send_stream_data(stream_id, b"")
        pair.client.stop_stream(stream_id, 0)

        serve_resets_first(pair.client)

        assert pair.client._streams_queue[0].stream_id == stream_id
        assert pair.send_unpaced() > 5

    def test_serve_resets_first_keeps_the_order_when_nothing_is_urgent(self) -> None:
        pair = QuicPair()
        pair.open_streams(3, 10)
        before = list(pair.client._streams_queue)

        serve_resets_first(pair.client)

        assert pair.client._streams_queue == before

    def test_finish_receiving_half_marks_our_unidirectional_stream_done(self) -> None:
        quic = self._connection()
        stream = self._stream_with(quic, b"x")
        assert stream.receiver.is_finished is False

        stream_id = stream.stream_id
        assert stream_id is not None
        finish_receiving_half(quic, stream_id)

        assert stream.receiver.is_finished is True
        finish_receiving_half(quic, 998)  # a stream aioquic does not hold: nothing to do

    def test_packets_sent_counts_every_packet_built(self) -> None:
        quic = self._connection()
        assert packets_sent(quic) == 0
        quic.connect(("127.0.0.1", 4433), now=0.0)
        datagrams = quic.datagrams_to_send(now=0.0)
        assert datagrams and packets_sent(quic) >= len(datagrams)
        before = packets_sent(quic)
        assert quic.datagrams_to_send(now=0.0) == []
        assert packets_sent(quic) == before  # nothing built, nothing counted

    def test_stream_credit_left_follows_the_peer_max_streams(self) -> None:
        pair = QuicPair()
        quic = pair.client
        uni = quic._remote_max_streams_uni
        bidi = quic._remote_max_streams_bidi
        assert uni > 0 and bidi > 0  # the transport parameters granted some

        for _ in range(uni):
            assert stream_credit_left(quic, unidirectional=True)
            quic.send_stream_data(quic.get_next_available_stream_id(is_unidirectional=True), b"", False)

        assert not stream_credit_left(quic, unidirectional=True)
        assert stream_credit_left(quic, unidirectional=False)  # each kind has its own limit
        quic._remote_max_streams_uni += 1  # what a MAX_STREAMS frame does
        assert stream_credit_left(quic, unidirectional=True)


class _Transport:
    def __init__(self) -> None:
        self.sent: list[bytes] = []

    def sendto(self, data: bytes, address: object) -> None:
        self.sent.append(data)


class TestProtocolTransmit:
    async def test_every_transmit_serves_a_queued_reset_first(self) -> None:
        # The live symptom (#160): a write's deadline resets its stream while a backlog is queued, and
        # without the reorder each transmit sends one packet, so the backlog drains one packet per ACK.
        pair = QuicPair()
        stream_ids = pair.open_streams(40, 1000)
        pair.client.reset_stream(stream_ids[-1], 0)
        transport = _Transport()
        protocol = QuicProtocol(pair.client)
        protocol.connection_made(transport)  # ty: ignore[invalid-argument-type]
        pair.client._loss._pacer.packet_time = None  # count what the congestion window allows

        protocol.transmit()

        assert len(transport.sent) > 5
        assert not pair.client._streams[stream_ids[-1]].sender.reset_pending  # the RESET_STREAM went out
        protocol.fin_retry.stop()
