"""Pins the aioquic internals the adapter reaches into, so a dependency bump fails here first.

``TestAioquicInternals`` asserts the private members by name and behaviour; ``TestAioquicFunctions`` checks
each function of ``mux_relay.webtransport._aioquic``, the one module that names those members, against
real aioquic objects one state transition at a time.
"""

import asyncio

from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.h3.connection import ErrorCode, FrameType, H3Connection, H3Stream
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.connection import QuicConnection
from aioquic.quic.events import StreamDataReceived
from aioquic.quic.packet_builder import QuicDeliveryState
from aioquic.quic.rangeset import RangeSet
from aioquic.quic.stream import QuicStream, QuicStreamReceiver, QuicStreamSender

from mux_relay.webtransport._aioquic import (
    acknowledged,
    fin_unsent,
    finish_receiving_half,
    forget_h3_stream,
    live_stream,
    mark_webtransport_stream,
    peer_control_stream_id,
    rearm_fin,
    reset_pending,
    transmitted,
)


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

        # The dropped-FIN retry (`_fin_retry`, #47) re-arms exactly these two after aioquic consumed a FIN it
        # could not send; delete this block with it.
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

        # The mirror image, which the adapter relies on and does not set: the unused send half of a
        # peer-initiated unidirectional stream is finished from the start, so the FIN alone lets
        # aioquic discard the stream and the settle path prune a received group's entries.
        assert QuicStreamSender(stream_id=3, writable=False).is_finished is True

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
        assert h3._peer_control_stream_id is None  # set once the peer's control stream type is read

        # QuicConnectionProtocol: the asyncio glue the adapter drives transmits through.
        protocol = QuicConnectionProtocol(connection)
        assert protocol._quic is connection
        assert callable(protocol._transmit_soon)
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
        assert not fin_unsent(stream)  # queued, not yet consumed

        frame = stream.sender.get_frame(1024)
        assert frame is not None and frame.fin
        assert fin_unsent(stream)

        rearm_fin(stream)

        assert not fin_unsent(stream)
        assert stream.sender._pending_eof is True
        assert stream.sender.buffer_is_empty is False
        again = stream.sender.get_frame(1024)
        assert again is not None and again.fin  # the next packet carries it

        stream.sender.on_data_delivery(QuicDeliveryState.ACKED, 0, 0, True)

        assert not fin_unsent(stream)

    def test_fin_unsent_is_false_once_the_stream_is_reset(self) -> None:
        stream = self._stream_with(self._connection(), b"", fin=True)
        assert stream.sender.get_frame(1024) is not None
        assert fin_unsent(stream)

        stream.sender.reset(0)

        assert not fin_unsent(stream)

    def test_finish_receiving_half_marks_our_unidirectional_stream_done(self) -> None:
        quic = self._connection()
        stream = self._stream_with(quic, b"x")
        assert stream.receiver.is_finished is False

        stream_id = stream.stream_id
        assert stream_id is not None
        finish_receiving_half(quic, stream_id)

        assert stream.receiver.is_finished is True
        finish_receiving_half(quic, 998)  # a stream aioquic does not hold: nothing to do

    def test_mark_webtransport_stream_records_the_frame_type_and_session(self) -> None:
        quic = self._connection()
        h3 = H3Connection(quic, enable_webtransport=True)
        stream_id = quic.get_next_available_stream_id()

        mark_webtransport_stream(h3, stream_id, 4)

        assert h3._stream[stream_id].frame_type == FrameType.WEBTRANSPORT_STREAM
        assert h3._stream[stream_id].session_id == 4

    def test_forget_h3_stream_drops_the_record(self) -> None:
        quic = self._connection()
        h3 = H3Connection(quic, enable_webtransport=True)
        stream_id = quic.get_next_available_stream_id()
        mark_webtransport_stream(h3, stream_id, 4)

        forget_h3_stream(h3, stream_id)

        assert stream_id not in h3._stream
        forget_h3_stream(h3, stream_id)  # already gone: nothing to do

    def test_peer_control_stream_id_names_the_stream_once_its_type_is_read(self) -> None:
        quic = self._connection()
        h3 = H3Connection(quic, enable_webtransport=True)
        assert peer_control_stream_id(h3) is None

        h3.handle_event(StreamDataReceived(data=b"\x00", end_stream=False, stream_id=3))  # stream type CONTROL

        assert peer_control_stream_id(h3) == 3

        # What the protocol holds that stream's FIN back from: aioquic closes the connection with
        # H3_CLOSED_CRITICAL_STREAM at once and ignores every later event (#14).
        h3.handle_event(StreamDataReceived(data=b"", end_stream=True, stream_id=3))

        assert quic._close_event is not None
        assert quic._close_event.error_code == ErrorCode.H3_CLOSED_CRITICAL_STREAM
        assert h3.handle_event(StreamDataReceived(data=b"\x00\x00", end_stream=False, stream_id=0)) == []
