"""Pins the aioquic internals the adapter reaches into, names and behaviour, so a dependency bump fails here first."""

import asyncio

from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.h3.connection import H3Connection, H3Stream
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.connection import QuicConnection
from aioquic.quic.packet_builder import QuicDeliveryState
from aioquic.quic.rangeset import RangeSet
from aioquic.quic.stream import QuicStream, QuicStreamReceiver, QuicStreamSender


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

        # QuicConnectionProtocol: the asyncio glue the adapter drives transmits through.
        protocol = QuicConnectionProtocol(connection)
        assert protocol._quic is connection
        assert callable(protocol._transmit_soon)
        assert protocol._connected_waiter is None
        assert protocol._transport is None
        assert protocol._loop is asyncio.get_running_loop()
