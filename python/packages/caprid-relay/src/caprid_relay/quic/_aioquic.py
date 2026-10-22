"""What aioquic 1.3 does not expose: every private member the adapter reads or writes, each behind one name.

The send half's backpressure, the close drain, stream pruning, the dropped-FIN retry, resets served
first, the keepalive and the stream-credit check depend on state aioquic's public API does not expose:
``QuicStreamSender._pending``, ``_pending_eof``, ``_buffer``, ``_buffer_fin``, ``_acked_fin`` and
``_reset_error_code`` (with the public ``buffer_is_empty``, ``is_finished`` and ``reset_pending``),
``QuicStreamReceiver.is_finished`` and ``stop_pending``, ``QuicConnection._streams``, ``_streams_queue``,
``_packet_number``, ``_remote_max_streams_uni``, ``_remote_max_streams_bidi`` and ``_loss`` (its ``spaces``,
their ``sent_packets`` and each packet's ``delivery_handlers``). This module is the only place that names
them, so a rename in a later aioquic 1.x fails in ``tests/test_quic_aioquic.py`` (which pins every member
and the behaviour of every function here) before it can fail silently under load, as a write that never
settles or a FIN that is never re-armed. The package pins ``aioquic>=1.3,<1.4`` for the same reason; widen
it after re-running that test module against the new minor.

The asyncio glue our own :class:`~._protocol.QuicProtocol` subclass reaches on its base class (``_quic``,
``_loop``, ``_transport``, ``_transmit_soon``, ``_process_events``, ``_connected_waiter``) stays in
``_protocol``; the pin test covers those names too.
"""

from __future__ import annotations

from collections.abc import Collection

from aioquic.quic.connection import QuicConnection
from aioquic.quic.stream import QuicStream, QuicStreamSender


def live_stream(quic: QuicConnection, stream_id: int) -> QuicStream | None:
    """The stream as aioquic still holds it, or ``None`` once aioquic discarded it (both halves done, all acked)."""
    return quic._streams.get(stream_id)


def transmitted(stream: QuicStream) -> bool:
    """Nothing queued on this stream still waits to be sent: every write and the FIN have been handed to the wire."""
    sender = stream.sender
    # `len`, never truthiness: RangeSet.__bool__ is not implemented.
    return len(sender._pending) == 0 and not sender._pending_eof


def acknowledged(stream: QuicStream) -> bool:
    """The peer acknowledged everything sent on this stream: no data, FIN or reset is outstanding."""
    sender = stream.sender

    if sender.is_finished:
        return True

    if sender._reset_error_code is not None:
        return False

    return not (sender._buffer or sender._pending_eof or (sender._buffer_fin is not None and not sender._acked_fin))


def reset_pending(stream: QuicStream) -> bool:
    """A reset was queued on this stream and the peer has not acknowledged it yet."""
    sender = stream.sender
    return sender._reset_error_code is not None and not sender.is_finished


def fins_in_flight(quic: QuicConnection) -> set[QuicStreamSender]:
    """The stream senders whose FIN is in a sent packet aioquic has not yet seen acknowledged or declared lost.

    A packet's delivery handlers are what aioquic calls on its acknowledgement or loss; a STREAM frame
    registers its sender's ``on_data_delivery`` with ``(start, stop, fin)``. One pass over every packet
    in flight, so a caller asking about many streams asks once.
    """
    senders: set[QuicStreamSender] = set()

    for space in quic._loss.spaces:
        for packet in space.sent_packets.values():
            for handler, args in packet.delivery_handlers:
                if getattr(handler, "__func__", None) is not QuicStreamSender.on_data_delivery:
                    continue

                _start, _stop, fin = args

                if fin:
                    senders.add(handler.__self__)  # ty: ignore[unresolved-attribute]

    return senders


def fin_unsent(stream: QuicStream, in_flight: Collection[QuicStreamSender]) -> bool:
    """A FIN aioquic consumed for a packet but never put on the wire.

    ``get_frame`` hands back a FIN-only frame even when the packet has no room for it, and the following
    ``start_frame`` then rejects it with nothing to reschedule the FIN. A FIN in no packet in flight
    (``in_flight``, from :func:`fins_in_flight`) is that one: a sent FIN is either still in a packet or,
    once acknowledged or declared lost, finished or queued again. Without that check a FIN in flight on a
    dead link looks the same, and re-arming it fills every probe packet with FINs (#160).
    """
    sender = stream.sender
    return (
        sender._buffer_fin is not None
        and not sender._acked_fin
        and not sender._pending_eof
        and sender._reset_error_code is None
        and not sender.is_finished
        and sender not in in_flight
    )


def rearm_fin(stream: QuicStream) -> None:
    """Queue the stream's FIN again, so the next packet carries it. Harmless if the first copy did leave."""
    sender = stream.sender
    sender._pending_eof = True
    sender.buffer_is_empty = False


def serve_resets_first(quic: QuicConnection) -> None:
    """Move the streams with a RESET_STREAM or STOP_SENDING to send to the front of aioquic's service order.

    ``_write_application`` writes those frames with ``start_frame``, which raises ``QuicPacketBuilderStop``
    when the packet has no room left, and that ends the whole ``datagrams_to_send`` call, not just the
    packet. Queued behind streams with data to send, the packet is always full by the time the loop reaches
    such a stream, so every transmit sends one packet, one per ACK, until the data ahead of it drains
    (#160). At the front the frame goes first into a fresh packet, and the rest of the call is unaffected.
    """
    queue = quic._streams_queue

    def urgent(stream: QuicStream) -> bool:
        return stream.sender.reset_pending or stream.receiver.stop_pending

    if not any(urgent(stream) for stream in queue):
        return

    first = [stream for stream in queue if urgent(stream)]
    quic._streams_queue = first + [stream for stream in queue if not urgent(stream)]


def finish_receiving_half(quic: QuicConnection, stream_id: int) -> None:
    """Mark a locally opened unidirectional stream's receiving half done so aioquic can discard the stream.

    aioquic 1.3 leaves that half unfinished although nothing can ever arrive on it, so the stream would
    otherwise stay in the live stream table (and in the per-packet scheduling loop) for the life of the
    connection after its FIN or reset was acknowledged.
    """
    stream = live_stream(quic, stream_id)

    if stream is not None:
        stream.receiver.is_finished = True


def stream_credit_left(quic: QuicConnection, unidirectional: bool) -> bool:
    """Whether the peer's MAX_STREAMS lets this end open one more stream of the kind now (RFC 9000 §4.6).

    aioquic opens a stream past the limit anyway and holds it, blocked, until the peer raises the limit,
    so a create would hang instead of failing; the session checks first (TA-21).
    """
    stream_id = quic.get_next_available_stream_id(is_unidirectional=unidirectional)
    limit = quic._remote_max_streams_uni if unidirectional else quic._remote_max_streams_bidi
    return stream_id // 4 < limit


def packets_sent(quic: QuicConnection) -> int:
    """How many QUIC packets the connection has built so far, ACK-only ones included.

    The number grows by one for every packet ``datagrams_to_send`` returns, so a change across a
    transmit means something left for the peer. The keepalive measures send silence with it.
    """
    return quic._packet_number
