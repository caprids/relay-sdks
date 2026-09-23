"""What aioquic 1.3 does not expose: every private member the adapter reads or writes, each behind one name.

The send half's backpressure, the close drain, stream pruning and the dropped-FIN retry depend on state
aioquic's public API does not expose: ``QuicStreamSender._pending``, ``_pending_eof``, ``_buffer``,
``_buffer_fin``, ``_acked_fin`` and ``_reset_error_code`` (with the public ``buffer_is_empty`` and
``is_finished``), ``QuicStreamReceiver.is_finished``, ``QuicConnection._streams``, ``H3Connection._stream``
and ``H3Connection._get_or_create_stream``. This module is the only place that names them, so a rename in
a later aioquic 1.x fails in ``tests/test_webtransport_aioquic.py`` (which pins every member and the
behaviour of every function here) before it can fail silently under load, as a write that never settles
or a FIN that is never re-armed. The package pins ``aioquic>=1.3,<1.4`` for the same reason; widen it
after re-running that test module against the new minor.

The asyncio glue our own :class:`WebTransportProtocol` subclass reaches on its base class (``_quic``,
``_loop``, ``_transport``, ``_transmit_soon``, ``_connected_waiter``) stays in ``_protocol``; the pin
test covers those names too.
"""

from __future__ import annotations

from aioquic.h3.connection import FrameType, H3Connection
from aioquic.quic.connection import QuicConnection
from aioquic.quic.stream import QuicStream


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


def fin_unsent(stream: QuicStream) -> bool:
    """A FIN aioquic consumed for a packet but never put on the wire, as far as anyone can tell.

    ``get_frame`` hands back a FIN-only frame even when the packet has no room for it, and the following
    ``start_frame`` then rejects it with nothing to reschedule the FIN. This is also what a healthy FIN in
    flight looks like until its acknowledgement arrives, which is why the retry waits an interval first.
    """
    sender = stream.sender
    return (
        sender._buffer_fin is not None
        and not sender._acked_fin
        and not sender._pending_eof
        and sender._reset_error_code is None
        and not sender.is_finished
    )


def rearm_fin(stream: QuicStream) -> None:
    """Queue the stream's FIN again, so the next packet carries it. Harmless if the first copy did leave."""
    sender = stream.sender
    sender._pending_eof = True
    sender.buffer_is_empty = False


def finish_receiving_half(quic: QuicConnection, stream_id: int) -> None:
    """Mark a locally opened unidirectional stream's receiving half done so aioquic can discard the stream.

    aioquic 1.3 leaves that half unfinished although nothing can ever arrive on it, so the stream would
    otherwise stay in the live stream table (and in the per-packet scheduling loop) for the life of the
    connection after its FIN or reset was acknowledged.
    """
    stream = live_stream(quic, stream_id)

    if stream is not None:
        stream.receiver.is_finished = True


def mark_webtransport_stream(h3: H3Connection, stream_id: int, session_id: int) -> None:
    """Record a bidirectional stream we opened as a WebTransport stream of ``session_id``.

    aioquic writes the WEBTRANSPORT_STREAM frame header but does not record the stream, so the peer's
    reply bytes would otherwise be parsed as HTTP/3 frames.
    """
    with h3._get_or_create_stream(stream_id) as h3_stream:
        h3_stream.frame_type = FrameType.WEBTRANSPORT_STREAM
        h3_stream.session_id = session_id


def forget_h3_stream(h3: H3Connection, stream_id: int) -> None:
    """Drop the HTTP/3 record of a stream aioquic discarded.

    aioquic's HTTP/3 layer forgets a stream only once both halves ended, which it never learns for
    WebTransport streams whose bytes bypass it: every incoming one, and our bidirectional ones.
    """
    h3._stream.pop(stream_id, None)
