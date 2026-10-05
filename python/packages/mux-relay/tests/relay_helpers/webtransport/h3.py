"""What aioquic 1.3's HTTP/3 layer does not expose: the ``H3Connection`` private members the accepting end uses.

``H3Connection._stream``, ``_get_or_create_stream`` and ``_peer_control_stream_id``, each behind one name, the
way ``mux_relay.quic._aioquic`` names the QUIC layer's. ``tests/test_webtransport_accepting_end.py`` pins them,
so a rename in a later aioquic 1.x fails there rather than in the TypeScript suite's Chromium target.
"""

from __future__ import annotations

from aioquic.h3.connection import FrameType, H3Connection


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


def peer_control_stream_id(h3: H3Connection) -> int | None:
    """The peer's HTTP/3 control stream, once its stream type has been read; ``None`` before that.

    The protocol needs it to recognise the peer ending that stream (see ``protocol.WebTransportProtocol``),
    which aioquic otherwise turns into a connection close before later events in the same flight.
    """
    return h3._peer_control_stream_id
