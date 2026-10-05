"""A WebTransport session on the SDK's QUIC plumbing: stream creation, the CONNECT stream's capsules, closure.

The stream plumbing (the halves' link, settlement, the close drain) is the SDK's
:class:`~mux_relay.quic._session.QuicStreamSession`; this adds what HTTP/3 puts around it: streams opened
through the HTTP/3 layer, the CLOSE_WEBTRANSPORT_SESSION capsule, and the mapping between application and
HTTP/3 error codes. The server role is ``relay_helpers.wt_server``'s.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable

from aioquic.h3.connection import ErrorCode
from aioquic.h3.events import HeadersReceived, WebTransportStreamDataReceived
from aioquic.quic.packet import QuicErrorCode

from moq_core._tasks import spawn
from moq_core.transport import BidirectionalStream, SendStream
from mux_relay.quic._aioquic import finish_receiving_half
from mux_relay.quic._errors import QuicTransportError
from mux_relay.quic._session import _CLOSE_DRAIN_TIMEOUT, QuicStreamSession

from .h3 import forget_h3_stream, mark_webtransport_stream
from .protocol import WebTransportProtocol
from .wire import (
    CAPSULE_CLOSE_WEBTRANSPORT_SESSION,
    _decode_uint_var,
    encode_close_session_capsule,
    h3_to_webtransport_error_code,
    webtransport_to_h3_error_code,
)


class WebTransportSessionBase(QuicStreamSession[WebTransportProtocol]):
    """Stream plumbing for one WebTransport session on a :class:`WebTransportProtocol`."""

    _CLOSED_MESSAGE = "WebTransport session closed"
    _NOT_ESTABLISHED_MESSAGE = "WebTransport session is not established"
    _CLEAN_CLOSE_CODES = frozenset({QuicErrorCode.NO_ERROR, ErrorCode.H3_NO_ERROR})

    def __init__(self) -> None:
        super().__init__()
        self.session_id: int = -1
        self._connect_buffer = b""
        self.close_info: tuple[int, str] | None = None
        """``(code, reason)`` from the peer's CLOSE_WEBTRANSPORT_SESSION capsule, once received."""

    # ------------------------------------------------------------------ MoqtTransport

    async def create_unidirectional_stream(self) -> SendStream:
        protocol = self._require_live()
        stream_id = protocol.h3.create_webtransport_stream(self.session_id, is_unidirectional=True)
        finish_receiving_half(protocol._quic, stream_id)
        send = self._add_send(protocol, stream_id)
        protocol.transmit()
        return send

    async def create_bidirectional_stream(self) -> BidirectionalStream:
        protocol = self._require_live()
        stream_id = protocol.h3.create_webtransport_stream(self.session_id)
        mark_webtransport_stream(protocol.h3, stream_id, self.session_id)
        stream = self._add_bidirectional(protocol, stream_id)
        protocol.transmit()
        return stream

    def close(self, close_code: int = 0, reason: str = "") -> None:
        """Wait for in-flight data to be acknowledged, send CLOSE_WEBTRANSPORT_SESSION, then close the connection.

        Writes fail from here on; the session reports itself closed once the connection is gone.
        """
        if self._closing:
            return

        self._closing = True
        protocol = self._protocol

        if protocol is None or protocol.terminated is not None:
            self._finish(None)
            return

        spawn(self._drain_then_close(protocol, close_code, reason))

    # ------------------------------------------------------------------ role hooks

    def _attach(self, protocol: WebTransportProtocol, session_id: int) -> None:
        self._protocol = protocol
        self.session_id = session_id
        protocol.register_session(self)

    def _on_connect_headers(self, event: HeadersReceived) -> None:
        """Headers on the CONNECT stream after establishment (trailers) carry nothing this session uses."""

    def _on_connect_data(self, data: bytes, stream_ended: bool) -> None:
        """Capsules ride the CONNECT stream's body."""
        self._connect_buffer += data
        offset = 0

        while True:
            header = _decode_uint_var(self._connect_buffer, offset)
            if header is None:
                break

            capsule_type, after_type = header
            length_field = _decode_uint_var(self._connect_buffer, after_type)
            if length_field is None:
                break

            length, payload_start = length_field
            if payload_start + length > len(self._connect_buffer):
                break

            payload = self._connect_buffer[payload_start : payload_start + length]
            offset = payload_start + length

            if capsule_type == CAPSULE_CLOSE_WEBTRANSPORT_SESSION:
                code = int.from_bytes(payload[:4], "big") if len(payload) >= 4 else 0
                self.close_info = (code, payload[4:].decode("utf-8", errors="replace"))
                self._peer_closed_session()
                return

        self._connect_buffer = self._connect_buffer[offset:]

        if stream_ended:
            self._peer_closed_session()

    def _peer_closed_session(self) -> None:
        """The peer ended the session: finish, then close the connection ourselves.

        A FIN without a capsule, or a capsule with code 0, is a clean end. A nonzero code is the
        peer's reason for ending the session (moq-relay refuses credentials with 0x2, "unauthorized"),
        so the session fails with it instead of reporting a clean close.
        """
        protocol = self._protocol
        code, reason = self.close_info or (0, "")
        self._finish(
            None
            if code == 0
            else QuicTransportError(
                f"peer closed the WebTransport session: {reason or 'no reason given'} (code 0x{code:x})",
                source="session",
                close_code=code,
                reason=reason,
            )
        )

        if protocol is not None and protocol.terminated is None and not self._closing:
            self._closing = True
            protocol.close_after_drain()

    async def _drain_then_close(self, protocol: WebTransportProtocol, close_code: int, reason: str) -> None:
        # Closing a QUIC connection discards queued stream data, and whatever the peer has not
        # acknowledged is lost with it. So wait until every send stream has been acknowledged, then
        # send the close capsule and wait for that too, all within one bound so a peer that went
        # silent cannot hold the close. The capsule goes last because the peer ends the session the
        # moment it reads it and resets every stream of it: a stream FIN that was lost and is still
        # being retransmitted would arrive after the capsule and be discarded (#209).
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _CLOSE_DRAIN_TIMEOUT

        try:
            await asyncio.wait({self._wait_drained(protocol)}, timeout=_CLOSE_DRAIN_TIMEOUT)

            if self._closed.done() or protocol.terminated is not None:
                return

            try:
                capsule = encode_close_session_capsule(close_code, reason)
                protocol.h3.send_data(self.session_id, capsule, end_stream=True)
                protocol.transmit()
            except Exception:
                return

            await asyncio.wait({self._wait_drained(protocol)}, timeout=max(0.0, deadline - loop.time()))
        finally:
            if protocol.terminated is None:
                protocol.close_after_drain()

    def _drain_stream_ids(self) -> Iterable[int]:
        return (*self._sends, self.session_id)

    # ------------------------------------------------------------------ event routing

    def _on_stream_data(self, event: WebTransportStreamDataReceived) -> None:
        self._on_stream_bytes(event.stream_id, event.data, event.stream_ended)

    def _to_wire_code(self, code: int) -> int:
        return webtransport_to_h3_error_code(code)

    def _from_wire_code(self, code: int) -> int | None:
        return h3_to_webtransport_error_code(code)

    def _forget_stream(self, stream_id: int) -> None:
        """aioquic discarded the stream: drop both halves, the routing entry and the HTTP/3 record it leaves behind."""
        self._sends.pop(stream_id, None)
        self._receives.pop(stream_id, None)
        protocol = self._protocol

        if protocol is None or stream_id == self.session_id:
            return

        protocol.stream_owners.pop(stream_id, None)
        forget_h3_stream(protocol.h3, stream_id)
