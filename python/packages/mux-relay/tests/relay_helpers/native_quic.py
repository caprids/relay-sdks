"""The accepting end of a native QUIC session: MOQT straight on a QUIC connection, with no HTTP/3 or WebTransport.

:class:`NativeQuicSession` is a :class:`~mux_relay.webtransport._streams._StreamLink` over raw QUIC streams, so the
conformance peer drives the very stream halves the WebTransport session uses. Codes go on the wire as they are: there
is no HTTP/3 error-code mapping to undo. The session never waits for acknowledgements; a write settles once it was
handed to aioquic, which is all the peer needs.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

from aioquic.asyncio import QuicConnectionProtocol
from aioquic.quic import events
from aioquic.quic.connection import stream_is_client_initiated, stream_is_unidirectional

from moq_core._tasks import observe
from moq_core.transport import BidirectionalStream
from mux_relay.webtransport._errors import WebTransportError
from mux_relay.webtransport._streams import _ReceiveHalf, _SendHalf, _StreamQueue


class NativeQuicSession:
    """The streams of one accepted native QUIC connection, served as the conformance peer's session."""

    def __init__(self, protocol: QuicConnectionProtocol) -> None:
        self._protocol = protocol
        self._quic = protocol._quic
        self._receives: dict[int, _ReceiveHalf] = {}
        self._sends: dict[int, _SendHalf] = {}
        self._incoming_uni = _StreamQueue()
        self._incoming_bidi = _StreamQueue()
        self._closed: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        observe(self._closed)
        self.close_info: tuple[int, str] | None = None
        """``(code, reason)`` of the dialer's application CONNECTION_CLOSE (frame type 0x1d), once received."""

    @property
    def ended(self) -> bool:
        """The connection is over."""
        return self._closed.done()

    def incoming_unidirectional_streams(self) -> AsyncIterator[Any]:
        return self._incoming_uni

    def incoming_bidirectional_streams(self) -> AsyncIterator[Any]:
        return self._incoming_bidi

    def _open(self, unidirectional: bool) -> int:
        self.check_live()
        stream_id = self._quic.get_next_available_stream_id(is_unidirectional=unidirectional)
        # Sending nothing creates the stream, so the next open gets the next id.
        self._quic.send_stream_data(stream_id, b"", False)
        return stream_id

    async def create_unidirectional_stream(self) -> _SendHalf:
        stream_id = self._open(unidirectional=True)
        send = _SendHalf(self, stream_id)
        self._sends[stream_id] = send
        return send

    async def create_bidirectional_stream(self) -> BidirectionalStream:
        stream_id = self._open(unidirectional=False)
        send, receive = _SendHalf(self, stream_id), _ReceiveHalf(self, stream_id)
        self._sends[stream_id] = send
        self._receives[stream_id] = receive
        return BidirectionalStream(readable=receive, writable=send)

    def close(self, code: int, reason: str) -> None:
        """The application close: CONNECTION_CLOSE with frame type 0x1d."""
        if not self.ended:
            self._quic.close(error_code=code, reason_phrase=reason)
            self._protocol.transmit()

    # ------------------------------------------------------------------ events

    def on_event(self, event: events.QuicEvent) -> None:
        if isinstance(event, events.StreamDataReceived):
            self._on_stream_data(event)
        elif isinstance(event, events.StreamReset):
            receive = self._receives.get(event.stream_id)
            if receive is not None:
                code = event.error_code
                receive.fail(WebTransportError(f"peer reset the stream (code {code})", code=code))
        elif isinstance(event, events.StopSendingReceived):
            send = self._sends.get(event.stream_id)
            if send is not None:
                send.stopped(event.error_code)
        elif isinstance(event, events.ConnectionTerminated):
            # aioquic reports no frame type for an application close.
            if event.frame_type is None:
                self.close_info = (event.error_code, event.reason_phrase)
            self._finish()

    def _on_stream_data(self, event: events.StreamDataReceived) -> None:
        receive = self._receives.get(event.stream_id)
        if receive is None:
            if not stream_is_client_initiated(event.stream_id):
                return  # our own unidirectional stream never carries inbound data
            receive = _ReceiveHalf(self, event.stream_id)
            self._receives[event.stream_id] = receive
            if stream_is_unidirectional(event.stream_id):
                self._incoming_uni.push(receive)
            else:
                send = _SendHalf(self, event.stream_id)
                self._sends[event.stream_id] = send
                self._incoming_bidi.push(BidirectionalStream(readable=receive, writable=send))
        receive.feed(event.data, event.end_stream)

    def _finish(self) -> None:
        if self._closed.done():
            return
        self._closed.set_result(None)
        error = WebTransportError("the connection is closed")
        for receive in self._receives.values():
            receive.fail(error)
        for send in self._sends.values():
            send.fail(error)
        self._incoming_uni.close()
        self._incoming_bidi.close()

    # ------------------------------------------------------------------ _StreamLink

    def check_live(self) -> None:
        if self.ended:
            raise WebTransportError("the connection is closed")

    async def send(self, half: _SendHalf, data: bytes, end_stream: bool) -> None:
        self.check_live()
        self._quic.send_stream_data(half.stream_id, data, end_stream)
        waiter = half.transmit_waiter()
        self._protocol.transmit()
        half.settled()
        await waiter

    def reset_stream(self, stream_id: int, code: int) -> None:
        if self.ended:
            return
        try:
            self._quic.reset_stream(stream_id, code)
            self._protocol.transmit()
        except (ValueError, AssertionError):
            pass

    def stop_stream(self, stream_id: int, code: int) -> None:
        if self.ended:
            return
        try:
            self._quic.stop_stream(stream_id, code)
            self._protocol.transmit()
        except (ValueError, AssertionError):
            pass
