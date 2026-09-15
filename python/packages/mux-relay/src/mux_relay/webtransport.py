"""WebTransport over HTTP/3 on aioquic: the default :class:`moq_core.MoqtTransport` for this SDK.

A browser hands the TypeScript SDK a ``WebTransport`` object. Python has no such built-in, so this
module implements the client side of draft-ietf-webtrans-http3 on top of aioquic's QUIC and HTTP/3
layers: the extended CONNECT handshake (with ``WT-Available-Protocols`` carrying the MOQT protocol
identifier), session-bound unidirectional and bidirectional streams, the CLOSE_WEBTRANSPORT_SESSION
capsule, and the application/HTTP-3 error-code mapping.

:class:`WebTransportSessionBase` holds everything that is common to both ends of a session; the
client role lives in :class:`WebTransportSession`, and the test suite adds a server role on the same
base so the loopback test exercises the real stream code.
"""

from __future__ import annotations

import asyncio
import socket
from collections import deque
from collections.abc import AsyncIterator, Callable, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.h3.connection import ErrorCode, FrameType, H3Connection
from aioquic.h3.events import DataReceived, H3Event, HeadersReceived, WebTransportStreamDataReceived
from aioquic.quic import events as quic_events
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.connection import QuicConnection, stream_is_client_initiated, stream_is_unidirectional
from aioquic.quic.packet import QuicErrorCode
from aioquic.quic.stream import QuicStream

from moq_core._tasks import observe, spawn
from moq_core.transport import BidirectionalStream, ReceiveStream, SendStream

from .connect_session import ConnectingTransport

# ---------------------------------------------------------------------------------------------------
# Wire constants
# ---------------------------------------------------------------------------------------------------

CAPSULE_CLOSE_WEBTRANSPORT_SESSION = 0x2843
CAPSULE_DRAIN_WEBTRANSPORT_SESSION = 0x78AE

_WT_FIRST_H3_ERROR_CODE = 0x52E4A40FA8DB
_WT_LAST_H3_ERROR_CODE = 0x52E5AC983162

H3_ALPN = "h3"
_DEFAULT_PORT = 443
_MAX_DATAGRAM_FRAME_SIZE = 65536
_CLOSE_DRAIN_TIMEOUT = 5.0
"""Seconds ``close()`` waits for the peer to acknowledge what is still in flight before the connection goes."""
_FIN_RETRY_INTERVAL = 0.25
"""Seconds between sweeps that re-queue a FIN aioquic dropped under a full congestion window."""

# The send half's backpressure, the close drain, stream pruning and the FIN retry read aioquic state
# its public API does not expose: QuicStreamSender._pending, _pending_eof, _buffer, _buffer_fin,
# _acked_fin and _reset_error_code (with buffer_is_empty and is_finished), QuicStreamReceiver.is_finished,
# QuicConnection._streams, H3Connection._stream, and the asyncio protocol's _transmit_soon. A rename in
# a later aioquic 1.x (the package pins ``aioquic>=1.2,<2``) would fail silently and only under load, as
# a write that never settles or a FIN that is never re-armed, so
# tests/test_webtransport.py::TestAioquicInternals pins every one of them. Revisit both with any bump.


def webtransport_to_h3_error_code(code: int) -> int:
    """Map a 32-bit WebTransport application error code onto the reserved HTTP/3 range."""
    if code < 0 or code > 0xFFFFFFFF:
        raise ValueError("WebTransport error codes are 32-bit unsigned integers")

    return _WT_FIRST_H3_ERROR_CODE + code + code // 0x1E


def h3_to_webtransport_error_code(code: int) -> int | None:
    """Recover the application error code from an HTTP/3 stream error, or ``None`` when it is not one."""
    if code < _WT_FIRST_H3_ERROR_CODE or code > _WT_LAST_H3_ERROR_CODE:
        return None

    shifted = code - _WT_FIRST_H3_ERROR_CODE

    if shifted % 0x1F == 0x1E:
        return None

    return shifted - shifted // 0x1F


def _encode_uint_var(value: int) -> bytes:
    """RFC 9000 variable-length integer (HTTP/3 framing, distinct from MOQT's varint)."""
    if value < 0x40:
        return value.to_bytes(1, "big")
    if value < 0x4000:
        return (value | 0x4000).to_bytes(2, "big")
    if value < 0x40000000:
        return (value | 0x80000000).to_bytes(4, "big")
    return (value | 0xC000000000000000).to_bytes(8, "big")


def _decode_uint_var(data: bytes, offset: int) -> tuple[int, int] | None:
    if offset >= len(data):
        return None

    length = 1 << (data[offset] >> 6)

    if offset + length > len(data):
        return None

    value = int.from_bytes(data[offset : offset + length], "big") & ((1 << (8 * length - 2)) - 1)
    return value, offset + length


def encode_close_session_capsule(code: int, reason: str) -> bytes:
    payload = code.to_bytes(4, "big") + reason.encode("utf-8")[:1024]
    return _encode_uint_var(CAPSULE_CLOSE_WEBTRANSPORT_SESSION) + _encode_uint_var(len(payload)) + payload


def sf_list_of_strings(items: Sequence[str]) -> bytes:
    """Serialize an RFC 9651 list of strings, the ``WT-Available-Protocols`` value shape."""
    return ", ".join('"' + item.replace("\\", "\\\\").replace('"', '\\"') + '"' for item in items).encode("ascii")


def parse_sf_string(value: bytes) -> str:
    """Parse one RFC 9651 string item, the ``WT-Protocol`` value shape. Tolerates a bare token."""
    text = value.decode("ascii", errors="replace").strip()

    if len(text) >= 2 and text[0] == '"' and text[-1] == '"':
        return text[1:-1].replace('\\"', '"').replace("\\\\", "\\")

    return text


def parse_sf_list_of_strings(value: bytes) -> list[str]:
    return [
        parse_sf_string(item.encode("ascii")) for item in value.decode("ascii", errors="replace").split(",") if item
    ]


# ---------------------------------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------------------------------


class WebTransportError(Exception):
    """A WebTransport session or stream failure. ``code`` is the peer's application error code when known."""

    def __init__(self, message: str, *, code: int | None = None, reason: str = "") -> None:
        super().__init__(message)
        self.code = code
        self.reason = reason


def _error_code_of(reason: object) -> int:
    if isinstance(reason, WebTransportError) and reason.code is not None:
        return reason.code

    if isinstance(reason, int) and not isinstance(reason, bool) and 0 <= reason <= 0xFFFFFFFF:
        return reason

    return 0


# ---------------------------------------------------------------------------------------------------
# Stream halves
# ---------------------------------------------------------------------------------------------------


class _ReceiveHalf:
    """The receiving side of a WebTransport stream; satisfies :class:`ReceiveStream`."""

    __slots__ = ("_cancelled", "_chunks", "_eof", "_error", "_session", "_stream_id", "_waiter")

    def __init__(self, session: WebTransportSessionBase, stream_id: int) -> None:
        self._session = session
        self._stream_id = stream_id
        self._chunks: deque[bytes] = deque()
        self._eof = False
        self._error: BaseException | None = None
        self._cancelled = False
        self._waiter: asyncio.Future[None] | None = None

    async def read(self) -> bytes | None:
        while True:
            if self._chunks:
                return self._chunks.popleft()

            if self._cancelled or self._eof:
                return None

            if self._error is not None:
                raise self._error

            self._waiter = asyncio.get_running_loop().create_future()
            await self._waiter

    def cancel(self, reason: object = None) -> None:
        if self._cancelled:
            return

        self._cancelled = True
        self._chunks.clear()
        self._session._stop_stream(self._stream_id, _error_code_of(reason))
        self._wake()

    def feed(self, data: bytes, end_stream: bool) -> None:
        if self._cancelled or self._eof or self._error is not None:
            return

        if data:
            self._chunks.append(data)

        if end_stream:
            self._eof = True

        self._wake()

    def fail(self, error: BaseException) -> None:
        if self._eof or self._error is not None:
            return

        self._error = error
        self._wake()

    @property
    def ended(self) -> bool:
        """The peer finished this half (FIN or reset); nothing more can arrive on it."""
        return self._eof or self._error is not None

    def _wake(self) -> None:
        waiter = self._waiter

        if waiter is not None and not waiter.done():
            waiter.set_result(None)

        self._waiter = None


class _SendHalf:
    """The sending side of a WebTransport stream; satisfies :class:`SendStream`.

    aioquic queues written bytes without limit and transmits them as congestion and flow control
    allow, so ``write()`` and ``close()`` settle only once everything queued on the stream so far
    has been handed to the network. That is the backpressure a browser ``WritableStream`` applies:
    a slow link is felt as a slow write, and a caller's deadline covers the transport write.
    """

    __slots__ = ("_aborted", "_closed", "_error", "_session", "_stopped", "_stream_id", "_transmitted")

    def __init__(self, session: WebTransportSessionBase, stream_id: int) -> None:
        self._session = session
        self._stream_id = stream_id
        self._closed = False
        self._aborted = False
        self._stopped: int | None = None
        self._error: BaseException | None = None
        self._transmitted: asyncio.Future[None] | None = None
        """Pending while queued bytes (or the FIN) await transmission; ``None`` when nothing is waiting."""

    def _check_writable(self) -> None:
        if self._error is not None:
            raise self._error

        if self._aborted:
            raise WebTransportError("stream was aborted")

        if self._closed:
            raise WebTransportError("stream is closed")

        if self._stopped is not None:
            raise WebTransportError("peer stopped the stream", code=self._stopped)

        self._session._check_live()

    async def write(self, data: bytes) -> None:
        self._check_writable()

        if data:
            await self._session._send(self, bytes(data), False)

    async def close(self) -> None:
        self._check_writable()
        self._closed = True
        await self._session._send(self, b"", True)

    def abort(self, reason: object = None) -> None:
        # A FIN that already went out is a complete stream at the peer; a reset now would only destroy it.
        if self._aborted or self._error is not None or (self._closed and self._transmitted is None):
            return

        self._aborted = True
        # Fail a pending write with the caller's reason before resetting: `_reset_stream` transmits,
        # and the reset branch of `_settle_stream` would otherwise settle the waiter first with a
        # generic "stream was aborted", masking a `write_timeout`'s RelayTimeoutError.
        self._fail_transmit(reason if isinstance(reason, BaseException) else WebTransportError("stream was aborted"))
        self._session._reset_stream(self._stream_id, _error_code_of(reason))

    def stopped(self, code: int) -> None:
        if self._stopped is None:
            self._stopped = code
            # aioquic answers STOP_SENDING with a reset of its own, so queued bytes will never go out.
            self._fail_transmit(WebTransportError("peer stopped the stream", code=code))

    def fail(self, error: BaseException) -> None:
        if self._error is None and not self._closed and not self._aborted:
            self._error = error

        self._fail_transmit(error)

    def _transmit_waiter(self) -> asyncio.Future[None]:
        # Never hand out a future that is already done (settled, failed, or cancelled from outside):
        # a later write or close on the stream must get a live waiter of its own.
        if self._transmitted is None or self._transmitted.done():
            self._transmitted = asyncio.get_running_loop().create_future()
            observe(self._transmitted)

        return self._transmitted

    def _settle_transmit(self) -> None:
        waiter, self._transmitted = self._transmitted, None

        if waiter is not None and not waiter.done():
            waiter.set_result(None)

    def _fail_transmit(self, error: BaseException) -> None:
        waiter, self._transmitted = self._transmitted, None

        if waiter is not None and not waiter.done():
            waiter.set_exception(error)


class _StreamQueue:
    """Incoming streams, iterated by the session drivers' accept loops."""

    def __init__(self) -> None:
        self._items: deque[Any] = deque()
        self._waiter: asyncio.Future[None] | None = None
        self._closed = False

    def push(self, item: Any) -> None:
        if self._closed:
            return

        self._items.append(item)
        self._wake()

    def close(self) -> None:
        self._closed = True
        self._wake()

    def _wake(self) -> None:
        if self._waiter is not None and not self._waiter.done():
            self._waiter.set_result(None)

        self._waiter = None

    def __aiter__(self) -> _StreamQueue:
        return self

    async def __anext__(self) -> Any:
        while True:
            if self._items:
                return self._items.popleft()

            if self._closed:
                raise StopAsyncIteration

            self._waiter = asyncio.get_running_loop().create_future()
            await self._waiter


# ---------------------------------------------------------------------------------------------------
# QUIC protocol: one HTTP/3 connection carrying WebTransport sessions
# ---------------------------------------------------------------------------------------------------


class WebTransportProtocol(QuicConnectionProtocol):
    """An aioquic connection protocol that drives HTTP/3 with WebTransport enabled.

    Routes HTTP/3 and QUIC stream events to the :class:`WebTransportSessionBase` that owns them and
    hands request headers that belong to no session (a server's incoming CONNECT) to
    ``on_request_headers``.
    """

    def __init__(self, quic: QuicConnection, stream_handler: Any = None, *, owns_transport: bool = False) -> None:
        super().__init__(quic, stream_handler)
        self.h3 = H3Connection(quic, enable_webtransport=True)
        self.sessions: dict[int, WebTransportSessionBase] = {}
        self.stream_owners: dict[int, WebTransportSessionBase] = {}
        self.on_request_headers: Callable[[HeadersReceived], None] | None = None
        self.terminated: quic_events.ConnectionTerminated | None = None
        self.terminated_event: asyncio.Event = asyncio.Event()
        self.owns_transport = owns_transport
        """Whether the UDP transport is this connection's own (a client) or a listener's shared socket (a server)."""
        self.transport_closed: asyncio.Event = asyncio.Event()
        """Set once the connection is over and its UDP socket is released (or was never ours to release)."""
        self._orphans: dict[int, list[WebTransportStreamDataReceived]] = {}
        self._settling: dict[int, WebTransportSessionBase] = {}
        """Streams whose owner waits on a transmission or acknowledgement (a write, a FIN, a reset)."""
        self._draining: list[WebTransportSessionBase] = []
        self._fin_retry_handle: asyncio.TimerHandle | None = None

    def register_session(self, session: WebTransportSessionBase) -> None:
        self.sessions[session.session_id] = session
        self.stream_owners[session.session_id] = session

        for event in self._orphans.pop(session.session_id, []):
            session._on_stream_data(event)

    def watch_stream(self, stream_id: int, session: WebTransportSessionBase) -> None:
        self._settling[stream_id] = session
        self._schedule_fin_retry()

    def watch_drain(self, session: WebTransportSessionBase) -> None:
        if session not in self._draining:
            self._draining.append(session)

    def _schedule_fin_retry(self) -> None:
        if self._fin_retry_handle is None and self._settling and self.terminated is None:
            self._fin_retry_handle = self._loop.call_later(_FIN_RETRY_INTERVAL, self._retry_dropped_fins)

    def _retry_dropped_fins(self) -> None:
        """Periodically re-queue FINs aioquic dropped under a full window (see ``_rearm_fin``).

        A time-based sweep, not a per-transmit one: a FIN can be dropped and the window then open
        before the next transmit, so a congestion-gated retry would miss it, while re-queuing on every
        transmit would resend a healthy in-flight FIN each round. On a healthy link a FIN is acknowledged
        within the interval and is never re-queued; a genuinely dropped one is retried until it lands.
        """
        self._fin_retry_handle = None

        if self.terminated is not None:
            return

        streams = self._quic._streams
        rearmed = False

        for stream_id, session in list(self._settling.items()):
            if session._rearm_fin(stream_id, streams.get(stream_id)):
                rearmed = True

        if rearmed:
            self.transmit()

        self._schedule_fin_retry()

    def transmit(self) -> None:
        super().transmit()

        # Everything that moves a stream along (a flight sent, an ACK or STOP_SENDING processed, a
        # timer) ends in a transmit, so this is the one place to settle waiting writers and drains.
        if self._settling or self._draining:
            self._settle()

    def _settle(self) -> None:
        streams = self._quic._streams

        for stream_id, session in list(self._settling.items()):
            if not session._settle_stream(stream_id, streams.get(stream_id)):
                self._settling.pop(stream_id, None)

        for session in list(self._draining):
            if session._check_drained():
                self._draining.remove(session)

    def connection_lost(self, exc: Exception | None) -> None:
        self._cancel_fin_retry()
        self.transport_closed.set()

    def _cancel_fin_retry(self) -> None:
        if self._fin_retry_handle is not None:
            self._fin_retry_handle.cancel()
            self._fin_retry_handle = None

    def quic_event_received(self, event: quic_events.QuicEvent) -> None:
        if isinstance(event, quic_events.StreamReset):
            owner = self.stream_owners.get(event.stream_id)

            if owner is not None:
                owner._on_stream_reset(event.stream_id, event.error_code)

            return

        if isinstance(event, quic_events.StopSendingReceived):
            owner = self.stream_owners.get(event.stream_id)

            if owner is not None:
                owner._on_stop_sending(event.stream_id, event.error_code)

            return

        if isinstance(event, quic_events.ConnectionTerminated):
            self.terminated = event
            self.terminated_event.set()
            self._orphans.clear()
            self._settling.clear()
            self._draining.clear()
            self._cancel_fin_retry()

            for session in list(self.sessions.values()):
                session._on_terminated(event)

            self._close_transport_soon()
            return

        for h3_event in self.h3.handle_event(event):
            self._handle_h3_event(h3_event)

    def _handle_h3_event(self, event: H3Event) -> None:
        if isinstance(event, WebTransportStreamDataReceived):
            session = self.sessions.get(event.session_id)

            if session is not None:
                session._on_stream_data(event)
            else:
                # A stream can outrun the CONNECT it belongs to; hold its data until that session exists.
                self._orphans.setdefault(event.session_id, []).append(event)

            return

        if isinstance(event, HeadersReceived):
            session = self.sessions.get(event.stream_id)

            if session is not None:
                session._on_connect_headers(event)
            elif self.on_request_headers is not None:
                self.on_request_headers(event)

            return

        if isinstance(event, DataReceived):
            session = self.sessions.get(event.stream_id)

            if session is not None:
                session._on_connect_data(event.data, event.stream_ended)

    def _close_transport_soon(self) -> None:
        transport = self._transport

        if self.owns_transport and transport is not None:
            self._loop.call_soon(transport.close)
        else:
            # A server connection shares its listener's socket; there is nothing of ours to release.
            self.transport_closed.set()

    def close_after_drain(self, error_code: int = ErrorCode.H3_NO_ERROR, reason_phrase: str = "") -> None:
        """Close the QUIC connection, and make sure the UDP socket is released even if the peer never answers."""
        self.close(error_code=error_code, reason_phrase=reason_phrase)

        def release() -> None:
            if self.owns_transport and self._transport is not None and not self.terminated_event.is_set():
                self._transport.close()

        self._loop.call_later(3.0, release)


def _finish_receiving_half(quic: QuicConnection, stream_id: int) -> None:
    """Mark a locally opened unidirectional stream's receiving half done so aioquic can discard the stream.

    aioquic 1.3 leaves that half unfinished although nothing can ever arrive on it, so the stream
    would otherwise stay in ``QuicConnection._streams`` (and in the per-packet scheduling loop) for
    the life of the connection after its FIN or reset was acknowledged.
    """
    stream = quic._streams.get(stream_id)

    if stream is not None:
        stream.receiver.is_finished = True


# ---------------------------------------------------------------------------------------------------
# Session base
# ---------------------------------------------------------------------------------------------------


class WebTransportSessionBase:
    """Stream plumbing for one WebTransport session on a :class:`WebTransportProtocol`."""

    def __init__(self) -> None:
        loop = asyncio.get_running_loop()
        self._protocol: WebTransportProtocol | None = None
        self.session_id: int = -1
        self._receives: dict[int, _ReceiveHalf] = {}
        self._sends: dict[int, _SendHalf] = {}
        self._incoming_uni = _StreamQueue()
        self._incoming_bidi = _StreamQueue()
        self._closed: asyncio.Future[None] = loop.create_future()
        observe(self._closed)
        self._closing = False
        self._drained: asyncio.Future[None] | None = None
        self._connect_buffer = b""
        self.close_info: tuple[int, str] | None = None
        """``(code, reason)`` from the peer's CLOSE_WEBTRANSPORT_SESSION capsule, once received."""

    # ------------------------------------------------------------------ MoqtTransport

    def incoming_unidirectional_streams(self) -> AsyncIterator[ReceiveStream]:
        return self._incoming_uni

    def incoming_bidirectional_streams(self) -> AsyncIterator[BidirectionalStream]:
        return self._incoming_bidi

    async def create_unidirectional_stream(self) -> SendStream:
        protocol = self._require_live()
        stream_id = protocol.h3.create_webtransport_stream(self.session_id, is_unidirectional=True)
        _finish_receiving_half(protocol._quic, stream_id)
        send = _SendHalf(self, stream_id)
        self._sends[stream_id] = send
        protocol.stream_owners[stream_id] = self
        protocol.transmit()
        return send

    async def create_bidirectional_stream(self) -> BidirectionalStream:
        protocol = self._require_live()
        stream_id = protocol.h3.create_webtransport_stream(self.session_id)
        # aioquic writes the WEBTRANSPORT_STREAM frame header but does not record the stream as a
        # WebTransport stream, so the peer's reply bytes would be parsed as HTTP/3 frames. Mark it.
        with protocol.h3._get_or_create_stream(stream_id) as h3_stream:
            h3_stream.frame_type = FrameType.WEBTRANSPORT_STREAM
            h3_stream.session_id = self.session_id

        send = _SendHalf(self, stream_id)
        receive = _ReceiveHalf(self, stream_id)
        self._sends[stream_id] = send
        self._receives[stream_id] = receive
        protocol.stream_owners[stream_id] = self
        protocol.transmit()
        return BidirectionalStream(readable=receive, writable=send)

    def close(self, close_code: int = 0, reason: str = "") -> None:
        """Send CLOSE_WEBTRANSPORT_SESSION, wait for in-flight data to be acknowledged, then close the connection.

        Writes fail from here on; the session reports itself closed once the connection is gone.
        """
        if self._closing:
            return

        self._closing = True
        protocol = self._protocol

        if protocol is None or protocol.terminated is not None:
            self._finish(None)
            return

        try:
            protocol.h3.send_data(self.session_id, encode_close_session_capsule(close_code, reason), end_stream=True)
            protocol.transmit()
        except Exception:
            pass

        spawn(self._drain_then_close(protocol))

    async def wait_closed(self) -> None:
        """Return once the session ended and its UDP socket is released, so a process may exit right after.

        Raises the error that ended the session, if any.
        """
        if not self._closed.done():
            await asyncio.wait({self._closed})

        protocol = self._transport_protocol()

        if protocol is not None:
            await protocol.transport_closed.wait()

        self._closed.result()

    # ------------------------------------------------------------------ role hooks

    def _transport_protocol(self) -> WebTransportProtocol | None:
        """The connection whose shutdown ``wait_closed`` also waits for."""
        return self._protocol

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
        """The peer ended the session cleanly: finish, then close the connection ourselves."""
        protocol = self._protocol
        self._finish(None)

        if protocol is not None and protocol.terminated is None and not self._closing:
            self._closing = True
            protocol.close_after_drain()

    async def _drain_then_close(self, protocol: WebTransportProtocol) -> None:
        # Closing a QUIC connection discards queued stream data, and whatever the peer has not
        # acknowledged is lost with it. So wait until every send stream, the close capsule included,
        # has been acknowledged, within a bound so a peer that went silent cannot hold the close.
        try:
            await asyncio.wait({self._wait_drained(protocol)}, timeout=_CLOSE_DRAIN_TIMEOUT)
        finally:
            if protocol.terminated is None:
                protocol.close_after_drain()

    def _wait_drained(self, protocol: WebTransportProtocol) -> asyncio.Future[None]:
        if self._drained is None:
            self._drained = asyncio.get_running_loop().create_future()
            observe(self._drained)
            protocol.watch_drain(self)
            self._check_drained()

        return self._drained

    def _check_drained(self) -> bool:
        """After a transmit while closing: settle ``_drained`` once the peer acknowledged everything we sent.

        Returns ``True`` when there is nothing left to watch.
        """
        drained = self._drained

        if drained is None or drained.done():
            return True

        protocol = self._protocol

        if protocol is None or protocol.terminated is not None:
            drained.set_result(None)
            return True

        streams = protocol._quic._streams

        for stream_id in (*self._sends, self.session_id):
            stream = streams.get(stream_id)

            if stream is None or stream.sender.is_finished:
                continue

            sender = stream.sender

            if sender._reset_error_code is not None:
                return False  # the reset is not acknowledged yet

            if sender._buffer or sender._pending_eof or (sender._buffer_fin is not None and not sender._acked_fin):
                return False  # data or the FIN is not acknowledged yet

        drained.set_result(None)
        return True

    # ------------------------------------------------------------------ event routing

    def _on_stream_data(self, event: WebTransportStreamDataReceived) -> None:
        receive = self._receives.get(event.stream_id)

        if receive is None:
            protocol = self._protocol
            if protocol is None or self._closing:
                return

            # A peer-initiated stream. Its ID parity tells the direction.
            is_client = protocol._quic.configuration.is_client

            if stream_is_client_initiated(event.stream_id) == is_client:
                return  # Our own unidirectional stream never carries inbound data; ignore stray events.

            receive = _ReceiveHalf(self, event.stream_id)
            self._receives[event.stream_id] = receive
            protocol.stream_owners[event.stream_id] = self

            if stream_is_unidirectional(event.stream_id):
                self._incoming_uni.push(receive)
            else:
                send = _SendHalf(self, event.stream_id)
                self._sends[event.stream_id] = send
                self._incoming_bidi.push(BidirectionalStream(readable=receive, writable=send))

        # A half that already ended ignores this (a retransmitted FIN reports end-of-stream again).
        receive.feed(event.data, event.stream_ended)

        if event.stream_ended:
            # Keep the entries until aioquic discards the stream, so a retransmitted FIN still finds
            # this half instead of looking like a new incoming stream; the watch does the forgetting.
            self._watch(event.stream_id)

    def _on_stream_reset(self, stream_id: int, error_code: int) -> None:
        receive = self._receives.get(stream_id)

        if receive is not None:
            code = h3_to_webtransport_error_code(error_code)
            receive.fail(WebTransportError(f"peer reset the stream (code {code})", code=code))
            self._watch(stream_id)

    def _on_stop_sending(self, stream_id: int, error_code: int) -> None:
        send = self._sends.get(stream_id)

        if send is not None:
            send.stopped(h3_to_webtransport_error_code(error_code) or 0)
            # aioquic resets the stream on our behalf; forget it once that reset is acknowledged.
            self._watch(stream_id)

    def _watch(self, stream_id: int) -> None:
        if self._protocol is not None:
            self._protocol.watch_stream(stream_id, self)

    # ------------------------------------------------------------------ per-stream bookkeeping

    def _settle_stream(self, stream_id: int, stream: QuicStream | None) -> bool:
        """After a transmit, for a watched stream: ``True`` while it still needs watching.

        A write and the FIN both settle once their bytes have been handed to the wire (nothing left
        pending on the stream), so a caller is not held for a full round trip on a healthy link. A
        closed send half stays watched until its FIN is acknowledged, though: aioquic can silently
        drop a FIN-only frame under a full congestion window, and :meth:`_rearm_fin` re-queues it
        until it truly lands, so the FIN reaches the peer even when the write it settled has returned.
        ``close()`` on the session waits for that acknowledgement before tearing the connection down.

        Per-stream state is dropped only once aioquic itself has discarded the stream (both halves
        done and the FIN or reset acknowledged). Until then a retransmitted FIN still finds the
        receive half and is ignored, instead of being taken for a new incoming stream.
        """
        send = self._sends.get(stream_id)
        receive = self._receives.get(stream_id)

        if stream is None:
            # aioquic discarded the stream: nothing more can arrive or be sent on it. For a closed
            # send half that is success, the peer has the whole stream.
            if send is not None:
                if send._closed:
                    send._settle_transmit()
                else:
                    send._fail_transmit(WebTransportError("stream is closed"))

            self._forget_stream(stream_id)
            return False

        if send is None:
            return receive is not None and receive.ended  # until aioquic discards the finished stream

        sender = stream.sender

        if sender.is_finished:
            if send._closed:
                send._settle_transmit()
            else:
                send._fail_transmit(WebTransportError("stream is closed"))

            # Watch on only while the discard is imminent; a bidirectional stream whose receive half
            # is still live re-registers itself when that half ends.
            return receive is None or receive.ended

        if sender._reset_error_code is not None:
            send._fail_transmit(WebTransportError("stream was aborted"))
            return True  # until the reset is acknowledged

        if len(sender._pending) == 0 and not sender._pending_eof:
            send._settle_transmit()

        # A closed send half stays watched until its FIN is acknowledged, even after the write that
        # queued it has settled, so the retry sweep keeps re-queuing a dropped FIN in the background.
        return send._closed or send._transmitted is not None

    def _rearm_fin(self, stream_id: int, stream: QuicStream | None) -> bool:
        """Re-queue a FIN that aioquic consumed but never put on the wire (the retry sweep's per-stream step).

        ``get_frame`` hands back a FIN-only frame even when there is no flight space for it, so the
        following ``start_frame`` rejects it (``QuicPacketBuilderStop``) and the FIN is lost with
        nothing to reschedule it: the peer never sees the stream end and the group is never marked
        complete. Re-arm a closed send half's buffered, unacknowledged FIN that is not currently
        queued. A duplicate FIN, if the frame did leave, is harmless: the peer's final size is
        unchanged. Returns whether it re-queued anything.
        """
        send = self._sends.get(stream_id)

        if stream is None or send is None or not send._closed:
            return False

        sender = stream.sender

        if (
            sender._buffer_fin is not None
            and not sender._acked_fin
            and not sender._pending_eof
            and sender._reset_error_code is None
            and not sender.is_finished
        ):
            sender._pending_eof = True
            sender.buffer_is_empty = False
            return True

        return False

    def _forget_stream(self, stream_id: int) -> None:
        """aioquic discarded the stream: drop both halves, the routing entry and the HTTP/3 record it leaves behind."""
        self._sends.pop(stream_id, None)
        self._receives.pop(stream_id, None)
        protocol = self._protocol

        if protocol is None or stream_id == self.session_id:
            return

        protocol.stream_owners.pop(stream_id, None)
        # aioquic's HTTP/3 layer forgets a stream only once both halves ended, which it never learns
        # for WebTransport streams whose bytes bypass it: every incoming one, and our bidirectional ones.
        protocol.h3._stream.pop(stream_id, None)

    def _on_terminated(self, event: quic_events.ConnectionTerminated) -> None:
        clean = self._closing or event.error_code in (QuicErrorCode.NO_ERROR, ErrorCode.H3_NO_ERROR)
        error = (
            None
            if clean
            else WebTransportError(
                f"QUIC connection terminated: {event.reason_phrase or 'no reason given'} (code 0x{event.error_code:x})",
                code=event.error_code,
                reason=event.reason_phrase,
            )
        )
        self._finish(error)

    def _finish(self, error: BaseException | None) -> None:
        if self._closed.done():
            return

        if error is None:
            self._closed.set_result(None)
        else:
            self._closed.set_exception(error)

        stream_error = error if error is not None else WebTransportError("WebTransport session closed")

        for receive in self._receives.values():
            receive.fail(stream_error)

        for send in self._sends.values():
            send.fail(stream_error)

        self._incoming_uni.close()
        self._incoming_bidi.close()

        if self._drained is not None and not self._drained.done():
            self._drained.set_result(None)

    # ------------------------------------------------------------------ QUIC plumbing for the halves

    def _require_live(self) -> WebTransportProtocol:
        self._check_live()
        assert self._protocol is not None
        return self._protocol

    def _check_live(self) -> None:
        if self._protocol is None:
            raise WebTransportError("WebTransport session is not established")

        if self._closed.done():
            error = self._closed.exception() if not self._closed.cancelled() else None
            raise error if error is not None else WebTransportError("WebTransport session closed")

        if self._closing:
            raise WebTransportError("WebTransport session closed")

    async def _send(self, send: _SendHalf, data: bytes, end_stream: bool) -> None:
        protocol = self._require_live()
        protocol._quic.send_stream_data(send._stream_id, data, end_stream)
        waiter = send._transmit_waiter()
        protocol.watch_stream(send._stream_id, self)
        # Coalesce concurrent writes into one transmit per loop turn, the way aioquic's own stream
        # adapter does. Transmitting per write instead packs packets so tightly that a following
        # FIN-only frame can be dropped (aioquic consumes it even when the packet is already full).
        protocol._transmit_soon()
        # Shielded: a caller that stops waiting (a deadline, a cancelled task) must not cancel the
        # future that concurrent and later writers on this stream share. Its bytes stay queued and
        # go out with the rest, the documented "a failed write may have delivered partial data".
        await asyncio.shield(waiter)

    def _reset_stream(self, stream_id: int, code: int) -> None:
        protocol = self._protocol

        if protocol is None or protocol.terminated is not None:
            return

        # A stream aioquic already discarded must not be recreated by the reset.
        if stream_id not in protocol._quic._streams:
            return

        try:
            protocol._quic.reset_stream(stream_id, webtransport_to_h3_error_code(code))
            protocol.watch_stream(stream_id, self)
            protocol.transmit()
        except (ValueError, AssertionError):
            pass

    def _stop_stream(self, stream_id: int, code: int) -> None:
        protocol = self._protocol

        if protocol is None or protocol.terminated is not None:
            return

        try:
            protocol._quic.stop_stream(stream_id, webtransport_to_h3_error_code(code))
            protocol.transmit()
        except (ValueError, AssertionError):
            pass


# ---------------------------------------------------------------------------------------------------
# Client role
# ---------------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Endpoint:
    host: str
    port: int
    authority: str
    path: str


def _parse_url(url: str) -> _Endpoint:
    parts = urlsplit(url)

    if parts.scheme != "https":
        raise ValueError("WebTransport endpoints must use HTTPS")

    host = parts.hostname
    if not host:
        raise ValueError("WebTransport endpoints must name a host")

    port = parts.port or _DEFAULT_PORT
    authority = host if port == _DEFAULT_PORT else f"{host}:{port}"

    if ":" in host and not authority.startswith("["):
        authority = f"[{host}]" if port == _DEFAULT_PORT else f"[{host}]:{port}"

    path = parts.path or "/"
    if parts.query:
        path = f"{path}?{parts.query}"

    return _Endpoint(host=host, port=port, authority=authority, path=path)


class WebTransportSession(WebTransportSessionBase):
    """A client WebTransport session. Create one with :func:`connect_webtransport`.

    ``ready`` resolves once the server accepted the CONNECT; ``protocol_id`` is the protocol the
    server selected from ``protocols`` (``None`` when it named none).
    """

    def __init__(self, url: str, protocols: Sequence[str], configuration: QuicConfiguration | None = None) -> None:
        super().__init__()
        loop = asyncio.get_running_loop()
        self.url = url
        self.protocols = list(protocols)
        self.protocol_id: str | None = None
        self.ready: asyncio.Future[None] = loop.create_future()
        observe(self.ready)
        self._configuration = configuration
        self._pending_protocol: WebTransportProtocol | None = None
        self._connect_task = spawn(self._connect())

    # ------------------------------------------------------------------ connecting

    async def _connect(self) -> None:
        loop = asyncio.get_running_loop()

        try:
            endpoint = _parse_url(self.url)
            infos = await loop.getaddrinfo(endpoint.host, endpoint.port, type=socket.SOCK_DGRAM)

            if not infos:
                raise WebTransportError(f"could not resolve {endpoint.host}")

            configuration = self._build_configuration(endpoint.host)
            connection = QuicConnection(configuration=configuration)
            sock, addr = _open_socket([info[4] for info in infos])
            udp_transport, protocol = await loop.create_datagram_endpoint(
                lambda: WebTransportProtocol(connection, owns_transport=True), sock=sock
            )
            assert isinstance(protocol, WebTransportProtocol)
            self._pending_protocol = protocol

            if self._closing:
                udp_transport.close()
                raise WebTransportError("closed before the session was established")

            protocol.connect(addr)
            await protocol.wait_connected()

            if protocol.terminated is not None:
                raise WebTransportError("QUIC handshake failed", reason=protocol.terminated.reason_phrase)

            stream_id = protocol._quic.get_next_available_stream_id()
            self._attach(protocol, stream_id)
            protocol.h3.send_headers(stream_id, self._connect_headers(endpoint))
            protocol.transmit()
        except BaseException as error:
            if not self.ready.done():
                self.ready.set_exception(
                    error
                    if isinstance(error, WebTransportError)
                    else WebTransportError(f"WebTransport connect failed: {error}")
                )

            self._finish(self.ready.exception() if not self.ready.cancelled() else None)
            self._teardown_connection()

            if isinstance(error, asyncio.CancelledError):
                raise

    def _build_configuration(self, host: str) -> QuicConfiguration:
        if self._configuration is not None:
            configuration = self._configuration
        else:
            configuration = QuicConfiguration(is_client=True)

        configuration.is_client = True
        configuration.alpn_protocols = [H3_ALPN]

        if configuration.server_name is None:
            configuration.server_name = host

        if configuration.max_datagram_frame_size is None:
            configuration.max_datagram_frame_size = _MAX_DATAGRAM_FRAME_SIZE

        return configuration

    def _connect_headers(self, endpoint: _Endpoint) -> list[tuple[bytes, bytes]]:
        headers = [
            (b":method", b"CONNECT"),
            (b":scheme", b"https"),
            (b":authority", endpoint.authority.encode("idna")),
            (b":path", endpoint.path.encode("utf-8")),
            (b":protocol", b"webtransport"),
            (b"origin", b"https://" + endpoint.authority.encode("idna")),
            (b"sec-webtransport-http3-draft02", b"1"),
        ]

        if self.protocols:
            headers.append((b"wt-available-protocols", sf_list_of_strings(self.protocols)))

        return headers

    def _on_connect_headers(self, event: HeadersReceived) -> None:
        # A response that crossed with a local close arrives at a session that already rejects
        # writes, so it must not revive `ready`; the connection is already on its way out.
        if self.ready.done() or self._closing:
            return

        headers = {key.lower(): value for key, value in event.headers}
        status = headers.get(b":status", b"")

        if not status.startswith(b"2"):
            error = WebTransportError(f"WebTransport CONNECT rejected with status {status.decode(errors='replace')}")
            self.ready.set_exception(error)
            self._finish(error)
            self._teardown_connection()
            return

        selected = headers.get(b"wt-protocol")

        if selected is not None:
            self.protocol_id = parse_sf_string(selected)

            if self.protocols and self.protocol_id not in self.protocols:
                error = WebTransportError(f"server selected an unoffered protocol {self.protocol_id!r}")
                self.ready.set_exception(error)
                self._finish(error)
                self._teardown_connection()
                return

        self.ready.set_result(None)

    def close(self, close_code: int = 0, reason: str = "") -> None:
        if self._protocol is None and not self._closing:
            self._closing = True
            self._finish(None)
            self._connect_task.cancel()
            self._teardown_connection()
            return

        # The connection itself goes only after the drain, but an unanswered handshake must not wait
        # with it: this session rejects writes from here on, so a caller awaiting `ready` learns at
        # once instead of after the drain's bound on a peer that stopped acknowledging.
        self._reject_ready(None)
        super().close(close_code, reason)

    def _reject_ready(self, error: BaseException | None) -> None:
        if not self.ready.done():
            self.ready.set_exception(
                error if error is not None else WebTransportError("closed before the session was established")
            )

    def _finish(self, error: BaseException | None) -> None:
        # Every way the session can end (a local close, the QUIC connection terminating, a peer close
        # capsule) runs through here, so a handshake still waiting on the CONNECT response fails now
        # instead of hanging until the caller's connect deadline.
        self._reject_ready(error)
        super()._finish(error)

    def _transport_protocol(self) -> WebTransportProtocol | None:
        return self._protocol or self._pending_protocol

    def _teardown_connection(self) -> None:
        protocol = self._protocol or self._pending_protocol

        if protocol is not None and protocol.terminated is None:
            protocol.close_after_drain(ErrorCode.H3_NO_ERROR)


def _open_socket(addresses: Sequence[tuple[Any, ...]]) -> tuple[socket.socket, tuple[Any, ...]]:
    """Bind a UDP socket and pick the resolved address it can reach.

    Like aioquic's client: a dual-stack IPv6 socket when the host offers one (IPv4 addresses are
    mapped onto it), otherwise an IPv4 socket with the first IPv4 address the resolver returned.
    """
    if not addresses:
        raise WebTransportError("no addresses to connect to")

    try:
        sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
        try:
            sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
            sock.bind(("::", 0, 0, 0))
        except OSError:
            sock.close()
            raise
    except OSError:
        ipv4 = [address for address in addresses if len(address) == 2]

        if not ipv4:
            raise WebTransportError(
                "the host resolves only to IPv6 addresses, but IPv6 sockets are unavailable"
            ) from None

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("0.0.0.0", 0))
        return sock, ipv4[0]

    address = addresses[0]

    if len(address) == 2:
        address = ("::ffff:" + address[0], address[1], 0, 0)

    return sock, address


def connect_webtransport(
    url: str, protocols: Sequence[str] = (), *, configuration: QuicConfiguration | None = None
) -> ConnectingTransport:
    """Start a WebTransport session to ``url``, offering ``protocols`` in ``WT-Available-Protocols``.

    Returns immediately; ``ready`` completes when the server accepts the CONNECT. Pass a
    ``QuicConfiguration`` to control TLS verification (``cafile``/``cadata``/``verify_mode``) or QUIC
    parameters; ALPN and datagram support are set for HTTP/3 regardless.
    """
    session = WebTransportSession(url, protocols, configuration)
    return ConnectingTransport(transport=session, ready=session.ready)


__all__ = [
    "CAPSULE_CLOSE_WEBTRANSPORT_SESSION",
    "CAPSULE_DRAIN_WEBTRANSPORT_SESSION",
    "WebTransportError",
    "WebTransportProtocol",
    "WebTransportSession",
    "WebTransportSessionBase",
    "connect_webtransport",
    "encode_close_session_capsule",
    "h3_to_webtransport_error_code",
    "parse_sf_list_of_strings",
    "parse_sf_string",
    "sf_list_of_strings",
    "webtransport_to_h3_error_code",
]
