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
_CLOSE_FLUSH_TIMEOUT = 0.5


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

    def _wake(self) -> None:
        waiter = self._waiter

        if waiter is not None and not waiter.done():
            waiter.set_result(None)

        self._waiter = None


class _SendHalf:
    """The sending side of a WebTransport stream; satisfies :class:`SendStream`."""

    __slots__ = ("_aborted", "_closed", "_error", "_session", "_stopped", "_stream_id")

    def __init__(self, session: WebTransportSessionBase, stream_id: int) -> None:
        self._session = session
        self._stream_id = stream_id
        self._closed = False
        self._aborted = False
        self._stopped: int | None = None
        self._error: BaseException | None = None

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
            self._session._send_stream_data(self._stream_id, bytes(data), False)

    async def close(self) -> None:
        self._check_writable()
        self._closed = True
        self._session._send_stream_data(self._stream_id, b"", True)

    def abort(self, reason: object = None) -> None:
        if self._closed or self._aborted or self._error is not None:
            return

        self._aborted = True
        self._session._reset_stream(self._stream_id, _error_code_of(reason))

    def stopped(self, code: int) -> None:
        if self._stopped is None:
            self._stopped = code

    def fail(self, error: BaseException) -> None:
        if self._error is None and not self._closed and not self._aborted:
            self._error = error


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

    def __init__(self, quic: QuicConnection, stream_handler: Any = None) -> None:
        super().__init__(quic, stream_handler)
        self.h3 = H3Connection(quic, enable_webtransport=True)
        self.sessions: dict[int, WebTransportSessionBase] = {}
        self.stream_owners: dict[int, WebTransportSessionBase] = {}
        self.on_request_headers: Callable[[HeadersReceived], None] | None = None
        self.terminated: quic_events.ConnectionTerminated | None = None
        self.terminated_event: asyncio.Event = asyncio.Event()
        self._orphans: dict[int, list[WebTransportStreamDataReceived]] = {}

    def register_session(self, session: WebTransportSessionBase) -> None:
        self.sessions[session.session_id] = session
        self.stream_owners[session.session_id] = session

        for event in self._orphans.pop(session.session_id, []):
            session._on_stream_data(event)

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

        if transport is not None:
            self._loop.call_soon(transport.close)

    def close_after_drain(self, error_code: int = ErrorCode.H3_NO_ERROR, reason_phrase: str = "") -> None:
        """Close the QUIC connection, and make sure the UDP socket is released even if the peer never answers."""
        self.close(error_code=error_code, reason_phrase=reason_phrase)

        def release() -> None:
            if self._transport is not None and not self.terminated_event.is_set():
                self._transport.close()

        self._loop.call_later(3.0, release)


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
        """Send CLOSE_WEBTRANSPORT_SESSION, give queued data a moment to drain, then close the connection."""
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
        self._finish(None)

    async def wait_closed(self) -> None:
        await self._closed

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
        """The peer ended the session cleanly: finish, then close the connection ourselves."""
        protocol = self._protocol
        self._finish(None)

        if protocol is not None and protocol.terminated is None and not self._closing:
            self._closing = True
            protocol.close_after_drain()

    async def _drain_then_close(self, protocol: WebTransportProtocol) -> None:
        # Closing a QUIC connection discards queued stream data, so give the capsule and any final
        # FINs a round trip to land: a PING acknowledgement means everything queued before it was
        # received.
        try:
            await asyncio.wait_for(protocol.ping(), _CLOSE_FLUSH_TIMEOUT)
        except Exception:
            pass

        if protocol.terminated is None:
            protocol.close_after_drain()

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

        receive.feed(event.data, event.stream_ended)

    def _on_stream_reset(self, stream_id: int, error_code: int) -> None:
        receive = self._receives.get(stream_id)

        if receive is not None:
            code = h3_to_webtransport_error_code(error_code)
            receive.fail(WebTransportError(f"peer reset the stream (code {code})", code=code))

    def _on_stop_sending(self, stream_id: int, error_code: int) -> None:
        send = self._sends.get(stream_id)

        if send is not None:
            send.stopped(h3_to_webtransport_error_code(error_code) or 0)

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

    def _send_stream_data(self, stream_id: int, data: bytes, end_stream: bool) -> None:
        protocol = self._require_live()
        protocol._quic.send_stream_data(stream_id, data, end_stream)
        protocol.transmit()

    def _reset_stream(self, stream_id: int, code: int) -> None:
        protocol = self._protocol

        if protocol is None or protocol.terminated is not None:
            return

        try:
            protocol._quic.reset_stream(stream_id, webtransport_to_h3_error_code(code))
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
            sock, addr = _open_socket(infos[0][4])
            udp_transport, protocol = await loop.create_datagram_endpoint(
                lambda: WebTransportProtocol(connection), sock=sock
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
        if self.ready.done():
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

            if not self.ready.done():
                self.ready.set_exception(WebTransportError("closed before the session was established"))

            self._finish(None)
            self._connect_task.cancel()
            self._teardown_connection()
            return

        super().close(close_code, reason)

    def _teardown_connection(self) -> None:
        protocol = self._protocol or self._pending_protocol

        if protocol is not None and protocol.terminated is None:
            protocol.close_after_drain(ErrorCode.H3_NO_ERROR)


def _open_socket(address: tuple[Any, ...]) -> tuple[socket.socket, tuple[Any, ...]]:
    """Bind a UDP socket the way aioquic's client does: dual-stack IPv6 when available, IPv4 otherwise."""
    try:
        sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
        try:
            sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
            sock.bind(("::", 0, 0, 0))
        except OSError:
            sock.close()
            raise
    except OSError:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("0.0.0.0", 0))
        return sock, address[:2]

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
