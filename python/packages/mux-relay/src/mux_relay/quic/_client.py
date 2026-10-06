"""The native QUIC dialing end: the connection protocol, the session, and :func:`connect_quic`."""

from __future__ import annotations

import asyncio
import socket
from collections.abc import Sequence
from typing import Any
from urllib.parse import urlsplit

from aioquic.quic import events as quic_events
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.connection import QuicConnection
from aioquic.quic.packet import QuicErrorCode, QuicFrameType

from moq_core._tasks import observe, spawn
from moq_core.transport import BidirectionalStream, SendStream

from ..connect_session import ConnectingTransport
from ._aioquic import finish_receiving_half, stream_credit_left
from ._errors import QuicTransportError, _from_native_wire_code, _session_error, _to_h3_error_code
from ._protocol import QuicProtocol
from ._session import _CLOSE_DRAIN_TIMEOUT, QuicStreamSession
from ._udp import open_socket

_DEFAULT_PORT = 443
_MAX_DATAGRAM_FRAME_SIZE = 65536
"""MOQT requires the QUIC DATAGRAM extension (draft-22 §6.2), so the dialer offers it; the core sends none."""

_NO_APPLICATION_PROTOCOL = QuicErrorCode.CRYPTO_ERROR + 120
"""The CONNECTION_CLOSE code for TLS's ``no_application_protocol`` alert (RFC 9001 §4.8, RFC 7301 §3.2)."""


def _dial_target(url: str) -> tuple[str, int]:
    """The host and port to dial, 443 by default (TA-3, TA-4). The path and the query are never read."""
    parts = urlsplit(url)

    if parts.scheme != "https":
        raise ValueError("native QUIC endpoints must use HTTPS")

    host = parts.hostname

    if not host:
        raise TypeError("native QUIC endpoints must name a host")

    return host, parts.port or _DEFAULT_PORT


class NativeQuicProtocol(QuicProtocol):
    """An aioquic connection protocol carrying MOQT directly on its streams: one session, no HTTP/3."""

    def __init__(self, quic: QuicConnection, session: QuicStreamSession[Any], **options: Any) -> None:
        super().__init__(quic, **options)
        self.session = session
        self.alpn_protocol: str | None = None
        """The protocol the handshake negotiated, ``None`` when the peer selected none."""

    def quic_event_received(self, event: quic_events.QuicEvent) -> None:
        # A peer can open a stream with RESET_STREAM or STOP_SENDING and no data at all; the stream
        # still arrives (TA-13), with that half already failed and the other half usable.
        if (
            isinstance(event, quic_events.StreamReset | quic_events.StopSendingReceived)
            and event.stream_id not in self.stream_owners
        ):
            self.session._on_stream_bytes(event.stream_id, b"", False)

        super().quic_event_received(event)

    def _on_quic_event(self, event: quic_events.QuicEvent) -> None:
        if isinstance(event, quic_events.StreamDataReceived):
            self.session._on_stream_bytes(event.stream_id, event.data, event.end_stream)
        elif isinstance(event, quic_events.HandshakeCompleted):
            self.alpn_protocol = event.alpn_protocol

    def _on_terminated(self, event: quic_events.ConnectionTerminated) -> None:
        self.session._on_terminated(event)


class QuicSession(QuicStreamSession[NativeQuicProtocol]):
    """A native QUIC session: MOQT straight on a QUIC connection. Create one with :func:`connect_quic`.

    ``ready`` resolves once the handshake negotiated one of ``protocols`` as its ALPN, and
    ``protocol_id`` is that protocol. Reset and stop codes go on the wire in the HTTP/3 encoding moq-relay reads
    them in, and a peer's code in that encoding is decoded (TA-27); close codes go as they are.
    """

    _CLOSED_MESSAGE = "QUIC session closed"
    _NOT_ESTABLISHED_MESSAGE = "QUIC session is not established"
    _CLEAN_CLOSE_CODES = frozenset()
    """None: only an application close is a clean end, never a QUIC-layer close, whatever its code (TA-9)."""

    def __init__(self, url: str, protocols: Sequence[str], configuration: QuicConfiguration | None = None) -> None:
        super().__init__()
        loop = asyncio.get_running_loop()
        self.protocols = list(protocols)
        self.protocol_id: str | None = None
        self.ready: asyncio.Future[None] = loop.create_future()
        observe(self.ready)
        self.close_info: tuple[int, str] | None = None
        """``(code, reason)`` of the peer's application CONNECTION_CLOSE (frame type 0x1d), once received."""
        self._peer_close_error: QuicTransportError | None = None
        """What every operation raises once the peer's application close ended the session (TA-24)."""
        self._configuration = configuration
        # Only the host and port leave this call: the path and the token travel in the core's SETUP (TA-3, TA-6).
        self._connect_task = spawn(self._connect(url))

    # ------------------------------------------------------------------ connecting

    async def _connect(self, url: str) -> None:
        loop = asyncio.get_running_loop()

        try:
            host, port = _dial_target(url)
            infos = await loop.getaddrinfo(host, port, type=socket.SOCK_DGRAM)

            if not infos:
                raise _session_error(f"could not resolve {host}")

            connection = QuicConnection(configuration=self._build_configuration(host))
            sock, addr = open_socket([info[4] for info in infos])
            udp_transport, protocol = await loop.create_datagram_endpoint(
                lambda: NativeQuicProtocol(connection, self, owns_transport=True, receive_socket=sock, keepalive=True),
                sock=sock,
            )
            assert isinstance(protocol, NativeQuicProtocol)

            if self._closing:
                udp_transport.close()
                raise _session_error("closed before the session was established")

            # Attached before the handshake, so a stream the peer opens in the same flight finds this session.
            self._protocol = protocol
            protocol.connect(addr)
            await protocol.wait_connected()

            if protocol.terminated is not None:
                raise self._ended_error() or _session_error("QUIC handshake failed")

            self._select_protocol(protocol)
            self.ready.set_result(None)
        except BaseException as error:
            if not self.ready.done():
                ended = self._ended_error()

                if ended is not None:
                    # The connection ended first, and how it ended says more (a peer's close carries its code).
                    self.ready.set_exception(ended)
                elif isinstance(error, QuicTransportError) and error.source == "session":
                    self.ready.set_exception(error)
                else:
                    # A failed dial is a session error (TA-22). The cause stays reachable: a refused port
                    # surfaces as the OS error behind TRANSPORT_ERROR.
                    failure = _session_error(f"QUIC connect failed: {error}")
                    failure.__cause__ = error
                    self.ready.set_exception(failure)

            self._finish(self.ready.exception() if not self.ready.cancelled() else None)
            self._teardown_connection()

            if isinstance(error, asyncio.CancelledError):
                raise

    def _build_configuration(self, host: str) -> QuicConfiguration:
        configuration = self._configuration if self._configuration is not None else QuicConfiguration()
        configuration.is_client = True
        # Exactly what the core offers, in its order, and nothing else: no `h3` beside it (TA-5).
        configuration.alpn_protocols = list(self.protocols)

        if configuration.server_name is None:
            configuration.server_name = host

        if configuration.max_datagram_frame_size is None:
            configuration.max_datagram_frame_size = _MAX_DATAGRAM_FRAME_SIZE

        return configuration

    def _select_protocol(self, protocol: NativeQuicProtocol) -> None:
        """Accept the handshake only with an offered ALPN: none, or another, fails the dial (TA-7, TA-8)."""
        selected = protocol.alpn_protocol

        if selected is not None and selected in self.protocols:
            self.protocol_id = selected
            return

        message = (
            "the peer selected no application protocol"
            if selected is None
            else f"the peer selected an unoffered application protocol {selected!r}"
        )
        self._closing = True
        protocol._quic.close(
            error_code=_NO_APPLICATION_PROTOCOL, frame_type=QuicFrameType.PADDING, reason_phrase=message
        )
        protocol.transmit()
        raise _session_error(message)

    def _ended_error(self) -> BaseException | None:
        """The error the session already ended with, if it ended with one."""
        if not self._closed.done() or self._closed.cancelled():
            return None

        return self._closed.exception()

    # ------------------------------------------------------------------ MoqtTransport

    async def create_unidirectional_stream(self) -> SendStream:
        protocol = self._require_open(unidirectional=True)
        stream_id = protocol._quic.get_next_available_stream_id(is_unidirectional=True)
        # Sending nothing creates the stream, so the next create gets the next id.
        protocol._quic.send_stream_data(stream_id, b"", False)
        finish_receiving_half(protocol._quic, stream_id)
        send = self._add_send(protocol, stream_id)
        protocol.transmit()
        return send

    async def create_bidirectional_stream(self) -> BidirectionalStream:
        protocol = self._require_open(unidirectional=False)
        stream_id = protocol._quic.get_next_available_stream_id()
        protocol._quic.send_stream_data(stream_id, b"", False)
        stream = self._add_bidirectional(protocol, stream_id)
        protocol.transmit()
        return stream

    def _require_open(self, *, unidirectional: bool) -> NativeQuicProtocol:
        """The live connection, once it has credit for one more stream of the kind; reject at once otherwise (TA-21)."""
        protocol = self._require_live()

        if not stream_credit_left(protocol._quic, unidirectional):
            kind = "unidirectional" if unidirectional else "bidirectional"
            # Neither a stream nor a session error: the session stays usable (TA-21, TA-23).
            raise QuicTransportError(f"the peer's {kind} stream limit is exhausted")

        return protocol

    def close(self, close_code: int = 0, reason: str = "") -> None:
        """Wait for in-flight data to be acknowledged, then end the connection with an application close (TA-10).

        The CONNECTION_CLOSE (frame type 0x1d) carries ``close_code`` and ``reason``. Before ``ready``
        this cancels the dial, and ``wait_closed`` raises (TA-11). Idempotent, and never raises.
        """
        if self._closing:
            return

        self._closing = True
        protocol = self._protocol

        if not self.ready.done():
            error = _session_error("closed before the session was established")
            self.ready.set_exception(error)
            self._finish(error)
            self._connect_task.cancel()
            self._teardown_connection()
            return

        if protocol is None or protocol.terminated is not None:
            self._finish(None)
            return

        spawn(self._drain_then_close(protocol, close_code, reason))

    async def _drain_then_close(self, protocol: NativeQuicProtocol, close_code: int, reason: str) -> None:
        # A CONNECTION_CLOSE discards queued stream data, and whatever the peer has not acknowledged is
        # lost with it (#209), so the close waits until every send stream has been acknowledged, within
        # a bound so a peer that went silent cannot hold it.
        try:
            await asyncio.wait({self._wait_drained(protocol)}, timeout=_CLOSE_DRAIN_TIMEOUT)
        finally:
            if protocol.terminated is None:
                protocol.close_after_drain(close_code, reason)

    def check_live(self) -> None:
        if self._peer_close_error is not None:
            raise self._peer_close_error

        if not self.ready.done() and not self._closed.done():
            raise QuicTransportError(self._NOT_ESTABLISHED_MESSAGE)

        super().check_live()

    # ------------------------------------------------------------------ framing hooks

    def _to_wire_code(self, code: int) -> int:
        return _to_h3_error_code(code)

    def _from_wire_code(self, code: int) -> int | None:
        return _from_native_wire_code(code)

    # ------------------------------------------------------------------ ending

    def _on_terminated(self, event: quic_events.ConnectionTerminated) -> None:
        """The connection ended. A peer's application close is a clean end, whatever its code (TA-9).

        aioquic reports no frame type for an application close (0x1d), from either end; one this end
        did not send is the peer's. Its code and reason stay in ``close_info``, and an operation that
        fails because of it raises with them (TA-24). Any other end (a QUIC-layer close, an idle
        timeout, a failed handshake) fails the session.
        """
        if self._closing or event.frame_type is not None:
            super()._on_terminated(event)
            return

        code, reason = event.error_code, event.reason_phrase
        self.close_info = (code, reason)
        error = QuicTransportError(
            f"peer closed the QUIC session: {reason or 'no reason given'} (code 0x{code:x})",
            source="session",
            close_code=code,
            reason=reason,
        )
        self._peer_close_error = error
        # Before `ready` the dial fails with the peer's close; after it the session ended cleanly.
        self._finish(None if self.ready.done() else error, stream_error=error)

    def _teardown_connection(self) -> None:
        protocol = self._protocol

        if protocol is not None and protocol.terminated is None:
            protocol.close_after_drain()


def connect_quic(
    url: str, protocols: Sequence[str] = (), *, configuration: QuicConfiguration | None = None
) -> ConnectingTransport:
    """Start a native QUIC session to ``url``'s host and port, offering ``protocols`` as the ALPN list.

    Returns immediately; ``ready`` completes once the handshake negotiated one of ``protocols``.
    Only the host and port are read from ``url``: the connection sends the path and the token in
    SETUP. Pass a ``QuicConfiguration`` to control TLS verification (``cafile``/``cadata``/
    ``verify_mode``) or QUIC parameters; ALPN and the client role are set regardless.
    """
    session = QuicSession(url, protocols, configuration)
    return ConnectingTransport(transport=session, ready=session.ready, framing="native-quic")
