"""The client role: URL parsing, the UDP socket, the extended CONNECT, and :func:`connect_webtransport`."""

from __future__ import annotations

import asyncio
import socket
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

from aioquic.h3.connection import ErrorCode
from aioquic.h3.events import HeadersReceived
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.connection import QuicConnection

from moq_core._tasks import observe, spawn

from ..connect_session import ConnectingTransport
from ._errors import WebTransportError
from ._protocol import WebTransportProtocol
from ._session import WebTransportSessionBase
from ._wire import parse_sf_string, sf_list_of_strings

H3_ALPN = "h3"
_DEFAULT_PORT = 443
_MAX_DATAGRAM_FRAME_SIZE = 65536


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
        raise TypeError("WebTransport endpoints must name a host")

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
                lambda: WebTransportProtocol(connection, owns_transport=True, receive_socket=sock, keepalive=True),
                sock=sock,
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
