"""A loopback WebTransport server on aioquic, so the real transport adapter can be tested without a relay.

Reuses :class:`mux_relay.webtransport._session.WebTransportSessionBase` for the server role: the loopback test
therefore exercises the very stream code the client ships, on both ends of a real QUIC connection.
"""

from __future__ import annotations

import asyncio
import datetime
import ipaddress
import random
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from typing import Any

from aioquic.asyncio import serve
from aioquic.h3.events import HeadersReceived
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.connection import QuicConnection
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from mux_relay.webtransport._protocol import WebTransportProtocol
from mux_relay.webtransport._session import WebTransportSessionBase
from mux_relay.webtransport._wire import parse_sf_list_of_strings, sf_list_of_strings

SERVER_NAME = "localhost"


def generate_self_signed_certificate() -> tuple[x509.Certificate, ec.EllipticCurvePrivateKey, bytes]:
    """An EC certificate for ``localhost`` / ``127.0.0.1``, valid for a day, plus its PEM."""
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, SERVER_NAME)])
    now = datetime.datetime.now(datetime.UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.DNSName(SERVER_NAME), x509.IPAddress(ipaddress.IPv4Address("127.0.0.1"))]
            ),
            critical=False,
        )
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    return certificate, key, certificate.public_bytes(serialization.Encoding.PEM)


@dataclass
class Impairment:
    """Shapes the client-to-server direction of one connection, the publisher's uplink in a test.

    aioquic's receiver grants flow-control credit as data arrives whether or not the application
    reads it, so "a peer that reads slowly" has to be modelled at the packet level: ``paused`` drops
    every inbound datagram (a blackout), ``delay``/``jitter`` add latency, and ``rate`` caps
    throughput with a bounded queue that tail-drops, the way a saturated bottleneck link does.
    """

    paused: bool = False
    delay: float = 0.0
    """Seconds added to every inbound datagram."""
    jitter: float = 0.0
    """Uniform +/- seconds around ``delay``; datagrams may reorder."""
    rate: int | None = None
    """Bytes per second of inbound capacity, or ``None`` for unlimited."""
    queue_seconds: float = 0.2
    """How far behind the rate cap's queue may fall before it drops the datagram."""
    _next_free_at: float = 0.0


class WebTransportServerSession(WebTransportSessionBase):
    """The server end of one accepted session: a ``MoqtTransport`` for the relay hub to serve."""

    def __init__(self, protocol: WebTransportProtocol, session_id: int, path: str, protocol_id: str | None) -> None:
        super().__init__()
        self.path = path
        self.protocol_id = protocol_id
        self._attach(protocol, session_id)

    def impair(self, **settings: Any) -> Impairment:
        """Replace the inbound impairment of this session's connection; ``impair()`` clears it."""
        protocol = self._protocol
        assert isinstance(protocol, WebTransportServerProtocol)
        impairment = Impairment(**settings) if settings else None
        protocol.impairment = impairment
        return impairment or Impairment()


SessionHandler = Callable[[WebTransportServerSession], None]


class WebTransportServerProtocol(WebTransportProtocol):
    """Accepts extended CONNECT requests and hands each session to ``on_session``."""

    def __init__(
        self,
        quic: QuicConnection,
        on_session: SessionHandler,
        stream_handler: Any = None,
        respond_to_connect: bool = True,
        on_request: Callable[[dict[bytes, bytes]], None] | None = None,
    ) -> None:
        super().__init__(quic, stream_handler)
        self._on_session = on_session
        self._respond_to_connect = respond_to_connect
        self._on_request = on_request
        self.on_request_headers = self._handle_request
        self.requests: list[dict[bytes, bytes]] = []
        self.impairment: Impairment | None = None

    def datagram_received(self, data: bytes | str, addr: Any) -> None:
        impairment = self.impairment

        if impairment is None:
            super().datagram_received(data, addr)
            return

        if impairment.paused:
            return

        delay = impairment.delay + random.uniform(-impairment.jitter, impairment.jitter)
        now = self._loop.time()

        if impairment.rate:
            free_at = max(impairment._next_free_at, now)
            queued = free_at - now

            if queued > impairment.queue_seconds:
                return  # the bottleneck queue is full: tail drop

            impairment._next_free_at = free_at + len(data) / impairment.rate
            delay += queued

        if delay <= 0:
            super().datagram_received(data, addr)
        else:
            self._loop.call_later(delay, super().datagram_received, data, addr)

    def _handle_request(self, event: HeadersReceived) -> None:
        headers = {key.lower(): value for key, value in event.headers}
        self.requests.append(headers)

        if self._on_request is not None:
            self._on_request(headers)

        if not self._respond_to_connect:
            return  # A server that accepts QUIC but never answers the CONNECT: the hung-handshake case.

        if headers.get(b":method") != b"CONNECT" or headers.get(b":protocol") != b"webtransport":
            self.h3.send_headers(event.stream_id, [(b":status", b"404")], end_stream=True)
            self.transmit()
            return

        offered = parse_sf_list_of_strings(headers.get(b"wt-available-protocols", b""))
        selected = offered[0] if offered else None
        response = [(b":status", b"200"), (b"sec-webtransport-http3-draft", b"draft02")]

        if selected is not None:
            response.append((b"wt-protocol", sf_list_of_strings([selected])))

        self.h3.send_headers(event.stream_id, response)
        self.transmit()
        session = WebTransportServerSession(
            self, event.stream_id, headers.get(b":path", b"/").decode("utf-8", errors="replace"), selected
        )
        self._on_session(session)


class LoopbackWebTransportServer:
    """A running server: ``url_for(path)`` addresses it, ``client_configuration()`` trusts its certificate."""

    def __init__(self, server: Any, port: int, certificate_pem: bytes) -> None:
        self._server = server
        self.port = port
        self.certificate_pem = certificate_pem

    def url_for(self, path: str = "/") -> str:
        return f"https://127.0.0.1:{self.port}{path}"

    def client_configuration(self) -> QuicConfiguration:
        return QuicConfiguration(is_client=True, cadata=self.certificate_pem, server_name=SERVER_NAME)

    def close(self) -> None:
        self._server.close()


async def start_webtransport_server(
    on_session: SessionHandler,
    *,
    respond_to_connect: bool = True,
    on_request: Callable[[dict[bytes, bytes]], None] | None = None,
) -> LoopbackWebTransportServer:
    certificate, key, pem = generate_self_signed_certificate()
    configuration = QuicConfiguration(
        is_client=False, alpn_protocols=["h3"], max_datagram_frame_size=65536, idle_timeout=20.0
    )
    configuration.certificate = certificate
    configuration.private_key = key
    server = await serve(
        "127.0.0.1",
        0,
        configuration=configuration,
        create_protocol=partial(
            WebTransportServerProtocol,
            on_session=on_session,
            respond_to_connect=respond_to_connect,
            on_request=on_request,
        ),
    )
    transport = server._transport
    assert transport is not None
    port = transport.get_extra_info("sockname")[1]
    await asyncio.sleep(0)
    return LoopbackWebTransportServer(server, port, pem)
