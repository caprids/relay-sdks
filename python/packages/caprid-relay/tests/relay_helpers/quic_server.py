"""A loopback native QUIC server on the SDK's own QUIC plumbing, so the dialing end can be tested without a relay.

:class:`QuicServerSession` is the accepting role of :class:`caprid_relay.quic._session.QuicStreamSession`, carried by
the dialing end's own :class:`~caprid_relay.quic._client.NativeQuicProtocol`: the loopback tests therefore exercise the
very stream code the SDK ships, on both ends of a real QUIC connection. :class:`Impairment` shapes what a connection
receives, here and on the WebTransport accepting end (``wt_server.py``).
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
from aioquic.quic import events as quic_events
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.connection import QuicConnection
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from caprid_relay.quic._aioquic import finish_receiving_half
from caprid_relay.quic._client import NativeQuicProtocol
from caprid_relay.quic._errors import _from_native_wire_code, _to_h3_error_code
from caprid_relay.quic._protocol import QuicProtocol
from caprid_relay.quic._session import _CLOSE_DRAIN_TIMEOUT, QuicStreamSession
from moq_core import MOQT_PROTOCOL_IDS
from moq_core._tasks import spawn
from moq_core.transport import BidirectionalStream, SendStream

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


def receive_impaired(
    protocol: QuicProtocol, impairment: Impairment | None, deliver: Callable[[Any, Any], None], data: Any, addr: Any
) -> None:
    """Hand ``deliver`` one inbound datagram as ``impairment`` shapes it: at once, later, or not at all."""
    if impairment is None:
        deliver(data, addr)
        return

    if impairment.paused:
        return

    loop = protocol._loop
    delay = impairment.delay + random.uniform(-impairment.jitter, impairment.jitter)
    now = loop.time()

    if impairment.rate:
        free_at = max(impairment._next_free_at, now)
        queued = free_at - now

        if queued > impairment.queue_seconds:
            return  # the bottleneck queue is full: tail drop

        impairment._next_free_at = free_at + len(data) / impairment.rate
        delay += queued

    if delay <= 0:
        deliver(data, addr)
    else:
        loop.call_later(delay, deliver, data, addr)


class QuicServerSession(QuicStreamSession["QuicServerProtocol"]):
    """The accepting end of one native QUIC connection: a ``MoqtTransport`` for a test or the relay hub to serve.

    Opens streams the way the dialing end does, without its credit check, and closes with an application close
    after the same drain. The dialer's application close ends it cleanly, with ``close_info`` set. Stream codes cross
    in the HTTP/3 encoding, as with moq-relay (TA-27).
    """

    _CLOSED_MESSAGE = "QUIC session closed"
    _NOT_ESTABLISHED_MESSAGE = "QUIC session is not established"

    def __init__(self) -> None:
        super().__init__()
        self.close_info: tuple[int, str] | None = None
        """``(code, reason)`` of the dialer's application CONNECTION_CLOSE (frame type 0x1d), once received."""

    def impair(self, **settings: Any) -> Impairment:
        """Replace the inbound impairment of this session's connection; ``impair()`` clears it."""
        protocol = self._protocol
        assert protocol is not None
        impairment = Impairment(**settings) if settings else None
        protocol.impairment = impairment
        return impairment or Impairment()

    async def create_unidirectional_stream(self) -> SendStream:
        protocol = self._require_live()
        stream_id = protocol._quic.get_next_available_stream_id(is_unidirectional=True)
        protocol._quic.send_stream_data(stream_id, b"", False)
        finish_receiving_half(protocol._quic, stream_id)
        send = self._add_send(protocol, stream_id)
        protocol.transmit()
        return send

    async def create_bidirectional_stream(self) -> BidirectionalStream:
        protocol = self._require_live()
        stream_id = protocol._quic.get_next_available_stream_id()
        protocol._quic.send_stream_data(stream_id, b"", False)
        stream = self._add_bidirectional(protocol, stream_id)
        protocol.transmit()
        return stream

    def close(self, close_code: int = 0, reason: str = "") -> None:
        """Wait for in-flight data to be acknowledged, then end the connection with an application close."""
        if self._closing:
            return

        self._closing = True
        protocol = self._protocol

        if protocol is None or protocol.terminated is not None:
            self._finish(None)
            return

        spawn(self._drain_then_close(protocol, close_code, reason))

    async def _drain_then_close(self, protocol: QuicServerProtocol, close_code: int, reason: str) -> None:
        try:
            await asyncio.wait({self._wait_drained(protocol)}, timeout=_CLOSE_DRAIN_TIMEOUT)
        finally:
            if protocol.terminated is None:
                protocol.close_after_drain(close_code, reason)

    def _to_wire_code(self, code: int) -> int:
        return _to_h3_error_code(code)

    def _from_wire_code(self, code: int) -> int | None:
        return _from_native_wire_code(code)

    def _on_terminated(self, event: quic_events.ConnectionTerminated) -> None:
        # aioquic reports no frame type for an application close; one this end did not send is the dialer's.
        if not self._closing and event.frame_type is None:
            self.close_info = (event.error_code, event.reason_phrase)
            self._finish(None)
            return

        super()._on_terminated(event)


SessionHandler = Callable[[QuicServerSession], None]


class QuicServerProtocol(NativeQuicProtocol):
    """One accepted native QUIC connection: its session goes to ``on_session`` once the handshake completes."""

    def __init__(self, quic: QuicConnection, stream_handler: Any = None, *, on_session: SessionHandler) -> None:
        session = QuicServerSession()
        super().__init__(quic, session, stream_handler=stream_handler)
        session._protocol = self
        self.session: QuicServerSession = session
        self._on_session = on_session
        self.impairment: Impairment | None = None

    def datagram_received(self, data: bytes | str, addr: Any) -> None:
        receive_impaired(self, self.impairment, super().datagram_received, data, addr)

    def _on_quic_event(self, event: quic_events.QuicEvent) -> None:
        super()._on_quic_event(event)

        if isinstance(event, quic_events.HandshakeCompleted):
            self._on_session(self.session)


class LoopbackQuicServer:
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


async def start_quic_server(on_session: SessionHandler) -> LoopbackQuicServer:
    """Listen on a loopback port, selecting the MOQT protocol, and hand every accepted session to ``on_session``."""
    certificate, key, pem = generate_self_signed_certificate()
    configuration = QuicConfiguration(
        is_client=False, alpn_protocols=list(MOQT_PROTOCOL_IDS), max_datagram_frame_size=65536, idle_timeout=20.0
    )
    configuration.certificate = certificate
    configuration.private_key = key
    server = await serve(
        "127.0.0.1", 0, configuration=configuration, create_protocol=partial(QuicServerProtocol, on_session=on_session)
    )
    transport = server._transport
    assert transport is not None
    port = transport.get_extra_info("sockname")[1]
    await asyncio.sleep(0)
    return LoopbackQuicServer(server, port, pem)
