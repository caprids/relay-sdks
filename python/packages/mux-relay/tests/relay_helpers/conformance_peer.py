"""A scriptable accepting end for the TypeScript transport adapter conformance suite.

The suite (``ts/conformance/``) drives the accepting end through the peer interface in its README. This module is
that peer, out of process, for a WebTransport session or, with ``--framing native-quic``, for MOQT straight on a QUIC
connection with ALPN carrying the protocol. A launcher runs it with ``tests`` on ``PYTHONPATH``::

    python -m relay_helpers.conformance_peer [--framing native-quic]

and it prints one JSON line, ``{"event": "listening", "port": <udp port>, "certificateHash": "<sha-256 hex>"}``. It
then answers one JSON request per stdin line, ``{"id": n, "method": "...", "params": {...}}``, with
``{"id": n, "result": ...}`` or ``{"id": n, "error": "..."}`` on stdout, and exits when stdin closes. Requests may
overlap: each runs as its own task.

``prepare`` sets up the next connection (handshake, protocol selection, stream credit) and forgets the previous
session; every other method acts on the session that connection carries. Stream ids are the QUIC stream ids.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime
import hashlib
import ipaddress
import json
import sys
from dataclasses import dataclass
from functools import partial
from typing import Any, cast

from aioquic import tls
from aioquic.asyncio import QuicConnectionProtocol, serve
from aioquic.h3.events import HeadersReceived
from aioquic.quic import events
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.connection import QuicConnection
from aioquic.quic.packet import QuicErrorCode, QuicFrameType
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from mux_relay.quic._errors import QuicTransportError
from mux_relay.quic._streams import _ReceiveHalf, _SendHalf
from relay_helpers.native_quic import NativeQuicSession
from relay_helpers.quic_server import SERVER_NAME, Impairment
from relay_helpers.webtransport.wire import encode_close_session_capsule, parse_sf_list_of_strings, sf_list_of_strings
from relay_helpers.wt_server import WebTransportServerProtocol, WebTransportServerSession

UNOFFERED_PROTOCOL = "moqt-unoffered"
"""What ``select: "unoffered"`` answers with."""

H3_UNI_STREAMS = 3
"""The unidirectional streams HTTP/3 itself takes from the dialer's credit: control, QPACK encoder and decoder."""

IDLE_TIMEOUT = 10.0
"""Seconds of silence before the connection times out: what ``loseConnection`` waits on at the dialer."""


def generate_leaf_certificate() -> tuple[x509.Certificate, ec.EllipticCurvePrivateKey, str]:
    """A P-256 leaf for ``localhost`` / ``127.0.0.1`` that a browser accepts through ``serverCertificateHashes``.

    W3C WebTransport takes a hash only for a certificate valid for 14 days or less, so this one is valid for 10.
    Returns the certificate, its key, and the SHA-256 of its DER encoding in hex.
    """
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
        .not_valid_after(now + datetime.timedelta(days=10))
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.DNSName(SERVER_NAME), x509.IPAddress(ipaddress.IPv4Address("127.0.0.1"))]
            ),
            critical=False,
        )
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
        .sign(key, hashes.SHA256())
    )
    digest = hashlib.sha256(certificate.public_bytes(serialization.Encoding.DER)).hexdigest()
    return certificate, key, digest


@dataclass
class DialOptions:
    """How the next connection is answered (the suite's ``session`` options)."""

    handshake: str = "accept"
    """``accept``, ``hold`` until ``releaseHandshake``, or ``refuse`` with a 404."""
    select: str = "first"
    """``first`` offered protocol, ``none``, or ``unoffered``."""
    max_streams: int | None = None
    """Streams of each direction the dialer may open; credit then returns one stream at a time, as the peer reads one
    to its FIN, instead of aioquic's doubling."""


def _hold_stream_credit(quic: QuicConnection, streams: int) -> None:
    """Grant the dialer ``streams`` of each kind beyond what HTTP/3 uses, and stop aioquic raising the limit itself."""
    uni, bidi = quic._local_max_streams_uni, quic._local_max_streams_bidi
    uni.value = uni.sent = H3_UNI_STREAMS + streams
    bidi.value = bidi.sent = 1 + streams  # the CONNECT request stream is a bidirectional stream too
    _hide_stream_use(quic)


def _hide_stream_use(quic: QuicConnection) -> None:
    """Credit returns only through ``grant_stream``: aioquic would otherwise double a limit once half of it is used."""
    uni, bidi = quic._local_max_streams_uni, quic._local_max_streams_bidi
    original = quic._write_connection_limits

    def write_connection_limits(builder: Any, space: Any) -> None:
        used = (uni.used, bidi.used)
        uni.used = bidi.used = 0
        try:
            original(builder=builder, space=space)
        finally:
            uni.used, bidi.used = used

    setattr(quic, "_write_connection_limits", write_connection_limits)  # noqa: B010 - replaces one connection's method


class _Discard:
    """A datagram transport that sends nothing: a connection that vanished."""

    def sendto(self, data: bytes, addr: Any = None) -> None:
        pass

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        return default


class ConformanceProtocol(WebTransportServerProtocol):
    """One accepted connection, answered as the peer's ``prepare`` said."""

    def __init__(self, quic: QuicConnection, peer: ConformancePeer, stream_handler: Any = None) -> None:
        super().__init__(quic, on_session=lambda session: None, stream_handler=stream_handler)
        self._peer = peer
        self._options = peer.next_options
        self._held: HeadersReceived | None = None
        self.credit_held = self._options.max_streams is not None
        if self._options.max_streams is not None:
            _hold_stream_credit(quic, self._options.max_streams)
        peer.attach(self)

    def _on_request_headers(self, event: HeadersReceived) -> None:
        headers = {key.lower(): value for key, value in event.headers}
        self.requests.append(headers)

        if headers.get(b":method") != b"CONNECT" or headers.get(b":protocol") != b"webtransport":
            self.h3.send_headers(event.stream_id, [(b":status", b"404")], end_stream=True)
            self.transmit()
            return

        self._peer.observed = {
            "path": headers.get(b":path", b"/").decode("utf-8", errors="replace"),
            "protocols": parse_sf_list_of_strings(headers.get(b"wt-available-protocols", b"")),
        }

        if self._options.handshake == "refuse":
            self.h3.send_headers(event.stream_id, [(b":status", b"404")], end_stream=True)
            self.transmit()
        elif self._options.handshake == "hold" and not self._peer.handshake_released:
            self._held = event
        else:
            self._answer(event, headers)

    def release_handshake(self) -> None:
        event, self._held = self._held, None
        if event is not None and self.terminated is None:
            self._answer(event, {key.lower(): value for key, value in event.headers})

    def _answer(self, event: HeadersReceived, headers: dict[bytes, bytes]) -> None:
        offered = parse_sf_list_of_strings(headers.get(b"wt-available-protocols", b""))
        select = self._options.select
        if select == "first":
            selected = offered[0] if offered else None
        else:
            selected = UNOFFERED_PROTOCOL if select == "unoffered" else None
        response = [(b":status", b"200"), (b"sec-webtransport-http3-draft", b"draft02")]

        if selected is not None:
            response.append((b"wt-protocol", sf_list_of_strings([selected])))

        self.h3.send_headers(event.stream_id, response)
        self.transmit()
        path = headers.get(b":path", b"/").decode("utf-8", errors="replace")
        self._peer.accepted(WebTransportServerSession(self, event.stream_id, path, selected))

    def grant_stream(self, bidirectional: bool) -> None:
        """Return one stream of credit to the dialer (a MAX_STREAMS one higher)."""
        if not self.credit_held:
            return
        limit = self._quic._local_max_streams_bidi if bidirectional else self._quic._local_max_streams_uni
        limit.value += 1
        self.transmit()

    def lose(self) -> None:
        """Vanish: drop everything in both directions, so the dialer sees only silence until its idle timeout."""
        self.impairment = Impairment(paused=True)
        setattr(self, "_transport", _Discard())  # noqa: B010 - stands in for the shared socket

    def dispose(self) -> None:
        """Close the connection, if it is still open."""
        if self.terminated is None:
            self._quic.close(error_code=QuicErrorCode.NO_ERROR)
            self.transmit()


def _hold_native_stream_credit(quic: QuicConnection, streams: int) -> None:
    """Grant the dialer exactly ``streams`` of each kind, and stop aioquic raising the limit itself."""
    uni, bidi = quic._local_max_streams_uni, quic._local_max_streams_bidi
    uni.value = uni.sent = streams
    bidi.value = bidi.sent = streams
    _hide_stream_use(quic)


def _select_protocol(quic: QuicConnection, peer: ConformancePeer, options: DialOptions) -> None:
    """Answer the dialer's ClientHello as ``options`` say: record its SNI and ALPN list, then select, or refuse.

    aioquic negotiates ALPN from its own list, by its own preference, and fails a handshake that has no common
    protocol. The connection's TLS context is created with the first datagram, so this wraps that; the context then
    skips its own negotiation, and the ALPN callback, which runs before the EncryptedExtensions are built, decides:
    the first offered protocol, none, or one that was not offered.
    """
    initialize = quic._initialize

    def initialize_then_hook(peer_cid: bytes) -> None:
        initialize(peer_cid)
        context = quic.tls
        context._alpn_protocols = None
        handle_hello = context._server_handle_hello
        on_alpn = context.alpn_cb
        offered: list[str] = []

        def server_handle_hello(input_buf: Any, *buffers: Any) -> None:
            start = input_buf.tell()
            hello = tls.pull_client_hello(input_buf)
            input_buf.seek(start)
            offered[:] = hello.alpn_protocols or []
            peer.observed = {"serverName": hello.server_name, "protocols": list(offered)}
            if options.handshake == "refuse":
                raise tls.AlertHandshakeFailure("the peer refuses the session")
            handle_hello(input_buf, *buffers)

        def select(_negotiated: str | None) -> None:
            if options.select == "first":
                selected = offered[0] if offered else None
            else:
                selected = UNOFFERED_PROTOCOL if options.select == "unoffered" else None
            context.alpn_negotiated = selected
            if on_alpn is not None:
                on_alpn(selected)  # ty: ignore[invalid-argument-type] - aioquic passes None for no protocol

        setattr(context, "_server_handle_hello", server_handle_hello)  # noqa: B010 - wraps one context's method
        context.alpn_cb = select

    setattr(quic, "_initialize", initialize_then_hook)  # noqa: B010 - wraps one connection's method


class NativeQuicConformanceProtocol(QuicConnectionProtocol):
    """One accepted native QUIC connection, answered as the peer's ``prepare`` said."""

    def __init__(self, quic: QuicConnection, peer: ConformancePeer, stream_handler: Any = None) -> None:
        super().__init__(quic, stream_handler)
        self._peer = peer
        self._options = peer.next_options
        self.impairment: Impairment | None = None
        self.terminated: events.ConnectionTerminated | None = None
        self._held: list[tuple[bytes | str, Any]] = []
        self.session = NativeQuicSession(self)
        self.credit_held = self._options.max_streams is not None
        if self._options.max_streams is not None:
            _hold_native_stream_credit(quic, self._options.max_streams)
        _select_protocol(quic, peer, self._options)
        peer.attach(self)

    def datagram_received(self, data: bytes | str, addr: Any) -> None:
        if self._options.handshake == "hold" and not self._peer.handshake_released:
            # Holding every datagram holds the handshake: the dialer only retransmits its Initial.
            self._held.append((data, addr))
            return
        if self.impairment is not None and self.impairment.paused:
            return
        super().datagram_received(data, addr)

    def release_handshake(self) -> None:
        held, self._held = self._held, []
        for data, addr in held:
            self.datagram_received(data, addr)

    def quic_event_received(self, event: events.QuicEvent) -> None:
        if isinstance(event, events.HandshakeCompleted):
            self._peer.accepted(self.session)
        elif isinstance(event, events.ConnectionTerminated):
            self.terminated = event
        self.session.on_event(event)

    def grant_stream(self, bidirectional: bool) -> None:
        """Return one stream of credit to the dialer (a MAX_STREAMS one higher)."""
        if not self.credit_held:
            return
        limit = self._quic._local_max_streams_bidi if bidirectional else self._quic._local_max_streams_uni
        limit.value += 1
        self.transmit()

    def lose(self) -> None:
        """Vanish: drop everything in both directions, so the dialer sees only silence until its idle timeout."""
        self.impairment = Impairment(paused=True)
        setattr(self, "_transport", _Discard())  # noqa: B010 - stands in for the shared socket

    def dispose(self) -> None:
        """Close the connection, if it is still open."""
        self._held = []
        if self.terminated is None:
            self.close()

    def close(self, error_code: int = QuicErrorCode.NO_ERROR, reason_phrase: str = "") -> None:
        # A connection whose every datagram was held never started, and aioquic cannot close it: just drop it.
        if self._quic._network_paths:
            super().close(error_code=error_code, reason_phrase=reason_phrase)


def _close_at_once(session: WebTransportServerSession, code: int, reason: str) -> None:
    """Send CLOSE_WEBTRANSPORT_SESSION now, and leave the connection for the dialer to close.

    A session's own ``close()`` first waits for the dialer to acknowledge everything in flight (#209). A paused peer
    drops those acknowledgements, so that wait would only run out its bound before the capsule went out.
    """
    protocol = session._protocol
    if session._closing or protocol is None or protocol.terminated is not None:
        return
    session._closing = True
    protocol.h3.send_data(session.session_id, encode_close_session_capsule(code, reason), end_stream=True)
    protocol.transmit()


@dataclass
class _Stream:
    send: Any = None
    receive: Any = None
    from_dialer: bool = False
    bidirectional: bool = False


class ConformancePeer:
    """The state behind the JSON-lines methods: the next connection's options and the current session."""

    def __init__(self, framing: str = "webtransport") -> None:
        self.framing = framing
        self.next_options = DialOptions()
        self.observed: dict[str, Any] | None = None
        self.handshake_released = False
        """``releaseHandshake`` came: a held CONNECT is answered, and so is one that has not arrived yet."""
        self._protocol: ConformanceProtocol | NativeQuicConformanceProtocol | None = None
        self._session: WebTransportServerSession | NativeQuicSession | None = None
        self._session_ready: asyncio.Future[WebTransportServerSession | NativeQuicSession] = (
            asyncio.get_running_loop().create_future()
        )
        self._streams: dict[int, _Stream] = {}

    def attach(self, protocol: ConformanceProtocol | NativeQuicConformanceProtocol) -> None:
        self._protocol = protocol

    def accepted(self, session: WebTransportServerSession | NativeQuicSession) -> None:
        self._session = session
        if not self._session_ready.done():
            self._session_ready.set_result(session)

    async def _current(self) -> WebTransportServerSession | NativeQuicSession:
        return await self._session_ready

    def _require_protocol(self) -> ConformanceProtocol | NativeQuicConformanceProtocol:
        if self._protocol is None:
            raise RuntimeError("no connection yet")
        return self._protocol

    def _stream(self, stream_id: int) -> _Stream:
        stream = self._streams.get(stream_id)
        if stream is None:
            raise RuntimeError(f"no stream {stream_id}")
        return stream

    # ------------------------------------------------------------------ methods

    async def prepare(self, handshake: str = "accept", select: str = "first", maxStreams: int | None = None) -> None:
        await self.dispose()
        self.next_options = DialOptions(handshake=handshake, select=select, max_streams=maxStreams)
        self.observed = None
        self.handshake_released = False
        self._protocol = None
        self._session = None
        self._session_ready = asyncio.get_running_loop().create_future()
        self._streams = {}

    async def dispose(self) -> None:
        if self._protocol is not None:
            self._protocol.dispose()

    async def acceptUni(self) -> int:
        session = await self._current()
        receive = cast(_ReceiveHalf, await anext(aiter(session.incoming_unidirectional_streams())))
        self._streams[receive.stream_id] = _Stream(receive=receive, from_dialer=True)
        return receive.stream_id

    async def acceptBi(self) -> int:
        session = await self._current()
        stream = await anext(aiter(session.incoming_bidirectional_streams()))
        stream_id = cast(_ReceiveHalf, stream.readable).stream_id
        self._streams[stream_id] = _Stream(
            send=stream.writable, receive=stream.readable, from_dialer=True, bidirectional=True
        )
        return stream_id

    async def openUni(self) -> int:
        send = cast(_SendHalf, await (await self._current()).create_unidirectional_stream())
        self._streams[send.stream_id] = _Stream(send=send)
        return send.stream_id

    async def openBi(self) -> int:
        stream = await (await self._current()).create_bidirectional_stream()
        stream_id = cast(_ReceiveHalf, stream.readable).stream_id
        self._streams[stream_id] = _Stream(send=stream.writable, receive=stream.readable, bidirectional=True)
        return stream_id

    async def write(self, stream: int, data: list[int]) -> None:
        await self._stream(stream).send.write(bytes(data))

    async def finish(self, stream: int) -> None:
        await self._stream(stream).send.close()

    async def reset(self, stream: int, code: int | str) -> None:
        session = await self._current()
        # On native QUIC a code may be a decimal string above 2^53-1, beyond what a reason carries (TA-17).
        if isinstance(session, NativeQuicSession):
            session.reset_codes[stream] = int(code)
        self._stream(stream).send.abort(int(code))

    async def stop(self, stream: int, code: int) -> None:
        self._stream(stream).receive.cancel(int(code))

    async def read(self, stream: int, count: int | None = None, discard: bool = False) -> dict[str, Any]:
        """Read until the FIN, a reset, or ``count`` bytes; a FIN on a dialer's stream returns one stream of credit."""
        entry = self._stream(stream)
        session = await self._current()
        out = bytearray()
        read = 0

        def result(fin: bool, reset_code: int | None) -> dict[str, Any]:
            return {"bytes": [] if discard else list(out), "byteLength": read, "fin": fin, "resetCode": reset_code}

        try:
            while count is None or read < count:
                chunk = await entry.receive.read()
                if chunk is None:
                    if entry.from_dialer:
                        self._require_protocol().grant_stream(entry.bidirectional)
                    return result(True, None)
                read += len(chunk)
                if not discard:
                    out += chunk
        except QuicTransportError as error:
            if session.ended:
                raise
            return result(False, error.stream_error_code)
        return result(False, None)

    async def stopped(self, stream: int) -> int | None:
        send = self._stream(stream).send
        session = await self._current()
        while send._stopped is None:
            if session.ended:
                raise RuntimeError("the session ended before the dialer stopped the stream")
            await asyncio.sleep(0.01)
        return send._stopped.stream_error_code

    async def close(self, code: int, reason: str) -> None:
        session = await self._current()
        protocol = self._protocol
        paused = protocol is not None and protocol.impairment is not None and protocol.impairment.paused
        if paused and isinstance(session, WebTransportServerSession):
            _close_at_once(session, code, reason)
        else:
            session.close(code, reason)

    async def closed(self) -> dict[str, Any]:
        session = await self._current()
        await asyncio.wait({session._closed})
        if session.close_info is None:
            raise RuntimeError("the session ended without a CLOSE_WEBTRANSPORT_SESSION capsule")
        code, reason = session.close_info
        return {"closeCode": code, "reason": reason}

    async def quicClose(self) -> None:
        protocol = self._require_protocol()
        # A frame type makes aioquic send CONNECTION_CLOSE 0x1c, the QUIC layer's, rather than the application's 0x1d.
        protocol._quic.close(
            error_code=QuicErrorCode.INTERNAL_ERROR, frame_type=QuicFrameType.PADDING, reason_phrase="conformance"
        )
        protocol.transmit()

    async def loseConnection(self) -> None:
        self._require_protocol().lose()

    async def pause(self) -> None:
        self._require_protocol().impairment = Impairment(paused=True)

    async def resume(self) -> None:
        self._require_protocol().impairment = None

    async def releaseHandshake(self) -> None:
        self.handshake_released = True
        if self._protocol is not None:
            self._protocol.release_handshake()

    async def observedDial(self) -> dict[str, Any] | None:
        return self.observed


METHODS = {
    "prepare",
    "dispose",
    "acceptUni",
    "acceptBi",
    "openUni",
    "openBi",
    "write",
    "finish",
    "reset",
    "stop",
    "read",
    "stopped",
    "close",
    "closed",
    "quicClose",
    "loseConnection",
    "pause",
    "resume",
    "releaseHandshake",
    "observedDial",
}


def _reply(message: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(message) + "\n")
    sys.stdout.flush()


async def _handle(peer: ConformancePeer, line: str) -> None:
    request = json.loads(line)
    request_id = request.get("id")
    method = request.get("method")
    try:
        if method not in METHODS:
            raise RuntimeError(f"unknown method {method!r}")
        result = await getattr(peer, method)(**(request.get("params") or {}))
        _reply({"id": request_id, "result": result})
    except Exception as error:
        _reply({"id": request_id, "error": f"{type(error).__name__}: {error}"})


async def main(framing: str, listen: str | None = None) -> None:
    loop = asyncio.get_running_loop()
    peer = ConformancePeer(framing)
    certificate, key, digest = generate_leaf_certificate()
    if framing == "native-quic":
        # ALPN is chosen per connection (`_select_protocol`). Listening on `::` takes both families, so a dialer that
        # resolves `localhost` to ::1 or to 127.0.0.1 reaches it.
        configuration = QuicConfiguration(is_client=False, idle_timeout=IDLE_TIMEOUT)
        host, create_protocol = "::", partial(NativeQuicConformanceProtocol, peer=peer)
    else:
        configuration = QuicConfiguration(
            is_client=False, alpn_protocols=["h3"], max_datagram_frame_size=65536, idle_timeout=IDLE_TIMEOUT
        )
        host, create_protocol = "127.0.0.1", partial(ConformanceProtocol, peer=peer)
    configuration.certificate = certificate
    configuration.private_key = key
    server = await serve(listen or host, 0, configuration=configuration, create_protocol=create_protocol)
    transport = server._transport
    assert transport is not None
    _reply(
        {
            "event": "listening",
            "port": transport.get_extra_info("sockname")[1],
            "certificateHash": digest,
            "certificatePem": certificate.public_bytes(serialization.Encoding.PEM).decode(),
        }
    )

    reader = asyncio.StreamReader()
    await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), sys.stdin)
    tasks: set[asyncio.Task[None]] = set()
    while line := await reader.readline():
        task = asyncio.create_task(_handle(peer, line.decode()))
        tasks.add(task)
        task.add_done_callback(tasks.discard)
    server.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--framing", choices=["webtransport", "native-quic"], default="webtransport")
    parser.add_argument("--host", help="the address to listen on, instead of the framing's default")
    arguments = parser.parse_args()
    asyncio.run(main(arguments.framing, arguments.host))
