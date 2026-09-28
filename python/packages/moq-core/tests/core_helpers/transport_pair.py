"""Symmetric in-memory ``MoqtTransport`` pair for loopback tests.

Two transports whose stream-creation methods surface on the peer's incoming stream queues, so a
publish session on one side can talk to a subscribe session on the other over real (in-process)
streams: the same structural seam the aioquic adapter satisfies in production.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

from moq_core.control_messages import MESSAGE_TYPE
from moq_core.transport import BidirectionalStream, MoqtTransport, ReceiveStream, SendStream
from moq_core.varint import decode_varint, encode_varint

from .streams import Pipe, PipeEvent, PushChannel, StreamClosedError


@dataclass(slots=True)
class CloseInfo:
    close_code: int = 0
    reason: str = ""


@dataclass(slots=True)
class _Side:
    incoming_uni: PushChannel[ReceiveStream] = field(default_factory=PushChannel)
    incoming_bidi: PushChannel[BidirectionalStream] = field(default_factory=PushChannel)
    closed: asyncio.Future[None] = field(default_factory=lambda: asyncio.get_running_loop().create_future())


class PairTransport:
    """One end of a :func:`create_transport_pair`."""

    def __init__(self, local: _Side, remote: _Side, shared: _Shared) -> None:
        self._local = local
        self._remote = remote
        self._shared = shared

    def incoming_unidirectional_streams(self) -> AsyncIterator[ReceiveStream]:
        return self._local.incoming_uni

    def incoming_bidirectional_streams(self) -> AsyncIterator[BidirectionalStream]:
        return self._local.incoming_bidi

    async def create_unidirectional_stream(self) -> SendStream:
        if self._shared.closed:
            raise StreamClosedError("transport closed")

        pipe = self._shared.pipe()
        self._remote.incoming_uni.push(pipe)
        return pipe

    async def create_bidirectional_stream(self) -> BidirectionalStream:
        if self._shared.closed:
            raise StreamClosedError("transport closed")

        local_to_remote = self._shared.pipe()
        remote_to_local = self._shared.pipe()
        self._remote.incoming_bidi.push(BidirectionalStream(readable=local_to_remote, writable=remote_to_local))
        return BidirectionalStream(readable=remote_to_local, writable=local_to_remote)

    def close(self, close_code: int = 0, reason: str = "") -> None:
        self._shared.close_both(CloseInfo(close_code, reason))

    async def wait_closed(self) -> None:
        await self._local.closed


class _Shared:
    def __init__(self, a: _Side, b: _Side) -> None:
        self._sides = (a, b)
        self._pipes: list[Pipe] = []
        self.closed = False
        self.close_info: CloseInfo | None = None
        self.events: list[PipeEvent] = []
        """Every writer-side step on every pipe, in the order it was issued."""

    def pipe(self) -> Pipe:
        pipe = Pipe(self.events.append)
        self._pipes.append(pipe)
        return pipe

    def close_both(self, info: CloseInfo | None) -> None:
        if self.closed:
            return

        self.closed = True
        self.close_info = info

        for side in self._sides:
            if not side.closed.done():
                side.closed.set_result(None)

            side.incoming_uni.close()
            side.incoming_bidi.close()

        # A real transport tears its streams down with the session; leaving them open would strand
        # every read loop forever.
        for pipe in self._pipes:
            pipe.break_pipe(StreamClosedError("transport closed"))


@dataclass(slots=True)
class TransportPair:
    client: PairTransport
    server: PairTransport

    @property
    def events(self) -> list[PipeEvent]:
        """Writer-side steps on both transports' streams, in issue order (see :class:`Pipe`)."""
        return self.client._shared.events


def create_transport_pair() -> TransportPair:
    """Create two transports wired back-to-back. Requires a running event loop."""
    a = _Side()
    b = _Side()
    shared = _Shared(a, b)
    return TransportPair(client=PairTransport(a, b, shared), server=PairTransport(b, a, shared))


# Request-initiating messages whose body opens with the sender's Request ID (§10.1).
_REQUEST_ID_MESSAGE_TYPES = frozenset(
    {
        MESSAGE_TYPE.SUBSCRIBE,
        MESSAGE_TYPE.FETCH,
        MESSAGE_TYPE.TRACK_STATUS,
        MESSAGE_TYPE.REQUEST_UPDATE,
        MESSAGE_TYPE.SUBSCRIBE_NAMESPACE,
        MESSAGE_TYPE.PUBLISH_NAMESPACE,
        MESSAGE_TYPE.PUBLISH,
    }
)


def set_request_id_parity(chunk: bytes) -> bytes:
    """Copy ``chunk`` with the least significant bit of every request-initiating frame's Request ID set.

    Uses the core's MOQT varint codec. Every length limit of that encoding is a power of two (2^7,
    2^14, ...), so an even value and its odd successor always encode to the same length and the
    field is re-encoded in place.
    """
    data = bytearray(chunk)
    offset = 0

    while offset < len(data):
        message_type, type_length = decode_varint(data, offset)
        length_at = offset + type_length
        body_start = length_at + 2

        if message_type in _REQUEST_ID_MESSAGE_TYPES:
            request_id, id_length = decode_varint(data, body_start)
            encoded = encode_varint(request_id | 1)

            if len(encoded) != id_length:
                raise AssertionError(f"request id {request_id | 1} would change the encoded length")

            data[body_start : body_start + id_length] = encoded

        offset = body_start + int.from_bytes(data[length_at:body_start], "big")

    return bytes(data)


class _ServerRequestIdStream:
    """A request stream's sending half that rewrites each write with :func:`set_request_id_parity`."""

    def __init__(self, inner: SendStream) -> None:
        self._inner = inner

    async def write(self, data: bytes) -> None:
        await self._inner.write(set_request_id_parity(data))

    async def close(self) -> None:
        await self._inner.close()

    def abort(self, reason: object = None) -> None:
        self._inner.abort(reason)


class ServerRequestIdTransport:
    """See :func:`with_server_request_ids`."""

    def __init__(self, inner: MoqtTransport) -> None:
        self._inner = inner

    def incoming_unidirectional_streams(self) -> AsyncIterator[ReceiveStream]:
        return self._inner.incoming_unidirectional_streams()

    def incoming_bidirectional_streams(self) -> AsyncIterator[BidirectionalStream]:
        return self._inner.incoming_bidirectional_streams()

    async def create_unidirectional_stream(self) -> SendStream:
        return await self._inner.create_unidirectional_stream()

    async def create_bidirectional_stream(self) -> BidirectionalStream:
        stream = await self._inner.create_bidirectional_stream()
        return BidirectionalStream(readable=stream.readable, writable=_ServerRequestIdStream(stream.writable))

    def close(self, close_code: int = 0, reason: str = "") -> None:
        self._inner.close(close_code, reason)

    async def wait_closed(self) -> None:
        await self._inner.wait_closed()


def with_server_request_ids(transport: MoqtTransport) -> ServerRequestIdTransport:
    """Give the subscribe driver a server's request ids.

    The driver always allocates client (even) ids, and a publish session is always the client too,
    so it requires the peer's ids to be odd (§10.1). A test that stands the driver in for the relay
    wraps the driver's transport with this: every request stream the driver opens has each Request
    ID's least significant bit set on the wire (0 becomes 1, 2 becomes 3). Assumes each write
    carries whole frames, which is how the driver writes them.
    """
    return ServerRequestIdTransport(transport)
