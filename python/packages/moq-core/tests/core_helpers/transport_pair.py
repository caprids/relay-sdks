"""Symmetric in-memory ``MoqtTransport`` pair for loopback tests.

Two transports whose stream-creation methods surface on the peer's incoming stream queues, so a
publish session on one side can talk to a subscribe session on the other over real (in-process)
streams: the same structural seam the aioquic adapter satisfies in production.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

from moq_core.transport import BidirectionalStream, ReceiveStream, SendStream

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
