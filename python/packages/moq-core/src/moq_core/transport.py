"""The abstract transport seam the session drivers run over.

The core never opens a network connection. A product package (``mux_relay``) supplies an object
satisfying :class:`MoqtTransport`, and tests supply in-memory fakes. The shape is the structural
subset of a WebTransport session the drivers need, spelled for asyncio: unidirectional and
bidirectional streams, incoming-stream acceptance, and closure observation.

Stream semantics follow QUIC and the WHATWG streams the TypeScript core is written against:

- :meth:`ReceiveStream.read` returns the next chunk of bytes, ``None`` once the peer FINed, and
  raises when the peer reset the stream. Chunks are never empty.
- :meth:`ReceiveStream.cancel` is STOP_SENDING: the reader wants no more bytes.
- :meth:`SendStream.write` returns once the transport accepted the bytes (not when the peer read
  them); :meth:`SendStream.close` FINs the stream; :meth:`SendStream.abort` is RESET_STREAM.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@runtime_checkable
class ReceiveStream(Protocol):
    """The receiving half of a stream."""

    async def read(self) -> bytes | None:
        """Return the next non-empty chunk, or ``None`` at a clean end of stream. Raises on a reset."""
        ...

    def cancel(self, reason: object = None) -> None:
        """Stop reading (STOP_SENDING). Later ``read`` calls end; the peer sees the stream stopped."""
        ...


@runtime_checkable
class SendStream(Protocol):
    """The sending half of a stream."""

    async def write(self, data: bytes) -> None:
        """Queue ``data``; returns once the transport accepted it.

        Raises once the stream is closed, aborted, or stopped by the peer.
        """
        ...

    async def close(self) -> None:
        """FIN the stream after every accepted write."""
        ...

    def abort(self, reason: object = None) -> None:
        """Reset the stream (RESET_STREAM); queued bytes may never arrive."""
        ...


@dataclass(frozen=True, slots=True)
class BidirectionalStream:
    """A bidirectional stream as its two halves."""

    readable: ReceiveStream
    writable: SendStream


@runtime_checkable
class MoqtTransport(Protocol):
    """Structural subset of a WebTransport session the session drivers need."""

    def incoming_unidirectional_streams(self) -> AsyncIterator[ReceiveStream]:
        """Iterate peer-initiated unidirectional streams until the transport ends."""
        ...

    def incoming_bidirectional_streams(self) -> AsyncIterator[BidirectionalStream]:
        """Iterate peer-initiated bidirectional streams until the transport ends."""
        ...

    async def create_unidirectional_stream(self) -> SendStream: ...

    async def create_bidirectional_stream(self) -> BidirectionalStream: ...

    def close(self, close_code: int = 0, reason: str = "") -> None:
        """Close the session. Idempotent; a transport that already ended ignores it."""
        ...

    async def wait_closed(self) -> None:
        """Return once the session ended cleanly (either side); raise the error that ended it otherwise."""
        ...


__all__ = ["BidirectionalStream", "MoqtTransport", "ReceiveStream", "SendStream"]
