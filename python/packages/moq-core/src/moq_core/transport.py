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
- A stream or session failure is classified by its shape, not its class: its ``source`` is ``"stream"`` or
  ``"session"`` (:func:`is_stream_error`, :func:`is_session_error`; ``spec/transport-adapter.md`` TA-22).
- A send stream may also report when the peer acknowledged it (:class:`AcknowledgedSendStream`).
  That is optional: :func:`wait_acknowledged` treats a stream without it as acknowledged once
  written, which is all an in-memory transport can say.
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


@runtime_checkable
class AcknowledgedSendStream(SendStream, Protocol):
    """A send stream that can tell when the peer acknowledged everything sent on it."""

    async def wait_acknowledged(self) -> None:
        """Return once the peer acknowledged every byte and the FIN.

        Raises when that can no longer happen: the stream was reset or stopped, or the session ended
        first. Meaningful only after ``close()``.
        """
        ...


async def wait_acknowledged(stream: SendStream) -> None:
    """Wait until the peer acknowledged ``stream``, FIN included, where its transport can tell.

    A stream that does not report acknowledgement counts as acknowledged at once. Raises as
    :meth:`AcknowledgedSendStream.wait_acknowledged` does.
    """
    if isinstance(stream, AcknowledgedSendStream):
        await stream.wait_acknowledged()


def is_stream_error(error: object) -> bool:
    """Whether ``error`` is a stream failure (``spec/transport-adapter.md`` TA-22): its ``source`` is ``"stream"``.

    The peer reset the stream (TA-19) or stopped it (TA-20). Such an error carries the peer's code as
    ``stream_error_code``, ``None`` when it is unknown. The error's class is not part of the contract: this
    reads the shape alone, so any transport's errors are understood.
    """
    return getattr(error, "source", None) == "stream"


def is_session_error(error: object) -> bool:
    """Whether ``error`` is a session failure (TA-22): its ``source`` is ``"session"``.

    The session is over, and the operation failed because of it (TA-23), or the dial failed.
    """
    return getattr(error, "source", None) == "session"


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

    async def create_unidirectional_stream(self) -> SendStream:
        """Open a unidirectional stream to the peer."""
        ...

    async def create_bidirectional_stream(self) -> BidirectionalStream:
        """Open a bidirectional stream to the peer."""
        ...

    def close(self, close_code: int = 0, reason: str = "") -> None:
        """Close the session. Idempotent; a transport that already ended ignores it."""
        ...

    async def wait_closed(self) -> None:
        """Return once the session ended cleanly (either side); raise the error that ended it otherwise."""
        ...


@runtime_checkable
class _EndAware(Protocol):
    @property
    def ended(self) -> bool: ...


def transport_ended(transport: MoqtTransport) -> bool:
    """Whether the transport has already ended, ahead of ``wait_closed`` returning.

    A transport may expose this as an optional synchronous ``ended`` property that turns true before
    it fails its streams, as the native transport does: a stream failure seen while it is true is the
    closure's doing, not a reset the peer sent. ``False`` for a transport that cannot say; its
    closure then arrives only through ``wait_closed``.
    """
    return isinstance(transport, _EndAware) and transport.ended


__all__ = [
    "AcknowledgedSendStream",
    "BidirectionalStream",
    "MoqtTransport",
    "ReceiveStream",
    "SendStream",
    "is_session_error",
    "is_stream_error",
    "transport_ended",
    "wait_acknowledged",
]
