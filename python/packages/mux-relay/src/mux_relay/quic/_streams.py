"""The two halves of a QUIC stream, the link they need from their session, and the incoming-stream queue.

A half never touches QUIC. It asks its :class:`_StreamLink` to queue bytes or a FIN, to reset or stop a
stream, and whether the session is still live; the link drives the half back through
:meth:`_ReceiveHalf.feed` and :meth:`_ReceiveHalf.fail`, and :meth:`_SendHalf.settled`,
:meth:`_SendHalf.fail_transmit`, :meth:`_SendHalf.finished`, :meth:`_SendHalf.stopped` and
:meth:`_SendHalf.fail`.
``QuicStreamSession`` is the production link; the tests connect a send half to a receive half
directly (``relay_helpers/stream_link.py``).
"""

from __future__ import annotations

import asyncio
from collections import deque
from typing import Any, Protocol

from moq_core._tasks import observe

from ._errors import QuicTransportError, _error_code_of, _stream_error


class _StreamLink(Protocol):
    """What a half needs from the session it belongs to."""

    def check_live(self) -> None:
        """Raise the session's closure error when it can no longer carry streams."""

    async def send(self, half: _SendHalf, data: bytes, end_stream: bool) -> None:
        """Queue ``data`` (and the FIN when ``end_stream``) on the half's stream; return once it was transmitted."""

    def reset_stream(self, stream_id: int, code: int) -> None:
        """RESET_STREAM with the application error ``code``."""

    def stop_stream(self, stream_id: int, code: int) -> None:
        """STOP_SENDING with the application error ``code``."""


class _ReceiveHalf:
    """The receiving side of a stream; satisfies :class:`ReceiveStream`."""

    __slots__ = ("_cancelled", "_chunks", "_eof", "_error", "_link", "_stream_id", "_waiter")

    def __init__(self, link: _StreamLink, stream_id: int) -> None:
        self._link = link
        self._stream_id = stream_id
        self._chunks: deque[bytes] = deque()
        self._eof = False
        self._error: BaseException | None = None
        self._cancelled = False
        self._waiter: asyncio.Future[None] | None = None

    @property
    def stream_id(self) -> int:
        return self._stream_id

    # ------------------------------------------------------------------ ReceiveStream

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
        self._link.stop_stream(self._stream_id, _error_code_of(reason))
        self._wake()

    # ------------------------------------------------------------------ driven by the link

    def feed(self, data: bytes, end_stream: bool) -> None:
        """Bytes (and possibly the FIN) arrived. Ignored once the half ended or was cancelled: a retransmitted FIN."""
        if self._cancelled or self._eof or self._error is not None:
            return

        if data:
            self._chunks.append(data)

        if end_stream:
            self._eof = True

        self._wake()

    def fail(self, error: BaseException) -> None:
        """The peer reset the stream, or the session ended. Buffered chunks are still read first."""
        if self._eof or self._error is not None:
            return

        self._error = error
        self._wake()

    @property
    def ended(self) -> bool:
        """The peer finished this half (FIN or reset); nothing more can arrive on it."""
        return self._eof or self._error is not None

    @property
    def cancelled(self) -> bool:
        """The application stopped reading (STOP_SENDING)."""
        return self._cancelled

    def _wake(self) -> None:
        waiter = self._waiter

        if waiter is not None and not waiter.done():
            waiter.set_result(None)

        self._waiter = None


class _SendHalf:
    """The sending side of a stream; satisfies :class:`SendStream`.

    aioquic queues written bytes without limit and transmits them as congestion and flow control
    allow, so ``write()`` and ``close()`` settle only once everything queued on the stream so far
    has been handed to the network. That is the backpressure a browser ``WritableStream`` applies:
    a slow link is felt as a slow write, and a caller's deadline covers the transport write.

    :meth:`wait_acknowledged` waits one step further, for the peer's acknowledgement of the whole
    stream, which the link reports through :meth:`finished`. It satisfies
    :class:`~moq_core.transport.AcknowledgedSendStream`.
    """

    __slots__ = (
        "_aborted",
        "_acknowledged",
        "_closed",
        "_delivery",
        "_delivery_error",
        "_error",
        "_link",
        "_stopped",
        "_stream_id",
        "_transmitted",
    )

    def __init__(self, link: _StreamLink, stream_id: int) -> None:
        self._link = link
        self._stream_id = stream_id
        self._closed = False
        self._aborted = False
        self._stopped: QuicTransportError | None = None
        """The peer's STOP_SENDING, once it arrived: what every later operation raises."""
        self._error: BaseException | None = None
        self._transmitted: asyncio.Future[None] | None = None
        """Pending while queued bytes (or the FIN) await transmission; ``None`` when nothing is waiting."""
        self._acknowledged = False
        self._delivery_error: BaseException | None = None
        """Why the stream will never be acknowledged, once that is known."""
        self._delivery: asyncio.Future[None] | None = None
        """What :meth:`wait_acknowledged` waits on; created by the first waiter."""

    @property
    def stream_id(self) -> int:
        return self._stream_id

    @property
    def closed(self) -> bool:
        """``close()`` was called: the FIN is queued, in flight, or acknowledged."""
        return self._closed

    @property
    def pending(self) -> bool:
        """A write or the FIN is waiting to be transmitted."""
        return self._transmitted is not None

    # ------------------------------------------------------------------ SendStream

    def _check_writable(self) -> None:
        if self._error is not None:
            raise self._error

        if self._aborted:
            raise QuicTransportError("stream was aborted")

        if self._closed:
            raise QuicTransportError("stream is closed")

        if self._stopped is not None:
            raise self._stopped

        self._link.check_live()

    async def write(self, data: bytes) -> None:
        self._check_writable()

        if data:
            await self._link.send(self, bytes(data), False)

    async def close(self) -> None:
        self._check_writable()
        self._closed = True
        await self._link.send(self, b"", True)

    async def wait_acknowledged(self) -> None:
        """Return once the peer acknowledged every byte and the FIN of this stream.

        Raises when that can no longer happen: the stream was reset or stopped, or the session ended
        first. Meaningful only after ``close()``.
        """
        if self._delivery is None:
            self._delivery = asyncio.get_running_loop().create_future()
            observe(self._delivery)
            self._settle_delivery()

        # Shielded: one waiter giving up must not cancel the future every other waiter shares.
        await asyncio.shield(self._delivery)

    def abort(self, reason: object = None) -> None:
        """RESET_STREAM now, in every state short of the peer's acknowledgement of the whole stream (TA-16).

        That includes a FIN that was transmitted but not yet acknowledged: the peer may never have
        received it, and a settling group's delivery timeout resets exactly that stream
        (draft-ietf-moq-transport-20 section 8). :meth:`wait_acknowledged` then raises ``reason``.
        Once the FIN is acknowledged there is nothing left to reset, and this does nothing.
        """
        if self._aborted or self._error is not None or self._acknowledged:
            return

        self._aborted = True
        self._end_delivery(reason if isinstance(reason, BaseException) else QuicTransportError("stream was aborted"))
        # Fail a pending write with the caller's reason before resetting: `reset_stream` transmits,
        # and the reset branch of the session's settlement would otherwise settle the waiter first with
        # a generic "stream was aborted", masking a `write_timeout`'s WRITE_TIMEOUT error.
        self.fail_transmit(reason if isinstance(reason, BaseException) else QuicTransportError("stream was aborted"))
        self._link.reset_stream(self._stream_id, _error_code_of(reason))

    # ------------------------------------------------------------------ driven by the link

    def stopped(self, code: int | None) -> None:
        """The peer sent STOP_SENDING with ``code`` (``None`` when unknown): queued bytes will never go out.

        aioquic answers with a reset of its own. Every later operation raises a ``source="stream"`` error with
        the code; on a send half that is what makes it a stop (TA-20).
        """
        if self._stopped is None:
            self._stopped = stopped = _stream_error("stopped", code)
            self._end_delivery(stopped)
            self.fail_transmit(stopped)

    def fail(self, error: BaseException) -> None:
        """The session ended, or the stream is gone: the pending transmission fails, and so does every later write."""
        if self._error is None and not self._closed and not self._aborted:
            self._error = error

        self._end_delivery(error)
        self.fail_transmit(error)

    def finished(self) -> None:
        """aioquic is done with the sending side: the FIN or a reset was acknowledged, or the stream discarded.

        For a closed half that was never reset or stopped, that is the peer's acknowledgement of the whole stream.
        """
        if not self._closed or self._aborted or self._stopped is not None:
            self._end_delivery(QuicTransportError("stream was reset"))
            return

        if self._delivery_error is None and not self._acknowledged:
            self._acknowledged = True
            self._settle_delivery()

    def _end_delivery(self, error: BaseException) -> None:
        """The stream can no longer be acknowledged. The first reason sticks; an acknowledgement already seen wins."""
        if self._acknowledged or self._delivery_error is not None:
            return

        self._delivery_error = error
        self._settle_delivery()

    def _settle_delivery(self) -> None:
        delivery = self._delivery

        if delivery is None or delivery.done():
            return

        if self._acknowledged:
            delivery.set_result(None)
        elif self._delivery_error is not None:
            delivery.set_exception(self._delivery_error)

    def transmit_waiter(self) -> asyncio.Future[None]:
        """The future the link settles once everything queued so far went out; concurrent writers share it."""
        # Never hand out a future that is already done (settled, failed, or cancelled from outside):
        # a later write or close on the stream must get a live waiter of its own.
        if self._transmitted is None or self._transmitted.done():
            self._transmitted = asyncio.get_running_loop().create_future()
            observe(self._transmitted)

        return self._transmitted

    def settled(self) -> None:
        """Everything queued so far has been handed to the wire."""
        waiter, self._transmitted = self._transmitted, None

        if waiter is not None and not waiter.done():
            waiter.set_result(None)

    def fail_transmit(self, error: BaseException) -> None:
        """What was queued will not be transmitted; the half itself is unchanged (see :meth:`fail` for that)."""
        waiter, self._transmitted = self._transmitted, None

        if waiter is not None and not waiter.done():
            waiter.set_exception(error)


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
