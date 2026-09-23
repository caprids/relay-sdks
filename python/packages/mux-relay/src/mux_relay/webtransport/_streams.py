"""The two halves of a WebTransport stream, and the queue incoming streams are accepted from."""

from __future__ import annotations

import asyncio
from collections import deque
from typing import TYPE_CHECKING, Any

from moq_core._tasks import observe

from ._errors import WebTransportError, _error_code_of

if TYPE_CHECKING:
    from ._session import WebTransportSessionBase


class _ReceiveHalf:
    """The receiving side of a WebTransport stream; satisfies :class:`ReceiveStream`."""

    __slots__ = ("_cancelled", "_chunks", "_eof", "_error", "_session", "_stream_id", "_waiter")

    def __init__(self, session: WebTransportSessionBase, stream_id: int) -> None:
        self._session = session
        self._stream_id = stream_id
        self._chunks: deque[bytes] = deque()
        self._eof = False
        self._error: BaseException | None = None
        self._cancelled = False
        self._waiter: asyncio.Future[None] | None = None

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
        self._session._stop_stream(self._stream_id, _error_code_of(reason))
        self._wake()

    def feed(self, data: bytes, end_stream: bool) -> None:
        if self._cancelled or self._eof or self._error is not None:
            return

        if data:
            self._chunks.append(data)

        if end_stream:
            self._eof = True

        self._wake()

    def fail(self, error: BaseException) -> None:
        if self._eof or self._error is not None:
            return

        self._error = error
        self._wake()

    @property
    def ended(self) -> bool:
        """The peer finished this half (FIN or reset); nothing more can arrive on it."""
        return self._eof or self._error is not None

    def _wake(self) -> None:
        waiter = self._waiter

        if waiter is not None and not waiter.done():
            waiter.set_result(None)

        self._waiter = None


class _SendHalf:
    """The sending side of a WebTransport stream; satisfies :class:`SendStream`.

    aioquic queues written bytes without limit and transmits them as congestion and flow control
    allow, so ``write()`` and ``close()`` settle only once everything queued on the stream so far
    has been handed to the network. That is the backpressure a browser ``WritableStream`` applies:
    a slow link is felt as a slow write, and a caller's deadline covers the transport write.
    """

    __slots__ = ("_aborted", "_closed", "_error", "_session", "_stopped", "_stream_id", "_transmitted")

    def __init__(self, session: WebTransportSessionBase, stream_id: int) -> None:
        self._session = session
        self._stream_id = stream_id
        self._closed = False
        self._aborted = False
        self._stopped: int | None = None
        self._error: BaseException | None = None
        self._transmitted: asyncio.Future[None] | None = None
        """Pending while queued bytes (or the FIN) await transmission; ``None`` when nothing is waiting."""

    def _check_writable(self) -> None:
        if self._error is not None:
            raise self._error

        if self._aborted:
            raise WebTransportError("stream was aborted")

        if self._closed:
            raise WebTransportError("stream is closed")

        if self._stopped is not None:
            raise WebTransportError("peer stopped the stream", code=self._stopped)

        self._session._check_live()

    async def write(self, data: bytes) -> None:
        self._check_writable()

        if data:
            await self._session._send(self, bytes(data), False)

    async def close(self) -> None:
        self._check_writable()
        self._closed = True
        await self._session._send(self, b"", True)

    def abort(self, reason: object = None) -> None:
        # A FIN that already went out is a complete stream at the peer; a reset now would only destroy it.
        if self._aborted or self._error is not None or (self._closed and self._transmitted is None):
            return

        self._aborted = True
        # Fail a pending write with the caller's reason before resetting: `_reset_stream` transmits,
        # and the reset branch of `_settle_stream` would otherwise settle the waiter first with a
        # generic "stream was aborted", masking a `write_timeout`'s WRITE_TIMEOUT error.
        self._fail_transmit(reason if isinstance(reason, BaseException) else WebTransportError("stream was aborted"))
        self._session._reset_stream(self._stream_id, _error_code_of(reason))

    def stopped(self, code: int) -> None:
        if self._stopped is None:
            self._stopped = code
            # aioquic answers STOP_SENDING with a reset of its own, so queued bytes will never go out.
            self._fail_transmit(WebTransportError("peer stopped the stream", code=code))

    def fail(self, error: BaseException) -> None:
        if self._error is None and not self._closed and not self._aborted:
            self._error = error

        self._fail_transmit(error)

    def _transmit_waiter(self) -> asyncio.Future[None]:
        # Never hand out a future that is already done (settled, failed, or cancelled from outside):
        # a later write or close on the stream must get a live waiter of its own.
        if self._transmitted is None or self._transmitted.done():
            self._transmitted = asyncio.get_running_loop().create_future()
            observe(self._transmitted)

        return self._transmitted

    def _settle_transmit(self) -> None:
        waiter, self._transmitted = self._transmitted, None

        if waiter is not None and not waiter.done():
            waiter.set_result(None)

    def _fail_transmit(self, error: BaseException) -> None:
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
