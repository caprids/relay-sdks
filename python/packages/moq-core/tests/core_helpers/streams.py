"""In-memory streams with the semantics of a WHATWG ``TransformStream`` pipe over a QUIC stream.

A :class:`Pipe` is one direction: the writer side satisfies :class:`moq_core.transport.SendStream`
and the reader side :class:`moq_core.transport.ReceiveStream`. Like a ``TransformStream`` with a
zero high-water mark, a write settles only once the far side reads it, so tests can hold a response
"in flight" by not reading. ``close()`` FINs after every accepted write, ``abort()`` resets (the
reader raises), and the reader's ``cancel()`` is STOP_SENDING (later writes raise).
"""

from __future__ import annotations

import asyncio
from collections import deque
from typing import Generic, TypeVar

T = TypeVar("T")


class StreamClosedError(Exception):
    """The stream ended before the operation could complete."""

    def __init__(self, message: str, reason: object = None) -> None:
        super().__init__(message)
        self.reason = reason


class Pipe:
    def __init__(self) -> None:
        self._chunks: deque[tuple[bytes, asyncio.Future[None]]] = deque()
        self._reader_waiter: asyncio.Future[None] | None = None
        self._closed = False
        self._closing_waiters: list[asyncio.Future[None]] = []
        self._error: BaseException | None = None
        self._cancelled = False
        self._cancel_reason: object = None
        self._broken: BaseException | None = None

    # ------------------------------------------------------------------ writer

    def _check_writable(self) -> None:
        if self._broken is not None:
            raise self._broken
        if self._error is not None:
            raise StreamClosedError("stream was aborted", self._error)
        if self._closed:
            raise StreamClosedError("stream is closed")
        if self._cancelled:
            raise StreamClosedError("reader cancelled the stream", self._cancel_reason)

    async def write(self, data: bytes) -> None:
        self._check_writable()

        if len(data) == 0:
            return

        accepted: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._chunks.append((bytes(data), accepted))
        self._wake_reader()
        await accepted

    async def close(self) -> None:
        self._check_writable()

        # Like WritableStreamDefaultWriter.close(): settle after every queued write.
        while self._chunks:
            self._check_writable()
            waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            self._closing_waiters.append(waiter)
            await waiter

        self._check_writable()
        self._closed = True
        self._wake_reader()

    def abort(self, reason: object = None) -> None:
        if self._closed or self._error is not None or self._broken is not None:
            return

        self._error = reason if isinstance(reason, BaseException) else StreamClosedError("stream reset", reason)
        self._fail_pending(StreamClosedError("stream was aborted", reason))
        self._wake_reader()

    # ------------------------------------------------------------------ reader

    async def read(self) -> bytes | None:
        while True:
            if self._chunks:
                data, accepted = self._chunks.popleft()

                if not accepted.done():
                    accepted.set_result(None)

                if not self._chunks:
                    for waiter in self._closing_waiters:
                        if not waiter.done():
                            waiter.set_result(None)

                    self._closing_waiters.clear()

                return data

            if self._cancelled:
                return None

            if self._broken is not None:
                raise self._broken

            if self._error is not None:
                raise self._error

            if self._closed:
                return None

            self._reader_waiter = asyncio.get_running_loop().create_future()
            await self._reader_waiter

    def cancel(self, reason: object = None) -> None:
        if self._cancelled:
            return

        self._cancelled = True
        self._cancel_reason = reason
        self._fail_pending(StreamClosedError("reader cancelled the stream", reason))
        self._wake_reader()

    # ------------------------------------------------------------------ transport teardown

    def break_pipe(self, error: BaseException) -> None:
        """The owning transport went away: pending and future operations fail on both sides."""
        if self._broken is not None:
            return

        self._broken = error
        self._fail_pending(error)
        self._wake_reader()

    # ------------------------------------------------------------------ internals

    def _fail_pending(self, error: BaseException) -> None:
        while self._chunks:
            _, accepted = self._chunks.popleft()

            if not accepted.done():
                accepted.set_exception(error)

        for waiter in self._closing_waiters:
            if not waiter.done():
                waiter.set_exception(error)

        self._closing_waiters.clear()

    def _wake_reader(self) -> None:
        waiter = self._reader_waiter

        if waiter is not None and not waiter.done():
            waiter.set_result(None)

        self._reader_waiter = None


class PushChannel(Generic[T]):
    """An unbounded async queue that can be closed, iterated by the transport's accept loops."""

    def __init__(self) -> None:
        self._items: deque[T] = deque()
        self._waiter: asyncio.Future[None] | None = None
        self._closed = False

    def push(self, item: T) -> None:
        self._items.append(item)
        self._wake()

    def close(self) -> None:
        self._closed = True
        self._wake()

    def _wake(self) -> None:
        if self._waiter is not None and not self._waiter.done():
            self._waiter.set_result(None)

        self._waiter = None

    def __aiter__(self) -> PushChannel[T]:
        return self

    async def __anext__(self) -> T:
        while True:
            if self._items:
                return self._items.popleft()

            if self._closed:
                raise StopAsyncIteration

            self._waiter = asyncio.get_running_loop().create_future()
            await self._waiter
