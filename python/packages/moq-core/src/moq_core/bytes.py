"""Byte-level readers and writers shared by the MOQT wire codecs.

:class:`ByteReader` and :class:`ByteWriter` are synchronous cursors over in-memory buffers, used for
control-message bodies (always fully buffered: the 16-bit message length bounds them).
:class:`StreamReader` is the asynchronous counterpart for unidirectional data streams, where headers
and objects arrive incrementally over a transport :class:`~moq_core.transport.ReceiveStream`.
"""

from __future__ import annotations

from collections import deque

from .errors import IncompleteInputError
from .transport import ReceiveStream
from .varint import MAX_VARINT_VALUE, decode_varint, encode_varint, varint_length_from_first_byte


def utf8_encode(value: str) -> bytes:
    return value.encode("utf-8")


def utf8_decode(data: bytes | bytearray | memoryview) -> str:
    # Lenient like TextDecoder's default: invalid sequences become U+FFFD instead of failing.
    return bytes(data).decode("utf-8", errors="replace")


class ByteReader:
    """Synchronous cursor over a byte buffer. Every read past the end raises :class:`IncompleteInputError`."""

    __slots__ = ("_data", "_offset")

    def __init__(self, data: bytes | bytearray | memoryview, offset: int = 0) -> None:
        self._data = bytes(data)
        self._offset = offset

    @property
    def offset(self) -> int:
        return self._offset

    @property
    def remaining(self) -> int:
        return len(self._data) - self._offset

    def read_varint(self) -> int:
        value, byte_length = decode_varint(self._data, self._offset)
        self._offset += byte_length
        return value

    def read_count_varint(self) -> int | None:
        """Read a count whose "unknown" sentinel is the maximal varint 2^64-1 (PUBLISH_DONE's Stream Count, §10.12).

        Exactly that sentinel reads as ``None``; every other value is returned as is.
        """
        value = self.read_varint()
        return None if value == MAX_VARINT_VALUE else value

    def read_uint8(self) -> int:
        if self.remaining < 1:
            raise IncompleteInputError("read past end of buffer")

        value = self._data[self._offset]
        self._offset += 1
        return value

    def read_uint16(self) -> int:
        if self.remaining < 2:
            raise IncompleteInputError("read past end of buffer")

        value = (self._data[self._offset] << 8) | self._data[self._offset + 1]
        self._offset += 2
        return value

    def read_bytes(self, length: int) -> bytes:
        if length < 0 or self.remaining < length:
            raise IncompleteInputError("read past end of buffer")

        view = self._data[self._offset : self._offset + length]
        self._offset += length
        return view


class ByteWriter:
    """Growable byte sink for encoding wire structures."""

    __slots__ = ("_buffer",)

    def __init__(self, initial_capacity: int = 0) -> None:
        del initial_capacity  # bytearray grows on its own; the parameter keeps call sites symmetric with the TS core
        self._buffer = bytearray()

    @property
    def length(self) -> int:
        return len(self._buffer)

    def write_varint(self, value: int) -> None:
        self._buffer += encode_varint(value)

    def write_uint8(self, value: int) -> None:
        self._buffer.append(value & 0xFF)

    def write_uint16(self, value: int) -> None:
        self._buffer += bytes(((value >> 8) & 0xFF, value & 0xFF))

    def write_bytes(self, data: bytes | bytearray | memoryview) -> None:
        self._buffer += data

    def write_length_prefixed(self, data: bytes | bytearray | memoryview) -> None:
        """Varint length prefix followed by the bytes."""
        self.write_varint(len(data))
        self.write_bytes(data)

    def to_bytes(self) -> bytes:
        """A copy of the written bytes."""
        return bytes(self._buffer)


class StreamReader:
    """Buffered asynchronous reader over a transport :class:`ReceiveStream`.

    Reads never split a varint or byte run: each method pulls chunks until it can satisfy the
    request. A clean end-of-stream mid-read raises :class:`IncompleteInputError`; use
    :meth:`at_end` to probe for a clean boundary first. A reset stream raises whatever the transport
    raises.
    """

    __slots__ = ("_buffered", "_chunks", "_done", "_stream")

    def __init__(self, stream: ReceiveStream) -> None:
        self._stream = stream
        self._chunks: deque[bytes] = deque()
        self._buffered = 0
        self._done = False

    async def _pull(self) -> bool:
        if self._done:
            return False

        chunk = await self._stream.read()

        if chunk is None:
            self._done = True
            return False

        if chunk:
            self._chunks.append(bytes(chunk))
            self._buffered += len(chunk)

        return True

    async def _fill(self, length: int) -> None:
        while self._buffered < length:
            if not await self._pull():
                raise IncompleteInputError("unexpected end of stream")

    def _take(self, length: int) -> bytes:
        out = bytearray()

        while len(out) < length:
            head = self._chunks[0]
            take = min(len(head), length - len(out))
            out += head[:take]

            if take == len(head):
                self._chunks.popleft()
            else:
                self._chunks[0] = head[take:]

        self._buffered -= length
        return bytes(out)

    async def at_end(self) -> bool:
        """True when the stream ended cleanly with no bytes left unread."""
        while self._buffered == 0:
            if not await self._pull():
                return True

        return False

    async def read_bytes(self, length: int) -> bytes:
        await self._fill(length)
        return self._take(length)

    async def read_uint8(self) -> int:
        await self._fill(1)
        return self._take(1)[0]

    async def read_varint(self) -> int:
        await self._fill(1)
        byte_length = varint_length_from_first_byte(self._chunks[0][0])
        await self._fill(byte_length)
        return decode_varint(self._take(byte_length))[0]

    def cancel(self, reason: object = None) -> None:
        """Cancel the underlying stream (drop an unwanted data stream)."""
        self._done = True
        self._stream.cancel(reason)


__all__ = ["ByteReader", "ByteWriter", "StreamReader", "utf8_decode", "utf8_encode"]
