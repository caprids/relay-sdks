"""In-memory transport fake for driving the subscribe-side session from a scripted "server".

Derived from the Video.js MOQT session tests (Apache-2.0); see ``packages/moq-core/NOTICE``.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator
from dataclasses import dataclass

from moq_core.bytes import ByteWriter, StreamReader
from moq_core.control_messages import (
    ControlMessage,
    ControlMessageDeframer,
    decode_control_message,
    encode_setup,
)
from moq_core.transport import BidirectionalStream, ReceiveStream, SendStream

from .streams import Pipe, PushChannel, StreamClosedError
from .transport_pair import CloseInfo
from .waiting import wait_for


@dataclass(slots=True)
class ServerRequestStream:
    """Server-side view of one client-initiated bidirectional stream."""

    messages: list[ControlMessage]
    """Decoded control messages received from the client, in order."""
    first_message: asyncio.Future[ControlMessage]
    """Resolves when the first client message has been decoded."""
    _writer: Pipe

    async def send(self, data: bytes) -> None:
        await self._writer.write(data)

    async def fin(self) -> None:
        await self._writer.close()

    def reset(self, reason: object = None) -> None:
        self._writer.abort(reason)


@dataclass(slots=True)
class ControlledDataStream:
    """A unidirectional data stream the test writes to (and may reset)."""

    _writer: Pipe

    async def write(self, data: bytes) -> None:
        await self._writer.write(data)

    def reset(self, error: object = None) -> None:
        self._writer.abort(error)


class FakeTransport:
    """The client's ``MoqtTransport``; the same object exposes the server-side controls."""

    def __init__(self) -> None:
        loop = asyncio.get_running_loop()
        self._incoming_uni: PushChannel[ReceiveStream] = PushChannel()
        self._incoming_bidi: PushChannel[BidirectionalStream] = PushChannel()
        self.client_uni_streams: list[Pipe] = []
        self._request_streams: deque[ServerRequestStream] = deque()
        self._request_waiters: deque[asyncio.Future[ServerRequestStream]] = deque()
        self._pipes: list[Pipe] = []
        self.close_info: CloseInfo | None = None
        self._closed: asyncio.Future[None] = loop.create_future()
        self._pumps: list[asyncio.Task[None]] = []
        self.close_calls = 0
        self.release_after_turns = 0
        """Extra loop turns ``wait_closed`` takes after the drop, like the native transport's socket release."""

    @property
    def ended(self) -> bool:
        """True once the transport has ended, before ``wait_closed`` returns, like the native transport."""
        return self._closed.done()

    # ------------------------------------------------------------------ MoqtTransport

    def incoming_unidirectional_streams(self) -> AsyncIterator[ReceiveStream]:
        return self._incoming_uni

    def incoming_bidirectional_streams(self) -> AsyncIterator[BidirectionalStream]:
        return self._incoming_bidi

    def _pipe(self) -> Pipe:
        pipe = Pipe()
        self._pipes.append(pipe)
        return pipe

    async def create_unidirectional_stream(self) -> SendStream:
        pipe = self._pipe()
        self.client_uni_streams.append(pipe)
        return pipe

    async def create_bidirectional_stream(self) -> BidirectionalStream:
        client_to_server = self._pipe()
        server_to_client = self._pipe()
        messages: list[ControlMessage] = []
        first_message: asyncio.Future[ControlMessage] = asyncio.get_running_loop().create_future()

        async def pump() -> None:
            deframer = ControlMessageDeframer()

            try:
                while True:
                    chunk = await client_to_server.read()
                    if chunk is None:
                        break

                    for frame in deframer.push(chunk):
                        message = decode_control_message(frame)
                        messages.append(message)

                        if len(messages) == 1 and not first_message.done():
                            first_message.set_result(message)
            except Exception:  # client reset its sending direction
                pass

        task = asyncio.get_running_loop().create_task(pump())
        self._pumps.append(task)

        server_stream = ServerRequestStream(messages, first_message, server_to_client)

        if self._request_waiters:
            self._request_waiters.popleft().set_result(server_stream)
        else:
            self._request_streams.append(server_stream)

        return BidirectionalStream(readable=server_to_client, writable=client_to_server)

    def close(self, close_code: int = 0, reason: str = "") -> None:
        self.close_calls += 1
        if self.close_info is None:
            self.close_info = CloseInfo(close_code, reason)
        self._finish()

    async def wait_closed(self) -> None:
        await self._closed
        for _ in range(self.release_after_turns):
            await asyncio.sleep(0)

    # ------------------------------------------------------------------ server-side controls

    async def client_control_stream(self) -> StreamReader:
        """The SETUP control stream the client opened (first client uni stream)."""
        await wait_for(lambda: len(self.client_uni_streams) > 0)
        return StreamReader(self.client_uni_streams[0])

    async def next_request_stream(self) -> ServerRequestStream:
        """Wait for the next client-initiated request stream."""
        if self._request_streams:
            return self._request_streams.popleft()

        waiter: asyncio.Future[ServerRequestStream] = asyncio.get_running_loop().create_future()
        self._request_waiters.append(waiter)
        return await waiter

    def send_server_setup(self) -> None:
        """Open the server's control stream and send its SETUP."""
        pipe = self._pipe()
        self._incoming_uni.push(pipe)
        asyncio.get_running_loop().create_task(self._write_quietly(pipe, encode_setup([])))

    def open_data_stream(self, data: bytes) -> None:
        """Open a unidirectional data stream to the client carrying ``data`` then FIN."""
        pipe = self._pipe()
        self._incoming_uni.push(pipe)

        async def write_and_close() -> None:
            await self._write_quietly(pipe, data)
            try:
                await pipe.close()
            except Exception:
                pass

        asyncio.get_running_loop().create_task(write_and_close())

    def open_controlled_data_stream(self) -> ControlledDataStream:
        pipe = self._pipe()
        self._incoming_uni.push(pipe)
        return ControlledDataStream(pipe)

    def open_incoming_bidi(self, data: bytes) -> asyncio.Future[list[bytes]]:
        """Open a server-initiated bidirectional stream carrying ``data`` then FIN.

        Resolves with the chunks the client wrote back once the client FINs its side.
        """
        server_to_client = self._pipe()
        client_to_server = self._pipe()
        self._incoming_bidi.push(BidirectionalStream(readable=server_to_client, writable=client_to_server))

        async def write_and_close() -> None:
            await self._write_quietly(server_to_client, data)
            try:
                await server_to_client.close()
            except Exception:
                pass

        asyncio.get_running_loop().create_task(write_and_close())

        async def collect() -> list[bytes]:
            chunks: list[bytes] = []

            while True:
                chunk = await client_to_server.read()
                if chunk is None:
                    return chunks

                chunks.append(chunk)

        return asyncio.get_running_loop().create_task(collect())

    def drop_transport(self) -> None:
        """Simulate a transport-side drop (peer/network close, not a local ``close()``)."""
        self._finish()

    def _finish(self) -> None:
        if not self._closed.done():
            self._closed.set_result(None)

        self._incoming_uni.close()
        self._incoming_bidi.close()

        for pipe in self._pipes:
            pipe.break_pipe(StreamClosedError("transport closed"))

    @staticmethod
    async def _write_quietly(pipe: Pipe, data: bytes) -> None:
        try:
            await pipe.write(data)
        except Exception:
            pass


def create_fake_transport() -> FakeTransport:
    return FakeTransport()


def encode_subgroup(track_alias: int, group_id: int, payload: bytes, object_id: int = 0) -> bytes:
    """Encode a one-object subgroup stream for ``track_alias``.

    Type 0x38: subgroup 0, default priority, end-of-group.
    """
    writer = ByteWriter()
    writer.write_varint(0x38)
    writer.write_varint(track_alias)
    writer.write_varint(group_id)
    writer.write_varint(object_id)  # first object id (absolute)
    writer.write_varint(len(payload))
    writer.write_bytes(payload)
    return writer.to_bytes()
