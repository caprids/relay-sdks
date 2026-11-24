"""A transport whose send streams are acknowledged only when the test says so.

The in-memory pairs report no acknowledgement, so tests wrap the publisher's end in this one: the part of
a real transport that the close drain and a group's SUBGROUP_DELIVERY_TIMEOUT wait on.
"""

import asyncio
from collections.abc import AsyncIterator
from typing import Any

from moq_core.transport import BidirectionalStream, MoqtTransport, SendStream


class AckedSend:
    """A send stream whose acknowledgement waits for the gate; a reset fails it, as on a real transport (TA-16)."""

    def __init__(self, inner: SendStream, gate: "AckGate") -> None:
        self._inner = inner
        self._gate = gate
        self.reset_codes: list[int] = []
        """The stream_error_code of every abort, in order."""
        self._reset: asyncio.Future[None] = asyncio.get_running_loop().create_future()

    async def write(self, data: bytes) -> None:
        await self._inner.write(data)

    async def close(self) -> None:
        await self._inner.close()

    def abort(self, reason: object = None) -> None:
        self.reset_codes.append(getattr(reason, "stream_error_code", 0))
        self._inner.abort(reason)

        if not self._reset.done():
            self._reset.set_exception(ConnectionResetError("stream was reset"))
            self._reset.exception()

    async def wait_acknowledged(self) -> None:
        self._gate.waiting += 1
        await asyncio.wait({self._gate.released, self._reset}, return_when=asyncio.FIRST_COMPLETED)

        if self._reset.done():
            self._reset.result()


class AckGate:
    """Holds every send stream's acknowledgement until :meth:`release`."""

    def __init__(self) -> None:
        self.released: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self.waiting = 0
        """Acknowledgements asked for so far."""

    def release(self) -> None:
        if not self.released.done():
            self.released.set_result(None)


class AckedTransport:
    """A transport whose send streams satisfy ``AcknowledgedSendStream``; records when it is closed."""

    def __init__(self, inner: MoqtTransport, gate: AckGate, request_gate: AckGate | None = None) -> None:
        self._inner = inner
        self._gate = gate
        self._request_gate = request_gate if request_gate is not None else gate
        """Gates the request streams' acknowledgements (PUBLISH_DONE and its FIN); the data streams' gate by default."""
        self.closed_at: float | None = None
        self.uni_streams: list[AckedSend] = []
        """The publisher's unidirectional streams, in the order it opened them: the control stream first."""

    async def _wrap_incoming(self) -> AsyncIterator[BidirectionalStream]:
        async for stream in self._inner.incoming_bidirectional_streams():
            yield BidirectionalStream(readable=stream.readable, writable=AckedSend(stream.writable, self._request_gate))

    def incoming_unidirectional_streams(self) -> Any:
        return self._inner.incoming_unidirectional_streams()

    def incoming_bidirectional_streams(self) -> AsyncIterator[BidirectionalStream]:
        return self._wrap_incoming()

    async def create_unidirectional_stream(self) -> SendStream:
        stream = AckedSend(await self._inner.create_unidirectional_stream(), self._gate)
        self.uni_streams.append(stream)
        return stream

    @property
    def data_streams(self) -> list[AckedSend]:
        """The publisher's data streams, in the order it opened them."""
        return self.uni_streams[1:]

    async def create_bidirectional_stream(self) -> BidirectionalStream:
        stream = await self._inner.create_bidirectional_stream()
        return BidirectionalStream(readable=stream.readable, writable=AckedSend(stream.writable, self._request_gate))

    def close(self, close_code: int = 0, reason: str = "") -> None:
        if self.closed_at is None:
            self.closed_at = asyncio.get_running_loop().time()
        self._inner.close(close_code, reason)

    async def wait_closed(self) -> None:
        await self._inner.wait_closed()
