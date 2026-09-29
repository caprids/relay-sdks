"""Per-request bidirectional-stream lifecycle (moq-transport draft-20 §3.3).

The control model is request-stream based: each request (SUBSCRIBE, FETCH, TRACK_STATUS, ...) opens
its own bidirectional stream, the first message on it is the request, and responses (SUBSCRIBE_OK /
REQUEST_OK / REQUEST_ERROR / PUBLISH_DONE / GOAWAY) arrive as control messages on the same stream.
There is no UNSUBSCRIBE message: cancellation IS the stream lifecycle. Abort our sending direction
(RESET_STREAM) and cancel the receiving direction (STOP_SENDING), per §3.3.3.

Callback-shaped: the session driver binds this to its own bookkeeping.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass

from ._tasks import spawn
from .control_messages import ControlMessage, ControlMessageDeframer, decode_control_message
from .errors import MoqtProtocolError
from .transport import BidirectionalStream


@dataclass(slots=True, kw_only=True)
class RequestStreamHandlers:
    on_message: Callable[[ControlMessage], None]
    on_fin: Callable[[], None] | None = None
    """The peer FINed its direction.

    Per §3.3.2 this signals the request is complete; an endpoint that receives a FIN before all
    required messages treats the request as failed. That judgement is the caller's (it knows which
    messages arrived).
    """
    on_error: Callable[[BaseException], None] | None = None
    """Read failure, stream reset, or a decode error (:class:`MoqtProtocolError`)."""


class RequestStream:
    """One open request.

    The encoded request has been queued, and responses pump to the handlers until FIN, reset, or
    cancellation.
    """

    __slots__ = ("_cancelled", "_finished", "_first_write", "_handlers", "_stream")

    def __init__(self, stream: BidirectionalStream, request_message: bytes, handlers: RequestStreamHandlers) -> None:
        self._stream = stream
        self._handlers = handlers
        self._cancelled = False
        self._finished = False
        self._first_write: asyncio.Task[None] = spawn(self._write_first(request_message))
        spawn(self._read_loop())

    async def _write_first(self, request_message: bytes) -> None:
        try:
            await self._stream.writable.write(request_message)
        except Exception as error:  # any write failure is the request's failure
            if not self._cancelled and self._handlers.on_error is not None:
                self._handlers.on_error(error)

    async def _read_loop(self) -> None:
        deframer = ControlMessageDeframer()
        readable = self._stream.readable

        try:
            while True:
                chunk = await readable.read()
                if chunk is None:
                    break

                for frame in deframer.push(chunk):
                    self._handlers.on_message(decode_control_message(frame))

            if not self._cancelled:
                # A FIN that lands mid-frame is a truncated response, not a clean completion:
                # surfacing it as FIN would leave the request pending with neither response nor
                # error.
                if deframer.pending_bytes > 0:
                    if self._handlers.on_error is not None:
                        self._handlers.on_error(MoqtProtocolError("request stream ended mid-control-message"))
                elif self._handlers.on_fin is not None:
                    self._handlers.on_fin()
        except Exception as error:  # a reset or decode failure ends the request
            if not self._cancelled and self._handlers.on_error is not None:
                self._handlers.on_error(error)

    async def send(self, data: bytes) -> None:
        """Send a follow-up message (e.g. REQUEST_UPDATE) on the request stream.

        Nothing is written once our direction has ended (FIN or cancellation).
        """
        await self._first_write
        if self._finished:
            return

        await self._stream.writable.write(data)

    async def fin_write(self) -> None:
        """Gracefully close our sending direction (FIN). Not a cancellation."""
        if self._finished or self._cancelled:
            return

        self._finished = True
        await self._first_write
        await self._stream.writable.close()

    def cancel(self, reason: object = None) -> None:
        """Cancel the request (§3.3.3): abort the sending direction and stop reading the receiving direction."""
        if self._cancelled:
            return

        self._cancelled = True

        if not self._finished:
            self._finished = True
            self._stream.writable.abort(reason)

        # STOP_SENDING on the receiving direction. The read loop's pending read ends; `_cancelled`
        # suppresses the FIN callback.
        self._stream.readable.cancel(reason)

    @property
    def cancelled(self) -> bool:
        return self._cancelled


def open_request_stream(
    stream: BidirectionalStream, request_message: bytes, handlers: RequestStreamHandlers
) -> RequestStream:
    """Open a request over an established bidirectional stream.

    Writes the encoded request message, then pumps incoming control messages to
    ``handlers.on_message`` until FIN, reset, or cancellation.
    """
    return RequestStream(stream, request_message, handlers)


__all__ = ["RequestStream", "RequestStreamHandlers", "open_request_stream"]
