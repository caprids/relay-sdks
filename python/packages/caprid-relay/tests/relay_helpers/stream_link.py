"""An in-process stream link: the aioquic adapter's send and receive halves with no QUIC underneath.

Satisfies ``_StreamLink`` the way ``QuicStreamSession`` does, but connects a ``_SendHalf`` straight
to the ``_ReceiveHalf`` that models the peer's end of the same stream id. Bytes, FINs and resets cross on
:meth:`StreamLink.transmit`, which the test calls, or which runs on the next loop turn when the link was
built with ``auto=True`` (the way the real link's ``_transmit_soon`` behaves), so a test decides when a
write settles. Like QUIC, a reset discards whatever was still queued on its stream, so no FIN follows it.
A STOP_SENDING is the peer's message to us, not ours to transmit, so it lands on the send half at once.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable

from caprid_relay.quic._errors import QuicTransportError, _stream_error
from caprid_relay.quic._streams import _ReceiveHalf, _SendHalf


class StreamLink:
    def __init__(self, *, auto: bool = False) -> None:
        self.auto = auto
        """Deliver queued operations on the next loop turn instead of waiting for ``transmit()``."""
        self.error: BaseException | None = None
        """Set once the session ended (see ``end``), or set directly to model a session that is closing."""
        self.transmits = 0
        self.resets: list[tuple[int, int]] = []
        """``(stream_id, code)`` of every RESET_STREAM asked for."""
        self.stops: list[tuple[int, int]] = []
        """``(stream_id, code)`` of every STOP_SENDING asked for."""
        self._sends: dict[int, _SendHalf] = {}
        self._receives: dict[int, _ReceiveHalf] = {}
        self._queue: deque[tuple[int, Callable[[], None]]] = deque()
        """``(stream_id, deliver)`` in the order the operations were queued."""
        self._scheduled = False

    def pair(self, stream_id: int = 0) -> tuple[_SendHalf, _ReceiveHalf]:
        """Our send half of ``stream_id`` and the peer's receive half of the same stream."""
        send = _SendHalf(self, stream_id)
        receive = _ReceiveHalf(self, stream_id)
        self._sends[stream_id] = send
        self._receives[stream_id] = receive
        return send, receive

    # ------------------------------------------------------------------ _StreamLink

    def check_live(self) -> None:
        if self.error is not None:
            raise self.error

    async def send(self, half: _SendHalf, data: bytes, end_stream: bool) -> None:
        self.check_live()
        receive = self._receives.get(half.stream_id)
        waiter = half.transmit_waiter()

        def deliver() -> None:
            if receive is not None:
                receive.feed(data, end_stream)

            half.settled()

        self._enqueue(half.stream_id, deliver)
        await asyncio.shield(waiter)

    def reset_stream(self, stream_id: int, code: int) -> None:
        self.resets.append((stream_id, code))
        # Whatever was still queued on the stream is discarded: aioquic stops sending its data and its FIN.
        self._queue = deque(entry for entry in self._queue if entry[0] != stream_id)
        send = self._sends.get(stream_id)
        receive = self._receives.get(stream_id)

        def deliver() -> None:
            if receive is not None:
                receive.fail(_stream_error("reset", code))

            if send is not None:
                # The reset branch of the session's settlement: whatever was still queued will not go out.
                send.fail_transmit(QuicTransportError("stream was aborted"))

        self._enqueue(stream_id, deliver)

    def stop_stream(self, stream_id: int, code: int) -> None:
        self.stops.append((stream_id, code))
        send = self._sends.get(stream_id)

        if send is not None:
            send.stopped(code)

    # ------------------------------------------------------------------ controls

    def transmit(self) -> None:
        """Deliver everything queued so far, in order."""
        self.transmits += 1

        while self._queue:
            _, deliver = self._queue.popleft()
            deliver()

    def end(self, error: BaseException) -> None:
        """The session ended: every half fails now and nothing queued is delivered, like ``_finish``."""
        self.error = error
        self._queue.clear()

        for receive in self._receives.values():
            receive.fail(error)

        for send in self._sends.values():
            send.fail(error)

    def _enqueue(self, stream_id: int, deliver: Callable[[], None]) -> None:
        self._queue.append((stream_id, deliver))

        if self.auto and not self._scheduled:
            self._scheduled = True
            asyncio.get_running_loop().call_soon(self._flush)

    def _flush(self) -> None:
        self._scheduled = False
        self.transmit()
