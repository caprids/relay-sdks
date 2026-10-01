"""What both ends of a WebTransport session share: stream creation, event routing, settlement, closure.

The session base is the production :class:`~._streams._StreamLink`: the halves ask it to queue bytes,
reset and stop streams, and it settles their transmissions after every transmit of the connection.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from aioquic.h3.connection import ErrorCode
from aioquic.h3.events import HeadersReceived, WebTransportStreamDataReceived
from aioquic.quic import events as quic_events
from aioquic.quic.connection import stream_is_client_initiated, stream_is_unidirectional
from aioquic.quic.packet import QuicErrorCode
from aioquic.quic.stream import QuicStream

from moq_core._tasks import observe, spawn
from moq_core.transport import BidirectionalStream, ReceiveStream, SendStream

from ._aioquic import (
    acknowledged,
    finish_receiving_half,
    forget_h3_stream,
    live_stream,
    mark_webtransport_stream,
    reset_pending,
    transmitted,
)
from ._errors import WebTransportError
from ._protocol import WebTransportProtocol
from ._streams import _ReceiveHalf, _SendHalf, _StreamQueue
from ._wire import (
    CAPSULE_CLOSE_WEBTRANSPORT_SESSION,
    _decode_uint_var,
    encode_close_session_capsule,
    h3_to_webtransport_error_code,
    webtransport_to_h3_error_code,
)

_CLOSE_DRAIN_TIMEOUT = 5.0
"""Seconds ``close()`` waits for the peer to acknowledge what is still in flight before the connection goes."""


class WebTransportSessionBase:
    """Stream plumbing for one WebTransport session on a :class:`WebTransportProtocol`."""

    def __init__(self) -> None:
        loop = asyncio.get_running_loop()
        self._protocol: WebTransportProtocol | None = None
        self.session_id: int = -1
        self._receives: dict[int, _ReceiveHalf] = {}
        self._sends: dict[int, _SendHalf] = {}
        self._incoming_uni = _StreamQueue()
        self._incoming_bidi = _StreamQueue()
        self._closed: asyncio.Future[None] = loop.create_future()
        observe(self._closed)
        self._closing = False
        self._drained: asyncio.Future[None] | None = None
        self._connect_buffer = b""
        self.close_info: tuple[int, str] | None = None
        """``(code, reason)`` from the peer's CLOSE_WEBTRANSPORT_SESSION capsule, once received."""

    # ------------------------------------------------------------------ MoqtTransport

    def incoming_unidirectional_streams(self) -> AsyncIterator[ReceiveStream]:
        return self._incoming_uni

    def incoming_bidirectional_streams(self) -> AsyncIterator[BidirectionalStream]:
        return self._incoming_bidi

    async def create_unidirectional_stream(self) -> SendStream:
        protocol = self._require_live()
        stream_id = protocol.h3.create_webtransport_stream(self.session_id, is_unidirectional=True)
        finish_receiving_half(protocol._quic, stream_id)
        send = _SendHalf(self, stream_id)
        self._sends[stream_id] = send
        protocol.stream_owners[stream_id] = self
        protocol.transmit()
        return send

    async def create_bidirectional_stream(self) -> BidirectionalStream:
        protocol = self._require_live()
        stream_id = protocol.h3.create_webtransport_stream(self.session_id)
        mark_webtransport_stream(protocol.h3, stream_id, self.session_id)
        send = _SendHalf(self, stream_id)
        receive = _ReceiveHalf(self, stream_id)
        self._sends[stream_id] = send
        self._receives[stream_id] = receive
        protocol.stream_owners[stream_id] = self
        protocol.transmit()
        return BidirectionalStream(readable=receive, writable=send)

    def close(self, close_code: int = 0, reason: str = "") -> None:
        """Wait for in-flight data to be acknowledged, send CLOSE_WEBTRANSPORT_SESSION, then close the connection.

        Writes fail from here on; the session reports itself closed once the connection is gone.
        """
        if self._closing:
            return

        self._closing = True
        protocol = self._protocol

        if protocol is None or protocol.terminated is not None:
            self._finish(None)
            return

        spawn(self._drain_then_close(protocol, close_code, reason))

    @property
    def ended(self) -> bool:
        """True once the session has ended, before its socket is released and ``wait_closed`` returns.

        The streams fail in the same turn the session ends, so a stream failure seen while this is
        true is the closure's doing.
        """
        return self._closed.done()

    async def wait_closed(self) -> None:
        """Return once the session ended and its UDP socket is released, so a process may exit right after.

        Raises the error that ended the session, if any.
        """
        if not self._closed.done():
            await asyncio.wait({self._closed})

        protocol = self._transport_protocol()

        if protocol is not None:
            await protocol.transport_closed.wait()

        self._closed.result()

    # ------------------------------------------------------------------ role hooks

    def _transport_protocol(self) -> WebTransportProtocol | None:
        """The connection whose shutdown ``wait_closed`` also waits for."""
        return self._protocol

    def _attach(self, protocol: WebTransportProtocol, session_id: int) -> None:
        self._protocol = protocol
        self.session_id = session_id
        protocol.register_session(self)

    def _on_connect_headers(self, event: HeadersReceived) -> None:
        """Headers on the CONNECT stream after establishment (trailers) carry nothing this session uses."""

    def _on_connect_data(self, data: bytes, stream_ended: bool) -> None:
        """Capsules ride the CONNECT stream's body."""
        self._connect_buffer += data
        offset = 0

        while True:
            header = _decode_uint_var(self._connect_buffer, offset)
            if header is None:
                break

            capsule_type, after_type = header
            length_field = _decode_uint_var(self._connect_buffer, after_type)
            if length_field is None:
                break

            length, payload_start = length_field
            if payload_start + length > len(self._connect_buffer):
                break

            payload = self._connect_buffer[payload_start : payload_start + length]
            offset = payload_start + length

            if capsule_type == CAPSULE_CLOSE_WEBTRANSPORT_SESSION:
                code = int.from_bytes(payload[:4], "big") if len(payload) >= 4 else 0
                self.close_info = (code, payload[4:].decode("utf-8", errors="replace"))
                self._peer_closed_session()
                return

        self._connect_buffer = self._connect_buffer[offset:]

        if stream_ended:
            self._peer_closed_session()

    def _peer_closed_session(self) -> None:
        """The peer ended the session: finish, then close the connection ourselves.

        A FIN without a capsule, or a capsule with code 0, is a clean end. A nonzero code is the
        peer's reason for ending the session (moq-relay refuses credentials with 0x2, "unauthorized"),
        so the session fails with it instead of reporting a clean close.
        """
        protocol = self._protocol
        code, reason = self.close_info or (0, "")
        self._finish(
            None
            if code == 0
            else WebTransportError(
                f"peer closed the WebTransport session: {reason or 'no reason given'} (code 0x{code:x})",
                code=code,
                reason=reason,
                peer_closed=True,
            )
        )

        if protocol is not None and protocol.terminated is None and not self._closing:
            self._closing = True
            protocol.close_after_drain()

    async def _drain_then_close(self, protocol: WebTransportProtocol, close_code: int, reason: str) -> None:
        # Closing a QUIC connection discards queued stream data, and whatever the peer has not
        # acknowledged is lost with it. So wait until every send stream has been acknowledged, then
        # send the close capsule and wait for that too, all within one bound so a peer that went
        # silent cannot hold the close. The capsule goes last because the peer ends the session the
        # moment it reads it and resets every stream of it: a stream FIN that was lost and is still
        # being retransmitted would arrive after the capsule and be discarded (#209).
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _CLOSE_DRAIN_TIMEOUT

        try:
            await asyncio.wait({self._wait_drained(protocol)}, timeout=_CLOSE_DRAIN_TIMEOUT)

            if self._closed.done() or protocol.terminated is not None:
                return

            try:
                capsule = encode_close_session_capsule(close_code, reason)
                protocol.h3.send_data(self.session_id, capsule, end_stream=True)
                protocol.transmit()
            except Exception:
                return

            await asyncio.wait({self._wait_drained(protocol)}, timeout=max(0.0, deadline - loop.time()))
        finally:
            if protocol.terminated is None:
                protocol.close_after_drain()

    def _wait_drained(self, protocol: WebTransportProtocol) -> asyncio.Future[None]:
        """A future settled once everything sent so far is acknowledged; a new one once the last has settled."""
        if self._drained is None or self._drained.done():
            self._drained = asyncio.get_running_loop().create_future()
            observe(self._drained)
            protocol.watch_drain(self)
            self._check_drained()

        return self._drained

    def _check_drained(self) -> bool:
        """After a transmit while closing: settle ``_drained`` once the peer acknowledged everything we sent.

        Returns ``True`` when there is nothing left to watch.
        """
        drained = self._drained

        if drained is None or drained.done():
            return True

        protocol = self._protocol

        if protocol is None or protocol.terminated is not None:
            drained.set_result(None)
            return True

        for stream_id in (*self._sends, self.session_id):
            stream = live_stream(protocol._quic, stream_id)

            if stream is not None and not acknowledged(stream):
                return False  # data, the FIN or a reset is not acknowledged yet

        drained.set_result(None)
        return True

    # ------------------------------------------------------------------ event routing

    def _on_stream_data(self, event: WebTransportStreamDataReceived) -> None:
        receive = self._receives.get(event.stream_id)

        if receive is None:
            protocol = self._protocol
            if protocol is None or self._closing:
                return

            # A peer-initiated stream. Its ID parity tells the direction.
            is_client = protocol._quic.configuration.is_client

            if stream_is_client_initiated(event.stream_id) == is_client:
                return  # Our own unidirectional stream never carries inbound data; ignore stray events.

            receive = _ReceiveHalf(self, event.stream_id)
            self._receives[event.stream_id] = receive
            protocol.stream_owners[event.stream_id] = self

            if stream_is_unidirectional(event.stream_id):
                self._incoming_uni.push(receive)
            else:
                send = _SendHalf(self, event.stream_id)
                self._sends[event.stream_id] = send
                self._incoming_bidi.push(BidirectionalStream(readable=receive, writable=send))

        # A half that already ended ignores this (a retransmitted FIN reports end-of-stream again).
        receive.feed(event.data, event.stream_ended)

        if event.stream_ended:
            # Keep the entries until aioquic discards the stream, so a retransmitted FIN still finds
            # this half instead of looking like a new incoming stream; the watch does the forgetting.
            self._watch(event.stream_id)

    def _on_stream_reset(self, stream_id: int, error_code: int) -> None:
        receive = self._receives.get(stream_id)

        if receive is not None:
            code = h3_to_webtransport_error_code(error_code)
            receive.fail(WebTransportError(f"peer reset the stream (code {code})", code=code))
            self._watch(stream_id)

    def _on_stop_sending(self, stream_id: int, error_code: int) -> None:
        send = self._sends.get(stream_id)

        if send is not None:
            send.stopped(h3_to_webtransport_error_code(error_code) or 0)
            # aioquic resets the stream on our behalf; forget it once that reset is acknowledged.
            self._watch(stream_id)

    def _watch(self, stream_id: int) -> None:
        if self._protocol is not None:
            self._protocol.watch_stream(stream_id, self)

    # ------------------------------------------------------------------ per-stream bookkeeping

    def _settle_stream(self, stream_id: int, stream: QuicStream | None) -> bool:
        """After a transmit, for a watched stream: ``True`` while it still needs watching.

        A write and the FIN both settle once their bytes have been handed to the wire (nothing left
        pending on the stream), so a caller is not held for a full round trip on a healthy link. A
        closed send half stays watched until aioquic discards the stream, though: that discard is how
        the session learns the FIN was acknowledged, tells the half (which is what a settling group's
        acknowledgement waits on), and the per-stream state can go. (The dropped-FIN
        retry in ``_fin_retry`` watches those FINs on its own and re-queues one aioquic consumed without
        sending; ``close()`` on the session waits for the acknowledgement before tearing the connection down.)

        Per-stream state is dropped only once aioquic itself has discarded the stream (both halves
        done and the FIN or reset acknowledged). Until then a retransmitted FIN still finds the
        receive half and is ignored, instead of being taken for a new incoming stream.
        """
        send = self._sends.get(stream_id)
        receive = self._receives.get(stream_id)

        if stream is None:
            # aioquic discarded the stream: nothing more can arrive or be sent on it. For a closed
            # send half that is success, the peer has the whole stream.
            if send is not None:
                if send.closed:
                    send.settled()
                else:
                    send.fail_transmit(WebTransportError("stream is closed"))

                send.finished()

            self._forget_stream(stream_id)
            return False

        if send is None:
            return receive is not None and receive.ended  # until aioquic discards the finished stream

        if stream.sender.is_finished:
            if send.closed:
                send.settled()
            else:
                send.fail_transmit(WebTransportError("stream is closed"))

            # The FIN (or a reset) is acknowledged: a settling group settles here (see CONTEXT.md).
            send.finished()

            # Watch on only while the discard is imminent; a bidirectional stream whose receive half
            # is still live re-registers itself when that half ends.
            return receive is None or receive.ended

        if reset_pending(stream):
            send.fail_transmit(WebTransportError("stream was aborted"))
            return True  # until the reset is acknowledged

        if transmitted(stream):
            send.settled()

        # A closed send half stays watched until aioquic discards the stream (see above), even after
        # the write that queued its FIN has settled.
        return send.closed or send.pending

    def _forget_stream(self, stream_id: int) -> None:
        """aioquic discarded the stream: drop both halves, the routing entry and the HTTP/3 record it leaves behind."""
        self._sends.pop(stream_id, None)
        self._receives.pop(stream_id, None)
        protocol = self._protocol

        if protocol is None or stream_id == self.session_id:
            return

        protocol.stream_owners.pop(stream_id, None)
        forget_h3_stream(protocol.h3, stream_id)

    def _on_terminated(self, event: quic_events.ConnectionTerminated) -> None:
        clean = self._closing or event.error_code in (QuicErrorCode.NO_ERROR, ErrorCode.H3_NO_ERROR)
        error = (
            None
            if clean
            else WebTransportError(
                f"QUIC connection terminated: {event.reason_phrase or 'no reason given'} (code 0x{event.error_code:x})",
                code=event.error_code,
                reason=event.reason_phrase,
            )
        )
        self._finish(error)

    def _finish(self, error: BaseException | None) -> None:
        if self._closed.done():
            return

        if error is None:
            self._closed.set_result(None)
        else:
            self._closed.set_exception(error)

        stream_error = error if error is not None else WebTransportError("WebTransport session closed")

        for receive in self._receives.values():
            receive.fail(stream_error)

        for send in self._sends.values():
            send.fail(stream_error)

        self._incoming_uni.close()
        self._incoming_bidi.close()

        if self._drained is not None and not self._drained.done():
            self._drained.set_result(None)

    # ------------------------------------------------------------------ _StreamLink: QUIC plumbing for the halves

    def _require_live(self) -> WebTransportProtocol:
        self.check_live()
        assert self._protocol is not None
        return self._protocol

    def check_live(self) -> None:
        if self._protocol is None:
            raise WebTransportError("WebTransport session is not established")

        if self._closed.done():
            error = self._closed.exception() if not self._closed.cancelled() else None
            raise error if error is not None else WebTransportError("WebTransport session closed")

        if self._closing:
            raise WebTransportError("WebTransport session closed")

    async def send(self, half: _SendHalf, data: bytes, end_stream: bool) -> None:
        protocol = self._require_live()
        protocol._quic.send_stream_data(half.stream_id, data, end_stream)
        waiter = half.transmit_waiter()
        protocol.watch_stream(half.stream_id, self)

        if end_stream:
            protocol.fin_retry.watch(half.stream_id)

        # Coalesce concurrent writes into one transmit per loop turn, the way aioquic's own stream
        # adapter does. Transmitting per write instead packs packets so tightly that a following
        # FIN-only frame can be dropped (aioquic consumes it even when the packet is already full).
        protocol._transmit_soon()
        # Shielded: a caller that stops waiting (a deadline, a cancelled task) must not cancel the
        # future that concurrent and later writers on this stream share. Its bytes stay queued and
        # go out with the rest, the documented "a failed write may have delivered partial data".
        await asyncio.shield(waiter)

    def reset_stream(self, stream_id: int, code: int) -> None:
        protocol = self._protocol

        if protocol is None or protocol.terminated is not None:
            return

        # A stream aioquic already discarded must not be recreated by the reset, and one whose FIN
        # (or an earlier reset) is acknowledged is past "Data Recvd": nothing is left to reset.
        stream = live_stream(protocol._quic, stream_id)

        if stream is None or stream.sender.is_finished:
            return

        try:
            protocol._quic.reset_stream(stream_id, webtransport_to_h3_error_code(code))
            protocol.watch_stream(stream_id, self)
            protocol.transmit()
        except (ValueError, AssertionError):
            pass

    def stop_stream(self, stream_id: int, code: int) -> None:
        protocol = self._protocol

        if protocol is None or protocol.terminated is not None:
            return

        try:
            protocol._quic.stop_stream(stream_id, webtransport_to_h3_error_code(code))
            protocol.transmit()
        except (ValueError, AssertionError):
            pass
