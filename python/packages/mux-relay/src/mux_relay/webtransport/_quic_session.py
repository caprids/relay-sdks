"""What every session on a :class:`~._quic_protocol.QuicProtocol` shares: the halves' link, settlement, the drain.

The session base is the production :class:`~._streams._StreamLink`: the halves ask it to queue bytes,
reset and stop streams, and it settles their transmissions after every transmit of the connection.
:class:`~._session.WebTransportSessionBase` adds a WebTransport session's streams, capsules and error-code
mapping; ``mux_relay.quic`` puts MOQT straight on the connection's streams, codes as they are.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterable
from typing import Generic, TypeVar

from aioquic.quic import events as quic_events
from aioquic.quic.connection import stream_is_client_initiated, stream_is_unidirectional
from aioquic.quic.packet import QuicErrorCode
from aioquic.quic.stream import QuicStream

from moq_core._tasks import observe
from moq_core.transport import BidirectionalStream, ReceiveStream

from ._aioquic import acknowledged, live_stream, reset_pending, transmitted
from ._errors import WebTransportError
from ._quic_protocol import QuicProtocol
from ._streams import _ReceiveHalf, _SendHalf, _StreamQueue

P = TypeVar("P", bound=QuicProtocol)

_CLOSE_DRAIN_TIMEOUT = 5.0
"""Seconds ``close()`` waits for the peer to acknowledge what is still in flight before the connection goes."""


class QuicStreamSession(Generic[P]):
    """Stream plumbing for one session on a :class:`QuicProtocol`.

    A framing supplies how it opens streams and closes the session, and overrides the hooks that differ
    between framings: the error codes on the wire (:meth:`_to_wire_code`, :meth:`_from_wire_code`), the
    streams a close drain waits on (:meth:`_drain_stream_ids`), and what a discarded stream leaves behind
    (:meth:`_forget_stream`).
    """

    _CLOSED_MESSAGE = "session closed"
    """The message of the error a stream operation raises once the session is over (without an error of its own)."""

    _NOT_ESTABLISHED_MESSAGE = "session is not established"
    """The message of the error a stream operation raises before the session is established."""

    _CLEAN_CLOSE_CODES: frozenset[int] = frozenset({QuicErrorCode.NO_ERROR})
    """Connection close codes that end the session cleanly, whoever sent them."""

    def __init__(self) -> None:
        loop = asyncio.get_running_loop()
        self._protocol: P | None = None
        self._receives: dict[int, _ReceiveHalf] = {}
        self._sends: dict[int, _SendHalf] = {}
        self._incoming_uni = _StreamQueue()
        self._incoming_bidi = _StreamQueue()
        self._closed: asyncio.Future[None] = loop.create_future()
        observe(self._closed)
        self._closing = False
        self._drained: asyncio.Future[None] | None = None

    # ------------------------------------------------------------------ MoqtTransport

    def incoming_unidirectional_streams(self) -> AsyncIterator[ReceiveStream]:
        return self._incoming_uni

    def incoming_bidirectional_streams(self) -> AsyncIterator[BidirectionalStream]:
        return self._incoming_bidi

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

    # ------------------------------------------------------------------ framing hooks

    def _transport_protocol(self) -> P | None:
        """The connection whose shutdown ``wait_closed`` also waits for."""
        return self._protocol

    def _to_wire_code(self, code: int) -> int:
        """The code a reset or a stop carries on the wire for the application's ``code``."""
        return code

    def _from_wire_code(self, code: int) -> int | None:
        """The application's code for a reset or a stop that carried ``code``; ``None`` when it has none."""
        return code

    def _drain_stream_ids(self) -> Iterable[int]:
        """The streams a close waits on: every send stream, and whatever else the framing sends on."""
        return self._sends

    # ------------------------------------------------------------------ opening streams

    def _add_send(self, protocol: P, stream_id: int) -> _SendHalf:
        send = _SendHalf(self, stream_id)
        self._sends[stream_id] = send
        protocol.stream_owners[stream_id] = self
        return send

    def _add_bidirectional(self, protocol: P, stream_id: int) -> BidirectionalStream:
        send = self._add_send(protocol, stream_id)
        receive = _ReceiveHalf(self, stream_id)
        self._receives[stream_id] = receive
        return BidirectionalStream(readable=receive, writable=send)

    # ------------------------------------------------------------------ the close drain

    def _wait_drained(self, protocol: P) -> asyncio.Future[None]:
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

        for stream_id in self._drain_stream_ids():
            stream = live_stream(protocol._quic, stream_id)

            if stream is not None and not acknowledged(stream):
                return False  # data, the FIN or a reset is not acknowledged yet

        drained.set_result(None)
        return True

    # ------------------------------------------------------------------ event routing

    def _on_stream_bytes(self, stream_id: int, data: bytes, stream_ended: bool) -> None:
        receive = self._receives.get(stream_id)

        if receive is None:
            protocol = self._protocol
            if protocol is None or self._closing:
                return

            # A peer-initiated stream. Its ID parity tells the direction.
            is_client = protocol._quic.configuration.is_client

            if stream_is_client_initiated(stream_id) == is_client:
                return  # Our own unidirectional stream never carries inbound data; ignore stray events.

            receive = _ReceiveHalf(self, stream_id)
            self._receives[stream_id] = receive
            protocol.stream_owners[stream_id] = self

            if stream_is_unidirectional(stream_id):
                self._incoming_uni.push(receive)
            else:
                send = _SendHalf(self, stream_id)
                self._sends[stream_id] = send
                self._incoming_bidi.push(BidirectionalStream(readable=receive, writable=send))

        # A half that already ended ignores this (a retransmitted FIN reports end-of-stream again).
        receive.feed(data, stream_ended)

        if stream_ended:
            # Keep the entries until aioquic discards the stream, so a retransmitted FIN still finds
            # this half instead of looking like a new incoming stream; the watch does the forgetting.
            self._watch(stream_id)

    def _on_stream_reset(self, stream_id: int, error_code: int) -> None:
        receive = self._receives.get(stream_id)

        if receive is not None:
            code = self._from_wire_code(error_code)
            receive.fail(WebTransportError(f"peer reset the stream (code {code})", code=code))
            self._watch(stream_id)

    def _on_stop_sending(self, stream_id: int, error_code: int) -> None:
        send = self._sends.get(stream_id)

        if send is not None:
            send.stopped(self._from_wire_code(error_code) or 0)
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
        """aioquic discarded the stream: drop both halves and the routing entry."""
        self._sends.pop(stream_id, None)
        self._receives.pop(stream_id, None)

        if self._protocol is not None:
            self._protocol.stream_owners.pop(stream_id, None)

    def _on_terminated(self, event: quic_events.ConnectionTerminated) -> None:
        clean = self._closing or event.error_code in self._CLEAN_CLOSE_CODES
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

        stream_error = error if error is not None else WebTransportError(self._CLOSED_MESSAGE)

        for receive in self._receives.values():
            receive.fail(stream_error)

        for send in self._sends.values():
            send.fail(stream_error)

        self._incoming_uni.close()
        self._incoming_bidi.close()

        if self._drained is not None and not self._drained.done():
            self._drained.set_result(None)

    # ------------------------------------------------------------------ _StreamLink: QUIC plumbing for the halves

    def _require_live(self) -> P:
        self.check_live()
        assert self._protocol is not None
        return self._protocol

    def check_live(self) -> None:
        if self._protocol is None:
            raise WebTransportError(self._NOT_ESTABLISHED_MESSAGE)

        if self._closed.done():
            error = self._closed.exception() if not self._closed.cancelled() else None
            raise error if error is not None else WebTransportError(self._CLOSED_MESSAGE)

        if self._closing:
            raise WebTransportError(self._CLOSED_MESSAGE)

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
            protocol._quic.reset_stream(stream_id, self._to_wire_code(code))
            protocol.watch_stream(stream_id, self)
            protocol.transmit()
        except (ValueError, AssertionError):
            pass

    def stop_stream(self, stream_id: int, code: int) -> None:
        protocol = self._protocol

        if protocol is None or protocol.terminated is not None:
            return

        try:
            protocol._quic.stop_stream(stream_id, self._to_wire_code(code))
            protocol.transmit()
        except (ValueError, AssertionError):
            pass
