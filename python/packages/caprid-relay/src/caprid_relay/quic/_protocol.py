"""The QUIC half of an aioquic connection protocol: the workarounds every connection needs, whatever it carries.

Everything here is QUIC-level or socket-level: the batched receive (#162), resets served first (#160), the
settling of writes and close drains after every transmit, the dropped-FIN retry (#47), the PING keepalive
(#158), the ICMP refusal reporting during the handshake (#163), and the release of the UDP socket.
:class:`~._client.NativeQuicProtocol` puts MOQT straight on the streams; the test suite's WebTransport
accepting end puts HTTP/3 on top.
"""

from __future__ import annotations

import asyncio
import socket
from typing import TYPE_CHECKING, Any, cast

from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic import events as quic_events
from aioquic.quic.connection import QuicConnection
from aioquic.quic.packet import QuicErrorCode

from moq_core._tasks import observe

from ._aioquic import live_stream, packets_sent, serve_resets_first
from ._fin_retry import DroppedFinRetry
from ._udp import drain_errors, report_refusals

if TYPE_CHECKING:
    from ._session import QuicStreamSession

_RECEIVE_BATCH = 64
"""Most datagrams taken in per transmit. Bounds how long one receive holds the loop (and delays ACKs):
about 64 full-size packets, or a few milliseconds of CPU."""

_MAX_DATAGRAM_SIZE = 65535

_KEEPALIVE_INTERVAL = 5.0
"""Seconds of send silence after which an established connection sends a QUIC PING.

Draft-22 §15.6.1 leaves idle-connection keepalive to the transport (QUIC PING frames among the
options). aioquic has none, and a receive-only client sends nothing when nothing reaches it, so after a
NAT rebinding the relay would keep sending to the old address until its idle timeout. The next PING
leaves from the new address and the relay migrates (RFC 9000 §9.3). Chromium does the same for the
TypeScript SDK.
"""

_MAX_CLOSE_REASON = 1024
"""Most bytes of a close reason sent. CONNECTION_CLOSE must fit in one packet (RFC 9000 §19.19), and aioquic
fails the transmit, closing nothing, when it does not."""

_SOCKET_RELEASE_DELAY = 3.0
"""Seconds ``close_after_drain`` waits for the connection to end on its own before it releases the socket."""


class QuicProtocol(QuicConnectionProtocol):
    """An aioquic connection protocol with the QUIC-level workarounds every framing needs.

    Routes RESET_STREAM and STOP_SENDING to the session that owns the stream, settles the sessions'
    waiting writes and drains after every transmit, and hands every other event to
    :meth:`_on_quic_event`, which the framing overrides. With ``keepalive``, an established connection
    sends a QUIC PING once it has sent nothing for ``_KEEPALIVE_INTERVAL``.
    """

    def __init__(
        self,
        quic: QuicConnection,
        stream_handler: Any = None,
        *,
        owns_transport: bool = False,
        receive_socket: socket.socket | None = None,
        keepalive: bool = False,
    ) -> None:
        super().__init__(quic, stream_handler)
        self._receive_socket = receive_socket
        """The connection's own non-blocking UDP socket, read dry on each receive (see ``datagram_received``).

        ``None`` for a connection on a listener's shared socket, which keeps aioquic's one transmit per datagram.
        """
        self.stream_owners: dict[int, QuicStreamSession[Any]] = {}
        self.terminated: quic_events.ConnectionTerminated | None = None
        self.terminated_event: asyncio.Event = asyncio.Event()
        self.owns_transport = owns_transport
        """Whether the UDP transport is this connection's own (a client) or a listener's shared socket (a server)."""
        self.transport_closed: asyncio.Event = asyncio.Event()
        """Set once the connection is over and its UDP socket is released (or was never ours to release)."""
        self.fin_retry = DroppedFinRetry(self._loop, quic, self.transmit)
        """The aioquic dropped-FIN workaround (#47); the session hands it every stream whose FIN it queues."""
        self._settling: dict[int, QuicStreamSession[Any]] = {}
        """Streams whose owner waits on a transmission or acknowledgement (a write, a FIN, a reset)."""
        self._draining: list[QuicStreamSession[Any]] = []
        self._keepalive: asyncio.TimerHandle | None = None
        """The next keepalive check, armed once the handshake completes and dropped when the connection closes."""
        self._keepalive_stopped = not keepalive
        """No keepalive: a server role never had one, and a client's stops once its connection closes."""
        self._packets_sent = 0
        self._last_sent_at = self._loop.time()
        self._ping_uid = 0
        self._handshake_completed = False
        self.refused: ConnectionRefusedError | None = None
        """The OS reported the peer's port unreachable before the handshake completed (a client only)."""
        self._reporting_refusals = owns_transport
        """The client socket reports ICMP errors (see ``_udp``) until the handshake ends, however it ends."""

    async def wait_connected(self) -> None:
        if self.refused is not None:
            raise self.refused

        try:
            await super().wait_connected()
        finally:
            # aioquic shields this waiter, so cancelling the connect task leaves it live. Its
            # eventual ConnectionError during teardown must still have an observer.
            if self._connected_waiter is not None:
                observe(self._connected_waiter)

    def watch_stream(self, stream_id: int, session: QuicStreamSession[Any]) -> None:
        self._settling[stream_id] = session

    def watch_drain(self, session: QuicStreamSession[Any]) -> None:
        if session not in self._draining:
            self._draining.append(session)

    def datagram_received(self, data: bytes | str, addr: Any) -> None:
        """Take in this datagram and any others already waiting on the socket, then transmit once.

        aioquic transmits after every datagram, and each transmit walks every open stream to raise its
        MAX_STREAM_DATA (#162), so with many streams open the receive path is bound by that walk. asyncio
        reads one datagram per loop turn, so deferring the transmit to a later turn would not batch
        anything; the batch comes from reading the socket dry here (up to ``_RECEIVE_BATCH``
        datagrams). ACKs still leave in this turn, and the settling that follows every transmit still runs.
        """
        self._receive(data, addr)
        sock = self._receive_socket

        if sock is not None:
            for _ in range(_RECEIVE_BATCH - 1):
                if self.terminated is not None:
                    break

                try:
                    data, addr = sock.recvfrom(_MAX_DATAGRAM_SIZE)
                except (BlockingIOError, InterruptedError):
                    break
                except OSError as exc:
                    # What asyncio's own read would do with it.
                    self.error_received(exc)
                    break

                self._receive(data, addr)

        self.transmit()

    def _receive(self, data: bytes | str, addr: Any) -> None:
        self._quic.receive_datagram(cast(bytes, data), addr, now=self._loop.time())
        self._process_events()

    def transmit(self) -> None:
        # aioquic 1.3 sends one packet per transmit while a reset or STOP_SENDING waits behind streams
        # with data (see `serve_resets_first`); every transmit comes through here, so reorder first.
        serve_resets_first(self._quic)
        super().transmit()

        sent = packets_sent(self._quic)

        if sent != self._packets_sent:
            self._packets_sent = sent
            self._last_sent_at = self._loop.time()

        # Everything that moves a stream along (a flight sent, an ACK or STOP_SENDING processed, a
        # timer) ends in a transmit, so this is the one place to settle waiting writers and drains.
        if self._settling or self._draining:
            self._settle()

    def _settle(self) -> None:
        for stream_id, session in list(self._settling.items()):
            if not session._settle_stream(stream_id, live_stream(self._quic, stream_id)):
                self._settling.pop(stream_id, None)

        for session in list(self._draining):
            if session._check_drained():
                self._draining.remove(session)

    def error_received(self, exc: Exception) -> None:
        """A socket error: only a refusal during the handshake counts, and it fails the connect at once.

        While the handshake is pending the client's socket reports ICMP errors (see ``_udp``), so a port
        unreachable arrives here as ``ConnectionRefusedError``: nothing listens there, and waiting out
        the connect deadline would only hide it. Asyncio calls this for a failed receive or send, and
        so may a batched read that meets the error mid-batch. Once the connection is established the
        reports are off, and one that was already pending changes nothing: QUIC's loss recovery and
        idle timeout decide.

        Every report also waits on the socket's error queue, which keeps the event loop's reader
        waking until it is read, so the queue is drained here even for an error that is ignored.
        """
        sock = self._socket() if self._reporting_refusals else None

        if sock is not None:
            drain_errors(sock)

        if not isinstance(exc, ConnectionRefusedError) or self._handshake_completed or self.refused is not None:
            return

        self.refused = exc
        self._stop_reporting_refusals()  # the connect fails now; its teardown must not be reported on
        waiter = self._connected_waiter

        if waiter is not None and not waiter.done():
            self._connected_waiter = None
            waiter.set_exception(exc)

    def _socket(self) -> Any:
        transport = self._transport
        return transport.get_extra_info("socket") if transport is not None and not transport.is_closing() else None

    def _stop_reporting_refusals(self) -> None:
        """Turn the socket's ICMP reports off, which also discards any still queued."""
        if not self._reporting_refusals:
            return

        sock = self._socket()

        if sock is not None:
            self._reporting_refusals = False
            report_refusals(sock, False)

    def connection_lost(self, exc: Exception | None) -> None:
        self._reporting_refusals = False
        self.fin_retry.stop()
        self._stop_keepalive()
        self.transport_closed.set()

    def quic_event_received(self, event: quic_events.QuicEvent) -> None:
        if isinstance(event, quic_events.StreamReset):
            owner = self.stream_owners.get(event.stream_id)

            if owner is not None:
                owner._on_stream_reset(event.stream_id, event.error_code)

            return

        if isinstance(event, quic_events.StopSendingReceived):
            owner = self.stream_owners.get(event.stream_id)

            if owner is not None:
                owner._on_stop_sending(event.stream_id, event.error_code)

            return

        if isinstance(event, quic_events.ConnectionTerminated):
            self.terminated = event
            self.terminated_event.set()
            self._settling.clear()
            self._draining.clear()
            self.fin_retry.stop()
            self._stop_keepalive()
            self._stop_reporting_refusals()
            self._on_terminated(event)
            self._close_transport_soon()
            return

        if isinstance(event, quic_events.HandshakeCompleted):
            self._handshake_completed = True
            self._stop_reporting_refusals()
            self._arm_keepalive(self._last_sent_at + _KEEPALIVE_INTERVAL)

        self._on_quic_event(event)

    # ------------------------------------------------------------------ framing hooks

    def _on_quic_event(self, event: quic_events.QuicEvent) -> None:
        """Every event the QUIC layer does not handle itself: stream data, the handshake's completion, and the rest."""

    def _on_terminated(self, event: quic_events.ConnectionTerminated) -> None:
        """The connection is over: end every session it carries."""

    # ------------------------------------------------------------------ keepalive

    def _arm_keepalive(self, when: float) -> None:
        if not self._keepalive_stopped and self._keepalive is None:
            self._keepalive = self._loop.call_at(when, self._keepalive_due)

    def _keepalive_due(self) -> None:
        """PING if nothing was sent for ``_KEEPALIVE_INTERVAL``; otherwise check again that long after the last send.

        Checking lazily keeps a busy connection at one timer per interval instead of one per transmit.
        """
        self._keepalive = None
        now = self._loop.time()
        due = self._last_sent_at + _KEEPALIVE_INTERVAL

        # The loop may run a timer a clock tick early; a PING a few milliseconds early does no harm.
        if now < due - 0.01:
            self._arm_keepalive(due)
            return

        self._ping_uid += 1
        self._quic.send_ping(self._ping_uid)
        self.transmit()
        # From now, not from the last send: a PING that could not leave must not re-arm in the past.
        self._arm_keepalive(now + _KEEPALIVE_INTERVAL)

    def _stop_keepalive(self) -> None:
        self._keepalive_stopped = True

        if self._keepalive is not None:
            self._keepalive.cancel()
            self._keepalive = None

    # ------------------------------------------------------------------ teardown

    def _close_transport_soon(self) -> None:
        transport = self._transport

        if self.owns_transport and transport is not None:
            self._loop.call_soon(transport.close)
        else:
            # A server connection shares its listener's socket; there is nothing of ours to release.
            self.transport_closed.set()

    def close(self, error_code: int = QuicErrorCode.NO_ERROR, reason_phrase: str = "") -> None:
        self._stop_keepalive()  # the connection is closing: nothing is left to keep alive
        # However the handshake ended (failed, timed out, cancelled, closed), its reports end with it.
        self._stop_reporting_refusals()
        # Cut on a character boundary, so what is sent still decodes.
        reason_phrase = reason_phrase.encode()[:_MAX_CLOSE_REASON].decode(errors="ignore")
        super().close(error_code=error_code, reason_phrase=reason_phrase)

    def close_after_drain(self, error_code: int = QuicErrorCode.NO_ERROR, reason_phrase: str = "") -> None:
        """Close the QUIC connection, and make sure the UDP socket is released even if the peer never answers."""

        def release() -> None:
            if self.owns_transport and self._transport is not None and not self.terminated_event.is_set():
                self._transport.close()

        # Armed first: the release must come even if sending the close fails.
        self._loop.call_later(_SOCKET_RELEASE_DELAY, release)
        self.close(error_code=error_code, reason_phrase=reason_phrase)
