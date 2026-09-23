"""The aioquic connection protocol: one HTTP/3 connection carrying WebTransport sessions."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.h3.connection import ErrorCode, H3Connection
from aioquic.h3.events import DataReceived, H3Event, HeadersReceived, WebTransportStreamDataReceived
from aioquic.quic import events as quic_events
from aioquic.quic.connection import QuicConnection

from moq_core._tasks import observe

if TYPE_CHECKING:
    from ._session import WebTransportSessionBase

_FIN_RETRY_INTERVAL = 0.25
"""Seconds between sweeps that re-queue a FIN aioquic dropped under a full congestion window."""


class WebTransportProtocol(QuicConnectionProtocol):
    """An aioquic connection protocol that drives HTTP/3 with WebTransport enabled.

    Routes HTTP/3 and QUIC stream events to the :class:`WebTransportSessionBase` that owns them and
    hands request headers that belong to no session (a server's incoming CONNECT) to
    ``on_request_headers``.
    """

    def __init__(self, quic: QuicConnection, stream_handler: Any = None, *, owns_transport: bool = False) -> None:
        super().__init__(quic, stream_handler)
        self.h3 = H3Connection(quic, enable_webtransport=True)
        self.sessions: dict[int, WebTransportSessionBase] = {}
        self.stream_owners: dict[int, WebTransportSessionBase] = {}
        self.on_request_headers: Callable[[HeadersReceived], None] | None = None
        self.terminated: quic_events.ConnectionTerminated | None = None
        self.terminated_event: asyncio.Event = asyncio.Event()
        self.owns_transport = owns_transport
        """Whether the UDP transport is this connection's own (a client) or a listener's shared socket (a server)."""
        self.transport_closed: asyncio.Event = asyncio.Event()
        """Set once the connection is over and its UDP socket is released (or was never ours to release)."""
        self._orphans: dict[int, list[WebTransportStreamDataReceived]] = {}
        self._settling: dict[int, WebTransportSessionBase] = {}
        """Streams whose owner waits on a transmission or acknowledgement (a write, a FIN, a reset)."""
        self._draining: list[WebTransportSessionBase] = []
        self._fin_retry_handle: asyncio.TimerHandle | None = None

    async def wait_connected(self) -> None:
        try:
            await super().wait_connected()
        finally:
            # aioquic shields this waiter, so cancelling the connect task leaves it live. Its
            # eventual ConnectionError during teardown must still have an observer.
            if self._connected_waiter is not None:
                observe(self._connected_waiter)

    def register_session(self, session: WebTransportSessionBase) -> None:
        self.sessions[session.session_id] = session
        self.stream_owners[session.session_id] = session

        for event in self._orphans.pop(session.session_id, []):
            session._on_stream_data(event)

    def watch_stream(self, stream_id: int, session: WebTransportSessionBase) -> None:
        self._settling[stream_id] = session
        self._schedule_fin_retry()

    def watch_drain(self, session: WebTransportSessionBase) -> None:
        if session not in self._draining:
            self._draining.append(session)

    def _schedule_fin_retry(self) -> None:
        if self._fin_retry_handle is None and self._settling and self.terminated is None:
            self._fin_retry_handle = self._loop.call_later(_FIN_RETRY_INTERVAL, self._retry_dropped_fins)

    def _retry_dropped_fins(self) -> None:
        """Periodically re-queue FINs aioquic dropped under a full window (see ``_rearm_fin``).

        A time-based sweep, not a per-transmit one: a FIN can be dropped and the window then open
        before the next transmit, so a congestion-gated retry would miss it, while re-queuing on every
        transmit would resend a healthy in-flight FIN each round. On a healthy link a FIN is acknowledged
        within the interval and is never re-queued; a genuinely dropped one is retried until it lands.
        """
        self._fin_retry_handle = None

        if self.terminated is not None:
            return

        streams = self._quic._streams
        rearmed = False

        for stream_id, session in list(self._settling.items()):
            if session._rearm_fin(stream_id, streams.get(stream_id)):
                rearmed = True

        if rearmed:
            self.transmit()

        self._schedule_fin_retry()

    def transmit(self) -> None:
        super().transmit()

        # Everything that moves a stream along (a flight sent, an ACK or STOP_SENDING processed, a
        # timer) ends in a transmit, so this is the one place to settle waiting writers and drains.
        if self._settling or self._draining:
            self._settle()

    def _settle(self) -> None:
        streams = self._quic._streams

        for stream_id, session in list(self._settling.items()):
            if not session._settle_stream(stream_id, streams.get(stream_id)):
                self._settling.pop(stream_id, None)

        for session in list(self._draining):
            if session._check_drained():
                self._draining.remove(session)

    def connection_lost(self, exc: Exception | None) -> None:
        self._cancel_fin_retry()
        self.transport_closed.set()

    def _cancel_fin_retry(self) -> None:
        if self._fin_retry_handle is not None:
            self._fin_retry_handle.cancel()
            self._fin_retry_handle = None

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
            self._orphans.clear()
            self._settling.clear()
            self._draining.clear()
            self._cancel_fin_retry()

            for session in list(self.sessions.values()):
                session._on_terminated(event)

            self._close_transport_soon()
            return

        for h3_event in self.h3.handle_event(event):
            self._handle_h3_event(h3_event)

    def _handle_h3_event(self, event: H3Event) -> None:
        if isinstance(event, WebTransportStreamDataReceived):
            session = self.sessions.get(event.session_id)

            if session is not None:
                session._on_stream_data(event)
            else:
                # A stream can outrun the CONNECT it belongs to; hold its data until that session exists.
                self._orphans.setdefault(event.session_id, []).append(event)

            return

        if isinstance(event, HeadersReceived):
            session = self.sessions.get(event.stream_id)

            if session is not None:
                session._on_connect_headers(event)
            elif self.on_request_headers is not None:
                self.on_request_headers(event)

            return

        if isinstance(event, DataReceived):
            session = self.sessions.get(event.stream_id)

            if session is not None:
                session._on_connect_data(event.data, event.stream_ended)

    def _close_transport_soon(self) -> None:
        transport = self._transport

        if self.owns_transport and transport is not None:
            self._loop.call_soon(transport.close)
        else:
            # A server connection shares its listener's socket; there is nothing of ours to release.
            self.transport_closed.set()

    def close_after_drain(self, error_code: int = ErrorCode.H3_NO_ERROR, reason_phrase: str = "") -> None:
        """Close the QUIC connection, and make sure the UDP socket is released even if the peer never answers."""
        self.close(error_code=error_code, reason_phrase=reason_phrase)

        def release() -> None:
            if self.owns_transport and self._transport is not None and not self.terminated_event.is_set():
                self._transport.close()

        self._loop.call_later(3.0, release)


def _finish_receiving_half(quic: QuicConnection, stream_id: int) -> None:
    """Mark a locally opened unidirectional stream's receiving half done so aioquic can discard the stream.

    aioquic 1.3 leaves that half unfinished although nothing can ever arrive on it, so the stream
    would otherwise stay in ``QuicConnection._streams`` (and in the per-packet scheduling loop) for
    the life of the connection after its FIN or reset was acknowledged.
    """
    stream = quic._streams.get(stream_id)

    if stream is not None:
        stream.receiver.is_finished = True
