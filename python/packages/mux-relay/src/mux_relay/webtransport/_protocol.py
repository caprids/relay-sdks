"""The aioquic connection protocol: one HTTP/3 connection carrying WebTransport sessions."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.h3.connection import ErrorCode, H3Connection
from aioquic.h3.events import DataReceived, H3Event, HeadersReceived, WebTransportStreamDataReceived
from aioquic.quic import events as quic_events
from aioquic.quic.connection import QuicConnection

from moq_core._tasks import observe

from ._aioquic import live_stream, peer_control_stream_id, serve_resets_first
from ._fin_retry import DroppedFinRetry

if TYPE_CHECKING:
    from ._session import WebTransportSessionBase

_CONTROL_STREAM_FIN_GRACE = 1.0
"""Seconds the peer's HTTP/3 control-stream FIN waits for a CLOSE_WEBTRANSPORT_SESSION capsule before it is fatal.

Long enough for a capsule sent in the same flight to arrive even after a lost packet's retransmission.
"""


class WebTransportProtocol(QuicConnectionProtocol):
    """An aioquic connection protocol that drives HTTP/3 with WebTransport enabled.

    Routes HTTP/3 and QUIC stream events to the :class:`WebTransportSessionBase` that owns them and
    hands request headers that belong to no session (a server's incoming CONNECT) to the
    :meth:`_on_request_headers` hook, which only a server role overrides.
    """

    def __init__(self, quic: QuicConnection, stream_handler: Any = None, *, owns_transport: bool = False) -> None:
        super().__init__(quic, stream_handler)
        self.h3 = H3Connection(quic, enable_webtransport=True)
        self.sessions: dict[int, WebTransportSessionBase] = {}
        self.stream_owners: dict[int, WebTransportSessionBase] = {}
        self.terminated: quic_events.ConnectionTerminated | None = None
        self.terminated_event: asyncio.Event = asyncio.Event()
        self.owns_transport = owns_transport
        """Whether the UDP transport is this connection's own (a client) or a listener's shared socket (a server)."""
        self.transport_closed: asyncio.Event = asyncio.Event()
        """Set once the connection is over and its UDP socket is released (or was never ours to release)."""
        self.fin_retry = DroppedFinRetry(self._loop, quic, self.transmit)
        """The aioquic dropped-FIN workaround (#47); ``_session`` hands it every stream whose FIN it queues."""
        self._orphans: dict[int, list[WebTransportStreamDataReceived]] = {}
        self._settling: dict[int, WebTransportSessionBase] = {}
        """Streams whose owner waits on a transmission or acknowledgement (a write, a FIN, a reset)."""
        self._draining: list[WebTransportSessionBase] = []
        self._control_stream_fin: asyncio.TimerHandle | None = None
        """The peer's control-stream FIN, held back a moment (see ``_hold_control_stream_fin``)."""

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

    def watch_drain(self, session: WebTransportSessionBase) -> None:
        if session not in self._draining:
            self._draining.append(session)

    def transmit(self) -> None:
        # aioquic 1.3 sends one packet per transmit while a reset or STOP_SENDING waits behind streams
        # with data (see `serve_resets_first`); every transmit comes through here, so reorder first.
        serve_resets_first(self._quic)
        super().transmit()

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

    def connection_lost(self, exc: Exception | None) -> None:
        self.fin_retry.stop()
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
            self._orphans.clear()
            self._settling.clear()
            self._draining.clear()
            self.fin_retry.stop()
            self._cancel_control_stream_fin()

            for session in list(self.sessions.values()):
                session._on_terminated(event)

            self._close_transport_soon()
            return

        if isinstance(event, quic_events.StreamDataReceived) and self._hold_control_stream_fin(event):
            return

        for h3_event in self.h3.handle_event(event):
            self._handle_h3_event(h3_event)

    # ------------------------------------------------------------------ the peer's control-stream FIN

    def _hold_control_stream_fin(self, event: quic_events.StreamDataReceived) -> bool:
        """Hold back the FIN on the peer's HTTP/3 control stream while a session could still hear why.

        Closing the control stream is an HTTP/3 connection error (RFC 9114 §6.2.1), and aioquic acts on
        it at once: it closes the connection with H3_CLOSED_CRITICAL_STREAM (0x104) and drops every
        later event, including a CLOSE_WEBTRANSPORT_SESSION capsule in the same flight. moq-relay 0.15.8
        refuses credentials exactly that way (the capsule with code 0x2, its control-stream FIN and
        STOP_SENDING together), and the control-stream FIN is processed first, so the refusal would
        surface as 0x104. The FIN therefore waits ``_CONTROL_STREAM_FIN_GRACE``: a capsule that
        arrives in the meantime ends its session with the peer's code and the connection closes
        normally; otherwise aioquic gets the FIN late and the connection fails with 0x104 as before.

        Returns ``True`` when the event was consumed here. Data the event carries still goes to
        aioquic now; only the end of the stream waits.
        """
        if (
            not event.end_stream
            or event.stream_id != peer_control_stream_id(self.h3)
            or self._control_stream_fin is not None
            or not any(not session.ended for session in self.sessions.values())
        ):
            return False

        if event.data:
            data = quic_events.StreamDataReceived(data=event.data, end_stream=False, stream_id=event.stream_id)

            for h3_event in self.h3.handle_event(data):
                self._handle_h3_event(h3_event)

        self._control_stream_fin = self._loop.call_later(
            _CONTROL_STREAM_FIN_GRACE, self._release_control_stream_fin, event.stream_id
        )
        return True

    def _release_control_stream_fin(self, stream_id: int) -> None:
        """No capsule settled it: hand aioquic the FIN, which closes the connection with 0x104."""
        if self.terminated is not None:
            return

        fin = quic_events.StreamDataReceived(data=b"", end_stream=True, stream_id=stream_id)

        for h3_event in self.h3.handle_event(fin):
            self._handle_h3_event(h3_event)

        self.transmit()

    def _cancel_control_stream_fin(self) -> None:
        # Leave the handle in place, cancelled: the FIN is not delivered twice, and a second hold must not start.
        if self._control_stream_fin is not None:
            self._control_stream_fin.cancel()

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
            else:
                self._on_request_headers(event)

            return

        if isinstance(event, DataReceived):
            session = self.sessions.get(event.stream_id)

            if session is not None:
                session._on_connect_data(event.data, event.stream_ended)

    # ------------------------------------------------------------------ role hooks

    def _on_request_headers(self, event: HeadersReceived) -> None:
        """Headers on a stream no session owns: a peer's CONNECT. A client has no such role and ignores them."""

    # ------------------------------------------------------------------ teardown

    def _close_transport_soon(self) -> None:
        transport = self._transport

        if self.owns_transport and transport is not None:
            self._loop.call_soon(transport.close)
        else:
            # A server connection shares its listener's socket; there is nothing of ours to release.
            self.transport_closed.set()

    def close_after_drain(self, error_code: int = ErrorCode.H3_NO_ERROR, reason_phrase: str = "") -> None:
        """Close the QUIC connection, and make sure the UDP socket is released even if the peer never answers."""
        self._cancel_control_stream_fin()  # the connection is going anyway; a held FIN must not turn it into 0x104
        self.close(error_code=error_code, reason_phrase=reason_phrase)

        def release() -> None:
            if self.owns_transport and self._transport is not None and not self.terminated_event.is_set():
                self._transport.close()

        self._loop.call_later(3.0, release)
