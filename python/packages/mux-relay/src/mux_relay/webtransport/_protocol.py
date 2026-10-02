"""The aioquic connection protocol: one HTTP/3 connection carrying WebTransport sessions."""

from __future__ import annotations

import asyncio
import socket
from typing import TYPE_CHECKING, Any

from aioquic.h3.connection import ErrorCode, H3Connection
from aioquic.h3.events import DataReceived, H3Event, HeadersReceived, WebTransportStreamDataReceived
from aioquic.quic import events as quic_events
from aioquic.quic.connection import QuicConnection

from ._aioquic import peer_control_stream_id
from ._quic_protocol import QuicProtocol

if TYPE_CHECKING:
    from ._session import WebTransportSessionBase

_CONTROL_STREAM_FIN_GRACE = 1.0
"""Seconds the peer's HTTP/3 control-stream FIN waits for a CLOSE_WEBTRANSPORT_SESSION capsule before it is fatal.

Long enough for a capsule sent in the same flight to arrive even after a lost packet's retransmission.
"""


class WebTransportProtocol(QuicProtocol):
    """An aioquic connection protocol that drives HTTP/3 with WebTransport enabled.

    Routes HTTP/3 stream events to the :class:`WebTransportSessionBase` that owns them and hands
    request headers that belong to no session (a server's incoming CONNECT) to the
    :meth:`_on_request_headers` hook, which only a server role overrides. The QUIC-level half (the
    keepalive, the ICMP reports, settling, the dropped-FIN retry) is :class:`QuicProtocol`'s.
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
        super().__init__(
            quic, stream_handler, owns_transport=owns_transport, receive_socket=receive_socket, keepalive=keepalive
        )
        self.h3 = H3Connection(quic, enable_webtransport=True)
        self.sessions: dict[int, WebTransportSessionBase] = {}
        self._orphans: dict[int, list[WebTransportStreamDataReceived]] = {}
        self._control_stream_fin: asyncio.TimerHandle | None = None
        """The peer's control-stream FIN, held back a moment (see ``_hold_control_stream_fin``)."""

    def register_session(self, session: WebTransportSessionBase) -> None:
        self.sessions[session.session_id] = session
        self.stream_owners[session.session_id] = session

        for event in self._orphans.pop(session.session_id, []):
            session._on_stream_data(event)

    def _on_terminated(self, event: quic_events.ConnectionTerminated) -> None:
        self._orphans.clear()
        self._cancel_control_stream_fin()

        for session in list(self.sessions.values()):
            session._on_terminated(event)

    def _on_quic_event(self, event: quic_events.QuicEvent) -> None:
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

    def close_after_drain(self, error_code: int = ErrorCode.H3_NO_ERROR, reason_phrase: str = "") -> None:
        self._cancel_control_stream_fin()  # the connection is going anyway; a held FIN must not turn it into 0x104
        super().close_after_drain(error_code=error_code, reason_phrase=reason_phrase)
