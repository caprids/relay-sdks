"""The server role of a WebTransport session: the accepting end of the TypeScript conformance suite's Chromium target.

Built on ``relay_helpers.webtransport``, which puts HTTP/3 on the SDK's own QUIC plumbing; ``conformance_peer.py``
answers each CONNECT as the suite's ``prepare`` said (#208).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from aioquic.h3.events import HeadersReceived
from aioquic.quic.connection import QuicConnection

from .quic_server import Impairment, receive_impaired
from .webtransport.protocol import WebTransportProtocol
from .webtransport.session import WebTransportSessionBase
from .webtransport.wire import parse_sf_list_of_strings, sf_list_of_strings


class WebTransportServerSession(WebTransportSessionBase):
    """The server end of one accepted session: a ``MoqtTransport`` for the conformance peer to drive."""

    def __init__(self, protocol: WebTransportProtocol, session_id: int, path: str, protocol_id: str | None) -> None:
        super().__init__()
        self.path = path
        self.protocol_id = protocol_id
        self._attach(protocol, session_id)


SessionHandler = Callable[[WebTransportServerSession], None]


class WebTransportServerProtocol(WebTransportProtocol):
    """Accepts extended CONNECT requests and hands each session to ``on_session``."""

    def __init__(
        self,
        quic: QuicConnection,
        on_session: SessionHandler,
        stream_handler: Any = None,
    ) -> None:
        super().__init__(quic, stream_handler)
        self._on_session = on_session
        self.requests: list[dict[bytes, bytes]] = []
        self.impairment: Impairment | None = None

    def datagram_received(self, data: bytes | str, addr: Any) -> None:
        receive_impaired(self, self.impairment, super().datagram_received, data, addr)

    def _on_request_headers(self, event: HeadersReceived) -> None:
        """Answer the peer's CONNECT: accept it as a WebTransport session, or 404 anything else."""
        headers = {key.lower(): value for key, value in event.headers}
        self.requests.append(headers)

        if headers.get(b":method") != b"CONNECT" or headers.get(b":protocol") != b"webtransport":
            self.h3.send_headers(event.stream_id, [(b":status", b"404")], end_stream=True)
            self.transmit()
            return

        offered = parse_sf_list_of_strings(headers.get(b"wt-available-protocols", b""))
        selected = offered[0] if offered else None
        response = [(b":status", b"200"), (b"sec-webtransport-http3-draft", b"draft02")]

        if selected is not None:
            response.append((b"wt-protocol", sf_list_of_strings([selected])))

        self.h3.send_headers(event.stream_id, response)
        self.transmit()
        session = WebTransportServerSession(
            self, event.stream_id, headers.get(b":path", b"/").decode("utf-8", errors="replace"), selected
        )
        self._on_session(session)
