"""The in-memory relay hub served over native QUIC, the way the SDK reaches a relay by default.

Native QUIC has no CONNECT request to route on, and the hub serves a connection in one role from its first byte, so
each role gets its own listener: :func:`start_quic_hub` returns a publisher listener and a subscriber listener on
loopback ports, each with a self-signed certificate that its :meth:`HubServer.factory` trusts.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from functools import partial
from typing import Any, Literal, cast

from aioquic.asyncio import QuicConnectionProtocol, serve
from aioquic.quic import events
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.connection import QuicConnection
from cryptography.hazmat.primitives import serialization

from moq_core import MOQT_PROTOCOL_IDS
from mux_relay import ConnectingTransport
from mux_relay.quic import connect_quic

from .conformance_peer import IDLE_TIMEOUT, generate_leaf_certificate
from .native_quic import NativeQuicSession
from .quic_server import SERVER_NAME
from .relay_hub import RelayHub

Role = Literal["publisher", "subscriber"]


class _HubProtocol(QuicConnectionProtocol):
    """One accepted native QUIC connection, served by the relay hub in one role."""

    def __init__(
        self,
        quic: QuicConnection,
        stream_handler: Any = None,
        *,
        hub: RelayHub,
        role: Role,
        sessions: list[NativeQuicSession],
    ) -> None:
        super().__init__(quic, stream_handler)
        self.session = NativeQuicSession(self)
        self._hub = hub
        self._role = role
        self._sessions = sessions

    def quic_event_received(self, event: events.QuicEvent) -> None:
        if isinstance(event, events.HandshakeCompleted):
            self._sessions.append(self.session)
            if self._role == "publisher":
                self._hub.serve_publisher(cast(Any, self.session))
            else:
                self._hub.serve_subscriber(cast(Any, self.session))
        self.session.on_event(event)


@dataclass
class HubServer:
    """One listener serving the hub in one role, and how to dial it."""

    port: int
    certificate_pem: bytes
    sessions: list[NativeQuicSession]
    server: Any

    def configuration(self) -> QuicConfiguration:
        """A client configuration that trusts this listener's certificate."""
        return QuicConfiguration(is_client=True, cadata=self.certificate_pem)

    def factory(self):
        """A ``create_transport`` that dials with ``connect_quic`` and trusts this listener."""

        def create_transport(url: str, protocols: Sequence[str]) -> ConnectingTransport:
            return connect_quic(url, protocols, configuration=self.configuration())

        return create_transport

    def url(self, path: str = "/moq") -> str:
        return f"https://{SERVER_NAME}:{self.port}{path}"


async def start_hub_server(hub: RelayHub, role: Role) -> HubServer:
    certificate, key, _ = generate_leaf_certificate()
    configuration = QuicConfiguration(
        is_client=False, alpn_protocols=list(MOQT_PROTOCOL_IDS), idle_timeout=IDLE_TIMEOUT
    )
    configuration.certificate = certificate
    configuration.private_key = key
    sessions: list[NativeQuicSession] = []
    # `::` takes both families, so `localhost` reaches it whichever address it resolves to.
    server = await serve(
        "::",
        0,
        configuration=configuration,
        create_protocol=partial(_HubProtocol, hub=hub, role=role, sessions=sessions),
    )
    transport = server._transport
    assert transport is not None
    pem = certificate.public_bytes(serialization.Encoding.PEM)
    return HubServer(transport.get_extra_info("sockname")[1], pem, sessions, server)


@dataclass
class QuicHub:
    """The hub's two listeners: publishers dial one, subscribers the other."""

    publishers: HubServer
    subscribers: HubServer

    async def close(self) -> None:
        self.publishers.server.close()
        self.subscribers.server.close()
        await asyncio.sleep(0.05)


async def start_quic_hub(hub: RelayHub) -> QuicHub:
    return QuicHub(await start_hub_server(hub, "publisher"), await start_hub_server(hub, "subscriber"))
