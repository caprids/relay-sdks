"""A client and server QuicConnection handshaken in memory, for tests that need real packets but no socket."""

from __future__ import annotations

from aioquic.buffer import Buffer
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.connection import QuicConnection
from aioquic.quic.packet import pull_quic_header

from .wt_server import generate_self_signed_certificate

CLIENT_ADDRESS = ("192.0.2.1", 4433)
SERVER_ADDRESS = ("192.0.2.2", 443)


class QuicPair:
    """Datagrams move only when the test calls :meth:`deliver`, so anything in between is an outage."""

    def __init__(self) -> None:
        certificate, key, _pem = generate_self_signed_certificate()
        server_configuration = QuicConfiguration(is_client=False, alpn_protocols=["h3"])
        server_configuration.certificate = certificate
        server_configuration.private_key = key
        self._server_configuration = server_configuration
        self.now = 1.0
        self.client = QuicConnection(
            configuration=QuicConfiguration(is_client=True, alpn_protocols=["h3"], verify_mode=0)
        )
        self.server: QuicConnection | None = None
        self._sent: list[tuple[bytes, tuple[str, int]]] = []
        self.client.connect(SERVER_ADDRESS, now=self.now)

        for _ in range(10):
            self.deliver()

        assert self.client._handshake_complete

    def deliver(self, step: float = 0.01) -> None:
        """One round trip: what the client has to send, and what it sent unpaced since, reaches the server;
        then the server's datagrams, acknowledgements included, reach the client."""
        sent, self._sent = self._sent + self.client.datagrams_to_send(self.now), []

        for datagram, _address in sent:
            if self.server is None:
                header = pull_quic_header(Buffer(data=datagram), host_cid_length=8)
                self.server = QuicConnection(
                    configuration=self._server_configuration,
                    original_destination_connection_id=header.destination_cid,
                )

            self.server.receive_datagram(datagram, CLIENT_ADDRESS, self.now)

        assert self.server is not None

        self.now += step  # past the server's acknowledgement delay, so its ACKs go out now

        for datagram, _address in self.server.datagrams_to_send(self.now):
            self.client.receive_datagram(datagram, SERVER_ADDRESS, self.now)

    def open_streams(self, count: int, size: int, *, fin: bool = False) -> list[int]:
        """``count`` client unidirectional streams with ``size`` bytes each queued and not yet sent."""
        stream_ids = []

        for _ in range(count):
            stream_id = self.client.get_next_available_stream_id(is_unidirectional=True)
            self.client.send_stream_data(stream_id, bytes(size), end_stream=fin)
            stream_ids.append(stream_id)

        return stream_ids

    def send_unpaced(self) -> int:
        """One client transmit with pacing taken out, so the count is what the congestion window allows.

        The datagrams reach the server with the next :meth:`deliver`.
        """
        self.client._loss._pacer.packet_time = None
        datagrams = self.client.datagrams_to_send(self.now)
        self._sent.extend(datagrams)
        return len(datagrams)
