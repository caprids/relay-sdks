"""Native QUIC on aioquic: MOQT straight on a QUIC connection, with no HTTP/3 or WebTransport (ADR 0004).

The dialing end of the framing ``@caprid/relay-quic`` speaks for Node: the connection offers the MOQT
protocol identifiers as its ALPN list, and the core sends the endpoint's path (with the token) and
authority in SETUP instead of a CONNECT request (``spec/transport-adapter.md`` TA-1 to TA-6). It is
the default transport of ``connect_relay_publisher`` and ``connect_relay_subscriber``.

The Python SDK's only dialing end: the package carries no HTTP/3. Its modules follow the layers of the
connection: ``_errors`` (the transport layer's own exception), ``_streams`` (the send and receive halves of
a stream, and the link they need from their session), ``_protocol`` (the aioquic connection protocol with
the QUIC-level workarounds: the dropped-FIN retry, resets served first, the keepalive, the ICMP refusal
reports), ``_session`` (the halves' link, settlement and the drain before close) and ``_client`` (the
dial, ALPN selection and the application close). Three modules hold what is aioquic's or the socket's
rather than MOQT's: ``_aioquic`` names every private member the adapter reaches into, ``_fin_retry`` is
the dropped-FIN workaround (#47), deletable as a unit, and ``_udp`` binds the socket and hears a refused
port during the handshake (#163). The test suite builds its accepting ends, a WebTransport one included,
on the same protocol and session.
"""

from ._client import connect_quic

__all__ = ["connect_quic"]
