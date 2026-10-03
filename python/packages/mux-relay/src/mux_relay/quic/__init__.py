"""Native QUIC on aioquic: MOQT straight on a QUIC connection, with no HTTP/3 or WebTransport (ADR 0004).

The dialing end of the framing ``@mux/relay-quic`` speaks for Node: the connection offers the MOQT
protocol identifiers as its ALPN list, and the core sends the endpoint's path (with the token) and
authority in SETUP instead of a CONNECT request (``spec/transport-adapter.md`` TA-1 to TA-6). It is
available through ``create_transport`` and is not the default yet (#231).

The QUIC-level workarounds it shares with the WebTransport session (the dropped-FIN retry, resets
served first, the keepalive, the ICMP refusal reports, the drain before close) live in
``mux_relay.webtransport._quic_protocol`` and ``_quic_session`` until #232 moves them out of that package.
"""

from ._client import connect_quic

__all__ = ["connect_quic"]
