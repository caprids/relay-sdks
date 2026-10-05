"""WebTransport over HTTP/3 on aioquic: the accepting end of the TypeScript conformance suite's Chromium target.

The SDK's dialing end is native QUIC (``mux_relay.quic``, ADR 0004); a browser still reaches an accepting end over
WebTransport, so this package keeps the server side of draft-ietf-webtrans-http3 for ``relay_helpers.wt_server``
and ``relay_helpers.conformance_peer`` (#208). It follows the layers of the protocol, on the SDK's own QUIC
plumbing: ``wire`` (capsules, structured-field lists and the HTTP/3 error-code mapping), ``h3`` (the
``H3Connection`` private members it reaches into), ``protocol`` (the aioquic connection protocol driving HTTP/3,
on :class:`mux_relay.quic._protocol.QuicProtocol`) and ``session`` (a WebTransport session's streams, capsules
and closure, on :class:`mux_relay.quic._session.QuicStreamSession`).
"""
