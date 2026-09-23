"""WebTransport over HTTP/3 on aioquic: the default :class:`moq_core.MoqtTransport` for this SDK.

A browser hands the TypeScript SDK a ``WebTransport`` object. Python has no such built-in, so this
module implements the client side of draft-ietf-webtrans-http3 on top of aioquic's QUIC and HTTP/3
layers: the extended CONNECT handshake (with ``WT-Available-Protocols`` carrying the MOQT protocol
identifier), session-bound unidirectional and bidirectional streams, the CLOSE_WEBTRANSPORT_SESSION
capsule, and the application/HTTP-3 error-code mapping.

:class:`WebTransportSessionBase` holds everything that is common to both ends of a session; the
client role lives in :class:`WebTransportSession`, and the test suite adds a server role on the same
base so the loopback test exercises the real stream code.

The package follows the layers of the protocol: ``_wire`` (capsules, structured-field lists and the
error-code mapping), ``_errors``, ``_streams`` (the send and receive halves of a stream, and the link
they need from their session), ``_protocol`` (the aioquic connection protocol driving HTTP/3),
``_session`` (what both ends of a session share) and ``_client`` (URL, socket and the extended
CONNECT). Two modules hold what is aioquic's rather than WebTransport's: ``_aioquic`` names every
private member the adapter reaches into, and ``_fin_retry`` is the dropped-FIN workaround (#47),
deletable as a unit. The product imports :func:`connect_webtransport` and catches
:class:`WebTransportError`; the test suite reaches the private modules directly.
"""

from ._client import connect_webtransport
from ._errors import WebTransportError

__all__ = ["WebTransportError", "connect_webtransport"]
