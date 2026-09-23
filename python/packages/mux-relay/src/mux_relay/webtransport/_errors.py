"""The transport layer's own exception, and how a caller's abort reason becomes an application error code."""

from __future__ import annotations


class WebTransportError(Exception):
    """A WebTransport session or stream failure. ``code`` is the peer's application error code when known.

    This is the transport layer's own exception, not a :class:`mux_relay.RelayError`: a Relay
    operation that fails on the transport raises ``TRANSPORT_ERROR`` with this chained as its cause.
    """

    def __init__(self, message: str, *, code: int | None = None, reason: str = "") -> None:
        super().__init__(message)
        self.code = code
        self.reason = reason


def _error_code_of(reason: object) -> int:
    if isinstance(reason, WebTransportError) and reason.code is not None:
        return reason.code

    # A publisher-side reset reason (the write slot's StreamReset) names the MOQT code it wants sent.
    stream_error_code = getattr(reason, "stream_error_code", None)
    if (
        isinstance(stream_error_code, int)
        and not isinstance(stream_error_code, bool)
        and 0 <= stream_error_code <= 0xFFFFFFFF
    ):
        return stream_error_code

    if isinstance(reason, int) and not isinstance(reason, bool) and 0 <= reason <= 0xFFFFFFFF:
        return reason

    return 0
