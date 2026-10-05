"""The transport layer's own exception, and how a caller's abort reason becomes an application error code."""

from __future__ import annotations


class WebTransportError(Exception):
    """A QUIC session or stream failure. ``code`` is the peer's application error code when known.

    The name is older than the native QUIC dialing end; #233 replaces this class with an error
    classified by its shape (TA-19, TA-20, TA-22).

    ``peer_closed`` is true when the peer ended the session with an application close (CONNECTION_CLOSE
    of frame type 0x1d): ``code`` and ``reason`` are then its close code (for MOQT, a session
    termination code) and reason, not a QUIC transport or stream error code.

    This is the transport layer's own exception, not a :class:`mux_relay.RelayError`: a Relay
    operation that fails on the transport raises ``TRANSPORT_ERROR`` with this chained as its cause
    (``UNAUTHORIZED`` when the peer closed the session for its credentials).
    """

    def __init__(self, message: str, *, code: int | None = None, reason: str = "", peer_closed: bool = False) -> None:
        super().__init__(message)
        self.code = code
        self.reason = reason
        self.peer_closed = peer_closed


class StreamStopped(WebTransportError):
    """The peer sent STOP_SENDING on a send stream: nothing more written to it will be delivered.

    ``code`` is the peer's application error code. A Relay write that fails this way on its own
    stream raises ``SUBSCRIPTION_CHANGED``, not ``TRANSPORT_ERROR``: the relay stops a subgroup
    stream when its group is no longer of interest (draft-ietf-moq-transport-20 §11.4).
    """


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
