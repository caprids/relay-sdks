"""Stable Relay failure identities. Messages are diagnostic text, not a programmatic contract.

One exception type carries every Relay failure: :class:`RelayError`, with a ``code`` to branch on and
the ``phase`` the failure came from, mirroring ``RelayError`` in ``@mux/relay``. The three
subscription errors in :mod:`mux_relay._subscription` subclass it because they carry payload; nothing
else does. Transport-layer exceptions are not Relay errors; a Relay operation that fails on the
transport raises ``TRANSPORT_ERROR`` with the transport exception chained as ``__cause__``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from moq_core import SESSION_ERROR

if TYPE_CHECKING:
    from .webtransport import WebTransportError

RelayErrorCode = Literal[
    "CONNECTION_TIMEOUT",
    "WRITE_TIMEOUT",
    "CLOSED",
    "WRITE_PENDING",
    "NAMESPACE_NOT_SOLICITED",
    "NAMESPACE_ENDED",
    "SUBSCRIPTION_CHANGED",
    "SUBSCRIPTION_REQUEST_FAILED",
    "SUBSCRIPTION_OVERFLOW",
    "UPDATE_QUEUE_FULL",
    "GOAWAY",
    "TRANSPORT_ERROR",
    "TRANSPORT_UNAVAILABLE",
    "UNAUTHORIZED",
]
RelayErrorPhase = Literal[
    "connecting",
    "connection",
    "subscription",
    "waiting-for-demand",
    "opening-stream",
    "writing",
    "closing",
]


class RelayError(Exception):
    """An SDK failure with a stable ``code`` and origin ``phase``; neither promises safe replay.

    Branch on ``code`` (see ``spec/behavior.md``), never on the message. The underlying exception,
    when there is one, is chained as ``__cause__``.
    """

    code: RelayErrorCode
    phase: RelayErrorPhase

    def __init__(self, message: str, *, code: RelayErrorCode, phase: RelayErrorPhase = "connection") -> None:
        super().__init__(message)
        self.code = code
        self.phase = phase


_CREDENTIAL_REFUSALS = frozenset(
    {SESSION_ERROR.UNAUTHORIZED, SESSION_ERROR.MALFORMED_AUTH_TOKEN, SESSION_ERROR.EXPIRED_AUTH_TOKEN}
)
"""MOQT session termination codes (draft-20 §3.5) that mean the relay refused the connection's credentials.

moq-relay sends ``UNAUTHORIZED`` (0x2) for every refusal: no token, a bad signature, a malformed or
expired token, a path outside the token's root.
"""


def _transport_error(error: BaseException, phase: RelayErrorPhase) -> BaseException:
    # asyncio task cancellation and Relay errors keep their identity; anything else, the transport's
    # own exceptions included, becomes a Relay failure with the original chained as its cause.
    if not isinstance(error, Exception) or isinstance(error, RelayError):
        return error
    refusal = _credential_refusal(error)
    wrapped = (
        RelayError(
            f"Relay refused the connection's credentials: {refusal.reason or 'no reason given'} "
            f"(session closed with code 0x{refusal.code:x})",
            code="UNAUTHORIZED",
            phase=phase,
        )
        if refusal is not None
        else RelayError(str(error), code="TRANSPORT_ERROR", phase=phase)
    )
    wrapped.__cause__ = error
    return wrapped


def _credential_refusal(error: Exception) -> WebTransportError | None:
    """The peer's session close, when it closed the WebTransport session because of the credentials."""
    # Imported here: the webtransport package imports the connection code, which imports this module.
    from .webtransport import WebTransportError

    if isinstance(error, WebTransportError) and error.peer_closed and error.code in _CREDENTIAL_REFUSALS:
        return error

    return None


__all__ = ["RelayError", "RelayErrorCode", "RelayErrorPhase"]
