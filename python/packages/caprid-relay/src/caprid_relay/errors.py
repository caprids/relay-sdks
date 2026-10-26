"""Stable Relay failure identities. Messages are diagnostic text, not a programmatic contract.

One exception type carries every Relay failure: :class:`RelayError`, with a ``code`` to branch on and
the ``phase`` the failure came from, mirroring ``RelayError`` in ``@caprid/relay``. The three
subscription errors in :mod:`caprid_relay._subscription` subclass it because they carry payload; nothing
else does. Transport-layer exceptions are not Relay errors; a Relay operation that fails on the
transport raises ``TRANSPORT_ERROR`` with the transport exception chained as ``__cause__``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from moq_core import SESSION_ERROR
from moq_core.transport import is_stream_error

if TYPE_CHECKING:
    pass

RelayErrorCode = Literal[
    "CONNECTION_TIMEOUT",
    "WRITE_TIMEOUT",
    "DELIVERY_TIMEOUT",
    "CLOSED",
    "WRITE_PENDING",
    "NAMESPACE_NOT_SOLICITED",
    "NAMESPACE_ENDED",
    "SUBSCRIPTION_CHANGED",
    "SUBSCRIPTION_REQUEST_FAILED",
    "SUBSCRIPTION_OVERFLOW",
    "UPDATE_QUEUE_FULL",
    # ``namespace_events`` refused a prefix that overlaps (equals, contains or falls under) one this
    # connection is already discovering, in phase ``subscription``; nothing was sent. Draft-22 §9.15
    # allows one SUBSCRIBE_NAMESPACE per prefix tree on a session. Widen one iterator's prefix and
    # filter, or use a second connection.
    "PREFIX_OVERLAP",
    "GOAWAY",
    # A supervised publisher's write made while no generation is up, or interrupted by the
    # connection's end, in phase ``connection``; nothing was replayed. An interruption, not a
    # retirement: write the next object (at the next keyframe) once the next generation is live.
    "RECONNECTING",
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
"""MOQT session termination codes (draft-22 §12.2) that mean the relay refused the connection's credentials.

moq-relay sends ``UNAUTHORIZED`` (0x2) for every refusal: no token, a bad signature, a malformed or
expired token, a path outside the token's root.
"""


def _transport_error(
    error: BaseException, phase: RelayErrorPhase, peer_close: tuple[int, str] | None = None
) -> BaseException:
    """Wrap a transport failure as a Relay failure; ``peer_close`` is the peer's application close, if one ended it."""
    # asyncio task cancellation and Relay errors keep their identity; anything else, the transport's
    # own exceptions included, becomes a Relay failure with the original chained as its cause.
    if not isinstance(error, Exception) or isinstance(error, RelayError):
        return error
    refusal = _credential_refusal(error) or _refusal(peer_close)
    wrapped = (
        _unauthorized(refusal, phase)
        if refusal is not None
        else RelayError(str(error), code="TRANSPORT_ERROR", phase=phase)
    )
    wrapped.__cause__ = error
    return wrapped


def _unauthorized(refusal: tuple[int, str], phase: RelayErrorPhase) -> RelayError:
    code, reason = refusal
    return RelayError(
        f"Relay refused the connection's credentials: {reason or 'no reason given'} "
        f"(session closed with code 0x{code:x})",
        code="UNAUTHORIZED",
        phase=phase,
    )


def _refusal(peer_close: tuple[int, str] | None) -> tuple[int, str] | None:
    """The peer's application close ``(code, reason)``, when it closed because of the credentials."""
    return peer_close if peer_close is not None and peer_close[0] in _CREDENTIAL_REFUSALS else None


def _credential_refusal(error: Exception) -> tuple[int, str] | None:
    """The peer's application close, when the session failed with one that refused the credentials.

    Read from the error's shape (TA-22, TA-24): a session error, or one with no ``source``, carrying the
    peer's ``close_code`` and ``reason``. A stream error never reports a close.
    """
    source = getattr(error, "source", None)
    close_code = getattr(error, "close_code", None)

    if source not in (None, "session") or not isinstance(close_code, int) or isinstance(close_code, bool):
        return None

    reason = getattr(error, "reason", None)
    return _refusal((close_code, reason if isinstance(reason, str) else ""))


def _peer_close(transport: object) -> tuple[int, str] | None:
    """The peer's application CONNECTION_CLOSE ``(code, reason)`` on a native QUIC session, once it arrived.

    A native QUIC session ends cleanly on any application close (TA-9), so a refusal reaches the SDK
    only through this, not through the error the session ends with.
    """
    # Imported here: the quic package imports the connection code, which imports this module.
    from .quic._client import QuicSession

    return transport.close_info if isinstance(transport, QuicSession) else None


def _stream_stopped(error: BaseException) -> bool:
    """Whether a send-stream failure is the peer's STOP_SENDING on that stream, not a session failure.

    A stream error (TA-22) on a send stream can only be a stop (TA-20): where it surfaces makes it one,
    not its class. The counterpart of ``isStreamStop`` in ``@caprid/relay``.
    """
    return is_stream_error(error)


__all__ = ["RelayError", "RelayErrorCode", "RelayErrorPhase"]
