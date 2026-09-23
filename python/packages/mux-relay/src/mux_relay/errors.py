"""Stable Relay failure identities. Messages are diagnostic text, not a programmatic contract.

One exception type carries every Relay failure: :class:`RelayError`, with a ``code`` to branch on and
the ``phase`` the failure came from, mirroring ``RelayError`` in ``@mux/relay``. The three
subscription errors in :mod:`mux_relay._subscription` subclass it because they carry payload; nothing
else does. Transport-layer exceptions are not Relay errors; a Relay operation that fails on the
transport raises ``TRANSPORT_ERROR`` with the transport exception chained as ``__cause__``.
"""

from __future__ import annotations

from typing import Literal

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


def _transport_error(error: BaseException, phase: RelayErrorPhase) -> BaseException:
    # asyncio task cancellation and Relay errors keep their identity; anything else, the transport's
    # own exceptions included, becomes a Relay failure with the original chained as its cause.
    if not isinstance(error, Exception) or isinstance(error, RelayError):
        return error
    wrapped = RelayError(str(error), code="TRANSPORT_ERROR", phase=phase)
    wrapped.__cause__ = error
    return wrapped


__all__ = ["RelayError", "RelayErrorCode", "RelayErrorPhase"]
