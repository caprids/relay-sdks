"""Stable Relay failure identities. Messages are diagnostic text, not a programmatic contract."""

from __future__ import annotations

from typing import Literal

RelayErrorCode = Literal[
    "RELAY_ERROR",
    "CONNECTION_TIMEOUT",
    "WRITE_TIMEOUT",
    "CLOSED",
    "WRITE_PENDING",
    "NAMESPACE_NOT_SOLICITED",
    "NAMESPACE_ENDED",
    "SUBSCRIPTION_CHANGED",
    "GOAWAY",
    "TRANSPORT_ERROR",
    "TRANSPORT_UNAVAILABLE",
]
RelayErrorPhase = Literal[
    "connecting",
    "connection",
    "waiting-for-demand",
    "opening-stream",
    "writing",
    "closing",
]


class RelayError(Exception):
    """Base SDK failure with stable ``code`` and origin ``phase``; neither promises safe replay."""

    # WebTransportError retains its pre-existing numeric application code.
    code: RelayErrorCode | int | None = "RELAY_ERROR"

    def __init__(
        self, message: str = "", *, phase: RelayErrorPhase = "connection", code: RelayErrorCode | None = None
    ) -> None:
        super().__init__(message)
        self.phase = phase
        if code is not None:
            self.code = code


class RelayClosedError(RelayError):
    """The connection, publisher, or track is closed and cannot accept the operation."""

    code: RelayErrorCode = "CLOSED"


class RelayTrackEndedError(RelayClosedError):
    """Legacy terminal-track error, retained for import compatibility.

    Individual write failures now reset their stream and keep the track open. Subscription
    changes raise :class:`RelayError`; explicitly closed tracks raise :class:`RelayClosedError`.
    """


class RelayNamespaceError(RelayError):
    """The relay's namespace solicitations do not cover the publisher namespace; check token and namespace."""

    code: RelayErrorCode = "NAMESPACE_NOT_SOLICITED"


class RelayTimeoutError(RelayError):
    """A deadline elapsed. Inspect ``phase`` before deciding what to publish next."""

    def __init__(self, message: str = "", *, phase: RelayErrorPhase = "writing") -> None:
        super().__init__(message, phase=phase, code="CONNECTION_TIMEOUT" if phase == "connecting" else "WRITE_TIMEOUT")


class RelayGoawayError(RelayError):
    """The relay asked this session to migrate; open a replacement connection."""

    code: RelayErrorCode = "GOAWAY"


class RelayWritePendingError(RelayError):
    """A second write was attempted on a track while one is still in flight."""

    code: RelayErrorCode = "WRITE_PENDING"


def _transport_error(error: BaseException, phase: RelayErrorPhase) -> BaseException:
    # asyncio task cancellation and existing Relay errors keep their identity.
    if not isinstance(error, Exception) or (isinstance(error, RelayError) and isinstance(error.code, str)):
        return error
    wrapped = RelayError(str(error), code="TRANSPORT_ERROR", phase=phase)
    wrapped.__cause__ = error
    return wrapped


__all__ = [
    "RelayClosedError",
    "RelayError",
    "RelayErrorCode",
    "RelayErrorPhase",
    "RelayGoawayError",
    "RelayNamespaceError",
    "RelayTimeoutError",
    "RelayTrackEndedError",
    "RelayWritePendingError",
]
