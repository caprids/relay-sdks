"""Exceptions the Relay SDK raises. Messages match the TypeScript SDK so the two read alike in logs."""

from __future__ import annotations


class RelayError(Exception):
    """Base class for every failure the SDK reports about a Relay connection."""


class RelayClosedError(RelayError):
    """The connection, publisher, or track is closed and cannot accept the operation."""


class RelayTrackEndedError(RelayClosedError):
    """Legacy terminal-track error, retained for import compatibility.

    Individual write failures now reset their stream and keep the track open. Subscription
    changes raise :class:`RelayError`; explicitly closed tracks raise :class:`RelayClosedError`.
    """


class RelayNamespaceError(RelayError):
    """The relay's namespace solicitations do not cover the publisher namespace; check token and namespace."""


class RelayTimeoutError(RelayError):
    """A deadline elapsed: connecting, or waiting for relay demand and the transport write."""


class RelayGoawayError(RelayError):
    """The relay asked this session to migrate; open a replacement connection."""


class RelayWritePendingError(RelayError):
    """A second write was attempted on a track while one is still in flight."""


__all__ = [
    "RelayClosedError",
    "RelayError",
    "RelayGoawayError",
    "RelayNamespaceError",
    "RelayTimeoutError",
    "RelayTrackEndedError",
    "RelayWritePendingError",
]
