"""Exceptions the Relay SDK raises. Messages match the TypeScript SDK so the two read alike in logs."""

from __future__ import annotations


class RelayError(Exception):
    """Base class for every failure the SDK reports about a Relay connection."""


class RelayClosedError(RelayError):
    """The connection, publisher, or track is closed and cannot accept the operation."""


class RelayTrackEndedError(RelayClosedError):
    """A failure while a write was bound to a relay subscription retired that track.

    A :class:`RelayClosedError`, because the track is terminally closed and its name cannot be
    reused on this connection, which is what tells it apart from a retryable :class:`RelayTimeoutError`
    waiting for demand.
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
