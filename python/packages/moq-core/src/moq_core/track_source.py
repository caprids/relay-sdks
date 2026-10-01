"""Borrowed access to tracks on a session.

Consumers own their requests; only the connection owner can close the underlying transport. No
media format is implied.
"""

from __future__ import annotations

import asyncio
from typing import Protocol

from .session import (
    Closed,
    FetchHandle,
    FetchHandlers,
    MoqtSubscribeOptions,
    Subscription,
    SubscriptionHandlers,
    TrackStatusHandlers,
)


class MoqTrackSource(Protocol):
    """The subscribe / fetch / track-status subset of a session, plus closure observation, without shutdown."""

    @property
    def closed(self) -> asyncio.Future[Closed]:
        """Resolves when the owning connection ends; an error denotes failure."""
        ...

    def subscribe(self, options: MoqtSubscribeOptions, handlers: SubscriptionHandlers | None = None) -> Subscription:
        """Subscribe to a track, as :meth:`MoqtSession.subscribe` does; the caller owns the request."""
        ...

    def fetch(self, options: MoqtSubscribeOptions, handlers: FetchHandlers | None = None) -> FetchHandle:
        """Fetch a range of a track, as :meth:`MoqtSession.fetch` does; the caller owns the request."""
        ...

    def track_status(self, options: MoqtSubscribeOptions, handlers: TrackStatusHandlers | None = None) -> None:
        """Ask for a track's status, as :meth:`MoqtSession.track_status` does."""
        ...


__all__ = ["MoqTrackSource"]
