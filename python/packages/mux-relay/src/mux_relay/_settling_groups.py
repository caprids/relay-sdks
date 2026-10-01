"""A publisher connection's settling groups (see ``CONTEXT.md``).

A settling group is one whose operation completed once its FIN was handed to the transport, but whose
FIN the relay has not acknowledged yet. aioquic queues without limit, so without a bound a dead uplink
keeps accepting groups until the congestion window fills (about 190 one-kilobyte groups). Settling
groups are what can pile up on a connection: open streams are already bounded by one per track. So
before an operation opens its stream it waits while the connection has :data:`SETTLING_GROUP_LIMIT`
settling groups, first come first served; it holds nothing while its stream is open, and its group
joins the set once its FIN is handed over. The count can briefly exceed the limit by the number of
groups open at that moment. The TypeScript SDK keeps the same set (``ts/src/settling-groups.ts``).

A settling group that fails later (STOP_SENDING, a reset, session teardown) leaves the set like one
that settles cleanly, and nobody hears of it: the cause surfaces as demand, a rebind, or the
connection's ``closed``. A group with a SUBGROUP_DELIVERY_TIMEOUT (draft-ietf-moq-transport-20 section
8) carries its timer here: started when the group joins, it resets the stream if the acknowledgement
has not come by then, which fails the acknowledgement wait, so the group leaves the set the same
silent way and frees its room. The timer belongs to the group: a subscription change or end and the
track's close leave it running, and only the acknowledgement or the connection's end settles the
group first. ``RelayPublisher.close()`` drains the set before it ends the session, because closing
the connection discards whatever the relay has not acknowledged; delivery timeouts only end some of
the drained groups sooner.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable
from typing import Any

from moq_core._tasks import observe, spawn

SETTLING_GROUP_LIMIT = 64
"""Settling groups per publisher connection before new streams wait. Internal, not an option."""


async def _settled(acknowledged: Awaitable[None]) -> None:
    try:
        await acknowledged
    except Exception:  # a late failure is dropped: it surfaces elsewhere (see the module doc)
        pass


class SettlingGroups:
    """The settling groups of one publisher connection, shared by every track on it."""

    __slots__ = ("_limit", "_settling", "_waiting")

    def __init__(self, limit: int = SETTLING_GROUP_LIMIT) -> None:
        self._limit = limit
        self._settling: set[asyncio.Task[None]] = set()
        self._waiting: deque[asyncio.Future[None]] = deque()

    @property
    def size(self) -> int:
        """How many groups are settling now."""
        return len(self._settling)

    def has_room(self) -> bool:
        """Whether a new stream may open now: the connection is below the limit."""
        return len(self._settling) < self._limit

    async def wait_for_room(self, interrupted: asyncio.Future[None]) -> None:
        """Wait until the connection is below the limit, in arrival order.

        Raises the exception ``interrupted`` settles with, if that comes first; the wait then gives up
        its place in the queue.
        """
        if self.has_room():
            return

        waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._waiting.append(waiter)

        try:
            await asyncio.wait({waiter, interrupted}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            if not waiter.done():
                self._waiting.remove(waiter)
                waiter.cancel()

        if not waiter.done() or waiter.cancelled():
            interrupted.result()

    def add(
        self,
        acknowledged: Awaitable[None],
        *,
        timeout: float | None = None,
        on_timeout: Callable[[], None] | None = None,
    ) -> None:
        """A group's FIN was handed over: count it until ``acknowledged`` settles, either way.

        With a ``timeout`` (seconds, the group's SUBGROUP_DELIVERY_TIMEOUT), ``on_timeout`` runs if
        ``acknowledged`` is still pending by then; it resets the group's stream, which settles
        ``acknowledged`` with the reset.
        """
        tracked = spawn(_settled(acknowledged))
        self._settling.add(tracked)
        tracked.add_done_callback(self._settle)

        if timeout is not None and on_timeout is not None:
            timer = asyncio.get_running_loop().call_later(timeout, on_timeout)
            tracked.add_done_callback(lambda _: timer.cancel())

    async def drain(self, seconds: float, stop: asyncio.Future[Any], *also: Awaitable[None]) -> None:
        """Wait for every settling group and each of ``also``, for at most ``seconds`` or until ``stop`` settles.

        Never raises; whatever is still unsettled afterwards is left to the transport.
        """
        # Futures at once, so a wait cancelled before it starts still counts as awaited.
        extra = [asyncio.ensure_future(awaitable) for awaitable in also]
        waits = [*self._settling, *extra]

        try:
            if waits and not stop.done():
                everything = asyncio.gather(*waits, return_exceptions=True)
                observe(everything)
                await asyncio.wait({everything, stop}, timeout=seconds, return_when=asyncio.FIRST_COMPLETED)
        finally:
            # Only the extra waits are ours to stop; the settling groups keep counting.
            for wait in extra:
                wait.cancel()

    def _settle(self, tracked: asyncio.Task[None]) -> None:
        self._settling.discard(tracked)

        # Every waiter goes on, in arrival order, once the connection is below the limit again.
        while self._waiting and self.has_room():
            waiter = self._waiting.popleft()

            if not waiter.done():
                waiter.set_result(None)


__all__ = ["SETTLING_GROUP_LIMIT", "SettlingGroups"]
