"""The workaround for aioquic dropping a FIN-only frame under a full congestion window (#47).

``QuicConnection._write_stream_frame`` (aioquic 1.2 through 1.3) asks the sender for its frame before it
knows whether the packet has room. ``get_frame``'s FIN-only branch clears the queued FIN regardless, so
when ``start_frame`` then raises ``QuicPacketBuilderStop`` the FIN has been consumed but never sent, and
nothing reschedules it: the peer never sees the stream end and a group is never marked complete. This
class sweeps the streams whose FIN was queued and re-arms any that aioquic consumed without sending.

A time-based sweep, not a per-transmit one: a FIN can be dropped and the window then open before the next
transmit, so a congestion-gated retry would miss it. A FIN that is in a packet still in flight is left to
aioquic's own loss recovery, which queues it again if that packet is lost; only a FIN in no packet at all
is re-armed. On a dead link that matters: re-arming in-flight FINs every interval would fill each probe
packet with FIN-only frames, starve the data of a blocked write and let its deadline fire (#160). A
duplicate FIN, if one ever leaves, is harmless: the peer's final size is unchanged.

To remove the workaround once a fixed aioquic is the floor, delete this module, its construction and
:meth:`DroppedFinRetry.stop` calls in ``_protocol``, the :meth:`DroppedFinRetry.watch` call in
``_session`` where a FIN is queued, ``tests/test_quic_fin_retry.py``, and ``fin_unsent``,
``fins_in_flight`` and ``rearm_fin`` in ``_aioquic`` with their block of the pin test.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

from aioquic.quic.connection import QuicConnection

from ._aioquic import acknowledged, fin_unsent, fins_in_flight, live_stream, rearm_fin, reset_pending

_FIN_RETRY_INTERVAL = 0.25
"""Seconds between sweeps."""


class DroppedFinRetry:
    """Re-queues the FINs aioquic consumed under a full congestion window but never sent."""

    __slots__ = ("_handle", "_interval", "_loop", "_quic", "_stopped", "_transmit", "_watched")

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        quic: QuicConnection,
        transmit: Callable[[], None],
        *,
        interval: float = _FIN_RETRY_INTERVAL,
    ) -> None:
        self._loop = loop
        self._quic = quic
        self._transmit = transmit
        self._interval = interval
        self._watched: set[int] = set()
        """Streams whose FIN was queued and is not yet known to have landed or to be impossible."""
        self._handle: asyncio.TimerHandle | None = None
        self._stopped = False

    @property
    def idle(self) -> bool:
        """No stream is watched and no sweep is scheduled."""
        return self._handle is None and not self._watched

    def watch(self, stream_id: int) -> None:
        """A FIN was queued on ``stream_id``: sweep it until it is acknowledged, reset, or the stream is discarded."""
        if self._stopped:
            return

        self._watched.add(stream_id)
        self._schedule()

    def stop(self) -> None:
        """The connection is over: nothing can be re-armed any more."""
        self._stopped = True
        self._watched.clear()

        if self._handle is not None:
            self._handle.cancel()
            self._handle = None

    def sweep(self) -> None:
        """One pass, now (the timer's callback): forget the streams whose FIN landed or never can, re-arm the rest."""
        self._handle = None

        if self._stopped:
            return

        rearmed = False
        in_flight = fins_in_flight(self._quic) if self._watched else set()

        for stream_id in list(self._watched):
            stream = live_stream(self._quic, stream_id)

            if stream is None or reset_pending(stream) or acknowledged(stream):
                self._watched.discard(stream_id)
            elif fin_unsent(stream, in_flight):
                rearm_fin(stream)
                rearmed = True

        if rearmed:
            self._transmit()

        self._schedule()

    def _schedule(self) -> None:
        if self._handle is None and self._watched and not self._stopped:
            self._handle = self._loop.call_later(self._interval, self.sweep)
