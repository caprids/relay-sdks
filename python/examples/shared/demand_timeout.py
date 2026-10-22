"""The demand timeout, and the application's side of it.

``write()`` waits for relay demand before it sends anything, and its write deadline covers that wait
and the transport steps after it together. A ``RelayError`` with code ``WRITE_TIMEOUT`` in phase
``waiting-for-demand`` therefore proves the attempt sent no object bytes: no subscriber had asked
for the track yet, and writing the same object again cannot duplicate it. A failure in a sending
phase (``opening-stream``, ``writing``, ``closing``) proves nothing of the kind: bytes may already
have reached the relay, so the same payload is never resent and the caller chooses what comes next
(``spec/behavior.md``, "Write phases and interruptions"). This module is that policy, used by the
mux-global demo. It branches on the error's code and phase, never on its message.
"""

from __future__ import annotations

from collections.abc import Callable

from caprid_relay import RelayError, RelayTrackWriter

SENDING_PHASES = frozenset({"opening-stream", "writing", "closing"})
"""The phases after which a failed write may have delivered some or all of its bytes."""


def is_demand_timeout(error: BaseException) -> bool:
    """Whether ``error`` is a write deadline that expired before the attempt sent anything."""
    return isinstance(error, RelayError) and error.code == "WRITE_TIMEOUT" and error.phase == "waiting-for-demand"


async def write_with_retry(
    writer: RelayTrackWriter,
    payload: bytes,
    *,
    on_retry: Callable[[int, RelayError], None] | None = None,
    on_uncertain: Callable[[RelayError], None] | None = None,
) -> None:
    """Write ``payload`` once, retrying only a demand timeout until a subscriber creates demand.

    Each attempt is one ``writer.write()`` with the track's own write deadline; the SDK does the
    waiting. ``on_retry(attempt, error)`` runs after a demand timeout, before the next attempt, and
    may raise to stop. Any other failure raises unchanged; when it happened in a sending phase,
    ``on_uncertain(error)`` runs first, because the relay may hold part or all of the object. Only
    cancellation bounds the retries: run it under ``asyncio.timeout`` or cancel the task, and the
    cancellation raises out of the SDK's wait at once.
    """
    attempt = 0

    while True:
        attempt += 1

        try:
            await writer.write(payload)
            return
        except RelayError as error:
            if not is_demand_timeout(error):
                if error.phase in SENDING_PHASES and on_uncertain is not None:
                    on_uncertain(error)

                raise

            if on_retry is not None:
                on_retry(attempt, error)


__all__ = ["SENDING_PHASES", "is_demand_timeout", "write_with_retry"]
