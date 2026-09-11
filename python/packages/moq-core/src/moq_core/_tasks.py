"""Background-task plumbing shared by the session drivers.

The TypeScript core fires promises with ``void`` and attaches ``.catch`` handlers where a failure
has a meaning. asyncio needs a strong reference to every task it should keep running, and an
unretrieved task exception is reported as a warning at garbage-collection time, so :func:`spawn`
keeps that reference and routes the outcome through an optional error handler.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Coroutine
from typing import Any

_log = logging.getLogger("moq_core")
_background: set[asyncio.Task[Any]] = set()


def spawn(
    coroutine: Coroutine[Any, Any, Any],
    *,
    on_error: Callable[[BaseException], None] | None = None,
) -> asyncio.Task[Any]:
    """Run ``coroutine`` in the background, keeping it referenced until it finishes.

    ``on_error`` receives an exception the coroutine raised (never a cancellation). Without it the
    exception is logged at debug level and otherwise dropped, the counterpart of ``.catch(() => {})``.
    """
    task = asyncio.get_running_loop().create_task(coroutine)
    _background.add(task)

    def finished(done: asyncio.Task[Any]) -> None:
        _background.discard(done)

        if done.cancelled():
            return

        error = done.exception()

        if error is None:
            return

        if on_error is not None:
            on_error(error)
        else:
            _log.debug("background task failed", exc_info=error)

    task.add_done_callback(finished)
    return task


def observe(future: asyncio.Future[Any]) -> None:
    """Mark a future's eventual exception as handled without acting on it."""

    def finished(done: asyncio.Future[Any]) -> None:
        if not done.cancelled():
            done.exception()

    future.add_done_callback(finished)


def call_later(delay: float, callback: Callable[[], None]) -> asyncio.TimerHandle:
    """Schedule ``callback`` after ``delay`` seconds on the running loop."""
    return asyncio.get_running_loop().call_later(delay, callback)


__all__ = ["call_later", "observe", "spawn"]
