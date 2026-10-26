"""The type of every fact the SDK settles once and lets any number of tasks await."""

from __future__ import annotations

import asyncio
import contextvars
from collections.abc import Callable, Generator
from typing import Any, Generic, TypeVar

_T = TypeVar("_T")


class RelayAwaitable(Generic[_T]):
    """A fact the SDK settles once: a subscription's ``subscribed`` or ``ended``, a publisher's
    ``announced``, or a connection's ``closed``.

    Await it as often as needed. Each waiter is isolated: a task cancelled while awaiting, or an
    ``asyncio.wait_for`` that times out, never touches the underlying result, where handing out the
    future itself would let one impatient waiter destroy the signal for every later one. ``done()``,
    ``result()``, ``exception()`` and ``add_done_callback()`` read the result directly and allocate
    nothing, so polling is free. It is not an :class:`asyncio.Future`: it cannot be cancelled or
    set. Wrap it with :func:`asyncio.ensure_future` for an API that needs a future.
    """

    __slots__ = ("_future",)

    def __init__(self, future: asyncio.Future[_T]) -> None:
        self._future = future

    def __await__(self) -> Generator[Any, None, _T]:
        future = self._future
        if not future.done():
            future = asyncio.shield(future)
        return future.__await__()

    def done(self) -> bool:
        """Whether the result is in."""
        return self._future.done()

    def result(self) -> _T:
        """The result; raises what awaiting would, or ``asyncio.InvalidStateError`` while pending."""
        return self._future.result()

    def exception(self) -> BaseException | None:
        """The exception awaiting would raise, ``None`` for a plain result."""
        return self._future.exception()

    def add_done_callback(
        self, callback: Callable[[asyncio.Future[_T]], object], *, context: contextvars.Context | None = None
    ) -> None:
        """Call ``callback`` with the settled future once the result is in."""
        self._future.add_done_callback(callback, context=context)

    def remove_done_callback(self, callback: Callable[[asyncio.Future[_T]], object]) -> int:
        """Forget a callback; returns how many registrations were removed."""
        return self._future.remove_done_callback(callback)


__all__ = ["RelayAwaitable"]
