"""Polling assertions, the ``vi.waitFor`` of these tests."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable


async def wait_for(check: Callable[[], object], *, timeout: float = 2.0, interval: float = 0.002) -> None:
    """Poll ``check`` until it returns a truthy value or stops raising.

    A ``check`` that returns ``None`` (an assertion-style callable) passes as soon as it stops
    raising. Fails with the last error once ``timeout`` seconds have elapsed. Like ``vi.waitFor``,
    a pass lets the loop turn a couple of times first, so continuations already scheduled by the
    observed event (a settled write, a resolved future) have run before the test goes on.
    """
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None

    while True:
        try:
            result = check()
        except Exception as error:
            last_error = error
        else:
            if result is None or result:
                for _ in range(3):
                    await asyncio.sleep(0)

                return

            last_error = AssertionError(f"condition still false: {result!r}")

        if time.monotonic() >= deadline:
            raise TimeoutError(f"wait_for timed out after {timeout}s") from last_error

        await asyncio.sleep(interval)


async def settle(seconds: float = 0.02) -> None:
    """Let the loop run for a while, for "nothing else happens" assertions."""
    await asyncio.sleep(seconds)
