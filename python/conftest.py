"""Shared pytest configuration for both packages' test suites."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest


@pytest.fixture(autouse=True)
async def _cancel_leftover_tasks() -> AsyncIterator[None]:
    """Cancel background tasks a test left running so teardown stays quiet.

    Session drivers spawn accept loops that end with their transport; a test that never closes its
    transport would otherwise leave them pending when the loop closes.
    """
    yield

    current = asyncio.current_task()
    leftovers = [task for task in asyncio.all_tasks() if task is not current and not task.done()]

    for task in leftovers:
        task.cancel()

    if leftovers:
        await asyncio.gather(*leftovers, return_exceptions=True)
