"""One fake ``RelayTrackWriter`` for the media module's tests, typed against the module's own protocol.

It records what the frame writer and the catalog service do to a track (writes, groups, objects,
FINs, resets), lets a test script a failure at a given step and call, drive relay demand, and end
the track the way the SDK does (``closed`` becomes true and the demand iterator finishes).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from typing import Literal

from moq_core import KeyValuePair, ObjectStatus
from mux_relay import RelayError, RelayErrorPhase

Step = Literal["write", "open", "group-write", "group-close"]


class FakeGroupWriter:
    """One open group as the frame writer sees it."""

    def __init__(self, track: FakeTrackWriter, group_id: int) -> None:
        self._track = track
        self.group_id = group_id
        self.objects: list[tuple[bytes, tuple[KeyValuePair, ...]]] = []
        self.finished = False
        self.aborts: list[object] = []
        self.failure: BaseException | None = None
        """Like the real group writer: once an operation was cancelled, every later step raises that reason."""
        self.aborted = asyncio.Event()

    @property
    def payloads(self) -> list[bytes]:
        return [payload for payload, _ in self.objects]

    async def write(
        self,
        payload: bytes,
        *,
        properties: Sequence[KeyValuePair] | None = None,
        status: ObjectStatus = "normal",
    ) -> None:
        if self.failure is not None:
            raise self.failure

        await self._track._step("group-write", self)
        self.objects.append((payload, tuple(properties or ())))

    async def close(self) -> None:
        if self.failure is not None:
            raise self.failure

        await self._track._step("group-close", self)
        self.finished = True

    def abort(self, reason: BaseException | None = None) -> None:
        self.aborts.append(reason)

        if not self.finished and self.failure is None:
            # Like the real group writer: a write in progress, and every later step, fails with the reason.
            aborted = RelayError("Group was aborted", code="CLOSED", phase="writing")
            self.failure = reason if reason is not None else aborted
            self.aborted.set()


class FakeTrackWriter:
    """Enough of ``RelayTrackWriter`` for ``mux_relay.media``: demand, closure, writes and groups.

        ``fail(step, error, on=n)`` makes the n-th call of that step (counting from 0) raise ``error``;
        ``retire=True`` also ends the track first, the way a closed connection does. ``block`` holds
        every group write until the event is set, for tests where time has to pass inside a write; aborting
    the group fails a held write with the abort's reason, as the SDK does.
    """

    def __init__(self, *, demand: bool = True, block: asyncio.Event | None = None) -> None:
        self.has_demand = demand
        self.closed = False
        self.block = block
        self.writes: list[bytes] = []
        self.groups: list[FakeGroupWriter] = []
        self.blocked = asyncio.Event()
        self.calls: dict[Step, int] = {"write": 0, "open": 0, "group-write": 0, "group-close": 0}
        self._failures: dict[tuple[Step, int], tuple[BaseException, bool]] = {}
        self._watchers: list[asyncio.Queue[bool | None]] = []

    # ------------------------------------------------------------------ scripting

    def fail(self, step: Step, error: BaseException, *, on: int = 0, retire: bool = False) -> None:
        self._failures[(step, on)] = (error, retire)

    def bind(self, demand: bool) -> None:
        """The relay subscribed (``True``, repeated for a replacement subscription) or unsubscribed."""
        self.has_demand = demand

        if not self.closed:
            for queue in self._watchers:
                queue.put_nowait(demand)

    def end(self) -> None:
        """The track ended: no demand, ``closed``, and every demand iterator finishes."""
        self.closed = True
        self.has_demand = False

        for queue in self._watchers:
            queue.put_nowait(None)

    async def _step(self, step: Step, group: FakeGroupWriter | None) -> None:
        index = self.calls[step]
        self.calls[step] += 1
        failure = self._failures.pop((step, index), None)

        if step == "group-write" and self.block is not None and group is not None:
            self.blocked.set()
            released = asyncio.ensure_future(self.block.wait())
            aborted = asyncio.ensure_future(group.aborted.wait())

            try:
                await asyncio.wait([released, aborted], return_when=asyncio.FIRST_COMPLETED)
            except BaseException as error:
                group.failure = error
                raise
            finally:
                released.cancel()
                aborted.cancel()

            if group.failure is not None:
                # Aborted while the write was held, as an overflowing frame writer does.
                raise group.failure

        if failure is not None:
            error, retire = failure

            if retire:
                self.end()

            raise error

    # ------------------------------------------------------------------ the track writer interface

    def demand_changes(self) -> AsyncIterator[bool]:
        queue: asyncio.Queue[bool | None] = asyncio.Queue()
        queue.put_nowait(None if self.closed else self.has_demand)
        self._watchers.append(queue)
        return self._iterate(queue)

    async def _iterate(self, queue: asyncio.Queue[bool | None]) -> AsyncIterator[bool]:
        try:
            while True:
                item = await queue.get()

                if item is None:
                    return

                yield item
        finally:
            self._watchers.remove(queue)

    async def write(self, payload: bytes, *, properties: Sequence[KeyValuePair] | None = None) -> None:
        await self._step("write", None)
        self.writes.append(payload)

    async def open_group(self) -> FakeGroupWriter:
        await self._step("open", None)
        group = FakeGroupWriter(self, len(self.groups))
        self.groups.append(group)
        return group


def closed_error() -> RelayError:
    return RelayError("Published track is closed", code="CLOSED", phase="writing")


def changed_error() -> RelayError:
    return RelayError(
        "Relay subscription changed during a write or group; objects may be incomplete",
        code="SUBSCRIPTION_CHANGED",
        phase="writing",
    )


def timeout_error(phase: RelayErrorPhase = "writing") -> RelayError:
    message = "Publish write timed out waiting for relay demand or transport"
    return RelayError(message, code="WRITE_TIMEOUT", phase=phase)
