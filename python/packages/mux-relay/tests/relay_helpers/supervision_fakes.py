"""Scripted connections for the supervisor tests: each fake stands in for one ``connect_relay_publisher``
or ``connect_relay_subscriber`` result, and the test drives what the relay would do to it.

Mirrors ``ts/tests/helpers/supervision-fakes.ts``.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime

from moq_core import Closed, KeyValuePair, MoqtObject, PublishDone, RequestError, SubscribeOk
from mux_relay import (
    NamespaceEvent,
    RelayAwaitable,
    RelayError,
    RelaySubscriptionHandlers,
    SubscriptionConnectionClosedError,
    SubscriptionEnd,
)


async def until(predicate: Callable[[], bool], label: str = "condition", timeout: float = 1.0) -> None:
    """Poll until ``predicate`` holds, failing after ``timeout`` seconds."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError(f"Timed out waiting for {label}")
        await asyncio.sleep(0.001)


class FakeTrack:
    """One track registered on a :class:`FakePublisher`, with the writer the supervisor binds to."""

    def __init__(self, name: str, track_properties: list[KeyValuePair], write_timeout: float) -> None:
        self.name = name
        self.track_properties = track_properties
        self.write_timeout = write_timeout
        self.writes: list[bytes] = []
        self.started: list[float] = []
        """When each supervised write or open_group said the application made its call."""
        self.fail_next: BaseException | None = None
        self.closed = False
        self._demand = False
        self._queues: set[asyncio.Queue[bool | None]] = set()
        self.groups: list[FakeGroup] = []
        self.writer = _FakeWriter(self)

    def reset_groups(self) -> None:
        """Close every open group the way the primitive does when the relay resets demand: no wrapper method runs."""
        for group in self.groups:
            group.closed = True

    def set_demand(self, value: bool) -> None:
        self._demand = value
        for queue in list(self._queues):
            queue.put_nowait(value)

    def end(self) -> None:
        self.closed = True
        for queue in list(self._queues):
            queue.put_nowait(None)


class _FakeWriter:
    def __init__(self, track: FakeTrack) -> None:
        self._track = track

    @property
    def has_demand(self) -> bool:
        return self._track._demand and not self._track.closed

    @property
    def closed(self) -> bool:
        return self._track.closed

    def demand_changes(self) -> AsyncIterator[bool]:
        queue: asyncio.Queue[bool | None] = asyncio.Queue()
        queue.put_nowait(None if self._track.closed else self._track._demand)
        self._track._queues.add(queue)

        async def iterate() -> AsyncIterator[bool]:
            try:
                while True:
                    item = await queue.get()
                    if item is None:
                        return
                    yield item
            finally:
                self._track._queues.discard(queue)

        return iterate()

    async def write(self, payload: bytes, *, properties: Sequence[KeyValuePair] | None = None) -> None:
        if self._track.closed:
            raise RelayError("Published track is closed", code="CLOSED", phase="waiting-for-demand")
        if self._track.fail_next is not None:
            error, self._track.fail_next = self._track.fail_next, None
            raise error
        self._track.writes.append(bytes(payload))

    async def _write_from(
        self, started: float, payload: bytes, properties: Sequence[KeyValuePair] | None = None
    ) -> None:
        self._track.started.append(started)
        await self.write(payload, properties=properties)

    async def _open_group_from(
        self, started: float, payload: bytes | None = None, properties: Sequence[KeyValuePair] | None = None
    ) -> FakeGroup:
        self._track.started.append(started)
        return await self.open_group(payload, properties=properties)

    async def open_group(
        self, payload: bytes | None = None, *, properties: Sequence[KeyValuePair] | None = None
    ) -> FakeGroup:
        if self._track.closed:
            raise RelayError("Published track is closed", code="CLOSED", phase="waiting-for-demand")
        return FakeGroup(self._track)

    def close(self) -> None:
        self._track.end()


class FakeGroup:
    """One open group on a :class:`FakeTrack`; its writes fail once the track's connection ended."""

    def __init__(self, track: FakeTrack) -> None:
        self._track = track
        self.group_id = len(track.writes)
        self.objects: list[bytes] = []
        self.closed = False
        track.groups.append(self)

    @property
    def object_count(self) -> int:
        return len(self.objects)

    async def write(self, payload: bytes, **_: object) -> None:
        if self._track.closed or self.closed:
            raise RelayError("Group is closed", code="CLOSED", phase="writing")
        self.objects.append(bytes(payload))

    async def close(self) -> None:
        if self._track.closed:
            raise RelayError("Published track is closed", code="CLOSED", phase="closing")
        if self._track.fail_next is not None:
            error, self._track.fail_next = self._track.fail_next, None
            raise error
        self.closed = True

    def abort(self, reason: BaseException | None = None) -> None:
        self.closed = True


class FakePublisher:
    """A publisher connection the test ends at will."""

    def __init__(self, namespace: Sequence[str], expires_at: datetime | None = None) -> None:
        self.namespace = tuple(namespace)
        self.expires_at = expires_at
        self.tracks: dict[str, FakeTrack] = {}
        self.close_calls = 0
        self._ended = False
        self._closed: asyncio.Future[Closed] = asyncio.get_running_loop().create_future()
        self.closed = RelayAwaitable(self._closed)

    def end(self, error: BaseException | None = None) -> None:
        """The connection ends, with ``error`` or cleanly."""
        if self._ended:
            return
        self._ended = True
        for track in self.tracks.values():
            track.end()
        self._closed.set_result(Closed(error))

    async def close(self) -> None:
        self.close_calls += 1
        self.end()

    def register_track(
        self, track: str, *, track_properties: Sequence[KeyValuePair] | None = None, write_timeout: float = 10.0
    ) -> _FakeWriter:
        if self._ended:
            raise RelayError("Relay publisher is closed", code="CLOSED")
        if track in self.tracks:
            raise ValueError("Track name is already registered")
        registered = FakeTrack(track, list(track_properties or []), write_timeout)
        self.tracks[track] = registered
        return registered.writer


class FakeSubscription:
    """One SUBSCRIBE on a :class:`FakeSubscriber`; the test answers it."""

    def __init__(
        self,
        namespace: tuple[str, ...],
        track: str,
        handlers: RelaySubscriptionHandlers,
        parameters: object,
        closers: set[Callable[[BaseException | None], None]],
    ) -> None:
        self.namespace = namespace
        self.track = track
        self.handlers = handlers
        self.parameters = parameters
        self.cancelled = False
        self._accepted = False
        self._closers = closers
        self._ended: asyncio.Future[SubscriptionEnd] = asyncio.get_running_loop().create_future()
        self.ended = RelayAwaitable(self._ended)
        closers.add(self._on_close)

    def _finish(self, end: SubscriptionEnd) -> None:
        if self._ended.done():
            return
        self._closers.discard(self._on_close)
        self._ended.set_result(end)

    def _on_close(self, error: BaseException | None) -> None:
        self._finish(
            SubscriptionEnd(kind="terminated", cause="connection-closed", accepted=self._accepted, error=error)
        )

    def accept(self) -> None:
        self._accepted = True
        if self.handlers.on_subscribed is not None:
            self.handlers.on_subscribed(SubscribeOk(track_alias=1))

    def reject(self, error_code: int, reason: str) -> None:
        error = RequestError(error_code=error_code, retry_interval=0, reason=reason)
        if self.handlers.on_error is not None:
            self.handlers.on_error(error)
        self._finish(SubscriptionEnd(kind="rejected", cause="request-error", accepted=False, error=error))

    def send_object(self, group_id: int, object_id: int = 0) -> None:
        if self._ended.done() or self.handlers.on_object is None:
            return
        self.handlers.on_object(
            MoqtObject(group_id=group_id, subgroup_id=0, object_id=object_id, payload=bytes([group_id]))
        )

    def done(self) -> None:
        if self.handlers.on_publisher_done is not None:
            self.handlers.on_publisher_done(PublishDone(status_code=2, stream_count=0, reason="track ended"))

    def terminate(self, error_code: int, reason: str) -> None:
        error = RequestError(error_code=error_code, retry_interval=0, reason=reason)
        self._finish(SubscriptionEnd(kind="terminated", cause="request-error", accepted=True, error=error))

    def cancel(self, reason: object = None) -> None:
        self.cancelled = True
        self._finish(SubscriptionEnd(kind="cancelled", cause="cancelled", accepted=self._accepted, error=reason))


@dataclass(eq=False)
class FakeDiscovery:
    """One ``namespace_events`` request; the test pushes events or fails it."""

    prefix: tuple[str, ...]
    closed: bool = False
    _queue: list[NamespaceEvent] = field(default_factory=list)
    _failure: list[BaseException] = field(default_factory=list)
    _wake: asyncio.Event = field(default_factory=asyncio.Event)

    def push(self, kind: str, namespace: Sequence[str]) -> None:
        self._queue.append(NamespaceEvent(kind, tuple(namespace)))  # ty: ignore[invalid-argument-type]
        self._wake.set()

    def fail(self, error: BaseException) -> None:
        self._failure.append(error)
        self._wake.set()

    def __aiter__(self) -> FakeDiscovery:
        return self

    async def __anext__(self) -> NamespaceEvent:
        while not self._queue and not self._failure and not self.closed:
            self._wake.clear()
            await self._wake.wait()
        if self.closed:
            raise StopAsyncIteration
        if self._queue:
            return self._queue.pop(0)
        raise self._failure[0]

    def close(self) -> None:
        self.closed = True
        self._wake.set()


class FakeSubscriber:
    """A subscriber connection the test ends at will."""

    def __init__(self, expires_at: datetime | None = None) -> None:
        self.expires_at = expires_at
        self.subscriptions: list[FakeSubscription] = []
        self.discoveries: list[FakeDiscovery] = []
        self._ended = False
        self._closers: set[Callable[[BaseException | None], None]] = set()
        self._closed: asyncio.Future[Closed] = asyncio.get_running_loop().create_future()
        self.closed = RelayAwaitable(self._closed)
        self.refuse_subscribe: BaseException | None = None
        self.refuse_discovery: BaseException | None = None

    def end(self, error: BaseException | None = None) -> None:
        if self._ended:
            return
        self._ended = True
        for close in list(self._closers):
            close(error)
        for discovery in self.discoveries:
            if not discovery.closed:
                discovery.fail(SubscriptionConnectionClosedError(error))
        self._closed.set_result(Closed(error))

    def close(self) -> None:
        self.end()

    def subscribe(
        self,
        namespace: Sequence[str],
        track: str,
        handlers: RelaySubscriptionHandlers | None = None,
        *,
        parameters: object = None,
    ) -> FakeSubscription:
        if self.refuse_subscribe is not None:
            raise self.refuse_subscribe
        if self._ended:
            raise RelayError("Relay connection is closed", code="CLOSED")
        subscription = FakeSubscription(
            tuple(namespace), track, handlers or RelaySubscriptionHandlers(), parameters, self._closers
        )
        self.subscriptions.append(subscription)
        return subscription

    def namespace_events(self, prefix: Sequence[str], *, capacity: int = 64) -> FakeDiscovery:
        if self.refuse_discovery is not None:
            raise self.refuse_discovery
        if self._ended:
            raise RelayError("Relay connection is closed", code="CLOSED")
        discovery = FakeDiscovery(tuple(prefix))
        self.discoveries.append(discovery)
        return discovery

    @property
    def latest(self) -> FakeSubscription:
        return self.subscriptions[-1]


__all__ = ["FakeDiscovery", "FakeGroup", "FakePublisher", "FakeSubscriber", "FakeSubscription", "FakeTrack", "until"]
