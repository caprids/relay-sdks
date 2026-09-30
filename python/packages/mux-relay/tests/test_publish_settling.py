"""Settling groups through the public publisher, against the in-memory relay hub (#160, #161).

The hub's transport reports no acknowledgement, so these tests wrap the publisher's end in one whose
send streams are acknowledged only when the test says so: the part of a real transport the close
drain waits on.
"""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from typing import Any

import pytest
from core_helpers.waiting import settle, wait_for
from relay_helpers.relay_hub import create_relay_hub

from moq_core.transport import AcknowledgedSendStream, BidirectionalStream, MoqtTransport, SendStream
from mux_relay import (
    ConnectingTransport,
    RelayPublisher,
    RelaySubscriber,
    RelaySubscriptionHandlers,
    connect_relay_publisher,
    connect_relay_subscriber,
)

NAMESPACE = ["testing", "settling"]
URL = "https://relay.example.com/moq"
SHORT = 0.2

Cleanup = list[Callable[[], Awaitable[None] | None]]


@pytest.fixture
async def cleanup() -> AsyncIterator[Cleanup]:
    steps: Cleanup = []
    yield steps

    for dispose in reversed(steps):
        result = dispose()

        if result is not None:
            await result


class AckedSend:
    """A send stream whose acknowledgement waits for the gate."""

    def __init__(self, inner: SendStream, gate: "AckGate") -> None:
        self._inner = inner
        self._gate = gate

    async def write(self, data: bytes) -> None:
        await self._inner.write(data)

    async def close(self) -> None:
        await self._inner.close()

    def abort(self, reason: object = None) -> None:
        self._inner.abort(reason)

    async def wait_acknowledged(self) -> None:
        self._gate.waiting += 1
        await asyncio.shield(self._gate.released)


class AckGate:
    """Holds every send stream's acknowledgement until :meth:`release`."""

    def __init__(self) -> None:
        self.released: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self.waiting = 0
        """Acknowledgements asked for so far."""

    def release(self) -> None:
        if not self.released.done():
            self.released.set_result(None)


class AckedTransport:
    """A transport whose send streams satisfy ``AcknowledgedSendStream``; records when it is closed."""

    def __init__(self, inner: MoqtTransport, gate: AckGate) -> None:
        self._inner = inner
        self._gate = gate
        self.closed_at: float | None = None

    async def _wrap_incoming(self) -> AsyncIterator[BidirectionalStream]:
        async for stream in self._inner.incoming_bidirectional_streams():
            yield BidirectionalStream(readable=stream.readable, writable=AckedSend(stream.writable, self._gate))

    def incoming_unidirectional_streams(self) -> Any:
        return self._inner.incoming_unidirectional_streams()

    def incoming_bidirectional_streams(self) -> AsyncIterator[BidirectionalStream]:
        return self._wrap_incoming()

    async def create_unidirectional_stream(self) -> SendStream:
        return AckedSend(await self._inner.create_unidirectional_stream(), self._gate)

    async def create_bidirectional_stream(self) -> BidirectionalStream:
        stream = await self._inner.create_bidirectional_stream()
        return BidirectionalStream(readable=stream.readable, writable=AckedSend(stream.writable, self._gate))

    def close(self, close_code: int = 0, reason: str = "") -> None:
        if self.closed_at is None:
            self.closed_at = asyncio.get_running_loop().time()
        self._inner.close(close_code, reason)

    async def wait_closed(self) -> None:
        await self._inner.wait_closed()


async def connect(cleanup: Cleanup) -> tuple[AckGate, AckedTransport, RelayPublisher, RelaySubscriber]:
    hub = create_relay_hub()
    cleanup.append(hub.destroy)
    gate = AckGate()
    transports: list[AckedTransport] = []

    def create_transport(url: str, protocols: Sequence[str]) -> ConnectingTransport:
        connecting = hub.connect_publisher(url, protocols)
        transport = AckedTransport(connecting.transport, gate)
        transports.append(transport)
        return ConnectingTransport(transport=transport, ready=connecting.ready)

    publisher = await connect_relay_publisher(URL, namespace=NAMESPACE, create_transport=create_transport)
    subscriber = await connect_relay_subscriber(URL, create_transport=hub.connect_subscriber)
    cleanup.append(subscriber.close)
    cleanup.append(publisher.close)
    cleanup.append(gate.release)  # runs first, so teardown does not wait out a write deadline
    return gate, transports[0], publisher, subscriber


def test_the_wrapper_reports_acknowledgement() -> None:
    assert issubclass(AckedSend, AcknowledgedSendStream)


async def subscribe(subscriber: RelaySubscriber, track: str) -> tuple[list[Any], list[Any]]:
    objects: list[Any] = []
    done: list[Any] = []
    subscriber.subscribe(
        NAMESPACE, track, RelaySubscriptionHandlers(on_object=objects.append, on_publisher_done=done.append)
    )
    return objects, done


async def test_writes_and_groups_complete_before_the_acknowledgement(cleanup: Cleanup) -> None:
    gate, _, publisher, subscriber = await connect(cleanup)
    track = publisher.register_track("events")
    objects, _ = await subscribe(subscriber, "events")

    await track.write(b"\x01")
    group = await track.open_group()
    await group.write(b"\x02")
    await group.write(b"\x03")
    await group.close()
    await track.write(b"\x04")

    await wait_for(lambda: len(objects) == 4)
    assert gate.waiting == 3
    assert not gate.released.done()


async def test_track_close_sends_publish_done_without_waiting_for_settling_groups(cleanup: Cleanup) -> None:
    gate, transport, publisher, subscriber = await connect(cleanup)
    track = publisher.register_track("events")
    objects, done = await subscribe(subscriber, "events")
    await track.write(b"\x01")

    track.close()

    await wait_for(lambda: len(done) == 1)
    assert done[0].status_code == 2
    assert len(objects) == 1
    assert not gate.released.done()
    assert transport.closed_at is None


async def test_publisher_close_waits_for_the_acknowledgements_before_ending_the_session(cleanup: Cleanup) -> None:
    gate, transport, publisher, subscriber = await connect(cleanup)
    track = publisher.register_track("events")
    objects, done = await subscribe(subscriber, "events")
    await track.write(b"\x01")
    await wait_for(lambda: len(objects) == 1)

    closing = asyncio.ensure_future(publisher.close())
    await wait_for(lambda: len(done) == 1)  # PUBLISH_DONE went out at once
    await settle(0.1)
    assert transport.closed_at is None
    assert not closing.done()

    released_at = asyncio.get_running_loop().time()
    gate.release()
    await asyncio.wait_for(closing, 1)
    assert transport.closed_at is not None
    assert transport.closed_at >= released_at
    assert (await publisher.closed).error is None


async def test_publisher_close_ends_the_session_after_the_largest_write_deadline_when_acks_never_come(
    cleanup: Cleanup,
) -> None:
    gate, transport, publisher, subscriber = await connect(cleanup)
    short = publisher.register_track("short", write_timeout=SHORT / 4)
    long = publisher.register_track("long", write_timeout=SHORT)
    short_objects, _ = await subscribe(subscriber, "short")
    long_objects, _ = await subscribe(subscriber, "long")
    await short.write(b"\x01")
    await long.write(b"\x02")
    await wait_for(lambda: len(short_objects) == 1 and len(long_objects) == 1)

    started = asyncio.get_running_loop().time()
    await asyncio.wait_for(publisher.close(), 2)

    assert transport.closed_at is not None
    assert transport.closed_at - started >= SHORT
    assert not gate.released.done()
    assert (await publisher.closed).error is None


async def test_publisher_close_does_not_wait_when_every_group_is_acknowledged(cleanup: Cleanup) -> None:
    gate, _, publisher, subscriber = await connect(cleanup)
    gate.release()
    track = publisher.register_track("events", write_timeout=5)
    objects, _ = await subscribe(subscriber, "events")
    await track.write(b"\x01")
    await wait_for(lambda: len(objects) == 1)

    started = asyncio.get_running_loop().time()
    await publisher.close()

    assert asyncio.get_running_loop().time() - started < 0.5
    assert (await publisher.closed).error is None


async def test_a_cancelled_close_still_ends_the_session(cleanup: Cleanup) -> None:
    gate, transport, publisher, subscriber = await connect(cleanup)
    track = publisher.register_track("events")
    objects, _ = await subscribe(subscriber, "events")
    await track.write(b"\x01")
    await wait_for(lambda: len(objects) == 1)

    closing = asyncio.ensure_future(publisher.close())
    await settle()
    closing.cancel()
    gate.release()

    await asyncio.wait_for(publisher.closed, 1)
    assert transport.closed_at is not None
