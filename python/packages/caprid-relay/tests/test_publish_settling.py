"""Settling groups through the public publisher, against the in-memory relay hub (#160, #161, #167).

The hub's transport reports no acknowledgement, so these tests wrap the publisher's end in one whose
send streams are acknowledged only when the test says so: the part of a real transport the close
drain and a group's SUBGROUP_DELIVERY_TIMEOUT wait on.
"""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from typing import Any

import pytest
from core_helpers.waiting import settle, wait_for
from relay_helpers.relay_hub import RelayHub, create_relay_hub

from caprid_relay import (
    ConnectingTransport,
    RelayError,
    RelayPublisher,
    RelaySubscriber,
    RelaySubscriptionHandlers,
    connect_relay_publisher,
    connect_relay_subscriber,
)
from moq_core import KeyValuePair
from moq_core.control_messages import STREAM_RESET_CODE, MessageParameters
from moq_core.transport import AcknowledgedSendStream, BidirectionalStream, MoqtTransport, SendStream

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
    """A send stream whose acknowledgement waits for the gate; a reset fails it, as on a real transport (TA-16)."""

    def __init__(self, inner: SendStream, gate: "AckGate") -> None:
        self._inner = inner
        self._gate = gate
        self.reset_codes: list[int] = []
        """The stream_error_code of every abort, in order."""
        self._reset: asyncio.Future[None] = asyncio.get_running_loop().create_future()

    async def write(self, data: bytes) -> None:
        await self._inner.write(data)

    async def close(self) -> None:
        await self._inner.close()

    def abort(self, reason: object = None) -> None:
        self.reset_codes.append(getattr(reason, "stream_error_code", 0))
        self._inner.abort(reason)

        if not self._reset.done():
            self._reset.set_exception(ConnectionResetError("stream was reset"))
            self._reset.exception()

    async def wait_acknowledged(self) -> None:
        self._gate.waiting += 1
        await asyncio.wait({self._gate.released, self._reset}, return_when=asyncio.FIRST_COMPLETED)

        if self._reset.done():
            self._reset.result()


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

    def __init__(self, inner: MoqtTransport, gate: AckGate, request_gate: AckGate | None = None) -> None:
        self._inner = inner
        self._gate = gate
        self._request_gate = request_gate if request_gate is not None else gate
        """Gates the request streams' acknowledgements (PUBLISH_DONE and its FIN); the data streams' gate by default."""
        self.closed_at: float | None = None
        self.uni_streams: list[AckedSend] = []
        """The publisher's unidirectional streams, in the order it opened them: the control stream first."""

    async def _wrap_incoming(self) -> AsyncIterator[BidirectionalStream]:
        async for stream in self._inner.incoming_bidirectional_streams():
            yield BidirectionalStream(readable=stream.readable, writable=AckedSend(stream.writable, self._request_gate))

    def incoming_unidirectional_streams(self) -> Any:
        return self._inner.incoming_unidirectional_streams()

    def incoming_bidirectional_streams(self) -> AsyncIterator[BidirectionalStream]:
        return self._wrap_incoming()

    async def create_unidirectional_stream(self) -> SendStream:
        stream = AckedSend(await self._inner.create_unidirectional_stream(), self._gate)
        self.uni_streams.append(stream)
        return stream

    @property
    def data_streams(self) -> list[AckedSend]:
        """The publisher's data streams, in the order it opened them."""
        return self.uni_streams[1:]

    async def create_bidirectional_stream(self) -> BidirectionalStream:
        stream = await self._inner.create_bidirectional_stream()
        return BidirectionalStream(readable=stream.readable, writable=AckedSend(stream.writable, self._request_gate))

    def close(self, close_code: int = 0, reason: str = "") -> None:
        if self.closed_at is None:
            self.closed_at = asyncio.get_running_loop().time()
        self._inner.close(close_code, reason)

    async def wait_closed(self) -> None:
        await self._inner.wait_closed()


async def connect(
    cleanup: Cleanup, hub: RelayHub | None = None, *, hold_requests: bool = True
) -> tuple[AckGate, AckedTransport, RelayPublisher, RelaySubscriber]:
    hub = hub if hub is not None else create_relay_hub()
    cleanup.append(hub.destroy)
    gate = AckGate()
    request_gate = gate

    if not hold_requests:
        request_gate = AckGate()
        request_gate.release()
    transports: list[AckedTransport] = []

    def create_transport(url: str, protocols: Sequence[str]) -> ConnectingTransport:
        connecting = hub.connect_publisher(url, protocols)
        transport = AckedTransport(connecting.transport, gate, request_gate)
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


# ---------------------------------------------------------------------------- delivery timeouts (#167)

SUBGROUP_DELIVERY_TIMEOUT = 0x06
OBJECT_DELIVERY_TIMEOUT = 0x02


async def test_a_track_subgroup_timeout_resets_a_settling_group_with_delivery_timeout_and_reports_nothing(
    cleanup: Cleanup,
) -> None:
    _, transport, publisher, subscriber = await connect(cleanup)
    track = publisher.register_track("events", track_properties=[KeyValuePair(SUBGROUP_DELIVERY_TIMEOUT, 50)])
    objects, done = await subscribe(subscriber, "events")
    loop = asyncio.get_running_loop()

    await track.write(b"\x01")
    handed_over = loop.time()
    assert publisher._settling_groups.size == 1
    await wait_for(lambda: publisher._settling_groups.size == 0)

    assert loop.time() - handed_over >= 0.05
    assert transport.data_streams[0].reset_codes == [STREAM_RESET_CODE.DELIVERY_TIMEOUT]
    assert not track.closed
    assert done == []
    await wait_for(lambda: len(objects) == 1)  # the hub had the bytes before the reset
    await track.write(b"\x02")  # nothing reached the track: it writes on
    assert transport.data_streams[1].reset_codes == []


async def test_the_relays_subgroup_timeout_applies_and_a_request_update_changes_it_for_later_groups(
    cleanup: Cleanup,
) -> None:
    hub = create_relay_hub()
    hub.upstream_subgroup_delivery_timeout = 50
    _, transport, publisher, subscriber = await connect(cleanup, hub)
    track = publisher.register_track("events")
    await subscribe(subscriber, "events")

    await track.write(b"\x01")
    await hub.update_upstream("events", MessageParameters(subgroup_delivery_timeout=0))
    await wait_for(lambda: hub.updates_acknowledged == ["events"])
    await track.write(b"\x02")

    await wait_for(lambda: transport.data_streams[0].reset_codes == [STREAM_RESET_CODE.DELIVERY_TIMEOUT])
    await settle(0.1)
    assert transport.data_streams[1].reset_codes == []  # opened after the update: no timeout
    assert publisher._settling_groups.size == 1
    assert track.has_demand  # the update was acknowledged, not refused as unsupported


async def test_the_timer_still_fires_after_the_track_closes(cleanup: Cleanup) -> None:
    _, transport, publisher, subscriber = await connect(cleanup)
    track = publisher.register_track("events", track_properties=[KeyValuePair(SUBGROUP_DELIVERY_TIMEOUT, 60)])
    _, done = await subscribe(subscriber, "events")
    await track.write(b"\x01")

    track.close()
    await wait_for(lambda: len(done) == 1)  # PUBLISH_DONE went out without waiting

    await wait_for(lambda: transport.data_streams[0].reset_codes == [STREAM_RESET_CODE.DELIVERY_TIMEOUT])
    assert publisher._settling_groups.size == 0


async def test_publisher_close_ends_sooner_when_delivery_timeouts_end_the_settling_groups(cleanup: Cleanup) -> None:
    # Only the data streams' acknowledgements are held: the drain also waits for each track's end.
    gate, transport, publisher, subscriber = await connect(cleanup, hold_requests=False)
    track = publisher.register_track(
        "events", write_timeout=2, track_properties=[KeyValuePair(SUBGROUP_DELIVERY_TIMEOUT, 50)]
    )
    objects, _ = await subscribe(subscriber, "events")
    await track.write(b"\x01")
    await wait_for(lambda: len(objects) == 1)

    started = asyncio.get_running_loop().time()
    await asyncio.wait_for(publisher.close(), 2)

    assert transport.closed_at is not None
    assert transport.closed_at - started < 1  # well under the 2 s drain bound
    assert not gate.released.done()
    assert (await publisher.closed).error is None


async def test_a_track_object_timeout_fails_a_write_waiting_for_demand_and_leaves_the_track_open(
    cleanup: Cleanup,
) -> None:
    _, transport, publisher, subscriber = await connect(cleanup)
    track = publisher.register_track("events", track_properties=[KeyValuePair(OBJECT_DELIVERY_TIMEOUT, 50)])

    with pytest.raises(RelayError) as failure:
        await track.write(b"\x01")

    assert (failure.value.code, failure.value.phase) == ("DELIVERY_TIMEOUT", "waiting-for-demand")
    assert transport.data_streams == []
    assert not track.closed

    objects, _ = await subscribe(subscriber, "events")
    await track.write(b"\x02")
    await wait_for(lambda: [obj.payload for obj in objects] == [b"\x02"])


@pytest.mark.parametrize("value", [0, 2**40, 2**62])
async def test_registration_accepts_any_integer_timeout(cleanup: Cleanup, value: int) -> None:
    gate, transport, publisher, subscriber = await connect(cleanup)
    gate.release()
    track = publisher.register_track(
        "events",
        track_properties=[KeyValuePair(SUBGROUP_DELIVERY_TIMEOUT, value), KeyValuePair(OBJECT_DELIVERY_TIMEOUT, value)],
    )
    objects, _ = await subscribe(subscriber, "events")
    await track.write(b"\x01")
    await wait_for(lambda: len(objects) == 1)
    assert transport.data_streams[0].reset_codes == []
