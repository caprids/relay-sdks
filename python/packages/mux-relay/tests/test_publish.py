"""``connect_relay_publisher`` behavior against the in-memory relay hub, ported from the TypeScript SDK tests."""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from typing import Any

import pytest
from core_helpers.transport_pair import create_transport_pair
from core_helpers.waiting import wait_for
from relay_helpers.relay_hub import ObservedNamespaceDone, ObservedSubscribeFin, RelayHub, create_relay_hub

from moq_core import KeyValuePair, MoqtObject, SubscriptionHandlers
from moq_core.transport import SendStream
from mux_relay import (
    CancellationToken,
    Closed,
    ConnectingTransport,
    RelayClosedError,
    RelayConnection,
    RelayPublisher,
    RelayTimeoutError,
    RelayWritePendingError,
    connect_relay,
    connect_relay_publisher,
)

NAMESPACE = ["testing", "data"]
URL = "https://relay.example.com/moq"

Cleanup = list[Callable[[], Awaitable[None] | None]]


@pytest.fixture
async def cleanup() -> AsyncIterator[Cleanup]:
    steps: Cleanup = []
    yield steps

    for dispose in reversed(steps):
        result = dispose()

        if result is not None:
            await result


def completed() -> asyncio.Future[None]:
    future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    future.set_result(None)
    return future


async def connect_hub(cleanup: Cleanup) -> tuple[RelayHub, RelayPublisher, RelayConnection]:
    hub = create_relay_hub()
    cleanup.append(hub.destroy)
    publisher = await connect_relay_publisher(URL, namespace=NAMESPACE, create_transport=hub.connect_publisher)
    subscriber = await connect_relay(URL, create_transport=hub.connect_subscriber)
    cleanup.append(subscriber.close)
    cleanup.append(publisher.close)
    return hub, publisher, subscriber


class Received:
    def __init__(self, subscriber: RelayConnection, track: str) -> None:
        self.objects: list[MoqtObject] = []
        self.done: list[Any] = []
        self.subscription = subscriber.subscribe(
            NAMESPACE, track, SubscriptionHandlers(on_object=self.objects.append, on_done=self.done.append)
        )


class TestConnectRelayPublisher:
    async def test_publishes_raw_binary_data_through_a_soliciting_relay_into_the_sdk_subscriber(
        self, cleanup: Cleanup
    ) -> None:
        hub, publisher, subscriber = await connect_hub(cleanup)
        events = publisher.publish("events")
        received = Received(subscriber, "events")
        payload = bytes([0xFF, 0, 0x80, 1])

        await events.write(payload)
        await wait_for(lambda: len(received.objects) == 1)
        obj = received.objects[0]
        assert (obj.group_id, obj.object_id, obj.properties, obj.payload) == (0, 0, [], payload)
        assert hub.subscribes == ["events"]
        assert hub.publisher_connections() == 1

    async def test_publishes_sequential_objects_and_an_empty_payload_with_explicit_object_properties(
        self, cleanup: Cleanup
    ) -> None:
        _, publisher, subscriber = await connect_hub(cleanup)
        events = publisher.publish("events")
        received = Received(subscriber, "events")
        properties = [KeyValuePair(2, 7), KeyValuePair(3, bytes([0xFF, 2]))]

        await events.write(bytes([1]), properties=properties)
        await events.write(bytes([2]))
        await events.write(b"")
        await wait_for(lambda: len(received.objects) == 3)
        assert sorted(obj.group_id for obj in received.objects) == [0, 1, 2]
        assert next(obj for obj in received.objects if obj.group_id == 0).properties == properties
        assert next(obj for obj in received.objects if obj.group_id == 2).payload == b""

    async def test_copies_payload_and_property_bytes_while_a_write_waits_for_relay_demand(
        self, cleanup: Cleanup
    ) -> None:
        _, publisher, subscriber = await connect_hub(cleanup)
        events = publisher.publish("events")
        payload = bytearray([7])
        prop = bytearray([8])
        writing = asyncio.ensure_future(events.write(bytes(payload), properties=[KeyValuePair(3, bytes(prop))]))
        await asyncio.sleep(0)

        payload[0] = 0
        prop[0] = 0
        received = Received(subscriber, "events")
        await writing
        await wait_for(lambda: len(received.objects) == 1)
        assert received.objects[0].payload == bytes([7])
        assert received.objects[0].properties == [KeyValuePair(3, bytes([8]))]

    async def test_ends_one_track_with_a_clean_fin_while_another_published_track_keeps_working(
        self, cleanup: Cleanup
    ) -> None:
        hub, publisher, subscriber = await connect_hub(cleanup)
        one = publisher.publish("one")
        two = publisher.publish("two")
        received_one = Received(subscriber, "one")
        received_two = Received(subscriber, "two")

        await one.write(bytes([1]))
        await two.write(bytes([2]))
        one.close()
        one.close()
        await wait_for(lambda: len(received_one.done) == 1)
        await two.write(bytes([3]))
        await wait_for(lambda: len(received_two.objects) == 2)
        assert ObservedSubscribeFin("one") in hub.track_ends

        with pytest.raises(RelayClosedError, match="closed"):
            await one.write(bytes([4]))

    async def test_bounds_a_write_waiting_for_demand_and_can_publish_a_later_write_after_the_timeout(
        self, cleanup: Cleanup
    ) -> None:
        _, publisher, subscriber = await connect_hub(cleanup)
        events = publisher.publish("events", write_timeout=0.05)

        with pytest.raises(RelayTimeoutError, match="timed out"):
            await events.write(bytes([1]))

        received = Received(subscriber, "events")
        await events.write(bytes([2]))
        await wait_for(lambda: len(received.objects) == 1)
        assert received.objects[0].payload == bytes([2])

    async def test_rejects_concurrent_writes_instead_of_accumulating_an_unbounded_queue(self, cleanup: Cleanup) -> None:
        _, publisher, _ = await connect_hub(cleanup)
        events = publisher.publish("events")
        writing = asyncio.ensure_future(events.write(bytes([1])))
        await asyncio.sleep(0)

        with pytest.raises(RelayWritePendingError, match="already pending"):
            await events.write(bytes([2]))

        events.close()

        with pytest.raises(RelayClosedError, match="closed"):
            await writing

    async def test_cancels_pending_writes_and_retracts_the_namespace_when_the_publisher_closes(
        self, cleanup: Cleanup
    ) -> None:
        hub, publisher, subscriber = await connect_hub(cleanup)
        live = publisher.publish("live")
        Received(subscriber, "live")
        await live.write(bytes([1]))
        pending = asyncio.ensure_future(publisher.publish("pending").write(bytes([2])))
        await asyncio.sleep(0)

        await publisher.close()

        with pytest.raises(RelayClosedError, match="closed"):
            await pending

        await publisher.close()
        assert ObservedNamespaceDone(NAMESPACE) in hub.track_ends
        assert await publisher.closed == Closed()

        with pytest.raises(RelayClosedError, match="closed"):
            publisher.publish("late")

    async def test_does_not_re_register_a_track_name_or_silently_restart_its_group_numbering(
        self, cleanup: Cleanup
    ) -> None:
        _, publisher, _ = await connect_hub(cleanup)
        events = publisher.publish("events")

        with pytest.raises(ValueError, match="already registered"):
            publisher.publish("events")

        events.close()

        with pytest.raises(ValueError, match="already registered"):
            publisher.publish("events")

    async def test_cancels_a_pending_write_when_the_owning_token_is_cancelled(self, cleanup: Cleanup) -> None:
        hub = create_relay_hub()
        owner = CancellationToken()
        cleanup.append(hub.destroy)
        publisher = await connect_relay_publisher(
            URL, namespace=NAMESPACE, cancellation=owner, create_transport=hub.connect_publisher
        )
        cleanup.append(publisher.close)
        writing = asyncio.ensure_future(publisher.publish("events").write(bytes([1])))
        await asyncio.sleep(0)
        error = RuntimeError("owner cancelled")

        owner.cancel(error)

        with pytest.raises(RuntimeError) as raised:
            await writing

        assert raised.value is error
        assert await publisher.closed == Closed(error)

    async def test_authenticates_the_publisher_independently_from_subscriber_credentials(
        self, cleanup: Cleanup
    ) -> None:
        hub = create_relay_hub()
        cleanup.append(hub.destroy)
        calls: list[tuple[str, list[str]]] = []

        def create_transport(url: str, protocols: Sequence[str]) -> ConnectingTransport:
            calls.append((url, list(protocols)))
            return hub.connect_publisher(url, protocols)

        publisher = await connect_relay_publisher(
            URL, namespace=NAMESPACE, get_token=lambda: "publish-token", create_transport=create_transport
        )
        cleanup.append(publisher.close)

        assert calls == [(f"{URL}?jwt=publish-token", ["moqt-20"])]

    async def test_preserves_explicit_track_properties_and_defaults_other_tracks_to_no_timescale(
        self, cleanup: Cleanup
    ) -> None:
        pair = create_transport_pair()
        publisher, subscriber = await asyncio.gather(
            connect_relay_publisher(
                URL,
                namespace=NAMESPACE,
                create_transport=lambda url, protocols: ConnectingTransport(transport=pair.client, ready=completed()),
            ),
            connect_relay(
                URL,
                create_transport=lambda url, protocols: ConnectingTransport(transport=pair.server, ready=completed()),
            ),
        )
        cleanup.append(subscriber.close)
        cleanup.append(publisher.close)
        track_properties = [KeyValuePair(2, 90_000)]
        publisher.publish("timed", track_properties=track_properties)
        publisher.publish("raw")
        on_timed: list[Any] = []
        on_raw: list[Any] = []
        subscriber.subscribe(NAMESPACE, "timed", SubscriptionHandlers(on_ok=on_timed.append))
        subscriber.subscribe(NAMESPACE, "raw", SubscriptionHandlers(on_ok=on_raw.append))

        await wait_for(lambda: len(on_raw) == 1 and len(on_timed) == 1)
        assert on_timed[0].track_properties == track_properties
        assert on_raw[0].track_properties == []

    async def test_reports_a_transport_write_failure_and_ends_that_track_without_silently_retrying(
        self, cleanup: Cleanup
    ) -> None:
        hub = create_relay_hub()
        cleanup.append(hub.destroy)
        failure = ConnectionError("data stream failed")

        def create_transport(url: str, protocols: Sequence[str]) -> ConnectingTransport:
            created = hub.connect_publisher(url, protocols)
            transport: Any = created.transport
            open_stream = transport.create_unidirectional_stream
            streams = [0]

            async def failing_open() -> SendStream:
                streams[0] += 1

                if streams[0] == 2:
                    raise failure

                return await open_stream()

            transport.create_unidirectional_stream = failing_open
            return created

        publisher = await connect_relay_publisher(URL, namespace=NAMESPACE, create_transport=create_transport)
        subscriber = await connect_relay(URL, create_transport=hub.connect_subscriber)
        cleanup.append(subscriber.close)
        cleanup.append(publisher.close)
        events = publisher.publish("events")
        Received(subscriber, "events")

        with pytest.raises(ConnectionError) as raised:
            await events.write(bytes([1]))

        assert raised.value is failure

        with pytest.raises(RelayClosedError, match="closed"):
            await events.write(bytes([2]))

        assert hub.object_count("events") == 0

    async def test_times_out_a_stalled_stream_open_and_aborts_a_late_stream_without_sending_payload_bytes(
        self, cleanup: Cleanup
    ) -> None:
        hub = create_relay_hub()
        cleanup.append(hub.destroy)
        late_stream: asyncio.Future[SendStream] = asyncio.get_running_loop().create_future()

        def create_transport(url: str, protocols: Sequence[str]) -> ConnectingTransport:
            created = hub.connect_publisher(url, protocols)
            transport: Any = created.transport
            open_stream = transport.create_unidirectional_stream
            streams = [0]

            async def stalling_open() -> SendStream:
                streams[0] += 1

                if streams[0] == 1:
                    return await open_stream()

                return await late_stream

            transport.create_unidirectional_stream = stalling_open
            return created

        publisher = await connect_relay_publisher(URL, namespace=NAMESPACE, create_transport=create_transport)
        subscriber = await connect_relay(URL, create_transport=hub.connect_subscriber)
        cleanup.append(subscriber.close)
        cleanup.append(publisher.close)
        events = publisher.publish("events", write_timeout=0.1)
        Received(subscriber, "events")

        with pytest.raises(RelayTimeoutError, match="timed out"):
            await events.write(bytes([1]))

        with pytest.raises(RelayClosedError, match="closed"):
            await events.write(bytes([2]))

        class RecordingStream:
            def __init__(self) -> None:
                self.aborted = 0
                self.writes: list[bytes] = []

            async def write(self, data: bytes) -> None:
                self.writes.append(data)

            async def close(self) -> None:
                pass

            def abort(self, reason: object = None) -> None:
                self.aborted += 1

        recording = RecordingStream()
        late_stream.set_result(recording)
        await wait_for(lambda: recording.aborted == 1)
        assert recording.writes == []

    async def test_rejects_oversized_objects_before_waiting_for_demand(self, cleanup: Cleanup) -> None:
        _, publisher, _ = await connect_hub(cleanup)
        track = publisher.publish("events")

        with pytest.raises(ValueError, match="16 MiB"):
            await track.write(bytes(16 * 1024 * 1024 + 1))

    async def test_rejects_an_empty_namespace_and_track_name(self, cleanup: Cleanup) -> None:
        hub = create_relay_hub()
        cleanup.append(hub.destroy)

        with pytest.raises(TypeError, match="non-empty"):
            await connect_relay_publisher(URL, namespace=[], create_transport=hub.connect_publisher)

        publisher = await connect_relay_publisher(URL, namespace=NAMESPACE, create_transport=hub.connect_publisher)
        cleanup.append(publisher.close)

        with pytest.raises(TypeError, match="non-empty"):
            publisher.publish("")

        with pytest.raises(ValueError, match="write_timeout"):
            publisher.publish("events", write_timeout=0)

    async def test_is_an_async_context_manager_that_closes_on_exit(self, cleanup: Cleanup) -> None:
        hub = create_relay_hub()
        cleanup.append(hub.destroy)
        publisher = await connect_relay_publisher(URL, namespace=NAMESPACE, create_transport=hub.connect_publisher)

        async with publisher:
            publisher.publish("events")

        assert await publisher.closed == Closed()
