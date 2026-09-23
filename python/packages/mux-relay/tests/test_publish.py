"""``connect_relay_publisher`` behavior against the in-memory relay hub, ported from the TypeScript SDK tests."""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from typing import Any

import pytest
from core_helpers.transport_pair import create_transport_pair
from core_helpers.waiting import wait_for
from relay_helpers.relay_hub import ObservedNamespaceDone, ObservedSubscribeFin, RelayHub, create_relay_hub

from moq_core import KeyValuePair, MoqtObject
from mux_relay import (
    CancellationToken,
    Closed,
    ConnectingTransport,
    RelayError,
    RelayGroupWriter,
    RelayPublisher,
    RelaySubscriber,
    RelaySubscriptionHandlers,
    connect_relay_publisher,
    connect_relay_subscriber,
    read_json,
    read_text,
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


async def connect_hub(cleanup: Cleanup) -> tuple[RelayHub, RelayPublisher, RelaySubscriber]:
    hub = create_relay_hub()
    cleanup.append(hub.destroy)
    publisher = await connect_relay_publisher(URL, namespace=NAMESPACE, create_transport=hub.connect_publisher)
    subscriber = await connect_relay_subscriber(URL, create_transport=hub.connect_subscriber)
    cleanup.append(subscriber.close)
    cleanup.append(publisher.close)
    return hub, publisher, subscriber


class Received:
    def __init__(self, subscriber: RelaySubscriber, track: str) -> None:
        self.objects: list[MoqtObject] = []
        self.done: list[Any] = []
        self.subscription = subscriber.subscribe(
            NAMESPACE,
            track,
            RelaySubscriptionHandlers(on_object=self.objects.append, on_publisher_done=self.done.append),
        )


class TestConnectRelayPublisher:
    @pytest.mark.parametrize(
        "values",
        [
            (-1, 0),
            (1.5, 0),
            (True, b"x"),
            (2**64, 0),
            (2, -1),
            (2, 1.5),
            (2, True),
            (2, 2**64),
            (2, b"x"),
            (3, "abc"),
            (3, [1, 2]),
            (3, 3),
        ],
    )
    async def test_rejects_invalid_properties_before_registering_or_taking_the_write_slot(
        self, cleanup: Cleanup, values: tuple[Any, Any]
    ) -> None:
        prop = KeyValuePair(*values)
        _, publisher, subscriber = await connect_hub(cleanup)
        with pytest.raises(TypeError, match="Property 0"):
            publisher.register_track("events", track_properties=[prop])
        events = publisher.register_track("events")
        received = Received(subscriber, "events")
        await wait_for(lambda: events.has_demand)
        with pytest.raises(TypeError, match="Property 0"):
            await events.write(b"bad", properties=[prop])
        assert not events.closed
        group = await events.open_group()
        with pytest.raises(TypeError, match="Property 0"):
            await group.write(b"bad", properties=[prop])
        assert not group.closed
        await group.write(b"good", properties=[KeyValuePair(2, 2**64 - 1)])
        await group.close()
        await events.write(b"after")
        await wait_for(lambda: len(received.objects) == 2)
        assert [obj.payload for obj in received.objects] == [b"good", b"after"]

    async def test_copies_only_the_property_buffer_view_and_keeps_numeric_boundaries(self, cleanup: Cleanup) -> None:
        _, publisher, subscriber = await connect_hub(cleanup)
        events = publisher.register_track("events")
        backing = bytearray(b"abcd")
        view: Any = memoryview(backing)[1:3]
        writing = asyncio.create_task(events.write(b"data", properties=[KeyValuePair(2**64 - 1, view)]))
        await asyncio.sleep(0)
        backing[:] = b"xxxx"
        received = Received(subscriber, "events")
        await writing
        await wait_for(lambda: len(received.objects) == 1)
        assert received.objects[0].properties == [KeyValuePair(2**64 - 1, b"bc")]

    async def test_publishes_raw_binary_data_through_a_soliciting_relay_into_the_sdk_subscriber(
        self, cleanup: Cleanup
    ) -> None:
        hub, publisher, subscriber = await connect_hub(cleanup)
        events = publisher.register_track("events")
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
        events = publisher.register_track("events")
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
        events = publisher.register_track("events")
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
        one = publisher.register_track("one")
        two = publisher.register_track("two")
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

        with pytest.raises(RelayError) as failure:
            await one.write(bytes([4]))
        assert failure.value.code == "CLOSED"
        assert failure.value.phase == "writing"

    async def test_bounds_a_write_waiting_for_demand_and_can_publish_a_later_write_after_the_timeout(
        self, cleanup: Cleanup
    ) -> None:
        _, publisher, subscriber = await connect_hub(cleanup)
        events = publisher.register_track("events", write_timeout=0.05)

        with pytest.raises(RelayError) as failure:
            await events.write(bytes([1]))
        assert failure.value.code == "WRITE_TIMEOUT"
        assert failure.value.phase == "waiting-for-demand"

        # Nothing was bound, so the track is still live and the next write can wait for demand again.
        assert not events.closed
        received = Received(subscriber, "events")
        await events.write(bytes([2]))
        await wait_for(lambda: len(received.objects) == 1)
        assert received.objects[0].payload == bytes([2])

    async def test_rejects_concurrent_writes_instead_of_accumulating_an_unbounded_queue(self, cleanup: Cleanup) -> None:
        _, publisher, _ = await connect_hub(cleanup)
        events = publisher.register_track("events")
        writing = asyncio.ensure_future(events.write(bytes([1])))
        await asyncio.sleep(0)

        with pytest.raises(RelayError) as failure:
            await events.write(bytes([2]))
        assert failure.value.code == "WRITE_PENDING"
        assert failure.value.phase == "writing"

        events.close()

        with pytest.raises(RelayError, match="closed") as failure:
            await writing
        assert failure.value.code == "CLOSED"

    async def test_cancels_pending_writes_and_retracts_the_namespace_when_the_publisher_closes(
        self, cleanup: Cleanup
    ) -> None:
        hub, publisher, subscriber = await connect_hub(cleanup)
        live = publisher.register_track("live")
        Received(subscriber, "live")
        await live.write(bytes([1]))
        pending = asyncio.ensure_future(publisher.register_track("pending").write(bytes([2])))
        await asyncio.sleep(0)

        await publisher.close()

        with pytest.raises(RelayError, match="closed") as failure:
            await pending
        assert failure.value.code == "CLOSED"

        await publisher.close()
        assert ObservedNamespaceDone(NAMESPACE) in hub.track_ends
        assert await publisher.closed == Closed()

        with pytest.raises(RelayError, match="closed") as failure:
            publisher.register_track("late")
        assert failure.value.code == "CLOSED"

    async def test_does_not_re_register_a_track_name_or_silently_restart_its_group_numbering(
        self, cleanup: Cleanup
    ) -> None:
        _, publisher, _ = await connect_hub(cleanup)
        events = publisher.register_track("events")

        with pytest.raises(ValueError, match="already registered"):
            publisher.register_track("events")

        events.close()

        with pytest.raises(ValueError, match="already registered"):
            publisher.register_track("events")

    async def test_cancels_a_pending_write_when_the_owning_token_is_cancelled(self, cleanup: Cleanup) -> None:
        hub = create_relay_hub()
        owner = CancellationToken()
        cleanup.append(hub.destroy)
        publisher = await connect_relay_publisher(
            URL, namespace=NAMESPACE, cancellation=owner, create_transport=hub.connect_publisher
        )
        cleanup.append(publisher.close)
        writing = asyncio.ensure_future(publisher.register_track("events").write(bytes([1])))
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
            connect_relay_subscriber(
                URL,
                create_transport=lambda url, protocols: ConnectingTransport(transport=pair.server, ready=completed()),
            ),
        )
        cleanup.append(subscriber.close)
        cleanup.append(publisher.close)
        track_properties = [KeyValuePair(2, 90_000)]
        publisher.register_track("timed", track_properties=track_properties)
        publisher.register_track("raw")
        on_timed: list[Any] = []
        on_raw: list[Any] = []
        subscriber.subscribe(NAMESPACE, "timed", RelaySubscriptionHandlers(on_subscribed=on_timed.append))
        subscriber.subscribe(NAMESPACE, "raw", RelaySubscriptionHandlers(on_subscribed=on_raw.append))

        await wait_for(lambda: len(on_raw) == 1 and len(on_timed) == 1)
        assert on_timed[0].track_properties == track_properties
        assert on_raw[0].track_properties == []

    async def test_rejects_oversized_objects_before_waiting_for_demand(self, cleanup: Cleanup) -> None:
        _, publisher, _ = await connect_hub(cleanup)
        track = publisher.register_track("events")

        with pytest.raises(ValueError, match="16 MiB"):
            await track.write(bytes(16 * 1024 * 1024 + 1))

    async def test_rejects_oversized_namespace_before_token_or_transport(self) -> None:
        def forbidden(*args: Any) -> Any:
            pytest.fail("oversized namespace must fail before token or transport")

        with pytest.raises(ValueError, match="at most 32"):
            await connect_relay_publisher(
                URL, namespace=["field"] * 33, get_token=forbidden, create_transport=forbidden
            )

    async def test_publishes_32_fields_after_invalid_track_names(self, cleanup: Cleanup) -> None:
        hub = create_relay_hub()
        cleanup.append(hub.destroy)
        namespace = [f"field-{index}" for index in range(32)]
        publisher = await connect_relay_publisher(URL, namespace=namespace, create_transport=hub.connect_publisher)
        cleanup.append(publisher.close)
        invalid: Any = 7
        with pytest.raises(TypeError, match="non-empty string"):
            publisher.register_track(invalid)
        with pytest.raises(TypeError, match="non-empty string"):
            publisher.register_track("")
        events = publisher.register_track("events")
        subscriber = await connect_relay_subscriber(URL, create_transport=hub.connect_subscriber)
        cleanup.append(subscriber.close)
        objects: list[MoqtObject] = []
        subscriber.subscribe(namespace, "events", RelaySubscriptionHandlers(on_object=objects.append))
        await events.write(bytes([1]))
        await wait_for(lambda: len(objects) == 1)

    async def test_rejects_an_empty_namespace_and_track_name(self, cleanup: Cleanup) -> None:
        hub = create_relay_hub()
        cleanup.append(hub.destroy)

        with pytest.raises(TypeError, match="non-empty"):
            await connect_relay_publisher(URL, namespace=[], create_transport=hub.connect_publisher)

        publisher = await connect_relay_publisher(URL, namespace=NAMESPACE, create_transport=hub.connect_publisher)
        cleanup.append(publisher.close)

        with pytest.raises(TypeError, match="non-empty"):
            publisher.register_track("")

        with pytest.raises(ValueError, match="write_timeout"):
            publisher.register_track("events", write_timeout=0)

    async def test_is_an_async_context_manager_that_closes_on_exit(self, cleanup: Cleanup) -> None:
        hub = create_relay_hub()
        cleanup.append(hub.destroy)
        publisher = await connect_relay_publisher(URL, namespace=NAMESPACE, create_transport=hub.connect_publisher)

        async with publisher:
            publisher.register_track("events")

        assert await publisher.closed == Closed()


async def test_text_and_json_round_trip_on_tracks_and_groups(cleanup: Cleanup) -> None:
    _, publisher, subscriber = await connect_hub(cleanup)
    track = publisher.register_track("helpers")
    received = Received(subscriber, "helpers")
    text = "\ufeffHello, 世界 🌍\n\0"
    value = {"text": text, "nested": [None, True, 42, {"empty": ""}]}
    properties = [KeyValuePair(2, 7)]
    await track.write_text(text, properties=properties)
    await track.write_json(value, properties=properties)
    group = await track.open_group()
    await group.write_text("")
    await group.write_json(value, properties=properties)
    await group.write_json(None)
    await group.write_text("", status="end-of-group")
    assert group.object_count == 4
    await group.close()
    # The relay fixture consumes the end-of-group marker instead of forwarding it.
    await wait_for(lambda: len(received.objects) == 5)
    objects = sorted(received.objects, key=lambda obj: (obj.group_id, obj.object_id))
    assert read_text(objects[0]) == text
    assert read_json(objects[1]) == value
    assert read_text(objects[2]) == ""
    assert read_json(objects[3]) == value
    assert read_json(objects[4]) is None
    assert [(obj.group_id, obj.object_id) for obj in objects] == [(0, 0), (1, 0), (2, 0), (2, 1), (2, 2)]
    assert all(objects[index].properties == properties for index in (0, 1, 3))


async def test_encoding_errors_leave_writers_usable(cleanup: Cleanup) -> None:
    _, publisher, subscriber = await connect_hub(cleanup)
    track = publisher.register_track("helpers")
    received = Received(subscriber, "helpers")
    circular: list[object] = []
    circular.append(circular)
    for grouped in (False, True):
        writer = await track.open_group() if grouped else track
        for value in (object(), circular, float("nan"), float("inf")):
            with pytest.raises((TypeError, ValueError)):
                await writer.write_json(value)
        invalid: Any = 42
        with pytest.raises(TypeError, match="string"):
            await writer.write_text(invalid)
        with pytest.raises(ValueError, match="16 MiB"):
            await writer.write_text("é" * (8 * 1024 * 1024 + 1))
        await writer.write_json(False)
        assert not writer.closed
        if isinstance(writer, RelayGroupWriter):
            await writer.close()
    await track.write_text("after")
    await wait_for(lambda: len(received.objects) == 3)
    assert [read_text(obj) for obj in received.objects] == ["false", "false", "after"]


async def test_helpers_keep_deadlines_concurrency_and_task_cancellation(cleanup: Cleanup) -> None:
    _, publisher, subscriber = await connect_hub(cleanup)
    track = publisher.register_track("helpers", write_timeout=0.05)
    with pytest.raises(RelayError) as failure:
        await track.write_text("no demand")
    assert failure.value.code == "WRITE_TIMEOUT"
    writing = asyncio.create_task(track.write_json({"waiting": True}))
    await asyncio.sleep(0)
    with pytest.raises(RelayError) as failure:
        await track.write_text("concurrent")
    assert failure.value.code == "WRITE_PENDING"
    writing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await writing
    received = Received(subscriber, "helpers")
    await track.write_json(0)
    await wait_for(lambda: len(received.objects) == 1)
    assert read_json(received.objects[0]) == 0
