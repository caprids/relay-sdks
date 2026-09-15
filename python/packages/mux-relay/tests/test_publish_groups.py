"""Multi-object groups and demand observation on ``RelayTrackWriter``, the additions video publishing needs."""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest
from core_helpers.waiting import settle, wait_for
from relay_helpers.relay_hub import RelayHub, create_relay_hub

from moq_core import KeyValuePair, MoqtObject, SubscriptionHandlers
from mux_relay import (
    RelayClosedError,
    RelayConnection,
    RelayError,
    RelayPublisher,
    RelayTimeoutError,
    RelayWritePendingError,
    connect_relay,
    connect_relay_publisher,
)

NAMESPACE = ["testing", "video"]
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


async def connect_hub(cleanup: Cleanup) -> tuple[RelayHub, RelayPublisher, RelayConnection]:
    hub = create_relay_hub()
    cleanup.append(hub.destroy)
    publisher = await connect_relay_publisher(URL, namespace=NAMESPACE, create_transport=hub.connect_publisher)
    subscriber = await connect_relay(URL, create_transport=hub.connect_subscriber)
    cleanup.append(subscriber.close)
    cleanup.append(publisher.close)
    return hub, publisher, subscriber


def receive(subscriber: RelayConnection, track: str) -> list[MoqtObject]:
    objects: list[MoqtObject] = []
    subscriber.subscribe(NAMESPACE, track, SubscriptionHandlers(on_object=objects.append))
    return objects


class TestOpenGroup:
    async def test_sends_several_objects_in_one_group_with_increasing_object_ids(self, cleanup: Cleanup) -> None:
        _, publisher, subscriber = await connect_hub(cleanup)
        video = publisher.publish("video")
        objects = receive(subscriber, "video")

        group = await video.open_group()
        assert group.group_id == 0
        await group.write(b"key", properties=[KeyValuePair(0x10, 0), KeyValuePair(0x0D, b"cfg")])
        await group.write(b"delta-1", properties=[KeyValuePair(0x10, 33_333)])
        await group.write(b"delta-2", properties=[KeyValuePair(0x10, 66_666)])
        assert group.object_count == 3
        await group.close()
        assert group.closed

        await wait_for(lambda: len(objects) == 3)
        assert sorted((obj.group_id, obj.object_id, obj.payload) for obj in objects) == [
            (0, 0, b"key"),
            (0, 1, b"delta-1"),
            (0, 2, b"delta-2"),
        ]
        assert next(obj for obj in objects if obj.object_id == 0).properties == [
            KeyValuePair(0x0D, b"cfg"),
            KeyValuePair(0x10, 0),
        ]

        # The next group continues the numbering, and single writes share it.
        second = await video.open_group()
        assert second.group_id == 1
        await second.write(b"key-2")
        await second.close()
        await video.write(b"single")
        await wait_for(lambda: len(objects) == 5)
        assert sorted(obj.group_id for obj in objects) == [0, 0, 0, 1, 2]

    async def test_only_one_write_or_group_may_be_pending(self, cleanup: Cleanup) -> None:
        _, publisher, subscriber = await connect_hub(cleanup)
        video = publisher.publish("video")
        receive(subscriber, "video")

        group = await video.open_group()

        with pytest.raises(RelayWritePendingError, match="already pending"):
            await video.open_group()

        with pytest.raises(RelayWritePendingError, match="already pending"):
            await video.write(b"x")

        await group.close()
        await video.write(b"after")

    async def test_open_group_waits_for_demand_and_times_out_without_a_subscriber(self, cleanup: Cleanup) -> None:
        _, publisher, subscriber = await connect_hub(cleanup)
        video = publisher.publish("video", write_timeout=0.05)

        with pytest.raises(RelayTimeoutError, match="timed out"):
            await video.open_group()

        objects = receive(subscriber, "video")
        group = await video.open_group()
        await group.write(b"key")
        await group.close()
        await wait_for(lambda: len(objects) == 1)

    async def test_closing_the_track_aborts_the_open_group(self, cleanup: Cleanup) -> None:
        _, publisher, subscriber = await connect_hub(cleanup)
        video = publisher.publish("video")
        receive(subscriber, "video")
        group = await video.open_group()
        await group.write(b"key")

        video.close()

        assert group.closed

        with pytest.raises(RelayClosedError, match="closed"):
            await group.write(b"late")

        with pytest.raises(RelayClosedError, match="closed"):
            await group.close()

    async def test_abort_resets_the_group_and_leaves_the_track_usable(self, cleanup: Cleanup) -> None:
        _, publisher, subscriber = await connect_hub(cleanup)
        video = publisher.publish("video")
        objects = receive(subscriber, "video")
        group = await video.open_group()
        await group.write(b"partial")

        group.abort()

        assert group.closed

        with pytest.raises(RelayClosedError):
            await group.write(b"more")

        # The track stays open: the next group starts fresh with the next id.
        replacement = await video.open_group()
        assert replacement.group_id == 1
        await replacement.write(b"key")
        await replacement.close()
        await wait_for(lambda: any(obj.group_id == 1 for obj in objects))

    async def test_a_replaced_relay_subscription_ends_the_group_but_not_the_track(self, cleanup: Cleanup) -> None:
        _, publisher, subscriber = await connect_hub(cleanup)
        video = publisher.publish("video")
        receive(subscriber, "video")
        group = await video.open_group()
        await group.write(b"key")

        # Simulate the relay re-subscribing: the binding moves to a new alias mid-group.
        video._bind(None)
        video._bind(42)

        with pytest.raises(RelayError, match="subscription changed"):
            await group.write(b"delta")

        assert group.closed
        assert video.bound

    async def test_demand_that_vanishes_before_the_write_starts_is_retryable(self, cleanup: Cleanup) -> None:
        _, publisher, subscriber = await connect_hub(cleanup)
        video = publisher.publish("video")
        writing = asyncio.ensure_future(video.write(b"x"))
        await asyncio.sleep(0)

        # The relay subscribes and unsubscribes before the waiting write resumes.
        video._bind(5)
        video._bind(None)

        with pytest.raises(RelayError, match="ended before the write started") as raised:
            await writing

        # Not a closed track: demand simply came and went, and the next write can wait again.
        assert not isinstance(raised.value, RelayClosedError)
        objects = receive(subscriber, "video")
        await video.write(b"y")
        await wait_for(lambda: len(objects) == 1)

    async def test_a_payload_that_cannot_be_converted_leaves_the_group_open(self, cleanup: Cleanup) -> None:
        _, publisher, subscriber = await connect_hub(cleanup)
        video = publisher.publish("video")
        objects = receive(subscriber, "video")
        group = await video.open_group()
        text: Any = "not bytes"

        with pytest.raises(TypeError):
            await group.write(text)

        # Nothing left the process, so the group keeps its object numbering and stays usable.
        assert not group.closed
        assert group.object_count == 0
        await group.write(b"key")
        await group.close()
        await wait_for(lambda: len(objects) == 1)

    async def test_rejects_oversized_objects(self, cleanup: Cleanup) -> None:
        _, publisher, subscriber = await connect_hub(cleanup)
        video = publisher.publish("video")
        receive(subscriber, "video")
        group = await video.open_group()

        with pytest.raises(ValueError, match="16 MiB"):
            await group.write(bytes(16 * 1024 * 1024 + 1))

        await group.close()


class TestBindings:
    async def test_reports_demand_as_the_relay_subscribes_and_unsubscribes(self, cleanup: Cleanup) -> None:
        _, publisher, subscriber = await connect_hub(cleanup)
        catalog = publisher.publish("catalog")
        seen: list[bool] = []

        async def watch() -> None:
            async for bound in catalog.bindings():
                seen.append(bound)

        watcher = asyncio.ensure_future(watch())
        await settle()
        assert seen == [False]
        assert not catalog.bound

        subscriber.subscribe(NAMESPACE, "catalog")
        await wait_for(lambda: seen == [False, True])
        assert catalog.bound

        # The hub keeps its upstream subscription while any viewer ever asked for the track, so the
        # relay's unsubscribe is simulated at the binding the publish session would report.
        catalog._bind(None)
        await wait_for(lambda: seen == [False, True, False])
        assert not catalog.bound

        catalog.close()
        await wait_for(lambda: watcher.done())

    async def test_yields_true_again_for_a_replacement_subscription(self, cleanup: Cleanup) -> None:
        _, publisher, subscriber = await connect_hub(cleanup)
        catalog = publisher.publish("catalog")
        seen: list[bool] = []

        async def watch() -> None:
            async for bound in catalog.bindings():
                seen.append(bound)

        watcher = asyncio.ensure_future(watch())
        subscriber.subscribe(NAMESPACE, "catalog")
        await wait_for(lambda: seen == [False, True])

        catalog._bind(7)
        await wait_for(lambda: seen == [False, True, True])
        watcher.cancel()

    async def test_replays_state_on_every_subscribe(self, cleanup: Cleanup) -> None:
        # The catalog pattern: a fresh subscription must not wait for the next change.
        _, publisher, subscriber = await connect_hub(cleanup)
        catalog = publisher.publish("catalog")
        sends = 0

        async def serve() -> None:
            nonlocal sends

            async for bound in catalog.bindings():
                if bound:
                    await catalog.write(b'{"version":"draft-01"}')
                    sends += 1

        server = asyncio.ensure_future(serve())
        received = receive(subscriber, "catalog")
        await wait_for(lambda: len(received) == 1 and sends == 1)
        assert received[0].payload == b'{"version":"draft-01"}'
        server.cancel()
