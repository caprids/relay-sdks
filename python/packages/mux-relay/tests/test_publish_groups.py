"""Multi-object groups and demand observation on ``RelayTrackWriter``, the additions video publishing needs."""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest
from core_helpers.waiting import settle, wait_for
from relay_helpers.relay_hub import RelayHub, create_relay_hub

from moq_core import KeyValuePair, MoqtObject
from mux_relay import (
    RelayError,
    RelayPublisher,
    RelaySubscriber,
    RelaySubscriptionHandlers,
    connect_relay_publisher,
    connect_relay_subscriber,
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


async def connect_hub(cleanup: Cleanup) -> tuple[RelayHub, RelayPublisher, RelaySubscriber]:
    hub = create_relay_hub()
    cleanup.append(hub.destroy)
    publisher = await connect_relay_publisher(URL, namespace=NAMESPACE, create_transport=hub.connect_publisher)
    subscriber = await connect_relay_subscriber(URL, create_transport=hub.connect_subscriber)
    cleanup.append(subscriber.close)
    cleanup.append(publisher.close)
    return hub, publisher, subscriber


def receive(subscriber: RelaySubscriber, track: str) -> list[MoqtObject]:
    objects: list[MoqtObject] = []
    subscriber.subscribe(NAMESPACE, track, RelaySubscriptionHandlers(on_object=objects.append))
    return objects


class TestOpenGroup:
    async def test_sends_several_objects_in_one_group_with_increasing_object_ids(self, cleanup: Cleanup) -> None:
        _, publisher, subscriber = await connect_hub(cleanup)
        video = publisher.register_track("video")
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
        video = publisher.register_track("video")
        receive(subscriber, "video")

        group = await video.open_group()

        with pytest.raises(RelayError, match="already pending") as failure:
            await video.open_group()
        assert failure.value.code == "WRITE_PENDING"

        with pytest.raises(RelayError, match="already pending") as failure:
            await video.write(b"x")
        assert failure.value.code == "WRITE_PENDING"

        await group.close()
        await video.write(b"after")

    async def test_open_group_waits_for_demand_and_times_out_without_a_subscriber(self, cleanup: Cleanup) -> None:
        _, publisher, subscriber = await connect_hub(cleanup)
        video = publisher.register_track("video", write_timeout=0.05)

        with pytest.raises(RelayError, match="timed out") as failure:
            await video.open_group()
        assert failure.value.code == "WRITE_TIMEOUT"

        objects = receive(subscriber, "video")
        group = await video.open_group()
        await group.write(b"key")
        await group.close()
        await wait_for(lambda: len(objects) == 1)

    async def test_closing_the_track_aborts_the_open_group(self, cleanup: Cleanup) -> None:
        _, publisher, subscriber = await connect_hub(cleanup)
        video = publisher.register_track("video")
        receive(subscriber, "video")
        group = await video.open_group()
        await group.write(b"key")

        video.close()

        assert group.closed

        with pytest.raises(RelayError, match="closed") as failure:
            await group.write(b"late")
        assert failure.value.code == "CLOSED"

        with pytest.raises(RelayError, match="closed") as failure:
            await group.close()
        assert failure.value.code == "CLOSED"

    async def test_abort_resets_the_group_and_leaves_the_track_usable(self, cleanup: Cleanup) -> None:
        _, publisher, subscriber = await connect_hub(cleanup)
        video = publisher.register_track("video")
        objects = receive(subscriber, "video")
        group = await video.open_group()
        await group.write(b"partial")

        group.abort()

        assert group.closed

        with pytest.raises(RelayError) as failure:
            await group.write(b"more")
        assert failure.value.code == "CLOSED"

        # The track stays open: the next group starts fresh with the next id.
        replacement = await video.open_group()
        assert replacement.group_id == 1
        await replacement.write(b"key")
        await replacement.close()
        await wait_for(lambda: any(obj.group_id == 1 for obj in objects))

    async def test_abort_after_a_clean_close_has_no_effect(self, cleanup: Cleanup) -> None:
        _, publisher, subscriber = await connect_hub(cleanup)
        video = publisher.register_track("video")
        objects: list[MoqtObject] = []
        resets: list[object] = []
        subscriber.subscribe(
            NAMESPACE,
            "video",
            RelaySubscriptionHandlers(on_object=objects.append, on_subgroup_reset=resets.append),
        )
        group = await video.open_group()
        await group.write(b"key")
        await group.close()
        assert group.closed

        # draft-ietf-moq-transport-20 section 11.4.3: a reset after all data and the FIN is ignorable,
        # so the SDK sends none and a repeated close stays a no-op.
        group.abort()
        assert group.closed
        await group.close()

        replacement = await video.open_group()
        assert replacement.group_id == 1
        await replacement.write(b"key-2")
        await replacement.close()
        await wait_for(lambda: len(objects) == 2)
        await settle()
        assert resets == []

    async def test_a_payload_that_cannot_be_converted_leaves_the_group_open(self, cleanup: Cleanup) -> None:
        _, publisher, subscriber = await connect_hub(cleanup)
        video = publisher.register_track("video")
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
        video = publisher.register_track("video")
        receive(subscriber, "video")
        group = await video.open_group()

        with pytest.raises(ValueError, match="16 MiB"):
            await group.write(bytes(16 * 1024 * 1024 + 1))

        await group.close()


class TestDemandChanges:
    async def test_reports_demand_as_the_relay_subscribes_and_unsubscribes(self, cleanup: Cleanup) -> None:
        _, publisher, subscriber = await connect_hub(cleanup)
        catalog = publisher.register_track("catalog")
        seen: list[bool] = []

        async def watch() -> None:
            async for bound in catalog.demand_changes():
                seen.append(bound)

        watcher = asyncio.ensure_future(watch())
        await settle()
        assert seen == [False]
        assert not catalog.has_demand

        subscriber.subscribe(NAMESPACE, "catalog")
        await wait_for(lambda: seen == [False, True])
        assert catalog.has_demand

        # The hub keeps its upstream subscription while any viewer ever asked for the track, so the
        # relay's unsubscribe is simulated at the binding the publish session would report.
        catalog._bind(None)
        await wait_for(lambda: seen == [False, True, False])
        assert not catalog.has_demand
        assert not catalog.closed

        catalog.close()
        await wait_for(lambda: watcher.done())
        assert catalog.closed

    async def test_replays_state_on_every_subscribe(self, cleanup: Cleanup) -> None:
        # The catalog pattern: a fresh subscription must not wait for the next change.
        _, publisher, subscriber = await connect_hub(cleanup)
        catalog = publisher.register_track("catalog")
        sends = 0

        async def serve() -> None:
            nonlocal sends

            async for bound in catalog.demand_changes():
                if bound:
                    await catalog.write(b'{"version":"draft-01"}')
                    sends += 1

        server = asyncio.ensure_future(serve())
        received = receive(subscriber, "catalog")
        await wait_for(lambda: len(received) == 1 and sends == 1)
        assert received[0].payload == b'{"version":"draft-01"}'
        server.cancel()
