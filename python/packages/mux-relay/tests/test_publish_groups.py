"""Multi-object groups and demand observation on ``RelayTrackWriter``, the additions video publishing needs."""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from typing import Any

import pytest
from core_helpers.waiting import settle, wait_for
from relay_helpers.relay_hub import RelayHub, create_relay_hub

from moq_core import KeyValuePair, MoqtObject, TrackBinding
from moq_core.transport import SendStream
from mux_relay import (
    ConnectingTransport,
    RelayClosedError,
    RelayError,
    RelayPublisher,
    RelaySubscriber,
    RelaySubscriptionHandlers,
    RelayTimeoutError,
    RelayTrackWriter,
    RelayWritePendingError,
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


class HeldStreams:
    """A publisher transport whose data-stream opens can be held, freezing a write while it is bound.

    A write takes the relay's alias just before it opens its stream, so holding that open is how a
    test keeps a write bound and in flight long enough for the relay to move the binding under it.
    """

    def __init__(self, hub: RelayHub) -> None:
        self._hub = hub
        self.holding = False
        self.holds = 0
        self._held: asyncio.Future[SendStream] = asyncio.get_running_loop().create_future()

    def connect(self, url: str, protocols: Sequence[str]) -> ConnectingTransport:
        created = self._hub.connect_publisher(url, protocols)
        transport: Any = created.transport
        open_stream = transport.create_unidirectional_stream

        async def open_or_hold() -> SendStream:
            if self.holding:
                self.holds += 1
                return await self._held

            return await open_stream()

        transport.create_unidirectional_stream = open_or_hold
        return created

    def release(self) -> None:
        """Let go of the held open so no send task is left parked when the test ends."""
        if not self._held.done():
            self._held.cancel()


async def connect_held_hub(cleanup: Cleanup) -> tuple[HeldStreams, RelayPublisher, RelaySubscriber]:
    hub = create_relay_hub()
    cleanup.append(hub.destroy)
    streams = HeldStreams(hub)
    cleanup.append(streams.release)
    publisher = await connect_relay_publisher(URL, namespace=NAMESPACE, create_transport=streams.connect)
    subscriber = await connect_relay_subscriber(URL, create_transport=hub.connect_subscriber)
    cleanup.append(subscriber.close)
    cleanup.append(publisher.close)
    return streams, publisher, subscriber


def watch_demand_changes(track: RelayTrackWriter, seen: list[bool]) -> asyncio.Task[None]:
    """Collect a track's demand changes in the background until its ``demand_changes()`` iterator stops."""

    async def collect() -> None:
        async for bound in track.demand_changes():
            seen.append(bound)

    return asyncio.ensure_future(collect())


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

        with pytest.raises(RelayWritePendingError, match="already pending"):
            await video.open_group()

        with pytest.raises(RelayWritePendingError, match="already pending"):
            await video.write(b"x")

        await group.close()
        await video.write(b"after")

    async def test_open_group_waits_for_demand_and_times_out_without_a_subscriber(self, cleanup: Cleanup) -> None:
        _, publisher, subscriber = await connect_hub(cleanup)
        video = publisher.register_track("video", write_timeout=0.05)

        with pytest.raises(RelayTimeoutError, match="timed out"):
            await video.open_group()

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

        with pytest.raises(RelayClosedError, match="closed"):
            await group.write(b"late")

        with pytest.raises(RelayClosedError, match="closed"):
            await group.close()

    async def test_abort_resets_the_group_and_leaves_the_track_usable(self, cleanup: Cleanup) -> None:
        _, publisher, subscriber = await connect_hub(cleanup)
        video = publisher.register_track("video")
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

    async def test_a_replaced_relay_subscription_ends_the_group_but_not_the_track(self, cleanup: Cleanup) -> None:
        _, publisher, subscriber = await connect_hub(cleanup)
        video = publisher.register_track("video")
        receive(subscriber, "video")
        group = await video.open_group()
        await group.write(b"key")

        # Simulate the relay re-subscribing: the binding moves to a new alias mid-group.
        video._bind(None)
        video._bind(42)

        with pytest.raises(RelayError, match="subscription changed"):
            await group.write(b"delta")

        assert group.closed
        assert video.has_demand

    async def test_demand_that_vanishes_before_the_write_starts_is_retryable(self, cleanup: Cleanup) -> None:
        _, publisher, subscriber = await connect_hub(cleanup)
        video = publisher.register_track("video")
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

    async def test_yields_true_again_for_a_replacement_subscription(self, cleanup: Cleanup) -> None:
        _, publisher, subscriber = await connect_hub(cleanup)
        catalog = publisher.register_track("catalog")
        seen: list[bool] = []

        async def watch() -> None:
            async for bound in catalog.demand_changes():
                seen.append(bound)

        watcher = asyncio.ensure_future(watch())
        subscriber.subscribe(NAMESPACE, "catalog")
        await wait_for(lambda: seen == [False, True])

        catalog._bind(7)
        await wait_for(lambda: seen == [False, True, True])
        watcher.cancel()

    async def test_a_relay_re_bind_during_a_bound_write_resets_it_and_preserves_its_watchers(
        self, cleanup: Cleanup
    ) -> None:
        streams, publisher, subscriber = await connect_held_hub(cleanup)
        audio = publisher.register_track("audio")
        seen: list[bool] = []
        watcher = watch_demand_changes(audio, seen)
        receive(subscriber, "audio")
        await wait_for(lambda: audio.has_demand)

        # Hold the write's stream open so it stays bound in flight, then move the relay's binding to
        # another alias: the shape of a relay re-subscribing under a running publisher.
        streams.holding = True
        writing = asyncio.ensure_future(audio.write(b"x"))
        await wait_for(lambda: streams.holds == 1)
        alias = audio._alias
        assert alias is not None
        publisher._on_track_binding(TrackBinding(track_name="audio", track_alias=alias + 1))

        with pytest.raises(RelayError, match="subscription changed") as raised:
            await writing

        assert not isinstance(raised.value, RelayClosedError)
        assert not audio.closed
        assert audio.has_demand
        await wait_for(lambda: seen == [False, True, True])
        assert not watcher.done()
        # Restore the alias understood by the hub, then send fresh data on the same track.
        audio._bind(alias)
        streams.holding = False
        await audio.write(b"y")
        audio.close()
        await watcher

    async def test_a_bound_write_timeout_leaves_the_track_and_its_watchers_open(self, cleanup: Cleanup) -> None:
        streams, publisher, subscriber = await connect_held_hub(cleanup)
        audio = publisher.register_track("audio", write_timeout=0.05)
        seen: list[bool] = []
        watcher = watch_demand_changes(audio, seen)
        objects = receive(subscriber, "audio")
        await wait_for(lambda: audio.has_demand)
        streams.holding = True

        with pytest.raises(RelayTimeoutError, match="timed out"):
            await audio.write(b"x")

        assert not audio.closed
        assert audio.has_demand
        assert not watcher.done()
        assert seen == [False, True]
        streams.holding = False
        await audio.write(b"y")
        await wait_for(lambda: len(objects) == 1)
        assert (objects[0].group_id, objects[0].payload) == (1, b"y")
        audio.close()
        await watcher

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
