"""``mux_relay.media`` through its own interface: the frame writer and the catalog service over one fake track
writer, plus end-to-end checks over the relay hub. Rows follow the "Media publishing" section of
``spec/behavior.md``.
"""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any, cast

import pytest
from core_helpers.waiting import settle, wait_for
from relay_helpers.fake_track_writer import FakeTrackWriter, changed_error, closed_error, timeout_error
from relay_helpers.relay_hub import create_relay_hub

from moq_core import KeyValuePair, MoqtObject
from moq_core.transport import SendStream
from mux_relay import (
    Cancelled,
    ConnectingTransport,
    RelayError,
    RelaySubscriptionHandlers,
    connect_relay_publisher,
    connect_relay_subscriber,
)
from mux_relay.media import CatalogService, FrameOutcome, FrameWriter

TIMESTAMP = 0x10


@dataclass(frozen=True, slots=True)
class Sample:
    """A frame the way a media application hands it over: payload, keyframe flag, ready-made properties.

    ``timestamp`` is the application's own field, which a ``stale`` predicate can read; the frame writer
    never looks at it.
    """

    payload: bytes
    key: bool = True
    properties: Sequence[KeyValuePair] = ()
    discontinuity: bool = False
    timestamp: int = 0


def key(timestamp: int = 0, *, discontinuity: bool = False) -> Sample:
    return Sample(b"key", True, (KeyValuePair(TIMESTAMP, timestamp),), discontinuity, timestamp)


def delta(timestamp: int, *, discontinuity: bool = False) -> Sample:
    return Sample(b"delta", False, (KeyValuePair(TIMESTAMP, timestamp),), discontinuity, timestamp)


async def kinds(writer: FrameWriter[Sample], frames: list[Sample]) -> list[str]:
    return [(await writer.write(frame)).kind for frame in frames]


def late(*timestamps: int) -> Callable[[Sample], bool]:
    """A stale predicate that rejects the listed timestamps, the way an age check would."""
    return lambda frame: frame.timestamp in timestamps


def answers(*values: bool) -> Callable[[Sample], bool]:
    """A stale predicate scripted per call, for the points where the writer asks again after an await."""
    queue = list(values)
    return lambda frame: queue.pop(0) if queue else False


class TestKeyframeGrouping:
    async def test_a_keyframe_opens_a_group_and_deltas_append_until_the_next_keyframe(self) -> None:
        track = FakeTrackWriter()
        writer: FrameWriter[Sample] = FrameWriter(track)

        outcomes = [await writer.write(frame) for frame in [key(0), delta(1), delta(2), key(3), delta(4)]]

        assert [outcome.kind for outcome in outcomes] == ["sent"] * 5
        assert [outcome.group_id for outcome in outcomes] == [0, 0, 0, 1, 1]
        assert track.groups[0].payloads == [b"key", b"delta", b"delta"]
        assert track.groups[0].finished, "the second keyframe FINs the first group"
        assert track.groups[1].payloads == [b"key", b"delta"]
        assert not track.groups[1].finished
        assert track.groups[0].objects[1][1] == (KeyValuePair(TIMESTAMP, 1),), "properties travel as supplied"
        assert (writer.groups, writer.objects) == (2, 5)
        assert writer.ended is None and not writer.closed

    async def test_frames_without_demand_are_skipped_at_once_and_publishing_resumes_at_a_keyframe(self) -> None:
        track = FakeTrackWriter(demand=False)
        writer: FrameWriter[Sample] = FrameWriter(track)

        assert await kinds(writer, [key(0), delta(1)]) == ["skipped", "skipped"]
        assert track.groups == [], "nothing is opened and nothing waits on the write deadline"

        track.bind(True)

        assert await kinds(writer, [delta(2)]) == ["skipped"], "mid-GOP demand waits for the next keyframe"
        assert await kinds(writer, [key(3), delta(4)]) == ["sent", "sent"]
        assert track.groups[0].payloads == [b"key", b"delta"]

    async def test_a_stale_keyframe_resets_the_open_group_and_deltas_wait_for_the_next_keyframe(self) -> None:
        track = FakeTrackWriter()
        writer = FrameWriter(track, stale=late(2, 3))

        assert await kinds(writer, [key(0), delta(1)]) == ["sent", "sent"]
        assert await writer.write(key(2)) == FrameOutcome("stale", group_id=0), "the reset GOP is named"
        assert track.groups[0].aborts == [None], "the GOP in progress cannot be completed"
        # Late, then dependent on the dropped keyframe; nothing left to reset.
        assert [await writer.write(frame) for frame in [delta(3), delta(4)]] == [FrameOutcome("stale")] * 2
        assert await kinds(writer, [key(5), delta(6)]) == ["sent", "sent"]
        assert track.groups[1].payloads == [b"key", b"delta"]
        assert (writer.groups, writer.objects) == (2, 4)

    async def test_a_discontinuity_resets_the_open_group_and_a_keyframe_with_one_starts_fresh(self) -> None:
        track = FakeTrackWriter()
        writer: FrameWriter[Sample] = FrameWriter(track)

        assert await kinds(writer, [key(0), delta(1)]) == ["sent", "sent"]
        assert await writer.write(delta(2, discontinuity=True)) == FrameOutcome("stale", group_id=0)
        assert await writer.write(delta(3)) == FrameOutcome("stale")
        assert track.groups[0].aborts == [None]
        assert await kinds(writer, [key(4, discontinuity=True), delta(5)]) == ["sent", "sent"]
        assert track.groups[1].payloads == [b"key", b"delta"]

    async def test_staleness_is_asked_again_after_the_fin_and_after_the_stream_opens(self) -> None:
        # Time passes inside those awaits; a keyframe that was fresh before them may not be after.
        track = FakeTrackWriter()
        writer = FrameWriter(track, stale=answers(False, False, False, False, True))

        assert await kinds(writer, [key(0)]) == ["sent"]  # asked before, after the FIN, before the write
        # Asked before, then after the FIN of group 0: nothing is open any more, so nothing is reset.
        assert await writer.write(key(1)) == FrameOutcome("stale")
        assert track.groups[0].finished
        assert len(track.groups) == 1, "no group is opened for a keyframe that went stale during the FIN"
        assert await kinds(writer, [delta(2)]) == ["stale"], "it depends on the dropped keyframe"

        writer = FrameWriter(track, stale=answers(False, False, True))

        # Fresh before and after the FIN, stale once the stream is open: the new group is reset and named.
        assert await writer.write(key(3)) == FrameOutcome("stale", group_id=1)
        assert track.groups[1].aborts == [None] and track.groups[1].objects == []
        assert (writer.groups, writer.objects) == (1, 0)

    async def test_an_interrupted_group_write_resets_the_group_and_the_next_keyframe_opens_another(self) -> None:
        track = FakeTrackWriter()
        error = changed_error()
        track.fail("group-write", error, on=1)
        writer: FrameWriter[Sample] = FrameWriter(track)

        assert await kinds(writer, [key(0)]) == ["sent"]
        interrupted = await writer.write(delta(1))

        assert interrupted == FrameOutcome("interrupted", error=error, group_id=0)
        assert await kinds(writer, [delta(2)]) == ["skipped"], "the group is gone; the track is fine"
        assert writer.ended is None and not writer.closed
        assert await kinds(writer, [key(3), delta(4)]) == ["sent", "sent"]
        assert track.groups[1].payloads == [b"key", b"delta"]
        assert (writer.groups, writer.objects) == (2, 3)

    async def test_an_interrupted_group_open_is_reported_on_the_keyframe(self) -> None:
        track = FakeTrackWriter()
        error = timeout_error("waiting-for-demand")
        track.fail("open", error)
        writer: FrameWriter[Sample] = FrameWriter(track)

        assert await writer.write(key(0)) == FrameOutcome("interrupted", error=error, group_id=None)
        assert await kinds(writer, [delta(1)]) == ["skipped"]
        assert await kinds(writer, [key(2)]) == ["sent"]
        assert writer.groups == 1

    async def test_a_closed_error_ends_the_writer_and_every_later_frame(self) -> None:
        track = FakeTrackWriter()
        error = closed_error()
        track.fail("group-write", error)
        writer: FrameWriter[Sample] = FrameWriter(track)

        assert await writer.write(key(0)) == FrameOutcome("ended", error=error, group_id=0)
        assert writer.ended is error and writer.closed
        assert await writer.write(key(1)) == FrameOutcome("ended", error=error)
        assert len(track.groups) == 1, "a retired writer never touches the track again"

        # The application's own close still wins: afterwards a write is a programming error.
        await writer.close()

        with pytest.raises(RelayError) as caught:
            await writer.write(key(2))
        assert caught.value.code == "CLOSED"

    async def test_a_transient_error_on_a_track_that_closed_is_a_retirement_too(self) -> None:
        # The SDK may report the change of subscription first and close the track in the same turn;
        # `closed` afterwards is what tells them apart, never `has_demand`.
        track = FakeTrackWriter()
        error = changed_error()
        track.fail("group-write", error, retire=True)
        writer: FrameWriter[Sample] = FrameWriter(track)

        assert await writer.write(key(0)) == FrameOutcome("ended", error=error, group_id=0)
        assert writer.ended is error

    async def test_a_transient_error_with_demand_gone_is_only_an_interruption(self) -> None:
        track = FakeTrackWriter()
        track.fail("group-write", changed_error())
        writer: FrameWriter[Sample] = FrameWriter(track)

        assert await kinds(writer, [key(0)]) == ["interrupted"]
        track.bind(False)
        assert await kinds(writer, [key(1)]) == ["skipped"]
        assert writer.ended is None

    async def test_close_fins_the_open_group_and_refuses_further_frames(self) -> None:
        track = FakeTrackWriter()
        writer: FrameWriter[Sample] = FrameWriter(track)
        await kinds(writer, [key(0), delta(1)])

        await writer.close()

        assert track.groups[0].finished and writer.closed
        with pytest.raises(RelayError) as caught:
            await writer.write(key(2))
        assert caught.value.code == "CLOSED"

        await writer.close()
        assert len(track.groups) == 1

    async def test_close_swallows_a_relay_error_from_the_fin_but_nothing_else(self) -> None:
        track = FakeTrackWriter()
        track.fail("group-close", changed_error())
        writer: FrameWriter[Sample] = FrameWriter(track)
        await kinds(writer, [key(0)])
        await writer.close()
        assert writer.closed

        stopped = Cancelled("the application stopped")
        track = FakeTrackWriter()
        track.fail("group-close", stopped)
        writer = FrameWriter(track)
        await kinds(writer, [key(0)])
        with pytest.raises(Cancelled) as caught:
            await writer.close()
        assert caught.value is stopped
        assert writer.closed, "the writer is closed either way"

    async def test_close_without_an_open_group_touches_nothing(self) -> None:
        track = FakeTrackWriter(demand=False)
        writer: FrameWriter[Sample] = FrameWriter(track)
        await kinds(writer, [key(0)])
        await writer.close()
        assert track.groups == []

    async def test_a_cancelled_write_abandons_the_group_and_the_next_keyframe_opens_another(self) -> None:
        # The caller's own deadline or cancellation lands inside group.write(). The SDK resets that
        # group and keeps failing it with the same reason; holding on to it would replay the reason
        # from the next FIN although the track is healthy.
        gate = asyncio.Event()
        track = FakeTrackWriter(block=gate)
        writer: FrameWriter[Sample] = FrameWriter(track)
        task = asyncio.create_task(writer.write(key(0)))
        await track.blocked.wait()

        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        track.block = None
        assert await kinds(writer, [delta(1)]) == ["skipped"], "the GOP in progress is gone"
        assert await kinds(writer, [key(2), delta(3)]) == ["sent", "sent"]
        assert track.groups[1].payloads == [b"key", b"delta"]
        assert not track.groups[0].finished, "a cancelled group is abandoned, never FINned"
        await writer.close()
        assert track.groups[1].finished and writer.closed
        assert (writer.groups, writer.objects) == (2, 2)

    async def test_a_frame_the_sdk_rejects_resets_the_group_and_propagates(self) -> None:
        # An oversized payload or a bad property is a programming error the SDK raises before the
        # group's operation is touched; the group must still be reset or the write slot stays taken.
        track = FakeTrackWriter()
        track.fail("group-write", TypeError("Payload must be bytes-like"), on=1)
        writer: FrameWriter[Sample] = FrameWriter(track)

        assert await kinds(writer, [key(0)]) == ["sent"]

        with pytest.raises(TypeError):
            await writer.write(delta(1))

        assert track.groups[0].aborts == [None]
        assert await kinds(writer, [delta(2)]) == ["skipped"]
        assert await kinds(writer, [key(3)]) == ["sent"]
        assert track.groups[1].payloads == [b"key"]

    @pytest.mark.parametrize("raising_call", [3, 4])
    async def test_a_stale_predicate_that_raises_resets_the_gop_in_progress(self, raising_call: int) -> None:
        # Call 3 is the check after the keyframe's group opened: the group never gets its keyframe.
        # Call 4 is the next delta's first check: the group has its keyframe but would lose a delta.
        # Either way the next delta must not be sent into that group, and the slot must be free.
        calls = 0

        def broken(frame: Sample) -> bool:
            nonlocal calls
            calls += 1

            if calls == raising_call:
                raise RuntimeError("the application's clock broke")

            return False

        track = FakeTrackWriter()
        writer = FrameWriter(track, stale=broken)
        frames = [key(0), delta(1)]

        with pytest.raises(RuntimeError, match="clock"):
            for frame in frames:
                await writer.write(frame)

        assert track.groups[0].aborts == [None]
        assert await kinds(writer, [delta(2)]) == ["skipped"], "never object 0 of a group without its keyframe"
        assert await kinds(writer, [key(3), delta(4)]) == ["sent", "sent"]
        assert track.groups[1].payloads == [b"key", b"delta"]
        assert track.groups[0].payloads == ([] if raising_call == 3 else [b"key"])

    def test_grouping_is_checked_as_an_argument(self) -> None:
        with pytest.raises(TypeError):
            FrameWriter(FakeTrackWriter(), grouping=3)  # ty: ignore[invalid-argument-type]
        with pytest.raises(ValueError, match="keyframe"):
            FrameWriter(FakeTrackWriter(), grouping="gop")  # ty: ignore[invalid-argument-type]


class TestFrameGrouping:
    async def test_every_frame_is_its_own_finished_group(self) -> None:
        track = FakeTrackWriter()
        writer: FrameWriter[Sample] = FrameWriter(track, grouping="frame")

        outcomes = [await writer.write(frame) for frame in [delta(0), delta(1), key(2)]]

        assert [outcome.kind for outcome in outcomes] == ["sent"] * 3
        assert [outcome.group_id for outcome in outcomes] == [0, 1, 2]
        assert [group.payloads for group in track.groups] == [[b"delta"], [b"delta"], [b"key"]]
        assert all(group.finished for group in track.groups)
        assert (writer.groups, writer.objects) == (3, 3)

    async def test_frames_without_demand_open_nothing(self) -> None:
        track = FakeTrackWriter(demand=False)
        writer: FrameWriter[Sample] = FrameWriter(track, grouping="frame")

        assert await kinds(writer, [key(0), key(1)]) == ["skipped", "skipped"]
        assert track.groups == []

    async def test_a_stale_frame_is_dropped_before_or_after_its_stream_opens(self) -> None:
        track = FakeTrackWriter()
        writer = FrameWriter(track, grouping="frame", stale=answers(True, False, True))

        assert await writer.write(key(0)) == FrameOutcome("stale")
        assert track.groups == []
        assert await writer.write(key(1)) == FrameOutcome("stale", group_id=0)
        assert track.groups[0].aborts == [None] and track.groups[0].objects == []
        assert (writer.groups, writer.objects) == (1, 0), "the group was opened, nothing was accepted"

    async def test_an_interrupted_frame_resets_its_group_and_the_next_frame_proceeds(self) -> None:
        track = FakeTrackWriter()
        error = timeout_error()
        track.fail("group-write", error)
        writer: FrameWriter[Sample] = FrameWriter(track, grouping="frame")

        assert await writer.write(key(0)) == FrameOutcome("interrupted", error=error, group_id=0)
        assert track.groups[0].aborts == [None]
        assert await writer.write(key(1)) == FrameOutcome("sent", group_id=1)
        assert (writer.groups, writer.objects) == (2, 1), "both groups were opened; one object was accepted"

    async def test_a_failed_fin_is_an_interruption(self) -> None:
        track = FakeTrackWriter()
        error = timeout_error("closing")
        track.fail("group-close", error)
        writer: FrameWriter[Sample] = FrameWriter(track, grouping="frame")

        assert await writer.write(key(0)) == FrameOutcome("interrupted", error=error, group_id=0)
        assert track.groups[0].aborts == [None]
        assert (writer.groups, writer.objects) == (1, 1), "the object was accepted before the FIN failed"

    async def test_a_retirement_ends_the_writer(self) -> None:
        track = FakeTrackWriter()
        error = closed_error()
        track.fail("open", error)
        writer: FrameWriter[Sample] = FrameWriter(track, grouping="frame")

        assert await writer.write(key(0)) == FrameOutcome("ended", error=error, group_id=None)
        assert await writer.write(key(1)) == FrameOutcome("ended", error=error)
        assert track.groups == []

    async def test_keyframe_and_discontinuity_flags_do_not_matter_for_independent_frames(self) -> None:
        track = FakeTrackWriter()
        writer: FrameWriter[Sample] = FrameWriter(track, grouping="frame")

        assert await kinds(writer, [delta(0, discontinuity=True), key(1, discontinuity=True)]) == ["sent", "sent"]
        assert [group.aborts for group in track.groups] == [[], []]

    async def test_a_stale_predicate_that_raises_after_the_stream_opened_resets_the_group(self) -> None:
        # The caller's bug propagates, but not with the track's write slot still taken.
        calls = 0

        def broken(frame: Sample) -> bool:
            nonlocal calls
            calls += 1

            if calls == 2:
                raise RuntimeError("the application's clock broke")

            return False

        track = FakeTrackWriter()
        writer = FrameWriter(track, grouping="frame", stale=broken)

        with pytest.raises(RuntimeError, match="clock"):
            await writer.write(key(0))

        assert track.groups[0].aborts == [None] and track.groups[0].objects == []
        assert await writer.write(key(1)) == FrameOutcome("sent", group_id=1)

    async def test_task_cancellation_inside_a_write_resets_the_group_and_propagates(self) -> None:
        gate = asyncio.Event()
        track = FakeTrackWriter(block=gate)
        writer: FrameWriter[Sample] = FrameWriter(track, grouping="frame")
        task = asyncio.create_task(writer.write(key(0)))
        await track.blocked.wait()

        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task
        assert track.groups[0].aborts == [None]

        track.block = None
        assert await writer.write(key(1)) == FrameOutcome("sent", group_id=1), "nothing of the cancelled group lingers"
        assert (writer.groups, writer.objects) == (2, 1)


class TestCatalogService:
    async def test_sends_the_document_on_current_demand_and_on_every_later_relay_subscription(self) -> None:
        track = FakeTrackWriter()
        service = CatalogService(track, b"catalog")
        serving = asyncio.create_task(service.serve())
        try:
            await wait_for(lambda: track.writes == [b"catalog"])

            track.bind(True)  # a replacement relay subscription needs the document again
            await wait_for(lambda: len(track.writes) == 2)

            track.bind(False)
            await settle()
            assert len(track.writes) == 2

            track.bind(True)
            await wait_for(lambda: len(track.writes) == 3)
            assert (service.sends, service.failures) == (3, 0)
        finally:
            serving.cancel()
            await asyncio.gather(serving, return_exceptions=True)

    async def test_nothing_is_sent_while_the_relay_has_no_subscription(self) -> None:
        track = FakeTrackWriter(demand=False)
        service = CatalogService(track, b"catalog")
        serving = asyncio.create_task(service.serve())
        try:
            await settle()
            assert track.writes == []

            track.bind(True)
            await wait_for(lambda: track.writes == [b"catalog"])
        finally:
            serving.cancel()
            await asyncio.gather(serving, return_exceptions=True)

    async def test_retries_a_transient_failure_after_the_delay_without_a_new_subscription(self) -> None:
        loop = asyncio.get_running_loop()
        attempts: list[float] = []

        class Timed(FakeTrackWriter):
            async def write(self, payload: bytes, *, properties: Sequence[KeyValuePair] | None = None) -> None:
                attempts.append(loop.time())
                await super().write(payload, properties=properties)

        track = Timed()
        track.fail("write", timeout_error())
        service = CatalogService(track, b"current catalog", retry_delay=0.01)
        serving = asyncio.create_task(service.serve())
        try:
            await wait_for(lambda: track.writes == [b"current catalog"])
            assert len(attempts) == 2
            assert attempts[1] - attempts[0] >= 0.01
            assert (service.sends, service.failures) == (1, 1)
        finally:
            serving.cancel()
            await asyncio.gather(serving, return_exceptions=True)

    @pytest.mark.parametrize("stop", ["cancel", "no_demand"])
    async def test_the_retry_pause_ends_on_cancellation_or_lost_demand(self, stop: str) -> None:
        track = FakeTrackWriter()
        failed = asyncio.Event()

        class Noticed(FakeTrackWriter):
            async def write(self, payload: bytes, *, properties: Sequence[KeyValuePair] | None = None) -> None:
                failed.set()
                await super().write(payload, properties=properties)

        track = Noticed()
        track.fail("write", timeout_error())
        service = CatalogService(track, b"{}", retry_delay=0.05)
        serving = asyncio.create_task(service.serve())
        await failed.wait()

        if stop == "cancel":
            serving.cancel()
            with pytest.raises(asyncio.CancelledError):
                await serving
        else:
            track.has_demand = False  # the relay unsubscribed during the pause
            await asyncio.sleep(0.1)
            track.end()
            await asyncio.wait_for(serving, 1)

        assert track.writes == [] and service.failures == 1
        assert track.calls["write"] == 1

    async def test_a_retired_catalog_track_raises_from_serve(self) -> None:
        track = FakeTrackWriter()
        error = closed_error()
        track.fail("write", error)
        service = CatalogService(track, b"{}")

        with pytest.raises(RelayError) as caught:
            await asyncio.wait_for(service.serve(), 1)

        assert caught.value is error
        assert (service.sends, service.failures) == (0, 0)

    async def test_a_transient_failure_on_a_track_that_closed_raises_too(self) -> None:
        track = FakeTrackWriter()
        error = changed_error()
        track.fail("write", error, retire=True)
        service = CatalogService(track, b"{}")

        with pytest.raises(RelayError) as caught:
            await asyncio.wait_for(service.serve(), 1)

        assert caught.value is error

    async def test_serve_returns_when_the_track_ends_without_a_failed_write(self) -> None:
        track = FakeTrackWriter()
        service = CatalogService(track, b"{}")
        serving = asyncio.create_task(service.serve())
        await wait_for(lambda: track.writes == [b"{}"])

        track.end()

        assert await asyncio.wait_for(serving, 1) is None
        assert service.sends == 1

    def test_arguments_are_checked_before_the_track_is_touched(self) -> None:
        track = FakeTrackWriter()

        with pytest.raises(TypeError):
            CatalogService(track, "text")  # ty: ignore[invalid-argument-type]
        with pytest.raises(ValueError, match="16 MiB"):
            CatalogService(track, bytes(16 * 1024 * 1024 + 1))
        with pytest.raises(TypeError):
            CatalogService(track, b"{}", retry_delay="1")  # ty: ignore[invalid-argument-type]
        with pytest.raises(ValueError):
            CatalogService(track, b"{}", retry_delay=-1)
        assert track.calls == {"write": 0, "open": 0, "group-write": 0, "group-close": 0}

    async def test_the_document_is_copied_once(self) -> None:
        track = FakeTrackWriter()
        document = bytearray(b"first")
        service = CatalogService(track, document)  # ty: ignore[invalid-argument-type]  # bytes-like is accepted
        document[:] = b"other"
        serving = asyncio.create_task(service.serve())
        try:
            await wait_for(lambda: track.writes == [b"first"])
        finally:
            serving.cancel()
            await asyncio.gather(serving, return_exceptions=True)


# ---------------------------------------------------------------------------------------------------
# Over the relay hub: the real RelayTrackWriter behind the module's protocol
# ---------------------------------------------------------------------------------------------------

NAMESPACE = ["testing", "media"]
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


def received(subscriber: Any, track: str) -> list[MoqtObject]:
    objects: list[MoqtObject] = []
    subscriber.subscribe(NAMESPACE, track, RelaySubscriptionHandlers(on_object=objects.append))
    return objects


class TestOverTheRelayHub:
    async def test_the_frame_writer_maps_a_gop_onto_one_group_and_independent_frames_onto_their_own(
        self, cleanup: Cleanup
    ) -> None:
        hub = create_relay_hub()
        cleanup.append(hub.destroy)
        publisher = await connect_relay_publisher(URL, namespace=NAMESPACE, create_transport=hub.connect_publisher)
        subscriber = await connect_relay_subscriber(URL, create_transport=hub.connect_subscriber)
        cleanup.extend([subscriber.close, publisher.close])
        video_track = publisher.register_track("video")
        audio_track = publisher.register_track("audio")
        video_objects = received(subscriber, "video")
        audio_objects = received(subscriber, "audio")
        await wait_for(lambda: video_track.has_demand and audio_track.has_demand)

        video: FrameWriter[Sample] = FrameWriter(video_track)
        audio: FrameWriter[Sample] = FrameWriter(audio_track, grouping="frame")

        assert await kinds(video, [key(0), delta(1), delta(2), key(3)]) == ["sent"] * 4
        assert await kinds(audio, [key(10), key(11)]) == ["sent", "sent"]
        await video.close()

        await wait_for(lambda: len(video_objects) == 4 and len(audio_objects) == 2)
        assert [(obj.group_id, obj.object_id) for obj in video_objects] == [(0, 0), (0, 1), (0, 2), (1, 0)]
        assert [(obj.group_id, obj.object_id) for obj in audio_objects] == [(0, 0), (1, 0)]
        assert [p.value for obj in video_objects for p in obj.properties if p.type == TIMESTAMP] == [0, 1, 2, 3]
        assert (video.groups, video.objects, audio.groups, audio.objects) == (2, 4, 2, 2)

    async def test_a_frame_the_sdk_rejects_frees_the_write_slot_for_the_next_keyframe(self, cleanup: Cleanup) -> None:
        hub = create_relay_hub()
        cleanup.append(hub.destroy)
        publisher = await connect_relay_publisher(URL, namespace=NAMESPACE, create_transport=hub.connect_publisher)
        subscriber = await connect_relay_subscriber(URL, create_transport=hub.connect_subscriber)
        cleanup.extend([subscriber.close, publisher.close])
        video_track = publisher.register_track("video")
        objects = received(subscriber, "video")
        await wait_for(lambda: video_track.has_demand)
        video: FrameWriter[Sample] = FrameWriter(video_track)

        assert await kinds(video, [key(0)]) == ["sent"]

        with pytest.raises(TypeError):
            await video.write(Sample(cast(Any, "not bytes"), False, (KeyValuePair(TIMESTAMP, 1),), False, 1))

        # Not WRITE_PENDING: the rejected frame's group was reset and the slot is free again.
        assert await kinds(video, [key(2), delta(3)]) == ["sent", "sent"]
        await video.close()

        await wait_for(lambda: len(objects) == 3)
        assert [(obj.group_id, obj.object_id) for obj in objects] == [(0, 0), (1, 0), (1, 1)]
        assert (video.groups, video.objects) == (2, 3)

    async def test_the_catalog_retries_after_a_bound_timeout_without_another_subscription(
        self, cleanup: Cleanup
    ) -> None:
        hub = create_relay_hub()
        cleanup.append(hub.destroy)

        def connect_publisher(url: str, protocols: Sequence[str]) -> ConnectingTransport:
            created = hub.connect_publisher(url, protocols)
            transport: Any = created.transport
            open_stream = transport.create_unidirectional_stream
            streams = 0

            async def fail_once() -> SendStream:
                nonlocal streams
                streams += 1
                if streams == 2:
                    raise RelayError("bound write timed out", code="WRITE_TIMEOUT", phase="opening-stream")
                return await open_stream()

            transport.create_unidirectional_stream = fail_once
            return created

        publisher = await connect_relay_publisher(URL, namespace=NAMESPACE, create_transport=connect_publisher)
        subscriber = await connect_relay_subscriber(URL, create_transport=hub.connect_subscriber)
        cleanup.extend([subscriber.close, publisher.close])
        catalog = publisher.register_track("catalog")
        objects = received(subscriber, "catalog")
        service = CatalogService(catalog, b"latest catalog", retry_delay=0.01)
        serving = asyncio.create_task(service.serve())
        try:
            await wait_for(lambda: len(objects) == 1)
            assert hub.subscribes == ["catalog"]
            assert (objects[0].group_id, objects[0].payload) == (1, b"latest catalog")
            assert (service.sends, service.failures) == (1, 1)
            assert not catalog.closed
        finally:
            serving.cancel()
            await asyncio.gather(serving, return_exceptions=True)

    async def test_a_closed_track_ends_the_frame_writer_and_the_catalog_service(self, cleanup: Cleanup) -> None:
        hub = create_relay_hub()
        cleanup.append(hub.destroy)
        publisher = await connect_relay_publisher(URL, namespace=NAMESPACE, create_transport=hub.connect_publisher)
        subscriber = await connect_relay_subscriber(URL, create_transport=hub.connect_subscriber)
        cleanup.extend([subscriber.close, publisher.close])
        video_track = publisher.register_track("video")
        catalog_track = publisher.register_track("catalog")
        received(subscriber, "video")
        received(subscriber, "catalog")
        await wait_for(lambda: video_track.has_demand and catalog_track.has_demand)
        video: FrameWriter[Sample] = FrameWriter(video_track)
        service = CatalogService(catalog_track, b"{}")
        serving = asyncio.create_task(service.serve())
        await wait_for(lambda: service.sends == 1)

        video_track.close()
        catalog_track.close()

        outcome = await video.write(key(0))
        assert outcome.kind == "ended" and outcome.error is not None and outcome.error.code == "CLOSED"
        assert video.ended is outcome.error
        assert await asyncio.wait_for(serving, 1) is None, "the track ended quietly: no write failed"
