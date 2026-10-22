"""What the example does with the SDK's outcomes: the counters, the track-end policy, and the run's verdict."""

import asyncio
import time
from typing import Any, Literal, cast

import pytest
from core_helpers.waiting import wait_for
from relay_helpers.fake_track_writer import FakeTrackWriter, changed_error, closed_error
from videojs_stream.stream import (
    EncodedSample,
    LiveQueue,
    Log,
    Stats,
    TrackEnded,
    TrackEndHandler,
    discard_queue,
    make_track_end_handler,
    publish_media,
    run_failure,
    serve_catalog,
    stale_for,
    summarize,
)

from caprid_relay import CancellationToken, Cancelled, RelayError
from caprid_relay.media import CatalogService, FrameWriter
from moq_media import loc_properties

Ended = list[tuple[str, RelayError]]


def audio_samples(count: int) -> list[EncodedSample]:
    return [
        EncodedSample("audio", i * 20_000, b"\xfc\xff", True, properties=loc_properties(i * 20_000))
        for i in range(count)
    ]


def video_samples(count: int) -> list[EncodedSample]:
    """One keyframe then delta frames, the shape a GOP arrives in."""
    return [
        EncodedSample("video", i * 33_333, b"\x00", i == 0, properties=loc_properties(i * 33_333)) for i in range(count)
    ]


def queue_of(samples: list[EncodedSample], kind: str, stats: Stats, *, stale: bool = False) -> LiveQueue:
    """A queue with the samples already in it and the encoder finished."""
    queue = LiveQueue(cast(Any, kind), stats, max_samples=10_000, max_bytes=2**30, max_lag=1.0)

    if stale:
        # The producer clock says these frames are a hundred seconds old.
        queue.started = time.monotonic() - 100

    for sample in samples:
        queue.put_nowait(sample)

    queue.put_nowait(None)
    return queue


async def drive(
    samples: list[EncodedSample],
    *,
    video: FakeTrackWriter | None = None,
    audio: FakeTrackWriter | None = None,
    on_track_end: TrackEndHandler | None = None,
    lifetime: CancellationToken | None = None,
    log: Log | None = None,
    stale: bool = False,
) -> tuple[Stats, Ended]:
    """Run the example's publish loop for each track over a scripted sample list and report what it counted."""
    stats = Stats()
    ended: Ended = []
    tracks: list[tuple[str, FakeTrackWriter | None, Literal["keyframe", "frame"]]] = [
        ("video", video, "keyframe"),
        ("audio", audio, "frame"),
    ]

    for kind, track, grouping in tracks:
        if track is None:
            continue

        queue = queue_of([sample for sample in samples if sample.kind == kind], kind, stats, stale=stale)
        writer = FrameWriter(track, grouping=grouping, stale=stale_for(queue))
        await publish_media(
            queue,
            writer,
            stats,
            log or (lambda event, fields: None),
            on_track_end or (lambda track, error: ended.append((track, error))),
            lifetime or CancellationToken(),
        )

    return stats, ended


class TestCounting:
    """Every sample the encoder produced lands in exactly one counter of the `finished` line."""

    async def test_a_retired_audio_track_is_reported_once_and_the_rest_counted_as_dropped(self) -> None:
        # #32: a 30-minute run reported skippedAudio=0 while ~74,700 frames were never sent.
        rebind = changed_error()
        audio = FakeTrackWriter()
        audio.fail("open", rebind, on=3, retire=True)
        stats, ended = await drive(audio_samples(10), audio=audio)

        assert (stats.audio_objects, stats.dropped_audio, stats.skipped_audio) == (3, 7, 0)
        assert ended == [("audio", rebind)]
        assert len(audio.groups) == 3
        assert summarize(stats)["droppedAudio"] == 7

    async def test_frames_with_no_subscription_stay_skipped_not_dropped(self) -> None:
        stats, ended = await drive(audio_samples(4), audio=FakeTrackWriter(demand=False))

        assert (stats.audio_objects, stats.skipped_audio, stats.dropped_audio) == (0, 4, 0)
        assert ended == []
        assert summarize(stats)["endedTracks"] is None

    async def test_an_interrupted_audio_frame_is_skipped_and_the_next_one_tries_again(self) -> None:
        vanished = RelayError(
            "Relay subscription ended before the write started", code="SUBSCRIPTION_CHANGED", phase="waiting-for-demand"
        )
        audio = FakeTrackWriter()
        audio.fail("open", vanished, on=1)
        stats, ended = await drive(audio_samples(3), audio=audio)

        assert (stats.audio_objects, stats.skipped_audio, stats.dropped_audio) == (2, 1, 0)
        assert ended == []

    async def test_a_closed_video_track_is_reported_once_and_its_frames_counted_as_dropped(self) -> None:
        closed = closed_error()
        video = FakeTrackWriter()
        video.fail("open", closed)
        keyframes = [
            EncodedSample("video", i * 33_333, b"\x00", True, properties=loc_properties(i * 33_333)) for i in range(3)
        ]
        stats, ended = await drive(keyframes, video=video)

        assert (stats.video_groups, stats.video_objects, stats.dropped_video, stats.skipped_video) == (0, 0, 3, 0)
        assert ended == [("video", closed)]

    async def test_an_interrupted_group_counts_its_own_sample_as_skipped(self) -> None:
        # The group is reset and the track survives, so the sample that failed is a skip like the
        # ones that follow it before the next keyframe, and it is in a counter either way.
        video = FakeTrackWriter()
        video.fail("group-write", changed_error())
        stats, ended = await drive(video_samples(3), video=video)

        # The keyframe failed inside its group's open, so no group came back to count.
        assert (stats.video_groups, stats.video_objects, stats.skipped_video, stats.dropped_video) == (0, 0, 3, 0)
        assert ended == []

    async def test_a_terminal_group_write_failure_counts_its_own_sample_as_dropped(self) -> None:
        closed = closed_error()
        video = FakeTrackWriter()
        video.fail("group-write", closed)
        stats, ended = await drive(video_samples(3), video=video)

        assert (stats.video_groups, stats.video_objects, stats.skipped_video, stats.dropped_video) == (0, 0, 0, 3)
        assert ended == [("video", closed)]

    async def test_stale_samples_are_counted_as_stale(self) -> None:
        video = FakeTrackWriter()
        stats, ended = await drive(video_samples(3), video=video, stale=True)

        assert (stats.video_objects, stats.stale_video, stats.skipped_video) == (0, 3, 0)
        assert video.groups == [] and ended == []

    async def test_video_flowing_is_logged_once_and_interruptions_name_their_group(self) -> None:
        events: list[tuple[str, dict[str, Any]]] = []
        video = FakeTrackWriter()
        video.fail("group-write", changed_error(), on=2)
        stats, _ = await drive(video_samples(4), video=video, log=lambda event, fields: events.append((event, fields)))

        assert [event for event, _ in events] == ["video flowing", "video group interrupted"]
        assert events[0][1] == {"group": 0}
        assert events[1][1]["group"] == 0 and "SUBSCRIPTION_CHANGED" not in events[1][1]["error"]
        assert (stats.video_objects, stats.skipped_video) == (2, 2)

    async def test_every_sample_lands_in_exactly_one_counter(self) -> None:
        # What `summarize` promises: sent + skipped + dropped accounts for the whole timeline.
        video = FakeTrackWriter()
        video.fail("group-write", changed_error(), on=2)
        stats, _ = await drive(video_samples(6) + audio_samples(4), video=video, audio=FakeTrackWriter(demand=False))

        video_total = stats.video_objects + stats.skipped_video + stats.dropped_video
        audio_total = stats.audio_objects + stats.skipped_audio + stats.dropped_audio

        assert (video_total, audio_total) == (6, 4)


class TestStopPolicy:
    """Cancelling for a retired track must not come back as an anonymous publishing failure."""

    async def test_the_lifetime_reason_escapes_the_publish_loop_as_itself(self) -> None:
        # The SDK re-raises the application's cancel reason out of the write still in flight and out
        # of the FIN in the teardown. Neither is a RelayError; the first ends the loop, the second is
        # the same object and must not replace it.
        lifetime = CancellationToken()
        reason = TrackEnded("the audio track ended mid-run: RelayError: gone")
        lifetime.cancel(reason)
        stats = Stats()
        video = FakeTrackWriter()
        video.fail("group-write", reason, on=1)
        video.fail("group-close", reason)
        queue = queue_of(video_samples(4), "video", stats)

        with pytest.raises(TrackEnded) as caught:
            await publish_media(
                queue, FrameWriter(video, stale=stale_for(queue)), stats, lambda *_: None, lambda *_: None, lifetime
            )

        assert caught.value is reason
        discard_queue(queue, stats)
        assert (stats.video_objects, stats.discarded_video) == (1, 3), "the one in flight plus two still queued"
        assert summarize(stats)["discardedVideo"] == 3

        failure = run_failure(
            producer_error=None, media_failure=caught.value, cancel_reason=lifetime.reason, track_end_reason=reason
        )

        # Not `RuntimeError: publishing failed: TrackEnded: ...`, which says nothing about the track.
        assert failure is reason

    async def test_a_teardown_failure_that_is_not_the_lifetime_reason_propagates(self) -> None:
        lifetime = CancellationToken()
        stats = Stats()
        video = FakeTrackWriter()
        video.fail("group-close", RuntimeError("a real bug in the FIN"))
        queue = queue_of(video_samples(1), "video", stats)

        with pytest.raises(RuntimeError, match="real bug"):
            await publish_media(
                queue, FrameWriter(video, stale=stale_for(queue)), stats, lambda *_: None, lambda *_: None, lifetime
            )

    async def test_a_track_end_under_the_stop_policy_is_reported_as_itself(self) -> None:
        lifetime = CancellationToken()
        stats = Stats()
        reasons: list[BaseException] = []

        def stop(reason: BaseException) -> None:
            reasons.append(reason)
            lifetime.cancel(reason)

        rebind = changed_error()
        audio = FakeTrackWriter()
        audio.fail("open", rebind, retire=True)
        handler = make_track_end_handler("stop", stats, lambda event, fields: None, stop)
        queue = queue_of(audio_samples(3), "audio", stats)
        await publish_media(queue, FrameWriter(audio, grouping="frame"), stats, lambda *_: None, handler, lifetime)

        assert stats.ended_tracks == {"audio": f"RelayError: {rebind}"}
        assert [type(reason).__name__ for reason in reasons] == ["TrackEnded"]
        assert stats.dropped_audio == 3

        # The video task dies with that same reason; the run reports the track end, not the death.
        failure = run_failure(
            producer_error=None,
            media_failure=lifetime.reason,
            cancel_reason=lifetime.reason,
            track_end_reason=reasons[0],
        )
        assert failure is reasons[0]
        assert "the audio track ended mid-run" in str(failure)


class TestRunFailure:
    def test_a_clean_finish_reports_nothing(self) -> None:
        assert run_failure(producer_error=None, media_failure=None, cancel_reason=None, track_end_reason=None) is None

    @pytest.mark.parametrize("noticed", [True, False])
    def test_a_deadline_is_a_clean_finish(self, noticed: bool) -> None:
        deadline = Cancelled("cancelled")
        assert (
            run_failure(
                producer_error=None,
                media_failure=deadline if noticed else None,
                cancel_reason=deadline,
                track_end_reason=None,
            )
            is None
        )

    @pytest.mark.parametrize("other", [RuntimeError("write failed"), Cancelled("cancelled")])
    def test_a_deadline_does_not_hide_an_unrelated_failure(self, other: BaseException) -> None:
        failure = run_failure(
            producer_error=None,
            media_failure=other,
            cancel_reason=Cancelled("cancelled"),
            track_end_reason=None,
        )
        assert isinstance(failure, RuntimeError)
        assert failure.__cause__ is other

    def test_the_media_pipeline_wins_over_everything_else(self) -> None:
        broken = RuntimeError("no frames")
        failure = run_failure(
            producer_error=broken, media_failure=None, cancel_reason=TrackEnded("x"), track_end_reason=None
        )

        assert isinstance(failure, RuntimeError)
        assert str(failure) == "media pipeline failed: RuntimeError: no frames"
        assert failure.__cause__ is broken

    def test_an_unrelated_publish_failure_is_still_wrapped(self) -> None:
        # Identity, not class: another failure during a track-end cancellation is its own story.
        ended = TrackEnded("the audio track ended mid-run: ...")
        other = RuntimeError("something else")
        failure = run_failure(producer_error=None, media_failure=other, cancel_reason=ended, track_end_reason=ended)

        assert isinstance(failure, RuntimeError)
        assert str(failure) == "publishing failed: RuntimeError: something else"


class TestTrackEndHandler:
    def test_logs_once_records_the_track_and_stops_the_run(self) -> None:
        stats = Stats()
        logged: list[str] = []
        stopped: list[BaseException] = []
        handler = make_track_end_handler("stop", stats, lambda event, fields: logged.append(event), stopped.append)
        error = changed_error()

        handler("audio", error)
        handler("audio", error)

        assert logged == ["audio track ended"]
        assert stats.ended_tracks == {"audio": f"RelayError: {error}"}
        assert [type(reason).__name__ for reason in stopped] == ["TrackEnded"]
        assert summarize(stats)["endedTracks"] == "audio"

    def test_continue_keeps_the_run_going(self) -> None:
        stats = Stats()
        stopped: list[BaseException] = []
        handler = make_track_end_handler("continue", stats, lambda event, fields: None, stopped.append)

        handler("audio", RelayError("gone", code="CLOSED"))

        assert stopped == []
        assert stats.ended_tracks == {"audio": "RelayError: gone"}


class TestServeCatalog:
    """The example's wrapper around the SDK's catalog service: the track-end policy and the counters."""

    async def test_a_retired_catalog_track_is_reported_through_the_track_end_policy(self) -> None:
        stats = Stats()
        ended: Ended = []
        error = closed_error()
        track = FakeTrackWriter()
        track.fail("write", error)

        await serve_catalog(CatalogService(track, b"{}"), stats, lambda name, failure: ended.append((name, failure)))

        assert ended == [("catalog", error)]
        assert (stats.catalog_sends, stats.catalog_failures) == (0, 0)

    async def test_counters_are_folded_in_when_the_service_ends(self) -> None:
        stats = Stats()
        ended: Ended = []
        track = FakeTrackWriter()
        service = CatalogService(track, b"{}", retry_delay=0.01)
        track.fail("write", changed_error())
        serving = asyncio.create_task(
            serve_catalog(service, stats, lambda name, failure: ended.append((name, failure)))
        )
        await wait_for(lambda: track.writes == [b"{}"])

        track.end()
        await asyncio.wait_for(serving, 1)

        assert (stats.catalog_sends, stats.catalog_failures) == (1, 1)
        assert ended == []
