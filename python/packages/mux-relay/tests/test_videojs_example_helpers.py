"""The Video.js streaming example's catalog serving and publish failure handling."""

import asyncio
from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import Any, cast

import pytest
from videojs_stream.stream import (
    EncodedSample,
    Stats,
    TrackEnded,
    discard_queue,
    failure_ends_track,
    make_track_end_handler,
    publish_media,
    run_failure,
    serve_catalog,
    summarize,
    track_has_ended,
    write_ended_track,
)

from mux_relay import CancellationToken, Cancelled, RelayClosedError, RelayError, RelayTimeoutError


class TestFailureEndsTrack:
    """The example's read of the SDK contract: which publish failures retire the track."""

    def test_a_timeout_waiting_for_demand_is_retryable(self) -> None:
        timeout = RelayTimeoutError("Publish write timed out waiting for relay demand or transport")

        assert failure_ends_track(timeout, operation="write", bound=False) is False

    def test_a_timeout_on_a_bound_write_is_retryable(self) -> None:
        # A bound write resets only its stream, just like a multi-object group.
        timeout = RelayTimeoutError("Publish write timed out waiting for relay demand or transport")

        assert failure_ends_track(timeout, operation="write", bound=True) is False

    def test_a_closed_track_is_always_terminal(self) -> None:
        closed = RelayClosedError("Published track is closed")

        assert failure_ends_track(closed, operation="write", bound=False) is True
        assert failure_ends_track(closed, operation="group", bound=True) is True

    def test_a_bare_relay_error_on_a_bound_write_is_retryable(self) -> None:
        # A changed relay subscription cancels the operation, leaving the track open.
        rebind = RelayError("Relay subscription changed during a write; the object may be incomplete")

        assert failure_ends_track(rebind, operation="write", bound=True) is False

    def test_a_bare_relay_error_on_an_unbound_write_is_retryable(self) -> None:
        vanished = RelayError("Relay subscription ended before the write started")

        assert failure_ends_track(vanished, operation="write", bound=False) is False

    def test_a_group_that_fails_part_way_keeps_its_track(self) -> None:
        # The SDK resets the group's stream and leaves the track open for the next keyframe.
        interrupted = RelayError("Relay subscription changed during the group; it may be incomplete")

        assert failure_ends_track(interrupted, operation="group", bound=True) is False


class FakeGroup:
    """Enough of RelayGroupWriter for the example's publish loop, with a scripted failing object."""

    def __init__(self, group_id: int, *, fail_on: int | None = None, error: RelayError | None = None) -> None:
        self.group_id = group_id
        self.objects = 0
        self._fail_on = fail_on
        self._error = error

    async def write(self, payload: bytes, *, properties: object = None) -> None:
        if self._fail_on is not None and self.objects == self._fail_on:
            assert self._error is not None
            self._fail_on = None
            raise self._error

        self.objects += 1

    async def close(self) -> None:
        return None


class FakeTrack:
    """A track writer with injected operation failures and optional terminal closure.

    `demand_changes()` mirrors RelayTrackWriter: current demand first, and no values after closure.
    A relay re-bind is now transient; terminal injections model an explicitly closed track.
    """

    def __init__(
        self,
        *,
        bound: bool = True,
        fail_on: int | None = None,
        error: RelayError | None = None,
        retires: bool = True,
        group_fail_on: int | None = None,
        group_error: RelayError | None = None,
    ) -> None:
        self.has_demand = bound
        self.ended = False
        self.writes = 0
        self.groups = 0
        self._fail_on = fail_on
        self._error = error
        self._retires = retires
        self._group_fail_on = group_fail_on
        self._group_error = group_error

    def _maybe_fail(self) -> None:
        if self._fail_on is not None and self.writes + self.groups == self._fail_on:
            assert self._error is not None
            self._fail_on = None
            self.has_demand = False
            self.ended = self._retires
            raise self._error

    async def write(self, payload: bytes, *, properties: object = None) -> None:
        self._maybe_fail()
        self.writes += 1

    async def open_group(self) -> FakeGroup:
        self._maybe_fail()
        group = FakeGroup(self.groups, fail_on=self._group_fail_on, error=self._group_error)
        self.groups += 1
        return group

    async def _demand_changes(self) -> AsyncIterator[bool]:
        if self.ended:
            return

        yield self.has_demand

    def demand_changes(self) -> AsyncIterator[bool]:
        return self._demand_changes()


class TestTrackHasEnded:
    async def test_reads_the_end_of_track_out_of_the_demand_changes_iterator(self) -> None:
        assert await track_has_ended(cast(Any, FakeTrack(bound=True))) is False
        assert await track_has_ended(cast(Any, FakeTrack(bound=False))) is False

        retired = FakeTrack()
        retired.ended = True

        assert await track_has_ended(cast(Any, retired)) is True

    async def test_prefers_the_closed_property_that_31_adds(self) -> None:
        # #31 gives RelayTrackWriter a `closed` property; the example uses it as soon as it exists
        # and falls back to `demand_changes()` against today's SDK.
        alive = SimpleNamespace(closed=False, demand_changes=lambda: pytest.fail("demand_changes() was consulted"))
        gone = SimpleNamespace(closed=True, demand_changes=lambda: pytest.fail("demand_changes() was consulted"))

        assert await track_has_ended(cast(Any, alive)) is False
        assert await track_has_ended(cast(Any, gone)) is True


class TestWriteEndedTrack:
    async def test_confirms_a_terminal_classification_against_the_writer(self) -> None:
        rebind = RelayError("Relay subscription changed during a write; the object may be incomplete")
        retired = FakeTrack()
        retired.ended = True

        assert await write_ended_track(cast(Any, retired), rebind, bound=True) is True
        assert await write_ended_track(cast(Any, FakeTrack()), rebind, bound=True) is False
        assert await write_ended_track(cast(Any, retired), rebind, bound=False) is True

    async def test_a_closed_track_needs_no_confirmation(self) -> None:
        # A connection that went away leaves the writer un-retired while every write still fails.
        closed = RelayClosedError("Published track is closed")

        assert await write_ended_track(cast(Any, FakeTrack()), closed, bound=True) is True


def audio_samples(count: int) -> list[EncodedSample]:
    return [EncodedSample(kind="audio", timestamp_us=i * 20_000, payload=b"\xfc\xff", key=True) for i in range(count)]


def video_samples(count: int) -> list[EncodedSample]:
    """One keyframe then delta frames, the shape a GOP arrives in."""
    return [EncodedSample(kind="video", timestamp_us=i * 33_333, payload=b"\x00", key=i == 0) for i in range(count)]


def queue_of(samples: list[EncodedSample]) -> asyncio.Queue[EncodedSample | None]:
    queue: asyncio.Queue[EncodedSample | None] = asyncio.Queue()

    for sample in samples:
        queue.put_nowait(sample)

    queue.put_nowait(None)
    return queue


async def drive(
    samples: list[EncodedSample], *, video: object, audio: object
) -> tuple[Stats, list[tuple[str, RelayError]]]:
    """Run the example's publish loop over a scripted sample list and report what it counted."""
    stats = Stats()
    ended: list[tuple[str, RelayError]] = []
    encoder = cast(Any, SimpleNamespace(avcc=b"\x01\x42"))
    await publish_media(
        queue_of(samples),
        cast(Any, video),
        cast(Any, audio),
        encoder,
        stats,
        cast(Any, lambda event, fields: None),
        lambda track, error: ended.append((track, error)),
        CancellationToken(),
    )
    return stats, ended


class TestPublishMediaTrackEnd:
    async def test_a_bound_audio_write_failure_ends_the_track_once_and_counts_the_rest(self) -> None:
        # #32: a 30-minute run reported skippedAudio=0 while ~74,700 frames were never sent.
        rebind = RelayError("Relay subscription changed during a write; the object may be incomplete")
        audio = FakeTrack(fail_on=3, error=rebind)
        stats, ended = await drive(audio_samples(10), video=FakeTrack(bound=False), audio=audio)

        assert stats.audio_objects == 3
        assert stats.dropped_audio == 7
        assert stats.skipped_audio == 0
        assert [(track, str(error)) for track, error in ended] == [("audio", str(rebind))]
        assert audio.groups == 3
        assert summarize(stats)["droppedAudio"] == 7

    async def test_frames_with_no_subscription_stay_skipped_not_dropped(self) -> None:
        stats, ended = await drive(audio_samples(4), video=FakeTrack(bound=False), audio=FakeTrack(bound=False))

        assert (stats.audio_objects, stats.skipped_audio, stats.dropped_audio) == (0, 4, 0)
        assert ended == []
        assert summarize(stats)["endedTracks"] is None

    async def test_a_vanished_subscription_only_skips_the_frame(self) -> None:
        # `has_demand` was True when the write started but demand disappeared before it bound, so the
        # writer is still open; the example must confirm that instead of ending the run.
        vanished = RelayError("Relay subscription ended before the write started")
        audio = FakeTrack(fail_on=2, error=vanished, retires=False)
        stats, ended = await drive(audio_samples(3), video=FakeTrack(bound=False), audio=audio)

        # The third frame then finds `has_demand` False, the way it does while nobody is subscribed.
        assert (stats.audio_objects, stats.skipped_audio, stats.dropped_audio) == (2, 1, 0)
        assert ended == []

    async def test_a_closed_video_track_is_reported_and_its_frames_counted_as_dropped(self) -> None:
        closed = RelayClosedError("Published track is closed")
        video = FakeTrack(fail_on=0, error=closed)
        keyframes = [EncodedSample(kind="video", timestamp_us=i * 33_333, payload=b"\x00", key=True) for i in range(3)]
        stats, ended = await drive(keyframes, video=video, audio=None)

        assert (stats.video_groups, stats.video_objects, stats.dropped_video, stats.skipped_video) == (0, 0, 3, 0)
        assert [track for track, _ in ended] == ["video"]

    async def test_an_interrupted_group_counts_its_own_sample_as_skipped(self) -> None:
        # The group is reset and the track survives, so the sample that failed is a skip like the
        # ones that follow it before the next keyframe -- and it is in a counter either way.
        interrupted = RelayError("Relay subscription changed during the group; it may be incomplete")
        video = FakeTrack(group_fail_on=0, group_error=interrupted)
        stats, ended = await drive(video_samples(3), video=video, audio=None)

        assert (stats.video_groups, stats.video_objects, stats.skipped_video, stats.dropped_video) == (1, 0, 3, 0)
        assert ended == []

    async def test_a_terminal_group_write_failure_counts_its_own_sample_as_dropped(self) -> None:
        closed = RelayClosedError("Published track is closed")
        video = FakeTrack(group_fail_on=0, group_error=closed)
        stats, ended = await drive(video_samples(3), video=video, audio=None)

        assert (stats.video_groups, stats.video_objects, stats.skipped_video, stats.dropped_video) == (1, 0, 0, 3)
        assert [track for track, _ in ended] == ["video"]

    async def test_every_sample_lands_in_exactly_one_counter(self) -> None:
        # What `summarize` promises: sent + skipped + dropped accounts for the whole timeline.
        interrupted = RelayError("Relay subscription changed during the group; it may be incomplete")
        samples = video_samples(6) + audio_samples(4)
        stats, _ = await drive(
            samples, video=FakeTrack(group_fail_on=2, group_error=interrupted), audio=FakeTrack(bound=False)
        )

        video_total = stats.video_objects + stats.skipped_video + stats.dropped_video
        audio_total = stats.audio_objects + stats.skipped_audio + stats.dropped_audio

        assert (video_total, audio_total) == (6, 4)


class CancellingGroup:
    """A group whose writes and close re-raise the lifetime's cancel reason, as `assert_open` does."""

    def __init__(self, lifetime: CancellationToken, track: "CancellingTrack") -> None:
        self.group_id = 0
        self._lifetime = lifetime
        self._track = track

    async def write(self, payload: bytes, *, properties: object = None) -> None:
        self._lifetime.throw_if_cancelled()

    async def close(self) -> None:
        self._track.closes += 1
        self._lifetime.throw_if_cancelled()


class CancellingTrack:
    """A video writer that fails everything in flight with the reason the lifetime was cancelled with."""

    def __init__(self, lifetime: CancellationToken) -> None:
        self.has_demand = True
        self.ended = False
        self.closes = 0
        self._lifetime = lifetime

    async def open_group(self) -> CancellingGroup:
        self._lifetime.throw_if_cancelled()
        return CancellingGroup(self._lifetime, self)


async def drive_until_stopped(
    samples: list[EncodedSample], lifetime: CancellationToken, video: CancellingTrack, audio: FakeTrack
) -> tuple[Stats, BaseException | None, BaseException | None]:
    """Publish under the `stop` policy and report what the media task and the handler each produced."""
    stats = Stats()
    reasons: list[BaseException] = []

    def stop(reason: BaseException) -> None:
        reasons.append(reason)
        lifetime.cancel(reason)

    on_track_end = make_track_end_handler("stop", stats, cast(Any, lambda event, fields: None), stop)
    media_failure: BaseException | None = None
    queue = queue_of(samples)

    try:
        await publish_media(
            queue,
            cast(Any, video),
            cast(Any, audio),
            cast(Any, SimpleNamespace(avcc=b"\x01\x42")),
            stats,
            cast(Any, lambda event, fields: None),
            on_track_end,
            lifetime,
        )
    except BaseException as error:
        media_failure = error

    # What `run`'s shutdown does once the publish loop is over.
    discard_queue(queue, stats)
    return stats, media_failure, reasons[0] if reasons else None


class TestStopPolicyReporting:
    """Cancelling for a retired track must not come back as an anonymous publishing failure."""

    async def test_a_track_end_under_the_stop_policy_is_reported_as_itself(self) -> None:
        # The SDK re-raises the application's cancel reason out of the video write still in flight,
        # and it is not a RelayError, so it escapes the publish loop.
        lifetime = CancellationToken()
        rebind = RelayError("Relay subscription changed during a write; the object may be incomplete")
        samples = [
            EncodedSample(kind="video", timestamp_us=0, payload=b"\x00", key=True),
            EncodedSample(kind="audio", timestamp_us=0, payload=b"\xfc\xff", key=True),
            EncodedSample(kind="video", timestamp_us=33_333, payload=b"\x00", key=False),
        ]
        video = CancellingTrack(lifetime)
        stats, media_failure, track_end_reason = await drive_until_stopped(
            samples, lifetime, video, FakeTrack(fail_on=0, error=rebind)
        )

        assert media_failure is lifetime.reason
        assert isinstance(track_end_reason, TrackEnded)
        assert stats.ended_tracks == {"audio": f"RelayError: {rebind}"}

        failure = run_failure(
            producer_error=None,
            media_failure=media_failure,
            cancel_reason=lifetime.reason,
            track_end_reason=track_end_reason,
        )

        # Not `RuntimeError: publishing failed: TrackEnded: ...`, which says nothing about the track.
        assert failure is track_end_reason
        assert "the audio track ended mid-run" in str(failure)

    async def test_every_sample_is_accounted_for_when_the_run_stops(self) -> None:
        # Stopping abandons the sample in flight and everything still queued behind it. Both are
        # exactly what the summary exists to report, so neither may vanish from the counters.
        lifetime = CancellationToken()
        rebind = RelayError("Relay subscription changed during a write; the object may be incomplete")
        samples = [
            EncodedSample(kind="video", timestamp_us=0, payload=b"\x00", key=True),
            EncodedSample(kind="audio", timestamp_us=0, payload=b"\xfc\xff", key=True),
            # Never reached: the write of the first of these raises the cancellation, and the rest
            # are still sitting in the queue when the run tears down.
            *[EncodedSample(kind="video", timestamp_us=i * 33_333, payload=b"\x00", key=False) for i in range(1, 5)],
            *[EncodedSample(kind="audio", timestamp_us=i * 20_000, payload=b"\xfc\xff", key=True) for i in range(1, 3)],
        ]
        stats, _, _ = await drive_until_stopped(
            samples, lifetime, CancellingTrack(lifetime), FakeTrack(fail_on=0, error=rebind)
        )

        video_total = stats.video_objects + stats.skipped_video + stats.dropped_video + stats.discarded_video
        audio_total = stats.audio_objects + stats.skipped_audio + stats.dropped_audio + stats.discarded_audio

        assert (video_total, audio_total) == (5, 3)
        assert stats.discarded_video == 4  # the one whose write was cancelled, plus three queued
        assert stats.discarded_audio == 2
        assert summarize(stats)["discardedVideo"] == 4

    async def test_abandoning_a_group_does_not_re_raise_the_cancellation(self) -> None:
        # `end_group` runs in the publish loop's finally; raising the cancellation again there would
        # replace whatever was already on its way out.
        lifetime = CancellationToken()
        rebind = RelayError("Relay subscription changed during a write; the object may be incomplete")
        samples = [
            EncodedSample(kind="video", timestamp_us=0, payload=b"\x00", key=True),
            EncodedSample(kind="audio", timestamp_us=0, payload=b"\xfc\xff", key=True),
        ]
        video = CancellingTrack(lifetime)
        _, media_failure, track_end_reason = await drive_until_stopped(
            samples, lifetime, video, FakeTrack(fail_on=0, error=rebind)
        )

        assert video.closes == 1
        assert media_failure is None
        assert isinstance(track_end_reason, TrackEnded)


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
        handler = make_track_end_handler(
            "stop", stats, cast(Any, lambda event, fields: logged.append(event)), stopped.append
        )
        error = RelayError("Relay subscription changed during a write; the object may be incomplete")

        handler("audio", error)
        handler("audio", error)

        assert logged == ["audio track ended"]
        assert stats.ended_tracks == {"audio": f"RelayError: {error}"}
        assert [type(reason).__name__ for reason in stopped] == ["TrackEnded"]
        assert summarize(stats)["endedTracks"] == "audio"

    def test_continue_keeps_the_run_going(self) -> None:
        stats = Stats()
        stopped: list[BaseException] = []
        handler = make_track_end_handler("continue", stats, cast(Any, lambda event, fields: None), stopped.append)

        handler("audio", RelayError("gone"))

        assert stopped == []
        assert stats.ended_tracks == {"audio": "RelayError: gone"}


class TestServeCatalog:
    async def test_retries_a_transient_bound_failure_without_resubscription(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("videojs_stream.stream.CATALOG_RETRY_DELAY", 0.01)
        attempts: list[float] = []
        delivered: list[bytes] = []

        class FakeCatalog:
            has_demand = True
            closed = False

            async def demand_changes(self) -> AsyncIterator[bool]:
                yield True  # No second subscription event is needed for recovery.

            async def write(self, payload: bytes) -> None:
                attempts.append(asyncio.get_running_loop().time())
                if len(attempts) == 1:
                    raise RelayTimeoutError("bound write timed out")
                delivered.append(payload)

        stats = Stats()
        await asyncio.wait_for(
            serve_catalog(
                cast(Any, FakeCatalog()),
                b"current catalog",
                stats,
                lambda event, fields: None,
                lambda track, error: pytest.fail("transient write ended catalog"),
            ),
            1,
        )
        assert delivered == [b"current catalog"]
        assert len(attempts) == 2
        assert attempts[1] - attempts[0] >= 0.01
        assert (stats.catalog_sends, stats.catalog_failures) == (1, 1)

    @pytest.mark.parametrize("stop", ["cancel", "no_demand"])
    async def test_retry_pause_stops_on_cancellation_or_lost_demand(
        self, monkeypatch: pytest.MonkeyPatch, stop: str
    ) -> None:
        monkeypatch.setattr("videojs_stream.stream.CATALOG_RETRY_DELAY", 0.05)
        failed = asyncio.Event()

        class FakeCatalog:
            has_demand = True
            closed = False
            attempts = 0

            async def demand_changes(self) -> AsyncIterator[bool]:
                yield True

            async def write(self, payload: bytes) -> None:
                self.attempts += 1
                failed.set()
                raise RelayTimeoutError("bound write timed out")

        catalog = FakeCatalog()
        serving = asyncio.create_task(
            serve_catalog(
                cast(Any, catalog),
                b"{}",
                Stats(),
                lambda event, fields: None,
                lambda track, error: pytest.fail("transient write ended catalog"),
            )
        )
        await failed.wait()
        if stop == "cancel":
            serving.cancel()
            with pytest.raises(asyncio.CancelledError):
                await serving
        else:
            catalog.has_demand = False
            await asyncio.wait_for(serving, 1)
        assert catalog.attempts == 1

    async def test_a_retired_catalog_track_stops_the_resend_loop(self) -> None:
        class FakeCatalog:
            """A catalog writer the relay subscribes to twice, whose write fails the terminal way."""

            def __init__(self) -> None:
                self.writes = 0

            async def _demand_changes(self) -> AsyncIterator[bool]:
                yield True
                yield True

            def demand_changes(self) -> AsyncIterator[bool]:
                return self._demand_changes()

            async def write(self, payload: bytes, *, properties: object = None) -> None:
                self.writes += 1
                raise RelayClosedError("Published track is closed")

        catalog = FakeCatalog()
        stats = Stats()
        ended: list[str] = []
        await serve_catalog(
            cast(Any, catalog),
            b"{}",
            stats,
            cast(Any, lambda event, fields: None),
            lambda track, error: ended.append(track),
        )

        assert (catalog.writes, stats.catalog_sends, stats.catalog_failures) == (1, 0, 0)
        assert ended == ["catalog"]
