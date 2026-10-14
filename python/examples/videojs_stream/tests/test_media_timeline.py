"""The example's media timeline: one entry per video group that went out, and what --verify checks."""

from relay_helpers.fake_track_writer import FakeTrackWriter, timeout_error
from videojs_stream.stream import (
    TIMELINE_MAX_ENTRIES,
    EncodedSample,
    LiveQueue,
    Stats,
    Verified,
    publish_media,
    timeline_mismatches,
    timeline_problems,
)

from moq_media import MediaTimelineEntry, loc_properties
from mux_relay import CancellationToken
from mux_relay.media import FrameWriter


def gop(start: int, frames: int) -> list[EncodedSample]:
    """A keyframe at ``start`` frames and its deltas, 30 fps."""
    return [
        EncodedSample("video", i * 33_333, b"\x00", i == start, properties=loc_properties(i * 33_333))
        for i in range(start, start + frames)
    ]


async def groups_indexed(samples: list[EncodedSample], track: FakeTrackWriter) -> list[tuple[int, int]]:
    stats = Stats()
    queue = LiveQueue("video", stats, max_samples=1_000, max_bytes=2**30, max_lag=60)
    for sample in samples:
        queue.put_nowait(sample)
    queue.put_nowait(None)
    indexed: list[tuple[int, int]] = []
    await publish_media(
        queue,
        FrameWriter(track),
        stats,
        lambda *_: None,
        lambda *_: None,
        CancellationToken(),
        on_group=lambda sample, group: indexed.append((sample.timestamp_us, group)),
    )
    return indexed


async def test_every_keyframe_that_opened_a_group_is_indexed_once() -> None:
    indexed = await groups_indexed([*gop(0, 3), *gop(3, 3)], FakeTrackWriter())
    assert indexed == [(0, 0), (99_999, 1)]


async def test_keyframes_that_opened_nothing_are_not_indexed() -> None:
    track = FakeTrackWriter()
    track.fail("open", timeout_error(), on=0)  # the first keyframe is interrupted
    assert await groups_indexed([*gop(0, 2), *gop(2, 2)], track) == [(66_666, 0)]

    unsubscribed = FakeTrackWriter(demand=False)  # skipped: the relay has no subscription
    assert await groups_indexed(gop(0, 2), unsubscribed) == []


def entry(group: int, media_time_ms: int, *, object_id: int = 0) -> MediaTimelineEntry:
    return MediaTimelineEntry(media_time_ms=media_time_ms, group=group, object=object_id, wallclock_ms=1)


def test_a_document_must_be_bounded_and_in_order() -> None:
    assert timeline_problems([entry(0, 0), entry(1, 2002)]) == []
    assert timeline_problems([entry(1, 2002), entry(0, 0)]) == ["entry for group 0 out of order after group 1"]
    too_many = [entry(group, group) for group in range(TIMELINE_MAX_ENTRIES + 1)]
    assert timeline_problems(too_many) == [f"513 entries, more than the {TIMELINE_MAX_ENTRIES} retained"]


def test_entries_must_match_the_keyframes_received() -> None:
    verified = Verified(keyframe_times_ms={0: 0, 1: 2002})
    verified.timeline = (entry(0, 0), entry(1, 2002), entry(2, 4004))  # group 2 may still be in flight
    assert timeline_mismatches(verified) == []

    verified.timeline = (entry(1, 2003),)
    assert timeline_mismatches(verified) == ["timeline: group 1 says 2003 ms at object 0, its keyframe is 2002 ms"]

    verified = Verified(keyframe_times_ms={0: 0, 2: 4004})
    verified.timeline = (entry(1, 2002),)
    assert timeline_mismatches(verified) == ["timeline: group 1 has no video keyframe received"]
