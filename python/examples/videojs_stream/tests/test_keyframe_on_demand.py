"""#165: a new relay subscription gets a keyframe forced for it rather than waiting up to a whole GOP."""

import asyncio
import threading

import pytest
from core_helpers.waiting import wait_for
from relay_helpers.fake_track_writer import FakeTrackWriter
from videojs_stream import stream


class TestRequestKeyframes:
    async def test_every_subscription_after_the_initial_snapshot_requests_one_including_a_replacement(self) -> None:
        track = FakeTrackWriter(demand=False)
        requested = threading.Event()
        watcher = asyncio.ensure_future(stream.request_keyframes(track, requested))
        await wait_for(lambda: len(track._watchers) == 1)
        await asyncio.sleep(0)
        assert not requested.is_set()

        track.bind(True)
        await wait_for(requested.is_set)
        requested.clear()

        # A repeated True is a replacement subscription: it starts at a keyframe of its own.
        track.bind(True)
        await wait_for(requested.is_set)
        requested.clear()

        track.bind(False)
        track.end()
        await wait_for(watcher.done)
        assert not requested.is_set()
        watcher.result()

    async def test_demand_already_present_at_start_needs_no_extra_keyframe(self) -> None:
        track = FakeTrackWriter(demand=True)
        requested = threading.Event()
        watcher = asyncio.ensure_future(stream.request_keyframes(track, requested))
        await wait_for(lambda: len(track._watchers) == 1)
        track.end()
        await wait_for(watcher.done)
        assert not requested.is_set()


def test_a_forced_keyframe_is_an_idr_and_shifts_the_gop_cadence() -> None:
    encoder = stream.VideoEncoder(320, 180, 30, 500_000, 2)
    frames = stream.pattern_frames(320, 180, 30)
    samples = []

    for i in range(100):
        samples.extend(encoder.encode(next(frames)[0], keyframe=i == 25))

    samples.extend(encoder.flush())
    assert [i for i, sample in enumerate(samples) if sample.key] == [0, 25, 85]
    forced = samples[25]
    assert forced.payload[4] & 0x1F == 5  # the first AVCC NAL unit is an IDR slice
    assert any(p.type == stream.LOC_VIDEO_CONFIG for p in forced.properties)


async def test_the_producer_forces_its_next_video_frame_when_requested(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RELAY_TOKEN", "test")
    config = stream.parse_args(["--gop", "10", "--audio", "off", "--width", "320", "--height", "180"])
    stats = stream.Stats()
    queue = stream.LiveQueue("video", stats, max_samples=1000, max_bytes=64 * 1024 * 1024, max_lag=10)
    producer = stream.Producer(config, asyncio.get_running_loop(), {"video": queue})
    producer.start()

    try:
        first = await asyncio.wait_for(queue.get(), 5)
        assert first is not None and first.key
        producer.keyframe_requested.set()

        async def next_keyframe() -> int:
            deltas = 0

            while True:
                sample = await queue.get()
                assert sample is not None

                if sample.key:
                    return deltas

                deltas += 1

        # Well inside the 10 s GOP: at most the frames encoded before the producer saw the request.
        assert await asyncio.wait_for(next_keyframe(), 2) <= 3
        assert not producer.keyframe_requested.is_set()
    finally:
        producer.stop()
        await producer.join()
