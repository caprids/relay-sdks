"""Bounded, decodable recovery after a live publisher's transport stalls."""

import asyncio
import time
from types import SimpleNamespace
from typing import Any, cast

import pytest
from videojs_stream.stream import EncodedSample, LiveQueue, Stats, discard_queue, publish_live_media, publish_media

from mux_relay import CancellationToken, RelayError


def sample(kind: str, timestamp: float, *, key: bool = True, size: int = 4) -> EncodedSample:
    return EncodedSample(cast(Any, kind), round(timestamp * 1_000_000), bytes(size), key)


def queues(stats: Stats, *, capacity: int = 8) -> dict[str, LiveQueue]:
    result = {
        kind: LiveQueue(cast(Any, kind), stats, max_samples=capacity, max_bytes=64, max_lag=lag)
        for kind, lag in [("video", 2.0), ("audio", 0.25)]
    }
    for queue in result.values():
        queue.started = time.monotonic()
    return result


class Group:
    def __init__(self, track: "Track", group_id: int) -> None:
        self.track = track
        self.group_id = group_id
        self.timestamps: list[int] = []
        self.aborted = False
        self.finished = False

    async def write(self, payload: bytes, *, properties: Any) -> None:
        if self.group_id == 0 and self.track.gate is not None:
            self.track.started.set()
            await self.track.gate.wait()
            if self.track.timeout:
                raise RelayError("transport stalled", code="WRITE_TIMEOUT", phase="writing")
        self.timestamps.append(next(p.value for p in properties if p.type == 0x10))
        self.track.written.set()

    async def close(self) -> None:
        self.finished = True

    def abort(self) -> None:
        self.aborted = True


class Track:
    def __init__(self, *, gate: asyncio.Event | None = None, timeout: bool = False) -> None:
        self.gate = gate
        self.timeout = timeout
        self.has_demand = True
        self.closed = False
        self.started = asyncio.Event()
        self.written = asyncio.Event()
        self.groups: list[Group] = []

    async def open_group(self) -> Group:
        group = Group(self, len(self.groups))
        self.groups.append(group)
        return group


async def publish(queue: LiveQueue, track: Track, stats: Stats) -> None:
    await publish_media(
        queue,
        cast(Any, track),
        cast(Any, track),
        cast(Any, SimpleNamespace(avcc=b"config")),
        stats,
        lambda *_: None,
        lambda *_: pytest.fail("healthy track ended"),
        CancellationToken(),
    )


def test_overflow_bounds_bytes_and_frames_and_preserves_eof() -> None:
    stats = Stats()
    queue = LiveQueue("video", stats, max_samples=3, max_bytes=10, max_lag=2)
    for i in range(100):
        queue.put_nowait(sample("video", i, size=4))
        assert queue.qsize() <= 3
        assert queue.buffered_bytes <= 10
    queue.put_nowait(None)
    queue.put_nowait(None)
    remaining = []
    while not queue.empty():
        remaining.append(queue.get_nowait())
    assert remaining[-1] is None
    assert remaining[0] is not None and remaining[0].discontinuity
    assert stats.stale_video + len(remaining) - 1 == 100
    assert queue.buffered_bytes == 0


def test_oversized_video_drops_dependents_and_resumes_with_discontinuity() -> None:
    stats = Stats()
    queue = LiveQueue("video", stats, max_samples=3, max_bytes=10, max_lag=2)
    queue.put_nowait(sample("video", 0))
    queue.put_nowait(sample("video", 0.03, key=False, size=11))
    queue.put_nowait(sample("video", 0.06, key=False))
    received = queue.get_nowait()
    assert received is not None and received.discontinuity
    assert stats.stale_video == 2
    assert queue.buffered_bytes == 0


async def test_video_stall_does_not_block_audio_and_overflow_recovers_at_live_keyframe() -> None:
    stats = Stats()
    buffers = queues(stats, capacity=4)
    gate = asyncio.Event()
    video, audio = Track(gate=gate), Track()
    task = asyncio.create_task(
        publish_live_media(
            buffers,
            cast(Any, video),
            cast(Any, audio),
            cast(Any, SimpleNamespace(avcc=b"config")),
            stats,
            lambda *_: None,
            lambda *_: pytest.fail("healthy track ended"),
            CancellationToken(),
        )
    )
    try:
        buffers["video"].put_nowait(sample("video", 0))
        await asyncio.wait_for(video.started.wait(), 1)
        buffers["audio"].put_nowait(sample("audio", 0))
        await asyncio.wait_for(audio.written.wait(), 1)
        assert not gate.is_set()
        for queue in buffers.values():
            assert queue.started is not None
            queue.started -= 20
        for i in range(100):
            buffers["video"].put_nowait(sample("video", i / 30, key=i % 60 == 0))
        for i in range(4):
            buffers["video"].put_nowait(sample("video", 19.8 + i / 30, key=False))
        buffers["video"].put_nowait(sample("video", 20))
        buffers["video"].put_nowait(sample("video", 20.03, key=False))
        for queue in buffers.values():
            queue.put_nowait(None)
        gate.set()
        await asyncio.wait_for(task, 1)
        assert video.groups[0].aborted
        assert video.groups[1].timestamps == [20_000_000, 20_030_000]
        assert video.groups[1].finished
        assert stats.audio_objects == 1
        assert stats.video_objects + stats.stale_video == 107
    finally:
        gate.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_age_drop_resets_open_gop_and_skips_fresh_delta_until_keyframe() -> None:
    stats = Stats()
    queue = queues(stats)["video"]
    gate = asyncio.Event()
    video = Track(gate=gate)
    task = asyncio.create_task(publish(queue, video, stats))
    try:
        queue.put_nowait(sample("video", 0))
        await asyncio.wait_for(video.started.wait(), 1)
        assert queue.started is not None
        queue.started -= 20
        queue.put_nowait(sample("video", 1, key=False))
        queue.put_nowait(sample("video", 20, key=False))
        queue.put_nowait(sample("video", 20.03))
        queue.put_nowait(None)
        gate.set()
        await asyncio.wait_for(task, 1)
        assert video.groups[0].aborted
        assert video.groups[1].timestamps == [20_030_000]
        assert stats.stale_video == 2
        assert stats.max_lag_seconds >= 19
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_audio_transport_timeout_resets_only_frame_and_drops_old_backlog() -> None:
    stats = Stats()
    queue = queues(stats)["audio"]
    gate = asyncio.Event()
    audio = Track(gate=gate, timeout=True)
    task = asyncio.create_task(publish(queue, audio, stats))
    try:
        queue.put_nowait(sample("audio", 0))
        await asyncio.wait_for(audio.started.wait(), 1)
        assert queue.started is not None
        queue.started -= 20
        queue.put_nowait(sample("audio", 1))
        queue.put_nowait(sample("audio", 20))
        queue.put_nowait(None)
        assert queue.current_lag() >= 20
        gate.set()
        await asyncio.wait_for(task, 1)
        assert audio.groups[0].aborted
        assert audio.groups[1].timestamps == [20_000_000]
        assert (stats.audio_objects, stats.skipped_audio, stats.stale_audio) == (1, 1, 1)
        assert stats.ended_tracks == {}
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_cancelling_independent_writers_accounts_for_inflight_and_queued_samples() -> None:
    stats = Stats()
    buffers = queues(stats)
    gate = asyncio.Event()
    video, audio = Track(gate=gate), Track(gate=gate)
    task = asyncio.create_task(
        publish_live_media(
            buffers,
            cast(Any, video),
            cast(Any, audio),
            cast(Any, SimpleNamespace(avcc=b"config")),
            stats,
            lambda *_: None,
            lambda *_: None,
            CancellationToken(),
        )
    )
    for kind, queue in buffers.items():
        queue.put_nowait(sample(kind, 0))
        queue.put_nowait(sample(kind, 0.03))
    await asyncio.wait_for(asyncio.gather(video.started.wait(), audio.started.wait()), 1)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    for queue in buffers.values():
        discard_queue(queue, stats)
    assert (stats.discarded_video, stats.discarded_audio) == (2, 2)
    assert audio.groups[0].aborted


async def test_stopping_producer_joins_handoff_and_flushes_both_eofs(monkeypatch: pytest.MonkeyPatch) -> None:
    from videojs_stream.stream import Producer

    stats = Stats()
    buffers = queues(stats, capacity=1)
    config = SimpleNamespace(
        width=64, height=64, fps=30, bitrate=100_000, rate_control="abr", gop_seconds=1.0, audio="tone", input=None
    )
    producer = Producer(cast(Any, config), asyncio.get_running_loop(), buffers)
    stopped = asyncio.Event()
    original_put = buffers["audio"].put_nowait

    def stop_during_handoff(item: EncodedSample | None) -> None:
        original_put(item)
        if item is not None:
            # Stop while the worker is still waiting for this event-loop handoff to finish.
            producer.stop()
            stopped.set()

    monkeypatch.setattr(buffers["audio"], "put_nowait", stop_during_handoff)
    producer.start()
    try:
        await asyncio.wait_for(stopped.wait(), 2)
        await asyncio.wait_for(producer.join(), 2)
        assert producer.error is None
        for queue in buffers.values():
            frames = []
            while not queue.empty():
                frames.append(queue.get_nowait())
            assert frames[-1] is None
            assert frames.count(None) == 1
            assert len(frames) <= 2  # one buffered frame plus the reserved EOF slot
    finally:
        producer.stop()
        await producer.join()
