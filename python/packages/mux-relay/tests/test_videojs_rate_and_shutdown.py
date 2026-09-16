"""Real encoder bitrate and deterministic deadline/error ordering for the video example."""

import asyncio
from types import SimpleNamespace
from typing import Any, cast

import pytest
from videojs_stream import stream

from mux_relay import CancellationToken, Cancelled


@pytest.mark.parametrize("width,height,bitrate", [(640, 360, 1_500_000), (1280, 720, 4_000_000)])
def test_cbr_payload_reaches_target_on_the_actual_test_pattern(width: int, height: int, bitrate: int) -> None:
    encoder = stream.VideoEncoder(width, height, 30, bitrate, 2, "cbr")
    frames = stream.pattern_frames(width, height, 30)
    samples = []
    for _ in range(180):
        samples.extend(encoder.encode(next(frames)[0]))
    samples.extend(encoder.flush())
    # Measure AVCC bytes actually handed to the publisher, including retained filler NALs.
    actual_bps = sum(len(sample.payload) for sample in samples) * 8 / 6
    assert 0.90 * bitrate <= actual_bps <= 1.10 * bitrate
    assert len(samples) == 180
    assert [i for i, sample in enumerate(samples) if sample.key] == [0, 60, 120]


def test_rate_control_cli_is_explicit_and_defaults_to_abr(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RELAY_TOKEN", "test")
    assert stream.parse_args([]).rate_control == "abr"
    assert stream.parse_args(["--rate-control", "cbr"]).rate_control == "cbr"


def test_rates_use_run_elapsed_time_and_do_not_change_after_finishing() -> None:
    stats = stream.Stats(
        started=10,
        finished=12,
        encoded_video_bytes=500_000,
        encoded_audio_bytes=25_000,
        published_video_bytes=250_000,
        published_audio_bytes=12_500,
    )
    result = stream.summarize(stats)
    assert result["encodedVideoKbps"] == 2000
    assert result["encodedAudioKbps"] == 100
    assert result["publishedVideoKbps"] == 1000
    assert result["publishedAudioKbps"] == 50
    assert stream.summarize(stream.Stats())["publishedVideoKbps"] == 0


@pytest.mark.parametrize("delayed", [False, True])
@pytest.mark.parametrize("failure", [None, "media", "producer", "unrelated_cancel"])
async def test_duration_drains_before_classifying_failures(
    monkeypatch: pytest.MonkeyPatch, delayed: bool, failure: str | None
) -> None:
    loop = asyncio.get_running_loop()
    lifetime: CancellationToken | None = None
    ready = asyncio.Event()
    problem = Cancelled("unrelated cancellation") if failure == "unrelated_cancel" else RuntimeError("real failure")

    class Producer:
        def __init__(self, *_: Any) -> None:
            self.video = SimpleNamespace(codec="avc1.42C014", avcc=b"config")
            self.audio = None
            self.error: BaseException | None = None

        def start(self) -> None:
            pass

        def stop(self) -> None:
            pass

        async def join(self) -> None:
            # The real producer join also gives its last handoff / writer time to settle.
            await asyncio.sleep(0.01)
            if failure == "producer":
                self.error = problem

    class Track:
        def close(self) -> None:
            pass

    class Publisher:
        closed = loop.create_future()

        def publish(self, *_: Any, **__: Any) -> Track:
            return Track()

        async def close(self) -> None:
            pass

    async def connect(*_: Any, cancellation: CancellationToken, **__: Any) -> Publisher:
        nonlocal lifetime
        lifetime = cancellation
        lifetime.add_callback(lambda _: ready.set())
        return Publisher()

    async def catalog(*_: Any) -> None:
        await asyncio.Future()

    async def media(*_: Any) -> None:
        await ready.wait()
        if delayed:
            await asyncio.sleep(0)
        assert lifetime is not None and lifetime.reason is not None
        raise problem if failure in ("media", "unrelated_cancel") else lifetime.reason

    monkeypatch.setattr(stream, "Producer", Producer)
    monkeypatch.setattr(stream, "connect_relay_publisher", connect)
    monkeypatch.setattr(stream, "serve_catalog", catalog)
    monkeypatch.setattr(stream, "publish_live_media", media)
    monkeypatch.setenv("RELAY_TOKEN", "test")
    config = stream.parse_args(["--duration", "0.001", "--audio", "off"])
    stats = stream.Stats()
    if failure is None:
        await stream.run(config, stats, lambda *_: None)
    else:
        with pytest.raises(RuntimeError) as caught:
            await stream.run(config, stats, lambda *_: None)
        assert caught.value.__cause__ is problem
    assert stats.finished is not None


@pytest.mark.parametrize("cancel_after_turns", [None, 2, 3, 4, 5, 6, 7, 8])
async def test_deadline_on_one_track_does_not_hide_other_track_failure(
    monkeypatch: pytest.MonkeyPatch, cancel_after_turns: int | None
) -> None:
    lifetime = CancellationToken()
    lifetime.cancel()
    problem = RuntimeError("audio encoder failed")
    stats = stream.Stats()
    queues = {
        kind: stream.LiveQueue(cast(Any, kind), stats, max_samples=1, max_bytes=10, max_lag=1)
        for kind in ("video", "audio")
    }

    async def publish(queue: stream.LiveQueue, *_: Any) -> None:
        if queue.kind == "video":
            lifetime.throw_if_cancelled()
        raise problem

    monkeypatch.setattr(stream, "publish_media", publish)
    task = asyncio.create_task(
        stream.publish_live_media(
            queues, cast(Any, None), None, cast(Any, None), stats, lambda *_: None, lambda *_: None, lifetime
        )
    )
    if cancel_after_turns is not None:
        # Both writers have failed by turn two; later turns enter the parent's drain.
        for _ in range(cancel_after_turns):
            await asyncio.sleep(0)
        task.cancel()
    with pytest.raises(RuntimeError) as caught:
        await task
    assert caught.value is problem
