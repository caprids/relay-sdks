"""Real encoder bitrate and deterministic deadline/error ordering for the video example."""

import asyncio
import json
import threading
from types import SimpleNamespace
from typing import Any, cast

import pytest
from videojs_stream import stream

from moq_media import LOC_TIMESTAMP
from mux_relay import CancellationToken, Cancelled


@pytest.mark.parametrize("width,height,bitrate", [(640, 360, 1_500_000), (640, 360, 1_500_600), (1280, 720, 4_000_000)])
def test_cbr_payload_reaches_target_on_the_actual_test_pattern(width: int, height: int, bitrate: int) -> None:
    import av

    # libavcodec truncates the target to kbit/s. VBV and HRD must use that same
    # value or x264 warns and silently downgrades constant-rate signalling.
    with av.logging.Capture() as encoder_logs:
        encoder = stream.VideoEncoder(width, height, 30, bitrate, 2, "cbr")
    assert not any("CBR HRD requires constant bitrate" in message for _, _, message in encoder_logs)
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
    # The encoder attaches the LOC properties the SDK's frame writer sends: a timestamp on every
    # frame, and the decoder configuration on keyframes only.
    assert all(any(p.type == LOC_TIMESTAMP for p in sample.properties) for sample in samples)
    assert [
        i for i, sample in enumerate(samples) if any(p.type == stream.LOC_VIDEO_CONFIG for p in sample.properties)
    ] == [
        0,
        60,
        120,
    ]


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
    connection: CancellationToken | None = None
    events: list[tuple[str, str]] = []
    problem = Cancelled("unrelated cancellation") if failure == "unrelated_cancel" else RuntimeError("real failure")

    class Producer:
        def __init__(self, *_: Any) -> None:
            # The catalog builder derives the declared codec from a real avcC header.
            self.video = SimpleNamespace(
                codec="avc1.42C014", avcc=bytes.fromhex("0142c014ffe1000a6742c014da05067e784001000468ce3c80")
            )
            self.audio = None
            self.error: BaseException | None = None
            self.keyframe_requested = threading.Event()

        def start(self, *_: Any) -> None:
            pass

        def stop(self) -> None:
            pass

        async def join(self) -> None:
            # The real producer join also gives its last handoff / writer time to settle.
            await asyncio.sleep(0.01)
            if failure == "producer":
                self.error = problem

    class Track:
        has_demand = True

        def __init__(self, name: str) -> None:
            self.name = name

        async def write(self, payload: bytes) -> None:
            events.append((self.name, payload.decode()))

        def close(self) -> None:
            events.append((self.name, "close"))

    class Publisher:
        closed = loop.create_future()

        def register_track(self, name: str, *_: Any, **__: Any) -> Track:
            return Track(name)

        async def close(self) -> None:
            pass

    async def connect(*_: Any, cancellation: CancellationToken, **__: Any) -> Publisher:
        nonlocal connection
        connection = cancellation
        return Publisher()

    async def catalog(*_: Any) -> None:
        await asyncio.Future()

    async def media(*args: Any, **_: Any) -> None:
        lifetime: CancellationToken = args[-1]
        if not lifetime.cancelled:
            ready = asyncio.Event()
            lifetime.add_callback(lambda _: ready.set())
            await ready.wait()
        if delayed:
            await asyncio.sleep(0)
        assert lifetime.reason is not None
        raise problem if failure in ("media", "unrelated_cancel") else lifetime.reason

    monkeypatch.setattr(stream, "Producer", Producer)
    monkeypatch.setattr(stream, "connect_relay_publisher", connect)
    monkeypatch.setattr(stream, "serve_catalog", catalog)
    monkeypatch.setattr(stream, "serve_timeline", catalog)
    monkeypatch.setattr(stream, "publish_live_media", media)
    monkeypatch.setattr(stream, "request_keyframes", catalog)
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

    # msf-01 §11.3: a deliberate stop keeps the connection open, ends the media tracks, then sends the
    # end-of-broadcast catalog before closing the catalog track. A failure never sends it.
    assert connection is not None and not connection.cancelled
    catalog_events = [event for event in events if event[0] == stream.CATALOG_TRACK]
    if failure is None:
        assert events.index((stream.VIDEO_TRACK, "close")) < events.index(catalog_events[0])
        ((_, document), closed) = catalog_events
        assert json.loads(document) | {"generatedAt": 0} == {
            "version": "draft-01",
            "generatedAt": 0,
            "isComplete": True,
            "tracks": [],
        }
        assert closed == (stream.CATALOG_TRACK, "close")
    else:
        assert catalog_events == [(stream.CATALOG_TRACK, "close")]


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

    async def publish(queue: stream.LiveQueue, *_: Any, **__: Any) -> None:
        if queue.kind == "video":
            lifetime.throw_if_cancelled()
        raise problem

    monkeypatch.setattr(stream, "publish_media", publish)
    writers = {kind: cast(Any, None) for kind in queues}
    idle_catalog = cast(Any, SimpleNamespace(sends=0, failures=0))
    task = asyncio.create_task(
        stream.publish_live_media(queues, writers, idle_catalog, stats, lambda *_: None, lambda *_: None, lifetime)
    )
    if cancel_after_turns is not None:
        # Both writers have failed by turn two; later turns enter the parent's drain.
        for _ in range(cancel_after_turns):
            await asyncio.sleep(0)
        task.cancel()
    with pytest.raises(RuntimeError) as caught:
        await task
    assert caught.value is problem
