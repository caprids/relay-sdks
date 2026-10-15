"""#302: the video track's default delivery timeout follows the encoder's actual keyframe interval."""

import asyncio
import itertools
from fractions import Fraction
from pathlib import Path

import av
import numpy as np
import pytest
from videojs_stream import stream


@pytest.mark.parametrize(
    ("fps", "gop_seconds", "expected_us"),
    [
        (30, 2.0, 2_000_000),
        (30, 1.0, 1_000_000),
        # The encoder rounds a GOP to whole frames, and never below one.
        (30, 0.001, 33_333),
        (30, 0.05, 66_667),
        (25, 0.5, 480_000),  # 12.5 frames rounds to 12, as libx264 is told
        (Fraction(3, 2), 2.0, 2_000_000),  # 3 frames of a 1.5 fps source
        (Fraction(30_000, 1001), 1.0, 1_001_000),  # 30 frames of NTSC's 29.97
    ],
)
def test_the_keyframe_interval_is_the_encoders_rounded_gop(
    fps: Fraction | int, gop_seconds: float, expected_us: int
) -> None:
    assert stream.keyframe_interval_us(fps, gop_seconds) == expected_us


def write_clip(path: Path, fps: Fraction, frames: int) -> None:
    with av.open(str(path), "w") as container:
        video = container.add_stream("libx264", rate=fps)
        video.width, video.height, video.pix_fmt = 64, 64, "yuv420p"
        for index in range(frames):
            frame = av.VideoFrame.from_ndarray(np.zeros((64, 64, 3), dtype=np.uint8), format="rgb24")
            frame.pts = index
            container.mux(video.encode(frame))
        container.mux(video.encode(None))


@pytest.mark.parametrize(
    ("fps", "expected_us"),
    [
        # 30 frames of a 15 fps file: a 2 s GOP, not the 4 s that 60 frames at --fps 30 would make.
        (Fraction(15), 2_000_000),
        # 3 frames of a 1.5 fps file: rounding the rate to 2 fps would count 4 frames, 2.667 s.
        (Fraction(3, 2), 2_000_000),
    ],
)
async def test_a_file_input_times_its_gop_and_timeout_by_the_files_exact_frame_rate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fps: Fraction, expected_us: int
) -> None:
    clip = tmp_path / "clip.mp4"
    write_clip(clip, fps, frames=6)
    monkeypatch.setenv("RELAY_TOKEN", "test")
    config = stream.parse_args(["--input", str(clip), "--audio", "off", "--fps", "30", "--gop", "2"])
    stats = stream.Stats()
    queue = stream.LiveQueue("video", stats, max_samples=10, max_bytes=1024 * 1024, max_lag=2)
    producer = stream.Producer(config, asyncio.get_running_loop(), {"video": queue})

    assert producer.video.fps == fps
    assert producer.video.keyframe_interval_us == expected_us


def test_the_registered_interval_is_the_spacing_of_the_encoders_keyframes_at_a_fractional_rate() -> None:
    """A 1.5 fps source must not be rounded to 2 fps: that would count 4 frames, 2.667 s, against a 2 s timeout."""
    rate = Fraction(3, 2)
    encoder = stream.VideoEncoder(64, 64, rate, 100_000, 2.0)
    samples = []
    for index in range(10):
        frame = av.VideoFrame.from_ndarray(np.zeros((64, 64, 3), dtype=np.uint8), format="rgb24")
        frame.pts = round(index * 1_000_000 / rate)
        samples.extend(encoder.encode(frame))
    samples.extend(encoder.flush())

    keyframes = [sample.timestamp_us for sample in samples if sample.key]
    assert len(keyframes) >= 3
    assert {b - a for a, b in itertools.pairwise(keyframes)} == {encoder.keyframe_interval_us}
