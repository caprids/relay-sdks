"""The shared broadcast timeline and the playout clock, held to spec/vectors/clock.json like the TypeScript SDK."""

import json
import time
from pathlib import Path
from typing import Any

import pytest

from moq_media import BroadcastClock, PlayoutClock, PlayoutDecision

SPEC = Path(__file__).resolve().parents[4] / "spec"
VECTORS = json.loads((SPEC / "vectors/clock.json").read_text())
OPTION_NAMES = {
    "lateDropUs": "late_drop_us",
    "maxVideoDelayUs": "max_video_delay_us",
    "audioTimeoutUs": "audio_timeout_us",
}


class Source:
    """A settable microsecond source."""

    def __init__(self, start_us: int) -> None:
        self.current = start_us

    def __call__(self) -> int:
        return self.current


def decision(raw: dict[str, str]) -> PlayoutDecision:
    return PlayoutDecision(raw["action"], int(raw["offsetUs"]), int(raw.get("waitUs", "0")))  # ty: ignore[invalid-argument-type]


@pytest.mark.parametrize("vector", VECTORS["broadcast"], ids=lambda vector: vector["name"])
def test_broadcast_clock_vectors(vector: dict[str, Any]) -> None:
    source = Source(int(vector["createdAt"]))
    origin = vector.get("originUs")
    clock = BroadcastClock(now_us=source, origin_us=None if origin is None else int(origin))
    for step in vector["steps"]:
        source.current = int(step["at"])
        if "now" in step:
            assert clock.now_us() == int(step["now"])
        if "fromSource" in step:
            assert clock.from_source_us(int(step["fromSource"])) == int(step["expect"])


@pytest.mark.parametrize("vector", VECTORS["epochOrigins"], ids=lambda vector: vector["name"])
def test_epoch_origin_vectors(vector: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    reads: list[int] = []

    def system_clock() -> int:
        reads.append(1)
        return int(vector["systemClockMs"]) * 1_000_000

    monkeypatch.setattr(time, "time_ns", system_clock)
    origin, given = vector.get("originUs"), vector.get("givenOriginEpochUs")
    clock = BroadcastClock(
        now_us=Source(int(vector["createdAt"])),
        origin_us=None if origin is None else int(origin),
        origin_epoch_us=None if given is None else int(given),
    )
    assert clock.origin_epoch_us == int(vector["originEpochUs"])
    assert len(reads) == (0 if given is not None else 1)


@pytest.mark.parametrize("vector", VECTORS["epochOriginErrors"], ids=lambda vector: vector["name"])
def test_epoch_origin_errors(vector: dict[str, Any]) -> None:
    given = vector["givenOriginEpochUs"]
    with pytest.raises(ValueError):
        BroadcastClock(now_us=Source(0), origin_epoch_us=float(given) if "." in given else int(given))  # ty: ignore[invalid-argument-type]


@pytest.mark.parametrize("vector", VECTORS["sampleTimelines"], ids=lambda vector: vector["name"])
def test_sample_timeline_vectors(vector: dict[str, Any]) -> None:
    source = Source(int(vector["createdAt"]))
    clock = BroadcastClock(now_us=source)
    source.current = int(vector["timelineAt"])
    first = vector.get("firstSampleUs")
    timeline = clock.sample_timeline(int(vector["sampleRate"]), first_sample_us=None if first is None else int(first))
    for index, timestamp in vector["samples"]:
        assert timeline.timestamp_us(int(index)) == int(timestamp)


@pytest.mark.parametrize("vector", VECTORS["sourceTimelines"], ids=lambda vector: vector["name"])
def test_source_timeline_vectors(vector: dict[str, Any]) -> None:
    source = Source(int(vector["createdAt"]))
    timeline = BroadcastClock(now_us=source).source_timeline()
    assert timeline.anchor_source_us is None
    for step in vector["steps"]:
        source.current = int(step["at"])
        assert timeline.timestamp_us(int(step["source"])) == int(step["expect"])
    assert timeline.anchor_source_us == int(vector["steps"][0]["source"])


@pytest.mark.parametrize("vector", VECTORS["calibratedTimelines"], ids=lambda vector: vector["name"])
def test_calibrated_timeline_vectors(vector: dict[str, Any]) -> None:
    tolerance = vector.get("toleranceUs")
    clock = BroadcastClock(now_us=Source(int(vector["createdAt"])))
    timeline = clock.source_timeline() if tolerance is None else clock.source_timeline(tolerance_us=int(tolerance))
    for step in vector["steps"]:
        if "calibrate" in step:
            assert timeline.calibrate(int(step["calibrate"]), at_us=int(step["calibrateAt"])) is step["expect"]
        else:
            assert timeline.timestamp_us(int(step["source"])) == int(step["expect"])


@pytest.mark.parametrize("vector", VECTORS["playout"], ids=lambda vector: vector["name"])
def test_playout_clock_vectors(vector: dict[str, Any]) -> None:
    source = Source(0)
    options = {OPTION_NAMES[key]: int(value) for key, value in vector.get("options", {}).items()}
    clock = PlayoutClock(now_us=source, **options)
    for step in vector["steps"]:
        source.current = int(step["at"])
        if "report" in step:
            at = step.get("reportAt")
            clock.report_audio(int(step["report"]), at_us=None if at is None else int(at))
        if step.get("stopAudio"):
            clock.stop_audio()
        if step.get("reset"):
            clock.reset()
        if "due" in step:
            assert clock.due(int(step["due"])) == decision(step["expect"])
        if "mode" in step:
            assert clock.mode == step["mode"]
        if "position" in step:
            expected = step["position"]
            assert clock.position_us() == (None if expected is None else int(expected))


class TestBroadcastClock:
    def test_reads_the_monotonic_clock_by_default(self) -> None:
        before = time.monotonic_ns() // 1000
        clock = BroadcastClock()
        assert before <= clock.origin_us <= time.monotonic_ns() // 1000
        assert 0 <= clock.now_us() < 1_000_000

    def test_reads_the_system_clock_for_the_origin_epoch_by_default(self) -> None:
        before = time.time_ns() // 1000
        clock = BroadcastClock()
        assert before - 1_000 <= clock.origin_epoch_us <= time.time_ns() // 1000 + 1_000

    def test_rounds_the_origin_epoch_measured_from_a_fractional_source(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(time, "time_ns", lambda: 2_000_000_000)
        clock = BroadcastClock(now_us=lambda: 1000.5, origin_us=0)  # ty: ignore[invalid-argument-type]
        assert clock.origin_epoch_us == 2_000_000 - 1000

    def test_calibrates_at_the_time_source_now_by_default(self) -> None:
        source = Source(0)
        context = BroadcastClock(now_us=source).source_timeline()
        source.current = 250_000
        assert context.calibrate(7_000_000) is True
        assert context.anchor_source_us == 7_000_000
        assert context.timestamp_us(7_020_000) == 270_000

    def test_rounds_a_fractional_default_origin(self) -> None:
        clock = BroadcastClock(now_us=lambda: 1000.25)  # ty: ignore[invalid-argument-type]
        assert clock.origin_us == 1000

    def test_rounds_fractional_source_times_to_the_nearest_microsecond_ties_upward(self) -> None:
        clock = BroadcastClock(now_us=lambda: 0, origin_us=0)
        assert clock.from_source_us(10.5) == 11  # ty: ignore[invalid-argument-type]
        assert clock.from_source_us(-0.5) == 0  # ty: ignore[invalid-argument-type]
        assert isinstance(clock.from_source_us(10.4), int)  # ty: ignore[invalid-argument-type]

    def test_rejects_bad_origins_source_times_rates_and_indexes(self) -> None:
        with pytest.raises(ValueError):
            BroadcastClock(now_us=lambda: 0, origin_us=float("nan"))  # ty: ignore[invalid-argument-type]
        with pytest.raises(ValueError):
            BroadcastClock(now_us=lambda: 0, origin_us=0.5)  # ty: ignore[invalid-argument-type]
        with pytest.raises(ValueError):
            BroadcastClock(now_us=lambda: float("nan"))  # ty: ignore[invalid-argument-type]
        clock = BroadcastClock(now_us=lambda: 0)
        with pytest.raises(ValueError):
            clock.source_timeline().timestamp_us(float("nan"))  # ty: ignore[invalid-argument-type]
        with pytest.raises(ValueError):
            clock.source_timeline(tolerance_us=-1)
        with pytest.raises(ValueError):
            clock.source_timeline().calibrate(0, at_us=float("nan"))  # ty: ignore[invalid-argument-type]
        with pytest.raises(ValueError):
            clock.from_source_us(float("inf"))  # ty: ignore[invalid-argument-type]
        with pytest.raises(ValueError):
            clock.from_source_us(True)
        for rate in (0, -48_000, 44_100.5, True):
            with pytest.raises(ValueError):
                clock.sample_timeline(rate)  # ty: ignore[invalid-argument-type]
        with pytest.raises(ValueError):
            clock.sample_timeline(48_000, first_sample_us=0.5)  # ty: ignore[invalid-argument-type]
        timeline = clock.sample_timeline(48_000)
        assert (timeline.sample_rate, timeline.first_sample_us) == (48_000, 0)
        for index in (-1, 1.5, True):
            with pytest.raises(ValueError):
                timeline.timestamp_us(index)  # ty: ignore[invalid-argument-type]


class TestPlayoutClock:
    def test_paces_by_wall_clock_when_no_audio_is_ever_reported(self) -> None:
        source = Source(0)
        clock = PlayoutClock(now_us=source)
        shown = []
        # 30 fps video, a frame arriving every 33 ms, with 10 ms of decode jitter.
        for frame in range(30):
            timestamp = frame * 33_333
            source.current = timestamp + (10_000 if frame % 2 else 0)
            first = clock.due(timestamp)
            assert first.action != "drop"
            source.current += first.wait_us
            if clock.due(timestamp).action == "show":
                shown.append(timestamp)
        assert len(shown) == 30
        assert clock.mode == "wall"

    def test_decisions_are_plain_values(self) -> None:
        clock = PlayoutClock(now_us=lambda: 0)
        clock.report_audio(0)
        assert clock.due(10) == PlayoutDecision("wait", 10, 10)
        assert clock.due(0) == PlayoutDecision("show", 0)
        assert clock.due(-50_000).wait_us == 0

    def test_reads_the_monotonic_clock_by_default(self) -> None:
        clock = PlayoutClock()
        clock.report_audio(5_000)
        position = clock.position_us()
        assert position is not None and 5_000 <= position < 1_005_000
        assert clock.mode == "audio"

    def test_rejects_bad_options_and_timestamps(self) -> None:
        with pytest.raises(ValueError):
            PlayoutClock(late_drop_us=-1)
        with pytest.raises(ValueError):
            PlayoutClock(max_video_delay_us=0)
        with pytest.raises(ValueError):
            PlayoutClock(audio_timeout_us=0)
        clock = PlayoutClock(now_us=lambda: 0)
        with pytest.raises(ValueError):
            clock.due(float("nan"))  # ty: ignore[invalid-argument-type]
        with pytest.raises(ValueError):
            clock.report_audio(0, at_us=float("inf"))  # ty: ignore[invalid-argument-type]
