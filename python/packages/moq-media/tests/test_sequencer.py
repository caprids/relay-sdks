"""The frame sequencer, held to spec/vectors/frame-sequencer.json like the TypeScript SDK."""

import json
from pathlib import Path
from typing import Any

import pytest

from moq_core import KeyValuePair, MoqtObject
from moq_media import LOC_TIMESCALE, LOC_TIMESTAMP, FrameSequencer, PlayoutClock, SequencedFrame

SPEC = Path(__file__).resolve().parents[4] / "spec"
VECTORS = json.loads((SPEC / "vectors/frame-sequencer.json").read_text())
STATS = {
    "delivered": "delivered",
    "late": "late",
    "skippedGroups": "skipped_groups",
    "resyncs": "resyncs",
    "untimed": "untimed",
}


class Source:
    """A settable microsecond source."""

    def __init__(self) -> None:
        self.current = 0

    def __call__(self) -> int:
        return self.current


def obj(group: int, object_id: int, properties: list[KeyValuePair]) -> MoqtObject:
    return MoqtObject(
        group_id=group, object_id=object_id, subgroup_id=0, properties=properties, payload=bytes([group, object_id])
    )


def stamp(timestamp: int) -> list[KeyValuePair]:
    return [KeyValuePair(LOC_TIMESTAMP, timestamp)]


@pytest.mark.parametrize("vector", VECTORS["sequences"], ids=lambda vector: vector["name"])
def test_frame_sequencer_vectors(vector: dict[str, Any]) -> None:
    source = Source()
    clock = PlayoutClock(now_us=source)
    sequencer = FrameSequencer(clock, grouping=vector.get("grouping", "keyframe"))
    for step in vector["steps"]:
        source.current = int(step["at"])
        if "report" in step:
            clock.report_audio(int(step["report"]))
        if "anchor" in step:
            clock.due(int(step["anchor"]))
        if "object" in step:
            group, object_id, timestamp = step["object"]
            sequencer.object(obj(int(group), int(object_id), [] if timestamp is None else stamp(int(timestamp))))
        if "groupEnd" in step:
            sequencer.group_ended(int(step["groupEnd"]))
        if "groupReset" in step:
            sequencer.group_reset(int(step["groupReset"]))
        if step.get("restart"):
            sequencer.restart()
        if "poll" in step:
            result = sequencer.poll()
            frames = [
                [str(f.group_id), str(f.object_id), *([f.discontinuity] if f.discontinuity else [])]
                for f in result.frames
            ]
            assert frames == step["poll"]["frames"]
            expected_wait = step["poll"]["waitUs"]
            assert result.wait_us == (None if expected_wait is None else int(expected_wait))
        if "stats" in step:
            for key, value in step["stats"].items():
                assert getattr(sequencer.stats, STATS[key]) == int(value), key
        if "targetLatency" in step:
            assert clock.target_latency_us == int(step["targetLatency"])


def test_reads_timestamps_through_the_track_properties() -> None:
    sequencer = FrameSequencer(PlayoutClock(now_us=lambda: 0))
    sequencer.track_properties = [KeyValuePair(LOC_TIMESCALE, 1000)]
    sequencer.object(obj(4, 0, stamp(1500)))
    assert sequencer.poll().frames == [SequencedFrame(4, 0, 1_500_000, True, bytes([4, 0]), stamp(1500))]


def test_an_end_of_group_status_object_ends_its_group() -> None:
    clock = PlayoutClock(now_us=lambda: 0)
    clock.due(0)
    sequencer = FrameSequencer(clock)
    sequencer.object(obj(0, 0, stamp(0)))
    sequencer.object(obj(1, 0, stamp(1_000_000)))
    assert [f.group_id for f in sequencer.poll().frames] == [0]
    sequencer.object(MoqtObject(group_id=0, object_id=1, subgroup_id=0, status="end-of-group"))
    assert [(f.group_id, f.discontinuity) for f in sequencer.poll().frames] == [(1, None)]


def test_ignores_a_repeated_object() -> None:
    sequencer = FrameSequencer(PlayoutClock(now_us=lambda: 0), grouping="frame")
    sequencer.object(obj(0, 0, stamp(0)))
    sequencer.object(obj(0, 0, stamp(0)))
    assert len(sequencer.poll().frames) == 1


def test_waits_on_a_gap_while_the_clock_is_idle_until_the_group_ends_checking_back_for_the_clock() -> None:
    sequencer = FrameSequencer(PlayoutClock(now_us=lambda: 0))
    sequencer.object(obj(0, 0, stamp(0)))
    sequencer.object(obj(1, 0, stamp(1_000_000)))
    result = sequencer.poll()
    assert [f.group_id for f in result.frames] == [0] and result.wait_us == 50_000
    sequencer.group_ended(0)
    assert [f.group_id for f in sequencer.poll().frames] == [1]


def test_rejects_an_unknown_grouping() -> None:
    with pytest.raises(ValueError):
        FrameSequencer(PlayoutClock(), grouping="gop")  # ty: ignore[invalid-argument-type]
