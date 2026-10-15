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


def test_takes_at_most_max_frames_judging_each_when_taken() -> None:
    now = [0]
    clock = PlayoutClock(now_us=lambda: now[0])
    clock.report_audio(0)
    sequencer = FrameSequencer(clock, grouping="frame")
    for index in range(3):
        sequencer.object(obj(index, 0, stamp(100_000 + index * 20_000)))
    assert [f.group_id for f in sequencer.poll(1).frames] == [0]
    now[0] = 5_000_000
    clock.report_audio(5_000_000)
    sequencer.object(obj(3, 0, stamp(5_100_000)))
    assert [(f.group_id, f.discontinuity) for f in sequencer.poll(1).frames] == [(3, "gap")]
    assert sequencer.stats.late == 2
    with pytest.raises(ValueError):
        sequencer.poll(0)


def test_bounds_what_it_holds_when_nobody_polls() -> None:
    sequencer = FrameSequencer(PlayoutClock(now_us=lambda: 0))
    sequencer.object(obj(0, 0, stamp(0)))
    assert len(sequencer.poll().frames) == 1
    for index in range(1, 3000):
        sequencer.object(obj(index // 30, index % 30, stamp(index * 33_333)))
    frames = sequencer.poll().frames
    assert len(frames) <= 1100
    assert frames[0].key and frames[0].discontinuity == "gap"
    assert sequencer.stats.late > 1800


def test_bounds_a_resync_candidate_too() -> None:
    now = [0]
    clock = PlayoutClock(now_us=lambda: now[0])
    clock.due(0)
    now[0] = 5_000_000
    sequencer = FrameSequencer(clock)
    sequencer.object(obj(1, 0, stamp(1_000_000)))
    assert sequencer.poll().frames == []
    for index in range(1, 3000):
        sequencer.object(
            MoqtObject(group_id=1, object_id=index, subgroup_id=0, properties=stamp(1_000_000 + index), payload=b"")
        )
    now[0] = 7_000_000
    assert len(sequencer.poll().frames) <= 1100
    assert sequencer.stats.late <= 3000


def test_terminal_events_do_not_accumulate_empty_groups() -> None:
    sequencer = FrameSequencer(PlayoutClock(now_us=lambda: 0))
    sequencer.object(obj(0, 0, stamp(0)))
    sequencer.group_ended(0)
    sequencer.poll()
    for group in range(5, 5005):
        sequencer.group_reset(group)
    assert len(sequencer._groups) <= 1100


def test_takes_keyframe_intervals_only_from_groups_that_can_still_be_delivered() -> None:
    clock = PlayoutClock(now_us=lambda: 0)
    sequencer = FrameSequencer(clock)
    for group in range(10, 18):
        sequencer.object(obj(group, 0, stamp(group * 1_000_000)))
        sequencer.group_ended(group)
    sequencer.poll()
    for _ in range(4):
        sequencer.object(obj(9, 0, stamp(9_900_000)))
    assert clock.target_latency_us == 2_000_000


def test_says_when_more_is_ready_past_max_frames() -> None:
    clock = PlayoutClock(now_us=lambda: 0)
    clock.report_audio(0)
    audio = FrameSequencer(clock, grouping="frame")
    audio.object(obj(0, 0, stamp(10_000)))
    audio.object(obj(1, 0, stamp(30_000)))
    assert audio.poll(1).wait_us == 0
    result = audio.poll(1)
    assert [f.group_id for f in result.frames] == [1] and result.wait_us is None
    video = FrameSequencer(clock)
    video.object(obj(0, 0, stamp(10_000)))
    video.object(obj(0, 1, stamp(43_333)))
    assert video.poll(1).wait_us == 0


def test_drops_a_group_older_than_the_one_being_delivered() -> None:
    clock = PlayoutClock(now_us=lambda: 0)
    sequencer = FrameSequencer(clock)
    sequencer.object(obj(10, 0, stamp(10_000_000)))
    sequencer.poll()
    sequencer.object(obj(9, 0, stamp(9_900_000)))
    assert sequencer.stats.late == 1
    assert clock.target_latency_us == 2_000_000


def test_ignores_a_repeated_keyframe_still_held_once_its_timestamp_is_forgotten() -> None:
    clock = PlayoutClock(now_us=lambda: 0)
    sequencer = FrameSequencer(clock)
    for group, time in enumerate(
        [0, 100_000, 1_100_000, 2_100_000, 3_100_000, 4_100_000, 5_100_000, 6_100_000, 7_100_000]
    ):
        sequencer.object(obj(group, 0, stamp(time)))
    assert clock.target_latency_us == 2_000_000
    for _ in range(4):
        sequencer.object(obj(0, 0, stamp(0)))
    assert clock.target_latency_us == 2_000_000
