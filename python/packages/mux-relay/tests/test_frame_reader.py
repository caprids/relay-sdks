"""The frame reader over a real sequencer: events in, frames out in decode order, polled again when a wait ends."""

import asyncio

import pytest

from moq_core import KeyValuePair, MoqtObject, SubgroupEnd, SubgroupReset, SubscribeOk
from moq_media import LOC_TIMESCALE, LOC_TIMESTAMP, FrameSequencer, PlayoutClock, SequencedFrame
from mux_relay.media import FrameReader
from mux_relay.supervised_subscriber import WantSwitchedInfo


def obj(group: int, object_id: int, timestamp_us: int) -> MoqtObject:
    return MoqtObject(
        group_id=group,
        object_id=object_id,
        subgroup_id=0,
        properties=[KeyValuePair(LOC_TIMESTAMP, timestamp_us)],
        payload=bytes([group, object_id]),
    )


def ids(frames: list[SequencedFrame]) -> list[tuple[int, int, str | None]]:
    return [(f.group_id, f.object_id, f.discontinuity) for f in frames]


def ok(track_properties: list[KeyValuePair]) -> SubscribeOk:
    return SubscribeOk(track_alias=1, track_properties=track_properties)


async def take(reader: FrameReader[SequencedFrame], count: int) -> list[SequencedFrame]:
    out: list[SequencedFrame] = []
    async for frame in reader:
        out.append(frame)
        if len(out) == count:
            break
    return out


class Clock:
    """A clock whose time is the event loop's, so a reader's timer and the playout position agree."""

    def __call__(self) -> int:
        return round(asyncio.get_running_loop().time() * 1_000_000)


async def test_delivers_interleaved_groups_in_decode_order() -> None:
    clock = PlayoutClock(now_us=Clock())
    clock.due(0)
    reader = FrameReader(FrameSequencer(clock))
    handlers = reader.subscription_handlers()
    assert handlers.on_object is not None and handlers.on_subgroup_end is not None
    handlers.on_object(obj(0, 0, 0))
    handlers.on_object(obj(1, 0, 1_000_000))
    handlers.on_object(obj(0, 1, 33_333))
    handlers.on_subgroup_end(SubgroupEnd(group_id=0, subgroup_id=0, end_of_group=True))
    assert ids(await take(reader, 3)) == [(0, 0, None), (0, 1, None), (1, 0, None)]


async def test_polls_again_when_a_wait_ends() -> None:
    clock = PlayoutClock(now_us=Clock(), target_latency_us=50_000)
    clock.due(0)
    reader = FrameReader(FrameSequencer(clock))
    handlers = reader.subscription_handlers()
    assert handlers.on_object is not None
    handlers.on_object(obj(0, 0, 0))
    handlers.on_object(obj(1, 0, 1_000_000))
    frames = await asyncio.wait_for(take(reader, 2), 1)
    assert ids(frames) == [(0, 0, None), (1, 0, "gap")]


async def test_reads_timestamps_through_the_track_properties() -> None:
    reader = FrameReader(FrameSequencer(PlayoutClock()))
    handlers = reader.subscription_handlers()
    assert handlers.on_subscribed is not None and handlers.on_object is not None
    handlers.on_subscribed(ok([KeyValuePair(LOC_TIMESCALE, 1000)]))
    handlers.on_object(obj(0, 0, 5))
    assert (await take(reader, 1))[0].timestamp_us == 5000


async def test_a_restart_and_a_move_start_over_and_reach_the_application() -> None:
    seen: list[str] = []
    reader = FrameReader(FrameSequencer(PlayoutClock()))
    handlers = reader.want_handlers(
        on_restarted=lambda reason, _namespace: seen.append(reason), on_switched=lambda _info: seen.append("moved")
    )
    assert handlers.on_object is not None and handlers.on_restarted is not None and handlers.on_switched is not None
    handlers.on_object(obj(7, 0, 0))
    handlers.on_restarted("stale", ("a",))
    handlers.on_object(obj(2, 0, 100))
    handlers.on_switched(WantSwitchedInfo(("a",), "video", ("b",), "video", ok([KeyValuePair(LOC_TIMESCALE, 1000)])))
    handlers.on_object(obj(0, 0, 1))
    frames = await take(reader, 3)
    assert ids(frames) == [(7, 0, None), (2, 0, "restart"), (0, 0, "restart")]
    assert frames[2].timestamp_us == 1000
    assert seen == ["stale", "moved"]


async def test_a_reset_group_is_passed_over() -> None:
    clock = PlayoutClock(now_us=Clock())
    clock.due(0)
    reader = FrameReader(FrameSequencer(clock))
    handlers = reader.subscription_handlers()
    assert handlers.on_object is not None and handlers.on_subgroup_reset is not None
    handlers.on_object(obj(0, 0, 0))
    handlers.on_object(obj(1, 0, 1_000_000))
    handlers.on_subgroup_reset(SubgroupReset(group_id=0, error=ConnectionResetError()))
    assert ids(await take(reader, 2)) == [(0, 0, None), (1, 0, "gap")]


async def test_close_ends_iteration_and_ignores_later_events() -> None:
    clock = PlayoutClock(now_us=Clock())
    clock.due(0)
    reader = FrameReader(FrameSequencer(clock))
    handlers = reader.subscription_handlers()
    assert handlers.on_object is not None
    handlers.on_object(obj(0, 0, 0))
    handlers.on_object(obj(1, 0, 1_000_000))
    pending = asyncio.ensure_future(take(reader, 5))
    await asyncio.sleep(0)
    reader.close()
    handlers.on_object(obj(0, 1, 33_333))
    assert ids(await asyncio.wait_for(pending, 1)) == [(0, 0, None)]
    assert reader.closed
    assert reader._timer is None


@pytest.mark.parametrize("name", ["subscription_handlers", "want_handlers"])
def test_handlers_need_no_running_loop_until_an_event(name: str) -> None:
    reader = FrameReader(FrameSequencer(PlayoutClock()))
    assert getattr(reader, name)() is not None
