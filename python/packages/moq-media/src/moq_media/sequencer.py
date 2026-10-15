"""The frame sequencer: turns one track's objects, which arrive in any order across group streams, into frames in
decode order, and drops what is too late against a playout clock. It is the engine of a frame reader; it has no
timers and no transport. See spec/behavior.md, "Media subscribing".
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field, replace
from typing import Literal

from moq_core import KeyValuePair, MoqtObject

from .clock import PlayoutClock
from .loc import loc_timestamp_us

FrameDiscontinuity = Literal["gap", "resync", "restart"]
"""Why a frame is not the one decoded after the frame before it: ``gap`` (frames before it were skipped: missing,
reset, or too late), ``resync`` (nothing arrived within the target latency for one target latency; the sequencer reset
the clock), ``restart`` (the subscription was replaced: a want's Restart or Move). A decoder resets there."""

# How many keyframe timestamps are kept to measure intervals between consecutive groups.
_KEYFRAMES_KEPT = 8
# While the clock is idle and frames are held, poll again after this long: the clock may have anchored meanwhile.
_IDLE_RECHECK_US = 50_000
# At most this many frames are held, about 20 s of audio; beyond it everything held is dropped as a gap.
_MAX_HELD_FRAMES = 1024


@dataclass(frozen=True, slots=True)
class SequencedFrame:
    """One frame in decode order."""

    group_id: int
    object_id: int
    timestamp_us: int
    """The LOC timestamp in microseconds on the broadcast timeline."""
    key: bool
    """Whether the frame starts its group: a keyframe under keyframe grouping, every frame under frame grouping."""
    payload: bytes
    properties: list[KeyValuePair]
    discontinuity: FrameDiscontinuity | None = None
    """Set on the first frame after a skip, a resync, or a restart."""


@dataclass(frozen=True, slots=True)
class SequencerPoll:
    """What :meth:`FrameSequencer.poll` returns."""

    frames: list[SequencedFrame]
    """The frames ready now, in decode order."""
    wait_us: int | None
    """Poll again after this many microseconds, when a wait can end; ``None`` when only new objects can change it."""


@dataclass(slots=True)
class FrameSequencerStats:
    """Counters, cumulative across restarts."""

    delivered: int = 0
    """Frames returned by ``poll()``."""
    late: int = 0
    """Frames discarded: too late, or behind what was already delivered."""
    skipped_groups: int = 0
    """Groups abandoned or passed over without being delivered whole."""
    resyncs: int = 0
    """Resyncs: deliveries after nothing arrived within the target latency."""
    untimed: int = 0
    """Objects without a LOC timestamp, which cannot be sequenced."""


@dataclass(slots=True)
class _Group:
    frames: dict[int, SequencedFrame] = field(default_factory=dict)
    ended: bool = False
    """The stream ended (FIN, or an end-of-group status): no further objects."""
    reset: bool = False
    """The stream was reset: objects after those received are lost."""


class FrameSequencer:
    """One track's frame sequencer. Feed it the subscription's events, then ``poll()`` for frames.

    ``clock`` is the broadcast's playout clock: it holds the target latency and is where lateness is measured.
    ``grouping`` is ``"keyframe"`` (a keyframe opens each group and later objects depend on it, as video) or ``"frame"``
    (every group is independently decodable, as audio). ``track_properties`` are SUBSCRIBE_OK's, which may declare the
    Timescale.
    """

    def __init__(
        self,
        clock: PlayoutClock,
        *,
        grouping: Literal["keyframe", "frame"] = "keyframe",
        track_properties: Iterable[KeyValuePair] = (),
    ) -> None:
        if grouping not in ("keyframe", "frame"):
            raise ValueError("grouping must be 'keyframe' or 'frame'")
        self._clock = clock
        self._keyframe_grouping = grouping == "keyframe"
        self.track_properties: Sequence[KeyValuePair] = list(track_properties)
        """The track properties used to read timestamps; set from each SUBSCRIBE_OK."""
        self.stats = FrameSequencerStats()
        # Groups above `floor` that have not been delivered or abandoned.
        self._groups: dict[int, _Group] = {}
        # Groups at or below `floor` are behind what was delivered or abandoned; None until the first.
        self._floor: int | None = None
        # The group being delivered and its next object, under keyframe grouping.
        self._cursor: list[int] | None = None
        self._last_delivered_us: int | None = None
        self._delivered_since_restart = False
        self._pending: FrameDiscontinuity | None = None
        # Starving: something arrived but none of it was within the budget, since this position.
        self._starved_at_us: float | None = None
        # The newest group (keyframe grouping) or frame (frame grouping) dropped as late: what a resync delivers.
        self._newest_late: tuple[int, _Group] | None = None
        self._keyframes: dict[int, int] = {}
        # The most frames one poll may return.
        self._limit: float = math.inf
        self._since_held_check = 0

    # --- events

    def object(self, obj: MoqtObject) -> None:
        """An object arrived."""
        if obj.status == "end-of-group":
            self.group_ended(obj.group_id)
            return
        if obj.status != "normal":
            return
        timestamp = loc_timestamp_us(obj.properties, track_properties=self.track_properties)
        if timestamp is None:
            self.stats.untimed += 1
            return
        late = self._newest_late
        if self._keyframe_grouping and late is not None and late[0] == obj.group_id:
            # The resync candidate keeps collecting its group: a resync delivers from its keyframe on.
            if obj.object_id not in late[1].frames:
                late[1].frames[obj.object_id] = self._frame_of(obj, timestamp)
                self.stats.late += 1
                self._bound_held()
            return
        if self._behind(obj.group_id, obj.object_id):
            self.stats.late += 1
            return
        # Only a keyframe that can still be delivered reports an interval: an old duplicate must not move the budget.
        if self._keyframe_grouping and obj.object_id == 0:
            self._note_keyframe(obj.group_id, timestamp)
        group = self._group_of(obj.group_id)
        if obj.object_id in group.frames:
            return
        group.frames[obj.object_id] = self._frame_of(obj, timestamp)
        self._bound_held()

    def group_ended(self, group_id: int) -> None:
        """A group's stream ended cleanly: no further objects of it will come."""
        self._group_over(group_id, reset=False)

    def group_reset(self, group_id: int) -> None:
        """A group's stream was reset: its remaining objects will not come."""
        self._group_over(group_id, reset=True)

    def _group_over(self, group_id: int, *, reset: bool) -> None:
        """A group's stream is over. One with nothing in it is kept only when it is the next group, where it says a
        gap; anything else would only accumulate."""
        late = self._newest_late
        if late is not None and late[0] == group_id:
            group: _Group | None = late[1]
        elif self._floor is not None and group_id <= self._floor:
            return
        else:
            group = self._groups.get(group_id)
            if group is None and self._floor is not None and group_id == self._floor + 1:
                group = self._group_of(group_id)
        if group is not None:
            if reset:
                group.reset = True
            else:
                group.ended = True
        self._bound_held()

    def restart(self) -> None:
        """The subscription was replaced (a Restart or a Move): forget everything; group IDs start over."""
        self._groups.clear()
        self._keyframes.clear()
        self._floor = None
        self._cursor = None
        self._last_delivered_us = None
        self._starved_at_us = None
        self._newest_late = None
        self._pending = "restart" if self._delivered_since_restart or self._pending is not None else None
        self._delivered_since_restart = False

    def poll(self, max_frames: int | None = None) -> SequencerPoll:
        """Take the frames that are ready, at most ``max_frames`` (default all), and when to poll again.

        A reader that cannot take frames as they come polls one at a time, so each is judged late when it is taken.
        """
        if max_frames is not None and (
            isinstance(max_frames, bool) or not isinstance(max_frames, int) or max_frames < 1
        ):
            raise ValueError("max_frames must be a positive integer")
        self._limit = math.inf if max_frames is None else max_frames
        out: list[SequencedFrame] = []
        wait = self._poll_keyframes(out) if self._keyframe_grouping else self._poll_frames(out)
        resync = self._resync(out)
        if wait is None or (resync is not None and resync < wait):
            wait = resync
        # Held frames wait on deadlines that only exist once the clock has a position.
        if wait is None and self._clock.position_us() is None and (self._groups or self._newest_late is not None):
            wait = _IDLE_RECHECK_US
        return SequencerPoll(out, None if wait is None else max(1, math.ceil(wait)))

    # --- internals

    @staticmethod
    def _frame_of(obj: MoqtObject, timestamp: int) -> SequencedFrame:
        return SequencedFrame(obj.group_id, obj.object_id, timestamp, obj.object_id == 0, obj.payload, obj.properties)

    def _group_of(self, group_id: int) -> _Group:
        group = self._groups.get(group_id)
        if group is None:
            group = self._groups[group_id] = _Group()
        return group

    def _note_keyframe(self, group_id: int, timestamp: int) -> None:
        keyframes = self._keyframes
        if group_id in keyframes:
            return
        keyframes[group_id] = timestamp
        before = keyframes.get(group_id - 1)
        if before is not None and timestamp > before:
            self._clock.report_keyframe_interval(timestamp - before)
        after = keyframes.get(group_id + 1)
        if after is not None and after > timestamp:
            self._clock.report_keyframe_interval(after - timestamp)
        if len(keyframes) > _KEYFRAMES_KEPT:
            del keyframes[min(keyframes)]

    def _behind(self, group_id: int, object_id: int) -> bool:
        if self._floor is not None and group_id <= self._floor:
            return True
        cursor = self._cursor
        return cursor is not None and group_id == cursor[0] and object_id < cursor[1]

    def _pass_over(self, group_id: int) -> _Group | None:
        """Mark a group as not delivered whole, and everything up to it as behind."""
        group = self._groups.pop(group_id, None)
        for other in [other for other in self._groups if other < group_id]:
            del self._groups[other]
            self.stats.skipped_groups += 1
        self._floor = group_id if self._floor is None else max(self._floor, group_id)
        if self._cursor is not None and self._cursor[0] == group_id:
            self._cursor = None
        self.stats.skipped_groups += 1
        if self._delivered_since_restart and self._pending is None:
            self._pending = "gap"
        return group

    def _late_group(self, group_id: int, position: float) -> None:
        group = self._pass_over(group_id)
        self.stats.late += len(group.frames) if group else 0
        # Only a group that still has its keyframe can be resynced on.
        if group is not None and 0 in group.frames and (self._newest_late is None or group_id > self._newest_late[0]):
            self._newest_late = (group_id, group)
        if self._starved_at_us is None:
            self._starved_at_us = position

    def _deliver(self, frame: SequencedFrame, out: list[SequencedFrame]) -> None:
        if self._pending is not None:
            frame = replace(frame, discontinuity=self._pending)
            self._pending = None
        out.append(frame)
        self._last_delivered_us = frame.timestamp_us
        self._delivered_since_restart = True
        self._starved_at_us = None
        self._newest_late = None
        self.stats.delivered += 1

    def _lowest_keyed(self, above: int | None) -> int | None:
        keyed = [gid for gid, group in self._groups.items() if (above is None or gid > above) and 0 in group.frames]
        return min(keyed) if keyed else None

    def _poll_keyframes(self, out: list[SequencedFrame]) -> float | None:
        """Keyframe grouping: deliver what is in order, wait for what may still come, skip what is too late."""
        position = self._clock.position_us()
        target = self._clock.target_latency_us
        late_before = -math.inf if position is None else position - target

        def gap_wait() -> float | None:
            # How long newer frames wait on a missing one: it is later than the last delivered frame, so it may still
            # come until that frame's timestamp plus the target latency. While the clock is idle nothing is late, and
            # the wait ends with the missing frame's group.
            if position is None:
                return None
            return 0 if self._last_delivered_us is None else self._last_delivered_us + target - position

        while True:
            if len(out) >= self._limit:
                return None
            cursor = self._cursor
            nxt = None if self._floor is None else self._floor + 1
            # A group that ended or was reset without its keyframe can never be delivered; only the next group's says
            # a gap.
            for gid in [
                gid
                for gid, group in self._groups.items()
                if gid != nxt
                and (cursor is None or gid != cursor[0])
                and (group.ended or group.reset)
                and 0 not in group.frames
            ]:
                del self._groups[gid]
                self.stats.skipped_groups += 1
            if cursor is not None:
                group = self._groups[cursor[0]]
                frame = group.frames.get(cursor[1])
                if frame is not None:
                    if frame.timestamp_us < late_before:
                        self._late_group(cursor[0], position)  # ty: ignore[invalid-argument-type]
                        continue
                    del group.frames[cursor[1]]
                    cursor[1] += 1
                    self._deliver(frame, out)
                    continue
                if group.ended and not group.frames:
                    # Delivered whole: the next group follows on.
                    del self._groups[cursor[0]]
                    self._floor = cursor[0]
                    self._cursor = None
                    continue
                if not group.ended and not group.reset:
                    # The group is open and its next frame missing, which costs nothing until a newer group is waiting.
                    if self._lowest_keyed(cursor[0]) is None:
                        return None
                    wait = gap_wait()
                    if wait is None or wait > 0:
                        return wait
                # Reset, ended with a hole, or waited out.
                self._pass_over(cursor[0])
                continue

            started = None if nxt is None else self._groups.get(nxt)
            if nxt is not None and started is not None and 0 not in started.frames:
                # The next group started without its keyframe, which may still come while its stream is open.
                if not started.ended and not started.reset:
                    if self._lowest_keyed(nxt) is None:
                        return None
                    wait = gap_wait()
                    if wait is None or wait > 0:
                        return wait
                self._pass_over(nxt)
                continue

            # The next group if it has its keyframe; otherwise, or before anything was delivered, the lowest that does.
            group_id = nxt if started is not None else self._lowest_keyed(self._floor)
            if group_id is None:
                return None
            keyframe = self._groups[group_id].frames[0]
            if keyframe.timestamp_us < late_before:
                self._late_group(group_id, position)  # ty: ignore[invalid-argument-type]
                continue
            if position is not None and keyframe.timestamp_us - position > target:
                # The clock is behind what arrives, anchored on a backlog: resync forward on this keyframe.
                self._resync_forward()
                return self._poll_keyframes(out)
            if nxt is not None and group_id > nxt:
                # An older group may still be on its way, or may never be sent (a failed group's ID is not reused).
                # Wait only while this keyframe is not yet due: until then, holding it costs no latency.
                if position is not None and keyframe.timestamp_us > position:
                    return keyframe.timestamp_us - position
                if self._delivered_since_restart and self._pending is None:
                    self._pending = "gap"
            for other in [other for other in self._groups if other < group_id]:
                del self._groups[other]
                self.stats.skipped_groups += 1
            self._cursor = [group_id, 0]

    def _poll_frames(self, out: list[SequencedFrame]) -> None:
        """Frame grouping: every frame stands alone; deliver in order at once, and drop what is late or behind."""
        position = self._clock.position_us()
        late_before = -math.inf if position is None else position - self._clock.late_drop_us
        for group_id in sorted(self._groups):
            group = self._groups[group_id]
            for object_id in sorted(group.frames):
                if len(out) >= self._limit:
                    return None
                frame = group.frames.pop(object_id)
                self._floor = group_id
                if position is not None and frame.timestamp_us - position > self._clock.target_latency_us:
                    # The clock is behind what arrives, anchored on a backlog: resync forward on this frame.
                    self._resync_forward()
                    position = None
                    late_before = -math.inf
                if frame.timestamp_us < late_before:
                    self.stats.late += 1
                    self._newest_late = (group_id, _Group({object_id: frame}, ended=True))
                    if self._starved_at_us is None:
                        self._starved_at_us = position
                    if self._delivered_since_restart and self._pending is None:
                        self._pending = "gap"
                    continue
                self._deliver(frame, out)
            del self._groups[group_id]
        return None

    def _bound_held(self) -> None:
        """Bound what is held when nobody polls: past the bound, everything held is dropped and the next frame is a
        gap."""
        self._since_held_check += 1
        if self._since_held_check < 64:
            return
        self._since_held_check = 0
        late = self._newest_late
        held = len(late[1].frames) if late else 0
        ids = [late[0]] if late else []
        for group_id, group in self._groups.items():
            held += len(group.frames)
            ids.append(group_id)
        if (held <= _MAX_HELD_FRAMES and len(self._groups) <= _MAX_HELD_FRAMES) or not ids:
            return
        self.stats.late += held
        self.stats.skipped_groups += len(self._groups)
        self._groups.clear()
        self._newest_late = None
        self._starved_at_us = None
        self._cursor = None
        highest = max(ids)
        self._floor = highest if self._floor is None else max(self._floor, highest)
        if self._delivered_since_restart and self._pending is None:
            self._pending = "gap"

    def _resync_forward(self) -> None:
        """A frame more than the target latency ahead of the position: the clock was anchored on a backlog (a resync
        during a long stall), and holding everything that far behind would make the backlog permanent."""
        self._clock.reset()
        self.stats.resyncs += 1
        self._pending = "resync"
        self._starved_at_us = None
        self._newest_late = None

    def _resync(self, out: list[SequencedFrame]) -> float | None:
        """Nothing within the budget for a whole target latency: deliver the newest late group or frame, and reset
        the clock so it re-anchors there. A lasting increase in delay must not keep the player black or silent."""
        if self._starved_at_us is None or self._newest_late is None or len(out) >= self._limit:
            return None
        position = self._clock.position_us()
        if position is None:
            return None
        wait = self._starved_at_us + self._clock.target_latency_us - position
        if wait > 0:
            return wait
        group_id, group = self._newest_late
        self._newest_late = None
        self._starved_at_us = None
        self._clock.reset()
        self.stats.resyncs += 1
        # Counted as late when dropped; it is delivered after all.
        self.stats.late -= len(group.frames)
        for other in [other for other in self._groups if other < group_id]:
            del self._groups[other]
        self._groups[group_id] = group
        self._floor = group_id - 1
        self._pending = "resync"
        if not self._keyframe_grouping:
            self._cursor = None
            self._poll_frames(out)
            return None
        # From the resynced keyframe on; the clock is idle, so nothing is late until it anchors again.
        self._cursor = [group_id, 0]
        return self._poll_keyframes(out)
