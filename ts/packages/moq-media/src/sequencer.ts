/**
 * The frame sequencer: turns one track's objects, which arrive in any order across group streams, into frames in
 * decode order, and drops what is too late against a playout clock. It is the engine of a frame reader; it has no
 * timers and no transport. See spec/behavior.md, "Media subscribing".
 */
import type { KeyValuePair, MoqtObject } from '@mux/moq-core';
import type { PlayoutClock } from './clock.js';
import { locTimestampUs } from './loc.js';

/** Why the frame is not the one decoded after the frame before it. A decoder resets there. */
export type FrameDiscontinuity =
  /** Frames before this one were skipped: missing, reset, or too late. */
  | 'gap'
  /** Nothing arrived within the target latency for one target latency; the sequencer reset the clock. */
  | 'resync'
  /** The subscription was replaced: a want's Restart or Move. */
  | 'restart';

/** One frame in decode order. */
export interface SequencedFrame {
  /** The object's group. */
  groupId: number;
  /** The object's ID within its group. */
  objectId: number;
  /** The LOC timestamp in microseconds on the broadcast timeline. */
  timestampUs: number;
  /** Whether the frame starts its group: a keyframe under keyframe grouping, every frame under frame grouping. */
  key: boolean;
  /** The object payload. */
  payload: Uint8Array;
  /** The object properties as received. */
  properties: KeyValuePair[];
  /** Set on the first frame after a skip, a resync, or a restart. */
  discontinuity?: FrameDiscontinuity;
}

/** Options for {@link createFrameSequencer}. */
export interface FrameSequencerOptions {
  /** The broadcast's playout clock. It holds the target latency and is where lateness is measured. */
  clock: PlayoutClock;
  /**
   * `'keyframe'` (default): a keyframe opens each group and later objects depend on it, as video. `'frame'`: every
   * group is independently decodable, as audio.
   */
  grouping?: 'keyframe' | 'frame';
  /** The track properties from SUBSCRIBE_OK, which may declare the Timescale. */
  trackProperties?: readonly KeyValuePair[];
}

/** What {@link FrameSequencer.poll} returns. */
export interface SequencerPoll {
  /** The frames ready now, in decode order. */
  frames: SequencedFrame[];
  /**
   * Poll again after this many microseconds, when a wait can end; 0 when more is ready now (past `maxFrames`, or a
   * resync that takes a poll of its own); `undefined` when only new objects can change it.
   */
  waitUs: number | undefined;
}

/** Counters, cumulative across restarts. */
export interface FrameSequencerStats {
  /** Frames returned by `poll()`. */
  delivered: number;
  /** Frames discarded: too late, or behind what was already delivered. */
  late: number;
  /** Groups abandoned or passed over without being delivered whole. */
  skippedGroups: number;
  /** Resyncs: deliveries after nothing arrived within the target latency. */
  resyncs: number;
  /** Objects without a LOC timestamp, which cannot be sequenced. */
  untimed: number;
}

/** One track's frame sequencer. Feed it the subscription's events, then `poll()` for frames. */
export interface FrameSequencer {
  /** The track properties used to read timestamps; set from each SUBSCRIBE_OK. */
  trackProperties: readonly KeyValuePair[];
  /** An object arrived. */
  object(object: MoqtObject): void;
  /** A group's stream ended cleanly: no further objects of it will come. */
  groupEnded(groupId: number): void;
  /** A group's stream was reset: its remaining objects will not come. */
  groupReset(groupId: number): void;
  /** The subscription was replaced (a Restart or a Move): forget everything and reset the clock; group IDs start over. */
  restart(): void;
  /**
   * Take the frames that are ready, at most `maxFrames` (default all), and when to poll again. A reader that cannot
   * take frames as they come polls one at a time, so each is judged late when it is taken.
   */
  poll(maxFrames?: number): SequencerPoll;
  /** Counters. */
  readonly stats: Readonly<FrameSequencerStats>;
}

interface Group {
  frames: Map<number, SequencedFrame>;
  /** The stream ended (FIN, or an end-of-group status): no further objects. */
  ended: boolean;
  /** The stream was reset: objects after those received are lost. */
  reset: boolean;
}

/** How many keyframe timestamps are kept to measure intervals between consecutive groups. */
const KEYFRAMES_KEPT = 8;
/** While the clock is idle and frames are held, poll again after this long: the clock may have anchored meanwhile. */
const IDLE_RECHECK_US = 50_000;
/** At most this many frames are held, about 20 s of audio; beyond it everything held is dropped as a gap. */
const MAX_HELD_FRAMES = 1024;

/** Create a frame sequencer for one track. */
export function createFrameSequencer(options: FrameSequencerOptions): FrameSequencer {
  const { clock } = options;
  const keyframeGrouping = (options.grouping ?? 'keyframe') === 'keyframe';
  let trackProperties: readonly KeyValuePair[] = options.trackProperties ?? [];
  const stats: FrameSequencerStats = { delivered: 0, late: 0, skippedGroups: 0, resyncs: 0, untimed: 0 };

  // Groups above `floor` that have not been delivered or abandoned.
  const groups = new Map<number, Group>();
  // Groups at or below `floor` are behind what was delivered or abandoned; `undefined` until the first.
  let floor: number | undefined;
  // The group being delivered and its next object, under keyframe grouping.
  let cursor: { groupId: number; nextObjectId: number } | undefined;
  let lastDeliveredUs: number | undefined;
  // Under frame grouping, the group of the last delivered frame.
  let lastDeliveredGroup: number | undefined;
  let deliveredSinceRestart = false;
  let pending: FrameDiscontinuity | undefined;
  // Starving: something arrived but none of it was within the budget, since this position, and late by about this much.
  let starve: { sinceUs: number; latenessUs: number } | undefined;
  // The newest group (keyframe grouping) or frame (frame grouping) dropped as late: what a resync delivers.
  let newestLate: { groupId: number; group: Group } | undefined;
  const keyframes = new Map<number, number>();
  // The most frames one poll may return.
  let limit = Number.POSITIVE_INFINITY;
  let sinceHeldCheck = 0;

  const frameOf = (object: MoqtObject, timestampUs: number): SequencedFrame => ({
    groupId: object.groupId,
    objectId: object.objectId,
    timestampUs,
    key: object.objectId === 0,
    payload: object.payload,
    properties: object.properties,
  });

  const groupOf = (groupId: number) => {
    let group = groups.get(groupId);
    if (!group) {
      group = { frames: new Map(), ended: false, reset: false };
      groups.set(groupId, group);
    }
    return group;
  };

  const noteKeyframe = (groupId: number, timestampUs: number) => {
    if (keyframes.has(groupId)) return;
    keyframes.set(groupId, timestampUs);
    const before = keyframes.get(groupId - 1);
    if (before !== undefined && timestampUs > before) clock.reportKeyframeInterval(timestampUs - before);
    const after = keyframes.get(groupId + 1);
    if (after !== undefined && after > timestampUs) clock.reportKeyframeInterval(after - timestampUs);
    if (keyframes.size > KEYFRAMES_KEPT) keyframes.delete(Math.min(...keyframes.keys()));
  };

  const behind = (groupId: number, objectId: number) => {
    if (floor !== undefined && groupId <= floor) return true;
    if (cursor === undefined) return false;
    return groupId < cursor.groupId || (groupId === cursor.groupId && objectId < cursor.nextObjectId);
  };

  // A late frame. Starving restarts whenever the lateness moves by more than a quarter of the target latency: a stall
  // (lateness growing) or a backlog draining (shrinking) is not a lasting change in delay, and a resync on it would
  // anchor the clock on frames that are about to be caught up with.
  const noteLate = (positionUs: number, latenessUs: number) => {
    if (starve === undefined || Math.abs(latenessUs - starve.latenessUs) > clock.targetLatencyUs / 4) {
      starve = { sinceUs: positionUs, latenessUs };
    }
  };

  // Mark a group as not delivered whole, and everything up to it as behind.
  const passOver = (groupId: number) => {
    const group = groups.get(groupId);
    groups.delete(groupId);
    for (const id of groups.keys()) {
      if (id < groupId) {
        groups.delete(id);
        stats.skippedGroups++;
      }
    }
    floor = Math.max(floor ?? groupId, groupId);
    if (cursor?.groupId === groupId) cursor = undefined;
    stats.skippedGroups++;
    if (deliveredSinceRestart) pending ??= 'gap';
    return group;
  };

  const lateGroup = (groupId: number, positionUs: number, timestampUs: number) => {
    const group = passOver(groupId);
    stats.late += group?.frames.size ?? 0;
    // Only a group that still has its keyframe can be resynced on.
    if (group?.frames.has(0) && (newestLate === undefined || groupId > newestLate.groupId)) {
      newestLate = { groupId, group };
    }
    noteLate(positionUs, positionUs - timestampUs);
    // Its later objects already held count too: a draining backlog's shrinking lateness shows in them.
    for (const objectId of [...(group?.frames.keys() ?? [])].sort((a, b) => a - b)) {
      const frame = group?.frames.get(objectId) as SequencedFrame;
      if (frame.timestampUs > timestampUs) noteLate(positionUs, positionUs - frame.timestampUs);
    }
  };

  const deliver = (frame: SequencedFrame, out: SequencedFrame[]) => {
    const discontinuity = pending;
    pending = undefined;
    out.push(discontinuity ? { ...frame, discontinuity } : frame);
    lastDeliveredUs = frame.timestampUs;
    deliveredSinceRestart = true;
    starve = undefined;
    newestLate = undefined;
    stats.delivered++;
  };

  const lowestKeyed = (above: number | undefined) => {
    let lowest: number | undefined;
    for (const [id, group] of groups) {
      if ((above === undefined || id > above) && group.frames.has(0) && (lowest === undefined || id < lowest)) {
        lowest = id;
      }
    }
    return lowest;
  };

  // Keyframe grouping: deliver what is in order, wait for what may still come, skip what is too late.
  const pollKeyframes = (out: SequencedFrame[]): number | undefined => {
    const positionUs = clock.position();
    const target = clock.targetLatencyUs;
    const lateBefore = positionUs === undefined ? Number.NEGATIVE_INFINITY : positionUs - target;
    // How long newer frames wait on a missing one: it is later than the last delivered frame, so it may still come
    // until that frame's timestamp plus the target latency. While the clock is idle nothing is late, and the wait
    // ends with the missing frame's group.
    const gapWait = () => {
      if (positionUs === undefined) return undefined;
      return lastDeliveredUs === undefined ? 0 : lastDeliveredUs + target - positionUs;
    };
    for (;;) {
      if (out.length >= limit) return 0;
      // A group that ended or was reset without its keyframe can never be delivered; only the next group's says a gap.
      const next = floor === undefined ? undefined : floor + 1;
      for (const [id, group] of groups) {
        if (id !== next && id !== cursor?.groupId && (group.ended || group.reset) && !group.frames.has(0)) {
          groups.delete(id);
          stats.skippedGroups++;
        }
      }
      if (cursor) {
        const group = groups.get(cursor.groupId) as Group;
        const frame = group.frames.get(cursor.nextObjectId);
        if (frame) {
          if (frame.timestampUs < lateBefore) {
            lateGroup(cursor.groupId, positionUs as number, frame.timestampUs);
            continue;
          }
          group.frames.delete(cursor.nextObjectId);
          cursor.nextObjectId++;
          deliver(frame, out);
          continue;
        }
        if (group.ended && group.frames.size === 0) {
          // Delivered whole: the next group follows on.
          groups.delete(cursor.groupId);
          floor = cursor.groupId;
          cursor = undefined;
          continue;
        }
        if (!group.ended && !group.reset) {
          // The group is open and its next frame missing, which costs nothing until a newer group is waiting.
          if (lowestKeyed(cursor.groupId) === undefined) return undefined;
          const wait = gapWait();
          if (wait === undefined || wait > 0) return wait;
        }
        // Reset, ended with a hole, or waited out.
        passOver(cursor.groupId);
        continue;
      }

      const started = next === undefined ? undefined : groups.get(next);
      if (next !== undefined && started && !started.frames.has(0)) {
        // The next group started without its keyframe, which may still come while its stream is open.
        if (!started.ended && !started.reset) {
          if (lowestKeyed(next) === undefined) return undefined;
          const wait = gapWait();
          if (wait === undefined || wait > 0) return wait;
        }
        passOver(next);
        continue;
      }

      // The next group if it has its keyframe; otherwise, or before anything was delivered, the lowest that does.
      const groupId = started ? (next as number) : lowestKeyed(floor);
      if (groupId === undefined) return undefined;
      const keyframe = (groups.get(groupId) as Group).frames.get(0) as SequencedFrame;
      if (keyframe.timestampUs < lateBefore) {
        lateGroup(groupId, positionUs as number, keyframe.timestampUs);
        continue;
      }
      if (positionUs !== undefined && keyframe.timestampUs - positionUs > target) {
        // The clock is behind what arrives, anchored on a backlog: resync forward on this keyframe, in a poll of its own
        // so the frames before it are handled on the clock they were judged on.
        if (out.length > 0) return 0;
        resyncForward();
        return pollKeyframes(out);
      }
      if (next !== undefined && groupId > next) {
        // An older group may still be on its way, or may never be sent (a failed group's ID is not reused). Wait only
        // while this keyframe is not yet due: until then, holding it costs no latency.
        if (positionUs !== undefined && keyframe.timestampUs > positionUs) return keyframe.timestampUs - positionUs;
        if (deliveredSinceRestart) pending ??= 'gap';
      }
      for (const id of [...groups.keys()]) {
        if (id < groupId) {
          groups.delete(id);
          stats.skippedGroups++;
        }
      }
      cursor = { groupId, nextObjectId: 0 };
    }
  };

  // Frame grouping: every frame stands alone; deliver in order at once, and drop what is late or behind.
  const pollFrames = (out: SequencedFrame[]): number | undefined => {
    let positionUs = clock.position();
    let lateBefore = positionUs === undefined ? Number.NEGATIVE_INFINITY : positionUs - clock.lateDropUs;
    for (const id of [...groups.keys()].sort((a, b) => a - b)) {
      const group = groups.get(id) as Group;
      for (const objectId of [...group.frames.keys()].sort((a, b) => a - b)) {
        if (out.length >= limit) return 0;
        const frame = group.frames.get(objectId) as SequencedFrame;
        if (positionUs !== undefined && frame.timestampUs - positionUs > clock.targetLatencyUs) {
          // The clock is behind what arrives, anchored on a backlog: resync forward on this frame, in a poll of its
          // own so the frames before it are handled on the clock they were judged on.
          if (out.length > 0) return 0;
          resyncForward();
          positionUs = undefined;
          lateBefore = Number.NEGATIVE_INFINITY;
        }
        group.frames.delete(objectId);
        floor = id;
        if (positionUs !== undefined && frame.timestampUs < lateBefore) {
          stats.late++;
          newestLate = { groupId: id, group: { frames: new Map([[objectId, frame]]), ended: true, reset: false } };
          noteLate(positionUs, positionUs - frame.timestampUs);
          if (deliveredSinceRestart) pending ??= 'gap';
          continue;
        }
        // A group ID skipped since the last delivered frame is a frame that never came, or was reset.
        if (deliveredSinceRestart && lastDeliveredGroup !== undefined && id > lastDeliveredGroup + 1) pending ??= 'gap';
        lastDeliveredGroup = id;
        deliver(frame, out);
      }
      groups.delete(id);
    }
    return undefined;
  };

  // Bound what is held when nobody polls: past the bound, everything held is dropped and the next frame is a gap.
  const boundHeld = () => {
    if (++sinceHeldCheck < 64) return;
    sinceHeldCheck = 0;
    const candidate = newestLate?.group.frames.size ?? 0;
    let held = candidate;
    let highest = newestLate?.groupId;
    for (const [id, group] of groups) {
      held += group.frames.size;
      highest = highest === undefined ? id : Math.max(highest, id);
    }
    if ((held <= MAX_HELD_FRAMES && groups.size <= MAX_HELD_FRAMES) || highest === undefined) return;
    // The resync candidate's frames were counted as late when they arrived.
    stats.late += held - candidate;
    stats.skippedGroups += groups.size;
    groups.clear();
    newestLate = undefined;
    starve = undefined;
    cursor = undefined;
    floor = Math.max(floor ?? highest, highest);
    if (deliveredSinceRestart) pending ??= 'gap';
  };

  // A frame more than the target latency ahead of the position: the clock was anchored on a backlog (a resync during a
  // long stall), and holding everything that far behind would make the backlog permanent.
  const resyncForward = () => {
    clock.reset();
    stats.resyncs++;
    pending = pending === 'restart' ? 'restart' : 'resync';
    starve = undefined;
    newestLate = undefined;
  };

  // Nothing within the budget for a whole target latency: deliver the newest late group or frame, and reset the
  // clock so it re-anchors there. A lasting increase in delay must not keep the player black or silent.
  const resyncWait = (out: SequencedFrame[]): number | undefined => {
    if (starve === undefined || !newestLate || out.length >= limit) return undefined;
    const positionUs = clock.position();
    if (positionUs === undefined) return undefined;
    const wait = starve.sinceUs + clock.targetLatencyUs - positionUs;
    if (wait > 0) return wait;
    const { groupId, group } = newestLate;
    newestLate = undefined;
    starve = undefined;
    clock.reset();
    stats.resyncs++;
    // Counted as late when dropped; it is delivered after all.
    stats.late -= group.frames.size;
    for (const id of [...groups.keys()]) {
      if (id < groupId) groups.delete(id);
    }
    groups.set(groupId, group);
    floor = groupId - 1;
    cursor = undefined;
    pending = pending === 'restart' ? 'restart' : 'resync';
    if (!keyframeGrouping) return pollFrames(out);
    // From the resynced keyframe on; the clock is idle, so nothing is late until it anchors again.
    cursor = { groupId, nextObjectId: 0 };
    return pollKeyframes(out);
  };

  // A group's stream is over. One with nothing in it is kept only when it is the next group, where it says a gap;
  // anything else would only accumulate.
  const groupOver = (groupId: number, how: 'ended' | 'reset') => {
    if (newestLate?.groupId === groupId) {
      newestLate.group[how] = true;
      return;
    }
    if (floor !== undefined && groupId <= floor) return;
    const group = groups.get(groupId);
    if (group) group[how] = true;
    else if (floor !== undefined && groupId === floor + 1) groupOf(groupId)[how] = true;
    boundHeld();
  };
  const groupEnded = (groupId: number) => groupOver(groupId, 'ended');
  const groupReset = (groupId: number) => groupOver(groupId, 'reset');

  const minWait = (a: number | undefined, b: number | undefined) =>
    a === undefined ? b : b === undefined ? a : Math.min(a, b);

  return {
    get trackProperties() {
      return trackProperties;
    },
    set trackProperties(properties) {
      trackProperties = properties;
    },
    get stats() {
      return stats;
    },
    object(object) {
      if (object.status === 'end-of-group') {
        groupEnded(object.groupId);
        return;
      }
      if (object.status !== 'normal') return;
      const timestampUs = locTimestampUs(object.properties, { trackProperties });
      if (timestampUs === undefined) {
        stats.untimed++;
        return;
      }
      if (keyframeGrouping && newestLate?.groupId === object.groupId) {
        // The resync candidate keeps collecting its group: a resync delivers from its keyframe on.
        if (!newestLate.group.frames.has(object.objectId)) {
          newestLate.group.frames.set(object.objectId, frameOf(object, timestampUs));
          stats.late++;
          const positionUs = clock.position();
          if (positionUs !== undefined) noteLate(positionUs, positionUs - timestampUs);
          boundHeld();
        }
        return;
      }
      if (behind(object.groupId, object.objectId)) {
        stats.late++;
        return;
      }
      const group = groupOf(object.groupId);
      if (group.frames.has(object.objectId)) return;
      // Only a keyframe that can still be delivered reports an interval: an old duplicate must not move the budget.
      if (keyframeGrouping && object.objectId === 0) noteKeyframe(object.groupId, timestampUs);
      group.frames.set(object.objectId, frameOf(object, timestampUs));
      boundHeld();
    },
    groupEnded,
    groupReset,
    restart() {
      // The new subscription's timeline is not judged against the old one's anchor.
      clock.reset();
      groups.clear();
      keyframes.clear();
      floor = undefined;
      cursor = undefined;
      lastDeliveredUs = undefined;
      lastDeliveredGroup = undefined;
      starve = undefined;
      newestLate = undefined;
      // Always marked: the clock was reset, and whatever else plays on it must hear of that.
      pending = 'restart';
      deliveredSinceRestart = false;
    },
    poll(maxFrames) {
      if (maxFrames !== undefined && !(Number.isSafeInteger(maxFrames) && maxFrames > 0)) {
        throw new RangeError('maxFrames must be a positive integer');
      }
      limit = maxFrames ?? Number.POSITIVE_INFINITY;
      const frames: SequencedFrame[] = [];
      let waitUs: number | undefined;
      waitUs = keyframeGrouping ? pollKeyframes(frames) : pollFrames(frames);
      waitUs = minWait(waitUs, resyncWait(frames));
      // Held frames wait on deadlines that only exist once the clock has a position.
      if (waitUs === undefined && clock.position() === undefined && (groups.size > 0 || newestLate)) {
        waitUs = IDLE_RECHECK_US;
      }
      return { frames, waitUs: waitUs === undefined ? undefined : Math.max(0, Math.ceil(waitUs)) };
    },
  };
}
