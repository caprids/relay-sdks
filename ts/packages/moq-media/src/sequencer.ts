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
  /** Poll again after this many microseconds, when a wait can end; `undefined` when only new objects can change it. */
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
  /** The subscription was replaced (a Restart or a Move): forget everything; group IDs start over. */
  restart(): void;
  /** Take the frames that are ready, and when to poll again. */
  poll(): SequencerPoll;
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
  let deliveredSinceRestart = false;
  let pending: FrameDiscontinuity | undefined;
  // Starving: something arrived but none of it was within the budget, since this position.
  let starvedAtUs: number | undefined;
  // The newest group (keyframe grouping) or frame (frame grouping) dropped as late: what a resync delivers.
  let newestLate: { groupId: number; group: Group } | undefined;
  const keyframes = new Map<number, number>();

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
    return cursor !== undefined && groupId === cursor.groupId && objectId < cursor.nextObjectId;
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

  const lateGroup = (groupId: number, positionUs: number) => {
    const group = passOver(groupId);
    stats.late += group?.frames.size ?? 0;
    // Only a group that still has its keyframe can be resynced on.
    if (group?.frames.has(0) && (newestLate === undefined || groupId > newestLate.groupId)) {
      newestLate = { groupId, group };
    }
    starvedAtUs ??= positionUs;
  };

  const deliver = (frame: SequencedFrame, out: SequencedFrame[]) => {
    const discontinuity = pending;
    pending = undefined;
    out.push(discontinuity ? { ...frame, discontinuity } : frame);
    lastDeliveredUs = frame.timestampUs;
    deliveredSinceRestart = true;
    starvedAtUs = undefined;
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
      if (cursor) {
        const group = groups.get(cursor.groupId) as Group;
        const frame = group.frames.get(cursor.nextObjectId);
        if (frame) {
          if (frame.timestampUs < lateBefore) {
            lateGroup(cursor.groupId, positionUs as number);
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

      const next = floor === undefined ? undefined : floor + 1;
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
        lateGroup(groupId, positionUs as number);
        continue;
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
  const pollFrames = (out: SequencedFrame[]) => {
    const positionUs = clock.position();
    const lateBefore = positionUs === undefined ? Number.NEGATIVE_INFINITY : positionUs - clock.lateDropUs;
    for (const id of [...groups.keys()].sort((a, b) => a - b)) {
      const group = groups.get(id) as Group;
      groups.delete(id);
      if (group.frames.size === 0) continue;
      floor = id;
      for (const objectId of [...group.frames.keys()].sort((a, b) => a - b)) {
        const frame = group.frames.get(objectId) as SequencedFrame;
        if (frame.timestampUs < lateBefore) {
          stats.late++;
          newestLate = { groupId: id, group: { frames: new Map([[objectId, frame]]), ended: true, reset: false } };
          starvedAtUs ??= positionUs;
          if (deliveredSinceRestart) pending ??= 'gap';
          continue;
        }
        deliver(frame, out);
      }
    }
  };

  // Nothing within the budget for a whole target latency: deliver the newest late group or frame, and reset the
  // clock so it re-anchors there. A lasting increase in delay must not keep the player black or silent.
  const resyncWait = (out: SequencedFrame[]): number | undefined => {
    if (starvedAtUs === undefined || !newestLate) return undefined;
    const positionUs = clock.position();
    if (positionUs === undefined) return undefined;
    const wait = starvedAtUs + clock.targetLatencyUs - positionUs;
    if (wait > 0) return wait;
    const { groupId, group } = newestLate;
    newestLate = undefined;
    starvedAtUs = undefined;
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
    pending = 'resync';
    if (!keyframeGrouping) {
      pollFrames(out);
      return undefined;
    }
    // From the resynced keyframe on; the clock is idle, so nothing is late until it anchors again.
    cursor = { groupId, nextObjectId: 0 };
    return pollKeyframes(out);
  };

  const groupEnded = (groupId: number) => {
    if (floor !== undefined && groupId <= floor) return;
    groupOf(groupId).ended = true;
  };

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
      if (keyframeGrouping && object.objectId === 0) noteKeyframe(object.groupId, timestampUs);
      if (behind(object.groupId, object.objectId)) {
        stats.late++;
        return;
      }
      const group = groupOf(object.groupId);
      if (group.frames.has(object.objectId)) return;
      group.frames.set(object.objectId, {
        groupId: object.groupId,
        objectId: object.objectId,
        timestampUs,
        key: object.objectId === 0,
        payload: object.payload,
        properties: object.properties,
      });
    },
    groupEnded,
    groupReset(groupId) {
      if (floor !== undefined && groupId <= floor) return;
      groupOf(groupId).reset = true;
    },
    restart() {
      groups.clear();
      keyframes.clear();
      floor = undefined;
      cursor = undefined;
      lastDeliveredUs = undefined;
      starvedAtUs = undefined;
      newestLate = undefined;
      pending = deliveredSinceRestart || pending !== undefined ? 'restart' : undefined;
      deliveredSinceRestart = false;
    },
    poll() {
      const frames: SequencedFrame[] = [];
      let waitUs: number | undefined;
      if (keyframeGrouping) waitUs = pollKeyframes(frames);
      else pollFrames(frames);
      waitUs = minWait(waitUs, resyncWait(frames));
      return { frames, waitUs: waitUs === undefined ? undefined : Math.max(1, Math.ceil(waitUs)) };
    },
  };
}
