/**
 * The frame sequencer: turns one track's objects, which arrive in any order across group streams, into frames in
 * decode order, and drops what is too late against a playout clock. It is the engine of a frame reader; it has no
 * timers and no transport. See spec/behavior.md, "Media subscribing".
 */
import type { KeyValuePair, MoqtObject } from '@caprid/moq-core';
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
  /**
   * The groups between the last frame delivered and this one of which nothing was delivered: never received, reset, or
   * dropped as late or behind. Inclusive bounds. Absent when no group ID was skipped, and on the first frame after a
   * restart, since group IDs start over there.
   */
  missingGroups?: MissingGroups;
}

/** An inclusive range of group IDs of which nothing was delivered: what an application may act on, such as a FETCH. */
export interface MissingGroups {
  /** The first group ID missing. */
  firstGroupId: number;
  /** The last group ID missing. */
  lastGroupId: number;
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
  /**
   * Frame grouping only: how far behind the clock's position a frame may still be delivered, in microseconds. Default:
   * the clock's `lateDropUs`, which every track on the clock shares. A listener set to drop late audio sets it near
   * its playout target; a reader that wants every frame, such as speech-to-text, sets `Infinity`. Not negative.
   */
  lateDropUs?: number;
  /**
   * Frame grouping only: how long, in microseconds of the clock's local time, a frame waits for an earlier group that
   * has not arrived, counted from when the frame arrived. Default 0: frames go out at once and an earlier group that
   * arrives after a later one is dropped as behind. A frame that turns late while it waits is dropped as late. For a
   * reader that wants every frame of a talker, set it to at least the publisher's delivery timeout: a group the
   * publisher's transport is still resending can arrive that long after the group that follows it. Finite, not
   * negative.
   */
  reorderWindowUs?: number;
  /**
   * Signal a catch-up when frames keep arriving late with none delivered: from the first frame dropped as late to the
   * latest is at least this long, in microseconds of the clock's local time. Late means past the late-drop limit
   * (frame grouping) or the target latency (keyframe grouping), as when a relay holds a growing backlog for this
   * subscriber. The poll after that frame returns `catchUp: true`, once, until a frame is delivered again; whoever owns
   * the subscription then replaces it at the live edge (a supervised subscriber's `catchUp(key)`), which drops the
   * relay's backlog, and restarts the sequencer. Set it below the clock's target latency, or a resync re-anchors on
   * the late frames first. Default: never. Positive and finite.
   */
  catchUpAfterUs?: number;
  /**
   * Frame grouping only: hold frames that arrive early and release each at its timestamp, for a talker that sends
   * faster than real time, such as a text-to-speech agent handing over a reply in bursts. Without it, a frame more than
   * the clock's target latency ahead of the position makes the sequencer resync forward, as after a stall, which drops
   * what the player holds and re-anchors on the newest frame. With it, a frame is held until its timestamp is within
   * `leadUs` of the position, and only a frame more than `horizonUs` ahead resyncs forward. While the clock has no
   * position, the first frame anchors one of the sequencer's own on the clock's local time, which gives way to the
   * clock's once the player reports. Late drop, resyncs and catch-up are unchanged, so a live talker's frames, which
   * are never early, play as without it. `true` takes the defaults of {@link TimestampPlayoutOptions}. Default off.
   */
  timestampPlayout?: boolean | TimestampPlayoutOptions;
}

/** Options for {@link FrameSequencerOptions.timestampPlayout}. Durations are microseconds. */
export interface TimestampPlayoutOptions {
  /**
   * The furthest ahead of the position a frame is held; a frame further ahead resyncs forward, as a timeline jump.
   * Positive and finite. Default 30,000,000.
   */
  horizonUs?: number;
  /**
   * How far ahead of the position a held frame is released: what the player wants to hold beyond what is audible. For
   * the audio player, pass `() => input.playoutLeadUs`, its output delay plus its jitter target: a lead longer than
   * that leaves more audio held than the jitter buffer's target, and the buffer trims the excess every second. A
   * function is read at every poll; a value it returns that is not a finite number from 0 up counts as the default.
   * Not negative. Default 100,000: the audio player's default 60 ms jitter target and a 40 ms output delay.
   */
  leadUs?: number | (() => number);
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
  /** Present, and `true`, on the one poll that signals a catch-up: see {@link FrameSequencerOptions.catchUpAfterUs}. */
  catchUp?: true;
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
  /** Frames held because they arrived ahead of their timestamp: see {@link FrameSequencerOptions.timestampPlayout}. */
  early: number;
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
  /** When its first frame arrived, on the clock's local time: where a reorder window's wait starts. */
  arrivedUs?: number;
}

/** How many keyframe timestamps are kept to measure intervals between consecutive groups. */
const KEYFRAMES_KEPT = 8;
/** While the clock is idle and frames are held, poll again after this long: the clock may have anchored meanwhile. */
const IDLE_RECHECK_US = 50_000;
/** At most this many frames are held, about 20 s of audio; beyond it everything held is dropped as a gap. */
const MAX_HELD_FRAMES = 1024;
/** Under timestamp playout, the held bound covers the horizon at this frame duration, the shortest Opus frame. */
const SHORTEST_FRAME_US = 2_500;
/** Timestamp playout's defaults. */
const DEFAULT_HORIZON_US = 30_000_000;
const DEFAULT_LEAD_US = 100_000;
/** Under timestamp playout, added delay up to this much is noise in the clock's reports, not given back. */
const ADDED_DELAY_TOLERANCE_US = 40_000;

/** Create a frame sequencer for one track. */
export function createFrameSequencer(options: FrameSequencerOptions): FrameSequencer {
  const { clock } = options;
  const keyframeGrouping = (options.grouping ?? 'keyframe') === 'keyframe';
  let trackProperties: readonly KeyValuePair[] = options.trackProperties ?? [];
  const lateDropOption = optionalDuration(options.lateDropUs, 'lateDropUs', { infinite: true });
  const reorderWindowUs = optionalDuration(options.reorderWindowUs, 'reorderWindowUs') ?? 0;
  const catchUpAfterUs = optionalDuration(options.catchUpAfterUs, 'catchUpAfterUs');
  if (catchUpAfterUs === 0) throw new RangeError('catchUpAfterUs must be positive');
  if (keyframeGrouping && (lateDropOption !== undefined || reorderWindowUs > 0)) {
    throw new RangeError('lateDropUs and reorderWindowUs apply to frame grouping only');
  }
  const lateDropUs = () => lateDropOption ?? clock.lateDropUs;
  const playout = timestampPlayout(options.timestampPlayout);
  if (keyframeGrouping && playout) throw new RangeError('timestampPlayout applies to frame grouping only');
  // How far ahead of the position a frame may arrive before the sequencer resyncs forward.
  const aheadUs = () => (playout ? playout.horizonUs : clock.targetLatencyUs);
  const maxHeld = playout
    ? Math.max(MAX_HELD_FRAMES, Math.ceil(playout.horizonUs / SHORTEST_FRAME_US))
    : MAX_HELD_FRAMES;
  // Under timestamp playout while the clock has no position: the sequencer's own, the first frame's timestamp at the
  // local time it was polled, until the clock has one or is reset.
  let ownAnchor: { mediaUs: number; atUs: number; resets: number } | undefined;
  // Frames already counted as early.
  const counted = new WeakSet<SequencedFrame>();
  // The newest timestamp a frame may have to be delivered now, under timestamp playout; anchors the sequencer's own
  // position on `frame` while the clock has none.
  // Under timestamp playout, the least delay seen from the clock's position to local time beyond the lead, since the
  // clock was last reset: what the player's delay was before underruns added to it.
  let leastDelay: { delayUs: number; resets: number } | undefined;
  const releaseBefore = (frame: SequencedFrame) => {
    const lead = (playout as { leadUs: () => number }).leadUs();
    const positionUs = clock.position();
    if (positionUs !== undefined) {
      ownAnchor = undefined;
      // Each underrun leaves the clock further behind local time, and a jitter buffer holding only its target never
      // trims that back. So release frames early by the delay added since the least seen (less a frame of noise): the
      // buffer then holds the excess and its trim gives it back, as it does for frames that arrive late. A talker whose
      // frames were early all along adds nothing, so its audio still plays at its timestamps.
      const delayUs = clock.localNow() - positionUs - lead;
      if (leastDelay === undefined || leastDelay.resets !== clock.resets || delayUs < leastDelay.delayUs) {
        leastDelay = { delayUs, resets: clock.resets };
      }
      return positionUs + lead + Math.max(0, delayUs - leastDelay.delayUs - ADDED_DELAY_TOLERANCE_US);
    }
    if (ownAnchor === undefined || ownAnchor.resets !== clock.resets) {
      ownAnchor = { mediaUs: frame.timestampUs, atUs: clock.localNow(), resets: clock.resets };
    }
    return ownAnchor.mediaUs + (clock.localNow() - ownAnchor.atUs) + lead;
  };
  const stats: FrameSequencerStats = { delivered: 0, late: 0, skippedGroups: 0, resyncs: 0, untimed: 0, early: 0 };

  // Groups above `floor` that have not been delivered or abandoned.
  const groups = new Map<number, Group>();
  // Groups at or below `floor` are behind what was delivered or abandoned; `undefined` until the first.
  let floor: number | undefined;
  // The group being delivered and its next object, under keyframe grouping.
  let cursor: { groupId: number; nextObjectId: number } | undefined;
  let lastDeliveredUs: number | undefined;
  // Under frame grouping, the group and object of the last delivered frame.
  let lastDeliveredGroup: number | undefined;
  let lastDeliveredObject = 0;
  let deliveredSinceRestart = false;
  let pending: FrameDiscontinuity | undefined;
  // Starving: something arrived but none of it was within the budget, since this position, and late by about this much.
  // The wait is measured on the clock's local time, which never jumps; the position can, when the clock re-anchors.
  let starve: { sinceUs: number; sinceLocalUs: number; latenessUs: number; resets: number } | undefined;
  // The newest group (keyframe grouping) or frame (frame grouping) dropped as late: what a resync delivers.
  let newestLate: { groupId: number; group: Group } | undefined;
  // Since when, on the clock's local time, every frame that arrived was dropped as late with none delivered, and when
  // the latest of them was.
  let lateSinceUs: number | undefined;
  let lateLatestUs = 0;
  // A catch-up was signalled and no frame has been delivered since: it is not signalled again until one is.
  let caughtUp = false;
  // The group of the last frame delivered, under either grouping, for `missingGroups`; reset with the group IDs.
  let lastGroupOut: number | undefined;
  // Under a reorder window, the highest group an end or reset with nothing in it said was over. It moves no floor: a
  // multi-frame group taken before it may still bring later objects.
  let emptyThrough: number | undefined;
  // Under a reorder window, the highest group taken, dropped or known to be over: the next one is due, and any further
  // one waits for the groups between. Frame grouping keeps the group being taken in `cursor` and `floor` one below it.
  const settledThrough = (): number | undefined => {
    const candidates = [floor, cursor?.groupId, floor === undefined && cursor === undefined ? undefined : emptyThrough];
    const known = candidates.filter((id): id is number => id !== undefined);
    return known.length === 0 ? undefined : Math.max(...known);
  };
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
      starve = { sinceUs: positionUs, sinceLocalUs: clock.localNow(), latenessUs, resets: clock.resets };
    }
  };

  // A frame judged late as it is taken, for catch-up. Not when a held candidate is judged again on a changed clock: a
  // catch-up follows late arrivals, not a re-anchor.
  const lateArrived = () => {
    lateLatestUs = clock.localNow();
    lateSinceUs ??= lateLatestUs;
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
    lateArrived();
    // Its later objects already held count too: a draining backlog's shrinking lateness shows in them.
    for (const objectId of [...(group?.frames.keys() ?? [])].sort((a, b) => a - b)) {
      const frame = group?.frames.get(objectId) as SequencedFrame;
      if (frame.timestampUs > timestampUs) noteLate(positionUs, positionUs - frame.timestampUs);
    }
  };

  const deliver = (frame: SequencedFrame, out: SequencedFrame[]) => {
    const discontinuity = pending;
    pending = undefined;
    const missing =
      lastGroupOut !== undefined && frame.groupId > lastGroupOut + 1
        ? { firstGroupId: lastGroupOut + 1, lastGroupId: frame.groupId - 1 }
        : undefined;
    lastGroupOut = frame.groupId;
    out.push(
      discontinuity || missing
        ? { ...frame, ...(discontinuity ? { discontinuity } : {}), ...(missing ? { missingGroups: missing } : {}) }
        : frame
    );
    lateSinceUs = undefined;
    caughtUp = false;
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

  // Frame grouping: every frame stands alone; deliver in order at once, and drop what is late or behind. A group may
  // hold several frames, one per object, as a subgroup writer sends them: the position taken is a (group, object) pair,
  // so the group's later objects are still ahead of it while its earlier ones are behind.
  const pollFrames = (out: SequencedFrame[]): number | undefined => {
    let positionUs = clock.position();
    let lateBefore = positionUs === undefined ? Number.NEGATIVE_INFINITY : positionUs - lateDropUs();
    for (const id of [...groups.keys()].sort((a, b) => a - b)) {
      const group = groups.get(id) as Group;
      if (reorderWindowUs > 0) {
        // A group that ended or was reset with nothing in it brings nothing: the next one need not wait for it. One
        // further on, or one seen before anything was delivered (so before there is a floor), is kept until the
        // groups before it are settled.
        const through = settledThrough();
        if (group.frames.size === 0 && (group.ended || group.reset)) {
          if (through !== undefined && id <= through + 1) {
            groups.delete(id);
            emptyThrough = Math.max(emptyThrough ?? id, id);
          }
          continue;
        }
        // An earlier group is missing: this one, and every newer one, waits for it until the window from its arrival
        // ends.
        if (through !== undefined && id > through + 1 && group.arrivedUs !== undefined) {
          const waitUs = group.arrivedUs + reorderWindowUs - clock.localNow();
          if (waitUs > 0) return waitUs;
        }
      }
      for (const objectId of [...group.frames.keys()].sort((a, b) => a - b)) {
        if (out.length >= limit) return 0;
        const frame = group.frames.get(objectId) as SequencedFrame;
        if (positionUs !== undefined && frame.timestampUs - positionUs > aheadUs()) {
          // The clock is behind what arrives, anchored on a backlog: resync forward on this frame, in a poll of its
          // own so the frames before it are handled on the clock they were judged on.
          if (out.length > 0) return 0;
          resyncForward();
          positionUs = undefined;
          lateBefore = Number.NEGATIVE_INFINITY;
        }
        if (playout) {
          // Early: hold it, and every frame after it, until its timestamp is within the lead.
          const waitUs = frame.timestampUs - releaseBefore(frame);
          if (waitUs > 0) {
            if (!counted.has(frame)) {
              counted.add(frame);
              stats.early++;
            }
            return waitUs;
          }
        }
        group.frames.delete(objectId);
        floor = id - 1;
        cursor = { groupId: id, nextObjectId: objectId + 1 };
        if (positionUs !== undefined && frame.timestampUs < lateBefore) {
          stats.late++;
          newestLate = { groupId: id, group: { frames: new Map([[objectId, frame]]), ended: true, reset: false } };
          noteLate(positionUs, positionUs - frame.timestampUs);
          lateArrived();
          if (deliveredSinceRestart) pending ??= 'gap';
          continue;
        }
        // A group ID or an object ID skipped since the last delivered frame is a frame that never came, or was reset.
        if (deliveredSinceRestart && lastDeliveredGroup !== undefined) {
          const skipped =
            id === lastDeliveredGroup
              ? objectId > lastDeliveredObject + 1
              : id > lastDeliveredGroup + 1 || objectId > 0;
          if (skipped) pending ??= 'gap';
        }
        lastDeliveredGroup = id;
        lastDeliveredObject = objectId;
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
    if ((held <= maxHeld && groups.size <= maxHeld) || highest === undefined) return;
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
    const wait = starve.sinceLocalUs + clock.targetLatencyUs - clock.localNow();
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
    // Under frame grouping the group's later objects may be held already: keep them beside the candidate.
    const held = groups.get(groupId);
    if (held) for (const [objectId, frame] of group.frames) held.frames.set(objectId, frame);
    else groups.set(groupId, group);
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
    // Under a reorder window, any missing group's end is kept, even before the first delivery: it releases the frames
    // waiting for that group.
    else if (reorderWindowUs > 0 || (floor !== undefined && groupId === floor + 1)) groupOf(groupId)[how] = true;
    boundHeld();
  };
  const groupEnded = (groupId: number) => groupOver(groupId, 'ended');
  const groupReset = (groupId: number) => groupOver(groupId, 'reset');

  // Another track can reset or re-anchor the shared clock. A resync candidate back within the budget then (or with
  // the clock idle, when nothing is late) goes back to normal delivery, and a wait measured on the old timeline starts
  // over.
  const revisit = () => {
    const positionUs = clock.position();
    // The timeline changed: the clock was reset, or re-anchored so that its position no longer moved with local time.
    if (
      starve !== undefined &&
      (positionUs === undefined ||
        starve.resets !== clock.resets ||
        Math.abs(positionUs - starve.sinceUs - (clock.localNow() - starve.sinceLocalUs)) > clock.targetLatencyUs / 4)
    ) {
      starve = undefined;
    }
    if (!newestLate) return;
    const { groupId, group } = newestLate;
    const first = group.frames.get(keyframeGrouping ? 0 : Math.min(...group.frames.keys()));
    if (!first) return;
    const bound = keyframeGrouping ? clock.targetLatencyUs : lateDropUs();
    if (positionUs !== undefined && first.timestampUs < positionUs - bound) {
      // Still late on the new timeline: the wait starts over from here.
      if (starve === undefined) {
        for (const frame of [...group.frames.values()].sort((a, b) => a.objectId - b.objectId)) {
          noteLate(positionUs, positionUs - frame.timestampUs);
        }
      }
      return;
    }
    newestLate = undefined;
    starve = undefined;
    // Counted as late when dropped; it is delivered after all.
    stats.late -= group.frames.size;
    for (const id of [...groups.keys()]) {
      if (id < groupId) groups.delete(id);
    }
    // Under frame grouping the group's later objects may be held already: keep them beside the candidate.
    const held = groups.get(groupId);
    if (held) for (const [objectId, frame] of group.frames) held.frames.set(objectId, frame);
    else groups.set(groupId, group);
    floor = groupId - 1;
    cursor = undefined;
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
      if (keyframeGrouping && newestLate?.groupId === object.groupId) {
        // The resync candidate keeps collecting its group: a resync delivers from its keyframe on.
        if (!newestLate.group.frames.has(object.objectId)) {
          newestLate.group.frames.set(object.objectId, frameOf(object, timestampUs));
          stats.late++;
          const positionUs = clock.position();
          if (positionUs !== undefined) {
            noteLate(positionUs, positionUs - timestampUs);
            lateArrived();
          }
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
      group.arrivedUs ??= clock.localNow();
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
      lastGroupOut = undefined;
      emptyThrough = undefined;
      lateSinceUs = undefined;
      starve = undefined;
      newestLate = undefined;
      // The new subscription's timestamps need not continue the old one's.
      ownAnchor = undefined;
      leastDelay = undefined;
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
      revisit();
      let waitUs: number | undefined;
      waitUs = keyframeGrouping ? pollKeyframes(frames) : pollFrames(frames);
      waitUs = minWait(waitUs, resyncWait(frames));
      let catchUp = false;
      // Judged at the latest late frame: lateness that stopped arriving (a talker who went quiet) is no backlog.
      if (
        catchUpAfterUs !== undefined &&
        lateSinceUs !== undefined &&
        !caughtUp &&
        lateLatestUs - lateSinceUs >= catchUpAfterUs
      ) {
        catchUp = true;
        caughtUp = true;
      }
      // Held frames wait on deadlines that only exist once the clock has a position.
      if (waitUs === undefined && clock.position() === undefined && (groups.size > 0 || newestLate)) {
        waitUs = IDLE_RECHECK_US;
      }
      const result: SequencerPoll = {
        frames,
        waitUs: waitUs === undefined ? undefined : Math.max(0, Math.ceil(waitUs)),
      };
      if (catchUp) result.catchUp = true;
      return result;
    },
  };
}

/** The timestamp playout options with their defaults; `undefined` when off. */
function timestampPlayout(value: boolean | TimestampPlayoutOptions | undefined) {
  if (value === undefined || value === false) return undefined;
  if (value !== true && (typeof value !== 'object' || value === null))
    throw new TypeError('timestampPlayout must be a boolean or an options object');
  const given = value === true ? {} : value;
  const horizonUs = optionalDuration(given.horizonUs, 'timestampPlayout.horizonUs') ?? DEFAULT_HORIZON_US;
  if (horizonUs === 0) throw new RangeError('timestampPlayout.horizonUs must be positive');
  const lead = given.leadUs;
  let leadUs: () => number;
  if (typeof lead === 'function') {
    leadUs = () => {
      const read = lead();
      return typeof read === 'number' && Number.isFinite(read) && read >= 0 ? read : DEFAULT_LEAD_US;
    };
  } else {
    const fixed = optionalDuration(lead, 'timestampPlayout.leadUs') ?? DEFAULT_LEAD_US;
    leadUs = () => fixed;
  }
  return { horizonUs, leadUs };
}

/** An optional duration option in microseconds: not negative, and finite unless `infinite` allows it. */
function optionalDuration(value: number | undefined, name: string, { infinite = false } = {}): number | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== 'number' || Number.isNaN(value)) throw new TypeError(`${name} must be a number`);
  if (!infinite && !Number.isFinite(value)) throw new RangeError(`${name} must be finite`);
  if (value < 0) throw new RangeError(`${name} must not be negative`);
  return value;
}
