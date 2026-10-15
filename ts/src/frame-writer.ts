import { RelayError } from './errors.js';
import type { PublishProperty, RelayGroupWriter, RelayTrackWriter } from './publish.js';

/**
 * How a {@link FrameWriter} maps frames onto groups. `'keyframe'` is the LOC mapping for video: a keyframe FINs the
 * open group and opens a new one, deltas append to it. `'frame'` sends every frame as its own single-object group, the
 * mapping for audio and any other independently decodable frames.
 */
export type FrameGrouping = 'keyframe' | 'frame';

/**
 * One encoded frame as the frame writer sees it. Any object with these fields qualifies, `@mux/moq-media`'s `LocFrame`
 * among them, so nothing is converted between the encoder and the writer.
 */
export interface Frame {
  /** The object payload. Keep it unchanged until the frame's write settles. */
  readonly payload: ArrayBufferView;
  /** A random-access point: under keyframe grouping it starts a new group. */
  readonly key: boolean;
  /** The object properties sent with the frame, already built (for LOC media, `locProperties`). */
  readonly properties?: readonly PublishProperty[];
  /** Frames before this one were dropped, so an open group can no longer be completed and is reset. */
  readonly discontinuity?: boolean;
}

/**
 * What became of a frame: `'sent'`, or one of the ways it was not. `'skipped'`: the relay had no subscription for it,
 * or it was a delta with no group to join. `'stale'`: the `stale` predicate rejected it or a frame it depended on.
 * `'dropped'`: the backlog overflowed, or the writer closed with it waiting. `'interrupted'`: its group failed and the
 * track stays open. `'ended'`: the track ended (a retirement).
 */
export type FrameOutcomeKind = 'sent' | 'skipped' | 'stale' | 'dropped' | 'interrupted' | 'ended';

/** What became of one frame handed to {@link FrameWriter.write}. */
export interface FrameOutcome {
  /** What happened to the frame. */
  readonly kind: FrameOutcomeKind;
  /** The failure behind `'interrupted'` or `'ended'`. */
  readonly error?: RelayError;
  /** The group the frame was written to, or the one opened or reset on its behalf. */
  readonly groupId?: number;
}

/** Options for {@link createFrameWriter}. */
export interface FrameWriterOptions<F extends Frame = Frame> {
  /** Default `'keyframe'`. */
  grouping?: FrameGrouping;
  /**
   * Whether a frame is too late to send, against the application's own clock. Consulted before a group opens and again
   * before each write, since time passes inside those steps.
   */
  stale?: (frame: F) => boolean;
  /**
   * How many frames may wait behind the one being written before the writer drops. A positive integer; default 8
   * under keyframe grouping, 25 under frame grouping.
   */
  maxBacklog?: number;
  /**
   * Called synchronously each time the writer comes to need a keyframe after its start (see
   * {@link FrameWriter.keyframeNeeded}), so an encoder can force one. An exception from it goes to `console.error`.
   */
  onKeyframeNeeded?: () => void;
}

/**
 * Maps encoded frames onto a registered track's groups, one frame at a time. Create one with
 * {@link createFrameWriter}.
 */
export interface FrameWriter<F extends Frame = Frame> {
  /**
   * Send one frame if the relay wants it, and say what happened. May be called without awaiting the previous call:
   * frames are served in call order, and those waiting are the backlog. Resolves when the transport accepted the
   * object, or as soon as the frame is known not to be sent. Rejects with `CLOSED` after {@link close}; a track that
   * ended on its own is an `'ended'` outcome instead, because the encoder keeps producing.
   */
  write(frame: F): Promise<FrameOutcome>;
  /**
   * Drop the backlog, FIN the open group once the frame being written settles, and refuse further frames. Idempotent.
   * A `RelayError` from that FIN is swallowed: the group was abandoned either way. The registered track stays the
   * application's to close.
   */
  close(): Promise<void>;
  /** True once {@link close} ran or the track ended under the writer. */
  readonly closed: boolean;
  /** The failure that ended the track, once a write has seen it; `undefined` while the track is open. */
  readonly ended: RelayError | undefined;
  /** Groups opened so far. */
  readonly groups: number;
  /** Objects the transport accepted so far. */
  readonly objects: number;
  /** Frames waiting behind the one being written. */
  readonly backlog: number;
  /**
   * Whether the next frame the writer can send must be a keyframe: from the start until a keyframe arrives, and again
   * after a relay demand change, an interruption, a stale or discontinuous frame, or a drop. Always false under frame
   * grouping. Check it before each `encode()`.
   */
  readonly keyframeNeeded: boolean;
}

interface Pending<F> {
  frame: F;
  resolve(outcome: FrameOutcome): void;
  reject(error: unknown): void;
}

const DEFAULT_BACKLOG: Record<FrameGrouping, number> = { keyframe: 8, frame: 25 };

/** A failure ends the track exactly when the SDK says so: `CLOSED`, or the track is closed afterwards. */
const retired = (track: RelayTrackWriter, error: RelayError) => error.code === 'CLOSED' || track.closed;
const never = () => false;

/**
 * Map encoded frames onto a registered track's groups (spec/behavior.md, "Media publishing").
 *
 * A frame the relay has no subscription for is `'skipped'` at once, because an encoder loop cannot block on the write
 * deadline; publishing resumes at the next keyframe. When the backlog is full the transport has fallen behind:
 * keyframe grouping drops the backlog and the open group, frame grouping drops the oldest frame. Under keyframe grouping
 * the writer watches the track's demand from now until it closes, and says when it needs a keyframe.
 *
 * ```ts
 * const writer = createFrameWriter(track, { onKeyframeNeeded: () => (forceKeyframe = true) });
 * // in the VideoEncoder output callback, without awaiting:
 * void writer.write(chunkToLocFrame(chunk, { videoConfig })).then(count);
 * ```
 */
export function createFrameWriter<F extends Frame = Frame>(
  track: RelayTrackWriter,
  options: FrameWriterOptions<F> = {},
): FrameWriter<F> {
  if (typeof options !== 'object' || options === null) throw new TypeError('options must be an object');
  const grouping = options.grouping ?? 'keyframe';
  if (grouping !== 'keyframe' && grouping !== 'frame') throw new TypeError("grouping must be 'keyframe' or 'frame'");
  const maxBacklog = options.maxBacklog ?? DEFAULT_BACKLOG[grouping];
  if (!Number.isSafeInteger(maxBacklog) || maxBacklog < 1)
    throw new RangeError('maxBacklog must be a positive integer');
  if (options.stale !== undefined && typeof options.stale !== 'function')
    throw new TypeError('stale must be a function');
  if (options.onKeyframeNeeded !== undefined && typeof options.onKeyframeNeeded !== 'function') {
    throw new TypeError('onKeyframeNeeded must be a function');
  }
  const stale: (frame: F) => boolean = options.stale ?? never;
  const onKeyframeNeeded = options.onKeyframeNeeded;
  const keyframes = grouping === 'keyframe';

  const waiting: Pending<F>[] = [];
  let current: Pending<F> | undefined;
  let group: RelayGroupWriter | undefined;
  /** A keyframe is being served and may still open a group: a demand change meanwhile is recovered from. */
  let keyOpening = false;
  /** The previous group while a keyframe's write FINs it: an overflow resets it too. */
  let finishing: RelayGroupWriter | undefined;
  /** Aborts the group a keyframe is opening with itself: an overflow resets it like an open group. */
  let opening: AbortController | undefined;
  /** Why deltas are discarded until the next keyframe, when it is not a missing group. */
  let recovering: 'stale' | 'dropped' | undefined;
  /** Counts keyframe-grouping overflows, so the frame in progress can tell that its group was dropped under it. */
  let drops = 0;
  let keyframeNeeded = keyframes;
  let closing = false;
  let ended: RelayError | undefined;
  let groups = 0;
  let objects = 0;
  let idle: Promise<void> = Promise.resolve();
  let demand: AsyncIterableIterator<boolean> | undefined;

  const needKeyframe = () => {
    if (!keyframes || keyframeNeeded || closing || ended) return;
    keyframeNeeded = true;
    try {
      onKeyframeNeeded?.();
    } catch (error) {
      // biome-ignore lint/suspicious/noConsole: the application's callback failed and has no caller to report to
      console.error(new Error('onKeyframeNeeded threw; the frame writer continues regardless', { cause: error }));
    }
  };

  /** Abort the open group, if any, and say which one it was. */
  const resetGroup = (): number | undefined => {
    const reset = group ?? finishing;
    group = undefined;
    finishing = undefined;
    opening?.abort();
    reset?.abort();
    return reset?.groupId;
  };

  /** FIN the open group, if any. Returns the failure if the FIN did not complete. */
  const endGroup = async (): Promise<RelayError | undefined> => {
    const ending = group;
    group = undefined;
    if (!ending) return undefined;
    finishing = ending;
    try {
      await ending.close();
    } catch (error) {
      if (error instanceof RelayError) return error;
      ending.abort();
      throw error;
    } finally {
      if (finishing === ending) finishing = undefined;
    }
    return undefined;
  };

  const failed = (error: RelayError, groupId: number | undefined): FrameOutcome => {
    if (retired(track, error)) {
      ended = error;
      group = undefined;
      stopWatching();
      return { kind: 'ended', error, ...(groupId === undefined ? {} : { groupId }) };
    }
    needKeyframe();
    return { kind: 'interrupted', error, ...(groupId === undefined ? {} : { groupId }) };
  };

  /** A frame as the first object of the group it opens. A missing payload would open a bare group and send nothing. */
  const firstObject = (frame: F) => {
    if (frame.payload == null) throw new TypeError('frame.payload must be an ArrayBufferView');
    return frame.properties ? { payload: frame.payload, properties: frame.properties } : { payload: frame.payload };
  };

  const withGroup = (kind: FrameOutcomeKind, groupId: number | undefined): FrameOutcome =>
    groupId === undefined ? { kind } : { kind, groupId };

  const intoGop = async (frame: F): Promise<FrameOutcome> => {
    const epoch = drops;
    const droppedUnder = () => drops !== epoch;
    const late = stale(frame);
    let reset: number | undefined;
    if (late || frame.discontinuity) {
      // Whatever group is open depended on frames that will never be sent.
      reset = resetGroup();
      recovering = 'stale';
      // A keyframe after the discontinuity is the recovery itself.
      if (late || !frame.key) needKeyframe();
    }
    if (late) return withGroup('stale', reset);
    if (recovering && !frame.key) return withGroup(recovering, reset);

    if (frame.key) {
      recovering = undefined;
      const previous = group?.groupId;
      const failure = await endGroup();
      if (droppedUnder()) return { kind: 'dropped' };
      if (failure && retired(track, failure)) {
        // The track ended under the FIN of the group before this keyframe: that is the retirement, with its own error.
        ended = failure;
        stopWatching();
        return { kind: 'ended', error: failure, ...(previous === undefined ? {} : { groupId: previous }) };
      }
      if (stale(frame)) {
        // The FIN took long enough that this keyframe is too old to start a group with.
        recovering = 'stale';
        needKeyframe();
        return { kind: 'stale' };
      }
      if (!track.hasDemand) {
        // This keyframe opens nothing, so it cannot recover from a demand change still to be reported.
        keyOpening = false;
        return { kind: 'skipped' };
      }
      // The keyframe opens its group as object 0, so its delivery timeout counts the wait for the stream (§5.2).
      let opened: RelayGroupWriter;
      const controller = new AbortController();
      opening = controller;
      try {
        opened = await track.openGroup({ ...firstObject(frame), signal: controller.signal });
      } catch (error) {
        if (droppedUnder()) return { kind: 'dropped' };
        if (error instanceof RelayError) return failed(error, undefined);
        throw error;
      } finally {
        if (opening === controller) opening = undefined;
      }
      groups++;
      objects++;
      // From here the open group itself says whether a demand change was recovered from.
      keyOpening = false;
      if (droppedUnder()) {
        // The backlog overflowed while the group opened: the transport took the keyframe, but it will not arrive.
        opened.abort();
        return { kind: 'dropped', groupId: opened.groupId };
      }
      // Held before the predicate runs, so a predicate that throws still has the group reset.
      group = opened;
      if (stale(frame)) {
        // Opening the group took long enough that its keyframe is too old: a reset costs less than a late GOP.
        recovering = 'stale';
        const groupId = resetGroup();
        needKeyframe();
        return withGroup('stale', groupId);
      }
      return { kind: 'sent', groupId: opened.groupId };
    }

    // No demand when the GOP started, or the group was lost: wait for the next keyframe.
    if (!group) return { kind: 'skipped' };
    if (stale(frame)) {
      recovering = 'stale';
      const groupId = resetGroup();
      needKeyframe();
      return withGroup('stale', groupId);
    }
    const target = group;
    try {
      await target.write(frame.payload, frame.properties ? { properties: frame.properties } : {});
    } catch (error) {
      // An overflow reset the group under this write: the frame is a drop, whatever error the reset produced.
      if (droppedUnder()) return { kind: 'dropped', groupId: target.groupId };
      if (!(error instanceof RelayError)) throw error;
      // The SDK reset the group's stream already; the next keyframe opens a fresh one.
      if (group === target) group = undefined;
      return failed(error, target.groupId);
    }
    objects++;
    // The transport took the object, but an overflow reset its group before this resumed: it will not arrive.
    if (droppedUnder()) return { kind: 'dropped', groupId: target.groupId };
    return { kind: 'sent', groupId: target.groupId };
  };

  const ownGroup = async (frame: F): Promise<FrameOutcome> => {
    if (stale(frame)) return { kind: 'stale' };
    if (!track.hasDemand) return { kind: 'skipped' };
    // The frame opens its group as object 0, so its delivery timeout counts the wait for the stream (§5.2).
    let opened: RelayGroupWriter;
    try {
      opened = await track.openGroup(firstObject(frame));
    } catch (error) {
      if (error instanceof RelayError) return failed(error, undefined);
      throw error;
    }
    groups++;
    objects++;
    // From here on the group holds the track's write slot: whatever happens, it is reset before this returns.
    try {
      if (stale(frame)) {
        // Opening the group took long enough that the frame is too old; a reset costs less than a late frame.
        opened.abort();
        return { kind: 'stale', groupId: opened.groupId };
      }
      await opened.close();
    } catch (error) {
      opened.abort();
      if (error instanceof RelayError) return failed(error, opened.groupId);
      throw error;
    }
    return { kind: 'sent', groupId: opened.groupId };
  };

  const serve = async (frame: F): Promise<FrameOutcome> => {
    if (ended) return { kind: 'ended', error: ended };
    if (track.closed) {
      // The track ended while nothing was in flight. No operation failed, so the state is named instead.
      ended = new RelayError('CLOSED', 'Published track is closed', 'waiting-for-demand');
      group = undefined;
      stopWatching();
      return { kind: 'ended', error: ended };
    }
    if (!keyframes) return ownGroup(frame);
    if (group?.closed) {
      // The write slot ended the group when relay demand changed (before the demand watcher may have run): the GOP in
      // progress cannot continue. A keyframe here is the recovery.
      group = undefined;
      if (!frame.key) needKeyframe();
    }
    keyOpening = frame.key;
    try {
      return await intoGop(frame);
    } catch (error) {
      // Whatever escapes (a payload the SDK rejected, the caller's predicate throwing) leaves the GOP with a gap. Reset
      // it so the write slot is free and the next delta is skipped rather than sent as object 0.
      resetGroup();
      needKeyframe();
      throw error;
    } finally {
      keyOpening = false;
    }
  };

  const pump = async () => {
    for (let next = waiting.shift(); next; next = waiting.shift()) {
      current = next;
      try {
        next.resolve(await serve(next.frame));
      } catch (error) {
        next.reject(error);
      } finally {
        current = undefined;
      }
    }
  };

  const stopWatching = () => {
    const iterator = demand;
    demand = undefined;
    void iterator?.return?.();
  };

  const write = (frame: F): Promise<FrameOutcome> => {
    if (closing) {
      return Promise.reject(new RelayError('CLOSED', 'Frame writer is closed', 'waiting-for-demand'));
    }
    if (typeof frame !== 'object' || frame === null) return Promise.reject(new TypeError('frame must be an object'));
    if (ended) return Promise.resolve({ kind: 'ended', error: ended });
    const callbacks: (() => void)[] = [];
    let arriving: FrameOutcome | undefined;
    if (waiting.length >= maxBacklog) {
      if (keyframes) {
        // Deltas cannot be skipped: throw the backlog away with the group it was feeding, and restart at a keyframe.
        drops++;
        for (const dropped of waiting.splice(0)) dropped.resolve({ kind: 'dropped' });
        const reset = resetGroup();
        recovering = 'dropped';
        if (!frame.key) {
          arriving = withGroup('dropped', reset);
          if (!keyframeNeeded) callbacks.push(needKeyframe);
        }
      } else {
        // Freshness beats completeness for independent frames: drop the oldest.
        waiting.shift()?.resolve({ kind: 'dropped' });
      }
    }
    if (keyframes && frame.key) keyframeNeeded = false;
    for (const callback of callbacks) callback();
    if (arriving) return Promise.resolve(arriving);
    const outcome = new Promise<FrameOutcome>((resolve, reject) => waiting.push({ frame, resolve, reject }));
    if (!current && waiting.length === 1) idle = pump();
    return outcome;
  };

  const close = async () => {
    if (closing) return idle;
    closing = true;
    for (const dropped of waiting.splice(0)) dropped.resolve({ kind: 'dropped' });
    stopWatching();
    idle = idle.then(async () => {
      await endGroup();
    });
    return idle;
  };

  if (keyframes && !track.closed) {
    const changes = track.demandChanges();
    demand = changes;
    void (async () => {
      let first = true;
      for await (const _ of changes) {
        if (demand !== changes) break;
        // The first value is the state at creation, which the writer already starts from.
        if (first) {
          first = false;
          continue;
        }
        // A new or replacement relay subscription, or its end. The write slot has already ended the open group, which
        // the next frame finds closed; the new subscription needs a keyframe. Unless it has one: the change closed every
        // group open at the time, so a group still open came after it, and a keyframe waiting or in progress opens one.
        const recovered = (group && !group.closed) || keyOpening || waiting.some(({ frame }) => frame.key);
        if (!recovered) needKeyframe();
      }
    })().catch(() => {});
  }

  return {
    write,
    close,
    get closed() {
      return closing || ended !== undefined;
    },
    get ended() {
      return ended;
    },
    get groups() {
      return groups;
    },
    get objects() {
      return objects;
    },
    get backlog() {
      return waiting.length;
    },
    get keyframeNeeded() {
      return keyframeNeeded;
    },
  };
}
