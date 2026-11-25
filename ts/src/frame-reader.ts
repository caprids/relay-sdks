import type { KeyValuePair, MoqtObject, SubscribeOk } from '@caprid/moq-core';
import type { Diagnostics } from './client-diagnostics.js';
import { delaySpread, diagnosticsOption, gapJudge, mediaTimeMs, reorderCount } from './diagnostics-report.js';
import type { WantSwitchedInfo } from './supervised-subscriber.js';

/**
 * What a {@link FrameReader} drives: `@caprid/moq-media`'s `FrameSequencer` qualifies. The reader feeds it a
 * subscription's events and polls it; the sequencer owns the ordering, the deadlines and the LOC format.
 */
export interface FrameReaderSequencer<F> {
  /** Set from each SUBSCRIBE_OK. */
  trackProperties: readonly KeyValuePair[];
  /** An object arrived. */
  object(object: MoqtObject): void;
  /** A group's stream ended cleanly. */
  groupEnded(groupId: number): void;
  /** A group's stream was reset. */
  groupReset(groupId: number): void;
  /** The subscription was replaced: forget everything. */
  restart(): void;
  /** Take the frames ready now, and when to poll again. */
  poll(): {
    /** The frames ready now, in decode order. */
    frames: F[];
    /** Poll again after this many microseconds, 0 at once; `undefined` when only new events can change it. */
    waitUs: number | undefined;
    /** The sequencer asks for the subscription to be replaced at the live edge: see `onCatchUp`. */
    catchUp?: boolean;
  };
  /** Counters a reader with diagnostics reports, when the sequencer keeps them: a `FrameSequencer`'s do. */
  readonly stats?: object;
}

/** Options for {@link createFrameReader}. */
export interface FrameReaderOptions<F> {
  /**
   * Called synchronously with each frame, in decode order. An exception from it goes to `console.error` and the reader
   * carries on.
   */
  onFrame(frame: F): void;
  /**
   * The sequencer signalled a catch-up (`@caprid/moq-media`'s `catchUpAfterUs`): frames keep arriving too late to use.
   * Replace the subscription at the live edge, which drops what the relay still holds queued for it: a supervised
   * subscriber's `catchUp(key)` does, and its `onRestarted` restarts this reader. Called synchronously, after the
   * frames of the same poll; an exception from it goes to `console.error`.
   */
  onCatchUp?(): void;
  /**
   * A scope from `@caprid/relay/diagnostics` (spec/behavior.md, "Client diagnostics"), such as
   * `diagnostics.scope('p2/audio')`: the reader reports a `receive` section with the objects it read and their
   * payload bytes, its arrival gaps and the longest, the objects that arrived after a later one and the longest such
   * wait, the spread of arrival delays over the last five seconds, and the sequencer's `stats`, and an `arrival-gap`
   * event when objects stop for longer than 250 ms, or eight times their usual interval if that is longer. The usual
   * interval is learned from the first four before any gap is judged. Give each reader its own scope.
   */
  diagnostics?: Diagnostics;
}

/**
 * A subscription's or a want's handlers that read its track as frames in decode order. Pass it, or spread it with
 * handlers of your own, to `subscribe` or `want`. Each group is one subgroup, as the frame writer sends it, so a
 * subgroup's end is its group's. A want read through a reader keeps `drainMs` at 0, the default: a drained old
 * subscription's groups would arrive after the Move, indistinguishable from the new one's.
 */
export interface FrameReader {
  /** Takes the track properties, which may declare the Timescale. */
  onSubscribed(info: SubscribeOk): void;
  /** Feeds the object to the sequencer and delivers what is ready. */
  onObject(object: MoqtObject): void;
  /** Ends the object's group. */
  onSubgroupEnd(info: { groupId: number }): void;
  /** Resets the object's group: its remaining objects will not come. */
  onSubgroupReset(info: { groupId: number }): void;
  /** A want's Restart: the sequencer starts over and marks the next frame. */
  onRestarted(): void;
  /** A want's Move: the same, with the new subscription's track properties. */
  onSwitched(info: WantSwitchedInfo): void;
  /** Stop delivering and cancel the pending poll. Later events are ignored. */
  close(): void;
}

/** Poll at least this often while a wait runs: another track may reset or re-anchor the shared clock meanwhile. */
const CLOCK_RECHECK_US = 50_000;

/**
 * Read a track as frames: feed the sequencer every event, poll it after each, and again when a wait it reports ends
 * (at most 50 ms on, since another track may move a shared clock).
 * See spec/behavior.md, "Media subscribing".
 *
 * ```ts
 * const clock = createPlayoutClock();
 * const video = createFrameReader(createFrameSequencer({ clock }), { onFrame: decode });
 * subscriber.subscribe({ namespace, track: 'video' }, { ...video, onError });
 * ```
 */
export function createFrameReader<F>(sequencer: FrameReaderSequencer<F>, options: FrameReaderOptions<F>): FrameReader {
  const arrivals = arrivalDiagnostics(sequencer, diagnosticsOption(options.diagnostics));
  let timer: ReturnType<typeof setTimeout> | undefined;
  let closed = false;
  // An onFrame that feeds the reader again (synchronously) must not interleave a second batch with the first, and a
  // restart it causes ends the batch in hand: those frames belong to the replaced subscription.
  let draining = false;
  let again = false;
  let generation = 0;

  const drain = () => {
    if (closed) return;
    if (draining) {
      again = true;
      return;
    }
    draining = true;
    try {
      do {
        again = false;
        if (timer !== undefined) {
          clearTimeout(timer);
          timer = undefined;
        }
        const current = generation;
        const { frames, waitUs, catchUp } = sequencer.poll();
        for (const frame of frames) {
          if (closed || generation !== current) break;
          try {
            options.onFrame(frame);
          } catch (error) {
            // biome-ignore lint/suspicious/noConsole: the application's callback failed and has no caller to report to
            console.error(new Error('onFrame threw; the frame reader carries on', { cause: error }));
          }
        }
        if (catchUp && !closed && generation === current) {
          try {
            options.onCatchUp?.();
          } catch (error) {
            // biome-ignore lint/suspicious/noConsole: the application's callback failed and has no caller to report to
            console.error(new Error('onCatchUp threw; the frame reader carries on', { cause: error }));
          }
        }
        if (waitUs === 0) again = true;
        else if (!closed && waitUs !== undefined) timer = setTimeout(drain, Math.min(waitUs, CLOCK_RECHECK_US) / 1000);
      } while (again && !closed);
    } finally {
      draining = false;
    }
  };
  const event = (apply: () => void) => {
    if (closed) return;
    apply();
    drain();
  };
  const restart = (apply: () => void) =>
    event(() => {
      generation++;
      apply();
    });

  return {
    onSubscribed(info) {
      if (!closed) sequencer.trackProperties = info.trackProperties;
    },
    onObject(object) {
      if (!closed) arrivals?.arrived(object);
      event(() => sequencer.object(object));
    },
    onSubgroupEnd({ groupId }) {
      event(() => sequencer.groupEnded(groupId));
    },
    onSubgroupReset({ groupId }) {
      event(() => sequencer.groupReset(groupId));
    },
    onRestarted() {
      arrivals?.restarted();
      restart(() => sequencer.restart());
    },
    onSwitched(info) {
      arrivals?.restarted();
      restart(() => {
        sequencer.restart();
        sequencer.trackProperties = info.subscribed.trackProperties;
      });
    },
    close() {
      closed = true;
      arrivals?.close();
      if (timer !== undefined) clearTimeout(timer);
      timer = undefined;
    },
  };
}

/**
 * A reader's arrival counting for diagnostics: objects and their bytes, gaps against the track's usual interval,
 * objects that arrived after a later one, and the spread of arrival delays.
 */
function arrivalDiagnostics(sequencer: FrameReaderSequencer<unknown>, diagnostics: Diagnostics | undefined) {
  if (!diagnostics) return undefined;
  let objects = 0;
  let bytes = 0;
  let gaps = 0;
  let longestGapMs = 0;
  let lastAt: number | undefined;
  const isGap = gapJudge();
  const reorder = reorderCount();
  const spread = delaySpread();
  const removeSection = diagnostics.section('receive', () => {
    const delaySpreadMs = spread.read(performance.now());
    return {
      objects,
      bytes,
      arrivalGaps: gaps,
      longestGapMs: Math.round(longestGapMs),
      reordered: reorder.reordered,
      longestReorderWaitMs: reorder.longestWaitMs,
      ...(delaySpreadMs ? { delaySpreadMs } : {}),
      ...(sequencer.stats ? { sequencer: { ...sequencer.stats } } : {}),
    };
  });
  return {
    arrived(object: MoqtObject) {
      objects++;
      bytes += object.payload.byteLength;
      const now = performance.now();
      if (lastAt !== undefined) {
        const interval = now - lastAt;
        if (isGap(interval)) {
          gaps++;
          longestGapMs = Math.max(longestGapMs, interval);
          diagnostics.event('arrival-gap', { ms: Math.round(interval) });
        }
      }
      lastAt = now;
      reorder.arrived(object, now);
      const mediaMs = mediaTimeMs(object.properties, sequencer.trackProperties);
      if (mediaMs !== undefined) spread.add(now, now - mediaMs);
    },
    /** A replaced subscription: the silence around it is the restart, which the want reports, and its timeline is new. */
    restarted() {
      lastAt = undefined;
      reorder.restart();
      spread.clear();
    },
    close() {
      removeSection();
    },
  };
}
