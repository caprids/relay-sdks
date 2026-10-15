import type { KeyValuePair, MoqtObject, SubscribeOk } from '@mux/moq-core';
import type { WantSwitchedInfo } from './supervised-subscriber.js';

/**
 * What a {@link FrameReader} drives: `@mux/moq-media`'s `FrameSequencer` qualifies. The reader feeds it a
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
    /** Poll again after this many microseconds; `undefined` when only new events can change it. */
    waitUs: number | undefined;
  };
}

/** Options for {@link createFrameReader}. */
export interface FrameReaderOptions<F> {
  /**
   * Called synchronously with each frame, in decode order. An exception from it goes to `console.error` and the reader
   * carries on.
   */
  onFrame(frame: F): void;
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

/**
 * Read a track as frames: feed the sequencer every event, poll it after each, and again when a wait it reports ends.
 * See spec/behavior.md, "Media subscribing".
 *
 * ```ts
 * const clock = createPlayoutClock();
 * const video = createFrameReader(createFrameSequencer({ clock }), { onFrame: decode });
 * subscriber.subscribe({ namespace, track: 'video' }, { ...video, onError });
 * ```
 */
export function createFrameReader<F>(sequencer: FrameReaderSequencer<F>, options: FrameReaderOptions<F>): FrameReader {
  let timer: ReturnType<typeof setTimeout> | undefined;
  let closed = false;

  const drain = () => {
    if (closed) return;
    if (timer !== undefined) {
      clearTimeout(timer);
      timer = undefined;
    }
    const { frames, waitUs } = sequencer.poll();
    for (const frame of frames) {
      if (closed) return;
      try {
        options.onFrame(frame);
      } catch (error) {
        // biome-ignore lint/suspicious/noConsole: the application's callback failed and has no caller to report to
        console.error(new Error('onFrame threw; the frame reader carries on', { cause: error }));
      }
    }
    if (!closed && waitUs !== undefined) timer = setTimeout(drain, waitUs / 1000);
  };
  const event = (apply: () => void) => {
    if (closed) return;
    apply();
    drain();
  };

  return {
    onSubscribed(info) {
      if (!closed) sequencer.trackProperties = info.trackProperties;
    },
    onObject(object) {
      event(() => sequencer.object(object));
    },
    onSubgroupEnd({ groupId }) {
      event(() => sequencer.groupEnded(groupId));
    },
    onSubgroupReset({ groupId }) {
      event(() => sequencer.groupReset(groupId));
    },
    onRestarted() {
      event(() => sequencer.restart());
    },
    onSwitched(info) {
      event(() => {
        sequencer.restart();
        sequencer.trackProperties = info.subscribed.trackProperties;
      });
    },
    close() {
      closed = true;
      if (timer !== undefined) clearTimeout(timer);
      timer = undefined;
    },
  };
}
