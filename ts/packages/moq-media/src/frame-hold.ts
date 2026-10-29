/**
 * Frames kept for a consumer that is not ready yet. A join can subscribe as soon as it knows the track, before the
 * audio player that will play it exists (an `AudioContext`, its worklet module and a decoder take tens of
 * milliseconds): the frame reader delivers into a hold, and the hold passes everything on, in order, once the player's
 * input is there. Nothing is dropped and nothing is judged: the frames reach the consumer as the reader delivered them,
 * only later. See spec/media-layer.md, "Joining sooner".
 */

/** The frames a {@link FrameHold} has kept and passed on, and when it was released. */
export interface FrameHoldStats {
  /** Frames kept now, waiting for `release`. */
  held: number;
  /** The most frames ever kept at once. */
  maxHeld: number;
  /** Frames passed on at `release` that had been kept. */
  released: number;
}

/** Keeps frames in order until `release`, then passes them, and every later one, straight on. */
export interface FrameHold<Frame> {
  /** Keep a frame, or pass it on once released. A frame reader's `onFrame` can be this method. */
  push(frame: Frame): void;
  /**
   * Pass every kept frame to `consumer`, oldest first, then every later frame as it comes. Once only. A frame pushed
   * from inside `consumer` while the kept ones are passed on waits its turn behind them. A kept frame the consumer
   * throws on does not stop the rest: each is passed on, and the first error is thrown after the last (as a frame
   * reader's `onFrame` failing for one frame does not lose the next). After `release`, `push` throws what the consumer
   * throws.
   */
  release(consumer: (frame: Frame) => void): void;
  /** Drop every kept frame; later frames are kept again until `release`. Does nothing once released. */
  clear(): void;
  /** Whether `release` was called. */
  readonly isReleased: boolean;
  /** Counters. */
  readonly stats: Readonly<FrameHoldStats>;
}

/** Create a hold. */
export function createFrameHold<Frame>(): FrameHold<Frame> {
  let kept: Frame[] = [];
  let consumer: ((frame: Frame) => void) | undefined;
  let releasing = false;
  const stats: FrameHoldStats = { held: 0, maxHeld: 0, released: 0 };
  const hold: FrameHold<Frame> = {
    push(frame) {
      if (consumer && !releasing) {
        consumer(frame);
        return;
      }
      kept.push(frame);
      if (!consumer) {
        stats.held = kept.length;
        stats.maxHeld = Math.max(stats.maxHeld, stats.held);
      }
    },
    release(next) {
      if (consumer) throw new Error('The frame hold was already released');
      consumer = next;
      releasing = true;
      stats.released = kept.length;
      stats.held = 0;
      let failed = false;
      let failure: unknown;
      // Frames pushed by the consumer meanwhile join the end of the queue.
      for (let index = 0; index < kept.length; index++) {
        try {
          next(kept[index] as Frame);
        } catch (error) {
          if (!failed) {
            failed = true;
            failure = error;
          }
        }
      }
      kept = [];
      releasing = false;
      if (failed) throw failure;
    },
    clear() {
      if (consumer) return;
      kept = [];
      stats.held = 0;
    },
    get isReleased() {
      return consumer !== undefined;
    },
    get stats() {
      return stats;
    },
  };
  hold.push = hold.push.bind(hold);
  return hold;
}
