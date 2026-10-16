/**
 * One timeline for every broadcast, in Unix-epoch microseconds, and a playout clock for audio/video sync. No DOM, no
 * transport: both clocks read an injected monotonic microsecond source. See spec/media-layer.md, "Shared timeline and
 * playout clock".
 */
import { MICROSECONDS_PER_SECOND } from './loc.js';

/** A monotonic time source in microseconds. The default is `performance.now() * 1000`. */
export type MicrosecondClock = () => number;

const performanceMicroseconds: MicrosecondClock = () => performance.now() * 1000;

function finite(value: number, name: string): number {
  if (!Number.isFinite(value)) throw new RangeError(`${name} must be a finite number`);
  return value;
}

function safeInteger(value: number, name: string, minimum: number): number {
  if (!Number.isSafeInteger(value) || value < minimum) {
    throw new RangeError(`${name} must be a safe integer of at least ${minimum}`);
  }
  return value;
}

/** Options for {@link createBroadcastClock}. */
export interface BroadcastClockOptions {
  /** The time source, in microseconds. Defaults to `performance.now()`. */
  now?: MicrosecondClock;
  /**
   * The moment on the source clock that is pinned to the system clock, in integer microseconds. Defaults to the
   * source's time now, rounded to whole microseconds.
   */
  originUs?: number;
  /**
   * The origin as integer Unix-epoch microseconds. Defaults to `Date.now()` at creation less the
   * source time elapsed since the origin, read once. `Date.now()` rather than `performance.timeOrigin +
   * performance.now()`, which can fall behind the system clock by however long the page has slept.
   */
  originEpochUs?: number;
}

/** Options for {@link BroadcastClock.sampleTimeline}. */
export interface SampleTimelineOptions {
  /** The broadcast time of sample 0, in integer Unix-epoch microseconds. Defaults to the clock's `now()`. */
  firstSampleUs?: number;
}

/** Maps an audio sample counter onto the broadcast timeline. */
export interface SampleTimeline {
  /** Samples per second, per channel. */
  readonly sampleRate: number;
  /** The broadcast time of sample 0. */
  readonly firstSampleUs: number;
  /** The broadcast time of the sample at this index: the first sample's time plus whole elapsed microseconds. */
  timestampUs(sampleIndex: number): number;
}

/** Options for {@link BroadcastClock.sourceTimeline}. */
export interface SourceTimelineOptions {
  /** How far a calibration may disagree with the current anchor before it replaces it, in µs. Default 5,000. */
  toleranceUs?: number;
  /** The capture device's input delay, subtracted from every timestamp, in integer µs. Default 0. */
  captureLatencyUs?: number;
}

/**
 * Maps timestamps from a clock with its own origin and rate onto the broadcast timeline: an `AudioContext`'s time
 * (works in every browser), or Chromium's `MediaStreamTrackProcessor` timestamps, which count from the system's
 * monotonic zero, not from `performance.now()`'s.
 */
export interface SourceTimeline {
  /** The source time of the current anchor; `undefined` until the first calibration or timestamp. */
  readonly anchorSourceUs: number | undefined;
  /**
   * How long the capture device holds a sample before the source sees it, in integer microseconds, subtracted from
   * every timestamp so it stays the capture time. No browser API reports it: set it from a known device latency, and
   * again when the device changes. It does not affect calibration.
   */
  captureLatencyUs: number;
  /**
   * Record that source time `sourceUs` happened at `atUs` on the broadcast clock's own source (default: now), for
   * example an `AudioContext` time and the `performance.now()` time it was rendered at, both in microseconds. The pair
   * becomes the anchor when there is none, or when the current anchor predicts it more than `toleranceUs` off: the
   * source drifted, or jumped, as an `AudioContext` does when it is replaced. Returns whether the anchor changed.
   */
  calibrate(sourceUs: number, atUs?: number): boolean;
  /**
   * The broadcast time of this source timestamp: the anchor's broadcast time plus the source's own elapsed time,
   * rounded to whole microseconds, less the capture latency. Without an anchor, the first call anchors this timestamp
   * at the clock's `now()`.
   */
  timestampUs(sourceUs: number): number;
}

/**
 * A publisher's single clock: every track in the broadcast is stamped from it, in microseconds since the Unix epoch.
 * One moment on a monotonic source is pinned to the system clock, and the timeline advances at the source's rate.
 */
export interface BroadcastClock {
  /** The pinned moment, on the source clock. */
  readonly originUs: number;
  /** The pinned moment as microseconds since the Unix epoch. */
  readonly originEpochUs: number;
  /** The timeline now: Unix-epoch microseconds. */
  now(): number;
  /**
   * Map a timestamp on this clock's own source, such as `performance.now()` microseconds taken at capture, to the
   * timeline: `originEpochUs + sourceUs − originUs`. For a capture clock with another origin, use
   * {@link BroadcastClock.sourceTimeline}.
   */
  fromSourceUs(sourceUs: number): number;
  /** Map a clock with its own origin and rate, kept on the broadcast timeline by calibration. */
  sourceTimeline(options?: SourceTimelineOptions): SourceTimeline;
  /**
   * Anchor a sample counter that starts at zero. The device clock that drives it drifts against the source, so
   * re-anchor with a fresh timeline when the two disagree by more than a frame.
   */
  sampleTimeline(sampleRate: number, options?: SampleTimelineOptions): SampleTimeline;
}

/** Pin a monotonic source to the system clock and map capture timestamps and sample counters onto the epoch. */
export function createBroadcastClock(options: BroadcastClockOptions = {}): BroadcastClock {
  const source = options.now ?? performanceMicroseconds;
  const createdUs = finite(source(), 'Source time');
  const originUs = options.originUs === undefined ? Math.round(createdUs) : options.originUs;
  safeInteger(originUs, 'originUs', Number.MIN_SAFE_INTEGER);
  const originEpochUs = options.originEpochUs ?? Math.round(Date.now() * 1000 - (createdUs - originUs));
  safeInteger(originEpochUs, 'originEpochUs', 0);
  const fromSourceUs = (sourceUs: number) => originEpochUs + Math.round(finite(sourceUs, 'Source time') - originUs);
  const now = () => fromSourceUs(source());
  return {
    originUs,
    originEpochUs,
    now,
    fromSourceUs,
    sourceTimeline({ toleranceUs = 5_000, captureLatencyUs = 0 } = {}) {
      if (!(finite(toleranceUs, 'toleranceUs') >= 0)) throw new RangeError('toleranceUs must not be negative');
      let latencyUs = safeInteger(captureLatencyUs, 'captureLatencyUs', 0);
      let anchor: { sourceUs: number; timestampUs: number } | undefined;
      return {
        get anchorSourceUs() {
          return anchor?.sourceUs;
        },
        get captureLatencyUs() {
          return latencyUs;
        },
        set captureLatencyUs(value: number) {
          latencyUs = safeInteger(value, 'captureLatencyUs', 0);
        },
        calibrate(sourceUs, atUs) {
          finite(sourceUs, 'Source time');
          const measured = atUs === undefined ? now() : fromSourceUs(atUs);
          if (
            anchor &&
            Math.abs(anchor.timestampUs + Math.round(sourceUs - anchor.sourceUs) - measured) <= toleranceUs
          ) {
            return false;
          }
          anchor = { sourceUs, timestampUs: measured };
          return true;
        },
        timestampUs(sourceUs) {
          finite(sourceUs, 'Source time');
          anchor ??= { sourceUs, timestampUs: now() };
          return anchor.timestampUs + Math.round(sourceUs - anchor.sourceUs) - latencyUs;
        },
      };
    },
    sampleTimeline(sampleRate, { firstSampleUs = now() } = {}) {
      safeInteger(sampleRate, 'sampleRate', 1);
      safeInteger(firstSampleUs, 'firstSampleUs', Number.MIN_SAFE_INTEGER);
      return {
        sampleRate,
        firstSampleUs,
        timestampUs(sampleIndex) {
          safeInteger(sampleIndex, 'Sample index', 0);
          // Whole seconds first, so the product stays exact past 2^53 sample-microseconds.
          const seconds = Math.floor(sampleIndex / sampleRate);
          const remainder = sampleIndex - seconds * sampleRate;
          return (
            firstSampleUs +
            seconds * MICROSECONDS_PER_SECOND +
            Math.floor((remainder * MICROSECONDS_PER_SECOND) / sampleRate)
          );
        },
      };
    },
  };
}

/** The target latency before any keyframe interval is known: twice a one-second keyframe interval. */
const DEFAULT_TARGET_LATENCY_US = 2_000_000;
/**
 * The target latency for a broadcast with no video: pass it as `targetLatencyUs`, since the 2 s default waits for
 * keyframe intervals an audio-only track never reports. It covers the audio player's 400 ms jitter-buffer ceiling plus
 * a wired output delay, and a frame sequencer resyncs after half a second of nothing on time instead of two.
 */
export const AUDIO_ONLY_TARGET_LATENCY_US = 500_000;
/** How many keyframe intervals the derived target latency looks back over. */
const KEYFRAME_INTERVALS_KEPT = 4;

/** Options for {@link createPlayoutClock}. Durations are microseconds. */
export interface PlayoutClockOptions {
  /** The time source, in microseconds. Defaults to `performance.now()`. */
  now?: MicrosecondClock;
  /** A frame later than this is dropped. Default 40,000 (EBU R37's audio-early limit). */
  lateDropUs?: number;
  /** The most a frame may lead the clock before the clock re-syncs. Default 1,000,000. */
  maxVideoDelayUs?: number;
  /**
   * The target latency: how far behind the position a frame may still be delivered for decoding, and how late a frame
   * must be before wall pacing re-anchors on it. Default: twice the longest of the last four keyframe intervals
   * reported, or 2,000,000 before any is.
   */
  targetLatencyUs?: number;
  /** Audio silent for longer than this hands pacing to the wall clock. Default 500,000. */
  audioTimeoutUs?: number;
}

/** What to do with a video frame. `offsetUs` is its timestamp minus the clock's position: positive is early. */
export type PlayoutDecision = PlayoutWait | PlayoutShow | PlayoutDrop;

/** The frame is early: hold it and ask again after `waitUs`. */
export interface PlayoutWait {
  /** Hold the frame. */
  action: 'wait';
  /** The frame's timestamp minus the clock's position, rounded to whole microseconds; positive. */
  offsetUs: number;
  /** How long to hold the frame: the offset, rounded up to whole microseconds. */
  waitUs: number;
}

/** Show the frame now: it is on time, late within `lateDropUs`, or a re-sync. */
export interface PlayoutShow {
  /** Show the frame now. */
  action: 'show';
  /** The frame's timestamp minus the clock's position, rounded to whole microseconds; the sync error to log. */
  offsetUs: number;
}

/** The frame is later than `lateDropUs`: audio has moved past it. */
export interface PlayoutDrop {
  /** Discard the frame. */
  action: 'drop';
  /** The frame's timestamp minus the clock's position, rounded to whole microseconds; negative. */
  offsetUs: number;
}

/** `idle` before anything anchors it, `audio` while audio reports arrive, `wall` when paced by the local clock. */
export type PlayoutMode = 'idle' | 'audio' | 'wall';

/** A player's clock for one broadcast's timeline. Audio is the master; video asks it when each frame is due. */
export interface PlayoutClock {
  /** What paces the clock now. */
  readonly mode: PlayoutMode;
  /**
   * Report that the audio with this timeline timestamp is audible at local time `atUs` (default now): what was
   * handed to the output, less the jitter-buffer depth, `AudioContext.outputLatency`, and any extra path.
   */
  reportAudio(timestampUs: number, atUs?: number): void;
  /** Audio is muted or ended: pace by wall clock from the current position. */
  stopAudio(): void;
  /** Decide whether the frame with this timeline timestamp should wait, be shown now, or be dropped. */
  due(timestampUs: number): PlayoutDecision;
  /** The timeline position now, or `undefined` while idle. */
  position(): number | undefined;
  /** How late a frame may be shown, in microseconds: the `lateDropUs` option. */
  readonly lateDropUs: number;
  /** The target latency now: the option, or twice the longest of the last four keyframe intervals reported. */
  readonly targetLatencyUs: number;
  /** Report the spacing between two consecutive keyframes on the timeline; a frame reader does this. */
  reportKeyframeInterval(intervalUs: number): void;
  /** How many times `reset()` has been called: a frame reader sees from it that the timeline changed under it. */
  readonly resets: number;
  /** The clock's local time source now, in microseconds: what a frame reader measures its waits in. */
  localNow(): number;
  /**
   * Forget every anchor, at a Restart, a Move, a resync, or a new catalog: the next frame anchors wall pacing. The
   * keyframe intervals reported so far are kept.
   */
  reset(): void;
}

/** Create a playout clock. See spec/media-layer.md, "Playout clock (player)". */
export function createPlayoutClock(options: PlayoutClockOptions = {}): PlayoutClock {
  const source = options.now ?? performanceMicroseconds;
  const lateDropUs = finite(options.lateDropUs ?? 40_000, 'lateDropUs');
  const maxVideoDelayUs = finite(options.maxVideoDelayUs ?? 1_000_000, 'maxVideoDelayUs');
  const audioTimeoutUs = finite(options.audioTimeoutUs ?? 500_000, 'audioTimeoutUs');
  if (lateDropUs < 0) throw new RangeError('lateDropUs must not be negative');
  if (maxVideoDelayUs <= 0) throw new RangeError('maxVideoDelayUs must be positive');
  if (audioTimeoutUs <= 0) throw new RangeError('audioTimeoutUs must be positive');
  const fixedTargetUs =
    options.targetLatencyUs === undefined ? undefined : finite(options.targetLatencyUs, 'targetLatencyUs');
  if (fixedTargetUs !== undefined && fixedTargetUs <= 0) throw new RangeError('targetLatencyUs must be positive');
  // The latest keyframe intervals: the longest, so that a forced keyframe does not shrink the budget.
  const intervals: number[] = [];
  let resets = 0;
  const targetLatency = () =>
    fixedTargetUs ?? (intervals.length > 0 ? 2 * Math.max(...intervals) : DEFAULT_TARGET_LATENCY_US);

  // The timeline position `mediaUs` was current at local time `atUs`; it advances in real time from there.
  // An audio anchor also keeps when its report arrived: silence is measured from arrival, not from `atUs`.
  let anchor: { mediaUs: number; atUs: number; receivedUs?: number } | undefined;
  const modeAt = (now: number): PlayoutMode => {
    if (!anchor) return 'idle';
    const received = anchor.receivedUs;
    return received !== undefined && now - received <= audioTimeoutUs ? 'audio' : 'wall';
  };
  const positionAt = (now: number) => (anchor ? anchor.mediaUs + (now - anchor.atUs) : undefined);

  return {
    get mode() {
      return modeAt(source());
    },
    reportAudio(timestampUs, atUs) {
      const mediaUs = finite(timestampUs, 'Audio timestamp');
      const receivedUs = source();
      anchor = { mediaUs, atUs: atUs === undefined ? receivedUs : finite(atUs, 'Audio report time'), receivedUs };
    },
    stopAudio() {
      const now = source();
      if (modeAt(now) === 'audio') anchor = { mediaUs: positionAt(now) as number, atUs: now };
    },
    due(timestampUs) {
      finite(timestampUs, 'Frame timestamp');
      const now = source();
      const mode = modeAt(now);
      const position = positionAt(now);
      if (mode === 'idle' || position === undefined) {
        anchor = { mediaUs: timestampUs, atUs: now };
        return { action: 'show', offsetUs: 0 };
      }
      const offset = timestampUs - position;
      const offsetUs = Math.round(offset);
      if (mode === 'wall' && (offset > maxVideoDelayUs || offset < -targetLatency())) {
        // A timeline jump or a stall: follow the video rather than drop or hold every later frame.
        anchor = { mediaUs: timestampUs, atUs: now };
        return { action: 'show', offsetUs };
      }
      // Audio stays the master: a frame that far ahead is not on its timeline, and holding it would freeze the picture.
      if (offset > maxVideoDelayUs) return { action: 'show', offsetUs };
      if (offset > 0) return { action: 'wait', offsetUs, waitUs: Math.ceil(offset) };
      if (offset >= -lateDropUs) return { action: 'show', offsetUs };
      return { action: 'drop', offsetUs };
    },
    position() {
      return positionAt(source());
    },
    lateDropUs,
    get targetLatencyUs() {
      return targetLatency();
    },
    reportKeyframeInterval(intervalUs) {
      const interval = finite(intervalUs, 'Keyframe interval');
      if (interval <= 0) throw new RangeError('A keyframe interval must be positive');
      intervals.push(interval);
      if (intervals.length > KEYFRAME_INTERVALS_KEPT) intervals.shift();
    },
    get resets() {
      return resets;
    },
    localNow() {
      return source();
    },
    reset() {
      anchor = undefined;
      resets++;
    },
  };
}
