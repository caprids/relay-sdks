/**
 * One timeline per broadcast, and a playout clock for audio/video sync. No DOM, no transport: both clocks read an
 * injected monotonic microsecond source. See spec/media-layer.md, "Shared timeline and playout clock".
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
   * The broadcast's zero on the source clock, in integer microseconds: at or before the first capture. Defaults to
   * the source's time now, rounded to whole microseconds.
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
  /** The broadcast time of sample 0, in integer microseconds. Defaults to the clock's `now()`. */
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
   * Record that source time `sourceUs` happened at `atUs` on the broadcast clock's own source (default: now), for
   * example `AudioContext.getOutputTimestamp()`'s `contextTime` and `performanceTime`, both in microseconds. The pair
   * becomes the anchor when there is none, or when the current anchor predicts it more than `toleranceUs` off: the
   * source drifted, or jumped, as an `AudioContext` does when it is replaced. Returns whether the anchor changed.
   */
  calibrate(sourceUs: number, atUs?: number): boolean;
  /**
   * The broadcast time of this source timestamp: the anchor's broadcast time plus the source's own elapsed time,
   * rounded to whole microseconds. Without an anchor, the first call anchors this timestamp at the clock's `now()`.
   */
  timestampUs(sourceUs: number): number;
}

/** A publisher's single clock: every track in the broadcast is stamped from it, in microseconds since its origin. */
export interface BroadcastClock {
  /** The broadcast's zero, on the source clock. */
  readonly originUs: number;
  /** The broadcast's zero as microseconds since the Unix epoch: the wall-clock time media timeline entries use. */
  readonly originEpochUs: number;
  /** Microseconds since the broadcast started. */
  now(): number;
  /**
   * Map a timestamp on this clock's own source, such as `performance.now()` microseconds taken at capture, to the
   * broadcast timeline. A time before the origin maps to a negative number, which the LOC helpers refuse. For a
   * capture clock with another origin, use {@link BroadcastClock.sourceTimeline}.
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

/** Fix a broadcast's origin and map capture timestamps and sample counters onto its microsecond timeline. */
export function createBroadcastClock(options: BroadcastClockOptions = {}): BroadcastClock {
  const source = options.now ?? performanceMicroseconds;
  const createdUs = finite(source(), 'Source time');
  const originUs = options.originUs === undefined ? Math.round(createdUs) : options.originUs;
  safeInteger(originUs, 'originUs', Number.MIN_SAFE_INTEGER);
  const originEpochUs = options.originEpochUs ?? Math.round(Date.now() * 1000 - (createdUs - originUs));
  safeInteger(originEpochUs, 'originEpochUs', 0);
  const fromSourceUs = (sourceUs: number) => Math.round(finite(sourceUs, 'Source time') - originUs);
  const now = () => fromSourceUs(source());
  return {
    originUs,
    originEpochUs,
    now,
    fromSourceUs,
    sourceTimeline({ toleranceUs = 5_000 } = {}) {
      if (!(finite(toleranceUs, 'toleranceUs') >= 0)) throw new RangeError('toleranceUs must not be negative');
      let anchor: { sourceUs: number; timestampUs: number } | undefined;
      return {
        get anchorSourceUs() {
          return anchor?.sourceUs;
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
          return anchor.timestampUs + Math.round(sourceUs - anchor.sourceUs);
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

/** Options for {@link createPlayoutClock}. Durations are microseconds. */
export interface PlayoutClockOptions {
  /** The time source, in microseconds. Defaults to `performance.now()`. */
  now?: MicrosecondClock;
  /** A frame later than this is dropped. Default 40,000 (EBU R37's audio-early limit). */
  lateDropUs?: number;
  /** The most a frame may lead the clock before the clock re-syncs. Default 1,000,000. */
  maxVideoDelayUs?: number;
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
  /** Forget every anchor, at a Restart, a Move, or a new catalog: the next frame anchors wall pacing. */
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
      if (mode === 'wall' && Math.abs(offset) > maxVideoDelayUs) {
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
    reset() {
      anchor = undefined;
    },
  };
}
