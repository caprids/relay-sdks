/**
 * The A/V sync measurement, DOM-free: where a beep starts in decoded audio, how a published flash and beep pair with
 * what the player showed and played, and the summary statistics. Times are microseconds; offsets are reported in
 * milliseconds, positive when audio is late.
 */

/** Options for {@link createOnsetDetector}. */
export interface OnsetDetectorOptions {
  /** The absolute sample value that counts as sound. Default 0.1. */
  threshold?: number;
  /** How long the signal must stay below the threshold before a new onset counts, in µs. Default 200,000. */
  quietUs?: number;
}

/** Finds where tone bursts start in a stream of sample blocks. */
export interface OnsetDetector {
  /** Feed one block that starts at `timestampUs`; returns the timestamps of the onsets in it. */
  push(samples: Float32Array, sampleRate: number, timestampUs: number): number[];
  /** Forget the history, at a decoder reset: the next sound counts as an onset only after `quietUs` of quiet. */
  reset(): void;
}

/** Create an onset detector. */
export function createOnsetDetector({ threshold = 0.1, quietUs = 200_000 }: OnsetDetectorOptions = {}): OnsetDetector {
  // When the signal last reached the threshold; undefined means quiet since the start or the last reset.
  let lastLoudUs: number | undefined;
  let quietSinceUs: number | undefined;
  return {
    push(samples, sampleRate, timestampUs) {
      const onsets: number[] = [];
      quietSinceUs ??= timestampUs;
      for (let index = 0; index < samples.length; index++) {
        if (Math.abs(samples[index] as number) < threshold) continue;
        const atUs = timestampUs + (index * 1_000_000) / sampleRate;
        const quietFrom = lastLoudUs ?? quietSinceUs;
        if (atUs - quietFrom >= quietUs) onsets.push(Math.round(atUs));
        lastLoudUs = atUs;
      }
      return onsets;
    },
    reset() {
      lastLoudUs = undefined;
      quietSinceUs = undefined;
    },
  };
}

/** One flash and beep the publisher emitted together. Times are on the publisher's `performance.now()` clock. */
export interface EmittedEvent {
  /** When the beep's first sample left the synthetic microphone. */
  beepAtUs: number;
  /** When the canvas turned white. */
  flashAtUs: number;
}

/** The first frame of a flash, as the player's renderer showed it. */
export interface ShownFlash {
  /** The frame's broadcast timestamp. */
  timestampUs: number;
  /** When the renderer showed it, on `performance.now()`'s clock. */
  shownAtUs: number;
}

/** A beep onset in decoded audio, and when it was audible. */
export interface HeardBeep {
  /** The onset's broadcast timestamp. */
  timestampUs: number;
  /** When it reached the output, on `performance.now()`'s clock. */
  audibleAtUs: number;
}

/** One matched event. Millisecond fields are rounded to 0.1 ms. */
export interface MatchedEvent {
  /** When the beep was emitted, on `performance.now()`'s clock. */
  beepAtUs: number;
  /** When it reached the player's output, on the same clock. */
  heardAtUs: number;
  /**
   * The A/V offset in ms, positive when audio is late: how much later the beep reached the output after it was emitted
   * than the flash frame was shown after it was captured. Frame quantization is not counted: the white frame's own
   * capture time is the reference for video.
   */
  offsetMs: number;
  /** The part from capture: the beep's broadcast timestamp minus when it was emitted on the timeline, in ms. */
  captureMs: number;
  /** The part from the player: `offsetMs − captureMs`. */
  playoutMs: number;
  /**
   * The part of `captureMs` before the capture worklet: how long the beep took to reach its input, which the recipe
   * cannot see. With a real microphone this is its input latency (issue #290). Absent when no arrival was timed.
   */
  pathMs?: number;
  /** `captureMs − pathMs`: the recipe's own stamping error. */
  calibrationMs?: number;
  /** `offsetMs − pathMs`: the A/V offset the recipe and the player are answerable for. */
  syncMs?: number;
}

/**
 * Pair each emitted event with the flash frame and the beep the player reported. `originUs` is the broadcast clock's
 * origin on the same `performance.now()` clock, which turns broadcast timestamps into capture times. A flash's frame is
 * the first one stamped at or after the canvas turned white, within `windowUs`; its beep is the onset nearest the
 * emitted beep, within `windowUs`. Events missing either are left out. `arrivals` are when beep onsets reached the
 * capture input, on the same clock; the first within `windowUs` after an emission splits off the path delay.
 */
export function matchEvents(
  emitted: readonly EmittedEvent[],
  flashes: readonly ShownFlash[],
  beeps: readonly HeardBeep[],
  originUs: number,
  arrivals: readonly number[] = [],
  windowUs = 200_000,
): MatchedEvent[] {
  const matched: MatchedEvent[] = [];
  const ms = (us: number) => Math.round(us / 100) / 10;
  for (const event of emitted) {
    const flashTimestampUs = event.flashAtUs - originUs;
    const flash = flashes
      .filter((candidate) => candidate.timestampUs >= flashTimestampUs - 1_000)
      .filter((candidate) => candidate.timestampUs < flashTimestampUs + windowUs)
      .sort((a, b) => a.timestampUs - b.timestampUs)[0];
    const beepTimestampUs = event.beepAtUs - originUs;
    const beep = beeps
      .filter((candidate) => Math.abs(candidate.timestampUs - beepTimestampUs) < windowUs)
      .sort((a, b) => Math.abs(a.timestampUs - beepTimestampUs) - Math.abs(b.timestampUs - beepTimestampUs))[0];
    if (!flash || !beep) continue;
    const audioDelayUs = beep.audibleAtUs - event.beepAtUs;
    const videoDelayUs = flash.shownAtUs - (flash.timestampUs + originUs);
    const offsetUs = audioDelayUs - videoDelayUs;
    const captureUs = beep.timestampUs - beepTimestampUs;
    const arrivalUs = arrivals
      .filter((candidate) => candidate >= event.beepAtUs - 50_000 && candidate < event.beepAtUs + windowUs)
      .sort((a, b) => a - b)[0];
    const pathUs = arrivalUs === undefined ? undefined : arrivalUs - event.beepAtUs;
    matched.push({
      beepAtUs: event.beepAtUs,
      heardAtUs: beep.audibleAtUs,
      offsetMs: ms(offsetUs),
      captureMs: ms(captureUs),
      playoutMs: ms(offsetUs - captureUs),
      ...(pathUs === undefined
        ? {}
        : { pathMs: ms(pathUs), calibrationMs: ms(captureUs - pathUs), syncMs: ms(offsetUs - pathUs) }),
    });
  }
  return matched;
}

/** Summary statistics of signed offsets, in ms. */
export interface OffsetSummary {
  /** How many values. */
  count: number;
  /** The mean. */
  mean: number;
  /** The 95th percentile of the absolute values (nearest rank). */
  p95: number;
  /** The value furthest from zero, with its sign. */
  worst: number;
  /** The smallest value: the most audio-early. */
  min: number;
  /** The largest value: the most audio-late. */
  max: number;
}

/** Summarize offsets; `undefined` when there are none. Results are rounded to 0.1 ms. */
export function summarize(values: readonly number[]): OffsetSummary | undefined {
  if (values.length === 0) return undefined;
  const round = (value: number) => Math.round(value * 10) / 10;
  const absolute = values.map(Math.abs).sort((a, b) => a - b);
  const worst = values.reduce((furthest, value) => (Math.abs(value) > Math.abs(furthest) ? value : furthest));
  return {
    count: values.length,
    mean: round(values.reduce((sum, value) => sum + value, 0) / values.length),
    p95: round(absolute[Math.ceil(0.95 * absolute.length) - 1] as number),
    worst: round(worst),
    min: round(Math.min(...values)),
    max: round(Math.max(...values)),
  };
}
