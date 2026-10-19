/**
 * The interrupt track: how a talker that stops mid-utterance tells its listeners to drop the audio they already hold.
 * See spec/media-layer.md, "Interrupt track". Format only: the talker registers and writes the track, and a listener
 * hands what it reads to the audio player's `cut`.
 */
import { AUDIO_TRACK } from './loc.js';

/** The interrupt track's default name, listed in the catalog as a data track. */
export const INTERRUPT_TRACK = 'interrupt';

/**
 * One interrupt: drop the named audio track's audio in a timeline range. It has the shape of the audio player's
 * `AudioCutRange`, so `input.cut(interrupt)` applies it.
 */
export interface Interrupt {
  /** The audio track in the same namespace whose audio to drop. */
  track: string;
  /** Drop from this timestamp on, in microseconds on the broadcast timeline. Absent: from the oldest audio held. */
  fromUs?: number;
  /**
   * Drop before this timestamp: the end of the last audio the talker sent. Frames that arrive later with an earlier
   * timestamp are dropped too, so the talker's next audio must start at or after it.
   */
  untilUs: number;
}

/** What {@link buildInterrupt} takes. */
export interface InterruptInput {
  /** The audio track whose audio to drop. Default {@link AUDIO_TRACK}. */
  track?: string;
  /** Drop from this timestamp on. Absent: from the oldest audio a listener holds. */
  fromUs?: number;
  /** Drop before this timestamp: the end of the last audio sent. */
  untilUs: number;
}

function timestamp(value: unknown, name: string): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0)
    throw new RangeError(`${name} must be a nonnegative safe integer`);
  return value;
}

function checked(track: unknown, fromUs: unknown, untilUs: unknown): Interrupt {
  if (typeof track !== 'string' || track.length === 0) throw new RangeError('track must be a non-empty string');
  if (/[\uD800-\uDFFF]/u.test(track)) throw new RangeError('track must be valid Unicode');
  const until = timestamp(untilUs, 'untilUs');
  if (fromUs === undefined) return { track, untilUs: until };
  const from = timestamp(fromUs, 'fromUs');
  if (from >= until) throw new RangeError('fromUs must be before untilUs');
  return { track, fromUs: from, untilUs: until };
}

/** An interrupt object's payload: compact JSON, `{"track":…,"fromUs":…,"untilUs":…}`, without `fromUs` when absent. */
export function buildInterrupt(input: InterruptInput): Uint8Array {
  const interrupt = checked(input.track ?? AUDIO_TRACK, input.fromUs, input.untilUs);
  return new TextEncoder().encode(JSON.stringify(interrupt));
}

/**
 * Read an interrupt object's payload. Throws on a payload that is not a UTF-8 JSON object with a non-empty string
 * `track` and a nonnegative safe-integer `untilUs`, or whose `fromUs`, when present, is not one before `untilUs`.
 * Other fields are ignored.
 */
export function parseInterrupt(payload: Uint8Array): Interrupt {
  const raw: unknown = JSON.parse(new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(payload));
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) throw new Error('interrupt is not a JSON object');
  const { track, fromUs, untilUs } = raw as Record<string, unknown>;
  return checked(track, fromUs, untilUs);
}
