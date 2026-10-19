/**
 * The transcript track: the text of a talker's audio, in segments placed on its timeline. See spec/media-layer.md,
 * "Transcript track". Format only: the talker registers and writes the track, and a listener keeps what it reads in a
 * {@link Transcript}, which drops what an interrupt cuts.
 */
import { AUDIO_TRACK } from './loc.js';

/** The transcript track's default name, listed in the catalog as a data track. */
export const TRANSCRIPT_TRACK = 'transcript';

/** One version of a transcript segment: the text of the named audio track's audio from `fromUs` on. */
export interface TranscriptSegment {
  /** The segment's number; a later version of the same segment repeats it. */
  seq: number;
  /** The audio track in the same namespace whose audio this is the text of. */
  track: string;
  /** Where the segment's audio starts, in microseconds on the broadcast timeline. */
  fromUs: number;
  /** Where it ends. Absent while the talker does not know yet; a final segment has it. */
  untilUs?: number;
  /** The text, which a later version replaces whole. */
  text: string;
  /** No later version follows. */
  final: boolean;
}

/** What {@link buildTranscriptSegment} takes. */
export interface TranscriptSegmentInput {
  /** The segment's number, repeated by each of its versions. */
  seq: number;
  /** The audio track this is the text of. Default {@link AUDIO_TRACK}. */
  track?: string;
  /** Where the segment's audio starts. */
  fromUs: number;
  /** Where it ends, if known; required when `final`. */
  untilUs?: number;
  /** The text so far. */
  text: string;
  /** No later version follows. Default `false`. */
  final?: boolean;
}

/** A segment as a {@link Transcript} holds it: its latest version, and whether an interrupt cut it short. */
export interface TranscriptLine extends TranscriptSegment {
  /** An interrupt cut the segment's audio short at `untilUs`: the listener heard only the start of the text. */
  cut: boolean;
}

/** The range an interrupt cuts, which a parsed `Interrupt` is. */
export interface TranscriptCutRange {
  /** Cut from this timestamp on. Absent: from the start of the timeline. */
  fromUs?: number;
  /** Cut before this timestamp. */
  untilUs: number;
}

/**
 * What a listener has read of one audio track's transcript: each segment's latest version, less what was cut.
 * `apply` each segment read for the track and `cut` on each interrupt for it; `linesAt` gives what to show as the
 * audio plays. A segment's final version is kept against later ones. Cuts are remembered, so a version that arrives
 * after one is clipped as if it had arrived before. A segment is known by its `seq` within one run of the talker's
 * track, so create a new transcript when the talker starts over.
 */
export interface Transcript {
  /**
   * Keep a segment's version, clipped by the cuts so far; `false` if it changed nothing. Ignored: a version of a
   * segment the talker already sent final, a segment that starts inside a range cut earlier (as audio arriving there
   * late is), and an interim version of a segment a cut ended. A cut segment takes the talker's final version,
   * clipped: one whose audio ended before the cut is no longer cut.
   */
  apply(segment: TranscriptSegment): boolean;
  /**
   * Apply an interrupt's range, with the playout position now (`PlayoutClock.position()`). Audio before the position
   * has been heard, so the cut starts at the later of `fromUs` and the position. It drops the segments that start in
   * the cut, and ends the one playing there at its start, final and cut.
   */
  cut(range: TranscriptCutRange, positionUs?: number): void;
  /** Every segment held, in timeline order. */
  readonly lines: TranscriptLine[];
  /** The segments whose audio has started playing at this position, in timeline order; none while idle. */
  linesAt(positionUs: number | undefined): TranscriptLine[];
}

function integer(value: unknown, name: string): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0)
    throw new RangeError(`${name} must be a nonnegative safe integer`);
  return value;
}

function checked(raw: Record<string, unknown>): TranscriptSegment {
  const { seq: rawSeq, track, fromUs: rawFrom, untilUs: rawUntil, text, final } = raw;
  const seq = integer(rawSeq, 'seq');
  if (typeof track !== 'string' || track.length === 0) throw new RangeError('track must be a non-empty string');
  if (typeof text !== 'string') throw new RangeError('text must be a string');
  if (/[\uD800-\uDFFF]/u.test(track)) throw new RangeError('track must be valid Unicode');
  if (/[\uD800-\uDFFF]/u.test(text)) throw new RangeError('text must be valid Unicode');
  if (typeof final !== 'boolean') throw new RangeError('final must be a boolean');
  const fromUs = integer(rawFrom, 'fromUs');
  if (rawUntil === undefined) {
    if (final) throw new RangeError('a final segment needs untilUs');
    return { seq, track, fromUs, text, final };
  }
  const untilUs = integer(rawUntil, 'untilUs');
  if (untilUs <= fromUs) throw new RangeError('untilUs must be after fromUs');
  return { seq, track, fromUs, untilUs, text, final };
}

/**
 * A transcript object's payload: compact JSON, `{"seq":…,"track":…,"fromUs":…,"untilUs":…,"text":…,"final":…}`,
 * without `untilUs` when absent.
 */
export function buildTranscriptSegment(input: TranscriptSegmentInput): Uint8Array {
  const segment = checked({
    ...input,
    track: input.track === undefined ? AUDIO_TRACK : input.track,
    final: input.final === undefined ? false : input.final,
  });
  return new TextEncoder().encode(JSON.stringify(segment));
}

/**
 * Read a transcript object's payload. Throws on a payload that is not a UTF-8 JSON object with a nonnegative
 * safe-integer `seq` and `fromUs`, a non-empty string `track`, a string `text` and a boolean `final`, or whose
 * `untilUs`, when present, is not one after `fromUs`, or that is final without `untilUs`. Other fields are ignored.
 */
export function parseTranscriptSegment(payload: Uint8Array): TranscriptSegment {
  const raw: unknown = JSON.parse(new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(payload));
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw))
    throw new Error('transcript segment is not a JSON object');
  const { seq, track, fromUs, untilUs, text, final } = raw as Record<string, unknown>;
  // A present untilUs must be a number: null is not "absent".
  if ('untilUs' in raw && untilUs === undefined) throw new RangeError('untilUs must be a nonnegative safe integer');
  return checked({ seq, track, fromUs, untilUs, text, final });
}

/** Create an empty {@link Transcript}. */
export function createTranscript(): Transcript {
  const held = new Map<number, TranscriptLine>();
  const cuts: { startUs: number; untilUs: number }[] = [];
  // The segments the talker sent final: a line a cut ended is final too, but takes the talker's final version.
  const finals = new Set<number>();
  // Ended where a cut starts: final, since nothing after it plays, and cut.
  const clipped = (line: TranscriptLine, startUs: number): TranscriptLine => ({
    ...line,
    untilUs: startUs,
    final: true,
    cut: true,
  });
  const same = (a: TranscriptLine, b: TranscriptLine | undefined) =>
    b !== undefined &&
    a.seq === b.seq &&
    a.track === b.track &&
    a.fromUs === b.fromUs &&
    a.untilUs === b.untilUs &&
    a.text === b.text &&
    a.final === b.final &&
    a.cut === b.cut;
  const lines = () =>
    [...held.values()].sort((a, b) => a.fromUs - b.fromUs || a.seq - b.seq).map((line) => ({ ...line }));
  return {
    apply(segment) {
      const previous = held.get(segment.seq);
      if (finals.has(segment.seq) || (previous?.cut && !segment.final)) return false;
      const { seq, track, fromUs, untilUs, text, final } = segment;
      // Final even if a cut drops it below: no later version may bring the segment back.
      if (final) finals.add(seq);
      let line: TranscriptLine = {
        seq,
        track,
        fromUs,
        ...(untilUs === undefined ? {} : { untilUs }),
        text,
        final,
        cut: false,
      };
      for (const cut of [...cuts].sort((a, b) => a.startUs - b.startUs || a.untilUs - b.untilUs)) {
        // Starts inside a cut: never heard. A final version says an earlier one held is wrong.
        if (cut.startUs <= fromUs && fromUs < cut.untilUs) return final && held.delete(seq);
        if (fromUs < cut.startUs && (untilUs === undefined || untilUs > cut.startUs)) {
          line = clipped(line, cut.startUs);
          break;
        }
      }
      if (same(line, previous)) return false;
      held.set(seq, line);
      return true;
    },
    cut(range, positionUs) {
      const untilUs = integer(range.untilUs, 'untilUs');
      let startUs = range.fromUs === undefined ? 0 : integer(range.fromUs, 'fromUs');
      if (range.fromUs !== undefined && startUs >= untilUs) throw new RangeError('fromUs must be before untilUs');
      if (positionUs !== undefined) {
        if (!Number.isFinite(positionUs) || positionUs < 0)
          throw new RangeError('positionUs must be a nonnegative number');
        startUs = Math.max(startUs, Math.floor(positionUs));
      }
      if (startUs >= untilUs) return;
      cuts.push({ startUs, untilUs });
      for (const [seq, line] of held) {
        if (line.fromUs >= untilUs) continue;
        if (line.fromUs >= startUs) held.delete(seq);
        else if (line.untilUs === undefined || line.untilUs > startUs) held.set(seq, clipped(line, startUs));
      }
    },
    get lines() {
      return lines();
    },
    linesAt(positionUs) {
      if (positionUs === undefined) return [];
      return lines().filter((line) => line.fromUs <= positionUs);
    },
  };
}
