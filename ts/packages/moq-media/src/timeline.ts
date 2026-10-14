/**
 * The MSF Media Timeline track (msf-01 §7): records that map media time and a location to wall-clock time. See
 * spec/media-layer.md, "Media Timeline track". Format only: the publisher decides which groups get an entry, how many
 * to retain, and when to send a document.
 */

/** One media timeline record, `[mediaTimeMs, [group, object], wallclockMs]` on the wire (msf-01 §7.1.1). */
export interface MediaTimelineEntry {
  /** Media time of the first sample in the referenced object, floored to whole milliseconds. */
  mediaTimeMs: number;
  /** Group ID of the referenced object, in the track the timeline indexes. */
  group: number;
  /** Object ID of the referenced object. */
  object: number;
  /** Milliseconds since the Unix epoch when that media was captured; 0 when unknown. */
  wallclockMs: number;
}

/** What {@link mediaTimelineEntry} needs to know about one object. */
export interface MediaTimelineEntryInput {
  /** The object's LOC timestamp, in integer microseconds. */
  timestampUs: number;
  /** The object's group ID. */
  group: number;
  /** The object's ID. Default 0, the group's first object. */
  object?: number;
  /**
   * When that media was captured, in integer Unix-epoch microseconds. Defaults to `timestampUs`, which on the
   * broadcast clock's timeline is that time; 0 means unknown, as msf-01 allows.
   */
  wallclockUs?: number;
}

function whole(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 0) throw new RangeError(`${name} must be a nonnegative safe integer`);
  return value;
}

/** The record for one object of the indexed track. */
export function mediaTimelineEntry(input: MediaTimelineEntryInput): MediaTimelineEntry {
  const timestampUs = whole(input.timestampUs, 'timestampUs');
  return {
    mediaTimeMs: Math.floor(timestampUs / 1000),
    group: whole(input.group, 'group'),
    object: whole(input.object ?? 0, 'object'),
    wallclockMs: Math.floor(whole(input.wallclockUs ?? timestampUs, 'wallclockUs') / 1000),
  };
}

/**
 * A timeline object's payload: the records in the order given, as compact JSON. The same form serves an independent
 * document (every retained record) and an incremental one (only new ones).
 */
export function buildMediaTimeline(entries: readonly MediaTimelineEntry[]): Uint8Array {
  const records = entries.map((entry) => [
    whole(entry.mediaTimeMs, 'mediaTimeMs'),
    [whole(entry.group, 'group'), whole(entry.object, 'object')],
    whole(entry.wallclockMs, 'wallclockMs'),
  ]);
  return new TextEncoder().encode(JSON.stringify(records));
}

const count = (value: unknown): number | undefined =>
  Number.isSafeInteger(value) && (value as number) >= 0 ? (value as number) : undefined;

function parseRecord(record: unknown): MediaTimelineEntry | undefined {
  if (!Array.isArray(record) || record.length < 3 || !Array.isArray(record[1]) || record[1].length < 2) return;
  const mediaTimeMs = count(record[0]);
  const group = count(record[1][0]);
  const object = count(record[1][1]);
  const wallclockMs = count(record[2]);
  if (mediaTimeMs === undefined || group === undefined || object === undefined || wallclockMs === undefined) return;
  return { mediaTimeMs, group, object, wallclockMs };
}

/**
 * Read a timeline object's payload into its records, in document order.
 *
 * Throws on a payload that is not a UTF-8 JSON array. Records that do not read as a nonnegative safe-integer
 * `[number, [number, number], number]` are skipped, a §7.4 template among them; extra items are ignored.
 * MSF_COMPRESSION payloads are not decompressed.
 */
export function parseMediaTimeline(payload: Uint8Array): MediaTimelineEntry[] {
  const raw: unknown = JSON.parse(new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(payload));
  if (!Array.isArray(raw)) throw new Error('media timeline is not a JSON array');
  return raw.map(parseRecord).filter((entry): entry is MediaTimelineEntry => entry !== undefined);
}
