/**
 * The MSF Media Timeline track (msf-01 §7): records that map media time and a location to wall-clock time. See
 * spec/media-layer.md, "Media Timeline track". Format only: the publisher decides which groups get an entry and when
 * to send a document. {@link createMediaTimeline} holds the retained entries of one broadcast and builds the independent
 * document each new timeline group carries.
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
  /** The object's LOC timestamp on the broadcast timeline, in integer microseconds. */
  timestampUs: number;
  /** The object's group ID. */
  group: number;
  /** The object's ID. Default 0, the group's first object. */
  object?: number;
  /**
   * The broadcast's origin as Unix-epoch microseconds (`BroadcastClock.originEpochUs`). Without it the wall-clock
   * time is 0, as msf-01 says for unknown wall-clock time.
   */
  originEpochUs?: number;
}

function whole(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 0) throw new RangeError(`${name} must be a nonnegative safe integer`);
  return value;
}

/** The record for one object of the indexed track, stamped from the broadcast timeline. */
export function mediaTimelineEntry(input: MediaTimelineEntryInput): MediaTimelineEntry {
  const timestampUs = whole(input.timestampUs, 'timestampUs');
  const wallclockUs = input.originEpochUs === undefined ? 0 : whole(input.originEpochUs, 'originEpochUs') + timestampUs;
  return {
    mediaTimeMs: Math.floor(timestampUs / 1000),
    group: whole(input.group, 'group'),
    object: whole(input.object ?? 0, 'object'),
    wallclockMs: Math.floor(whole(wallclockUs, 'wall-clock time') / 1000),
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

/** Options for {@link createMediaTimeline}. */
export interface MediaTimelineOptions {
  /**
   * The broadcast's origin as Unix-epoch microseconds (`BroadcastClock.originEpochUs`). Without it every wall-clock
   * time is 0.
   */
  originEpochUs?: number;
  /** How many entries the document keeps; past it, the oldest is dropped. Default 512, about 18 KB. */
  maxEntries?: number;
}

/** One object of the indexed track, for {@link MediaTimeline.add}. */
export type MediaTimelineAddInput = Omit<MediaTimelineEntryInput, 'originEpochUs'>;

/** The retained entries of one broadcast's media timeline, and the independent document they make. */
export interface MediaTimeline {
  /** The retained entries, oldest first. */
  readonly entries: readonly MediaTimelineEntry[];
  /** The independent document of the retained entries; `[]` before the first. */
  readonly document: Uint8Array;
  /**
   * Record one object, once per group of the indexed track for its first object, when that object was sent; return
   * the document. A rejected argument throws and leaves the timeline unchanged.
   */
  add(input: MediaTimelineAddInput): Uint8Array;
}

/**
 * Hold one broadcast's media timeline entries, bounded by `maxEntries`. A supervised publisher's next generation is a
 * new broadcast and starts a new timeline.
 */
export function createMediaTimeline(options: MediaTimelineOptions = {}): MediaTimeline {
  const { originEpochUs, maxEntries = 512 } = options;
  if (originEpochUs !== undefined) whole(originEpochUs, 'originEpochUs');
  if (whole(maxEntries, 'maxEntries') < 1) throw new RangeError('maxEntries must be a positive safe integer');
  const entries: MediaTimelineEntry[] = [];
  return {
    get entries() {
      return entries.slice();
    },
    get document() {
      return buildMediaTimeline(entries);
    },
    add(input) {
      entries.push(mediaTimelineEntry({ ...input, ...(originEpochUs === undefined ? {} : { originEpochUs }) }));
      if (entries.length > maxEntries) entries.shift();
      return buildMediaTimeline(entries);
    },
  };
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
