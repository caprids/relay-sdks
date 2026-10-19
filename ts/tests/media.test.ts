import { readFileSync } from 'node:fs';
import * as media from '@mux/moq-media';
import { describe, expect, it } from 'vitest';

interface RawTrack {
  name?: string;
  codec?: string;
  width?: string;
  height?: string;
  framerate?: string;
  bitrate?: string;
  initData?: string;
  samplerate?: string;
  channelConfig?: string;
}
interface RawCatalog {
  namespace: string[];
  video?: RawTrack & { codec: string };
  audio?: RawTrack;
  data?: media.DataTrackInfo[];
  mediaTimeline?: media.MediaTimelineTrackInfo;
  generatedAt?: string;
}
type TimelineRecord = [number, [number, number], number];
type RawTimelineAdd = { timestampUs: string; group: string; object?: string; wallclockUs?: string };
interface RawTimelineEntry {
  name: string;
  timestampUs: string;
  group: string;
  object?: string;
  wallclockUs?: string;
}
interface Vectors {
  catalogs: { name: string; input: RawCatalog; json: string; parsed: unknown }[];
  catalogErrors: { name: string; input: RawCatalog }[];
  endCatalogs: { name: string; input: { generatedAt?: string }; json: string; parsed: unknown }[];
  endCatalogErrors: { name: string; input: { generatedAt?: string } }[];
  catalogReads: { name: string; catalogNamespace: string[]; json: string; parsed: unknown }[];
  catalogReadErrors: { name: string; catalogNamespace: string[]; json: string }[];
  loc: {
    name: string;
    timestampUs: string;
    timescale?: string;
    videoConfig?: string;
    audioConfig?: string;
    properties: [number, string][];
  }[];
  locTimestamps: {
    name: string;
    properties: [number, string][];
    trackProperties?: [number, string][];
    timestampUs: string | null;
  }[];
  urls: { name: string; origin: string; namespace: string[]; token?: string; trackName?: string; url: string }[];
  urlErrors: { origin: string; namespace: string[] }[];
  h264: { name: string; avcc: string; codec: string }[];
  timelineEntries: (RawTimelineEntry & { entry: TimelineRecord })[];
  timelineEntryErrors: RawTimelineEntry[];
  timelines: { name: string; entries: TimelineRecord[]; json: string }[];
  timelineErrors: { name: string; entries: TimelineRecord[] }[];
  timelineReads: { name: string; json: string; entries: TimelineRecord[] }[];
  timelineReadErrors: { name: string; json: string }[];
  timelineRetention: {
    name: string;
    maxEntries: string;
    adds: RawTimelineAdd[];
    json: string;
  }[];
  timelineRetentionErrors: { name: string; maxEntries: string }[];
  opusPacketDurations: { name: string; packet: string; durationUs: string | null }[];
}
const vectors = JSON.parse(readFileSync(new URL('../../spec/vectors/media.json', import.meta.url), 'utf8')) as Vectors;
const hex = (bytes: Uint8Array) => Buffer.from(bytes).toString('hex');
const bytes = (value: string) => new Uint8Array(Buffer.from(value, 'hex'));
/** The vectors' `parsed` form: init data as hex, everything else as the reader returns it. */
const readable = (catalog: media.MsfCatalog) => ({
  ...catalog,
  tracks: catalog.tracks.map(({ initData, ...fields }) => ({
    ...fields,
    ...(initData === undefined ? {} : { initData: hex(initData) }),
  })),
});
function track(raw: RawTrack): Record<string, unknown> {
  const result: Record<string, unknown> = { ...raw };
  for (const key of ['width', 'height', 'framerate', 'bitrate', 'samplerate'] as const) {
    if (raw[key] !== undefined) result[key] = Number(raw[key]);
  }
  if (raw.initData !== undefined) Object.assign(result, { initData: bytes(raw.initData) });
  return result;
}
function input(raw: RawCatalog): media.MsfCatalogInput {
  return {
    namespace: raw.namespace,
    ...(raw.video ? { video: track(raw.video) as unknown as media.VideoTrackInfo } : {}),
    ...(raw.audio ? { audio: track(raw.audio) as media.AudioTrackInfo } : {}),
    ...(raw.data ? { data: raw.data } : {}),
    ...(raw.mediaTimeline ? { mediaTimeline: raw.mediaTimeline } : {}),
    ...(raw.generatedAt !== undefined ? { generatedAt: Number(raw.generatedAt) } : {}),
  };
}

/** The vectors' form of an entry: msf-01's `[mediaTimeMs, [group, object], wallclockMs]`. */
const record = (entry: media.MediaTimelineEntry): TimelineRecord => [
  entry.mediaTimeMs,
  [entry.group, entry.object],
  entry.wallclockMs,
];
const entry = ([mediaTimeMs, [group, object], wallclockMs]: TimelineRecord): media.MediaTimelineEntry => ({
  mediaTimeMs,
  group,
  object,
  wallclockMs,
});
const timelineEntry = (raw: RawTimelineEntry) =>
  media.mediaTimelineEntry({
    timestampUs: Number(raw.timestampUs),
    group: Number(raw.group),
    ...(raw.object !== undefined ? { object: Number(raw.object) } : {}),
    ...(raw.wallclockUs !== undefined ? { wallclockUs: Number(raw.wallclockUs) } : {}),
  });

const mediaTimeline = (raw: { maxEntries: string }) =>
  media.createMediaTimeline({ maxEntries: Number(raw.maxEntries) });

describe('shared media timeline vectors', () => {
  for (const vector of vectors.timelineEntries) {
    it(`entry ${vector.name}`, () => expect(record(timelineEntry(vector))).toEqual(vector.entry));
  }
  for (const vector of vectors.timelineEntryErrors) {
    it(`rejects entry ${vector.name}`, () => expect(() => timelineEntry(vector)).toThrow(RangeError));
  }
  for (const vector of vectors.timelines) {
    it(`timeline ${vector.name}: exact bytes and reader round trip`, () => {
      const document = media.buildMediaTimeline(vector.entries.map(entry));
      expect(hex(document)).toBe(hex(new TextEncoder().encode(vector.json)));
      expect(media.parseMediaTimeline(document).map(record)).toEqual(vector.entries);
    });
  }
  for (const vector of vectors.timelineErrors) {
    it(`rejects timeline ${vector.name}`, () =>
      expect(() => media.buildMediaTimeline(vector.entries.map(entry))).toThrow(RangeError));
  }
  for (const vector of vectors.timelineReads) {
    it(`reads timeline ${vector.name}`, () =>
      expect(media.parseMediaTimeline(new TextEncoder().encode(vector.json)).map(record)).toEqual(vector.entries));
  }
  for (const vector of vectors.timelineReadErrors) {
    it(`rejects reading timeline ${vector.name}`, () =>
      expect(() => media.parseMediaTimeline(new TextEncoder().encode(vector.json))).toThrow());
  }
  it('rejects a timeline payload that is not UTF-8', () => {
    const payload = new Uint8Array([...new TextEncoder().encode('[[0,[0,0],0],"'), 0xff, 0x22, 0x5d]);
    expect(() => media.parseMediaTimeline(payload)).toThrow();
  });
  for (const vector of vectors.timelineRetention) {
    it(`retains ${vector.name}`, () => {
      const timeline = mediaTimeline(vector);
      let document = timeline.document;
      for (const add of vector.adds) {
        document = timeline.add({
          timestampUs: Number(add.timestampUs),
          group: Number(add.group),
          ...(add.object !== undefined ? { object: Number(add.object) } : {}),
          ...(add.wallclockUs !== undefined ? { wallclockUs: Number(add.wallclockUs) } : {}),
        });
      }
      const expected = hex(new TextEncoder().encode(vector.json));
      expect(hex(document)).toBe(expected);
      expect(hex(timeline.document)).toBe(expected);
      expect(timeline.entries.map(record)).toEqual(JSON.parse(vector.json));
    });
  }
  for (const vector of vectors.timelineRetentionErrors) {
    it(`rejects retention ${vector.name}`, () => expect(() => mediaTimeline(vector)).toThrow(RangeError));
  }
  it('leaves the timeline unchanged after a rejected entry', () => {
    const timeline = media.createMediaTimeline({ maxEntries: 1 });
    timeline.add({ timestampUs: 1_000, group: 0 });
    expect(() => timeline.add({ timestampUs: -1, group: 1 })).toThrow(RangeError);
    expect(new TextDecoder().decode(timeline.document)).toBe('[[1,[0,0],1]]');
  });
  it('starts each broadcast with an empty timeline', () => {
    // A supervised publisher's next generation is a new broadcast: it makes a new timeline, not a cleared one.
    media.createMediaTimeline().add({ timestampUs: 1_000, group: 0 });
    expect(new TextDecoder().decode(media.createMediaTimeline().document)).toBe('[]');
  });
  it('stamps entries from a broadcast clock', () => {
    const clock = media.createBroadcastClock({
      now: () => 2_002_000,
      originUs: 0,
      originEpochUs: 1_759_924_158_381_000,
    });
    const timeline = media.mediaTimelineEntry({ timestampUs: clock.now(), group: 1 });
    expect(record(timeline)).toEqual([1_759_924_160_383, [1, 0], 1_759_924_160_383]);
  });
});

describe('shared media vectors', () => {
  for (const vector of vectors.catalogs) {
    it(`catalog ${vector.name}: exact bytes and reader round trip`, () => {
      const document = media.buildMsfCatalog(input(vector.input));
      expect(hex(document)).toBe(hex(new TextEncoder().encode(vector.json)));
      expect(readable(media.parseMsfCatalog(document, { catalogNamespace: vector.input.namespace }))).toEqual(
        vector.parsed,
      );
    });
  }
  for (const vector of vectors.catalogErrors) {
    it(`rejects catalog ${vector.name}`, () => expect(() => media.buildMsfCatalog(input(vector.input))).toThrow());
  }
  const endInput = (raw: { generatedAt?: string }): media.MsfEndCatalogInput =>
    raw.generatedAt !== undefined ? { generatedAt: Number(raw.generatedAt) } : {};
  for (const vector of vectors.endCatalogs) {
    it(`end catalog ${vector.name}: exact bytes and reader round trip`, () => {
      const document = media.buildMsfEndCatalog(endInput(vector.input));
      expect(hex(document)).toBe(hex(new TextEncoder().encode(vector.json)));
      expect(readable(media.parseMsfCatalog(document, { catalogNamespace: ['room'] }))).toEqual(vector.parsed);
    });
  }
  for (const vector of vectors.endCatalogErrors) {
    it(`rejects end catalog ${vector.name}`, () =>
      expect(() => media.buildMsfEndCatalog(endInput(vector.input))).toThrow());
  }
  for (const vector of vectors.catalogReads) {
    it(`reads catalog ${vector.name}`, () => {
      const payload = new TextEncoder().encode(vector.json);
      expect(readable(media.parseMsfCatalog(payload, vector))).toEqual(vector.parsed);
    });
  }
  for (const vector of vectors.catalogReadErrors) {
    it(`rejects reading catalog ${vector.name}`, () =>
      expect(() => media.parseMsfCatalog(new TextEncoder().encode(vector.json), vector)).toThrow());
  }
  for (const vector of vectors.loc) {
    it(`LOC ${vector.name}`, () => {
      const properties = media.locProperties(Number(vector.timestampUs), {
        ...(vector.timescale !== undefined ? { timescale: Number(vector.timescale) } : {}),
        ...(vector.videoConfig !== undefined ? { videoConfig: bytes(vector.videoConfig) } : {}),
        ...(vector.audioConfig !== undefined ? { audioConfig: bytes(vector.audioConfig) } : {}),
      });
      expect(properties.map((p) => [p.type, typeof p.value === 'number' ? String(p.value) : hex(p.value)])).toEqual(
        vector.properties,
      );
      expect(media.locTrackProperties(Number(vector.timescale ?? '1000000'))).toEqual([properties[1]]);
    });
  }
  const pairs = (raw: [number, string][]) =>
    raw.map(([type, value]) => ({ type, value: type % 2 === 0 ? Number(value) : bytes(value) }));
  for (const vector of vectors.locTimestamps) {
    it(`LOC timestamp ${vector.name}`, () =>
      expect(
        media.locTimestampUs(pairs(vector.properties), {
          ...(vector.trackProperties ? { trackProperties: pairs(vector.trackProperties) } : {}),
        }),
      ).toBe(vector.timestampUs === null ? undefined : Number(vector.timestampUs)));
  }
  for (const vector of vectors.opusPacketDurations) {
    it(`Opus packet duration ${vector.name}`, () =>
      expect(media.opusPacketDurationUs(bytes(vector.packet))).toBe(
        vector.durationUs === null ? undefined : Number(vector.durationUs),
      ));
  }
  for (const vector of vectors.urls) {
    it(`URL ${vector.name}`, () =>
      expect(media.composeMoqSource(vector.origin, vector.namespace, vector)).toBe(vector.url));
  }
  for (const vector of vectors.urlErrors) {
    it(`rejects URL ${JSON.stringify(vector)}`, () =>
      expect(() => media.composeMoqSource(vector.origin, vector.namespace)).toThrow());
  }
  for (const vector of vectors.h264) {
    it(`AVC codec string ${vector.name}`, () => {
      expect(media.avcCodecString(bytes(vector.avcc))).toBe(vector.codec);
    });
  }
});

it.each([
  [-1, 1000000],
  [0, 0],
  [0, -1],
  [2 ** 53, 1000000],
  [2 ** 53 - 1, 2 ** 53 - 1],
])('rejects LOC timestamp %s and timescale %s', (timestamp, timescale) => {
  expect(() => media.locProperties(timestamp, { timescale })).toThrow();
});
it.each(['', '0142', '0242c014'])('rejects invalid avcC header %s', (record) => {
  expect(() => media.avcCodecString(bytes(record))).toThrow();
});
it('rejects a catalog payload that is not UTF-8', () => {
  const payload = new Uint8Array([
    ...new TextEncoder().encode('{"version":"draft-01","tracks":[],"x":"'),
    0xff,
    0x22,
    0x7d,
  ]);
  expect(() => media.parseMsfCatalog(payload, { catalogNamespace: ['room'] })).toThrow();
});
it('pins every media constant', () => {
  const spec = JSON.parse(readFileSync(new URL('../../spec/protocol.json', import.meta.url), 'utf8')) as {
    media: unknown;
  };
  expect(spec.media).toEqual({
    locDocument: media.LOC_DOCUMENT,
    msfDocument: media.MSF_DOCUMENT,
    catalogVersion: media.MSF_CATALOG_VERSION,
    defaultTimescale: media.MICROSECONDS_PER_SECOND,
    tracks: {
      catalog: media.CATALOG_TRACK,
      video: media.VIDEO_TRACK,
      audio: media.AUDIO_TRACK,
      mediaTimeline: media.MEDIA_TIMELINE_TRACK,
    },
    properties: {
      timestamp: media.LOC_TIMESTAMP,
      timescale: media.LOC_TIMESCALE,
      videoConfig: media.LOC_VIDEO_CONFIG,
      audioConfig: media.LOC_AUDIO_CONFIG,
    },
  });
});
