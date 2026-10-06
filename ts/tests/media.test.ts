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
  generatedAt?: string;
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
  urls: { name: string; origin: string; namespace: string[]; token?: string; trackName?: string; url: string }[];
  urlErrors: { origin: string; namespace: string[] }[];
  h264: { name: string; avcc: string; codec: string }[];
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
    ...(raw.generatedAt !== undefined ? { generatedAt: Number(raw.generatedAt) } : {}),
  };
}

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
    tracks: { catalog: media.CATALOG_TRACK, video: media.VIDEO_TRACK, audio: media.AUDIO_TRACK },
    properties: {
      timestamp: media.LOC_TIMESTAMP,
      timescale: media.LOC_TIMESCALE,
      videoConfig: media.LOC_VIDEO_CONFIG,
      audioConfig: media.LOC_AUDIO_CONFIG,
    },
  });
});
