import { readFileSync } from 'node:fs';
import * as media from '@mux/moq-media';
import { describe, expect, it } from 'vitest';
import { parseIndependentCatalog } from '../packages/moq-media/src/parse-catalog.js';

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
  catalogs: { name: string; input: RawCatalog; json: string }[];
  catalogErrors: { name: string; input: RawCatalog }[];
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
  h264: { name: string; annexb: string; sps: string[]; pps: string[]; avcc: string; codec: string }[];
  h264Errors: { name: string; sps: string[]; pps: string[] }[];
  accessUnits: { name: string; annexb: string; avcc: string; allAvcc: string }[];
}
const vectors = JSON.parse(readFileSync(new URL('../../spec/vectors/media.json', import.meta.url), 'utf8')) as Vectors;
const hex = (bytes: Uint8Array) => Buffer.from(bytes).toString('hex');
const bytes = (value: string) => new Uint8Array(Buffer.from(value, 'hex'));
const text = (value: Uint8Array) => new TextDecoder().decode(value);
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
    it(`catalog ${vector.name}: exact bytes and Video.js parser round trip`, () => {
      const document = media.buildMsfCatalog(input(vector.input));
      expect(hex(document)).toBe(hex(new TextEncoder().encode(vector.json)));
      const parsed = parseIndependentCatalog(text(document), { catalogNamespace: vector.input.namespace });
      const raw = JSON.parse(vector.json) as {
        tracks: { name: string; role?: string; codec?: string; initRef?: string }[];
      };
      expect(parsed.tracks.map(({ name, role, codec }) => ({ name, role, codec }))).toEqual(
        raw.tracks.map(({ name, role, codec }) => ({ name, role, codec })),
      );
      expect(parsed.tracks.every((entry) => entry.namespace.join('/') === vector.input.namespace.join('/'))).toBe(true);
      for (const entry of parsed.tracks) {
        const original = [vector.input.video, vector.input.audio].find(
          (value) => value && (value.name ?? (value === vector.input.video ? 'video' : 'audio')) === entry.name,
        );
        if (original?.initData !== undefined) expect(hex(entry.initData ?? new Uint8Array())).toBe(original.initData);
      }
    });
  }
  for (const vector of vectors.catalogErrors) {
    it(`rejects catalog ${vector.name}`, () => expect(() => media.buildMsfCatalog(input(vector.input))).toThrow());
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
    it(`H.264 ${vector.name}`, () => {
      const data = bytes(vector.annexb);
      const [sps, pps] = media.extractParameterSets(data);
      expect(sps.map(hex)).toEqual(vector.sps);
      expect(pps.map(hex)).toEqual(vector.pps);
      const config = media.H264Config.fromAnnexb(data);
      expect(hex(config.avcc)).toBe(vector.avcc);
      expect(config.codec).toBe(vector.codec);
    });
  }
  for (const vector of vectors.h264Errors) {
    it(`rejects avcC ${vector.name}`, () => {
      expect(() => media.buildAvcc(vector.sps.map(bytes), vector.pps.map(bytes))).toThrow(/profiles/);
      const annexb = bytes([...vector.sps, ...vector.pps].map((nal) => `000001${nal}`).join(''));
      expect(() => media.H264Config.fromAnnexb(annexb)).toThrow(/profiles/);
    });
  }
  for (const vector of vectors.accessUnits) {
    it(`access unit ${vector.name}`, () => {
      const data = bytes(vector.annexb);
      expect(hex(media.annexbToAvcc(data))).toBe(vector.avcc);
      expect(hex(media.annexbToAvcc(data, { dropParameterSets: false }))).toBe(vector.allAvcc);
      expect(hex(new media.H264Config(bytes('0142c014')).toAvcc(data))).toBe(vector.avcc);
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
it('checks parameter-set type, count and length', () => {
  const sps = bytes('6742c014');
  const pps = bytes('68');
  for (const [ss, ps] of [
    [[], [pps]],
    [[sps], []],
    [new Array<Uint8Array>(32).fill(sps), [pps]],
    [[sps], new Array<Uint8Array>(256).fill(pps)],
    [[pps], [pps]],
    [[sps], [new Uint8Array(65536).fill(0x68)]],
  ] as const) {
    expect(() => media.buildAvcc(ss, ps)).toThrow();
  }
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
