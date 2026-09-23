/** Independent LOC/MSF catalogs, adapted from Video.js build-catalog.ts. */
import { avcCodecString } from './h264.js';
import { AUDIO_TRACK, VIDEO_TRACK } from './loc.js';
/** Version emitted — the newest version `parse-catalog.ts` accepts. */
export const MSF_CATALOG_VERSION = 'draft-01';

export interface VideoTrackInfo {
  name?: string;
  /** WebCodecs registry codec string, e.g. `'avc1.42E01F'` or `'vp8'`. */
  codec: string;
  width?: number;
  height?: number;
  framerate?: number;
  bitrate?: number;
  /**
   * Decoder init data (WebCodecs `decoderConfig.description`, e.g. avcC for `avc`-format H.264) — emitted as an inline
   * `initDataList` entry referenced by the track's `initRef`.
   */
  initData?: Uint8Array;
}

export interface AudioTrackInfo {
  name?: string;
  /** WebCodecs registry codec string, e.g. `'opus'`. */
  codec?: string;
  /** Output sample rate (§5.2.20). */
  samplerate?: number;
  /** Channel config string (§5.2.21), e.g. `'2'` or `'5.1'`. */
  channelConfig?: string;
  bitrate?: number;
  /** Decoder init data, e.g. an AAC AudioSpecificConfig — Opus carries none. */
  initData?: Uint8Array;
}

export interface DataTrackInfo {
  name: string;
  /**
   * MSF role label (§5.2.6) classifying the non-media track for consumers, e.g. `'data'` or `'metadata'`. MUST NOT be a
   * media role: the entry carries no codec or media fields, so a media role would round-trip into an undecodable
   * renderable track. The builder rejects these roles.
   */
  role?: string;
}

export interface MsfCatalogInput {
  /** Track namespace tuple every track is published under. */
  namespace: readonly string[];
  video?: VideoTrackInfo;
  audio?: AudioTrackInfo;
  /**
   * Application data tracks published beside the media — advertised so consumers can discover and subscribe to them,
   * but carrying none of the media fields (`mediaKindOf` on the parse side classifies them as non-renderable
   * plumbing).
   */
  data?: readonly DataTrackInfo[];
  /** Catalog generation time (§5.2.24), epoch milliseconds. */
  generatedAt?: number;
}

function pruneUndefined(value: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(value).filter(([, entry]) => entry !== undefined));
}

/** Inverse of `parse-catalog.ts`'s `base64ToBytes`. */
function bytesToBase64(bytes: Uint8Array): string {
  let binary = '';

  for (const byte of bytes) binary += String.fromCharCode(byte);

  return btoa(binary);
}

/** One inline `initDataList` entry (§5.2.16). */
interface MsfInitDataEntry {
  id: string;
  type: 'inline';
  data: string;
}

/**
 * Register a track's init data and return the `initRef` naming it. Ids derive from the track name, which is unique
 * within the catalog.
 */
function registerInitData(entries: MsfInitDataEntry[], name: string, initData: Uint8Array | undefined) {
  if (initData === undefined || initData.length === 0) return {};

  const id = `${name}-init`;

  entries.push({ id, type: 'inline', data: bytesToBase64(initData) });
  return { initRef: id };
}

/**
 * Build the publisher's independent MSF catalog JSON.
 *
 * Namespaces are emitted explicitly per track as the path-joined string form `parse-catalog.ts` splits back into tuple
 * fields — the same (interop-pending) encoding the parser assumes, so the pair stays self-consistent whatever the
 * drafts settle on.
 */
export function buildMsfCatalog(input: MsfCatalogInput): Uint8Array {
  if (input.namespace.length === 0 || input.namespace.some((field) => !field || field.includes('/')))
    throw new Error('catalog namespace fields must be non-empty and contain no slash');
  const video = input.video ? { ...input.video, name: input.video.name ?? VIDEO_TRACK } : undefined;
  const audio = input.audio
    ? { codec: 'opus', samplerate: 48000, channelConfig: '2', ...input.audio, name: input.audio.name ?? AUDIO_TRACK }
    : undefined;
  const names = [video, audio, ...(input.data ?? [])].filter((track) => track !== undefined).map((track) => track.name);
  if (names.some((name) => !name) || new Set(names).size !== names.length)
    throw new Error('catalog track names must be non-empty and unique');
  const mediaRoles = new Set(['video', 'audio', 'caption', 'subtitle', 'audiodescription', 'signlanguage']);
  if (input.data?.some((track) => track.role !== undefined && mediaRoles.has(track.role)))
    throw new Error('data tracks must not use media roles');
  const h264 = video !== undefined && /^(avc1|avc3)(\.|$)/.test(video.codec);
  const namespace = input.namespace.join('/');
  const shared = { namespace, packaging: 'loc', isLive: true };
  const initDataList: MsfInitDataEntry[] = [];
  const tracks: Record<string, unknown>[] = [];

  if (video && (!h264 || (video.initData !== undefined && video.initData.length > 0))) {
    const codec =
      h264 && video.initData ? video.codec.slice(0, 4) + avcCodecString(video.initData).slice(4) : video.codec;
    tracks.push(
      pruneUndefined({
        ...shared,
        ...registerInitData(initDataList, video.name, video.initData),
        name: video.name,
        role: 'video',
        codec,
        width: video.width,
        height: video.height,
        framerate: video.framerate,
        bitrate: video.bitrate,
      })
    );
  }

  if (audio) {
    tracks.push(
      pruneUndefined({
        ...shared,
        ...registerInitData(initDataList, audio.name, audio.initData),
        name: audio.name,
        role: 'audio',
        codec: audio.codec,
        samplerate: audio.codec === 'opus' ? 48000 : audio.samplerate,
        channelConfig: audio.channelConfig,
        bitrate: audio.bitrate,
      })
    );
  }

  // Data tracks carry no media fields, so the player treats them as non-renderable.
  for (const data of input.data ?? []) {
    tracks.push(
      pruneUndefined({
        ...shared,
        name: data.name,
        role: data.role,
      })
    );
  }

  return new TextEncoder().encode(
    JSON.stringify(
      pruneUndefined({
        version: MSF_CATALOG_VERSION,
        generatedAt: input.generatedAt,
        isComplete: true,
        tracks,
        initDataList: initDataList.length > 0 ? initDataList : undefined,
      }),
      (_key, value: unknown) => {
        if (typeof value === 'string' && /[\uD800-\uDFFF]/u.test(value))
          throw new Error('catalog strings must be valid Unicode');
        if (typeof value === 'number') {
          if (!Number.isFinite(value)) throw new Error('catalog numbers must be finite');
          if (Number.isInteger(value) && !Number.isSafeInteger(value))
            throw new Error('catalog integers must be safe integers');
        }
        return value;
      }
    )
  );
}
