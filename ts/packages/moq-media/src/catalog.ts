/** Independent LOC/MSF catalogs, adapted from Video.js build-catalog.ts. */
import { avcCodecString } from './h264.js';
import { AUDIO_TRACK, MEDIA_TIMELINE_TRACK, VIDEO_TRACK } from './loc.js';
/** Version emitted — the newest version `parseMsfCatalog` accepts. */
export const MSF_CATALOG_VERSION = 'draft-01';

/**
 * The video track {@link buildMsfCatalog} advertises. An H.264 (`avc1`/`avc3`) track without `initData` is left out
 * of the catalog, since its codec string is derived from the avcC record.
 */
export interface VideoTrackInfo {
  /** Track name. Default {@link VIDEO_TRACK}. */
  name?: string;
  /** WebCodecs registry codec string, e.g. `'avc1.42E01F'` or `'vp8'`. */
  codec: string;
  /** Coded width in pixels. */
  width?: number;
  /** Coded height in pixels. */
  height?: number;
  /** Frames per second. */
  framerate?: number;
  /** Bits per second. */
  bitrate?: number;
  /**
   * Decoder init data (WebCodecs `decoderConfig.description`, e.g. avcC for `avc`-format H.264) — emitted as an inline
   * `initDataList` entry referenced by the track's `initRef`.
   */
  initData?: Uint8Array;
  /**
   * Render group (msf-01 §5.2.11): tracks sharing one play together on one timeline. Default 1; give a second source,
   * such as a screen share and its audio, its own.
   */
  renderGroup?: number;
  /**
   * Alternate group (msf-01 §5.2.12): tracks sharing one are alternate versions of the same content, such as simulcast
   * layers, and a viewer subscribes to one of them. Members share a role and a render group.
   */
  altGroup?: number;
}

/** The audio track {@link buildMsfCatalog} advertises. Unset fields default to Opus, 48 kHz, stereo. */
export interface AudioTrackInfo {
  /** Track name. Default {@link AUDIO_TRACK}. */
  name?: string;
  /** WebCodecs registry codec string, e.g. `'opus'`. */
  codec?: string;
  /** Output sample rate (§5.2.20). Always 48000 for Opus. */
  samplerate?: number;
  /** Channel config string (§5.2.21), e.g. `'2'` or `'5.1'`. */
  channelConfig?: string;
  /** Bits per second. */
  bitrate?: number;
  /** Decoder init data, e.g. an AAC AudioSpecificConfig — Opus carries none. */
  initData?: Uint8Array;
  /**
   * Render group (msf-01 §5.2.11): tracks sharing one play together on one timeline. Default 1; give a second source,
   * such as a screen share and its audio, its own.
   */
  renderGroup?: number;
  /**
   * Alternate group (msf-01 §5.2.12): tracks sharing one are alternate versions of the same content, such as simulcast
   * layers, and a viewer subscribes to one of them. Members share a role and a render group.
   */
  altGroup?: number;
}

/** One entry of {@link MsfCatalogInput.media}: a video or audio track, tagged with its MSF role. */
export type MediaTrackInfo =
  | ({
      /** MSF role (§5.2.6) of the entry. */
      role: 'video';
    } & VideoTrackInfo)
  | ({
      /** MSF role (§5.2.6) of the entry. */
      role: 'audio';
    } & AudioTrackInfo);

/** A non-media track {@link buildMsfCatalog} advertises, such as application data. */
export interface DataTrackInfo {
  /** Track name, unique within the catalog. */
  name: string;
  /**
   * MSF role label (§5.2.6) classifying the non-media track for consumers, e.g. `'data'` or `'metadata'`. MUST NOT be a
   * media role: the entry carries no codec or media fields, so a media role would round-trip into an undecodable
   * renderable track. The builder rejects these roles.
   */
  role?: string;
}

/**
 * The Media Timeline track (msf-01 §7) {@link buildMsfCatalog} announces. The entry is listed only while a track it
 * depends on is listed; see spec/media-layer.md, "Media Timeline track".
 */
export interface MediaTimelineTrackInfo {
  /** Track name. Default {@link MEDIA_TIMELINE_TRACK}. */
  name?: string;
  /**
   * The video and audio tracks the timeline applies to (§5.2.14, §7.2); its locations point into the first. Default:
   * the first video track, then the first audio track, whichever exist.
   */
  depends?: readonly string[];
}

/** What {@link buildMsfCatalog} advertises: the namespace and the tracks published under it. */
export interface MsfCatalogInput {
  /** Track namespace tuple every track is published under. */
  namespace: readonly string[];
  /** A video track, if any: shorthand for a one-entry {@link media} list, listed first. */
  video?: VideoTrackInfo;
  /** An audio track, if any: shorthand for a one-entry {@link media} list, listed after `video`. */
  audio?: AudioTrackInfo;
  /**
   * Video and audio tracks, listed in this order after `video` and `audio`: simulcast layers sharing an `altGroup`,
   * or several sources in their own render groups.
   */
  media?: readonly MediaTrackInfo[];
  /**
   * Application data tracks published beside the media — advertised so consumers can discover and subscribe to them,
   * but carrying none of the media fields (`mediaKindOf` on the parse side classifies them as non-renderable
   * plumbing).
   */
  data?: readonly DataTrackInfo[];
  /** The Media Timeline track that maps the broadcast's media time to wall-clock time, if any. */
  mediaTimeline?: MediaTimelineTrackInfo;
  /** Catalog generation time (§5.2.24), epoch milliseconds. */
  generatedAt?: number;
}

function pruneUndefined(value: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(value).filter(([, entry]) => entry !== undefined));
}

/** Inverse of `parseMsfCatalog`'s `base64ToBytes`. */
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
 * The render group (msf-01 §5.2.11) a video or audio entry joins unless it names another: tracks stamped on one
 * broadcast timeline and meant to play together. The builder sets no target latency or buffer, which §5.2.8 and
 * §5.2.9 require to be identical across a render group.
 */
const MEDIA_RENDER_GROUP = 1;

const isGroupNumber = (value: number | undefined) => value === undefined || (Number.isSafeInteger(value) && value >= 0);

/**
 * Build the publisher's independent MSF catalog JSON for a live broadcast.
 *
 * The catalog leaves `isComplete` out: msf-01 §5.1.3 forbids it while false, and once present it can never be
 * removed. End a broadcast with {@link buildMsfEndCatalog}.
 *
 * Namespaces are emitted explicitly per track as the slash-joined string form `parseMsfCatalog` splits back into
 * tuple fields — the catalog dialect's encoding, which msf-01 leaves open, so the pair stays self-consistent whatever
 * the drafts settle on.
 */
export function buildMsfCatalog(input: MsfCatalogInput): Uint8Array {
  if (input.namespace.length === 0 || input.namespace.some((field) => !field || field.includes('/')))
    throw new Error('catalog namespace fields must be non-empty and contain no slash');
  const media = [
    ...(input.video ? [{ ...input.video, role: 'video' as const }] : []),
    ...(input.audio ? [{ ...input.audio, role: 'audio' as const }] : []),
    ...(input.media ?? []),
  ].map((track) =>
    track.role === 'video'
      ? { ...track, name: track.name ?? VIDEO_TRACK }
      : { codec: 'opus', samplerate: 48000, channelConfig: '2', ...track, name: track.name ?? AUDIO_TRACK }
  );
  const timeline = input.mediaTimeline
    ? { ...input.mediaTimeline, name: input.mediaTimeline.name ?? MEDIA_TIMELINE_TRACK }
    : undefined;
  const names = [...media, timeline, ...(input.data ?? [])]
    .filter((track) => track !== undefined)
    .map((track) => track.name);
  if (names.some((name) => !name) || new Set(names).size !== names.length)
    throw new Error('catalog track names must be non-empty and unique');
  const mediaRoles = new Set(['video', 'audio', 'caption', 'subtitle', 'audiodescription', 'signlanguage']);
  if (input.data?.some((track) => track.role !== undefined && mediaRoles.has(track.role)))
    throw new Error('data tracks must not use media roles');
  if (media.some((track) => track.role !== 'video' && track.role !== 'audio'))
    throw new Error('media tracks must have the role video or audio');
  if (media.some((track) => !isGroupNumber(track.renderGroup) || !isGroupNumber(track.altGroup)))
    throw new Error('render and alternate groups must be non-negative safe integers');
  const alternates = new Map<number, { role: string; renderGroup: number }>();
  for (const { altGroup, role, renderGroup = MEDIA_RENDER_GROUP } of media) {
    if (altGroup === undefined) continue;
    const first = alternates.get(altGroup) ?? { role, renderGroup };
    if (first.role !== role || first.renderGroup !== renderGroup)
      throw new Error('alternate group members must share a role and a render group');
    alternates.set(altGroup, first);
  }
  const mediaNames = media.map((track) => track.name);
  const defaultDepends = (['video', 'audio'] as const)
    .map((role) => media.find((track) => track.role === role)?.name)
    .filter((name) => name !== undefined);
  const depends = timeline ? [...(timeline.depends ?? defaultDepends)] : [];
  if (
    timeline &&
    (depends.length === 0 || new Set(depends).size !== depends.length || depends.some((n) => !mediaNames.includes(n)))
  )
    throw new Error('media timeline depends must name distinct video or audio tracks of this catalog');
  const namespace = input.namespace.join('/');
  const shared = { namespace, packaging: 'loc', isLive: true };
  const initDataList: MsfInitDataEntry[] = [];
  const tracks: Record<string, unknown>[] = [];

  for (const track of media) {
    const groups = { renderGroup: track.renderGroup ?? MEDIA_RENDER_GROUP, altGroup: track.altGroup };
    if (track.role === 'video') {
      const h264 = /^(avc1|avc3)(\.|$)/.test(track.codec);
      if (h264 && (track.initData === undefined || track.initData.length === 0)) continue;
      const codec =
        h264 && track.initData ? track.codec.slice(0, 4) + avcCodecString(track.initData).slice(4) : track.codec;
      tracks.push(
        pruneUndefined({
          ...shared,
          ...registerInitData(initDataList, track.name, track.initData),
          name: track.name,
          role: 'video',
          ...groups,
          codec,
          width: track.width,
          height: track.height,
          framerate: track.framerate,
          bitrate: track.bitrate,
        })
      );
    } else {
      tracks.push(
        pruneUndefined({
          ...shared,
          ...registerInitData(initDataList, track.name, track.initData),
          name: track.name,
          role: 'audio',
          ...groups,
          codec: track.codec,
          samplerate: track.codec === 'opus' ? 48000 : track.samplerate,
          channelConfig: track.channelConfig,
          bitrate: track.bitrate,
        })
      );
    }
  }

  const listed = new Set(tracks.map(({ name }) => name));
  const indexed = depends.filter((name) => listed.has(name));
  if (timeline && indexed.length > 0) {
    tracks.push({
      ...shared,
      packaging: 'mediatimeline',
      name: timeline.name,
      role: 'mediatimeline',
      mimetype: 'application/json',
      depends: indexed,
    });
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

  return encodeCatalog({
    version: MSF_CATALOG_VERSION,
    generatedAt: input.generatedAt,
    tracks,
    initDataList: initDataList.length > 0 ? initDataList : undefined,
  });
}

/** What {@link buildMsfEndCatalog} stamps on the end-of-broadcast catalog. */
export interface MsfEndCatalogInput {
  /** Catalog generation time (§5.2.24), epoch milliseconds. */
  generatedAt?: number;
}

/**
 * Build the catalog that ends a broadcast (msf-01 §11.3): `isComplete: true` and no tracks.
 *
 * It commits that no track will be added and no content published, so send it only for a broadcast that ends on
 * purpose, once every media track has ended (closing a track sends PUBLISH_DONE Track Ended), and never on a crash
 * or lost connection.
 */
export function buildMsfEndCatalog(input: MsfEndCatalogInput = {}): Uint8Array {
  return encodeCatalog({ version: MSF_CATALOG_VERSION, generatedAt: input.generatedAt, isComplete: true, tracks: [] });
}

function encodeCatalog(catalog: Record<string, unknown>): Uint8Array {
  return new TextEncoder().encode(
    JSON.stringify(pruneUndefined(catalog), (_key, value: unknown) => {
      if (typeof value === 'string' && /[\uD800-\uDFFF]/u.test(value))
        throw new Error('catalog strings must be valid Unicode');
      if (typeof value === 'number') {
        if (!Number.isFinite(value)) throw new Error('catalog numbers must be finite');
        if (Number.isInteger(value) && !Number.isSafeInteger(value))
          throw new Error('catalog integers must be safe integers');
      }
      return value;
    })
  );
}
