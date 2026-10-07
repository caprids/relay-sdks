// biome-ignore-all lint/complexity/useLiteralKeys: TypeScript requires bracket access for index signatures.
/** Independent MSF catalog reader, extracted from Video.js parse-catalog.ts. */
function isString(value: unknown): value is string {
  return typeof value === 'string';
}
function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
function pruneUndefined<T extends object>(value: T): { [K in keyof T]?: Exclude<T[K], undefined> } {
  return Object.fromEntries(Object.entries(value).filter(([, entry]) => entry !== undefined)) as {
    [K in keyof T]?: Exclude<T[K], undefined>;
  };
}

/** One track object of an independent MSF catalog (msf-01 §5.2), as `parseMsfCatalog` reads it. */
export interface MsfCatalogTrack {
  /** Namespace tuple: the track's own slash-joined `namespace` split into fields, else the catalog track's. */
  namespace: string[];
  /** Track name, as declared. */
  name: string;
  /** Payload encapsulation as declared (`'loc'`, or e.g. moq-rs `'cmaf'`/`'legacy'`); `''` when absent. */
  packaging: string;
  /** `false` when absent. */
  isLive: boolean;
  /** MSF role (§5.2.6), e.g. `'video'`, `'audio'` or `'data'`. */
  role?: string;
  /** WebCodecs registry codec string. */
  codec?: string;
  /** Width in pixels; integral values only. */
  width?: number;
  /** Height in pixels; integral values only. */
  height?: number;
  /** Frames per second. */
  framerate?: number;
  /** Audio sample rate in hertz; integral values only. */
  samplerate?: number;
  /** Channel config string, e.g. `'2'` or `'5.1'`. */
  channelConfig?: string;
  /** Bits per second; integral values only. */
  bitrate?: number;
  /** Render group (§5.2.11): tracks sharing one are meant to play together; integral values only. */
  renderGroup?: number;
  /** Decoder init data, resolved from `initRef` through the catalog's inline `initDataList`. */
  initData?: Uint8Array;
}

/** An independent MSF catalog (msf-01 §5.1), as `parseMsfCatalog` reads it. */
export interface MsfCatalog {
  /** The catalog's declared version: `'draft-01'`, or the lenient alias `'1'`. */
  version: string;
  /** Catalog generation time (§5.1.2), epoch milliseconds. */
  generatedAt?: number;
  /** `true` only when the catalog declares the broadcast complete (§5.1.3). */
  isComplete: boolean;
  /** The track entries that have a string `name`, in catalog order. */
  tracks: MsfCatalogTrack[];
}

/** Options for {@link parseMsfCatalog}. */
export interface ParseMsfCatalogOptions {
  /** Namespace of the catalog track itself — inherited by tracks that omit one (§5.2.2). */
  catalogNamespace: readonly string[];
}

function base64ToBytes(base64: string): Uint8Array {
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);

  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);

  return bytes;
}

/**
 * Split a catalog namespace string into its tuple fields. msf-01 does not say how a tuple is spelled in catalog
 * JSON; this dialect (Video.js, and `buildMsfCatalog`) joins the fields with `/`, which the builder therefore
 * rejects inside a field. Empty fields are dropped.
 */
function parseNamespaceString(namespace: string): string[] {
  return namespace.split('/').filter((field) => field.length > 0);
}

function parseInitDataList(raw: unknown): Map<string, Uint8Array> {
  const initData = new Map<string, Uint8Array>();

  if (!Array.isArray(raw)) return initData;

  for (const entry of raw) {
    if (!isPlainObject(entry)) continue;

    const { id, type, data } = entry;
    if (!isString(id) || !isString(data)) continue;

    if (type !== undefined && type !== 'inline') continue;

    initData.set(id, base64ToBytes(data));
  }

  return initData;
}

const number = (value: unknown): number | undefined => (typeof value === 'number' ? value : undefined);
/** Counts (pixels, bits per second, hertz, milliseconds) are read only when integral. */
const integer = (value: unknown): number | undefined => (Number.isInteger(value) ? (value as number) : undefined);
const string = (value: unknown): string | undefined => (isString(value) ? value : undefined);

function parseCatalogTrack(
  raw: Record<string, unknown>,
  catalogNamespace: readonly string[],
  initDataList: Map<string, Uint8Array>
): MsfCatalogTrack | null {
  const name = raw['name'];
  if (!isString(name)) return null;

  const initRef = string(raw['initRef']);

  return {
    namespace: isString(raw['namespace']) ? parseNamespaceString(raw['namespace']) : [...catalogNamespace],
    name,
    packaging: string(raw['packaging']) ?? '',
    isLive: raw['isLive'] === true,
    ...pruneUndefined({
      role: string(raw['role']),
      codec: string(raw['codec']),
      width: integer(raw['width']),
      height: integer(raw['height']),
      framerate: number(raw['framerate']),
      samplerate: integer(raw['samplerate']),
      channelConfig: string(raw['channelConfig']),
      bitrate: integer(raw['bitrate']),
      renderGroup: integer(raw['renderGroup']),
      initData: initRef === undefined ? undefined : initDataList.get(initRef),
    }),
  };
}

// §5.1.1: a subscriber MUST NOT parse a catalog version it does not understand. Versions follow the 'draft-XX'
// convention; the bare '1' is a lenient alias deployed producers emit.
const SUPPORTED_MSF_VERSIONS = new Set(['draft-01', '1']);

/**
 * Read an independent MSF catalog object's payload into its track descriptions.
 *
 * Throws on a payload that is not a UTF-8 JSON object, a missing or unsupported `version`, a missing `tracks` array,
 * or a delta update (`deltaUpdate`, not supported). Everything else is lenient, as msf-01 requires: unknown fields
 * and fields of the wrong type are ignored, track entries without a string `name` are skipped, and packaging is
 * returned as declared, not interpreted.
 */
export function parseMsfCatalog(payload: Uint8Array, options: ParseMsfCatalogOptions): MsfCatalog {
  const raw: unknown = JSON.parse(new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(payload));
  if (!isPlainObject(raw)) throw new Error('MSF catalog is not a JSON object');

  if (raw['deltaUpdate'] !== undefined) throw new Error('MSF delta updates are not supported');

  if (!isString(raw['version'])) throw new Error('MSF catalog is missing its version');

  if (!SUPPORTED_MSF_VERSIONS.has(raw['version'])) {
    throw new Error(`unsupported MSF catalog version ${raw['version']}`);
  }

  if (!Array.isArray(raw['tracks'])) throw new Error('MSF catalog is missing its tracks array');

  const initDataList = parseInitDataList(raw['initDataList']);
  const tracks = raw['tracks']
    .filter(isPlainObject)
    .map((entry) => parseCatalogTrack(entry, options.catalogNamespace, initDataList))
    .filter((track): track is MsfCatalogTrack => track !== null);

  const generatedAt = integer(raw['generatedAt']);

  return {
    version: raw['version'],
    ...(generatedAt === undefined ? {} : { generatedAt }),
    isComplete: raw['isComplete'] === true,
    tracks,
  };
}
