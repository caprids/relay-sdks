// biome-ignore-all lint/complexity/useLiteralKeys: TypeScript requires bracket access for index signatures.
/** Independent-catalog extraction from Video.js. Internal builder compatibility oracle. */
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

/** Parsed MSF track-object fields the engine consumes (§5.2). */
export interface MoqCatalogTrack {
  namespace: string[];
  name: string;
  packaging: string;
  isLive: boolean;
  role?: string;
  label?: string;
  language?: string;
  codec?: string;
  mimeType?: string;
  bitrate?: number;
  avgBitrate?: number;
  width?: number;
  height?: number;
  framerate?: number;
  timescale?: number;
  samplerate?: number;
  channelConfig?: string;
  renderGroup?: number;
  altGroup?: number;
  targetLatency?: number;
  /**
   * Publisher-declared minimum buffer in milliseconds (msf-01 §5.2.9) — the packaging/encode jitter a receiver has to
   * absorb even on a perfect path. Read by the adaptive latency controller as one additive term of its margin; unused
   * by the fixed-setpoint chain.
   */
  jitter?: number;
  buffers?: { target?: number; min?: number; max?: number };
  maxGopDuration?: number;
  maxGroupDuration?: number;
  temporalId?: number;
  spatialId?: number;
  dependencies?: string[];
  /** Decoder init data (`description`), resolved from initRef/initDataList. */
  initData?: Uint8Array;
  authInfo?: Record<string, unknown>;
}

export interface MoqCatalog {
  version: string;
  generatedAt?: number;
  isComplete?: boolean;
  tracks: MoqCatalogTrack[];
  /** Retained init-data map so delta-added tracks can resolve initRef (§5.3). */
  initDataList?: Map<string, Uint8Array>;
}

function base64ToBytes(base64: string): Uint8Array {
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);

  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);

  return bytes;
}

/** Split a catalog namespace string into its tuple fields. */
function parseNamespaceString(namespace: string): string[] {
  // The MSF catalog examples carry namespaces as path-like strings
  // ("conference.example.com/conference123/alice"); the tuple fields are
  // the path segments. (Interop check pending — the encoding of tuple
  // boundaries inside catalog JSON is not spelled out by msf-01.)
  return namespace.split('/').filter((field) => field.length > 0);
}

interface RawCatalog {
  version?: unknown;
  generatedAt?: unknown;
  isComplete?: unknown;
  tracks?: unknown;
  publishTracks?: unknown;
  deltaUpdate?: unknown;
  initDataList?: unknown;
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

/**
 * Extract only the fields present in a raw track entry — no defaults. Clone operations layer this partial over the
 * parent so inherited values survive (§5.1.6: a clone inherits all attributes except Track Name).
 */
function parseCatalogTrackFields(
  raw: Record<string, unknown>,
  initDataList: Map<string, Uint8Array>
): { [K in keyof MoqCatalogTrack]?: MoqCatalogTrack[K] | undefined } {
  const number = (value: unknown): number | undefined => (typeof value === 'number' ? value : undefined);
  const initRef = isString(raw['initRef']) ? raw['initRef'] : undefined;

  const fields: { [K in keyof MoqCatalogTrack]?: MoqCatalogTrack[K] | undefined } = {};

  if (isString(raw['name'])) fields.name = raw['name'];

  if (isString(raw['namespace'])) fields.namespace = parseNamespaceString(raw['namespace']);

  if (isString(raw['packaging'])) fields.packaging = raw['packaging'];

  if (typeof raw['isLive'] === 'boolean') fields.isLive = raw['isLive'];

  if (isString(raw['role'])) fields.role = raw['role'];

  if (isString(raw['label'])) fields.label = raw['label'];

  if (isString(raw['lang'])) fields.language = raw['lang'];

  if (isString(raw['codec'])) fields.codec = raw['codec'];

  if (isString(raw['mimeType'])) fields.mimeType = raw['mimeType'];

  if (isString(raw['channelConfig'])) fields.channelConfig = raw['channelConfig'];

  fields.bitrate = number(raw['bitrate']);
  fields.avgBitrate = number(raw['avgBitrate']);
  fields.width = number(raw['width']);
  fields.height = number(raw['height']);
  fields.framerate = number(raw['framerate']);
  fields.timescale = number(raw['timescale']);
  fields.samplerate = number(raw['samplerate']);
  fields.renderGroup = number(raw['renderGroup']);
  fields.altGroup = number(raw['altGroup']);
  fields.targetLatency = number(raw['targetLatency']);
  fields.jitter = number(raw['jitter']);
  fields.maxGopDuration = number(raw['maxGopDuration']);
  fields.maxGroupDuration = number(raw['maxGroupDuration']);
  fields.temporalId = number(raw['temporalId']);
  fields.spatialId = number(raw['spatialId']);

  if (isPlainObject(raw['buffers'])) {
    fields.buffers = pruneUndefined({
      target: number(raw['buffers']['target']),
      min: number(raw['buffers']['min']),
      max: number(raw['buffers']['max']),
    });
  }

  if (Array.isArray(raw['depends'])) fields.dependencies = raw['depends'].filter(isString);

  if (initRef) fields.initData = initDataList.get(initRef);

  if (isPlainObject(raw['authInfo'])) fields.authInfo = raw['authInfo'];

  return fields;
}

function parseCatalogTrack(
  raw: Record<string, unknown>,
  fallbackNamespace: string[],
  initDataList: Map<string, Uint8Array>
): MoqCatalogTrack | null {
  const name = raw['name'];
  if (!isString(name)) return null;

  return {
    namespace: fallbackNamespace,
    packaging: '',
    isLive: false,
    ...pruneUndefined(parseCatalogTrackFields(raw, initDataList)),
    name,
  };
}

// ============================================================================
// Catalog updates (independent + delta)
// ============================================================================

// §5.1.1: a subscriber MUST NOT parse a catalog version it does not
// understand; versions follow the 'draft-XX' convention. The bare '1' is a
// lenient interop alias seen in early catalogs. (Interop check pending —
// verify which form publishers actually emit.)
const SUPPORTED_MSF_VERSIONS = new Set(['draft-01', '1']);

export interface MoqCatalogUpdateOptions {
  /** Namespace of the catalog track itself — inherited by tracks that omit one (§5.2.2). */
  catalogNamespace: string[];
}

/** Parse an independent catalog for builder compatibility tests; delta updates are rejected. */
export function parseIndependentCatalog(text: string, options: MoqCatalogUpdateOptions): MoqCatalog {
  const substituted = JSON.parse(text);
  if (!isPlainObject(substituted)) throw new Error('MSF catalog is not a JSON object');

  const raw = substituted as RawCatalog;

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
    .filter((track): track is MoqCatalogTrack => track !== null);

  const catalog: MoqCatalog = { version: raw['version'], tracks, initDataList };

  if (typeof raw['generatedAt'] === 'number') catalog.generatedAt = raw['generatedAt'];

  if (raw['isComplete'] === true) catalog.isComplete = true;

  return catalog;
}
