/**
 * MOQT control-message wire codecs (moq-transport draft-20 §10).
 *
 * Every control or request-stream message is framed as:
 *
 *     MOQT Control Message {
 *       Message Type (vi64),
 *       Message Length (16),
 *       Message Body (..),
 *     }
 *
 * Scope follows the subscribe-only engine: the encoders cover what a subscriber sends (SETUP, SUBSCRIBE, FETCH,
 * TRACK_STATUS, SUBSCRIBE_NAMESPACE, REQUEST_UPDATE, GOAWAY, and REQUEST_OK / REQUEST_ERROR responses to incoming
 * PUBLISH); the decoders cover what it receives. All draft-version specifics live in this directory — this is the
 * churn-absorption layer for pre-WGLC drafts.
 *
 * Where the draft text and the deployed relay (moq-relay, moq-dev/moq) disagree on a wire detail, the codec follows the
 * relay and says so at the site — interop is with the relay, not the prose.
 *
 * Namespace fields and track names are byte strings on the wire; this codec surfaces them as UTF-8 `string`s, which is
 * what MSF (the streaming format this engine speaks) constrains them to.
 */
import { ByteReader, ByteWriter, utf8Decode, utf8Encode } from './bytes.js';
import { MoqtProtocolError } from './errors.js';
import { decodeVarint, MAX_VARINT_VALUE } from './varint.js';

// ============================================================================
// Wire constants
// ============================================================================

/**
 * ALPN / WebTransport protocol identifier for the implemented draft (§3.1: `moqt-` plus the draft number). Version
 * negotiation happens at connection time (`WT-Available-Protocols` in WebTransport, ALPN in native QUIC) — not in
 * SETUP.
 */
export const MOQT_PROTOCOL_ID = 'moqt-20';

/** Control/request message type codes (§10, Table 5). */
export const MESSAGE_TYPE = {
  SETUP: 0x2f00,
  GOAWAY: 0x10,
  SUBSCRIBE: 0x3,
  SUBSCRIBE_OK: 0x4,
  REQUEST_ERROR: 0x5,
  PUBLISH_NAMESPACE: 0x6,
  REQUEST_OK: 0x7,
  NAMESPACE: 0x8,
  PUBLISH_DONE: 0xb,
  TRACK_STATUS: 0xd,
  NAMESPACE_DONE: 0xe,
  PUBLISH_SKIPPED: 0xf,
  FETCH: 0x16,
  FETCH_OK: 0x18,
  PUBLISH: 0x1d,
  PUBLISH_STATE_NOTIFY: 0x22,
  REQUEST_UPDATE: 0x2,
  SUBSCRIBE_NAMESPACE: 0x50,
  SUBSCRIBE_TRACKS: 0x51,
} as const;

/** Setup Option types (§10.3.1). A namespace separate from message parameters. */
export const SETUP_OPTION = {
  PATH: 0x01,
  AUTHORIZATION_TOKEN: 0x03,
  MAX_AUTH_TOKEN_CACHE_SIZE: 0x04,
  AUTHORITY: 0x05,
  MAX_FILTER_RANGES: 0x06,
  MOQT_IMPLEMENTATION: 0x07,
  MAX_REQUEST_UPDATES: 0x08,
} as const;

/** Message parameter types (§10.2, §15.7). */
export const PARAMETER_TYPE = {
  OBJECT_DELIVERY_TIMEOUT: 0x02,
  AUTHORIZATION_TOKEN: 0x03,
  RENDEZVOUS_TIMEOUT: 0x04,
  SUBGROUP_DELIVERY_TIMEOUT: 0x06,
  EXPIRES: 0x08,
  LARGEST_OBJECT: 0x09,
  FILL_TIMEOUT: 0x0a,
  FORWARD: 0x10,
  SUBSCRIBER_PRIORITY: 0x20,
  LOCATION_FILTER: 0x21,
  GROUP_ORDER: 0x22,
  FILL_PARAMETERS: 0x23,
  SUBGROUP_FILTER: 0x25,
  OBJECTID_FILTER: 0x26,
  PRIORITY_FILTER: 0x27,
  OBJECT_PROPERTY_FILTER: 0x28,
  TRACK_PROPERTY_FILTER: 0x29,
  NEW_GROUP_REQUEST: 0x32,
  TRACK_NAMESPACE_PREFIX: 0x34,
  INCLUDE_PROPERTIES: 0x35,
} as const;

/**
 * Track Property types (§10.8) — the KVP block trailing SUBSCRIBE_OK / PUBLISH / FETCH_OK. A registry of its own,
 * distinct from both Setup Options and message parameters.
 */
export const TRACK_PROPERTY = {
  /**
   * Units-per-second for the track's object TIMESTAMP extensions. Declaring it in SUBSCRIBE_OK is what opts the track
   * into timestamp forwarding on moq-lite-rs relays — without it they parse and discard the extensions and re-stamp
   * frames on arrival.
   */
  TIMESCALE: 0x08,
} as const;

/** REQUEST_ERROR codes (§15.11.2). */
export const REQUEST_ERROR_CODE = {
  INTERNAL_ERROR: 0x0,
  UNAUTHORIZED: 0x1,
  TIMEOUT: 0x2,
  NOT_SUPPORTED: 0x3,
  MALFORMED_AUTH_TOKEN: 0x4,
  EXPIRED_AUTH_TOKEN: 0x5,
  GOING_AWAY: 0x6,
  EXCESSIVE_LOAD: 0x9,
  DOES_NOT_EXIST: 0x10,
  INVALID_RANGE: 0x11,
  MALFORMED_TRACK: 0x12,
  UNINTERESTED: 0x20,
  PREFIX_OVERLAP: 0x30,
  NAMESPACE_TOO_LARGE: 0x31,
  UNSUPPORTED_EXTENSION: 0x33,
  REDIRECT: 0x34,
  CONFLICTING_FILTERS: 0x35,
  INVALID_FILTER: 0x36,
} as const;

/** PUBLISH_DONE status codes (§10.12). */
export const PUBLISH_DONE_STATUS = {
  INTERNAL_ERROR: 0x0,
  UNAUTHORIZED: 0x1,
  TRACK_ENDED: 0x2,
  GOING_AWAY: 0x4,
  TOO_FAR_BEHIND: 0x5,
  EXPIRED: 0x6,
  UPDATE_FAILED: 0x8,
  EXCESSIVE_LOAD: 0x9,
  MALFORMED_TRACK: 0x12,
} as const;

/**
 * REQUEST_ERROR codes for which an identical retry can never succeed: the request or its credentials are wrong in a way
 * that does not change between attempts. Everything else — including codes this table does not know — is treated as
 * retryable, so a relay introducing a new transient code degrades to paced retries rather than a dead player.
 * EXPIRED_AUTH_TOKEN is deliberately absent: it has its own refresh path wherever requests are retried.
 */
const PERMANENT_REQUEST_ERROR_CODES: ReadonlySet<number> = new Set([
  REQUEST_ERROR_CODE.UNAUTHORIZED,
  REQUEST_ERROR_CODE.NOT_SUPPORTED,
  REQUEST_ERROR_CODE.MALFORMED_AUTH_TOKEN,
  REQUEST_ERROR_CODE.INVALID_RANGE,
  // The track cannot be served as requested; an identical resubscribe
  // re-earns the rejection. A publisher fixing the track surfaces as a
  // catalog update, which re-selects and re-subscribes through fresh state.
  REQUEST_ERROR_CODE.MALFORMED_TRACK,
  REQUEST_ERROR_CODE.UNINTERESTED,
  REQUEST_ERROR_CODE.PREFIX_OVERLAP,
  REQUEST_ERROR_CODE.NAMESPACE_TOO_LARGE,
  REQUEST_ERROR_CODE.UNSUPPORTED_EXTENSION,
  // Until session migration exists, the redirect target is unreachable and
  // re-asking the same relay just re-earns the redirect.
  REQUEST_ERROR_CODE.REDIRECT,
  REQUEST_ERROR_CODE.CONFLICTING_FILTERS,
  REQUEST_ERROR_CODE.INVALID_FILTER,
]);

/**
 * Whether a failed request is worth re-issuing unchanged after a backoff. The transient family (`DOES_NOT_EXIST` while
 * a broadcast has not started, `TIMEOUT`, `INTERNAL_ERROR`, overload, `GOING_AWAY`) reflects relay or publisher _state_
 * that time can fix; the permanent family reflects the request itself and loops forever if retried.
 */
export function isRetryableRequestErrorCode(errorCode: number): boolean {
  return !PERMANENT_REQUEST_ERROR_CODES.has(errorCode);
}

/**
 * PUBLISH_DONE statuses after which an identical re-subscribe can never succeed: auth-shaped ends (same credentials the
 * relay just rejected) and a malformed track (same request re-earns the same end; a publisher fix arrives as a catalog
 * update through fresh state). Every other status describes publisher/relay state a broadcaster restart or live-edge
 * rejoin legitimately recovers from (`TRACK_ENDED`, `GOING_AWAY`, `TOO_FAR_BEHIND`, overload, …), and unknown statuses
 * degrade to paced retries for the same reason as {@link isRetryableRequestErrorCode}.
 */
const PERMANENT_PUBLISH_DONE_STATUSES: ReadonlySet<number> = new Set([
  PUBLISH_DONE_STATUS.UNAUTHORIZED,
  PUBLISH_DONE_STATUS.EXPIRED,
  PUBLISH_DONE_STATUS.MALFORMED_TRACK,
]);

/** Whether a PUBLISH_DONE'd subscription is worth re-subscribing. */
export function isRetryablePublishDoneStatus(statusCode: number): boolean {
  return !PERMANENT_PUBLISH_DONE_STATUSES.has(statusCode);
}

/** Stream reset / STOP_SENDING error codes (§3.3.4). */
export const STREAM_RESET_CODE = {
  INTERNAL_ERROR: 0x0,
  CANCELLED: 0x1,
  DELIVERY_TIMEOUT: 0x2,
  SESSION_CLOSED: 0x3,
  GOING_AWAY: 0x4,
  TOO_FAR_BEHIND: 0x5,
  UNKNOWN_OBJECT_STATUS: 0x6,
  EXPIRED_AUTH_TOKEN: 0x7,
  EXCESSIVE_LOAD: 0x9,
  MALFORMED_TRACK: 0x12,
} as const;

/** Maximum body size — the frame length field is 16 bits. */
export const MAX_CONTROL_MESSAGE_LENGTH = 0xffff;

export const MAX_NAMESPACE_FIELDS = 32;
const MAX_FULL_TRACK_NAME_LENGTH = 4096;
const MAX_REASON_PHRASE_LENGTH = 1024;
const MAX_NEW_SESSION_URI_LENGTH = 8192;
const MAX_KVP_VALUE_LENGTH = 0xffff;
const MAX_LOCATION_FILTER_FIELDS = 4;

// ============================================================================
// Shared structures
// ============================================================================

/** A particular Object in a Group within a Track (§1.4.2). */
export interface Location {
  group: number;
  object: number;
}

/** `A < B` in Location order (group first, then object). */
export function compareLocations(a: Location, b: Location): number {
  return a.group - b.group || a.object - b.object;
}

/** Track namespace as an ordered tuple of UTF-8 fields. Byte strings on the wire; MSF constrains them to UTF-8 text. */
export type TrackNamespace = string[];

export interface FullTrackName {
  namespace: TrackNamespace;
  name: string;
}

function writeTrackNamespace(writer: ByteWriter, namespace: TrackNamespace): void {
  if (namespace.length > MAX_NAMESPACE_FIELDS) {
    throw new MoqtProtocolError(`track namespace has more than ${MAX_NAMESPACE_FIELDS} fields`);
  }

  writer.writeVarint(namespace.length);

  for (const field of namespace) {
    const bytes = utf8Encode(field);
    if (bytes.length === 0) throw new MoqtProtocolError('track namespace field must not be empty');

    writer.writeLengthPrefixed(bytes);
  }
}

function readTrackNamespace(reader: ByteReader): TrackNamespace {
  const fieldCount = reader.readVarint();

  if (fieldCount > MAX_NAMESPACE_FIELDS) {
    throw new MoqtProtocolError(`track namespace has more than ${MAX_NAMESPACE_FIELDS} fields`);
  }

  const namespace: TrackNamespace = [];
  let totalLength = 0;

  for (let i = 0; i < fieldCount; i++) {
    const length = reader.readVarint();
    if (length === 0) throw new MoqtProtocolError('track namespace field must not be empty');

    totalLength += length;

    if (totalLength > MAX_FULL_TRACK_NAME_LENGTH) {
      throw new MoqtProtocolError('track namespace exceeds 4096 bytes');
    }

    namespace.push(utf8Decode(reader.readBytes(length)));
  }

  return namespace;
}

function writeReasonPhrase(writer: ByteWriter, reason: string): void {
  const bytes = utf8Encode(reason);
  if (bytes.length > MAX_REASON_PHRASE_LENGTH) throw new MoqtProtocolError('reason phrase exceeds 1024 bytes');

  writer.writeLengthPrefixed(bytes);
}

function readReasonPhrase(reader: ByteReader): string {
  const length = reader.readVarint();
  if (length > MAX_REASON_PHRASE_LENGTH) throw new MoqtProtocolError('reason phrase exceeds 1024 bytes');

  return utf8Decode(reader.readBytes(length));
}

function writeLocation(writer: ByteWriter, location: Location): void {
  writer.writeVarint(location.group);
  writer.writeVarint(location.object);
}

function readLocation(reader: ByteReader): Location {
  return { group: reader.readVarint(), object: reader.readVarint() };
}

/** LARGEST_OBJECT is two bare varints in draft-17 onward (§10.2), matching moq-relay 0.14.17 (moq-dev/moq#3561). */
function writeLocationParameter(writer: ByteWriter, location: Location): void {
  writeLocation(writer, location);
}

function readLocationParameter(reader: ByteReader): Location {
  return readLocation(reader);
}

// ============================================================================
// Key-Value-Pairs (§1.4.3) — Setup Options and Track Properties
// ============================================================================

/**
 * A generic MOQT Key-Value-Pair. Even types carry a varint value, odd types a byte field. Used for Setup Options and
 * Track/Object Properties (message parameters use per-type encodings instead — see `MessageParameters`).
 */
export interface KeyValuePair {
  type: number;
  value: number | Uint8Array;
}

/** Serialize pairs in ascending type order with delta-encoded types. */
export function encodeKeyValuePairs(writer: ByteWriter, pairs: readonly KeyValuePair[]): void {
  const sorted = [...pairs].sort((a, b) => a.type - b.type);
  let previousType = 0;

  for (const { type, value } of sorted) {
    writer.writeVarint(type - previousType);
    previousType = type;

    if (type % 2 === 0) {
      if (typeof value !== 'number') throw new MoqtProtocolError(`even KVP type ${type} requires a varint value`);

      writer.writeVarint(value);
    } else {
      if (typeof value === 'number') throw new MoqtProtocolError(`odd KVP type ${type} requires a byte value`);

      if (value.length > MAX_KVP_VALUE_LENGTH) throw new MoqtProtocolError('KVP value exceeds 2^16-1 bytes');

      writer.writeLengthPrefixed(value);
    }
  }
}

/** Parse pairs until `reader` reaches `endOffset` (KVP blocks are length-bounded). */
export function decodeKeyValuePairs(reader: ByteReader, endOffset: number): KeyValuePair[] {
  const pairs: KeyValuePair[] = [];
  let previousType = 0;

  while (reader.offset < endOffset) {
    const type = previousType + reader.readVarint();

    previousType = type;

    if (type % 2 === 0) {
      pairs.push({ type, value: reader.readVarint() });
    } else {
      const length = reader.readVarint();
      if (length > MAX_KVP_VALUE_LENGTH) throw new MoqtProtocolError('KVP value exceeds 2^16-1 bytes');

      pairs.push({ type, value: reader.readBytes(length) });
    }
  }

  if (reader.offset !== endOffset) throw new MoqtProtocolError('KVP block overran its bound');

  return pairs;
}

// ============================================================================
// Location Filters (§5.1.2) — carried in the LOCATION_FILTER parameter
// ============================================================================

/**
 * Which Objects a subscription or fetch covers. On the wire the LOCATION_FILTER value is up to four varints —
 * StartGroup, StartObject, EndGroupDelta, EndObject — and how many are present selects the meaning:
 *
 * - `none` — zero fields. Unfiltered: the whole track for a subscription, `{0,0}` up to Largest Object for a fetch; in
 *   REQUEST_UPDATE it clears the filter.
 * - `relative-group` — one field. Start at `{Largest.Group + 1 − groupsBeforeNext, 0}`, open-ended: `0` is the next
 *   group, `1` the start of the current group (a decodable join), `N` N−1 groups before it.
 * - `next-object` — the two-field spelling `{0,0}`: `{Largest.Group, Largest.Object + 1}`, the live edge, which can begin
 *   mid-group.
 * - `absolute` — two to four fields: an explicit start, optionally bounded by an end group (delta from the start group,
 *   whole group included unless `endObject` narrows it). An open-ended absolute `{0,0}` is unfiltered by definition and
 *   encodes as `none` — its two-field spelling would read as `next-object`.
 */
export type LocationFilter =
  | { type: 'none' }
  | { type: 'next-object' }
  | { type: 'relative-group'; groupsBeforeNext: number }
  | { type: 'absolute'; start: Location; endGroupDelta?: number; endObject?: number };

/** Serialize a filter as the LOCATION_FILTER parameter's value (without its length prefix). */
export function encodeLocationFilter(filter: LocationFilter): Uint8Array {
  const writer = new ByteWriter(32);

  switch (filter.type) {
    case 'none':
      break;
    case 'next-object':
      writer.writeVarint(0);
      writer.writeVarint(0);
      break;
    case 'relative-group':
      writer.writeVarint(filter.groupsBeforeNext);
      break;
    case 'absolute': {
      const { start, endGroupDelta, endObject } = filter;

      if (endObject !== undefined && endGroupDelta === undefined) {
        throw new MoqtProtocolError('location filter end object requires an end group');
      }

      if (start.group === 0 && start.object === 0 && endGroupDelta === undefined) break;

      writeLocation(writer, start);

      if (endGroupDelta !== undefined) writer.writeVarint(endGroupDelta);

      if (endObject !== undefined) writer.writeVarint(endObject);

      break;
    }
  }

  return writer.toBytes();
}

export function decodeLocationFilter(bytes: Uint8Array): LocationFilter {
  const reader = new ByteReader(bytes);
  const fields: number[] = [];

  while (reader.remaining > 0) {
    if (fields.length === MAX_LOCATION_FILTER_FIELDS) {
      throw new MoqtProtocolError('location filter has more than four fields');
    }

    fields.push(reader.readVarint());
  }

  if (fields.length === 0) return { type: 'none' };

  if (fields.length === 1) return { type: 'relative-group', groupsBeforeNext: fields[0]! };

  const [group, object, endGroupDelta, endObject] = fields as [number, number, number?, number?];
  if (fields.length === 2 && group === 0 && object === 0) return { type: 'next-object' };

  const filter: LocationFilter = { type: 'absolute', start: { group, object } };

  if (endGroupDelta !== undefined) {
    if (group + endGroupDelta > MAX_VARINT_VALUE) {
      throw new MoqtProtocolError('location filter end group exceeds supported range');
    }

    filter.endGroupDelta = endGroupDelta;
  }

  if (endObject !== undefined) filter.endObject = endObject;

  return filter;
}

// ============================================================================
// Message Parameters (§10.2)
// ============================================================================

export type GroupOrder = 'ascending' | 'descending';

/**
 * Decoded message parameters. Unlike Setup Options, message parameters use per-type encodings (uint8 / varint /
 * length-prefixed), so the codec is registry-driven. Per §10.2 an unknown parameter type is a PROTOCOL_VIOLATION
 * (parameters cannot be skipped — the block is bounded by count, not length).
 */
export interface MessageParameters {
  objectDeliveryTimeout?: number;
  subgroupDeliveryTimeout?: number;
  rendezvousTimeout?: number;
  fillTimeout?: number;
  /** Serialized Token structures (§10.2.2). May repeat. */
  authorizationTokens?: Uint8Array[];
  expires?: number;
  largestObject?: Location;
  forward?: 0 | 1;
  subscriberPriority?: number;
  locationFilter?: LocationFilter;
  groupOrder?: GroupOrder;
  /**
   * FILL_PARAMETERS (§10.2.15): asks the publisher to deliver a fill range on a fetch stream alongside the subscription
   * — its presence alone requests the fill. A nested parameter scope: only the fill-scoped subset is legal inside, and
   * omitted entries inherit the subscription's own values.
   */
  fillParameters?: FillParameters;
  newGroupRequest?: number;
  /** Range-filter parameters (§5.1.4), kept opaque — the engine never sends or interprets them. */
  rangeFilters?: { type: number; value: Uint8Array }[];
  trackNamespacePrefix?: TrackNamespace;
  /** INCLUDE_PROPERTIES (§10.2.21): `0` asks the responder to send empty Track Properties. Default `1`. */
  includeProperties?: 0 | 1;
}

/** The parameters FILL_PARAMETERS may carry (§10.2.15, Table 6). */
export type FillParameters = Pick<
  MessageParameters,
  'fillTimeout' | 'subscriberPriority' | 'locationFilter' | 'groupOrder' | 'rangeFilters'
>;

type ParameterScope = 'message' | 'fill';

const FILL_SCOPE_TYPES: ReadonlySet<number> = new Set([
  PARAMETER_TYPE.FILL_TIMEOUT,
  PARAMETER_TYPE.SUBSCRIBER_PRIORITY,
  PARAMETER_TYPE.LOCATION_FILTER,
  PARAMETER_TYPE.GROUP_ORDER,
  PARAMETER_TYPE.SUBGROUP_FILTER,
  PARAMETER_TYPE.OBJECTID_FILTER,
  PARAMETER_TYPE.PRIORITY_FILTER,
  PARAMETER_TYPE.OBJECT_PROPERTY_FILTER,
]);

const GROUP_ORDER_WIRE: Record<GroupOrder, number> = { ascending: 0x1, descending: 0x2 };

interface ParameterEntry {
  type: number;
  write(writer: ByteWriter): void;
}

function collectParameterEntries(parameters: MessageParameters): ParameterEntry[] {
  const entries: ParameterEntry[] = [];
  const push = (type: number, write: (writer: ByteWriter) => void) => entries.push({ type, write });

  if (parameters.objectDeliveryTimeout !== undefined) {
    push(PARAMETER_TYPE.OBJECT_DELIVERY_TIMEOUT, (w) => w.writeVarint(parameters.objectDeliveryTimeout!));
  }

  if (parameters.subgroupDeliveryTimeout !== undefined) {
    push(PARAMETER_TYPE.SUBGROUP_DELIVERY_TIMEOUT, (w) => w.writeVarint(parameters.subgroupDeliveryTimeout!));
  }

  if (parameters.rendezvousTimeout !== undefined) {
    push(PARAMETER_TYPE.RENDEZVOUS_TIMEOUT, (w) => w.writeVarint(parameters.rendezvousTimeout!));
  }

  if (parameters.fillTimeout !== undefined) {
    push(PARAMETER_TYPE.FILL_TIMEOUT, (w) => w.writeVarint(parameters.fillTimeout!));
  }

  for (const token of parameters.authorizationTokens ?? []) {
    push(PARAMETER_TYPE.AUTHORIZATION_TOKEN, (w) => w.writeLengthPrefixed(token));
  }

  if (parameters.expires !== undefined) {
    push(PARAMETER_TYPE.EXPIRES, (w) => w.writeVarint(parameters.expires!));
  }

  if (parameters.largestObject !== undefined) {
    push(PARAMETER_TYPE.LARGEST_OBJECT, (w) => writeLocationParameter(w, parameters.largestObject!));
  }

  if (parameters.forward !== undefined) {
    push(PARAMETER_TYPE.FORWARD, (w) => w.writeUint8(parameters.forward!));
  }

  if (parameters.subscriberPriority !== undefined) {
    push(PARAMETER_TYPE.SUBSCRIBER_PRIORITY, (w) => w.writeUint8(parameters.subscriberPriority!));
  }

  if (parameters.locationFilter !== undefined) {
    push(PARAMETER_TYPE.LOCATION_FILTER, (w) =>
      w.writeLengthPrefixed(encodeLocationFilter(parameters.locationFilter!))
    );
  }

  if (parameters.groupOrder !== undefined) {
    push(PARAMETER_TYPE.GROUP_ORDER, (w) => w.writeUint8(GROUP_ORDER_WIRE[parameters.groupOrder!]));
  }

  if (parameters.fillParameters !== undefined) {
    push(PARAMETER_TYPE.FILL_PARAMETERS, (w) =>
      w.writeLengthPrefixed(encodeFillParameters(parameters.fillParameters!))
    );
  }

  if (parameters.newGroupRequest !== undefined) {
    push(PARAMETER_TYPE.NEW_GROUP_REQUEST, (w) => w.writeVarint(parameters.newGroupRequest!));
  }

  for (const filter of parameters.rangeFilters ?? []) {
    push(filter.type, (w) => w.writeLengthPrefixed(filter.value));
  }

  if (parameters.trackNamespacePrefix !== undefined) {
    push(PARAMETER_TYPE.TRACK_NAMESPACE_PREFIX, (w) => writeTrackNamespace(w, parameters.trackNamespacePrefix!));
  }

  if (parameters.includeProperties !== undefined) {
    push(PARAMETER_TYPE.INCLUDE_PROPERTIES, (w) => w.writeLengthPrefixed(Uint8Array.of(parameters.includeProperties!)));
  }

  return entries;
}

/**
 * Serialize as `Number of Parameters (vi64)` + delta-typed parameter list. The `'fill'` scope is the nested list inside
 * FILL_PARAMETERS, where only Table 6's types are legal.
 */
export function encodeMessageParameters(
  writer: ByteWriter,
  parameters: MessageParameters = {},
  scope: ParameterScope = 'message'
): void {
  const entries = collectParameterEntries(parameters).sort((a, b) => a.type - b.type);

  if (scope === 'fill') {
    const disallowed = entries.find((entry) => !FILL_SCOPE_TYPES.has(entry.type));

    if (disallowed) {
      throw new MoqtProtocolError(`parameter type ${disallowed.type} is not allowed inside FILL_PARAMETERS`);
    }
  }

  writer.writeVarint(entries.length);
  let previousType = 0;

  for (const entry of entries) {
    writer.writeVarint(entry.type - previousType);
    previousType = entry.type;
    entry.write(writer);
  }
}

/** FILL_PARAMETERS' value: a parameter list encoded as if for a separate message (§10.2.15). */
function encodeFillParameters(fill: FillParameters): Uint8Array {
  const writer = new ByteWriter(64);

  encodeMessageParameters(writer, fill, 'fill');
  return writer.toBytes();
}

const RANGE_FILTER_TYPES: readonly number[] = [
  PARAMETER_TYPE.SUBGROUP_FILTER,
  PARAMETER_TYPE.OBJECTID_FILTER,
  PARAMETER_TYPE.PRIORITY_FILTER,
  PARAMETER_TYPE.OBJECT_PROPERTY_FILTER,
  PARAMETER_TYPE.TRACK_PROPERTY_FILTER,
];

export function decodeMessageParameters(reader: ByteReader, scope: ParameterScope = 'message'): MessageParameters {
  const count = reader.readVarint();
  const parameters: MessageParameters = {};
  let previousType = 0;

  for (let i = 0; i < count; i++) {
    const type = previousType + reader.readVarint();

    previousType = type;

    // §10.2.15: a type outside Table 6 inside FILL_PARAMETERS is a
    // PROTOCOL_VIOLATION, the nested scope included.
    if (scope === 'fill' && !FILL_SCOPE_TYPES.has(type)) {
      throw new MoqtProtocolError(`parameter type ${type} is not allowed inside FILL_PARAMETERS`);
    }

    switch (type) {
      case PARAMETER_TYPE.OBJECT_DELIVERY_TIMEOUT:
        parameters.objectDeliveryTimeout = reader.readVarint();
        break;
      case PARAMETER_TYPE.SUBGROUP_DELIVERY_TIMEOUT:
        parameters.subgroupDeliveryTimeout = reader.readVarint();
        break;
      case PARAMETER_TYPE.RENDEZVOUS_TIMEOUT:
        parameters.rendezvousTimeout = reader.readVarint();
        break;
      case PARAMETER_TYPE.FILL_TIMEOUT:
        parameters.fillTimeout = reader.readVarint();
        break;
      case PARAMETER_TYPE.AUTHORIZATION_TOKEN: {
        const token = reader.readBytes(reader.readVarint());

        parameters.authorizationTokens ??= [];
        parameters.authorizationTokens.push(token);
        break;
      }
      case PARAMETER_TYPE.EXPIRES:
        parameters.expires = reader.readVarint();
        break;
      case PARAMETER_TYPE.LARGEST_OBJECT:
        parameters.largestObject = readLocationParameter(reader);
        break;
      case PARAMETER_TYPE.FORWARD: {
        const forward = reader.readUint8();
        if (forward !== 0 && forward !== 1) throw new MoqtProtocolError(`invalid FORWARD value ${forward}`);

        parameters.forward = forward;
        break;
      }
      case PARAMETER_TYPE.SUBSCRIBER_PRIORITY:
        parameters.subscriberPriority = reader.readUint8();
        break;
      case PARAMETER_TYPE.LOCATION_FILTER:
        parameters.locationFilter = decodeLocationFilter(reader.readBytes(reader.readVarint()));
        break;
      case PARAMETER_TYPE.GROUP_ORDER: {
        const order = reader.readUint8();
        if (order !== 0x1 && order !== 0x2) throw new MoqtProtocolError(`invalid GROUP_ORDER value ${order}`);

        parameters.groupOrder = order === 0x1 ? 'ascending' : 'descending';
        break;
      }
      case PARAMETER_TYPE.FILL_PARAMETERS: {
        const value = new ByteReader(reader.readBytes(reader.readVarint()));

        parameters.fillParameters = decodeMessageParameters(value, 'fill');

        if (value.remaining !== 0) throw new MoqtProtocolError('FILL_PARAMETERS has trailing bytes');

        break;
      }
      case PARAMETER_TYPE.NEW_GROUP_REQUEST:
        parameters.newGroupRequest = reader.readVarint();
        break;
      case PARAMETER_TYPE.TRACK_NAMESPACE_PREFIX:
        parameters.trackNamespacePrefix = readTrackNamespace(reader);
        break;
      case PARAMETER_TYPE.INCLUDE_PROPERTIES: {
        // §10.2.21 calls the value a uint8; moq-relay frames it
        // length-prefixed by the odd-type parity rule (moq-dev/moq#3255),
        // and the codec follows the relay. This remains length-prefixed in moq-relay 0.14.17.
        const value = reader.readBytes(reader.readVarint());

        if (value.length !== 1 || value[0]! > 1) {
          throw new MoqtProtocolError(`invalid INCLUDE_PROPERTIES value ${Array.from(value).join(',')}`);
        }

        parameters.includeProperties = value[0] as 0 | 1;
        break;
      }
      default: {
        if (RANGE_FILTER_TYPES.includes(type)) {
          const value = reader.readBytes(reader.readVarint());

          parameters.rangeFilters ??= [];
          parameters.rangeFilters.push({ type, value });
          break;
        }

        // §10.2: unknown parameters cannot be skipped (the block is bounded
        // by count, and the value encoding is per-type) — session error.
        throw new MoqtProtocolError(`unknown message parameter type ${type}`);
      }
    }
  }

  return parameters;
}

// ============================================================================
// Authorization tokens (§10.2.2)
// ============================================================================

/**
 * Serialize a Token structure with Alias Type USE_VALUE (0x3) — the stateless form the engine uses to attach an
 * MSF-provided token to a request without alias registration.
 */
export function encodeAuthTokenUseValue(tokenType: number, value: Uint8Array): Uint8Array {
  const writer = new ByteWriter(value.length + 8);

  writer.writeVarint(0x3);
  writer.writeVarint(tokenType);
  writer.writeBytes(value);
  return writer.toBytes();
}

// ============================================================================
// Message framing
// ============================================================================

/** A decoded frame: message type + body bytes (not yet interpreted). */
export interface ControlMessageFrame {
  type: number;
  body: Uint8Array;
}

function frameMessage(type: number, body: ByteWriter): Uint8Array {
  const bodyBytes = body.toBytes();

  if (bodyBytes.length > MAX_CONTROL_MESSAGE_LENGTH) {
    throw new MoqtProtocolError('control message exceeds 2^16-1 bytes');
  }

  const writer = new ByteWriter(bodyBytes.length + 8);

  writer.writeVarint(type);
  writer.writeUint16(bodyBytes.length);
  writer.writeBytes(bodyBytes);
  return writer.toBytes();
}

/**
 * Incremental control-message deframer. Feed it stream chunks; it yields complete `ControlMessageFrame`s as they
 * materialize. Used for both the control streams and request streams (same framing).
 */
export class ControlMessageDeframer {
  #buffer: Uint8Array = new Uint8Array(0);

  push(chunk: Uint8Array): ControlMessageFrame[] {
    if (this.#buffer.length === 0) {
      this.#buffer = chunk;
    } else {
      const merged = new Uint8Array(this.#buffer.length + chunk.length);

      merged.set(this.#buffer);
      merged.set(chunk, this.#buffer.length);
      this.#buffer = merged;
    }

    const frames: ControlMessageFrame[] = [];
    let offset = 0;

    while (true) {
      const frame = this.#tryReadFrame(offset);
      if (!frame) break;

      frames.push(frame.frame);
      offset = frame.nextOffset;
    }

    this.#buffer = offset > 0 ? this.#buffer.subarray(offset) : this.#buffer;
    return frames;
  }

  /** Bytes buffered but not yet forming a complete frame. */
  get pendingBytes(): number {
    return this.#buffer.length;
  }

  #tryReadFrame(offset: number): { frame: ControlMessageFrame; nextOffset: number } | null {
    const buffer = this.#buffer;
    if (offset >= buffer.length) return null;

    let type: number;
    let headerLength: number;

    try {
      const decoded = decodeVarint(buffer, offset);

      type = decoded.value;
      headerLength = decoded.byteLength;
    } catch (error) {
      // Incomplete varint — wait for more bytes.
      if (error instanceof RangeError) return null;

      throw error;
    }

    if (offset + headerLength + 2 > buffer.length) return null;

    const length = buffer[offset + headerLength]! * 256 + buffer[offset + headerLength + 1]!;
    const bodyStart = offset + headerLength + 2;
    if (bodyStart + length > buffer.length) return null;

    return {
      frame: { type, body: buffer.slice(bodyStart, bodyStart + length) },
      nextOffset: bodyStart + length,
    };
  }
}

// ============================================================================
// Encoders (subscriber → publisher)
// ============================================================================

/** SETUP (§10.3) — first message on the endpoint's control stream. */
export function encodeSetup(options: readonly KeyValuePair[] = []): Uint8Array {
  const body = new ByteWriter();

  encodeKeyValuePairs(body, options);
  return frameMessage(MESSAGE_TYPE.SETUP, body);
}

export interface SubscribeRequest {
  requestId: number;
  trackNamespace: TrackNamespace;
  trackName: string;
  parameters?: MessageParameters | undefined;
}

/**
 * FETCH (§10.13) has the SUBSCRIBE body: the range is `parameters.locationFilter`, evaluated with the fetch rules of
 * §5.1.2 — relative starts count back from Largest Object, an omitted end stops there, and no filter means the whole
 * track. Nothing selects "standalone" or "joining" any more; a join is a subscription with FILL_PARAMETERS.
 */
export type FetchRequest = SubscribeRequest;

/** SUBSCRIBE, TRACK_STATUS, and FETCH share one body: Request ID, Full Track Name, Parameters. */
function encodeTrackRequest(type: number, request: SubscribeRequest): Uint8Array {
  const body = new ByteWriter();

  body.writeVarint(request.requestId);
  writeTrackNamespace(body, request.trackNamespace);
  body.writeLengthPrefixed(utf8Encode(request.trackName));
  encodeMessageParameters(body, request.parameters);
  return frameMessage(type, body);
}

/** SUBSCRIBE (§10.7) — first message on a new request stream. */
export function encodeSubscribe(request: SubscribeRequest): Uint8Array {
  return encodeTrackRequest(MESSAGE_TYPE.SUBSCRIBE, request);
}

/** TRACK_STATUS (§10.15) — identical wire format to SUBSCRIBE. */
export function encodeTrackStatus(request: SubscribeRequest): Uint8Array {
  return encodeTrackRequest(MESSAGE_TYPE.TRACK_STATUS, request);
}

/** FETCH (§10.13) — first message on a new request stream. */
export function encodeFetch(request: FetchRequest): Uint8Array {
  return encodeTrackRequest(MESSAGE_TYPE.FETCH, request);
}

export interface SubscribeNamespaceRequest {
  requestId: number;
  trackNamespacePrefix: TrackNamespace;
  parameters?: MessageParameters;
}

/** SUBSCRIBE_NAMESPACE (§10.19) — first message on a new request stream. */
export function encodeSubscribeNamespace(request: SubscribeNamespaceRequest): Uint8Array {
  const body = new ByteWriter();

  body.writeVarint(request.requestId);
  writeTrackNamespace(body, request.trackNamespacePrefix);
  encodeMessageParameters(body, request.parameters);
  return frameMessage(MESSAGE_TYPE.SUBSCRIBE_NAMESPACE, body);
}

/**
 * REQUEST_UPDATE (§10.9) — sent on an existing request stream. `requestId` is the update's own, freshly allocated
 * Request ID (§10.1 counts REQUEST_UPDATE among the messages that consume one); the stream identifies the request being
 * updated.
 */
export function encodeRequestUpdate(requestId: number, parameters: MessageParameters = {}): Uint8Array {
  const body = new ByteWriter();

  body.writeVarint(requestId);
  encodeMessageParameters(body, parameters);
  return frameMessage(MESSAGE_TYPE.REQUEST_UPDATE, body);
}

/**
 * GOAWAY (§10.4). Clients MUST send a zero-length New Session URI, so the encoder takes only the timeout (milliseconds;
 * 0 = no specific timeout).
 */
export function encodeGoaway(timeout = 0): Uint8Array {
  const body = new ByteWriter();

  body.writeVarint(0);
  body.writeVarint(timeout);
  return frameMessage(MESSAGE_TYPE.GOAWAY, body);
}

/**
 * REQUEST_OK (§10.5). The subscriber sends this to accept an incoming PUBLISH (PUBLISH_OK). Track Properties are always
 * empty in that role, and since draft-20 so are the parameters — a subscriber changes subscription parameters with
 * REQUEST_UPDATE, not in PUBLISH_OK (§10.11); the session driver's `accept()` therefore takes none. The parameter slot
 * stays for the publisher-side roles of REQUEST_OK (TRACK_STATUS_OK, REQUEST_UPDATE_OK), which the fake peer in tests
 * exercises.
 */
export function encodeRequestOk(parameters: MessageParameters = {}): Uint8Array {
  const body = new ByteWriter();

  encodeMessageParameters(body, parameters);
  return frameMessage(MESSAGE_TYPE.REQUEST_OK, body);
}

/** REQUEST_ERROR (§10.6) without a Redirect — enough to reject incoming requests. */
export function encodeRequestError(errorCode: number, reason = '', retryInterval = 0): Uint8Array {
  const body = new ByteWriter();

  body.writeVarint(errorCode);
  body.writeVarint(retryInterval);
  writeReasonPhrase(body, reason);
  return frameMessage(MESSAGE_TYPE.REQUEST_ERROR, body);
}

// ============================================================================
// Encoders (publisher → subscriber)
//
// The engine is subscribe-only, but the codec is symmetric: these are used
// by the in-memory fake peer in tests (the same seam a future publish
// path would build on).
// ============================================================================

/** SUBSCRIBE_OK (§10.8). */
export function encodeSubscribeOk(
  trackAlias: number,
  parameters: MessageParameters = {},
  trackProperties: readonly KeyValuePair[] = []
): Uint8Array {
  const body = new ByteWriter();

  body.writeVarint(trackAlias);
  encodeMessageParameters(body, parameters);
  encodeKeyValuePairs(body, trackProperties);
  return frameMessage(MESSAGE_TYPE.SUBSCRIBE_OK, body);
}

/** PUBLISH_DONE (§10.12). */
export function encodePublishDone(statusCode: number, streamCount: number, reason = ''): Uint8Array {
  const body = new ByteWriter();

  body.writeVarint(statusCode);
  body.writeVarint(streamCount);
  writeReasonPhrase(body, reason);
  return frameMessage(MESSAGE_TYPE.PUBLISH_DONE, body);
}

/** PUBLISH_STATE_NOTIFY (§10.10) — the publisher's unilateral "these subscription parameters changed". */
export function encodePublishStateNotify(parameters: MessageParameters = {}): Uint8Array {
  const body = new ByteWriter();

  encodeMessageParameters(body, parameters);
  return frameMessage(MESSAGE_TYPE.PUBLISH_STATE_NOTIFY, body);
}

/** FETCH_OK (§10.14). */
export function encodeFetchOk(
  endOfTrack: boolean,
  endLocation: Location,
  parameters: MessageParameters = {},
  trackProperties: readonly KeyValuePair[] = []
): Uint8Array {
  const body = new ByteWriter();

  body.writeUint8(endOfTrack ? 1 : 0);
  writeLocation(body, endLocation);
  encodeMessageParameters(body, parameters);
  encodeKeyValuePairs(body, trackProperties);
  return frameMessage(MESSAGE_TYPE.FETCH_OK, body);
}

/** PUBLISH (§10.11). */
export function encodePublish(
  request: SubscribeRequest & { trackAlias: number },
  trackProperties: readonly KeyValuePair[] = []
): Uint8Array {
  const body = new ByteWriter();

  body.writeVarint(request.requestId);
  writeTrackNamespace(body, request.trackNamespace);
  body.writeLengthPrefixed(utf8Encode(request.trackName));
  body.writeVarint(request.trackAlias);
  encodeMessageParameters(body, request.parameters);
  encodeKeyValuePairs(body, trackProperties);
  return frameMessage(MESSAGE_TYPE.PUBLISH, body);
}

export interface PublishNamespaceRequest {
  requestId: number;
  trackNamespace: TrackNamespace;
  parameters?: MessageParameters;
}

/** PUBLISH_NAMESPACE (§10.16) — a publisher announcing a namespace. */
export function encodePublishNamespace(request: PublishNamespaceRequest): Uint8Array {
  const body = new ByteWriter();

  body.writeVarint(request.requestId);
  writeTrackNamespace(body, request.trackNamespace);
  encodeMessageParameters(body, request.parameters);
  return frameMessage(MESSAGE_TYPE.PUBLISH_NAMESPACE, body);
}

/**
 * NAMESPACE (§10.19) — announce one namespace on an accepted inbound SUBSCRIBE_NAMESPACE request stream. The body is
 * the namespace suffix relative to that stream's subscribed prefix and nothing else: peers bound-check the tuple
 * against the frame length (moq-lite-rs treats even a zero parameter count as a malformed entry and kills the announce
 * stream), so no parameter block is written.
 */
export function encodeNamespace(trackNamespaceSuffix: TrackNamespace): Uint8Array {
  const body = new ByteWriter();

  writeTrackNamespace(body, trackNamespaceSuffix);
  return frameMessage(MESSAGE_TYPE.NAMESPACE, body);
}

/** NAMESPACE_DONE (§10.20) — retract a NAMESPACE announcement (same body shape). */
export function encodeNamespaceDone(trackNamespaceSuffix: TrackNamespace): Uint8Array {
  const body = new ByteWriter();

  writeTrackNamespace(body, trackNamespaceSuffix);
  return frameMessage(MESSAGE_TYPE.NAMESPACE_DONE, body);
}

// ============================================================================
// Decoders (publisher → subscriber)
// ============================================================================

export interface Redirect {
  connectUri: string;
  trackNamespace: TrackNamespace;
  trackName: string;
}

export type ControlMessage =
  | { kind: 'setup'; options: KeyValuePair[] }
  | { kind: 'goaway'; newSessionUri: string; timeout: number }
  | {
      kind: 'subscribe';
      requestId: number;
      trackNamespace: TrackNamespace;
      trackName: string;
      parameters: MessageParameters;
    }
  | {
      kind: 'track-status';
      requestId: number;
      trackNamespace: TrackNamespace;
      trackName: string;
      parameters: MessageParameters;
    }
  | { kind: 'fetch'; request: FetchRequest & { parameters: MessageParameters } }
  | {
      kind: 'subscribe-namespace';
      requestId: number;
      trackNamespacePrefix: TrackNamespace;
      parameters: MessageParameters;
    }
  | { kind: 'subscribe-ok'; trackAlias: number; parameters: MessageParameters; trackProperties: KeyValuePair[] }
  | { kind: 'request-ok'; parameters: MessageParameters; trackProperties: KeyValuePair[] }
  | { kind: 'request-error'; errorCode: number; retryInterval: number; reason: string; redirect?: Redirect }
  | { kind: 'request-update'; requestId: number; parameters: MessageParameters }
  | { kind: 'publish-state-notify'; parameters: MessageParameters }
  | {
      kind: 'publish-done';
      statusCode: number;
      /** `undefined` when the publisher could not count its streams (the 2^64−1 sentinel, §10.12). */
      streamCount: number | undefined;
      reason: string;
    }
  | {
      kind: 'publish';
      requestId: number;
      trackNamespace: TrackNamespace;
      trackName: string;
      trackAlias: number;
      parameters: MessageParameters;
      trackProperties: KeyValuePair[];
    }
  | { kind: 'publish-namespace'; requestId: number; trackNamespace: TrackNamespace; parameters: MessageParameters }
  | {
      kind: 'fetch-ok';
      endOfTrack: boolean;
      endLocation: Location;
      parameters: MessageParameters;
      trackProperties: KeyValuePair[];
    }
  | { kind: 'namespace'; trackNamespaceSuffix: TrackNamespace }
  | { kind: 'namespace-done'; trackNamespaceSuffix: TrackNamespace }
  | { kind: 'publish-skipped'; trackNamespaceSuffix: TrackNamespace; trackName: string };

function readRedirect(reader: ByteReader): Redirect {
  const uriLength = reader.readVarint();
  const connectUri = utf8Decode(reader.readBytes(uriLength));
  const trackNamespace = readTrackNamespace(reader);
  const nameLength = reader.readVarint();
  const trackName = utf8Decode(reader.readBytes(nameLength));

  return { connectUri, trackNamespace, trackName };
}

/**
 * Decode one control-message frame into a typed message. Throws `MoqtProtocolError` for unknown message types (§10: "An
 * endpoint that receives an unknown message type MUST close the session") and for malformed bodies.
 */
export function decodeControlMessage(frame: ControlMessageFrame): ControlMessage {
  const reader = new ByteReader(frame.body);
  const end = frame.body.length;
  let message: ControlMessage;

  try {
    message = decodeControlMessageBody(frame.type, reader, end);
  } catch (error) {
    // Field reads past the declared frame length surface as `RangeError`
    // from `ByteReader`. Normalize them: the session's stream loops treat
    // only `MoqtProtocolError` as fatal, and a truncated body must
    // terminate the session (§10) rather than vanish as a read error.
    if (error instanceof RangeError) {
      throw new MoqtProtocolError(`control message 0x${frame.type.toString(16)} body is truncated`);
    }

    throw error;
  }

  if (reader.offset !== end) {
    throw new MoqtProtocolError(`control message 0x${frame.type.toString(16)} body length mismatch`);
  }

  return message;
}

function decodeControlMessageBody(type: number, reader: ByteReader, end: number): ControlMessage {
  switch (type) {
    case MESSAGE_TYPE.SETUP:
      return { kind: 'setup', options: decodeKeyValuePairs(reader, end) };
    case MESSAGE_TYPE.GOAWAY: {
      const uriLength = reader.readVarint();

      if (uriLength > MAX_NEW_SESSION_URI_LENGTH) {
        throw new MoqtProtocolError('GOAWAY new session URI exceeds 8192 bytes');
      }

      const newSessionUri = utf8Decode(reader.readBytes(uriLength));

      return { kind: 'goaway', newSessionUri, timeout: reader.readVarint() };
    }
    case MESSAGE_TYPE.SUBSCRIBE_OK: {
      const trackAlias = reader.readVarint();
      const parameters = decodeMessageParameters(reader);

      return { kind: 'subscribe-ok', trackAlias, parameters, trackProperties: decodeKeyValuePairs(reader, end) };
    }
    case MESSAGE_TYPE.REQUEST_OK: {
      const parameters = decodeMessageParameters(reader);

      return { kind: 'request-ok', parameters, trackProperties: decodeKeyValuePairs(reader, end) };
    }
    case MESSAGE_TYPE.REQUEST_ERROR: {
      const errorCode = reader.readVarint();
      const retryInterval = reader.readVarint();
      const reason = readReasonPhrase(reader);

      if (errorCode === REQUEST_ERROR_CODE.REDIRECT) {
        return { kind: 'request-error', errorCode, retryInterval, reason, redirect: readRedirect(reader) };
      }

      return { kind: 'request-error', errorCode, retryInterval, reason };
    }
    case MESSAGE_TYPE.REQUEST_UPDATE: {
      const requestId = reader.readVarint();

      return { kind: 'request-update', requestId, parameters: decodeMessageParameters(reader) };
    }
    case MESSAGE_TYPE.PUBLISH_STATE_NOTIFY:
      return { kind: 'publish-state-notify', parameters: decodeMessageParameters(reader) };
    case MESSAGE_TYPE.PUBLISH_DONE: {
      const statusCode = reader.readVarint();
      // A publisher that cannot count its streams MUST send 2^64−1 (§10.12),
      // which is above this codec's varint ceiling — read that one value as
      // "unknown" rather than failing the session over a legitimate
      // PUBLISH_DONE. Any other count past the ceiling stays a protocol error.
      const streamCount = reader.readCountVarint();

      return { kind: 'publish-done', statusCode, streamCount, reason: readReasonPhrase(reader) };
    }
    case MESSAGE_TYPE.PUBLISH: {
      const requestId = reader.readVarint();
      const trackNamespace = readTrackNamespace(reader);
      const trackName = utf8Decode(reader.readBytes(reader.readVarint()));
      const trackAlias = reader.readVarint();
      const parameters = decodeMessageParameters(reader);

      return {
        kind: 'publish',
        requestId,
        trackNamespace,
        trackName,
        trackAlias,
        parameters,
        trackProperties: decodeKeyValuePairs(reader, end),
      };
    }
    case MESSAGE_TYPE.PUBLISH_NAMESPACE: {
      const requestId = reader.readVarint();
      const trackNamespace = readTrackNamespace(reader);

      return { kind: 'publish-namespace', requestId, trackNamespace, parameters: decodeMessageParameters(reader) };
    }
    case MESSAGE_TYPE.FETCH_OK: {
      const endOfTrackWire = reader.readUint8();
      if (endOfTrackWire > 1) throw new MoqtProtocolError(`invalid FETCH_OK end-of-track value ${endOfTrackWire}`);

      const endOfTrack = endOfTrackWire === 1;
      const endLocation = readLocation(reader);
      const parameters = decodeMessageParameters(reader);

      return {
        kind: 'fetch-ok',
        endOfTrack,
        endLocation,
        parameters,
        trackProperties: decodeKeyValuePairs(reader, end),
      };
    }
    case MESSAGE_TYPE.SUBSCRIBE:
    case MESSAGE_TYPE.TRACK_STATUS:
    case MESSAGE_TYPE.FETCH: {
      const requestId = reader.readVarint();
      const trackNamespace = readTrackNamespace(reader);
      const trackName = utf8Decode(reader.readBytes(reader.readVarint()));
      const parameters = decodeMessageParameters(reader);

      if (type === MESSAGE_TYPE.FETCH) {
        return { kind: 'fetch', request: { requestId, trackNamespace, trackName, parameters } };
      }

      const kind = type === MESSAGE_TYPE.SUBSCRIBE ? 'subscribe' : 'track-status';

      return { kind, requestId, trackNamespace, trackName, parameters };
    }
    case MESSAGE_TYPE.SUBSCRIBE_NAMESPACE: {
      const requestId = reader.readVarint();
      const trackNamespacePrefix = readTrackNamespace(reader);

      return {
        kind: 'subscribe-namespace',
        requestId,
        trackNamespacePrefix,
        parameters: decodeMessageParameters(reader),
      };
    }
    case MESSAGE_TYPE.NAMESPACE:
      return { kind: 'namespace', trackNamespaceSuffix: readTrackNamespace(reader) };
    case MESSAGE_TYPE.NAMESPACE_DONE:
      return { kind: 'namespace-done', trackNamespaceSuffix: readTrackNamespace(reader) };
    case MESSAGE_TYPE.PUBLISH_SKIPPED: {
      const trackNamespaceSuffix = readTrackNamespace(reader);
      const trackName = utf8Decode(reader.readBytes(reader.readVarint()));

      return { kind: 'publish-skipped', trackNamespaceSuffix, trackName };
    }
    default:
      throw new MoqtProtocolError(`unknown control message type 0x${type.toString(16)}`);
  }
}
