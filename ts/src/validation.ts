/**
 * What the SDK admits from the caller before anything reaches the core: the wire-facing values
 * (namespace, track name, properties, payload), each checked in its original container and copied
 * once. Both roles use the same rules, so the publisher and the subscriber cannot drift apart, and
 * the "Arguments" table in spec/behavior.md is the list these functions implement.
 *
 * Errors keep their idiomatic types: a wrong type or shape is a `TypeError`; a value of the right
 * type outside its bound is a `RangeError`. Nothing here is a `RelayError`: these are programming
 * errors, raised before any connection state is consulted.
 */
import {
  ByteWriter,
  encodeKeyValuePairs,
  type KeyValuePair,
  MAX_NAMESPACE_FIELDS,
  MAX_OBJECT_PAYLOAD_LENGTH,
  MAX_OBJECT_PROPERTIES_LENGTH,
} from '@mux/moq-core';
import type { PublishProperty } from './publish.js';

/**
 * A key-value pair's byte value carries a 16-bit length on the wire (draft-ietf-moq-transport-20
 * §1.4.2), pinned in spec/protocol.json. The core enforces the same bound while encoding.
 */
export const MAX_PROPERTY_VALUE_LENGTH = 0xffff;

/**
 * moq-relay drops an object whose encoded property block is larger than this (`MAX_OBJECT_EXTENSIONS`
 * since 0.17.0, an implementation limit rather than a draft one), so a write refuses it up front.
 * Pinned in spec/protocol.json as `relay.limits.maxObjectPropertiesBytes`.
 */
export const MAX_RELAY_OBJECT_PROPERTIES_LENGTH = 64 * 1024;

/**
 * Check a namespace in its original container and return a fresh copy of its fields. The copy is
 * the SDK's own; it is handed to the core (whose namespace type is mutable) and never to the caller.
 */
export function namespaceFields(namespace: unknown): string[] {
  // Array.from materializes holes as undefined, so a sparse array fails the field check below.
  const fields = Array.isArray(namespace) ? Array.from(namespace as unknown[]) : undefined;
  if (fields === undefined || fields.length === 0 || !fields.every(isNonEmptyString)) {
    throw new TypeError('A namespace must be a sequence of non-empty string fields');
  }
  if (fields.length > MAX_NAMESPACE_FIELDS) {
    throw new RangeError(`A namespace must contain at most ${MAX_NAMESPACE_FIELDS} fields`);
  }
  return fields;
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

/** Check a track name: the string part of a full track name. */
export function trackName(track: unknown): string {
  if (typeof track !== 'string' || track.length === 0) throw new TypeError('A track name must be a non-empty string');
  return track;
}

/** Check a write payload and copy the bytes its view spans, before any slot or stream is taken. */
export function payloadBytes(payload: unknown): Uint8Array {
  if (!ArrayBuffer.isView(payload))
    throw new TypeError('Payload must be a Uint8Array or other ArrayBufferView; wrap an ArrayBuffer in one');
  if (payload.byteLength > MAX_OBJECT_PAYLOAD_LENGTH) throw new RangeError('Payload exceeds the 16 MiB object limit');
  return new Uint8Array(payload.buffer, payload.byteOffset, payload.byteLength).slice();
}

/**
 * Check track properties and copy any byte values, so later mutation cannot leak in.
 * Integer types and values are bounded by what the core's varint can carry, which in this
 * language is `Number.MAX_SAFE_INTEGER`. Byte values are bounded by their 16-bit wire length, and
 * the whole list by the encoded size the core accepts on an object; both are measured here, with
 * the core's own encoder, so an oversized property fails as an argument error instead of as a
 * transport failure once demand arrives.
 */
export function propertyPairs(properties: unknown = []): KeyValuePair[] {
  const { pairs, length } = measuredPairs(properties);
  if (length > MAX_OBJECT_PROPERTIES_LENGTH)
    throw new RangeError(`Properties exceed the ${MAX_OBJECT_PROPERTIES_LENGTH}-byte encoded limit`);
  return pairs;
}

/**
 * Check one object's properties like {@link propertyPairs}, against the relay's tighter bound. The
 * relay would drop an object whose block is over its limit, silently for the subscriber, so the
 * write fails here instead. The relay forwards no object property but LOC Timestamp anyway.
 */
export function objectPropertyPairs(properties: unknown = []): KeyValuePair[] {
  const { pairs, length } = measuredPairs(properties);
  if (length > MAX_RELAY_OBJECT_PROPERTIES_LENGTH)
    throw new RangeError(
      `Object properties encode to ${length} bytes, over the relay's ${MAX_RELAY_OBJECT_PROPERTIES_LENGTH}-byte ` +
        'limit, so the relay would drop the object; it forwards only LOC Timestamp (0x10), so carry larger ' +
        'data in the payload',
    );
  return pairs;
}

/** Check and copy a property list, returning it with its encoded length. */
function measuredPairs(properties: unknown): { pairs: KeyValuePair[]; length: number } {
  if (!Array.isArray(properties)) throw new TypeError('Properties must be an array of {type, value} pairs');
  const pairs = (properties as readonly (Partial<PublishProperty> | null | undefined)[]).map(
    (property, index): KeyValuePair => {
      const { type, value } = property ?? {};
      if (typeof type !== 'number' || !Number.isSafeInteger(type) || type < 0)
        throw new TypeError(`Property ${index} type must be a non-negative integer`);
      if (type % 2 === 0) {
        if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0)
          throw new TypeError(`Property ${index} value must be a non-negative integer for an even type`);
        return { type, value };
      }
      if (!ArrayBuffer.isView(value))
        throw new TypeError(`Property ${index} value must be an ArrayBufferView for an odd type`);
      if (value.byteLength > MAX_PROPERTY_VALUE_LENGTH)
        throw new RangeError(`Property ${index} value exceeds the ${MAX_PROPERTY_VALUE_LENGTH}-byte limit`);
      return { type, value: new Uint8Array(value.buffer, value.byteOffset, value.byteLength).slice() };
    },
  );
  if (pairs.length === 0) return { pairs, length: 0 };
  const encoded = new ByteWriter(64);
  encodeKeyValuePairs(encoded, pairs);
  return { pairs, length: encoded.length };
}
