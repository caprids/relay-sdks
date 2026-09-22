import { type KeyValuePair, MAX_OBJECT_PAYLOAD_LENGTH, type MoqtObject } from '@mux/moq-core';
import type { PublishProperty } from './publish.js';

const encoder = new TextEncoder();
// Preserve a leading U+FEFF, and reject malformed UTF-8 instead of replacing bytes.
const decoder = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });

export function encodeText(text: string): Uint8Array {
  if (typeof text !== 'string') throw new TypeError('Text must be a string');
  return encoder.encode(text);
}

export function encodeJSON(value: unknown): Uint8Array {
  const text = JSON.stringify(value);
  if (text === undefined) throw new TypeError('Value cannot be serialized as JSON');
  return encodeText(text);
}

/** Validate a write payload and copy the bytes its view spans, before any slot or stream is taken. */
export function copyPayload(payload: ArrayBufferView): Uint8Array {
  if (!ArrayBuffer.isView(payload))
    throw new TypeError('Payload must be a Uint8Array or other ArrayBufferView; wrap an ArrayBuffer in one');
  if (payload.byteLength > MAX_OBJECT_PAYLOAD_LENGTH) throw new RangeError('Payload exceeds the 16 MiB object limit');
  return new Uint8Array(payload.buffer, payload.byteOffset, payload.byteLength).slice();
}

/** Validate track or object properties and copy any byte values, so later mutation cannot leak in. */
export function copyProperties(properties: readonly PublishProperty[] = []): KeyValuePair[] {
  return Array.from(properties, (property, index) => {
    const { type, value } = property ?? {};
    if (!Number.isSafeInteger(type) || type < 0)
      throw new TypeError(`Property ${index} type must be a non-negative safe integer`);
    if (type % 2 === 0) {
      if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0)
        throw new TypeError(`Property ${index} value must be a non-negative safe integer for an even type`);
      return { type, value };
    }
    if (!ArrayBuffer.isView(value))
      throw new TypeError(`Property ${index} value must be an ArrayBufferView for an odd type`);
    return { type, value: new Uint8Array(value.buffer, value.byteOffset, value.byteLength).slice() };
  });
}

/** Decode one complete object as UTF-8. Malformed UTF-8 throws a TypeError. */
export function readText(object: Pick<MoqtObject, 'payload'>): string {
  return decoder.decode(object.payload);
}

/** Parse one UTF-8 JSON object payload. The result is not schema-validated. */
export function readJSON(object: Pick<MoqtObject, 'payload'>): unknown {
  return JSON.parse(readText(object));
}
