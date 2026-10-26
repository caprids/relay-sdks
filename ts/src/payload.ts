import type { MoqtObject } from '@caprid/moq-core';

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

/** Decode one complete object as UTF-8. Malformed UTF-8 throws a TypeError. */
export function readText(object: Pick<MoqtObject, 'payload'>): string {
  return decoder.decode(object.payload);
}

/** Parse one UTF-8 JSON object payload. The result is not schema-validated. */
export function readJSON(object: Pick<MoqtObject, 'payload'>): unknown {
  return JSON.parse(readText(object));
}
