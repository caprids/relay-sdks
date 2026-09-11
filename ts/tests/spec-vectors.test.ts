/** Language-neutral encoding vectors shared by every SDK; see ../../spec/vectors/README.md. */
import { readFile } from 'node:fs/promises';

import { describe, expect, it } from 'vitest';

import { MoqtProtocolError } from '../packages/moq-core/src/protocol/errors.js';
import { decodeVarint, encodeVarint } from '../packages/moq-core/src/protocol/varint.js';

interface VarintVector {
  /** Decimal string, so 64-bit values survive JSON. */
  value: string;
  /** Lowercase hex, no separators. */
  bytes: string;
}

interface VarintVectors {
  minimal: VarintVector[];
  nonMinimal: VarintVector[];
  beyond53Bits: VarintVector[];
}

const vectors = JSON.parse(
  await readFile(new URL('../../spec/vectors/varint.json', import.meta.url), 'utf8'),
) as VarintVectors;
const hex = (bytes: Uint8Array) => Buffer.from(bytes).toString('hex');
const fromHex = (text: string) => Uint8Array.from(Buffer.from(text, 'hex'));

describe('spec/vectors/varint.json', () => {
  it('encodes every minimal vector to its bytes and decodes it back', () => {
    for (const { value, bytes } of vectors.minimal) {
      expect(hex(encodeVarint(Number(value)))).toBe(bytes);
      expect(decodeVarint(fromHex(bytes))).toEqual({ value: Number(value), byteLength: bytes.length / 2 });
    }
  });

  it('decodes non-minimal encodings without ever producing them', () => {
    for (const { value, bytes } of vectors.nonMinimal) {
      expect(decodeVarint(fromHex(bytes)).value).toBe(Number(value));
      expect(hex(encodeVarint(Number(value)))).not.toBe(bytes);
    }
  });

  it('rejects values above 2^53-1 instead of losing precision', () => {
    for (const { bytes } of vectors.beyond53Bits) {
      expect(() => decodeVarint(fromHex(bytes))).toThrow(MoqtProtocolError);
    }
  });
});
