import { describe, expect, it } from 'vitest';

import { MoqtProtocolError } from '../protocol/errors.js';
import { decodeVarint, encodeVarint, varintByteLength, varintLengthFromFirstByte } from '../protocol/varint.js';

function bytes(...values: number[]): Uint8Array {
  return new Uint8Array(values);
}

// Golden vectors from moq-transport draft-22 §8.1, Table 4.
const SPEC_VECTORS: [Uint8Array, number][] = [
  [bytes(0x25), 37],
  [bytes(0x80, 0x25), 37],
  [bytes(0xbb, 0xbd), 15_293],
  [bytes(0xed, 0x7f, 0x3e, 0x7d), 226_442_877],
  [bytes(0xfa, 0xa1, 0xa0, 0xe4, 0x03, 0xd8), 2_893_212_287_960],
  [bytes(0xfc, 0x89, 0x98, 0xab, 0xc6, 0x6b, 0xc0), 151_288_809_941_952],
];

describe('decodeVarint', () => {
  it('decodes the spec example encodings', () => {
    for (const [encoded, value] of SPEC_VECTORS) {
      expect(decodeVarint(encoded)).toEqual({ value, byteLength: encoded.length });
    }
  });

  it('accepts non-minimal encodings (any representable length is valid)', () => {
    expect(decodeVarint(bytes(0x00)).value).toBe(0);
    expect(decodeVarint(bytes(0x80, 0x00)).value).toBe(0);
    expect(decodeVarint(bytes(0xc0, 0x00, 0x00)).value).toBe(0);
  });

  it('decodes at an offset', () => {
    expect(decodeVarint(bytes(0xff, 0xff, 0x25), 2)).toEqual({ value: 37, byteLength: 1 });
  });

  it('throws RangeError on truncated input', () => {
    expect(() => decodeVarint(bytes())).toThrow(RangeError);
    expect(() => decodeVarint(bytes(0xbb))).toThrow(RangeError);
    expect(() => decodeVarint(bytes(0xed, 0x7f, 0x3e))).toThrow(RangeError);
  });

  it('rejects values above 2^53-1 with MoqtProtocolError', () => {
    // 0xfefa318fa8e3ca11 = 70,423,237,261,249,041 (> 2^53-1) from the spec table.
    expect(() => decodeVarint(bytes(0xfe, 0xfa, 0x31, 0x8f, 0xa8, 0xe3, 0xca, 0x11))).toThrow(MoqtProtocolError);
    expect(() => decodeVarint(bytes(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff))).toThrow(MoqtProtocolError);
  });

  it('accepts 9-byte encodings of in-range values', () => {
    // 9-byte non-minimal encoding of 1.
    expect(decodeVarint(bytes(0xff, 0, 0, 0, 0, 0, 0, 0, 1))).toEqual({ value: 1, byteLength: 9 });
  });
});

describe('encodeVarint', () => {
  it('round-trips boundary values at each length', () => {
    const boundaries = [
      0,
      127,
      128,
      16_383,
      16_384,
      2 ** 21 - 1,
      2 ** 21,
      2 ** 28 - 1,
      2 ** 28,
      2 ** 35 - 1,
      2 ** 35,
      2 ** 42 - 1,
      2 ** 42,
      2 ** 49 - 1,
      2 ** 49,
      Number.MAX_SAFE_INTEGER,
    ];

    for (const value of boundaries) {
      const encoded = encodeVarint(value);

      expect(encoded.length).toBe(varintByteLength(value));
      expect(decodeVarint(encoded)).toEqual({ value, byteLength: encoded.length });
    }
  });

  it('produces minimal encodings matching the spec vectors', () => {
    expect(encodeVarint(37)).toEqual(bytes(0x25));
    expect(encodeVarint(15_293)).toEqual(bytes(0xbb, 0xbd));
    expect(encodeVarint(226_442_877)).toEqual(bytes(0xed, 0x7f, 0x3e, 0x7d));
    expect(encodeVarint(2_893_212_287_960)).toEqual(bytes(0xfa, 0xa1, 0xa0, 0xe4, 0x03, 0xd8));
  });

  it('rejects negative and unsafe values', () => {
    expect(() => encodeVarint(-1)).toThrow(RangeError);
    expect(() => encodeVarint(1.5)).toThrow(RangeError);
    expect(() => encodeVarint(Number.MAX_SAFE_INTEGER + 2)).toThrow(RangeError);
  });
});

describe('varintLengthFromFirstByte', () => {
  it('maps prefixes to lengths', () => {
    expect(varintLengthFromFirstByte(0x00)).toBe(1);
    expect(varintLengthFromFirstByte(0x7f)).toBe(1);
    expect(varintLengthFromFirstByte(0x80)).toBe(2);
    expect(varintLengthFromFirstByte(0xbf)).toBe(2);
    expect(varintLengthFromFirstByte(0xc0)).toBe(3);
    expect(varintLengthFromFirstByte(0xe0)).toBe(4);
    expect(varintLengthFromFirstByte(0xf0)).toBe(5);
    expect(varintLengthFromFirstByte(0xf8)).toBe(6);
    expect(varintLengthFromFirstByte(0xfc)).toBe(7);
    expect(varintLengthFromFirstByte(0xfe)).toBe(8);
    expect(varintLengthFromFirstByte(0xff)).toBe(9);
  });
});
