/**
 * MOQT variable-length integers (moq-transport draft-22 §8.1).
 *
 * NOT the QUIC RFC 9000 varint: draft-15 replaced it with a leading-ones scheme. The number of leading 1 bits of the
 * first byte gives the encoded length minus one (1-9 bytes); the bits after the first 0, plus any subsequent bytes,
 * hold the value in network byte order.
 *
 * Values are represented as JS `number`. The wire format allows up to 2^64-1, but this decoder rejects anything above
 * `Number.MAX_SAFE_INTEGER` (2^53-1) with a `MoqtProtocolError` — no field a media subscriber consumes legitimately
 * needs more (Group IDs derived from Unix-epoch milliseconds are ~2^41), and silent precision loss would corrupt IDs.
 */
import { MoqtProtocolError } from './errors.js';

export interface VarintDecodeResult {
  value: number;
  /** Total encoded size in bytes (1-9). */
  byteLength: number;
}

/** Largest value `encodeVarint` accepts (`Number.MAX_SAFE_INTEGER`). */
export const MAX_VARINT_VALUE = Number.MAX_SAFE_INTEGER;

/** Encoded length in bytes implied by a varint's first byte. */
export function varintLengthFromFirstByte(firstByte: number): number {
  let leadingOnes = 0;
  let mask = 0x80;

  while (leadingOnes < 8 && (firstByte & mask) !== 0) {
    leadingOnes++;
    mask >>= 1;
  }

  return leadingOnes + 1;
}

/**
 * Decode a varint at `offset`. Throws `RangeError` when `bytes` ends before the encoding completes, and
 * `MoqtProtocolError` when the decoded value exceeds `MAX_VARINT_VALUE`.
 */
export function decodeVarint(bytes: Uint8Array, offset = 0): VarintDecodeResult {
  const firstByte = bytes[offset];
  if (firstByte === undefined) throw new RangeError('varint: unexpected end of input');

  const byteLength = varintLengthFromFirstByte(firstByte);
  if (offset + byteLength > bytes.length) throw new RangeError('varint: unexpected end of input');

  // Value bits contributed by the first byte: everything after the
  // leading-ones prefix and its terminating 0. For a 9-byte encoding
  // (prefix 0xFF) the first byte contributes nothing.
  const firstByteBits = byteLength <= 8 ? firstByte & (0xff >> byteLength) : 0;

  if (byteLength <= 7) {
    // Up to 49 value bits — exact in double-precision number math.
    let value = firstByteBits;

    for (let i = 1; i < byteLength; i++) value = value * 256 + bytes[offset + i]!;

    return { value, byteLength };
  }

  // 8-9 byte encodings can exceed 2^53 — accumulate in bigint, then bound.
  let big = BigInt(firstByteBits);

  for (let i = 1; i < byteLength; i++) big = big * 256n + BigInt(bytes[offset + i]!);

  if (big > BigInt(MAX_VARINT_VALUE)) {
    throw new MoqtProtocolError(`varint: value ${big} exceeds supported range (2^53-1)`);
  }

  return { value: Number(big), byteLength };
}

/** Minimal encoded length in bytes for `value`. */
export function varintByteLength(value: number): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError(`varint: value out of range: ${value}`);
  }

  if (value < 2 ** 7) return 1;

  if (value < 2 ** 14) return 2;

  if (value < 2 ** 21) return 3;

  if (value < 2 ** 28) return 4;

  if (value < 2 ** 35) return 5;

  if (value < 2 ** 42) return 6;

  if (value < 2 ** 49) return 7;

  // 8 bytes carry 56 value bits, which covers everything up to 2^53-1.
  return 8;
}

/** Encode `value` into `target` at `offset` using the minimal length. Returns the number of bytes written. */
export function encodeVarintInto(target: Uint8Array, offset: number, value: number): number {
  const byteLength = varintByteLength(value);
  // Fill value bytes from the least-significant end. Number math is exact
  // here: value < 2^53 and each step divides by 256.
  let remaining = value;

  for (let i = byteLength - 1; i >= 1; i--) {
    target[offset + i] = remaining % 256;
    remaining = Math.floor(remaining / 256);
  }

  // First byte: leading-ones prefix for (byteLength - 1) ones, then the
  // top value bits.
  const prefix = byteLength === 1 ? 0 : (0xff << (9 - byteLength)) & 0xff;

  target[offset] = prefix | remaining;
  return byteLength;
}

export function encodeVarint(value: number): Uint8Array {
  const bytes = new Uint8Array(varintByteLength(value));

  encodeVarintInto(bytes, 0, value);
  return bytes;
}
