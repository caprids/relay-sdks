/**
 * Byte-level readers/writers shared by the MOQT wire codecs.
 *
 * `ByteReader`/`ByteWriter` are synchronous cursors over in-memory buffers, used for control-message bodies (always
 * fully buffered — the 16-bit message length bounds them). `StreamReader` is the asynchronous counterpart for
 * unidirectional data streams, where headers and objects arrive incrementally over a WebTransport `ReadableStream`.
 */
import { decodeVarint, encodeVarintInto, varintByteLength, varintLengthFromFirstByte } from './varint.js';

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

export function utf8Encode(value: string): Uint8Array {
  return textEncoder.encode(value);
}

export function utf8Decode(bytes: Uint8Array): string {
  return textDecoder.decode(bytes);
}

/** Synchronous cursor over a byte buffer. All reads throw `RangeError` past the end. */
export class ByteReader {
  #bytes: Uint8Array;
  #offset: number;

  constructor(bytes: Uint8Array, offset = 0) {
    this.#bytes = bytes;
    this.#offset = offset;
  }

  get offset(): number {
    return this.#offset;
  }

  get remaining(): number {
    return this.#bytes.length - this.#offset;
  }

  readVarint(): number {
    const { value, byteLength } = decodeVarint(this.#bytes, this.#offset);

    this.#offset += byteLength;
    return value;
  }

  /**
   * Read a count whose "unknown" sentinel is the maximal varint 2^64−1 — PUBLISH_DONE's Stream Count (§9.9). Exactly
   * that sentinel reads as `undefined`; any other value above this codec's 2^53−1 ceiling is still the protocol error
   * `readVarint` raises, since no real count gets there.
   */
  readCountVarint(): number | undefined {
    const offset = this.#offset;

    // 2^64−1 has one encoding: the 9-byte form with every bit set.
    if (this.remaining >= 9 && this.#bytes.subarray(offset, offset + 9).every((byte) => byte === 0xff)) {
      this.#offset += 9;
      return undefined;
    }

    return this.readVarint();
  }

  readUint8(): number {
    if (this.remaining < 1) throw new RangeError('read past end of buffer');

    return this.#bytes[this.#offset++]!;
  }

  readUint16(): number {
    if (this.remaining < 2) throw new RangeError('read past end of buffer');

    const value = this.#bytes[this.#offset]! * 256 + this.#bytes[this.#offset + 1]!;

    this.#offset += 2;
    return value;
  }

  /** Returns a view (not a copy) of the next `length` bytes. */
  readBytes(length: number): Uint8Array {
    if (length < 0 || this.remaining < length) throw new RangeError('read past end of buffer');

    const view = this.#bytes.subarray(this.#offset, this.#offset + length);

    this.#offset += length;
    return view;
  }
}

/** Growable byte sink for encoding wire structures. */
export class ByteWriter {
  #buffer: Uint8Array;
  #length = 0;

  constructor(initialCapacity = 256) {
    this.#buffer = new Uint8Array(initialCapacity);
  }

  /** Number of bytes written so far. */
  get length(): number {
    return this.#length;
  }

  #ensure(extra: number): void {
    const needed = this.#length + extra;
    if (needed <= this.#buffer.length) return;

    const grown = new Uint8Array(Math.max(needed, this.#buffer.length * 2));

    grown.set(this.#buffer.subarray(0, this.#length));
    this.#buffer = grown;
  }

  /** Appends a MOQT varint (§8.1). Throws `RangeError` unless `value` is a non-negative safe integer. */
  writeVarint(value: number): void {
    this.#ensure(varintByteLength(value));
    this.#length += encodeVarintInto(this.#buffer, this.#length, value);
  }

  /** Appends the low 8 bits of `value`. */
  writeUint8(value: number): void {
    this.#ensure(1);
    this.#buffer[this.#length++] = value & 0xff;
  }

  /** Appends the low 16 bits of `value`, big-endian. */
  writeUint16(value: number): void {
    this.#ensure(2);
    this.#buffer[this.#length++] = (value >> 8) & 0xff;
    this.#buffer[this.#length++] = value & 0xff;
  }

  /** Appends `bytes` as-is, with no length prefix. */
  writeBytes(bytes: Uint8Array): void {
    this.#ensure(bytes.length);
    this.#buffer.set(bytes, this.#length);
    this.#length += bytes.length;
  }

  /** Varint length prefix followed by the bytes. */
  writeLengthPrefixed(bytes: Uint8Array): void {
    this.writeVarint(bytes.length);
    this.writeBytes(bytes);
  }

  /** Returns a copy of the written bytes. */
  toBytes(): Uint8Array {
    return this.#buffer.slice(0, this.#length);
  }
}

/**
 * Buffered asynchronous reader over a `ReadableStream<Uint8Array>`.
 *
 * Reads never split a varint or byte run: each method pulls chunks until it can satisfy the request. A clean
 * end-of-stream mid-read throws `RangeError`; use `atEnd()` to probe for a clean boundary first.
 */
export class StreamReader {
  #reader: ReadableStreamDefaultReader<Uint8Array>;
  #chunks: Uint8Array[] = [];
  #buffered = 0;
  #done = false;

  constructor(stream: ReadableStream<Uint8Array>) {
    this.#reader = stream.getReader();
  }

  async #pull(): Promise<boolean> {
    if (this.#done) return false;

    const { done, value } = await this.#reader.read();

    if (done) {
      this.#done = true;
      return false;
    }

    if (value.length > 0) {
      this.#chunks.push(value);
      this.#buffered += value.length;
    }

    return true;
  }

  async #fill(length: number): Promise<void> {
    while (this.#buffered < length) {
      if (!(await this.#pull())) throw new RangeError('unexpected end of stream');
    }
  }

  #take(length: number): Uint8Array {
    const out = new Uint8Array(length);
    let copied = 0;

    while (copied < length) {
      const head = this.#chunks[0]!;
      const take = Math.min(head.length, length - copied);

      out.set(head.subarray(0, take), copied);

      if (take === head.length) {
        this.#chunks.shift();
      } else {
        this.#chunks[0] = head.subarray(take);
      }

      copied += take;
    }

    this.#buffered -= length;
    return out;
  }

  /** True when the stream ended cleanly with no bytes left unread. */
  async atEnd(): Promise<boolean> {
    while (this.#buffered === 0) {
      if (!(await this.#pull())) return true;
    }

    return false;
  }

  async readBytes(length: number): Promise<Uint8Array> {
    await this.#fill(length);
    return this.#take(length);
  }

  async readUint8(): Promise<number> {
    await this.#fill(1);
    return this.#take(1)[0]!;
  }

  async readVarint(): Promise<number> {
    await this.#fill(1);
    const firstByte = this.#chunks[0]![0]!;
    const byteLength = varintLengthFromFirstByte(firstByte);

    await this.#fill(byteLength);
    return decodeVarint(this.#take(byteLength)).value;
  }

  /** Cancel the underlying stream (e.g. drop an unwanted data stream). */
  async cancel(reason?: unknown): Promise<void> {
    try {
      await this.#reader.cancel(reason);
    } finally {
      this.#reader.releaseLock();
    }
  }

  releaseLock(): void {
    this.#reader.releaseLock();
  }
}
