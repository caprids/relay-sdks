/**
 * A synchronous Opus decoder that can fill a lost frame: libopus compiled to WebAssembly (src/opus-decoder-wasm.ts,
 * built by scripts/build-opus-wasm.sh). WebCodecs' AudioDecoder has no packet-loss concealment and no way to decode the
 * in-band FEC an Opus encoder puts in the next packet, so a player built on it can only play a lost frame as silence.
 * Internal to the audio player; not exported from the package.
 */
import { OPUS_DECODER_WASM_BASE64 } from './opus-decoder-wasm.js';

/** The module's exports. Pointers and sizes are byte offsets into `memory`. */
interface OpusExports {
  readonly memory: WebAssembly.Memory;
  mux_opus_create(sampleRate: number, channels: number): number;
  mux_opus_decode(decoder: number, data: number, length: number, pcm: number, frameSize: number, fec: number): number;
  mux_opus_reset(decoder: number): number;
  mux_opus_has_lbrr(data: number, length: number): number;
  mux_opus_destroy(decoder: number): void;
  mux_opus_malloc(size: number): number;
  mux_opus_free(pointer: number): void;
}

/** Decoded audio: one plane per channel, at the decoder's sample rate. */
export type OpusPlanes = Float32Array[];

/** One libopus decoder. Every method is synchronous; a negative libopus result throws. */
export interface WasmOpusDecoder {
  readonly sampleRate: number;
  readonly channels: number;
  /** Decode a packet. */
  decode(packet: Uint8Array): OpusPlanes;
  /** Conceal `samples` of lost audio (libopus PLC), continuing from what was decoded last. */
  conceal(samples: number): OpusPlanes;
  /** Rebuild the `samples` before `next` from the in-band FEC `next` carries; `undefined` when it carries none. */
  recover(next: Uint8Array, samples: number): OpusPlanes | undefined;
  /** Forget the decoding state, as at a new stream. */
  reset(): void;
  /** Free the decoder. */
  close(): void;
}

/** Creates decoders on one instance of the module. */
export interface WasmOpusModule {
  createDecoder(sampleRate: number, channels: number): WasmOpusDecoder;
}

/** The most samples per channel one Opus packet decodes to at 48 kHz (120 ms). */
const MAX_FRAME_SAMPLES = 5760;
/** The largest valid Opus packet: 48 frames of at most 1275 bytes, and their framing (RFC 6716 §3.2.5). */
const MAX_PACKET_BYTES = 48 * 1275 + 100;
/** The input buffer a decoder starts with; it grows to a larger packet's size. */
const INITIAL_PACKET_BYTES = 1500;

let loading: Promise<WasmOpusModule> | undefined;

/** Options for {@link loadWasmOpus}. */
export interface LoadWasmOpusOptions {
  /**
   * Decode the embedded module's base64 with the engine's own `Uint8Array.fromBase64` where it exists, instead of a
   * JavaScript loop that holds the main thread for several milliseconds (tens on a busy page). Same bytes either way.
   * Only the first load in a realm decodes; later calls share it. Default false.
   */
  nativeBase64?: boolean;
}

/** Compile and instantiate the module, once per realm. Rejects where WebAssembly is unavailable. */
export function loadWasmOpus(options: LoadWasmOpusOptions = {}): Promise<WasmOpusModule> {
  loading ??= instantiate(options.nativeBase64 === true).catch((error: unknown) => {
    loading = undefined;
    throw error;
  });
  return loading;
}

function base64Bytes(base64: string): Uint8Array {
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index++) bytes[index] = binary.charCodeAt(index);
  return bytes;
}

/** The engine's own base64 decoder (`Uint8Array.fromBase64`), where it has one. */
const nativeBase64Decoder = (): ((base64: string) => Uint8Array) | undefined => {
  const typed = Uint8Array as unknown as { fromBase64?: (base64: string) => Uint8Array };
  return typeof typed.fromBase64 === 'function'
    ? (base64) => (typed.fromBase64 as (b: string) => Uint8Array)(base64)
    : undefined;
};

async function instantiate(nativeBase64: boolean): Promise<WasmOpusModule> {
  if (typeof WebAssembly === 'undefined') throw new Error('WebAssembly is unavailable');
  const native = nativeBase64 ? nativeBase64Decoder() : undefined;
  // Asynchronous compilation: browsers refuse a synchronous compile of a module this size on the main thread.
  const bytes = (
    native ? native(OPUS_DECODER_WASM_BASE64) : base64Bytes(OPUS_DECODER_WASM_BASE64)
  ) as Uint8Array<ArrayBuffer>;
  const { instance } = await WebAssembly.instantiate(bytes, {});
  const exports = instance.exports as unknown as OpusExports;
  return { createDecoder: (sampleRate, channels) => createDecoder(exports, sampleRate, channels) };
}

function createDecoder(wasm: OpusExports, sampleRate: number, channels: number): WasmOpusDecoder {
  const handle = wasm.mux_opus_create(sampleRate, channels);
  if (!handle) throw new Error(`libopus refused a decoder at ${sampleRate} Hz, ${channels} channels`);
  let input = 0;
  let inputBytes = 0;
  let output = 0;
  const release = () => {
    if (input) wasm.mux_opus_free(input);
    if (output) wasm.mux_opus_free(output);
    wasm.mux_opus_destroy(handle);
  };
  try {
    input = wasm.mux_opus_malloc(INITIAL_PACKET_BYTES);
    inputBytes = INITIAL_PACKET_BYTES;
    output = wasm.mux_opus_malloc(MAX_FRAME_SAMPLES * channels * 4);
    if (!input || !output) throw new Error('libopus could not allocate its buffers');
  } catch (error) {
    release();
    throw error;
  }
  let closed = false;

  const write = (packet: Uint8Array) => {
    if (packet.byteLength > MAX_PACKET_BYTES) throw new RangeError('Opus packet too large');
    if (packet.byteLength > inputBytes) {
      const grown = wasm.mux_opus_malloc(packet.byteLength);
      if (!grown) throw new Error('libopus could not allocate its input buffer');
      wasm.mux_opus_free(input);
      input = grown;
      inputBytes = packet.byteLength;
    }
    // The memory may have grown since the last call: take a fresh view each time.
    new Uint8Array(wasm.memory.buffer, input, packet.byteLength).set(packet);
  };
  const planes = (count: number): OpusPlanes => {
    if (count < 0) throw new Error(`libopus error ${count}`);
    const interleaved = new Float32Array(wasm.memory.buffer, output, count * channels);
    return Array.from({ length: channels }, (_, channel) => {
      const plane = new Float32Array(count);
      for (let index = 0; index < count; index++) plane[index] = interleaved[index * channels + channel] as number;
      return plane;
    });
  };
  const live = () => {
    if (closed) throw new Error('The Opus decoder is closed');
  };

  return {
    sampleRate,
    channels,
    decode(packet) {
      live();
      // The shim reads a zero length as a concealment request: an empty packet is not one.
      if (packet.byteLength === 0) throw new RangeError('An Opus packet has at least one byte');
      write(packet);
      return planes(wasm.mux_opus_decode(handle, input, packet.byteLength, output, MAX_FRAME_SAMPLES, 0));
    },
    conceal(samples) {
      live();
      return planes(wasm.mux_opus_decode(handle, 0, 0, output, Math.min(samples, MAX_FRAME_SAMPLES), 0));
    },
    recover(next, samples) {
      live();
      if (next.byteLength === 0) return undefined;
      write(next);
      if (wasm.mux_opus_has_lbrr(input, next.byteLength) !== 1) return undefined;
      return planes(
        wasm.mux_opus_decode(handle, input, next.byteLength, output, Math.min(samples, MAX_FRAME_SAMPLES), 1)
      );
    },
    reset() {
      live();
      wasm.mux_opus_reset(handle);
    },
    close() {
      if (closed) return;
      closed = true;
      release();
    },
  };
}
