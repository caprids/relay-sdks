import {
  audioTrackFromDecoderConfig,
  chunkToLocFrame,
  LOC_AUDIO_CONFIG,
  LOC_TIMESTAMP,
  LOC_VIDEO_CONFIG,
  videoTrackFromDecoderConfig,
} from '@caprid/moq-media';
import { expect, it } from 'vitest';

const avcc = Uint8Array.from(Buffer.from('0142c014ffe1000a6742c014da05067e784001000468ce3c80', 'hex'));
const config = { codec: 'avc1.42001F', codedWidth: 320, codedHeight: 180, description: avcc };

it.each(['view', 'buffer', 'shared'] as const)('copies the exact decoder description bytes from a %s', (kind) => {
  const backing = kind === 'shared' ? new SharedArrayBuffer(avcc.length + 4) : new ArrayBuffer(avcc.length + 4);
  new Uint8Array(backing).set(avcc, 2);
  const description = kind === 'buffer' ? avcc.slice().buffer : new DataView(backing, 2, avcc.length);
  const video = videoTrackFromDecoderConfig({ ...config, codec: 'avc3.000000', description });
  expect(video.initData).toEqual(avcc);
  expect(video.codec).toBe('avc3.42C014');
  new Uint8Array(backing).fill(0);
  if (description instanceof ArrayBuffer) new Uint8Array(description).fill(0);
  expect(video.initData).toEqual(avcc);
});

it('rejects Annex B, unsupported codecs, and non-four-byte NAL framing before advertising a track', () => {
  const { description: _, ...annexb } = config;
  expect(() => videoTrackFromDecoderConfig(annexb)).toThrow(/Annex B/);
  expect(() => videoTrackFromDecoderConfig({ ...config, description: new Uint8Array() })).toThrow();
  expect(() => videoTrackFromDecoderConfig({ ...config, codec: 'vp8' })).toThrow(/AVC/);
  const shortLengths = avcc.slice();
  shortLengths[4] = 0xfd;
  expect(() => videoTrackFromDecoderConfig({ ...config, description: shortLengths })).toThrow(/four-byte/);
});

it('maps mono Opus decoder configuration', () => {
  const audio = audioTrackFromDecoderConfig({ codec: 'opus', sampleRate: 24000, numberOfChannels: 1 });
  expect(audio).toEqual({ codec: 'opus', samplerate: 48000, channelConfig: '1' });
  expect(() => audioTrackFromDecoderConfig({ codec: 'aac', sampleRate: 48000, numberOfChannels: 2 })).toThrow();
  expect(() => audioTrackFromDecoderConfig({ codec: 'opus', sampleRate: 48000, numberOfChannels: 0 })).toThrow();
});

// Structural WebCodecs chunk: the package must not require a browser runtime to import or call.
function chunk(timestamp: number, type: EncodedVideoChunkType = 'key'): EncodedVideoChunk {
  const bytes = new Uint8Array([0, 0, 0, 2, 0x65, 0x88]);
  return {
    timestamp,
    type,
    duration: 33333,
    byteLength: bytes.length,
    copyTo(destination) {
      const view = ArrayBuffer.isView(destination)
        ? new Uint8Array(destination.buffer, destination.byteOffset, destination.byteLength)
        : new Uint8Array(destination);
      view.set(bytes);
    },
  };
}

it('copies unchanged encoded bytes, preserves microseconds, and attaches config only to key chunks', () => {
  const input = chunk(123456);
  const frame = chunkToLocFrame(input, { videoConfig: avcc });
  expect(frame.payload).toEqual(new Uint8Array([0, 0, 0, 2, 0x65, 0x88]));
  expect(frame.timestampUs).toBe(123456);
  expect(frame.key).toBe(true);
  expect(frame.properties).toContainEqual({ type: LOC_TIMESTAMP, value: 123456 });
  expect(frame.properties).toContainEqual({ type: LOC_VIDEO_CONFIG, value: avcc });
  frame.payload.fill(0);
  expect(chunkToLocFrame(input).payload[4]).toBe(0x65);
  const delta = chunkToLocFrame(chunk(156789, 'delta'), { videoConfig: avcc });
  expect(delta.key).toBe(false);
  expect(delta.properties).toHaveLength(2);
});

it('accepts audio chunks and copies the supplied audio configuration', () => {
  const input: EncodedAudioChunk = chunk(20000);
  const description = new Uint8Array([1, 2]);
  const audio = audioTrackFromDecoderConfig({ codec: 'opus', sampleRate: 48000, numberOfChannels: 2, description });
  const frame = chunkToLocFrame(input, { audioConfig: audio.initData! });
  description.fill(0);
  expect(frame.properties).toContainEqual({ type: LOC_AUDIO_CONFIG, value: new Uint8Array([1, 2]) });
});

it('preserves output order and normalizes negative presentation timestamps only with an explicit offset', () => {
  expect([0, 300000, 100000, 200000].map((pts) => chunkToLocFrame(chunk(pts)).timestampUs)).toEqual([
    0, 300000, 100000, 200000,
  ]);
  expect(() => chunkToLocFrame(chunk(-20000))).toThrow();
  const frame = chunkToLocFrame(chunk(-20000), { timestampOffsetUs: 30000, timescale: 90000 });
  expect(frame.timestampUs).toBe(10000);
  expect(frame.properties[0]).toEqual({ type: LOC_TIMESTAMP, value: 900 });
  expect(() => chunkToLocFrame(chunk(0.5), { timestampOffsetUs: 0.5 })).toThrow();
  expect(() => chunkToLocFrame(chunk(0.5))).toThrow();
  expect(() => chunkToLocFrame(chunk(Number.MAX_SAFE_INTEGER), { timestampOffsetUs: 1 })).toThrow();
});
