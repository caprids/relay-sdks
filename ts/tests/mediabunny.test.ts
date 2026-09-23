import { readFileSync } from 'node:fs';
import { buildMsfCatalog, LOC_AUDIO_CONFIG, LOC_TIMESTAMP, LOC_VIDEO_CONFIG } from '@mux/moq-media';
import { audioTrackFromDecoderConfig, packetToLocFrame, videoTrackFromDecoderConfig } from '@mux/moq-mediabunny';
import { BufferSource, EncodedPacket, EncodedPacketSink, Input, MP4 } from 'mediabunny';
import { expect, it } from 'vitest';

const fixture = readFileSync(new URL('./fixtures/mediabunny-avc.mp4', import.meta.url));
const avcc = Uint8Array.from(Buffer.from('0142c014ffe1000a6742c014da05067e784001000468ce3c80', 'hex'));
const config = { codec: 'avc1.42001F', codedWidth: 320, codedHeight: 180, description: avcc };

it('demuxes a real High-profile MP4 into catalog configuration and unchanged LOC payloads', async () => {
  const input = new Input({ source: new BufferSource(fixture), formats: [MP4] });
  try {
    const track = (await input.getPrimaryVideoTrack())!;
    const decoder = (await track.getDecoderConfig())!;
    const video = videoTrackFromDecoderConfig(decoder);
    expect(video.codec).toMatch(/^avc1\.64/);
    expect(video.width).toBe(32);
    expect(video.height).toBe(32);
    const catalog = JSON.parse(new TextDecoder().decode(buildMsfCatalog({ namespace: ['demo'], video })));
    expect(catalog.tracks[0].codec).toBe(video.codec);
    expect(catalog.initDataList[0].data).toBe(Buffer.from(video.initData!).toString('base64'));
    const frames: ReturnType<typeof packetToLocFrame>[] = [];
    for await (const packet of new EncodedPacketSink(track).packets()) {
      const frame = packetToLocFrame(packet, { videoConfig: video.initData! });
      expect(frame.payload).toBe(packet.data);
      expect(frame.properties.some((property) => property.type === LOC_VIDEO_CONFIG)).toBe(frame.key);
      frames.push(frame);
    }
    expect(frames.map((frame) => frame.key)).toEqual([true, false, true, false]);
    expect(frames.map((frame) => frame.timestampUs)).toEqual([0, 100000, 200000, 300000]);
    const metadata = (await new EncodedPacketSink(track).getFirstPacket({ metadataOnly: true }))!;
    expect(() => packetToLocFrame(metadata)).toThrow(/metadata-only/);
  } finally {
    input.dispose();
  }
});

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

it('maps mono Opus and attaches its config only to key packets', () => {
  const audio = audioTrackFromDecoderConfig({ codec: 'opus', sampleRate: 24000, numberOfChannels: 1 });
  expect(audio).toEqual({ codec: 'opus', samplerate: 48000, channelConfig: '1' });
  expect(() => audioTrackFromDecoderConfig({ codec: 'aac', sampleRate: 48000, numberOfChannels: 2 })).toThrow();
  expect(() => audioTrackFromDecoderConfig({ codec: 'opus', sampleRate: 48000, numberOfChannels: 0 })).toThrow();
  const packet = new EncodedPacket(new Uint8Array([1]), 'key', 0.02, 0.02);
  expect(packetToLocFrame(packet, { audioConfig: new Uint8Array([2]) }).properties).toContainEqual({
    type: LOC_AUDIO_CONFIG,
    value: new Uint8Array([2]),
  });
  const delta = new EncodedPacket(packet.data, 'delta', 0.04, 0.02);
  expect(packetToLocFrame(delta, { audioConfig: new Uint8Array([2]) }).properties).toHaveLength(2);
});

it('preserves B-frame presentation timing and permits an explicit shared timeline offset', () => {
  const packets = [0, 0.3, 0.1, 0.2].map((pts) => new EncodedPacket(new Uint8Array([1]), 'delta', pts, 0.1));
  expect(packets.map((packet) => packetToLocFrame(packet).timestampUs)).toEqual([0, 300000, 100000, 200000]);
  const negative = new EncodedPacket(new Uint8Array([1]), 'key', -0.02, 0.02);
  expect(() => packetToLocFrame(negative)).toThrow();
  const frame = packetToLocFrame(negative, { timestampOffsetUs: 20000, timescale: 90000 });
  expect(frame.timestampUs).toBe(0);
  expect(frame.properties[0]).toEqual({ type: LOC_TIMESTAMP, value: 0 });
  expect(() => packetToLocFrame(negative, { timestampOffsetUs: 0.5 })).toThrow();
  const overflow = new EncodedPacket(new Uint8Array([1]), 'key', Number.MAX_SAFE_INTEGER, 0);
  expect(() => packetToLocFrame(overflow)).toThrow();
});
