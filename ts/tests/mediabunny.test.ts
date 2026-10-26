import { readFileSync } from 'node:fs';
import {
  buildMsfCatalog,
  LOC_AUDIO_CONFIG,
  LOC_TIMESTAMP,
  LOC_VIDEO_CONFIG,
  videoTrackFromDecoderConfig,
} from '@caprid/moq-media';
import { packetToLocFrame } from '@caprid/moq-mediabunny';
import { BufferSource, EncodedPacket, EncodedPacketSink, Input, MP4 } from 'mediabunny';
import { expect, it } from 'vitest';

const fixture = readFileSync(new URL('./fixtures/mediabunny-avc.mp4', import.meta.url));
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

it('attaches audio configuration only to key packets', () => {
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
