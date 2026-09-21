import {
  type AudioTrackInfo,
  avcCodecString,
  type LocPropertiesOptions,
  locProperties,
  type VideoTrackInfo,
} from '@mux/moq-media';
import type { EncodedPacket } from 'mediabunny';

function copyDescription(description: AllowSharedBufferSource | undefined): Uint8Array | undefined {
  if (!description || description.byteLength === 0) return undefined;
  return ArrayBuffer.isView(description)
    ? new Uint8Array(description.buffer, description.byteOffset, description.byteLength).slice()
    : new Uint8Array(description).slice();
}

/** Map an AVC decoder config from Mediabunny or WebCodecs; encoded packets must already be length-prefixed. */
export function videoTrackFromDecoderConfig(config: VideoDecoderConfig): VideoTrackInfo {
  if (!/^avc[13]\./.test(config.codec)) throw new Error('The video adapter currently supports AVC (H.264)');
  const initData = copyDescription(config.description);
  if (!initData || initData.length < 7 || ((initData[4] ?? 0) & 3) !== 3) {
    throw new Error('AVC requires decoderConfig.description with four-byte NAL lengths; Annex B is not supported');
  }
  const codec = config.codec.slice(0, 4) + avcCodecString(initData).slice(4);
  if (
    !config.codedWidth ||
    !Number.isSafeInteger(config.codedWidth) ||
    config.codedWidth < 1 ||
    !config.codedHeight ||
    !Number.isSafeInteger(config.codedHeight) ||
    config.codedHeight < 1
  )
    throw new Error('AVC requires positive integer codedWidth and codedHeight');
  return { codec, width: config.codedWidth, height: config.codedHeight, initData };
}

/** Map an Opus decoder config, preserving its channel count and using the LOC/MSF 48 kHz convention. */
export function audioTrackFromDecoderConfig(config: AudioDecoderConfig): AudioTrackInfo {
  if (config.codec !== 'opus') throw new Error('The audio adapter currently supports Opus');
  if (!Number.isSafeInteger(config.numberOfChannels) || config.numberOfChannels < 1) {
    throw new Error('Opus requires a positive channel count');
  }
  const initData = copyDescription(config.description);
  return {
    codec: 'opus',
    samplerate: 48000,
    channelConfig: String(config.numberOfChannels),
    ...(initData ? { initData } : {}),
  };
}

export interface LocFrame {
  /** Borrowed packet data; keep it unchanged until the write completes. */
  payload: Uint8Array;
  timestampUs: number;
  key: boolean;
  properties: ReturnType<typeof locProperties>;
}

export interface PacketToLocFrameOptions extends LocPropertiesOptions {
  /** Added to the packet timestamp. Use one shared offset for all tracks with negative file timestamps. */
  timestampOffsetUs?: number;
}

/** Preserve encoded bytes and presentation timing; attach decoder configuration only to key packets. */
export function packetToLocFrame(packet: EncodedPacket, options: PacketToLocFrameOptions = {}): LocFrame {
  if (packet.isMetadataOnly) throw new Error('LOC requires packet payloads; metadata-only packets cannot be published');
  const offset = options.timestampOffsetUs ?? 0;
  if (!Number.isSafeInteger(offset)) throw new Error('timestampOffsetUs must be a safe integer');
  const timestamp = packet.microsecondTimestamp;
  if (!Number.isSafeInteger(timestamp)) throw new Error('Packet timestamp must be safe integer microseconds');
  const timestampUs = timestamp + offset;
  const key = packet.type === 'key';
  const properties = locProperties(timestampUs, {
    ...(options.timescale !== undefined ? { timescale: options.timescale } : {}),
    ...(key && options.videoConfig ? { videoConfig: options.videoConfig } : {}),
    ...(key && options.audioConfig ? { audioConfig: options.audioConfig } : {}),
  });
  return { payload: packet.data, timestampUs, key, properties };
}
