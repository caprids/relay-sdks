/** WebCodecs input helpers. No runtime dependency on a browser or media engine. */
import type { AudioTrackInfo, VideoTrackInfo } from './catalog.js';
import { avcCodecString } from './h264.js';
import { type LocPropertiesOptions, locProperties } from './loc.js';

function copyDescription(description: AllowSharedBufferSource | undefined): Uint8Array | undefined {
  if (!description || description.byteLength === 0) return undefined;
  return ArrayBuffer.isView(description)
    ? new Uint8Array(description.buffer, description.byteOffset, description.byteLength).slice()
    : new Uint8Array(description).slice();
}

/** Map an AVC WebCodecs decoder config; encoded packets must already be length-prefixed. */
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
  /** Chunk helpers copy these bytes; packet adapters may borrow them. Keep stable until the write completes. */
  payload: Uint8Array;
  timestampUs: number;
  key: boolean;
  properties: ReturnType<typeof locProperties>;
}

export interface ChunkToLocFrameOptions extends LocPropertiesOptions {
  /** Added to the chunk timestamp. Use one shared offset across tracks to normalize negative timestamps. */
  timestampOffsetUs?: number;
}

/** Copy the chunk's encoded bytes unchanged and attach LOC metadata; no muxing or bitstream conversion. */
export function chunkToLocFrame(
  chunk: EncodedVideoChunk | EncodedAudioChunk,
  options: ChunkToLocFrameOptions = {}
): LocFrame {
  const offset = options.timestampOffsetUs ?? 0;
  if (!Number.isSafeInteger(offset)) throw new Error('timestampOffsetUs must be a safe integer');
  if (!Number.isSafeInteger(chunk.timestamp)) throw new Error('Chunk timestamp must be safe integer microseconds');
  const timestampUs = chunk.timestamp + offset;
  const key = chunk.type === 'key';
  const properties = locProperties(timestampUs, {
    ...(options.timescale !== undefined ? { timescale: options.timescale } : {}),
    ...(key && options.videoConfig ? { videoConfig: options.videoConfig } : {}),
    ...(key && options.audioConfig ? { audioConfig: options.audioConfig } : {}),
  });
  const payload = new Uint8Array(chunk.byteLength);
  chunk.copyTo(payload);
  return { payload, timestampUs, key, properties };
}
