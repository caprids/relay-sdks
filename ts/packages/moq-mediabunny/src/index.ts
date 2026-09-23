import { type LocFrame, type LocPropertiesOptions, locProperties } from '@mux/moq-media';
import type { EncodedPacket } from 'mediabunny';

// Compatibility exports. New code should import the WebCodecs helpers/types from @mux/moq-media.
export { audioTrackFromDecoderConfig, type LocFrame, videoTrackFromDecoderConfig } from '@mux/moq-media';

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
