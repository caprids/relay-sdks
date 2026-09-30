/** LOC draft-04 property helpers, independent of transport and encoders. */
import type { KeyValuePair } from '@mux/moq-core';

/** The LOC draft these helpers implement. */
export const LOC_DOCUMENT = 'draft-ietf-moq-loc-04';
/** The MSF draft the catalog helpers implement. */
export const MSF_DOCUMENT = 'draft-ietf-moq-msf-01';
/** LOC Timestamp object property type: the object's presentation time in timescale ticks. */
export const LOC_TIMESTAMP = 0x10;
/** LOC Timescale property type: ticks per second, on the track and on each object. */
export const LOC_TIMESCALE = 0x08;
/** LOC Video Config object property type: codec configuration bytes, such as an avcC record. */
export const LOC_VIDEO_CONFIG = 0x0d;
/** LOC Audio Config object property type: codec configuration bytes, such as an AAC AudioSpecificConfig. */
export const LOC_AUDIO_CONFIG = 0x0f;
/** The default timescale: microsecond ticks, matching WebCodecs timestamps. */
export const MICROSECONDS_PER_SECOND = 1_000_000;
/** Default track name for the MSF catalog. */
export const CATALOG_TRACK = 'catalog';
/** Default track name for the catalog's video track. */
export const VIDEO_TRACK = 'video';
/** Default track name for the catalog's audio track. */
export const AUDIO_TRACK = 'audio';

function integer(value: number, name: string, positive = false): void {
  if (!Number.isSafeInteger(value) || value < (positive ? 1 : 0)) {
    throw new RangeError(`${name} must be a ${positive ? 'positive' : 'nonnegative'} safe integer`);
  }
}

/**
 * Track properties declaring a LOC timescale, for `registerTrack`'s `trackProperties`. The timescale must be a
 * positive safe integer; the default is microseconds.
 */
export function locTrackProperties(timescale = MICROSECONDS_PER_SECOND): KeyValuePair[] {
  integer(timescale, 'timescale', true);
  return [{ type: LOC_TIMESCALE, value: timescale }];
}

/** Options for {@link locProperties}. */
export interface LocPropertiesOptions {
  /** Ticks per second for the emitted Timestamp; a positive safe integer. Default {@link MICROSECONDS_PER_SECOND}. */
  timescale?: number;
  /** Supply decoder configuration on keyframes only. */
  videoConfig?: Uint8Array;
  /** Audio decoder configuration, emitted as Audio Config. Supply it on keyframes only. */
  audioConfig?: Uint8Array;
}

/** Convert microseconds to ticks, rounding ties upward, and attach LOC properties. */
export function locProperties(timestampUs: number, options: LocPropertiesOptions = {}): KeyValuePair[] {
  const timescale = options.timescale ?? MICROSECONDS_PER_SECOND;
  integer(timestampUs, 'timestampUs');
  integer(timescale, 'timescale', true);
  // Intermediate products can exceed Number.MAX_SAFE_INTEGER even when the result does not.
  const timestamp = Number((BigInt(timestampUs) * BigInt(timescale) + 500_000n) / 1_000_000n);
  integer(timestamp, 'timestamp');
  const properties: KeyValuePair[] = [
    { type: LOC_TIMESTAMP, value: timestamp },
    { type: LOC_TIMESCALE, value: timescale },
  ];
  if (options.videoConfig !== undefined)
    properties.push({ type: LOC_VIDEO_CONFIG, value: options.videoConfig.slice() });
  if (options.audioConfig !== undefined)
    properties.push({ type: LOC_AUDIO_CONFIG, value: options.audioConfig.slice() });
  return properties;
}
