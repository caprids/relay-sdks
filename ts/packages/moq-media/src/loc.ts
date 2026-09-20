/** LOC draft-04 property helpers, independent of transport and encoders. */
import type { KeyValuePair } from '@mux/moq-core';

export const LOC_DOCUMENT = 'draft-ietf-moq-loc-04';
export const MSF_DOCUMENT = 'draft-ietf-moq-msf-01';
export const LOC_TIMESTAMP = 0x10;
export const LOC_TIMESCALE = 0x08;
export const LOC_VIDEO_CONFIG = 0x0d;
export const LOC_AUDIO_CONFIG = 0x0f;
export const MICROSECONDS_PER_SECOND = 1_000_000;
export const CATALOG_TRACK = 'catalog';
export const VIDEO_TRACK = 'video';
export const AUDIO_TRACK = 'audio';

function integer(value: number, name: string, positive = false): void {
  if (!Number.isSafeInteger(value) || value < (positive ? 1 : 0)) {
    throw new RangeError(`${name} must be a ${positive ? 'positive' : 'nonnegative'} safe integer`);
  }
}

export function locTrackProperties(timescale = MICROSECONDS_PER_SECOND): KeyValuePair[] {
  integer(timescale, 'timescale', true);
  return [{ type: LOC_TIMESCALE, value: timescale }];
}

export interface LocPropertiesOptions {
  timescale?: number;
  /** Supply decoder configuration on keyframes only. */
  videoConfig?: Uint8Array;
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
