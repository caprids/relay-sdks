import { MAX_TIMEOUT_MS } from './connect-session.js';
import { SUBGROUP_DELIVERY_TIMEOUT } from './delivery-timeouts.js';
import type { PublishProperty } from './publish.js';
import { DEFAULT_PUBLISHER_PRIORITY, MAX_DEFAULT_PUBLISHER_PRIORITY } from './validation.js';

/** What a media track carries, which decides its default delivery timeout and publisher priority. */
export type MediaKind = 'audio' | 'video';

/** Options for {@link mediaTrackProperties}. Every duration is a positive integer number of microseconds. */
export interface MediaTrackPropertiesOptions {
  /** The encoder's keyframe interval. Required for video; audio ignores it. */
  keyframeIntervalUs?: number;
  /**
   * How far behind live a viewer plays (see `CONTEXT.md`); at least the keyframe interval. Default twice the keyframe
   * interval, the default the frame reader takes too (#303). Audio ignores it.
   */
  targetLatencyUs?: number;
  /** The subgroup timeout itself, replacing the default for either kind. */
  subgroupTimeoutUs?: number;
  /**
   * The track's Default Publisher Priority (0x0E), an integer from 0 to 255 where lower is more urgent, replacing the
   * default for either kind: 16 for audio, 96 for video.
   */
  publisherPriority?: number;
}

/** Audio's default subgroup timeout: measured under a 2 s uplink stall to sjc ([#302](https://github.com/muxinc/relay-sdks/issues/302)). */
const AUDIO_SUBGROUP_TIMEOUT_US = 500_000;
/** Default Publisher Priority per kind, lower being more urgent: audio ahead of video, as Huddle sets them by hand. */
const PUBLISHER_PRIORITY: Readonly<Record<MediaKind, number>> = { audio: 16, video: 96 };
/** The largest value whose whole milliseconds the write slot's timers hold. */
const MAX_TIMEOUT_US = MAX_TIMEOUT_MS * 1000;

/**
 * The track properties that give a media track its default delivery timeout and publisher priority (spec/behavior.md,
 * "Media publishing"), so a stall costs stale frames rather than lasting latency. Pass them to `registerTrack` with the track's format
 * properties, such as `@mux/moq-media`'s `locTrackProperties()`:
 *
 * ```ts
 * const video = publisher.registerTrack({
 *   track: 'video',
 *   trackProperties: [...locTrackProperties(), ...mediaTrackProperties('video', { keyframeIntervalUs: 1_000_000 })],
 * });
 * ```
 *
 * The result is a SUBGROUP_DELIVERY_TIMEOUT (0x06) in whole milliseconds, rounded up: 500 ms for audio, and for
 * video the target latency less the keyframe interval, but never less than the interval. Then a Default Publisher
 * Priority (0x0E) that puts audio ahead of video: 16 for audio, 96 for video. moq-relay 0.17 passes it on to viewers
 * in SUBSCRIBE_OK but orders delivery by subscriber priority alone, so on that relay it changes nothing yet. No object
 * timeout is set:
 * through a frame writer it would never fire, because a group's wait for settling room is not counted and, once its
 * stream is open, a browser's transport takes each object at once even during a stall.
 */
export function mediaTrackProperties(kind: MediaKind, options: MediaTrackPropertiesOptions = {}): PublishProperty[] {
  if (typeof kind !== 'string') throw new TypeError("kind must be 'audio' or 'video'");
  if (kind !== 'audio' && kind !== 'video') throw new RangeError("kind must be 'audio' or 'video'");
  const keyframeIntervalUs = optionalMicroseconds(options.keyframeIntervalUs, 'keyframeIntervalUs');
  const targetLatencyUs = optionalMicroseconds(options.targetLatencyUs, 'targetLatencyUs');
  const subgroupTimeoutUs = optionalMicroseconds(options.subgroupTimeoutUs, 'subgroupTimeoutUs');
  const publisherPriority = optionalPriority(options.publisherPriority);
  if (kind === 'video' && keyframeIntervalUs === undefined) {
    throw new TypeError('keyframeIntervalUs is required for video');
  }
  if (keyframeIntervalUs !== undefined && targetLatencyUs !== undefined && targetLatencyUs < keyframeIntervalUs) {
    throw new RangeError('targetLatencyUs must be at least keyframeIntervalUs');
  }
  return [
    { type: SUBGROUP_DELIVERY_TIMEOUT, value: Math.ceil(timeoutUs() / 1000) },
    { type: DEFAULT_PUBLISHER_PRIORITY, value: publisherPriority ?? PUBLISHER_PRIORITY[kind] },
  ];

  function timeoutUs(): number {
    if (subgroupTimeoutUs !== undefined) return subgroupTimeoutUs;
    if (kind === 'audio') return AUDIO_SUBGROUP_TIMEOUT_US;
    const interval = keyframeIntervalUs as number;
    return Math.max(interval, (targetLatencyUs ?? 2 * interval) - interval);
  }
}

/** A positive safe integer of microseconds whose whole milliseconds fit the timer bound, or `undefined`. */
function optionalMicroseconds(value: unknown, name: string): number | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== 'number') throw new TypeError(`${name} must be a number of microseconds`);
  if (!Number.isSafeInteger(value) || value <= 0 || value > MAX_TIMEOUT_US) {
    throw new RangeError(`${name} must be a positive integer of at most ${MAX_TIMEOUT_US} microseconds`);
  }
  return value;
}

/** An integer Default Publisher Priority from 0 to 255, or `undefined`. */
function optionalPriority(value: unknown): number | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== 'number') throw new TypeError('publisherPriority must be a number');
  if (!Number.isInteger(value) || value < 0 || value > MAX_DEFAULT_PUBLISHER_PRIORITY) {
    throw new RangeError(`publisherPriority must be an integer from 0 to ${MAX_DEFAULT_PUBLISHER_PRIORITY}`);
  }
  return value;
}
