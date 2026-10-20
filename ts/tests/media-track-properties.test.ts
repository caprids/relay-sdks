import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import { mediaTrackProperties } from '../src/media.js';
import { trackPropertyPairs } from '../src/validation.js';

const SUBGROUP_DELIVERY_TIMEOUT = 0x06;
const DEFAULT_PUBLISHER_PRIORITY = 0x0e;
const PRIORITY = (
  JSON.parse(readFileSync(new URL('../../spec/protocol.json', import.meta.url), 'utf8')) as {
    mediaTrackProperties: { publisherPriority: { audio: number; video: number } };
  }
).mediaTrackProperties.publisherPriority;
const timeoutMs = (properties: ReturnType<typeof mediaTrackProperties>) => {
  expect(properties).toHaveLength(2);
  expect(properties[0]?.type).toBe(SUBGROUP_DELIVERY_TIMEOUT);
  return properties[0]?.value;
};
const priority = (properties: ReturnType<typeof mediaTrackProperties>) => {
  expect(properties).toHaveLength(2);
  expect(properties[1]?.type).toBe(DEFAULT_PUBLISHER_PRIORITY);
  return properties[1]?.value;
};

describe('mediaTrackProperties', () => {
  it('gives audio a 500 ms subgroup timeout, no object timeout and publisher priority 16', () => {
    expect(mediaTrackProperties('audio')).toEqual([
      { type: SUBGROUP_DELIVERY_TIMEOUT, value: 500 },
      { type: DEFAULT_PUBLISHER_PRIORITY, value: 16 },
    ]);
  });

  it('puts audio ahead of video with the priorities spec/protocol.json pins', () => {
    expect(PRIORITY).toEqual({ audio: 16, video: 96 });
    expect(priority(mediaTrackProperties('audio'))).toBe(PRIORITY.audio);
    expect(priority(mediaTrackProperties('video', { keyframeIntervalUs: 1_000_000 }))).toBe(PRIORITY.video);
    expect(PRIORITY.audio).toBeLessThan(PRIORITY.video);
  });

  it('takes an explicit publisher priority for either kind, from 0 to 255', () => {
    expect(priority(mediaTrackProperties('audio', { publisherPriority: 0 }))).toBe(0);
    expect(priority(mediaTrackProperties('video', { keyframeIntervalUs: 1_000_000, publisherPriority: 255 }))).toBe(
      255,
    );
  });

  it('refuses a publisher priority that is not an integer from 0 to 255', () => {
    expect(() => mediaTrackProperties('audio', { publisherPriority: '16' as never })).toThrow(TypeError);
    expect(() => mediaTrackProperties('audio', { publisherPriority: true as never })).toThrow(TypeError);
    for (const value of [-1, 256, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(() => mediaTrackProperties('audio', { publisherPriority: value })).toThrow(RangeError);
    }
  });

  it('ignores the keyframe interval and target latency for audio', () => {
    expect(
      timeoutMs(mediaTrackProperties('audio', { keyframeIntervalUs: 2_000_000, targetLatencyUs: 8_000_000 })),
    ).toBe(500);
  });

  it('gives video one keyframe interval by default: twice the interval, less the interval', () => {
    expect(timeoutMs(mediaTrackProperties('video', { keyframeIntervalUs: 1_000_000 }))).toBe(1000);
  });

  it('gives video the target latency less the keyframe interval', () => {
    expect(timeoutMs(mediaTrackProperties('video', { keyframeIntervalUs: 500_000, targetLatencyUs: 3_000_000 }))).toBe(
      2500,
    );
  });

  it('never gives video less than one keyframe interval', () => {
    expect(
      timeoutMs(mediaTrackProperties('video', { keyframeIntervalUs: 1_000_000, targetLatencyUs: 1_200_000 })),
    ).toBe(1000);
  });

  it('takes an explicit subgroup timeout for either kind', () => {
    expect(timeoutMs(mediaTrackProperties('audio', { subgroupTimeoutUs: 250_000 }))).toBe(250);
    expect(
      timeoutMs(mediaTrackProperties('video', { keyframeIntervalUs: 1_000_000, subgroupTimeoutUs: 4_000_000 })),
    ).toBe(4000);
  });

  it('rounds up to whole milliseconds, so a timeout is never shorter than asked', () => {
    expect(timeoutMs(mediaTrackProperties('audio', { subgroupTimeoutUs: 1 }))).toBe(1);
    expect(timeoutMs(mediaTrackProperties('video', { keyframeIntervalUs: 33_334 }))).toBe(34);
  });

  it('returns properties registerTrack accepts', () => {
    expect(trackPropertyPairs(mediaTrackProperties('video', { keyframeIntervalUs: 2_000_000 }))).toEqual([
      { type: SUBGROUP_DELIVERY_TIMEOUT, value: 2000 },
      { type: DEFAULT_PUBLISHER_PRIORITY, value: 96 },
    ]);
    expect(() => trackPropertyPairs(mediaTrackProperties('audio', { publisherPriority: 255 }))).not.toThrow();
  });

  it('refuses a kind that is not a string with a TypeError and another string with a RangeError', () => {
    expect(() => mediaTrackProperties(1 as never)).toThrow(TypeError);
    expect(() => mediaTrackProperties('data' as never)).toThrow(RangeError);
  });

  it('refuses video without a keyframe interval', () => {
    expect(() => mediaTrackProperties('video')).toThrow(TypeError);
    expect(() => mediaTrackProperties('video', { subgroupTimeoutUs: 1_000_000 })).toThrow(TypeError);
  });

  it('refuses a target latency below the keyframe interval', () => {
    expect(() => mediaTrackProperties('video', { keyframeIntervalUs: 1_000_000, targetLatencyUs: 999_999 })).toThrow(
      RangeError,
    );
  });

  it.each(['keyframeIntervalUs', 'targetLatencyUs', 'subgroupTimeoutUs'] as const)('checks %s', (name) => {
    const base = { keyframeIntervalUs: 1, targetLatencyUs: 1 };
    expect(() => mediaTrackProperties('video', { ...base, [name]: '1000' as never })).toThrow(TypeError);
    expect(() => mediaTrackProperties('video', { ...base, [name]: true as never })).toThrow(TypeError);
    for (const value of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, 2_147_483_647_001]) {
      expect(() => mediaTrackProperties('video', { ...base, [name]: value })).toThrow(RangeError);
    }
    expect(() => mediaTrackProperties('audio', { [name]: 2_147_483_647_000 })).not.toThrow();
  });
});
