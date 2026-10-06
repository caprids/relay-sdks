import { readFileSync } from 'node:fs';
import { createBroadcastClock, createPlayoutClock, type PlayoutDecision } from '@mux/moq-media';
import { describe, expect, it, vi } from 'vitest';

type Step = {
  at: string;
  now?: string;
  fromSource?: string;
  expect?: string | { action: string; offsetUs: string; waitUs?: string };
  report?: string;
  reportAt?: string;
  stopAudio?: true;
  reset?: true;
  mode?: string;
  position?: string | null;
  due?: string;
};
type Vectors = {
  broadcast: { name: string; createdAt: string; originUs?: string; steps: Step[] }[];
  sampleTimelines: {
    name: string;
    createdAt: string;
    timelineAt: string;
    sampleRate: string;
    firstSampleUs?: string;
    samples: [string, string][];
  }[];
  playout: { name: string; options?: Record<string, string>; steps: Step[] }[];
};
const vectors = JSON.parse(readFileSync(new URL('../../spec/vectors/clock.json', import.meta.url), 'utf8')) as Vectors;

function source(startUs: number) {
  let current = startUs;
  return { now: () => current, set: (us: number) => (current = us) };
}

function decision(raw: { action: string; offsetUs: string; waitUs?: string }): PlayoutDecision {
  return raw.action === 'wait'
    ? { action: 'wait', offsetUs: Number(raw.offsetUs), waitUs: Number(raw.waitUs) }
    : ({ action: raw.action, offsetUs: Number(raw.offsetUs) } as PlayoutDecision);
}

describe('spec/vectors/clock.json', () => {
  it.each(vectors.broadcast)('broadcast clock: $name', ({ createdAt, originUs, steps }) => {
    const time = source(Number(createdAt));
    const clock = createBroadcastClock({
      now: time.now,
      ...(originUs !== undefined ? { originUs: Number(originUs) } : {}),
    });
    for (const step of steps) {
      time.set(Number(step.at));
      if (step.now !== undefined) expect(clock.now()).toBe(Number(step.now));
      if (step.fromSource !== undefined) expect(clock.fromSourceUs(Number(step.fromSource))).toBe(Number(step.expect));
    }
  });

  it.each(vectors.sampleTimelines)('sample timeline: $name', (vector) => {
    const time = source(Number(vector.createdAt));
    const clock = createBroadcastClock({ now: time.now });
    time.set(Number(vector.timelineAt));
    const timeline = clock.sampleTimeline(
      Number(vector.sampleRate),
      vector.firstSampleUs !== undefined ? { firstSampleUs: Number(vector.firstSampleUs) } : {},
    );
    for (const [index, timestamp] of vector.samples)
      expect(timeline.timestampUs(Number(index))).toBe(Number(timestamp));
  });

  it.each(vectors.playout)('playout clock: $name', ({ options = {}, steps }) => {
    const time = source(0);
    const clock = createPlayoutClock({
      now: time.now,
      ...Object.fromEntries(Object.entries(options).map(([key, value]) => [key, Number(value)])),
    });
    for (const step of steps) {
      time.set(Number(step.at));
      if (step.report !== undefined) {
        clock.reportAudio(Number(step.report), step.reportAt !== undefined ? Number(step.reportAt) : undefined);
      }
      if (step.stopAudio) clock.stopAudio();
      if (step.reset) clock.reset();
      if (step.due !== undefined && typeof step.expect === 'object') {
        expect(clock.due(Number(step.due))).toEqual(decision(step.expect));
      }
      if (step.mode !== undefined) expect(clock.mode).toBe(step.mode);
      if (step.position !== undefined) {
        expect(clock.position()).toBe(step.position === null ? undefined : Number(step.position));
      }
    }
  });
});

describe('createBroadcastClock', () => {
  it('reads performance.now() in microseconds by default', () => {
    const now = vi.spyOn(performance, 'now').mockReturnValue(2000.25);
    try {
      const clock = createBroadcastClock();
      expect(clock.originUs).toBe(2_000_250);
      now.mockReturnValue(2033.5);
      expect(clock.now()).toBe(33_250);
      // A VideoFrame.timestamp from MediaStreamTrackProcessor is on the same clock.
      expect(clock.fromSourceUs(2_100_000)).toBe(99_750);
    } finally {
      now.mockRestore();
    }
  });

  it('rounds a fractional source time to the nearest microsecond, ties upward', () => {
    const clock = createBroadcastClock({ now: () => 10.5, originUs: 0 });
    expect(clock.now()).toBe(11);
    expect(clock.fromSourceUs(0.4)).toBe(0);
  });

  it('rejects a non-finite origin or source time and an invalid sample rate or index', () => {
    expect(() => createBroadcastClock({ now: () => 0, originUs: Number.NaN })).toThrow(RangeError);
    const clock = createBroadcastClock({ now: () => 0 });
    expect(() => clock.fromSourceUs(Number.POSITIVE_INFINITY)).toThrow(RangeError);
    expect(() => clock.sampleTimeline(0)).toThrow(RangeError);
    expect(() => clock.sampleTimeline(44_100.5)).toThrow(RangeError);
    expect(() => clock.sampleTimeline(48_000, { firstSampleUs: 0.5 })).toThrow(RangeError);
    const timeline = clock.sampleTimeline(48_000);
    expect(timeline).toMatchObject({ sampleRate: 48_000, firstSampleUs: 0 });
    expect(() => timeline.timestampUs(-1)).toThrow(RangeError);
    expect(() => timeline.timestampUs(1.5)).toThrow(RangeError);
  });
});

describe('createPlayoutClock', () => {
  it('waits whole microseconds and reports a rounded offset for a fractional source', () => {
    let now = 0.25;
    const clock = createPlayoutClock({ now: () => now });
    clock.reportAudio(0);
    now = 0.5;
    expect(clock.due(1)).toEqual({ action: 'wait', offsetUs: 1, waitUs: 1 });
    expect(clock.position()).toBe(0.25);
  });

  it('paces by wall clock when no audio track is ever reported', () => {
    let now = 0;
    const clock = createPlayoutClock({ now: () => now });
    const shown: number[] = [];
    // 30 fps video, a frame arriving every 33 ms, with 10 ms of decode jitter.
    for (let frame = 0; frame < 30; frame++) {
      const timestamp = frame * 33_333;
      now = timestamp + (frame % 2 ? 10_000 : 0);
      const decision = clock.due(timestamp);
      expect(decision.action).not.toBe('drop');
      if (decision.action === 'wait') now += decision.waitUs;
      if (clock.due(timestamp).action === 'show') shown.push(timestamp);
    }
    expect(shown).toHaveLength(30);
    expect(clock.mode).toBe('wall');
  });

  it('reads performance.now() by default and rejects invalid options and timestamps', () => {
    const now = vi.spyOn(performance, 'now').mockReturnValue(1);
    try {
      const clock = createPlayoutClock();
      clock.reportAudio(5_000);
      now.mockReturnValue(2);
      expect(clock.position()).toBe(6_000);
      expect(() => clock.due(Number.NaN)).toThrow(RangeError);
      expect(() => clock.reportAudio(0, Number.POSITIVE_INFINITY)).toThrow(RangeError);
    } finally {
      now.mockRestore();
    }
    expect(() => createPlayoutClock({ lateDropUs: -1 })).toThrow(RangeError);
    expect(() => createPlayoutClock({ maxVideoDelayUs: 0 })).toThrow(RangeError);
    expect(() => createPlayoutClock({ audioTimeoutUs: 0 })).toThrow(RangeError);
    expect(() => createPlayoutClock({ lateDropUs: Number.NaN })).toThrow(RangeError);
  });
});
