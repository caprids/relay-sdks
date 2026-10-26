import { readFileSync } from 'node:fs';
import type { MoqtObject } from '@caprid/moq-core';
import { createFrameSequencer, createPlayoutClock, LOC_TIMESCALE, LOC_TIMESTAMP } from '@caprid/moq-media';
import { describe, expect, it } from 'vitest';

type Step = {
  at: string;
  anchor?: string;
  report?: string;
  object?: [string, string, string | null];
  groupEnd?: string;
  groupReset?: string;
  restart?: true;
  clockReset?: true;
  poll?: { frames: string[][]; waitUs: string | null };
  stats?: Record<string, string>;
  targetLatency?: string;
};
type Vectors = { sequences: { name: string; grouping?: 'keyframe' | 'frame'; steps: Step[] }[] };

const vectors = JSON.parse(
  readFileSync(new URL('../../spec/vectors/frame-sequencer.json', import.meta.url), 'utf8'),
) as Vectors;

const object = (groupId: number, objectId: number, properties: MoqtObject['properties']): MoqtObject => ({
  groupId,
  objectId,
  subgroupId: 0,
  status: 'normal',
  properties,
  payload: new Uint8Array([groupId, objectId]),
});

describe('spec/vectors/frame-sequencer.json', () => {
  it.each(vectors.sequences)('frame sequencer: $name', ({ grouping, steps }) => {
    let now = 0;
    const clock = createPlayoutClock({ now: () => now });
    const sequencer = createFrameSequencer({ clock, ...(grouping ? { grouping } : {}) });
    for (const step of steps) {
      now = Number(step.at);
      if (step.report !== undefined) clock.reportAudio(Number(step.report));
      if (step.anchor !== undefined) clock.due(Number(step.anchor));
      if (step.object) {
        const [group, id, timestamp] = step.object;
        sequencer.object(
          object(
            Number(group),
            Number(id),
            timestamp === null ? [] : [{ type: LOC_TIMESTAMP, value: Number(timestamp) }],
          ),
        );
      }
      if (step.groupEnd !== undefined) sequencer.groupEnded(Number(step.groupEnd));
      if (step.groupReset !== undefined) sequencer.groupReset(Number(step.groupReset));
      if (step.restart) sequencer.restart();
      if (step.clockReset) clock.reset();
      if (step.poll) {
        const { frames, waitUs } = sequencer.poll();
        expect(
          frames.map((frame) => [
            String(frame.groupId),
            String(frame.objectId),
            ...(frame.discontinuity ? [frame.discontinuity] : []),
          ]),
        ).toEqual(step.poll.frames);
        expect(waitUs).toBe(step.poll.waitUs === null ? undefined : Number(step.poll.waitUs));
      }
      if (step.stats) {
        expect(sequencer.stats).toEqual(
          Object.fromEntries(Object.entries(step.stats).map(([key, value]) => [key, Number(value)])),
        );
      }
      if (step.targetLatency !== undefined) expect(clock.targetLatencyUs).toBe(Number(step.targetLatency));
    }
  });
});

describe('createFrameSequencer', () => {
  it('reads timestamps through the track properties and passes the frame through', () => {
    const sequencer = createFrameSequencer({ clock: createPlayoutClock({ now: () => 0 }) });
    sequencer.trackProperties = [{ type: LOC_TIMESCALE, value: 1000 }];
    sequencer.object(object(4, 0, [{ type: LOC_TIMESTAMP, value: 1500 }]));
    expect(sequencer.poll().frames).toEqual([
      {
        groupId: 4,
        objectId: 0,
        timestampUs: 1_500_000,
        key: true,
        payload: new Uint8Array([4, 0]),
        properties: [{ type: LOC_TIMESTAMP, value: 1500 }],
      },
    ]);
  });

  it('treats an end-of-group status object as the end of its group', () => {
    const clock = createPlayoutClock({ now: () => 0 });
    clock.due(0);
    const sequencer = createFrameSequencer({ clock });
    sequencer.object(object(0, 0, [{ type: LOC_TIMESTAMP, value: 0 }]));
    sequencer.object(object(1, 0, [{ type: LOC_TIMESTAMP, value: 1_000_000 }]));
    expect(sequencer.poll().frames.map((frame) => frame.groupId)).toEqual([0]);
    sequencer.object({ ...object(0, 1, []), status: 'end-of-group', payload: new Uint8Array() });
    expect(sequencer.poll().frames.map((frame) => [frame.groupId, frame.discontinuity])).toEqual([[1, undefined]]);
  });

  it('ignores a repeated object', () => {
    const sequencer = createFrameSequencer({ clock: createPlayoutClock({ now: () => 0 }), grouping: 'frame' });
    sequencer.object(object(0, 0, [{ type: LOC_TIMESTAMP, value: 0 }]));
    sequencer.object(object(0, 0, [{ type: LOC_TIMESTAMP, value: 0 }]));
    expect(sequencer.poll().frames).toHaveLength(1);
  });

  it('waits on a gap while the clock is idle until the group ends, checking back for the clock to anchor', () => {
    const sequencer = createFrameSequencer({ clock: createPlayoutClock({ now: () => 0 }) });
    sequencer.object(object(0, 0, [{ type: LOC_TIMESTAMP, value: 0 }]));
    sequencer.object(object(1, 0, [{ type: LOC_TIMESTAMP, value: 1_000_000 }]));
    expect(sequencer.poll()).toEqual({ frames: [expect.objectContaining({ groupId: 0 })], waitUs: 50_000 });
    sequencer.groupEnded(0);
    expect(sequencer.poll().frames.map((frame) => frame.groupId)).toEqual([1]);
  });

  it('takes at most maxFrames, judging each when it is taken', () => {
    let now = 0;
    const clock = createPlayoutClock({ now: () => now });
    clock.reportAudio(0);
    const sequencer = createFrameSequencer({ clock, grouping: 'frame' });
    sequencer.object(object(0, 0, [{ type: LOC_TIMESTAMP, value: 100_000 }]));
    sequencer.object(object(1, 0, [{ type: LOC_TIMESTAMP, value: 120_000 }]));
    sequencer.object(object(2, 0, [{ type: LOC_TIMESTAMP, value: 140_000 }]));
    expect(sequencer.poll(1).frames.map((frame) => frame.groupId)).toEqual([0]);
    now = 5_000_000;
    clock.reportAudio(5_000_000);
    sequencer.object(object(3, 0, [{ type: LOC_TIMESTAMP, value: 5_100_000 }]));
    expect(sequencer.poll(1).frames.map((frame) => [frame.groupId, frame.discontinuity])).toEqual([[3, 'gap']]);
    expect(sequencer.stats.late).toBe(2);
    expect(() => sequencer.poll(0)).toThrow(RangeError);
  });

  it('bounds what it holds when nobody polls, resuming at a keyframe after a gap', () => {
    const clock = createPlayoutClock({ now: () => 0 });
    const sequencer = createFrameSequencer({ clock });
    sequencer.object(object(0, 0, [{ type: LOC_TIMESTAMP, value: 0 }]));
    expect(sequencer.poll().frames).toHaveLength(1);
    for (let index = 1; index < 3000; index++) {
      sequencer.object(object(Math.floor(index / 30), index % 30, [{ type: LOC_TIMESTAMP, value: index * 33_333 }]));
    }
    const { frames } = sequencer.poll();
    expect(frames.length).toBeLessThanOrEqual(1100);
    expect(frames[0]).toMatchObject({ key: true, discontinuity: 'gap' });
    expect(sequencer.stats.late).toBeGreaterThan(1800);
  });

  it('bounds a resync candidate too', () => {
    let now = 0;
    const clock = createPlayoutClock({ now: () => now });
    clock.due(0);
    now = 5_000_000;
    const sequencer = createFrameSequencer({ clock });
    sequencer.object(object(1, 0, [{ type: LOC_TIMESTAMP, value: 1_000_000 }]));
    expect(sequencer.poll().frames).toEqual([]);
    for (let index = 1; index < 3000; index++) {
      sequencer.object(object(1, index, [{ type: LOC_TIMESTAMP, value: 1_000_000 + index }]));
    }
    now = 7_000_000;
    expect(sequencer.poll().frames.length).toBeLessThanOrEqual(1100);
    expect(sequencer.stats.late).toBeLessThanOrEqual(3000);
  });

  it('takes keyframe intervals only from groups that can still be delivered', () => {
    const clock = createPlayoutClock({ now: () => 0 });
    const sequencer = createFrameSequencer({ clock });
    for (let group = 10; group < 18; group++) {
      sequencer.object(object(group, 0, [{ type: LOC_TIMESTAMP, value: group * 1_000_000 }]));
      sequencer.groupEnded(group);
    }
    sequencer.poll();
    for (let repeat = 0; repeat < 4; repeat++) {
      sequencer.object(object(9, 0, [{ type: LOC_TIMESTAMP, value: 9_900_000 }]));
    }
    expect(clock.targetLatencyUs).toBe(2_000_000);
  });

  it('says when more is ready past maxFrames', () => {
    const clock = createPlayoutClock({ now: () => 0 });
    clock.reportAudio(0);
    const audio = createFrameSequencer({ clock, grouping: 'frame' });
    audio.object(object(0, 0, [{ type: LOC_TIMESTAMP, value: 10_000 }]));
    audio.object(object(1, 0, [{ type: LOC_TIMESTAMP, value: 30_000 }]));
    expect(audio.poll(1).waitUs).toBe(0);
    expect(audio.poll(1)).toMatchObject({ frames: [expect.objectContaining({ groupId: 1 })], waitUs: undefined });
    const video = createFrameSequencer({ clock });
    video.object(object(0, 0, [{ type: LOC_TIMESTAMP, value: 10_000 }]));
    video.object(object(0, 1, [{ type: LOC_TIMESTAMP, value: 43_333 }]));
    expect(video.poll(1).waitUs).toBe(0);
  });

  it('drops a group older than the one being delivered', () => {
    const clock = createPlayoutClock({ now: () => 0 });
    const sequencer = createFrameSequencer({ clock });
    sequencer.object(object(10, 0, [{ type: LOC_TIMESTAMP, value: 10_000_000 }]));
    sequencer.poll();
    sequencer.object(object(9, 0, [{ type: LOC_TIMESTAMP, value: 9_900_000 }]));
    expect(sequencer.stats.late).toBe(1);
    expect(clock.targetLatencyUs).toBe(2_000_000);
  });

  it('ignores a repeated keyframe still held, even once its timestamp is no longer remembered', () => {
    const clock = createPlayoutClock({ now: () => 0 });
    const sequencer = createFrameSequencer({ clock });
    const times = [0, 100_000, 1_100_000, 2_100_000, 3_100_000, 4_100_000, 5_100_000, 6_100_000, 7_100_000];
    times.forEach((time, group) => {
      sequencer.object(object(group, 0, [{ type: LOC_TIMESTAMP, value: time }]));
    });
    expect(clock.targetLatencyUs).toBe(2_000_000);
    for (let repeat = 0; repeat < 4; repeat++) sequencer.object(object(0, 0, [{ type: LOC_TIMESTAMP, value: 0 }]));
    expect(clock.targetLatencyUs).toBe(2_000_000);
  });
});

// The clock is anchored at local time 0 on timestamp 0, so its position is the local time `now` throughout.
const harness = (options: Partial<Parameters<typeof createFrameSequencer>[0]> = {}) => {
  const state = { now: 0 };
  const clock = createPlayoutClock({ now: () => state.now });
  clock.due(0);
  const sequencer = createFrameSequencer({ clock, ...options });
  const put = (groupId: number, timestampUs: number, objectId = 0) =>
    sequencer.object(object(groupId, objectId, [{ type: LOC_TIMESTAMP, value: timestampUs }]));
  const groupIds = (frames: { groupId: number }[]) => frames.map((frame) => frame.groupId);
  return { state, clock, sequencer, put, groupIds };
};

describe('createFrameSequencer: lateDropUs', () => {
  it('judges lateness by the clock when the option is not set', () => {
    const { state, clock, sequencer, put, groupIds } = harness({ grouping: 'frame' });
    expect(clock.lateDropUs).toBe(40_000);
    state.now = 1_000_000;
    put(0, 900_000);
    put(1, 980_000);
    expect(groupIds(sequencer.poll().frames)).toEqual([1]);
    expect(sequencer.stats.late).toBe(1);
  });

  it("overrides the clock's limit for this sequencer, to drop sooner or keep longer", () => {
    const strict = harness({ grouping: 'frame', lateDropUs: 10_000 });
    strict.state.now = 1_000_000;
    strict.put(0, 980_000);
    expect(strict.sequencer.poll().frames).toEqual([]);
    expect(strict.sequencer.stats.late).toBe(1);

    const lenient = harness({ grouping: 'frame', lateDropUs: 200_000 });
    lenient.state.now = 1_000_000;
    lenient.put(0, 850_000);
    lenient.put(1, 700_000);
    expect(lenient.groupIds(lenient.sequencer.poll().frames)).toEqual([0]);
    expect(lenient.sequencer.stats.late).toBe(1);
    // The clock itself is untouched.
    expect(lenient.clock.lateDropUs).toBe(40_000);
  });

  it('accepts zero: only a frame at or ahead of the position is delivered', () => {
    const { state, sequencer, put, groupIds } = harness({ grouping: 'frame', lateDropUs: 0 });
    state.now = 1_000_000;
    put(0, 999_999);
    put(1, 1_000_000);
    expect(groupIds(sequencer.poll().frames)).toEqual([1]);
  });

  it('never drops a frame as late with Infinity', () => {
    const { state, sequencer, put, groupIds } = harness({ grouping: 'frame', lateDropUs: Number.POSITIVE_INFINITY });
    state.now = 60_000_000;
    put(0, 0);
    put(1, 1_000);
    const { frames } = sequencer.poll();
    expect(groupIds(frames)).toEqual([0, 1]);
    expect(sequencer.stats.late).toBe(0);
  });

  it('refuses a negative or NaN limit', () => {
    const clock = createPlayoutClock({ now: () => 0 });
    expect(() => createFrameSequencer({ clock, grouping: 'frame', lateDropUs: -1 })).toThrow(RangeError);
    expect(() => createFrameSequencer({ clock, grouping: 'frame', lateDropUs: Number.NaN })).toThrow();
    expect(() => createFrameSequencer({ clock, grouping: 'frame', lateDropUs: Number.NEGATIVE_INFINITY })).toThrow(
      RangeError,
    );
  });

  it('refuses it with keyframe grouping, the default or named', () => {
    const clock = createPlayoutClock({ now: () => 0 });
    expect(() => createFrameSequencer({ clock, lateDropUs: 10_000 })).toThrow(RangeError);
    expect(() => createFrameSequencer({ clock, grouping: 'keyframe', lateDropUs: Number.POSITIVE_INFINITY })).toThrow(
      RangeError,
    );
  });
});

describe('createFrameSequencer: reorderWindowUs', () => {
  it('refuses a negative, NaN or infinite window', () => {
    const clock = createPlayoutClock({ now: () => 0 });
    expect(() => createFrameSequencer({ clock, grouping: 'frame', reorderWindowUs: -1 })).toThrow(RangeError);
    expect(() => createFrameSequencer({ clock, grouping: 'frame', reorderWindowUs: Number.NaN })).toThrow();
    expect(() => createFrameSequencer({ clock, grouping: 'frame', reorderWindowUs: Number.POSITIVE_INFINITY })).toThrow(
      RangeError,
    );
  });

  it('refuses a window above zero with keyframe grouping, and allows zero', () => {
    const clock = createPlayoutClock({ now: () => 0 });
    expect(() => createFrameSequencer({ clock, reorderWindowUs: 1 })).toThrow(RangeError);
    expect(() => createFrameSequencer({ clock, grouping: 'keyframe', reorderWindowUs: 100_000 })).toThrow(RangeError);
    expect(() => createFrameSequencer({ clock, reorderWindowUs: 0 })).not.toThrow();
    expect(() => createFrameSequencer({ clock, grouping: 'frame', reorderWindowUs: 0 })).not.toThrow();
  });

  it('keeps today’s behaviour by default: a later group goes out at once and an earlier one that follows is behind', () => {
    const { state, sequencer, put, groupIds } = harness({ grouping: 'frame' });
    state.now = 1_000_000;
    put(0, 1_000_000);
    expect(groupIds(sequencer.poll().frames)).toEqual([0]);
    put(2, 1_100_000);
    const { frames, waitUs } = sequencer.poll();
    expect(frames).toEqual([
      expect.objectContaining({ groupId: 2, discontinuity: 'gap', missingGroups: { firstGroupId: 1, lastGroupId: 1 } }),
    ]);
    expect(waitUs).toBeUndefined();
    put(1, 1_050_000);
    expect(sequencer.poll().frames).toEqual([]);
    expect(sequencer.stats.late).toBe(1);
    expect(sequencer.stats.delivered).toBe(2);
  });

  it('holds a frame for the missing group and delivers both in order, with no gap, when it arrives in time', () => {
    const { state, sequencer, put, groupIds } = harness({ grouping: 'frame', reorderWindowUs: 100_000 });
    state.now = 1_000_000;
    put(0, 1_000_000);
    expect(groupIds(sequencer.poll().frames)).toEqual([0]);
    state.now = 1_010_000;
    put(2, 1_500_000);
    put(3, 1_520_000);
    expect(sequencer.poll()).toEqual({ frames: [], waitUs: 100_000 });
    state.now = 1_060_000;
    put(1, 1_400_000);
    const { frames } = sequencer.poll();
    expect(groupIds(frames)).toEqual([1, 2, 3]);
    for (const frame of frames) {
      expect(frame.discontinuity).toBeUndefined();
      expect(frame.missingGroups).toBeUndefined();
    }
    expect(sequencer.stats).toMatchObject({ delivered: 4, late: 0 });
  });

  it('reports the wait that remains while it holds, counted from when the held group arrived', () => {
    const { state, sequencer, put } = harness({ grouping: 'frame', reorderWindowUs: 100_000 });
    state.now = 1_000_000;
    put(0, 1_000_000);
    sequencer.poll();
    state.now = 1_010_000;
    put(2, 1_500_000);
    expect(sequencer.poll().waitUs).toBe(100_000);
    state.now = 1_040_000;
    expect(sequencer.poll()).toEqual({ frames: [], waitUs: 70_000 });
    // A later frame of the same group, or a newer group, does not move the end of the wait.
    put(2, 1_510_000, 1);
    put(3, 1_520_000);
    state.now = 1_100_000;
    expect(sequencer.poll()).toEqual({ frames: [], waitUs: 10_000 });
  });

  it('delivers the held frames as a gap when the window ends, naming the groups that never came', () => {
    const { state, sequencer, put } = harness({ grouping: 'frame', reorderWindowUs: 100_000 });
    state.now = 1_000_000;
    put(0, 1_000_000);
    sequencer.poll();
    state.now = 1_010_000;
    put(3, 1_500_000);
    put(5, 1_520_000);
    expect(sequencer.poll().waitUs).toBe(100_000);
    state.now = 1_109_999;
    expect(sequencer.poll()).toEqual({ frames: [], waitUs: 1 });
    state.now = 1_110_000;
    const { frames, waitUs } = sequencer.poll();
    expect(frames.map((frame) => [frame.groupId, frame.discontinuity, frame.missingGroups])).toEqual([
      [3, 'gap', { firstGroupId: 1, lastGroupId: 2 }],
      [5, 'gap', { firstGroupId: 4, lastGroupId: 4 }],
    ]);
    expect(waitUs).toBeUndefined();
    // What arrives for the missing groups now is behind.
    put(1, 1_400_000);
    put(4, 1_400_000);
    expect(sequencer.poll().frames).toEqual([]);
    expect(sequencer.stats.late).toBe(2);
  });

  it('does not hold the frame that follows the last delivered group', () => {
    const { state, sequencer, put, groupIds } = harness({ grouping: 'frame', reorderWindowUs: 100_000 });
    state.now = 1_000_000;
    put(7, 1_000_000);
    put(8, 1_020_000);
    // The first group has no earlier one to wait for.
    expect(groupIds(sequencer.poll().frames)).toEqual([7, 8]);
    put(9, 1_040_000);
    expect(sequencer.poll()).toMatchObject({ waitUs: undefined });
  });

  it('ends the wait at once when the group missing ended or was reset with nothing in it', () => {
    for (const end of ['groupEnded', 'groupReset'] as const) {
      const { state, sequencer, put } = harness({ grouping: 'frame', reorderWindowUs: 100_000 });
      state.now = 1_000_000;
      put(0, 1_000_000);
      sequencer.poll();
      state.now = 1_010_000;
      put(2, 1_500_000);
      expect(sequencer.poll()).toEqual({ frames: [], waitUs: 100_000 });
      sequencer[end](1);
      const { frames } = sequencer.poll();
      // The group is lost, which the frame says; but it did not wait for the window.
      expect(frames).toEqual([
        expect.objectContaining({
          groupId: 2,
          discontinuity: 'gap',
          missingGroups: { firstGroupId: 1, lastGroupId: 1 },
        }),
      ]);
    }
  });

  it('remembers a missing group that ended before the group in front of it arrived', () => {
    const { state, sequencer, put, groupIds } = harness({
      grouping: 'frame',
      reorderWindowUs: 2_000_000,
      lateDropUs: Number.POSITIVE_INFINITY,
    });
    state.now = 1_000_000;
    put(0, 1_000_000);
    expect(groupIds(sequencer.poll().frames)).toEqual([0]);
    state.now = 1_060_000;
    put(3, 1_060_000);
    expect(sequencer.poll().frames).toEqual([]);
    // Group 2 is over while group 1 is still missing: when 1 comes, 3 does not wait out the window for 2.
    sequencer.groupReset(2);
    expect(sequencer.poll().frames).toEqual([]);
    state.now = 1_070_000;
    put(1, 1_020_000);
    const { frames } = sequencer.poll();
    expect(groupIds(frames)).toEqual([1, 3]);
    expect(frames[1]).toMatchObject({ discontinuity: 'gap', missingGroups: { firstGroupId: 2, lastGroupId: 2 } });
  });

  it('remembers a group that ended before anything was delivered, and again after a restart', () => {
    for (const end of ['groupEnded', 'groupReset'] as const) {
      const { state, sequencer, put, groupIds } = harness({
        grouping: 'frame',
        reorderWindowUs: 2_000_000,
        lateDropUs: Number.POSITIVE_INFINITY,
      });
      for (const round of [0, 1]) {
        if (round === 1) sequencer.restart();
        const base = 10_000_000 * (round + 1);
        state.now = base;
        // Concurrent group streams: group 1's empty end lands before group 0's frame, and before any floor exists.
        sequencer[end](1);
        expect(sequencer.poll().frames).toEqual([]);
        put(0, base);
        expect(groupIds(sequencer.poll().frames)).toEqual([0]);
        state.now = base + 40_000;
        put(2, base + 40_000);
        const { frames, waitUs } = sequencer.poll();
        expect(waitUs).toBeUndefined();
        expect(groupIds(frames)).toEqual([2]);
        expect(frames[0]).toMatchObject({ missingGroups: { firstGroupId: 1, lastGroupId: 1 } });
      }
    }
  });

  it('waits by group with groups of several frames: the next group is due, a later object of the last one still plays', () => {
    const { state, sequencer, put } = harness({
      grouping: 'frame',
      reorderWindowUs: 2_000_000,
      lateDropUs: Number.POSITIVE_INFINITY,
    });
    const taken = () => sequencer.poll().frames.map((frame) => [frame.groupId, frame.objectId]);
    state.now = 1_000_000;
    put(0, 1_000_000, 0);
    put(0, 1_020_000, 1);
    expect(taken()).toEqual([
      [0, 0],
      [0, 1],
    ]);
    // Group 2 waits for group 1; group 0's next object is not held behind that wait.
    state.now = 1_060_000;
    put(2, 1_060_000, 0);
    expect(taken()).toEqual([]);
    put(0, 1_040_000, 2);
    expect(taken()).toEqual([[0, 2]]);
    put(1, 1_050_000, 0);
    expect(taken()).toEqual([
      [1, 0],
      [2, 0],
    ]);
    // After a group of several frames, the next group goes out at once.
    put(3, 1_080_000, 0);
    expect(taken()).toEqual([[3, 0]]);
  });

  it('ends the wait for an empty group reset before the later one arrives', () => {
    const { state, sequencer, put, groupIds } = harness({ grouping: 'frame', reorderWindowUs: 100_000 });
    state.now = 1_000_000;
    put(0, 1_000_000);
    sequencer.poll();
    sequencer.groupReset(1);
    put(2, 1_500_000);
    expect(groupIds(sequencer.poll().frames)).toEqual([2]);
  });

  it('keeps waiting when a group that ended is not the one next in line', () => {
    const { state, sequencer, put } = harness({ grouping: 'frame', reorderWindowUs: 100_000 });
    state.now = 1_000_000;
    put(0, 1_000_000);
    sequencer.poll();
    put(3, 1_500_000);
    sequencer.groupEnded(2);
    expect(sequencer.poll().frames).toEqual([]);
  });

  it('drops a held frame that is late by the time its wait ends', () => {
    const { state, sequencer, put } = harness({ grouping: 'frame', reorderWindowUs: 500_000 });
    state.now = 1_000_000;
    put(0, 1_000_000);
    sequencer.poll();
    state.now = 1_010_000;
    put(2, 1_050_000);
    expect(sequencer.poll()).toEqual({ frames: [], waitUs: 500_000 });
    state.now = 1_510_000;
    expect(sequencer.poll().frames).toEqual([]);
    expect(sequencer.stats.late).toBe(1);
    put(3, 1_520_000);
    const { frames } = sequencer.poll();
    expect(frames).toEqual([
      expect.objectContaining({ groupId: 3, discontinuity: 'gap', missingGroups: { firstGroupId: 1, lastGroupId: 2 } }),
    ]);
  });

  it('judges a held frame against the lateDropUs of the sequencer', () => {
    const { state, sequencer, put, groupIds } = harness({
      grouping: 'frame',
      reorderWindowUs: 100_000,
      lateDropUs: Number.POSITIVE_INFINITY,
    });
    state.now = 1_000_000;
    put(0, 1_000_000);
    sequencer.poll();
    state.now = 1_010_000;
    put(2, 1_050_000);
    sequencer.poll();
    state.now = 5_000_000;
    expect(groupIds(sequencer.poll().frames)).toEqual([2]);
    expect(sequencer.stats.late).toBe(0);
  });

  it('starts over after a restart: the first group is not held', () => {
    const { state, clock, sequencer, put, groupIds } = harness({ grouping: 'frame', reorderWindowUs: 100_000 });
    state.now = 1_000_000;
    put(0, 1_000_000);
    sequencer.poll();
    put(2, 1_500_000);
    expect(sequencer.poll().frames).toEqual([]);
    sequencer.restart();
    clock.due(1_000_000);
    put(40, 1_000_000);
    expect(sequencer.poll().frames).toEqual([expect.objectContaining({ groupId: 40, discontinuity: 'restart' })]);
    expect(groupIds(sequencer.poll().frames)).toEqual([]);
  });
});

describe('createFrameSequencer: catchUpAfterUs', () => {
  it('refuses a value that is not positive and finite', () => {
    const clock = createPlayoutClock({ now: () => 0 });
    expect(() => createFrameSequencer({ clock, catchUpAfterUs: 0 })).toThrow(RangeError);
    expect(() => createFrameSequencer({ clock, catchUpAfterUs: -5 })).toThrow(RangeError);
    expect(() => createFrameSequencer({ clock, catchUpAfterUs: Number.POSITIVE_INFINITY })).toThrow(RangeError);
    expect(() => createFrameSequencer({ clock, catchUpAfterUs: Number.NaN })).toThrow();
    expect(() => createFrameSequencer({ clock, catchUpAfterUs: 1 })).not.toThrow();
    expect(() => createFrameSequencer({ clock, grouping: 'frame', catchUpAfterUs: 200_000 })).not.toThrow();
  });

  it('signals a catch-up once frames have kept arriving late for that long with none delivered', () => {
    const { state, sequencer, put } = harness({ grouping: 'frame', catchUpAfterUs: 200_000 });
    put(0, 0);
    expect(sequencer.poll().frames).toHaveLength(1);
    state.now = 1_000_000;
    put(1, 100_000);
    expect(sequencer.poll().catchUp).toBeUndefined();
    state.now = 1_100_000;
    put(2, 200_000);
    expect(sequencer.poll().catchUp).toBeUndefined();
    state.now = 1_199_999;
    put(3, 300_000);
    expect(sequencer.poll().catchUp).toBeUndefined();
    state.now = 1_200_000;
    put(4, 400_000);
    const poll = sequencer.poll();
    expect(poll).toMatchObject({ frames: [], catchUp: true });
    expect(sequencer.stats.late).toBe(4);
  });

  it('signals it once, until a frame is delivered', () => {
    const { state, sequencer, put } = harness({ grouping: 'frame', catchUpAfterUs: 200_000 });
    put(0, 0);
    sequencer.poll();
    for (const at of [1_000_000, 1_250_000]) {
      state.now = at;
      put(Math.floor(at / 1000), at - 900_000);
      expect(sequencer.poll().catchUp).toBe(at === 1_250_000 ? true : undefined);
    }
    state.now = 1_400_000;
    put(2000, 500_000);
    expect(sequencer.poll().catchUp).toBeUndefined();
    state.now = 2_000_000;
    put(3000, 1_000_000);
    expect(sequencer.poll().catchUp).toBeUndefined();
  });

  it('is armed again by a delivery', () => {
    const { state, sequencer, put, groupIds } = harness({ grouping: 'frame', catchUpAfterUs: 200_000 });
    put(0, 0);
    sequencer.poll();
    state.now = 1_000_000;
    put(1, 100_000);
    sequencer.poll();
    state.now = 1_300_000;
    put(2, 200_000);
    expect(sequencer.poll().catchUp).toBe(true);
    // A frame on time: the backlog is gone.
    put(3, 1_300_000);
    const delivered = sequencer.poll();
    expect(groupIds(delivered.frames)).toEqual([3]);
    expect(delivered.catchUp).toBeUndefined();
    // Lateness counts from its start again, not from the earlier one.
    state.now = 2_000_000;
    put(4, 1_000_000);
    expect(sequencer.poll().catchUp).toBeUndefined();
    state.now = 2_300_000;
    put(5, 1_100_000);
    expect(sequencer.poll().catchUp).toBe(true);
  });

  it('does not count lateness that began before a delivery', () => {
    const { state, sequencer, put } = harness({ grouping: 'frame', catchUpAfterUs: 200_000 });
    put(0, 0);
    sequencer.poll();
    state.now = 1_000_000;
    put(1, 100_000);
    sequencer.poll();
    state.now = 1_150_000;
    put(2, 1_150_000);
    expect(sequencer.poll().frames).toHaveLength(1);
    state.now = 1_300_000;
    put(3, 200_000);
    expect(sequencer.poll().catchUp).toBeUndefined();
  });

  it('does not signal for one late frame followed by silence', () => {
    const { state, sequencer, put } = harness({ grouping: 'frame', catchUpAfterUs: 200_000 });
    put(0, 0);
    sequencer.poll();
    state.now = 1_000_000;
    put(1, 100_000);
    expect(sequencer.poll().catchUp).toBeUndefined();
    for (const at of [1_300_000, 3_000_000, 60_000_000]) {
      state.now = at;
      expect(sequencer.poll().catchUp).toBeUndefined();
    }
  });

  it('does not count a late frame judged again when another track moves the shared clock', () => {
    const { state, clock, sequencer, put } = harness({ grouping: 'frame', catchUpAfterUs: 200_000 });
    state.now = 1_000_000;
    put(0, 1_000_000);
    sequencer.poll();
    state.now = 1_100_000;
    put(1, 900_000);
    expect(sequencer.poll().catchUp).toBeUndefined();
    expect(sequencer.stats.late).toBe(1);
    // Audio on another track re-anchors the clock far ahead: the held candidate is judged late again, with no arrival.
    state.now = 1_500_000;
    clock.reportAudio(5_000_000);
    expect(sequencer.poll().catchUp).toBeUndefined();
    state.now = 2_000_000;
    expect(sequencer.poll().catchUp).toBeUndefined();
  });

  it('is never signalled when the option is not set', () => {
    const { state, sequencer, put } = harness({ grouping: 'frame' });
    put(0, 0);
    sequencer.poll();
    for (let step = 1; step <= 10; step++) {
      state.now = step * 100_000;
      put(step, 0);
      expect(sequencer.poll().catchUp).toBeUndefined();
    }
  });

  it('is never signalled when no frame is late, as with a limit of Infinity', () => {
    const { state, sequencer, put } = harness({
      grouping: 'frame',
      catchUpAfterUs: 100_000,
      lateDropUs: Number.POSITIVE_INFINITY,
    });
    put(0, 0);
    sequencer.poll();
    for (let step = 1; step <= 10; step++) {
      state.now = step * 100_000;
      put(step, 0);
      expect(sequencer.poll().catchUp).toBeUndefined();
    }
  });

  it('forgets the lateness seen before a restart', () => {
    const { state, clock, sequencer, put } = harness({ grouping: 'frame', catchUpAfterUs: 200_000 });
    put(0, 0);
    sequencer.poll();
    state.now = 1_000_000;
    put(1, 100_000);
    expect(sequencer.poll().catchUp).toBeUndefined();
    sequencer.restart();
    state.now = 1_150_000;
    clock.due(1_150_000);
    state.now = 1_250_000;
    put(2, 0);
    const poll = sequencer.poll();
    expect(poll.frames).toEqual([]);
    expect(poll.catchUp).toBeUndefined();
    expect(sequencer.stats.late).toBe(2);
  });

  it('signals a catch-up again after a restart once a frame has been delivered on the new subscription', () => {
    const { state, clock, sequencer, put } = harness({ grouping: 'frame', catchUpAfterUs: 200_000 });
    put(0, 0);
    sequencer.poll();
    state.now = 1_000_000;
    put(1, 100_000);
    sequencer.poll();
    state.now = 1_300_000;
    put(2, 200_000);
    expect(sequencer.poll().catchUp).toBe(true);
    sequencer.restart();
    clock.due(1_300_000);
    put(0, 1_300_000);
    expect(sequencer.poll().frames).toHaveLength(1);
    state.now = 2_000_000;
    put(1, 0);
    expect(sequencer.poll().catchUp).toBeUndefined();
    state.now = 2_200_000;
    put(2, 0);
    expect(sequencer.poll().catchUp).toBe(true);
  });

  // By design: a replacement that is late from its first frame does not catch up again until something is delivered
  // (a resync, at worst), so a link too slow for the track cannot make the subscription spin.
  it('does not signal again after a restart until a frame is delivered', () => {
    const { state, clock, sequencer, put } = harness({ grouping: 'frame', catchUpAfterUs: 200_000 });
    put(0, 0);
    sequencer.poll();
    state.now = 1_000_000;
    put(1, 100_000);
    sequencer.poll();
    state.now = 1_300_000;
    put(2, 200_000);
    expect(sequencer.poll().catchUp).toBe(true);
    sequencer.restart();
    clock.due(1_300_000);
    state.now = 2_000_000;
    put(0, 0);
    expect(sequencer.poll().catchUp).toBeUndefined();
    state.now = 2_200_000;
    put(1, 0);
    expect(sequencer.poll().catchUp).toBeUndefined();
  });
});

describe('createFrameSequencer: missingGroups', () => {
  it('is absent while group IDs follow one another', () => {
    const { state, sequencer, put } = harness({ grouping: 'frame' });
    state.now = 1_000;
    put(5, 1_000);
    put(6, 1_010);
    put(7, 1_020);
    const { frames } = sequencer.poll();
    expect(frames).toHaveLength(3);
    for (const frame of frames) expect('missingGroups' in frame).toBe(false);
  });

  it('names the group IDs skipped under frame grouping, inclusive', () => {
    const { state, sequencer, put } = harness({ grouping: 'frame' });
    state.now = 1_000;
    put(5, 1_000);
    put(6, 1_010);
    put(9, 1_020);
    put(10, 1_030);
    const { frames } = sequencer.poll();
    expect(frames.map((frame) => [frame.groupId, frame.missingGroups])).toEqual([
      [5, undefined],
      [6, undefined],
      [9, { firstGroupId: 7, lastGroupId: 8 }],
      [10, undefined],
    ]);
    expect(frames[2]?.discontinuity).toBe('gap');
  });

  it('includes a group dropped as late between two delivered', () => {
    const { state, sequencer, put } = harness({ grouping: 'frame' });
    state.now = 1_000_000;
    put(0, 1_000_000);
    put(1, 500_000);
    put(2, 1_000_000);
    const { frames } = sequencer.poll();
    expect(frames.map((frame) => [frame.groupId, frame.missingGroups])).toEqual([
      [0, undefined],
      [2, { firstGroupId: 1, lastGroupId: 1 }],
    ]);
  });

  it('is not set for a gap that the frames within one group make', () => {
    const { state, sequencer, put } = harness({ grouping: 'frame' });
    state.now = 1_000;
    put(3, 1_000, 0);
    put(3, 1_010, 5);
    const { frames } = sequencer.poll();
    expect(frames).toHaveLength(2);
    expect(frames[1]?.missingGroups).toBeUndefined();
  });

  it('is absent on the first frame after a restart, and counts again from it', () => {
    const { state, clock, sequencer, put } = harness({ grouping: 'frame' });
    state.now = 1_000;
    put(100, 1_000);
    expect(sequencer.poll().frames).toHaveLength(1);
    sequencer.restart();
    clock.due(1_000);
    put(3, 1_000);
    put(4, 1_010);
    put(8, 1_020);
    const { frames } = sequencer.poll();
    expect(frames.map((frame) => [frame.groupId, frame.discontinuity, frame.missingGroups])).toEqual([
      [3, 'restart', undefined],
      [4, undefined, undefined],
      [8, 'gap', { firstGroupId: 5, lastGroupId: 7 }],
    ]);
  });

  it('names the groups skipped under keyframe grouping too', () => {
    const { state, sequencer, put } = harness();
    state.now = 1_000_000;
    put(0, 1_000_000);
    sequencer.groupEnded(0);
    expect(sequencer.poll().frames.map((frame) => frame.missingGroups)).toEqual([undefined]);
    put(1, 1_100_000);
    sequencer.groupEnded(1);
    const second = sequencer.poll().frames;
    // Groups 2 and 3 never come; the later group goes out once the wait for them ends.
    state.now = 3_200_000;
    put(4, 3_200_000);
    const third = sequencer.poll().frames;
    expect([...second, ...third].map((frame) => [frame.groupId, frame.discontinuity, frame.missingGroups])).toEqual([
      [1, undefined, undefined],
      [4, 'gap', { firstGroupId: 2, lastGroupId: 3 }],
    ]);
  });

  it('is absent on the first keyframe-grouped frame after a restart', () => {
    const { state, clock, sequencer, put } = harness();
    state.now = 1_000_000;
    put(7, 1_000_000);
    sequencer.groupEnded(7);
    sequencer.poll();
    sequencer.restart();
    clock.due(1_000_000);
    put(2, 1_000_000);
    const { frames } = sequencer.poll();
    expect(frames).toEqual([expect.objectContaining({ groupId: 2, discontinuity: 'restart' })]);
    expect(frames[0]?.missingGroups).toBeUndefined();
  });
});
