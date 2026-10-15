import { readFileSync } from 'node:fs';
import type { MoqtObject } from '@mux/moq-core';
import { createFrameSequencer, createPlayoutClock, LOC_TIMESCALE, LOC_TIMESTAMP } from '@mux/moq-media';
import { describe, expect, it } from 'vitest';

type Step = {
  at: string;
  anchor?: string;
  report?: string;
  object?: [string, string, string | null];
  groupEnd?: string;
  groupReset?: string;
  restart?: true;
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
});
