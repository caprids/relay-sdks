import type { MoqtObject } from '@mux/moq-core';
import {
  createFrameSequencer,
  createPlayoutClock,
  LOC_TIMESCALE,
  LOC_TIMESTAMP,
  type SequencedFrame,
} from '@mux/moq-media';
import { afterEach, describe, expect, expectTypeOf, it, vi } from 'vitest';
import type { RelaySubscriptionHandlers, WantHandlers } from '../src/index.js';
import { createFrameReader } from '../src/media.js';

const object = (groupId: number, objectId: number, timestampUs: number): MoqtObject => ({
  groupId,
  objectId,
  subgroupId: 0,
  status: 'normal',
  properties: [{ type: LOC_TIMESTAMP, value: timestampUs }],
  payload: new Uint8Array([groupId, objectId]),
});
const ids = (frames: SequencedFrame[]) =>
  frames.map((frame) => [frame.groupId, frame.objectId, ...(frame.discontinuity ? [frame.discontinuity] : [])]);

/** A reader over a real sequencer and a clock whose time moves with the fake timers. */
function setup() {
  vi.useFakeTimers();
  let nowUs = 0;
  const clock = createPlayoutClock({ now: () => nowUs });
  const frames: SequencedFrame[] = [];
  const reader = createFrameReader(createFrameSequencer({ clock }), { onFrame: (frame) => frames.push(frame) });
  const advance = (us: number) => {
    nowUs += us;
    vi.advanceTimersByTime(us / 1000);
  };
  return { clock, frames, reader, advance };
}

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe('createFrameReader', () => {
  it("is a subscription's and a want's handlers", () => {
    const reader = createFrameReader(createFrameSequencer({ clock: createPlayoutClock() }), { onFrame() {} });
    expectTypeOf(reader).toMatchTypeOf<RelaySubscriptionHandlers>();
    expectTypeOf(reader).toMatchTypeOf<WantHandlers>();
  });

  it('delivers interleaved groups in decode order as their events arrive', () => {
    const { clock, frames, reader } = setup();
    clock.due(0);
    reader.onObject(object(0, 0, 0));
    reader.onObject(object(1, 0, 1_000_000));
    reader.onObject(object(0, 1, 33_333));
    expect(ids(frames)).toEqual([
      [0, 0],
      [0, 1],
    ]);
    reader.onSubgroupEnd({ groupId: 0 });
    expect(ids(frames)).toEqual([
      [0, 0],
      [0, 1],
      [1, 0],
    ]);
  });

  it('polls again when a wait ends, and skips to the next keyframe', () => {
    const { clock, frames, reader, advance } = setup();
    clock.due(0);
    reader.onObject(object(0, 0, 0));
    reader.onObject(object(1, 0, 1_000_000));
    expect(ids(frames)).toEqual([[0, 0]]);
    advance(1_999_000);
    expect(ids(frames)).toEqual([[0, 0]]);
    advance(1000);
    expect(ids(frames)).toEqual([
      [0, 0],
      [1, 0, 'gap'],
    ]);
  });

  it("reads timestamps through the subscription's track properties", () => {
    const { frames, reader } = setup();
    reader.onSubscribed({
      trackAlias: 1,
      parameters: {} as never,
      trackProperties: [{ type: LOC_TIMESCALE, value: 1000 }],
    });
    reader.onObject(object(0, 0, 5));
    expect(frames[0]?.timestampUs).toBe(5000);
  });

  it('starts over at a Restart and at a Move, marking the next frame', () => {
    const { frames, reader } = setup();
    reader.onObject(object(7, 0, 0));
    reader.onRestarted();
    reader.onObject(object(2, 0, 100));
    reader.onSwitched({
      from: { namespace: ['a'], track: 'video' },
      to: { namespace: ['b'], track: 'video' },
      subscribed: { trackAlias: 2, parameters: {} as never, trackProperties: [{ type: LOC_TIMESCALE, value: 1000 }] },
    });
    reader.onObject(object(0, 0, 1));
    expect(ids(frames)).toEqual([
      [7, 0],
      [2, 0, 'restart'],
      [0, 0, 'restart'],
    ]);
    expect(frames[2]?.timestampUs).toBe(1000);
  });

  it('carries on after onFrame throws', () => {
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    const seen: number[] = [];
    const reader = createFrameReader(createFrameSequencer({ clock: createPlayoutClock(), grouping: 'frame' }), {
      onFrame(frame) {
        seen.push(frame.groupId);
        if (frame.groupId === 0) throw new Error('decoder');
      },
    });
    reader.onObject(object(0, 0, 0));
    reader.onObject(object(1, 0, 20_000));
    expect(seen).toEqual([0, 1]);
    expect(error).toHaveBeenCalledOnce();
  });

  it('stops delivering and cancels its pending poll when closed', () => {
    const { clock, frames, reader, advance } = setup();
    clock.due(0);
    reader.onObject(object(0, 0, 0));
    reader.onObject(object(1, 0, 1_000_000));
    reader.close();
    advance(3_000_000);
    reader.onSubgroupEnd({ groupId: 0 });
    expect(ids(frames)).toEqual([[0, 0]]);
    expect(vi.getTimerCount()).toBe(0);
  });
});
