import type { MoqtObject } from '@mux/moq-core';
import {
  createFrameSequencer,
  createPlayoutClock,
  LOC_TIMESCALE,
  LOC_TIMESTAMP,
  type SequencedFrame,
} from '@mux/moq-media';
import { afterEach, describe, expect, expectTypeOf, it, vi } from 'vitest';
import {
  connectRelayPublisher,
  connectRelaySubscriber,
  type RelaySubscriptionHandlers,
  type WantHandlers,
} from '../src/index.js';
import { createFrameReader, type FrameReader, type FrameReaderSequencer } from '../src/media.js';
import { createRelayHub } from './helpers/relay-hub.js';

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

  it('ends the batch in hand when onFrame restarts the reader, and never interleaves batches', () => {
    vi.useFakeTimers();
    const clock = createPlayoutClock({ now: () => 0 });
    clock.due(0);
    const seen: SequencedFrame[] = [];
    const reader = createFrameReader(createFrameSequencer({ clock }), {
      onFrame(frame) {
        seen.push(frame);
        if (frame.groupId === 1 && frame.objectId === 0) {
          reader.onRestarted();
          reader.onObject(object(20, 0, 2_000_000));
        }
      },
    });
    reader.onObject(object(0, 0, 0));
    reader.onObject(object(1, 0, 1_000_000));
    reader.onObject(object(1, 1, 1_033_333));
    reader.onSubgroupEnd({ groupId: 0 });
    expect(ids(seen)).toEqual([
      [0, 0],
      [1, 0],
      [20, 0, 'restart'],
    ]);
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

  it('releases held frames once the clock anchors, with no further event', () => {
    const { clock, frames, reader, advance } = setup();
    reader.onObject(object(0, 0, 0));
    reader.onObject(object(1, 0, 1_000_000));
    expect(ids(frames)).toEqual([[0, 0]]);
    clock.due(0); // the player shows the first frame: the clock anchors
    advance(2_050_000);
    expect(ids(frames)).toEqual([
      [0, 0],
      [1, 0, 'gap'],
    ]);
  });

  it('notices another track resetting the shared clock before its own wait ends', () => {
    const { clock, frames, reader, advance } = setup();
    clock.due(0);
    advance(5_000_000);
    reader.onObject(object(1, 0, 2_000_000)); // 3 s late: held as a resync candidate, with a 2 s wait
    expect(frames).toEqual([]);
    clock.reset(); // the other track resyncs; no further event reaches this reader
    advance(100_000);
    expect(ids(frames)).toEqual([[1, 0]]);
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

describe('createFrameReader: onCatchUp', () => {
  /** A sequencer that returns the polls it is given, one per call, then empty ones. */
  const scripted = (polls: { frames: number[]; catchUp?: boolean }[]): FrameReaderSequencer<number> => {
    const queue = [...polls];
    return {
      trackProperties: [],
      object() {},
      groupEnded() {},
      groupReset() {},
      restart() {},
      poll() {
        const next = queue.shift();
        return { frames: next?.frames ?? [], waitUs: undefined, ...(next?.catchUp ? { catchUp: true } : {}) };
      },
    };
  };

  it('is called after the frames of the poll that signalled it', () => {
    const calls: string[] = [];
    const reader = createFrameReader(scripted([{ frames: [1, 2], catchUp: true }]), {
      onFrame: (frame) => calls.push(`frame ${frame}`),
      onCatchUp: () => calls.push('catch-up'),
    });
    reader.onObject(object(0, 0, 0));
    expect(calls).toEqual(['frame 1', 'frame 2', 'catch-up']);
  });

  it('is called for a poll with no frames', () => {
    const onCatchUp = vi.fn();
    const reader = createFrameReader(scripted([{ frames: [], catchUp: true }]), { onFrame() {}, onCatchUp });
    reader.onObject(object(0, 0, 0));
    expect(onCatchUp).toHaveBeenCalledOnce();
  });

  it('is called once per signal, not on polls that do not carry one', () => {
    const onCatchUp = vi.fn();
    const reader = createFrameReader(scripted([{ frames: [1] }, { frames: [2], catchUp: true }, { frames: [3] }]), {
      onFrame() {},
      onCatchUp,
    });
    reader.onObject(object(0, 0, 0));
    expect(onCatchUp).not.toHaveBeenCalled();
    reader.onObject(object(0, 1, 0));
    expect(onCatchUp).toHaveBeenCalledOnce();
    reader.onObject(object(0, 2, 0));
    expect(onCatchUp).toHaveBeenCalledOnce();
  });

  it('is optional: a signal without a handler changes nothing', () => {
    const seen: number[] = [];
    const reader = createFrameReader(scripted([{ frames: [1], catchUp: true }, { frames: [2] }]), {
      onFrame: (frame) => seen.push(frame),
    });
    reader.onObject(object(0, 0, 0));
    reader.onObject(object(0, 1, 0));
    expect(seen).toEqual([1, 2]);
  });

  it('reports a throw to console.error and carries on delivering', () => {
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    const seen: number[] = [];
    const cause = new Error('resubscribe failed');
    const reader = createFrameReader(scripted([{ frames: [1], catchUp: true }, { frames: [2] }]), {
      onFrame: (frame) => seen.push(frame),
      onCatchUp() {
        throw cause;
      },
    });
    reader.onObject(object(0, 0, 0));
    expect(error).toHaveBeenCalledOnce();
    const reported = error.mock.calls[0]?.[0] as Error;
    expect(reported.message).toContain('onCatchUp threw');
    expect(reported.cause).toBe(cause);
    reader.onObject(object(0, 1, 0));
    expect(seen).toEqual([1, 2]);
  });

  it('is not called once the reader was closed, or restarted from onFrame, in the same poll', () => {
    const closing = vi.fn();
    const closer: FrameReader = createFrameReader(scripted([{ frames: [1], catchUp: true }]), {
      onFrame: () => closer.close(),
      onCatchUp: closing,
    });
    closer.onObject(object(0, 0, 0));
    expect(closing).not.toHaveBeenCalled();

    const restarting = vi.fn();
    const restarter: FrameReader = createFrameReader(scripted([{ frames: [1], catchUp: true }]), {
      onFrame: () => restarter.onRestarted(),
      onCatchUp: restarting,
    });
    restarter.onObject(object(0, 0, 0));
    expect(restarting).not.toHaveBeenCalled();
  });

  it('is called by a real sequencer that signals it, on the poll after the late frame', () => {
    vi.useFakeTimers();
    let nowUs = 0;
    const clock = createPlayoutClock({ now: () => nowUs });
    clock.due(0);
    const onCatchUp = vi.fn();
    const reader = createFrameReader(createFrameSequencer({ clock, grouping: 'frame', catchUpAfterUs: 200_000 }), {
      onFrame() {},
      onCatchUp,
    });
    reader.onObject(object(0, 0, 0));
    for (const [step, at] of [1_000_000, 1_100_000, 1_200_000].entries()) {
      nowUs = at;
      reader.onObject(object(step + 1, 0, step * 100_000));
    }
    expect(onCatchUp).toHaveBeenCalledOnce();
  });
});

describe('createFrameReader through the relay hub', () => {
  // #339: the hub forwarded each object on a subgroup stream of its own and FINed it, so the reader took the group as
  // ended after its keyframe and dropped every later frame as behind.
  it('reads every frame of a keyframe group, which the hub forwards on one stream', async () => {
    const hub = createRelayHub();
    const url = 'https://relay.example.com/moq';
    const namespace = ['testing', 'video'];
    const publisher = await connectRelayPublisher({ url, namespace, createTransport: hub.connectPublisher });
    const subscriber = await connectRelaySubscriber({ url, createTransport: hub.connectSubscriber });
    try {
      const track = publisher.registerTrack({ track: 'video' });
      const sequencer = createFrameSequencer({ clock: createPlayoutClock() });
      const frames: SequencedFrame[] = [];
      subscriber.subscribe(
        { namespace, track: 'video' },
        createFrameReader(sequencer, { onFrame: (frame) => frames.push(frame) }),
      );
      const group = await track.openGroup();
      for (let objectId = 0; objectId < 10; objectId++) {
        await group.write(new Uint8Array([objectId]), {
          properties: [{ type: LOC_TIMESTAMP, value: objectId * 33_333 }],
        });
      }
      await group.close();
      await vi.waitFor(() => expect(frames).toHaveLength(10));
      expect(frames.map((frame) => frame.objectId)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
      expect(sequencer.stats.late).toBe(0);
    } finally {
      subscriber.close();
      publisher.close();
      hub.destroy();
    }
  });
});
