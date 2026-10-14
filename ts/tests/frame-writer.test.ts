import type { LocFrame } from '@mux/moq-media';
import { afterEach, describe, expect, expectTypeOf, it, vi } from 'vitest';
import { RelayError } from '../src/errors.js';
import { createFrameWriter, type Frame, type FrameOutcome } from '../src/media.js';
import { settle, slotTrack } from './helpers/slot-track.js';

let sequence = 0;
const key = (extra: Partial<Frame> = {}): Frame => ({ payload: Uint8Array.of(0x65, ++sequence), key: true, ...extra });
const delta = (extra: Partial<Frame> = {}): Frame => ({
  payload: Uint8Array.of(0x41, ++sequence),
  key: false,
  ...extra,
});
const kinds = (outcomes: FrameOutcome[]) => outcomes.map((outcome) => outcome.kind);
const payloads = (group: { objects: { bytes: Uint8Array }[] }) => group.objects.map((object) => object.bytes[1]);

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe('keyframe grouping', () => {
  it('opens a group on each keyframe, appends deltas, and FINs the previous group', async () => {
    const track = slotTrack();
    const writer = createFrameWriter(track.writer);
    const frames = [key(), delta(), delta(), key(), delta()];
    const outcomes = await Promise.all(frames.map((frame) => writer.write(frame)));
    expect(outcomes.map((outcome) => [outcome.kind, outcome.groupId])).toEqual([
      ['sent', 0],
      ['sent', 0],
      ['sent', 0],
      ['sent', 1],
      ['sent', 1],
    ]);
    expect(track.groups.map((group) => group.objects.length)).toEqual([3, 2]);
    expect(track.groups[0]?.finished).toBe(true);
    expect([writer.groups, writer.objects]).toEqual([2, 5]);
    await writer.close();
    expect(track.groups[1]?.finished).toBe(true);
  });

  it('sends properties as the frame supplies them and serves unawaited calls in call order', async () => {
    const track = slotTrack();
    const writer = createFrameWriter(track.writer);
    const properties = [{ type: 0x06, value: 2 }];
    const frames = [key({ properties }), delta(), delta(), delta()];
    const pending = frames.map((frame) => writer.write(frame));
    expect(writer.backlog).toBe(3);
    await Promise.all(pending);
    expect(payloads(track.groups[0] as never)).toEqual(frames.map((frame) => (frame.payload as Uint8Array)[1]));
    expect(track.groups[0]?.objects[0]?.properties).toEqual(properties);
    expect(writer.backlog).toBe(0);
  });

  it('needs a keyframe from the start, skipping deltas until one arrives, without calling back for the start', async () => {
    const onKeyframeNeeded = vi.fn();
    const track = slotTrack();
    const writer = createFrameWriter(track.writer, { onKeyframeNeeded });
    expect(writer.keyframeNeeded).toBe(true);
    expect((await writer.write(delta())).kind).toBe('skipped');
    const sent = writer.write(key());
    expect(writer.keyframeNeeded).toBe(false);
    expect((await sent).kind).toBe('sent');
    expect(onKeyframeNeeded).not.toHaveBeenCalled();
  });

  it('skips at once without demand, never waiting on the write deadline', async () => {
    vi.useFakeTimers();
    const track = slotTrack({ demand: false });
    const writer = createFrameWriter(track.writer);
    expect(kinds(await Promise.all([writer.write(key()), writer.write(delta())]))).toEqual(['skipped', 'skipped']);
    expect(track.groups).toEqual([]);
  });

  it('resets the open group on a replacement subscription and asks for a keyframe', async () => {
    const onKeyframeNeeded = vi.fn();
    const track = slotTrack();
    const writer = createFrameWriter(track.writer, { onKeyframeNeeded });
    await writer.write(key());
    await writer.write(delta());
    track.demand(true);
    await settle();
    expect(track.groups[0]?.aborted).toBe(true);
    expect(writer.keyframeNeeded).toBe(true);
    expect(onKeyframeNeeded).toHaveBeenCalledTimes(1);
    expect((await writer.write(delta())).kind).toBe('skipped');
    const resumed = await writer.write(key());
    expect(resumed).toEqual({ kind: 'sent', groupId: 1 });
    expect(writer.keyframeNeeded).toBe(false);
  });

  it('asks for a keyframe when demand ends and again when it returns', async () => {
    const onKeyframeNeeded = vi.fn();
    const track = slotTrack();
    const writer = createFrameWriter(track.writer, { onKeyframeNeeded });
    await writer.write(key());
    track.demand(false);
    await settle();
    expect(onKeyframeNeeded).toHaveBeenCalledTimes(1);
    track.demand(true);
    await settle();
    // Still needed since the end: entering the state again is what calls back, not each change.
    expect(onKeyframeNeeded).toHaveBeenCalledTimes(1);
    expect((await writer.write(key())).kind).toBe('sent');
    track.demand(true);
    await settle();
    expect(onKeyframeNeeded).toHaveBeenCalledTimes(2);
  });

  it('never writes into a retired group, whichever turn of a completing write the demand change lands on', async () => {
    for (let turns = 0; turns < 12; turns++) {
      const track = slotTrack();
      const writer = createFrameWriter(track.writer);
      await writer.write(key());
      const writing = writer.write(delta());
      await settle(turns);
      track.demand(true);
      await writing;
      await settle();
      const next = await writer.write(delta());
      expect([turns, next.kind]).toEqual([turns, 'skipped']);
      expect(await writer.write(key())).toMatchObject({ kind: 'sent' });
    }
  });

  it('reports the frame being written when demand changes as interrupted by the write slot', async () => {
    const track = slotTrack();
    const writer = createFrameWriter(track.writer);
    await writer.write(key());
    track.stall();
    const writing = writer.write(delta());
    await settle();
    track.demand(true);
    await settle();
    expect(writer.keyframeNeeded).toBe(true);
    track.release();
    // The slot itself ends the old subscription's group; the writer only reports what became of the frame.
    const outcome = await writing;
    expect([outcome.kind, outcome.error?.code]).toEqual(['interrupted', 'SUBSCRIPTION_CHANGED']);
    expect((await writer.write(delta())).kind).toBe('skipped');
    expect((await writer.write(key())).kind).toBe('sent');
  });
});

describe('backlog', () => {
  it('drops the backlog, the open group and the frame being written when a delta overflows it', async () => {
    const onKeyframeNeeded = vi.fn();
    const track = slotTrack();
    const writer = createFrameWriter(track.writer, { maxBacklog: 2, onKeyframeNeeded });
    await writer.write(key());
    track.stall();
    const writing = writer.write(delta());
    const waiting = [writer.write(delta()), writer.write(delta())];
    expect(writer.backlog).toBe(2);
    const overflowing = await writer.write(delta());
    expect(overflowing).toEqual({ kind: 'dropped', groupId: 0 });
    expect(kinds(await Promise.all(waiting))).toEqual(['dropped', 'dropped']);
    expect(await writing).toEqual({ kind: 'dropped', groupId: 0 });
    expect(track.groups[0]?.aborted).toBe(true);
    expect(writer.keyframeNeeded).toBe(true);
    expect(onKeyframeNeeded).toHaveBeenCalledTimes(1);
    track.release();
    // Deltas until the next keyframe are casualties of the drop too.
    expect(await writer.write(delta())).toEqual({ kind: 'dropped' });
    expect(await writer.write(key())).toEqual({ kind: 'sent', groupId: 1 });
    expect(await writer.write(delta())).toEqual({ kind: 'sent', groupId: 1 });
  });

  it('resets the previous group when the backlog overflows while a keyframe is FINning it', async () => {
    const track = slotTrack();
    const writer = createFrameWriter(track.writer, { maxBacklog: 1 });
    await writer.write(key());
    await writer.write(delta());
    // Hold the FIN the next keyframe sends for group 0.
    track.stallFin();
    const finning = writer.write(key());
    await settle();
    const waiting = writer.write(delta());
    const recovery = writer.write(key());
    expect((await waiting).kind).toBe('dropped');
    expect(track.groups[0]?.aborted).toBe(true);
    expect((await finning).kind).toBe('dropped');
    track.release();
    expect(await recovery).toEqual({ kind: 'sent', groupId: 1 });
  });

  it('lets an overflowing keyframe open the next group, without asking for another', async () => {
    const onKeyframeNeeded = vi.fn();
    const track = slotTrack();
    const writer = createFrameWriter(track.writer, { maxBacklog: 1, onKeyframeNeeded });
    await writer.write(key());
    track.stall();
    const writing = writer.write(delta());
    const waiting = writer.write(delta());
    const next = writer.write(key());
    expect(writer.keyframeNeeded).toBe(false);
    expect((await waiting).kind).toBe('dropped');
    expect((await writing).kind).toBe('dropped');
    track.release();
    expect(await next).toEqual({ kind: 'sent', groupId: 1 });
    expect(onKeyframeNeeded).not.toHaveBeenCalled();
    expect(payloads(track.groups[1] as never)).toHaveLength(1);
  });

  it('drops a group whose stream was still opening when the backlog overflowed', async () => {
    const track = slotTrack();
    const writer = createFrameWriter(track.writer, { maxBacklog: 1 });
    track.stallOpen();
    const first = writer.write(key());
    const second = writer.write(delta());
    await settle();
    const third = writer.write(key());
    expect((await second).kind).toBe('dropped');
    track.release();
    expect(await first).toEqual({ kind: 'dropped', groupId: 0 });
    expect(track.groups[0]?.aborted).toBe(true);
    expect(track.groups[0]?.objects).toEqual([]);
    expect(await third).toEqual({ kind: 'sent', groupId: 1 });
  });

  it('drops the oldest waiting frame under frame grouping', async () => {
    const track = slotTrack();
    const writer = createFrameWriter(track.writer, { grouping: 'frame', maxBacklog: 2 });
    track.stall();
    const frames = [key(), key(), key(), key()];
    const outcomes = frames.map((frame) => writer.write(frame));
    expect(writer.backlog).toBe(2);
    expect((await outcomes[1])?.kind).toBe('dropped');
    track.release();
    expect(kinds(await Promise.all(outcomes))).toEqual(['sent', 'dropped', 'sent', 'sent']);
    expect(track.groups.map((group) => [group.objects.length, group.finished])).toEqual([
      [1, true],
      [1, true],
      [1, true],
    ]);
    expect(writer.keyframeNeeded).toBe(false);
  });

  it('defaults to 8 frames under keyframe grouping and 25 under frame grouping', async () => {
    for (const [grouping, bound] of [
      ['keyframe', 8],
      ['frame', 25],
    ] as const) {
      const track = slotTrack();
      const writer = createFrameWriter(track.writer, { grouping });
      track.stall();
      const outcomes = [writer.write(key())];
      for (let index = 0; index < bound; index++) outcomes.push(writer.write(key()));
      expect(writer.backlog).toBe(bound);
      outcomes.push(writer.write(key()));
      expect(writer.backlog).toBeLessThanOrEqual(bound);
      track.release();
      expect(kinds(await Promise.all(outcomes))).toContain('dropped');
    }
  });
});

describe('failures', () => {
  it('reports an interruption, asks for a keyframe, and resumes at the next one', async () => {
    const onKeyframeNeeded = vi.fn();
    const track = slotTrack();
    const writer = createFrameWriter(track.writer, { onKeyframeNeeded });
    await writer.write(key());
    track.failNextWrite(new Error('stream reset'));
    const interrupted = await writer.write(delta());
    expect(interrupted.kind).toBe('interrupted');
    expect(interrupted.error).toBeInstanceOf(RelayError);
    expect(interrupted.groupId).toBe(0);
    expect(onKeyframeNeeded).toHaveBeenCalledTimes(1);
    expect((await writer.write(delta())).kind).toBe('skipped');
    expect(await writer.write(key())).toEqual({ kind: 'sent', groupId: 1 });
    expect(writer.closed).toBe(false);
  });

  it('ends when the connection does, reporting every later frame as ended', async () => {
    const track = slotTrack();
    const writer = createFrameWriter(track.writer);
    await writer.write(key());
    track.disconnect(new Error('gone'));
    const ended = await writer.write(delta());
    expect(ended.kind).toBe('ended');
    expect(writer.ended).toBe(ended.error);
    expect(writer.closed).toBe(true);
    expect(await writer.write(key())).toEqual({ kind: 'ended', error: ended.error });
  });

  it('reports a closed track as ended with nothing in flight', async () => {
    const track = slotTrack();
    const writer = createFrameWriter(track.writer);
    track.writer.close();
    const outcome = await writer.write(key());
    expect(outcome.kind).toBe('ended');
    expect(outcome.error?.code).toBe('CLOSED');
  });

  it('rejects a frame the SDK refuses, resetting the group so the next delta is skipped', async () => {
    const track = slotTrack();
    const writer = createFrameWriter(track.writer);
    await writer.write(key());
    await expect(writer.write({ payload: 'nope' as never, key: false })).rejects.toThrow(TypeError);
    expect(track.groups[0]?.aborted).toBe(true);
    expect((await writer.write(delta())).kind).toBe('skipped');
  });

  it('reports a throwing callback and carries on', async () => {
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    const track = slotTrack();
    const writer = createFrameWriter(track.writer, {
      onKeyframeNeeded() {
        throw new Error('encoder gone');
      },
    });
    await writer.write(key());
    track.demand(true);
    await settle();
    expect(error).toHaveBeenCalledTimes(1);
    expect((await writer.write(key())).kind).toBe('sent');
  });
});

describe('lateness and discontinuity', () => {
  it('drops stale frames, resets the group they broke, and needs a keyframe', async () => {
    const late = new Set<Frame>();
    const onKeyframeNeeded = vi.fn();
    const track = slotTrack();
    const writer = createFrameWriter(track.writer, { stale: (frame) => late.has(frame), onKeyframeNeeded });
    await writer.write(key());
    const old = delta();
    late.add(old);
    expect(await writer.write(old)).toEqual({ kind: 'stale', groupId: 0 });
    expect(track.groups[0]?.aborted).toBe(true);
    expect((await writer.write(delta())).kind).toBe('stale');
    expect(onKeyframeNeeded).toHaveBeenCalledTimes(1);
    expect((await writer.write(key())).kind).toBe('sent');
  });

  it('resets on a discontinuity and lets a discontinuous keyframe start the next group', async () => {
    const onKeyframeNeeded = vi.fn();
    const track = slotTrack();
    const writer = createFrameWriter(track.writer, { onKeyframeNeeded });
    await writer.write(key());
    expect(await writer.write(key({ discontinuity: true }))).toEqual({ kind: 'sent', groupId: 1 });
    expect(track.groups[0]?.aborted).toBe(true);
    expect(onKeyframeNeeded).not.toHaveBeenCalled();
    expect((await writer.write(delta({ discontinuity: true }))).kind).toBe('stale');
    expect(onKeyframeNeeded).toHaveBeenCalledTimes(1);
  });

  it('checks staleness again once a frame-grouped stream has opened', async () => {
    let calls = 0;
    const track = slotTrack();
    const writer = createFrameWriter(track.writer, { grouping: 'frame', stale: () => ++calls > 1 });
    expect(await writer.write(key())).toEqual({ kind: 'stale', groupId: 0 });
    expect(track.groups[0]?.aborted).toBe(true);
  });
});

describe('closing and arguments', () => {
  it('drops the backlog, FINs the open group after the frame in progress, and refuses later frames', async () => {
    const track = slotTrack();
    const writer = createFrameWriter(track.writer);
    await writer.write(key());
    track.stall();
    const writing = writer.write(delta());
    const waiting = writer.write(delta());
    const closing = writer.close();
    expect((await waiting).kind).toBe('dropped');
    expect(writer.closed).toBe(true);
    await expect(writer.write(key())).rejects.toMatchObject({ code: 'CLOSED' });
    track.release();
    expect((await writing).kind).toBe('sent');
    await closing;
    expect(track.groups[0]?.finished).toBe(true);
    expect(track.writer.closed).toBe(false);
  });

  it('refuses bad options before touching the track', () => {
    const track = slotTrack();
    expect(() => createFrameWriter(track.writer, { grouping: 'gop' as never })).toThrow(TypeError);
    expect(() => createFrameWriter(track.writer, { maxBacklog: 0 })).toThrow(RangeError);
    expect(() => createFrameWriter(track.writer, { maxBacklog: 1.5 })).toThrow(RangeError);
    expect(() => createFrameWriter(track.writer, { stale: true as never })).toThrow(TypeError);
    expect(() => createFrameWriter(track.writer, { onKeyframeNeeded: 1 as never })).toThrow(TypeError);
  });

  it('never needs a keyframe under frame grouping', async () => {
    const onKeyframeNeeded = vi.fn();
    const track = slotTrack();
    const writer = createFrameWriter(track.writer, { grouping: 'frame', onKeyframeNeeded });
    expect(writer.keyframeNeeded).toBe(false);
    track.demand(true);
    track.failNextWrite(new Error('reset'));
    expect((await writer.write(key())).kind).toBe('interrupted');
    expect(onKeyframeNeeded).not.toHaveBeenCalled();
  });

  it("takes @mux/moq-media's LocFrame as a frame", () => {
    const writer = createFrameWriter<LocFrame>(slotTrack().writer);
    expectTypeOf(writer.write).parameter(0).toEqualTypeOf<LocFrame>();
  });
});
