import { afterEach, describe, expect, it, vi } from 'vitest';
import type { PublishProperty, RelayTrackWriter } from '../src/index.js';
import {
  createReplaceableTrack,
  type Frame,
  type ReplaceableTrackOptions,
  type TrackReplacement,
} from '../src/media.js';
import { settle, slotTrack } from './helpers/slot-track.js';

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

const text = (label: string): Frame => ({ payload: new TextEncoder().encode(label), key: true });
const labels = (track: { groups: { objects: { bytes: Uint8Array }[] }[] }) =>
  track.groups.flatMap((group) => group.objects.map((object) => new TextDecoder().decode(object.bytes)));

/** A publisher whose tracks are slot tracks, kept by name. A replacement has demand only when `replacementDemand`. */
function setup(replacementDemand = false) {
  const tracks = new Map<string, ReturnType<typeof slotTrack>>();
  const registerTrack = vi.fn(({ track }: { track: string }) => {
    const created = slotTrack({ demand: tracks.size === 0 || replacementDemand });
    tracks.set(track, created);
    return created.writer;
  });
  const update = vi.fn();
  const build = vi.fn((name: string) => new TextEncoder().encode(name));
  const create = (options: Partial<ReplaceableTrackOptions> = {}) =>
    createReplaceableTrack({ registerTrack } as never, {
      track: 'audio',
      catalog: { service: { update }, build },
      ...options,
    });
  const named = (name: string) => tracks.get(name) as ReturnType<typeof slotTrack>;
  const catalogs = () => update.mock.calls.map(([document]) => new TextDecoder().decode(document));
  return { tracks, registerTrack, update, build, create, named, catalogs };
}

describe('creation', () => {
  it('registers the first track with its options and lists it in the catalog', () => {
    const { create, registerTrack, catalogs } = setup();
    const trackProperties: PublishProperty[] = [{ type: 0x06, value: 500 }];
    const reply = create({ trackProperties, writeTimeoutMs: 3000 });
    expect(registerTrack).toHaveBeenCalledTimes(1);
    expect(registerTrack).toHaveBeenCalledWith({ track: 'audio', trackProperties, writeTimeoutMs: 3000 });
    expect(catalogs()).toEqual(['audio']);
    expect([reply.track, reply.replacements, reply.closed]).toEqual(['audio', 0, false]);
  });

  it('passes on only the options it was given', () => {
    const { create, registerTrack } = setup();
    create();
    expect(registerTrack).toHaveBeenCalledWith({ track: 'audio' });
  });

  it('closes the first track and throws when the catalog cannot be built', () => {
    const { create, build, named } = setup();
    build.mockImplementation(() => {
      throw new Error('no catalog');
    });
    expect(() => create()).toThrow('no catalog');
    expect(named('audio').writer.closed).toBe(true);
  });

  it('sends each frame as its own group on the current track', async () => {
    const { create, named } = setup();
    const reply = create();
    const outcomes = await Promise.all([reply.write(text('a')), reply.write(text('b'))]);
    expect(outcomes.map((outcome) => outcome.kind)).toEqual(['sent', 'sent']);
    expect(named('audio').groups.map((group) => group.objects.length)).toEqual([1, 1]);
    expect(labels(named('audio'))).toEqual(['a', 'b']);
  });

  it('hands writer options to every frame writer', async () => {
    const { create, named } = setup(true);
    const reply = create({ writer: { grouping: 'keyframe' } });
    await reply.write({ payload: Uint8Array.of(1), key: true });
    await reply.write({ payload: Uint8Array.of(2), key: false });
    expect(named('audio').groups.map((group) => group.objects.length)).toEqual([2]);
  });

  it('checks its options', () => {
    const { create, registerTrack } = setup();
    const bad = (options: Record<string, unknown>) => () => create(options as never);
    expect(bad({ track: '' })).toThrow(TypeError);
    expect(bad({ track: 3 })).toThrow(TypeError);
    expect(bad({ catalog: undefined })).toThrow(TypeError);
    expect(bad({ catalog: { service: { update() {} } } })).toThrow(TypeError);
    expect(bad({ catalog: { build: () => new Uint8Array() } })).toThrow(TypeError);
    expect(bad({ name: 'audio-2' })).toThrow(TypeError);
    expect(bad({ demandWaitMs: -1 })).toThrow(RangeError);
    expect(bad({ demandWaitMs: Number.NaN })).toThrow(RangeError);
    expect(bad({ demandWaitMs: 2 ** 31 })).toThrow(RangeError);
    expect(bad({ demandWaitMs: '1' })).toThrow(RangeError);
    expect(() => createReplaceableTrack({ registerTrack } as never, undefined as never)).toThrow(TypeError);
    expect(registerTrack).not.toHaveBeenCalled();
  });
});

describe('replace', () => {
  it('registers the next track, lists it, closes the old one, and says what it did', async () => {
    const { create, registerTrack, named, catalogs } = setup(true);
    const trackProperties: PublishProperty[] = [{ type: 0x06, value: 500 }];
    const reply = create({ trackProperties, writeTimeoutMs: 3000 });
    const replacement: TrackReplacement = await reply.replace();
    expect(replacement).toEqual({ from: 'audio', to: 'audio-2', replacements: 1 });
    expect(registerTrack).toHaveBeenLastCalledWith({ track: 'audio-2', trackProperties, writeTimeoutMs: 3000 });
    expect(catalogs()).toEqual(['audio', 'audio-2']);
    expect(named('audio').writer.closed).toBe(true);
    expect(named('audio-2').writer.closed).toBe(false);
    expect([reply.track, reply.replacements]).toEqual(['audio-2', 1]);
  });

  it('sends later writes on the new track, and counts on from there', async () => {
    const { create, named, catalogs } = setup(true);
    const reply = create();
    await reply.write(text('old'));
    await reply.replace();
    expect((await reply.write(text('new'))).kind).toBe('sent');
    expect(labels(named('audio'))).toEqual(['old']);
    expect(labels(named('audio-2'))).toEqual(['new']);
    expect(await reply.replace()).toEqual({ from: 'audio-2', to: 'audio-3', replacements: 2 });
    expect(catalogs().at(-1)).toBe('audio-3');
  });

  it('names replacements with the name option', async () => {
    const { create, named, catalogs } = setup(true);
    const name = vi.fn((replacement: number) => `reply/${replacement}`);
    const reply = create({ name });
    expect(await reply.replace()).toEqual({ from: 'audio', to: 'reply/1', replacements: 1 });
    expect(name).toHaveBeenCalledWith(1);
    expect(named('reply/1')).toBeDefined();
    expect(catalogs().at(-1)).toBe('reply/1');
  });

  it('rejects a name that is not a non-empty string and changes nothing', async () => {
    const { create, tracks } = setup(true);
    const reply = create({ name: () => '' });
    await expect(reply.replace()).rejects.toThrow(TypeError);
    expect([...tracks.keys()]).toEqual(['audio']);
    expect(reply.track).toBe('audio');
  });

  it('rejects when the catalog cannot be built, closes the new track, and keeps the current one', async () => {
    const { create, build, named, catalogs } = setup(true);
    const reply = create();
    build.mockImplementation((name: string) => {
      if (name === 'audio-2') throw new Error('no catalog');
      return new TextEncoder().encode(name);
    });
    await expect(reply.replace()).rejects.toThrow('no catalog');
    expect(named('audio-2').writer.closed).toBe(true);
    expect(named('audio').writer.closed).toBe(false);
    expect([reply.track, reply.replacements]).toEqual(['audio', 0]);
    expect(catalogs()).toEqual(['audio']);
    expect((await reply.write(text('still'))).kind).toBe('sent');
    expect(labels(named('audio'))).toEqual(['still']);
  });

  it('asks for a new name after a failed attempt, since the publisher may hold the old one', async () => {
    const { create, build, registerTrack, named } = setup(true);
    // Names are unique per connection, including after a track closes, as a real publisher holds them.
    const seen = new Set<string>();
    const original = registerTrack.getMockImplementation() as (options: { track: string }) => RelayTrackWriter;
    registerTrack.mockImplementation((options: { track: string }) => {
      if (seen.has(options.track)) throw new Error(`${options.track} is taken`);
      seen.add(options.track);
      return original(options);
    });
    const reply = create();
    build.mockImplementationOnce(() => {
      throw new Error('no catalog');
    });
    await expect(reply.replace()).rejects.toThrow('no catalog');
    expect(named('audio-2').writer.closed).toBe(true);
    expect(await reply.replace()).toEqual({ from: 'audio', to: 'audio-3', replacements: 1 });
    expect(reply.track).toBe('audio-3');
  });

  it('rejects when the publisher cannot register the track and keeps the current one', async () => {
    const { create, registerTrack, named } = setup(true);
    const reply = create();
    registerTrack.mockImplementationOnce(() => {
      throw new Error('already registered');
    });
    await expect(reply.replace()).rejects.toThrow('already registered');
    expect(named('audio').writer.closed).toBe(false);
    expect(reply.replacements).toBe(0);
  });

  it('drops the frames the old track has not sent, and closes it', async () => {
    const { create, named } = setup(true);
    const reply = create();
    named('audio').stall();
    const writes = ['a', 'b', 'c'].map((label) => reply.write(text(label)));
    await settle();
    const replacing = reply.replace();
    await settle();
    named('audio').release();
    await replacing;
    const outcomes = await Promise.all(writes);
    expect(outcomes.map((outcome) => outcome.kind)).not.toContain('skipped');
    expect(outcomes.slice(1).map((outcome) => outcome.kind)).toEqual(['dropped', 'dropped']);
    expect(labels(named('audio'))).not.toContain('c');
    expect(named('audio').writer.closed).toBe(true);
  });
});

describe('the wait for demand', () => {
  /** A replacement the relay has not subscribed to. */
  async function replaced(options: Partial<ReplaceableTrackOptions> = {}) {
    const context = setup(false);
    const reply = context.create(options);
    await reply.replace();
    return { ...context, reply, replacement: context.named('audio-2') };
  }

  it('holds frames written before the relay subscribes, and sends them in order once it does', async () => {
    const { reply, replacement } = await replaced();
    const outcomes: string[] = [];
    const writes = ['one', 'two', 'three'].map((label) =>
      reply.write(text(label)).then((outcome) => outcomes.push(outcome.kind)),
    );
    await settle();
    expect(outcomes).toEqual([]);
    expect(replacement.groups).toEqual([]);
    replacement.demand(true);
    await Promise.all(writes);
    expect(outcomes).toEqual(['sent', 'sent', 'sent']);
    expect(labels(replacement)).toEqual(['one', 'two', 'three']);
  });

  it('puts frames written while the waiting ones go out behind them', async () => {
    const { reply, replacement } = await replaced();
    const first = reply.write(text('one'));
    await settle();
    replacement.demand(true);
    const rest = [reply.write(text('two')), reply.write(text('three'))];
    const outcomes = await Promise.all([first, ...rest]);
    expect(outcomes.map((outcome) => outcome.kind)).toEqual(['sent', 'sent', 'sent']);
    expect(labels(replacement)).toEqual(['one', 'two', 'three']);
  });

  it('skips the frames still waiting when demandWaitMs runs out', async () => {
    vi.useFakeTimers();
    const { reply, replacement } = await replaced({ demandWaitMs: 500 });
    const writes = [reply.write(text('one')), reply.write(text('two'))];
    await vi.advanceTimersByTimeAsync(499);
    let done = false;
    void Promise.all(writes).then(() => {
      done = true;
    });
    await settle();
    expect(done).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    expect((await Promise.all(writes)).map((outcome) => outcome.kind)).toEqual(['skipped', 'skipped']);
    expect(replacement.groups).toEqual([]);
    // The wait is over: later frames go straight to the writer.
    replacement.demand(true);
    expect((await reply.write(text('three'))).kind).toBe('sent');
  });

  it('waits 2 seconds by default', async () => {
    vi.useFakeTimers();
    const { reply } = await replaced();
    let kind: string | undefined;
    void reply.write(text('one')).then((outcome) => {
      kind = outcome.kind;
    });
    await vi.advanceTimersByTimeAsync(1999);
    expect(kind).toBeUndefined();
    await vi.advanceTimersByTimeAsync(1);
    expect(kind).toBe('skipped');
  });

  it('does not wait with demandWaitMs 0', async () => {
    const { reply, replacement } = await replaced({ demandWaitMs: 0 });
    expect((await reply.write(text('one'))).kind).toBe('skipped');
    expect(replacement.groups).toEqual([]);
  });

  it('does not wait for a replacement the relay already subscribed to, or on the first track', async () => {
    const subscribed = setup(true);
    const reply = subscribed.create();
    expect((await reply.write(text('first'))).kind).toBe('sent');
    await reply.replace();
    expect((await reply.write(text('second'))).kind).toBe('sent');

    const unsubscribed = setup(false);
    const lonely = unsubscribed.create();
    unsubscribed.named('audio').demand(false);
    expect((await lonely.write(text('first'))).kind).toBe('skipped');
  });

  it('drops the waiting frames when the track is replaced again', async () => {
    const { reply, replacement } = await replaced();
    const waiting = [reply.write(text('one')), reply.write(text('two'))];
    await settle();
    await reply.replace();
    expect((await Promise.all(waiting)).map((outcome) => outcome.kind)).toEqual(['dropped', 'dropped']);
    expect(replacement.writer.closed).toBe(true);
    expect(replacement.groups).toEqual([]);
    expect(reply.track).toBe('audio-3');
  });

  it('hands waiting frames to the writer when the track ends', async () => {
    const { reply, replacement } = await replaced();
    const waiting = reply.write(text('one'));
    await settle();
    (replacement.writer as RelayTrackWriter).close();
    expect(['skipped', 'ended']).toContain((await waiting).kind);
  });
});

describe('close', () => {
  it('closes the current track, and write and replace reject afterwards', async () => {
    const { create, named, registerTrack } = setup(true);
    const reply = create();
    await reply.close();
    await reply.close();
    expect(reply.closed).toBe(true);
    expect(named('audio').writer.closed).toBe(true);
    await expect(reply.write(text('late'))).rejects.toThrow();
    await expect(reply.replace()).rejects.toThrow();
    expect(registerTrack).toHaveBeenCalledTimes(1);
  });

  it('closes the replacement and drops the frames waiting for it', async () => {
    const context = setup(false);
    const reply = context.create();
    await reply.replace();
    const waiting = reply.write(text('one'));
    await settle();
    await reply.close();
    expect((await waiting).kind).toBe('dropped');
    expect(context.named('audio').writer.closed).toBe(true);
    expect(context.named('audio-2').writer.closed).toBe(true);
  });

  it('retires the current track at once, so frames waiting for it are dropped even if the relay subscribes meanwhile', async () => {
    const context = setup(false);
    const reply = context.create();
    // The old track's frame is stuck in the transport, so its retirement takes a while.
    context.named('audio').stall();
    const old = reply.write(text('old'));
    await settle();
    const replaced = reply.replace();
    const waiting = reply.write(text('new'));
    await settle();
    const closing = reply.close();
    // The relay subscribes to the replacement while the old track is still closing.
    context.named('audio-2').demand(true);
    await settle();
    expect((await waiting).kind).toBe('dropped');
    context.named('audio').release();
    await closing;
    await replaced;
    await old;
    expect(labels(context.named('audio-2'))).toEqual([]);
    expect(context.named('audio-2').writer.closed).toBe(true);
  });

  it('waits for every old track still closing, after overlapping replacements', async () => {
    const context = setup(true);
    const reply = context.create();
    context.named('audio').stallFin();
    void reply.write(text('a'));
    await settle();
    const first = reply.replace();
    context.named('audio-2').stallFin();
    void reply.write(text('b'));
    await settle();
    const second = reply.replace();
    let closed = false;
    const closing = reply.close().then(() => {
      closed = true;
    });
    await settle();
    expect(closed).toBe(false);
    context.named('audio-2').release();
    await settle();
    expect(closed).toBe(false);
    context.named('audio').release();
    await closing;
    await Promise.all([first, second]);
    expect(['audio', 'audio-2', 'audio-3'].map((name) => context.named(name).writer.closed)).toEqual([
      true,
      true,
      true,
    ]);
    // A second close shares the first.
    await reply.close();
  });

  it('drops what the current track has not sent', async () => {
    const { create, named } = setup(true);
    const reply = create();
    named('audio').stall();
    const writes = [reply.write(text('a')), reply.write(text('b'))];
    await settle();
    const closing = reply.close();
    await settle();
    named('audio').release();
    await closing;
    expect((await Promise.all(writes)).map((outcome) => outcome.kind)[1]).toBe('dropped');
  });
});
