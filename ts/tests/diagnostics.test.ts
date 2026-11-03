import type { MoqtObject } from '@caprid/moq-core';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  createDiagnostics,
  DIAGNOSTICS_TRACKS,
  type DiagnosticEvent,
  type DiagnosticsEventsObject,
  type DiagnosticsStatsObject,
  followDiagnostics,
  publishDiagnostics,
} from '../src/diagnostics.js';
import type { RegisterTrackOptions, WantHandlers, WantOptions } from '../src/index.js';
import { type RecordedGroup, settle, slotTrack } from './helpers/slot-track.js';

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

const decoder = new TextDecoder();
const bodies = (groups: RecordedGroup[]) =>
  groups.flatMap((group) => group.objects.map((object) => JSON.parse(decoder.decode(object.bytes))));

/** A publisher whose tracks are slot tracks, by name. */
function fakePublisher(options: { demand?: boolean } = {}) {
  const tracks = new Map<string, ReturnType<typeof slotTrack>>();
  return {
    tracks,
    registerTrack({ track }: RegisterTrackOptions) {
      const made = slotTrack(options);
      tracks.set(track, made);
      return made.writer;
    },
  };
}

/** A subscriber that records wants and lets a test deliver objects to them. */
function fakeSubscriber() {
  const wants = new Map<string, { options: WantOptions; handlers: WantHandlers }>();
  return {
    wants,
    want(key: string, options: WantOptions, handlers: WantHandlers = {}) {
      wants.set(key, { options, handlers });
    },
    unwant(key: string) {
      wants.delete(key);
    },
    deliver(key: string, body: unknown) {
      const payload =
        typeof body === 'string' ? new TextEncoder().encode(body) : new TextEncoder().encode(JSON.stringify(body));
      wants.get(key)?.handlers.onObject?.({ groupId: 0, objectId: 0, payload } as MoqtObject);
    },
  };
}

describe('createDiagnostics', () => {
  it('numbers events from 0, stamps them, and keeps the recorder’s own fields', () => {
    vi.useFakeTimers({ now: 1_000 });
    const diagnostics = createDiagnostics();
    const first = diagnostics.event('link', { link: 'publisher', status: 'live' });
    vi.setSystemTime(1_500);
    const second = diagnostics.event('app.joined', { t: 0, seq: 99, kind: 'forged', who: 'p1' });
    expect(first).toEqual({ t: 1_000, seq: 0, kind: 'link', link: 'publisher', status: 'live' });
    expect(second).toEqual({ t: 1_500, seq: 1, kind: 'app.joined', who: 'p1' });
    expect(diagnostics.events()).toEqual([first, second]);
    expect(diagnostics.events(1)).toEqual([second]);
    expect(diagnostics.instance).toMatch(/^[0-9a-z]{12}$/);
    expect(createDiagnostics().instance).not.toBe(diagnostics.instance);
  });

  it('holds at most keepEvents, and nothing older than keepMs', () => {
    vi.useFakeTimers({ now: 0 });
    const diagnostics = createDiagnostics({ keepEvents: 3, keepMs: 10_000 });
    for (let i = 0; i < 5; i++) diagnostics.event('tick');
    expect(diagnostics.events().map((event) => event.seq)).toEqual([2, 3, 4]);
    vi.setSystemTime(10_001);
    diagnostics.event('tick');
    expect(diagnostics.events().map((event) => event.seq)).toEqual([5]);
  });

  it('tells listeners each event, survives one that throws, and stops one that unlistens', () => {
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    const diagnostics = createDiagnostics();
    const heard: string[] = [];
    diagnostics.listen(() => {
      throw new Error('listener bug');
    });
    const stop = diagnostics.listen((event) => heard.push(event.kind));
    diagnostics.event('a');
    stop();
    diagnostics.event('b');
    expect(heard).toEqual(['a']);
    expect(error).toHaveBeenCalledTimes(2);
  });

  it('snapshots every section by name; a replaced section stays, a throwing one reads its error', () => {
    const diagnostics = createDiagnostics();
    const removeOld = diagnostics.section('send', () => ({ old: true }));
    diagnostics.section('send', () => ({ objects: 3 }));
    removeOld();
    diagnostics.section('broken', () => {
      throw new Error('gone');
    });
    expect(diagnostics.snapshot()).toEqual({ send: { objects: 3 }, broken: { error: 'gone' } });
  });

  it('scopes events and sections under a name, sharing one sequence and one snapshot', () => {
    const diagnostics = createDiagnostics();
    const roster = diagnostics.scope('roster');
    const audio = diagnostics.scope('p2').scope('audio');
    diagnostics.event('app.joined');
    roster.event('link', { status: 'live', source: 'forged' });
    audio.event('audio-dropout', { ms: 400 });
    expect(diagnostics.events().map((event) => [event.seq, event.source, event.kind])).toEqual([
      [0, undefined, 'app.joined'],
      [1, 'roster', 'link'],
      [2, 'p2/audio', 'audio-dropout'],
    ]);
    expect(roster.events()).toHaveLength(3);
    expect(roster.instance).toBe(diagnostics.instance);
    audio.section('sequencer', () => ({ late: 1 }));
    roster.section('link', () => ({ status: 'live' }));
    expect(diagnostics.snapshot()).toEqual({ 'p2/audio.sequencer': { late: 1 }, 'roster.link': { status: 'live' } });
    expect(roster.snapshot()).toEqual(diagnostics.snapshot());
    expect(() => diagnostics.scope('')).toThrow(TypeError);
  });

  it('checks its arguments', () => {
    expect(() => createDiagnostics({ keepMs: 0 })).toThrow(RangeError);
    expect(() => createDiagnostics({ keepEvents: 1.5 })).toThrow(RangeError);
    const diagnostics = createDiagnostics();
    expect(() => diagnostics.event('')).toThrow(TypeError);
    expect(() => diagnostics.event('x', null as never)).toThrow(TypeError);
    expect(() => diagnostics.section('', () => 0)).toThrow(TypeError);
    expect(() => diagnostics.listen(3 as never)).toThrow(TypeError);
  });
});

describe('publishDiagnostics', () => {
  it('sends nothing without demand, then the held events first and each new event live', async () => {
    const publisher = fakePublisher({ demand: false });
    const diagnostics = createDiagnostics();
    diagnostics.event('before');
    const publication = publishDiagnostics(diagnostics, publisher);
    const events = publisher.tracks.get(DIAGNOSTICS_TRACKS.events);
    diagnostics.event('unwatched');
    await settle();
    expect(events?.groups).toEqual([]);

    events?.demand(true);
    await settle();
    diagnostics.event('live', { n: 1 });
    await settle();
    const sent = bodies(events?.groups ?? []) as DiagnosticsEventsObject[];
    expect(sent.map((body) => [body.backlog ?? false, body.events.map((event) => event.kind)])).toEqual([
      [true, ['before', 'unwatched']],
      [false, ['live']],
    ]);
    expect(sent.every((body) => body.instance === diagnostics.instance)).toBe(true);
    await publication.close();
  });

  it('replays again for a replacement relay subscription, in chunks of 200 events', async () => {
    const publisher = fakePublisher();
    const diagnostics = createDiagnostics();
    for (let i = 0; i < 250; i++) diagnostics.event('tick');
    const publication = publishDiagnostics(diagnostics, publisher);
    const events = publisher.tracks.get(DIAGNOSTICS_TRACKS.events);
    await settle();
    events?.demand(true);
    await settle(60);
    const sizes = (bodies(events?.groups ?? []) as DiagnosticsEventsObject[]).map((body) => body.events.length);
    expect(sizes).toEqual([200, 50, 200, 50]);
    await publication.close();
  });

  it('sends a snapshot of every section each interval while diag.stats has demand', async () => {
    vi.useFakeTimers({ now: 5_000 });
    const publisher = fakePublisher();
    const diagnostics = createDiagnostics();
    let n = 0;
    diagnostics.section('counter', () => ({ n: ++n }));
    const publication = publishDiagnostics(diagnostics, publisher, { statsIntervalMs: 1_000 });
    const stats = publisher.tracks.get(DIAGNOSTICS_TRACKS.stats);
    await vi.advanceTimersByTimeAsync(2_000);
    stats?.demand(false);
    await vi.advanceTimersByTimeAsync(3_000);
    const sent = bodies(stats?.groups ?? []) as DiagnosticsStatsObject[];
    expect(sent).toEqual([
      { instance: diagnostics.instance, t: 6_000, stats: { counter: { n: 1 } } },
      { instance: diagnostics.instance, t: 7_000, stats: { counter: { n: 2 } } },
    ]);
    await publication.close();
  });

  it('stops on close: no more objects, and both tracks closed', async () => {
    const publisher = fakePublisher();
    const diagnostics = createDiagnostics();
    const publication = publishDiagnostics(diagnostics, publisher);
    await settle();
    await publication.close();
    await publication.close();
    const events = publisher.tracks.get(DIAGNOSTICS_TRACKS.events);
    const before = events?.groups.length;
    diagnostics.event('after');
    await settle();
    expect(events?.groups.length).toBe(before);
    expect(events?.writer.closed).toBe(true);
    expect(publisher.tracks.get(DIAGNOSTICS_TRACKS.stats)?.writer.closed).toBe(true);
  });

  it('checks the stats interval', () => {
    expect(() => publishDiagnostics(createDiagnostics(), fakePublisher(), { statsIntervalMs: 0 })).toThrow(RangeError);
  });
});

describe('followDiagnostics', () => {
  const event = (seq: number, kind = 'tick'): DiagnosticEvent => ({ t: 1_000 + seq, seq, kind });

  it('wants both tracks under the key, following generations, with the events parameters', () => {
    const subscriber = fakeSubscriber();
    const parameters = { subscriberPriority: 200 };
    followDiagnostics(subscriber, { namespace: ['room', 'p1'], generations: true, eventsParameters: parameters }, {});
    expect([...subscriber.wants.keys()]).toEqual(['room/p1/diag.events', 'room/p1/diag.stats']);
    expect(subscriber.wants.get('room/p1/diag.events')?.options).toEqual({
      namespace: ['room', 'p1'],
      track: 'diag.events',
      generations: true,
      parameters,
    });
    expect(subscriber.wants.get('room/p1/diag.stats')?.options).toEqual({
      namespace: ['room', 'p1'],
      track: 'diag.stats',
      generations: true,
    });
  });

  it('delivers each event once per recorder, across replays, and skips what does not parse', () => {
    const subscriber = fakeSubscriber();
    const delivered: [string, boolean, number[]][] = [];
    followDiagnostics(
      subscriber,
      { namespace: ['p1'], key: 'p1' },
      { onEvents: (events, info) => delivered.push([info.instance, info.backlog, events.map((e) => e.seq)]) },
    );
    subscriber.deliver('p1/diag.events', { instance: 'a', events: [event(2)] });
    subscriber.deliver('p1/diag.events', { instance: 'a', backlog: true, events: [event(0), event(1), event(2)] });
    subscriber.deliver('p1/diag.events', { instance: 'a', events: [event(2)] });
    subscriber.deliver('p1/diag.events', { instance: 'b', events: [event(0)] });
    subscriber.deliver('p1/diag.events', 'not json');
    subscriber.deliver('p1/diag.events', { instance: 'a', events: [{ seq: 'x' }] });
    expect(delivered).toEqual([
      ['a', false, [2]],
      ['a', true, [0, 1]],
      ['b', false, [0]],
    ]);
  });

  it('delivers stats objects, and unwants both tracks when unfollowed', () => {
    const subscriber = fakeSubscriber();
    const stats: DiagnosticsStatsObject[] = [];
    const unfollow = followDiagnostics(subscriber, { namespace: ['p1'] }, { onStats: (body) => stats.push(body) });
    const body = { instance: 'a', t: 7, stats: { send: { objects: 1 } } };
    subscriber.deliver('p1/diag.stats', body);
    subscriber.deliver('p1/diag.stats', { instance: 'a' });
    expect(stats).toEqual([body]);
    unfollow();
    expect(subscriber.wants.size).toBe(0);
  });

  it('reads what publishDiagnostics writes', async () => {
    const publisher = fakePublisher();
    const diagnostics = createDiagnostics();
    diagnostics.event('link', { link: 'subscriber', status: 'live' });
    const publication = publishDiagnostics(diagnostics, publisher);
    await settle();
    diagnostics.event('want', { want: 'p2/audio', what: 'subscribed' });
    await settle();
    const subscriber = fakeSubscriber();
    const kinds: string[] = [];
    followDiagnostics(
      subscriber,
      { namespace: ['p1'] },
      { onEvents: (events) => kinds.push(...events.map((e) => e.kind)) },
    );
    for (const group of publisher.tracks.get(DIAGNOSTICS_TRACKS.events)?.groups ?? []) {
      for (const object of group.objects) {
        subscriber.wants.get('p1/diag.events')?.handlers.onObject?.({ payload: object.bytes } as MoqtObject);
      }
    }
    expect(kinds).toEqual(['link', 'want']);
    await publication.close();
  });
});
