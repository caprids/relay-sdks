import type { KeyValuePair, MoqtObject } from '@caprid/moq-core';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { publisherTimeouts } from '../src/delivery-timeouts.js';
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
import type { RelaySession } from '../src/relay-session.js';
import { superviseRelay } from '../src/supervised-relay.js';
import { type RecordedGroup, settle, slotTrack } from './helpers/slot-track.js';
import { type FakeSession, fakeSession, until } from './helpers/supervision-fakes.js';

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

const decoder = new TextDecoder();
const bodies = (groups: RecordedGroup[]) =>
  groups.flatMap((group) => group.objects.map((object) => JSON.parse(decoder.decode(object.bytes))));

/** A publisher whose tracks are slot tracks, by name, with the delivery timeouts their properties declare. */
function fakePublisher(options: { demand?: boolean } = {}) {
  const tracks = new Map<string, ReturnType<typeof slotTrack>>();
  const registered = new Map<string, RegisterTrackOptions>();
  return {
    tracks,
    registered,
    registerTrack(register: RegisterTrackOptions) {
      const made = slotTrack({
        ...options,
        trackTimeouts: publisherTimeouts((register.trackProperties ?? []) as readonly KeyValuePair[]),
      });
      tracks.set(register.track, made);
      registered.set(register.track, register);
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

  it('holds back an event recorded as demand arrives, so the replay carries it first', async () => {
    const publisher = fakePublisher({ demand: false });
    const diagnostics = createDiagnostics();
    diagnostics.event('before');
    const publication = publishDiagnostics(diagnostics, publisher);
    const events = publisher.tracks.get(DIAGNOSTICS_TRACKS.events);
    await settle();
    events?.demand(true);
    // Before the publisher has seen the new demand: the track already has it, the replay has not gone out.
    diagnostics.event('racing');
    await settle();
    diagnostics.event('after');
    await settle();
    const sent = bodies(events?.groups ?? []) as DiagnosticsEventsObject[];
    expect(sent.map((body) => [body.backlog ?? false, body.events.map((event) => event.kind)])).toEqual([
      [true, ['before', 'racing']],
      [false, ['after']],
    ]);
    await publication.close();
  });

  it('replays a large hold in at most 32 objects, starting from the oldest event', async () => {
    const publisher = fakePublisher();
    const diagnostics = createDiagnostics({ keepEvents: 20_000 });
    for (let i = 0; i < 20_000; i++) diagnostics.event('tick');
    const publication = publishDiagnostics(diagnostics, publisher);
    await settle(1_000);
    const sent = bodies(publisher.tracks.get(DIAGNOSTICS_TRACKS.events)?.groups ?? []) as DiagnosticsEventsObject[];
    expect(sent.length).toBeLessThanOrEqual(32);
    expect(sent[0]?.events[0]?.seq).toBe(0);
    expect(sent.reduce((total, body) => total + body.events.length, 0)).toBe(20_000);
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

  it('registers both tracks behind media, and gives stats a delivery timeout of two intervals', async () => {
    const publisher = fakePublisher();
    const publication = publishDiagnostics(createDiagnostics(), publisher);
    expect(publisher.registered.get(DIAGNOSTICS_TRACKS.events)?.trackProperties).toEqual([{ type: 0x0e, value: 224 }]);
    expect(publisher.registered.get(DIAGNOSTICS_TRACKS.stats)?.trackProperties).toEqual([
      { type: 0x0e, value: 240 },
      { type: 0x02, value: 2_000 },
    ]);
    await publication.close();

    const custom = fakePublisher();
    const other = publishDiagnostics(createDiagnostics(), custom, {
      statsIntervalMs: 5_000,
      eventsPriority: 128,
      statsPriority: 255,
    });
    expect(custom.registered.get(DIAGNOSTICS_TRACKS.events)?.trackProperties).toEqual([{ type: 0x0e, value: 128 }]);
    expect(custom.registered.get(DIAGNOSTICS_TRACKS.stats)?.trackProperties).toEqual([
      { type: 0x0e, value: 255 },
      { type: 0x02, value: 10_000 },
    ]);
    await other.close();
  });

  it('checks the priorities', () => {
    const publish = (options: object) => () => publishDiagnostics(createDiagnostics(), fakePublisher(), options);
    expect(publish({ eventsPriority: 256 })).toThrow(RangeError);
    expect(publish({ statsPriority: -1 })).toThrow(RangeError);
    expect(publish({ statsPriority: 1.5 })).toThrow(RangeError);
    expect(publish({ eventsPriority: '1' })).toThrow(TypeError);
  });

  it('drops a snapshot the transport has not taken within two intervals, and sends the next', async () => {
    vi.useFakeTimers({ now: 0 });
    const publisher = fakePublisher();
    const diagnostics = createDiagnostics();
    let n = 0;
    diagnostics.section('counter', () => ({ n: ++n }));
    const publication = publishDiagnostics(diagnostics, publisher, { statsIntervalMs: 1_000 });
    const stats = publisher.tracks.get(DIAGNOSTICS_TRACKS.stats);
    stats?.stallOpen();
    // Snapshot 1 waits for its stream from 1 s and expires at 3 s. Only the newest snapshot waits behind it, and one
    // queued over two intervals is dropped before it is served.
    await vi.advanceTimersByTimeAsync(3_500);
    stats?.release();
    await vi.advanceTimersByTimeAsync(1_000);
    const sent = (bodies(stats?.groups ?? []) as DiagnosticsStatsObject[]).map((body) => body.stats['counter']);
    expect(sent).not.toContainEqual({ n: 1 });
    expect(sent.at(-1)).toEqual({ n: 4 });
    expect(sent.length).toBeLessThanOrEqual(3);

    // Behind a stream that never opens, each snapshot's own age drops it: nothing older than two intervals goes out.
    stats?.stallOpen();
    await vi.advanceTimersByTimeAsync(10_000);
    stats?.release();
    const before = stats?.groups.length ?? 0;
    await vi.advanceTimersByTimeAsync(500);
    const late = (bodies(stats?.groups ?? []) as DiagnosticsStatsObject[]).slice(before);
    expect(late.every((body) => Date.now() - body.t <= 2_500)).toBe(true);
    await publication.close();
  });

  it('replaces an event or a section JSON cannot encode, and keeps publishing', async () => {
    const publisher = fakePublisher();
    const diagnostics = createDiagnostics();
    diagnostics.section('good', () => ({ n: 1 }));
    diagnostics.section('bad', () => ({ n: 1n }));
    const publication = publishDiagnostics(diagnostics, publisher, { statsIntervalMs: 10 });
    const events = publisher.tracks.get(DIAGNOSTICS_TRACKS.events);
    await settle();
    diagnostics.event('app.bad', { n: 2n });
    diagnostics.event('app.good', { n: 2 });
    await settle();
    const sent = bodies(events?.groups ?? []) as DiagnosticsEventsObject[];
    expect(sent.flatMap((body) => body.events)).toEqual([
      expect.objectContaining({ seq: 0, kind: 'app.bad', error: 'not encodable as JSON' }),
      expect.objectContaining({ seq: 1, kind: 'app.good', n: 2 }),
    ]);
    await until(() => (publisher.tracks.get(DIAGNOSTICS_TRACKS.stats)?.groups.length ?? 0) > 0);
    const [snapshot] = bodies(publisher.tracks.get(DIAGNOSTICS_TRACKS.stats)?.groups ?? []) as DiagnosticsStatsObject[];
    expect(snapshot?.stats).toEqual({ good: { n: 1 }, bad: { error: 'not encodable as JSON' } });
    await publication.close();
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
  it('publishes on a supervised relay’s publication and follows on another supervised relay', async () => {
    const fast = { initialDelayMs: 0, maxDelayMs: 0 };
    const relay = async () => {
      const connections: FakeSession[] = [];
      const supervised = await superviseRelay(
        async () => {
          const session = fakeSession();
          connections.push(session);
          return session as unknown as RelaySession;
        },
        { url: 'https://relay.example.com/moq', reconnect: fast },
      );
      return { supervised, connections };
    };
    const alice = await relay();
    const bob = await relay();
    try {
      const diagnostics = createDiagnostics();
      diagnostics.event('link', { link: 'relay', status: 'live' });
      const publication = publishDiagnostics(diagnostics, alice.supervised.publish(['p', 'alice']));
      const generation = alice.supervised.generation!;
      const served = alice.connections[0]!.publications.get(`p/alice/${generation}`)!;
      expect([...served.tracks.keys()]).toEqual([DIAGNOSTICS_TRACKS.events, DIAGNOSTICS_TRACKS.stats]);
      served.tracks.get(DIAGNOSTICS_TRACKS.events)!.setDemand(true);
      await until(() => served.tracks.get(DIAGNOSTICS_TRACKS.events)!.writes.length === 1, 'the replay');
      diagnostics.event('want', { want: 'p/bob/audio', what: 'subscribed' });
      await until(() => served.tracks.get(DIAGNOSTICS_TRACKS.events)!.writes.length === 2, 'the live event');

      const kinds: string[] = [];
      const unfollow = followDiagnostics(
        bob.supervised,
        { namespace: ['p', 'alice'], generations: true },
        { onEvents: (events) => kinds.push(...events.map((event) => event.kind)) },
      );
      // generations: true discovers the newest generation under the root before it subscribes.
      await until(() => bob.connections[0]!.discoveries.length > 0, 'discovery');
      for (const discovery of bob.connections[0]!.discoveries) {
        discovery.push({ type: 'announced', namespace: ['p', 'alice', generation] } as never);
      }
      await until(() => bob.connections[0]!.subscriptions.length === 2, 'both tracks wanted');
      const subscription = bob.connections[0]!.subscriptions.find(
        (sub) => sub.options.track === DIAGNOSTICS_TRACKS.events,
      )!;
      expect(subscription.options.namespace).toEqual(['p', 'alice', generation]);
      subscription.accept();
      // Deliver what alice wrote twice, as a replacement subscription's replay would: each event arrives once.
      const writes = served.tracks.get(DIAGNOSTICS_TRACKS.events)!.writes;
      for (const [groupId, payload] of [...writes, ...writes].entries()) {
        subscription.handlers.onObject?.({ groupId, objectId: 0, status: 'normal', payload } as MoqtObject);
      }
      expect(kinds).toEqual(['link', 'want']);
      unfollow();
      await publication.close();
    } finally {
      await alice.supervised.close();
      await bob.supervised.close();
    }
  });
});
