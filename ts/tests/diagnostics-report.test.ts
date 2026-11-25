import type { MoqtObject } from '@caprid/moq-core';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { reportEndpoint } from '../src/connect-session.js';
import { createDiagnostics, type DiagnosticEvent, watchAudioInput } from '../src/diagnostics.js';
import { describeFailure, endpointHost } from '../src/diagnostics-report.js';
import { RelayError } from '../src/errors.js';
import { createFrameReader, createFrameWriter } from '../src/media.js';
import type { ConnectRelayPublisherOptions } from '../src/publish.js';
import type { RelaySession } from '../src/relay-session.js';
import { superviseRelayPublisher } from '../src/supervised-publisher.js';
import { superviseRelay } from '../src/supervised-relay.js';
import { superviseRelaySubscriber, type WantHandlers } from '../src/supervised-subscriber.js';
import { settle, slotTrack } from './helpers/slot-track.js';
import {
  type FakePublisher,
  type FakeSession,
  type FakeSubscriber,
  fakePublisher,
  fakeSession,
  fakeSubscriber,
  until,
} from './helpers/supervision-fakes.js';

const fast = { initialDelayMs: 0, maxDelayMs: 0 };
const url = 'https://relay.example.com/moq';
const cleanup: (() => unknown)[] = [];
afterEach(async () => {
  for (const close of cleanup.splice(0)) await close();
  vi.useRealTimers();
  vi.restoreAllMocks();
});

/** The events' kinds and chosen fields, without times and sequence numbers. */
const brief = (events: DiagnosticEvent[], ...fields: string[]) =>
  events.map((event) => [event.source, event.kind, ...fields.map((field) => event[field])]);

describe('a supervised subscriber with diagnostics', () => {
  async function start() {
    const diagnostics = createDiagnostics();
    const connections: FakeSubscriber[] = [];
    const subscriber = await superviseRelaySubscriber(
      async (options) => {
        const fake = fakeSubscriber();
        connections.push(fake);
        (options as { [reportEndpoint]?: (endpoint: string, framing: string) => void })[reportEndpoint]?.(
          url,
          'websocket',
        );
        return fake;
      },
      { url, reconnect: fast, diagnostics: diagnostics.scope('roster') },
    );
    cleanup.push(() => subscriber.close());
    return { diagnostics, subscriber, connections };
  }

  it('reports each status change, with the carrier once live, and the error that ended a connection', async () => {
    const { diagnostics, connections } = await start();
    connections[0]?.end(new RelayError('TRANSPORT_ERROR', 'lost https://r/moq?jwt=abc.def.ghi', 'connection'));
    await until(() => connections.length === 2, 'reconnect');
    await until(() => diagnostics.events().filter((event) => event.kind === 'link').length === 3, 'live again');
    expect(brief(diagnostics.events(), 'status', 'carrier', 'error')).toEqual([
      ['roster/subscriber', 'link', 'live', 'websocket', undefined],
      [
        'roster/subscriber',
        'link',
        'reconnecting',
        undefined,
        'TRANSPORT_ERROR (connection): lost https://r/moq?jwt=[redacted]',
      ],
      ['roster/subscriber', 'link', 'live', 'websocket', undefined],
    ]);
    expect(diagnostics.snapshot()['roster/subscriber.link']).toEqual({
      status: 'live',
      carrier: 'websocket',
      relay: 'relay.example.com',
      connects: 2,
    });
    expect(
      diagnostics
        .events()
        .filter((event) => event['status'] === 'live')
        .map((event) => event['relay']),
    ).toEqual(['relay.example.com', 'relay.example.com']);
  });

  it('reports a want’s life and counts its objects, and still calls the application’s handlers', async () => {
    const { diagnostics, subscriber, connections } = await start();
    const seen: string[] = [];
    const handlers: WantHandlers = {
      onSubscribed: () => seen.push('subscribed'),
      onRestarted: ({ reason }) => seen.push(`restarted ${reason}`),
    };
    subscriber.want('p2/audio', { namespace: ['room', 'p2'], track: 'audio', retry: fast }, handlers);
    await until(() => connections[0]?.subscriptions.length === 1, 'subscribe');
    const first = connections[0]?.subscriptions[0];
    first?.accept();
    first?.object(0);
    first?.object(1);
    first?.done();
    await until(() => connections[0]?.subscriptions.length === 2, 'resubscribe');
    connections[0]?.subscriptions[1]?.reject({ errorCode: 0x1, retryInterval: 0, reason: 'unauthorized' });
    await until(() => brief(diagnostics.events(), 'what').some(([, , what]) => what === 'error'), 'error');
    expect(seen).toEqual(['subscribed', 'restarted publisher-done']);
    expect(
      brief(
        diagnostics.events().filter((event) => event.kind === 'want'),
        'want',
        'what',
        'reason',
      ),
    ).toEqual([
      ['roster/subscriber', 'want', 'p2/audio', 'subscribed', undefined],
      ['roster/subscriber', 'want', 'p2/audio', 'first-object', undefined],
      ['roster/subscriber', 'want', 'p2/audio', 'publisher-done', undefined],
      ['roster/subscriber', 'want', 'p2/audio', 'restarted', 'publisher-done'],
      ['roster/subscriber', 'want', 'p2/audio', 'error', undefined],
    ]);
    // A want that failed for good is forgotten, as the subscriber forgets it.
    expect(diagnostics.snapshot()['roster/subscriber.wants']).toEqual({});
  });

  it('shows each live want’s track, subscription, objects and restarts in the wants section', async () => {
    const { diagnostics, subscriber, connections } = await start();
    subscriber.want('p2/video', { namespace: ['room', 'p2'], track: 'video', retry: fast });
    await until(() => connections[0]?.subscriptions.length === 1, 'subscribe');
    connections[0]?.subscriptions[0]?.accept();
    connections[0]?.subscriptions[0]?.object(0);
    expect(diagnostics.snapshot()['roster/subscriber.wants']).toEqual({
      'p2/video': {
        track: 'video',
        subscribed: true,
        objects: 1,
        restarts: 0,
        firstObjectMs: expect.any(Number),
        groupResets: 0,
        deliveryTimeouts: 0,
      },
    });
    subscriber.unwant('p2/video');
    expect(diagnostics.snapshot()['roster/subscriber.wants']).toEqual({});
  });

  it('empties the wants section when the subscriber closes', async () => {
    const { diagnostics, subscriber, connections } = await start();
    subscriber.want('p2/audio', { namespace: ['room', 'p2'], track: 'audio', retry: fast });
    await until(() => connections[0]?.subscriptions.length === 1, 'subscribe');
    connections[0]?.subscriptions[0]?.accept();
    expect(Object.keys(diagnostics.snapshot()['roster/subscriber.wants'] as object)).toEqual(['p2/audio']);
    subscriber.close();
    expect(diagnostics.snapshot()['roster/subscriber.wants']).toEqual({});
  });

  it('says how long a want took to its first object, once, and a move to the object that cut over', async () => {
    vi.useFakeTimers({ toFake: ['performance'] });
    const { diagnostics, subscriber, connections } = await start();
    subscriber.want('p2/video', { namespace: ['room', 'p2'], track: 'video-hd', retry: fast });
    await until(() => connections[0]?.subscriptions.length === 1, 'subscribe');
    vi.advanceTimersByTime(120);
    connections[0]?.subscriptions[0]?.accept();
    vi.advanceTimersByTime(30);
    connections[0]?.subscriptions[0]?.object(0);
    connections[0]?.subscriptions[0]?.object(1);
    // A layer move: the replacement subscribes at once and cuts over at its first object, which waits on a keyframe.
    subscriber.want('p2/video', { namespace: ['room', 'p2'], track: 'video-sd', retry: fast });
    await until(() => connections[0]?.subscriptions.length === 2, 'move');
    connections[0]?.subscriptions[1]?.accept();
    vi.advanceTimersByTime(700);
    connections[0]?.subscriptions[1]?.object(5);
    const wants = diagnostics.events().filter((event) => event.kind === 'want');
    expect(brief(wants, 'what', 'firstObjectMs', 'to')).toEqual([
      ['roster/subscriber', 'want', 'subscribed', undefined, undefined],
      ['roster/subscriber', 'want', 'first-object', 150, undefined],
      ['roster/subscriber', 'want', 'switched', 700, 'room/p2/video-sd'],
    ]);
    expect(diagnostics.snapshot()['roster/subscriber.wants']).toMatchObject({ 'p2/video': { firstObjectMs: 150 } });
  });

  it('says how far behind a want was when it caught up, from its arrival delay over the least', async () => {
    vi.useFakeTimers({ toFake: ['performance'] });
    const { diagnostics, subscriber, connections } = await start();
    subscriber.want('p2/audio', { namespace: ['room', 'p2'], track: 'audio', retry: fast });
    await until(() => connections[0]?.subscriptions.length === 1, 'subscribe');
    const subscription = connections[0]?.subscriptions[0];
    // The relay rescales LOC timestamps to microseconds and says so.
    subscription?.accept({ trackProperties: [{ type: 0x08, value: 1_000_000 }] });
    // 20 ms frames: the first three on time, then each 500 ms later than the one before, as a relay backlog grows.
    for (let i = 0; i < 6; i++) {
      const media = { groupId: i, objectId: 0, status: 'normal', payload: new Uint8Array(1) };
      subscription?.handlers.onObject?.({ ...media, properties: [{ type: 0x10, value: i * 20_000 }] } as MoqtObject);
      vi.advanceTimersByTime(i < 2 ? 20 : 520);
    }
    subscriber.catchUp('p2/audio');
    await until(() => diagnostics.events().some((event) => event['what'] === 'restarted'), 'catch-up');
    const restarted = diagnostics.events().find((event) => event['what'] === 'restarted');
    expect(restarted).toMatchObject({ reason: 'catch-up', behindMs: 1_500 });
  });

  it('counts a want’s reset groups, and those reset for a delivery timeout', async () => {
    const { diagnostics, subscriber, connections } = await start();
    subscriber.want('p2/audio', { namespace: ['room', 'p2'], track: 'audio', retry: fast });
    await until(() => connections[0]?.subscriptions.length === 1, 'subscribe');
    const subscription = connections[0]?.subscriptions[0];
    subscription?.accept();
    subscription?.object(0);
    subscription?.handlers.onSubgroupReset?.({ groupId: 1, error: { source: 'stream', streamErrorCode: 0x2 } });
    subscription?.handlers.onSubgroupReset?.({ groupId: 2, error: { source: 'stream', streamErrorCode: 0x1 } });
    subscription?.handlers.onSubgroupReset?.({ groupId: 3, error: new Error('no code') });
    expect(diagnostics.snapshot()['roster/subscriber.wants']).toMatchObject({
      'p2/audio': { groupResets: 3, deliveryTimeouts: 1 },
    });
  });

  it('refuses a diagnostics option that is not a recorder', async () => {
    await expect(
      superviseRelaySubscriber(async () => fakeSubscriber(), { url, diagnostics: {} as never }),
    ).rejects.toThrow(TypeError);
  });
});

describe('a supervised publisher with diagnostics', () => {
  it('reports going live with its carrier, and each generation it serves', async () => {
    const diagnostics = createDiagnostics();
    const connections: FakePublisher[] = [];
    const publisher = await superviseRelayPublisher(
      async (options: ConnectRelayPublisherOptions) => {
        const fake = fakePublisher([...options.namespace]);
        connections.push(fake);
        (options as { [reportEndpoint]?: (endpoint: string, framing: string) => void })[reportEndpoint]?.(
          url,
          'webtransport',
        );
        return fake;
      },
      { url, namespace: ['room', 'p1'], reconnect: fast, diagnostics },
    );
    cleanup.push(() => publisher.close());
    const generation = publisher.generation?.generation;
    expect(brief(diagnostics.events(), 'status', 'carrier', 'generation')).toEqual([
      ['publisher', 'link', 'live', 'webtransport', undefined],
      ['publisher', 'generation', undefined, undefined, generation],
    ]);
    expect(diagnostics.snapshot()['publisher.link']).toEqual({
      status: 'live',
      carrier: 'webtransport',
      relay: 'relay.example.com',
      generation,
      connects: 1,
    });
  });
});

describe('a supervised relay with diagnostics', () => {
  async function start() {
    const diagnostics = createDiagnostics();
    const connections: FakeSession[] = [];
    const relay = await superviseRelay(
      async (options) => {
        const session = fakeSession();
        connections.push(session);
        (options as { [reportEndpoint]?: (endpoint: string, framing: string) => void })[reportEndpoint]?.(
          url,
          'native-quic',
        );
        return session as unknown as RelaySession;
      },
      { url, reconnect: fast, diagnostics },
    );
    cleanup.push(() => relay.close());
    return { diagnostics, relay, connections };
  }
  const of = (events: DiagnosticEvent[], kind: string) => events.filter((event) => event.kind === kind);

  it('reports its link with the carrier, each generation, and a want’s life across a reconnect', async () => {
    const { diagnostics, relay, connections } = await start();
    relay.publish(['room', 'p1']);
    const seen: string[] = [];
    relay.want(
      'p2/audio',
      { namespace: ['room', 'p2'], track: 'audio', retry: fast },
      { onRestarted: ({ reason }) => seen.push(`restarted ${reason}`) },
    );
    await until(() => connections[0]?.subscriptions.length === 1, 'subscribe');
    connections[0]?.subscriptions[0]?.accept();
    connections[0]?.subscriptions[0]?.object(0);
    const first = relay.generation;
    const counts = { firstObjectMs: expect.any(Number), groupResets: 0, deliveryTimeouts: 0 };
    expect(diagnostics.snapshot()['relay.wants']).toEqual({
      'p2/audio': { track: 'audio', subscribed: true, objects: 1, restarts: 0, ...counts },
    });

    connections[0]?.end(new RelayError('TRANSPORT_ERROR', 'lost', 'connection'));
    await until(() => relay.status === 'live' && connections.length === 2, 'reconnect');
    await until(() => connections[1]?.subscriptions.length === 1, 'resubscribe');
    connections[1]?.subscriptions[0]?.accept();
    const second = relay.generation;
    expect(second).not.toEqual(first);
    expect(seen).toEqual(['restarted connection']);

    const events = diagnostics.events();
    expect(brief(of(events, 'link'), 'status', 'carrier', 'error')).toEqual([
      ['relay', 'link', 'live', 'native-quic', undefined],
      ['relay', 'link', 'reconnecting', undefined, 'TRANSPORT_ERROR (connection): lost'],
      ['relay', 'link', 'live', 'native-quic', undefined],
    ]);
    expect(brief(of(events, 'generation'), 'generation')).toEqual([
      ['relay', 'generation', first],
      ['relay', 'generation', second],
    ]);
    expect(brief(of(events, 'want'), 'want', 'what', 'reason', 'namespace')).toEqual([
      ['relay', 'want', 'p2/audio', 'subscribed', undefined, 'room/p2'],
      ['relay', 'want', 'p2/audio', 'first-object', undefined, undefined],
      ['relay', 'want', 'p2/audio', 'restarted', 'connection', undefined],
      ['relay', 'want', 'p2/audio', 'subscribed', undefined, 'room/p2'],
    ]);
    expect(diagnostics.snapshot()['relay.link']).toEqual({
      status: 'live',
      carrier: 'native-quic',
      relay: 'relay.example.com',
      generation: second,
      connects: 2,
    });
    expect(diagnostics.snapshot()['relay.wants']).toEqual({
      'p2/audio': { track: 'audio', subscribed: true, objects: 1, restarts: 1, lastRestart: 'connection', ...counts },
    });

    relay.unwant('p2/audio');
    expect(diagnostics.snapshot()['relay.wants']).toEqual({});
  });

  it('reports a generation for a rotation, and empties the wants section when it closes', async () => {
    const { diagnostics, relay, connections } = await start();
    relay.want('p2/audio', { namespace: ['room', 'p2'], track: 'audio', retry: fast });
    await until(() => connections[0]?.subscriptions.length === 1, 'subscribe');
    connections[0]?.subscriptions[0]?.accept();
    const first = relay.generation;
    relay.rotate();
    await until(() => relay.generation !== first, 'rotation');
    expect(brief(of(diagnostics.events(), 'generation'), 'generation')).toEqual([
      ['relay', 'generation', first],
      ['relay', 'generation', relay.generation],
    ]);
    await relay.close();
    expect(diagnostics.snapshot()['relay.wants']).toEqual({});
    expect(of(diagnostics.events(), 'link').at(-1)?.['status']).toBe('closed');
  });

  it('refuses a diagnostics option that is not a recorder', async () => {
    await expect(
      superviseRelay(async () => fakeSession() as unknown as RelaySession, { url, diagnostics: {} as never }),
    ).rejects.toThrow(TypeError);
  });
});

describe('a frame writer with diagnostics', () => {
  it('reports demand, objects, groups, backlog and each outcome until it closes', async () => {
    const diagnostics = createDiagnostics();
    const track = slotTrack();
    const writer = createFrameWriter(track.writer, { grouping: 'frame', diagnostics: diagnostics.scope('audio') });
    const frame = () => ({ payload: Uint8Array.of(1), key: true });
    await Promise.all([writer.write(frame()), writer.write(frame())]);
    track.demand(false);
    await writer.write(frame());
    await settle();
    expect(diagnostics.snapshot()['audio.send']).toEqual({
      demand: false,
      objects: 2,
      bytes: 2,
      groups: 2,
      backlog: 0,
      outcomes: { sent: 2, skipped: 1, stale: 0, dropped: 0, interrupted: 0, ended: 0 },
      slowWrites: 0,
      keyframeRequests: 0,
      inputGaps: 0,
      longestInputGapMs: 0,
    });
    await writer.close();
    expect(diagnostics.snapshot()).toEqual({});
  });

  it('reports a gap in the media timestamps handed to it, as the reader judges arrival gaps', async () => {
    const diagnostics = createDiagnostics();
    const track = slotTrack();
    const writer = createFrameWriter(track.writer, { grouping: 'frame', diagnostics: diagnostics.scope('audio') });
    const frame = (timestampUs?: number) => ({
      payload: Uint8Array.of(1, 2, 3),
      key: true,
      ...(timestampUs === undefined ? {} : { timestampUs }),
    });
    // 20 ms frames, then 800 ms of frames lost before the writer, then 20 ms frames again.
    for (let i = 0; i < 10; i++) await writer.write(frame(i * 20_000));
    await writer.write(frame());
    for (let i = 0; i < 5; i++) await writer.write(frame(980_000 + i * 20_000));
    // Without timestampUs, the LOC Timestamp in the frame's properties, here in milliseconds.
    const loc = (ms: number) => ({
      payload: Uint8Array.of(1),
      key: true,
      properties: [
        { type: 0x10, value: ms },
        { type: 0x08, value: 1_000 },
      ],
    });
    await writer.write(loc(1_060));
    await writer.write(loc(3_060));
    expect(brief(diagnostics.events(), 'ms')).toEqual([
      ['audio', 'input-gap', 800],
      ['audio', 'input-gap', 2_000],
    ]);
    expect(diagnostics.snapshot()['audio.send']).toMatchObject({
      objects: 18,
      bytes: 50,
      inputGaps: 2,
      longestInputGapMs: 2_000,
    });
    await writer.close();
  });

  it('counts each time it asks for a keyframe after its start', async () => {
    const diagnostics = createDiagnostics();
    const track = slotTrack();
    let asked = 0;
    const writer = createFrameWriter(track.writer, { diagnostics, onKeyframeNeeded: () => asked++ });
    await writer.write({ payload: Uint8Array.of(1), key: true });
    // A replacement relay subscription ends the open group: the new one needs a keyframe.
    track.demand(true);
    await settle();
    await writer.write({ payload: Uint8Array.of(1), key: true });
    track.demand(true);
    await settle();
    expect(asked).toBe(2);
    expect(diagnostics.snapshot()['send']).toMatchObject({ keyframeRequests: 2 });
    await writer.close();
  });
});

describe('a frame reader with diagnostics', () => {
  /** A sequencer that delivers nothing and keeps one counter. */
  const sequencer = () => ({
    trackProperties: [],
    stats: { delivered: 0, late: 3 },
    object() {},
    groupEnded() {},
    groupReset() {},
    restart() {},
    poll: () => ({ frames: [], waitUs: undefined }),
  });
  const object = (groupId: number) => ({ groupId, objectId: 0, payload: new Uint8Array() }) as unknown as MoqtObject;

  it('counts objects, reports a gap against the usual interval, and shows the sequencer’s stats', () => {
    vi.useFakeTimers({ toFake: ['performance'] });
    const diagnostics = createDiagnostics();
    const reader = createFrameReader(sequencer(), { onFrame() {}, diagnostics: diagnostics.scope('p2/audio') });
    for (let group = 0; group < 20; group++) {
      reader.onObject(object(group));
      vi.advanceTimersByTime(20);
    }
    vi.advanceTimersByTime(400);
    reader.onObject(object(20));
    // A replaced subscription's silence is not a gap.
    reader.onRestarted();
    vi.advanceTimersByTime(2_000);
    reader.onObject(object(0));
    expect(brief(diagnostics.events(), 'ms')).toEqual([['p2/audio', 'arrival-gap', 420]]);
    expect(diagnostics.snapshot()['p2/audio.receive']).toEqual({
      objects: 22,
      bytes: 0,
      arrivalGaps: 1,
      longestGapMs: 420,
      reordered: 0,
      longestReorderWaitMs: 0,
      sequencer: { delivered: 0, late: 3 },
    });
    reader.close();
    expect(diagnostics.snapshot()).toEqual({});
  });

  it('learns a cadence slower than the 250 ms floor before judging, then still catches a stall', () => {
    vi.useFakeTimers({ toFake: ['performance'] });
    const diagnostics = createDiagnostics();
    const reader = createFrameReader(sequencer(), { onFrame() {}, diagnostics });
    for (let group = 0; group < 10; group++) {
      reader.onObject(object(group));
      vi.advanceTimersByTime(500);
    }
    expect(diagnostics.events()).toEqual([]);
    vi.advanceTimersByTime(4_500);
    reader.onObject(object(10));
    expect(brief(diagnostics.events(), 'ms')).toEqual([[undefined, 'arrival-gap', 5_000]]);
  });

  it('allows a slow track its own interval: eight of them before a gap', () => {
    vi.useFakeTimers({ toFake: ['performance'] });
    const diagnostics = createDiagnostics();
    const reader = createFrameReader(sequencer(), { onFrame() {}, diagnostics });
    for (let group = 0; group < 30; group++) {
      reader.onObject(object(group));
      vi.advanceTimersByTime(200);
    }
    vi.advanceTimersByTime(1_000);
    reader.onObject(object(30));
    expect(diagnostics.events()).toEqual([]);
  });
});

describe('a frame reader’s reorder and delay spread', () => {
  const sequencer = () => ({
    trackProperties: [],
    object() {},
    groupEnded() {},
    groupReset() {},
    restart() {},
    poll: () => ({ frames: [], waitUs: undefined }),
  });
  /** An object of `bytes` bytes with a LOC timestamp in microseconds. */
  const timed = (groupId: number, timestampUs: number, bytes = 10) =>
    ({
      groupId,
      objectId: 0,
      payload: new Uint8Array(bytes),
      properties: [{ type: 0x10, value: timestampUs }],
    }) as unknown as MoqtObject;

  it('counts objects that arrive after a later one, and how long the longest waited behind it', () => {
    vi.useFakeTimers({ toFake: ['performance'] });
    const diagnostics = createDiagnostics();
    const reader = createFrameReader(sequencer(), { onFrame() {}, diagnostics });
    for (const group of [0, 1, 3, 4]) {
      reader.onObject(timed(group, group * 20_000));
      vi.advanceTimersByTime(20);
    }
    // Group 2 arrives 40 ms after group 3 overtook it.
    reader.onObject(timed(2, 40_000));
    expect(diagnostics.snapshot()['receive']).toMatchObject({
      objects: 5,
      bytes: 50,
      reordered: 1,
      longestReorderWaitMs: 40,
    });
    // A new subscription numbers its groups again: group 0 there is not late.
    reader.onRestarted();
    reader.onObject(timed(0, 0));
    expect(diagnostics.snapshot()['receive']).toMatchObject({ reordered: 1 });
  });

  it('reports the spread of arrival delays over the least, over the last five seconds', () => {
    vi.useFakeTimers({ toFake: ['performance'] });
    const diagnostics = createDiagnostics();
    const reader = createFrameReader(sequencer(), { onFrame() {}, diagnostics });
    // A clock 10 s ahead of the publisher's: only the spread counts. Eighteen on time, then 80 and 120 ms late.
    const start = performance.now();
    const late = [0, 80, 120];
    for (let i = 0; i < 20; i++) {
      const arrival = start + 10_000 + i * 20 + (late[i - 17] ?? 0);
      vi.advanceTimersByTime(arrival - performance.now());
      reader.onObject(timed(i, i * 20_000));
    }
    expect(diagnostics.snapshot()['receive']).toMatchObject({ delaySpreadMs: { p50: 0, p95: 80, max: 120 } });
    // Five seconds later the window is empty.
    vi.advanceTimersByTime(5_100);
    expect(diagnostics.snapshot()['receive']).not.toHaveProperty('delaySpreadMs');
  });
});

describe('endpointHost', () => {
  it('keeps the host and port, never the path or a token in the query', () => {
    expect(endpointHost('https://oci1.ord.relay.mux.global:4443/room?jwt=abc.def.ghi')).toBe(
      'oci1.ord.relay.mux.global:4443',
    );
    expect(endpointHost('not a url')).toBeUndefined();
  });
});

describe('watchAudioInput', () => {
  it('reports an episode of filled audio once received audio has played for 300 ms', () => {
    vi.useFakeTimers({ now: 0, toFake: ['setInterval', 'clearInterval', 'performance', 'Date'] });
    const diagnostics = createDiagnostics();
    const stats = { concealedUs: 0, synthesizedUs: 0, underruns: 0, late: 0, decoded: 10 };
    const stop = watchAudioInput(diagnostics.scope('p2/audio'), { stats });
    vi.advanceTimersByTime(200);
    for (let tick = 0; tick < 4; tick++) {
      stats.synthesizedUs += 80_000;
      stats.concealedUs += 20_000;
      vi.advanceTimersByTime(100);
    }
    stats.underruns++;
    vi.advanceTimersByTime(500);
    stats.synthesizedUs += 70_000;
    vi.advanceTimersByTime(500);
    stats.synthesizedUs += 20_000;
    vi.advanceTimersByTime(500);
    expect(brief(diagnostics.events(), 'ms', 'concealedMs', 'synthesizedMs', 'underruns')).toEqual([
      ['p2/audio', 'audio-dropout', 400, 80, 320, 1],
      ['p2/audio', 'audio-conceal', 100, 0, 70, 0],
    ]);
    expect(diagnostics.snapshot()['p2/audio.audio']).toEqual(stats);
    stop();
    expect(diagnostics.snapshot()).toEqual({});
  });
});

describe('describeFailure', () => {
  it('names a RelayError by code and phase, any other error by name, and redacts tokens', () => {
    expect(describeFailure(new RelayError('UNAUTHORIZED', 'denied', 'connecting'))).toBe(
      'UNAUTHORIZED (connecting): denied',
    );
    expect(describeFailure(new TypeError('dial wss://r/x?jwt=secret&a=1 with eyJhbGci.eyJzdWIi.c2ln'))).toBe(
      'TypeError: dial wss://r/x?jwt=[redacted]&a=1 with [jwt]',
    );
    expect(describeFailure('x'.repeat(400))).toHaveLength(300);
  });
});
