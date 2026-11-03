import type { MoqtObject } from '@caprid/moq-core';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { reportEndpoint } from '../src/connect-session.js';
import { createDiagnostics, type DiagnosticEvent, watchAudioInput } from '../src/diagnostics.js';
import { describeFailure } from '../src/diagnostics-report.js';
import { RelayError } from '../src/errors.js';
import { createFrameReader, createFrameWriter } from '../src/media.js';
import type { ConnectRelayPublisherOptions } from '../src/publish.js';
import { superviseRelayPublisher } from '../src/supervised-publisher.js';
import { superviseRelaySubscriber, type WantHandlers } from '../src/supervised-subscriber.js';
import { settle, slotTrack } from './helpers/slot-track.js';
import {
  type FakePublisher,
  type FakeSubscriber,
  fakePublisher,
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
      connects: 2,
    });
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
      'p2/video': { track: 'video', subscribed: true, objects: 1, restarts: 0 },
    });
    subscriber.unwant('p2/video');
    expect(diagnostics.snapshot()['roster/subscriber.wants']).toEqual({});
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
      generation,
      connects: 1,
    });
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
      groups: 2,
      backlog: 0,
      outcomes: { sent: 2, skipped: 1, stale: 0, dropped: 0, interrupted: 0, ended: 0 },
      slowWrites: 0,
    });
    await writer.close();
    expect(diagnostics.snapshot()).toEqual({});
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
      arrivalGaps: 1,
      longestGapMs: 420,
      sequencer: { delivered: 0, late: 3 },
    });
    reader.close();
    expect(diagnostics.snapshot()).toEqual({});
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
