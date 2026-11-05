/**
 * The default transport factory, `webTransport()`, as a caller wraps it: a wrapper that keeps every transport a
 * connection opens and closes them on demand, the "drop the connection" hook for recovery tests (#445). The runtime's
 * `WebTransport` is a stub whose `new` returns an in-memory hub transport, so the SDK drives it as the browser one.
 */
import type { MoqtObject } from '@caprid/moq-core';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  connectRelaySubscriber,
  connectSupervisedPublisher,
  connectSupervisedSubscriber,
  type MoqtTransport,
  type RelayTransportFactory,
  webTransport,
} from '../src/index.js';
import { connectRelayPublisher } from '../src/publish.js';
import type { SupervisedStatus } from '../src/supervision.js';
import { createRelayHub, type RelayHub } from './helpers/relay-hub.js';

const fast = { initialDelayMs: 0, maxDelayMs: 0 };
const cleanup: (() => unknown)[] = [];

afterEach(async () => {
  for (const dispose of cleanup.splice(0).reverse()) await dispose();
  vi.unstubAllGlobals();
});

/** A `WebTransport` global that dials the hub: `/publish` as the publisher's relay end, anything else as a subscriber's. */
function stubWebTransport(hub: RelayHub) {
  // biome-ignore lint/complexity/useArrowFunction: an arrow function cannot be called with new.
  const StubWebTransport = vi.fn(function (url: string, options: { protocols: string[] }) {
    const dial = new URL(url).pathname === '/publish' ? hub.connectPublisher : hub.connectSubscriber;
    const { transport, ready } = dial(url, options.protocols);
    return Object.assign(transport, { ready });
  });
  Object.defineProperty(StubWebTransport.prototype, 'protocol', { get: () => '' });
  vi.stubGlobal('WebTransport', StubWebTransport);
  return StubWebTransport;
}

/** The recipe in GUIDE.md: wrap the default factory, keep the open transports, close them on demand. */
function droppable() {
  const dial = webTransport();
  const open = new Set<MoqtTransport>();
  const createTransport: RelayTransportFactory = (url, protocols) => {
    const connection = dial(url, protocols);
    open.add(connection.transport);
    connection.transport.closed.then(
      () => open.delete(connection.transport),
      () => open.delete(connection.transport),
    );
    return connection;
  };
  const dropAll = () => {
    for (const transport of open) transport.close({ closeCode: 0, reason: 'chaos: dropped' });
  };
  return { createTransport, dropAll, open };
}

function hub() {
  const relay = createRelayHub();
  cleanup.push(() => relay.destroy());
  return relay;
}

describe('webTransport', () => {
  it('takes a DOM WebTransport as the factory transport, with no cast', () => {
    const factory: RelayTransportFactory = (url, protocols) => {
      const transport = new WebTransport(url, { protocols });
      return { transport, ready: transport.ready, framing: 'webtransport' };
    };
    expect(factory).toBeTypeOf('function');
  });

  it('is the factory a connection uses when createTransport is left out', async () => {
    const relay = hub();
    const StubWebTransport = stubWebTransport(relay);
    const url = 'https://relay.example.com/moq';

    const defaulted = await connectRelaySubscriber({ url });
    cleanup.push(() => defaulted.close());
    const wrapped = await connectRelaySubscriber({ url, createTransport: webTransport() });
    cleanup.push(() => wrapped.close());

    expect(StubWebTransport.mock.calls).toEqual([
      [url, { protocols: ['moqt-22', 'moqt-20'] }],
      [url, { protocols: ['moqt-22', 'moqt-20'] }],
    ]);
  });

  it.each([
    ['has no WebTransport', undefined, /WebTransport is unavailable/],
    ['has a WebTransport that cannot negotiate protocols', class {}, /cannot negotiate protocols/],
  ])('fails with TRANSPORT_UNAVAILABLE in a runtime that %s', (_runtime, global, message) => {
    vi.stubGlobal('WebTransport', global);
    const dial = webTransport();

    expect(() => dial('https://relay.example.com/moq', ['moqt-22'])).toThrow(
      expect.objectContaining({
        code: 'TRANSPORT_UNAVAILABLE',
        phase: 'connecting',
        message: expect.stringMatching(message),
      }),
    );
  });
});

describe('dropping the transport under a supervised connection', () => {
  it('reconnects a supervised publisher, whose writer then reaches the relay again', async () => {
    const relay = hub();
    stubWebTransport(relay);
    const { createTransport, dropAll, open } = droppable();
    const statuses: SupervisedStatus[] = [];
    relay.subscribeUpstream('events');

    const publisher = await connectSupervisedPublisher({
      url: 'https://relay.example.com/publish',
      namespace: ['room', 'alice'],
      createTransport,
      reconnect: fast,
      onStatus: (status) => statuses.push(status),
    });
    cleanup.push(() => publisher.close());
    const events = publisher.registerTrack({ track: 'events' });
    await vi.waitFor(() => expect(relay.subscribes).toContain('events'));
    await events.write(new Uint8Array([1]));
    await vi.waitFor(() => expect(relay.objectCount('events')).toBe(1));

    dropAll();

    await vi.waitFor(() => expect(statuses).toEqual(['live', 'reconnecting', 'live']));
    expect(relay.publisherConnections()).toBe(2);
    expect(open.size).toBe(1);
    await vi.waitFor(() => expect(relay.subscribes.filter((track) => track === 'events')).toHaveLength(2));
    await events.write(new Uint8Array([2]));
    await vi.waitFor(() => expect(relay.objectCount('events')).toBe(2));
  });

  it('reconnects a supervised subscriber, whose want then delivers again', async () => {
    const relay = hub();
    stubWebTransport(relay);
    const { createTransport, dropAll, open } = droppable();
    const statuses: SupervisedStatus[] = [];
    const namespace = ['room', 'alice'];
    const publisher = await connectRelayPublisher({
      url: 'https://relay.example.com/moq',
      namespace,
      createTransport: relay.connectPublisher,
    });
    cleanup.push(() => publisher.close());
    const events = publisher.registerTrack({ track: 'events' });

    const subscriber = await connectSupervisedSubscriber({
      url: 'https://relay.example.com/moq',
      createTransport,
      reconnect: fast,
      onStatus: (status) => statuses.push(status),
    });
    cleanup.push(() => subscriber.close());
    const objects: MoqtObject[] = [];
    const onSubscribed = vi.fn();
    subscriber.want(
      'events',
      { namespace, track: 'events', retry: fast },
      { onSubscribed, onObject: (object) => objects.push(object) },
    );
    await vi.waitFor(() => expect(onSubscribed).toHaveBeenCalledOnce());
    await events.write(new Uint8Array([1]));
    await vi.waitFor(() => expect(objects.map((object) => [...object.payload])).toEqual([[1]]));

    dropAll();

    await vi.waitFor(() => expect(statuses).toEqual(['live', 'reconnecting', 'live']));
    expect(open.size).toBe(1);
    await vi.waitFor(() => expect(onSubscribed).toHaveBeenCalledTimes(2));
    await events.write(new Uint8Array([2]));
    await vi.waitFor(() => expect(objects.map((object) => [...object.payload]).at(-1)).toEqual([2]));
  });
});
