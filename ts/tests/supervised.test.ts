import { afterEach, describe, expect, it } from 'vitest';
import { RelayError } from '../src/errors.js';
import type { ConnectRelayPublisherOptions } from '../src/publish.js';
import { SubscriptionConnectionClosedError } from '../src/subscription.js';
import { type SupervisedPublisher, superviseRelayPublisher } from '../src/supervised-publisher.js';
import {
  type SupervisedSubscriber,
  superviseRelaySubscriber,
  type WantHandlers,
} from '../src/supervised-subscriber.js';
import { backoffDelay, newGeneration } from '../src/supervision.js';
import {
  deferred,
  type FakePublisher,
  type FakeSubscriber,
  fakePublisher,
  fakeSubscriber,
  until,
} from './helpers/supervision-fakes.js';

const fast = { initialDelayMs: 0, maxDelayMs: 0 };
const cleanup: (() => unknown)[] = [];
afterEach(async () => {
  for (const close of cleanup.splice(0)) await close();
});

const UNROUTABLE = { errorCode: 0x10, retryInterval: 0, reason: 'unroutable' };

describe('supervision helpers', () => {
  it('names generations so they sort by start time', () => {
    const older = newGeneration(1_000);
    const newer = newGeneration(1_001);
    expect(older).toHaveLength(13);
    expect(newer > older).toBe(true);
    expect(newGeneration(Date.parse('2026-10-06')) > newGeneration(Date.parse('2026-10-05'))).toBe(true);
  });

  it('doubles the delay up to the cap, drawn from its upper half', () => {
    const backoff = { initialDelayMs: 100, maxDelayMs: 400 };
    for (const [attempt, ceiling] of [
      [0, 100],
      [1, 200],
      [2, 400],
      [5, 400],
    ] as const) {
      for (let i = 0; i < 20; i++) {
        const delay = backoffDelay(attempt, backoff);
        expect(delay).toBeGreaterThanOrEqual(ceiling / 2);
        expect(delay).toBeLessThanOrEqual(ceiling);
      }
    }
  });
});

describe('connectSupervisedPublisher', () => {
  async function start(options: { connect?: (options: ConnectRelayPublisherOptions) => Promise<FakePublisher> } = {}) {
    const connections: FakePublisher[] = [];
    const statuses: [string, unknown][] = [];
    const generations: string[][] = [];
    const connect =
      options.connect ??
      (async (connection: ConnectRelayPublisherOptions) => {
        const publisher = fakePublisher([...connection.namespace]);
        connections.push(publisher);
        return publisher;
      });
    const publisher = await superviseRelayPublisher(connect, {
      url: 'https://relay.example.com/moq',
      namespace: ['room', 'alice'],
      reconnect: fast,
      onStatus: (status, error) => statuses.push([status, error]),
      onGeneration: ({ namespace }) => generations.push(namespace),
    });
    cleanup.push(() => publisher.close());
    return { publisher, connections, statuses, generations };
  }

  it('serves the root plus a generation field and reports it', async () => {
    const { publisher, connections, generations } = await start();
    expect(connections).toHaveLength(1);
    const [root0, root1, generation] = connections[0]!.namespace;
    expect([root0, root1]).toEqual(['room', 'alice']);
    expect(publisher.generation).toEqual({ generation, namespace: ['room', 'alice', generation] });
    expect(generations).toEqual([['room', 'alice', generation]]);
    expect(publisher.status).toBe('live');
  });

  it('reconnects as a newer generation and rebinds the same writer to it', async () => {
    const { publisher, connections, statuses, generations } = await start();
    const writer = publisher.registerTrack({ track: 'video' });
    await writer.write(new Uint8Array([1]));
    expect(connections[0]!.tracks.get('video')!.writes).toEqual([new Uint8Array([1])]);

    const lost = new RelayError('TRANSPORT_ERROR', 'gone', 'connection');
    connections[0]!.end(lost);
    await until(() => connections.length === 2 && publisher.status === 'live', 'second generation');
    expect(statuses).toContainEqual(['reconnecting', lost]);
    expect(generations).toHaveLength(2);
    expect(generations[1]![2]! > generations[0]![2]!).toBe(true);
    await writer.write(new Uint8Array([2]));
    expect(connections[1]!.tracks.get('video')!.writes).toEqual([new Uint8Array([2])]);
    expect(writer.closed).toBe(false);
  });

  it('fails a write between generations at once with RECONNECTING and replays nothing', async () => {
    const gate = deferred();
    let calls = 0;
    const connections: FakePublisher[] = [];
    const { publisher } = await start({
      connect: async (options) => {
        calls += 1;
        if (calls === 2) await gate.promise;
        const fake = fakePublisher([...options.namespace]);
        connections.push(fake);
        return fake;
      },
    });
    const writer = publisher.registerTrack({ track: 'audio' });
    connections[0]!.end(new Error('lost'));
    await until(() => publisher.status === 'reconnecting', 'reconnecting');
    await expect(writer.write(new Uint8Array([9]))).rejects.toMatchObject({
      code: 'RECONNECTING',
      phase: 'connection',
    });
    expect(writer.hasDemand).toBe(false);
    gate.resolve();
    await until(() => publisher.status === 'live', 'live again');
    expect(connections[1]!.tracks.get('audio')!.writes).toEqual([]);
  });

  it('reports an operation the connection end interrupted as RECONNECTING, not CLOSED', async () => {
    const { publisher, connections } = await start();
    const writer = publisher.registerTrack({ track: 'video' });
    connections[0]!.tracks.get('video')!.failNext = new RelayError('WRITE_TIMEOUT', 'slow', 'waiting-for-demand');
    await expect(writer.write(new Uint8Array([1]))).rejects.toMatchObject({ code: 'WRITE_TIMEOUT' });
    // The write fails a moment before the connection reports its end, as it does on a real transport loss.
    connections[0]!.tracks.get('video')!.failNext = new RelayError('CLOSED', 'Published track is closed', 'writing');
    const failing = writer.write(new Uint8Array([1]));
    setTimeout(() => connections[0]!.end(new Error('lost')), 20);
    await expect(failing).rejects.toMatchObject({ code: 'RECONNECTING' });
  });

  it('reports demand across generations: false when one is lost, then the next one', async () => {
    const { publisher, connections } = await start();
    const writer = publisher.registerTrack({ track: 'video' });
    const changes = writer.demandChanges();
    expect((await changes.next()).value).toBe(false);
    connections[0]!.tracks.get('video')!.setDemand(true);
    expect((await changes.next()).value).toBe(true);
    connections[0]!.end(new Error('lost'));
    expect((await changes.next()).value).toBe(false);
    await until(() => connections.length === 2 && publisher.status === 'live', 'second generation');
    connections[1]!.tracks.get('video')!.setDemand(true);
    expect((await changes.next()).value).toBe(true);
    writer.close();
    expect((await changes.next()).done).toBe(true);
  });

  it('keeps track names unique, never registers a closed track again, and checks options at once', async () => {
    const { publisher, connections } = await start();
    const writer = publisher.registerTrack({ track: 'a' });
    expect(() => publisher.registerTrack({ track: 'a' })).toThrow(RangeError);
    expect(() => publisher.registerTrack({ track: '' })).toThrow();
    writer.close();
    expect(() => publisher.registerTrack({ track: 'a' })).toThrow(RangeError);
    await expect(writer.write(new Uint8Array([1]))).rejects.toMatchObject({ code: 'CLOSED' });
    connections[0]!.end(new Error('lost'));
    await until(() => connections.length === 2 && publisher.status === 'live', 'second generation');
    expect(connections[1]!.tracks.has('a')).toBe(false);
  });

  it('registers a track added between generations on the next one', async () => {
    const gate = deferred();
    let calls = 0;
    const connections: FakePublisher[] = [];
    const { publisher } = await start({
      connect: async (options) => {
        calls += 1;
        if (calls === 2) await gate.promise;
        const fake = fakePublisher([...options.namespace]);
        connections.push(fake);
        return fake;
      },
    });
    connections[0]!.end(new Error('lost'));
    await until(() => publisher.status === 'reconnecting', 'reconnecting');
    publisher.registerTrack({ track: 'late' });
    gate.resolve();
    await until(() => publisher.status === 'live', 'live again');
    expect(connections[1]!.tracks.has('late')).toBe(true);
  });

  it('rejects when the first attempt is refused for its credentials, and retries other failures', async () => {
    const refused = new RelayError('UNAUTHORIZED', 'no', 'connecting');
    await expect(
      superviseRelayPublisher(
        async () => {
          throw refused;
        },
        { url: 'https://relay.example.com/moq', namespace: ['room'], reconnect: fast },
      ),
    ).rejects.toBe(refused);

    let calls = 0;
    const publisher = await superviseRelayPublisher(
      async (options) => {
        calls += 1;
        if (calls < 3) throw new RelayError('TRANSPORT_ERROR', 'down', 'connecting');
        return fakePublisher([...options.namespace]);
      },
      { url: 'https://relay.example.com/moq', namespace: ['room'], reconnect: fast },
    );
    cleanup.push(() => publisher.close());
    expect(calls).toBe(3);
  });

  it('stops on close: closes the live generation, ends the writers, resolves closed', async () => {
    const { publisher, connections, statuses } = await start();
    const writer = publisher.registerTrack({ track: 'video' });
    await publisher.close();
    expect(connections[0]!.closeCalls).toBe(1);
    expect(writer.closed).toBe(true);
    expect(publisher.status).toBe('closed');
    expect(publisher.generation).toBeUndefined();
    await expect(publisher.closed).resolves.toEqual({});
    expect(statuses.at(-1)).toEqual(['closed', undefined]);
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(connections).toHaveLength(1);
    expect(() => publisher.registerTrack({ track: 'more' })).toThrow(RelayError);
  });

  it('stops when its signal aborts, before or after the first generation', async () => {
    const owner = new AbortController();
    const reason = new Error('owner done');
    const connections: FakePublisher[] = [];
    const publisher: SupervisedPublisher = await superviseRelayPublisher(
      async (options) => {
        const fake = fakePublisher([...options.namespace]);
        connections.push(fake);
        return fake;
      },
      { url: 'https://relay.example.com/moq', namespace: ['room'], reconnect: fast, signal: owner.signal },
    );
    owner.abort(reason);
    await expect(publisher.closed).resolves.toEqual({ error: reason });
    expect(connections[0]!.closeCalls).toBe(1);

    const never = new AbortController();
    const pending = superviseRelayPublisher(() => new Promise(() => {}), {
      url: 'https://relay.example.com/moq',
      namespace: ['room'],
      signal: never.signal,
    });
    never.abort(reason);
    await expect(pending).rejects.toBe(reason);
  });
});

describe('connectSupervisedSubscriber', () => {
  async function start() {
    const connections: FakeSubscriber[] = [];
    const statuses: string[] = [];
    const subscriber = await superviseRelaySubscriber(
      async () => {
        const fake = fakeSubscriber();
        connections.push(fake);
        return fake;
      },
      { url: 'https://relay.example.com/moq', reconnect: fast, onStatus: (status) => statuses.push(status) },
    );
    cleanup.push(() => subscriber.close());
    return { subscriber, connections, statuses };
  }

  function recorder() {
    const seen: string[] = [];
    const handlers: WantHandlers = {
      onSubscribed: (_, { namespace }) => seen.push(`subscribed ${namespace.join('/')}`),
      onObject: (object) => seen.push(`object ${object.groupId}`),
      onPublisherDone: () => seen.push('done'),
      onRestarted: ({ reason }) => seen.push(`restarted ${reason}`),
      onError: (error) => seen.push(`error ${(error as RelayError).code}`),
    };
    return { seen, handlers };
  }

  const latest = (connection: FakeSubscriber) => connection.subscriptions.at(-1)!;

  it('retries the announce race without reporting it, then delivers', async () => {
    const { subscriber, connections } = await start();
    const { seen, handlers } = recorder();
    subscriber.want('video', { namespace: ['room'], track: 'video', retry: fast }, handlers);
    await until(() => connections[0]!.subscriptions.length === 1, 'first subscribe');
    latest(connections[0]!).reject(UNROUTABLE);
    await until(() => connections[0]!.subscriptions.length === 2, 'retry');
    latest(connections[0]!).accept();
    latest(connections[0]!).object(0);
    expect(seen).toEqual(['subscribed room', 'object 0']);
  });

  it('resubscribes after a publisher done and tells the application to restart', async () => {
    const { subscriber, connections } = await start();
    const { seen, handlers } = recorder();
    subscriber.want('video', { namespace: ['room'], track: 'video', retry: fast }, handlers);
    await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
    const first = latest(connections[0]!);
    first.accept();
    first.object(3);
    first.done();
    await until(() => connections[0]!.subscriptions.length === 2, 'resubscribe');
    expect(first.cancelled).toBe(true);
    latest(connections[0]!).accept();
    latest(connections[0]!).object(4);
    expect(seen).toEqual([
      'subscribed room',
      'object 3',
      'done',
      'restarted publisher-done',
      'subscribed room',
      'object 4',
    ]);
  });

  it('resubscribes a subscription that went stale', async () => {
    const { subscriber, connections } = await start();
    const { seen, handlers } = recorder();
    subscriber.want('video', { namespace: ['room'], track: 'video', staleAfterMs: 30, retry: fast }, handlers);
    await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
    latest(connections[0]!).accept();
    latest(connections[0]!).object(7); // the dead publisher's cached object, then silence
    await until(() => connections[0]!.subscriptions.length === 2, 'stale resubscribe');
    expect(seen).toContain('restarted stale');
  });

  it('moves every want to the replacement connection', async () => {
    const { subscriber, connections, statuses } = await start();
    const { seen, handlers } = recorder();
    subscriber.want('video', { namespace: ['room'], track: 'video', retry: fast }, handlers);
    await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
    latest(connections[0]!).accept();
    connections[0]!.end(new RelayError('TRANSPORT_ERROR', 'idle', 'connection'));
    await until(() => connections[1]?.subscriptions.length === 1, 'resubscribe on the new connection');
    latest(connections[1]!).accept();
    expect(seen).toEqual(['subscribed room', 'restarted connection', 'subscribed room']);
    expect(statuses).toEqual(['live', 'reconnecting', 'live']);
  });

  it('surfaces a rejection no wait fixes and forgets the want', async () => {
    const { subscriber, connections } = await start();
    const { seen, handlers } = recorder();
    subscriber.want('video', { namespace: ['room'], track: 'video', retry: fast }, handlers);
    await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
    latest(connections[0]!).reject({ errorCode: 0x1, retryInterval: 0, reason: 'unauthorized' });
    await until(() => seen.length === 1, 'error');
    expect(seen).toEqual(['error SUBSCRIPTION_REQUEST_FAILED']);
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(connections[0]!.subscriptions).toHaveLength(1);
  });

  it('retries other rejections with backoff instead', async () => {
    const { subscriber, connections } = await start();
    const { seen, handlers } = recorder();
    subscriber.want('video', { namespace: ['room'], track: 'video', retry: fast }, handlers);
    await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
    latest(connections[0]!).reject({ errorCode: 0x10, retryInterval: 0, reason: 'not found' });
    await until(() => connections[0]!.subscriptions.length === 2, 'retry');
    expect(seen).toEqual([]);
  });

  it('follows the newest generation under a root and moves to a newer one', async () => {
    const { subscriber, connections } = await start();
    const { seen, handlers } = recorder();
    subscriber.want(
      'video',
      { namespace: ['room', 'alice'], track: 'video', generations: true, retry: fast },
      handlers,
    );
    await until(() => connections[0]!.discoveries.length === 1, 'discovery');
    const discovery = connections[0]!.discoveries[0]!;
    expect(discovery.prefix).toEqual(['room', 'alice']);
    const old = newGeneration(1_000);
    const replacement = newGeneration(2_000);
    // A deeper namespace under the root is not a generation.
    discovery.push({ type: 'announced', namespace: ['room', 'alice', 'x', 'y'] });
    discovery.push({ type: 'announced', namespace: ['room', 'alice', old] });
    await until(() => connections[0]!.subscriptions.length === 1, 'subscribe to the old generation');
    expect(latest(connections[0]!).options.namespace).toEqual(['room', 'alice', old]);
    latest(connections[0]!).accept();
    latest(connections[0]!).object(1);

    // The crashed generation is still announced when the replacement arrives: the newer one wins at once.
    discovery.push({ type: 'announced', namespace: ['room', 'alice', replacement] });
    await until(() => connections[0]!.subscriptions.length === 2, 'subscribe to the replacement');
    expect(latest(connections[0]!).options.namespace).toEqual(['room', 'alice', replacement]);
    latest(connections[0]!).accept();
    expect(seen).toEqual([
      `subscribed room/alice/${old}`,
      'object 1',
      'restarted generation',
      `subscribed room/alice/${replacement}`,
    ]);

    // Its withdrawal does not move the want back: the old one is gone too.
    discovery.push({ type: 'unannounced', namespace: ['room', 'alice', old] });
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(connections[0]!.subscriptions).toHaveLength(2);
  });

  it('waits for a generation when none is announced', async () => {
    const { subscriber, connections } = await start();
    subscriber.want('video', { namespace: ['room'], track: 'video', generations: true, retry: fast });
    await until(() => connections[0]!.discoveries.length === 1, 'discovery');
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(connections[0]!.subscriptions).toHaveLength(0);
    connections[0]!.discoveries[0]!.push({ type: 'announced', namespace: ['room', newGeneration()] });
    await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
  });

  it('shares one discovery among the wants on a root', async () => {
    const { subscriber, connections } = await start();
    subscriber.want('video', { namespace: ['room'], track: 'video', generations: true });
    subscriber.want('audio', { namespace: ['room'], track: 'audio', generations: true });
    await until(() => connections[0]!.discoveries.length === 1, 'discovery');
    connections[0]!.discoveries[0]!.push({ type: 'announced', namespace: ['room', newGeneration()] });
    await until(() => connections[0]!.subscriptions.length === 2, 'both subscribed');
    expect(connections[0]!.discoveries).toHaveLength(1);
  });

  it('swaps handlers for the same want and replaces a different one', async () => {
    const { subscriber, connections } = await start();
    const first = recorder();
    const second = recorder();
    subscriber.want('video', { namespace: ['room'], track: 'video' }, first.handlers);
    await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
    latest(connections[0]!).accept();
    subscriber.want('video', { namespace: ['room'], track: 'video' }, second.handlers);
    latest(connections[0]!).object(1);
    expect(first.seen).toEqual(['subscribed room']);
    expect(second.seen).toEqual(['object 1']);
    expect(connections[0]!.subscriptions).toHaveLength(1);

    subscriber.want('video', { namespace: ['room'], track: 'screen' }, second.handlers);
    await until(() => connections[0]!.subscriptions.length === 2, 'replacement want');
    expect(connections[0]!.subscriptions[0]!.cancelled).toBe(true);
    expect(latest(connections[0]!).options.track).toBe('screen');
  });

  it('unwant cancels; close cancels everything and stops reconnecting', async () => {
    const { subscriber, connections } = await start();
    subscriber.want('a', { namespace: ['room'], track: 'a' });
    subscriber.want('b', { namespace: ['room'], track: 'b' });
    await until(() => connections[0]!.subscriptions.length === 2, 'subscribe');
    subscriber.unwant('a');
    await until(() => connections[0]!.subscriptions[0]!.cancelled, 'a cancelled');
    subscriber.unwant('missing');
    subscriber.close();
    await expect(subscriber.closed).resolves.toEqual({});
    expect(subscriber.status).toBe('closed');
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(connections).toHaveLength(1);
    expect(() => subscriber.want('c', { namespace: ['room'], track: 'c' })).toThrow(RelayError);
  });

  it('checks a want at once', async () => {
    const { subscriber } = await start();
    const s: SupervisedSubscriber = subscriber;
    expect(() => s.want('k', { namespace: [], track: 'v' })).toThrow();
    expect(() => s.want('k', { namespace: ['room'], track: '' })).toThrow();
    expect(() => s.want('k', { namespace: ['room'], track: 'v', staleAfterMs: -1 })).toThrow(RangeError);
    expect(() => s.want('k', { namespace: ['room'], track: 'v' }, { onObject: async () => {} })).toThrow(TypeError);
  });
});

describe('connectSupervisedSubscriber at a connection end', () => {
  it('waits for the connection to report its end instead of treating the refusal as fatal', async () => {
    const connections: FakeSubscriber[] = [];
    const subscriber = await superviseRelaySubscriber(
      async () => {
        const fake = fakeSubscriber();
        connections.push(fake);
        return fake;
      },
      { url: 'https://relay.example.com/moq', reconnect: fast },
    );
    cleanup.push(() => subscriber.close());
    const errors: unknown[] = [];
    subscriber.want('v', { namespace: ['room'], track: 'v', generations: true }, { onError: (e) => errors.push(e) });
    await until(() => connections[0]!.discoveries.length === 1, 'discovery');
    // The discovery fails with the closure, and further requests are refused, a turn before `closed` resolves.
    const first = connections[0]!;
    const namespaceEvents = first.namespaceEvents.bind(first);
    first.namespaceEvents = () => {
      throw new RelayError('CLOSED', 'Relay connection is closed', 'connection');
    };
    first.discoveries[0]!.fail(new SubscriptionConnectionClosedError());
    await new Promise((resolve) => setTimeout(resolve, 20));
    first.namespaceEvents = namespaceEvents;
    first.end(new Error('lost'));
    await until(() => connections[1]?.discoveries.length === 1, 'discovery on the replacement connection');
    expect(errors).toEqual([]);
  });
});

describe('connectSupervisedSubscriber following generations (review fixes)', () => {
  async function start() {
    const connections: FakeSubscriber[] = [];
    const subscriber = await superviseRelaySubscriber(
      async () => {
        const fake = fakeSubscriber();
        connections.push(fake);
        return fake;
      },
      { url: 'https://relay.example.com/moq', reconnect: fast },
    );
    cleanup.push(() => subscriber.close());
    return { subscriber, connections };
  }

  it('moves to a newer generation announced together with the older one', async () => {
    const { subscriber, connections } = await start();
    subscriber.want('v', { namespace: ['room'], track: 'v', generations: true, retry: fast });
    await until(() => connections[0]!.discoveries.length === 1, 'discovery');
    const older = newGeneration(1_000);
    const newer = newGeneration(2_000);
    const discovery = connections[0]!.discoveries[0]!;
    discovery.push({ type: 'announced', namespace: ['room', older] });
    discovery.push({ type: 'announced', namespace: ['room', newer] });
    await until(
      () => connections[0]!.subscriptions.at(-1)?.options.namespace[1] === newer,
      'subscription on the newer generation',
    );
  });

  it('surfaces a discovery that fails for good while subscribed', async () => {
    const { subscriber, connections } = await start();
    const errors: unknown[] = [];
    subscriber.want('v', { namespace: ['room'], track: 'v', generations: true }, { onError: (e) => errors.push(e) });
    await until(() => connections[0]!.discoveries.length === 1, 'discovery');
    connections[0]!.discoveries[0]!.push({ type: 'announced', namespace: ['room', newGeneration()] });
    await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
    connections[0]!.subscriptions[0]!.accept();
    const { SubscriptionRequestError } = await import('../src/subscription.js');
    connections[0]!.discoveries[0]!.fail(
      new SubscriptionRequestError({ errorCode: 0x1, retryInterval: 0, reason: 'unauthorized' }),
    );
    await until(() => errors.length === 1, 'onError');
    expect(connections[0]!.subscriptions[0]!.cancelled).toBe(true);
  });

  it('lets an overlapping root replace one whose last want just went away', async () => {
    const { subscriber, connections } = await start();
    const errors: unknown[] = [];
    subscriber.want('a', { namespace: ['room'], track: 'v', generations: true }, { onError: (e) => errors.push(e) });
    await until(() => connections[0]!.discoveries.length === 1, 'first discovery');
    subscriber.want(
      'a',
      { namespace: ['room', 'alice'], track: 'v', generations: true },
      { onError: (e) => errors.push(e) },
    );
    await until(() => connections[0]!.discoveries.length === 2, 'second discovery');
    expect(connections[0]!.discoveries[0]!.closed).toBe(true);
    expect(errors).toEqual([]);
  });

  it('cuts a retry delay short when a newer generation is announced', async () => {
    const { subscriber, connections } = await start();
    subscriber.want('v', {
      namespace: ['room'],
      track: 'v',
      generations: true,
      retry: { initialDelayMs: 60_000, maxDelayMs: 60_000 },
    });
    await until(() => connections[0]!.discoveries.length === 1, 'discovery');
    const discovery = connections[0]!.discoveries[0]!;
    discovery.push({ type: 'announced', namespace: ['room', newGeneration(1_000)] });
    await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
    connections[0]!.subscriptions[0]!.reject({ errorCode: 0x10, retryInterval: 0, reason: 'not found' });
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(connections[0]!.subscriptions).toHaveLength(1); // waiting out a minute-long delay
    discovery.push({ type: 'announced', namespace: ['room', newGeneration(2_000)] });
    await until(() => connections[0]!.subscriptions.length === 2, 'resubscribe at once');
  });
});

describe('connectSupervisedSubscriber (second review fixes)', () => {
  async function start() {
    const connections: FakeSubscriber[] = [];
    const subscriber = await superviseRelaySubscriber(
      async () => {
        const fake = fakeSubscriber();
        connections.push(fake);
        return fake;
      },
      { url: 'https://relay.example.com/moq', reconnect: fast },
    );
    cleanup.push(() => subscriber.close());
    return { subscriber, connections };
  }

  it('keeps a want whose subscribe the closing connection refused with its own error', async () => {
    const { subscriber, connections } = await start();
    const errors: unknown[] = [];
    const first = await (async () => {
      await until(() => connections.length === 1, 'connection');
      return connections[0]!;
    })();
    first.subscribe = () => {
      throw new RelayError('TRANSPORT_ERROR', 'lost', 'connection');
    };
    subscriber.want('v', { namespace: ['room'], track: 'v' }, { onError: (e) => errors.push(e) });
    await new Promise((resolve) => setTimeout(resolve, 20));
    first.end(new Error('lost'));
    await until(() => connections[1]?.subscriptions.length === 1, 'resubscribe on the replacement');
    expect(errors).toEqual([]);
  });

  it('retries a discovery that keeps failing no faster than the backoff', async () => {
    const { subscriber, connections } = await start();
    const { SubscriptionRequestError } = await import('../src/subscription.js');
    subscriber.want('v', {
      namespace: ['room'],
      track: 'v',
      generations: true,
      retry: { initialDelayMs: 200, maxDelayMs: 200 },
    });
    const failing = () => new SubscriptionRequestError({ errorCode: 0x0, retryInterval: 0, reason: 'internal error' });
    const started = Date.now();
    while (Date.now() - started < 300) {
      for (const discovery of connections[0]!.discoveries) if (!discovery.closed) discovery.fail(failing());
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    // One request at the start, one per 100-200 ms delay, and one opened in each delay: far from one per response.
    expect(connections[0]!.discoveries.length).toBeLessThanOrEqual(8);
  });
});

describe('PR review fixes: Bugbot and Codex pass 3', () => {
  it('keeps CLOSED while the generation is live, e.g. an abort without a reason', async () => {
    const connections: FakePublisher[] = [];
    const publisher = await superviseRelayPublisher(
      async (options) => {
        const fake = fakePublisher([...options.namespace]);
        connections.push(fake);
        return fake;
      },
      { url: 'https://relay.example.com/moq', namespace: ['room'], reconnect: fast },
    );
    cleanup.push(() => publisher.close());
    const writer = publisher.registerTrack({ track: 'v' });
    connections[0]!.tracks.get('v')!.failNext = new RelayError('CLOSED', 'Publish operation aborted', 'writing');
    await expect(writer.write(new Uint8Array([1]))).rejects.toMatchObject({ code: 'CLOSED' });
    expect(publisher.status).toBe('live');
  });

  async function subscriberWith() {
    const connections: FakeSubscriber[] = [];
    const subscriber = await superviseRelaySubscriber(
      async () => {
        const fake = fakeSubscriber();
        connections.push(fake);
        return fake;
      },
      { url: 'https://relay.example.com/moq', reconnect: fast },
    );
    cleanup.push(() => subscriber.close());
    return { subscriber, connections };
  }

  it('replaces the want when only staleAfterMs changes, so the new threshold applies', async () => {
    const { subscriber, connections } = await subscriberWith();
    subscriber.want('v', { namespace: ['room'], track: 'v' });
    await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
    connections[0]!.subscriptions[0]!.accept();
    subscriber.want('v', { namespace: ['room'], track: 'v', staleAfterMs: 20, retry: fast });
    await until(() => connections[0]!.subscriptions.length === 2, 'replacement');
    connections[0]!.subscriptions[1]!.accept();
    await until(() => connections[0]!.subscriptions.length === 3, 'stale resubscribe under the new threshold');
  });

  it('does not cut a backoff short for a fresh, still empty discovery', async () => {
    const { subscriber, connections } = await subscriberWith();
    const { SubscriptionRequestError } = await import('../src/subscription.js');
    subscriber.want('v', {
      namespace: ['room'],
      track: 'v',
      generations: true,
      retry: { initialDelayMs: 60_000, maxDelayMs: 60_000 },
    });
    await until(() => connections[0]!.discoveries.length === 1, 'discovery');
    const generation = newGeneration();
    connections[0]!.discoveries[0]!.push({ type: 'announced', namespace: ['room', generation] });
    await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
    connections[0]!.subscriptions[0]!.accept();
    connections[0]!.discoveries[0]!.fail(
      new SubscriptionRequestError({ errorCode: 0x0, retryInterval: 0, reason: 'internal error' }),
    );
    await until(() => connections[0]!.discoveries.length === 2, 'discovery reopened during the backoff');
    connections[0]!.discoveries[1]!.push({ type: 'announced', namespace: ['room', generation] });
    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(connections[0]!.subscriptions).toHaveLength(1);
  });

  it('surfaces a request the core cannot encode instead of retrying it', async () => {
    const { subscriber, connections } = await subscriberWith();
    const { MoqtProtocolError } = await import('../packages/moq-core/src/index.js');
    connections[0]!.subscribe = () => {
      throw new MoqtProtocolError('field too long to encode');
    };
    const errors: unknown[] = [];
    subscriber.want('v', { namespace: ['room'], track: 'v' }, { onError: (e) => errors.push(e) });
    await until(() => errors.length === 1, 'onError');
    expect(errors[0]).toBeInstanceOf(MoqtProtocolError);
  });
});

describe('PR review fixes: Codex pass 4', () => {
  async function subscriberWith() {
    const connections: FakeSubscriber[] = [];
    const subscriber = await superviseRelaySubscriber(
      async () => {
        const fake = fakeSubscriber();
        connections.push(fake);
        return fake;
      },
      { url: 'https://relay.example.com/moq', reconnect: fast },
    );
    cleanup.push(() => subscriber.close());
    return { subscriber, connections };
  }

  it('lets a synchronous replacement with an overlapping root win', async () => {
    const { subscriber, connections } = await subscriberWith();
    const errors: unknown[] = [];
    subscriber.want('a', { namespace: ['room'], track: 'v', generations: true }, { onError: (e) => errors.push(e) });
    subscriber.want(
      'a',
      { namespace: ['room', 'alice'], track: 'v', generations: true },
      { onError: (e) => errors.push(e) },
    );
    await until(() => connections[0]!.discoveries.length >= 1, 'discovery');
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(connections[0]!.discoveries.map((d) => d.prefix)).toEqual([['room', 'alice']]);
    expect(errors).toEqual([]);
  });

  it('observes a promise a handler returns instead of leaving it unhandled', async () => {
    const { subscriber, connections } = await subscriberWith();
    const logged: unknown[] = [];
    const original = console.error;
    console.error = (error: unknown) => logged.push(error);
    try {
      subscriber.want('v', { namespace: ['room'], track: 'v' }, { onObject: () => Promise.reject(new Error('boom')) });
      await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
      connections[0]!.subscriptions[0]!.accept();
      connections[0]!.subscriptions[0]!.object(0);
      await new Promise((resolve) => setTimeout(resolve, 10));
    } finally {
      console.error = original;
    }
    expect(String(logged[0])).toContain('onObject returned a promise');
  });

  it('surfaces a permanent discovery failure seen while backing off', async () => {
    const { subscriber, connections } = await subscriberWith();
    const { SubscriptionRequestError } = await import('../src/subscription.js');
    const errors: unknown[] = [];
    subscriber.want(
      'v',
      { namespace: ['room'], track: 'v', generations: true, retry: { initialDelayMs: 60_000, maxDelayMs: 60_000 } },
      { onError: (e) => errors.push(e) },
    );
    await until(() => connections[0]!.discoveries.length === 1, 'discovery');
    connections[0]!.discoveries[0]!.push({ type: 'announced', namespace: ['room', newGeneration()] });
    await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
    connections[0]!.subscriptions[0]!.reject({ errorCode: 0x10, retryInterval: 0, reason: 'not found' });
    // The backoff holds the discovery; the relay now refuses it for good.
    await new Promise((resolve) => setTimeout(resolve, 10));
    connections[0]!.discoveries[0]!.fail(
      new SubscriptionRequestError({ errorCode: 0x1, retryInterval: 0, reason: 'unauthorized' }),
    );
    await until(() => errors.length === 1, 'onError');
  });
});
