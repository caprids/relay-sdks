import { afterEach, describe, expect, it, vi } from 'vitest';
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

    // The old generation is still announced when the replacement arrives: the newer one is subscribed at once, and
    // the old subscription keeps delivering until the replacement's first object.
    const previous = latest(connections[0]!);
    discovery.push({ type: 'announced', namespace: ['room', 'alice', replacement] });
    await until(() => connections[0]!.subscriptions.length === 2, 'subscribe to the replacement');
    expect(latest(connections[0]!).options.namespace).toEqual(['room', 'alice', replacement]);
    latest(connections[0]!).accept();
    previous.object(2);
    expect(previous.cancelled).toBe(false);
    latest(connections[0]!).object(0);
    expect(previous.cancelled).toBe(true);
    previous.object(3);
    expect(seen).toEqual([
      `subscribed room/alice/${old}`,
      'object 1',
      'object 2',
      'restarted generation',
      `subscribed room/alice/${replacement}`,
      'object 0',
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
    await until(() => connections[0]!.discoveries.length > 0, 'discovery');
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

describe('callback failures and progress (#272)', () => {
  const url = 'https://relay.example.com/moq';
  const boom = new Error('callback failed');

  function publishers() {
    const connections: FakePublisher[] = [];
    const connect = async (options: ConnectRelayPublisherOptions) => {
      const fake = fakePublisher([...options.namespace]);
      connections.push(fake);
      return fake;
    };
    return { connections, connect };
  }

  function subscribers() {
    const connections: FakeSubscriber[] = [];
    const connect = async () => {
      const fake = fakeSubscriber();
      connections.push(fake);
      return fake;
    };
    return { connections, connect };
  }

  const throwOn =
    (when: string) =>
    (status: string): void => {
      if (status === when) throw boom;
    };

  it('rejects connectSupervisedPublisher and closes the connection when a callback throws on the first', async () => {
    const live = publishers();
    await expect(
      superviseRelayPublisher(live.connect, { url, namespace: ['room'], onStatus: throwOn('live') }),
    ).rejects.toBe(boom);
    await until(() => live.connections[0]!.closeCalls === 1, 'first connection closed');

    const generation = publishers();
    await expect(
      superviseRelayPublisher(generation.connect, {
        url,
        namespace: ['room'],
        onGeneration: () => {
          throw boom;
        },
      }),
    ).rejects.toBe(boom);
    await until(() => generation.connections[0]!.closeCalls === 1, 'first connection closed');
  });

  it('stops the publisher with the error when a callback throws on a later generation', async () => {
    let generations = 0;
    const secondGeneration = () => {
      generations += 1;
      if (generations === 2) throw boom;
    };
    for (const callbacks of [{ onStatus: throwOn('reconnecting') }, { onGeneration: secondGeneration }]) {
      const { connections, connect } = publishers();
      const publisher = await superviseRelayPublisher(connect, {
        url,
        namespace: ['room'],
        reconnect: fast,
        ...callbacks,
      });
      connections[0]!.end(new Error('lost'));
      await expect(publisher.closed).resolves.toEqual({ error: boom });
      expect(publisher.status).toBe('closed');
      expect(publisher.generation).toBeUndefined();
      await new Promise((resolve) => setTimeout(resolve, 10));
      // Nothing reconnects after the stop, and a generation the callback threw on is closed.
      expect(connections.at(-1)!.closeCalls).toBe(connections.length - 1);
    }
  });

  it('settles the publisher closed when onStatus throws reporting it', async () => {
    const { connect } = publishers();
    const publisher = await superviseRelayPublisher(connect, { url, namespace: ['room'], onStatus: throwOn('closed') });
    await expect(publisher.close()).rejects.toBe(boom);
    await expect(publisher.closed).resolves.toEqual({});
    expect(publisher.status).toBe('closed');

    const owner = new AbortController();
    const reason = new Error('owner done');
    const aborted = await superviseRelayPublisher(publishers().connect, {
      url,
      namespace: ['room'],
      onStatus: throwOn('closed'),
      signal: owner.signal,
    });
    owner.abort(reason);
    await expect(aborted.closed).resolves.toEqual({ error: reason });
  });

  it('rejects connectSupervisedSubscriber and closes the connection when onStatus throws on the first', async () => {
    const { connections, connect } = subscribers();
    await expect(superviseRelaySubscriber(connect, { url, onStatus: throwOn('live') })).rejects.toBe(boom);
    await expect(connections[0]!.closed).resolves.toEqual({});
  });

  it('stops the subscriber with the error when onStatus throws after the first connection', async () => {
    const { connections, connect } = subscribers();
    const subscriber = await superviseRelaySubscriber(connect, {
      url,
      reconnect: fast,
      onStatus: throwOn('reconnecting'),
    });
    connections[0]!.end(new Error('lost'));
    await expect(subscriber.closed).resolves.toEqual({ error: boom });
    expect(subscriber.status).toBe('closed');
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(connections).toHaveLength(1);
  });

  it('settles the subscriber closed when onStatus throws reporting it', async () => {
    const subscriber = await superviseRelaySubscriber(subscribers().connect, { url, onStatus: throwOn('closed') });
    expect(() => subscriber.close()).toThrow(boom);
    await expect(subscriber.closed).resolves.toEqual({});
    expect(subscriber.status).toBe('closed');

    const owner = new AbortController();
    const reason = new Error('owner done');
    const aborted = await superviseRelaySubscriber(subscribers().connect, {
      url,
      onStatus: throwOn('closed'),
      signal: owner.signal,
    });
    owner.abort(reason);
    await expect(aborted.closed).resolves.toEqual({ error: reason });
  });

  async function silenced(body: (logged: unknown[]) => Promise<void>): Promise<unknown[]> {
    const logged: unknown[] = [];
    const original = console.error;
    console.error = (error: unknown) => logged.push(error);
    try {
      await body(logged);
    } finally {
      console.error = original;
    }
    return logged;
  }

  it('keeps a want subscribed when onRestarted throws, and logs the failure', async () => {
    const { connections, connect } = subscribers();
    const subscriber = await superviseRelaySubscriber(connect, { url });
    cleanup.push(() => subscriber.close());
    const objects: number[] = [];
    const logged = await silenced(async () => {
      subscriber.want(
        'v',
        { namespace: ['room'], track: 'v', retry: fast },
        {
          onObject: (object) => objects.push(object.groupId),
          onRestarted: () => {
            throw boom;
          },
        },
      );
      await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
      connections[0]!.subscriptions[0]!.accept();
      connections[0]!.subscriptions[0]!.done();
      await until(() => connections[0]!.subscriptions.length === 2, 'resubscribed despite the throwing callback');
    });
    connections[0]!.subscriptions[1]!.accept();
    connections[0]!.subscriptions[1]!.object(7);
    expect(objects).toEqual([7]);
    expect(logged).toHaveLength(1);
    expect((logged[0] as Error).cause).toBe(boom);
  });

  it('forgets a want whose loop fails unexpectedly, here a throwing onError, and logs it', async () => {
    const { connections, connect } = subscribers();
    const subscriber = await superviseRelaySubscriber(connect, { url });
    cleanup.push(() => subscriber.close());
    const logged = await silenced(async (logged) => {
      subscriber.want(
        'v',
        { namespace: ['room'], track: 'v' },
        {
          onError: () => {
            throw boom;
          },
        },
      );
      await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
      connections[0]!.subscriptions[0]!.reject({ errorCode: 0x1, retryInterval: 0, reason: 'unauthorized' });
      await until(() => logged.length === 1, 'logged');
    });
    expect((logged[0] as Error).cause).toBe(boom);
    // Forgotten: wanting the same key again subscribes afresh instead of swapping handlers on a dead entry.
    subscriber.want('v', { namespace: ['room'], track: 'v' });
    await until(() => connections[0]!.subscriptions.length === 2, 'subscribed again');
  });

  it('counts only a later group as progress: cached groups replayed newest first do not reset the backoff', async () => {
    const { connections, connect } = subscribers();
    const subscriber = await superviseRelaySubscriber(connect, { url });
    cleanup.push(() => subscriber.close());
    subscriber.want('v', { namespace: ['room'], track: 'v', retry: { initialDelayMs: 40, maxDelayMs: 1_000 } });
    const started: number[] = [];
    for (let count = 1; count <= 4; count++) {
      await until(() => connections[0]!.subscriptions.length === count, `subscription ${count}`);
      started.push(performance.now());
      // A finished track still announced: its cached groups, newest first, then a done.
      const subscription = connections[0]!.subscriptions.at(-1)!;
      subscription.accept();
      subscription.object(10);
      subscription.object(9);
      subscription.done();
    }
    // The delays keep doubling (20-40, 40-80, 80-160 ms): nothing reset the backoff.
    expect(started[3]! - started[2]!).toBeGreaterThanOrEqual(80);
  });
});

describe('rotation before the credential expires (#261)', () => {
  async function publishers(expiries: (Date | undefined)[], options: { rotateBeforeMs?: number } = {}) {
    const connections: FakePublisher[] = [];
    const generations: string[] = [];
    const publisher = await superviseRelayPublisher(
      async (connection) => {
        const fake = fakePublisher([...connection.namespace], expiries[connections.length]);
        connections.push(fake);
        return fake;
      },
      {
        url: 'https://relay.example.com/moq',
        namespace: ['room'],
        reconnect: fast,
        ...options,
        onGeneration: ({ generation }) => generations.push(generation),
      },
    );
    cleanup.push(() => publisher.close());
    return { publisher, connections, generations };
  }

  it('moves a track to the next generation once it has demand there, then closes the old one', async () => {
    const { publisher, connections, generations } = await publishers([]);
    const writer = publisher.registerTrack({ track: 'video' });
    const old = connections[0]!;
    old.tracks.get('video')!.setDemand(true);
    await until(() => writer.hasDemand, 'demand on the first generation');
    // Let the supervisor hear the change before the iterator snapshots the current state.
    await new Promise((resolve) => setTimeout(resolve, 5));
    const changes = writer.demandChanges();
    expect((await changes.next()).value).toBe(true);

    publisher.rotate();
    await until(() => connections[1]?.tracks.has('video') === true, 'track registered on the next generation');
    expect(generations).toHaveLength(2);
    expect(publisher.generation?.generation).toBe(generations[1]);
    expect(publisher.status).toBe('live');
    // The viewers are still on the old generation: writes stay there.
    await writer.write(new Uint8Array([1]));
    expect(old.tracks.get('video')!.writes).toEqual([new Uint8Array([1])]);
    expect(old.closeCalls).toBe(0);

    connections[1]!.tracks.get('video')!.setDemand(true);
    await writer.write(new Uint8Array([2]));
    expect(connections[1]!.tracks.get('video')!.writes).toEqual([new Uint8Array([2])]);
    await until(() => old.closeCalls === 1, 'old generation closed once the track moved');
    expect(writer.hasDemand).toBe(true);
    expect(writer.closed).toBe(false);
    // Demand carried over: the move repeats `true` (so demand-driven state is written to the new generation), and no
    // `false` comes in between.
    expect((await changes.next()).value).toBe(true);
    connections[1]!.tracks.get('video')!.setDemand(false);
    expect((await changes.next()).value).toBe(false);
    await changes.return?.();
  });

  it('moves a track with no demand on the old generation at once', async () => {
    const { publisher, connections } = await publishers([]);
    const writer = publisher.registerTrack({ track: 'audio' });
    publisher.rotate();
    await until(() => connections[0]!.closeCalls === 1, 'old generation closed');
    await writer.write(new Uint8Array([9]));
    expect(connections[1]!.tracks.get('audio')!.writes).toEqual([new Uint8Array([9])]);
    expect(connections[0]!.tracks.get('audio')!.writes).toEqual([]);
  });

  it('rotates on its own before the credential expires', async () => {
    const soon = new Date(Date.now() + 200);
    const later = new Date(Date.now() + 3_600_000);
    const { connections, generations } = await publishers([soon, later], { rotateBeforeMs: 150 });
    expect(connections).toHaveLength(1);
    await until(() => connections[0]!.closeCalls === 1, 'rotated before the expiry');
    expect(connections).toHaveLength(2);
    expect(generations).toHaveLength(2);
  });

  it('keeps the live generation when the next credential expires no later', async () => {
    const soon = new Date(Date.now() + 100);
    const { publisher, connections } = await publishers([soon, soon], { rotateBeforeMs: 60_000 });
    await until(() => connections.length === 2, 'replacement attempted');
    await until(() => connections[1]!.closeCalls === 1, 'replacement discarded');
    expect(connections[0]!.closeCalls).toBe(0);
    expect(publisher.generation?.namespace).toEqual(connections[0]!.namespace);
  });

  it('checks rotateBeforeMs at once', async () => {
    for (const [value, type] of [
      ['60', TypeError],
      [-1, RangeError],
      [1.5, RangeError],
    ] as const) {
      await expect(
        superviseRelayPublisher(async () => fakePublisher(['room', 'x']), {
          url: 'https://relay.example.com/moq',
          namespace: ['room'],
          rotateBeforeMs: value as never,
        }),
      ).rejects.toThrow(type);
    }
  });

  async function subscribers(expiries: (Date | undefined)[] = []) {
    const connections: FakeSubscriber[] = [];
    const statuses: string[] = [];
    const subscriber = await superviseRelaySubscriber(
      async () => {
        const fake = fakeSubscriber(expiries[connections.length]);
        connections.push(fake);
        return fake;
      },
      {
        url: 'https://relay.example.com/moq',
        reconnect: fast,
        rotateBeforeMs: 150,
        onStatus: (status) => statuses.push(status),
      },
    );
    cleanup.push(() => subscriber.close());
    return { subscriber, connections, statuses };
  }

  it('moves a want to the new connection make-before-break', async () => {
    const { subscriber, connections, statuses } = await subscribers();
    const seen: string[] = [];
    subscriber.want(
      'video',
      { namespace: ['room'], track: 'video', retry: fast },
      {
        onSubscribed: () => seen.push('subscribed'),
        onObject: (object) => seen.push(`object ${object.groupId}`),
        onRestarted: ({ reason }) => seen.push(`restarted ${reason}`),
      },
    );
    await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
    const old = connections[0]!.subscriptions[0]!;
    old.accept();
    old.object(1);

    subscriber.rotate();
    await until(() => connections[1]?.subscriptions.length === 1, 'resubscribe on the new connection');
    const replacement = connections[1]!.subscriptions[0]!;
    replacement.accept();
    old.object(2);
    expect(old.cancelled).toBe(false);
    expect(connections[0]!.subscriptions).toHaveLength(1);
    // The replacement starts at the latest group, which the old subscription already delivered: not repeated.
    replacement.object(2);
    expect(old.cancelled).toBe(false);
    replacement.object(3);
    old.object(4);
    await until(() => old.cancelled, 'old subscription cancelled');
    expect(seen).toEqual(['subscribed', 'object 1', 'object 2', 'restarted rotation', 'subscribed', 'object 3']);
    expect(statuses).toEqual(['live']);
    // The old connection closes once every want moved; nothing reconnects after it.
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(connections).toHaveLength(2);
    expect(subscriber.status).toBe('live');
  });

  it('reports the restart when the old subscription ends before the replacement delivers', async () => {
    const { subscriber, connections } = await subscribers();
    const seen: string[] = [];
    subscriber.want(
      'video',
      { namespace: ['room'], track: 'video', retry: fast },
      {
        onSubscribed: () => seen.push('subscribed'),
        onRestarted: ({ reason }) => seen.push(`restarted ${reason}`),
      },
    );
    await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
    connections[0]!.subscriptions[0]!.accept();
    subscriber.rotate();
    await until(() => connections[1]?.subscriptions.length === 1, 'resubscribe');
    connections[1]!.subscriptions[0]!.accept();
    expect(seen).toEqual(['subscribed']);
    connections[0]!.end();
    await until(() => seen.length === 3, 'restart reported');
    expect(seen).toEqual(['subscribed', 'restarted rotation', 'subscribed']);
  });

  it('rotates the subscriber on its own before the credential expires', async () => {
    const { connections, statuses } = await subscribers([new Date(Date.now() + 200), new Date(Date.now() + 3_600_000)]);
    const closed = connections[0]!.closed;
    let ended = false;
    void closed.then(() => {
      ended = true;
    });
    // No wants: the old connection closes as soon as the new one is up.
    await until(() => connections.length === 2 && ended, 'rotated');
    expect(statuses).toEqual(['live']);
  });

  describe('Codex pass 1 fixes', () => {
    it('keeps the old generation open until a group opened on it is closed', async () => {
      const { publisher, connections } = await publishers([]);
      const writer = publisher.registerTrack({ track: 'video' });
      connections[0]!.tracks.get('video')!.setDemand(true);
      await until(() => writer.hasDemand, 'demand');
      const group = await writer.openGroup();
      publisher.rotate();
      await until(() => connections[1]?.tracks.has('video') === true, 'next generation');
      connections[1]!.tracks.get('video')!.setDemand(true);
      await writer.write(new Uint8Array([7])); // moves the track
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(connections[0]!.closeCalls).toBe(0);
      await group.write(new Uint8Array([1]));
      await group.close();
      await until(() => connections[0]!.closeCalls === 1, 'old generation closed after the group');
      expect(connections[0]!.tracks.get('video')!.writes).toEqual([new Uint8Array([1])]);
    });

    it('still rotates on rotate() after an automatic rotation was discarded', async () => {
      const soon = new Date(Date.now() + 100);
      const { publisher, connections } = await publishers([soon, soon, new Date(Date.now() + 3_600_000)]);
      await until(() => connections[1]?.closeCalls === 1, 'automatic replacement discarded');
      publisher.rotate();
      await until(() => connections[0]!.closeCalls === 1, 'manual rotation replaced the generation');
      expect(publisher.generation?.namespace).toEqual(connections[2]!.namespace);
    });

    it('keeps cutting repeats after the old subscription ended before the replacement delivered', async () => {
      const { subscriber, connections } = await subscribers();
      const seen: number[] = [];
      subscriber.want('v', { namespace: ['room'], track: 'v', retry: fast }, { onObject: (o) => seen.push(o.groupId) });
      await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
      const old = connections[0]!.subscriptions[0]!;
      old.accept();
      old.object(2);
      subscriber.rotate();
      await until(() => connections[1]?.subscriptions.length === 1, 'resubscribe');
      const replacement = connections[1]!.subscriptions[0]!;
      replacement.accept();
      connections[0]!.end();
      await until(() => old.cancelled || connections[0]!.subscriptions.length === 1, 'old ended');
      await new Promise((resolve) => setTimeout(resolve, 5));
      replacement.object(2);
      replacement.object(1);
      replacement.object(3);
      expect(seen).toEqual([2, 3]);
    });

    it('reconnects when the old connection ends while replacements keep failing', async () => {
      const connections: FakeSubscriber[] = [];
      const statuses: [string, unknown][] = [];
      let failing = false;
      const subscriber = await superviseRelaySubscriber(
        async () => {
          if (failing) throw new RelayError('TRANSPORT_ERROR', 'down', 'connecting');
          const fake = fakeSubscriber();
          connections.push(fake);
          return fake;
        },
        {
          url: 'https://relay.example.com/moq',
          reconnect: { initialDelayMs: 20, maxDelayMs: 20 },
          onStatus: (status, error) => statuses.push([status, error]),
        },
      );
      cleanup.push(() => subscriber.close());
      failing = true;
      subscriber.rotate();
      await new Promise((resolve) => setTimeout(resolve, 30));
      const lost = new RelayError('UNAUTHORIZED', 'expired', 'connection');
      connections[0]!.end(lost);
      await until(() => statuses.some(([status]) => status === 'reconnecting'), 'reconnecting reported');
      expect(statuses).toContainEqual(['reconnecting', lost]);
      failing = false;
      await until(() => subscriber.status === 'live' && connections.length === 2, 'reconnected');
    });

    it('reports a throwing held-back onSubscribed instead of rejecting unhandled', async () => {
      const { subscriber, connections } = await subscribers();
      const errors = vi.spyOn(console, 'error').mockImplementation(() => {});
      let calls = 0;
      subscriber.want(
        'v',
        { namespace: ['room'], track: 'v', retry: fast },
        {
          onSubscribed: () => {
            calls += 1;
            if (calls === 2) throw new Error('app bug');
          },
        },
      );
      await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
      connections[0]!.subscriptions[0]!.accept();
      subscriber.rotate();
      await until(() => connections[1]?.subscriptions.length === 1, 'resubscribe');
      connections[1]!.subscriptions[0]!.accept();
      connections[0]!.end();
      await until(() => errors.mock.calls.length > 0, 'logged');
      expect(calls).toBe(2);
      errors.mockRestore();
    });

    it('reports nothing for a pending handoff after close()', async () => {
      const { subscriber, connections } = await subscribers();
      const seen: string[] = [];
      subscriber.want(
        'v',
        { namespace: ['room'], track: 'v', retry: fast },
        { onSubscribed: () => seen.push('subscribed'), onRestarted: () => seen.push('restarted') },
      );
      await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
      connections[0]!.subscriptions[0]!.accept();
      subscriber.rotate();
      await until(() => connections[1]?.subscriptions.length === 1, 'resubscribe');
      connections[1]!.subscriptions[0]!.accept();
      subscriber.close();
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(seen).toEqual(['subscribed']);
    });
  });
  describe('Codex pass 2 fixes', () => {
    it('reconnects at once when the replacement connection ends during the overlap', async () => {
      const { subscriber, connections, statuses } = await subscribers();
      subscriber.want('a', { namespace: ['room'], track: 'a', retry: fast });
      subscriber.want('b', { namespace: ['room'], track: 'b', retry: fast });
      await until(() => connections[0]!.subscriptions.length === 2, 'subscribe both');
      for (const sub of connections[0]!.subscriptions) sub.accept();
      subscriber.rotate();
      await until(() => connections[1]?.subscriptions.length === 2, 'resubscribe both');
      connections[1]!.subscriptions[0]!.accept();
      connections[1]!.subscriptions[0]!.object(1); // 'a' moved, 'b' still on the old connection
      connections[1]!.end(new RelayError('TRANSPORT_ERROR', 'lost', 'connection'));
      await until(() => connections.length === 3, 'a third connection well within the overlap', 500);
      expect(statuses).toContain('reconnecting');
    });

    it('closes a group still open on the old generation when the track closes', async () => {
      const { publisher, connections } = await publishers([]);
      const writer = publisher.registerTrack({ track: 'video' });
      connections[0]!.tracks.get('video')!.setDemand(true);
      await until(() => writer.hasDemand, 'demand');
      const group = await writer.openGroup();
      publisher.rotate();
      await until(() => connections[1]?.tracks.has('video') === true, 'next generation');
      connections[1]!.tracks.get('video')!.setDemand(true);
      await writer.write(new Uint8Array([7]));
      writer.close();
      expect(connections[0]!.tracks.get('video')!.closed).toBe(true);
      await expect(group.write(new Uint8Array([1]))).rejects.toMatchObject({ code: 'CLOSED' });
    });

    it('keeps the cutoff when a replacement ends before taking over', async () => {
      const { subscriber, connections } = await subscribers();
      const seen: number[] = [];
      subscriber.want('v', { namespace: ['room'], track: 'v', retry: fast }, { onObject: (o) => seen.push(o.groupId) });
      await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
      const old = connections[0]!.subscriptions[0]!;
      old.accept();
      old.object(2);
      subscriber.rotate();
      await until(() => connections[1]?.subscriptions.length === 1, 'resubscribe');
      connections[1]!.subscriptions[0]!.accept();
      connections[1]!.subscriptions[0]!.done();
      old.object(3);
      await until(() => connections[1]!.subscriptions.length === 2, 'retry on the new connection');
      const retry = connections[1]!.subscriptions[1]!;
      retry.accept();
      retry.object(3);
      retry.object(4);
      expect(seen).toEqual([2, 3, 4]);
    });

    it('delivers nothing more when onRestarted cancels the want', async () => {
      const { subscriber, connections } = await subscribers();
      const seen: string[] = [];
      subscriber.want(
        'v',
        { namespace: ['room'], track: 'v', retry: fast },
        {
          onSubscribed: () => seen.push('subscribed'),
          onObject: (o) => seen.push(`object ${o.groupId}`),
          onRestarted: () => {
            seen.push('restarted');
            subscriber.unwant('v');
          },
        },
      );
      await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
      connections[0]!.subscriptions[0]!.accept();
      subscriber.rotate();
      await until(() => connections[1]?.subscriptions.length === 1, 'resubscribe');
      connections[1]!.subscriptions[0]!.accept();
      connections[1]!.subscriptions[0]!.object(5);
      expect(seen).toEqual(['subscribed', 'restarted']);
    });
  });
  describe('Codex pass 3 fixes', () => {
    it('passes a repeated true through, as demand-driven state needs', async () => {
      const { publisher, connections } = await publishers([]);
      const writer = publisher.registerTrack({ track: 'catalog' });
      const changes = writer.demandChanges();
      expect((await changes.next()).value).toBe(false);
      connections[0]!.tracks.get('catalog')!.setDemand(true);
      expect((await changes.next()).value).toBe(true);
      connections[0]!.tracks.get('catalog')!.setDemand(true); // the relay replaced its upstream subscription
      expect((await changes.next()).value).toBe(true);
      await changes.return?.();
    });

    it('keeps cutting what was delivered across two rotations with nothing new in between', async () => {
      const { subscriber, connections } = await subscribers();
      const seen: number[] = [];
      subscriber.want('v', { namespace: ['room'], track: 'v', retry: fast }, { onObject: (o) => seen.push(o.groupId) });
      await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
      connections[0]!.subscriptions[0]!.accept();
      connections[0]!.subscriptions[0]!.object(2);
      subscriber.rotate();
      await until(() => connections[1]?.subscriptions.length === 1, 'first rotation');
      connections[1]!.subscriptions[0]!.accept();
      connections[1]!.subscriptions[0]!.object(2); // cached: cut
      connections[0]!.end();
      await until(() => connections[0]!.subscriptions[0]!.cancelled || true, 'old ended');
      await new Promise((resolve) => setTimeout(resolve, 10));
      subscriber.rotate();
      await until(() => connections[2]?.subscriptions.length === 1, 'second rotation');
      connections[2]!.subscriptions[0]!.accept();
      connections[2]!.subscriptions[0]!.object(2);
      connections[2]!.subscriptions[0]!.object(3);
      expect(seen).toEqual([2, 3]);
    });

    it('reports reconnecting when the connection ends while its replacement is still connecting', async () => {
      const gate = deferred();
      const connections: FakeSubscriber[] = [];
      const statuses: [string, unknown][] = [];
      const subscriber = await superviseRelaySubscriber(
        async () => {
          if (connections.length === 1) await gate.promise;
          const fake = fakeSubscriber();
          connections.push(fake);
          return fake;
        },
        { url: 'https://relay.example.com/moq', reconnect: fast, onStatus: (s, e) => statuses.push([s, e]) },
      );
      cleanup.push(() => subscriber.close());
      subscriber.rotate();
      await new Promise((resolve) => setTimeout(resolve, 10));
      const lost = new RelayError('TRANSPORT_ERROR', 'lost', 'connection');
      connections[0]!.end(lost);
      await until(() => subscriber.status === 'reconnecting', 'reconnecting while the replacement connects');
      expect(statuses).toContainEqual(['reconnecting', lost]);
      gate.resolve();
      await until(() => subscriber.status === 'live', 'live on the replacement');
      expect(connections).toHaveLength(2);
    });

    it('unbinds a track left on the old generation when the next one ends before it moved', async () => {
      const { publisher, connections } = await publishers([]);
      const writer = publisher.registerTrack({ track: 'video' });
      connections[0]!.tracks.get('video')!.setDemand(true);
      await until(() => writer.hasDemand, 'demand');
      await new Promise((resolve) => setTimeout(resolve, 5));
      const changes = writer.demandChanges();
      expect((await changes.next()).value).toBe(true);
      publisher.rotate();
      await until(() => connections[1]?.tracks.has('video') === true, 'next generation');
      connections[1]!.end(new Error('lost'));
      await until(() => connections[0]!.closeCalls === 1, 'old generation closed');
      expect(writer.hasDemand).toBe(false);
      expect((await changes.next()).value).toBe(false);
      await changes.return?.();
    });
  });
  describe('Codex pass 4 fixes', () => {
    it('closes the old generation at once when a track with a group open on it closes', async () => {
      const { publisher, connections } = await publishers([]);
      const writer = publisher.registerTrack({ track: 'video' });
      connections[0]!.tracks.get('video')!.setDemand(true);
      await until(() => writer.hasDemand, 'demand');
      await writer.openGroup();
      publisher.rotate();
      await until(() => connections[1]?.tracks.has('video') === true, 'next generation');
      connections[1]!.tracks.get('video')!.setDemand(true);
      await writer.write(new Uint8Array([7]));
      writer.close();
      await until(() => connections[0]!.closeCalls === 1, 'old generation closed well before the overlap ends', 500);
    });
  });
  describe('Codex pass 5 fixes', () => {
    it('rotates a short-lived replacement during the overlap instead of letting it expire', async () => {
      const { subscriber, connections } = await subscribers([
        undefined,
        new Date(Date.now() + 300),
        new Date(Date.now() + 3_600_000),
      ]);
      subscriber.want('silent', { namespace: ['room'], track: 'silent', retry: fast });
      await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
      connections[0]!.subscriptions[0]!.accept(); // silent: no object will take the handoff over
      subscriber.rotate();
      await until(() => connections[1]?.subscriptions.length === 1, 'resubscribe');
      connections[1]!.subscriptions[0]!.accept();
      await until(() => connections.length === 3, 'the replacement rotated well within the 10 s overlap', 2_000);
    });
  });
  describe('Codex pass 6 fixes', () => {
    it("keeps a replacement's rotation deadline from when it connected, overlap or not", async () => {
      const connections: FakeSubscriber[] = [];
      const connectedAt: number[] = [];
      const expiries = [undefined, 600, 3_600_000];
      const subscriber = await superviseRelaySubscriber(
        async () => {
          const lifetime = expiries[connections.length];
          const fake = fakeSubscriber(lifetime === undefined ? undefined : new Date(Date.now() + lifetime));
          connections.push(fake);
          connectedAt.push(performance.now());
          return fake;
        },
        { url: 'https://relay.example.com/moq', reconnect: fast },
      );
      cleanup.push(() => subscriber.close());
      subscriber.want('silent', { namespace: ['room'], track: 'silent', retry: fast });
      await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
      connections[0]!.subscriptions[0]!.accept(); // silent: the overlap lasts until the replacement's own rotation
      subscriber.rotate();
      await until(() => connections[1]?.subscriptions.length === 1, 'resubscribe');
      connections[1]!.subscriptions[0]!.accept();
      await until(() => connections.length === 3, 'second rotation', 2_000);
      // Half of the 600 ms credential: 300 ms after the replacement connected, not 300 ms plus half what was left.
      expect(connectedAt[2]! - connectedAt[1]!).toBeLessThan(400);
    });
  });
});
