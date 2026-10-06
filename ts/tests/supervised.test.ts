import { afterEach, describe, expect, it } from 'vitest';
import { RelayError } from '../src/errors.js';
import type { ConnectRelayPublisherOptions } from '../src/publish.js';
import { SubscriptionConnectionClosedError } from '../src/subscription.js';
import { type SupervisedPublisher, superviseRelayPublisher } from '../src/supervised-publisher.js';
import {
  type SupervisedSubscriber,
  superviseRelaySubscriber,
  type WantHandlers,
  type WantOptions,
  type WantSwitchedInfo,
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

    // The crashed generation is still announced when the replacement arrives: the newer one wins at once, made
    // before the break.
    discovery.push({ type: 'announced', namespace: ['room', 'alice', replacement] });
    await until(() => connections[0]!.subscriptions.length === 2, 'subscribe to the replacement');
    expect(latest(connections[0]!).options.namespace).toEqual(['room', 'alice', replacement]);
    latest(connections[0]!).accept();
    latest(connections[0]!).object(0);
    expect(connections[0]!.subscriptions[0]!.cancelled).toBe(true);
    expect(seen).toEqual([`subscribed room/alice/${old}`, 'object 1', 'object 0']);

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

    subscriber.want('video', { namespace: ['lobby'], track: 'video' }, second.handlers);
    await until(() => connections[0]!.subscriptions.length === 2, 'replacement want');
    expect(connections[0]!.subscriptions[0]!.cancelled).toBe(true);
    expect(latest(connections[0]!).options.namespace).toEqual(['lobby']);
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

  it('applies a new staleAfterMs in place, without resubscribing first', async () => {
    const { subscriber, connections } = await subscriberWith();
    subscriber.want('v', { namespace: ['room'], track: 'v' });
    await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
    connections[0]!.subscriptions[0]!.accept();
    subscriber.want('v', { namespace: ['room'], track: 'v', staleAfterMs: 20, retry: fast });
    expect(connections[0]!.subscriptions[0]!.cancelled).toBe(false);
    await until(() => connections[0]!.subscriptions.length === 2, 'stale resubscribe under the new threshold');
    expect(connections[0]!.subscriptions[0]!.cancelled).toBe(true);
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

describe('make-before-break moves (#282)', () => {
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

  function recorder() {
    const seen: string[] = [];
    const switched: WantSwitchedInfo[] = [];
    const handlers: WantHandlers = {
      onSubscribed: (_, { namespace }) => seen.push(`subscribed ${namespace.join('/')}`),
      onObject: (object) => seen.push(`object ${object.groupId}`),
      onSubgroupEnd: ({ groupId }) => seen.push(`end ${groupId}`),
      onPublisherDone: () => seen.push('done'),
      onRestarted: ({ reason }) => seen.push(`restarted ${reason}`),
      onSwitched: (info) => {
        switched.push(info);
        seen.push(
          `switched ${info.from.namespace.join('/')}:${info.from.track} > ${info.to.namespace.join('/')}:${info.to.track}`,
        );
      },
      onError: (error) => seen.push(`error ${(error as RelayError).code}`),
    };
    return { seen, switched, handlers };
  }

  const subs = (connection: FakeSubscriber) => connection.subscriptions;

  /** A want following generations under `room`, subscribed and delivering on the older one. */
  async function onOldGeneration(options: Partial<WantOptions> = {}) {
    const { subscriber, connections } = await start();
    const { seen, switched, handlers } = recorder();
    subscriber.want('v', { namespace: ['room'], track: 'v', generations: true, retry: fast, ...options }, handlers);
    await until(() => connections[0]!.discoveries.length === 1, 'discovery');
    const discovery = connections[0]!.discoveries[0]!;
    const old = newGeneration(1_000);
    const replacement = newGeneration(2_000);
    discovery.push({ type: 'announced', namespace: ['room', old] });
    await until(() => subs(connections[0]!).length === 1, 'subscribe to the old generation');
    const first = subs(connections[0]!)[0]!;
    first.accept();
    first.object(10);
    return { subscriber, connections, seen, switched, discovery, old, replacement, first };
  }

  it('keeps the old generation delivering until the new one delivers, then reports a switch, not a restart', async () => {
    const { connections, seen, switched, discovery, old, replacement, first } = await onOldGeneration();
    discovery.push({ type: 'announced', namespace: ['room', replacement] });
    await until(() => subs(connections[0]!).length === 2, 'replacement opened');
    const second = subs(connections[0]!)[1]!;
    expect(second.options.namespace).toEqual(['room', replacement]);
    first.object(11);
    second.accept();
    first.object(12);
    expect(first.cancelled).toBe(false);
    second.object(0);
    expect(first.cancelled).toBe(true);
    first.object(13);
    second.object(1);
    expect(seen).toEqual([
      `subscribed room/${old}`,
      'object 10',
      'object 11',
      'object 12',
      `switched room/${old}:v > room/${replacement}:v`,
      'object 0',
      'object 1',
    ]);
    expect(switched[0]!.subscribed.trackAlias).toBe(1);
  });

  it('cuts over on the first object until accepts, discarding the ones it refuses', async () => {
    const { connections, seen, discovery, replacement, first } = await onOldGeneration({
      until: (object) => object.groupId >= 5,
    });
    discovery.push({ type: 'announced', namespace: ['room', replacement] });
    await until(() => subs(connections[0]!).length === 2, 'replacement opened');
    const second = subs(connections[0]!)[1]!;
    second.accept();
    second.object(3);
    first.object(11);
    second.object(5);
    expect(seen.slice(1)).toEqual(['object 10', 'object 11', expect.stringMatching(/^switched/), 'object 5']);
  });

  it('follows the cutover object with the later groups that reached the replacement early', async () => {
    const { connections, seen, discovery, replacement } = await onOldGeneration({
      until: (object) => object.groupId === 5,
    });
    discovery.push({ type: 'announced', namespace: ['room', replacement] });
    await until(() => subs(connections[0]!).length === 2, 'replacement opened');
    const second = subs(connections[0]!)[1]!;
    second.accept();
    second.object(6); // ahead: refused, held
    second.handlers.onSubgroupEnd?.({ groupId: 6, subgroupId: 0, endOfGroup: true });
    second.object(3); // earlier than the cutover: the old subscription's to deliver
    second.object(5);
    expect(seen.slice(1)).toEqual(['object 10', expect.stringMatching(/^switched/), 'object 5', 'object 6', 'end 6']);
  });

  it('drains the old subscription: its groups up to the largest delivered still arrive, for drainMs', async () => {
    const { connections, seen, discovery, replacement, first } = await onOldGeneration({ drainMs: 30 });
    discovery.push({ type: 'announced', namespace: ['room', replacement] });
    await until(() => subs(connections[0]!).length === 2, 'replacement opened');
    const second = subs(connections[0]!)[1]!;
    first.object(12);
    second.accept();
    second.object(0);
    expect(first.cancelled).toBe(false);
    first.object(11); // in flight, older than the largest it delivered
    first.handlers.onSubgroupEnd?.({ groupId: 11, subgroupId: 0, endOfGroup: true });
    first.object(13); // later than the cutover: discarded
    await until(() => first.cancelled, 'drain over');
    expect(seen.slice(1)).toEqual([
      'object 10',
      'object 12',
      expect.stringMatching(/^switched/),
      'object 0',
      'object 11',
      'end 11',
    ]);
  });

  it('moves a re-want with another track make-before-break, delivering to the new handlers', async () => {
    const { subscriber, connections } = await start();
    const before = recorder();
    const after = recorder();
    subscriber.want('v', { namespace: ['room'], track: 'video-lo' }, before.handlers);
    await until(() => subs(connections[0]!).length === 1, 'subscribe');
    const lo = subs(connections[0]!)[0]!;
    lo.accept();
    lo.object(1);
    subscriber.want('v', { namespace: ['room'], track: 'video-hi' }, after.handlers);
    await until(() => subs(connections[0]!).length === 2, 'pending move');
    const hi = subs(connections[0]!)[1]!;
    expect(hi.options.track).toBe('video-hi');
    lo.object(2);
    hi.accept();
    hi.object(7);
    expect(lo.cancelled).toBe(true);
    expect(before.seen).toEqual(['subscribed room', 'object 1']);
    expect(after.seen).toEqual(['object 2', 'switched room:video-lo > room:video-hi', 'object 7']);
    expect(after.switched[0]).toMatchObject({ from: { track: 'video-lo' }, to: { track: 'video-hi' } });
  });

  it('moves on new parameters alone', async () => {
    const { subscriber, connections } = await start();
    const { seen, handlers } = recorder();
    subscriber.want('v', { namespace: ['room'], track: 'v' }, handlers);
    await until(() => subs(connections[0]!).length === 1, 'subscribe');
    subs(connections[0]!)[0]!.accept();
    subscriber.want('v', { namespace: ['room'], track: 'v', parameters: { subscriberPriority: 1 } }, handlers);
    await until(() => subs(connections[0]!).length === 2, 'pending move');
    expect(subs(connections[0]!)[1]!.options.parameters).toEqual({ subscriberPriority: 1 });
    subs(connections[0]!)[1]!.accept();
    subs(connections[0]!)[1]!.object(0);
    expect(seen).toEqual(['subscribed room', 'switched room:v > room:v', 'object 0']);
  });

  it('replaces an unanswered subscription at once, with nothing to report', async () => {
    const { subscriber, connections } = await start();
    const { seen, handlers } = recorder();
    subscriber.want('v', { namespace: ['room'], track: 'a' }, handlers);
    await until(() => subs(connections[0]!).length === 1, 'subscribe');
    subscriber.want('v', { namespace: ['room'], track: 'b' }, handlers);
    await until(() => subs(connections[0]!).length === 2, 'replacement');
    expect(subs(connections[0]!)[0]!.cancelled).toBe(true);
    subs(connections[0]!)[1]!.accept();
    expect(seen).toEqual(['subscribed room']);
  });

  it('cancels a pending move when the want asks for the active track again, and supersedes one with another', async () => {
    const { subscriber, connections } = await start();
    const { seen, handlers } = recorder();
    subscriber.want('v', { namespace: ['room'], track: 'a' }, handlers);
    await until(() => subs(connections[0]!).length === 1, 'subscribe');
    const a = subs(connections[0]!)[0]!;
    a.accept();
    subscriber.want('v', { namespace: ['room'], track: 'b' }, handlers);
    await until(() => subs(connections[0]!).length === 2, 'pending b');
    subscriber.want('v', { namespace: ['room'], track: 'c' }, handlers);
    await until(() => subs(connections[0]!).length === 3, 'pending c');
    expect(subs(connections[0]!)[1]!.cancelled).toBe(true);
    subscriber.want('v', { namespace: ['room'], track: 'a' }, handlers);
    expect(subs(connections[0]!)[2]!.cancelled).toBe(true);
    a.object(4);
    expect(a.cancelled).toBe(false);
    expect(seen).toEqual(['subscribed room', 'object 4']);
  });

  it('keeps the old subscription delivering when the replacement is refused, and retries the move', async () => {
    const { connections, seen, discovery, replacement, first } = await onOldGeneration();
    discovery.push({ type: 'announced', namespace: ['room', replacement] });
    await until(() => subs(connections[0]!).length === 2, 'replacement opened');
    subs(connections[0]!)[1]!.reject(UNROUTABLE);
    await until(() => subs(connections[0]!).length === 3, 'move retried');
    expect(first.cancelled).toBe(false);
    first.object(11);
    subs(connections[0]!)[2]!.accept();
    subs(connections[0]!)[2]!.object(0);
    expect(seen.slice(1)).toEqual(['object 10', 'object 11', expect.stringMatching(/^switched/), 'object 0']);
  });

  it('surfaces a replacement refused for good and stops the want', async () => {
    const { connections, seen, discovery, replacement, first } = await onOldGeneration();
    discovery.push({ type: 'announced', namespace: ['room', replacement] });
    await until(() => subs(connections[0]!).length === 2, 'replacement opened');
    subs(connections[0]!)[1]!.reject({ errorCode: 0x1, retryInterval: 0, reason: 'unauthorized' });
    await until(() => seen.includes('error SUBSCRIPTION_REQUEST_FAILED'), 'error');
    expect(first.cancelled).toBe(true);
    expect(seen).not.toContain('restarted generation');
  });

  it('cuts over on the next object, whatever until says, once the old subscription has ended', async () => {
    const { connections, seen, discovery, replacement, first } = await onOldGeneration({ until: () => false });
    discovery.push({ type: 'announced', namespace: ['room', replacement] });
    await until(() => subs(connections[0]!).length === 2, 'replacement opened');
    const second = subs(connections[0]!)[1]!;
    second.accept();
    second.object(0); // refused while the old one still delivers
    first.done(); // the old publisher finished: a supervised publisher's rotation
    expect(first.cancelled).toBe(true);
    second.object(1);
    expect(seen.slice(1)).toEqual(['object 10', 'done', expect.stringMatching(/^switched/), 'object 1']);
  });

  it("reports the old subscription's ending as a restart when the replacement then fails", async () => {
    const { connections, seen, discovery, replacement, first } = await onOldGeneration();
    discovery.push({ type: 'announced', namespace: ['room', replacement] });
    await until(() => subs(connections[0]!).length === 2, 'replacement opened');
    first.done();
    subs(connections[0]!)[1]!.reject(UNROUTABLE);
    await until(() => seen.includes('restarted publisher-done'), 'restart');
    await until(() => subs(connections[0]!).length === 3, 'resubscribe');
    expect(subs(connections[0]!)[2]!.options.namespace).toEqual(['room', replacement]);
  });

  it('cuts over on the object a throwing until was offered, and logs the throw', async () => {
    const logged: unknown[] = [];
    const original = console.error;
    console.error = (error: unknown) => logged.push(error);
    cleanup.push(() => {
      console.error = original;
    });
    const { connections, seen, discovery, replacement } = await onOldGeneration({
      until: () => {
        throw new Error('bad predicate');
      },
    });
    discovery.push({ type: 'announced', namespace: ['room', replacement] });
    await until(() => subs(connections[0]!).length === 2, 'replacement opened');
    subs(connections[0]!)[1]!.accept();
    subs(connections[0]!)[1]!.object(0);
    expect(seen.slice(-2)).toEqual([expect.stringMatching(/^switched/), 'object 0']);
    expect(String((logged[0] as Error).message)).toMatch(/until threw/);
  });

  it('keeps a pending move when the old subscription goes stale, restarting only if the replacement stays silent', async () => {
    const { connections, seen, discovery, replacement, first } = await onOldGeneration({
      staleAfterMs: 40,
      until: () => false,
    });
    discovery.push({ type: 'announced', namespace: ['room', replacement] });
    await until(() => subs(connections[0]!).length === 2, 'replacement opened');
    const second = subs(connections[0]!)[1]!;
    second.accept();
    await until(() => first.cancelled, 'old subscription stale');
    expect(second.cancelled).toBe(false);
    second.object(0); // the old one is gone: cuts over whatever until says
    expect(seen.slice(1)).toEqual(['object 10', expect.stringMatching(/^switched/), 'object 0']);
    await until(() => seen.includes('restarted stale'), 'the replacement went stale too');
  });

  it('replays the held later groups even when a handler throws at the cutover', async () => {
    const logged: unknown[] = [];
    const original = console.error;
    console.error = (error: unknown) => logged.push(error);
    cleanup.push(() => {
      console.error = original;
    });
    const { subscriber, connections } = await start();
    const objects: number[] = [];
    subscriber.want(
      'v',
      { namespace: ['room'], track: 'a', until: (object) => object.groupId === 5 },
      {
        onObject: (object) => {
          objects.push(object.groupId);
          if (object.groupId === 5) throw new Error('decoder failed');
        },
      },
    );
    await until(() => subs(connections[0]!).length === 1, 'subscribe');
    subs(connections[0]!)[0]!.accept();
    subscriber.want(
      'v',
      { namespace: ['room'], track: 'b', until: (object) => object.groupId === 5 },
      {
        onObject: (object) => {
          objects.push(object.groupId);
          if (object.groupId === 5) throw new Error('decoder failed');
        },
      },
    );
    await until(() => subs(connections[0]!).length === 2, 'pending move');
    const b = subs(connections[0]!)[1]!;
    b.accept();
    b.object(6);
    b.object(7);
    b.object(5);
    expect(objects).toEqual([5, 6, 7]);
    expect(logged).toHaveLength(1);
  });

  it("starts a new move target on its own backoff, not the superseded one's", async () => {
    const { subscriber, connections } = await start();
    const retry = { initialDelayMs: 60, maxDelayMs: 60_000 };
    subscriber.want('v', { namespace: ['room'], track: 'a', retry });
    await until(() => subs(connections[0]!).length === 1, 'subscribe');
    subs(connections[0]!)[0]!.accept();
    subscriber.want('v', { namespace: ['room'], track: 'b', retry });
    for (let n = 2; n <= 4; n++) {
      await until(() => subs(connections[0]!).length === n, `move to b, attempt ${n - 1}`);
      subs(connections[0]!)[n - 1]!.reject(UNROUTABLE);
    }
    // b has failed three times: its next retry would wait 240 to 480 ms.
    subscriber.want('v', { namespace: ['room'], track: 'c', retry });
    await until(() => subs(connections[0]!).at(-1)!.options.track === 'c', 'move to c');
    const rejectedAt = Date.now();
    subs(connections[0]!).at(-1)!.reject(UNROUTABLE);
    const count = subs(connections[0]!).length;
    await until(() => subs(connections[0]!).length === count + 1, 'c retried');
    expect(Date.now() - rejectedAt).toBeLessThan(200);
  });

  it('keeps a re-want made after the old target failed for good, and subscribes to the new one', async () => {
    const { subscriber, connections } = await start();
    const { seen, handlers } = recorder();
    subscriber.want('v', { namespace: ['room'], track: 'a' }, handlers);
    await until(() => subs(connections[0]!).length === 1, 'subscribe');
    subs(connections[0]!)[0]!.reject({ errorCode: 0x1, retryInterval: 0, reason: 'unauthorized' });
    await Promise.resolve();
    subscriber.want('v', { namespace: ['room'], track: 'b' }, handlers);
    await until(() => subs(connections[0]!).length === 2, 'subscribe to b');
    expect(subs(connections[0]!)[1]!.options.track).toBe('b');
    expect(seen).toEqual([]);
  });

  it('surfaces a replacement refused for good after a re-want', async () => {
    const { subscriber, connections } = await start();
    const { seen, handlers } = recorder();
    subscriber.want('v', { namespace: ['room'], track: 'a' }, handlers);
    await until(() => subs(connections[0]!).length === 1, 'subscribe');
    subs(connections[0]!)[0]!.accept();
    subscriber.want('v', { namespace: ['room'], track: 'b' }, handlers);
    await until(() => subs(connections[0]!).length === 2, 'pending b');
    subs(connections[0]!)[1]!.reject({ errorCode: 0x1, retryInterval: 0, reason: 'unauthorized' });
    await until(() => seen.includes('error SUBSCRIPTION_REQUEST_FAILED'), 'error');
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(subs(connections[0]!)).toHaveLength(2);
  });

  it('tries a target wanted from onRestarted at once, not after the old backoff', async () => {
    const { subscriber, connections } = await start();
    const retry = { initialDelayMs: 60_000, maxDelayMs: 60_000 };
    let rewanted = false;
    const handlers: WantHandlers = {
      onRestarted: () => {
        if (rewanted) return;
        rewanted = true;
        subscriber.want('v', { namespace: ['room'], track: 'b', retry }, handlers);
      },
    };
    subscriber.want('v', { namespace: ['room'], track: 'a', retry }, handlers);
    await until(() => subs(connections[0]!).length === 1, 'subscribe');
    subs(connections[0]!)[0]!.accept();
    subs(connections[0]!)[0]!.done();
    await until(() => subs(connections[0]!).length === 2, 'b subscribed at once');
    expect(subs(connections[0]!)[1]!.options.track).toBe('b');
  });

  it('cuts over on a replacement object queued in the turn the old subscription ended', async () => {
    const { connections, seen, discovery, replacement, first } = await onOldGeneration({ until: () => false });
    discovery.push({ type: 'announced', namespace: ['room', replacement] });
    await until(() => subs(connections[0]!).length === 2, 'replacement opened');
    const second = subs(connections[0]!)[1]!;
    second.accept();
    // The relay ends the old subscription, and the replacement's object arrives before `ended` settles.
    first.terminate({ errorCode: 0x0, retryInterval: 0, reason: 'internal' });
    second.object(0);
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(seen.slice(1)).toEqual(['object 10', expect.stringMatching(/^switched/), 'object 0']);
  });

  it('reports a restart when a re-want supersedes a move that failed for good', async () => {
    const { subscriber, connections } = await start();
    const { seen, handlers } = recorder();
    subscriber.want('v', { namespace: ['room'], track: 'a' }, handlers);
    await until(() => subs(connections[0]!).length === 1, 'subscribe');
    subs(connections[0]!)[0]!.accept();
    subscriber.want('v', { namespace: ['room'], track: 'b' }, handlers);
    await until(() => subs(connections[0]!).length === 2, 'pending b');
    subs(connections[0]!)[1]!.reject({ errorCode: 0x1, retryInterval: 0, reason: 'unauthorized' });
    await Promise.resolve();
    subscriber.want('v', { namespace: ['room'], track: 'c' }, handlers);
    await until(() => subs(connections[0]!).length === 3, 'subscribe to c');
    subs(connections[0]!)[2]!.accept();
    expect(seen).toEqual(['subscribed room', 'restarted terminated', 'subscribed room']);
  });

  it("keeps the old subscription's reason when a re-want supersedes a move that failed for good", async () => {
    const { subscriber, connections } = await start();
    const { seen, handlers } = recorder();
    subscriber.want('v', { namespace: ['room'], track: 'a' }, handlers);
    await until(() => subs(connections[0]!).length === 1, 'subscribe');
    subs(connections[0]!)[0]!.accept();
    subscriber.want('v', { namespace: ['room'], track: 'b' }, handlers);
    await until(() => subs(connections[0]!).length === 2, 'pending b');
    subs(connections[0]!)[0]!.done();
    subs(connections[0]!)[1]!.reject({ errorCode: 0x1, retryInterval: 0, reason: 'unauthorized' });
    await Promise.resolve();
    subscriber.want('v', { namespace: ['room'], track: 'c' }, handlers);
    await until(() => seen.includes('restarted publisher-done'), 'restart');
    expect(seen).not.toContain('restarted terminated');
  });

  it('still restarts when the generation is withdrawn with none newer', async () => {
    const { connections, seen, discovery, old } = await onOldGeneration();
    discovery.push({ type: 'unannounced', namespace: ['room', old] });
    await until(() => seen.includes('restarted generation'), 'restart');
    expect(subs(connections[0]!)[0]!.cancelled).toBe(true);
  });

  it('swaps until and drainMs in place, and checks them at once', async () => {
    const { subscriber, connections } = await start();
    subscriber.want('v', { namespace: ['room'], track: 'v' });
    await until(() => subs(connections[0]!).length === 1, 'subscribe');
    subscriber.want('v', { namespace: ['room'], track: 'v', until: () => true, drainMs: 100 });
    expect(subs(connections[0]!)).toHaveLength(1);
    expect(subs(connections[0]!)[0]!.cancelled).toBe(false);
    expect(() => subscriber.want('v', { namespace: ['room'], track: 'v', until: 'first' as 'first-object' })).toThrow(
      TypeError,
    );
    expect(() => subscriber.want('v', { namespace: ['room'], track: 'v', until: (async () => true) as never })).toThrow(
      TypeError,
    );
    expect(() => subscriber.want('v', { namespace: ['room'], track: 'v', drainMs: -1 })).toThrow(RangeError);
    expect(() => subscriber.want('v', { namespace: ['room'], track: 'v', drainMs: 2 ** 31 })).toThrow(RangeError);
  });

  it('moves to the replacement connection when the connection ends during a pending move', async () => {
    const { connections, seen, discovery, replacement } = await onOldGeneration();
    discovery.push({ type: 'announced', namespace: ['room', replacement] });
    await until(() => subs(connections[0]!).length === 2, 'replacement opened');
    connections[0]!.end(new RelayError('TRANSPORT_ERROR', 'idle', 'connection'));
    await until(() => seen.includes('restarted connection'), 'restart');
    expect(seen.filter((entry) => entry.startsWith('switched'))).toEqual([]);
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

  it('moves a want to the new connection make-before-break, with no callback and nothing repeated', async () => {
    const { subscriber, connections, statuses } = await subscribers();
    const seen: string[] = [];
    subscriber.want(
      'video',
      { namespace: ['room'], track: 'video', retry: fast },
      {
        onSubscribed: () => seen.push('subscribed'),
        onObject: (object) => seen.push(`object ${object.groupId}`),
        onRestarted: ({ reason }) => seen.push(`restarted ${reason}`),
        onSwitched: () => seen.push('switched'),
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
    expect(seen).toEqual(['subscribed', 'object 1', 'object 2', 'object 3']);
    expect(statuses).toEqual(['live']);
    // The old connection closes once every want moved; nothing reconnects after it.
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(connections).toHaveLength(2);
    expect(subscriber.status).toBe('live');
  });

  it('carries on to the replacement when the old connection ends before it delivered', async () => {
    const { subscriber, connections } = await subscribers();
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
    connections[0]!.subscriptions[0]!.accept();
    connections[0]!.subscriptions[0]!.object(4);
    subscriber.rotate();
    await until(() => connections[1]?.subscriptions.length === 1, 'resubscribe');
    connections[1]!.subscriptions[0]!.accept();
    connections[0]!.end();
    await new Promise((resolve) => setTimeout(resolve, 10));
    connections[1]!.subscriptions[0]!.object(4); // already delivered
    connections[1]!.subscriptions[0]!.object(5);
    expect(seen).toEqual(['subscribed', 'object 4', 'object 5']);
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
      connections[1]!.subscriptions[0]!.terminate({ errorCode: 0x0, retryInterval: 0, reason: 'internal error' });
      old.object(3);
      await until(() => connections[1]!.subscriptions.length === 2, 'retry on the new connection');
      const retry = connections[1]!.subscriptions[1]!;
      retry.accept();
      retry.object(3);
      retry.object(4);
      expect(seen).toEqual([2, 3, 4]);
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
  describe('Codex pass 7 fixes', () => {
    it('serves the next generation while the old one drains, and close() waits for the drain', async () => {
      const drained = deferred();
      const connections: FakePublisher[] = [];
      const publisher = await superviseRelayPublisher(
        async (options) => {
          const fake = fakePublisher([...options.namespace]);
          if (connections.length === 0) {
            const close = fake.close.bind(fake);
            fake.close = async () => {
              fake.closeCalls += 1;
              await drained.promise;
              await close();
            };
          }
          connections.push(fake);
          return fake;
        },
        { url: 'https://relay.example.com/moq', namespace: ['room'], reconnect: fast },
      );
      cleanup.push(() => {
        drained.resolve();
        return publisher.close();
      });
      publisher.rotate();
      await until(() => connections[0]!.closeCalls === 1, 'old generation draining');
      publisher.rotate(); // the next generation is served while the old one drains
      await until(() => connections.length === 3, 'a rotation during the drain');
      let closed = false;
      const closing = publisher.close().then(() => {
        closed = true;
      });
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(closed).toBe(false);
      drained.resolve();
      await closing;
    });

    it('takes up a rotate() made during the overlap once the overlap ends', async () => {
      const { subscriber, connections } = await subscribers();
      subscriber.want('silent', { namespace: ['room'], track: 'silent', retry: fast });
      await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
      connections[0]!.subscriptions[0]!.accept();
      subscriber.rotate();
      await until(() => connections[1]?.subscriptions.length === 1, 'resubscribe');
      subscriber.rotate(); // during the overlap: the silent old subscription still holds it open
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(connections).toHaveLength(2);
      connections[0]!.end();
      await until(() => connections.length === 3, 'the rotation asked for during the overlap');
    });
  });
  describe('Codex pass 8 fixes', () => {
    it('takes the replacement when rotate() is called while an automatic rotation connects it', async () => {
      const gate = deferred();
      const soon = new Date(Date.now() + 200);
      const connections: FakePublisher[] = [];
      const publisher = await superviseRelayPublisher(
        async (options) => {
          if (connections.length === 1) await gate.promise;
          const fake = fakePublisher([...options.namespace], soon); // no fresher credential
          connections.push(fake);
          return fake;
        },
        { url: 'https://relay.example.com/moq', namespace: ['room'], reconnect: fast, rotateBeforeMs: 150 },
      );
      cleanup.push(() => publisher.close());
      await new Promise((resolve) => setTimeout(resolve, 150)); // the automatic rotation is connecting
      publisher.rotate();
      gate.resolve();
      await until(() => connections.length === 2 && connections[0]!.closeCalls === 1, 'replacement taken');
      expect(connections[1]!.closeCalls).toBe(0);
      expect(publisher.generation?.namespace).toEqual(connections[1]!.namespace);
    });
  });
  describe('Codex pass 9 fixes', () => {
    it('closes a replacement subscriber that close() could not see yet', async () => {
      const connections: FakeSubscriber[] = [];
      const statuses: string[] = [];
      let supervised: SupervisedSubscriber | undefined;
      const subscriber = await superviseRelaySubscriber(
        async () => {
          const fake = fakeSubscriber();
          if (connections.length === 1) {
            // Read once the helper has checked for shutdown and is handing the connection back.
            Object.defineProperty(fake, 'expiresAt', {
              get: () => {
                supervised?.close();
                return undefined;
              },
            });
          }
          connections.push(fake);
          return fake;
        },
        { url: 'https://relay.example.com/moq', reconnect: fast, onStatus: (status) => statuses.push(status) },
      );
      supervised = subscriber;
      subscriber.rotate();
      await until(() => statuses.includes('closed'), 'closed');
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(statuses).toEqual(['live', 'closed']);
      let replacementEnded = false;
      void connections[1]!.closed.then(() => {
        replacementEnded = true;
      });
      await until(() => replacementEnded, 'replacement closed');
    });

    it('closes a replacement generation that close() could not see yet', async () => {
      const connections: FakePublisher[] = [];
      let supervised: SupervisedPublisher | undefined;
      const publisher = await superviseRelayPublisher(
        async (options) => {
          const fake = fakePublisher([...options.namespace]);
          if (connections.length === 1) {
            Object.defineProperty(fake, 'expiresAt', {
              get: () => {
                void supervised?.close();
                return undefined;
              },
            });
          }
          connections.push(fake);
          return fake;
        },
        { url: 'https://relay.example.com/moq', namespace: ['room'], reconnect: fast },
      );
      supervised = publisher;
      publisher.rotate();
      await until(() => connections.length === 2 && connections[1]!.closeCalls === 1, 'replacement closed');
      expect(publisher.status).toBe('closed');
    });
  });
  describe('Codex pass 10 fixes', () => {
    it('keeps the old generation open when closing a group there is refused', async () => {
      const { publisher, connections } = await publishers([]);
      const writer = publisher.registerTrack({ track: 'video' });
      connections[0]!.tracks.get('video')!.setDemand(true);
      await until(() => writer.hasDemand, 'demand');
      const group = await writer.openGroup();
      publisher.rotate();
      await until(() => connections[1]?.tracks.has('video') === true, 'next generation');
      connections[1]!.tracks.get('video')!.setDemand(true);
      await writer.write(new Uint8Array([7]));
      connections[0]!.tracks.get('video')!.failNext = new RelayError('WRITE_PENDING', 'busy', 'closing');
      await expect(group.close()).rejects.toMatchObject({ code: 'WRITE_PENDING' });
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(connections[0]!.closeCalls).toBe(0);
      await group.close();
      await until(() => connections[0]!.closeCalls === 1, 'old generation closed once the group did');
    });

    it('answers a rotate() made while a discarded replacement closes', async () => {
      const soon = new Date(Date.now() + 200);
      const gate = deferred();
      const connections: FakePublisher[] = [];
      const publisher = await superviseRelayPublisher(
        async (options) => {
          const fake = fakePublisher([...options.namespace], soon);
          if (connections.length === 1) {
            const close = fake.close.bind(fake);
            fake.close = async () => {
              await gate.promise;
              await close();
            };
          }
          connections.push(fake);
          return fake;
        },
        { url: 'https://relay.example.com/moq', namespace: ['room'], reconnect: fast, rotateBeforeMs: 150 },
      );
      cleanup.push(() => {
        gate.resolve();
        return publisher.close();
      });
      await until(() => connections.length === 2, 'automatic replacement connected');
      await new Promise((resolve) => setTimeout(resolve, 10)); // being discarded: its close is held
      publisher.rotate();
      gate.resolve();
      await until(() => connections.length === 3, 'the manual rotation');
      await until(() => connections[0]!.closeCalls === 1, 'the replacement taken');
      expect(publisher.generation?.namespace).toEqual(connections[2]!.namespace);
    });

    it('answers a rotate() called from onGeneration after a reconnect', async () => {
      const connections: FakePublisher[] = [];
      let rotateFromCallback = false;
      let supervised: SupervisedPublisher | undefined;
      const publisher = await superviseRelayPublisher(
        async (options) => {
          const fake = fakePublisher([...options.namespace]);
          connections.push(fake);
          return fake;
        },
        {
          url: 'https://relay.example.com/moq',
          namespace: ['room'],
          reconnect: fast,
          onGeneration: () => {
            if (rotateFromCallback) {
              rotateFromCallback = false;
              supervised?.rotate();
            }
          },
        },
      );
      supervised = publisher;
      cleanup.push(() => publisher.close());
      rotateFromCallback = true;
      connections[0]!.end(new Error('lost'));
      await until(() => connections.length === 3, 'the rotation asked for from onGeneration');
    });
  });
  describe('Codex pass 11 fixes', () => {
    it('releases the old generation when a group there is aborted through its signal', async () => {
      const { publisher, connections } = await publishers([]);
      const writer = publisher.registerTrack({ track: 'video' });
      connections[0]!.tracks.get('video')!.setDemand(true);
      await until(() => writer.hasDemand, 'demand');
      const abort = new AbortController();
      await writer.openGroup({ signal: abort.signal });
      publisher.rotate();
      await until(() => connections[1]?.tracks.has('video') === true, 'next generation');
      connections[1]!.tracks.get('video')!.setDemand(true);
      await writer.write(new Uint8Array([7]));
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(connections[0]!.closeCalls).toBe(0);
      abort.abort();
      await until(() => connections[0]!.closeCalls === 1, 'old generation closed after the abort', 500);
    });

    it('drops the cutoff when the outgoing publisher ends, so a restarted one is delivered from group 0', async () => {
      const { subscriber, connections } = await subscribers();
      const seen: number[] = [];
      subscriber.want('v', { namespace: ['room'], track: 'v', retry: fast }, { onObject: (o) => seen.push(o.groupId) });
      await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
      const old = connections[0]!.subscriptions[0]!;
      old.accept();
      old.object(9);
      subscriber.rotate();
      await until(() => connections[1]?.subscriptions.length === 1, 'resubscribe');
      connections[1]!.subscriptions[0]!.reject(UNROUTABLE); // the replacement is not up yet
      old.done(); // the publisher ended
      await until(() => connections[1]!.subscriptions.length === 2, 'retry');
      const restarted = connections[1]!.subscriptions[1]!;
      restarted.accept();
      restarted.object(0);
      restarted.object(1);
      expect(seen).toEqual([9, 0, 1]);
    });
  });
  describe('Codex pass 12 fixes', () => {
    it('drops the cutoff when the replacement hears the publisher end before taking over', async () => {
      const { subscriber, connections } = await subscribers();
      const seen: number[] = [];
      subscriber.want('v', { namespace: ['room'], track: 'v', retry: fast }, { onObject: (o) => seen.push(o.groupId) });
      await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
      connections[0]!.subscriptions[0]!.accept();
      connections[0]!.subscriptions[0]!.object(10);
      subscriber.rotate();
      await until(() => connections[1]?.subscriptions.length === 1, 'resubscribe');
      connections[1]!.subscriptions[0]!.accept();
      connections[1]!.subscriptions[0]!.done(); // the publisher ended, heard on the replacement
      await until(() => connections[1]!.subscriptions.length === 2, 'retry');
      const restarted = connections[1]!.subscriptions[1]!;
      restarted.accept();
      restarted.object(0); // the restarted publisher, while the old subscription is still open
      connections[0]!.end(); // the old connection closes without a done
      restarted.object(1);
      expect(seen).toEqual([10, 0, 1]);
    });

    it('answers a queued rotate() with the replacement even when the old connection ended first', async () => {
      const gate = deferred();
      const connections: FakeSubscriber[] = [];
      const subscriber = await superviseRelaySubscriber(
        async () => {
          if (connections.length === 1) await gate.promise;
          const fake = fakeSubscriber(connections.length === 0 ? new Date(Date.now() + 200) : undefined);
          connections.push(fake);
          return fake;
        },
        { url: 'https://relay.example.com/moq', reconnect: fast, rotateBeforeMs: 150 },
      );
      cleanup.push(() => subscriber.close());
      await new Promise((resolve) => setTimeout(resolve, 150)); // the automatic rotation is connecting
      subscriber.rotate();
      connections[0]!.end(new Error('lost'));
      gate.resolve();
      await until(() => connections.length === 2 && subscriber.status === 'live', 'replacement adopted');
      await new Promise((resolve) => setTimeout(resolve, 50));
      expect(connections).toHaveLength(2);
    });
  });
  describe('Codex pass 13 fixes', () => {
    it('does not promote a replacement that ended in the turn the old connection did', async () => {
      const { subscriber, connections } = await subscribers();
      const seen: number[] = [];
      subscriber.want('v', { namespace: ['room'], track: 'v', retry: fast }, { onObject: (o) => seen.push(o.groupId) });
      await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
      connections[0]!.subscriptions[0]!.accept();
      connections[0]!.subscriptions[0]!.object(7);
      subscriber.rotate();
      await until(() => connections[1]?.subscriptions.length === 1, 'resubscribe');
      connections[1]!.subscriptions[0]!.accept();
      connections[1]!.subscriptions[0]!.terminate({ errorCode: 0x0, retryInterval: 0, reason: 'internal error' });
      connections[0]!.end(); // the same turn
      await until(() => connections[1]!.subscriptions.length === 2, 'retry');
      const retry = connections[1]!.subscriptions[1]!;
      retry.accept();
      retry.object(7);
      retry.object(8);
      expect(seen).toEqual([7, 8]);
    });

    it('releases the old generation when the relay resets demand under an open group there', async () => {
      const { publisher, connections } = await publishers([]);
      const writer = publisher.registerTrack({ track: 'video' });
      connections[0]!.tracks.get('video')!.setDemand(true);
      await until(() => writer.hasDemand, 'demand');
      await writer.openGroup();
      publisher.rotate();
      await until(() => connections[1]?.tracks.has('video') === true, 'next generation');
      connections[1]!.tracks.get('video')!.setDemand(true);
      await writer.write(new Uint8Array([7]));
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(connections[0]!.closeCalls).toBe(0);
      connections[0]!.tracks.get('video')!.resetGroups();
      await until(() => connections[0]!.closeCalls === 1, 'old generation closed once the group was reset', 500);
    });
  });
  describe('rotation with generations (merged with #282)', () => {
    async function following() {
      const { subscriber, connections } = await subscribers();
      const seen: string[] = [];
      subscriber.want(
        'v',
        { namespace: ['room'], track: 'v', generations: true, retry: fast },
        {
          onObject: (object) => seen.push(`object ${object.groupId}`),
          onRestarted: ({ reason }) => seen.push(`restarted ${reason}`),
          onSwitched: ({ from, to }) => seen.push(`switched ${from.namespace[1]} -> ${to.namespace[1]}`),
        },
      );
      await until(() => connections[0]!.discoveries.length === 1, 'discovery');
      return { subscriber, connections, seen };
    }

    it('moves a followed generation to the new connection silently', async () => {
      const { subscriber, connections, seen } = await following();
      const generation = newGeneration(1_000);
      connections[0]!.discoveries[0]!.push({ type: 'announced', namespace: ['room', generation] });
      await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
      connections[0]!.subscriptions[0]!.accept();
      connections[0]!.subscriptions[0]!.object(3);
      subscriber.rotate();
      await until(() => connections[1]?.discoveries.length === 1, 'discovery on the new connection');
      connections[1]!.discoveries[0]!.push({ type: 'announced', namespace: ['room', generation] });
      await until(() => connections[1]!.subscriptions.length === 1, 'replacement');
      const replacement = connections[1]!.subscriptions[0]!;
      expect(replacement.options.namespace).toEqual(['room', generation]);
      replacement.accept();
      replacement.object(3);
      replacement.object(4);
      expect(seen).toEqual(['object 3', 'object 4']);
      await until(() => connections[0]!.subscriptions[0]!.cancelled, 'old subscription cancelled');
    });

    it('keeps skipping repeats on a rotation after the generation a move left behind ended', async () => {
      const { subscriber, connections, seen } = await following();
      const older = newGeneration(1_000);
      const newer = newGeneration(2_000);
      const discovery = connections[0]!.discoveries[0]!;
      discovery.push({ type: 'announced', namespace: ['room', older] });
      await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
      const first = connections[0]!.subscriptions[0]!;
      first.accept();
      first.object(5);
      discovery.push({ type: 'announced', namespace: ['room', newer] });
      await until(() => connections[0]!.subscriptions.length === 2, 'move');
      const moved = connections[0]!.subscriptions[1]!;
      moved.accept();
      moved.object(0);
      first.done(); // the old generation's publisher closed after the move: not the current publisher
      moved.object(1);
      subscriber.rotate();
      await until(() => connections[1]?.discoveries.length === 1, 'discovery on the new connection');
      connections[1]!.discoveries[0]!.push({ type: 'announced', namespace: ['room', newer] });
      await until(() => connections[1]!.subscriptions.length === 1, 'replacement');
      const replacement = connections[1]!.subscriptions[0]!;
      replacement.accept();
      replacement.object(1); // already delivered
      replacement.object(2);
      expect(seen).toEqual(['object 5', `switched ${older} -> ${newer}`, 'object 0', 'object 1', 'object 2']);
    });

    it('reports a newer generation found on the new connection as a move', async () => {
      const { subscriber, connections, seen } = await following();
      const older = newGeneration(1_000);
      const newer = newGeneration(2_000);
      connections[0]!.discoveries[0]!.push({ type: 'announced', namespace: ['room', older] });
      await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
      connections[0]!.subscriptions[0]!.accept();
      connections[0]!.subscriptions[0]!.object(3);
      subscriber.rotate();
      await until(() => connections[1]?.discoveries.length === 1, 'discovery on the new connection');
      connections[1]!.discoveries[0]!.push({ type: 'announced', namespace: ['room', older] });
      connections[1]!.discoveries[0]!.push({ type: 'announced', namespace: ['room', newer] });
      await until(
        () => connections[1]!.subscriptions.at(-1)?.options.namespace[1] === newer,
        'replacement on the newer generation',
      );
      const replacement = connections[1]!.subscriptions.at(-1)!;
      replacement.accept();
      replacement.object(0);
      expect(seen).toEqual(['object 3', `switched ${older} -> ${newer}`, 'object 0']);
    });
  });
  describe('Codex review of the merge', () => {
    it('skips an object the old subscription delivered that reaches the replacement late, after the cutover', async () => {
      const { subscriber, connections } = await subscribers();
      const seen: number[] = [];
      subscriber.want('v', { namespace: ['room'], track: 'v', retry: fast }, { onObject: (o) => seen.push(o.groupId) });
      await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
      connections[0]!.subscriptions[0]!.accept();
      connections[0]!.subscriptions[0]!.object(4);
      subscriber.rotate();
      await until(() => connections[1]?.subscriptions.length === 1, 'resubscribe');
      const replacement = connections[1]!.subscriptions[0]!;
      replacement.accept();
      replacement.object(5); // cuts over
      replacement.object(4); // group 4's stream arrives late
      replacement.object(6);
      expect(seen).toEqual([4, 5, 6]);
    });

    it("keeps skipping repeats when only another target's publisher ended", async () => {
      const { subscriber, connections } = await subscribers();
      const seen: string[] = [];
      const handlers = { onObject: (o: { groupId: number }) => seen.push(`${o.groupId}`) } as never;
      subscriber.want('v', { namespace: ['room'], track: 'a', retry: fast }, handlers);
      await until(() => connections[0]!.subscriptions.length === 1, 'subscribe a');
      connections[0]!.subscriptions[0]!.accept();
      connections[0]!.subscriptions[0]!.object(4);
      subscriber.want('v', { namespace: ['room'], track: 'b', retry: fast }, handlers);
      await until(() => connections[0]!.subscriptions.length === 2, 'pending b');
      connections[0]!.subscriptions[1]!.done(); // b's publisher ended
      subscriber.want('v', { namespace: ['room'], track: 'a', retry: fast }, handlers); // back to a
      await new Promise((resolve) => setTimeout(resolve, 10));
      subscriber.rotate();
      await until(() => connections[1]?.subscriptions.length === 1, 'replacement for a');
      const replacement = connections[1]!.subscriptions[0]!;
      expect(replacement.options.track).toBe('a');
      replacement.accept();
      replacement.object(4); // a's cached group, already delivered
      replacement.object(5);
      expect(seen).toEqual(['4', '5']);
    });
  });
  describe('Codex review of the merge, pass 2', () => {
    it('keeps skipping repeats when the old subscription went stale during a rotation', async () => {
      const { subscriber, connections } = await subscribers();
      const seen: number[] = [];
      subscriber.want(
        'v',
        { namespace: ['room'], track: 'v', staleAfterMs: 40, retry: fast },
        { onObject: (o) => seen.push(o.groupId) },
      );
      await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
      connections[0]!.subscriptions[0]!.accept();
      connections[0]!.subscriptions[0]!.object(10);
      subscriber.rotate();
      await until(() => connections[1]?.subscriptions.length === 1, 'resubscribe');
      const replacement = connections[1]!.subscriptions[0]!;
      replacement.accept();
      await until(() => connections[0]!.subscriptions[0]!.cancelled, 'old subscription went stale');
      replacement.object(10); // already delivered
      replacement.object(9); // late, already behind what was delivered
      replacement.object(11);
      expect(seen).toEqual([10, 11]);
    });
  });
});
