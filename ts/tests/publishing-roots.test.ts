import { afterEach, describe, expect, it } from 'vitest';
import {
  encodeNamespace,
  encodeNamespaceDone,
  encodeRequestOk,
} from '../packages/moq-core/src/protocol/control-messages.js';
import type { DiscoveryIndex } from '../src/discovery.js';
import { RelayError } from '../src/errors.js';
import { connectRelaySubscriber } from '../src/index.js';
import {
  applyNamespace,
  type Presence,
  type PublishingRoot,
  type PublishingRootsChange,
  type PublishingRootsConnection,
  type PublishingRootsHandlers,
  type PublishingRootsPhase,
  rootField,
  type Source,
  transition,
} from '../src/publishing-roots.js';
import { SubscriptionConnectionClosedError, SubscriptionRequestError } from '../src/subscription.js';
import { superviseRelaySubscriber, type WantHandlers } from '../src/supervised-subscriber.js';
import { newGeneration } from '../src/supervision.js';
import { createFakeTransport, type FakeTransport } from './helpers/relay.js';
import { type FakeSubscriber, fakeSubscriber, until } from './helpers/supervision-fakes.js';

const fast = { initialDelayMs: 0, maxDelayMs: 0 };
const cleanup: (() => unknown)[] = [];
afterEach(async () => {
  for (const close of cleanup.splice(0)) await close();
});

const PREFIX = ['room', 'p'];
const at = (...fields: string[]) => [...PREFIX, ...fields];
const pause = (ms = 10) => new Promise((resolve) => setTimeout(resolve, ms));

async function start(expiries: (Date | undefined)[] = []) {
  const connections: FakeSubscriber[] = [];
  const subscriber = await superviseRelaySubscriber(
    async () => {
      const fake = fakeSubscriber(expiries[connections.length]);
      connections.push(fake);
      return fake;
    },
    { url: 'https://relay.example.com/moq', reconnect: fast, rotateBeforeMs: 150 },
  );
  cleanup.push(() => subscriber.close());
  const discovery = async (n = 0, count = 1) => {
    await until(() => (connections[n]?.discoveries.length ?? 0) >= count, `discovery ${count} on connection ${n}`);
    return connections[n]!.discoveries[count - 1]!;
  };
  return { subscriber, connections, discovery };
}

function recorder() {
  const changes: PublishingRootsChange[] = [];
  const errors: unknown[] = [];
  const handlers: PublishingRootsHandlers = {
    onChanged: (change) => changes.push(change),
    onError: (error) => errors.push(error),
  };
  return { changes, errors, handlers };
}

const ids = (roots: readonly PublishingRoot[]) => roots.map((m) => m.id);
const summary = (change: PublishingRootsChange) => ({
  added: ids(change.added),
  removed: ids(change.removed),
  present: [...change.present.keys()],
  initial: change.initial,
});

describe('SupervisedSubscriber.publishingRoots', () => {
  it('loads with every snapshot root in added, and calls nothing before', async () => {
    const { subscriber, discovery } = await start();
    const { changes, handlers } = recorder();
    const view = subscriber.publishingRoots({ prefix: PREFIX }, handlers);
    expect(view.loaded).toBe(false);
    expect(view.present.size).toBe(0);
    const d = await discovery();
    expect(d.prefix).toEqual(PREFIX);
    d.push({ type: 'announced', namespace: at('alice', newGeneration()) });
    d.push({ type: 'announced', namespace: at('bob') });
    await pause();
    expect(changes).toEqual([]);
    expect(view.loaded).toBe(false);
    d.push({ type: 'loaded' });
    await until(() => changes.length === 1, 'load');
    expect(summary(changes[0]!)).toEqual({
      added: ['alice', 'bob'],
      removed: [],
      present: ['alice', 'bob'],
      initial: true,
    });
    expect(changes[0]!.added[0]!.namespace).toEqual(at('alice'));
    expect(view.loaded).toBe(true);
    expect(view.present).toBe(changes[0]!.present);
  });

  it('loads an empty prefix once, with no root', async () => {
    const { subscriber, discovery } = await start();
    const { changes, handlers } = recorder();
    const view = subscriber.publishingRoots({ prefix: PREFIX }, handlers);
    (await discovery()).push({ type: 'loaded' });
    await until(() => changes.length === 1, 'load');
    expect(summary(changes[0]!)).toEqual({ added: [], removed: [], present: [], initial: true });
    expect(view.loaded).toBe(true);
    expect(view.present.size).toBe(0);
  });

  it('reports one added and one removed for a live announce and withdrawal', async () => {
    const { subscriber, discovery } = await start();
    const { changes, handlers } = recorder();
    const view = subscriber.publishingRoots({ prefix: PREFIX }, handlers);
    const d = await discovery();
    d.push({ type: 'loaded' });
    const generation = newGeneration();
    d.push({ type: 'announced', namespace: at('carol', generation) });
    await until(() => changes.length === 2, 'join');
    expect(summary(changes[1]!)).toEqual({ added: ['carol'], removed: [], present: ['carol'], initial: false });
    expect(view.present).toBe(changes[1]!.present);
    d.push({ type: 'unannounced', namespace: at('carol', generation) });
    await until(() => changes.length === 3, 'leave');
    expect(summary(changes[2]!)).toEqual({ added: [], removed: ['carol'], present: [], initial: false });
  });

  it('reports nothing for a publisher rotation, a relay repeat or a namespace below a generation', async () => {
    const { subscriber, discovery } = await start();
    const { changes, handlers } = recorder();
    subscriber.publishingRoots({ prefix: PREFIX }, handlers);
    const d = await discovery();
    const old = newGeneration(1_000);
    d.push({ type: 'announced', namespace: at('alice', old) });
    d.push({ type: 'loaded' });
    await until(() => changes.length === 1, 'load');
    const present = changes[0]!.present;
    const replacement = newGeneration(2_000);
    d.push({ type: 'announced', namespace: at('alice', replacement) });
    d.push({ type: 'unannounced', namespace: at('alice', old) });
    d.push({ type: 'announced', namespace: at('alice', replacement) });
    d.push({ type: 'announced', namespace: at('alice', replacement, 'deeper') });
    d.push({ type: 'announced', namespace: at('mallory', newGeneration(), 'deeper') });
    d.push({ type: 'announced', namespace: at('zed') });
    await until(() => changes.length === 2, 'the marker join');
    expect(summary(changes[1]!)).toEqual({ added: ['zed'], removed: [], present: ['alice', 'zed'], initial: false });
    expect(changes[1]!.present).not.toBe(present);
  });

  it('follows an unsupervised publisher, one field below the prefix, in and out', async () => {
    const { subscriber, discovery } = await start();
    const { changes, handlers } = recorder();
    subscriber.publishingRoots({ prefix: PREFIX }, handlers);
    const d = await discovery();
    d.push({ type: 'loaded' });
    d.push({ type: 'announced', namespace: at('dave') });
    await until(() => changes.length === 2, 'join');
    expect(summary(changes[1]!)).toEqual({ added: ['dave'], removed: [], present: ['dave'], initial: false });
    d.push({ type: 'unannounced', namespace: at('dave') });
    await until(() => changes.length === 3, 'leave');
    expect(summary(changes[2]!)).toEqual({ added: [], removed: ['dave'], present: [], initial: false });
  });

  it('reports nothing for rotate()', async () => {
    const { subscriber, discovery } = await start();
    const { changes, handlers } = recorder();
    const view = subscriber.publishingRoots({ prefix: PREFIX }, handlers);
    const generation = newGeneration();
    const first = await discovery();
    first.push({ type: 'announced', namespace: at('alice', generation) });
    first.push({ type: 'loaded' });
    await until(() => changes.length === 1, 'load');
    subscriber.rotate();
    const next = await discovery(1);
    expect(next.prefix).toEqual(PREFIX);
    expect(view.present.has('alice')).toBe(true);
    next.push({ type: 'announced', namespace: at('alice', generation) });
    next.push({ type: 'loaded' });
    next.push({ type: 'announced', namespace: at('zed') });
    await until(() => changes.length === 2, 'the marker join on the new connection');
    expect(summary(changes[1]!)).toEqual({ added: ['zed'], removed: [], present: ['alice', 'zed'], initial: false });
  });

  it('follows the old discovery through rotate() until the new one loads, with no want holding the connection', async () => {
    const { connections, subscriber, discovery } = await start();
    const { changes, handlers } = recorder();
    subscriber.publishingRoots({ prefix: PREFIX }, handlers);
    const first = await discovery();
    first.push({ type: 'announced', namespace: at('alice') });
    first.push({ type: 'loaded' });
    await until(() => changes.length === 1, 'load');
    let oldClosed = false;
    void connections[0]!.closed.then(() => {
      oldClosed = true;
    });
    subscriber.rotate();
    const next = await discovery(1);
    await pause();
    expect(oldClosed).toBe(false);
    first.push({ type: 'announced', namespace: at('bob') });
    await until(() => changes.length === 2, 'a join on the old discovery while the new one loads');
    expect(summary(changes[1]!)).toEqual({ added: ['bob'], removed: [], present: ['alice', 'bob'], initial: false });
    next.push({ type: 'announced', namespace: at('alice') });
    next.push({ type: 'announced', namespace: at('bob') });
    next.push({ type: 'loaded' });
    await until(() => oldClosed, 'the old connection closes once the new discovery has loaded');
    expect(changes).toHaveLength(2);
  });

  it('reports nothing for a rotation before the credential expires', async () => {
    const { connections, subscriber, discovery } = await start([
      new Date(Date.now() + 600),
      new Date(Date.now() + 3_600_000),
    ]);
    const { changes, handlers } = recorder();
    subscriber.publishingRoots({ prefix: PREFIX }, handlers);
    const first = await discovery();
    first.push({ type: 'announced', namespace: at('alice') });
    first.push({ type: 'loaded' });
    await until(() => changes.length === 1, 'load');
    const next = await discovery(1);
    next.push({ type: 'announced', namespace: at('alice') });
    next.push({ type: 'loaded' });
    await until(() => connections[0]!.discoveries[0]!.closed, 'the old connection released');
    next.push({ type: 'announced', namespace: at('zed') });
    await until(() => changes.length === 2, 'the marker join on the new connection');
    expect(summary(changes[1]!)).toEqual({ added: ['zed'], removed: [], present: ['alice', 'zed'], initial: false });
  });

  it('holds the view through a reconnect and reports nothing when the set is unchanged', async () => {
    const { connections, subscriber, discovery } = await start();
    const { changes, handlers } = recorder();
    const view = subscriber.publishingRoots({ prefix: PREFIX }, handlers);
    const first = await discovery();
    first.push({ type: 'announced', namespace: at('alice') });
    first.push({ type: 'announced', namespace: at('bob') });
    first.push({ type: 'loaded' });
    await until(() => changes.length === 1, 'load');
    connections[0]!.end(new RelayError('TRANSPORT_ERROR', 'idle', 'connection'));
    const next = await discovery(1);
    expect(view.loaded).toBe(true);
    expect([...view.present.keys()]).toEqual(['alice', 'bob']);
    next.push({ type: 'announced', namespace: at('bob') });
    next.push({ type: 'announced', namespace: at('alice') });
    next.push({ type: 'loaded' });
    next.push({ type: 'announced', namespace: at('zed') });
    await until(() => changes.length === 2, 'the marker join');
    expect(summary(changes[1]!)).toEqual({
      added: ['zed'],
      removed: [],
      present: ['alice', 'bob', 'zed'],
      initial: false,
    });
  });

  it('reports exactly the root removed during an outage, once the new discovery loads', async () => {
    const { connections, subscriber, discovery } = await start();
    const { changes, handlers } = recorder();
    subscriber.publishingRoots({ prefix: PREFIX }, handlers);
    const first = await discovery();
    first.push({ type: 'announced', namespace: at('alice') });
    first.push({ type: 'announced', namespace: at('bob') });
    first.push({ type: 'loaded' });
    await until(() => changes.length === 1, 'load');
    connections[0]!.end(new RelayError('TRANSPORT_ERROR', 'idle', 'connection'));
    const next = await discovery(1);
    next.push({ type: 'announced', namespace: at('alice') });
    await pause();
    expect(changes).toHaveLength(1);
    next.push({ type: 'loaded' });
    await until(() => changes.length === 2, 'the difference');
    expect(summary(changes[1]!)).toEqual({ added: [], removed: ['bob'], present: ['alice'], initial: false });
    await pause();
    expect(changes).toHaveLength(2);
  });

  it('holds the view when discovery ends with a retryable error, and reloads', async () => {
    const { subscriber, discovery } = await start();
    const { changes, errors, handlers } = recorder();
    const view = subscriber.publishingRoots({ prefix: PREFIX }, handlers);
    const first = await discovery();
    first.push({ type: 'announced', namespace: at('alice') });
    first.push({ type: 'loaded' });
    await until(() => changes.length === 1, 'load');
    first.fail(new SubscriptionRequestError({ errorCode: 0x0, retryInterval: 0, reason: 'internal error' }));
    const next = await discovery(0, 2);
    expect([...view.present.keys()]).toEqual(['alice']);
    next.push({ type: 'announced', namespace: at('alice') });
    next.push({ type: 'announced', namespace: at('bob') });
    next.push({ type: 'loaded' });
    await until(() => changes.length === 2, 'the difference');
    expect(summary(changes[1]!)).toEqual({ added: ['bob'], removed: [], present: ['alice', 'bob'], initial: false });
    expect(errors).toEqual([]);
  });

  it('calls onError and closes for a refusal no wait fixes', async () => {
    const { connections, subscriber, discovery } = await start();
    const { changes, errors, handlers } = recorder();
    const view = subscriber.publishingRoots({ prefix: PREFIX }, handlers);
    const first = await discovery();
    first.push({ type: 'loaded' });
    await until(() => changes.length === 1, 'load');
    const refusal = new SubscriptionRequestError({ errorCode: 0x1, retryInterval: 0, reason: 'unauthorized' });
    first.fail(refusal);
    await until(() => errors.length === 1, 'onError');
    expect(errors[0]).toBe(refusal);
    await pause(300);
    expect(connections[0]!.discoveries).toHaveLength(1);
    expect(changes).toHaveLength(1);
    expect(view.loaded).toBe(true);
  });

  it('is safe to close from onChanged: nothing is delivered after it', async () => {
    const { subscriber, discovery } = await start();
    const changes: PublishingRootsChange[] = [];
    const view = subscriber.publishingRoots(
      { prefix: PREFIX },
      {
        onChanged: (change) => {
          changes.push(change);
          view.close();
          view.close();
        },
      },
    );
    const d = await discovery();
    d.push({ type: 'announced', namespace: at('alice') });
    d.push({ type: 'loaded' });
    await until(() => changes.length === 1, 'load');
    d.push({ type: 'announced', namespace: at('bob') });
    await pause();
    expect(changes).toHaveLength(1);
    expect([...view.present.keys()]).toEqual(['alice']);
  });

  it('loads a second view of a prefix already loaded on the connection, never inside publishingRoots()', async () => {
    const { subscriber, connections, discovery } = await start();
    const first = recorder();
    subscriber.publishingRoots({ prefix: PREFIX }, first.handlers);
    const d = await discovery();
    d.push({ type: 'announced', namespace: at('alice') });
    d.push({ type: 'loaded' });
    await until(() => first.changes.length === 1, 'first load');
    const second = recorder();
    const view = subscriber.publishingRoots({ prefix: PREFIX }, second.handlers);
    expect(second.changes).toEqual([]);
    expect(view.loaded).toBe(false);
    await until(() => second.changes.length === 1, 'second load');
    expect(summary(second.changes[0]!)).toEqual({ added: ['alice'], removed: [], present: ['alice'], initial: true });
    expect(connections[0]!.discoveries).toHaveLength(1);
  });

  it('ends silently when the supervisor closes', async () => {
    const { subscriber, discovery } = await start();
    const { changes, errors, handlers } = recorder();
    const view = subscriber.publishingRoots({ prefix: PREFIX }, handlers);
    const d = await discovery();
    d.push({ type: 'announced', namespace: at('alice') });
    d.push({ type: 'loaded' });
    await until(() => changes.length === 1, 'load');
    subscriber.close();
    d.push({ type: 'announced', namespace: at('bob') });
    await pause();
    expect(changes).toHaveLength(1);
    expect(errors).toEqual([]);
    expect(view.loaded).toBe(true);
    expect([...view.present.keys()]).toEqual(['alice']);
    expect(() => subscriber.publishingRoots({ prefix: PREFIX })).toThrow(RelayError);
  });

  it('checks its arguments at once', async () => {
    const { subscriber } = await start();
    expect(() => subscriber.publishingRoots(undefined as never)).toThrow(TypeError);
    expect(() => subscriber.publishingRoots({ prefix: [] })).toThrow(TypeError);
    expect(() => subscriber.publishingRoots({ prefix: Array.from({ length: 31 }, () => 'f') })).toThrow(RangeError);
    expect(() => subscriber.publishingRoots({ prefix: PREFIX }, { onChanged: async () => {} })).toThrow(TypeError);
  });
});

describe('a root leaving', () => {
  it('reports nothing when a rotation withdraws the only generation just before announcing the next', async () => {
    const { subscriber, discovery } = await start();
    const { changes, handlers } = recorder();
    const view = subscriber.publishingRoots({ prefix: PREFIX }, handlers);
    const d = await discovery();
    const old = newGeneration(1_000);
    d.push({ type: 'announced', namespace: at('alice', old) });
    d.push({ type: 'loaded' });
    await until(() => changes.length === 1, 'load');
    d.push({ type: 'unannounced', namespace: at('alice', old) });
    await pause(20);
    expect(view.present.has('alice')).toBe(true);
    d.push({ type: 'announced', namespace: at('alice', newGeneration(2_000)) });
    await pause(150);
    d.push({ type: 'announced', namespace: at('zed') });
    await until(() => changes.length === 2, 'the marker join');
    expect(summary(changes[1]!)).toEqual({ added: ['zed'], removed: [], present: ['alice', 'zed'], initial: false });
  });

  it('reports a withdrawal with nothing after it once, after the grace', async () => {
    const { subscriber, discovery } = await start();
    const { changes, handlers } = recorder();
    const view = subscriber.publishingRoots({ prefix: PREFIX }, handlers);
    const d = await discovery();
    const generation = newGeneration();
    d.push({ type: 'announced', namespace: at('alice', generation) });
    d.push({ type: 'loaded' });
    await until(() => changes.length === 1, 'load');
    const withdrawn = Date.now();
    d.push({ type: 'unannounced', namespace: at('alice', generation) });
    await pause();
    expect(changes).toHaveLength(1);
    expect(view.present).toBe(changes[0]!.present);
    await until(() => changes.length === 2, 'leave');
    expect(Date.now() - withdrawn).toBeGreaterThanOrEqual(90);
    expect(summary(changes[1]!)).toEqual({ added: [], removed: ['alice'], present: [], initial: false });
    await pause(150);
    expect(changes).toHaveLength(2);
  });

  it("reports a reconnect's additions and removals in one change, with no grace", async () => {
    const { connections, subscriber, discovery } = await start();
    const { changes, handlers } = recorder();
    subscriber.publishingRoots({ prefix: PREFIX }, handlers);
    const first = await discovery();
    for (const id of ['alice', 'bob', 'carol']) first.push({ type: 'announced', namespace: at(id) });
    first.push({ type: 'loaded' });
    await until(() => changes.length === 1, 'load');
    connections[0]!.end(new RelayError('TRANSPORT_ERROR', 'idle', 'connection'));
    const next = await discovery(1);
    next.push({ type: 'announced', namespace: at('carol') });
    next.push({ type: 'announced', namespace: at('dave') });
    next.push({ type: 'loaded' });
    await until(() => changes.length === 2, 'the difference');
    expect(summary(changes[1]!)).toEqual({
      added: ['dave'],
      removed: ['alice', 'bob'],
      present: ['carol', 'dave'],
      initial: false,
    });
    await pause(150);
    expect(changes).toHaveLength(2);
  });

  it('reports nothing for a root the old discovery still shows when the new snapshot misses its announce', async () => {
    const { subscriber, discovery } = await start();
    const { changes, handlers } = recorder();
    subscriber.publishingRoots({ prefix: PREFIX }, handlers);
    const first = await discovery();
    first.push({ type: 'announced', namespace: at('alice') });
    first.push({ type: 'loaded' });
    await until(() => changes.length === 1, 'load');
    subscriber.rotate();
    const next = await discovery(1);
    first.push({ type: 'announced', namespace: at('bob') });
    await until(() => changes.length === 2, 'bob joins on the old discovery');
    next.push({ type: 'announced', namespace: at('alice') });
    next.push({ type: 'loaded' });
    await pause(5);
    next.push({ type: 'announced', namespace: at('bob') });
    await pause(150);
    next.push({ type: 'announced', namespace: at('zed') });
    await until(() => changes.length === 3, 'the marker join');
    expect(summary(changes[2]!)).toEqual({
      added: ['zed'],
      removed: [],
      present: ['alice', 'bob', 'zed'],
      initial: false,
    });
  });

  it('reports a root the old discovery still shows as removed after the grace when the new one never does', async () => {
    const { subscriber, discovery } = await start();
    const { changes, handlers } = recorder();
    subscriber.publishingRoots({ prefix: PREFIX }, handlers);
    const first = await discovery();
    first.push({ type: 'announced', namespace: at('alice') });
    first.push({ type: 'announced', namespace: at('bob') });
    first.push({ type: 'loaded' });
    await until(() => changes.length === 1, 'load');
    subscriber.rotate();
    const next = await discovery(1);
    next.push({ type: 'announced', namespace: at('alice') });
    next.push({ type: 'announced', namespace: at('carol') });
    const loaded = Date.now();
    next.push({ type: 'loaded' });
    await until(() => changes.length === 2, 'the join at the load');
    expect(summary(changes[1]!)).toEqual({
      added: ['carol'],
      removed: [],
      present: ['alice', 'bob', 'carol'],
      initial: false,
    });
    await until(() => changes.length === 3, 'the leave');
    expect(Date.now() - loaded).toBeGreaterThanOrEqual(90);
    expect(summary(changes[2]!)).toEqual({ added: [], removed: ['bob'], present: ['alice', 'carol'], initial: false });
  });

  it("reports nothing when a reconnect's snapshot falls between a rotation's withdrawal and its announce", async () => {
    const { connections, subscriber, discovery } = await start();
    const { changes, handlers } = recorder();
    subscriber.publishingRoots({ prefix: PREFIX }, handlers);
    const first = await discovery();
    const old = newGeneration(1_000);
    first.push({ type: 'announced', namespace: at('alice', old) });
    first.push({ type: 'loaded' });
    await until(() => changes.length === 1, 'load');
    first.push({ type: 'unannounced', namespace: at('alice', old) });
    await pause(20);
    connections[0]!.end(new RelayError('TRANSPORT_ERROR', 'idle', 'connection'));
    const next = await discovery(1);
    next.push({ type: 'loaded' });
    await pause(5);
    next.push({ type: 'announced', namespace: at('alice', newGeneration(2_000)) });
    await pause(150);
    next.push({ type: 'announced', namespace: at('zed') });
    await until(() => changes.length === 2, 'the marker join');
    expect(summary(changes[1]!)).toEqual({ added: ['zed'], removed: [], present: ['alice', 'zed'], initial: false });
  });

  it('holds a leave whose grace runs out during an outage for the new load to settle', async () => {
    const { connections, subscriber, discovery } = await start();
    const { changes, handlers } = recorder();
    subscriber.publishingRoots({ prefix: PREFIX }, handlers);
    const first = await discovery();
    const old = newGeneration(1_000);
    first.push({ type: 'announced', namespace: at('alice', old) });
    first.push({ type: 'loaded' });
    await until(() => changes.length === 1, 'load');
    first.push({ type: 'unannounced', namespace: at('alice', old) });
    await pause(20);
    connections[0]!.end(new RelayError('TRANSPORT_ERROR', 'idle', 'connection'));
    const next = await discovery(1);
    await pause(150);
    next.push({ type: 'announced', namespace: at('alice', newGeneration(2_000)) });
    next.push({ type: 'loaded' });
    next.push({ type: 'announced', namespace: at('zed') });
    await until(() => changes.length === 2, 'the marker join');
    expect(summary(changes[1]!)).toEqual({ added: ['zed'], removed: [], present: ['alice', 'zed'], initial: false });
  });

  it('holds a pending leave while a covering request replaces the discovery, until it loads', async () => {
    const { subscriber, discovery } = await start();
    const { changes, handlers } = recorder();
    subscriber.publishingRoots({ prefix: PREFIX }, handlers);
    const narrow = await discovery();
    const old = newGeneration(1_000);
    narrow.push({ type: 'announced', namespace: at('alice', old) });
    narrow.push({ type: 'loaded' });
    await until(() => changes.length === 1, 'load');
    narrow.push({ type: 'unannounced', namespace: at('alice', old) });
    await pause(20);
    subscriber.publishingRoots({ prefix: ['room'] });
    const covering = await discovery(0, 2);
    expect(covering.prefix).toEqual(['room']);
    await pause(150);
    covering.push({ type: 'announced', namespace: at('alice', newGeneration(2_000)) });
    covering.push({ type: 'loaded' });
    covering.push({ type: 'announced', namespace: at('zed') });
    await until(() => changes.length === 2, 'the marker join');
    expect(summary(changes[1]!)).toEqual({ added: ['zed'], removed: [], present: ['alice', 'zed'], initial: false });
  });

  it('starts a pending leave again once the covering request loads without the root', async () => {
    const { subscriber, discovery } = await start();
    const { changes, handlers } = recorder();
    subscriber.publishingRoots({ prefix: PREFIX }, handlers);
    const narrow = await discovery();
    const generation = newGeneration();
    narrow.push({ type: 'announced', namespace: at('alice', generation) });
    narrow.push({ type: 'loaded' });
    await until(() => changes.length === 1, 'load');
    narrow.push({ type: 'unannounced', namespace: at('alice', generation) });
    await pause(20);
    subscriber.publishingRoots({ prefix: ['room'] });
    const covering = await discovery(0, 2);
    await pause(150);
    expect(changes).toHaveLength(1);
    covering.push({ type: 'loaded' });
    await until(() => changes.length === 2, 'the leave');
    expect(summary(changes[1]!)).toEqual({ added: [], removed: ['alice'], present: [], initial: false });
  });

  it('cancels a pending leave without reporting it when the view closes', async () => {
    const { subscriber, discovery } = await start();
    const { changes, handlers } = recorder();
    const view = subscriber.publishingRoots({ prefix: PREFIX }, handlers);
    const d = await discovery();
    d.push({ type: 'announced', namespace: at('alice') });
    d.push({ type: 'loaded' });
    await until(() => changes.length === 1, 'load');
    d.push({ type: 'unannounced', namespace: at('alice') });
    await pause();
    view.close();
    await pause(150);
    expect(changes).toHaveLength(1);
    expect([...view.present.keys()]).toEqual(['alice']);
  });
});

describe('a publishing-roots view over the relay wire, when the transport or the request stream is lost', () => {
  async function startOverWire() {
    const fakes: FakeTransport[] = [];
    const subscriber = await superviseRelaySubscriber(
      async (options) => {
        const fake = createFakeTransport();
        fakes.push(fake);
        const connecting = connectRelaySubscriber({
          ...options,
          createTransport: () => ({ transport: fake.transport, ready: Promise.resolve() }),
        });
        fake.sendServerSetup();
        return connecting;
      },
      { url: 'https://relay.example.com/moq' },
    );
    cleanup.push(() => subscriber.close());
    const { changes, handlers } = recorder();
    subscriber.publishingRoots({ prefix: PREFIX }, handlers);
    return { fakes, changes };
  }

  async function reconnectUnchanged(resetFirst: boolean) {
    const { fakes, changes } = await startOverWire();
    const first = await fakes[0]!.nextRequestStream();
    await first.firstMessage;
    await first.send(encodeRequestOk());
    await first.send(encodeNamespace(['alice', 'g1']));
    await first.send(encodeNamespace(['bob', 'g1']));
    await until(() => changes.length === 1, 'load');
    if (resetFirst) {
      first.reset(new Error('connection lost'));
      await pause(1);
    }
    fakes[0]!.dropTransport();
    await until(() => fakes.length === 2, 'reconnect', 3_000);
    const second = await fakes[1]!.nextRequestStream();
    await second.firstMessage;
    await second.send(encodeRequestOk());
    await second.send(encodeNamespace(['alice', 'g1']));
    await second.send(encodeNamespace(['bob', 'g1']));
    await pause(300);
    return changes.map(summary);
  }

  const loadedAliceAndBob = { added: ['alice', 'bob'], removed: [], present: ['alice', 'bob'], initial: true };

  it('reports nothing for an unchanged room after the transport drops', async () => {
    expect(await reconnectUnchanged(false)).toEqual([loadedAliceAndBob]);
  });

  it('reports nothing for an unchanged room when the request stream fails before the transport drops', async () => {
    expect(await reconnectUnchanged(true)).toEqual([loadedAliceAndBob]);
  });

  it('reports nothing for an unchanged room when the request stream resets with the connection up', async () => {
    const { fakes, changes } = await startOverWire();
    const first = await fakes[0]!.nextRequestStream();
    await first.firstMessage;
    await first.send(encodeRequestOk());
    await first.send(encodeNamespace(['alice', 'g1']));
    await until(() => changes.length === 1, 'load');
    first.reset(new Error('relay reset the request stream'));
    expect(await fakes[0]!.nextRequestStream()).toBe(first);
    const second = await fakes[0]!.nextRequestStream();
    await second.firstMessage;
    await second.send(encodeRequestOk());
    await second.send(encodeNamespace(['alice', 'g1']));
    await pause(300);
    expect(changes.map(summary)).toEqual([{ added: ['alice'], removed: [], present: ['alice'], initial: true }]);
  });

  it('holds a pending leave once the request stream fails, through the closure grace and the retry', async () => {
    const { fakes, changes } = await startOverWire();
    const first = await fakes[0]!.nextRequestStream();
    await first.firstMessage;
    await first.send(encodeRequestOk());
    await first.send(encodeNamespace(['alice', 'g1']));
    await until(() => changes.length === 1, 'load');
    await first.send(encodeNamespaceDone(['alice', 'g1']));
    await pause(20);
    first.reset(new Error('relay reset the request stream'));
    expect(await fakes[0]!.nextRequestStream()).toBe(first);
    const second = await fakes[0]!.nextRequestStream();
    await second.firstMessage;
    await second.send(encodeRequestOk());
    await second.send(encodeNamespace(['alice', 'g2']));
    await pause(300);
    expect(changes.map(summary)).toEqual([{ added: ['alice'], removed: [], present: ['alice'], initial: true }]);
  });

  it('reports nothing for an unchanged room when the relay finishes the request with the connection up', async () => {
    const { fakes, changes } = await startOverWire();
    const first = await fakes[0]!.nextRequestStream();
    await first.firstMessage;
    await first.send(encodeRequestOk());
    await first.send(encodeNamespace(['alice', 'g1']));
    await until(() => changes.length === 1, 'load');
    await first.fin();
    expect(await fakes[0]!.nextRequestStream()).toBe(first);
    const second = await fakes[0]!.nextRequestStream();
    await second.firstMessage;
    await second.send(encodeRequestOk());
    await second.send(encodeNamespace(['alice', 'g1']));
    await pause(300);
    expect(changes.map(summary)).toEqual([{ added: ['alice'], removed: [], present: ['alice'], initial: true }]);
  });
});

describe('one discovery per connection, shared by wants and publishing-roots views', () => {
  function wantRecorder() {
    const seen: string[] = [];
    const handlers: WantHandlers = {
      onSubscribed: (_, { namespace }) => seen.push(`subscribed ${namespace.join('/')}`),
      onObject: (object) => seen.push(`object ${object.groupId}`),
      onRestarted: ({ reason }) => seen.push(`restarted ${reason}`),
      onError: (error) => seen.push(`error ${String(error)}`),
    };
    return { seen, handlers };
  }

  it('serves a want under a publishing-roots prefix from the same request, without waiting for the load', async () => {
    const { subscriber, connections, discovery } = await start();
    const view = subscriber.publishingRoots({ prefix: PREFIX });
    const d = await discovery();
    const generation = newGeneration();
    d.push({ type: 'announced', namespace: at('alice', generation) });
    await pause();
    subscriber.want('alice', { namespace: at('alice'), track: 'audio', generations: true });
    await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
    expect(connections[0]!.subscriptions[0]!.options.namespace).toEqual(at('alice', generation));
    expect(connections[0]!.discoveries).toHaveLength(1);
    expect(view.loaded).toBe(false);
  });

  async function wantThenRoots() {
    const started = await start();
    const { subscriber, connections, discovery } = started;
    const want = wantRecorder();
    subscriber.want('alice', { namespace: at('alice'), track: 'audio', generations: true, retry: fast }, want.handlers);
    const narrow = await discovery();
    expect(narrow.prefix).toEqual(at('alice'));
    const generation = newGeneration();
    narrow.push({ type: 'announced', namespace: at('alice', generation) });
    await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
    const subscription = connections[0]!.subscriptions[0]!;
    subscription.accept();
    subscription.object(0);
    const roots = recorder();
    subscriber.publishingRoots({ prefix: PREFIX }, roots.handlers);
    const covering = await discovery(0, 2);
    expect(covering.prefix).toEqual(PREFIX);
    expect(narrow.closed).toBe(true);
    subscription.object(1);
    await pause();
    expect(subscription.cancelled).toBe(false);
    return { ...started, want, roots, covering, subscription, generation };
  }

  it("replaces a want's request with one covering request; the want stays subscribed until the load withdraws its generation", async () => {
    const { connections, want, roots, covering, subscription, generation } = await wantThenRoots();
    covering.push({ type: 'loaded' });
    await until(() => want.seen.includes('restarted generation'), 'the withdrawal reported at the load');
    expect(want.seen).toEqual([
      `subscribed ${at('alice', generation).join('/')}`,
      'object 0',
      'object 1',
      'restarted generation',
    ]);
    expect(subscription.cancelled).toBe(true);
    expect(connections[0]!.discoveries).toHaveLength(2);
    await until(() => roots.changes.length === 1, 'publishing-roots load');
    expect(summary(roots.changes[0]!)).toEqual({ added: [], removed: [], present: [], initial: true });
  });

  const unauthorized = () =>
    new SubscriptionRequestError({ errorCode: 0x1, retryInterval: 0, reason: 'token does not allow the room' });
  const failures = (seen: string[]) => seen.filter((s) => s.startsWith('error') || s.startsWith('restarted'));

  it('keeps a want under the prefix delivering when the covering publishing-roots request is refused', async () => {
    const { want, roots, covering, subscription } = await wantThenRoots();
    covering.fail(unauthorized());
    await until(() => roots.errors.length === 1, 'publishing-roots onError');
    await pause(100);
    subscription.object(2);
    await pause();
    expect(failures(want.seen)).toEqual([]);
    expect(want.seen.at(-1)).toBe('object 2');
    expect(subscription.cancelled).toBe(false);
  });

  it('keeps a want served from a publishing-roots request delivering when that request is refused', async () => {
    const { subscriber, connections, discovery } = await start();
    const roots = recorder();
    subscriber.publishingRoots({ prefix: PREFIX }, roots.handlers);
    const wide = await discovery();
    wide.push({ type: 'announced', namespace: at('alice', newGeneration()) });
    await pause();
    const want = wantRecorder();
    subscriber.want('alice', { namespace: at('alice'), track: 'audio', generations: true, retry: fast }, want.handlers);
    await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
    const subscription = connections[0]!.subscriptions[0]!;
    subscription.accept();
    subscription.object(0);
    await pause();
    wide.fail(unauthorized());
    await until(() => roots.errors.length === 1, 'publishing-roots onError');
    await pause(100);
    subscription.object(1);
    await pause();
    expect(failures(want.seen)).toEqual([]);
    expect(want.seen.at(-1)).toBe('object 1');
    expect(subscription.cancelled).toBe(false);
    expect(connections[0]!.discoveries.map((d) => d.prefix)).toEqual([PREFIX, at('alice')]);
  });

  it('never restarts a want under the prefix while the publishing-roots request is refused again and again', async () => {
    const { subscriber, connections } = await start();
    const want = wantRecorder();
    subscriber.want('alice', { namespace: at('alice'), track: 'audio', generations: true, retry: fast }, want.handlers);
    const generation = newGeneration();
    const served = new Set<object>();
    let refusals = 0;
    const relay = setInterval(() => {
      for (const d of connections[0]!.discoveries) {
        if (d.closed || served.has(d)) continue;
        served.add(d);
        if (d.prefix.length === PREFIX.length) {
          refusals += 1;
          d.fail(new SubscriptionRequestError({ errorCode: 0x9, retryInterval: 0, reason: 'excessive load' }));
        } else {
          d.push({ type: 'announced', namespace: at('alice', generation) });
          d.push({ type: 'loaded' });
        }
      }
      for (const s of connections[0]!.subscriptions) {
        if (served.has(s)) continue;
        served.add(s);
        s.accept();
        s.object(0);
      }
    }, 1);
    cleanup.push(() => clearInterval(relay));
    await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
    subscriber.publishingRoots({ prefix: PREFIX });
    await until(() => refusals >= 3, 'three refusals', 5_000);
    await pause();
    expect(failures(want.seen)).toEqual([]);
    expect(connections[0]!.subscriptions).toHaveLength(1);
  });

  it('reports nothing for a held namespace the covering request confirms', async () => {
    const { connections, want, roots, covering, subscription, generation } = await wantThenRoots();
    covering.push({ type: 'announced', namespace: at('alice', generation) });
    covering.push({ type: 'loaded' });
    await until(() => roots.changes.length === 1, 'publishing-roots load');
    expect(summary(roots.changes[0]!)).toEqual({ added: ['alice'], removed: [], present: ['alice'], initial: true });
    await pause();
    expect(want.seen).toEqual([`subscribed ${at('alice', generation).join('/')}`, 'object 0', 'object 1']);
    expect(subscription.cancelled).toBe(false);
    expect(connections[0]!.subscriptions).toHaveLength(1);
  });
});

describe('the publishing-roots reducer', () => {
  it('maps a namespace to a root and the field it adds', () => {
    expect(rootField(PREFIX, at('alice'))).toEqual({ id: 'alice', field: '' });
    expect(rootField(PREFIX, at('alice', 'g1'))).toEqual({ id: 'alice', field: 'g1' });
    expect(rootField(PREFIX, PREFIX)).toBeUndefined();
    expect(rootField(PREFIX, at('alice', 'g1', 'x'))).toBeUndefined();
  });

  it('adds a root on its first field and removes it on its last', () => {
    const presence: Presence = new Map();
    expect(applyNamespace(presence, PREFIX, 'announced', at('alice', 'g1'))).toEqual({ kind: 'added', id: 'alice' });
    expect(applyNamespace(presence, PREFIX, 'announced', at('alice', 'g2'))).toBeUndefined();
    expect(applyNamespace(presence, PREFIX, 'unannounced', at('alice', 'g1'))).toBeUndefined();
    expect(applyNamespace(presence, PREFIX, 'unannounced', at('alice', 'g1'))).toBeUndefined();
    expect(applyNamespace(presence, PREFIX, 'unannounced', at('alice', 'g2'))).toEqual({
      kind: 'removed',
      id: 'alice',
    });
    expect(presence.size).toBe(0);
  });
});

describe('the publishing-roots transition', () => {
  const connection = (): PublishingRootsConnection => ({
    discovery: undefined as unknown as DiscoveryIndex,
    signal: new AbortController().signal,
    retiring: new AbortController().signal,
  });
  const loading: PublishingRootsPhase = { kind: 'loading', candidate: undefined, failures: 0 };

  function loaded(...roots: string[]) {
    const on = connection();
    const opened = transition(loading, { kind: 'connection', connection: on }, PREFIX);
    const source = (opened.effects[0] as { source: Source }).source;
    let phase = opened.phase;
    for (const id of roots) {
      phase = transition(phase, { kind: 'namespace', source, type: 'announced', namespace: at(id) }, PREFIX).phase;
    }
    const step = transition(phase, { kind: 'loaded', source }, PREFIX);
    return { on, source, step };
  }

  it('opens on a connection and emits the load once', () => {
    const { source, step } = loaded('alice');
    expect(step.phase.kind).toBe('live');
    expect(step.effects).toEqual([
      {
        kind: 'emit',
        change: expect.objectContaining({ initial: true, added: [expect.objectContaining({ id: 'alice' })] }),
      },
    ]);
    expect(transition(step.phase, { kind: 'loaded', source }, PREFIX).effects).toEqual([]);
  });

  it('ignores an input about a source the phase no longer holds', () => {
    const { step } = loaded('alice');
    const stranger: Source = { connection: connection(), presence: new Map() };
    const next = transition(
      step.phase,
      { kind: 'namespace', source: stranger, type: 'announced', namespace: at('bob') },
      PREFIX,
    );
    expect(next).toEqual({ phase: step.phase, effects: [] });
  });

  it('follows a rotation make-before-break; a root the old discovery shows and the new load misses leaves after its grace', () => {
    const { on, source, step } = loaded('alice', 'bob');
    const to = connection();
    const retired = transition(step.phase, { kind: 'retiring', connection: on, next: to }, PREFIX);
    expect(retired.phase.kind).toBe('replacing');
    const candidate = (retired.effects[0] as { source: Source }).source;
    expect(retired.effects).toEqual([{ kind: 'open', source: candidate }]);
    let phase = retired.phase;
    phase = transition(
      phase,
      { kind: 'namespace', source: candidate, type: 'announced', namespace: at('alice') },
      PREFIX,
    ).phase;
    const done = transition(phase, { kind: 'loaded', source: candidate }, PREFIX);
    expect(done.phase).toMatchObject({ kind: 'live', source: candidate, leaving: new Set(['bob']) });
    expect(done.effects).toEqual([
      { kind: 'release', source },
      { kind: 'schedule-leave', id: 'bob' },
    ]);
    const due = transition(done.phase, { kind: 'leave-due', id: 'bob', following: () => true }, PREFIX);
    expect(due.effects).toEqual([
      {
        kind: 'emit',
        change: expect.objectContaining({ added: [], removed: [expect.objectContaining({ id: 'bob' })] }),
      },
    ]);
  });

  it("settles every pending leave by a new load's snapshot: one it shows stays, one it does not keeps its grace", () => {
    const { on, source, step } = loaded('alice', 'bob', 'carol');
    const withdraw = (phase: PublishingRootsPhase, id: string) =>
      transition(phase, { kind: 'namespace', source, type: 'unannounced', namespace: at(id) }, PREFIX).phase;
    const leaving = withdraw(withdraw(step.phase, 'alice'), 'bob');
    const to = connection();
    const retired = transition(leaving, { kind: 'retiring', connection: on, next: to }, PREFIX);
    const candidate = (retired.effects[0] as { source: Source }).source;
    let phase = retired.phase;
    for (const id of ['alice', 'carol']) {
      phase = transition(
        phase,
        { kind: 'namespace', source: candidate, type: 'announced', namespace: at(id) },
        PREFIX,
      ).phase;
    }
    const done = transition(phase, { kind: 'loaded', source: candidate }, PREFIX);
    expect(done.phase).toMatchObject({ kind: 'live', leaving: new Set(['bob']) });
    expect(done.effects).toEqual([
      { kind: 'release', source },
      { kind: 'cancel-leave', id: 'alice' },
      { kind: 'schedule-leave', id: 'bob' },
    ]);
    const due = transition(done.phase, { kind: 'leave-due', id: 'bob', following: () => true }, PREFIX);
    const emitted = due.effects.at(-1) as { change: PublishingRootsChange };
    expect(ids(emitted.change.removed)).toEqual(['bob']);
    expect([...emitted.change.present.keys()]).toEqual(['alice', 'carol']);
  });

  it('on a reconnect, reports a root shown when the old discovery ended as removed at once, and holds one in its grace', () => {
    const { source, step } = loaded('alice', 'bob');
    const withdrawn = transition(
      step.phase,
      { kind: 'namespace', source, type: 'unannounced', namespace: at('bob') },
      PREFIX,
    ).phase;
    const ended = transition(
      withdrawn,
      { kind: 'ended', source, error: new SubscriptionConnectionClosedError() },
      PREFIX,
    );
    expect(ended.phase).toMatchObject({ kind: 'replacing', source: undefined });
    const opened = transition(ended.phase, { kind: 'connection', connection: connection() }, PREFIX);
    const candidate = (opened.effects[0] as { source: Source }).source;
    const done = transition(opened.phase, { kind: 'loaded', source: candidate }, PREFIX);
    expect(done.phase).toMatchObject({ kind: 'live', leaving: new Set(['bob']) });
    expect(done.effects).toEqual([
      { kind: 'schedule-leave', id: 'bob' },
      {
        kind: 'emit',
        change: expect.objectContaining({ added: [], removed: [expect.objectContaining({ id: 'alice' })] }),
      },
    ]);
  });

  it('holds a root whose last namespace went until its grace runs out, and cancels it if one comes back', () => {
    const { source, step } = loaded('alice');
    const namespace = (type: 'announced' | 'unannounced', ...fields: string[]) =>
      ({ kind: 'namespace', source, type, namespace: at(...fields) }) as const;
    const gone = transition(step.phase, namespace('unannounced', 'alice'), PREFIX);
    expect(gone.effects).toEqual([{ kind: 'schedule-leave', id: 'alice' }]);
    const back = transition(gone.phase, namespace('announced', 'alice', 'g2'), PREFIX);
    expect(back.effects).toEqual([{ kind: 'cancel-leave', id: 'alice' }]);
    expect(transition(back.phase, { kind: 'leave-due', id: 'alice', following: () => true }, PREFIX).effects).toEqual(
      [],
    );
    const closed = transition(gone.phase, { kind: 'close' }, PREFIX);
    expect(closed.effects).toEqual([
      { kind: 'release', source },
      { kind: 'cancel-leave', id: 'alice' },
    ]);
  });

  it('waits for a connection, retries with backoff, or fails, by how discovery ended', () => {
    const { on, source, step } = loaded('alice');
    const ended = (error: unknown) => transition(step.phase, { kind: 'ended', source, error }, PREFIX);
    expect(ended(new SubscriptionConnectionClosedError()).effects).toEqual([
      { kind: 'release', source },
      { kind: 'await-connection', after: on },
    ]);
    const retried = ended(undefined);
    expect(retried.phase).toMatchObject({ kind: 'replacing', failures: 1 });
    expect(retried.effects).toEqual([
      { kind: 'release', source },
      { kind: 'retry', attempt: 0 },
    ]);
    const refusal = new SubscriptionRequestError({ errorCode: 0x1, retryInterval: 0, reason: 'unauthorized' });
    const failed = ended(refusal);
    expect(failed.phase).toMatchObject({ kind: 'closed' });
    expect(failed.effects.at(-1)).toEqual({ kind: 'error', error: refusal });
  });

  it('does nothing once closed', () => {
    const { source, step } = loaded('alice');
    const closed = transition(step.phase, { kind: 'close' }, PREFIX);
    expect(closed.effects).toEqual([{ kind: 'release', source }]);
    expect(transition(closed.phase, { kind: 'connection', connection: connection() }, PREFIX).effects).toEqual([]);
    expect(transition(closed.phase, { kind: 'close' }, PREFIX).effects).toEqual([]);
  });
});
