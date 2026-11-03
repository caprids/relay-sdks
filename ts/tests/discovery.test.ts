import { afterEach, describe, expect, it, vi } from 'vitest';
import { covers, createDiscoveryIndex, DISCOVERY_LINGER_MS, type DiscoveryWatchHandlers } from '../src/discovery.js';
import { RelayError } from '../src/errors.js';
import type { NamespaceEvents } from '../src/namespace-events.js';
import { SubscriptionConnectionClosedError, SubscriptionRequestError } from '../src/subscription.js';
import { type FakeSubscriber, fakeSubscriber } from './helpers/supervision-fakes.js';

afterEach(() => {
  vi.useRealTimers();
});

const settle = async () => {
  for (let i = 0; i < 10; i++) await Promise.resolve();
};

function scripted() {
  const fake = fakeSubscriber();
  const refuse = new Set<string>();
  const claimed = new Set<FakeSubscriber['discoveries'][number]>();
  const index = createDiscoveryIndex((prefix): NamespaceEvents => {
    if (refuse.has(JSON.stringify(prefix))) throw new RelayError('CLOSED', 'Relay connection is closed', 'connection');
    for (const held of claimed) {
      if (!held.closed && (covers(held.prefix, prefix) || covers(prefix, held.prefix))) {
        throw new RelayError('PREFIX_OVERLAP', 'overlap', 'subscription');
      }
    }
    const events = fake.namespaceEvents({ prefix });
    const discovery = fake.discoveries.at(-1)!;
    claimed.add(discovery);
    const next = events.next.bind(events);
    events.next = async () => {
      try {
        return await next();
      } catch (error) {
        claimed.delete(discovery);
        throw error;
      }
    };
    return events;
  });
  return { fake, index, refuse };
}

function heard() {
  const seen: string[] = [];
  const handlers: DiscoveryWatchHandlers = {
    onAnnounced: (namespace) => seen.push(`+${namespace.join('/')}`),
    onUnannounced: (namespace) => seen.push(`-${namespace.join('/')}`),
    onLoaded: () => seen.push('loaded'),
    onEnded: (error) => seen.push(`ended ${error instanceof Error ? error.name : String(error)}`),
  };
  return { seen, handlers };
}

describe('the discovery index', () => {
  it('serves a watch under an open request by filtering it', async () => {
    const { fake, index } = scripted();
    const wide = heard();
    index.watch(['r'], wide.handlers);
    fake.discoveries[0]!.push({ type: 'announced', namespace: ['r', 'a', 'g1'] });
    fake.discoveries[0]!.push({ type: 'announced', namespace: ['r', 'b', 'g1'] });
    await settle();
    const narrow = heard();
    const watch = index.watch(['r', 'a'], narrow.handlers);
    expect(watch.namespaces()).toEqual([['r', 'a', 'g1']]);
    expect(watch.loaded).toBe(false);
    fake.discoveries[0]!.push({ type: 'announced', namespace: ['r', 'a', 'g2'] });
    fake.discoveries[0]!.push({ type: 'announced', namespace: ['r', 'b', 'g2'] });
    fake.discoveries[0]!.push({ type: 'loaded' });
    await settle();
    expect(fake.discoveries).toHaveLength(1);
    expect(narrow.seen).toEqual(['+r/a/g2', 'loaded']);
    expect(wide.seen).toEqual(['+r/a/g1', '+r/b/g1', '+r/a/g2', '+r/b/g2', 'loaded']);
    expect(watch.loaded).toBe(true);
  });

  it('replaces requests under a wider watch, break before make, and reports only the differences at its load', async () => {
    const { fake, index } = scripted();
    const narrow = heard();
    const moved = index.watch(['r', 'a'], narrow.handlers);
    const first = fake.discoveries[0]!;
    first.push({ type: 'announced', namespace: ['r', 'a', 'g1'] });
    first.push({ type: 'announced', namespace: ['r', 'a', 'g2'] });
    first.push({ type: 'loaded' });
    await settle();
    const wide = heard();
    const covering = index.watch(['r'], wide.handlers);
    expect(first.closed).toBe(true);
    expect(fake.discoveries[1]!.prefix).toEqual(['r']);
    expect(covering.namespaces()).toEqual([
      ['r', 'a', 'g1'],
      ['r', 'a', 'g2'],
    ]);
    expect(moved.loaded).toBe(true);
    const second = fake.discoveries[1]!;
    second.push({ type: 'announced', namespace: ['r', 'a', 'g1'] });
    second.push({ type: 'announced', namespace: ['r', 'a', 'g3'] });
    second.push({ type: 'loaded' });
    await settle();
    expect(narrow.seen).toEqual(['+r/a/g1', '+r/a/g2', 'loaded', '+r/a/g3', '-r/a/g2']);
    expect(wide.seen).toEqual(['+r/a/g3', '-r/a/g2', 'loaded']);
  });

  it('ends the watches it moved, after the call, when the covering request cannot open', async () => {
    const { fake, index, refuse } = scripted();
    const narrow = heard();
    const moved = index.watch(['r', 'a'], narrow.handlers);
    fake.discoveries[0]!.push({ type: 'loaded' });
    await settle();
    refuse.add(JSON.stringify(['r']));
    expect(() => index.watch(['r'], {})).toThrow(RelayError);
    expect(narrow.seen).toEqual(['loaded']);
    expect(moved.ended?.error).toBeInstanceOf(SubscriptionConnectionClosedError);
    await settle();
    expect(narrow.seen).toEqual(['loaded', 'ended SubscriptionConnectionClosedError']);
  });

  it('reports a request ending to its watches, and asks again for the next watch', async () => {
    const { fake, index } = scripted();
    const first = heard();
    const watch = index.watch(['r'], first.handlers);
    const refusal = new SubscriptionRequestError({ errorCode: 0x1, retryInterval: 0, reason: 'unauthorized' });
    fake.discoveries[0]!.fail(refusal);
    await settle();
    expect(first.seen).toEqual(['ended SubscriptionRequestError']);
    expect(watch.ended?.error).toBe(refusal);
    index.watch(['r'], {});
    expect(fake.discoveries).toHaveLength(2);
  });

  it('moves the narrower watches a request served to a request of their own when it ends, holding their view', async () => {
    const { fake, index } = scripted();
    const narrow = heard();
    const moved = index.watch(['r', 'a'], narrow.handlers);
    fake.discoveries[0]!.push({ type: 'announced', namespace: ['r', 'a', 'g1'] });
    fake.discoveries[0]!.push({ type: 'loaded' });
    await settle();
    const wide = heard();
    index.watch(['r'], wide.handlers);
    const deeper = heard();
    const filtered = index.watch(['r', 'a', 'g2'], deeper.handlers);
    const covering = fake.discoveries[1]!;
    covering.push({ type: 'announced', namespace: ['r', 'a', 'g1'] });
    covering.push({ type: 'announced', namespace: ['r', 'a', 'g2'] });
    covering.push({ type: 'announced', namespace: ['r', 'b', 'g1'] });
    await settle();
    covering.fail(new SubscriptionRequestError({ errorCode: 0x1, retryInterval: 0, reason: 'unauthorized' }));
    await settle();
    expect(wide.seen).toEqual(['+r/a/g2', '+r/b/g1', 'ended SubscriptionRequestError']);
    expect(fake.discoveries.map((discovery) => discovery.prefix)).toEqual([['r', 'a'], ['r'], ['r', 'a']]);
    expect(moved.ended).toBeUndefined();
    expect(moved.loaded).toBe(true);
    expect(moved.namespaces()).toEqual([
      ['r', 'a', 'g1'],
      ['r', 'a', 'g2'],
    ]);
    expect(filtered.loaded).toBe(false);
    const rehomed = fake.discoveries[2]!;
    rehomed.push({ type: 'announced', namespace: ['r', 'a', 'g2'] });
    rehomed.push({ type: 'loaded' });
    await settle();
    expect(narrow.seen).toEqual(['+r/a/g1', 'loaded', '+r/a/g2', '-r/a/g1']);
    expect(deeper.seen).toEqual(['+r/a/g2', 'loaded']);
  });

  it('ends a narrower watch whose own request cannot open after the wider one ended', async () => {
    const { fake, index, refuse } = scripted();
    const wide = heard();
    index.watch(['r'], wide.handlers);
    const narrow = heard();
    const watch = index.watch(['r', 'a'], narrow.handlers);
    refuse.add(JSON.stringify(['r', 'a']));
    fake.discoveries[0]!.fail(
      new SubscriptionRequestError({ errorCode: 0x1, retryInterval: 0, reason: 'unauthorized' }),
    );
    await settle();
    expect(wide.seen).toEqual(['ended SubscriptionRequestError']);
    expect(narrow.seen).toEqual(['ended SubscriptionConnectionClosedError']);
    expect(watch.ended?.error).toBeInstanceOf(SubscriptionConnectionClosedError);
  });

  it('keeps a request a while after its last watch, for reuse, then closes it', () => {
    vi.useFakeTimers();
    const { fake, index } = scripted();
    index.watch(['r'], {}).release();
    vi.advanceTimersByTime(DISCOVERY_LINGER_MS - 1);
    const again = index.watch(['r', 'a'], {});
    expect(fake.discoveries).toHaveLength(1);
    again.release();
    again.release();
    vi.advanceTimersByTime(DISCOVERY_LINGER_MS);
    expect(fake.discoveries[0]!.closed).toBe(true);
    index.watch(['r'], {});
    expect(fake.discoveries).toHaveLength(2);
  });

  it('ends every watch when the connection ends, and refuses new ones', () => {
    const { fake, index } = scripted();
    const one = heard();
    const two = heard();
    index.watch(['r'], one.handlers);
    index.watch(['s'], two.handlers);
    index.close();
    index.close();
    expect(one.seen).toEqual(['ended SubscriptionConnectionClosedError']);
    expect(two.seen).toEqual(['ended SubscriptionConnectionClosedError']);
    expect(fake.discoveries.every((discovery) => discovery.closed)).toBe(true);
    expect(() => index.watch(['r'], {})).toThrow(RelayError);
  });

  it('isolates a handler that throws', async () => {
    const { fake, index } = scripted();
    const logged: unknown[] = [];
    const original = console.error;
    console.error = (error: unknown) => logged.push(error);
    try {
      index.watch(['r'], {
        onAnnounced: () => {
          throw new Error('boom');
        },
      });
      const other = heard();
      index.watch(['r', 'a'], other.handlers);
      fake.discoveries[0]!.push({ type: 'announced', namespace: ['r', 'a', 'g1'] });
      await settle();
      expect(other.seen).toEqual(['+r/a/g1']);
      expect(logged).toHaveLength(1);
    } finally {
      console.error = original;
    }
  });
});
