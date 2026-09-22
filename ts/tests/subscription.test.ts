/**
 * The subscription through its own interface: a fake core subscribe and a fake closure observer,
 * recording consumers. Relay behaviour over a real connection stays in connect.test.ts and
 * subscription-events.test.ts.
 */
import type { RequestError, SubscriptionHandlers } from '@mux/moq-core';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  closureObservers,
  createSubscription,
  type SubscriptionConsumer,
  type SubscriptionEnd,
  type SubscriptionEvent,
  synchronousCallback,
} from '../src/subscription.js';

afterEach(() => {
  vi.restoreAllMocks();
});

function recorder() {
  const events: SubscriptionEvent[] = [];
  const endings: SubscriptionEnd[] = [];
  const consumer: SubscriptionConsumer = {
    event: (event) => events.push(event),
    ended: (end) => endings.push(end),
  };
  return { consumer, events, endings };
}

function harness(options: { closedAlready?: unknown; subscribeThrows?: unknown } = {}) {
  let handlers: SubscriptionHandlers | undefined;
  let observer: ((error?: unknown) => void) | undefined;
  const cancel = vi.fn();
  const update = vi.fn();
  const remove = vi.fn();
  const subscribe = vi.fn((value: SubscriptionHandlers) => {
    if ('subscribeThrows' in options) throw options.subscribeThrows;
    handlers = value;
    return { requestId: 7, cancel, update };
  });
  const observeClosed = vi.fn((value: (error?: unknown) => void) => {
    if ('closedAlready' in options) value(options.closedAlready);
    else observer = value;
    return remove;
  });
  return {
    deps: { subscribe, observeClosed },
    core: () => {
      if (!handlers) throw new Error('core subscribe was not called');
      return handlers;
    },
    closeConnection: (error?: unknown) => observer?.(error),
    cancel,
    update,
    remove,
    subscribe,
  };
}

const requestError: RequestError = { errorCode: 404, retryInterval: 0, reason: 'missing' };
const ok = { trackAlias: 11, parameters: {}, trackProperties: [] } as unknown as Parameters<
  NonNullable<SubscriptionHandlers['onOk']>
>[0];

describe('subscription: translating the core once', () => {
  it('delivers each core callback as one product event to every consumer, synchronously, and tracks acceptance', () => {
    const h = harness();
    const first = recorder();
    const second = recorder();
    const subscription = createSubscription(h.deps, [first.consumer]);
    subscription.attach(second.consumer);
    expect(subscription.requestId).toBe(7);
    expect(subscription.accepted).toBe(false);
    const core = h.core();
    core.onOk?.(ok);
    expect(subscription.accepted).toBe(true);
    core.onObject?.({ groupId: 1 } as never);
    core.onDone?.({ statusCode: 0, streamCount: 1, reason: '' });
    core.onSubgroupEnd?.({ groupId: 1, subgroupId: 0, endOfGroup: true });
    core.onSubgroupReset?.({ groupId: 2, error: 'gap' });
    core.onStateNotify?.({});
    core.onGoaway?.({ newSessionUri: '' } as never);
    core.onUpdateOk?.();
    const types = first.events.map((event) => event.type);
    expect(types).toEqual([
      'subscribed',
      'object',
      'publisherDone',
      'subgroupEnd',
      'subgroupReset',
      'stateNotify',
      'goaway',
      'updateOk',
    ]);
    expect(second.events.map((event) => event.type)).toEqual(types);
    expect(first.endings).toEqual([]);
    expect(subscription.end).toBeUndefined();
  });

  it('forwards update() to the core handle', () => {
    const h = harness();
    const subscription = createSubscription(h.deps);
    subscription.update({ forward: 0 } as never);
    expect(h.update).toHaveBeenCalledWith({ forward: 0 });
  });

  it('stops delivering to a detached consumer', () => {
    const h = harness();
    const first = recorder();
    const subscription = createSubscription(h.deps);
    const detach = subscription.attach(first.consumer);
    h.core().onUpdateOk?.();
    detach();
    h.core().onUpdateOk?.();
    expect(first.events).toHaveLength(1);
  });
});

describe('subscription: endings', () => {
  it.each([
    ['before', false],
    ['after', true],
  ])(
    'records a REQUEST_ERROR %s acceptance, cancels the core, releases the closure observer, and drops later events',
    (_, accepted) => {
      const h = harness();
      const r = recorder();
      const subscription = createSubscription(h.deps, [r.consumer]);
      if (accepted) h.core().onOk?.(ok);
      h.core().onError?.(requestError);
      expect(r.endings).toEqual([{ cause: 'request-error', accepted, error: requestError }]);
      expect(subscription.end).toEqual(r.endings[0]);
      expect(h.cancel).toHaveBeenCalledExactlyOnceWith(requestError);
      expect(h.remove).toHaveBeenCalledOnce();
      h.core().onUpdateOk?.();
      h.core().onError?.(requestError);
      expect(r.events).toEqual(accepted ? [{ type: 'subscribed', value: ok }] : []);
      expect(r.endings).toHaveLength(1);
    },
  );

  it.each([
    ['before', false],
    ['after', true],
  ])(
    'records connection closure %s acceptance with the connection error and cancels the core with it',
    (_, accepted) => {
      const h = harness();
      const r = recorder();
      createSubscription(h.deps, [r.consumer]);
      if (accepted) h.core().onOk?.(ok);
      const error = new Error('lost');
      h.closeConnection(error);
      expect(r.endings).toEqual([{ cause: 'connection-closed', accepted, error }]);
      expect(h.cancel).toHaveBeenCalledExactlyOnceWith(error);
    },
  );

  it('records a local cancel with its reason once, idempotently, and tells consumers attached afterwards', () => {
    const h = harness();
    const r = recorder();
    const late = recorder();
    const subscription = createSubscription(h.deps, [r.consumer]);
    h.core().onOk?.(ok);
    const reason = new Error('done with it');
    subscription.cancel(reason);
    subscription.cancel(new Error('again'));
    expect(r.endings).toEqual([{ cause: 'cancelled', accepted: true, error: reason }]);
    expect(h.cancel).toHaveBeenCalledExactlyOnceWith(reason);
    expect(h.remove).toHaveBeenCalledOnce();
    const detach = subscription.attach(late.consumer);
    expect(late.endings).toEqual(r.endings);
    detach();
  });

  it('ends before sending when the connection had already closed, and refuses the handle', () => {
    const h = harness({ closedAlready: new Error('gone') });
    const r = recorder();
    const subscription = createSubscription(h.deps, [r.consumer]);
    expect(h.subscribe).not.toHaveBeenCalled();
    expect(r.endings).toEqual([{ cause: 'connection-closed', accepted: false, error: new Error('gone') }]);
    expect(() => subscription.requestId).toThrow('gone');
  });

  it('releases the closure observer, tells consumers, and rethrows when the core subscribe throws', () => {
    const h = harness({ subscribeThrows: new Error('bad options') });
    const r = recorder();
    expect(() => createSubscription(h.deps, [r.consumer])).toThrow('bad options');
    expect(h.remove).toHaveBeenCalledOnce();
    expect(r.endings).toEqual([{ cause: 'cancelled', accepted: false, error: new Error('bad options') }]);
  });

  it('cancels the core handle when the source ends the subscription synchronously during subscribe', () => {
    const cancel = vi.fn();
    const deps = {
      subscribe: (handlers: SubscriptionHandlers) => {
        handlers.onError?.(requestError);
        return { requestId: 1, cancel, update() {} };
      },
      observeClosed: () => () => {},
    };
    const r = recorder();
    const subscription = createSubscription(deps, [r.consumer]);
    expect(r.endings).toEqual([{ cause: 'request-error', accepted: false, error: requestError }]);
    expect(cancel).toHaveBeenCalledExactlyOnceWith(requestError);
    expect(subscription.requestId).toBe(1);
  });
});

describe('closure observers', () => {
  it('fans one connection promise out and forgets removed observers', async () => {
    let end!: (value: { error?: unknown }) => void;
    const observe = closureObservers(
      new Promise((resolve) => {
        end = resolve;
      }),
    );
    const removed = vi.fn();
    const live = vi.fn();
    observe(removed)();
    observe(live);
    end({ error: 'lost' });
    await Promise.resolve();
    expect(removed).not.toHaveBeenCalled();
    expect(live).toHaveBeenCalledExactlyOnceWith('lost');
    const late = vi.fn();
    observe(late);
    expect(late).toHaveBeenCalledExactlyOnceWith('lost');
  });
});

describe('synchronous callbacks', () => {
  it('refuses an async function before anything is sent', () => {
    expect(() => synchronousCallback('onObject', async () => {}, undefined)).toThrow('synchronous');
  });

  it('preserves the receiver and diagnoses a returned promise once without an unhandled rejection', async () => {
    const diagnostic = vi.spyOn(console, 'error').mockImplementation(() => {});
    const receiver = {
      calls: 0,
      onObject() {
        this.calls++;
        return Promise.reject(new Error('async failure'));
      },
    };
    const callback = synchronousCallback('onObject', receiver.onObject, receiver);
    callback(1);
    callback(2);
    await Promise.resolve();
    expect(receiver.calls).toBe(2);
    expect(diagnostic).toHaveBeenCalledOnce();
    expect(diagnostic.mock.calls[0]?.[0]).toBeInstanceOf(TypeError);
  });
});
