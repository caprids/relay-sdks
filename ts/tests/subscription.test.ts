/**
 * The subscription through its own interface: a fake core subscribe and a fake closure observer,
 * recording consumers. Relay behaviour over a real connection stays in connect.test.ts and
 * subscription-events.test.ts.
 */
import type { MessageParameters, RequestError, SubscriptionHandlers } from '@mux/moq-core';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  closureObservers,
  createSubscription,
  SubscriptionConnectionClosedError,
  type SubscriptionConsumer,
  type SubscriptionEnd,
  type SubscriptionEvent,
  SubscriptionRequestError,
  synchronousCallback,
  UPDATE_QUEUE_BOUND,
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

  it('forwards update() to the core handle once the subscription is accepted', () => {
    const h = harness();
    const subscription = createSubscription(h.deps);
    h.core().onOk?.(ok);
    subscription.update({ forward: 0 });
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
      expect(r.endings).toEqual([
        { kind: accepted ? 'terminated' : 'rejected', cause: 'request-error', accepted, error: requestError },
      ]);
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
      expect(r.endings).toEqual([{ kind: 'terminated', cause: 'connection-closed', accepted, error }]);
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
    expect(r.endings).toEqual([{ kind: 'cancelled', cause: 'cancelled', accepted: true, error: reason }]);
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
    expect(r.endings).toEqual([
      { kind: 'terminated', cause: 'connection-closed', accepted: false, error: new Error('gone') },
    ]);
    expect(() => subscription.requestId).toThrow('gone');
  });

  it('releases the closure observer, tells consumers, and rethrows when the core subscribe throws', () => {
    const h = harness({ subscribeThrows: new Error('bad options') });
    const r = recorder();
    expect(() => createSubscription(h.deps, [r.consumer])).toThrow('bad options');
    expect(h.remove).toHaveBeenCalledOnce();
    expect(r.endings).toEqual([
      { kind: 'cancelled', cause: 'cancelled', accepted: false, error: new Error('bad options') },
    ]);
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
    expect(r.endings).toEqual([{ kind: 'rejected', cause: 'request-error', accepted: false, error: requestError }]);
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

describe('subscription: readiness and termination', () => {
  it('resolves subscribed on SUBSCRIBE_OK and keeps ended pending through publisher completion', async () => {
    const h = harness();
    const subscription = createSubscription(h.deps);
    let endedSettled = false;
    void subscription.ended.then(() => {
      endedSettled = true;
    });
    h.core().onOk?.(ok);
    await expect(subscription.subscribed).resolves.toBe(ok);
    h.core().onDone?.({ statusCode: 0, streamCount: 1, reason: '' });
    await Promise.resolve();
    expect(endedSettled).toBe(false);
    subscription.cancel(new Error('done'));
    await expect(subscription.ended).resolves.toMatchObject({ kind: 'cancelled', cause: 'cancelled', accepted: true });
  });

  it('rejects subscribed with SubscriptionRequestError and ends as rejected on a REQUEST_ERROR before acceptance', async () => {
    const h = harness();
    const subscription = createSubscription(h.deps);
    h.core().onError?.(requestError);
    await expect(subscription.subscribed).rejects.toBeInstanceOf(SubscriptionRequestError);
    await expect(subscription.subscribed).rejects.toMatchObject({ requestError });
    await expect(subscription.ended).resolves.toEqual({
      kind: 'rejected',
      cause: 'request-error',
      accepted: false,
      error: requestError,
    });
  });

  it('ends as terminated on a REQUEST_ERROR after acceptance, with subscribed already resolved', async () => {
    const h = harness();
    const subscription = createSubscription(h.deps);
    h.core().onOk?.(ok);
    h.core().onError?.(requestError);
    await expect(subscription.subscribed).resolves.toBe(ok);
    await expect(subscription.ended).resolves.toMatchObject({
      kind: 'terminated',
      cause: 'request-error',
      accepted: true,
    });
  });

  it.each([
    ['before', false],
    ['after', true],
  ])(
    'ends as terminated when the connection closes %s acceptance, rejecting subscribed only before',
    async (_, accepted) => {
      const h = harness();
      const subscription = createSubscription(h.deps);
      if (accepted) h.core().onOk?.(ok);
      const error = new Error('lost');
      h.closeConnection(error);
      if (accepted) await expect(subscription.subscribed).resolves.toBe(ok);
      else {
        await expect(subscription.subscribed).rejects.toBeInstanceOf(SubscriptionConnectionClosedError);
        await expect(subscription.subscribed).rejects.toMatchObject({ cause: error });
      }
      await expect(subscription.ended).resolves.toEqual({
        kind: 'terminated',
        cause: 'connection-closed',
        accepted,
        error,
      });
    },
  );

  it('rejects subscribed with the cancel reason, or CLOSED when none was given', async () => {
    const h = harness();
    const withReason = createSubscription(h.deps);
    const reason = new Error('changed my mind');
    withReason.cancel(reason);
    await expect(withReason.subscribed).rejects.toBe(reason);
    await expect(withReason.ended).resolves.toEqual({
      kind: 'cancelled',
      cause: 'cancelled',
      accepted: false,
      error: reason,
    });
    const bare = createSubscription(harness().deps);
    bare.cancel();
    await expect(bare.subscribed).rejects.toMatchObject({ code: 'CLOSED', phase: 'subscription' });
  });

  it('settles each awaitable once and pre-handles a readiness rejection nobody awaits', async () => {
    const h = harness();
    const subscription = createSubscription(h.deps);
    h.core().onError?.(requestError);
    // No await on `subscribed` here: an unhandled rejection would fail the run.
    await Promise.resolve();
    subscription.cancel(new Error('too late'));
    await expect(subscription.ended).resolves.toMatchObject({ kind: 'rejected' });
    await expect(subscription.subscribed).rejects.toBeInstanceOf(SubscriptionRequestError);
  });
});

describe('subscription: updates before acceptance', () => {
  it('queues updates issued before SUBSCRIBE_OK, sends them in order once accepted, then forwards directly', () => {
    const h = harness();
    const subscription = createSubscription(h.deps);
    subscription.update({ forward: 0 });
    subscription.update({ forward: 1 });
    expect(h.update).not.toHaveBeenCalled();
    h.core().onOk?.(ok);
    expect(h.update.mock.calls).toEqual([[{ forward: 0 }], [{ forward: 1 }]]);
    subscription.update({ subscriberPriority: 3 });
    expect(h.update).toHaveBeenCalledTimes(3);
  });

  it('refuses a further queued update with UPDATE_QUEUE_FULL once the bound is reached, keeping the others', () => {
    const h = harness();
    const subscription = createSubscription(h.deps);
    for (let priority = 0; priority < UPDATE_QUEUE_BOUND; priority++)
      subscription.update({ subscriberPriority: priority });
    expect(() => subscription.update({ forward: 0 })).toThrowError(
      expect.objectContaining({ code: 'UPDATE_QUEUE_FULL', phase: 'subscription' }),
    );
    h.core().onOk?.(ok);
    expect(h.update).toHaveBeenCalledTimes(UPDATE_QUEUE_BOUND);
  });

  it('rejects a malformed update synchronously and leaves the subscription and its queue intact', () => {
    const h = harness();
    const r = recorder();
    const subscription = createSubscription(h.deps, [r.consumer]);
    subscription.update({ forward: 0 });
    expect(() => subscription.update({ expires: -1 })).toThrow(RangeError);
    h.core().onOk?.(ok);
    expect(h.update.mock.calls).toEqual([[{ forward: 0 }]]);
    expect(r.events.map((event) => event.type)).toEqual(['subscribed']);
    expect(subscription.end).toBeUndefined();
  });

  it('queues a snapshot, so mutating the caller object afterwards does not change what is sent', () => {
    const h = harness();
    const subscription = createSubscription(h.deps);
    const parameters: MessageParameters = { forward: 0, authorizationTokens: [new Uint8Array([1])] };
    subscription.update(parameters);
    parameters.forward = 1;
    parameters.authorizationTokens?.[0]?.set([9]);
    h.core().onOk?.(ok);
    expect(h.update).toHaveBeenCalledExactlyOnceWith({ forward: 0, authorizationTokens: [new Uint8Array([1])] });
  });

  it('discards queued updates when the subscription ends before acceptance', () => {
    const h = harness();
    const subscription = createSubscription(h.deps);
    subscription.update({ forward: 0 });
    h.core().onError?.(requestError);
    expect(h.update).not.toHaveBeenCalled();
  });

  it('throws CLOSED for an update after the subscription ended', () => {
    const h = harness();
    const subscription = createSubscription(h.deps);
    h.core().onOk?.(ok);
    subscription.cancel(new Error('done'));
    expect(() => subscription.update({ forward: 0 })).toThrowError(
      expect.objectContaining({ code: 'CLOSED', phase: 'subscription' }),
    );
    expect(h.update).not.toHaveBeenCalled();
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
