/**
 * The subscription through its own interface: a fake core subscribe and a fake closure observer,
 * recording consumers. Relay behaviour over a real connection stays in connect.test.ts and
 * subscription-events.test.ts.
 */
import {
  encodeRequestUpdate,
  type MessageParameters,
  MOQT_PROTOCOL_ID,
  type RequestError,
  type SubscriptionHandlers,
} from '@caprid/moq-core';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  CLOSURE_GRACE_MS,
  createSubscription,
  SubscriptionConnectionClosedError,
  type SubscriptionConsumer,
  type SubscriptionEnd,
  type SubscriptionEvent,
  SubscriptionRequestError,
  synchronousCallback,
  UPDATE_QUEUE_BOUND,
} from '../src/subscription.js';

/** Failures the core synthesized from a stream event rather than read from a REQUEST_ERROR. */
const streamFailures = vi.hoisted(() => new WeakSet<object>());

vi.mock('@caprid/moq-core', async (importOriginal) => {
  const core = await importOriginal<typeof import('@caprid/moq-core')>();
  return { ...core, isStreamFailure: (error: RequestError) => streamFailures.has(error) };
});

function streamFailure(reason: string, errorCode = 0): RequestError {
  const failure = { errorCode, retryInterval: 0, reason };
  streamFailures.add(failure);
  return failure;
}

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
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

function harness(options: { closedAlready?: unknown; subscribeThrows?: unknown; requestId?: number } = {}) {
  let handlers: SubscriptionHandlers | undefined;
  let observer: ((error?: unknown) => void) | undefined;
  const cancel = vi.fn();
  const update = vi.fn();
  const remove = vi.fn();
  const subscribe = vi.fn((value: SubscriptionHandlers) => {
    if ('subscribeThrows' in options) throw options.subscribeThrows;
    handlers = value;
    return { requestId: options.requestId ?? 7, cancel, update };
  });
  const observeClosed = vi.fn((value: (error?: unknown) => void) => {
    if ('closedAlready' in options) value(options.closedAlready);
    else observer = value;
    return remove;
  });
  return {
    deps: { subscribe, protocol: MOQT_PROTOCOL_ID, observeClosed },
    /** The transport's closed settles; the session reacts and reports the connection ended. */
    endTransport: (error: unknown) => {
      void Promise.reject(error).catch((reason: unknown) => observer?.(reason));
    },
    core: () => {
      if (!handlers) throw new Error('core subscribe was not called');
      return handlers;
    },
    closeConnection: (error?: unknown) => observer?.(error),
    cancel,
    update,
    remove,
    observeClosed,
    subscribe,
  };
}

/** The core records a pending request failure when the turn ends. */
const turnEnds = () => Promise.resolve();

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
  it('records a REQUEST_ERROR after acceptance at once, cancels the core, releases the closure observer, and drops later events', () => {
    const h = harness();
    const r = recorder();
    const subscription = createSubscription(h.deps, [r.consumer]);
    h.core().onOk?.(ok);
    h.core().onError?.(requestError);
    expect(r.endings).toEqual([{ kind: 'terminated', cause: 'request-error', accepted: true, error: requestError }]);
    expect(subscription.end).toEqual(r.endings[0]);
    expect(h.cancel).toHaveBeenCalledExactlyOnceWith(requestError);
    expect(h.remove).toHaveBeenCalledOnce();
    h.core().onUpdateOk?.();
    h.core().onError?.(requestError);
    expect(r.events).toEqual([{ type: 'subscribed', value: ok }]);
    expect(r.endings).toHaveLength(1);
  });

  it('records a REQUEST_ERROR before acceptance when the turn ends, as rejected, then drops later events', async () => {
    const h = harness();
    const r = recorder();
    const subscription = createSubscription(h.deps, [r.consumer]);
    h.core().onError?.(requestError);
    expect(r.endings).toEqual([]);
    await turnEnds();
    expect(r.endings).toEqual([{ kind: 'rejected', cause: 'request-error', accepted: false, error: requestError }]);
    expect(subscription.end).toEqual(r.endings[0]);
    expect(h.cancel).toHaveBeenCalledExactlyOnceWith(requestError);
    expect(h.remove).toHaveBeenCalledOnce();
    h.core().onUpdateOk?.();
    h.core().onError?.(requestError);
    await turnEnds();
    expect(r.events).toEqual([]);
    expect(r.endings).toHaveLength(1);
  });

  it('contains a consumer that throws while a deferred rejection is recorded, as the core does', async () => {
    const h = harness();
    const throwing: SubscriptionConsumer = {
      event: () => {},
      ended: () => {
        throw new Error('application callback failed');
      },
    };
    const subscription = createSubscription(h.deps, [throwing]);
    h.core().onError?.(requestError);
    await turnEnds();
    // Reaching here without an unhandled error is the point; vitest fails the run on one.
    expect(subscription.end).toMatchObject({ kind: 'rejected' });
    await expect(subscription.subscribed).rejects.toBeInstanceOf(SubscriptionRequestError);
    expect(h.cancel).toHaveBeenCalledExactlyOnceWith(requestError);
  });

  it('lets a closure reported in the same turn as a pending request failure win, as the core orders them', async () => {
    const h = harness();
    const r = recorder();
    const subscription = createSubscription(h.deps, [r.consumer]);
    const lost = new Error('transport dropped');
    h.core().onError?.({ errorCode: 0, retryInterval: 0, reason: 'session closed before the request was answered' });
    h.closeConnection(lost);
    expect(r.endings).toEqual([{ kind: 'terminated', cause: 'connection-closed', accepted: false, error: lost }]);
    await turnEnds();
    expect(r.endings).toHaveLength(1);
    expect(h.cancel).toHaveBeenCalledExactlyOnceWith(lost);
    await expect(subscription.subscribed).rejects.toBeInstanceOf(SubscriptionConnectionClosedError);
    await expect(subscription.subscribed).rejects.toMatchObject({ cause: lost });
    await expect(subscription.ended).resolves.toMatchObject({ kind: 'terminated', cause: 'connection-closed' });
  });

  it('leaves a stream failure that precedes the transport closure by several tasks to the closure', async () => {
    // Chromium fails the request stream of a lost connection a task or more before WebTransport's
    // closed settles (issue #102); the core reports that as a request failure first.
    const h = harness();
    const r = recorder();
    const subscription = createSubscription(h.deps, [r.consumer]);
    const lost = new Error('Connection lost.');
    h.core().onError?.(streamFailure('The stream was aborted by the remote server'));
    for (let task = 0; task < 5; task++) await new Promise((resolve) => setTimeout(resolve, 0));
    expect(r.endings).toEqual([]);
    h.endTransport(lost);
    await expect(subscription.ended).resolves.toEqual({
      kind: 'terminated',
      cause: 'connection-closed',
      accepted: false,
      error: lost,
    });
    await expect(subscription.subscribed).rejects.toBeInstanceOf(SubscriptionConnectionClosedError);
    expect(r.endings).toHaveLength(1);
    expect(h.cancel).toHaveBeenCalledExactlyOnceWith(lost);
  });

  it('records a stream failure on a connection that stays up as rejected once the closure grace passes', async () => {
    vi.useFakeTimers();
    const h = harness();
    const r = recorder();
    const subscription = createSubscription(h.deps, [r.consumer]);
    const failure = streamFailure('request stream closed before a response');
    h.core().onError?.(failure);
    await vi.advanceTimersByTimeAsync(CLOSURE_GRACE_MS - 1);
    expect(r.endings).toEqual([]);
    await vi.advanceTimersByTimeAsync(1);
    expect(r.endings).toEqual([{ kind: 'rejected', cause: 'request-error', accepted: false, error: failure }]);
    await expect(subscription.subscribed).rejects.toBeInstanceOf(SubscriptionRequestError);
    // Nothing outlives the rejection on a connection that stays up: no timer, no closure observer.
    expect(vi.getTimerCount()).toBe(0);
    expect(h.remove).toHaveBeenCalledOnce();
    expect(h.observeClosed).toHaveBeenCalledOnce();
    // A closure afterwards changes nothing.
    h.endTransport(new Error('later'));
    await vi.advanceTimersByTimeAsync(0);
    expect(r.endings).toHaveLength(1);
  });

  it('holds a response timeout for one microtask only, like a REQUEST_ERROR', async () => {
    const h = harness();
    const r = recorder();
    createSubscription(h.deps, [r.consumer]);
    const timeout = streamFailure('no response before the request timeout', 0x2);
    h.core().onError?.(timeout);
    await turnEnds();
    expect(r.endings).toEqual([{ kind: 'rejected', cause: 'request-error', accepted: false, error: timeout }]);
  });

  it('drops the closure grace when the subscription ends another way', () => {
    vi.useFakeTimers();
    const h = harness();
    const r = recorder();
    const subscription = createSubscription(h.deps, [r.consumer]);
    h.core().onError?.(streamFailure('request stream failed'));
    expect(vi.getTimerCount()).toBe(1);
    subscription.cancel('stop');
    expect(vi.getTimerCount()).toBe(0);
    expect(r.endings).toEqual([{ kind: 'cancelled', cause: 'cancelled', accepted: false, error: 'stop' }]);
  });

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

  it('cancels the core handle when the source fails the request synchronously during subscribe, once the turn ends', async () => {
    const cancel = vi.fn();
    const deps = {
      subscribe: (handlers: SubscriptionHandlers) => {
        handlers.onError?.(requestError);
        return { requestId: 1, cancel, update() {} };
      },
      protocol: MOQT_PROTOCOL_ID,
      observeClosed: () => () => {},
    };
    const r = recorder();
    const subscription = createSubscription(deps, [r.consumer]);
    expect(subscription.requestId).toBe(1);
    await turnEnds();
    expect(r.endings).toEqual([{ kind: 'rejected', cause: 'request-error', accepted: false, error: requestError }]);
    expect(cancel).toHaveBeenCalledExactlyOnceWith(requestError);
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

  it('rejects subscribed with the cancel reason, null included, or CLOSED when none was given', async () => {
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
    const withNull = createSubscription(harness().deps);
    withNull.cancel(null);
    await expect(withNull.subscribed).rejects.toBeNull();
    await expect(withNull.ended).resolves.toMatchObject({ kind: 'cancelled', error: null });
    const bare = createSubscription(harness().deps);
    bare.cancel();
    await expect(bare.subscribed).rejects.toMatchObject({ code: 'CLOSED', phase: 'subscription' });
  });

  it('settles each awaitable once and pre-handles a readiness rejection nobody awaits', async () => {
    const h = harness();
    const subscription = createSubscription(h.deps);
    h.core().onError?.(requestError);
    // No await on `subscribed` here: an unhandled rejection would fail the run.
    await turnEnds();
    subscription.cancel(new Error('too late'));
    await expect(subscription.ended).resolves.toMatchObject({ kind: 'rejected' });
    await expect(subscription.subscribed).rejects.toBeInstanceOf(SubscriptionRequestError);
  });

  it('settles subscribed before flushing queued updates, so a flush failure ends loudly without hanging readiness', async () => {
    const h = harness();
    const r = recorder();
    const subscription = createSubscription(h.deps, [r.consumer]);
    subscription.update({ forward: 0 });
    const fault = new Error('core refused the update');
    h.update.mockImplementationOnce(() => {
      throw fault;
    });
    h.core().onOk?.(ok);
    await expect(subscription.subscribed).resolves.toBe(ok);
    await expect(subscription.ended).resolves.toEqual({
      kind: 'cancelled',
      cause: 'cancelled',
      accepted: true,
      error: fault,
    });
    expect(r.events).toEqual([]);
    expect(h.cancel).toHaveBeenCalledExactlyOnceWith(fault);
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

  it('validates a queued update with its own request id, so an update the flush could not frame is refused now', () => {
    const fits = (requestId: number, parameters: MessageParameters) => {
      try {
        encodeRequestUpdate(requestId, parameters);
        return true;
      } catch {
        return false;
      }
    };
    // A two-byte request id can push a message over the control-message frame limit.
    let parameters: MessageParameters | undefined;
    for (let length = 0xffff; length > 0xffff - 64 && !parameters; length--) {
      const candidate = { authorizationTokens: [new Uint8Array(length)] };
      if (fits(7, candidate) && !fits(128, candidate)) parameters = candidate;
    }
    if (!parameters) throw new Error('no token length sits on the frame boundary');
    const wide = createSubscription(harness({ requestId: 128 }).deps);
    expect(() => wide.update(parameters)).toThrow('control message exceeds');
    const narrow = createSubscription(harness({ requestId: 7 }).deps);
    expect(() => narrow.update(parameters)).not.toThrow();
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

  it('discards queued updates when the subscription ends before acceptance', async () => {
    const h = harness();
    const subscription = createSubscription(h.deps);
    subscription.update({ forward: 0 });
    h.core().onError?.(requestError);
    await turnEnds();
    expect(subscription.end).toMatchObject({ kind: 'rejected' });
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
