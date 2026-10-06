/**
 * The switchable subscription through its own interface: each subscription underneath is a real
 * product subscription over a fake core, so the tests drive SUBSCRIBE_OK, objects and endings per
 * track. The live switch with no gap is in scripts/relay-scenario.js.
 */
import type { MessageParameters, MoqtObject, RequestError, SubscribeOk, SubscriptionHandlers } from '@mux/moq-core';
import { describe, expect, it, vi } from 'vitest';
import { RelayError } from '../src/errors.js';
import { connectRelayPublisher, connectRelaySubscriber } from '../src/index.js';
import {
  createSubscription,
  SubscriptionConnectionClosedError,
  SubscriptionRequestError,
} from '../src/subscription.js';
import {
  createSwitchable,
  type SwitchableHandlers,
  type SwitchedInfo,
  snapshotSwitchableHandlers,
} from '../src/switchable.js';
import { createRelayHub } from './helpers/relay-hub.js';

const OK = { trackAlias: 1, parameters: {}, trackProperties: [] } as unknown as SubscribeOk;

function object(groupId: number, objectId = 0, payload: number[] = [groupId]): MoqtObject {
  return {
    groupId,
    subgroupId: 0,
    objectId,
    publisherPriority: 0,
    status: 'normal',
    properties: [],
    payload: new Uint8Array(payload),
  } as unknown as MoqtObject;
}

/** One opened subscription: its fake core's handlers and what the switchable did to it. */
interface Opened {
  track: string;
  parameters: MessageParameters | undefined;
  core: SubscriptionHandlers;
  cancel: ReturnType<typeof vi.fn>;
}

function harness(handlers: SwitchableHandlers = {}, initial = 'video-mid', parameters?: MessageParameters) {
  const opened: Opened[] = [];
  const closers = new Set<(error?: unknown) => void>();
  let closed = false;
  const switchable = createSwitchable(
    {
      subscribe(track, params, consumer) {
        if (closed) throw new RelayError('CLOSED', 'Relay connection is closed', 'connection');
        return createSubscription(
          {
            subscribe(core) {
              const cancel = vi.fn();
              opened.push({ track, parameters: params, core, cancel });
              return { requestId: opened.length, cancel, update: vi.fn() };
            },
            protocol: 'moqt-22' as never,
            observeClosed(observer) {
              closers.add(observer);
              return () => closers.delete(observer);
            },
          },
          [consumer],
        );
      },
    },
    initial,
    parameters,
    snapshotSwitchableHandlers(handlers),
  );
  const track = (name: string, nth = 0) => {
    const found = opened.filter((entry) => entry.track === name)[nth];
    if (!found) throw new Error(`no subscription to ${name}`);
    return found;
  };
  return {
    switchable,
    opened,
    track,
    closeConnection(error?: unknown) {
      closed = true;
      for (const close of [...closers]) close(error);
    },
  };
}

/** Records every handler call as `[name, ...args]`, the track name last. */
function recording() {
  const calls: unknown[][] = [];
  const handlers: SwitchableHandlers = {
    onSubscribed: (_info, track) => calls.push(['subscribed', track]),
    onError: (error, track) => calls.push(['error', error.errorCode, track]),
    onObject: (value, track) => calls.push(['object', value.groupId, value.objectId, track]),
    onSubgroupEnd: (info, track) => calls.push(['subgroupEnd', info.groupId, track]),
    onSubgroupReset: (info, track) => calls.push(['subgroupReset', info.groupId, track]),
    onPublisherDone: (_info, track) => calls.push(['publisherDone', track]),
    onSwitched: ({ from, to }) => calls.push(['switched', from, to]),
  };
  return { calls, handlers };
}

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

describe('subscribeSwitchable', () => {
  it('delivers the first subscription with its track name', async () => {
    const { calls, handlers } = recording();
    const { switchable, track } = harness(handlers);
    track('video-mid').core.onOk?.(OK);
    track('video-mid').core.onObject?.(object(5));
    await expect(switchable.subscribed).resolves.toBe(OK);
    expect(switchable.track).toBe('video-mid');
    expect(switchable.pending).toBeUndefined();
    expect(calls).toEqual([
      ['subscribed', 'video-mid'],
      ['object', 5, 0, 'video-mid'],
    ]);
  });

  it('keeps the old track delivering until the new one cuts over, then cancels it', async () => {
    const { calls, handlers } = recording();
    const { switchable, track } = harness(handlers);
    track('video-mid').core.onOk?.(OK);
    track('video-mid').core.onObject?.(object(5));
    const switched = switchable.switchTo('video-hi');
    expect(switchable.pending).toBe('video-hi');
    const hi = track('video-hi');
    hi.core.onOk?.(OK);
    track('video-mid').core.onObject?.(object(5, 1));
    hi.core.onObject?.(object(5));
    track('video-mid').core.onObject?.(object(5, 2));
    await expect(switched).resolves.toEqual({ from: 'video-mid', to: 'video-hi', subscribed: OK });
    expect(track('video-mid').cancel).toHaveBeenCalledOnce();
    expect(hi.cancel).not.toHaveBeenCalled();
    expect(switchable.track).toBe('video-hi');
    expect(switchable.pending).toBeUndefined();
    hi.core.onObject?.(object(5, 1));
    expect(calls).toEqual([
      ['subscribed', 'video-mid'],
      ['object', 5, 0, 'video-mid'],
      ['object', 5, 1, 'video-mid'],
      ['switched', 'video-mid', 'video-hi'],
      ['object', 5, 0, 'video-hi'],
      ['object', 5, 1, 'video-hi'],
    ]);
  });

  it('holds the new track until the predicate passes, discarding what it refuses', async () => {
    const { calls, handlers } = recording();
    const { switchable, track } = harness(handlers);
    track('video-mid').core.onOk?.(OK);
    const switched = switchable.switchTo('video-lo', { until: (value) => value.groupId >= 6 });
    const lo = track('video-lo');
    lo.core.onOk?.(OK);
    lo.core.onObject?.(object(5));
    lo.core.onSubgroupEnd?.({ groupId: 5, subgroupId: 0, endOfGroup: true });
    lo.core.onSubgroupReset?.({ groupId: 5, error: new Error('reset') });
    track('video-mid').core.onObject?.(object(6));
    lo.core.onObject?.(object(6));
    await switched;
    expect(calls).toEqual([
      ['subscribed', 'video-mid'],
      ['object', 6, 0, 'video-mid'],
      ['switched', 'video-mid', 'video-lo'],
      ['object', 6, 0, 'video-lo'],
    ]);
  });

  it('drains the old track through its largest group after the cutover, then cancels it', async () => {
    const { calls, handlers } = recording();
    const { switchable, track } = harness(handlers);
    const mid = track('video-mid');
    mid.core.onOk?.(OK);
    mid.core.onObject?.(object(44));
    mid.core.onObject?.(object(46));
    const switched = switchable.switchTo('video-hi', { drainMs: 20 });
    const hi = track('video-hi');
    hi.core.onOk?.(OK);
    // Each track numbers its own groups.
    hi.core.onObject?.(object(3));
    await switched;
    expect(mid.cancel).not.toHaveBeenCalled();
    mid.core.onObject?.(object(45));
    mid.core.onObject?.(object(46, 1));
    mid.core.onSubgroupReset?.({ groupId: 43, error: new Error('reset') });
    mid.core.onObject?.(object(47));
    mid.core.onSubgroupEnd?.({ groupId: 47, subgroupId: 0, endOfGroup: true });
    hi.core.onObject?.(object(4));
    await vi.waitFor(() => expect(mid.cancel).toHaveBeenCalledOnce());
    mid.core.onObject?.(object(45, 1));
    expect(calls.slice(1)).toEqual([
      ['object', 44, 0, 'video-mid'],
      ['object', 46, 0, 'video-mid'],
      ['switched', 'video-mid', 'video-hi'],
      ['object', 3, 0, 'video-hi'],
      ['object', 45, 0, 'video-mid'],
      ['object', 46, 1, 'video-mid'],
      ['subgroupReset', 43, 'video-mid'],
      ['object', 4, 0, 'video-hi'],
    ]);
  });

  it('drains nothing when the old track delivered nothing', async () => {
    const { switchable, track } = harness();
    track('video-mid').core.onOk?.(OK);
    const switched = switchable.switchTo('video-hi', { drainMs: 60_000 });
    track('video-hi').core.onOk?.(OK);
    track('video-hi').core.onObject?.(object(1));
    await switched;
    expect(track('video-mid').cancel).toHaveBeenCalledOnce();
  });

  it('stops a drain when the switchable is cancelled', async () => {
    const { switchable, track } = harness();
    track('video-mid').core.onOk?.(OK);
    track('video-mid').core.onObject?.(object(1));
    const switched = switchable.switchTo('video-hi', { drainMs: 60_000 });
    track('video-hi').core.onOk?.(OK);
    track('video-hi').core.onObject?.(object(1));
    await switched;
    switchable.cancel();
    expect(track('video-mid').cancel).toHaveBeenCalledOnce();
    expect(track('video-hi').cancel).toHaveBeenCalledOnce();
  });

  it('switches to the same track with new parameters, which are not inherited', async () => {
    const parameters = { subscriberPriority: 3 } as MessageParameters;
    const { switchable, track, opened } = harness({}, 'video-mid', parameters);
    track('video-mid').core.onOk?.(OK);
    const switched = switchable.switchTo('video-mid');
    expect(opened.map((entry) => entry.parameters)).toEqual([parameters, undefined]);
    track('video-mid', 1).core.onOk?.(OK);
    track('video-mid', 1).core.onObject?.(object(1));
    await expect(switched).resolves.toMatchObject({ from: 'video-mid', to: 'video-mid' });
    expect(track('video-mid', 0).cancel).toHaveBeenCalledOnce();
  });

  it('rejects a switch whose subscription is refused and keeps the active track', async () => {
    const { calls, handlers } = recording();
    const { switchable, track } = harness(handlers);
    track('video-mid').core.onOk?.(OK);
    const switched = switchable.switchTo('video-hi');
    const refusal: RequestError = { errorCode: 0x10, retryInterval: 0, reason: 'not found' };
    track('video-hi').core.onError?.(refusal);
    const error = await switched.catch((reason: unknown) => reason);
    expect(error).toBeInstanceOf(SubscriptionRequestError);
    expect((error as SubscriptionRequestError).requestError).toBe(refusal);
    expect(switchable.pending).toBeUndefined();
    track('video-mid').core.onObject?.(object(9));
    expect(calls).toEqual([
      ['subscribed', 'video-mid'],
      ['object', 9, 0, 'video-mid'],
    ]);
  });

  it('cancels a pending switch that a newer one supersedes', async () => {
    const { switchable, track } = harness();
    track('video-mid').core.onOk?.(OK);
    const first = switchable.switchTo('video-hi');
    const second = switchable.switchTo('video-lo');
    await expect(first).rejects.toMatchObject({ code: 'CLOSED' });
    expect(track('video-hi').cancel).toHaveBeenCalledOnce();
    expect(switchable.pending).toBe('video-lo');
    track('video-hi').core.onOk?.(OK);
    track('video-hi').core.onObject?.(object(1));
    expect(switchable.track).toBe('video-mid');
    track('video-lo').core.onOk?.(OK);
    track('video-lo').core.onObject?.(object(1));
    await expect(second).resolves.toMatchObject({ to: 'video-lo' });
  });

  it('abandons a pending switch when its signal aborts', async () => {
    const { switchable, track } = harness();
    track('video-mid').core.onOk?.(OK);
    const controller = new AbortController();
    const switched = switchable.switchTo('video-hi', { signal: controller.signal });
    const reason = new Error('probe took too long');
    controller.abort(reason);
    await expect(switched).rejects.toBe(reason);
    expect(track('video-hi').cancel).toHaveBeenCalledOnce();
    expect(switchable.track).toBe('video-mid');
  });

  it('abandons a switch whose predicate throws', async () => {
    const { switchable, track } = harness();
    track('video-mid').core.onOk?.(OK);
    const failure = new Error('bad predicate');
    const switched = switchable.switchTo('video-hi', {
      until: () => {
        throw failure;
      },
    });
    track('video-hi').core.onOk?.(OK);
    track('video-hi').core.onObject?.(object(1));
    await expect(switched).rejects.toBe(failure);
    expect(track('video-hi').cancel).toHaveBeenCalledOnce();
    expect(switchable.track).toBe('video-mid');
  });

  it('ends with the active subscription and cancels a pending switch', async () => {
    const { calls, handlers } = recording();
    const { switchable, track } = harness(handlers);
    track('video-mid').core.onOk?.(OK);
    const switched = switchable.switchTo('video-hi');
    const refusal: RequestError = { errorCode: 0x0, retryInterval: 0, reason: 'refused' };
    track('video-mid').core.onError?.(refusal);
    await expect(switchable.ended).resolves.toMatchObject({ kind: 'terminated', cause: 'request-error' });
    await expect(switched).rejects.toMatchObject({ code: 'CLOSED' });
    expect(track('video-hi').cancel).toHaveBeenCalledOnce();
    expect(calls.at(-1)).toEqual(['error', 0, 'video-mid']);
    expect(() => switchable.switchTo('video-lo')).toThrow(expect.objectContaining({ code: 'CLOSED' }));
  });

  it('rejects subscribed and ends when the first subscription is refused', async () => {
    const { calls, handlers } = recording();
    const { switchable, track } = harness(handlers);
    const refusal: RequestError = { errorCode: 0x10, retryInterval: 0, reason: 'unroutable' };
    track('video-mid').core.onError?.(refusal);
    await expect(switchable.subscribed).rejects.toBeInstanceOf(SubscriptionRequestError);
    await expect(switchable.ended).resolves.toMatchObject({ kind: 'rejected' });
    expect(calls).toEqual([['error', 0x10, 'video-mid']]);
  });

  it('settles subscribed from a switch that cut over before the first subscription answered', async () => {
    const { switchable, track } = harness();
    const switched = switchable.switchTo('video-lo');
    const other = { ...OK, trackAlias: 2 } as SubscribeOk;
    track('video-lo').core.onOk?.(other);
    track('video-lo').core.onObject?.(object(1));
    await switched;
    await expect(switchable.subscribed).resolves.toBe(other);
    expect(track('video-mid').cancel).toHaveBeenCalledOnce();
  });

  it('cancels both subscriptions on cancel', async () => {
    const { switchable, track } = harness();
    track('video-mid').core.onOk?.(OK);
    const switched = switchable.switchTo('video-hi');
    const reason = new Error('viewer left');
    switchable.cancel(reason);
    await expect(switched).rejects.toBe(reason);
    await expect(switchable.ended).resolves.toEqual({
      kind: 'cancelled',
      cause: 'cancelled',
      accepted: true,
      error: reason,
    });
    expect(track('video-mid').cancel).toHaveBeenCalledOnce();
    expect(track('video-hi').cancel).toHaveBeenCalledOnce();
  });

  it('ends on connection closure, rejecting a pending switch', async () => {
    const { switchable, track, closeConnection } = harness();
    track('video-mid').core.onOk?.(OK);
    const switched = switchable.switchTo('video-hi');
    const failure = new Error('lost');
    closeConnection(failure);
    await expect(switched).rejects.toBeInstanceOf(SubscriptionConnectionClosedError);
    await expect(switchable.ended).resolves.toMatchObject({ cause: 'connection-closed', error: failure });
  });

  it('lets onSwitched switch again before the cutover object is delivered', async () => {
    const calls: unknown[][] = [];
    let switchedAgain: Promise<SwitchedInfo> | undefined;
    const { switchable, track } = harness({
      onObject: (value, name) => calls.push(['object', value.groupId, name]),
      onSwitched: ({ to }) => {
        calls.push(['switched', to]);
        if (to === 'video-hi') switchedAgain = switchable.switchTo('video-lo');
      },
    });
    track('video-mid').core.onOk?.(OK);
    void switchable.switchTo('video-hi');
    track('video-hi').core.onOk?.(OK);
    track('video-hi').core.onObject?.(object(1));
    expect(switchable.pending).toBe('video-lo');
    expect(calls).toEqual([
      ['switched', 'video-hi'],
      ['object', 1, 'video-hi'],
    ]);
    track('video-lo').core.onOk?.(OK);
    track('video-lo').core.onObject?.(object(2));
    await expect(switchedAgain).resolves.toMatchObject({ from: 'video-hi', to: 'video-lo' });
  });

  it('ignores a predicate that switched again, and leaves the newer switch pending', async () => {
    const { switchable, track } = harness();
    track('video-mid').core.onOk?.(OK);
    let replacement: Promise<SwitchedInfo> | undefined;
    const first = switchable.switchTo('video-hi', {
      until: () => {
        replacement = switchable.switchTo('video-lo');
        return true;
      },
    });
    track('video-hi').core.onOk?.(OK);
    track('video-hi').core.onObject?.(object(1));
    await expect(first).rejects.toMatchObject({ code: 'CLOSED' });
    expect(switchable.track).toBe('video-mid');
    expect(switchable.pending).toBe('video-lo');
    track('video-lo').core.onOk?.(OK);
    track('video-lo').core.onObject?.(object(2));
    await expect(replacement).resolves.toMatchObject({ from: 'video-mid', to: 'video-lo', subscribed: OK });
  });

  it('observes a promise a predicate returned before refusing it', async () => {
    const unhandled = vi.fn();
    process.on('unhandledRejection', unhandled);
    try {
      const { switchable, track } = harness();
      track('video-mid').core.onOk?.(OK);
      const switched = switchable.switchTo('video-hi', { until: (() => Promise.reject(new Error('async'))) as never });
      track('video-hi').core.onOk?.(OK);
      track('video-hi').core.onObject?.(object(1));
      await expect(switched).rejects.toBeInstanceOf(TypeError);
      await flush();
      expect(unhandled).not.toHaveBeenCalled();
    } finally {
      process.off('unhandledRejection', unhandled);
    }
  });

  it('checks its arguments at once and leaves a pending switch alone when one is bad', async () => {
    const { switchable, track } = harness();
    track('video-mid').core.onOk?.(OK);
    void switchable.switchTo('video-hi');
    expect(() => switchable.switchTo('video-lo', { until: 'never' as never })).toThrow(TypeError);
    expect(() => switchable.switchTo('video-lo', { until: (async () => true) as never })).toThrow(TypeError);
    expect(() => switchable.switchTo('video-lo', null as never)).toThrow(TypeError);
    expect(() => switchable.switchTo('video-lo', { drainMs: -1 })).toThrow(TypeError);
    expect(switchable.pending).toBe('video-hi');
    expect(() => snapshotSwitchableHandlers({ onObject: async () => {} } as never)).toThrow(TypeError);
    await flush();
  });
});

describe('subscribeSwitchable over a relay', () => {
  it('switches layers on a live connection with no gap in groups', async () => {
    const hub = createRelayHub();
    const url = 'https://relay.example.com/moq';
    const namespace = ['room', 'layers'];
    const publisher = await connectRelayPublisher({ url, namespace, createTransport: hub.connectPublisher });
    const subscriber = await connectRelaySubscriber({ url, createTransport: hub.connectSubscriber });
    try {
      const writers = Object.fromEntries(
        ['video-hi', 'video-lo'].map((track) => [track, publisher.registerTrack({ track, writeTimeoutMs: 2_000 })]),
      );
      const received: [string, number][] = [];
      const switchable = subscriber.subscribeSwitchable(
        { namespace, track: 'video-lo' },
        { onObject: (value, track) => received.push([track, value.payload[0] as number]) },
      );
      await switchable.subscribed;
      // Every layer carries the same frame numbers, as a simulcast encoder's layers do.
      const frame = async (n: number) => {
        await Promise.all(
          Object.entries(writers)
            .filter(([, writer]) => writer.hasDemand)
            .map(([, writer]) => writer.write(new Uint8Array([n]))),
        );
      };
      await vi.waitFor(() => expect(writers['video-lo']?.hasDemand).toBe(true));
      let n = 0;
      await frame(n++);
      await vi.waitFor(() => expect(received).toHaveLength(1));
      const switched = switchable.switchTo('video-hi');
      await vi.waitFor(() => expect(writers['video-hi']?.hasDemand).toBe(true));
      while (switchable.track !== 'video-hi') {
        await frame(n++);
        await new Promise((resolve) => setTimeout(resolve, 5));
      }
      await switched;
      await frame(n++);
      await vi.waitFor(() => expect(received.at(-1)).toEqual(['video-hi', n - 1]));
      const frames = received.map(([, value]) => value);
      expect(new Set(frames)).toEqual(new Set(Array.from({ length: n }, (_, i) => i)));
      expect(received.findIndex(([track]) => track === 'video-hi')).toBeGreaterThan(0);
    } finally {
      subscriber.close();
      await publisher.close();
      hub.destroy();
    }
  });
});
