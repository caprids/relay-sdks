import type { SubscriptionHandlers } from '@mux/moq-core';
import { afterEach, expect, it, vi } from 'vitest';
import {
  encodePublishDone,
  encodeRequestError,
  encodeSubscribeOk,
} from '../packages/moq-core/src/protocol/control-messages.js';
import {
  connectRelay,
  type RelayConnection,
  SubscriptionConnectionClosedError,
  SubscriptionOverflowError,
  SubscriptionRequestError,
} from '../src/index.js';
import { closureObservers, createSubscriptionEvents } from '../src/subscription-events.js';
import { createFakeTransport, encodeSubgroup } from './helpers/relay.js';

const connections: RelayConnection[] = [];
afterEach(() => {
  for (const connection of connections.splice(0)) connection.close();
  vi.restoreAllMocks();
});

async function setup() {
  const fake = createFakeTransport();
  const connecting = connectRelay({
    url: 'https://relay.example.com/moq',
    requestTimeoutMs: 30,
    createTransport: () => ({ transport: fake.transport, ready: Promise.resolve() }),
  });
  fake.sendServerSetup();
  const connection = await connecting;
  connections.push(connection);
  return { fake, connection };
}

it('preserves publisher completion, trailing objects and arrival order across streams', async () => {
  const { fake, connection } = await setup();
  const events = connection.subscribeEvents({ namespace: ['test'], track: 'events' });
  const request = await fake.nextRequestStream();
  await request.firstMessage;
  await request.send(encodeSubscribeOk(11));
  expect((await events.next()).value.type).toBe('subscribed');
  await request.send(encodePublishDone(0, 2));
  expect((await events.next()).value.type).toBe('publisherDone');
  for (const groupId of [9, 2]) {
    fake.openDataStream(encodeSubgroup(11, groupId, new Uint8Array([groupId])));
    expect((await events.next()).value).toMatchObject({ type: 'object', value: { groupId } });
    expect((await events.next()).value.type).toBe('subgroupEnd');
  }
  const pending = events.next();
  events.close();
  expect(await pending).toMatchObject({ done: true });
  expect(fake.getCloseInfo()).toBeUndefined();
});

it('surfaces subgroup resets as gap events and continues delivery', async () => {
  const { fake, connection } = await setup();
  const events = connection.subscribeEvents({ namespace: ['test'], track: 'events' });
  const request = await fake.nextRequestStream();
  await request.send(encodeSubscribeOk(11));
  await events.next();
  const stream = fake.openControlledDataStream();
  await stream.write(encodeSubgroup(11, 4, new Uint8Array([1])));
  expect((await events.next()).value.type).toBe('object');
  stream.reset(new Error('gap'));
  expect((await events.next()).value).toMatchObject({ type: 'subgroupReset', value: { groupId: 4 } });
  fake.openDataStream(encodeSubgroup(11, 5, new Uint8Array([2])));
  expect((await events.next()).value.type).toBe('object');
  events.close();
});

it.each(['reject', 'timeout', 'drop', 'close', 'drop-pending', 'close-pending'] as const)(
  'settles reads on %s',
  async (mode) => {
    const { fake, connection } = await setup();
    const events = connection.subscribeEvents({ namespace: ['test'], track: 'events' });
    const request = await fake.nextRequestStream();
    if (mode === 'drop' || mode === 'close') {
      await request.send(encodeSubscribeOk(11));
      await events.next();
    }
    const read = expect(events.next()).rejects.toBeInstanceOf(
      mode === 'reject' || mode === 'timeout' || mode === 'drop-pending'
        ? SubscriptionRequestError
        : SubscriptionConnectionClosedError,
    );
    if (mode === 'reject') await request.send(encodeRequestError(1, 'denied'));
    if (mode === 'drop' || mode === 'drop-pending') fake.dropTransport();
    if (mode === 'close' || mode === 'close-pending') connection.close();
    await read;
  },
);

it.each([false, true])('for-await break or consumer exception releases only its subscription: %s', async (throws) => {
  const { fake, connection } = await setup();
  const events = connection.subscribeEvents({ namespace: ['test'], track: 'events' });
  const request = await fake.nextRequestStream();
  await request.send(encodeSubscribeOk(11));
  const consume = async () => {
    for await (const _event of events) {
      if (throws) throw new Error('consumer');
      break;
    }
  };
  if (throws) await expect(consume()).rejects.toThrow('consumer');
  else await consume();
  expect(await events.next()).toMatchObject({ done: true });
  const onOk = vi.fn();
  connection.subscribe({ namespace: ['test'], track: 'other' }, { onOk });
  await (await fake.nextRequestStream()).send(encodeSubscribeOk(12));
  await vi.waitFor(() => expect(onOk).toHaveBeenCalledOnce());
  expect(fake.getCloseInfo()).toBeUndefined();
});

it('bounds stalled consumers, fails without draining, and removes observers', async () => {
  let handlers!: SubscriptionHandlers;
  const cancel = vi.fn();
  const remove = vi.fn();
  const events = createSubscriptionEvents(
    (value) => {
      handlers = value;
      return { requestId: 0, cancel, update() {} };
    },
    () => remove,
    { capacity: 2 },
  );
  handlers.onDone?.({ statusCode: 0, streamCount: undefined, reason: '' });
  handlers.onUpdateOk?.();
  handlers.onUpdateOk?.();
  for (let i = 0; i < 1000; i++) handlers.onUpdateOk?.();
  await expect(events.next()).rejects.toBeInstanceOf(SubscriptionOverflowError);
  expect(cancel).toHaveBeenCalledOnce();
  expect(remove).toHaveBeenCalledOnce();
});

it('supports slow reads within capacity and rejects concurrent reads', async () => {
  let handlers!: SubscriptionHandlers;
  const events = createSubscriptionEvents(
    (value) => {
      handlers = value;
      return { requestId: 0, cancel() {}, update() {} };
    },
    () => () => {},
    { capacity: 2 },
  );
  handlers.onUpdateOk?.();
  handlers.onDone?.({ statusCode: 0, streamCount: 0, reason: 'done' });
  expect((await events.next()).value.type).toBe('updateOk');
  expect((await events.next()).value.type).toBe('publisherDone');
  const pending = events.next();
  await expect(events.next()).rejects.toThrow('one pending');
  await events.return();
  expect(await pending).toMatchObject({ done: true });
});

it.each([false, true])('abort settles reads and removes listeners, pre-aborted: %s', async (already) => {
  const { fake, connection } = await setup();
  const controller = new AbortController();
  const remove = vi.spyOn(controller.signal, 'removeEventListener');
  const reason = new Error('stop');
  if (already) controller.abort(reason);
  const events = connection.subscribeEvents({ namespace: ['test'], track: 'events' }, { signal: controller.signal });
  const read = expect(events.next()).rejects.toBe(reason);
  controller.abort(reason);
  await read;
  expect(remove).toHaveBeenCalled();
  expect(fake.getCloseInfo()).toBeUndefined();
});

it('removes closed observers without retaining iterators on the connection promise', async () => {
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
});

it('rejects async handlers before sending and diagnoses returned promises without unhandled rejection', async () => {
  const { fake, connection } = await setup();
  expect(() => connection.subscribe({ namespace: ['test'], track: 'events' }, { onObject: async () => {} })).toThrow(
    'synchronous',
  );
  const diagnostic = vi.spyOn(console, 'error').mockImplementation(() => {});
  const onObject = vi.fn(() => Promise.reject(new Error('async failure')));
  const subscription = connection.subscribe({ namespace: ['test'], track: 'events' }, { onObject });
  expect(subscription.requestId).toBe(0);
  const request = await fake.nextRequestStream();
  await request.send(encodeSubscribeOk(11));
  fake.openDataStream(encodeSubgroup(11, 1, new Uint8Array([1])));
  await vi.waitFor(() => expect(diagnostic).toHaveBeenCalledOnce());
  expect(diagnostic.mock.calls[0]?.[0]).toBeInstanceOf(TypeError);
  fake.openDataStream(encodeSubgroup(11, 2, new Uint8Array([2])));
  await vi.waitFor(() => expect(onObject).toHaveBeenCalledTimes(2));
  expect(diagnostic).toHaveBeenCalledOnce();
});

it.each([0, -1, 1.2, Number.POSITIVE_INFINITY, Number.NaN])(
  'rejects invalid capacity %s before opening a subscription',
  (capacity) => {
    const subscribe = vi.fn();
    expect(() => createSubscriptionEvents(subscribe, () => () => {}, { capacity })).toThrow('capacity');
    expect(subscribe).not.toHaveBeenCalled();
  },
);

it('cancels an idle subgroup reader on early exit without a spurious reset callback', async () => {
  const { fake, connection } = await setup();
  const events = connection.subscribeEvents({ namespace: ['test'], track: 'events' });
  const request = await fake.nextRequestStream();
  await request.send(encodeSubscribeOk(11));
  await events.next();
  const stream = fake.openControlledDataStream();
  await stream.write(encodeSubgroup(11, 4, new Uint8Array([1])));
  await events.next();
  await events.return();
  await expect(stream.write(new Uint8Array([0, 0, 0]))).rejects.toBeUndefined();
  expect(await events.next()).toMatchObject({ done: true });
  expect(fake.getCloseInfo()).toBeUndefined();
});

it('detaches cancellation listeners even when subscribe throws', () => {
  const signal = new AbortController().signal;
  const remove = vi.spyOn(signal, 'removeEventListener');
  const unsubscribe = vi.fn();
  expect(() =>
    createSubscriptionEvents(
      () => {
        throw new Error('bad options');
      },
      () => unsubscribe,
      { signal },
    ),
  ).toThrow('bad options');
  expect(unsubscribe).toHaveBeenCalledOnce();
  expect(remove).toHaveBeenCalledOnce();
});

it('releases the request stream after rejection even if the peer leaves it open', async () => {
  const { fake, connection } = await setup();
  const events = connection.subscribeEvents({ namespace: ['test'], track: 'events' });
  const request = await fake.nextRequestStream();
  // A later frame already buffered in the same chunk must not resurrect alias routing.
  await request.send(new Uint8Array([...encodeRequestError(1, 'denied'), ...encodeSubscribeOk(11)]));
  await expect(events.next()).rejects.toBeInstanceOf(SubscriptionRequestError);
  await expect(request.send(encodeSubscribeOk(11))).rejects.toBeUndefined();
  const other = connection.subscribeEvents({ namespace: ['test'], track: 'other' });
  await (await fake.nextRequestStream()).send(encodeSubscribeOk(11));
  expect((await other.next()).value.type).toBe('subscribed');
  other.close();
  expect(fake.getCloseInfo()).toBeUndefined();
});

it.each([false, true])('a direct handoff is outside capacity and survives teardown, overflow: %s', async (overflow) => {
  let handlers!: SubscriptionHandlers;
  const cancel = vi.fn();
  const events = createSubscriptionEvents(
    (value) => {
      handlers = value;
      return { requestId: 0, cancel, update() {} };
    },
    () => () => {},
    { capacity: 1 },
  );
  const pending = events.next();
  handlers.onUpdateOk?.(); // direct handoff
  handlers.onUpdateOk?.(); // exactly one queued event
  expect(cancel).not.toHaveBeenCalled();
  if (overflow) handlers.onUpdateOk?.();
  else events.close();
  expect((await pending).value.type).toBe('updateOk');
  if (overflow) await expect(events.next()).rejects.toBeInstanceOf(SubscriptionOverflowError);
  else expect(await events.next()).toMatchObject({ done: true });
});

it('snapshots handlers at subscription creation', async () => {
  const { fake, connection } = await setup();
  const original = vi.fn();
  const replacement = vi.fn();
  const handlers = { onOk: original };
  connection.subscribe({ namespace: ['test'], track: 'events' }, handlers);
  handlers.onOk = replacement;
  await (await fake.nextRequestStream()).send(encodeSubscribeOk(11));
  await vi.waitFor(() => expect(original).toHaveBeenCalledOnce());
  expect(replacement).not.toHaveBeenCalled();
});
