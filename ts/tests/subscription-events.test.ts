import { afterEach, expect, it, vi } from 'vitest';
import {
  encodePublishDone,
  encodeRequestError,
  encodeSubscribeOk,
} from '../packages/moq-core/src/protocol/control-messages.js';
import {
  connectRelaySubscriber,
  RelayError,
  type RelaySubscriber,
  SubscriptionConnectionClosedError,
  SubscriptionOverflowError,
  SubscriptionRequestError,
} from '../src/index.js';
import type { ProductSubscription, SubscriptionConsumer } from '../src/subscription.js';
import { createSubscriptionEvents } from '../src/subscription-events.js';
import { createFakeTransport, encodeSubgroup } from './helpers/relay.js';

const connections: RelaySubscriber[] = [];
afterEach(() => {
  for (const connection of connections.splice(0)) connection.close();
  vi.restoreAllMocks();
});

/** A subscription that records its consumer, so a test can drive events into the iterator directly. */
function fakeSubscription() {
  let consumer!: SubscriptionConsumer;
  const cancel = vi.fn((reason?: unknown) =>
    consumer.ended({ kind: 'cancelled', cause: 'cancelled', accepted: false, error: reason }),
  );
  const subscribe = (value: SubscriptionConsumer): ProductSubscription => {
    consumer = value;
    return {
      requestId: 0,
      cancel,
      update() {},
      attach: () => () => {},
      accepted: false,
      end: undefined,
      subscribed: new Promise(() => {}),
      ended: new Promise(() => {}),
    };
  };
  return {
    subscribe,
    cancel,
    event: (type: 'updateOk' | 'publisherDone') => consumer.event({ type, value: undefined } as never),
  };
}

async function setup() {
  const fake = createFakeTransport();
  const connecting = connectRelaySubscriber({
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
    // A transport drop before SUBSCRIBE_OK is a closure, not a rejection, although the core fails
    // the pending request first.
    const read = expect(events.next()).rejects.toBeInstanceOf(
      mode === 'reject' || mode === 'timeout' ? SubscriptionRequestError : SubscriptionConnectionClosedError,
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
  const onSubscribed = vi.fn();
  connection.subscribe({ namespace: ['test'], track: 'other' }, { onSubscribed });
  await (await fake.nextRequestStream()).send(encodeSubscribeOk(12));
  await vi.waitFor(() => expect(onSubscribed).toHaveBeenCalledOnce());
  expect(fake.getCloseInfo()).toBeUndefined();
});

it('bounds stalled consumers, fails without draining, and cancels its subscription once', async () => {
  const fake = fakeSubscription();
  const events = createSubscriptionEvents(fake.subscribe, { capacity: 2 });
  fake.event('publisherDone');
  fake.event('updateOk');
  fake.event('updateOk');
  for (let i = 0; i < 1000; i++) fake.event('updateOk');
  await expect(events.next()).rejects.toBeInstanceOf(SubscriptionOverflowError);
  expect(fake.cancel).toHaveBeenCalledOnce();
});

it('cancels a subscription that overflowed before subscribe returned', async () => {
  // An eager transport can deliver events during construction. The ending recorded then had no
  // subscription to cancel, so the iterator must cancel it once it holds it.
  const fake = fakeSubscription();
  const events = createSubscriptionEvents(
    (consumer) => {
      const subscription = fake.subscribe(consumer);
      for (let i = 0; i < 3; i++) fake.event('updateOk');
      return subscription;
    },
    { capacity: 1 },
  );
  expect(fake.cancel).toHaveBeenCalledOnce();
  expect(fake.cancel.mock.calls[0]?.[0]).toBeInstanceOf(SubscriptionOverflowError);
  await expect(events.next()).rejects.toBeInstanceOf(SubscriptionOverflowError);
});

it('supports slow reads within capacity and rejects concurrent reads', async () => {
  const fake = fakeSubscription();
  const events = createSubscriptionEvents(fake.subscribe, { capacity: 2 });
  fake.event('updateOk');
  fake.event('publisherDone');
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
    expect(() => createSubscriptionEvents(subscribe, { capacity })).toThrow('capacity');
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
  expect(() =>
    createSubscriptionEvents(
      () => {
        throw new Error('bad options');
      },
      { signal },
    ),
  ).toThrow('bad options');
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
  const fake = fakeSubscription();
  const events = createSubscriptionEvents(fake.subscribe, { capacity: 1 });
  const pending = events.next();
  fake.event('updateOk'); // direct handoff
  fake.event('updateOk'); // exactly one queued event
  expect(fake.cancel).not.toHaveBeenCalled();
  if (overflow) fake.event('updateOk');
  else events.close();
  expect((await pending).value.type).toBe('updateOk');
  if (overflow) await expect(events.next()).rejects.toBeInstanceOf(SubscriptionOverflowError);
  else expect(await events.next()).toMatchObject({ done: true });
});

it('snapshots handlers at subscription creation', async () => {
  const { fake, connection } = await setup();
  const original = vi.fn();
  const replacement = vi.fn();
  const handlers = { onSubscribed: original };
  connection.subscribe({ namespace: ['test'], track: 'events' }, handlers);
  handlers.onSubscribed = replacement;
  await (await fake.nextRequestStream()).send(encodeSubscribeOk(11));
  await vi.waitFor(() => expect(original).toHaveBeenCalledOnce());
  expect(replacement).not.toHaveBeenCalled();
});

it.each(['onSubscribed', 'onPublisherDone'] as const)(
  'rejects async product %s before sending a request',
  async (name) => {
    const { connection } = await setup();
    expect(() => connection.subscribe({ namespace: ['test'], track: 'events' }, { [name]: async () => {} })).toThrow(
      'synchronous',
    );
    expect(connection.subscribe({ namespace: ['test'], track: 'events' }).requestId).toBe(0);
  },
);

it('sends an update issued before acceptance once SUBSCRIBE_OK arrives, under its own request ID', async () => {
  const { fake, connection } = await setup();
  const subscription = connection.subscribe({ namespace: ['test'], track: 'events' });
  subscription.update({ forward: 0 });
  const request = await fake.nextRequestStream();
  await request.firstMessage;
  expect(request.messages).toHaveLength(1);
  await request.send(encodeSubscribeOk(11));
  await vi.waitFor(() => expect(request.messages).toHaveLength(2));
  expect(request.messages[1]).toMatchObject({ kind: 'request-update', parameters: { forward: 0 } });
  expect(request.messages[1]).not.toMatchObject({ requestId: 0 });
});

it('settles subscribed on SUBSCRIBE_OK and ended on connection close, over the wire', async () => {
  const { fake, connection } = await setup();
  const subscription = connection.subscribe({ namespace: ['test'], track: 'events' });
  const request = await fake.nextRequestStream();
  await request.send(encodeSubscribeOk(11));
  await expect(subscription.subscribed).resolves.toMatchObject({ trackAlias: 11 });
  connection.close();
  await expect(subscription.ended).resolves.toMatchObject({
    kind: 'terminated',
    cause: 'connection-closed',
    accepted: true,
  });
});

it('rejects subscribed with the relay rejection and ends as rejected, over the wire', async () => {
  const { fake, connection } = await setup();
  const subscription = connection.subscribe({ namespace: ['test'], track: 'events' });
  const request = await fake.nextRequestStream();
  await request.send(encodeRequestError(1, 'denied'));
  await expect(subscription.subscribed).rejects.toMatchObject({ requestError: { errorCode: 1, reason: 'denied' } });
  await expect(subscription.ended).resolves.toMatchObject({
    kind: 'rejected',
    cause: 'request-error',
    accepted: false,
  });
  expect(fake.getCloseInfo()).toBeUndefined();
});

it('preserves the handler receiver when mapping product callback names', async () => {
  const { fake, connection } = await setup();
  const handlers = {
    calls: 0,
    onSubscribed() {
      this.calls++;
    },
  };
  connection.subscribe({ namespace: ['test'], track: 'events' }, handlers);
  await (await fake.nextRequestStream()).send(encodeSubscribeOk(11));
  await vi.waitFor(() => expect(handlers.calls).toBe(1));
});

it('integrates iterator errors with stable Relay error codes and phases', () => {
  const request = { errorCode: 404, retryInterval: 0, reason: 'missing' };
  const cause = new Error('lost');
  const errors = [
    [new SubscriptionOverflowError(), 'SUBSCRIPTION_OVERFLOW', 'subscription'],
    [new SubscriptionRequestError(request), 'SUBSCRIPTION_REQUEST_FAILED', 'subscription'],
    [new SubscriptionConnectionClosedError(cause), 'CLOSED', 'connection'],
  ] as const;
  for (const [error, code, phase] of errors) {
    expect(error).toBeInstanceOf(RelayError);
    expect(error).toMatchObject({ code, phase });
  }
  expect(errors[1][0].requestError).toBe(request);
  expect(errors[2][0].cause).toBe(cause);
});
