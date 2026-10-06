import { afterEach, expect, it } from 'vitest';
import {
  encodeNamespace,
  encodeNamespaceDone,
  encodeRequestError,
  encodeRequestOk,
  REQUEST_ERROR_CODE,
  SETUP_OPTION,
} from '../packages/moq-core/src/protocol/control-messages.js';
import {
  connectRelayPublisher,
  connectRelaySubscriber,
  type RelaySubscriber,
  SubscriptionConnectionClosedError,
  SubscriptionOverflowError,
  SubscriptionRequestError,
} from '../src/index.js';
import { createFakeTransport } from './helpers/relay.js';

const connections: { close(): void }[] = [];
afterEach(() => {
  for (const connection of connections.splice(0)) connection.close();
});

async function setup() {
  const fake = createFakeTransport();
  const connecting = connectRelaySubscriber({
    url: 'https://relay.example.com/moq',
    requestTimeoutMs: 30,
    createTransport: () => ({ transport: fake.transport, ready: Promise.resolve() }),
  });
  fake.sendServerSetup();
  const connection: RelaySubscriber = await connecting;
  connections.push(connection);
  return { fake, connection };
}

it('asks the relay to advertise namespaces only when solicited', async () => {
  const { fake } = await setup();
  const options = await fake.clientSetupOptions();
  expect(options.filter((option) => option.type === SETUP_OPTION.SOLICIT)).toEqual([
    { type: SETUP_OPTION.SOLICIT, value: 1 },
  ]);
});

it('leaves the publisher setup without SOLICIT', async () => {
  const fake = createFakeTransport();
  const owner = new AbortController();
  void connectRelayPublisher({
    url: 'https://relay.example.com/moq',
    namespace: ['room'],
    signal: owner.signal,
    createTransport: () => ({ transport: fake.transport, ready: Promise.resolve() }),
  }).catch(() => {});
  const options = await fake.clientSetupOptions();
  owner.abort();
  expect(options.some((option) => option.type === SETUP_OPTION.SOLICIT)).toBe(false);
});

it('sends SUBSCRIBE_NAMESPACE for the prefix and yields full namespaces, edge-triggered', async () => {
  const { fake, connection } = await setup();
  const events = connection.namespaceEvents({ prefix: ['room', 'a/b'] });
  const request = await fake.nextRequestStream();
  expect(await request.firstMessage).toMatchObject({
    kind: 'subscribe-namespace',
    trackNamespacePrefix: ['room', 'a/b'],
  });
  await request.send(encodeRequestOk());
  await request.send(encodeNamespace(['alice']));
  await request.send(encodeNamespace(['alice'])); // a relay repeat (route change) is not passed on
  await request.send(encodeNamespace([]));
  await request.send(encodeNamespaceDone(['bob'])); // never announced here
  await request.send(encodeNamespaceDone(['alice']));
  await request.send(encodeNamespace(['alice']));

  const seen: unknown[] = [];
  for (let i = 0; i < 4; i++) seen.push((await events.next()).value);
  expect(seen).toEqual([
    { type: 'announced', namespace: ['room', 'a/b', 'alice'] },
    { type: 'announced', namespace: ['room', 'a/b'] },
    { type: 'unannounced', namespace: ['room', 'a/b', 'alice'] },
    { type: 'announced', namespace: ['room', 'a/b', 'alice'] },
  ]);
  events.close();
  expect(await events.next()).toMatchObject({ done: true });
});

it('checks the prefix before connection state and before sending anything', async () => {
  const { connection } = await setup();
  expect(() => connection.namespaceEvents({ prefix: [] })).toThrow(TypeError);
  expect(() => connection.namespaceEvents({ prefix: ['ok', ''] })).toThrow(TypeError);
  expect(() => connection.namespaceEvents(undefined as never)).toThrow(TypeError);
  expect(() => connection.namespaceEvents({ prefix: ['room'], capacity: 0 })).toThrow(RangeError);
  connection.close();
  expect(() => connection.namespaceEvents({ prefix: [] })).toThrow(TypeError);
  expect(() => connection.namespaceEvents({ prefix: ['room'] })).toThrow(expect.objectContaining({ code: 'CLOSED' }));
});

it('ends with SubscriptionRequestError when the relay rejects the request', async () => {
  const { fake, connection } = await setup();
  const events = connection.namespaceEvents({ prefix: ['room'] });
  const request = await fake.nextRequestStream();
  await request.firstMessage;
  await request.send(encodeRequestError(REQUEST_ERROR_CODE.UNAUTHORIZED, 'not yours'));
  const error = await events.next().catch((caught: unknown) => caught);
  expect(error).toBeInstanceOf(SubscriptionRequestError);
  expect((error as SubscriptionRequestError).requestError).toMatchObject({
    errorCode: REQUEST_ERROR_CODE.UNAUTHORIZED,
    reason: 'not yours',
  });
});

it('ends cleanly when the relay finishes the request', async () => {
  const { fake, connection } = await setup();
  const events = connection.namespaceEvents({ prefix: ['room'] });
  const request = await fake.nextRequestStream();
  await request.firstMessage;
  await request.send(encodeRequestOk());
  await request.send(encodeNamespace(['alice']));
  expect((await events.next()).value).toEqual({ type: 'announced', namespace: ['room', 'alice'] });
  await request.fin();
  expect(await events.next()).toMatchObject({ done: true });
});

it('ends with SubscriptionConnectionClosedError when the connection drops', async () => {
  const { fake, connection } = await setup();
  const events = connection.namespaceEvents({ prefix: ['room'] });
  const request = await fake.nextRequestStream();
  await request.firstMessage;
  await request.send(encodeRequestOk());
  const read = events.next();
  fake.dropTransport();
  await expect(read).rejects.toBeInstanceOf(SubscriptionConnectionClosedError);
});

it('ends with SubscriptionOverflowError past its capacity and cancels the request', async () => {
  const { fake, connection } = await setup();
  const events = connection.namespaceEvents({ prefix: ['room'], capacity: 1 });
  const request = await fake.nextRequestStream();
  await request.firstMessage;
  await request.send(encodeRequestOk());
  await request.send(encodeNamespace(['a']));
  await request.send(encodeNamespace(['b']));
  await expect
    .poll(() =>
      events.next().then(
        () => 'value',
        (error: unknown) => error,
      ),
    )
    .toBeInstanceOf(SubscriptionOverflowError);
});

it('cancels the request when the signal aborts, and break cancels it too', async () => {
  const { fake, connection } = await setup();
  const owner = new AbortController();
  const events = connection.namespaceEvents({ prefix: ['room'], signal: owner.signal });
  const request = await fake.nextRequestStream();
  await request.firstMessage;
  owner.abort(new Error('done here'));
  await expect(events.next()).rejects.toThrow('done here');

  const second = connection.namespaceEvents({ prefix: ['room'] });
  const secondRequest = await fake.nextRequestStream();
  await secondRequest.firstMessage;
  await secondRequest.send(encodeRequestOk());
  await secondRequest.send(encodeNamespace(['alice']));
  for await (const event of second) {
    expect(event.namespace).toEqual(['room', 'alice']);
    break;
  }
  expect(await second.next()).toMatchObject({ done: true });
});
