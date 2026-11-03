import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  encodeNamespace,
  encodeNamespaceDone,
  encodeRequestError,
  encodeRequestOk,
  type KeyValuePair,
  REQUEST_ERROR_CODE,
  SETUP_OPTION,
} from '../packages/moq-core/src/protocol/control-messages.js';
import {
  connectRelayPublisher,
  connectRelaySubscriber,
  type NamespaceEvent,
  type NamespaceEvents,
  type RelaySubscriber,
  SNAPSHOT_LIMIT_MS,
  SNAPSHOT_QUIET_MS,
  SubscriptionConnectionClosedError,
  SubscriptionOverflowError,
  SubscriptionRequestError,
} from '../src/index.js';
import { createFakeTransport } from './helpers/relay.js';

const connections: { close(): void }[] = [];
afterEach(() => {
  for (const connection of connections.splice(0)) connection.close();
});

async function setup({ requestTimeoutMs = 30, serverSetup = [] as KeyValuePair[] } = {}) {
  const fake = createFakeTransport();
  const connecting = connectRelaySubscriber({
    url: 'https://relay.example.com/moq',
    requestTimeoutMs,
    createTransport: () => ({ transport: fake.transport, ready: Promise.resolve() }),
  });
  fake.sendServerSetup(serverSetup);
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

it('throws when the core cannot encode the prefix, and leaves the connection usable', async () => {
  const { fake, connection } = await setup();
  expect(() => connection.namespaceEvents({ prefix: ['x'.repeat(70_000)] })).toThrow();
  const events = connection.namespaceEvents({ prefix: ['room'] });
  const request = await fake.nextRequestStream();
  expect(await request.firstMessage).toMatchObject({ kind: 'subscribe-namespace', trackNamespacePrefix: ['room'] });
  events.close();
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

it('withdraws what is still announced, then ends cleanly, when the relay finishes the request', async () => {
  const { fake, connection } = await setup();
  const events = connection.namespaceEvents({ prefix: ['room'] });
  const request = await fake.nextRequestStream();
  await request.firstMessage;
  await request.send(encodeRequestOk());
  await request.send(encodeNamespace(['alice']));
  await request.fin();
  // Nothing was read before the end: the queued events survive it.
  await new Promise((resolve) => setTimeout(resolve, 20));
  expect((await events.next()).value).toEqual({ type: 'announced', namespace: ['room', 'alice'] });
  expect((await events.next()).value).toEqual({ type: 'unannounced', namespace: ['room', 'alice'] });
  expect(await events.next()).toMatchObject({ done: true });
});

it('withdraws nothing when the request stream fails: the error ends it, and everything counts as withdrawn', async () => {
  const { fake, connection } = await setup();
  const events = connection.namespaceEvents({ prefix: ['room'] });
  const request = await fake.nextRequestStream();
  await request.firstMessage;
  await request.send(encodeRequestOk());
  await request.send(encodeNamespace(['alice']));
  expect((await events.next()).value).toEqual({ type: 'announced', namespace: ['room', 'alice'] });
  request.reset(new Error('gone'));
  await expect(events.next()).rejects.toBeInstanceOf(SubscriptionRequestError);
});

it('refuses a prefix overlapping one this connection is discovering, until that one ends', async () => {
  const { fake, connection } = await setup();
  const first = connection.namespaceEvents({ prefix: ['room', 'a'] });
  await fake.nextRequestStream();
  for (const prefix of [['room'], ['room', 'a'], ['room', 'a', 'b']]) {
    expect(() => connection.namespaceEvents({ prefix })).toThrow(
      expect.objectContaining({ code: 'PREFIX_OVERLAP', phase: 'subscription' }),
    );
  }
  // A sibling is no overlap: fields match whole.
  const sibling = connection.namespaceEvents({ prefix: ['room', 'ab'] });
  first.close();
  sibling.close();
  const again = connection.namespaceEvents({ prefix: ['room'] });
  expect(again).toBeDefined();
  again.close();
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
    expect(event).toEqual({ type: 'announced', namespace: ['room', 'alice'] });
    break;
  }
  expect(await second.next()).toMatchObject({ done: true });
});

/** Read every event `events` yields into `seen`, in the background, until it ends. */
function collect(events: NamespaceEvents): NamespaceEvent[] {
  const seen: NamespaceEvent[] = [];
  void (async () => {
    for await (const event of events) seen.push(event);
  })().catch(() => {});
  return seen;
}

/**
 * Let the fake relay's bytes reach the iterator without moving time: the stream pipeline is all microtasks, and
 * `vi.waitFor` would advance fake timers while it polls.
 */
const delivered = () => new Promise((resolve) => setImmediate(resolve));

const LOADED: NamespaceEvent = { type: 'loaded' };
const announced = (...namespace: string[]): NamespaceEvent => ({ type: 'announced', namespace });

it('declares MoQ Active Count on the subscriber setup and not on the publisher setup', async () => {
  const { fake } = await setup();
  expect((await fake.clientSetupOptions()).filter((option) => option.type === SETUP_OPTION.ACTIVE_COUNT)).toEqual([
    { type: SETUP_OPTION.ACTIVE_COUNT, value: 1 },
  ]);

  const publisherFake = createFakeTransport();
  const owner = new AbortController();
  void connectRelayPublisher({
    url: 'https://relay.example.com/moq',
    namespace: ['room'],
    signal: owner.signal,
    createTransport: () => ({ transport: publisherFake.transport, ready: Promise.resolve() }),
  }).catch(() => {});
  const options = await publisherFake.clientSetupOptions();
  owner.abort();
  expect(options.some((option) => option.type === SETUP_OPTION.ACTIVE_COUNT)).toBe(false);
});

describe("'loaded' without MoQ Active Count", () => {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("yields 'loaded' after the snapshot's entries once the stream stays quiet, and once only", async () => {
    const { fake, connection } = await setup();
    const seen = collect(connection.namespaceEvents({ prefix: ['room'] }));
    const request = await fake.nextRequestStream();
    await request.firstMessage;
    await request.send(encodeRequestOk());
    await request.send(encodeNamespace(['alice']));
    await request.send(encodeNamespace(['bob']));
    await delivered();
    expect(seen).toHaveLength(2);

    await vi.advanceTimersByTimeAsync(SNAPSHOT_QUIET_MS - 1);
    expect(seen).toEqual([announced('room', 'alice'), announced('room', 'bob')]);
    await vi.advanceTimersByTimeAsync(1);
    expect(seen).toEqual([announced('room', 'alice'), announced('room', 'bob'), LOADED]);

    await request.send(encodeNamespace(['carol']));
    await delivered();
    expect(seen).toHaveLength(4);
    await vi.advanceTimersByTimeAsync(SNAPSHOT_LIMIT_MS);
    expect(seen).toEqual([announced('room', 'alice'), announced('room', 'bob'), LOADED, announced('room', 'carol')]);
  });

  it("yields 'loaded' alone on an empty prefix: REQUEST_OK, then nothing", async () => {
    const { fake, connection } = await setup();
    const seen = collect(connection.namespaceEvents({ prefix: ['room'] }));
    const request = await fake.nextRequestStream();
    await request.firstMessage;
    await request.send(encodeRequestOk());

    await vi.advanceTimersByTimeAsync(SNAPSHOT_QUIET_MS);
    expect(seen).toEqual([LOADED]);
  });

  it("yields no 'loaded' before REQUEST_OK", async () => {
    const { fake, connection } = await setup({ requestTimeoutMs: 10 * SNAPSHOT_LIMIT_MS });
    const seen = collect(connection.namespaceEvents({ prefix: ['room'] }));
    const request = await fake.nextRequestStream();
    await request.firstMessage;

    await vi.advanceTimersByTimeAsync(2 * SNAPSHOT_LIMIT_MS);
    expect(seen).toEqual([]);
    await request.send(encodeRequestOk());
    await vi.advanceTimersByTimeAsync(SNAPSHOT_QUIET_MS);
    expect(seen).toEqual([LOADED]);
  });

  it('restarts the quiet window on each entry inside it', async () => {
    const { fake, connection } = await setup();
    const seen = collect(connection.namespaceEvents({ prefix: ['room'] }));
    const request = await fake.nextRequestStream();
    await request.firstMessage;
    await request.send(encodeRequestOk());
    await vi.advanceTimersByTimeAsync(SNAPSHOT_QUIET_MS - 10);
    await request.send(encodeNamespace(['alice']));
    await delivered();
    expect(seen).toHaveLength(1);

    await vi.advanceTimersByTimeAsync(SNAPSHOT_QUIET_MS - 1);
    expect(seen).toEqual([announced('room', 'alice')]);
    await vi.advanceTimersByTimeAsync(1);
    expect(seen).toEqual([announced('room', 'alice'), LOADED]);
  });

  it("yields 'loaded' no later than SNAPSHOT_LIMIT_MS after REQUEST_OK under steady churn", async () => {
    const { fake, connection } = await setup();
    const seen = collect(connection.namespaceEvents({ prefix: ['room'], capacity: 1_000 }));
    const request = await fake.nextRequestStream();
    await request.firstMessage;
    await request.send(encodeRequestOk());

    const step = SNAPSHOT_QUIET_MS - 10;
    let elapsed = 0;
    while (!seen.some((event) => event.type === 'loaded') && elapsed <= SNAPSHOT_LIMIT_MS) {
      const entries = seen.length;
      await request.send(elapsed % (2 * step) === 0 ? encodeNamespace(['alice']) : encodeNamespaceDone(['alice']));
      await delivered();
      expect(seen.length).toBeGreaterThan(entries);
      await vi.advanceTimersByTimeAsync(step);
      await delivered();
      elapsed += step;
    }
    expect(seen.filter((event) => event.type === 'loaded')).toHaveLength(1);
    expect(elapsed).toBeGreaterThanOrEqual(SNAPSHOT_LIMIT_MS);
    expect(elapsed).toBeLessThan(SNAPSHOT_LIMIT_MS + step);
  });

  it("yields no 'loaded' once the stream fails inside the quiet window", async () => {
    const { fake, connection } = await setup();
    const events = connection.namespaceEvents({ prefix: ['room'] });
    const seen = collect(events);
    const request = await fake.nextRequestStream();
    await request.firstMessage;
    await request.send(encodeRequestOk());
    await request.send(encodeNamespace(['alice']));
    await delivered();
    expect(seen).toHaveLength(1);
    request.reset(new Error('gone'));

    await vi.advanceTimersByTimeAsync(SNAPSHOT_LIMIT_MS);
    expect(seen.some((event) => event.type === 'loaded')).toBe(false);
  });
});

describe("'loaded' with MoQ Active Count", () => {
  const ACTIVE_COUNT: KeyValuePair = { type: SETUP_OPTION.ACTIVE_COUNT, value: 1 };

  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("yields 'loaded' right after the Count-th NAMESPACE, a repeat counted, and never from the quiet window", async () => {
    const { fake, connection } = await setup({ serverSetup: [ACTIVE_COUNT] });
    const seen = collect(connection.namespaceEvents({ prefix: ['room'] }));
    const request = await fake.nextRequestStream();
    await request.firstMessage;
    await request.send(encodeRequestOk({ activeCount: 3 }));
    await request.send(encodeNamespace(['alice']));
    await request.send(encodeNamespace(['alice']));
    await delivered();
    expect(seen).toHaveLength(1);

    await vi.advanceTimersByTimeAsync(2 * SNAPSHOT_LIMIT_MS);
    expect(seen).toEqual([announced('room', 'alice')]);

    await request.send(encodeNamespace(['bob']));
    await request.send(encodeNamespace(['carol']));
    await delivered();
    expect(seen).toHaveLength(4);
    expect(seen).toEqual([announced('room', 'alice'), announced('room', 'bob'), LOADED, announced('room', 'carol')]);
  });

  it("yields 'loaded' at once on a count of 0", async () => {
    const { fake, connection } = await setup({ serverSetup: [ACTIVE_COUNT] });
    const seen = collect(connection.namespaceEvents({ prefix: ['room'] }));
    const request = await fake.nextRequestStream();
    await request.firstMessage;
    await request.send(encodeRequestOk({ activeCount: 0 }));

    await delivered();
    expect(seen).toEqual([LOADED]);
  });
});
