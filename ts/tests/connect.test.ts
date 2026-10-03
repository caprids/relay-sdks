import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  encodeGoaway,
  encodePublishDone,
  encodeRequestError,
  encodeSetup,
  encodeSubscribeOk,
  PUBLISH_DONE_STATUS,
  REQUEST_ERROR_CODE,
} from '../packages/moq-core/src/protocol/control-messages.js';
import { openRelaySession } from '../src/connect-session.js';
import {
  type ConnectRelaySubscriberOptions,
  connectRelaySubscriber,
  type MoqtObject,
  RelayError,
  type RelaySubscriber,
  type RelayTransportFactory,
  SubscriptionConnectionClosedError,
  SubscriptionRequestError,
} from '../src/index.js';
import { CLOSURE_GRACE_MS } from '../src/subscription.js';
import { createFakeTransport, encodeSubgroup } from './helpers/relay.js';

const connections: RelaySubscriber[] = [];
const endpoint = 'https://relay.example.com/moq';

afterEach(() => {
  for (const connection of connections.splice(0)) connection.close();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

async function connectFake(options: Partial<ConnectRelaySubscriberOptions> = {}) {
  const fake = createFakeTransport();
  const createTransport = vi.fn<RelayTransportFactory>(() => ({ transport: fake.transport, ready: Promise.resolve() }));
  const connecting = connectRelaySubscriber({ url: endpoint, createTransport, ...options });

  fake.sendServerSetup();
  const connection = await connecting;

  connections.push(connection);
  return { ...fake, connection, createTransport };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });

  return { promise, resolve, reject };
}

describe('connectRelaySubscriber', () => {
  it.each([new Error('owner stopped'), 'stop', null])('preserves cancellation identity: %s', async (reason) => {
    const owner = new AbortController();
    const { connection } = await connectFake({ signal: owner.signal });
    owner.abort(reason);
    expect((await connection.closed).error).toBe(reason);
    let caught: unknown = Symbol('not thrown');
    try {
      connection.subscribe({ namespace: ['room'], track: 'events' });
    } catch (error) {
      caught = error;
    }
    expect(caught).toBe(reason);
  });

  it('preserves token-provider exceptions', async () => {
    const cause = new TypeError('token provider failed');
    await expect(
      connectRelaySubscriber({
        url: endpoint,
        getToken: () => {
          throw cause;
        },
      }),
    ).rejects.toBe(cause);
  });

  it('shares a wrapped transport failure between closure and rejected operations', async () => {
    const fake = createFakeTransport();
    const failure = deferred<never>();
    const connecting = connectRelaySubscriber({
      url: endpoint,
      createTransport: () => ({ transport: { ...fake.transport, closed: failure.promise }, ready: Promise.resolve() }),
    });
    fake.sendServerSetup();
    const connection = await connecting;
    connections.push(connection);
    const cause = new Error('transport failed');
    failure.reject(cause);
    const { error } = await connection.closed;
    expect(error).toBeInstanceOf(RelayError);
    expect(error).toMatchObject({ code: 'TRANSPORT_ERROR', phase: 'connection', cause });
    let rejected: unknown;
    try {
      connection.subscribe({ namespace: ['room'], track: 'events' });
    } catch (failure) {
      rejected = failure;
    }
    expect(rejected).toBe(error);
  });

  it.each([[], ['live', ''], ['live', 7], new Array(1), 'live', null].map((namespace) => ({ namespace })))(
    'rejects invalid subscription namespace %j before sending',
    async ({ namespace }) => {
      const fake = await connectFake();
      const invalid = namespace as unknown as string[];
      expect(() => fake.connection.subscribe({ namespace: invalid, track: 'events' })).toThrow(TypeError);
      expect(() => fake.connection.trackSource.subscribe({ trackNamespace: invalid, trackName: 'events' })).toThrow(
        TypeError,
      );
      fake.connection.subscribe({ namespace: ['live', 'room/42'], track: 'events' });
      const request = await fake.nextRequestStream();
      expect(await request.firstMessage).toMatchObject({ requestId: 0, trackNamespace: ['live', 'room/42'] });
    },
  );

  it('rejects oversized namespaces without consuming a request ID and accepts 32 fields', async () => {
    const fake = await connectFake();
    const namespace = Array.from({ length: 32 }, (_, index) => `field-${index}`);
    expect(() => fake.connection.subscribe({ namespace: [...namespace, 'extra'], track: 'events' })).toThrow(
      /at most 32/,
    );
    expect(() =>
      fake.connection.trackSource.subscribe({ trackNamespace: [...namespace, 'extra'], trackName: 'events' }),
    ).toThrow(/at most 32/);
    fake.connection.subscribe({ namespace, track: 'events' });
    const request = await fake.nextRequestStream();
    expect(await request.firstMessage).toMatchObject({ requestId: 0, trackNamespace: namespace });
  });

  it('subscribes to arbitrary binary tracks without WebTransport globals, a catalog, or media timestamps', async () => {
    vi.stubGlobal('WebTransport', undefined);
    const fake = await connectFake();
    const objects: MoqtObject[] = [];

    fake.connection.subscribe(
      { namespace: ['sensors', 'room/42'], track: 'readings' },
      { onObject: (object) => objects.push(object) },
    );
    const request = await fake.nextRequestStream();

    expect(await request.firstMessage).toMatchObject({
      kind: 'subscribe',
      requestId: 0,
      trackNamespace: ['sensors', 'room/42'],
      trackName: 'readings',
    });
    await request.send(encodeSubscribeOk(11));
    const payload = new Uint8Array([0xff, 0, 0x80, 1]);

    fake.openDataStream(encodeSubgroup(11, 8, payload));
    await vi.waitFor(() => expect(objects).toHaveLength(1));
    expect(objects[0]).toMatchObject({ groupId: 8, objectId: 0, properties: [], payload });
  });

  it('ends pending, accepted and cancelled subscriptions with a GOAWAY as a closure, never as a request error', async () => {
    const fake = createFakeTransport();
    const control = fake.openControlledDataStream();
    const connecting = connectRelaySubscriber({
      url: endpoint,
      createTransport: () => ({ transport: fake.transport, ready: Promise.resolve() }),
    });
    await control.write(encodeSetup([]));
    const connection = await connecting;
    connections.push(connection);
    const pendingError = vi.fn();
    const acceptedError = vi.fn();
    const cancelledError = vi.fn();
    const onSubscribed = vi.fn();
    const pending = connection.subscribe(
      { namespace: ['live'], track: 'pending' },
      { onError: pendingError, onSubscribed },
    );
    await (await fake.nextRequestStream()).firstMessage;
    const acceptedSubscription = connection.subscribe(
      { namespace: ['live'], track: 'accepted' },
      { onError: acceptedError },
    );
    const accepted = await fake.nextRequestStream();
    await accepted.firstMessage;
    await accepted.send(encodeSubscribeOk(11));
    await acceptedSubscription.subscribed;
    const cancelled = connection.subscribe({ namespace: ['live'], track: 'cancelled' }, { onError: cancelledError });
    await (await fake.nextRequestStream()).firstMessage;
    cancelled.cancel();
    await control.write(encodeGoaway(1000));
    const { error } = await connection.closed;
    expect(error).toBeInstanceOf(RelayError);
    expect(error).toMatchObject({ code: 'GOAWAY', phase: 'connection' });
    expect((error as Error).message).toContain('migration');
    expect(fake.getCloseInfo()).toMatchObject({ closeCode: 0, reason: '' });
    connection.close();
    await expect(pending.subscribed).rejects.toBeInstanceOf(SubscriptionConnectionClosedError);
    await expect(pending.subscribed).rejects.toMatchObject({ cause: error });
    await expect(pending.ended).resolves.toEqual({
      kind: 'terminated',
      cause: 'connection-closed',
      accepted: false,
      error,
    });
    await expect(acceptedSubscription.ended).resolves.toMatchObject({ kind: 'terminated', accepted: true, error });
    await expect(cancelled.ended).resolves.toMatchObject({ kind: 'cancelled' });
    expect(onSubscribed).not.toHaveBeenCalled();
    expect(pendingError).not.toHaveBeenCalled();
    expect(acceptedError).not.toHaveBeenCalled();
    expect(cancelledError).not.toHaveBeenCalled();
  });

  it('contains an onError that throws on rejection and keeps the connection usable', async () => {
    const fake = await connectFake();
    const onError = vi.fn(() => {
      throw new Error('application callback failed');
    });
    const subscription = fake.connection.subscribe({ namespace: ['room'], track: 'missing' }, { onError });
    const request = await fake.nextRequestStream();
    await request.firstMessage;
    await request.send(encodeRequestError(REQUEST_ERROR_CODE.DOES_NOT_EXIST, 'missing track'));
    await expect(subscription.subscribed).rejects.toBeInstanceOf(SubscriptionRequestError);
    await expect(subscription.ended).resolves.toMatchObject({ kind: 'rejected' });
    expect(onError).toHaveBeenCalledOnce();
    const other = fake.connection.subscribe({ namespace: ['room'], track: 'present' });
    await (await fake.nextRequestStream()).send(encodeSubscribeOk(11));
    await expect(other.subscribed).resolves.toMatchObject({ trackAlias: 11 });
  });

  it('reports a transport drop before SUBSCRIBE_OK as a closure with the connection error, not as a rejection', async () => {
    const fake = await connectFake();
    const onError = vi.fn();
    const subscription = fake.connection.subscribe({ namespace: ['room'], track: 'pending' }, { onError });
    await (await fake.nextRequestStream()).firstMessage;
    fake.dropTransport();
    // The fake drops without a transport error, so the closure carries none; the core still fails
    // the pending request first, and that failure must not become the subscription's ending.
    const { error } = await fake.connection.closed;
    expect(error).toBeUndefined();
    const failure: unknown = await subscription.subscribed.catch((reason: unknown) => reason);
    expect(failure).toBeInstanceOf(SubscriptionConnectionClosedError);
    expect((failure as SubscriptionConnectionClosedError).cause).toBeUndefined();
    await expect(subscription.ended).resolves.toEqual({
      kind: 'terminated',
      cause: 'connection-closed',
      accepted: false,
      error: undefined,
    });
    expect(onError).not.toHaveBeenCalled();
  });

  it('reports a transport drop as a closure when the request stream fails tasks before closed settles', async () => {
    // Chromium's order for a connection lost while a request stream opens (issue #102): the stream
    // is aborted first and WebTransport's closed settles a task or more later.
    const fake = await connectFake();
    const onError = vi.fn();
    const subscription = fake.connection.subscribe({ namespace: ['room'], track: 'pending' }, { onError });
    const request = await fake.nextRequestStream();
    await request.firstMessage;
    request.reset(new Error('The stream was aborted by the remote server'));
    for (let task = 0; task < 5; task++) await new Promise((resolve) => setTimeout(resolve, 0));
    fake.dropTransport();
    await expect(subscription.subscribed).rejects.toBeInstanceOf(SubscriptionConnectionClosedError);
    await expect(subscription.ended).resolves.toMatchObject({
      kind: 'terminated',
      cause: 'connection-closed',
      accepted: false,
    });
    expect(onError).not.toHaveBeenCalled();
  });

  it('rejects a request stream the relay resets on a live connection once the closure grace passes', async () => {
    vi.useFakeTimers();
    const fake = await connectFake();
    const onError = vi.fn();
    const subscription = fake.connection.subscribe({ namespace: ['room'], track: 'reset' }, { onError });
    const request = await fake.nextRequestStream();
    await request.firstMessage;
    request.reset(new Error('stream reset'));
    await vi.advanceTimersByTimeAsync(CLOSURE_GRACE_MS);
    await expect(subscription.subscribed).rejects.toBeInstanceOf(SubscriptionRequestError);
    await expect(subscription.ended).resolves.toMatchObject({ kind: 'rejected', cause: 'request-error' });
    expect(onError).toHaveBeenCalledOnce();
  });

  it('keeps quiet data tracks alive after their subscription is accepted', async () => {
    vi.useFakeTimers();
    const fake = await connectFake();
    const onError = vi.fn();
    const onObject = vi.fn();

    fake.connection.subscribe({ namespace: ['scores'], track: 'current' }, { onError, onObject });
    const request = await fake.nextRequestStream();

    await request.firstMessage;
    await request.send(encodeSubscribeOk(11));
    await vi.advanceTimersByTimeAsync(60_000);
    expect(onError).not.toHaveBeenCalled();

    const payload = new Uint8Array([1, 0]);

    fake.openDataStream(encodeSubgroup(11, 0, payload));
    await vi.advanceTimersByTimeAsync(0);
    expect(onObject).toHaveBeenCalledWith(expect.objectContaining({ payload }));
  });

  it('cancels one subscription while another consumer on the same connection keeps receiving', async () => {
    const fake = await connectFake();
    const close = vi.spyOn(fake.transport, 'close');
    const first = fake.connection.subscribe({ namespace: ['room'], track: 'one' });
    const firstRequest = await fake.nextRequestStream();

    await firstRequest.firstMessage;
    await firstRequest.send(encodeSubscribeOk(11));

    const onObject = vi.fn();
    const second = fake.connection.trackSource.subscribe({ trackNamespace: ['room'], trackName: 'two' }, { onObject });
    const secondRequest = await fake.nextRequestStream();

    await secondRequest.firstMessage;
    await secondRequest.send(encodeSubscribeOk(12));
    first.cancel();
    first.cancel();
    fake.openDataStream(encodeSubgroup(12, 0, new Uint8Array([42])));

    await vi.waitFor(() => expect(onObject).toHaveBeenCalledOnce());
    expect(close).not.toHaveBeenCalled();
    expect(first.requestId).not.toBe(second.requestId);
    expect(fake.createTransport).toHaveBeenCalledOnce();
  });

  it('maps product handler names onto the core and keeps protocol names on trackSource', async () => {
    const fake = await connectFake();
    const onSubscribed = vi.fn();
    const onPublisherDone = vi.fn();
    fake.connection.subscribe({ namespace: ['live'], track: 'events' }, { onSubscribed, onPublisherDone });
    const request = await fake.nextRequestStream();
    await request.firstMessage;
    await request.send(encodeSubscribeOk(11));
    await request.send(encodePublishDone(PUBLISH_DONE_STATUS.TRACK_ENDED, 0, 'track ended'));
    await vi.waitFor(() => expect(onPublisherDone).toHaveBeenCalledOnce());
    expect(onSubscribed).toHaveBeenCalledExactlyOnceWith(expect.objectContaining({ trackAlias: 11 }));
    expect(onPublisherDone).toHaveBeenCalledWith(
      expect.objectContaining({ statusCode: PUBLISH_DONE_STATUS.TRACK_ENDED, reason: 'track ended' }),
    );

    const onOk = vi.fn();
    const onDone = vi.fn();
    fake.connection.trackSource.subscribe({ trackNamespace: ['live'], trackName: 'raw' }, { onOk, onDone });
    const second = await fake.nextRequestStream();
    await second.firstMessage;
    await second.send(encodeSubscribeOk(12));
    await second.send(encodePublishDone(PUBLISH_DONE_STATUS.TRACK_ENDED, 0, 'track ended'));
    await vi.waitFor(() => expect(onDone).toHaveBeenCalledOnce());
    expect(onOk).toHaveBeenCalledExactlyOnceWith(expect.objectContaining({ trackAlias: 12 }));
  });

  it('provides a borrowed generic track interface without connection shutdown', async () => {
    const { connection } = await connectFake();

    expect(connection.trackSource).not.toHaveProperty('close');
    expect(connection.trackSource).not.toHaveProperty('destroy');
    expect(connection.trackSource.closed).toBe(connection.closed);
    connection.close();
    await expect(connection.closed).resolves.toEqual({});
    expect(() => connection.trackSource.subscribe({ trackNamespace: ['room'], trackName: 'events' })).toThrow(/closed/);
  });

  it('preserves request error codes without closing unrelated consumers', async () => {
    const fake = await connectFake();
    const close = vi.spyOn(fake.transport, 'close');
    const onError = vi.fn();

    fake.connection.subscribe({ namespace: ['room'], track: 'missing' }, { onError });
    const request = await fake.nextRequestStream();

    await request.firstMessage;
    await request.send(encodeRequestError(REQUEST_ERROR_CODE.DOES_NOT_EXIST, 'missing track'));
    await vi.waitFor(() => expect(onError).toHaveBeenCalled());
    expect(onError).toHaveBeenCalledWith(
      expect.objectContaining({ errorCode: REQUEST_ERROR_CODE.DOES_NOT_EXIST, reason: 'missing track' }),
    );
    expect(close).not.toHaveBeenCalled();
  });

  it('adds Relay authentication to the connection URL and negotiates the pinned protocol', async () => {
    const getToken = vi.fn(async () => 'token with/&?');
    const { createTransport } = await connectFake({ url: `${endpoint}?keep=1`, getToken });
    const url = new URL(createTransport.mock.calls[0]![0]);

    expect(url.searchParams.get('keep')).toBe('1');
    expect(url.searchParams.get('jwt')).toBe('token with/&?');
    expect(createTransport.mock.calls[0]![1]).toEqual(['moqt-20']);
    expect(getToken).toHaveBeenCalledOnce();
  });

  it('does not mint a token when the endpoint already contains an explicit jwt', async () => {
    const getToken = vi.fn(() => {
      throw new Error('must not run');
    });
    const { createTransport } = await connectFake({ url: `${endpoint}?jwt=explicit`, getToken });

    expect(createTransport).toHaveBeenCalledWith(`${endpoint}?jwt=explicit`, ['moqt-20']);
    expect(getToken).not.toHaveBeenCalled();
  });

  it.each(['?jwt=', '?jwt', '?keep=1&jwt=', '?jwt=&jwt=explicit', '?jwt=explicit&jwt='])(
    'rejects an empty jwt on the endpoint before the token provider or transport: %s',
    async (query) => {
      const getToken = vi.fn(() => 'token');
      const createTransport = vi.fn<RelayTransportFactory>();

      const connecting = connectRelaySubscriber({ url: `${endpoint}${query}`, getToken, createTransport });

      await expect(connecting).rejects.toThrow(RangeError);
      await expect(connecting).rejects.toThrow(/empty jwt query parameter.*supply getToken/);
      expect(getToken).not.toHaveBeenCalled();
      expect(createTransport).not.toHaveBeenCalled();
    },
  );

  it('closes an established connection when the owner aborts and closes the transport only once', async () => {
    const owner = new AbortController();
    const fake = await connectFake({ signal: owner.signal });
    const close = vi.spyOn(fake.transport, 'close');
    const error = new Error('owner finished');

    owner.abort(error);
    await expect(fake.connection.closed).resolves.toEqual({ error });
    fake.connection.close();
    expect(close).toHaveBeenCalledOnce();
  });

  it('observes peer closure and prevents new requests on the dead connection', async () => {
    const fake = await connectFake();

    fake.dropTransport();
    await fake.connection.closed;
    expect(() => fake.connection.subscribe({ namespace: ['room'], track: 'events' })).toThrow(
      expect.objectContaining({ code: 'CLOSED', phase: 'connection' }),
    );
  });

  it('aborts while a token provider is pending without opening a late transport', async () => {
    const owner = new AbortController();
    const token = deferred<string>();
    const createTransport = vi.fn();
    const connecting = connectRelaySubscriber({
      url: endpoint,
      signal: owner.signal,
      getToken: () => token.promise,
      createTransport,
    });
    const rejected = expect(connecting).rejects.toThrow('cancelled');

    owner.abort(new Error('cancelled'));
    await rejected;
    token.resolve('late token');
    await Promise.resolve();
    expect(createTransport).not.toHaveBeenCalled();
  });

  it('closes a pending transport immediately on abort without closing it again when ready settles', async () => {
    const owner = new AbortController();
    const fake = createFakeTransport();
    const ready = deferred<void>();
    const close = vi.spyOn(fake.transport, 'close');
    const connecting = connectRelaySubscriber({
      url: endpoint,
      signal: owner.signal,
      createTransport: () => ({ transport: fake.transport, ready: ready.promise }),
    });
    const rejected = expect(connecting).rejects.toThrow('cancelled');

    owner.abort(new Error('cancelled'));
    await rejected;
    expect(close).toHaveBeenCalledOnce();
    ready.resolve();
    await Promise.resolve();
    expect(close).toHaveBeenCalledOnce();
  });

  it('closes a transport returned by a factory that aborts synchronously', async () => {
    const owner = new AbortController();
    const fake = createFakeTransport();
    const close = vi.spyOn(fake.transport, 'close');

    await expect(
      connectRelaySubscriber({
        url: endpoint,
        signal: owner.signal,
        createTransport: () => {
          owner.abort(new Error('factory aborted'));
          return { transport: fake.transport, ready: Promise.resolve() };
        },
      }),
    ).rejects.toThrow('factory aborted');
    expect(close).toHaveBeenCalledOnce();
  });

  it('times out a transport that never completes the handshake', async () => {
    vi.useFakeTimers();
    const fake = createFakeTransport();
    const close = vi.spyOn(fake.transport, 'close');
    const connecting = connectRelaySubscriber({
      url: endpoint,
      connectTimeoutMs: 20,
      createTransport: () => ({ transport: fake.transport, ready: new Promise(() => {}) }),
    });
    const rejected = expect(connecting).rejects.toMatchObject({ code: 'CONNECTION_TIMEOUT', phase: 'connecting' });

    await vi.advanceTimersByTimeAsync(20);
    await rejected;
    expect(close).toHaveBeenCalledOnce();
  });

  it('times out a peer that connects but never sends protocol SETUP', async () => {
    vi.useFakeTimers();
    const fake = createFakeTransport();
    const close = vi.spyOn(fake.transport, 'close');
    const connecting = connectRelaySubscriber({
      url: endpoint,
      connectTimeoutMs: 20,
      createTransport: () => ({ transport: fake.transport, ready: Promise.resolve() }),
    });
    const rejected = expect(connecting).rejects.toMatchObject({ code: 'CONNECTION_TIMEOUT', phase: 'connecting' });

    await vi.advanceTimersByTimeAsync(20);
    await rejected;
    expect(close).toHaveBeenCalledOnce();
  });

  // draft-20 §3.5 UNAUTHORIZED; moq-relay closes with it, reason "unauthorized", after reading SETUP (#14, TA-9).
  it('reports a peer close with UNAUTHORIZED before server SETUP as a credentials refusal', async () => {
    const fake = createFakeTransport();
    const connecting = connectRelaySubscriber({
      url: endpoint,
      getToken: () => 'secret-token',
      createTransport: () => ({ transport: fake.transport, ready: Promise.resolve() }),
    });

    await fake.clientSetupOptions();
    fake.peerClose({ closeCode: 0x2, reason: 'unauthorized' });
    const error = await connecting.catch((caught: unknown) => caught);

    expect(error).toMatchObject({ name: 'RelayError', code: 'UNAUTHORIZED', phase: 'connecting' });
    expect((error as Error).message).toMatch(/credentials/);
    expect((error as Error).message).toMatch(/unauthorized/);
    expect((error as Error).message).not.toMatch(/secret-token/);
  });

  it('reports a session error carrying UNAUTHORIZED while connecting as a credentials refusal (TA-24)', async () => {
    const cause = Object.assign(new Error('the peer closed the session (code 2, unauthorized)'), {
      source: 'session',
      closeCode: 0x2,
      reason: 'unauthorized',
    });
    const fake = createFakeTransport();

    await expect(
      connectRelaySubscriber({
        url: endpoint,
        createTransport: () => ({ transport: fake.transport, ready: Promise.reject(cause) }),
      }),
    ).rejects.toMatchObject({ code: 'UNAUTHORIZED', phase: 'connecting', cause });
  });

  // The session can end in the turn server SETUP arrives, before the connect call resumes; that is no refusal either.
  it('does not read a session error with UNAUTHORIZED in the turn server SETUP arrives as a credentials refusal', async () => {
    const sessionError = Object.assign(new Error('the peer closed the session (code 2, unauthorized)'), {
      source: 'session',
      closeCode: 0x2,
      reason: 'unauthorized',
    });
    const fake = createFakeTransport();
    const opening = openRelaySession(
      { url: endpoint, createTransport: () => ({ transport: fake.transport, ready: Promise.resolve() }) },
      (_transport, callbacks) => {
        const session = { ready: Promise.resolve(), destroy: () => {} };
        queueMicrotask(() => {
          callbacks.onReady?.([]);
          callbacks.onClosed?.({ error: sessionError });
        });
        return session;
      },
    );

    await expect(opening).rejects.toMatchObject({ code: 'TRANSPORT_ERROR', cause: sessionError });
  });

  it('reports any other peer close code before server SETUP as a transport error', async () => {
    const fake = createFakeTransport();
    const connecting = connectRelaySubscriber({
      url: endpoint,
      createTransport: () => ({ transport: fake.transport, ready: Promise.resolve() }),
    });

    await fake.clientSetupOptions();
    fake.peerClose({ closeCode: 0x3, reason: 'protocol violation' });
    await expect(connecting).rejects.toMatchObject({ code: 'TRANSPORT_ERROR', phase: 'connecting' });
  });

  it('does not read a peer close with UNAUTHORIZED after server SETUP as a credentials refusal', async () => {
    const fake = await connectFake();

    fake.peerClose({ closeCode: 0x2, reason: 'unauthorized' });
    const { error } = await fake.connection.closed;
    expect(error).not.toMatchObject({ code: 'UNAUTHORIZED' });
  });

  it('closes a transport whose handshake rejects', async () => {
    const cause = new Error('handshake failed');
    const fake = createFakeTransport();
    const close = vi.spyOn(fake.transport, 'close');

    await expect(
      connectRelaySubscriber({
        url: endpoint,
        createTransport: () => ({ transport: fake.transport, ready: Promise.reject(cause) }),
      }),
    ).rejects.toMatchObject({ code: 'TRANSPORT_ERROR', phase: 'connecting', cause });
    expect(close).toHaveBeenCalledOnce();
  });

  it('rejects an already aborted owner before minting tokens or creating a transport', async () => {
    const owner = new AbortController();
    const getToken = vi.fn();
    const createTransport = vi.fn();

    owner.abort(new Error('already cancelled'));
    await expect(
      connectRelaySubscriber({ url: endpoint, signal: owner.signal, getToken, createTransport }),
    ).rejects.toThrow('already cancelled');
    expect(getToken).not.toHaveBeenCalled();
    expect(createTransport).not.toHaveBeenCalled();
  });

  describe('default transport', () => {
    function stubWebTransport(options: { protocol: boolean }) {
      const fake = createFakeTransport();
      // `new` on this returns the scripted fake, so the SDK drives it as the browser transport.
      // biome-ignore lint/complexity/useArrowFunction: an arrow function cannot be called with new.
      const StubWebTransport = vi.fn(function () {
        return Object.assign(fake.transport, { ready: Promise.resolve() });
      });
      if (options.protocol) {
        Object.defineProperty(StubWebTransport.prototype, 'protocol', { get: () => 'moqt-20' });
      }
      vi.stubGlobal('WebTransport', StubWebTransport);
      return { fake, constructed: StubWebTransport.mock.calls };
    }

    it('reports a runtime with no WebTransport global and names createTransport', async () => {
      vi.stubGlobal('WebTransport', undefined);
      const rejected = connectRelaySubscriber({ url: endpoint });

      await expect(rejected).rejects.toMatchObject({ code: 'TRANSPORT_UNAVAILABLE', phase: 'connecting' });
      await expect(rejected).rejects.toThrow('createTransport');
    });

    it('refuses a WebTransport global that cannot negotiate protocols, as in Deno', async () => {
      const { constructed } = stubWebTransport({ protocol: false });
      const rejected = connectRelaySubscriber({ url: endpoint });

      await expect(rejected).rejects.toMatchObject({ code: 'TRANSPORT_UNAVAILABLE', phase: 'connecting' });
      await expect(rejected).rejects.toThrow(/cannot negotiate protocols.*createTransport/);
      expect(constructed).toEqual([]);
    });

    it('connects through a WebTransport global that negotiates protocols', async () => {
      const { fake, constructed } = stubWebTransport({ protocol: true });
      const connecting = connectRelaySubscriber({ url: endpoint });

      fake.sendServerSetup();
      connections.push(await connecting);
      expect(constructed).toEqual([[endpoint, { protocols: ['moqt-20'] }]]);
    });
  });

  // Fake timers never advance here, so a rejection can only come from validating the argument.
  it.each([0, -1, Number.NaN, Number.POSITIVE_INFINITY, 2_147_483_648])(
    'rejects invalid connection deadlines: %s',
    async (connectTimeoutMs) => {
      vi.useFakeTimers();
      const createTransport = vi.fn<RelayTransportFactory>();
      const rejected = connectRelaySubscriber({ url: endpoint, connectTimeoutMs, createTransport });

      await expect(rejected).rejects.toThrow(RangeError);
      await expect(rejected).rejects.toThrow(
        'connectTimeoutMs must be between 0 (exclusive) and 2147483647 milliseconds',
      );
      expect(createTransport).not.toHaveBeenCalled();
    },
  );

  it.each([0, -1, Number.NaN, Number.POSITIVE_INFINITY, 2_147_483_648])(
    'rejects invalid request deadlines: %s',
    async (requestTimeoutMs) => {
      vi.useFakeTimers();
      const createTransport = vi.fn<RelayTransportFactory>();
      const rejected = connectRelaySubscriber({ url: endpoint, requestTimeoutMs, createTransport });

      await expect(rejected).rejects.toThrow(RangeError);
      await expect(rejected).rejects.toThrow(
        'requestTimeoutMs must be between 0 (exclusive) and 2147483647 milliseconds',
      );
      expect(createTransport).not.toHaveBeenCalled();
    },
  );

  it.each(['connectTimeoutMs', 'requestTimeoutMs'])(
    'rejects a %s that is not a number as a TypeError',
    async (name) => {
      const createTransport = vi.fn<RelayTransportFactory>();
      const rejected = connectRelaySubscriber({ url: endpoint, [name]: '100', createTransport });

      await expect(rejected).rejects.toThrow(TypeError);
      await expect(rejected).rejects.toThrow(`${name} must be a number of milliseconds`);
      expect(createTransport).not.toHaveBeenCalled();
    },
  );

  it.each([
    'http://relay.example.com/moq',
    'wss://relay.example.com/moq',
    'https://relay.example.com/moq#frag',
    'https://user@relay.example.com/moq',
    'https://user:pass@relay.example.com/moq',
  ])('rejects an endpoint Relay does not accept as a RangeError before anything connects: %s', async (url) => {
    const getToken = vi.fn(() => 'token');
    const createTransport = vi.fn<RelayTransportFactory>();

    await expect(connectRelaySubscriber({ url, getToken, createTransport })).rejects.toThrow(RangeError);
    expect(getToken).not.toHaveBeenCalled();
    expect(createTransport).not.toHaveBeenCalled();
  });

  it.each<[string, unknown]>([
    ['a relative URL', 'relay.example.com/moq'],
    ['an https URL with no host', 'https://'],
    ['a number', 7],
    ['a URL object', new URL(endpoint)],
  ])('rejects an endpoint that is not a URL string as a TypeError: %s', async (_label, url) => {
    const createTransport = vi.fn<RelayTransportFactory>();

    await expect(connectRelaySubscriber({ url: url as string, createTransport })).rejects.toThrow(TypeError);
    expect(createTransport).not.toHaveBeenCalled();
  });

  it('accepts the largest deadline a timer can hold rather than firing it on the next tick', async () => {
    vi.useFakeTimers();
    const fake = await connectFake({ connectTimeoutMs: 2_147_483_647, requestTimeoutMs: 2_147_483_647 });
    const onError = vi.fn();
    const onSubscribed = vi.fn();

    fake.connection.subscribe({ namespace: ['scores'], track: 'current' }, { onSubscribed, onError });
    const request = await fake.nextRequestStream();

    await request.firstMessage;
    await vi.advanceTimersByTimeAsync(1_000);
    expect(onError).not.toHaveBeenCalled();
    await request.send(encodeSubscribeOk(11));
    await vi.waitFor(() => expect(onSubscribed).toHaveBeenCalled());
    expect(onError).not.toHaveBeenCalled();
  });
});
