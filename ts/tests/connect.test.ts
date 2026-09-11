import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  encodeRequestError,
  encodeSubscribeOk,
  REQUEST_ERROR_CODE,
} from '../packages/moq-core/src/protocol/control-messages.js';
import {
  type ConnectRelayOptions,
  connectRelay,
  type MoqtObject,
  type RelayConnection,
  type RelayTransportFactory,
} from '../src/index.js';
import { createFakeTransport, encodeSubgroup } from './helpers/relay.js';

const connections: RelayConnection[] = [];
const endpoint = 'https://relay.example.com/moq';

afterEach(() => {
  for (const connection of connections.splice(0)) connection.close();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

async function connectFake(options: Partial<ConnectRelayOptions> = {}) {
  const fake = createFakeTransport();
  const createTransport = vi.fn<RelayTransportFactory>(() => ({ transport: fake.transport, ready: Promise.resolve() }));
  const connecting = connectRelay({ url: endpoint, createTransport, ...options });

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

describe('connectRelay', () => {
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
    const second = fake.connection.tracks.subscribe({ trackNamespace: ['room'], trackName: 'two' }, { onObject });
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

  it('provides a borrowed generic track interface without connection shutdown', async () => {
    const { connection } = await connectFake();

    expect(connection.tracks).not.toHaveProperty('close');
    expect(connection.tracks).not.toHaveProperty('destroy');
    expect(connection.tracks.closed).toBe(connection.closed);
    connection.close();
    await expect(connection.closed).resolves.toEqual({});
    expect(() => connection.tracks.subscribe({ trackNamespace: ['room'], trackName: 'events' })).toThrow(/closed/);
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
    expect(() => fake.connection.subscribe({ namespace: ['room'], track: 'events' })).toThrow(/closed/);
  });

  it('aborts while a token provider is pending without opening a late transport', async () => {
    const owner = new AbortController();
    const token = deferred<string>();
    const createTransport = vi.fn();
    const connecting = connectRelay({
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
    const connecting = connectRelay({
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
      connectRelay({
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
    const connecting = connectRelay({
      url: endpoint,
      connectTimeoutMs: 20,
      createTransport: () => ({ transport: fake.transport, ready: new Promise(() => {}) }),
    });
    const rejected = expect(connecting).rejects.toThrow(/timed out/);

    await vi.advanceTimersByTimeAsync(20);
    await rejected;
    expect(close).toHaveBeenCalledOnce();
  });

  it('times out a peer that connects but never sends protocol SETUP', async () => {
    vi.useFakeTimers();
    const fake = createFakeTransport();
    const close = vi.spyOn(fake.transport, 'close');
    const connecting = connectRelay({
      url: endpoint,
      connectTimeoutMs: 20,
      createTransport: () => ({ transport: fake.transport, ready: Promise.resolve() }),
    });
    const rejected = expect(connecting).rejects.toThrow(/timed out/);

    await vi.advanceTimersByTimeAsync(20);
    await rejected;
    expect(close).toHaveBeenCalledOnce();
  });

  it('closes a transport whose handshake rejects', async () => {
    const fake = createFakeTransport();
    const close = vi.spyOn(fake.transport, 'close');

    await expect(
      connectRelay({
        url: endpoint,
        createTransport: () => ({ transport: fake.transport, ready: Promise.reject(new Error('handshake failed')) }),
      }),
    ).rejects.toThrow('handshake failed');
    expect(close).toHaveBeenCalledOnce();
  });

  it('rejects an already aborted owner before minting tokens or creating a transport', async () => {
    const owner = new AbortController();
    const getToken = vi.fn();
    const createTransport = vi.fn();

    owner.abort(new Error('already cancelled'));
    await expect(connectRelay({ url: endpoint, signal: owner.signal, getToken, createTransport })).rejects.toThrow(
      'already cancelled',
    );
    expect(getToken).not.toHaveBeenCalled();
    expect(createTransport).not.toHaveBeenCalled();
  });

  it('reports missing browser transport support', async () => {
    vi.stubGlobal('WebTransport', undefined);
    await expect(connectRelay({ url: endpoint })).rejects.toThrow(/createTransport/);
  });

  it.each([0, -1, Number.NaN, Number.POSITIVE_INFINITY])(
    'rejects invalid connection deadlines: %s',
    async (connectTimeoutMs) => {
      await expect(connectRelay({ url: endpoint, connectTimeoutMs })).rejects.toThrow(RangeError);
    },
  );
});
