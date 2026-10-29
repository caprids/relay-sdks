/**
 * A connect call given no url dials DEFAULT_ENDPOINT rooted at its token's root claim ("Token-rooted endpoint" in
 * spec/behavior.md). The cases come from spec/vectors/token-rooted-endpoint.json and run in every SDK.
 */
import { readFile } from 'node:fs/promises';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  connectRelayPublisher,
  connectRelaySubscriber,
  connectSupervisedPublisher,
  connectSupervisedSubscriber,
  type RelayCredential,
  type RelayTransportFactory,
} from '../src/index.js';
import { createFakeTransport, type FakeTransport } from './helpers/relay.js';

type ErrorKind = 'type' | 'range';
interface TokenRootedCase {
  name: string;
  payload?: unknown;
  payloadText?: string;
  token?: string | null;
  expected?: string;
  error?: ErrorKind;
}
const vectors = JSON.parse(
  await readFile(new URL('../../spec/vectors/token-rooted-endpoint.json', import.meta.url), 'utf8'),
) as { cases: TokenRootedCase[] };
const ERROR_CLASS: Record<ErrorKind, ErrorConstructor> = { type: TypeError, range: RangeError };

const base64url = (text: string) => Buffer.from(text, 'utf8').toString('base64url');
const jwt = (payloadText: string) => `${base64url('{"alg":"HS256","typ":"JWT"}')}.${base64url(payloadText)}.sig`;
const rooted = (root: string, claims: object = {}) => jwt(JSON.stringify({ root, ...claims }));

/** The token a vector case's provider returns. */
function caseToken({ payload, payloadText, token }: TokenRootedCase): string | undefined {
  if (payloadText !== undefined) return jwt(payloadText);
  if (payload !== undefined) return jwt(JSON.stringify(payload));
  return token ?? undefined;
}

const cleanup: (() => unknown)[] = [];
afterEach(async () => {
  for (const close of cleanup.splice(0)) await close();
});

/** A transport factory that records each URL it dials and completes SETUP on a fresh fake transport. */
function relay() {
  const urls: string[] = [];
  const transports: FakeTransport[] = [];
  const createTransport = vi.fn<RelayTransportFactory>((url) => {
    const fake = createFakeTransport();
    urls.push(url);
    transports.push(fake);
    fake.sendServerSetup();
    return { transport: fake.transport, ready: Promise.resolve() };
  });
  return { urls, transports, createTransport };
}

describe('spec/vectors/token-rooted-endpoint.json', () => {
  it.each(vectors.cases.filter((item) => item.error === undefined))('$name', async (item) => {
    const token = caseToken(item) as string;
    const { urls, createTransport } = relay();
    const subscriber = await connectRelaySubscriber({ getToken: () => token, createTransport });
    cleanup.push(() => subscriber.close());

    expect(urls).toEqual([`${item.expected}?jwt=${token}`]);
  });

  it.each(vectors.cases.filter((item) => item.error !== undefined))('$name is refused', async (item) => {
    const token = caseToken(item);
    const { createTransport } = relay();
    const connecting = connectRelaySubscriber({ getToken: () => token, createTransport });

    await expect(connecting).rejects.toThrow(ERROR_CLASS[item.error as ErrorKind]);
    await expect(connecting).rejects.toThrow(/pass url/);
    expect(createTransport).not.toHaveBeenCalled();
  });
});

describe('a connect call with no url', () => {
  const token = rooted('acme/rooms/42');
  const expected = `https://relay.mux.global/acme/rooms/42?jwt=${token}`;

  it.each([
    [
      'connectRelaySubscriber',
      (createTransport: RelayTransportFactory) => connectRelaySubscriber({ getToken: () => token, createTransport }),
    ],
    [
      'connectRelayPublisher',
      (createTransport: RelayTransportFactory) =>
        connectRelayPublisher({ namespace: ['p'], getToken: () => token, createTransport }),
    ],
    [
      'connectSupervisedSubscriber',
      (createTransport: RelayTransportFactory) =>
        connectSupervisedSubscriber({ getToken: () => token, createTransport }),
    ],
    [
      'connectSupervisedPublisher',
      (createTransport: RelayTransportFactory) =>
        connectSupervisedPublisher({ namespace: ['p'], getToken: () => token, createTransport }),
    ],
  ])('%s dials the hosted Relay rooted at the token', async (_, connect) => {
    const { urls, createTransport } = relay();
    const connection = await connect(createTransport);
    cleanup.push(() => connection.close());

    expect(urls).toEqual([expected]);
  });

  it("roots the endpoint at a RelayCredential's token", async () => {
    const { urls, createTransport } = relay();
    const credential: RelayCredential = { token, expiresAt: new Date('2030-01-01T00:00:00Z') };
    const subscriber = await connectRelaySubscriber({ getToken: async () => credential, createTransport });
    cleanup.push(() => subscriber.close());

    expect(urls).toEqual([expected]);
    expect(subscriber.expiresAt).toEqual(credential.expiresAt);
  });

  it("still reads the token's expiry", async () => {
    const { createTransport } = relay();
    const subscriber = await connectRelaySubscriber({
      getToken: () => rooted('acme', { exp: 1_900_000_000 }),
      createTransport,
    });
    cleanup.push(() => subscriber.close());

    expect(subscriber.expiresAt).toEqual(new Date(1_900_000_000_000));
  });

  it('needs getToken, and says so before anything else runs', async () => {
    const { createTransport } = relay();
    const connecting = connectRelaySubscriber({ createTransport });

    await expect(connecting).rejects.toThrow(TypeError);
    await expect(connecting).rejects.toThrow(/no getToken.*pass url/);
    expect(createTransport).not.toHaveBeenCalled();
  });

  it('refuses a null url as it did before', async () => {
    const getToken = vi.fn(() => token);
    const connecting = connectRelaySubscriber({ url: null as never, getToken, createTransport: vi.fn() });

    await expect(connecting).rejects.toThrow(TypeError);
    expect(getToken).not.toHaveBeenCalled();
  });

  it('rejects a supervisor whose first token names no root, without dialing', async () => {
    const { createTransport } = relay();

    await expect(connectSupervisedSubscriber({ getToken: () => 'opaque', createTransport })).rejects.toThrow(TypeError);
    await expect(
      connectSupervisedPublisher({ namespace: ['p'], getToken: () => rooted('a b'), createTransport }),
    ).rejects.toThrow(RangeError);
    expect(createTransport).not.toHaveBeenCalled();
  });
});

describe('a supervisor given no url', () => {
  const fast = { initialDelayMs: 0, maxDelayMs: 0 };
  const first = rooted('acme');
  const moved = rooted('elsewhere');

  /** A provider that hands out `first`, then `moved` on every later call. */
  const provider = () => {
    let calls = 0;
    return () => (calls++ === 0 ? first : moved);
  };

  it.each([
    [
      'subscriber',
      (createTransport: RelayTransportFactory) =>
        connectSupervisedSubscriber({ getToken: provider(), createTransport, reconnect: fast }),
    ],
    [
      'publisher',
      (createTransport: RelayTransportFactory) =>
        connectSupervisedPublisher({ namespace: ['p'], getToken: provider(), createTransport, reconnect: fast }),
    ],
  ])('%s keeps the endpoint of its first connection across a reconnect and a rotation', async (_, connect) => {
    const { urls, transports, createTransport } = relay();
    const supervised = await connect(createTransport);
    cleanup.push(() => supervised.close());

    transports[0]!.dropTransport();
    await vi.waitFor(() => expect(urls).toHaveLength(2));
    await vi.waitFor(() => expect(supervised.status).toBe('live'));
    supervised.rotate();
    await vi.waitFor(() => expect(urls).toHaveLength(3));

    expect(urls).toEqual([
      `https://relay.mux.global/acme?jwt=${first}`,
      `https://relay.mux.global/acme?jwt=${moved}`,
      `https://relay.mux.global/acme?jwt=${moved}`,
    ]);
  });
});
