/**
 * The setup options the core sends for each framing (spec/transport-adapter.md TA-1, TA-4, TA-6), driven by the
 * language-neutral cases in spec/vectors/setup-options.json.
 */
import { readFile } from 'node:fs/promises';

import { afterEach, describe, expect, it, vi } from 'vitest';
import { utf8Decode } from '../packages/moq-core/src/protocol/bytes.js';
import { SETUP_OPTION } from '../packages/moq-core/src/protocol/control-messages.js';
import { connectRelayPublisher, connectRelaySubscriber, type RelayTransportFactory } from '../src/index.js';
import { createFakeTransport } from './helpers/relay.js';

interface SetupOptionsVector {
  name: string;
  url: string;
  token?: string;
  framing?: 'webtransport' | 'native-quic';
  path: string | null;
  authority: string | null;
}

const { cases } = JSON.parse(
  await readFile(new URL('../../spec/vectors/setup-options.json', import.meta.url), 'utf8'),
) as { cases: SetupOptionsVector[] };

const roles = {
  subscriber: (options: Parameters<typeof connectRelaySubscriber>[0]) => connectRelaySubscriber(options),
  publisher: (options: Parameters<typeof connectRelaySubscriber>[0]) =>
    connectRelayPublisher({ ...options, namespace: ['room'] }),
};

const owners: AbortController[] = [];

afterEach(() => {
  for (const owner of owners.splice(0)) owner.abort();
});

/** Connect `role` through a fake transport and return the client's SETUP options and the URL the factory saw. */
async function clientSetup(role: keyof typeof roles, vector: SetupOptionsVector) {
  const fake = createFakeTransport();
  const owner = new AbortController();
  const createTransport = vi.fn<RelayTransportFactory>(() => ({
    transport: fake.transport,
    ready: Promise.resolve(),
    ...(vector.framing === undefined ? {} : { framing: vector.framing }),
  }));

  owners.push(owner);
  void roles[role]({
    url: vector.url,
    signal: owner.signal,
    createTransport,
    ...(vector.token === undefined ? {} : { getToken: () => vector.token }),
  }).catch(() => {});

  const options = await fake.clientSetupOptions();
  const value = (type: number) => {
    const matches = options.filter((option) => option.type === type);

    expect(matches.length).toBeLessThanOrEqual(1);
    const [option] = matches;
    if (option === undefined) return null;
    if (!(option.value instanceof Uint8Array)) throw new Error(`setup option ${type} is not a byte field`);
    return utf8Decode(option.value);
  };

  return {
    path: value(SETUP_OPTION.PATH),
    authority: value(SETUP_OPTION.AUTHORITY),
    factoryUrl: createTransport.mock.calls[0]![0],
  };
}

describe.each(Object.keys(roles) as (keyof typeof roles)[])('%s CLIENT_SETUP (TA-6)', (role) => {
  it.each(cases)('$name', async (vector) => {
    const setup = await clientSetup(role, vector);

    expect({ path: setup.path, authority: setup.authority }).toEqual({
      path: vector.path,
      authority: vector.authority,
    });
  });

  it('hands every framing the https URL with the token in its jwt query parameter (TA-4)', async () => {
    for (const framing of ['webtransport', 'native-quic'] as const) {
      const setup = await clientSetup(role, {
        name: framing,
        url: 'https://relay.example.com/moq',
        token: 'abc',
        framing,
        path: null,
        authority: null,
      });

      expect(setup.factoryUrl).toBe('https://relay.example.com/moq?jwt=abc');
    }
  });
});

describe('an unknown framing (TA-1)', () => {
  it.each(Object.keys(roles) as (keyof typeof roles)[])(
    'fails the %s connect as a transport error and closes the transport',
    async (role) => {
      const fake = createFakeTransport();
      const close = vi.spyOn(fake.transport, 'close');
      const connecting = roles[role]({
        url: 'https://relay.example.com/moq',
        createTransport: () =>
          ({
            transport: fake.transport,
            ready: Promise.resolve(),
            framing: 'quic',
          }) as unknown as ReturnType<RelayTransportFactory>,
      });

      await expect(connecting).rejects.toMatchObject({ code: 'TRANSPORT_ERROR', message: /unknown framing "quic"/ });
      expect(close).toHaveBeenCalledOnce();
    },
  );
});
