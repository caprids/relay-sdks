/**
 * The core's check of the protocol the transport negotiated (spec/transport-adapter.md TA-7, TA-8): a connection
 * goes ahead only on `moqt-20`, and anything else fails before CLIENT_SETUP is sent.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { connectRelayPublisher, connectRelaySubscriber } from '../src/index.js';
import { createFakeTransport } from './helpers/relay.js';

const roles = {
  subscriber: (options: Parameters<typeof connectRelaySubscriber>[0]) => connectRelaySubscriber(options),
  publisher: (options: Parameters<typeof connectRelaySubscriber>[0]) =>
    connectRelayPublisher({ ...options, namespace: ['room'] }),
};

const owners: AbortController[] = [];

afterEach(() => {
  for (const owner of owners.splice(0)) owner.abort();
});

describe.each(Object.keys(roles) as (keyof typeof roles)[])('%s negotiated protocol (TA-8)', (role) => {
  it('sends CLIENT_SETUP when the transport negotiated moqt-20', async () => {
    const fake = createFakeTransport({ protocol: 'moqt-20' });
    const owner = new AbortController();

    owners.push(owner);
    void roles[role]({
      url: 'https://relay.example.com/moq',
      signal: owner.signal,
      createTransport: () => ({ transport: fake.transport, ready: Promise.resolve() }),
    }).catch(() => {});

    await expect(fake.clientSetupOptions()).resolves.toBeDefined();
  });

  it.each([
    { protocol: '', received: /none/ },
    { protocol: 'moq-lite-03', received: /moq-lite-03/ },
  ])('fails the connect without CLIENT_SETUP when it negotiated "$protocol"', async ({ protocol, received }) => {
    const fake = createFakeTransport({ protocol });
    const openUni = vi.spyOn(fake.transport, 'createUnidirectionalStream');
    const openBidi = vi.spyOn(fake.transport, 'createBidirectionalStream');
    const close = vi.spyOn(fake.transport, 'close');

    const failure = await roles[role]({
      url: 'https://relay.example.com/moq',
      createTransport: () => ({ transport: fake.transport, ready: Promise.resolve() }),
    }).catch((error: unknown) => error);

    expect(failure).toMatchObject({ code: 'TRANSPORT_ERROR', phase: 'connecting' });
    expect((failure as Error).message).toMatch(/moqt-20/);
    expect((failure as Error).message).toMatch(received);
    expect(openUni).not.toHaveBeenCalled();
    expect(openBidi).not.toHaveBeenCalled();
    expect(close).toHaveBeenCalledOnce();
  });
});
