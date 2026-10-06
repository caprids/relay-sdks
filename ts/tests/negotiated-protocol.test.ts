/**
 * The core's check of the protocol the transport negotiated (spec/transport-adapter.md TA-7, TA-8): a connection
 * goes ahead only on an offered protocol, `moqt-22` or `moqt-20`, and anything else fails before CLIENT_SETUP is sent.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { MessageParameters } from '../packages/moq-core/src/index.js';
import { connectRelayPublisher, connectRelaySubscriber } from '../src/index.js';
import { createFakeTransport } from './helpers/relay.js';
import { createRelayHub } from './helpers/relay-hub.js';

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
  it.each(['moqt-22', 'moqt-20'])('sends CLIENT_SETUP when the transport negotiated %s', async (protocol) => {
    const fake = createFakeTransport({ protocol });
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
    expect((failure as Error).message).toMatch(/"moqt-22", "moqt-20"/);
    expect((failure as Error).message).toMatch(received);
    expect(openUni).not.toHaveBeenCalled();
    expect(openBidi).not.toHaveBeenCalled();
    expect(close).toHaveBeenCalledOnce();
  });
});

describe('the negotiated protocol end to end', () => {
  it.each([
    { relayProtocols: ['moqt-22', 'moqt-20'], negotiated: 'moqt-22' },
    { relayProtocols: ['moqt-20'], negotiated: 'moqt-20' },
    { relayProtocols: ['moqt-20', 'moqt-22'], negotiated: 'moqt-20' },
  ] as const)(
    'carries a subscription filter to a relay that speaks $relayProtocols in $negotiated',
    async ({ relayProtocols, negotiated }) => {
      const hub = createRelayHub({ protocols: relayProtocols });
      const url = 'https://relay.example.com/moq';
      const namespace = ['testing', 'negotiated'];
      const publisher = await connectRelayPublisher({ url, namespace, createTransport: hub.connectPublisher });
      const subscriber = await connectRelaySubscriber({ url, createTransport: hub.connectSubscriber });

      try {
        const track = publisher.registerTrack({ track: 'events' });
        const payloads: Uint8Array[] = [];
        const subscribed = vi.fn();
        const parameters: MessageParameters = {
          locationFilter: { type: 'next-object' },
          fillParameters: { locationFilter: { type: 'relative-group', groupsBeforeNext: 1 } },
        };

        subscriber.subscribe(
          { namespace, track: 'events', parameters },
          { onSubscribed: subscribed, onObject: (object) => payloads.push(object.payload) },
        );
        await vi.waitFor(() => expect(subscribed).toHaveBeenCalledOnce());

        // The hub decodes each connection in the draft that connection negotiated, so the filter it read is the one
        // the subscriber sent only if both sides agreed on the framing.
        expect(hub.downstreamSubscribes).toEqual([parameters]);
        expect(hub.negotiated).toEqual([negotiated, negotiated]);

        await track.write(new TextEncoder().encode('after the filter'));
        await vi.waitFor(() => expect(payloads).toHaveLength(1));
        expect(new TextDecoder().decode(payloads[0])).toBe('after the filter');
      } finally {
        subscriber.close();
        await publisher.close();
        hub.destroy();
      }
    },
  );
});
