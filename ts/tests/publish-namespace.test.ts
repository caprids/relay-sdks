import { createMoqtSession, type MoqtObject } from '@mux/moq-core';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { solicitNamespace } from '../packages/moq-core/src/tests/helpers/raw-peer.js';
import { createTransportPair } from '../packages/moq-core/src/tests/helpers/transport-pair.js';
import { connectRelayPublisher } from '../src/index.js';

const namespace = ['live', 'example'];
const cleanup: (() => void | Promise<void>)[] = [];

afterEach(async () => {
  for (const dispose of cleanup.splice(0).reverse()) await dispose();
});

async function connectPublisher() {
  const pair = createTransportPair();
  const subscriber = createMoqtSession(pair.server);
  cleanup.push(() => subscriber.destroy());
  const publisher = await connectRelayPublisher({
    url: 'https://relay.example.com/moq',
    namespace,
    createTransport: () => ({ transport: pair.client, ready: Promise.resolve() }),
  });
  cleanup.push(() => publisher.close());
  const track = publisher.registerTrack({ track: 'events', writeTimeoutMs: 100 });
  return { pair, subscriber, track };
}

describe('publisher namespace coverage', () => {
  it.each(['write', 'openGroup'] as const)(
    '%s distinguishes an uncovered namespace and recovers after a later matching solicitation',
    async (method) => {
      const { pair, subscriber, track } = await connectPublisher();
      const unrelated = await solicitNamespace(pair.server, ['other']);
      await vi.waitFor(() => expect(unrelated.received.map((message) => message.kind)).toEqual(['request-ok']));

      const pending = method === 'write' ? track.write(new Uint8Array([1])) : track.openGroup();
      await expect(pending).rejects.toThrow('Publisher namespace is not covered by the relay namespace solicitations');
      expect(track.closed).toBe(false);

      const matching = await solicitNamespace(pair.server, ['live'], 3);
      await vi.waitFor(() =>
        expect(matching.received.map((message) => message.kind)).toEqual(['request-ok', 'namespace']),
      );
      const objects: MoqtObject[] = [];
      subscriber.subscribe(
        { trackNamespace: namespace, trackName: 'events' },
        { onObject: (object) => objects.push(object) },
      );
      if (method === 'write') {
        await track.write(new Uint8Array([2]));
      } else {
        const group = await track.openGroup();
        await group.write(new Uint8Array([2]));
        await group.close();
      }
      await vi.waitFor(() => expect(objects).toHaveLength(1));
      expect(objects[0]?.payload).toEqual(new Uint8Array([2]));
    },
  );

  it.each([false, true])(
    'keeps an ordinary demand timeout when a matching solicitation exists: %s',
    async (solicit) => {
      const { pair, track } = await connectPublisher();
      if (solicit) {
        const matching = await solicitNamespace(pair.server, ['live']);
        await vi.waitFor(() => expect(matching.received).toHaveLength(2));
      }
      await expect(track.write(new Uint8Array([1]))).rejects.toThrow(
        'Publish write timed out waiting for relay demand or transport',
      );
      expect(track.closed).toBe(false);
    },
  );
});
