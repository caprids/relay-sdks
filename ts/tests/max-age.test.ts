/**
 * Subscriber max age (the MoQ Timestamp extension's MAX_AGE, parameter 0x915C6) through the product entry points: the
 * SDK sends it only to a relay whose SETUP declared SETUP_OPTION.MAX_AGE (0x915C4), and reports whether it did.
 */
import { describe, expect, it } from 'vitest';
import { type KeyValuePair, SETUP_OPTION } from '../packages/moq-core/src/index.js';
import { connectRelaySubscriber } from '../src/index.js';
import { createRelayHub } from './helpers/relay-hub.js';

const url = 'https://relay.example.com/moq';
const namespace = ['testing', 'max-age'];
const declared: KeyValuePair[] = [{ type: SETUP_OPTION.MAX_AGE, value: 1 }];

async function withHub(
  subscriberSetupOptions: KeyValuePair[],
  run: (
    hub: ReturnType<typeof createRelayHub>,
    subscriber: Awaited<ReturnType<typeof connectRelaySubscriber>>,
  ) => Promise<void>,
) {
  const hub = createRelayHub({ subscriberSetupOptions });
  const subscriber = await connectRelaySubscriber({ url, createTransport: hub.connectSubscriber });

  try {
    await run(hub, subscriber);
  } finally {
    subscriber.close();
    hub.destroy();
  }
}

describe('subscriber max age end to end', () => {
  it('reports a relay that declared MAX_AGE and sends maxAge through subscribe and trackSource.subscribe', async () => {
    await withHub(declared, async (hub, subscriber) => {
      expect(subscriber.maxAgeSupported).toBe(true);

      subscriber.subscribe({ namespace, track: 'one', parameters: { maxAge: 500, subscriberPriority: 16 } });
      subscriber.trackSource.subscribe(
        { trackNamespace: namespace, trackName: 'two', parameters: { maxAge: 750 } },
        {},
      );

      await expect.poll(() => hub.downstreamSubscribes.length).toBe(2);
      expect(hub.downstreamSubscribes).toEqual([{ maxAge: 500, subscriberPriority: 16 }, { maxAge: 750 }]);
    });
  });

  it('reports a relay that did not, and leaves maxAge off both entry points', async () => {
    await withHub([], async (hub, subscriber) => {
      expect(subscriber.maxAgeSupported).toBe(false);

      const parameters = { maxAge: 500, subscriberPriority: 16 };

      subscriber.subscribe({ namespace, track: 'one', parameters });
      subscriber.trackSource.subscribe({ trackNamespace: namespace, trackName: 'two', parameters }, {});

      await expect.poll(() => hub.downstreamSubscribes.length).toBe(2);
      expect(hub.downstreamSubscribes).toEqual([{ subscriberPriority: 16 }, { subscriberPriority: 16 }]);
      // The caller's object is untouched.
      expect(parameters).toEqual({ maxAge: 500, subscriberPriority: 16 });
    });
  });

  it('treats a relay whose MAX_AGE option carries value 0 as declaring it', async () => {
    await withHub([{ type: SETUP_OPTION.MAX_AGE, value: 0 }], async (hub, subscriber) => {
      expect(subscriber.maxAgeSupported).toBe(true);

      subscriber.subscribe({ namespace, track: 'one', parameters: { maxAge: 0 } });
      await expect.poll(() => hub.downstreamSubscribes.length).toBe(1);
      expect(hub.downstreamSubscribes).toEqual([{ maxAge: 0 }]);
    });
  });
});
