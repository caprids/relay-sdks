import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { runDemo } from '../examples/mux-global/demo.js';
import { connectRelayPublisher, connectRelaySubscriber, SubscriptionRequestError } from '../src/index.js';
import { createRelayHub } from './helpers/relay-hub.js';

const connectors = vi.hoisted(() => ({ connectRelaySubscriber: vi.fn(), connectRelayPublisher: vi.fn() }));
// The demo imports the SDK by its package name; the connectors are replaced, everything else is real.
vi.mock('@mux/relay', async () => ({ ...(await vi.importActual('../src/index.js')), ...connectors }));

const config = {
  url: 'https://relay.example.com',
  token: 'test-token',
  namespace: ['quickstart', 'test'],
  track: 'ticker',
  mode: 'both',
  count: 1,
  intervalMs: 1,
  timeoutMs: 2000,
};
let hub;
let connections;
beforeEach(() => {
  hub = createRelayHub();
  connections = [];
  connectors.connectRelayPublisher.mockImplementation(async (options) => {
    const publisher = await connectRelayPublisher({ ...options, createTransport: hub.connectPublisher });
    connections.push(publisher);
    return publisher;
  });
  connectors.connectRelaySubscriber.mockImplementation(async (options) => {
    const subscriber = await connectRelaySubscriber({ ...options, createTransport: hub.connectSubscriber });
    connections.push(subscriber);
    return subscriber;
  });
});
afterEach(() => {
  hub.destroy();
  vi.restoreAllMocks();
});

async function expectClosed() {
  for (const connection of connections) {
    await expect(connection.closed).resolves.toBeDefined();
  }
}

it('runs the documented one-object round trip and closes both connections', async () => {
  const log = vi.fn();
  await expect(runDemo(config, log)).resolves.toEqual({ published: 1, received: 1 });
  expect(log).toHaveBeenCalledWith(expect.objectContaining({ event: 'received', seq: 1 }));
  await expectClosed();
  for (const connection of connections) expect((await connection.closed).error).toBeUndefined();
});

it('bounds waiting for demand without attempting a write', async () => {
  const write = vi.fn();
  connectors.connectRelayPublisher.mockImplementation(async (options) => {
    const publisher = await connectRelayPublisher({ ...options, createTransport: hub.connectPublisher });
    connections.push(publisher);
    vi.spyOn(publisher, 'registerTrack').mockReturnValue({ hasDemand: false, closed: false, write });
    return publisher;
  });
  await expect(runDemo({ ...config, mode: 'publish', timeoutMs: 50 }, vi.fn())).rejects.toThrow('Run exceeded');
  expect(write).not.toHaveBeenCalled();
  await expectClosed();
});

it('does not replay a write that times out after partial progress', async () => {
  let attempts = 0;
  const failure = new Error('transport write timed out after partial progress');
  connectors.connectRelayPublisher.mockImplementation(async (options) => {
    const publisher = await connectRelayPublisher({ ...options, createTransport: hub.connectPublisher });
    connections.push(publisher);
    const publish = publisher.registerTrack.bind(publisher);
    vi.spyOn(publisher, 'registerTrack').mockImplementation((options) => {
      const writer = publish(options);
      const write = writer.write.bind(writer);
      vi.spyOn(writer, 'write').mockImplementation(async (bytes) => {
        attempts += 1;
        await write(bytes); // Bytes actually reach the relay before the reported failure.
        throw failure;
      });
      return writer;
    });
    return publisher;
  });
  await expect(runDemo(config, vi.fn())).rejects.toBe(failure);
  expect(attempts).toBe(1);
  await expectClosed();
});

it('closes the publisher when subscriber connection setup fails', async () => {
  connectors.connectRelaySubscriber.mockRejectedValue(new Error('handshake failed'));
  await expect(runDemo(config, vi.fn())).rejects.toThrow('subscriber failed to connect');
  expect(connections).toHaveLength(1);
  await expectClosed();
});

it('allows subscriber-first startup with a matching publisher', async () => {
  const subscriber = runDemo({ ...config, mode: 'subscribe', timeoutMs: 4000 }, vi.fn());
  await new Promise((resolve) => setTimeout(resolve, 100));
  const publisher = runDemo({ ...config, mode: 'publish' }, vi.fn());
  await expect(Promise.all([subscriber, publisher])).resolves.toEqual([
    { published: 0, received: 1 },
    { published: 1, received: 0 },
  ]);
  await expectClosed();
});

it('retries a fresh-namespace 404 with backoff before receiving an object', async () => {
  let subscribe;
  connectors.connectRelaySubscriber.mockImplementation(async (options) => {
    const subscriber = await connectRelaySubscriber({ ...options, createTransport: hub.connectSubscriber });
    connections.push(subscriber);
    const original = subscriber.subscribe.bind(subscriber);
    subscribe = vi
      .spyOn(subscriber, 'subscribe')
      .mockImplementationOnce(() => {
        const rejection = new SubscriptionRequestError({
          errorCode: 404,
          retryInterval: 0,
          reason: 'Broadcast not found',
        });
        return { cancel() {}, subscribed: Promise.reject(rejection), ended: new Promise(() => {}) };
      })
      .mockImplementation(original);
    return subscriber;
  });
  const log = vi.fn();
  await expect(runDemo(config, log)).resolves.toEqual({ published: 1, received: 1 });
  expect(subscribe).toHaveBeenCalledTimes(2);
  expect(log).toHaveBeenCalledWith({ event: 'waiting', detail: 'namespace not announced yet; retrying the subscribe' });
  await expectClosed();
});

it('fails without writing when the track closes while waiting for demand', async () => {
  const write = vi.fn();
  connectors.connectRelayPublisher.mockImplementation(async (options) => {
    const publisher = await connectRelayPublisher({ ...options, createTransport: hub.connectPublisher });
    connections.push(publisher);
    const publish = publisher.registerTrack.bind(publisher);
    vi.spyOn(publisher, 'registerTrack').mockImplementation((options) => {
      const writer = publish(options);
      vi.spyOn(writer, 'write').mockImplementation(write);
      queueMicrotask(() => writer.close());
      return writer;
    });
    return publisher;
  });
  await expect(runDemo({ ...config, mode: 'publish' }, vi.fn())).rejects.toThrow(
    'Published track closed while waiting for demand',
  );
  expect(write).not.toHaveBeenCalled();
  await expectClosed();
});
