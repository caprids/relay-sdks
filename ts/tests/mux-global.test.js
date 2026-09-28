import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { runDemo } from '../examples/mux-global/demo.js';
import { connectRelayPublisher, connectRelaySubscriber, RelayError } from '../src/index.js';
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
  // The combined run waits for the announce to reach the wire before it subscribes, and says so.
  const events = log.mock.calls.map(([entry]) => entry.event);
  expect(events.indexOf('announced')).toBeGreaterThanOrEqual(0);
  expect(events.indexOf('announced')).toBeLessThan(events.indexOf('subscribed'));
  await expectClosed();
  for (const connection of connections) expect((await connection.closed).error).toBeUndefined();
});

/** Replace each registered track's write() with `write(original, bytes, options)`. */
function wrapWrites(write) {
  connectors.connectRelayPublisher.mockImplementation(async (options) => {
    const publisher = await connectRelayPublisher({ ...options, createTransport: hub.connectPublisher });
    connections.push(publisher);
    const register = publisher.registerTrack.bind(publisher);
    vi.spyOn(publisher, 'registerTrack').mockImplementation((options) => {
      const writer = register(options);
      const original = writer.write.bind(writer);
      vi.spyOn(writer, 'write').mockImplementation((bytes, options) => write(original, bytes, options));
      return writer;
    });
    return publisher;
  });
}

it('ends the wait for demand at the run deadline before anything is sent', async () => {
  const log = vi.fn();
  await expect(runDemo({ ...config, mode: 'publish', timeoutMs: 50 }, log)).rejects.toThrow('Run exceeded');
  // The SDK was still waiting for a subscriber inside write() when the run deadline aborted it.
  expect(log).toHaveBeenCalledWith({ event: 'waiting', detail: 'no relay demand yet; write() waits for a subscriber' });
  expect(log).not.toHaveBeenCalledWith(expect.objectContaining({ event: 'published' }));
  await expectClosed();
});

it('writes the same object again after a demand timeout', async () => {
  const payloads = [];
  wrapWrites(async (write, bytes, options) => {
    payloads.push(bytes);
    // The write deadline expired before any subscriber asked for the track.
    if (payloads.length === 1) throw new RelayError('WRITE_TIMEOUT', 'no demand', 'waiting-for-demand');
    await write(bytes, options);
  });
  const log = vi.fn();
  await expect(runDemo(config, log)).resolves.toEqual({ published: 1, received: 1 });
  expect(payloads).toHaveLength(2);
  expect(payloads[1]).toBe(payloads[0]);
  expect(log).toHaveBeenCalledWith(
    expect.objectContaining({ event: 'waiting', detail: expect.stringContaining('nothing was sent') }),
  );
  expect(log).not.toHaveBeenCalledWith(expect.objectContaining({ event: 'write failed' }));
  await expectClosed();
});

it('does not replay a write that times out after partial progress', async () => {
  let attempts = 0;
  const failure = new RelayError('WRITE_TIMEOUT', 'transport write timed out after partial progress', 'writing');
  wrapWrites(async (write, bytes, options) => {
    attempts += 1;
    await write(bytes, options); // Bytes actually reach the relay before the reported failure.
    throw failure;
  });
  const log = vi.fn();
  await expect(runDemo(config, log)).rejects.toBe(failure);
  expect(attempts).toBe(1);
  expect(log.mock.calls.map(([entry]) => entry).filter((entry) => entry.event === 'write failed')).toEqual([
    {
      event: 'write failed',
      code: 'WRITE_TIMEOUT',
      phase: 'writing',
      detail: 'sending had started; the relay may hold the object, so it is not resent',
    },
  ]);
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

it('fails without retrying when the track closes while waiting for demand', async () => {
  connectors.connectRelayPublisher.mockImplementation(async (options) => {
    const publisher = await connectRelayPublisher({ ...options, createTransport: hub.connectPublisher });
    connections.push(publisher);
    const register = publisher.registerTrack.bind(publisher);
    vi.spyOn(publisher, 'registerTrack').mockImplementation((options) => {
      const writer = register(options);
      queueMicrotask(() => writer.close());
      return writer;
    });
    return publisher;
  });
  const log = vi.fn();
  // Nothing was sent, and the closed track is not a demand timeout: no retry, no uncertain delivery.
  await expect(runDemo({ ...config, mode: 'publish' }, log)).rejects.toMatchObject({
    code: 'CLOSED',
    phase: 'waiting-for-demand',
  });
  expect(log).not.toHaveBeenCalledWith(expect.objectContaining({ event: 'published' }));
  expect(log).not.toHaveBeenCalledWith(expect.objectContaining({ event: 'write failed' }));
  await expectClosed();
});
