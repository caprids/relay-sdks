import { afterEach, describe, expect, it, vi } from 'vitest';
import { createTransportPair } from '../packages/moq-core/src/tests/helpers/transport-pair.js';
import {
  connectRelay,
  connectRelayPublisher,
  type MoqtObject,
  type RelayConnection,
  type RelayTransportFactory,
} from '../src/index.js';
import { createRelayHub } from './helpers/relay-hub.js';

const namespace = ['testing', 'data'];
const url = 'https://relay.example.com/moq';
const cleanup: (() => void | Promise<void>)[] = [];

afterEach(async () => {
  for (const dispose of cleanup.splice(0).reverse()) await dispose();
});

async function connectHub() {
  const hub = createRelayHub();
  cleanup.push(() => hub.destroy());
  const publisher = await connectRelayPublisher({ url, namespace, createTransport: hub.connectPublisher });
  const subscriber = await connectRelay({ url, createTransport: hub.connectSubscriber });
  cleanup.push(() => subscriber.close());
  cleanup.push(() => publisher.close());
  return { hub, publisher, subscriber };
}

function receive(subscriber: RelayConnection, track: string) {
  const objects: MoqtObject[] = [];
  const onDone = vi.fn();
  const subscription = subscriber.subscribe(
    { namespace, track },
    { onObject: (object) => objects.push(object), onDone },
  );
  return { objects, onDone, subscription };
}

describe('connectRelayPublisher', () => {
  it('publishes raw binary data through a soliciting relay into the SDK subscriber without a catalog or media', async () => {
    const { publisher, subscriber, hub } = await connectHub();
    const events = publisher.publish({ track: 'events' });
    const received = receive(subscriber, 'events');
    const payload = new Uint8Array([0xff, 0, 0x80, 1]);

    await events.write(payload);
    await vi.waitFor(() => expect(received.objects).toHaveLength(1));
    expect(received.objects[0]).toMatchObject({ groupId: 0, objectId: 0, properties: [], payload });
    expect(hub.subscribes).toEqual(['events']);
    expect(hub.publisherConnections()).toBe(1);
  });

  it('publishes sequential objects and an empty payload with explicit object properties', async () => {
    const { publisher, subscriber } = await connectHub();
    const events = publisher.publish({ track: 'events' });
    const received = receive(subscriber, 'events');
    const properties = [
      { type: 2, value: 7 },
      { type: 3, value: new Uint8Array([0xff, 2]) },
    ];

    await events.write(new Uint8Array([1]), { properties });
    await events.write(new Uint8Array([2]));
    await events.write(new Uint8Array());
    await vi.waitFor(() => expect(received.objects).toHaveLength(3));
    expect(received.objects.map((object) => object.groupId).sort()).toEqual([0, 1, 2]);
    expect(received.objects.find((object) => object.groupId === 0)?.properties).toEqual(properties);
    expect(received.objects.find((object) => object.groupId === 2)?.payload).toEqual(new Uint8Array());
  });

  it('copies payload and property bytes while a write waits for relay demand', async () => {
    const { publisher, subscriber } = await connectHub();
    const events = publisher.publish({ track: 'events' });
    const payload = new Uint8Array([7]);
    const property = new Uint8Array([8]);
    const writing = events.write(payload, { properties: [{ type: 3, value: property }] });

    payload[0] = 0;
    property[0] = 0;
    const received = receive(subscriber, 'events');
    await writing;
    await vi.waitFor(() => expect(received.objects).toHaveLength(1));
    expect(received.objects[0]?.payload).toEqual(new Uint8Array([7]));
    expect(received.objects[0]?.properties).toEqual([{ type: 3, value: new Uint8Array([8]) }]);
  });

  it('ends one track with a clean FIN while another published track keeps working', async () => {
    const { publisher, subscriber, hub } = await connectHub();
    const one = publisher.publish({ track: 'one' });
    const two = publisher.publish({ track: 'two' });
    const receivedOne = receive(subscriber, 'one');
    const receivedTwo = receive(subscriber, 'two');

    await one.write(new Uint8Array([1]));
    await two.write(new Uint8Array([2]));
    one.close();
    one.close();
    await vi.waitFor(() => expect(receivedOne.onDone).toHaveBeenCalled());
    await two.write(new Uint8Array([3]));
    await vi.waitFor(() => expect(receivedTwo.objects).toHaveLength(2));
    expect(hub.trackEnds).toContainEqual({ kind: 'subscribe-fin', trackName: 'one' });
    await expect(one.write(new Uint8Array([4]))).rejects.toThrow(/closed/);
  });

  it('bounds a write waiting for demand and can publish a later write after the timeout', async () => {
    const { publisher, subscriber } = await connectHub();
    const events = publisher.publish({ track: 'events', writeTimeoutMs: 50 });

    await expect(events.write(new Uint8Array([1]))).rejects.toThrow(/timed out/);
    const received = receive(subscriber, 'events');
    await events.write(new Uint8Array([2]));
    await vi.waitFor(() => expect(received.objects).toHaveLength(1));
    expect(received.objects[0]?.payload).toEqual(new Uint8Array([2]));
  });

  it('rejects concurrent writes instead of accumulating an unbounded queue', async () => {
    const { publisher } = await connectHub();
    const events = publisher.publish({ track: 'events' });
    const writing = events.write(new Uint8Array([1]));
    const rejected = expect(writing).rejects.toThrow(/closed/);

    await expect(events.write(new Uint8Array([2]))).rejects.toThrow(/already pending/);
    events.close();
    await rejected;
  });

  it('cancels pending writes and retracts the namespace when the publisher closes', async () => {
    const { publisher, subscriber, hub } = await connectHub();
    const live = publisher.publish({ track: 'live' });
    receive(subscriber, 'live');
    await live.write(new Uint8Array([1]));
    const pending = publisher.publish({ track: 'pending' }).write(new Uint8Array([2]));
    const rejected = expect(pending).rejects.toThrow(/closed/);

    await publisher.close();
    await rejected;
    await publisher.close();
    expect(hub.trackEnds).toContainEqual({ kind: 'namespace-done', namespace });
    await expect(publisher.closed).resolves.toEqual({});
    expect(() => publisher.publish({ track: 'late' })).toThrow(/closed/);
  });

  it('does not re-register a track name or silently restart its group numbering', async () => {
    const { publisher } = await connectHub();
    const events = publisher.publish({ track: 'events' });

    expect(() => publisher.publish({ track: 'events' })).toThrow(/already registered/);
    events.close();
    expect(() => publisher.publish({ track: 'events' })).toThrow(/already registered/);
  });

  it('cancels a pending write when the owning AbortSignal is aborted', async () => {
    const hub = createRelayHub();
    const owner = new AbortController();
    cleanup.push(() => hub.destroy());
    const publisher = await connectRelayPublisher({
      url,
      namespace,
      signal: owner.signal,
      createTransport: hub.connectPublisher,
    });
    cleanup.push(() => publisher.close());
    const writing = publisher.publish({ track: 'events' }).write(new Uint8Array([1]));
    const error = new Error('owner aborted');
    const rejected = expect(writing).rejects.toBe(error);

    owner.abort(error);
    await rejected;
    await expect(publisher.closed).resolves.toEqual({ error });
  });

  it('authenticates the publisher independently from subscriber credentials', async () => {
    const hub = createRelayHub();
    cleanup.push(() => hub.destroy());
    const createTransport = vi.fn<RelayTransportFactory>(hub.connectPublisher);
    const publisher = await connectRelayPublisher({ url, namespace, getToken: () => 'publish-token', createTransport });
    cleanup.push(() => publisher.close());

    expect(createTransport).toHaveBeenCalledWith(`${url}?jwt=publish-token`, ['moqt-20']);
  });

  it('preserves explicit track properties and defaults other tracks to no timescale', async () => {
    const pair = createTransportPair();
    const [publisher, subscriber] = await Promise.all([
      connectRelayPublisher({
        url,
        namespace,
        createTransport: () => ({ transport: pair.client, ready: Promise.resolve() }),
      }),
      connectRelay({ url, createTransport: () => ({ transport: pair.server, ready: Promise.resolve() }) }),
    ]);
    cleanup.push(() => subscriber.close());
    cleanup.push(() => publisher.close());
    const trackProperties = [{ type: 2, value: 90_000 }];
    publisher.publish({ track: 'timed', trackProperties });
    publisher.publish({ track: 'raw' });
    const onTimed = vi.fn();
    const onRaw = vi.fn();
    subscriber.subscribe({ namespace, track: 'timed' }, { onOk: onTimed });
    subscriber.subscribe({ namespace, track: 'raw' }, { onOk: onRaw });

    await vi.waitFor(() => expect(onRaw).toHaveBeenCalled());
    expect(onTimed).toHaveBeenCalledWith(expect.objectContaining({ trackProperties }));
    expect(onRaw).toHaveBeenCalledWith(expect.objectContaining({ trackProperties: [] }));
  });

  it('reports a transport write failure and ends that track without silently retrying', async () => {
    const hub = createRelayHub();
    cleanup.push(() => hub.destroy());
    const failure = new Error('data stream failed');
    const publisher = await connectRelayPublisher({
      url,
      namespace,
      createTransport: (...args) => {
        const created = hub.connectPublisher(...args);
        const open = created.transport.createUnidirectionalStream.bind(created.transport);
        let streams = 0;
        created.transport.createUnidirectionalStream = () => (++streams === 2 ? Promise.reject(failure) : open());
        return created;
      },
    });
    const subscriber = await connectRelay({ url, createTransport: hub.connectSubscriber });
    cleanup.push(() => subscriber.close());
    cleanup.push(() => publisher.close());
    const events = publisher.publish({ track: 'events' });
    receive(subscriber, 'events');

    await expect(events.write(new Uint8Array([1]))).rejects.toBe(failure);
    await expect(events.write(new Uint8Array([2]))).rejects.toThrow(/closed/);
    expect(hub.objectCount('events')).toBe(0);
  });

  it('times out a stalled stream open and aborts a late stream without sending payload bytes', async () => {
    const hub = createRelayHub();
    cleanup.push(() => hub.destroy());
    let resolveStream!: (stream: WritableStream<Uint8Array>) => void;
    const publisher = await connectRelayPublisher({
      url,
      namespace,
      createTransport: (...args) => {
        const created = hub.connectPublisher(...args);
        const open = created.transport.createUnidirectionalStream.bind(created.transport);
        let streams = 0;
        created.transport.createUnidirectionalStream = () =>
          ++streams === 1
            ? open()
            : new Promise((resolve) => {
                resolveStream = resolve;
              });
        return created;
      },
    });
    const subscriber = await connectRelay({ url, createTransport: hub.connectSubscriber });
    cleanup.push(() => subscriber.close());
    cleanup.push(() => publisher.close());
    const events = publisher.publish({ track: 'events', writeTimeoutMs: 100 });
    receive(subscriber, 'events');
    await expect(events.write(new Uint8Array([1]))).rejects.toThrow(/timed out/);
    await expect(events.write(new Uint8Array([2]))).rejects.toThrow(/closed/);
    const abort = vi.fn();
    const write = vi.fn();

    resolveStream(new WritableStream({ abort, write }));
    await vi.waitFor(() => expect(abort).toHaveBeenCalledOnce());
    expect(write).not.toHaveBeenCalled();
  });

  it('rejects oversized objects before waiting for demand', async () => {
    const { publisher } = await connectHub();
    const track = publisher.publish({ track: 'events' });

    await expect(track.write(new Uint8Array(16 * 1024 * 1024 + 1))).rejects.toThrow(/16 MiB/);
  });

  it.each<[string, unknown]>([
    ['a string', 'hello'],
    ['an ArrayBuffer', new Uint8Array([1, 2, 3]).buffer],
    ['a plain array', [1, 2, 3]],
    ['null', null],
  ])('rejects %s payload without taking the write slot or ending the track', async (_label, payload) => {
    const { publisher, subscriber } = await connectHub();
    const events = publisher.publish({ track: 'events' });
    const received = receive(subscriber, 'events');
    const rejected = events.write(payload as ArrayBufferView);
    // Issued in the same tick: the rejected write must not have claimed the single write slot.
    const writing = events.write(new Uint8Array([9]));

    await expect(rejected).rejects.toThrow(TypeError);
    await expect(rejected).rejects.toThrow(/Uint8Array/);
    await writing;
    await vi.waitFor(() => expect(received.objects).toHaveLength(1));
    expect(received.objects[0]?.payload).toEqual(new Uint8Array([9]));
  });

  it('publishes the bytes a view spans rather than its whole buffer, for any ArrayBufferView', async () => {
    const { publisher, subscriber } = await connectHub();
    const events = publisher.publish({ track: 'events' });
    const received = receive(subscriber, 'events');
    const buffer = new Uint8Array([9, 9, 9, 1, 2, 3, 9, 9]).buffer;

    await events.write(new Uint8Array(buffer, 3, 3));
    await events.write(new DataView(buffer, 3, 3));
    await vi.waitFor(() => expect(received.objects).toHaveLength(2));
    expect(received.objects.find((object) => object.groupId === 0)?.payload).toEqual(new Uint8Array([1, 2, 3]));
    expect(received.objects.find((object) => object.groupId === 1)?.payload).toEqual(new Uint8Array([1, 2, 3]));
  });
});
