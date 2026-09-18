import { afterEach, describe, expect, it, vi } from 'vitest';
import { createTransportPair } from '../packages/moq-core/src/tests/helpers/transport-pair.js';
import {
  connectRelay,
  connectRelayPublisher,
  type MoqtObject,
  type PublishProperty,
  type RelayConnection,
  type RelayTransportFactory,
  readJSON,
  readText,
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
  it('rejects oversized namespaces before resolving credentials or opening transport', async () => {
    const getToken = vi.fn();
    const createTransport = vi.fn();
    await expect(
      connectRelayPublisher({ url, namespace: new Array(33).fill('field'), getToken, createTransport }),
    ).rejects.toThrow(/at most 32/);
    expect(getToken).not.toHaveBeenCalled();
    expect(createTransport).not.toHaveBeenCalled();
  });

  it('publishes with 32 namespace fields and diagnoses track-name type separately from emptiness', async () => {
    const hub = createRelayHub();
    cleanup.push(() => hub.destroy());
    const namespace = Array.from({ length: 32 }, (_, index) => `field-${index}`);
    const publisher = await connectRelayPublisher({ url, namespace, createTransport: hub.connectPublisher });
    cleanup.push(() => publisher.close());
    expect(() => publisher.publish({ track: 7 as unknown as string })).toThrow('Track name must be a string');
    expect(() => publisher.publish({ track: '' })).toThrow('Track name must be non-empty');
    const events = publisher.publish({ track: 'events' });
    const subscriber = await connectRelay({ url, createTransport: hub.connectSubscriber });
    cleanup.push(() => subscriber.close());
    const objects: MoqtObject[] = [];
    subscriber.subscribe({ namespace, track: 'events' }, { onObject: (object) => objects.push(object) });
    await events.write(new Uint8Array([1]));
    await vi.waitFor(() => expect(objects).toHaveLength(1));
  });

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

  it('reports a stream open failure and allows a fresh write without silently retrying', async () => {
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
    expect(events.closed).toBe(false);
    expect(hub.objectCount('events')).toBe(0);
    await events.write(new Uint8Array([2]));
    await vi.waitFor(() => expect(hub.objectCount('events')).toBe(1));
  });

  it.each(['header', 'payload', 'fin'] as const)(
    'recovers after a bound %s timeout or cancellation without closing the track',
    async (stage) => {
      const hub = createRelayHub();
      cleanup.push(() => hub.destroy());
      let stalled = false;
      let resetCount = 0;
      const publisher = await connectRelayPublisher({
        url,
        namespace,
        createTransport: (...args) => {
          const created = hub.connectPublisher(...args);
          const open = created.transport.createUnidirectionalStream.bind(created.transport);
          let streams = 0;
          created.transport.createUnidirectionalStream = async () => {
            if (++streams !== 2 && streams !== 4) return open();
            let writes = 0;
            let rejectPending: (error: unknown) => void;
            const stall = () => {
              stalled = true;
              return new Promise<void>((_, reject) => {
                rejectPending = reject;
              });
            };
            return new WritableStream<Uint8Array>({
              start(controller) {
                controller.signal.addEventListener('abort', () => {
                  resetCount++;
                  rejectPending(controller.signal.reason);
                });
              },
              write() {
                writes++;
                if ((stage === 'header' && writes === 1) || (stage === 'payload' && writes === 2)) return stall();
                return undefined;
              },
              close() {
                if (stage === 'fin') return stall();
                return undefined;
              },
            });
          };
          return created;
        },
      });
      cleanup.push(() => publisher.close());
      const track = publisher.publish({ track: 'events', writeTimeoutMs: 100 });
      hub.subscribeUpstream('events');
      for (const mode of ['timeout', 'cancel'] as const) {
        stalled = false;
        const owner = new AbortController();
        const failure = new Error('discard stale object');
        const writing = track.write(new Uint8Array([1]), { signal: owner.signal });
        const rejected = expect(writing).rejects.toThrow(mode === 'timeout' ? /timed out/ : failure);
        await vi.waitFor(() => expect(stalled).toBe(true), { interval: 1 });
        await expect(track.write(new Uint8Array([2]))).rejects.toThrow(/already pending/);
        if (mode === 'cancel') owner.abort(failure);
        await rejected;
        expect(track.closed).toBe(false);
        expect(track.bound).toBe(true);
        await track.write(new Uint8Array([3]));
      }
      expect(resetCount).toBe(2);
      await vi.waitFor(() => expect(hub.objectCount('events')).toBe(2));
      expect(hub.trackEnds).toEqual([]);
    },
  );

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
          ++streams !== 2
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
    expect(events.closed).toBe(false);
    await events.write(new Uint8Array([2]));
    await vi.waitFor(() => expect(hub.objectCount('events')).toBe(1));
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

  it.each([
    { type: 3, value: 'abc' },
    { type: 3, value: new ArrayBuffer(1) },
    { type: -1, value: 0 },
    { type: 0.5, value: 0 },
    { type: Number.MAX_SAFE_INTEGER + 1, value: 0 },
    { type: 2, value: -1 },
    { type: 2, value: 1.5 },
    { type: 2, value: Number.NaN },
    { type: 2, value: Number.MAX_SAFE_INTEGER + 1 },
    { type: 2, value: new Uint8Array([1]) },
    { type: 3, value: 1 },
  ])('rejects invalid property $type/$value before registering or taking the write slot', async (property) => {
    const { publisher, subscriber } = await connectHub();
    const properties = [property] as PublishProperty[];
    expect(() => publisher.publish({ track: 'events', trackProperties: properties })).toThrow(/Property 0/);
    const events = publisher.publish({ track: 'events' });
    const received = receive(subscriber, 'events');
    const rejected = events.write(new Uint8Array([1]), { properties });
    const writing = events.write(new Uint8Array([9]));
    await expect(rejected).rejects.toThrow(TypeError);
    await expect(rejected).rejects.toThrow(/Property 0/);
    await writing;
    await vi.waitFor(() => expect(received.objects).toHaveLength(1));
    expect(received.objects[0]?.groupId).toBe(0);
  });

  it('copies the bytes spanned by property views before waiting for demand', async () => {
    const { publisher, subscriber } = await connectHub();
    const events = publisher.publish({ track: 'events' });
    const bytes = new Uint8Array([9, 1, 2, 9]);
    const writing = events.write(new Uint8Array([3]), {
      properties: [{ type: 3, value: new DataView(bytes.buffer, 1, 2) }],
    });
    bytes.fill(0);
    const received = receive(subscriber, 'events');
    await writing;
    await vi.waitFor(() => expect(received.objects).toHaveLength(1));
    expect(received.objects[0]?.properties).toEqual([{ type: 3, value: new Uint8Array([1, 2]) }]);
  });

  it('publishes multiple objects on one group stream and preserves increasing group IDs', async () => {
    const { publisher, subscriber } = await connectHub();
    const track = publisher.publish({ track: 'events' });
    const received = receive(subscriber, 'events');
    const group = await track.openGroup();
    expect(group.groupId).toBe(0);
    expect(group.objectCount).toBe(0);
    await group.write(new Uint8Array([1]));
    await group.write(new Uint8Array([2]), { properties: [{ type: 2, value: 7 }] });
    await group.write(new Uint8Array());
    expect(group.objectCount).toBe(3);
    await group.close();
    await group.close();
    expect(group.closed).toBe(true);
    await expect(group.write(new Uint8Array([9]))).rejects.toThrow(/closed/);
    await track.write(new Uint8Array([4]));
    await vi.waitFor(() => expect(received.objects).toHaveLength(4));
    expect(received.objects.map(({ groupId, objectId }) => [groupId, objectId])).toEqual([
      [0, 0],
      [0, 1],
      [0, 2],
      [1, 0],
    ]);
  });

  it('reserves one track slot for a group and keeps it open after local argument errors', async () => {
    const { publisher, hub } = await connectHub();
    const track = publisher.publish({ track: 'events' });
    hub.subscribeUpstream('events');
    const group = await track.openGroup();
    await expect(track.write(new Uint8Array([1]))).rejects.toThrow(/already pending/);
    await expect(track.openGroup()).rejects.toThrow(/already pending/);
    await expect(group.write('bad' as unknown as ArrayBufferView)).rejects.toThrow(TypeError);
    await expect(group.write(new Uint8Array([1]), { properties: [{ type: 3, value: 2 }] })).rejects.toThrow(TypeError);
    await expect(group.write(new Uint8Array([1]), { properties: new Array<PublishProperty>(1) })).rejects.toThrow(
      TypeError,
    );
    await group.write(new Uint8Array([2]));
    expect(group.objectCount).toBe(1);
    await group.close();
    expect(track.closed).toBe(false);
  });

  it('bounds group demand and releases its slot after timeout or an already-aborted signal', async () => {
    const { publisher, hub } = await connectHub();
    const track = publisher.publish({ track: 'events', writeTimeoutMs: 50 });
    await expect(track.openGroup()).rejects.toThrow(/timed out/);
    const owner = new AbortController();
    const reason = new Error('cancelled before opening');
    owner.abort(reason);
    await expect(track.openGroup({ signal: owner.signal })).rejects.toBe(reason);
    expect(track.closed).toBe(false);
    hub.subscribeUpstream('events');
    const group = await track.openGroup();
    await group.write(new Uint8Array([1]));
    await group.close();
  });

  it.each(['write', 'close'] as const)(
    'bounds a stalled group %s, resets it, and permits the next group',
    async (mode) => {
      const hub = createRelayHub();
      cleanup.push(() => hub.destroy());
      const reset = vi.fn();
      let rejectPending: ((reason: unknown) => void) | undefined;
      let stalled = false;
      const publisher = await connectRelayPublisher({
        url,
        namespace,
        createTransport: (...args) => {
          const created = hub.connectPublisher(...args);
          const open = created.transport.createUnidirectionalStream.bind(created.transport);
          let streams = 0;
          created.transport.createUnidirectionalStream = async () => {
            if (++streams !== 2) return open();
            let writes = 0;
            return new WritableStream<Uint8Array>({
              start(controller) {
                controller.signal.addEventListener('abort', () => {
                  reset(controller.signal.reason);
                  rejectPending?.(controller.signal.reason);
                });
              },
              write() {
                if (++writes === 2 && mode === 'write') {
                  stalled = true;
                  return new Promise<void>((_, reject) => {
                    rejectPending = reject;
                  });
                }
                return undefined;
              },
              close() {
                if (mode === 'close') {
                  stalled = true;
                  return new Promise<void>((_, reject) => {
                    rejectPending = reject;
                  });
                }
                return undefined;
              },
            });
          };
          return created;
        },
      });
      cleanup.push(() => publisher.close());
      const track = publisher.publish({ track: 'events', writeTimeoutMs: 100 });
      hub.subscribeUpstream('events');
      const group = await track.openGroup();
      if (mode === 'close') await group.write(new Uint8Array([1]));
      const pending = mode === 'write' ? group.write(new Uint8Array([1])) : group.close();
      const rejected = expect(pending).rejects.toThrow(/timed out/);
      await vi.waitFor(() => expect(stalled).toBe(true));
      await expect(group.write(new Uint8Array([2]))).rejects.toThrow(/already pending/);
      await expect(group.close()).rejects.toThrow(/already pending/);
      await rejected;
      expect(reset).toHaveBeenCalledOnce();
      expect(group.closed).toBe(true);
      expect(track.closed).toBe(false);
      const next = await track.openGroup();
      expect(next.groupId).toBe(1);
      await next.write(new Uint8Array([3]));
      await next.close();
      await vi.waitFor(() => expect(hub.objectCount('events')).toBe(1));
    },
  );

  it('aborts idle groups explicitly or through their signal and permits another group', async () => {
    const { publisher, hub } = await connectHub();
    const track = publisher.publish({ track: 'events' });
    hub.subscribeUpstream('events');
    const owner = new AbortController();
    const first = await track.openGroup({ signal: owner.signal });
    await first.write(new Uint8Array([1]));
    const reason = new Error('drop incomplete GOP');
    owner.abort(reason);
    await expect(first.write(new Uint8Array([2]))).rejects.toBe(reason);
    await expect(first.close()).rejects.toBe(reason);
    expect(first.closed).toBe(true);
    const second = await track.openGroup();
    second.abort(reason);
    expect(second.closed).toBe(true);
    const third = await track.openGroup();
    await third.write(new Uint8Array([3]));
    await third.close();
  });

  it('resets a bound write when demand disappears and publishes again after resubscription', async () => {
    const pair = createTransportPair();
    const open = pair.client.createUnidirectionalStream.bind(pair.client);
    let streams = 0;
    let stalled = false;
    const reset = vi.fn();
    pair.client.createUnidirectionalStream = async () => {
      if (++streams !== 2) return open();
      let rejectPending: (error: unknown) => void;
      return new WritableStream<Uint8Array>({
        start(controller) {
          controller.signal.addEventListener('abort', () => {
            reset();
            rejectPending(controller.signal.reason);
          });
        },
        write() {
          stalled = true;
          return new Promise<void>((_, reject) => {
            rejectPending = reject;
          });
        },
      });
    };
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
    const track = publisher.publish({ track: 'events' });
    const bindings = track.bindings();
    expect(await bindings.next()).toEqual({ done: false, value: false });
    const first = subscriber.subscribe({ namespace, track: 'events' }, {});
    expect(await bindings.next()).toEqual({ done: false, value: true });
    const writing = track.write(new Uint8Array([1]));
    const rejected = expect(writing).rejects.toThrow(/subscription changed/);
    await vi.waitFor(() => expect(stalled).toBe(true));
    first.cancel();
    expect(await bindings.next()).toEqual({ done: false, value: false });
    await rejected;
    expect(reset).toHaveBeenCalledOnce();
    expect(track.closed).toBe(false);
    const received = receive(subscriber, 'events');
    expect(await bindings.next()).toEqual({ done: false, value: true });
    await track.write(new Uint8Array([2]));
    await vi.waitFor(() => expect(received.objects).toHaveLength(1));
    expect(received.objects[0]).toMatchObject({ groupId: 1, payload: new Uint8Array([2]) });
    await bindings.return?.();
  });

  it('observes demand, cancellation, resubscription, and track end without hanging iterator return', async () => {
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
    const track = publisher.publish({ track: 'events' });
    const bindings = track.bindings();
    expect(await bindings.next()).toEqual({ done: false, value: false });
    expect(track.bound).toBe(false);
    const subscription = subscriber.subscribe({ namespace, track: 'events' }, {});
    expect(await bindings.next()).toEqual({ done: false, value: true });
    expect(track.bound).toBe(true);
    const group = await track.openGroup();
    await group.write(new Uint8Array([1]));
    subscription.cancel();
    expect(await bindings.next()).toEqual({ done: false, value: false });
    expect(group.closed).toBe(true);
    expect(track.closed).toBe(false);
    await expect(group.close()).rejects.toThrow(/subscription changed/);
    subscriber.subscribe({ namespace, track: 'events' }, {});
    expect(await bindings.next()).toEqual({ done: false, value: true });
    const next = await track.openGroup();
    await next.write(new Uint8Array([2]));
    await next.close();
    const other = track.bindings();
    await other.next();
    const pending = other.next();
    await other.return?.();
    expect(await pending).toEqual({ done: true, value: undefined });
    track.close();
    expect(track.closed).toBe(true);
    expect(track.bound).toBe(false);
    expect(await bindings.next()).toEqual({ done: true, value: undefined });
    expect(await track.bindings().next()).toEqual({ done: true, value: undefined });
  });

  it('ends demand iterators and cancels an open group when the publisher closes', async () => {
    const { publisher, hub } = await connectHub();
    const track = publisher.publish({ track: 'events' });
    hub.subscribeUpstream('events');
    const group = await track.openGroup();
    const bindings = track.bindings();
    await bindings.next();
    await group.write(new Uint8Array([1]));
    await publisher.close();
    await expect(group.close()).rejects.toThrow(/closed/);
    expect(track.closed).toBe(true);
    expect(track.bound).toBe(false);
    expect(await bindings.next()).toEqual({ done: true, value: undefined });
  });
});

describe('text and JSON helpers', () => {
  it('round-trips Unicode, empty text, and JSON on tracks and groups with metadata', async () => {
    const { publisher, subscriber } = await connectHub();
    const track = publisher.publish({ track: 'helpers' });
    const { objects } = receive(subscriber, 'helpers');
    const text = '\uFEFFHello, 世界 🌍\n\0';
    const value = { text, nested: [null, true, 42, { empty: '' }] };
    const properties = [{ type: 2, value: 7 }];
    await track.writeText(text, { properties });
    await track.writeJSON(value, { properties });
    const group = await track.openGroup();
    await group.writeText('');
    await group.writeJSON(value, { properties });
    await group.writeJSON(null);
    await group.writeText('', { status: 'end-of-group' });
    expect(group.objectCount).toBe(4);
    await group.close();
    // The relay fixture consumes the end-of-group marker instead of forwarding it.
    await vi.waitFor(() => expect(objects).toHaveLength(5));
    objects.sort((a, b) => a.groupId - b.groupId || a.objectId - b.objectId);
    expect(readText(objects[0]!)).toBe(text);
    expect(readJSON(objects[1]!)).toEqual(value);
    expect(readText(objects[2]!)).toBe('');
    expect(readJSON(objects[3]!)).toEqual(value);
    expect(readJSON(objects[4]!)).toBeNull();
    expect(objects.map(({ groupId, objectId }) => [groupId, objectId])).toEqual([
      [0, 0],
      [1, 0],
      [2, 0],
      [2, 1],
      [2, 2],
    ]);
    for (const index of [0, 1, 3]) expect(objects[index]?.properties).toEqual(properties);
  });

  it('leaves tracks and open groups usable after encoding or size errors', async () => {
    const { publisher, subscriber } = await connectHub();
    const track = publisher.publish({ track: 'helpers' });
    const { objects } = receive(subscriber, 'helpers');
    const circular: { self?: unknown } = {};
    circular.self = circular;
    for (const grouped of [false, true]) {
      const writer = grouped ? await track.openGroup() : track;
      for (const value of [undefined, () => {}, Symbol('bad'), 1n, circular]) {
        await expect(writer.writeJSON(value)).rejects.toBeInstanceOf(TypeError);
      }
      await expect(writer.writeText(42 as unknown as string)).rejects.toBeInstanceOf(TypeError);
      await expect(writer.writeText('é'.repeat(8 * 1024 * 1024 + 1))).rejects.toThrow(/16 MiB/);
      await writer.writeJSON(false);
      expect(writer.closed).toBe(false);
      if (grouped) await writer.close();
    }
    await track.writeText('after');
    await vi.waitFor(() => expect(objects).toHaveLength(3));
    expect(objects.map(readText)).toEqual(['false', 'false', 'after']);
  });

  it('retains demand deadlines, concurrency rejection, and write cancellation', async () => {
    const { publisher, subscriber } = await connectHub();
    const track = publisher.publish({ track: 'helpers', writeTimeoutMs: 50 });
    await expect(track.writeText('no demand')).rejects.toThrow(/timed out/);
    const controller = new AbortController();
    const writing = track.writeJSON({ waiting: true }, { signal: controller.signal });
    const rejected = expect(writing).rejects.toThrow('stop');
    await expect(track.writeText('concurrent')).rejects.toThrow(/already pending/);
    controller.abort(new Error('stop'));
    await rejected;
    const { objects } = receive(subscriber, 'helpers');
    await track.writeJSON(0);
    await vi.waitFor(() => expect(objects).toHaveLength(1));
    expect(readJSON(objects[0]!)).toBe(0);
  });
});
