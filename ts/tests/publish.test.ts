import { afterEach, describe, expect, it, vi } from 'vitest';
import { StreamReader } from '../packages/moq-core/src/protocol/bytes.js';
import { decodeControlMessage, encodePublish } from '../packages/moq-core/src/protocol/control-messages.js';
import { createTransportPair, withServerRequestIds } from '../packages/moq-core/src/tests/helpers/transport-pair.js';
import {
  connectRelayPublisher,
  connectRelaySubscriber,
  type MoqtObject,
  type PublishProperty,
  type RelaySubscriber,
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
  const subscriber = await connectRelaySubscriber({ url, createTransport: hub.connectSubscriber });
  cleanup.push(() => subscriber.close());
  cleanup.push(() => publisher.close());
  return { hub, publisher, subscriber };
}

function receive(subscriber: RelaySubscriber, track: string) {
  const objects: MoqtObject[] = [];
  const onPublisherDone = vi.fn();
  const subscription = subscriber.subscribe(
    { namespace, track },
    { onObject: (object) => objects.push(object), onPublisherDone },
  );
  return { objects, onPublisherDone, subscription };
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

  it('rejects an empty jwt on the endpoint before resolving credentials or opening transport', async () => {
    const getToken = vi.fn(() => 'token');
    const createTransport = vi.fn();
    const connecting = connectRelayPublisher({ url: `${url}?jwt=`, namespace: ['room'], getToken, createTransport });

    await expect(connecting).rejects.toThrow(RangeError);
    await expect(connecting).rejects.toThrow(/empty jwt query parameter/);
    expect(getToken).not.toHaveBeenCalled();
    expect(createTransport).not.toHaveBeenCalled();
  });

  it('validates the shared connection deadline before resolving credentials or opening transport', async () => {
    const getToken = vi.fn();
    const createTransport = vi.fn();
    const connecting = connectRelayPublisher({ url, namespace, connectTimeoutMs: 0, getToken, createTransport });

    await expect(connecting).rejects.toThrow(RangeError);
    await expect(connecting).rejects.toThrow(
      'connectTimeoutMs must be between 0 (exclusive) and 2147483647 milliseconds',
    );
    expect(getToken).not.toHaveBeenCalled();
    expect(createTransport).not.toHaveBeenCalled();
  });

  it('rejects a write deadline that is not a number as a TypeError and one out of range as a RangeError', async () => {
    const { publisher } = await connectHub();
    expect(() => publisher.registerTrack({ track: 'a', writeTimeoutMs: '100' as unknown as number })).toThrow(
      new TypeError('writeTimeoutMs must be a number of milliseconds'),
    );
    expect(() => publisher.registerTrack({ track: 'a', writeTimeoutMs: 0 })).toThrow(
      new RangeError('writeTimeoutMs must be between 0 (exclusive) and 2147483647 milliseconds'),
    );
    expect(() => publisher.registerTrack({ track: 'a', writeTimeoutMs: 2_147_483_648 })).toThrow(RangeError);
  });

  it('has no request deadline: a stray requestTimeoutMs is a type error and is ignored at runtime', async () => {
    const hub = createRelayHub();
    cleanup.push(() => hub.destroy());
    // An out-of-range value would be a RangeError on a subscriber; a publisher never reads it.
    const publisher = await connectRelayPublisher({
      url,
      namespace,
      createTransport: hub.connectPublisher,
      // @ts-expect-error requestTimeoutMs is subscriber-only (issue #72).
      requestTimeoutMs: -1,
    });
    cleanup.push(() => publisher.close());
    const events = publisher.registerTrack({ track: 'events' });
    const subscriber = await connectRelaySubscriber({ url, createTransport: hub.connectSubscriber });
    cleanup.push(() => subscriber.close());
    const { objects } = receive(subscriber, 'events');

    await events.write(new Uint8Array([1]));
    await vi.waitFor(() => expect(objects).toHaveLength(1));
  });

  it('publishes with 32 namespace fields and diagnoses track-name type separately from emptiness', async () => {
    const hub = createRelayHub();
    cleanup.push(() => hub.destroy());
    const namespace = Array.from({ length: 32 }, (_, index) => `field-${index}`);
    const publisher = await connectRelayPublisher({ url, namespace, createTransport: hub.connectPublisher });
    cleanup.push(() => publisher.close());
    expect(() => publisher.registerTrack({ track: 7 as unknown as string })).toThrow(TypeError);
    expect(() => publisher.registerTrack({ track: '' })).toThrow('A track name must be a non-empty string');
    const events = publisher.registerTrack({ track: 'events' });
    const subscriber = await connectRelaySubscriber({ url, createTransport: hub.connectSubscriber });
    cleanup.push(() => subscriber.close());
    const objects: MoqtObject[] = [];
    subscriber.subscribe({ namespace, track: 'events' }, { onObject: (object) => objects.push(object) });
    await events.write(new Uint8Array([1]));
    await vi.waitFor(() => expect(objects).toHaveLength(1));
  });

  it('publishes raw binary data through a soliciting relay into the SDK subscriber without a catalog or media', async () => {
    const { publisher, subscriber, hub } = await connectHub();
    const events = publisher.registerTrack({ track: 'events' });
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
    const events = publisher.registerTrack({ track: 'events' });
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
    const events = publisher.registerTrack({ track: 'events' });
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
    const one = publisher.registerTrack({ track: 'one' });
    const two = publisher.registerTrack({ track: 'two' });
    const receivedOne = receive(subscriber, 'one');
    const receivedTwo = receive(subscriber, 'two');

    await one.write(new Uint8Array([1]));
    await two.write(new Uint8Array([2]));
    one.close();
    one.close();
    await vi.waitFor(() => expect(receivedOne.onPublisherDone).toHaveBeenCalled());
    await two.write(new Uint8Array([3]));
    await vi.waitFor(() => expect(receivedTwo.objects).toHaveLength(2));
    expect(hub.trackEnds).toContainEqual({ kind: 'subscribe-fin', trackName: 'one' });
    await expect(one.write(new Uint8Array([4]))).rejects.toMatchObject({ code: 'CLOSED', phase: 'waiting-for-demand' });
  });

  it('bounds a write waiting for demand and can publish a later write after the timeout', async () => {
    const { publisher, subscriber } = await connectHub();
    const events = publisher.registerTrack({ track: 'events', writeTimeoutMs: 50 });

    await expect(events.write(new Uint8Array([1]))).rejects.toMatchObject({
      code: 'WRITE_TIMEOUT',
      phase: 'waiting-for-demand',
    });
    const received = receive(subscriber, 'events');
    await events.write(new Uint8Array([2]));
    await vi.waitFor(() => expect(received.objects).toHaveLength(1));
    expect(received.objects[0]?.payload).toEqual(new Uint8Array([2]));
  });

  it('rejects concurrent writes instead of accumulating an unbounded queue', async () => {
    const { publisher } = await connectHub();
    const events = publisher.registerTrack({ track: 'events' });
    const writing = events.write(new Uint8Array([1]));
    const rejected = expect(writing).rejects.toThrow(/closed/);

    await expect(events.write(new Uint8Array([2]))).rejects.toMatchObject({ code: 'WRITE_PENDING', phase: 'writing' });
    events.close();
    await rejected;
  });

  it('cancels pending writes and retracts the namespace when the publisher closes', async () => {
    const { publisher, subscriber, hub } = await connectHub();
    const live = publisher.registerTrack({ track: 'live' });
    receive(subscriber, 'live');
    await live.write(new Uint8Array([1]));
    const pending = publisher.registerTrack({ track: 'pending' }).write(new Uint8Array([2]));
    const rejected = expect(pending).rejects.toThrow(/closed/);

    await publisher.close();
    await rejected;
    await publisher.close();
    expect(hub.trackEnds).toContainEqual({ kind: 'namespace-done', namespace });
    await expect(publisher.closed).resolves.toEqual({});
    expect(() => publisher.registerTrack({ track: 'late' })).toThrow(/closed/);
  });

  it('does not re-register a track name or silently restart its group numbering', async () => {
    const { publisher } = await connectHub();
    const events = publisher.registerTrack({ track: 'events' });

    expect(() => publisher.registerTrack({ track: 'events' })).toThrow(RangeError);
    expect(() => publisher.registerTrack({ track: 'events' })).toThrow(/already registered/);
    events.close();
    expect(() => publisher.registerTrack({ track: 'events' })).toThrow(RangeError);
    expect(() => publisher.registerTrack({ track: 'events' })).toThrow(/already registered/);
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
    const writing = publisher.registerTrack({ track: 'events' }).write(new Uint8Array([1]));
    const error = new Error('owner aborted');
    const rejected = expect(writing).rejects.toBe(error);

    owner.abort(error);
    await rejected;
    await expect(publisher.closed).resolves.toEqual({ error });
  });

  it('settles announced once the first registered track puts the announce on the wire', async () => {
    const { publisher } = await connectHub();
    let settled = false;
    void publisher.announced.then(() => {
      settled = true;
    });
    await new Promise((resolve) => setTimeout(resolve, 10));
    // The hub solicited at connect time, but an announce invites subscribes, so it waits for a track.
    expect(settled).toBe(false);

    publisher.registerTrack({ track: 'events' });

    await expect(publisher.announced).resolves.toBeUndefined();
    publisher.registerTrack({ track: 'status' });
    await expect(publisher.announced).resolves.toBeUndefined();
  });

  it('terminates announced with CLOSED when a clean close comes first', async () => {
    const { publisher } = await connectHub();

    await publisher.close();

    await expect(publisher.announced).rejects.toMatchObject({
      code: 'CLOSED',
      phase: 'connection',
      message: /before its namespace was announced/,
    });
  });

  it('pre-handles announced, so a publisher nobody awaits can close first without an unhandled rejection', async () => {
    const { publisher } = await connectHub();

    await publisher.close();
    // Nothing awaits `announced`; vitest fails the run if its rejection surfaces as unhandled.
    await new Promise((resolve) => setTimeout(resolve, 10));
  });

  it('rejects announced with the closure error when the connection fails first', async () => {
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
    const error = new Error('operator stopped the stream');
    const rejected = expect(publisher.announced).rejects.toBe(error);

    owner.abort(error);
    await rejected;
    await expect(publisher.closed).resolves.toEqual({ error });
  });

  it('rejects announced with a null abort reason instead of inventing a CLOSED error', async () => {
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
    const rejected = expect(publisher.announced).rejects.toBeNull();

    owner.abort(null);
    await rejected;
    await expect(publisher.closed).resolves.toEqual({ error: null });
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
      connectRelaySubscriber({
        url,
        createTransport: () => ({ transport: withServerRequestIds(pair.server), ready: Promise.resolve() }),
      }),
    ]);
    cleanup.push(() => subscriber.close());
    cleanup.push(() => publisher.close());
    const trackProperties = [{ type: 2, value: 90_000 }];
    publisher.registerTrack({ track: 'timed', trackProperties });
    publisher.registerTrack({ track: 'raw' });
    const onTimed = vi.fn();
    const onRaw = vi.fn();
    subscriber.subscribe({ namespace, track: 'timed' }, { onSubscribed: onTimed });
    subscriber.subscribe({ namespace, track: 'raw' }, { onSubscribed: onRaw });

    await vi.waitFor(() => expect(onRaw).toHaveBeenCalled());
    expect(onTimed).toHaveBeenCalledWith(expect.objectContaining({ trackProperties }));
    expect(onRaw).toHaveBeenCalledWith(expect.objectContaining({ trackProperties: [] }));
  });

  it('closes ahead of an SDK subscriber without waiting out the end-ack bound', async () => {
    const pair = createTransportPair();
    const [publisher, subscriber] = await Promise.all([
      connectRelayPublisher({
        url,
        namespace,
        createTransport: () => ({ transport: pair.client, ready: Promise.resolve() }),
      }),
      connectRelaySubscriber({
        url,
        createTransport: () => ({ transport: withServerRequestIds(pair.server), ready: Promise.resolve() }),
      }),
    ]);
    cleanup.push(() => subscriber.close());
    publisher.registerTrack({ track: 'events' });
    const onSubscribed = vi.fn();
    const onPublisherDone = vi.fn();
    subscriber.subscribe({ namespace, track: 'events' }, { onSubscribed, onPublisherDone });
    await vi.waitFor(() => expect(onSubscribed).toHaveBeenCalled());

    // The subscriber FINs back once the publisher's FIN follows PUBLISH_DONE, which is what the publisher's close
    // waits for (bounded by CLOSE_END_ACK_TIMEOUT_MS, 2 s).
    const started = performance.now();
    await publisher.close();

    expect(performance.now() - started).toBeLessThan(500);
    expect(onPublisherDone).toHaveBeenCalledTimes(1);
  });

  it('rejects oversized objects before waiting for demand', async () => {
    const { publisher } = await connectHub();
    const track = publisher.registerTrack({ track: 'events' });

    await expect(track.write(new Uint8Array(16 * 1024 * 1024 + 1))).rejects.toThrow(/16 MiB/);
  });

  it.each<[string, unknown]>([
    ['a string', 'hello'],
    ['an ArrayBuffer', new Uint8Array([1, 2, 3]).buffer],
    ['a plain array', [1, 2, 3]],
    ['null', null],
  ])('rejects %s payload without taking the write slot or ending the track', async (_label, payload) => {
    const { publisher, subscriber } = await connectHub();
    const events = publisher.registerTrack({ track: 'events' });
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
    const events = publisher.registerTrack({ track: 'events' });
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
    expect(() => publisher.registerTrack({ track: 'events', trackProperties: properties })).toThrow(/Property 0/);
    const events = publisher.registerTrack({ track: 'events' });
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
    const events = publisher.registerTrack({ track: 'events' });
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
    const track = publisher.registerTrack({ track: 'events' });
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

  it('ignores abort after a clean group close and keeps the track usable', async () => {
    const { publisher, subscriber } = await connectHub();
    const track = publisher.registerTrack({ track: 'events' });
    const { objects } = receive(subscriber, 'events');
    const group = await track.openGroup();
    await group.write(new Uint8Array([1]));
    await group.close();
    expect(group.closed).toBe(true);

    // draft-ietf-moq-transport-20 section 11.4.3: a reset after all data and the FIN is ignorable.
    // write-slot.test.ts proves no reset is sent; this pins the end-to-end shape through a relay.
    group.abort(new Error('late abort'));
    expect(group.closed).toBe(true);
    await group.close();

    const next = await track.openGroup();
    expect(next.groupId).toBe(1);
    await next.write(new Uint8Array([2]));
    await next.close();
    await vi.waitFor(() => expect(objects).toHaveLength(2));
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
      connectRelaySubscriber({
        url,
        createTransport: () => ({ transport: withServerRequestIds(pair.server), ready: Promise.resolve() }),
      }),
    ]);
    cleanup.push(() => subscriber.close());
    cleanup.push(() => publisher.close());
    const track = publisher.registerTrack({ track: 'events' });
    const demand = track.demandChanges();
    expect(await demand.next()).toEqual({ done: false, value: false });
    const first = subscriber.subscribe({ namespace, track: 'events' }, {});
    expect(await demand.next()).toEqual({ done: false, value: true });
    const writing = track.write(new Uint8Array([1]));
    const rejected = expect(writing).rejects.toMatchObject({ code: 'SUBSCRIPTION_CHANGED', phase: 'writing' });
    await vi.waitFor(() => expect(stalled).toBe(true));
    first.cancel();
    expect(await demand.next()).toEqual({ done: false, value: false });
    await rejected;
    expect(reset).toHaveBeenCalledOnce();
    expect(track.closed).toBe(false);
    const received = receive(subscriber, 'events');
    expect(await demand.next()).toEqual({ done: false, value: true });
    await track.write(new Uint8Array([2]));
    await vi.waitFor(() => expect(received.objects).toHaveLength(1));
    expect(received.objects[0]).toMatchObject({ groupId: 1, payload: new Uint8Array([2]) });
    await demand.return?.();
  });

  it('observes demand, cancellation, resubscription, and track end without hanging iterator return', async () => {
    const pair = createTransportPair();
    const [publisher, subscriber] = await Promise.all([
      connectRelayPublisher({
        url,
        namespace,
        createTransport: () => ({ transport: pair.client, ready: Promise.resolve() }),
      }),
      connectRelaySubscriber({
        url,
        createTransport: () => ({ transport: withServerRequestIds(pair.server), ready: Promise.resolve() }),
      }),
    ]);
    cleanup.push(() => subscriber.close());
    cleanup.push(() => publisher.close());
    const track = publisher.registerTrack({ track: 'events' });
    const demand = track.demandChanges();
    expect(await demand.next()).toEqual({ done: false, value: false });
    expect(track.hasDemand).toBe(false);
    const subscription = subscriber.subscribe({ namespace, track: 'events' }, {});
    expect(await demand.next()).toEqual({ done: false, value: true });
    expect(track.hasDemand).toBe(true);
    const group = await track.openGroup();
    await group.write(new Uint8Array([1]));
    subscription.cancel();
    expect(await demand.next()).toEqual({ done: false, value: false });
    expect(group.closed).toBe(true);
    expect(track.closed).toBe(false);
    await expect(group.close()).rejects.toThrow(/subscription changed/);
    subscriber.subscribe({ namespace, track: 'events' }, {});
    expect(await demand.next()).toEqual({ done: false, value: true });
    const next = await track.openGroup();
    await next.write(new Uint8Array([2]));
    await next.close();
    const other = track.demandChanges();
    await other.next();
    const pending = other.next();
    await other.return?.();
    expect(await pending).toEqual({ done: true, value: undefined });
    track.close();
    expect(track.closed).toBe(true);
    expect(track.hasDemand).toBe(false);
    expect(await demand.next()).toEqual({ done: true, value: undefined });
    expect(await track.demandChanges().next()).toEqual({ done: true, value: undefined });
  });

  it('ends demand iterators and cancels an open group when the publisher closes', async () => {
    const { publisher, hub } = await connectHub();
    const track = publisher.registerTrack({ track: 'events' });
    hub.subscribeUpstream('events');
    const group = await track.openGroup();
    const demand = track.demandChanges();
    await demand.next();
    await group.write(new Uint8Array([1]));
    await publisher.close();
    await expect(group.close()).rejects.toThrow(/closed/);
    expect(track.closed).toBe(true);
    expect(track.hasDemand).toBe(false);
    expect(await demand.next()).toEqual({ done: true, value: undefined });
  });
});

describe('text and JSON helpers', () => {
  it('round-trips Unicode, empty text, and JSON on tracks and groups with metadata', async () => {
    const { publisher, subscriber } = await connectHub();
    const track = publisher.registerTrack({ track: 'helpers' });
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
    const track = publisher.registerTrack({ track: 'helpers' });
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
});

describe('relay hub', () => {
  it('declines a proactive PUBLISH with NOT_SUPPORTED (0x3), as moq-relay 0.15.8 does', async () => {
    const hub = createRelayHub();
    cleanup.push(() => hub.destroy());
    const { transport } = hub.connectPublisher(url, []);
    const stream = await transport.createBidirectionalStream();
    const writer = stream.writable.getWriter();
    await writer.write(encodePublish({ requestId: 0, trackNamespace: namespace, trackName: 'video', trackAlias: 1 }));

    const reader = new StreamReader(stream.readable);
    const type = await reader.readVarint();
    const length = (await reader.readUint8()) * 256 + (await reader.readUint8());
    const message = decodeControlMessage({ type, body: await reader.readBytes(length) });

    expect(message).toEqual({
      kind: 'request-error',
      errorCode: 0x3,
      retryInterval: 0,
      reason: 'PUBLISH is not supported',
    });
  });
});
