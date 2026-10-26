/**
 * The validation module through its interface, and each entry point's use of it. Portable cases come
 * from spec/vectors/arguments.json and run in every SDK; the local tables hold what JSON cannot carry
 * (sparse arrays, ArrayBufferViews, integer bounds) and the Python suite mirrors them by name. Rows
 * follow the "Arguments" table in spec/behavior.md.
 */
import { readFile } from 'node:fs/promises';
import {
  ByteWriter,
  encodeKeyValuePairs,
  MAX_OBJECT_PAYLOAD_LENGTH,
  MAX_TRACK_PROPERTIES_LENGTH,
} from '@caprid/moq-core';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  connectRelayPublisher,
  connectRelaySubscriber,
  type PublishProperty,
  RelayError,
  type RelaySubscriber,
  type RelayTransportFactory,
} from '../src/index.js';
import {
  DEFAULT_PUBLISHER_GROUP_ORDER,
  DEFAULT_PUBLISHER_GROUP_ORDERS,
  DEFAULT_PUBLISHER_PRIORITY,
  MAX_DEFAULT_PUBLISHER_PRIORITY,
  MAX_PROPERTY_VALUE_LENGTH,
  MAX_RELAY_OBJECT_PROPERTIES_LENGTH,
  MAX_RELAY_TRACK_PROPERTIES,
  namespaceFields,
  objectPropertyPairs,
  payloadBytes,
  trackName,
  trackPropertyPairs,
} from '../src/validation.js';
import { createFakeTransport } from './helpers/relay.js';
import { createRelayHub } from './helpers/relay-hub.js';

type ErrorKind = 'type' | 'range';
interface ArgumentCase {
  name: string;
  error?: ErrorKind;
}
interface NamespaceCase extends ArgumentCase {
  namespace: unknown;
}
interface TrackNameCase extends ArgumentCase {
  track: unknown;
}
interface ArgumentVectors {
  namespaces: NamespaceCase[];
  trackNames: TrackNameCase[];
}

const vectors = JSON.parse(
  await readFile(new URL('../../spec/vectors/arguments.json', import.meta.url), 'utf8'),
) as ArgumentVectors;
const ERROR_CLASS: Record<ErrorKind, ErrorConstructor> = { type: TypeError, range: RangeError };
const url = 'https://relay.example.com/moq';
const cleanup: (() => void | Promise<void>)[] = [];

afterEach(async () => {
  for (const dispose of cleanup.splice(0).reverse()) await dispose();
});

function accepted<T extends ArgumentCase>(cases: T[]): T[] {
  return cases.filter((item) => item.error === undefined);
}

function rejected<T extends ArgumentCase>(cases: T[]): (T & { error: ErrorKind })[] {
  return cases.filter((item): item is T & { error: ErrorKind } => item.error !== undefined);
}

async function connectFakeSubscriber() {
  const fake = createFakeTransport();
  const connecting = connectRelaySubscriber({
    url,
    createTransport: () => ({ transport: fake.transport, ready: Promise.resolve() }),
  });
  fake.sendServerSetup();
  const connection = await connecting;
  cleanup.push(() => connection.close());
  return { ...fake, connection };
}

async function connectHubPublisher() {
  const hub = createRelayHub();
  cleanup.push(() => hub.destroy());
  const publisher = await connectRelayPublisher({
    url,
    namespace: ['live', 'example'],
    createTransport: hub.connectPublisher,
  });
  cleanup.push(() => publisher.close());
  return { hub, publisher };
}

/** Every subscriber entry point that takes a namespace and a track name. */
function subscriberEntryPoints(connection: RelaySubscriber, namespace: unknown, track: unknown) {
  const fields = namespace as string[];
  const name = track as string;
  return {
    subscribe: () => connection.subscribe({ namespace: fields, track: name }),
    subscribeEvents: () => connection.subscribeEvents({ namespace: fields, track: name }),
    trackSource: () => connection.trackSource.subscribe({ trackNamespace: fields, trackName: name }),
  };
}

describe('spec/vectors/arguments.json: namespaces', () => {
  it.each(accepted(vectors.namespaces))('$name is admitted as a fresh copy', ({ namespace }) => {
    const input = [...(namespace as string[])];
    const fields = namespaceFields(input);
    expect(fields).toEqual(namespace);
    expect(fields).not.toBe(input);
    input.push('later');
    expect(fields).toEqual(namespace);
  });

  it.each(rejected(vectors.namespaces))('$name is a $error error', ({ namespace, error }) => {
    expect(() => namespaceFields(namespace)).toThrow(ERROR_CLASS[error]);
  });

  it.each(rejected(vectors.namespaces))(
    '$name is rejected by every subscriber entry point before a request id is taken',
    async ({ namespace, error }) => {
      const fake = await connectFakeSubscriber();
      for (const call of Object.values(subscriberEntryPoints(fake.connection, namespace, 'events'))) {
        expect(call).toThrow(ERROR_CLASS[error]);
      }
      fake.connection.subscribe({ namespace: ['live'], track: 'events' });
      const request = await fake.nextRequestStream();
      expect(await request.firstMessage).toMatchObject({ requestId: 0, trackNamespace: ['live'] });
    },
  );

  it.each(rejected(vectors.namespaces))(
    '$name is rejected by connectRelayPublisher before the token provider or the transport factory',
    async ({ namespace, error }) => {
      const getToken = vi.fn();
      const createTransport = vi.fn<RelayTransportFactory>();
      await expect(
        connectRelayPublisher({ url, namespace: namespace as string[], getToken, createTransport }),
      ).rejects.toThrow(ERROR_CLASS[error]);
      expect(getToken).not.toHaveBeenCalled();
      expect(createTransport).not.toHaveBeenCalled();
    },
  );

  it.each(accepted(vectors.namespaces))('$name is sent verbatim by subscribe', async ({ namespace }) => {
    const fake = await connectFakeSubscriber();
    fake.connection.subscribe({ namespace: namespace as string[], track: 'events' });
    const request = await fake.nextRequestStream();
    expect(await request.firstMessage).toMatchObject({ trackNamespace: namespace });
  });

  it('is not spread into characters when a string is passed to the publisher (#65)', async () => {
    const createTransport = vi.fn<RelayTransportFactory>(() => {
      throw new Error('FACTORY_REACHED');
    });
    await expect(
      connectRelayPublisher({ url, namespace: 'lab' as unknown as string[], createTransport }),
    ).rejects.toThrow(TypeError);
    expect(createTransport).not.toHaveBeenCalled();
  });

  it.each([
    ['a sparse array', new Array(1)],
    ['a Set of fields', new Set(['live'])],
    ['an arguments-like object', { length: 1, 0: 'live' }],
  ])('rejects %s, which JSON cannot express, as a TypeError', (_label, namespace) => {
    expect(() => namespaceFields(namespace)).toThrow(TypeError);
  });
});

describe('spec/vectors/arguments.json: track names', () => {
  it.each(accepted(vectors.trackNames))('$name is admitted unchanged', ({ track }) => {
    expect(trackName(track)).toBe(track);
  });

  it.each(rejected(vectors.trackNames))('$name is a $error error', ({ track, error }) => {
    expect(() => trackName(track)).toThrow(ERROR_CLASS[error]);
  });

  it.each(rejected(vectors.trackNames))(
    '$name is rejected by every subscriber entry point before a request id is taken',
    async ({ track, error }) => {
      const fake = await connectFakeSubscriber();
      for (const call of Object.values(subscriberEntryPoints(fake.connection, ['live'], track))) {
        expect(call).toThrow(ERROR_CLASS[error]);
      }
      fake.connection.subscribe({ namespace: ['live'], track: 'events' });
      const request = await fake.nextRequestStream();
      expect(await request.firstMessage).toMatchObject({ requestId: 0, trackName: 'events' });
    },
  );

  it('is rejected by registerTrack for every invalid case', async () => {
    const { publisher } = await connectHubPublisher();
    for (const { track, error } of rejected(vectors.trackNames)) {
      expect(() => publisher.registerTrack({ track: track as string })).toThrow(ERROR_CLASS[error]);
    }
    expect(publisher.registerTrack({ track: 'events' }).closed).toBe(false);
  });
});

describe('arguments are checked before connection or slot state', () => {
  it('reports an invalid namespace on a closed subscriber as a TypeError, not CLOSED', async () => {
    const fake = await connectFakeSubscriber();
    fake.connection.close();
    await fake.connection.closed;
    for (const call of Object.values(subscriberEntryPoints(fake.connection, 'live', 'events'))) {
      expect(call).toThrow(TypeError);
    }
    for (const call of Object.values(subscriberEntryPoints(fake.connection, ['live'], 'events'))) {
      expect(call).toThrow(RelayError);
    }
  });

  it('checks subscribeEvents arguments and connection state even when the signal is already aborted', async () => {
    const fake = await connectFakeSubscriber();
    const aborted = { signal: AbortSignal.abort() };
    expect(() => fake.connection.subscribeEvents({ namespace: [], track: '' }, aborted)).toThrow(TypeError);
    expect(() => fake.connection.subscribeEvents({ namespace: ['live'], track: '' }, aborted)).toThrow(TypeError);
    const events = fake.connection.subscribeEvents({ namespace: ['live'], track: 'events' }, aborted);
    await expect(events.next()).rejects.toBe(aborted.signal.reason);
    fake.connection.close();
    await fake.connection.closed;
    expect(() => fake.connection.subscribeEvents({ namespace: ['live'], track: 'events' }, aborted)).toThrow(
      RelayError,
    );
  });

  it('reports an invalid track name on a closed publisher as a TypeError, not CLOSED', async () => {
    const { publisher } = await connectHubPublisher();
    await publisher.close();
    expect(() => publisher.registerTrack({ track: '' })).toThrow(TypeError);
    expect(() => publisher.registerTrack({ track: 'events' })).toThrow(RelayError);
  });

  it('reports an invalid payload on a closed track writer as a TypeError, not CLOSED', async () => {
    const { publisher } = await connectHubPublisher();
    const track = publisher.registerTrack({ track: 'events' });
    track.close();
    await expect(track.write('bad' as unknown as ArrayBufferView)).rejects.toThrow(TypeError);
    await expect(track.write(new Uint8Array([1]))).rejects.toMatchObject({ code: 'CLOSED' });
  });
});

/** One odd pair whose encoding is exactly `length` bytes: its type and length take 4 bytes. */
function blockOf(length: number): PublishProperty[] {
  const properties = [{ type: 1, value: new Uint8Array(length - 4) }];
  const encoded = new ByteWriter(64);
  encodeKeyValuePairs(encoded, properties);
  expect(encoded.length).toBe(length);
  return properties;
}

/** Maximal byte values: each is legal on its own, but two already encode past every property bound. */
const maximalValues = (count: number): PublishProperty[] =>
  Array.from({ length: count }, (_, index) => ({
    type: 2 * index + 1,
    value: new Uint8Array(MAX_PROPERTY_VALUE_LENGTH),
  }));

/** Odd-typed pairs with empty values, to count pairs without nearing a byte bound or a draft-20 value rule. */
const emptyValues = (count: number): PublishProperty[] =>
  Array.from({ length: count }, (_, index) => ({ type: 2 * index + 1, value: new Uint8Array(0) }));

describe('properties', () => {
  it.each<[string, unknown]>([
    ['a string container', 'abc'],
    ['an object container', { type: 2, value: 1 }],
    ['a null pair', [null]],
    ['an empty pair', [{}]],
    ['an odd type with a string value', [{ type: 3, value: 'abc' }]],
    ['an odd type with an ArrayBuffer value', [{ type: 3, value: new ArrayBuffer(1) }]],
    ['an odd type with an integer value', [{ type: 3, value: 1 }]],
    ['a negative type', [{ type: -1, value: 0 }]],
    ['a fractional type', [{ type: 0.5, value: 0 }]],
    ['a type above the varint bound', [{ type: Number.MAX_SAFE_INTEGER + 1, value: 0 }]],
    ['an even type with a negative value', [{ type: 2, value: -1 }]],
    ['an even type with a fractional value', [{ type: 2, value: 1.5 }]],
    ['an even type with a NaN value', [{ type: 2, value: Number.NaN }]],
    ['an even type with a value above the varint bound', [{ type: 2, value: Number.MAX_SAFE_INTEGER + 1 }]],
    ['an even type with a byte value', [{ type: 2, value: new Uint8Array([1]) }]],
  ])('rejects %s as a TypeError', (_label, properties) => {
    expect(() => trackPropertyPairs(properties)).toThrow(TypeError);
  });

  it('rejects a byte value longer than its 16-bit wire length as a RangeError', () => {
    const properties = [{ type: 3, value: new Uint8Array(MAX_PROPERTY_VALUE_LENGTH + 1) }];
    expect(() => trackPropertyPairs(properties)).toThrow(RangeError);
    expect(() => trackPropertyPairs(properties)).toThrow(/Property 0 value exceeds/);
  });

  it('rejects a track list past the SUBSCRIBE_OK room as a RangeError naming the size', () => {
    // #252: SUBSCRIBE_OK carries the list, and a control message's length is 16 bits.
    expect(() => trackPropertyPairs(blockOf(MAX_TRACK_PROPERTIES_LENGTH + 1))).toThrow(RangeError);
    expect(() => trackPropertyPairs(blockOf(MAX_TRACK_PROPERTIES_LENGTH + 1))).toThrow(
      /encode to 65526 bytes, over the 65525 bytes SUBSCRIBE_OK has room for/,
    );
    expect(() => trackPropertyPairs(maximalValues(2))).toThrow(/SUBSCRIBE_OK has room for/);
  });

  it('rejects more track properties than the relay reads as a RangeError', () => {
    // moq-relay 0.17.0 refuses every subscriber with REQUEST_ERROR 18 "too many" past 64 pairs (#252).
    expect(() => trackPropertyPairs(emptyValues(MAX_RELAY_TRACK_PROPERTIES + 1))).toThrow(RangeError);
    expect(() => trackPropertyPairs(emptyValues(MAX_RELAY_TRACK_PROPERTIES + 1))).toThrow(
      /hold 65 pairs, over the relay's 64-pair limit/,
    );
  });

  it.each<[PublishProperty[], RegExp]>([
    [[{ type: DEFAULT_PUBLISHER_PRIORITY, value: MAX_DEFAULT_PUBLISHER_PRIORITY + 1 }], /Priority, 0x0E/],
    [
      [
        { type: 2, value: 0 },
        { type: DEFAULT_PUBLISHER_GROUP_ORDER, value: 0 },
      ],
      /Property 1 .*Group Order, 0x22/,
    ],
    [[{ type: DEFAULT_PUBLISHER_GROUP_ORDER, value: 3 }], /1 \(ascending\) or 2 \(descending\)/],
  ])('rejects a track property value draft-20 rules out as a RangeError (%#)', (properties, message) => {
    expect(() => trackPropertyPairs(properties)).toThrow(RangeError);
    expect(() => trackPropertyPairs(properties)).toThrow(message);
  });

  it('admits the boundaries: the SUBSCRIBE_OK room, the relay pair count, the draft-20 values and the varint ceiling', () => {
    expect(trackPropertyPairs(blockOf(MAX_TRACK_PROPERTIES_LENGTH))).toEqual(blockOf(MAX_TRACK_PROPERTIES_LENGTH));
    expect(trackPropertyPairs(emptyValues(MAX_RELAY_TRACK_PROPERTIES))).toHaveLength(MAX_RELAY_TRACK_PROPERTIES);
    expect(trackPropertyPairs([{ type: Number.MAX_SAFE_INTEGER - 1, value: Number.MAX_SAFE_INTEGER }])).toEqual([
      { type: Number.MAX_SAFE_INTEGER - 1, value: Number.MAX_SAFE_INTEGER },
    ]);
    for (const order of DEFAULT_PUBLISHER_GROUP_ORDERS) {
      const properties = [
        { type: DEFAULT_PUBLISHER_PRIORITY, value: MAX_DEFAULT_PUBLISHER_PRIORITY },
        { type: DEFAULT_PUBLISHER_GROUP_ORDER, value: order },
      ];
      expect(trackPropertyPairs(properties)).toEqual(properties);
    }
  });

  it('admits object properties that encode to exactly the relay limit', () => {
    expect(objectPropertyPairs(blockOf(MAX_RELAY_OBJECT_PROPERTIES_LENGTH))).toEqual(
      blockOf(MAX_RELAY_OBJECT_PROPERTIES_LENGTH),
    );
    expect(objectPropertyPairs(undefined)).toEqual([]);
  });

  it('rejects object properties past the relay limit as a RangeError naming the size', () => {
    expect(() => objectPropertyPairs(blockOf(MAX_RELAY_OBJECT_PROPERTIES_LENGTH + 1))).toThrow(RangeError);
    expect(() => objectPropertyPairs(blockOf(MAX_RELAY_OBJECT_PROPERTIES_LENGTH + 1))).toThrow(
      /encode to 65537 bytes, over the relay's 65536-byte limit/,
    );
    expect(() => objectPropertyPairs(maximalValues(16))).toThrow(/relay's 65536-byte limit/);
  });

  it('copies the bytes a view spans and admits an absent list as empty', () => {
    const bytes = new Uint8Array([9, 1, 2, 9]);
    const pairs = trackPropertyPairs([{ type: 3, value: new DataView(bytes.buffer, 1, 2) }]);
    bytes.fill(0);
    expect(pairs).toEqual([{ type: 3, value: new Uint8Array([1, 2]) }]);
    expect(trackPropertyPairs(undefined)).toEqual([]);
    expect(trackPropertyPairs([])).toEqual([]);
  });

  it('reports an oversized property as an argument error at registerTrack and write, not as a transport failure', async () => {
    const { publisher } = await connectHubPublisher();
    for (const refused of [
      maximalValues(2),
      emptyValues(MAX_RELAY_TRACK_PROPERTIES + 1),
      [{ type: DEFAULT_PUBLISHER_GROUP_ORDER, value: 3 }],
    ]) {
      expect(() => publisher.registerTrack({ track: 'events', trackProperties: refused })).toThrow(RangeError);
    }
    const track = publisher.registerTrack({ track: 'events' });
    await expect(track.write(new Uint8Array([1]), { properties: maximalValues(16) })).rejects.toThrow(RangeError);
    await expect(
      track.write(new Uint8Array([1]), {
        properties: [{ type: 3, value: new Uint8Array(MAX_PROPERTY_VALUE_LENGTH + 1) }],
      }),
    ).rejects.toThrow(RangeError);
    expect(track.closed).toBe(false);
  });
});

describe('payloads', () => {
  it.each<[string, unknown]>([
    ['a string', 'hello'],
    ['an ArrayBuffer', new Uint8Array([1, 2, 3]).buffer],
    ['a plain array', [1, 2, 3]],
    ['null', null],
    ['undefined', undefined],
    ['a number', 7],
  ])('rejects %s as a TypeError', (_label, payload) => {
    expect(() => payloadBytes(payload)).toThrow(TypeError);
  });

  it('rejects more than the object payload limit as a RangeError and admits exactly the limit', () => {
    expect(() => payloadBytes(new Uint8Array(MAX_OBJECT_PAYLOAD_LENGTH + 1))).toThrow(RangeError);
    expect(payloadBytes(new Uint8Array(MAX_OBJECT_PAYLOAD_LENGTH))).toHaveLength(MAX_OBJECT_PAYLOAD_LENGTH);
  });

  it('copies the bytes a view spans, for any ArrayBufferView, including an empty one', () => {
    const buffer = new Uint8Array([9, 9, 9, 1, 2, 3, 9, 9]).buffer;
    const fromView = payloadBytes(new DataView(buffer, 3, 3));
    expect(fromView).toEqual(new Uint8Array([1, 2, 3]));
    expect(fromView.buffer).not.toBe(buffer);
    expect(payloadBytes(new Uint8Array(0))).toHaveLength(0);
  });
});
