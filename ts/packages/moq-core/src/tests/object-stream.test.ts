import { describe, expect, it } from 'vitest';

import { ByteWriter, StreamReader, utf8Encode } from '../protocol/bytes.js';
import { MoqtProtocolError } from '../protocol/errors.js';
import {
  type FetchStreamEntry,
  isSubgroupHeaderType,
  MAX_OBJECT_PAYLOAD_LENGTH,
  MAX_OBJECT_PROPERTIES_LENGTH,
  type MoqtObject,
  readFetchEntries,
  readFetchHeader,
  readSubgroupHeader,
  readSubgroupObjects,
} from '../protocol/object-stream.js';

function streamOf(bytes: Uint8Array, chunkSize = Number.POSITIVE_INFINITY): ReadableStream<Uint8Array> {
  return new ReadableStream({
    start(controller) {
      for (let offset = 0; offset < bytes.length; offset += chunkSize) {
        controller.enqueue(bytes.subarray(offset, Math.min(offset + chunkSize, bytes.length)));
      }

      controller.close();
    },
  });
}

/** Build a subgroup stream: header (with explicit subgroup id + priority) and objects. */
function encodeSubgroupStream(options: {
  type: number;
  trackAlias: number;
  groupId: number;
  subgroupId?: number;
  priority?: number;
  objects: { objectIdDelta: number; payload?: Uint8Array; status?: number; properties?: Uint8Array }[];
}): Uint8Array {
  const writer = new ByteWriter();

  writer.writeVarint(options.type);
  writer.writeVarint(options.trackAlias);
  writer.writeVarint(options.groupId);

  if (options.subgroupId !== undefined) writer.writeVarint(options.subgroupId);

  if (options.priority !== undefined) writer.writeUint8(options.priority);

  for (const object of options.objects) {
    writer.writeVarint(object.objectIdDelta);

    if (object.properties !== undefined) {
      writer.writeVarint(object.properties.length);
      writer.writeBytes(object.properties);
    }

    const payload = object.payload ?? new Uint8Array(0);

    writer.writeVarint(payload.length);

    if (payload.length === 0) writer.writeVarint(object.status ?? 0x0);
    else writer.writeBytes(payload);
  }

  return writer.toBytes();
}

async function collect<T>(iterable: AsyncIterable<T>): Promise<T[]> {
  const items: T[] = [];

  for await (const item of iterable) items.push(item);

  return items;
}

describe('isSubgroupHeaderType', () => {
  it('accepts the valid 0b0XX1XXXX ranges', () => {
    for (const type of [0x10, 0x15, 0x18, 0x1d, 0x30, 0x3d, 0x50, 0x5d, 0x70, 0x7d]) {
      expect(isSubgroupHeaderType(type)).toBe(true);
    }
  });

  it('rejects reserved SUBGROUP_ID_MODE 0b11 and out-of-form values', () => {
    for (const type of [0x16, 0x17, 0x1e, 0x1f, 0x36, 0x76, 0x7e]) {
      expect(isSubgroupHeaderType(type)).toBe(false);
    }

    for (const type of [0x00, 0x05, 0x0f, 0x20, 0x2f, 0x40, 0x80, 0x2f00]) {
      expect(isSubgroupHeaderType(type)).toBe(false);
    }
  });
});

describe('readSubgroupHeader', () => {
  it('parses an explicit-subgroup-id header with priority', () => {
    // 0x14: bit4 set, mode 0b10 (explicit), no properties, priority present.
    const bytes = encodeSubgroupStream({
      type: 0x14,
      trackAlias: 3,
      groupId: 41,
      subgroupId: 2,
      priority: 128,
      objects: [],
    });
    const reader = new StreamReader(streamOf(bytes));

    return reader.readVarint().then(async (type) => {
      const header = await readSubgroupHeader(reader, type);

      expect(header).toMatchObject({
        trackAlias: 3,
        groupId: 41,
        subgroupIdMode: 'explicit',
        subgroupId: 2,
        priority: 128,
        hasProperties: false,
        endOfGroup: false,
        firstObject: false,
      });
    });
  });

  it('parses default-priority and end-of-group flag bits', async () => {
    // 0x38: bit4 set, mode 0b00 (zero), END_OF_GROUP (0x08), DEFAULT_PRIORITY (0x20).
    const bytes = encodeSubgroupStream({ type: 0x38, trackAlias: 1, groupId: 7, objects: [] });
    const reader = new StreamReader(streamOf(bytes));
    const header = await readSubgroupHeader(reader, await reader.readVarint());

    expect(header).toMatchObject({ subgroupIdMode: 'zero', endOfGroup: true, priority: undefined });
  });
});

describe('readSubgroupObjects', () => {
  it('delta-decodes object IDs (first absolute, then +delta+1)', async () => {
    const bytes = encodeSubgroupStream({
      type: 0x38,
      trackAlias: 1,
      groupId: 7,
      objects: [
        { objectIdDelta: 0, payload: utf8Encode('a') },
        { objectIdDelta: 0, payload: utf8Encode('b') }, // 0 + 0 + 1 = 1
        { objectIdDelta: 2, payload: utf8Encode('c') }, // 1 + 2 + 1 = 4
      ],
    });
    const reader = new StreamReader(streamOf(bytes, 3)); // tiny chunks

    await reader.readVarint();
    const header = await readSubgroupHeader(reader, 0x38);
    const objects = await collect(readSubgroupObjects(reader, header));

    expect(objects.map((o: MoqtObject) => o.objectId)).toEqual([0, 1, 4]);
    expect(objects.map((o) => new TextDecoder().decode(o.payload))).toEqual(['a', 'b', 'c']);
    expect(objects[0]).toMatchObject({ groupId: 7, subgroupId: 0, status: 'normal' });
  });

  it('resolves subgroup id from the first object in first-object-id mode', async () => {
    // 0x32: bit4 set... 0x32 = 0b0110010 — bit4 (0x10) set, mode bits (0x06)>>1 = 0b01, DEFAULT_PRIORITY set.
    const bytes = encodeSubgroupStream({
      type: 0x32,
      trackAlias: 1,
      groupId: 9,
      objects: [{ objectIdDelta: 5, payload: utf8Encode('x') }],
    });
    const reader = new StreamReader(streamOf(bytes));

    await reader.readVarint();
    const header = await readSubgroupHeader(reader, 0x32);

    expect(header.subgroupIdMode).toBe('first-object-id');
    const objects = await collect(readSubgroupObjects(reader, header));

    expect(objects[0]).toMatchObject({ objectId: 5, subgroupId: 5 });
  });

  it('parses zero-length objects with explicit status', async () => {
    const bytes = encodeSubgroupStream({
      type: 0x38,
      trackAlias: 1,
      groupId: 7,
      objects: [
        { objectIdDelta: 0, payload: utf8Encode('a') },
        { objectIdDelta: 0, status: 0x3 }, // end of group
      ],
    });
    const reader = new StreamReader(streamOf(bytes));

    await reader.readVarint();
    const header = await readSubgroupHeader(reader, 0x38);
    const objects = await collect(readSubgroupObjects(reader, header));

    expect(objects[1]).toMatchObject({ objectId: 1, status: 'end-of-group', payload: new Uint8Array(0) });
  });

  it('throws MoqtProtocolError on an unknown object status', async () => {
    const bytes = encodeSubgroupStream({
      type: 0x38,
      trackAlias: 1,
      groupId: 7,
      objects: [{ objectIdDelta: 0, status: 0x9 }],
    });
    const reader = new StreamReader(streamOf(bytes));

    await reader.readVarint();
    const header = await readSubgroupHeader(reader, 0x38);

    await expect(collect(readSubgroupObjects(reader, header))).rejects.toThrow(MoqtProtocolError);
  });

  it('throws MoqtProtocolError when delta-decoded object IDs overflow the varint range', async () => {
    const bytes = encodeSubgroupStream({
      type: 0x38,
      trackAlias: 1,
      groupId: 7,
      objects: [
        { objectIdDelta: Number.MAX_SAFE_INTEGER, payload: utf8Encode('a') },
        { objectIdDelta: 0, payload: utf8Encode('b') }, // 2^53-1 + 0 + 1 overflows
      ],
    });
    const reader = new StreamReader(streamOf(bytes));

    await reader.readVarint();
    const header = await readSubgroupHeader(reader, 0x38);

    await expect(collect(readSubgroupObjects(reader, header))).rejects.toThrow(MoqtProtocolError);
  });

  // The declared lengths below are followed by NO bytes: reaching the read
  // would surface a RangeError from the stream, so a MoqtProtocolError proves
  // the declaration is rejected before anything is allocated.
  it('rejects a payload length beyond MAX_OBJECT_PAYLOAD_LENGTH before allocating', async () => {
    const writer = new ByteWriter();

    writer.writeVarint(0x38);
    writer.writeVarint(1); // track alias
    writer.writeVarint(7); // group id
    writer.writeVarint(0); // object id delta
    writer.writeVarint(MAX_OBJECT_PAYLOAD_LENGTH + 1);
    const reader = new StreamReader(streamOf(writer.toBytes()));

    await reader.readVarint();
    const header = await readSubgroupHeader(reader, 0x38);

    await expect(collect(readSubgroupObjects(reader, header))).rejects.toThrow(/payload length .* exceeds/);
  });

  it('rejects a properties length beyond MAX_OBJECT_PROPERTIES_LENGTH before allocating', async () => {
    const writer = new ByteWriter();

    writer.writeVarint(0x39); // 0x38 | PROPERTIES
    writer.writeVarint(1);
    writer.writeVarint(7);
    writer.writeVarint(0); // object id delta
    writer.writeVarint(MAX_OBJECT_PROPERTIES_LENGTH + 1);
    const reader = new StreamReader(streamOf(writer.toBytes()));

    await reader.readVarint();
    const header = await readSubgroupHeader(reader, 0x39);

    await expect(collect(readSubgroupObjects(reader, header))).rejects.toThrow(/properties length .* exceeds/);
  });

  it('accepts a payload exactly at MAX_OBJECT_PAYLOAD_LENGTH', async () => {
    const bytes = encodeSubgroupStream({
      type: 0x38,
      trackAlias: 1,
      groupId: 7,
      objects: [{ objectIdDelta: 0, payload: new Uint8Array(MAX_OBJECT_PAYLOAD_LENGTH) }],
    });
    const reader = new StreamReader(streamOf(bytes));

    await reader.readVarint();
    const header = await readSubgroupHeader(reader, 0x38);
    const objects = await collect(readSubgroupObjects(reader, header));

    expect(objects[0]!.payload.length).toBe(MAX_OBJECT_PAYLOAD_LENGTH);
  });

  it('accepts a properties block holding a maximum-size KVP value', async () => {
    // One odd-type KVP whose byte value is the largest the KVP codec
    // permits (2^16-1): the aggregate block bound must not reject what the
    // per-value rule allows.
    const props = new ByteWriter();

    props.writeVarint(0x07);
    props.writeVarint(0xffff);
    props.writeBytes(new Uint8Array(0xffff).fill(1));
    const bytes = encodeSubgroupStream({
      type: 0x39, // 0x38 | PROPERTIES
      trackAlias: 1,
      groupId: 7,
      objects: [{ objectIdDelta: 0, payload: utf8Encode('a'), properties: props.toBytes() }],
    });
    const reader = new StreamReader(streamOf(bytes));

    await reader.readVarint();
    const header = await readSubgroupHeader(reader, 0x39);
    const objects = await collect(readSubgroupObjects(reader, header));

    expect(objects[0]!.properties).toHaveLength(1);
    expect(objects[0]!.properties[0]).toMatchObject({ type: 0x07 });
    expect((objects[0]!.properties[0]!.value as Uint8Array).length).toBe(0xffff);
  });

  it('parses per-object properties when the header PROPERTIES bit is set', async () => {
    // Properties KVP: type 0x06 (Timestamp), varint value 90000.
    const props = new ByteWriter();

    props.writeVarint(0x06);
    props.writeVarint(90_000);
    const bytes = encodeSubgroupStream({
      type: 0x39, // 0x38 | PROPERTIES
      trackAlias: 1,
      groupId: 7,
      objects: [{ objectIdDelta: 0, payload: utf8Encode('a'), properties: props.toBytes() }],
    });
    const reader = new StreamReader(streamOf(bytes));

    await reader.readVarint();
    const header = await readSubgroupHeader(reader, 0x39);
    const objects = await collect(readSubgroupObjects(reader, header));

    expect(objects[0]!.properties).toEqual([{ type: 0x06, value: 90_000 }]);
  });
});

describe('readFetchEntries', () => {
  function encodeFetchStream(build: (writer: ByteWriter) => void): Uint8Array {
    const writer = new ByteWriter();

    writer.writeVarint(0x05); // FETCH_HEADER
    writer.writeVarint(8); // request id
    build(writer);
    return writer.toBytes();
  }

  it('parses the fetch header request id', async () => {
    const reader = new StreamReader(streamOf(encodeFetchStream(() => {})));

    await reader.readVarint();
    expect(await readFetchHeader(reader)).toEqual({ requestId: 8 });
  });

  it('decodes absolute-first then delta-encoded objects', async () => {
    const bytes = encodeFetchStream((w) => {
      // First object: flags need GROUP_ID_DELTA (0x08) + OBJECT_ID_DELTA (0x04)
      // + PRIORITY (0x10); subgroup mode 0 (subgroup id zero).
      w.writeVarint(0x1c);
      w.writeVarint(10); // group id (absolute)
      w.writeVarint(0); // object id (absolute)
      w.writeUint8(100); // priority
      w.writeVarint(1); // payload length
      w.writeBytes(utf8Encode('a'));
      // Second object: same group, next object id (no fields at all).
      w.writeVarint(0x00);
      w.writeVarint(1);
      w.writeBytes(utf8Encode('b'));
      // Third object: next group (delta 0 → +1), object id resets via delta.
      w.writeVarint(0x0c);
      w.writeVarint(0); // group delta → 10 + 0 + 1 = 11
      w.writeVarint(0); // object id absolute within new group
      w.writeVarint(1);
      w.writeBytes(utf8Encode('c'));
    });
    const reader = new StreamReader(streamOf(bytes, 4));

    await reader.readVarint();
    await readFetchHeader(reader);
    const entries = await collect(readFetchEntries(reader));

    expect(entries.map((e: FetchStreamEntry) => (e.kind === 'object' ? [e.groupId, e.objectId] : null))).toEqual([
      [10, 0],
      [10, 1],
      [11, 0],
    ]);
  });

  it('rejects a payload length beyond MAX_OBJECT_PAYLOAD_LENGTH before allocating', async () => {
    const bytes = encodeFetchStream((w) => {
      w.writeVarint(0x1c); // GROUP_ID_DELTA | OBJECT_ID_DELTA | PRIORITY
      w.writeVarint(10); // group id
      w.writeVarint(0); // object id
      w.writeUint8(100); // priority
      w.writeVarint(MAX_OBJECT_PAYLOAD_LENGTH + 1); // no bytes follow
    });
    const reader = new StreamReader(streamOf(bytes));

    await reader.readVarint();
    await readFetchHeader(reader);
    await expect(collect(readFetchEntries(reader))).rejects.toThrow(/payload length .* exceeds/);
  });

  it('yields end-of-range markers for all three statuses (§11.4.1, Table 7)', async () => {
    const bytes = encodeFetchStream((w) => {
      w.writeVarint(0x8c); // End of Non-Existent Range
      w.writeVarint(4);
      w.writeVarint(20);
      w.writeVarint(0x10c); // End of Unknown Range
      w.writeVarint(5);
      w.writeVarint(0);
      w.writeVarint(0x20c); // End of Timed-Out Range
      w.writeVarint(6);
      w.writeVarint(2);
    });
    const reader = new StreamReader(streamOf(bytes));

    await reader.readVarint();
    await readFetchHeader(reader);
    const entries = await collect(readFetchEntries(reader));

    expect(entries).toEqual([
      { kind: 'end-of-range', status: 'non-existent', groupId: 4, objectId: 20 },
      { kind: 'end-of-range', status: 'unknown', groupId: 5, objectId: 0 },
      { kind: 'end-of-range', status: 'timed-out', groupId: 6, objectId: 2 },
    ]);
  });

  it('rejects a first object that references the prior object', async () => {
    const bytes = encodeFetchStream((w) => {
      w.writeVarint(0x00); // no group/object deltas on the first object
    });
    const reader = new StreamReader(streamOf(bytes));

    await reader.readVarint();
    await readFetchHeader(reader);
    await expect(collect(readFetchEntries(reader))).rejects.toThrow(MoqtProtocolError);
  });

  it('rejects delta-decoded locations that overflow the varint range', async () => {
    const bytes = encodeFetchStream((w) => {
      // First object: absolute group at the varint ceiling.
      w.writeVarint(0x1c);
      w.writeVarint(Number.MAX_SAFE_INTEGER); // group id (absolute)
      w.writeVarint(0); // object id (absolute)
      w.writeUint8(100); // priority
      w.writeVarint(1); // payload length
      w.writeBytes(utf8Encode('a'));
      // Second object: group delta 0 → 2^53-1 + 0 + 1 overflows.
      w.writeVarint(0x0c);
      w.writeVarint(0); // group delta
      w.writeVarint(0); // object id
      w.writeVarint(1);
      w.writeBytes(utf8Encode('b'));
    });
    const reader = new StreamReader(streamOf(bytes));

    await reader.readVarint();
    await readFetchHeader(reader);
    await expect(collect(readFetchEntries(reader))).rejects.toThrow(MoqtProtocolError);
  });
});
