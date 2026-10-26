/** Language-neutral encoding vectors shared by every SDK; see ../../spec/vectors/README.md. */
import { readFile } from 'node:fs/promises';

import { describe, expect, it } from 'vitest';

import { ByteReader, ByteWriter } from '../packages/moq-core/src/protocol/bytes.js';
import {
  ControlMessageDeframer,
  decodeControlMessage,
  decodeMessageParameters,
  encodeMessageParameters,
  encodeRequestOk,
  type KeyValuePair,
  type Location,
  type LocationFilter,
  MOQT_PROTOCOL_IDS,
  type MoqtProtocolId,
} from '../packages/moq-core/src/protocol/control-messages.js';
import { MoqtProtocolError } from '../packages/moq-core/src/protocol/errors.js';
import { decodeVarint, encodeVarint } from '../packages/moq-core/src/protocol/varint.js';

interface VarintVector {
  /** Decimal string, so 64-bit values survive JSON. */
  value: string;
  /** Lowercase hex, no separators. */
  bytes: string;
}

interface VarintVectors {
  minimal: VarintVector[];
  nonMinimal: VarintVector[];
  beyond53Bits: VarintVector[];
}

const vectors = JSON.parse(
  await readFile(new URL('../../spec/vectors/varint.json', import.meta.url), 'utf8'),
) as VarintVectors;
const hex = (bytes: Uint8Array) => Buffer.from(bytes).toString('hex');
const fromHex = (text: string) => Uint8Array.from(Buffer.from(text, 'hex'));

describe('spec/vectors/varint.json', () => {
  it('encodes every minimal vector to its bytes and decodes it back', () => {
    for (const { value, bytes } of vectors.minimal) {
      expect(hex(encodeVarint(Number(value)))).toBe(bytes);
      expect(decodeVarint(fromHex(bytes))).toEqual({ value: Number(value), byteLength: bytes.length / 2 });
    }
  });

  it('decodes non-minimal encodings without ever producing them', () => {
    for (const { value, bytes } of vectors.nonMinimal) {
      expect(decodeVarint(fromHex(bytes)).value).toBe(Number(value));
      expect(hex(encodeVarint(Number(value)))).not.toBe(bytes);
    }
  });

  it('rejects values above 2^53-1 instead of losing precision', () => {
    for (const { bytes } of vectors.beyond53Bits) {
      expect(() => decodeVarint(fromHex(bytes))).toThrow(MoqtProtocolError);
    }
  });
});

interface VectorFilter {
  type: LocationFilter['type'];
  groupsBeforeNext?: string;
  start?: { group: string; object: string };
  endGroupDelta?: string;
  endObject?: string;
}

interface LocationFilterVectors {
  filters: ({ filter: VectorFilter } & Record<MoqtProtocolId, string>)[];
  encodeOnly: ({ filter: VectorFilter; 'moqt-20 decodes as': VectorFilter } & Record<MoqtProtocolId, string>)[];
  rejected: { protocol: MoqtProtocolId; bytes: string; reason: string }[];
}

const locationFilters = JSON.parse(
  await readFile(new URL('../../spec/vectors/location-filter.json', import.meta.url), 'utf8'),
) as LocationFilterVectors;

/** The vector's decimal strings as the core's numbers. */
function toFilter(vector: VectorFilter): LocationFilter {
  switch (vector.type) {
    case 'none':
    case 'next-object':
      return { type: vector.type };
    case 'relative-group':
      return { type: 'relative-group', groupsBeforeNext: Number(vector.groupsBeforeNext) };
    case 'absolute':
      return {
        type: 'absolute',
        start: { group: Number(vector.start!.group), object: Number(vector.start!.object) },
        ...(vector.endGroupDelta === undefined ? {} : { endGroupDelta: Number(vector.endGroupDelta) }),
        ...(vector.endObject === undefined ? {} : { endObject: Number(vector.endObject) }),
      };
  }
}

function encodeFilterParameter(filter: LocationFilter, protocol: MoqtProtocolId): string {
  const writer = new ByteWriter();

  encodeMessageParameters(writer, { locationFilter: filter }, protocol);
  return hex(writer.toBytes());
}

function decodeFilterParameter(bytes: string, protocol: MoqtProtocolId): LocationFilter | undefined {
  const reader = new ByteReader(fromHex(bytes));
  const parameters = decodeMessageParameters(reader, protocol);

  expect(reader.remaining).toBe(0);
  return parameters.locationFilter;
}

describe('spec/vectors/location-filter.json', () => {
  it.each(MOQT_PROTOCOL_IDS)('encodes every filter to its %s bytes and decodes it back', (protocol) => {
    for (const vector of locationFilters.filters) {
      const filter = toFilter(vector.filter);

      expect(encodeFilterParameter(filter, protocol)).toBe(vector[protocol]);
      expect(decodeFilterParameter(vector[protocol], protocol)).toEqual(filter);
    }
  });

  it('encodes an open-ended absolute {0,0} as no filter on draft-20 only', () => {
    for (const vector of locationFilters.encodeOnly) {
      const filter = toFilter(vector.filter);

      expect(encodeFilterParameter(filter, 'moqt-22')).toBe(vector['moqt-22']);
      expect(decodeFilterParameter(vector['moqt-22'], 'moqt-22')).toEqual(filter);
      expect(encodeFilterParameter(filter, 'moqt-20')).toBe(vector['moqt-20']);
      expect(decodeFilterParameter(vector['moqt-20'], 'moqt-20')).toEqual(toFilter(vector['moqt-20 decodes as']));
    }
  });

  it('rejects every rejected vector', () => {
    for (const { protocol, bytes } of locationFilters.rejected) {
      expect(() => decodeMessageParameters(new ByteReader(fromHex(bytes)), protocol)).toThrow();
    }
  });
});

interface TrackStatusOkCase {
  name: string;
  largestObject?: { group: string; object: string };
  trackProperties: ({ type: number; value: string } | { type: number; bytes: string })[];
  bytes: string;
}

interface TrackStatusOkVectors {
  protocols: MoqtProtocolId[];
  cases: TrackStatusOkCase[];
}

const trackStatusOk = JSON.parse(
  await readFile(new URL('../../spec/vectors/track-status-ok.json', import.meta.url), 'utf8'),
) as TrackStatusOkVectors;

/** The vector's track properties as the core's pairs, sorted by type as the encoder emits them. */
function toTrackProperties(vector: TrackStatusOkCase): KeyValuePair[] {
  return vector.trackProperties
    .map(
      (property): KeyValuePair =>
        'bytes' in property
          ? { type: property.type, value: fromHex(property.bytes) }
          : { type: property.type, value: Number(property.value) },
    )
    .sort((a, b) => a.type - b.type);
}

function toParameters(vector: TrackStatusOkCase): { largestObject?: Location } {
  return vector.largestObject === undefined
    ? {}
    : { largestObject: { group: Number(vector.largestObject.group), object: Number(vector.largestObject.object) } };
}

describe('spec/vectors/track-status-ok.json', () => {
  it('lists both negotiated protocols', () => {
    expect(trackStatusOk.protocols).toEqual([...MOQT_PROTOCOL_IDS]);
  });

  it.each(MOQT_PROTOCOL_IDS)('encodes every case to its bytes and decodes it back on %s', (protocol) => {
    expect(trackStatusOk.protocols).toContain(protocol);
    for (const vector of trackStatusOk.cases) {
      const parameters = toParameters(vector);
      const trackProperties = toTrackProperties(vector);
      const frames = new ControlMessageDeframer().push(fromHex(vector.bytes));

      expect(hex(encodeRequestOk(parameters, protocol, trackProperties)), vector.name).toBe(vector.bytes);
      expect(frames, vector.name).toHaveLength(1);
      expect(decodeControlMessage(frames[0]!, protocol), vector.name).toEqual({
        kind: 'request-ok',
        parameters,
        trackProperties,
      });
    }
  });

  it('encodes properties given out of order the same as sorted ones', () => {
    for (const vector of trackStatusOk.cases) {
      const given = vector.trackProperties.map(
        (property): KeyValuePair =>
          'bytes' in property
            ? { type: property.type, value: fromHex(property.bytes) }
            : { type: property.type, value: Number(property.value) },
      );

      expect(hex(encodeRequestOk(toParameters(vector), 'moqt-22', given)), vector.name).toBe(vector.bytes);
    }
  });
});
