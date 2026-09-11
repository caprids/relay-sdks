import { describe, expect, it } from 'vitest';

import { ByteReader, ByteWriter, utf8Encode } from '../protocol/bytes.js';
import {
  ControlMessageDeframer,
  decodeControlMessage,
  decodeKeyValuePairs,
  decodeLocationFilter,
  decodeMessageParameters,
  encodeFetch,
  encodeFetchOk,
  encodeGoaway,
  encodeKeyValuePairs,
  encodeLocationFilter,
  encodeMessageParameters,
  encodePublishDone,
  encodePublishNamespace,
  encodePublishStateNotify,
  encodeRequestError,
  encodeSetup,
  encodeSubscribe,
  encodeSubscribeOk,
  isRetryablePublishDoneStatus,
  isRetryableRequestErrorCode,
  type LocationFilter,
  MESSAGE_TYPE,
  type MessageParameters,
  PARAMETER_TYPE,
  PUBLISH_DONE_STATUS,
  REQUEST_ERROR_CODE,
  SETUP_OPTION,
} from '../protocol/control-messages.js';
import { MoqtProtocolError } from '../protocol/errors.js';

function frames(...messages: Uint8Array[]): Uint8Array {
  const merged = new Uint8Array(messages.reduce((total, m) => total + m.length, 0));
  let offset = 0;

  for (const message of messages) {
    merged.set(message, offset);
    offset += message.length;
  }

  return merged;
}

function decodeAll(bytes: Uint8Array) {
  const deframer = new ControlMessageDeframer();

  return deframer.push(bytes).map(decodeControlMessage);
}

function encodeParameters(parameters: MessageParameters): Uint8Array {
  const writer = new ByteWriter();

  encodeMessageParameters(writer, parameters);
  return writer.toBytes();
}

describe('ControlMessageDeframer', () => {
  it('reassembles messages split across arbitrary chunk boundaries', () => {
    const encoded = frames(
      encodeSubscribe({ requestId: 0, trackNamespace: ['live', 'a'], trackName: 'video' }),
      encodeGoaway(250)
    );

    // Split at every possible boundary and confirm both frames come out.
    for (let split = 1; split < encoded.length; split++) {
      const deframer = new ControlMessageDeframer();
      const first = deframer.push(encoded.subarray(0, split));
      const second = deframer.push(encoded.subarray(split));
      const all = [...first, ...second];

      expect(all).toHaveLength(2);
      expect(all[0]!.type).toBe(MESSAGE_TYPE.SUBSCRIBE);
      expect(all[1]!.type).toBe(MESSAGE_TYPE.GOAWAY);
      expect(deframer.pendingBytes).toBe(0);
    }
  });

  it('yields one frame per push when messages arrive whole', () => {
    const deframer = new ControlMessageDeframer();

    expect(deframer.push(encodeGoaway(0))).toHaveLength(1);
    expect(deframer.push(encodePublishDone(0x2, 5, 'track ended'))).toHaveLength(1);
  });
});

describe('encodeSubscribe', () => {
  it('round-trips through the decoder', () => {
    const parameters: MessageParameters = {
      subscriberPriority: 16,
      groupOrder: 'ascending',
      locationFilter: { type: 'next-object' },
      forward: 1,
    };
    const [message] = decodeAll(
      encodeSubscribe({ requestId: 4, trackNamespace: ['live', 'stream1'], trackName: 'catalog', parameters })
    );

    expect(message).toMatchObject({
      kind: 'subscribe',
      requestId: 4,
      trackNamespace: ['live', 'stream1'],
      trackName: 'catalog',
      parameters: {
        subscriberPriority: 16,
        groupOrder: 'ascending',
        locationFilter: { type: 'next-object' },
        forward: 1,
      },
    });
  });

  it('round-trips the canonical current-group join: Next Object plus a StartGroup=1 fill', () => {
    const parameters: MessageParameters = {
      locationFilter: { type: 'next-object' },
      fillParameters: { locationFilter: { type: 'relative-group', groupsBeforeNext: 1 } },
    };
    const [message] = decodeAll(
      encodeSubscribe({ requestId: 0, trackNamespace: ['live'], trackName: 'video', parameters })
    );

    expect(message).toMatchObject({ kind: 'subscribe', parameters });
  });

  it('rejects empty namespace fields', () => {
    expect(() => encodeSubscribe({ requestId: 0, trackNamespace: [''], trackName: 't' })).toThrow(MoqtProtocolError);
  });
});

describe('encodeFetch', () => {
  it('round-trips a fetch whose range rides in LOCATION_FILTER', () => {
    const [message] = decodeAll(
      encodeFetch({
        requestId: 2,
        trackNamespace: ['live'],
        trackName: 'video',
        parameters: { locationFilter: { type: 'absolute', start: { group: 10, object: 0 }, endGroupDelta: 2 } },
      })
    );

    expect(message).toEqual({
      kind: 'fetch',
      request: {
        requestId: 2,
        trackNamespace: ['live'],
        trackName: 'video',
        parameters: { locationFilter: { type: 'absolute', start: { group: 10, object: 0 }, endGroupDelta: 2 } },
      },
    });
  });

  it('round-trips an unfiltered fetch (the whole track up to Largest Object)', () => {
    const [message] = decodeAll(encodeFetch({ requestId: 6, trackNamespace: ['live'], trackName: 'catalog' }));

    expect(message).toEqual({
      kind: 'fetch',
      request: { requestId: 6, trackNamespace: ['live'], trackName: 'catalog', parameters: {} },
    });
  });
});

describe('encodeSubscribeOk', () => {
  it('round-trips alias, parameters, and track properties', () => {
    const [message] = decodeAll(
      encodeSubscribeOk(7, { expires: 30_000, largestObject: { group: 41, object: 12 } }, [{ type: 0x02, value: 1 }])
    );

    expect(message).toMatchObject({
      kind: 'subscribe-ok',
      trackAlias: 7,
      parameters: { expires: 30_000, largestObject: { group: 41, object: 12 } },
    });
    expect(message).toHaveProperty('trackProperties', [{ type: 0x02, value: 1 }]);
  });
});

describe('encodePublishDone', () => {
  it('round-trips a counted stream total', () => {
    const [message] = decodeAll(encodePublishDone(PUBLISH_DONE_STATUS.TRACK_ENDED, 5, 'ended'));

    expect(message).toEqual({
      kind: 'publish-done',
      statusCode: PUBLISH_DONE_STATUS.TRACK_ENDED,
      streamCount: 5,
      reason: 'ended',
    });
  });

  it('reads the 2^64-1 "could not count" sentinel as an undefined stream count', () => {
    // Status, then the 9-byte varint 0xFF..FF (2^64-1), then an empty reason.
    const body = Uint8Array.of(PUBLISH_DONE_STATUS.GOING_AWAY, ...new Array<number>(9).fill(0xff), 0x00);
    const message = decodeControlMessage({ type: MESSAGE_TYPE.PUBLISH_DONE, body });

    expect(message).toEqual({
      kind: 'publish-done',
      statusCode: PUBLISH_DONE_STATUS.GOING_AWAY,
      streamCount: undefined,
      reason: '',
    });
  });

  it('rejects a stream count above 2^53-1 that is not the sentinel', () => {
    // A 9-byte varint one bit short of the sentinel: 2^63 + …, not a count any publisher can mean.
    const body = Uint8Array.of(PUBLISH_DONE_STATUS.GOING_AWAY, 0xff, 0x80, ...new Array<number>(7).fill(0xff), 0x00);

    expect(() => decodeControlMessage({ type: MESSAGE_TYPE.PUBLISH_DONE, body })).toThrow(MoqtProtocolError);
  });
});

describe('encodePublishStateNotify', () => {
  it('round-trips the changed parameters', () => {
    const parameters: MessageParameters = {
      largestObject: { group: 9, object: 4 },
      locationFilter: { type: 'relative-group', groupsBeforeNext: 0 },
    };
    const [message] = decodeAll(encodePublishStateNotify(parameters));

    expect(message).toEqual({ kind: 'publish-state-notify', parameters });
  });
});

describe('encodeRequestError', () => {
  it('round-trips code, retry interval, and reason', () => {
    const [message] = decodeAll(encodeRequestError(REQUEST_ERROR_CODE.DOES_NOT_EXIST, 'no such track', 1500));

    expect(message).toEqual({
      kind: 'request-error',
      errorCode: REQUEST_ERROR_CODE.DOES_NOT_EXIST,
      retryInterval: 1500,
      reason: 'no such track',
    });
  });
});

describe('encodeSetup', () => {
  it('round-trips setup options as generic key-value pairs', () => {
    const [message] = decodeAll(
      encodeSetup([
        { type: SETUP_OPTION.MOQT_IMPLEMENTATION, value: utf8Encode('spf-test') },
        { type: SETUP_OPTION.MAX_REQUEST_UPDATES, value: 3 },
      ])
    );

    expect(message?.kind).toBe('setup');

    if (message?.kind !== 'setup') return;

    // Serialized in ascending type order regardless of input order.
    expect(message.options.map((o) => o.type)).toEqual([
      SETUP_OPTION.MOQT_IMPLEMENTATION,
      SETUP_OPTION.MAX_REQUEST_UPDATES,
    ]);
    expect(message.options[1]!.value).toBe(3);
  });
});

describe('encodeGoaway', () => {
  it('encodes a zero-length URI (client rule) and round-trips the timeout', () => {
    const [message] = decodeAll(encodeGoaway(500));

    expect(message).toEqual({ kind: 'goaway', newSessionUri: '', timeout: 500 });
  });
});

describe('encodePublishNamespace', () => {
  it('round-trips through the decoder', () => {
    const [message] = decodeAll(
      encodePublishNamespace({ requestId: 1, trackNamespace: ['anon'], parameters: { forward: 1 } })
    );

    expect(message).toMatchObject({
      kind: 'publish-namespace',
      requestId: 1,
      trackNamespace: ['anon'],
      parameters: { forward: 1 },
    });
  });
});

describe('decodeControlMessage', () => {
  it('throws MoqtProtocolError for unknown message types', () => {
    expect(() => decodeControlMessage({ type: 0x99, body: new Uint8Array(0) })).toThrow(MoqtProtocolError);
  });

  it('throws when the body has trailing bytes', () => {
    // GOAWAY body: uri length 0, timeout 0, plus a stray trailing byte.
    expect(() => decodeControlMessage({ type: MESSAGE_TYPE.GOAWAY, body: new Uint8Array([0, 0, 0xab]) })).toThrow(
      MoqtProtocolError
    );
  });

  it('throws MoqtProtocolError (not RangeError) for a truncated body', () => {
    // PUBLISH_NAMESPACE body cut short: request id, then no namespace —
    // the RangeError from reading past the frame must be normalized so
    // malformed frames terminate the session.
    expect(() => decodeControlMessage({ type: MESSAGE_TYPE.PUBLISH_NAMESPACE, body: new Uint8Array([0x01]) })).toThrow(
      MoqtProtocolError
    );

    // A namespace field whose declared length overruns the frame.
    const body = new ByteWriter();

    body.writeVarint(1); // request id
    body.writeVarint(1); // field count
    body.writeVarint(10); // field length, followed by only 2 bytes
    body.writeBytes(utf8Encode('ab'));
    expect(() => decodeControlMessage({ type: MESSAGE_TYPE.PUBLISH_NAMESPACE, body: body.toBytes() })).toThrow(
      MoqtProtocolError
    );
  });
});

describe('encodeMessageParameters', () => {
  it('round-trips every supported parameter', () => {
    const parameters: MessageParameters = {
      objectDeliveryTimeout: 100,
      subgroupDeliveryTimeout: 200,
      authorizationTokens: [utf8Encode('tok-a'), utf8Encode('tok-b')],
      expires: 60_000,
      largestObject: { group: 100, object: 3 },
      forward: 0,
      subscriberPriority: 200,
      locationFilter: { type: 'absolute', start: { group: 5, object: 1 }, endGroupDelta: 4 },
      groupOrder: 'descending',
      fillParameters: {
        fillTimeout: 500,
        subscriberPriority: 8,
        locationFilter: { type: 'relative-group', groupsBeforeNext: 1 },
        groupOrder: 'ascending',
      },
      newGroupRequest: 42,
      includeProperties: 0,
    };
    const decoded = decodeMessageParameters(new ByteReader(encodeParameters(parameters)));

    expect(decoded).toEqual(parameters);
  });

  it('throws MoqtProtocolError on an unknown parameter type', () => {
    // Hand-craft: 1 parameter, type 0x99 (unknown).
    const writer = new ByteWriter();

    writer.writeVarint(1);
    writer.writeVarint(0x99);
    expect(() => decodeMessageParameters(new ByteReader(writer.toBytes()))).toThrow(MoqtProtocolError);
  });

  // §10.2 calls LARGEST_OBJECT a bare Location; moq-relay frames it with a
  // length by the odd-type parity rule, and the codec follows the relay.
  // Vector from moq-dev/moq `test_param_location_wire_vectors`.
  it('frames LARGEST_OBJECT length-prefixed, byte-for-byte with moq-relay', () => {
    const bytes = encodeParameters({ largestObject: { group: 255, object: 128 } });

    expect(Array.from(bytes)).toEqual([0x01, 0x09, 0x04, 0x80, 0xff, 0x80, 0x80]);
    expect(decodeMessageParameters(new ByteReader(bytes))).toEqual({ largestObject: { group: 255, object: 128 } });
  });

  it('rejects a LARGEST_OBJECT value with trailing bytes', () => {
    const writer = new ByteWriter();

    writer.writeVarint(1);
    writer.writeVarint(PARAMETER_TYPE.LARGEST_OBJECT);
    writer.writeVarint(3); // length: two varints plus a stray byte
    writer.writeVarint(1);
    writer.writeVarint(2);
    writer.writeUint8(0);
    expect(() => decodeMessageParameters(new ByteReader(writer.toBytes()))).toThrow(/trailing bytes/);
  });

  it('frames INCLUDE_PROPERTIES as a length-prefixed byte and rejects values other than 0 and 1', () => {
    expect(Array.from(encodeParameters({ includeProperties: 1 }))).toEqual([0x01, 0x35, 0x01, 0x01]);

    const writer = new ByteWriter();

    writer.writeVarint(1);
    writer.writeVarint(PARAMETER_TYPE.INCLUDE_PROPERTIES);
    writer.writeVarint(1);
    writer.writeUint8(2);
    expect(() => decodeMessageParameters(new ByteReader(writer.toBytes()))).toThrow(/INCLUDE_PROPERTIES/);
  });

  it('encodes FILL_PARAMETERS as a nested parameter list', () => {
    const bytes = encodeParameters({
      fillParameters: { locationFilter: { type: 'relative-group', groupsBeforeNext: 1 } },
    });

    // count 1, type 0x23, length 4, then the nested list: count 1, type 0x21, length 1, StartGroup 1.
    expect(Array.from(bytes)).toEqual([0x01, 0x23, 0x04, 0x01, 0x21, 0x01, 0x01]);
  });

  it('rejects parameters outside Table 6 inside FILL_PARAMETERS, on both sides', () => {
    // Decode: FILL_PARAMETERS whose nested list carries FORWARD (0x10).
    const writer = new ByteWriter();

    writer.writeVarint(1);
    writer.writeVarint(PARAMETER_TYPE.FILL_PARAMETERS);
    writer.writeVarint(3);
    writer.writeVarint(1); // nested count
    writer.writeVarint(PARAMETER_TYPE.FORWARD);
    writer.writeUint8(1);
    expect(() => decodeMessageParameters(new ByteReader(writer.toBytes()))).toThrow(/FILL_PARAMETERS/);

    // Encode: a range filter type the fill scope excludes (TRACK_PROPERTY_FILTER).
    expect(() =>
      encodeParameters({
        fillParameters: { rangeFilters: [{ type: PARAMETER_TYPE.TRACK_PROPERTY_FILTER, value: new Uint8Array(0) }] },
      })
    ).toThrow(/FILL_PARAMETERS/);
  });
});

describe('encodeLocationFilter', () => {
  it('round-trips every filter shape', () => {
    const filters: LocationFilter[] = [
      { type: 'none' },
      { type: 'next-object' },
      { type: 'relative-group', groupsBeforeNext: 0 },
      { type: 'relative-group', groupsBeforeNext: 1 },
      { type: 'relative-group', groupsBeforeNext: 5 },
      { type: 'absolute', start: { group: 3, object: 9 } },
      { type: 'absolute', start: { group: 3, object: 9 }, endGroupDelta: 0 },
      { type: 'absolute', start: { group: 3, object: 9 }, endGroupDelta: 2, endObject: 7 },
    ];

    for (const filter of filters) {
      expect(decodeLocationFilter(encodeLocationFilter(filter))).toEqual(filter);
    }
  });

  it('selects the meaning by field count (§5.1.2)', () => {
    expect(Array.from(encodeLocationFilter({ type: 'none' }))).toEqual([]);
    expect(Array.from(encodeLocationFilter({ type: 'relative-group', groupsBeforeNext: 1 }))).toEqual([1]);
    expect(Array.from(encodeLocationFilter({ type: 'next-object' }))).toEqual([0, 0]);
    expect(Array.from(encodeLocationFilter({ type: 'absolute', start: { group: 3, object: 9 } }))).toEqual([3, 9]);
    expect(
      Array.from(
        encodeLocationFilter({ type: 'absolute', start: { group: 3, object: 9 }, endGroupDelta: 2, endObject: 7 })
      )
    ).toEqual([3, 9, 2, 7]);
  });

  it('normalizes an open-ended absolute {0,0} to no filter rather than the Next Object spelling', () => {
    expect(Array.from(encodeLocationFilter({ type: 'absolute', start: { group: 0, object: 0 } }))).toEqual([]);
    // Bounded, {0,0} is a real absolute start.
    expect(
      Array.from(encodeLocationFilter({ type: 'absolute', start: { group: 0, object: 0 }, endGroupDelta: 3 }))
    ).toEqual([0, 0, 3]);
  });

  it('rejects an end object without an end group', () => {
    expect(() => encodeLocationFilter({ type: 'absolute', start: { group: 1, object: 0 }, endObject: 4 })).toThrow(
      MoqtProtocolError
    );
  });

  it('rejects more than four fields', () => {
    expect(() => decodeLocationFilter(Uint8Array.of(1, 2, 3, 4, 5))).toThrow(MoqtProtocolError);
  });

  it('rejects an end group past the supported range', () => {
    const writer = new ByteWriter();

    writer.writeVarint(Number.MAX_SAFE_INTEGER);
    writer.writeVarint(0);
    writer.writeVarint(1);
    expect(() => decodeLocationFilter(writer.toBytes())).toThrow(MoqtProtocolError);
  });
});

describe('encodeFetchOk', () => {
  it('round-trips and rejects end-of-track values other than 0 and 1', () => {
    const notEnded = encodeFetchOk(false, { group: 3, object: 9 });
    const ended = encodeFetchOk(true, { group: 3, object: 9 });

    expect(decodeAll(notEnded)[0]).toMatchObject({ kind: 'fetch-ok', endOfTrack: false });
    expect(decodeAll(ended)[0]).toMatchObject({ kind: 'fetch-ok', endOfTrack: true });

    // The end-of-track byte is the single position where the two differ.
    const offset = notEnded.findIndex((byte, index) => byte !== ended[index]);
    const invalid = notEnded.slice();

    invalid[offset] = 2;
    expect(() => decodeAll(invalid)).toThrow(MoqtProtocolError);
  });
});

describe('encodeKeyValuePairs', () => {
  it('delta-encodes types and round-trips even/odd values', () => {
    const pairs = [
      { type: 0x02, value: 7 },
      { type: 0x07, value: utf8Encode('bytes') },
      { type: 0x08, value: 1_000_000 },
    ];
    const writer = new ByteWriter();

    encodeKeyValuePairs(writer, pairs);
    const bytes = writer.toBytes();

    // First delta is the absolute type (0x02), then 5, then 1.
    expect(bytes[0]).toBe(0x02);
    const decoded = decodeKeyValuePairs(new ByteReader(bytes), bytes.length);

    expect(decoded).toEqual(pairs);
  });
});

describe('isRetryableRequestErrorCode', () => {
  it('keeps transient state-shaped failures retryable', () => {
    expect(isRetryableRequestErrorCode(REQUEST_ERROR_CODE.DOES_NOT_EXIST)).toBe(true);
    expect(isRetryableRequestErrorCode(REQUEST_ERROR_CODE.TIMEOUT)).toBe(true);
    expect(isRetryableRequestErrorCode(REQUEST_ERROR_CODE.INTERNAL_ERROR)).toBe(true);
    expect(isRetryableRequestErrorCode(REQUEST_ERROR_CODE.GOING_AWAY)).toBe(true);
    expect(isRetryableRequestErrorCode(REQUEST_ERROR_CODE.EXCESSIVE_LOAD)).toBe(true);
    // EXPIRED_AUTH_TOKEN has its own refresh path; the generic classifier
    // must not preempt it.
    expect(isRetryableRequestErrorCode(REQUEST_ERROR_CODE.EXPIRED_AUTH_TOKEN)).toBe(true);
  });

  it('treats request-shaped rejections as permanent', () => {
    expect(isRetryableRequestErrorCode(REQUEST_ERROR_CODE.UNAUTHORIZED)).toBe(false);
    expect(isRetryableRequestErrorCode(REQUEST_ERROR_CODE.MALFORMED_AUTH_TOKEN)).toBe(false);
    expect(isRetryableRequestErrorCode(REQUEST_ERROR_CODE.NOT_SUPPORTED)).toBe(false);
    expect(isRetryableRequestErrorCode(REQUEST_ERROR_CODE.MALFORMED_TRACK)).toBe(false);
    expect(isRetryableRequestErrorCode(REQUEST_ERROR_CODE.INVALID_FILTER)).toBe(false);
    expect(isRetryableRequestErrorCode(REQUEST_ERROR_CODE.REDIRECT)).toBe(false);
  });

  it('degrades unknown codes to retryable, not dead', () => {
    expect(isRetryableRequestErrorCode(0x7fff)).toBe(true);
  });
});

describe('isRetryablePublishDoneStatus', () => {
  it('keeps publisher/relay life-cycle ends retryable', () => {
    expect(isRetryablePublishDoneStatus(PUBLISH_DONE_STATUS.TRACK_ENDED)).toBe(true);
    expect(isRetryablePublishDoneStatus(PUBLISH_DONE_STATUS.GOING_AWAY)).toBe(true);
    expect(isRetryablePublishDoneStatus(PUBLISH_DONE_STATUS.TOO_FAR_BEHIND)).toBe(true);
    expect(isRetryablePublishDoneStatus(0x7fff)).toBe(true);
  });

  it('treats auth-shaped and malformed-track ends as permanent', () => {
    expect(isRetryablePublishDoneStatus(PUBLISH_DONE_STATUS.UNAUTHORIZED)).toBe(false);
    expect(isRetryablePublishDoneStatus(PUBLISH_DONE_STATUS.EXPIRED)).toBe(false);
    expect(isRetryablePublishDoneStatus(PUBLISH_DONE_STATUS.MALFORMED_TRACK)).toBe(false);
  });
});
