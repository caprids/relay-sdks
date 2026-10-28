/**
 * The `@caprid/moq-core/wire` entry point (#367): the codec a test relay drives a session with, byte for byte.
 */
import { describe, expect, it } from 'vitest';
import type { MoqtObject } from '../protocol/object-stream.js';
import { createSubgroupWriter } from '../protocol/subgroup-writer.js';
import {
  type ControlMessage,
  ControlMessageDeframer,
  decodeControlMessage,
  decodeVarint,
  encodePublishDone,
  encodeSubscribe,
  encodeSubscribeOk,
  encodeVarint,
  isSubgroupHeaderType,
  readSubgroupHeader,
  readSubgroupObjects,
  STREAM_TYPE,
  StreamReader,
  utf8Encode,
} from '../wire.js';

function decodeAll(bytes: Uint8Array): ControlMessage[] {
  return new ControlMessageDeframer().push(bytes).map((frame) => decodeControlMessage(frame));
}

describe('@caprid/moq-core/wire', () => {
  it('round-trips control messages through the deframer, split anywhere', () => {
    const subscribe = encodeSubscribe({ requestId: 1, trackNamespace: ['live'], trackName: 'video' });
    const ok = encodeSubscribeOk(1);
    const done = encodePublishDone(0, 2, 'track ended');
    const bytes = new Uint8Array([...subscribe, ...ok, ...done]);
    const deframer = new ControlMessageDeframer();
    const frames = [...deframer.push(bytes.subarray(0, 3)), ...deframer.push(bytes.subarray(3))];

    expect(frames.map((frame) => decodeControlMessage(frame))).toEqual([
      ...decodeAll(subscribe),
      ...decodeAll(ok),
      ...decodeAll(done),
    ]);
    expect(frames.map((frame) => decodeControlMessage(frame).kind)).toEqual([
      'subscribe',
      'subscribe-ok',
      'publish-done',
    ]);
    expect(decodeAll(subscribe)[0]).toMatchObject({ requestId: 1, trackNamespace: ['live'], trackName: 'video' });
  });

  it('encodes and decodes varints', () => {
    for (const value of [0, 63, 64, 16_383, 16_384, 2 ** 41]) {
      const bytes = encodeVarint(value);

      expect(decodeVarint(bytes)).toEqual({ value, byteLength: bytes.length });
    }
  });

  it('reads a subgroup stream a publisher wrote', async () => {
    const pipe = new TransformStream<Uint8Array, Uint8Array>();
    const group = createSubgroupWriter(pipe.writable, { trackAlias: 3, groupId: 7 });
    const written = (async () => {
      await group.writeObject({ objectId: 0, payload: utf8Encode('frame') });
      await group.fin();
    })();
    const reader = new StreamReader(pipe.readable);
    const type = await reader.readVarint();

    expect(isSubgroupHeaderType(type)).toBe(true);
    expect(type).not.toBe(STREAM_TYPE.FETCH_HEADER);
    const header = await readSubgroupHeader(reader, type);

    expect(header).toMatchObject({ trackAlias: 3, groupId: 7 });
    const objects: MoqtObject[] = [];

    for await (const object of readSubgroupObjects(reader, header)) objects.push(object);
    await written;
    expect(objects.map((object) => [object.groupId, object.objectId, [...object.payload]])).toEqual([
      [7, 0, [...utf8Encode('frame')]],
    ]);
  });
});
