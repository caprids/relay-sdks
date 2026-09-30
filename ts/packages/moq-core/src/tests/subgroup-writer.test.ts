import { describe, expect, it, vi } from 'vitest';

import { StreamReader } from '../protocol/bytes.js';
import { MoqtProtocolError } from '../protocol/errors.js';
import {
  MAX_OBJECT_PAYLOAD_LENGTH,
  MAX_OBJECT_PROPERTIES_LENGTH,
  type MoqtObject,
  readSubgroupHeader,
  readSubgroupObjects,
} from '../protocol/object-stream.js';
import { createSubgroupWriter } from '../protocol/subgroup-writer.js';

// LOC property ids (media/moq/loc.ts) — used numerically so the wire
// round-trip matches what LOC packaging emits without a media import.
const TIMESTAMP = 0x06;
const TIMESCALE = 0x08;
const VIDEO_CONFIG = 13;

function collectingStream() {
  const chunks: Uint8Array[] = [];
  let closed = false;
  let aborted = false;
  let abortReason: unknown;
  const stream = new WritableStream<Uint8Array>({
    write(chunk) {
      chunks.push(chunk);
    },
    close() {
      closed = true;
    },
    abort(reason) {
      aborted = true;
      abortReason = reason;
    },
  });

  return {
    stream,
    chunks,
    isClosed: () => closed,
    isAborted: () => aborted,
    abortReason: () => abortReason,
  };
}

function readableFrom(chunks: Uint8Array[]): ReadableStream<Uint8Array> {
  return new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);

      controller.close();
    },
  });
}

/** Parse a fully written subgroup stream with the existing reader. */
async function parseSubgroup(chunks: Uint8Array[]) {
  const reader = new StreamReader(readableFrom(chunks));
  const type = await reader.readVarint();
  const header = await readSubgroupHeader(reader, type);
  const objects: MoqtObject[] = [];

  for await (const object of readSubgroupObjects(reader, header)) objects.push(object);

  return { header, objects };
}

describe('createSubgroupWriter', () => {
  it('round-trips a multi-object group through the existing reader', async () => {
    const sink = collectingStream();
    const writer = createSubgroupWriter(sink.stream, { trackAlias: 7, groupId: 3 });

    await writer.writeObject({
      objectId: 0,
      properties: [
        { type: TIMESTAMP, value: 1_000_000 },
        { type: TIMESCALE, value: 1_000_000 },
        { type: VIDEO_CONFIG, value: new Uint8Array([1, 2, 3]) },
      ],
      payload: new Uint8Array([0xaa, 0xbb]),
    });
    await writer.writeObject({
      objectId: 1,
      properties: [{ type: TIMESTAMP, value: 1_033_333 }],
      payload: new Uint8Array([0xcc]),
    });
    await writer.fin();

    expect(sink.isClosed()).toBe(true);
    const { header, objects } = await parseSubgroup(sink.chunks);

    expect(header.trackAlias).toBe(7);
    expect(header.groupId).toBe(3);
    expect(header.subgroupIdMode).toBe('zero');
    expect(header.hasProperties).toBe(true);
    expect(header.endOfGroup).toBe(true);
    expect(header.firstObject).toBe(true);
    expect(header.priority).toBeUndefined();

    expect(objects.map((o) => o.objectId)).toEqual([0, 1]);
    expect(objects.map((o) => o.subgroupId)).toEqual([0, 0]);
    expect(objects[0]!.properties).toEqual([
      { type: TIMESTAMP, value: 1_000_000 },
      { type: TIMESCALE, value: 1_000_000 },
      { type: VIDEO_CONFIG, value: new Uint8Array([1, 2, 3]) },
    ]);
    expect(objects[0]!.payload).toEqual(new Uint8Array([0xaa, 0xbb]));
    expect(objects[1]!.properties).toEqual([{ type: TIMESTAMP, value: 1_033_333 }]);
    expect(objects[1]!.payload).toEqual(new Uint8Array([0xcc]));
  });

  it('round-trips object-id gaps via delta encoding', async () => {
    const sink = collectingStream();
    const writer = createSubgroupWriter(sink.stream, { trackAlias: 1, groupId: 0, hasProperties: false });

    await writer.writeObject({ objectId: 0, payload: new Uint8Array([1]) });
    await writer.writeObject({ objectId: 5, payload: new Uint8Array([2]) });
    await writer.writeObject({ objectId: 9, payload: new Uint8Array([3]) });
    await writer.fin();

    const { header, objects } = await parseSubgroup(sink.chunks);

    expect(header.hasProperties).toBe(false);
    expect(objects.map((o) => o.objectId)).toEqual([0, 5, 9]);
    expect(objects.every((o) => o.properties.length === 0)).toBe(true);
  });

  it('carries an explicit publisher priority', async () => {
    const sink = collectingStream();
    const writer = createSubgroupWriter(sink.stream, { trackAlias: 2, groupId: 1, priority: 42 });

    await writer.writeObject({ objectId: 0, properties: [], payload: new Uint8Array([1]) });
    await writer.fin();

    const { header, objects } = await parseSubgroup(sink.chunks);

    expect(header.priority).toBe(42);
    expect(objects[0]!.priority).toBe(42);
  });

  it('round-trips zero-length payloads with an explicit status', async () => {
    const sink = collectingStream();
    const writer = createSubgroupWriter(sink.stream, { trackAlias: 2, groupId: 1 });

    await writer.writeObject({ objectId: 0, properties: [], payload: new Uint8Array([1]) });
    await writer.writeObject({ objectId: 1, properties: [], payload: new Uint8Array(0), status: 'end-of-group' });
    await writer.fin();

    const { objects } = await parseSubgroup(sink.chunks);

    expect(objects[1]!.status).toBe('end-of-group');
    expect(objects[1]!.payload).toEqual(new Uint8Array(0));
  });

  it('accepts a payload exactly at MAX_OBJECT_PAYLOAD_LENGTH and rejects one past it', async () => {
    const sink = collectingStream();
    const writer = createSubgroupWriter(sink.stream, { trackAlias: 1, groupId: 0, hasProperties: false });

    await writer.writeObject({ objectId: 0, payload: new Uint8Array(MAX_OBJECT_PAYLOAD_LENGTH) });
    await expect(
      writer.writeObject({ objectId: 1, payload: new Uint8Array(MAX_OBJECT_PAYLOAD_LENGTH + 1) })
    ).rejects.toThrow(MoqtProtocolError);
    await writer.fin();

    const { objects } = await parseSubgroup(sink.chunks);

    expect(objects).toHaveLength(1);
    expect(objects[0]!.payload.length).toBe(MAX_OBJECT_PAYLOAD_LENGTH);
  });

  it('rejects a properties block past MAX_OBJECT_PROPERTIES_LENGTH', async () => {
    const sink = collectingStream();
    const writer = createSubgroupWriter(sink.stream, { trackAlias: 1, groupId: 0 });

    await expect(
      writer.writeObject({
        objectId: 0,
        properties: [{ type: VIDEO_CONFIG, value: new Uint8Array(MAX_OBJECT_PROPERTIES_LENGTH + 1) }],
        payload: new Uint8Array([1]),
      })
    ).rejects.toThrow(MoqtProtocolError);
  });

  it('rejects non-increasing object ids and properties without the PROPERTIES flag', async () => {
    const sink = collectingStream();
    const writer = createSubgroupWriter(sink.stream, { trackAlias: 1, groupId: 0, hasProperties: false });

    await writer.writeObject({ objectId: 3, payload: new Uint8Array([1]) });
    await expect(writer.writeObject({ objectId: 3, payload: new Uint8Array([2]) })).rejects.toThrow(MoqtProtocolError);
    await expect(
      writer.writeObject({ objectId: 4, properties: [{ type: TIMESTAMP, value: 1 }], payload: new Uint8Array([2]) })
    ).rejects.toThrow(MoqtProtocolError);
  });

  it('rejects writes after fin', async () => {
    const sink = collectingStream();
    const writer = createSubgroupWriter(sink.stream, { trackAlias: 1, groupId: 0 });

    await writer.fin();
    await expect(writer.writeObject({ objectId: 0, properties: [], payload: new Uint8Array([1]) })).rejects.toThrow(
      MoqtProtocolError
    );
  });

  it('abort() resets the stream and the reader surfaces the truncation', async () => {
    const sink = collectingStream();
    const writer = createSubgroupWriter(sink.stream, { trackAlias: 9, groupId: 4 });

    await writer.writeObject({ objectId: 0, properties: [], payload: new Uint8Array([1, 2, 3]) });
    const reason = new Error('dropped under backpressure');

    writer.abort(reason);

    await vi.waitFor(() => {
      expect(sink.isAborted()).toBe(true);
    });
    expect(sink.abortReason()).toBe(reason);
  });

  it('aborts fin while the subgroup header is stalled on reader backpressure', async () => {
    const pipe = new TransformStream<Uint8Array, Uint8Array>();
    const writer = createSubgroupWriter(pipe.writable, { trackAlias: 1, groupId: 0 });
    const finishing = writer.fin();
    const reason = new Error('deadline');
    const rejected = expect(finishing).rejects.toBe(reason);
    await expect(writer.writeObject({ objectId: 0, payload: new Uint8Array([1]) })).rejects.toThrow(/finished/);
    writer.abort(reason);
    const reader = pipe.readable.getReader();
    // A TransformStream releases its pending write once the peer reads.
    await expect(reader.read()).rejects.toBe(reason);
    await rejected;
    await expect(reader.read()).rejects.toBe(reason);
  });

  it('interrupts an in-flight transport close through the controller abort signal', async () => {
    let rejectClose!: (reason: unknown) => void;
    let closeStarted = false;
    const abort = vi.fn();
    // WebTransport uses controller.signal to reject its pending operation. An arbitrary
    // sink that ignores that signal cannot interrupt an already in-flight close.
    const stream = new WritableStream<Uint8Array>({
      start(controller) {
        controller.signal.addEventListener('abort', () => {
          // The WebTransport abort-signal algorithm resets the transport itself,
          // then rejects the close; Streams does not call sink.abort after close fails.
          abort(controller.signal.reason);
          rejectClose(controller.signal.reason);
        });
      },
      close() {
        closeStarted = true;
        return new Promise<void>((_, reject) => {
          rejectClose = reject;
        });
      },
      abort,
    });
    const writer = createSubgroupWriter(stream, { trackAlias: 1, groupId: 0 });
    await writer.writeObject({ objectId: 0, payload: new Uint8Array([1]) });
    const finishing = writer.fin();
    expect(writer.fin()).toBe(finishing);
    await vi.waitFor(() => expect(closeStarted).toBe(true));
    const reason = new Error('deadline while closing');
    const rejected = expect(finishing).rejects.toBe(reason);
    writer.abort(reason);
    await rejected;
    await vi.waitFor(() => expect(abort).toHaveBeenCalledWith(reason));
  });

  describe('handing the FIN over', () => {
    /** A send stream whose close settles only when the test says, as Chromium's waits for the peer's ack. */
    function acknowledgingStream() {
      let closeStarted = false;
      let settle!: { resolve(): void; reject(reason: unknown): void };
      const abort = vi.fn();
      const stream = new WritableStream<Uint8Array>({
        start(controller) {
          controller.signal.addEventListener('abort', () => {
            abort(controller.signal.reason);
            settle?.reject(controller.signal.reason);
          });
        },
        close() {
          closeStarted = true;
          return new Promise<void>((resolve, reject) => {
            settle = { resolve, reject };
          });
        },
        abort,
      });

      return { stream, abort, closeStarted: () => closeStarted, settle: () => settle };
    }

    it('resolves once the close call is made, leaving fin() to report the acknowledgment', async () => {
      const sink = acknowledgingStream();
      const writer = createSubgroupWriter(sink.stream, { trackAlias: 1, groupId: 0 });
      await writer.writeObject({ objectId: 0, payload: new Uint8Array([1]) });
      const handedOver = writer.handOverFin();
      expect(writer.handOverFin()).toBe(handedOver);
      await handedOver;
      let acknowledged = false;
      const settled = writer.fin().then(() => {
        acknowledged = true;
      });
      await vi.waitFor(() => expect(sink.closeStarted()).toBe(true));
      expect(acknowledged).toBe(false);
      await expect(writer.writeObject({ objectId: 1, payload: new Uint8Array([2]) })).rejects.toThrow(/finished/);
      sink.settle().resolve();
      await settled;
      expect(acknowledged).toBe(true);
    });

    it('pre-handles a late failure nobody asked about, and fin() reports it', async () => {
      const sink = acknowledgingStream();
      const writer = createSubgroupWriter(sink.stream, { trackAlias: 1, groupId: 0 });
      await writer.handOverFin();
      await vi.waitFor(() => expect(sink.closeStarted()).toBe(true));
      const failure = new Error('STOP_SENDING');
      sink.settle().reject(failure);
      // An unhandled rejection would fail the run; asking afterwards still sees the failure.
      await new Promise((resolve) => setTimeout(resolve, 0));
      await expect(writer.fin()).rejects.toBe(failure);
    });

    it('still resets a stream whose handed-over FIN is not yet acknowledged', async () => {
      const sink = acknowledgingStream();
      const writer = createSubgroupWriter(sink.stream, { trackAlias: 1, groupId: 0 });
      await writer.handOverFin();
      await vi.waitFor(() => expect(sink.closeStarted()).toBe(true));
      const reason = new Error('reset after hand-over');
      writer.abort(reason);
      await expect(writer.fin()).rejects.toBe(reason);
      expect(sink.abort).toHaveBeenCalledWith(reason);
    });
  });
});
