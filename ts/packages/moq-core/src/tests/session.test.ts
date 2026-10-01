import { describe, expect, it, vi } from 'vitest';

import { ByteWriter, StreamReader, utf8Encode } from '../protocol/bytes.js';
import {
  type ControlMessage,
  ControlMessageDeframer,
  decodeControlMessage,
  encodePublish,
  encodePublishDone,
  encodePublishNamespace,
  encodePublishStateNotify,
  encodeRequestError,
  encodeRequestOk,
  encodeSetup,
  encodeSubscribeOk,
  type LocationFilter,
  PUBLISH_DONE_STATUS,
  REQUEST_ERROR_CODE,
} from '../protocol/control-messages.js';
import { SESSION_ERROR } from '../protocol/errors.js';
import type { MoqtObject } from '../protocol/object-stream.js';
import type { BidirectionalStreamLike } from '../protocol/request-stream.js';
import {
  createMoqtSession,
  isStreamFailure,
  type MoqtSessionCallbacks,
  type MoqtTransport,
} from '../protocol/session.js';

// ============================================================================
// In-memory transport fake — the same seam pattern as SPF's fetch injection.
// ============================================================================

interface PushChannel<T> {
  stream: ReadableStream<T>;
  push(value: T): void;
  close(): void;
}

function pushChannel<T>(): PushChannel<T> {
  let controller!: ReadableStreamDefaultController<T>;
  const stream = new ReadableStream<T>({
    start(c) {
      controller = c;
    },
  });

  return {
    stream,
    push: (value) => controller.enqueue(value),
    close: () => controller.close(),
  };
}

/** Server-side view of one client-initiated bidirectional stream. */
interface ServerRequestStream {
  /** Decoded control messages received from the client, in order. */
  messages: ControlMessage[];
  /** Resolves when the first client message has been decoded. */
  firstMessage: Promise<ControlMessage>;
  /** Resolves when the client's direction ends: `fin` for a clean FIN, `reset` for a reset. */
  clientEnd: Promise<'fin' | 'reset'>;
  send(bytes: Uint8Array): Promise<void>;
  fin(): Promise<void>;
  reset(reason?: unknown): void;
}

function createFakeTransport() {
  const incomingUni = pushChannel<ReadableStream<Uint8Array>>();
  const incomingBidi = pushChannel<BidirectionalStreamLike>();
  const clientUniStreams: ReadableStream<Uint8Array>[] = [];
  const requestStreams: ServerRequestStream[] = [];
  const requestWaiters: ((stream: ServerRequestStream) => void)[] = [];
  let closeInfo: { closeCode?: number; reason?: string } | undefined;
  let resolveClosed!: (info: unknown) => void;
  const closed = new Promise<unknown>((resolve) => {
    resolveClosed = resolve;
  });

  const transport: MoqtTransport = {
    incomingUnidirectionalStreams: incomingUni.stream,
    incomingBidirectionalStreams: incomingBidi.stream,
    async createUnidirectionalStream() {
      const pipe = new TransformStream<Uint8Array, Uint8Array>();

      clientUniStreams.push(pipe.readable);
      return pipe.writable;
    },
    async createBidirectionalStream() {
      const clientToServer = new TransformStream<Uint8Array, Uint8Array>();
      const serverToClient = new TransformStream<Uint8Array, Uint8Array>();
      const writer = serverToClient.writable.getWriter();

      const messages: ControlMessage[] = [];
      let resolveFirst!: (message: ControlMessage) => void;
      const firstMessage = new Promise<ControlMessage>((resolve) => {
        resolveFirst = resolve;
      });
      let resolveClientEnd!: (end: 'fin' | 'reset') => void;
      const clientEnd = new Promise<'fin' | 'reset'>((resolve) => {
        resolveClientEnd = resolve;
      });

      // Server-side read pump for the client's messages.
      void (async () => {
        const reader = clientToServer.readable.getReader();
        const deframer = new ControlMessageDeframer();

        try {
          while (true) {
            const { done, value } = await reader.read();
            if (done) {
              resolveClientEnd('fin');
              break;
            }

            for (const frame of deframer.push(value)) {
              const message = decodeControlMessage(frame);

              messages.push(message);

              if (messages.length === 1) resolveFirst(message);
            }
          }
        } catch {
          // client reset its sending direction
          resolveClientEnd('reset');
        }
      })();

      const serverStream: ServerRequestStream = {
        messages,
        firstMessage,
        clientEnd,
        send: (bytes) => writer.write(bytes),
        fin: () => writer.close(),
        reset: (reason) => {
          writer.abort(reason).catch(() => {});
        },
      };

      requestStreams.push(serverStream);
      requestWaiters.shift()?.(serverStream);
      return { readable: serverToClient.readable, writable: clientToServer.writable };
    },
    close(info) {
      closeInfo = info;
      resolveClosed(info);
    },
    closed,
  };

  return {
    transport,
    /** The SETUP control stream the client opened (first client uni stream). */
    async clientControlStream(): Promise<StreamReader> {
      await vi.waitFor(() => {
        if (clientUniStreams.length === 0) throw new Error('no client uni stream yet');
      });
      return new StreamReader(clientUniStreams[0]!);
    },
    /** Wait for the next client-initiated request stream. */
    nextRequestStream(): Promise<ServerRequestStream> {
      const existing = requestStreams.shift();
      if (existing) return Promise.resolve(existing);

      return new Promise((resolve) => requestWaiters.push(resolve));
    },
    /** Open the server's control stream and send its SETUP. */
    sendServerSetup(): void {
      const pipe = new TransformStream<Uint8Array, Uint8Array>();
      const writer = pipe.writable.getWriter();

      void writer.write(encodeSetup([]));
      incomingUni.push(pipe.readable);
    },
    /** Open a unidirectional data stream to the client. */
    openDataStream(bytes: Uint8Array): void {
      const pipe = new TransformStream<Uint8Array, Uint8Array>();
      const writer = pipe.writable.getWriter();

      void writer.write(bytes).then(() => writer.close());
      incomingUni.push(pipe.readable);
    },
    /** Open a unidirectional data stream the test writes to (and may reset). */
    openControlledDataStream(): { write(bytes: Uint8Array): Promise<void>; reset(error: unknown): void } {
      const pipe = new TransformStream<Uint8Array, Uint8Array>();
      const writer = pipe.writable.getWriter();

      incomingUni.push(pipe.readable);
      return {
        write: (bytes) => writer.write(bytes),
        reset: (error) => {
          writer.abort(error).catch(() => {});
        },
      };
    },
    openIncomingBidi(bytes: Uint8Array): { responses: Promise<Uint8Array[]> } {
      const serverToClient = new TransformStream<Uint8Array, Uint8Array>();
      const clientToServer = new TransformStream<Uint8Array, Uint8Array>();
      const writer = serverToClient.writable.getWriter();

      void writer.write(bytes).then(() => writer.close());
      incomingBidi.push({ readable: serverToClient.readable, writable: clientToServer.writable });
      const responses = (async () => {
        const chunks: Uint8Array[] = [];
        const reader = clientToServer.readable.getReader();

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;

          chunks.push(value);
        }

        return chunks;
      })();

      return { responses };
    },
    clientUniStreamCount: () => clientUniStreams.length,
    /** A client-opened unidirectional stream, read raw (the control stream is index 0). */
    clientUniStream: (index: number) => clientUniStreams[index]!,
    getCloseInfo: () => closeInfo,
    /** Simulate a transport-side drop (peer/network close, not a local `close()`). */
    dropTransport: () => resolveClosed(undefined),
  };
}

/** Encode a one-object subgroup stream for `trackAlias` (type 0x38: subgroup 0, default priority, end-of-group). */
function encodeSubgroup(trackAlias: number, groupId: number, payload: Uint8Array, objectId = 0): Uint8Array {
  const writer = new ByteWriter();

  writer.writeVarint(0x38);
  writer.writeVarint(trackAlias);
  writer.writeVarint(groupId);
  writer.writeVarint(objectId); // first object id (absolute)
  writer.writeVarint(payload.length);
  writer.writeBytes(payload);
  return writer.toBytes();
}

/** The fetch range playback would ask for if it fetched: the current group from its start. */
const CURRENT_GROUP: LocationFilter = { type: 'relative-group', groupsBeforeNext: 1 };

function createSessionHarness(callbacks?: MoqtSessionCallbacks) {
  const fake = createFakeTransport();
  const session = createMoqtSession(fake.transport, { callbacks, unknownAliasTimeoutMs: 200 });

  return { ...fake, session };
}

describe('createMoqtSession', () => {
  it('sends SETUP on a fresh unidirectional control stream', async () => {
    const harness = createSessionHarness();
    const control = await harness.clientControlStream();
    const type = await control.readVarint();

    expect(type).toBe(0x2f00);
    const high = await control.readUint8();
    const low = await control.readUint8();
    const body = await control.readBytes(high * 256 + low);
    const setup = decodeControlMessage({ type, body });

    expect(setup.kind).toBe('setup');
    harness.session.destroy();
  });

  it('resolves ready and reports server options once the server SETUP arrives', async () => {
    const onReady = vi.fn();
    const harness = createSessionHarness({ onReady });

    harness.sendServerSetup();
    await harness.session.ready;
    expect(onReady).toHaveBeenCalledWith([]);
    harness.session.destroy();
  });

  it('subscribes on a new request stream and routes objects by track alias', async () => {
    const harness = createSessionHarness();

    harness.sendServerSetup();

    const onOk = vi.fn();
    const objects: MoqtObject[] = [];
    const subgroupEnds: unknown[] = [];

    harness.session.subscribe(
      { trackNamespace: ['live', 'stream1'], trackName: 'video' },
      {
        onOk,
        onObject: (object) => objects.push(object),
        onSubgroupEnd: (info) => subgroupEnds.push(info),
      }
    );

    const request = await harness.nextRequestStream();
    const message = await request.firstMessage;

    expect(message).toMatchObject({
      kind: 'subscribe',
      requestId: 0,
      trackNamespace: ['live', 'stream1'],
      trackName: 'video',
    });

    await request.send(encodeSubscribeOk(11));
    await vi.waitFor(() => expect(onOk).toHaveBeenCalled());
    expect(onOk).toHaveBeenCalledWith(expect.objectContaining({ trackAlias: 11 }));

    harness.openDataStream(encodeSubgroup(11, 41, utf8Encode('frame')));
    await vi.waitFor(() => expect(objects).toHaveLength(1));
    expect(objects[0]).toMatchObject({ groupId: 41, objectId: 0, subgroupId: 0, status: 'normal' });
    await vi.waitFor(() => expect(subgroupEnds).toHaveLength(1));
    expect(subgroupEnds[0]).toEqual({ groupId: 41, subgroupId: 0, endOfGroup: true });

    harness.session.destroy();
  });

  it('reports a peer reset with a shape-only stream error as onSubgroupReset (TA-19, TA-22)', async () => {
    const harness = createSessionHarness();

    harness.sendServerSetup();

    const onOk = vi.fn();
    const onError = vi.fn();
    const objects: MoqtObject[] = [];
    const subgroupEnds: unknown[] = [];
    const resets: unknown[] = [];

    harness.session.subscribe(
      { trackNamespace: ['live'], trackName: 'video' },
      {
        onOk,
        onError,
        onObject: (object) => objects.push(object),
        onSubgroupEnd: (info) => subgroupEnds.push(info),
        onSubgroupReset: (info) => resets.push(info),
      }
    );
    const request = await harness.nextRequestStream();

    await request.firstMessage;
    await request.send(encodeSubscribeOk(11));
    await vi.waitFor(() => expect(onOk).toHaveBeenCalled());

    const dataStream = harness.openControlledDataStream();

    await dataStream.write(encodeSubgroup(11, 41, utf8Encode('frame')));
    await vi.waitFor(() => expect(objects).toHaveLength(1));
    // A non-browser adapter's RESET_STREAM: the TA-22 shape, with no WebTransportError name.
    const reset = Object.assign(new Error('peer reset the stream'), { source: 'stream', streamErrorCode: 2 });
    dataStream.reset(reset);

    await vi.waitFor(() => expect(resets).toEqual([{ groupId: 41, error: reset }]));
    expect(subgroupEnds).toEqual([]);
    expect(onError).not.toHaveBeenCalled();

    harness.session.destroy();
  });

  it('buffers a data stream that arrives before SUBSCRIBE_OK registers the alias', async () => {
    const harness = createSessionHarness();

    harness.sendServerSetup();

    const objects: MoqtObject[] = [];

    harness.session.subscribe(
      { trackNamespace: ['live'], trackName: 'audio' },
      { onObject: (object) => objects.push(object) }
    );
    const request = await harness.nextRequestStream();

    await request.firstMessage;

    // Objects outrace the SUBSCRIBE_OK.
    harness.openDataStream(encodeSubgroup(5, 100, utf8Encode('early')));
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(objects).toHaveLength(0);

    await request.send(encodeSubscribeOk(5));
    await vi.waitFor(() => expect(objects).toHaveLength(1));
    expect(objects[0]).toMatchObject({ groupId: 100 });

    harness.session.destroy();
  });

  it('drops a data stream whose alias never registers (timeout)', async () => {
    const harness = createSessionHarness();

    harness.sendServerSetup();
    // No subscription at all — stream must be cancelled after the timeout,
    // and the session must survive.
    harness.openDataStream(encodeSubgroup(99, 1, utf8Encode('orphan')));
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(harness.getCloseInfo()).toBeUndefined();
    harness.session.destroy();
  });

  it('surfaces REQUEST_ERROR to the subscription handlers', async () => {
    const harness = createSessionHarness();

    harness.sendServerSetup();

    const onError = vi.fn();

    harness.session.subscribe({ trackNamespace: ['live'], trackName: 'nope' }, { onError });
    const request = await harness.nextRequestStream();

    await request.firstMessage;
    await request.send(encodeRequestError(REQUEST_ERROR_CODE.DOES_NOT_EXIST, 'no such track'));

    await vi.waitFor(() => expect(onError).toHaveBeenCalled());
    expect(onError).toHaveBeenCalledWith(
      expect.objectContaining({ errorCode: REQUEST_ERROR_CODE.DOES_NOT_EXIST, reason: 'no such track' })
    );
    expect(isStreamFailure(onError.mock.calls[0]![0])).toBe(false);
    harness.session.destroy();
  });

  // §3.3.2 leaves "was this FIN premature?" to the caller; the session is the
  // only layer that knows a SUBSCRIBE needs a SUBSCRIBE_OK. Without these the
  // subscription sits pending forever with neither response nor error.
  it('fails a subscription when the peer FINs before SUBSCRIBE_OK', async () => {
    const harness = createSessionHarness();

    harness.sendServerSetup();

    const onError = vi.fn();

    harness.session.subscribe({ trackNamespace: ['live'], trackName: 'video' }, { onError });
    const request = await harness.nextRequestStream();

    await request.firstMessage;
    await request.fin();

    await vi.waitFor(() => expect(onError).toHaveBeenCalled());
    expect(onError).toHaveBeenCalledWith(
      expect.objectContaining({ reason: 'request stream closed before a response' })
    );
    harness.session.destroy();
  });

  it('fails a subscription when the request stream is reset before SUBSCRIBE_OK', async () => {
    const harness = createSessionHarness();

    harness.sendServerSetup();

    const onError = vi.fn();

    harness.session.subscribe({ trackNamespace: ['live'], trackName: 'video' }, { onError });
    const request = await harness.nextRequestStream();

    await request.firstMessage;
    request.reset(new Error('stream reset'));

    await vi.waitFor(() => expect(onError).toHaveBeenCalled());
    harness.session.destroy();
  });

  it('does not fail a subscription when the FIN follows SUBSCRIBE_OK', async () => {
    const harness = createSessionHarness();

    harness.sendServerSetup();

    const onOk = vi.fn();
    const onError = vi.fn();

    harness.session.subscribe({ trackNamespace: ['live'], trackName: 'video' }, { onOk, onError });
    const request = await harness.nextRequestStream();

    await request.firstMessage;
    await request.send(encodeSubscribeOk(7));
    await vi.waitFor(() => expect(onOk).toHaveBeenCalled());
    await request.fin();

    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(onError).not.toHaveBeenCalled();
    harness.session.destroy();
  });

  it('fails a subscription that gets no response before the request timeout', async () => {
    const fake = createFakeTransport();
    const session = createMoqtSession(fake.transport, { requestTimeoutMs: 10 });

    fake.sendServerSetup();

    const onError = vi.fn();

    session.subscribe({ trackNamespace: ['live'], trackName: 'video' }, { onError });
    const request = await fake.nextRequestStream();

    await request.firstMessage;

    await vi.waitFor(() => expect(onError).toHaveBeenCalled());
    expect(onError).toHaveBeenCalledWith(expect.objectContaining({ errorCode: REQUEST_ERROR_CODE.TIMEOUT }));
    expect(isStreamFailure(onError.mock.calls[0]![0])).toBe(true);
    session.destroy();
  });

  it('ignores a response that arrives after the request timeout', async () => {
    const fake = createFakeTransport();
    const session = createMoqtSession(fake.transport, { requestTimeoutMs: 10, unknownAliasTimeoutMs: 50 });

    fake.sendServerSetup();

    const onOk = vi.fn();
    const onError = vi.fn();
    const objects: MoqtObject[] = [];

    session.subscribe(
      { trackNamespace: ['live'], trackName: 'video' },
      { onOk, onError, onObject: (object) => objects.push(object) }
    );
    const request = await fake.nextRequestStream();

    await request.firstMessage;
    await vi.waitFor(() => expect(onError).toHaveBeenCalled());

    // A SUBSCRIBE_OK after the timeout must not fire onOk after onError,
    // and must not resurrect the alias route of the dead subscription.
    await request.send(encodeSubscribeOk(11)).catch(() => {});
    fake.openDataStream(encodeSubgroup(11, 41, utf8Encode('late')));
    await new Promise((resolve) => setTimeout(resolve, 80));
    expect(onOk).not.toHaveBeenCalled();
    expect(objects).toHaveLength(0);
    session.destroy();
  });

  it('fails a pending trackStatus when the transport drops', async () => {
    const harness = createSessionHarness();

    harness.sendServerSetup();

    const onError = vi.fn();

    harness.session.trackStatus({ trackNamespace: ['live'], trackName: 'video' }, { onError });
    await (await harness.nextRequestStream()).firstMessage;
    harness.dropTransport();

    await vi.waitFor(() => expect(onError).toHaveBeenCalled());
    expect(onError).toHaveBeenCalledWith(
      expect.objectContaining({ reason: 'session closed before the request was answered' })
    );
  });

  it('fails pending requests when the transport drops, but not on a local close()', async () => {
    const dropped = createSessionHarness();

    dropped.sendServerSetup();
    const onDropError = vi.fn();

    dropped.session.subscribe({ trackNamespace: ['live'], trackName: 'video' }, { onError: onDropError });
    await (await dropped.nextRequestStream()).firstMessage;
    dropped.dropTransport();
    await vi.waitFor(() => expect(onDropError).toHaveBeenCalled());

    const closed = createSessionHarness();

    closed.sendServerSetup();
    const onCloseError = vi.fn();

    closed.session.subscribe({ trackNamespace: ['live'], trackName: 'video' }, { onError: onCloseError });
    await (await closed.nextRequestStream()).firstMessage;
    closed.session.close();
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(onCloseError).not.toHaveBeenCalled();
  });

  it('surfaces PUBLISH_DONE and keeps late subgroups flowing until cancel', async () => {
    const harness = createSessionHarness();

    harness.sendServerSetup();

    const onDone = vi.fn();
    const objects: MoqtObject[] = [];
    const subscription = harness.session.subscribe(
      { trackNamespace: ['live'], trackName: 'video' },
      { onDone, onObject: (object) => objects.push(object) }
    );
    const request = await harness.nextRequestStream();

    await request.firstMessage;
    await request.send(encodeSubscribeOk(7));
    await request.send(encodePublishDone(PUBLISH_DONE_STATUS.TRACK_ENDED, 1, 'ended'));

    await vi.waitFor(() => expect(onDone).toHaveBeenCalled());
    expect(onDone).toHaveBeenCalledWith(expect.objectContaining({ statusCode: PUBLISH_DONE_STATUS.TRACK_ENDED }));

    // A late-opening stream still routes (PUBLISH_DONE often beats data).
    harness.openDataStream(encodeSubgroup(7, 1, utf8Encode('late')));
    await vi.waitFor(() => expect(objects).toHaveLength(1));

    subscription.cancel();
    harness.session.destroy();
  });

  it("FINs its request stream once the publisher's FIN follows PUBLISH_DONE", async () => {
    const harness = createSessionHarness();

    harness.sendServerSetup();

    const objects: MoqtObject[] = [];
    const subscription = harness.session.subscribe(
      { trackNamespace: ['live'], trackName: 'video' },
      { onObject: (object) => objects.push(object) }
    );
    const request = await harness.nextRequestStream();
    let clientEnd: 'fin' | 'reset' | undefined;

    void request.clientEnd.then((end) => {
      clientEnd = end;
    });
    await request.firstMessage;
    await request.send(encodeSubscribeOk(7));
    await request.send(encodePublishDone(PUBLISH_DONE_STATUS.TRACK_ENDED, 0, 'ended'));
    await new Promise((resolve) => setTimeout(resolve, 20));

    // PUBLISH_DONE alone does not complete the request (§3.3.2): the publisher's FIN does.
    expect(clientEnd).toBeUndefined();

    await request.fin();
    await vi.waitFor(() => expect(clientEnd).toBe('fin'));

    // Late data still routes, and an update() after the FIN writes nothing.
    harness.openDataStream(encodeSubgroup(7, 1, utf8Encode('late')));
    await vi.waitFor(() => expect(objects).toHaveLength(1));
    subscription.update({ forward: 0 });
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(request.messages).toHaveLength(1);

    subscription.cancel();
    harness.session.destroy();
  });

  it('does not FIN back for a publisher FIN without PUBLISH_DONE', async () => {
    const harness = createSessionHarness();

    harness.sendServerSetup();
    harness.session.subscribe({ trackNamespace: ['live'], trackName: 'video' });

    const request = await harness.nextRequestStream();
    let clientEnd: 'fin' | 'reset' | undefined;

    void request.clientEnd.then((end) => {
      clientEnd = end;
    });
    await request.firstMessage;
    await request.send(encodeSubscribeOk(7));
    await request.fin();
    await new Promise((resolve) => setTimeout(resolve, 20));

    expect(clientEnd).toBeUndefined();

    harness.session.destroy();
  });

  it('allocates even request IDs starting at 0', async () => {
    const harness = createSessionHarness();

    harness.sendServerSetup();
    const first = harness.session.subscribe({ trackNamespace: ['a'], trackName: 'x' });
    const second = harness.session.subscribe({ trackNamespace: ['a'], trackName: 'y' });

    expect(first.requestId).toBe(0);
    expect(second.requestId).toBe(2);
    harness.session.destroy();
  });

  it('routes fetch streams by request id', async () => {
    const harness = createSessionHarness();

    harness.sendServerSetup();

    harness.session.subscribe({ trackNamespace: ['live'], trackName: 'catalog' });
    const subscribeStream = await harness.nextRequestStream();

    await subscribeStream.firstMessage;

    const entries: unknown[] = [];
    const onEnd = vi.fn();

    harness.session.fetch(
      { trackNamespace: ['live'], trackName: 'catalog', parameters: { locationFilter: CURRENT_GROUP } },
      { onEntry: (entry) => entries.push(entry), onEnd }
    );
    const fetchStream = await harness.nextRequestStream();
    const fetchMessage = await fetchStream.firstMessage;

    expect(fetchMessage).toMatchObject({
      kind: 'fetch',
      request: {
        requestId: 2,
        trackNamespace: ['live'],
        trackName: 'catalog',
        parameters: { locationFilter: CURRENT_GROUP },
      },
    });

    // Fetch data stream: FETCH_HEADER + one object.
    const writer = new ByteWriter();

    writer.writeVarint(0x05);
    writer.writeVarint(2); // fetch request id
    writer.writeVarint(0x1c); // group delta + object delta + priority present
    writer.writeVarint(41); // group (absolute)
    writer.writeVarint(0); // object (absolute)
    writer.writeUint8(128);
    const payload = utf8Encode('{"version":"1"}');

    writer.writeVarint(payload.length);
    writer.writeBytes(payload);
    harness.openDataStream(writer.toBytes());

    await vi.waitFor(() => expect(entries).toHaveLength(1));
    expect(entries[0]).toMatchObject({ kind: 'object', groupId: 41, objectId: 0 });
    await vi.waitFor(() => expect(onEnd).toHaveBeenCalled());

    harness.session.destroy();
  });

  it('tolerates REQUEST_OK in place of FETCH_OK on a fetch stream (relay deviation)', async () => {
    const onClosed = vi.fn();
    const harness = createSessionHarness({ onClosed });

    harness.sendServerSetup();

    harness.session.subscribe({ trackNamespace: ['live'], trackName: 'catalog' });

    await harness.nextRequestStream();

    const entries: unknown[] = [];
    const onEnd = vi.fn();

    harness.session.fetch(
      { trackNamespace: ['live'], trackName: 'catalog', parameters: { locationFilter: CURRENT_GROUP } },
      { onEntry: (entry) => entries.push(entry), onEnd }
    );
    const fetchStream = await harness.nextRequestStream();

    await fetchStream.firstMessage;
    await fetchStream.send(encodeRequestOk());

    // The data stream still delivers normally — REQUEST_OK on the bidi
    // stream has no bearing on the separate unidirectional data stream.
    const writer = new ByteWriter();

    writer.writeVarint(0x05);
    writer.writeVarint(2); // fetch request id
    writer.writeVarint(0x1c); // group delta + object delta + priority present
    writer.writeVarint(41); // group (absolute)
    writer.writeVarint(0); // object (absolute)
    writer.writeUint8(128);
    const payload = utf8Encode('{"version":"1"}');

    writer.writeVarint(payload.length);
    writer.writeBytes(payload);
    harness.openDataStream(writer.toBytes());

    await vi.waitFor(() => expect(entries).toHaveLength(1));
    await vi.waitFor(() => expect(onEnd).toHaveBeenCalled());
    expect(onClosed).not.toHaveBeenCalled();

    harness.session.destroy();
  });

  // REQUEST_OK *is* the response on such a relay, so it has to disarm the
  // response timer. If it doesn't, the deadline fires on an answered fetch,
  // deletes the record, and `#runFetchStream` then cancels the data stream
  // instead of delivering it — the replay vanishes with no protocol error.
  it('settles a fetch answered with REQUEST_OK so the request timeout cannot fail it', async () => {
    const fake = createFakeTransport();
    // 25ms: below the 30ms wait (an un-disarmed timer would still fire in
    // time to be observed) with headroom for the REQUEST_OK on slow CI.
    const session = createMoqtSession(fake.transport, { requestTimeoutMs: 25, unknownAliasTimeoutMs: 200 });

    fake.sendServerSetup();

    session.subscribe({ trackNamespace: ['live'], trackName: 'catalog' });

    await fake.nextRequestStream();

    const onError = vi.fn();
    const entries: unknown[] = [];

    session.fetch(
      { trackNamespace: ['live'], trackName: 'catalog', parameters: { locationFilter: CURRENT_GROUP } },
      { onEntry: (entry) => entries.push(entry), onError }
    );
    const fetchStream = await fake.nextRequestStream();

    await fetchStream.firstMessage;
    await fetchStream.send(encodeRequestOk());

    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(onError).not.toHaveBeenCalled();

    // The data stream arrives after the deadline would have passed.
    const writer = new ByteWriter();

    writer.writeVarint(0x05);
    writer.writeVarint(2); // fetch request id
    writer.writeVarint(0x1c); // group delta + object delta + priority present
    writer.writeVarint(41); // group (absolute)
    writer.writeVarint(0); // object (absolute)
    writer.writeUint8(128);
    const payload = utf8Encode('{"version":"1"}');

    writer.writeVarint(payload.length);
    writer.writeBytes(payload);
    fake.openDataStream(writer.toBytes());

    await vi.waitFor(() => expect(entries).toHaveLength(1));
    session.destroy();
  });

  it('rejects ready when the session closes before the server SETUP', async () => {
    const harness = createSessionHarness();

    harness.session.close();
    await expect(harness.session.ready).rejects.toThrow('session closed before server SETUP');
  });

  it('reports a transport drop before the server SETUP as an error close', async () => {
    const onClosed = vi.fn();
    const harness = createSessionHarness({ onClosed });

    harness.dropTransport();

    // Callback-only consumers must observe the failure too — a clean
    // `onClosed({})` would read as a normal session close.
    await vi.waitFor(() => expect(onClosed).toHaveBeenCalled());
    expect(onClosed).toHaveBeenCalledWith({
      error: expect.objectContaining({ message: 'session closed before server SETUP' }),
    });
    await expect(harness.session.ready).rejects.toThrow('session closed before server SETUP');
  });

  it('writes nothing on the control stream after SETUP when closed: no GOAWAY', async () => {
    // A GOAWAY sent as the transport closes gives the peer nothing to act on, so close() sends none;
    // the fake keeps every written byte.
    const harness = createSessionHarness();
    const frames: ControlMessage[] = [];

    void (async () => {
      await vi.waitFor(() => {
        if (harness.clientUniStreamCount() === 0) throw new Error('no client uni stream yet');
      });
      const reader = harness.clientUniStream(0).getReader();
      const deframer = new ControlMessageDeframer();

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        for (const frame of deframer.push(value)) frames.push(decodeControlMessage(frame));
      }
    })();

    harness.sendServerSetup();
    await harness.session.ready;
    await vi.waitFor(() => expect(frames).toHaveLength(1));

    harness.session.close();
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(frames.map((frame) => frame.kind)).toEqual(['setup']);
    expect(harness.getCloseInfo()).toEqual({ closeCode: 0, reason: '' });
  });

  it('reports a local close() before the server SETUP as a clean close', async () => {
    const onClosed = vi.fn();
    const harness = createSessionHarness({ onClosed });

    harness.session.close();

    await vi.waitFor(() => expect(onClosed).toHaveBeenCalled());
    expect(onClosed).toHaveBeenCalledWith({});
  });

  it('reports a reset fetch stream as onReset, not a clean onEnd', async () => {
    const harness = createSessionHarness();

    harness.sendServerSetup();

    harness.session.subscribe({ trackNamespace: ['live'], trackName: 'catalog' });
    const subscribeStream = await harness.nextRequestStream();

    await subscribeStream.firstMessage;

    const entries: unknown[] = [];
    const onEnd = vi.fn();
    const onReset = vi.fn();

    harness.session.fetch(
      { trackNamespace: ['live'], trackName: 'catalog', parameters: { locationFilter: CURRENT_GROUP } },
      { onEntry: (entry) => entries.push(entry), onEnd, onReset }
    );
    const fetchStream = await harness.nextRequestStream();

    await fetchStream.firstMessage;

    // Fetch data stream: header + one full entry, then a reset mid-replay.
    const writer = new ByteWriter();

    writer.writeVarint(0x05);
    writer.writeVarint(2); // fetch request id
    writer.writeVarint(0x1c); // group delta + object delta + priority present
    writer.writeVarint(41);
    writer.writeVarint(0);
    writer.writeUint8(128);
    const payload = utf8Encode('{"version":"1"}');

    writer.writeVarint(payload.length);
    writer.writeBytes(payload);
    const dataStream = harness.openControlledDataStream();

    await dataStream.write(writer.toBytes());
    await vi.waitFor(() => expect(entries).toHaveLength(1));
    dataStream.reset(new Error('reset mid-replay'));

    await vi.waitFor(() => expect(onReset).toHaveBeenCalled());
    expect(onEnd).not.toHaveBeenCalled();

    harness.session.destroy();
  });

  it('treats a truncated response frame on a request stream as a protocol error', async () => {
    const onClosed = vi.fn();
    const harness = createSessionHarness({ onClosed });

    harness.sendServerSetup();
    await harness.session.ready;

    harness.session.subscribe({ trackNamespace: ['live'], trackName: 'video' });
    const request = await harness.nextRequestStream();

    await request.firstMessage;

    // A frame header promising a 100-byte body, then FIN with none of it.
    const partial = new ByteWriter();

    partial.writeVarint(0x04);
    partial.writeUint16(100);
    await request.send(partial.toBytes());
    await request.fin();

    await vi.waitFor(() => expect(onClosed).toHaveBeenCalled());
    expect(harness.getCloseInfo()).toMatchObject({ closeCode: 0x3 });
  });

  it('reports GOAWAY from the control stream', async () => {
    const onGoaway = vi.fn();
    const harness = createSessionHarness({ onGoaway });
    // Server control stream with SETUP then GOAWAY.
    const writer = new ByteWriter();
    const setup = encodeSetup([]);

    writer.writeBytes(setup);
    // GOAWAY with a migration URI.
    const goawayBody = new ByteWriter();
    const uri = utf8Encode('https://relay2.example.com/moq');

    goawayBody.writeVarint(uri.length);
    goawayBody.writeBytes(uri);
    goawayBody.writeVarint(1000);
    const goawayFrame = new ByteWriter();

    goawayFrame.writeVarint(0x10);
    goawayFrame.writeUint16(goawayBody.length);
    goawayFrame.writeBytes(goawayBody.toBytes());
    writer.writeBytes(goawayFrame.toBytes());
    harness.openDataStream(writer.toBytes());

    await vi.waitFor(() => expect(onGoaway).toHaveBeenCalled());
    expect(onGoaway).toHaveBeenCalledWith(
      expect.objectContaining({ newSessionUri: 'https://relay2.example.com/moq', timeout: 1000 })
    );
    harness.session.destroy();
  });

  it('rejects incoming PUBLISH with UNINTERESTED by default', async () => {
    const harness = createSessionHarness();

    harness.sendServerSetup();

    // Server-initiated PUBLISH request stream.
    const publishBody = new ByteWriter();

    publishBody.writeVarint(1); // server request id (odd)
    publishBody.writeVarint(1); // namespace field count
    const field = utf8Encode('live');

    publishBody.writeVarint(field.length);
    publishBody.writeBytes(field);
    const name = utf8Encode('unwanted');

    publishBody.writeVarint(name.length);
    publishBody.writeBytes(name);
    publishBody.writeVarint(9); // track alias
    publishBody.writeVarint(0); // parameter count
    const publishFrame = new ByteWriter();

    publishFrame.writeVarint(0x1d);
    publishFrame.writeUint16(publishBody.length);
    publishFrame.writeBytes(publishBody.toBytes());

    const { responses } = harness.openIncomingBidi(publishFrame.toBytes());
    const chunks = await responses;
    const deframer = new ControlMessageDeframer();
    const messages = chunks.flatMap((chunk) => deframer.push(chunk)).map(decodeControlMessage);

    expect(messages[0]).toMatchObject({ kind: 'request-error', errorCode: REQUEST_ERROR_CODE.UNINTERESTED });

    harness.session.destroy();
  });

  it('rejects incoming PUBLISH_NAMESPACE with NOT_SUPPORTED by default', async () => {
    const harness = createSessionHarness();

    harness.sendServerSetup();

    // Server-initiated PUBLISH_NAMESPACE request stream — relays commonly
    // announce active namespaces unsolicited to newly connected clients.
    const { responses } = harness.openIncomingBidi(encodePublishNamespace({ requestId: 1, trackNamespace: ['anon'] }));
    const chunks = await responses;
    const deframer = new ControlMessageDeframer();
    const messages = chunks.flatMap((chunk) => deframer.push(chunk)).map(decodeControlMessage);

    expect(messages[0]).toMatchObject({ kind: 'request-error', errorCode: REQUEST_ERROR_CODE.NOT_SUPPORTED });

    harness.session.destroy();
  });

  describe('inbound request IDs (§10.1)', () => {
    const publish = (requestId: number) =>
      encodePublish({ requestId, trackNamespace: ['live'], trackName: 'offered', trackAlias: 9 });
    const publishNamespace = (requestId: number) => encodePublishNamespace({ requestId, trackNamespace: ['anon'] });

    async function expectInvalidRequestId(harness: ReturnType<typeof createSessionHarness>, reason: string) {
      await vi.waitFor(() => expect(harness.getCloseInfo()).toBeDefined());
      expect(harness.getCloseInfo()).toEqual({ closeCode: SESSION_ERROR.INVALID_REQUEST_ID, reason });
    }

    it('closes with INVALID_REQUEST_ID on an even PUBLISH id, before offering it', async () => {
      const onIncomingPublish = vi.fn();
      const harness = createSessionHarness({ onIncomingPublish });

      harness.sendServerSetup();
      harness.openIncomingBidi(publish(2));

      await expectInvalidRequestId(harness, 'peer sent even request id 2');
      expect(onIncomingPublish).not.toHaveBeenCalled();
    });

    it('closes with INVALID_REQUEST_ID on a reused PUBLISH id', async () => {
      const onIncomingPublish = vi.fn((_publish, responder) => responder.accept());
      const harness = createSessionHarness({ onIncomingPublish });

      harness.sendServerSetup();
      await harness.openIncomingBidi(publish(1)).responses;
      harness.openIncomingBidi(publish(1));

      await expectInvalidRequestId(harness, 'peer reused request id 1');
      expect(onIncomingPublish).toHaveBeenCalledTimes(1);
    });

    it('closes with INVALID_REQUEST_ID on an even id of a kind it rejects', async () => {
      const harness = createSessionHarness();

      harness.sendServerSetup();
      harness.openIncomingBidi(publishNamespace(4));

      await expectInvalidRequestId(harness, 'peer sent even request id 4');
    });

    it('counts a rejected request as consuming its id', async () => {
      const harness = createSessionHarness();

      harness.sendServerSetup();
      await harness.openIncomingBidi(publishNamespace(1)).responses;
      harness.openIncomingBidi(publish(1));

      await expectInvalidRequestId(harness, 'peer reused request id 1');
    });

    it('answers distinct odd ids and stays open', async () => {
      const harness = createSessionHarness();

      harness.sendServerSetup();
      await harness.openIncomingBidi(publish(1)).responses;
      const chunks = await harness.openIncomingBidi(publishNamespace(3)).responses;
      const deframer = new ControlMessageDeframer();
      const messages = chunks.flatMap((chunk) => deframer.push(chunk)).map(decodeControlMessage);

      expect(messages[0]).toMatchObject({ kind: 'request-error', errorCode: REQUEST_ERROR_CODE.NOT_SUPPORTED });
      expect(harness.getCloseInfo()).toBeUndefined();

      harness.session.destroy();
    });
  });

  it('closes the session when an incoming request frame has a truncated body', async () => {
    const onClosed = vi.fn();
    const harness = createSessionHarness({ onClosed });

    harness.sendServerSetup();

    // A PUBLISH_NAMESPACE frame whose declared body length cuts the
    // namespace off — decoding must terminate the session, not vanish as
    // an unhandled RangeError in the accept loop.
    const frame = new ByteWriter();

    frame.writeVarint(0x06); // PUBLISH_NAMESPACE
    frame.writeUint16(1);
    frame.writeVarint(1); // request id, then no namespace at all
    harness.openIncomingBidi(frame.toBytes());

    await vi.waitFor(() => expect(onClosed).toHaveBeenCalled());
    expect(harness.getCloseInfo()).toMatchObject({ closeCode: 0x3 });
  });

  it('closes the session with PROTOCOL_VIOLATION on an unknown data stream type', async () => {
    const onClosed = vi.fn();
    const harness = createSessionHarness({ onClosed });

    harness.sendServerSetup();

    const writer = new ByteWriter();

    writer.writeVarint(0x0f); // not a valid unidirectional stream type
    harness.openDataStream(writer.toBytes());

    await vi.waitFor(() => expect(onClosed).toHaveBeenCalled());
    expect(harness.getCloseInfo()).toMatchObject({ closeCode: 0x3 });
  });

  it('surfaces PUBLISH_STATE_NOTIFY to the subscription without replying', async () => {
    const harness = createSessionHarness();

    harness.sendServerSetup();

    const onStateNotify = vi.fn();

    harness.session.subscribe({ trackNamespace: ['live'], trackName: 'video' }, { onStateNotify });
    const request = await harness.nextRequestStream();

    await request.firstMessage;
    await request.send(encodeSubscribeOk(7));
    await request.send(encodePublishStateNotify({ largestObject: { group: 12, object: 3 } }));

    await vi.waitFor(() => expect(onStateNotify).toHaveBeenCalled());
    expect(onStateNotify).toHaveBeenCalledWith({ largestObject: { group: 12, object: 3 } });

    // §10.10: unilateral — no REQUEST_OK or REQUEST_ERROR goes back.
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(request.messages).toHaveLength(1);
    harness.session.destroy();
  });

  it('closes the session on PUBLISH_STATE_NOTIFY over a fetch stream', async () => {
    const onClosed = vi.fn();
    const harness = createSessionHarness({ onClosed });

    harness.sendServerSetup();
    harness.session.fetch({ trackNamespace: ['live'], trackName: 'catalog' });
    const request = await harness.nextRequestStream();

    await request.firstMessage;
    await request.send(encodePublishStateNotify({}));

    await vi.waitFor(() => expect(onClosed).toHaveBeenCalled());
    expect(harness.getCloseInfo()).toMatchObject({ closeCode: 0x3 });
  });

  // §10.1 counts REQUEST_UPDATE among the messages that consume a Request
  // ID; reusing the subscription's would be a duplicate the peer closes on.
  it('gives each REQUEST_UPDATE a fresh request id', async () => {
    const harness = createSessionHarness();

    harness.sendServerSetup();

    const subscription = harness.session.subscribe({ trackNamespace: ['live'], trackName: 'video' });
    const request = await harness.nextRequestStream();

    await request.firstMessage;
    subscription.update({ forward: 0 });
    subscription.update({ forward: 1 });

    await vi.waitFor(() => expect(request.messages).toHaveLength(3));
    expect(request.messages[1]).toMatchObject({ kind: 'request-update', requestId: 2, parameters: { forward: 0 } });
    expect(request.messages[2]).toMatchObject({ kind: 'request-update', requestId: 4, parameters: { forward: 1 } });
    harness.session.destroy();
  });

  it('reports an uncountable PUBLISH_DONE stream count as undefined instead of failing the session', async () => {
    const onClosed = vi.fn();
    const harness = createSessionHarness({ onClosed });

    harness.sendServerSetup();

    const onDone = vi.fn();

    harness.session.subscribe({ trackNamespace: ['live'], trackName: 'video' }, { onDone });
    const request = await harness.nextRequestStream();

    await request.firstMessage;
    await request.send(encodeSubscribeOk(7));

    // Status, the 9-byte varint 2^64-1 (§10.12's sentinel), empty reason.
    const body = Uint8Array.of(PUBLISH_DONE_STATUS.TRACK_ENDED, ...new Array<number>(9).fill(0xff), 0x00);
    const frame = new ByteWriter();

    frame.writeVarint(0x0b);
    frame.writeUint16(body.length);
    frame.writeBytes(body);
    await request.send(frame.toBytes());

    await vi.waitFor(() => expect(onDone).toHaveBeenCalled());
    expect(onDone).toHaveBeenCalledWith(
      expect.objectContaining({ statusCode: PUBLISH_DONE_STATUS.TRACK_ENDED, streamCount: undefined, reason: '' })
    );
    expect(onClosed).not.toHaveBeenCalled();
    harness.session.destroy();
  });
});
