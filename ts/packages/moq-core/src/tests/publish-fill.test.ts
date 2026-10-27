/**
 * LARGEST_OBJECT and fill fetch streams on the publish session (§9.20.17, §3.4). Ported from Video.js v10's publish
 * session tests (f1067b323, 8f3e84fe5, f0577b432), adapted to this core: data streams are opened with
 * `openUniStream(trackAlias)`, which counts them itself, and PUBLISH_DONE waits for opens in flight.
 */
import { describe, expect, it, vi } from 'vitest';

import { StreamReader } from '../protocol/bytes.js';
import {
  encodeRequestUpdate,
  encodeSetup,
  encodeSubscribe,
  type MessageParameters,
  PUBLISH_DONE_STATUS,
} from '../protocol/control-messages.js';
import { readFetchHeader, STREAM_TYPE } from '../protocol/object-stream.js';
import { createMoqtPublishSession, type MoqtPublishSessionCallbacks } from '../protocol/publish-session.js';
import { openRawRequest, rawSubscribe as rawSubscribeTo } from './helpers/raw-peer.js';
import { createTransportPair } from './helpers/transport-pair.js';

const NAMESPACE = ['live', 'abc123'];
const LARGEST = { group: 4, object: 2 };

/** The relay's pull: Next Object, plus a fill of the current group (moq-relay 0.14.17 through 0.17.0). */
function rawSubscribe(server: Parameters<typeof rawSubscribeTo>[0], trackName: string, requestId: number) {
  return rawSubscribeTo(server, NAMESPACE, trackName, requestId);
}

function subscribeWith(
  server: Parameters<typeof openRawRequest>[0],
  trackName: string,
  requestId: number,
  parameters: MessageParameters
) {
  return openRawRequest(server, encodeSubscribe({ requestId, trackNamespace: NAMESPACE, trackName, parameters }));
}

/**
 * A publish session whose peer is raw: SETUP by hand, and every unidirectional stream the publisher opens read as a
 * possible fill. `fills` records each FETCH_HEADER's Request ID and whether the stream then reset (true) or FINed.
 * With `reliableReset`, the publisher's data-stream writers have WebTransport's `commit()`. `openFillStream` replaces
 * every unidirectional stream after the control stream.
 */
async function makeFillHarness({
  reliableReset = true,
  openFillStream,
  callbacks,
}: {
  reliableReset?: boolean;
  openFillStream?: () => Promise<WritableStream<Uint8Array>>;
  callbacks?: MoqtPublishSessionCallbacks;
} = {}) {
  const pair = createTransportPair();
  const openUniStream = pair.client.createUnidirectionalStream.bind(pair.client);
  let openedControl = false;

  pair.client.createUnidirectionalStream = async (options) => {
    if (openedControl && openFillStream) return openFillStream();

    openedControl = true;
    const stream = await openUniStream(options);
    const getWriter = stream.getWriter.bind(stream);

    // The pair's writes settle once the bytes are in the peer's buffer, so the prefix is already delivered at commit.
    if (reliableReset) stream.getWriter = () => Object.assign(getWriter(), { commit() {} });

    return stream;
  };

  const session = createMoqtPublishSession(pair.client, callbacks ? { callbacks } : {});
  const fills: { requestId: number; reset: boolean }[] = [];
  const streams = pair.server.incomingUnidirectionalStreams.getReader();

  void (async () => {
    while (true) {
      const { done, value } = await streams.read();
      if (done) break;

      void (async () => {
        const reader = new StreamReader(value);
        const type = await reader.readVarint().catch(() => -1);
        if (type !== STREAM_TYPE.FETCH_HEADER) return;

        const { requestId } = await readFetchHeader(reader);

        try {
          while (!(await reader.atEnd())) await reader.readUint8();

          fills.push({ requestId, reset: false });
        } catch {
          fills.push({ requestId, reset: true });
        }
      })();
    }
  })().catch(() => {});

  const control = await pair.server.createUnidirectionalStream();

  await control.getWriter().write(encodeSetup());
  await session.ready;

  return {
    pair,
    session,
    fills,
    finish: async () => {
      session.destroy();
      await streams.cancel().catch(() => {});
    },
  };
}

/**
 * A buffered send stream: a reset discards everything but the committed prefix. Without `reliableReset` the writer has
 * no `commit()`, as in Chromium today, so a reset discards everything still unread.
 */
function makeBufferedFillStream({ reliableReset = true }: { reliableReset?: boolean } = {}) {
  const chunks: Uint8Array[] = [];
  let committed = 0;
  let readIndex = 0;
  let reset: unknown;
  let pendingRead: ReadableStreamDefaultController<Uint8Array> | undefined;
  const deliver = () => {
    if (!pendingRead) return;

    const controller = pendingRead;
    const limit = reset ? committed : chunks.length;

    if (readIndex < limit) {
      pendingRead = undefined;
      controller.enqueue(chunks[readIndex++]!);
    } else if (reset) {
      pendingRead = undefined;
      controller.error(reset);
    }
  };
  const readable = new ReadableStream<Uint8Array>(
    {
      pull(controller) {
        pendingRead = controller;
        deliver();
      },
    },
    { highWaterMark: 0 }
  );
  const writable = new WritableStream<Uint8Array>({
    write(chunk) {
      chunks.push(chunk);
      deliver();
    },
    abort(reason) {
      reset = reason;
      deliver();
    },
  });
  const getWriter = writable.getWriter.bind(writable);

  if (reliableReset) {
    writable.getWriter = () =>
      Object.assign(getWriter(), {
        commit() {
          committed = chunks.length;
        },
      });
  }

  return { writable, readable, reset: () => reset };
}

/** A pending stream open the test settles by hand. */
function deferredOpen() {
  let resolveOpen!: (stream: WritableStream<Uint8Array>) => void;
  const openFillStream = vi.fn(
    () =>
      new Promise<WritableStream<Uint8Array>>((resolve) => {
        resolveOpen = resolve;
      })
  );

  return { openFillStream, resolveOpen: (stream: WritableStream<Uint8Array>) => resolveOpen(stream) };
}

describe('LARGEST_OBJECT', () => {
  it('reports the track Largest Object in SUBSCRIBE_OK once content exists', async () => {
    const { pair, session, finish } = await makeFillHarness();
    let largest: { group: number; object: number } | undefined;

    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video', getLargestObject: () => largest });

    // No content yet: SUBSCRIBE_OK leaves LARGEST_OBJECT off (§9.20.17).
    const early = await rawSubscribe(pair.server, 'video', 201);

    await vi.waitFor(() => expect(early.received.map((m) => m.kind)).toEqual(['subscribe-ok']));
    expect(early.received[0]).toMatchObject({ kind: 'subscribe-ok', parameters: {} });

    // Once objects have been written, the next SUBSCRIBE_OK carries it.
    largest = { group: 35, object: 28 };
    const later = await rawSubscribe(pair.server, 'video', 203);

    await vi.waitFor(() => expect(later.received.map((m) => m.kind)).toEqual(['subscribe-ok']));
    expect(later.received[0]).toMatchObject({
      kind: 'subscribe-ok',
      parameters: { largestObject: { group: 35, object: 28 } },
    });
    await finish();
  });

  it('reports it in the REQUEST_OK answering a REQUEST_UPDATE, read when the update arrives', async () => {
    const { pair, session, finish } = await makeFillHarness();
    let largest = LARGEST;

    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video', getLargestObject: () => largest });
    const sub = await subscribeWith(pair.server, 'video', 205, {});

    await vi.waitFor(() => expect(sub.received.map((m) => m.kind)).toEqual(['subscribe-ok']));
    largest = { group: 9, object: 0 };
    await sub.send(encodeRequestUpdate(207, { forward: 1 }));
    await vi.waitFor(() => expect(sub.received.map((m) => m.kind)).toEqual(['subscribe-ok', 'request-ok']));
    expect(sub.received[0]).toMatchObject({ parameters: { largestObject: LARGEST } });
    expect(sub.received[1]).toMatchObject({ parameters: { largestObject: { group: 9, object: 0 } } });
    await finish();
  });

  it('refuses a SUBSCRIBE when reading the Largest Object throws, keeping the session', async () => {
    const failures: unknown[] = [];
    const { pair, session, finish } = await makeFillHarness({
      callbacks: { onSubscribeFailed: ({ error }) => failures.push(error) },
    });
    const error = new Error('no largest object');

    session.registerTrack({
      trackNamespace: NAMESPACE,
      trackName: 'video',
      getLargestObject: () => {
        throw error;
      },
    });
    const sub = await subscribeWith(pair.server, 'video', 209, {});

    await vi.waitFor(() => expect(sub.ended()).toBe(true));
    expect(sub.received.map((m) => m.kind)).toEqual(['request-error']);
    expect(failures).toEqual([error]);
    await finish();
  });
});

describe('fill fetch streams', () => {
  it('opens and resets a fill fetch stream for a SUBSCRIBE with a nonempty fill range', async () => {
    const { pair, session, fills, finish } = await makeFillHarness();

    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video', getLargestObject: () => LARGEST });
    const sub = await rawSubscribe(pair.server, 'video', 211);

    await vi.waitFor(() => expect(fills).toEqual([{ requestId: 211, reset: true }]));
    // A failed fill does not affect the subscription (§3.4.1).
    expect(sub.received.map((m) => m.kind)).toEqual(['subscribe-ok']);
    expect(sub.ended()).toBe(false);
    await finish();
  });

  it('checks the fill range against the Largest Object its SUBSCRIBE_OK advertised, not a later in-place update', async () => {
    const largest = { group: 4, object: 2 };
    const { pair, session, fills, finish } = await makeFillHarness({
      // The track's owner advances its Largest Object in place as the subscription binds and data starts.
      callbacks: {
        onTrackBinding: () => {
          largest.group = 9;
        },
      },
    });

    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video', getLargestObject: () => largest });
    const sub = await subscribeWith(pair.server, 'video', 263, {
      fillParameters: { locationFilter: { type: 'absolute', start: { group: 5, object: 0 } } },
    });

    await vi.waitFor(() => expect(sub.received.map((m) => m.kind)).toEqual(['subscribe-ok']));
    expect(sub.received[0]).toMatchObject({ parameters: { largestObject: { group: 4, object: 2 } } });
    await new Promise((resolve) => setTimeout(resolve, 20));
    // A fill from {5,0} is empty against the advertised {4,2}.
    expect(fills).toEqual([]);
    await finish();
  });

  it('opens no fill fetch stream for an empty fill range (§3.4)', async () => {
    const { pair, session, fills, finish } = await makeFillHarness();
    let largest: { group: number; object: number } | undefined;

    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video', getLargestObject: () => largest });
    const subscribeWithFill = async (requestId: number, parameters: MessageParameters) => {
      const request = await subscribeWith(pair.server, 'video', requestId, {
        forward: 1,
        locationFilter: { type: 'next-object' },
        ...parameters,
      });

      await vi.waitFor(() => expect(request.received.map((m) => m.kind)).toEqual(['subscribe-ok']));
    };

    // Nothing published yet: there is nothing to fill, whatever the filter.
    await subscribeWithFill(213, {
      fillParameters: { locationFilter: { type: 'relative-group', groupsBeforeNext: 1 } },
    });

    // With content, a fill from the Next Object or the Next Group starts past Largest Object, and a fill without a
    // filter inherits the subscription's Next Object.
    largest = LARGEST;
    await subscribeWithFill(215, { fillParameters: { locationFilter: { type: 'next-object' } } });
    await subscribeWithFill(217, {
      fillParameters: { locationFilter: { type: 'relative-group', groupsBeforeNext: 0 } },
    });
    await subscribeWithFill(219, { fillParameters: {} });

    // A paused subscription opens no fill either (§3.4.1).
    await subscribeWithFill(221, {
      forward: 0,
      fillParameters: { locationFilter: { type: 'relative-group', groupsBeforeNext: 1 } },
    });

    // Control: a fill of the current group is nonempty, and it is the only fill the peer saw.
    await subscribeWithFill(223, {
      fillParameters: { locationFilter: { type: 'relative-group', groupsBeforeNext: 1 } },
    });
    await vi.waitFor(() => expect(fills).toEqual([{ requestId: 223, reset: true }]));
    await finish();
  });

  it('keys a fill requested by REQUEST_UPDATE to the update Request ID and acknowledges it', async () => {
    const updates: unknown[] = [];
    const { pair, session, fills, finish } = await makeFillHarness({
      callbacks: { onRequestUpdate: ({ updateRequestId }) => updates.push(updateRequestId) },
    });
    let largest: { group: number; object: number } | undefined;

    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video', getLargestObject: () => largest });
    const sub = await rawSubscribe(pair.server, 'video', 225);

    await vi.waitFor(() => expect(sub.received.map((m) => m.kind)).toEqual(['subscribe-ok']));

    // An empty-range fill is acknowledged like any update but opens no stream; a nonempty one opens (and resets) a
    // stream carrying the update's own Request ID, not the subscription's.
    largest = LARGEST;
    await sub.send(encodeRequestUpdate(227, { fillParameters: { locationFilter: { type: 'next-object' } } }));
    await vi.waitFor(() => expect(sub.received.map((m) => m.kind)).toEqual(['subscribe-ok', 'request-ok']));
    largest = { group: 8, object: 3 };
    await sub.send(
      encodeRequestUpdate(229, { fillParameters: { locationFilter: { type: 'relative-group', groupsBeforeNext: 1 } } })
    );
    await vi.waitFor(() => {
      expect(fills).toEqual([{ requestId: 229, reset: true }]);
      expect(sub.received.map((m) => m.kind)).toEqual(['subscribe-ok', 'request-ok', 'request-ok']);
    });
    expect(sub.received[2]).toMatchObject({ parameters: { largestObject: { group: 8, object: 3 } } });
    expect(updates).toEqual([227, 229]);
    expect(sub.ended()).toBe(false);
    await finish();
  });

  it('acknowledges a fill update on a track that reports no Largest Object, opening nothing', async () => {
    const { pair, session, fills, finish } = await makeFillHarness();

    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video' });
    const sub = await rawSubscribe(pair.server, 'video', 231);

    await vi.waitFor(() => expect(sub.received.map((m) => m.kind)).toEqual(['subscribe-ok']));
    await sub.send(encodeRequestUpdate(233, { fillParameters: {} }));
    await vi.waitFor(() => expect(sub.received.map((m) => m.kind)).toEqual(['subscribe-ok', 'request-ok']));
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(fills).toEqual([]);
    expect(sub.ended()).toBe(false);
    await finish();
  });

  it('counts fill fetch streams in the PUBLISH_DONE Stream Count (§9.9)', async () => {
    const { pair, session, fills, finish } = await makeFillHarness();
    const track = session.registerTrack({
      trackNamespace: NAMESPACE,
      trackName: 'video',
      getLargestObject: () => LARGEST,
    });
    const sub = await rawSubscribe(pair.server, 'video', 235);

    await vi.waitFor(() => expect(fills).toEqual([{ requestId: 235, reset: true }]));

    for (let i = 0; i < 3; i++) await (await session.openUniStream(235)).close();

    track.end();
    await vi.waitFor(() => expect(sub.ended()).toBe(true));
    // Three data streams plus the reset fill.
    expect(sub.received.at(-1)).toMatchObject({
      kind: 'publish-done',
      statusCode: PUBLISH_DONE_STATUS.TRACK_ENDED,
      streamCount: 4,
    });
    await finish();
  });

  it('preserves the fill header across the reset where the writer can commit it', async () => {
    const fill = makeBufferedFillStream();
    const { pair, session, finish } = await makeFillHarness({ openFillStream: async () => fill.writable });

    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video', getLargestObject: () => LARGEST });
    await subscribeWith(pair.server, 'video', 237, { fillParameters: {} });
    await vi.waitFor(() => expect(fill.reset()).toBeDefined());

    // The peer reads only after the reset, and still finds the header.
    const reader = new StreamReader(fill.readable);

    expect(await reader.readVarint()).toBe(STREAM_TYPE.FETCH_HEADER);
    expect(await readFetchHeader(reader)).toEqual({ requestId: 237 });
    await expect(reader.atEnd()).rejects.toThrow('fill fetch streams are not served');
    await finish();
  });

  it('still serves the subscription and resets the fill when the reset may discard its header', async () => {
    const fill = makeBufferedFillStream({ reliableReset: false });
    const { pair, session, finish } = await makeFillHarness({
      reliableReset: false,
      openFillStream: async () => fill.writable,
    });

    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video', getLargestObject: () => LARGEST });
    const sub = await subscribeWith(pair.server, 'video', 239, { fillParameters: {} });

    await vi.waitFor(() => {
      expect(sub.received.map((m) => m.kind)).toEqual(['subscribe-ok']);
      expect(fill.reset()).toBeDefined();
    });

    // Nothing kept the header through the reset: the peer sees only the reset, and the subscription is unaffected.
    await expect(new StreamReader(fill.readable).readVarint()).rejects.toThrow('fill fetch streams are not served');
    expect(sub.ended()).toBe(false);
    await finish();
  });

  it.each([{ headerSurvives: true }, { headerSurvives: false }])(
    'counts a fill reset without reliable reset in the Stream Count (header survives: $headerSurvives)',
    async ({ headerSurvives }) => {
      const buffered = makeBufferedFillStream({ reliableReset: false });
      const { pair, session, fills, finish } = await makeFillHarness({
        reliableReset: false,
        ...(headerSurvives ? {} : { openFillStream: async () => buffered.writable }),
      });
      const track = session.registerTrack({
        trackNamespace: NAMESPACE,
        trackName: 'video',
        getLargestObject: () => LARGEST,
      });
      const sub = await subscribeWith(pair.server, 'video', 241, { fillParameters: {} });

      await vi.waitFor(() => {
        expect(sub.received.map((m) => m.kind)).toEqual(['subscribe-ok']);

        if (headerSurvives) expect(fills).toEqual([{ requestId: 241, reset: true }]);
        else expect(buffered.reset()).toBeDefined();
      });

      track.end();
      await vi.waitFor(() => expect(sub.ended()).toBe(true));
      // §9.9 counts the streams the publisher opened, fill fetch streams included, whether or not the header outlived
      // the reset. A subscriber waits out a timeout for a stream reset before its header, but one that receives more
      // streams than the count may close the session.
      expect(sub.received.at(-1)).toMatchObject({
        kind: 'publish-done',
        statusCode: PUBLISH_DONE_STATUS.TRACK_ENDED,
        streamCount: 1,
      });
      await finish();
    }
  );

  it.each(['cancel', 'replace', 'end', 'destroy'] as const)(
    'aborts a backpressured fill when the subscription ends by %s',
    async (action) => {
      let writing = false;
      const aborted = vi.fn();
      const writable = new WritableStream<Uint8Array>({
        write(_, controller) {
          writing = true;
          return new Promise<void>((_, reject) => {
            controller.signal.addEventListener('abort', () => reject(controller.signal.reason), { once: true });
          });
        },
        abort: aborted,
      });
      const getWriter = writable.getWriter.bind(writable);

      writable.getWriter = () => Object.assign(getWriter(), { commit() {} });

      const { pair, session, finish } = await makeFillHarness({ openFillStream: async () => writable });
      const track = session.registerTrack({
        trackNamespace: NAMESPACE,
        trackName: 'video',
        getLargestObject: () => LARGEST,
      });
      const sub = await subscribeWith(pair.server, 'video', 243, {});

      await vi.waitFor(() => expect(sub.received[0]?.kind).toBe('subscribe-ok'));
      await sub.send(encodeRequestUpdate(245, { fillParameters: {} }));
      await vi.waitFor(() => expect(writing).toBe(true));

      if (action === 'cancel') await sub.reset();
      else if (action === 'replace') await subscribeWith(pair.server, 'video', 247, {});
      else if (action === 'destroy') session.destroy();
      else track.end();

      await vi.waitFor(() => expect(aborted).toHaveBeenCalledOnce());
      await finish();
    }
  );

  it('resets a fill whose open completes after the subscription was cancelled, without its header', async () => {
    const { openFillStream, resolveOpen } = deferredOpen();
    const { pair, session, finish } = await makeFillHarness({ openFillStream });

    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video', getLargestObject: () => LARGEST });
    const sub = await subscribeWith(pair.server, 'video', 249, {});

    await vi.waitFor(() => expect(sub.received[0]?.kind).toBe('subscribe-ok'));
    await sub.send(encodeRequestUpdate(251, { fillParameters: {} }));
    await vi.waitFor(() => expect(openFillStream).toHaveBeenCalledOnce());
    await sub.reset();
    await vi.waitFor(() => expect(sub.ended()).toBe(true));

    const write = vi.fn();
    const abort = vi.fn();

    resolveOpen(new WritableStream<Uint8Array>({ write, abort }));
    await vi.waitFor(() => expect(abort).toHaveBeenCalledOnce());
    expect(write).not.toHaveBeenCalled();
    await finish();
  });

  it.each([{ reliableReset: true }, { reliableReset: false }])(
    'holds PUBLISH_DONE for a fill still opening when the track ends, then resets it bare and uncounted (reliable reset: $reliableReset)',
    async ({ reliableReset }) => {
      const { openFillStream, resolveOpen } = deferredOpen();
      const { pair, session, finish } = await makeFillHarness({ reliableReset, openFillStream });
      const track = session.registerTrack({
        trackNamespace: NAMESPACE,
        trackName: 'video',
        getLargestObject: () => LARGEST,
      });
      const sub = await subscribeWith(pair.server, 'video', 253, {});

      await vi.waitFor(() => expect(sub.received[0]?.kind).toBe('subscribe-ok'));
      await sub.send(encodeRequestUpdate(255, { fillParameters: {} }));
      await vi.waitFor(() => expect(openFillStream).toHaveBeenCalledOnce());

      // The track ends while the fill's stream is still opening. PUBLISH_DONE waits for the open, as it does for a data
      // stream's (§9.9: no PUBLISH_DONE until every stream is closed).
      track.end();
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(sub.received.map((m) => m.kind)).toEqual(['subscribe-ok', 'request-ok']);

      const write = vi.fn();
      const abort = vi.fn();

      resolveOpen(new WritableStream<Uint8Array>({ write, abort }));
      await vi.waitFor(() => expect(sub.ended()).toBe(true));

      // A fill counts in the step that commits it to its FETCH_HEADER. This one opened after its subscription ended, so
      // it is reset with no header: the peer cannot attribute it to the subscription, and it is left out of the count.
      expect(abort).toHaveBeenCalledOnce();
      expect(write).not.toHaveBeenCalled();
      expect(sub.received.at(-1)).toMatchObject({
        kind: 'publish-done',
        statusCode: PUBLISH_DONE_STATUS.TRACK_ENDED,
        streamCount: 0,
      });
      await finish();
    }
  );

  it.each([{ probeEnds: 'before' }, { probeEnds: 'after' }])(
    "serves a relay's second pull of a live track without reliable reset (probe ends $probeEnds the pull)",
    async ({ probeEnds }) => {
      const bindings: (number | undefined)[] = [];
      const ended: number[] = [];
      const { pair, session, fills, finish } = await makeFillHarness({
        reliableReset: false,
        callbacks: {
          onTrackBinding: ({ trackAlias }) => bindings.push(trackAlias),
          onSubscribeEnd: ({ requestId }) => ended.push(requestId),
        },
      });
      let largest: { group: number; object: number } | undefined;

      session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'catalog', getLargestObject: () => largest });

      // A cross-relay viewer's first pull is the relay's TRACK_INFO probe: nothing published yet, so nothing to fill.
      const probe = await rawSubscribe(pair.server, 'catalog', 257);

      await vi.waitFor(() => expect(probe.received.map((m) => m.kind)).toEqual(['subscribe-ok']));

      // The probe's binding put the catalog on the wire, then the relay cancelled it and pulled again for the viewer,
      // now with a nonempty fill this transport cannot fail reliably. The pull is served anyway.
      largest = { group: 0, object: 0 };

      if (probeEnds === 'before') await probe.reset();

      const pull = await rawSubscribe(pair.server, 'catalog', 259);

      await vi.waitFor(() => {
        expect(pull.received.map((m) => m.kind)).toEqual(['subscribe-ok']);
        expect(bindings.at(-1)).toBe(259);
        expect(fills).toEqual([{ requestId: 259, reset: true }]);
      });

      // The probe's end may be handled after the pull is bound: it reports the newest live binding, not an unbind.
      if (probeEnds === 'after') await probe.reset();

      await vi.waitFor(() => expect(ended).toContain(257));
      expect(bindings.at(-1)).toBe(259);
      expect(pull.ended()).toBe(false);

      // A fill requested by REQUEST_UPDATE is acknowledged and failed the same way.
      await pull.send(
        encodeRequestUpdate(261, {
          fillParameters: { locationFilter: { type: 'relative-group', groupsBeforeNext: 1 } },
        })
      );
      await vi.waitFor(() => {
        expect(pull.received.map((m) => m.kind)).toEqual(['subscribe-ok', 'request-ok']);
        expect(fills).toEqual([
          { requestId: 259, reset: true },
          { requestId: 261, reset: true },
        ]);
      });
      expect(pull.ended()).toBe(false);
      await finish();
    }
  );
});
