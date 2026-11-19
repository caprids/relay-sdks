/**
 * The teardown rule (issue #527): after a driver's `onClosed`, none of its callbacks run, and nothing it accepted
 * outlives its drain. One test per path the issue lists.
 */
import { describe, expect, it, vi } from 'vitest';

import { ByteWriter } from '../protocol/bytes.js';
import {
  encodeGoaway,
  encodeNamespace,
  encodePublish,
  encodeRequestOk,
  encodeRequestUpdate,
  encodeSetup,
  encodeSubscribe,
  encodeSubscribeNamespace,
  encodeTrackStatus,
  type KeyValuePair,
  REQUEST_ERROR_CODE,
  SETUP_OPTION,
} from '../protocol/control-messages.js';
import { createMoqtDuplexSession } from '../protocol/duplex-session.js';
import { encodeFetchHeader } from '../protocol/object-stream.js';
import {
  createMoqtPublishSession,
  type MoqtPublishSession,
  type MoqtPublishSessionCallbacks,
} from '../protocol/publish-session.js';
import type { BidirectionalStreamLike } from '../protocol/request-stream.js';
import {
  createMoqtSession,
  type MoqtSessionCallbacks,
  type MoqtSessionConfig,
  type MoqtTransport,
} from '../protocol/session.js';
import { openRawRequest, rawSubscribe, solicitNamespace } from './helpers/raw-peer.js';
import { createTransportPair } from './helpers/transport-pair.js';

const NAMESPACE = ['live', 'abc123'];
const TRACK = { trackNamespace: NAMESPACE, trackName: 'video' };

const settle = (ms = 30) => new Promise((resolve) => setTimeout(resolve, ms));

/** A transport whose `close()` leaves it open, as a duplex session's subscriber sees it while the publisher drains. */
function heldOpen(transport: MoqtTransport): MoqtTransport {
  return { ...transport, close: () => {} };
}

/** Each request stream the client opens, as the server sees it. */
function clientRequests(server: MoqtTransport) {
  const streams: BidirectionalStreamLike[] = [];

  void (async () => {
    const reader = server.incomingBidirectionalStreams.getReader();

    for (;;) {
      const { done, value } = await reader.read();
      if (done) return;
      streams.push(value);
    }
  })().catch(() => {});

  return {
    streams,
    async next(): Promise<BidirectionalStreamLike> {
      const index = streams.length;

      await vi.waitFor(() => expect(streams.length).toBeGreaterThan(index));
      return streams[index]!;
    },
  };
}

/** Whether the peer has stopped reading what `writer` sends (STOP_SENDING). */
function stoppedFlag(writer: WritableStreamDefaultWriter<Uint8Array>): () => boolean {
  let stopped = false;

  writer.closed.catch(() => {
    stopped = true;
  });
  return () => stopped;
}

/** One fetch entry: an object at an absolute location (§11.4.2). */
function fetchEntry(group: number, object: number): Uint8Array {
  const writer = new ByteWriter();

  writer.writeVarint(0x1c);
  writer.writeVarint(group);
  writer.writeVarint(object);
  writer.writeUint8(128);
  writer.writeVarint(1);
  writer.writeBytes(new Uint8Array([7]));
  return writer.toBytes();
}

function concat(...chunks: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(chunks.reduce((length, chunk) => length + chunk.length, 0));
  let offset = 0;

  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }

  return out;
}

describe('subscribe driver teardown', () => {
  async function harness(
    callbacks: MoqtSessionCallbacks = {},
    {
      setup = true,
      hold = true,
      setupOptions = [],
      wrap = (transport: MoqtTransport) => transport,
    }: {
      setup?: boolean;
      hold?: boolean;
      setupOptions?: KeyValuePair[];
      wrap?: (transport: MoqtTransport) => MoqtTransport;
    } = {}
  ) {
    const pair = createTransportPair();
    const requests = clientRequests(pair.server);
    const config: MoqtSessionConfig = { callbacks, setupOptions };
    const session = createMoqtSession(wrap(hold ? heldOpen(pair.client) : pair.client), config);
    const control = (await pair.server.createUnidirectionalStream()).getWriter();

    if (setup) {
      await control.write(encodeSetup(setupOptions));
      await session.ready;
    }

    return { pair, session, control, requests };
  }

  it('answers no request whose stream opened across teardown', async () => {
    const order: string[] = [];
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const { session, requests } = await harness(
      { onClosed: () => order.push('closed') },
      {
        wrap: (transport) => ({
          ...transport,
          createBidirectionalStream: async () => {
            await gate;
            return transport.createBidirectionalStream();
          },
        }),
      }
    );

    session.trackStatus(TRACK, {
      onOk: () => order.push('status.ok'),
      onError: () => order.push('status.error'),
    });
    session.destroy();
    release();

    const stream = await requests.next();
    await stream.writable
      .getWriter()
      .write(encodeRequestOk())
      .catch(() => {});
    await settle();
    expect(order).toEqual(['closed']);
  });

  it('stops reading an answered request at teardown', async () => {
    const onOk = vi.fn();
    const { session, requests } = await harness();

    session.trackStatus(TRACK, { onOk });
    const writer = (await requests.next()).writable.getWriter();
    const stopped = stoppedFlag(writer);
    await writer.write(encodeRequestOk());
    await vi.waitFor(() => expect(onOk).toHaveBeenCalled());

    session.destroy();
    await vi.waitFor(() => expect(stopped()).toBe(true));
  });

  it('resets a request the peer has finished answering but whose data is outstanding', async () => {
    const { session, requests } = await harness();

    session.fetch(TRACK);
    const stream = await requests.next();
    let reset = false;
    void stream.readable.pipeTo(new WritableStream()).catch(() => {
      reset = true;
    });
    const writer = stream.writable.getWriter();
    await writer.write(encodeRequestOk());
    await writer.close();
    await settle();

    session.destroy();
    await vi.waitFor(() => expect(reset).toBe(true));
  });

  it('answers no incoming request from a responder kept past teardown', async () => {
    let responder!: { accept(): void };
    const { pair, session } = await harness({
      onIncomingPublish: (_publish, respond) => {
        responder = respond;
      },
    });
    const publish = await openRawRequest(pair.server, encodePublish({ requestId: 1, ...TRACK, trackAlias: 9 }));
    await vi.waitFor(() => expect(responder).toBeDefined());

    session.destroy();
    responder.accept();
    await vi.waitFor(() => expect(publish.ended()).toBe(true));
    expect(publish.received).toEqual([]);
    expect(publish.failure()).toBeDefined();
  });

  it('starts no request once it has ended', async () => {
    const { session, requests } = await harness();

    session.destroy();
    session.subscribe(TRACK);
    session.fetch(TRACK);
    session.trackStatus(TRACK);
    session.subscribeNamespace({ trackNamespacePrefix: ['live'] });

    await settle();
    expect(requests.streams).toHaveLength(0);
  });

  it('stops a fetch’s data stream at teardown and reports nothing more from it', async () => {
    const order: string[] = [];
    const { pair, session, requests } = await harness({ onClosed: () => order.push('closed') });

    const fetch = session.fetch(TRACK, {
      onEntry: () => order.push('entry'),
      onEnd: () => order.push('end'),
      onReset: () => order.push('reset'),
    });
    await requests.next();
    const data = (await pair.server.createUnidirectionalStream()).getWriter();
    const stopped = stoppedFlag(data);
    await data.write(concat(encodeFetchHeader(fetch.requestId), fetchEntry(4, 0)));
    await vi.waitFor(() => expect(order).toEqual(['entry']));

    session.destroy();
    await data.write(fetchEntry(4, 1)).catch(() => {});
    await data.close().catch(() => {});
    await settle();
    expect(order).toEqual(['entry', 'closed']);
    expect(stopped()).toBe(true);
  });

  it('reports no readiness from a SETUP completed after teardown', async () => {
    const onReady = vi.fn();
    const { session, control } = await harness({ onReady }, { setup: false });
    const setup = encodeSetup();

    await control.write(setup.subarray(0, 3));
    await settle();
    session.destroy();
    await control.write(setup.subarray(3));
    await settle();
    expect(onReady).not.toHaveBeenCalled();
  });

  it('reports no control-stream GOAWAY after teardown', async () => {
    const onGoaway = vi.fn();
    const { session, control } = await harness({ onGoaway });

    session.destroy();
    await control.write(encodeGoaway(0, 'https://relay.example.com/next'));
    await settle();
    expect(onGoaway).not.toHaveBeenCalled();
  });

  describe('namespace subscriptions', () => {
    const ACTIVE_COUNT: KeyValuePair = { type: SETUP_OPTION.ACTIVE_COUNT, value: 1 };

    async function namespaceHarness(destroyOn: 'ok' | 'namespace' | 'namespaceDone') {
      const order: string[] = [];
      const h = await harness({ onClosed: () => order.push('closed') }, { hold: false, setupOptions: [ACTIVE_COUNT] });
      const event = (name: typeof destroyOn | 'synced' | 'end') => {
        order.push(name);
        if (name === destroyOn) h.session.destroy();
      };

      h.session.subscribeNamespace(
        { trackNamespacePrefix: ['room'] },
        {
          onOk: () => event('ok'),
          onSynced: () => event('synced'),
          onNamespace: () => event('namespace'),
          onNamespaceDone: () => event('namespaceDone'),
          onEnd: () => event('end'),
        }
      );
      const writer = (await h.requests.next()).writable.getWriter();

      return { order, writer };
    }

    it('reports no sync after a destroy from onOk', async () => {
      const { order, writer } = await namespaceHarness('ok');

      await writer.write(encodeRequestOk({ activeCount: 0 }));
      await settle();
      expect(order).toEqual(['ok', 'closed']);
    });

    it('reports no sync after a destroy from the last counted onNamespace', async () => {
      const { order, writer } = await namespaceHarness('namespace');

      await writer.write(concat(encodeRequestOk({ activeCount: 1 }), encodeNamespace(['alice'])));
      await settle();
      expect(order).toEqual(['ok', 'namespace', 'closed']);
    });

    it('stops the end-of-request reports after a destroy from the first onNamespaceDone', async () => {
      const { order, writer } = await namespaceHarness('namespaceDone');

      await writer.write(
        concat(encodeRequestOk({ activeCount: 2 }), encodeNamespace(['alice']), encodeNamespace(['bob']))
      );
      await vi.waitFor(() => expect(order).toEqual(['ok', 'namespace', 'namespace', 'synced']));
      await writer.close();
      await settle();
      expect(order).toEqual(['ok', 'namespace', 'namespace', 'synced', 'namespaceDone', 'closed']);
    });
  });

  it('resets its side of an incoming request that ends before a whole message', async () => {
    const { pair } = await harness();
    const stream = await pair.server.createBidirectionalStream();
    let reset = false;

    stream.readable
      .getReader()
      .read()
      .catch(() => {
        reset = true;
      });
    const writer = stream.writable.getWriter();
    await writer.write(encodePublish({ requestId: 1, ...TRACK, trackAlias: 9 }).subarray(0, 2));
    await writer.close();

    await vi.waitFor(() => expect(reset).toBe(true));
  });
});

describe('publish driver teardown', () => {
  async function harness(
    callbacks: MoqtPublishSessionCallbacks = {},
    {
      bufferBytes,
      wrap = (transport: MoqtTransport) => transport,
      getLargestObject,
    }: {
      bufferBytes?: number;
      wrap?: (transport: MoqtTransport) => MoqtTransport;
      getLargestObject?: () => undefined;
    } = {}
  ) {
    const pair = createTransportPair(bufferBytes === undefined ? {} : { bufferBytes });
    const session = createMoqtPublishSession(wrap(pair.client), { callbacks });
    // The publisher's own control stream needs a reader when the pair has no buffer.
    void pair.server.incomingUnidirectionalStreams
      .getReader()
      .read()
      .then(({ value }) => value?.pipeTo(new WritableStream()))
      .catch(() => {});
    const control = (await pair.server.createUnidirectionalStream()).getWriter();

    await control.write(encodeSetup());
    await session.ready;
    session.announce(NAMESPACE);
    session.registerTrack(getLargestObject ? { ...TRACK, getLargestObject } : TRACK);
    return { pair, session, control };
  }

  /** Close the transport from inside a callback: the driver's `onClosed` lands a microtask later. */
  function orderedCallbacks(closeOn: string, close: () => void) {
    const order: string[] = [];
    const event = (name: string) => () => {
      order.push(name);
      if (name === closeOn) close();
    };

    return { order, event };
  }

  it('stops reading every request stream it accepted when it ends', async () => {
    const onClosed = vi.fn();
    const { pair, session } = await harness({ onClosed }, { wrap: heldOpen });
    const stream = await pair.server.createBidirectionalStream();
    const writer = stream.writable.getWriter();
    const stopped = stoppedFlag(writer);
    void stream.readable.pipeTo(new WritableStream()).catch(() => {});
    await writer.write(encodeSubscribeNamespace({ requestId: 1, trackNamespacePrefix: ['live'] }));
    await vi.waitFor(() => expect(session.isSolicited(NAMESPACE)).toBe(true));

    session.close();
    await vi.waitFor(() => expect(onClosed).toHaveBeenCalled());
    await vi.waitFor(() => expect(stopped()).toBe(true));
  });

  it('reports nothing from a subscription’s buffered REQUEST_UPDATE or GOAWAY once closed', async () => {
    let pair!: ReturnType<typeof createTransportPair>;
    const { order, event } = orderedCallbacks('update', () => pair.client.close());
    ({ pair } = await harness({
      onRequestUpdate: event('update'),
      onGoaway: event('goaway'),
      onClosed: event('closed'),
    }));
    const subscribe = await rawSubscribe(pair.server, NAMESPACE, 'video', 3);
    await vi.waitFor(() => expect(subscribe.received).toHaveLength(1));

    await subscribe.send(concat(encodeRequestUpdate(5, {}), encodeRequestUpdate(7, {}), encodeGoaway()));
    await settle();
    expect(order).toEqual(['update', 'closed']);
  });

  it('reports nothing from a solicitation’s buffered GOAWAY or REQUEST_UPDATE once closed', async () => {
    let pair!: ReturnType<typeof createTransportPair>;
    const { order, event } = orderedCallbacks('goaway', () => pair.client.close());
    ({ pair } = await harness({
      onRequestUpdate: event('update'),
      onGoaway: event('goaway'),
      onClosed: event('closed'),
    }));
    const solicitation = await solicitNamespace(pair.server, ['live'], 1);
    await vi.waitFor(() => expect(solicitation.received).toHaveLength(2));

    await solicitation.send(concat(encodeGoaway(), encodeGoaway(), encodeRequestUpdate(3, {})));
    await settle();
    expect(order).toEqual(['goaway', 'closed']);
  });

  it('reports no control-stream GOAWAY once closed', async () => {
    const onGoaway = vi.fn();
    const onClosed = vi.fn();
    const { session, control } = await harness({ onGoaway, onClosed }, { wrap: heldOpen });

    session.close();
    await vi.waitFor(() => expect(onClosed).toHaveBeenCalled());
    await control.write(encodeGoaway(0, 'https://relay.example.com/next'));
    await settle();
    expect(onGoaway).not.toHaveBeenCalled();
  });

  describe('once closing', () => {
    it('binds no alias after a destroy from onSubscribe', async () => {
      let session!: MoqtPublishSession;
      const bindings: (number | undefined)[] = [];
      const h = await harness({
        onSubscribe: () => session.destroy(),
        onTrackBinding: ({ trackAlias }) => bindings.push(trackAlias),
      });
      session = h.session;

      const subscribe = await rawSubscribe(h.pair.server, NAMESPACE, 'video', 3);
      await vi.waitFor(() => expect(subscribe.received.map((m) => m.kind)).toContain('publish-done'));
      expect(bindings.filter((alias) => alias !== undefined)).toEqual([]);
    });

    it('reports no update after a destroy from onTrackBinding', async () => {
      let session!: MoqtPublishSession;
      let destroyOnBinding = false;
      const onRequestUpdate = vi.fn();
      const h = await harness({
        onTrackBinding: () => {
          if (destroyOnBinding) session.destroy();
        },
        onRequestUpdate,
      });
      session = h.session;

      const subscribe = await rawSubscribe(h.pair.server, NAMESPACE, 'video', 3);
      await vi.waitFor(() => expect(subscribe.received).toHaveLength(1));
      destroyOnBinding = true;
      await subscribe.send(encodeRequestUpdate(5, { forward: 0 }));
      await vi.waitFor(() => expect(subscribe.received.map((m) => m.kind)).toContain('publish-done'));
      expect(onRequestUpdate).not.toHaveBeenCalled();
    });

    it('binds no alias after a destroy from onSubscribeEnd', async () => {
      let session!: MoqtPublishSession;
      const order: string[] = [];
      const h = await harness({
        onSubscribeEnd: ({ requestId }) => {
          order.push(`end ${requestId}`);
          session.destroy();
        },
        onTrackBinding: ({ trackAlias }) => order.push(`binding ${trackAlias}`),
      });
      session = h.session;

      const first = await rawSubscribe(h.pair.server, NAMESPACE, 'video', 3);
      await vi.waitFor(() => expect(first.received).toHaveLength(1));
      const second = await rawSubscribe(h.pair.server, NAMESPACE, 'video', 5);
      await vi.waitFor(() => expect(second.received).toHaveLength(1));
      expect(order).toEqual(['binding 3', 'binding 5']);

      // The superseded subscription's request stream ends: its end is reported, then the binding its successor holds.
      await first.fin();
      await vi.waitFor(() => expect(order).toContain('end 3'));
      await settle();
      expect(order.slice(2, 4)).not.toEqual(['end 3', 'binding 5']);
      expect(order.filter((entry) => entry === 'binding 5')).toHaveLength(1);
    });

    it('asks no getLargestObject for a TRACK_STATUS', async () => {
      const getLargestObject = vi.fn(() => undefined);
      const { pair, session } = await harness({}, { getLargestObject });

      session.close();
      const status = await openRawRequest(pair.server, encodeTrackStatus({ requestId: 1, ...TRACK }));
      await vi.waitFor(() => expect(status.received).toHaveLength(1));
      expect(status.received[0]).toMatchObject({ kind: 'request-error', errorCode: REQUEST_ERROR_CODE.GOING_AWAY });
      expect(getLargestObject).not.toHaveBeenCalled();
    });

    it('asks no getLargestObject for a SUBSCRIBE, and accepts none', async () => {
      const getLargestObject = vi.fn(() => undefined);
      const onSubscribe = vi.fn();
      const { pair, session } = await harness({ onSubscribe }, { getLargestObject });

      session.close();
      const subscribe = await openRawRequest(pair.server, encodeSubscribe({ requestId: 1, ...TRACK }));
      await vi.waitFor(() => expect(subscribe.received).toHaveLength(1));
      expect(subscribe.received[0]).toMatchObject({
        kind: 'request-error',
        errorCode: REQUEST_ERROR_CODE.GOING_AWAY,
      });
      expect(getLargestObject).not.toHaveBeenCalled();
      expect(onSubscribe).not.toHaveBeenCalled();
    });

    it('asks no getLargestObject for an update after a destroy from onRequestUpdate', async () => {
      let session!: MoqtPublishSession;
      const getLargestObject = vi.fn(() => undefined);
      const h = await harness({ onRequestUpdate: () => session.destroy() }, { getLargestObject });
      session = h.session;

      const subscribe = await rawSubscribe(h.pair.server, NAMESPACE, 'video', 3);
      await vi.waitFor(() => expect(subscribe.received).toHaveLength(1));
      getLargestObject.mockClear();
      await subscribe.send(encodeRequestUpdate(5, {}));
      await vi.waitFor(() => expect(subscribe.received.map((m) => m.kind)).toContain('publish-done'));
      expect(getLargestObject).not.toHaveBeenCalled();
    });

    it('reports no GOAWAY and no solicitation update during the drain', async () => {
      const onGoaway = vi.fn();
      const onRequestUpdate = vi.fn();
      const { pair, session, control } = await harness({ onGoaway, onRequestUpdate });
      const solicitation = await solicitNamespace(pair.server, ['live'], 1);
      await vi.waitFor(() => expect(solicitation.received).toHaveLength(2));
      // An unanswered subscription keeps the drain waiting for the peer.
      const subscribe = await rawSubscribe(pair.server, NAMESPACE, 'video', 3);
      await vi.waitFor(() => expect(subscribe.received).toHaveLength(1));

      session.close();
      await settle(50);
      await control.write(encodeGoaway(0, 'https://relay.example.com/next'));
      await subscribe.send(encodeGoaway());
      await solicitation.send(encodeRequestUpdate(5, {}));
      await vi.waitFor(() => expect(solicitation.received.map((m) => m.kind)).toContain('request-error'));
      expect(onGoaway).not.toHaveBeenCalled();
      expect(onRequestUpdate).not.toHaveBeenCalled();
    });

    it('reports no announce whose NAMESPACE lands during the drain', async () => {
      const onAnnounced = vi.fn();
      const { pair, session } = await harness({ onAnnounced }, { bufferBytes: 0 });
      const subscribe = await rawSubscribe(pair.server, NAMESPACE, 'video', 3);
      await vi.waitFor(() => expect(subscribe.received).toHaveLength(1));

      // The peer reads REQUEST_OK and then stops, so the NAMESPACE write waits across the start of the drain.
      const stream = await pair.server.createBidirectionalStream();
      await stream.writable
        .getWriter()
        .write(encodeSubscribeNamespace({ requestId: 5, trackNamespacePrefix: ['live'] }));
      const reader = stream.readable.getReader();
      await reader.read();
      await settle();
      session.close();
      await settle(50);
      reader.releaseLock();
      void stream.readable.pipeTo(new WritableStream()).catch(() => {});
      await settle(50);
      expect(onAnnounced).not.toHaveBeenCalled();
    });

    it('reports no readiness from a SETUP that lands during the drain', async () => {
      const onReady = vi.fn();
      const pair = createTransportPair();
      const session = createMoqtPublishSession(pair.client, { callbacks: { onReady } });
      session.registerTrack(TRACK);
      const subscribe = await rawSubscribe(pair.server, NAMESPACE, 'video', 3);
      await vi.waitFor(() => expect(subscribe.received).toHaveLength(1));

      session.close();
      await settle(50);
      await (await pair.server.createUnidirectionalStream()).getWriter().write(encodeSetup());
      await settle();
      expect(onReady).not.toHaveBeenCalled();
    });

    it('refuses a namespace solicitation that arrives during the drain', async () => {
      const { pair, session } = await harness();
      // An unanswered subscription keeps the drain waiting for the peer.
      const subscribe = await rawSubscribe(pair.server, NAMESPACE, 'video', 3);
      await vi.waitFor(() => expect(subscribe.received).toHaveLength(1));

      session.close();
      await settle(50);
      const solicitation = await solicitNamespace(pair.server, ['live'], 5);
      await vi.waitFor(() => expect(solicitation.received).toHaveLength(1));
      await settle();
      expect(solicitation.received).toMatchObject([
        { kind: 'request-error', errorCode: REQUEST_ERROR_CODE.GOING_AWAY },
      ]);
    });

    it('announces nothing on a solicitation whose acceptance lands during the drain', async () => {
      const { pair, session } = await harness({}, { bufferBytes: 0 });
      const subscribe = await rawSubscribe(pair.server, NAMESPACE, 'video', 3);
      await vi.waitFor(() => expect(subscribe.received).toHaveLength(1));

      // The peer reads nothing yet, so the REQUEST_OK write waits across the start of the drain.
      const stream = await pair.server.createBidirectionalStream();
      await stream.writable
        .getWriter()
        .write(encodeSubscribeNamespace({ requestId: 5, trackNamespacePrefix: ['live'] }));
      await settle();
      session.close();
      await settle(50);

      const chunks: Uint8Array[] = [];
      const reader = stream.readable.getReader();
      void (async () => {
        for (;;) {
          const { done, value } = await reader.read();
          if (done) return;
          chunks.push(value);
        }
      })().catch(() => {});
      await settle(50);
      expect([...concat(...chunks)]).toEqual([...encodeRequestOk()]);
    });
  });

  it('resets its side of an incoming request that ends before a whole message', async () => {
    const { pair } = await harness();
    const stream = await pair.server.createBidirectionalStream();
    let reset = false;

    stream.readable
      .getReader()
      .read()
      .catch(() => {
        reset = true;
      });
    const writer = stream.writable.getWriter();
    await writer.write(encodeSubscribe({ requestId: 1, ...TRACK }).subarray(0, 2));
    await writer.close();

    await vi.waitFor(() => expect(reset).toBe(true));
  });
});

describe('duplex session teardown', () => {
  async function harness(wrap = (transport: MoqtTransport) => transport) {
    const pair = createTransportPair();
    const publisherClosed = vi.fn();
    const session = createMoqtDuplexSession(wrap(pair.client), {
      publisher: { callbacks: { onClosed: publisherClosed } },
    });
    // The peer reads the client's control stream.
    void pair.server.incomingUnidirectionalStreams
      .getReader()
      .read()
      .then(({ value }) => value?.pipeTo(new WritableStream()))
      .catch(() => {});
    const control = (await pair.server.createUnidirectionalStream()).getWriter();

    await control.write(encodeSetup([{ type: SETUP_OPTION.SOLICIT, value: 1 }]));
    await Promise.all([session.subscriber.ready, session.publisher.ready]);
    session.publisher.announce(NAMESPACE);
    session.publisher.registerTrack(TRACK);
    const solicitation = await solicitNamespace(pair.server, [], 1);
    await vi.waitFor(() => expect(solicitation.received).toHaveLength(2));
    return { pair, session, solicitation, publisherClosed };
  }

  it.each([
    ['close()', (session: ReturnType<typeof createMoqtDuplexSession>) => session.subscriber.close()],
    ['destroy()', (session: ReturnType<typeof createMoqtDuplexSession>) => session.subscriber.destroy()],
  ])('lets the publisher drain when the subscriber’s own %s ends the session', async (_name, end) => {
    const { session, solicitation, publisherClosed } = await harness();

    end(session);
    await vi.waitFor(() => expect(publisherClosed).toHaveBeenCalled());
    expect(solicitation.received.map((m) => m.kind)).toContain('namespace-done');
  });

  it('closes the transport with the code and reason the subscriber’s close() was given', async () => {
    const { pair, session } = await harness();

    session.subscriber.close(0x10, 'done here');
    await expect(pair.server.closed).resolves.toEqual({ closeCode: 0x10, reason: 'done here' });
  });

  it('releases a routed request stream once the driver has read it to its end', async () => {
    const routed: BidirectionalStreamLike[] = [];
    const { pair } = await harness((transport) => ({
      ...transport,
      incomingBidirectionalStreams: transport.incomingBidirectionalStreams.pipeThrough(
        new TransformStream({
          transform(stream, controller) {
            routed.push(stream);
            controller.enqueue(stream);
          },
        })
      ),
    }));

    const publish = await openRawRequest(pair.server, encodePublish({ requestId: 3, ...TRACK, trackAlias: 9 }));
    await vi.waitFor(() => expect(publish.received.map((m) => m.kind)).toEqual(['request-error']));
    await vi.waitFor(() => expect(routed.at(-1)!.readable.locked).toBe(false));
  });
});
