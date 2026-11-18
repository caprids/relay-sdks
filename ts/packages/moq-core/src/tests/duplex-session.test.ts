import { describe, expect, it, vi } from 'vitest';

import {
  ControlMessageDeframer,
  decodeControlMessage,
  encodeGoaway,
  encodePublish,
  encodeSetup,
  encodeSubscribe,
  MESSAGE_TYPE,
  type MoqtProtocolId,
  SETUP_OPTION,
} from '../protocol/control-messages.js';
import { createMoqtDuplexSession, type MoqtDuplexSessionConfig } from '../protocol/duplex-session.js';
import { SESSION_ERROR } from '../protocol/errors.js';
import type { MoqtTransport } from '../protocol/session.js';
import { createSubgroupWriter } from '../protocol/subgroup-writer.js';
import { decodeVarint } from '../protocol/varint.js';
import { rawSubscribe, solicitNamespace } from './helpers/raw-peer.js';
import { createTransportPair } from './helpers/transport-pair.js';

const OWN = ['room', 'p', 'alice'];
const OTHER = ['room', 'p', 'bob'];

/** Read the first varint of each stream the client opens, by direction. */
function watchClientStreams(server: MoqtTransport) {
  const uni: number[] = [];
  const bidi: { type: number; first: ReturnType<typeof decodeControlMessage> | undefined }[] = [];
  void (async () => {
    const reader = server.incomingUnidirectionalStreams.getReader();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) return;
      void value
        .getReader()
        .read()
        .then(({ value: bytes }) => {
          if (bytes) uni.push(decodeVarint(bytes).value);
        });
    }
  })();
  void (async () => {
    const reader = server.incomingBidirectionalStreams.getReader();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) return;
      void value.readable
        .getReader()
        .read()
        .then(({ value: bytes }) => {
          if (!bytes) return;
          const frames = new ControlMessageDeframer().push(bytes);
          bidi.push({
            type: decodeVarint(bytes).value,
            first: frames[0] ? decodeControlMessage(frames[0]) : undefined,
          });
        });
    }
  })();
  return { uni, bidi };
}

async function harness(config: MoqtDuplexSessionConfig = {}, protocol?: MoqtProtocolId) {
  const pair = createTransportPair(protocol === undefined ? {} : { protocol });
  const seen = watchClientStreams(pair.server);
  const session = createMoqtDuplexSession(pair.client, config);
  const control = (await pair.server.createUnidirectionalStream()).getWriter();
  await control.write(encodeSetup([{ type: SETUP_OPTION.SOLICIT, value: 1 }]));
  await Promise.all([session.subscriber.ready, session.publisher.ready]);
  return { pair, session, seen, control };
}

describe('createMoqtDuplexSession', () => {
  it('sends one SETUP, with the subscriber’s options, and readies both sides on the peer’s', async () => {
    const serverOptions: unknown[] = [];
    const { session, seen } = await harness({
      subscriber: {
        setupOptions: [{ type: SETUP_OPTION.SOLICIT, value: 1 }],
        callbacks: { onReady: (options) => serverOptions.push(options) },
      },
    });

    await vi.waitFor(() => expect(seen.uni).toEqual([MESSAGE_TYPE.SETUP]));
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(seen.uni).toEqual([MESSAGE_TYPE.SETUP]);
    expect(serverOptions).toEqual([[{ type: SETUP_OPTION.SOLICIT, value: 1 }]]);
    session.subscriber.destroy();
  });

  it('announces on the peer’s solicitation and serves its SUBSCRIBE with data on the same transport', async () => {
    const { pair, session, seen } = await harness();
    session.publisher.announce(OWN);
    session.publisher.registerTrack({ trackNamespace: OWN, trackName: 'video' });

    const solicitation = await solicitNamespace(pair.server, ['room'], 1);
    await vi.waitFor(() => expect(solicitation.received.map((m) => m.kind)).toEqual(['request-ok', 'namespace']));
    expect(solicitation.received[1]).toMatchObject({ trackNamespaceSuffix: ['p', 'alice'] });

    const subscribe = await rawSubscribe(pair.server, OWN, 'video', 3);
    await vi.waitFor(() => expect(subscribe.received.map((m) => m.kind)).toEqual(['subscribe-ok']));

    const writer = createSubgroupWriter(await session.publisher.openUniStream(3), {
      trackAlias: 3,
      groupId: 0,
      hasProperties: true,
    });
    await writer.writeObject({ objectId: 0, payload: new Uint8Array([1]) });
    await writer.fin();
    await vi.waitFor(() => expect(seen.uni).toHaveLength(2));
    session.subscriber.destroy();
  });

  it('reports announces through the publisher config', async () => {
    const announced: string[][] = [];
    const { pair, session } = await harness({
      publisher: { callbacks: { onAnnounced: ({ namespace }) => announced.push(namespace) } },
    });
    session.publisher.announce(OWN);
    session.publisher.registerTrack({ trackNamespace: OWN, trackName: 'video' });
    await solicitNamespace(pair.server, [], 1);
    await vi.waitFor(() => expect(announced).toEqual([OWN]));
    session.subscriber.destroy();
  });

  it('sends this endpoint’s requests from the subscriber, with its own request IDs', async () => {
    const { session, seen } = await harness();

    session.subscriber.subscribeNamespace({ trackNamespacePrefix: ['room', 'p'] });
    session.subscriber.subscribe({ trackNamespace: OTHER, trackName: 'video' }, {});

    await vi.waitFor(() => expect(seen.bidi).toHaveLength(2));
    expect(seen.bidi.map(({ type }) => type).sort()).toEqual(
      [MESSAGE_TYPE.SUBSCRIBE, MESSAGE_TYPE.SUBSCRIBE_NAMESPACE].sort()
    );
    expect(seen.bidi.map(({ first }) => (first as { requestId: number }).requestId).sort()).toEqual([0, 2]);
    session.subscriber.destroy();
  });

  it('routes the peer’s PUBLISH to the subscriber', async () => {
    const offered: string[] = [];
    const { pair, session } = await harness({
      subscriber: {
        callbacks: {
          onIncomingPublish: (publish, responder) => {
            offered.push(publish.trackName);
            responder.reject();
          },
        },
      },
    });

    const stream = await pair.server.createBidirectionalStream();
    await stream.writable
      .getWriter()
      .write(encodePublish({ requestId: 5, trackNamespace: OTHER, trackName: 'offered', trackAlias: 9 }));
    await vi.waitFor(() => expect(offered).toEqual(['offered']));
    session.subscriber.destroy();
  });

  it('hands the peer’s GOAWAY to both sides', async () => {
    const subscriberGoaways = vi.fn();
    const publisherGoaways = vi.fn();
    const { control, session } = await harness({
      subscriber: { callbacks: { onGoaway: subscriberGoaways } },
      publisher: { callbacks: { onGoaway: publisherGoaways } },
    });

    await control.write(encodeGoaway(1500, 'https://relay.example.com/next'));
    await vi.waitFor(() => {
      expect(subscriberGoaways).toHaveBeenCalledTimes(1);
      expect(publisherGoaways).toHaveBeenCalledTimes(1);
    });
    expect(publisherGoaways.mock.calls[0]![0]).toEqual(subscriberGoaways.mock.calls[0]![0]);
    session.subscriber.destroy();
  });

  it('ends the whole session when the publisher closes, after retracting its announces', async () => {
    const closed = vi.fn();
    const { pair, session } = await harness({ subscriber: { callbacks: { onClosed: closed } } });
    session.publisher.announce(OWN);
    session.publisher.registerTrack({ trackNamespace: OWN, trackName: 'video' });
    const solicitation = await solicitNamespace(pair.server, [], 1);
    await vi.waitFor(() => expect(solicitation.received).toHaveLength(2));

    session.publisher.close();
    await vi.waitFor(() => {
      expect(solicitation.received.map((m) => m.kind)).toContain('namespace-done');
      expect(closed).toHaveBeenCalled();
    });
  });
  it('ends the subscriber at once with the error, and leaves the transport to the publisher’s drain', async () => {
    const subscriberClosed: unknown[] = [];
    const publisherClosed: unknown[] = [];
    const { pair, session } = await harness({
      subscriber: { callbacks: { onClosed: (info) => subscriberClosed.push(info) } },
      publisher: { callbacks: { onClosed: (info) => publisherClosed.push(info) } },
    });
    session.publisher.announce(OWN);
    session.publisher.registerTrack({ trackNamespace: OWN, trackName: 'video' });
    const solicitation = await solicitNamespace(pair.server, [], 1);
    await vi.waitFor(() => expect(solicitation.received).toHaveLength(2));

    const reason = new Error('going');
    session.destroy(reason);
    session.destroy();
    expect(subscriberClosed).toEqual([{ error: reason }]);
    // The retraction still reaches the peer: the subscriber's end did not close the transport under the drain.
    await vi.waitFor(() => {
      expect(solicitation.received.map((m) => m.kind)).toContain('namespace-done');
      expect(publisherClosed).toEqual([{ error: reason }]);
    });
  });

  it('closes the session on a peer request ID reused across the two drivers (§6.4.2.1)', async () => {
    const closed: { error?: unknown }[] = [];
    const { pair } = await harness({ subscriber: { callbacks: { onClosed: (info) => closed.push(info) } } });

    const subscribe = await rawSubscribe(pair.server, OWN, 'video', 1);
    await vi.waitFor(() => expect(subscribe.received).toHaveLength(1));
    const stream = await pair.server.createBidirectionalStream();
    await stream.writable
      .getWriter()
      .write(encodePublish({ requestId: 1, trackNamespace: OTHER, trackName: 'offered', trackAlias: 9 }));

    await vi.waitFor(() => expect(closed).toHaveLength(1));
    expect(closed[0]!.error).toMatchObject({ code: SESSION_ERROR.INVALID_REQUEST_ID });
  });

  it('routes a request stream with an unrepresentable type at once instead of buffering it', async () => {
    const closed: { error?: unknown }[] = [];
    const { pair } = await harness({ subscriber: { callbacks: { onClosed: (info) => closed.push(info) } } });

    const stream = await pair.server.createBidirectionalStream();
    const writer = stream.writable.getWriter();
    await writer.write(new Uint8Array(9).fill(0xff));
    await writer.write(new Uint8Array(64));

    // The subscriber reads it and refuses the session; nothing waits for more bytes.
    await vi.waitFor(() => expect(closed).toHaveLength(1));
    expect(closed[0]!.error).toBeDefined();
  });
  it('waits for a whole nine-byte request type before routing it', async () => {
    const { pair, session } = await harness();
    session.publisher.announce(OWN);
    session.publisher.registerTrack({ trackNamespace: OWN, trackName: 'video' });
    await solicitNamespace(pair.server, [], 1);

    // SUBSCRIBE (0x03) spelled with MOQT's longest prefix, 0xff and eight value bytes, split after eight bytes.
    const subscribe = encodeSubscribe({ requestId: 3, trackNamespace: OWN, trackName: 'video' });
    const longType = new Uint8Array([0xff, 0, 0, 0, 0, 0, 0, 0, 0x03]);
    const bytes = new Uint8Array(longType.length + subscribe.length - 1);
    bytes.set(longType);
    bytes.set(subscribe.subarray(1), longType.length);
    const stream = await pair.server.createBidirectionalStream();
    const writer = stream.writable.getWriter();
    await writer.write(bytes.subarray(0, 8));
    await new Promise((resolve) => setTimeout(resolve, 10));
    await writer.write(bytes.subarray(8));

    const { value } = await stream.readable.getReader().read();
    const [frame] = new ControlMessageDeframer().push(value!);
    expect(decodeControlMessage(frame!).kind).toBe('subscribe-ok');
    session.destroy();
  });
  it('ends the session when the peer stops this endpoint’s control stream (§6.3)', async () => {
    const pair = createTransportPair();
    const closed: { error?: unknown }[] = [];
    createMoqtDuplexSession(pair.client, { subscriber: { callbacks: { onClosed: (info) => closed.push(info) } } });
    const control = (await pair.server.createUnidirectionalStream()).getWriter();
    await control.write(encodeSetup([]));

    const { value: clientControl } = await pair.server.incomingUnidirectionalStreams.getReader().read();
    await clientControl!.cancel(new Error('stop'));

    await vi.waitFor(() => expect(closed).toHaveLength(1));
    expect(closed[0]!.error).toBeDefined();
  });
  it('reports a clean peer close as clean, though it fails the control stream too', async () => {
    const pair = createTransportPair();
    const closed: { error?: unknown }[] = [];
    createMoqtDuplexSession(pair.client, { subscriber: { callbacks: { onClosed: (info) => closed.push(info) } } });
    const control = (await pair.server.createUnidirectionalStream()).getWriter();
    await control.write(encodeSetup([]));
    await new Promise((resolve) => setTimeout(resolve, 10));

    pair.server.close({ closeCode: 0, reason: '' });
    await vi.waitFor(() => expect(closed).toHaveLength(1));
    expect(closed[0]).toEqual({});
  });
});
