import { describe, expect, it, vi } from 'vitest';

import {
  type ControlMessage,
  ControlMessageDeframer,
  decodeControlMessage,
  encodeGoaway,
  encodePublish,
  encodePublishDone,
  encodeRequestUpdate,
  encodeSetup,
  encodeSubscribe,
  encodeSubscribeNamespace,
  encodeSubscribeOk,
  PUBLISH_DONE_STATUS,
  REQUEST_ERROR_CODE,
  STREAM_RESET_CODE,
  type TrackNamespace,
} from '../protocol/control-messages.js';
import { SESSION_ERROR } from '../protocol/errors.js';
import type { MoqtObject } from '../protocol/object-stream.js';
import {
  CLOSE_END_ACK_TIMEOUT_MS,
  CLOSE_FLUSH_TIMEOUT_MS,
  createMoqtPublishSession,
  type IncomingSubscribe,
  type MoqtPublishSessionCallbacks,
} from '../protocol/publish-session.js';
import { createMoqtSession, type MoqtSession, type MoqtTransport, type PublishDone } from '../protocol/session.js';
import { createSubgroupWriter } from '../protocol/subgroup-writer.js';
import {
  openRawRequest,
  type RawRequest,
  rawSubscribe as rawSubscribeTo,
  solicitNamespace,
} from './helpers/raw-peer.js';
import {
  createTransportPair,
  type PipeEvent,
  setRequestIdParity,
  withServerRequestIds,
} from './helpers/transport-pair.js';

const NAMESPACE = ['live', 'abc123'];

/** The relay's per-track pull under the test namespace. */
function rawSubscribe(server: Parameters<typeof rawSubscribeTo>[0], trackName: string, requestId: number) {
  return rawSubscribeTo(server, NAMESPACE, trackName, requestId);
}

function makePublishHarness(
  callbacks: MoqtPublishSessionCallbacks = {},
  { bufferBytes }: { bufferBytes?: number } = {}
) {
  const events: PipeEvent[] = [];
  const pair = createTransportPair({ events, ...(bufferBytes === undefined ? {} : { bufferBytes }) });
  const subscriber = createMoqtSession(withServerRequestIds(pair.server), { unknownAliasTimeoutMs: 500 });
  const session = createMoqtPublishSession(pair.client, { callbacks });

  return { pair, session, subscriber, events };
}

/** PUBLISH_DONE spelled out byte by byte (§10.12), for statuses and counts under 64. */
function publishDoneBytes(status: number, streamCount: number, reason: string): number[] {
  const encoded = [...new TextEncoder().encode(reason)];
  const body = [status, streamCount, encoded.length, ...encoded];

  return [0x0b, 0x00, body.length, ...body];
}

/** What the publisher wrote on a subscription's request stream after SUBSCRIBE_OK. */
function afterSubscribeOk(request: RawRequest, trackAlias: number): number[] {
  const raw = request.raw();
  const subscribeOk = encodeSubscribeOk(trackAlias, {}, []);

  expect([...raw.subarray(0, subscribeOk.length)]).toEqual([...subscribeOk]);
  return [...raw.subarray(subscribeOk.length)];
}

const PUBLISH_DONE_TYPE = 0x0b;

/** Collect, in order, the pair-level writables under the data streams the publisher opens on `transport`. */
function recordDataStreams(transport: MoqtTransport): WritableStream<Uint8Array>[] {
  const streams: WritableStream<Uint8Array>[] = [];
  const create = transport.createUnidirectionalStream.bind(transport);

  transport.createUnidirectionalStream = async () => {
    const stream = await create();

    streams.push(stream);
    return stream;
  };
  return streams;
}

function collectTrack(
  subscriber: MoqtSession,
  trackName: string
): { objects: MoqtObject[]; aliases: number[]; subscription: ReturnType<MoqtSession['subscribe']> } {
  const objects: MoqtObject[] = [];
  const aliases: number[] = [];
  const subscription = subscriber.subscribe(
    { trackNamespace: NAMESPACE, trackName },
    {
      onOk: (ok) => aliases.push(ok.trackAlias),
      onObject: (object) => objects.push(object),
    }
  );

  return { objects, aliases, subscription };
}

function decodeFrames(bytes: Uint8Array): ControlMessage[] {
  return new ControlMessageDeframer().push(bytes).map(decodeControlMessage);
}

describe('createMoqtPublishSession', () => {
  it('completes SETUP both ways against the existing subscribe driver', async () => {
    const { session, subscriber } = makePublishHarness();

    await expect(session.ready).resolves.toBeUndefined();
    await expect(subscriber.ready).resolves.toBeUndefined();
    session.destroy();
    subscriber.destroy();
  });

  it('accepts a namespace solicitation and announces the namespace on it', async () => {
    const announced: TrackNamespace[] = [];
    const { pair, session, subscriber } = makePublishHarness({
      onAnnounced: ({ namespace }) => announced.push(namespace),
    });

    await session.ready;
    session.announce(NAMESPACE);
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'catalog' });

    const solicitation = await solicitNamespace(pair.server, []);

    await vi.waitFor(() => {
      expect(solicitation.received.map((m) => m.kind)).toEqual(['request-ok', 'namespace']);
    });
    // Empty prefix → the suffix is the full namespace.
    expect(solicitation.received[1]).toMatchObject({ kind: 'namespace', trackNamespaceSuffix: NAMESPACE });
    expect(announced).toEqual([NAMESPACE]);
    session.destroy();
    subscriber.destroy();
  });

  it('defers the announce until the first track is registered', async () => {
    const { pair, session, subscriber } = makePublishHarness();

    await session.ready;
    session.announce(NAMESPACE);

    // Solicited and announced, but nothing to serve yet: an announce now
    // would invite SUBSCRIBEs that DOES_NOT_EXIST terminally.
    const solicitation = await solicitNamespace(pair.server, []);

    await vi.waitFor(() => {
      expect(solicitation.received.map((m) => m.kind)).toEqual(['request-ok']);
    });
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(solicitation.received).toHaveLength(1);

    // The first registration releases it.
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'catalog' });
    await vi.waitFor(() => {
      expect(solicitation.received.map((m) => m.kind)).toEqual(['request-ok', 'namespace']);
    });
    session.destroy();
    subscriber.destroy();
  });

  it('announces on a solicitation that arrived before announce()', async () => {
    const announced: TrackNamespace[] = [];
    const { pair, session, subscriber } = makePublishHarness({
      onAnnounced: ({ namespace }) => announced.push(namespace),
    });

    await session.ready;

    const solicitation = await solicitNamespace(pair.server, []);

    await vi.waitFor(() => {
      expect(solicitation.received.map((m) => m.kind)).toEqual(['request-ok']);
    });

    session.announce(NAMESPACE);
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'catalog' });
    await vi.waitFor(() => {
      expect(solicitation.received.map((m) => m.kind)).toEqual(['request-ok', 'namespace']);
      expect(announced).toEqual([NAMESPACE]);
    });
    session.destroy();
    subscriber.destroy();
  });

  it('announces the suffix relative to a non-empty solicited prefix', async () => {
    const { pair, session, subscriber } = makePublishHarness();

    await session.ready;
    session.announce(NAMESPACE);
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'catalog' });

    const solicitation = await solicitNamespace(pair.server, ['live']);

    await vi.waitFor(() => {
      expect(solicitation.received).toHaveLength(2);
    });
    expect(solicitation.received[1]).toMatchObject({ kind: 'namespace', trackNamespaceSuffix: ['abc123'] });
    session.destroy();
    subscriber.destroy();
  });

  it('accepts but never announces on a solicitation whose prefix does not cover the namespace', async () => {
    const announced: TrackNamespace[] = [];
    const { pair, session, subscriber } = makePublishHarness({
      onAnnounced: ({ namespace }) => announced.push(namespace),
    });

    await session.ready;
    session.announce(NAMESPACE);
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'catalog' });

    expect(session.namespaceSolicited).toBeUndefined();
    const solicitation = await solicitNamespace(pair.server, ['other']);

    await vi.waitFor(() => {
      expect(solicitation.received.map((m) => m.kind)).toEqual(['request-ok']);
    });
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(solicitation.received).toHaveLength(1);
    expect(announced).toEqual([]);
    expect(session.namespaceSolicited).toBe(false);
    const matching = await solicitNamespace(pair.server, ['live'], 3);
    await vi.waitFor(() => expect(matching.received.map((m) => m.kind)).toEqual(['request-ok', 'namespace']));
    expect(session.namespaceSolicited).toBe(true);
    matching.reset();
    await vi.waitFor(() => expect(session.namespaceSolicited).toBe(false));
    session.destroy();
    subscriber.destroy();
  });

  it('reports onAnnounceEnded when the solicitation carrying the announce ends', async () => {
    const endings: unknown[] = [];
    const { pair, session, subscriber } = makePublishHarness({
      onAnnounceEnded: ({ error }) => endings.push(error),
    });

    await session.ready;
    session.announce(NAMESPACE);
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'catalog' });

    const solicitation = await solicitNamespace(pair.server, []);

    await vi.waitFor(() => {
      expect(solicitation.received).toHaveLength(2);
    });

    // The peer withdrawing the namespace subscription (a reset — a clean
    // FIN is only half-closure) takes the announce with it — the ingest
    // path is gone.
    void solicitation.reset();
    await vi.waitFor(() => {
      expect(endings).toHaveLength(1);
    });
    expect(endings[0]).toBeInstanceOf(Error);
    session.destroy();
    subscriber.destroy();
  });

  it('keeps the carrier alive across the requester half-closing its side', async () => {
    const endings: unknown[] = [];
    const { pair, session, subscriber } = makePublishHarness({
      onAnnounceEnded: ({ error }) => endings.push(error),
    });

    await session.ready;
    session.announce(NAMESPACE);
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'catalog' });

    const solicitation = await solicitNamespace(pair.server, []);

    await vi.waitFor(() => {
      expect(solicitation.received.map((m) => m.kind)).toEqual(['request-ok', 'namespace']);
    });

    // §3.3.2: a clean requester FIN only commits to sending no updates —
    // it is not a withdrawal, and the response side keeps carrying the
    // announce.
    void solicitation.fin();
    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(endings).toEqual([]);

    // The carrier is still the retraction path at close().
    session.close();
    await vi.waitFor(() => {
      expect(solicitation.received.map((m) => m.kind)).toEqual(['request-ok', 'namespace', 'namespace-done']);
    });
    subscriber.destroy();
  });

  it('rejects an overlapping namespace solicitation with PREFIX_OVERLAP', async () => {
    const endings: unknown[] = [];
    const { pair, session, subscriber } = makePublishHarness({
      onAnnounceEnded: ({ error }) => endings.push(error),
    });

    await session.ready;
    session.announce(NAMESPACE);
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'catalog' });

    const broad = await solicitNamespace(pair.server, [], 1);

    await vi.waitFor(() => {
      expect(broad.received.map((m) => m.kind)).toEqual(['request-ok', 'namespace']);
    });

    // §10.18: a second solicitation whose prefix nests with an
    // established one would produce duplicate namespace state — refused,
    // and the established carrier is untouched.
    const narrow = await solicitNamespace(pair.server, ['live'], 3);

    await vi.waitFor(() => {
      expect(narrow.received.map((m) => m.kind)).toEqual(['request-error']);
      expect(narrow.ended()).toBe(true);
    });
    expect(narrow.received[0]).toMatchObject({ errorCode: REQUEST_ERROR_CODE.PREFIX_OVERLAP });
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(broad.ended()).toBe(false);
    expect(endings).toEqual([]);
    session.destroy();
    subscriber.destroy();
  });

  it('holds the overlap guard across the acceptance write of a racing solicitation', async () => {
    const endings: unknown[] = [];
    const { pair, session, subscriber } = makePublishHarness({
      onAnnounceEnded: ({ error }) => endings.push(error),
    });

    await session.ready;
    session.announce(NAMESPACE);
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'catalog' });

    // Both solicitations are in flight before either REQUEST_OK write
    // settles — request handlers run concurrently, so §10.18's
    // single-carrier rule must hold across the acceptance await, not
    // only at the synchronous check.
    const [broad, narrow] = await Promise.all([
      solicitNamespace(pair.server, [], 1),
      solicitNamespace(pair.server, ['live'], 3),
    ]);

    await vi.waitFor(() => {
      expect(broad.received.length).toBeGreaterThanOrEqual(1);
      expect(narrow.received.length).toBeGreaterThanOrEqual(1);
    });
    expect([broad.received[0]?.kind, narrow.received[0]?.kind].sort()).toEqual(['request-error', 'request-ok']);
    const refused = broad.received[0]?.kind === 'request-error' ? broad : narrow;
    const accepted = refused === broad ? narrow : broad;

    expect(refused.received[0]).toMatchObject({ errorCode: REQUEST_ERROR_CODE.PREFIX_OVERLAP });
    await vi.waitFor(() => {
      expect(accepted.received.map((m) => m.kind)).toEqual(['request-ok', 'namespace']);
      expect(refused.ended()).toBe(true);
    });
    expect(endings).toEqual([]);
    session.destroy();
    subscriber.destroy();
  });

  it('answers inbound SUBSCRIBE with the request id as the alias and routes REQUEST_UPDATE', async () => {
    const subscribes: IncomingSubscribe[] = [];
    const updates: { requestId: number; updateRequestId: number }[] = [];
    const bindings: { trackName: string; trackAlias: number | undefined }[] = [];
    const { session, subscriber } = makePublishHarness({
      onSubscribe: (subscribe) => subscribes.push(subscribe),
      onRequestUpdate: ({ requestId, updateRequestId }) => updates.push({ requestId, updateRequestId }),
      onTrackBinding: (binding) => bindings.push(binding),
    });

    await session.ready;
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video' });

    const video = collectTrack(subscriber, 'video');

    // The driver's even id reaches the publisher with its low bit set.
    const videoRequestId = video.subscription.requestId + 1;

    await vi.waitFor(() => {
      // The peer's request ids are session-unique, so they double as the
      // alias — two live request ids sharing an alias is a session-fatal
      // Duplicate on moq-lite-rs.
      expect(video.aliases).toEqual([videoRequestId]);
      expect(subscribes.map((s) => s.trackName)).toEqual(['video']);
      expect(bindings).toEqual([{ trackName: 'video', trackAlias: videoRequestId }]);
    });

    // The ack lands as the driver's update completion (`onUpdateOk` sits
    // in the subscribe-time handlers).
    const updateAcks: number[] = [];
    const updated = subscriber.subscribe(
      { trackNamespace: NAMESPACE, trackName: 'video' },
      { onUpdateOk: () => updateAcks.push(1) }
    );

    await vi.waitFor(() => {
      expect(subscribes).toHaveLength(2);
    });
    updated.update({ subscriberPriority: 9 });
    await vi.waitFor(() => {
      // The update is attributed to the subscription it rode in on, while
      // consuming a Request ID of its own (§10.1).
      expect(updates).toEqual([{ requestId: updated.requestId + 1, updateRequestId: expect.any(Number) }]);
      expect(updates[0]?.updateRequestId).not.toBe(updated.requestId + 1);
      expect(updateAcks).toHaveLength(1);
    });
    session.destroy();
    subscriber.destroy();
  });

  it('gives the subscribe driver odd request ids past the one-byte varint range', async () => {
    const closes: unknown[] = [];
    const { session, subscriber } = makePublishHarness({ onClosed: (info) => closes.push(info) });

    await session.ready;

    // 70 subscriptions take the driver's ids 0..138, past 128, the one-byte MOQT varint limit.
    const names = Array.from({ length: 70 }, (_, index) => `track-${index}`);

    for (const trackName of names) session.registerTrack({ trackNamespace: NAMESPACE, trackName });

    const tracks = names.map((trackName) => collectTrack(subscriber, trackName));

    await vi.waitFor(() => {
      expect(tracks.map((track) => track.aliases)).toEqual(tracks.map((track) => [track.subscription.requestId + 1]));
    });
    expect(tracks.at(-1)?.subscription.requestId).toBe(138);
    expect(closes).toEqual([]);
    session.destroy();
    subscriber.destroy();
  });

  it('sets only the request id bit, at every MOQT varint length boundary', () => {
    for (const requestId of [
      0,
      62,
      64,
      126,
      128,
      16_382,
      16_384,
      2 ** 21 - 2,
      2 ** 21,
      2 ** 31,
      2 ** 32 + 2 ** 28,
      2 ** 53 - 2,
    ]) {
      const [subscribe] = decodeFrames(
        setRequestIdParity(encodeSubscribe({ requestId, trackNamespace: NAMESPACE, trackName: 'video' }))
      );

      expect(subscribe).toMatchObject({ kind: 'subscribe', requestId: requestId + 1, trackName: 'video' });
    }

    // SUBSCRIBE_NAMESPACE's type (0x50) takes two bytes in MOQT's encoding.
    const [solicitation] = decodeFrames(
      setRequestIdParity(encodeSubscribeNamespace({ requestId: 64, trackNamespacePrefix: NAMESPACE }))
    );

    expect(solicitation).toMatchObject({
      kind: 'subscribe-namespace',
      requestId: 65,
      trackNamespacePrefix: NAMESPACE,
    });
  });

  it('does not declare a media timescale for raw tracks', async () => {
    const { pair, session, subscriber } = makePublishHarness();

    await session.ready;
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video' });

    const subscribe = await rawSubscribe(pair.server, 'video', 11);

    await vi.waitFor(() => {
      expect(subscribe.received).toHaveLength(1);
    });
    // Undeclared, moq-lite-rs relays parse and DISCARD the objects'
    // TIMESTAMP extensions and re-stamp frames on arrival — viewers would
    // sync to relay arrival time instead of capture time.
    expect(subscribe.received[0]).toMatchObject({
      kind: 'subscribe-ok',
      trackAlias: 11,
      trackProperties: [],
    });
    session.destroy();
    subscriber.destroy();
  });

  it('rejects inbound SUBSCRIBE for unknown tracks with DOES_NOT_EXIST', async () => {
    const { session, subscriber } = makePublishHarness();

    await session.ready;

    const errors: number[] = [];

    subscriber.subscribe(
      { trackNamespace: NAMESPACE, trackName: 'nope' },
      { onError: (error) => errors.push(error.errorCode) }
    );
    await vi.waitFor(() => {
      expect(errors).toEqual([REQUEST_ERROR_CODE.DOES_NOT_EXIST]);
    });
    session.destroy();
    subscriber.destroy();
  });

  it('ends a track with PUBLISH_DONE TRACK_ENDED, then FIN', async () => {
    const endedRequests: number[] = [];
    const bindings: (number | undefined)[] = [];
    const { pair, session, subscriber } = makePublishHarness({
      onSubscribeEnd: ({ requestId }) => endedRequests.push(requestId),
      onTrackBinding: ({ trackAlias }) => bindings.push(trackAlias),
    });

    await session.ready;
    const track = session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video' });

    const subscribe = await rawSubscribe(pair.server, 'video', 21);

    await vi.waitFor(() => {
      expect(subscribe.received).toHaveLength(1);
    });

    track.end();
    // The local end is authoritative: the subscription is reported ended
    // and the binding cleared immediately — not when the peer eventually
    // closes its request direction.
    expect(endedRequests).toEqual([21]);
    expect(bindings.at(-1)).toBeUndefined();
    await vi.waitFor(() => {
      expect(subscribe.ended()).toBe(true);
    });
    // PUBLISH_DONE before the FIN (§3.3.2): TRACK_ENDED, no data streams
    // opened, then the FIN. moq-relay 0.15.4 and later treat a FIN
    // without it as a failed request.
    expect(subscribe.failure()).toBeUndefined();
    expect(afterSubscribeOk(subscribe, 21)).toEqual(publishDoneBytes(0x2, 0, 'track ended'));
    expect(afterSubscribeOk(subscribe, 21)).toEqual([
      ...encodePublishDone(PUBLISH_DONE_STATUS.TRACK_ENDED, 0, 'track ended'),
    ]);

    // The peer closing its side afterwards must not double-report.
    void subscribe.fin();
    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(endedRequests).toEqual([21]);
    session.destroy();
    subscriber.destroy();
  });

  it('hands the binding to the newest subscription and FINs the replaced stream', async () => {
    const bindings: (number | undefined)[] = [];
    const endedRequests: number[] = [];
    const { pair, session, subscriber } = makePublishHarness({
      onTrackBinding: ({ trackAlias }) => bindings.push(trackAlias),
      onSubscribeEnd: ({ requestId }) => endedRequests.push(requestId),
    });

    await session.ready;
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video' });

    const first = await rawSubscribe(pair.server, 'video', 31);

    await vi.waitFor(() => {
      expect(first.received).toHaveLength(1);
    });

    const second = await rawSubscribe(pair.server, 'video', 33);

    await vi.waitFor(() => {
      expect(second.received).toHaveLength(1);
      // The replaced subscription ends cleanly: PUBLISH_DONE TRACK_ENDED, then FIN.
      expect(first.ended()).toBe(true);
    });
    expect(afterSubscribeOk(first, 31)).toEqual(publishDoneBytes(0x2, 0, 'superseded by a newer subscription'));
    // The relay closes its own side after seeing ours; only then does the
    // replaced subscription's stream fully end.
    void first.fin();
    await vi.waitFor(() => {
      expect(endedRequests).toEqual([31]);
    });
    // Rebinding lands on the newest alias; the replaced stream's own end
    // re-derives the same binding rather than clearing it.
    expect(bindings[0]).toBe(31);
    expect(bindings[bindings.length - 1]).toBe(33);
    expect(bindings).not.toContain(undefined);
    session.destroy();
    subscriber.destroy();
  });

  it('converges on the newest subscription regardless of acceptance order', async () => {
    const bindings: (number | undefined)[] = [];
    const { pair, session, subscriber } = makePublishHarness(
      {
        onTrackBinding: ({ trackAlias }) => bindings.push(trackAlias),
      },
      { bufferBytes: 0 }
    );

    await session.ready;
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video' });

    // Both SUBSCRIBEs land with their acceptances backpressured (unread
    // responses); the OLDER one is then released first.
    const openHeld = async (requestId: number) => {
      const stream = await pair.server.createBidirectionalStream();
      const writer = stream.writable.getWriter();

      await writer.write(encodeSubscribe({ requestId, trackNamespace: NAMESPACE, trackName: 'video', parameters: {} }));
      let ended = false;

      return {
        ended: () => ended,
        drain: () => {
          void (async () => {
            const reader = stream.readable.getReader();

            while (true) {
              const { done } = await reader.read();
              if (done) break;
            }
          })().then(
            () => {
              ended = true;
            },
            () => {
              ended = true;
            }
          );
        },
      };
    };
    const older = await openHeld(91);

    await new Promise((resolve) => setTimeout(resolve, 20));
    const newer = await openHeld(93);

    await new Promise((resolve) => setTimeout(resolve, 20));

    // Older accepts first — it may bind transiently, but must not sweep
    // the newer request that arrived after it.
    older.drain();
    await vi.waitFor(() => {
      expect(bindings.at(-1)).toBe(91);
    });

    // The newer acceptance lands and takes the binding; the older is
    // FINed as its predecessor.
    newer.drain();
    await vi.waitFor(() => {
      expect(bindings.at(-1)).toBe(93);
      expect(older.ended()).toBe(true);
    });
    session.destroy();
    subscriber.destroy();
  });

  it('a stale acceptance does not steal the binding from a newer subscription', async () => {
    const bindings: (number | undefined)[] = [];
    const subscribed: number[] = [];
    const { pair, session, subscriber } = makePublishHarness(
      {
        onTrackBinding: ({ trackAlias }) => bindings.push(trackAlias),
        onSubscribe: ({ requestId }) => subscribed.push(requestId),
      },
      { bufferBytes: 0 }
    );

    await session.ready;
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video' });

    // Subscription A: write the SUBSCRIBE but do NOT read the response —
    // over the in-memory pair a write only settles once the far side
    // reads, so A's SUBSCRIBE_OK hangs in flight.
    const streamA = await pair.server.createBidirectionalStream();
    const writerA = streamA.writable.getWriter();

    await writerA.write(
      encodeSubscribe({ requestId: 61, trackNamespace: NAMESPACE, trackName: 'video', parameters: {} })
    );
    await new Promise((resolve) => setTimeout(resolve, 20));

    // Subscription B reads immediately: its acceptance settles first,
    // replaces A, and takes the binding.
    const b = await rawSubscribe(pair.server, 'video', 63);

    await vi.waitFor(() => {
      expect(b.received).toHaveLength(1);
      expect(bindings.at(-1)).toBe(63);
    });

    // Now drain A: its stale acceptance completes against a subscription
    // that was already replaced — it must not rebind the track.
    const readerA = streamA.readable.getReader();

    void (async () => {
      while (true) {
        const { done } = await readerA.read();
        if (done) break;
      }
    })().catch(() => {});
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(bindings).not.toContain(61);
    expect(bindings.at(-1)).toBe(63);
    // The swept-while-pending subscription was never live, so it is never
    // reported — subscriber counts stay balanced.
    expect(subscribed).toEqual([63]);
    session.destroy();
    subscriber.destroy();
  });

  it('withholds the binding while Forward State is 0 and follows REQUEST_UPDATE toggles', async () => {
    const bindings: (number | undefined)[] = [];
    const { pair, session, subscriber } = makePublishHarness({
      onTrackBinding: ({ trackAlias }) => bindings.push(trackAlias),
    });

    await session.ready;
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video' });

    // `forward: 0` subscribes without authorizing data.
    const sub = await openRawRequest(
      pair.server,
      encodeSubscribe({ requestId: 81, trackNamespace: NAMESPACE, trackName: 'video', parameters: { forward: 0 } })
    );

    await vi.waitFor(() => {
      expect(sub.received.map((m) => m.kind)).toEqual(['subscribe-ok']);
      expect(bindings).toEqual([undefined]);
    });

    // Each REQUEST_UPDATE consumes a Request ID of its own (§10.1).
    await sub.send(encodeRequestUpdate(83, { forward: 1 }));
    await vi.waitFor(() => {
      expect(bindings.at(-1)).toBe(81);
    });

    await sub.send(encodeRequestUpdate(85, { forward: 0 }));
    await vi.waitFor(() => {
      expect(bindings.at(-1)).toBeUndefined();
    });
    // Each accepted update is acknowledged — the subscribe driver treats
    // REQUEST_OK as the update's completion. (The binding update above is
    // synchronous server-side while the ack rides the transport, so this
    // must wait too.)
    await vi.waitFor(() => {
      expect(sub.received.map((m) => m.kind)).toEqual(['subscribe-ok', 'request-ok', 'request-ok']);
    });
    session.destroy();
    subscriber.destroy();
  });

  it('sweeps every live predecessor when a new subscription takes the binding', async () => {
    const bindings: (number | undefined)[] = [];
    const { pair, session, subscriber } = makePublishHarness({
      onTrackBinding: ({ trackAlias }) => bindings.push(trackAlias),
    });

    await session.ready;
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video' });

    // A: fully accepted and bound.
    const a = await rawSubscribe(pair.server, 'video', 71);

    await vi.waitFor(() => {
      expect(bindings.at(-1)).toBe(71);
    });

    // B: acceptance held in flight (unread response).
    const streamB = await pair.server.createBidirectionalStream();
    const writerB = streamB.writable.getWriter();

    await writerB.write(
      encodeSubscribe({ requestId: 73, trackNamespace: NAMESPACE, trackName: 'video', parameters: {} })
    );
    await new Promise((resolve) => setTimeout(resolve, 20));

    // C settles first and must sweep BOTH A and B — leaving A live would
    // let it reclaim the binding when C ends.
    const c = await rawSubscribe(pair.server, 'video', 75);

    await vi.waitFor(() => {
      expect(c.received).toHaveLength(1);
      expect(bindings.at(-1)).toBe(75);
      expect(a.ended()).toBe(true);
    });

    // C ends (the relay closes both directions): the binding clears
    // instead of falling back to the superseded A.
    void c.fin();
    await vi.waitFor(() => {
      expect(bindings.at(-1)).toBeUndefined();
    });
    expect(bindings.filter((alias) => alias === 71)).toEqual([71]);
    session.destroy();
    subscriber.destroy();
  });

  it('surfaces the announce loss when only the response half of the solicitation dies', async () => {
    const endings: unknown[] = [];
    const { pair, session, subscriber } = makePublishHarness({
      onAnnounceEnded: ({ error }) => endings.push(error),
    });

    await session.ready;

    const solicitation = await solicitNamespace(pair.server, []);

    await vi.waitFor(() => {
      expect(solicitation.received.map((m) => m.kind)).toEqual(['request-ok']);
    });

    // The peer resets only the response direction; its request half — the
    // read loop's end signal — stays open, so the entry write's failure
    // is the only evidence the carrier is unusable.
    await solicitation.abandonReads();
    session.announce(NAMESPACE);
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'catalog' });
    await vi.waitFor(() => {
      expect(endings).toHaveLength(1);
    });
    session.destroy();
    subscriber.destroy();
  });

  it('accepts a replacement solicitation after the response half of the carrier dies', async () => {
    const endings: unknown[] = [];
    const { pair, session, subscriber } = makePublishHarness({
      onAnnounceEnded: ({ error }) => endings.push(error),
    });

    await session.ready;

    const dead = await solicitNamespace(pair.server, [], 1);

    await vi.waitFor(() => {
      expect(dead.received.map((m) => m.kind)).toEqual(['request-ok']);
    });
    await dead.abandonReads();
    session.announce(NAMESPACE);
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'catalog' });
    await vi.waitFor(() => {
      expect(endings).toHaveLength(1);
    });

    // The write failure deregistered the dead carrier along with the
    // loss report, so the relay's retry is a fresh start — not a
    // PREFIX_OVERLAP refusal against a corpse.
    const retry = await solicitNamespace(pair.server, [], 3);

    await vi.waitFor(() => {
      expect(retry.received.map((m) => m.kind)).toEqual(['request-ok', 'namespace']);
    });
    session.destroy();
    subscriber.destroy();
  });

  it('rejects a namespace REQUEST_UPDATE and closes the carrier, never the session', async () => {
    const updates: number[] = [];
    const closes: unknown[] = [];
    const endings: unknown[] = [];
    const { pair, session, subscriber } = makePublishHarness({
      onRequestUpdate: ({ requestId }) => updates.push(requestId),
      onClosed: (info) => closes.push(info),
      onAnnounceEnded: ({ error }) => endings.push(error),
    });

    await session.ready;
    session.announce(NAMESPACE);
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'catalog' });

    const solicitation = await solicitNamespace(pair.server, []);

    await vi.waitFor(() => {
      expect(solicitation.received).toHaveLength(2);
    });

    // A legal §10.9 update (on a fresh Request ID) must never be
    // session-fatal; v1 applies no prefix changes, so per §10.9.1 the
    // update is answered with an error and the request stream closes —
    // which costs the announce carrier, surfaced as announce loss.
    await solicitation.send(encodeRequestUpdate(3, { subscriberPriority: 5 }));
    await vi.waitFor(() => {
      expect(updates).toEqual([1]);
      expect(solicitation.received.map((m) => m.kind)).toEqual(['request-ok', 'namespace', 'request-error']);
      expect(solicitation.ended()).toBe(true);
      expect(endings).toHaveLength(1);
    });
    expect(closes).toEqual([]);
    session.destroy();
    subscriber.destroy();
  });

  it('closes the session when the peer reuses a request id', async () => {
    const closes: unknown[] = [];
    const { pair, session, subscriber } = makePublishHarness({
      onClosed: (info) => closes.push(info),
    });

    await session.ready;
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video' });
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'audio' });

    const first = await rawSubscribe(pair.server, 'video', 101);

    await vi.waitFor(() => {
      expect(first.received).toHaveLength(1);
    });

    // Aliases ARE the peer's request ids: a reused id would hand two
    // tracks the same alias and make their subgroup streams
    // indistinguishable — session-fatal with INVALID_REQUEST_ID (§10.1).
    await rawSubscribe(pair.server, 'audio', 101);
    await vi.waitFor(() => {
      expect(closes).toHaveLength(1);
    });
    expect((closes[0] as { error?: unknown }).error).toBeInstanceOf(Error);
    await expect(pair.client.closed).resolves.toEqual({
      closeCode: SESSION_ERROR.INVALID_REQUEST_ID,
      reason: 'peer reused request id 101',
    });
    subscriber.destroy();
  });

  it('closes the session when the peer sends a request id with the client parity', async () => {
    const closes: unknown[] = [];
    const { pair, session, subscriber } = makePublishHarness({
      onClosed: (info) => closes.push(info),
    });

    await session.ready;
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video' });

    // The peer is the server, so its request ids are odd (§10.1).
    await rawSubscribe(pair.server, 'video', 100);
    await vi.waitFor(() => {
      expect(closes).toHaveLength(1);
    });
    await expect(pair.client.closed).resolves.toEqual({
      closeCode: SESSION_ERROR.INVALID_REQUEST_ID,
      reason: 'peer sent even request id 100',
    });
    subscriber.destroy();
  });

  it('closes the session when a REQUEST_UPDATE reuses a request id', async () => {
    const closes: unknown[] = [];
    const { pair, session, subscriber } = makePublishHarness({
      onClosed: (info) => closes.push(info),
    });

    await session.ready;
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video' });

    const sub = await rawSubscribe(pair.server, 'video', 111);

    await vi.waitFor(() => {
      expect(sub.received.map((m) => m.kind)).toEqual(['subscribe-ok']);
    });

    // REQUEST_UPDATE consumes a Request ID of its own (§10.1); riding on
    // the subscription's id is the same duplicate as a repeated SUBSCRIBE.
    await sub.send(encodeRequestUpdate(111, { subscriberPriority: 9 }));
    await vi.waitFor(() => {
      expect(closes).toHaveLength(1);
    });
    expect((closes[0] as { error?: unknown }).error).toBeInstanceOf(Error);
    await expect(pair.client.closed).resolves.toEqual({
      closeCode: SESSION_ERROR.INVALID_REQUEST_ID,
      reason: 'peer reused request id 111',
    });
    subscriber.destroy();
  });

  it('closes the session when a REQUEST_UPDATE carries a request id with the client parity', async () => {
    const closes: unknown[] = [];
    const { pair, session, subscriber } = makePublishHarness({
      onClosed: (info) => closes.push(info),
    });

    await session.ready;
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video' });

    const sub = await rawSubscribe(pair.server, 'video', 111);

    await vi.waitFor(() => {
      expect(sub.received.map((m) => m.kind)).toEqual(['subscribe-ok']);
    });

    await sub.send(encodeRequestUpdate(112, { subscriberPriority: 9 }));
    await vi.waitFor(() => {
      expect(closes).toHaveLength(1);
    });
    await expect(pair.client.closed).resolves.toEqual({
      closeCode: SESSION_ERROR.INVALID_REQUEST_ID,
      reason: 'peer sent even request id 112',
    });
    subscriber.destroy();
  });

  it('reserves request ids across every inbound request kind', async () => {
    const closes: unknown[] = [];
    const { pair, session, subscriber } = makePublishHarness({
      onClosed: (info) => closes.push(info),
    });

    await session.ready;
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video' });

    // A rejected PUBLISH burns its id session-wide…
    const publish = await openRawRequest(
      pair.server,
      encodePublish({ requestId: 121, trackNamespace: NAMESPACE, trackName: 'video', trackAlias: 9 })
    );

    await vi.waitFor(() => {
      expect(publish.received.map((m) => m.kind)).toEqual(['request-error']);
    });
    expect(closes).toEqual([]);

    // …so a later SUBSCRIBE cannot reuse it as an alias.
    await rawSubscribe(pair.server, 'video', 121);
    await vi.waitFor(() => {
      expect(closes).toHaveLength(1);
    });
    await expect(pair.client.closed).resolves.toMatchObject({ closeCode: SESSION_ERROR.INVALID_REQUEST_ID });
    subscriber.destroy();
  });

  it('rejects a subscription update it cannot apply instead of acknowledging it', async () => {
    const endedRequests: number[] = [];
    const bindings: (number | undefined)[] = [];
    const { pair, session, subscriber } = makePublishHarness({
      onSubscribeEnd: ({ requestId }) => endedRequests.push(requestId),
      onTrackBinding: ({ trackAlias }) => bindings.push(trackAlias),
    });

    await session.ready;
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video' });

    const sub = await rawSubscribe(pair.server, 'video', 111);

    await vi.waitFor(() => {
      expect(bindings.at(-1)).toBe(111);
    });

    // A filter change is not applied — acknowledging it would leave the
    // peer believing its new range is in effect.
    await sub.send(encodeRequestUpdate(113, { locationFilter: { type: 'relative-group', groupsBeforeNext: 0 } }));
    await vi.waitFor(() => {
      expect(sub.received.map((m) => m.kind)).toEqual(['subscribe-ok', 'request-error', 'publish-done']);
      expect(sub.ended()).toBe(true);
      expect(endedRequests).toEqual([111]);
      expect(bindings.at(-1)).toBeUndefined();
    });
    // A failed update terminates the subscription with PUBLISH_DONE
    // UPDATE_FAILED (§10.9.1).
    expect(sub.received.at(-1)).toEqual({
      kind: 'publish-done',
      statusCode: PUBLISH_DONE_STATUS.UPDATE_FAILED,
      streamCount: 0,
      reason: 'unsupported subscription update',
    });
    session.destroy();
    subscriber.destroy();
  });

  it('treats announce-carrier loss after a GOAWAY as migration, not failure', async () => {
    const goaways: number[] = [];
    const endings: unknown[] = [];
    const { pair, session, subscriber } = makePublishHarness({
      onGoaway: (goaway) => goaways.push(goaway.timeout),
      onAnnounceEnded: ({ error }) => endings.push(error),
    });

    await session.ready;
    session.announce(NAMESPACE);
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'catalog' });

    const solicitation = await solicitNamespace(pair.server, []);

    await vi.waitFor(() => {
      expect(solicitation.received).toHaveLength(2);
    });

    // The peer migrates: GOAWAY, then its streams drain. The announce
    // ending here is the orderly drain — surfacing it as a failure would
    // override the actor's 'draining' with 'failed'.
    await solicitation.send(encodeGoaway(1500));
    await vi.waitFor(() => {
      expect(goaways).toEqual([1500]);
    });
    void solicitation.reset();
    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(endings).toEqual([]);
    session.destroy();
    subscriber.destroy();
  });

  it('clears the binding when the only subscription ends', async () => {
    const bindings: (number | undefined)[] = [];
    const { pair, session, subscriber } = makePublishHarness({
      onTrackBinding: ({ trackAlias }) => bindings.push(trackAlias),
    });

    await session.ready;
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video' });

    const subscribe = await rawSubscribe(pair.server, 'video', 41);

    await vi.waitFor(() => {
      expect(bindings).toEqual([41]);
    });

    void subscribe.fin();
    await vi.waitFor(() => {
      expect(bindings).toEqual([41, undefined]);
      // The response direction closes too — a peer-initiated unsubscribe
      // must not leak a half-open stream.
      expect(subscribe.ended()).toBe(true);
    });
    // The subscription was Established, so PUBLISH_DONE precedes that FIN
    // (§3.3.2).
    expect(afterSubscribeOk(subscribe, 41)).toEqual(publishDoneBytes(0x0, 0, 'subscriber closed its request stream'));
    session.destroy();
    subscriber.destroy();
  });

  it('sends no PUBLISH_DONE for a cancelled subscription and resets its data streams', async () => {
    const bindings: (number | undefined)[] = [];
    const { pair, session, subscriber, events } = makePublishHarness({
      onTrackBinding: ({ trackAlias }) => bindings.push(trackAlias),
    });

    await session.ready;
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video' });

    const subscribe = await rawSubscribe(pair.server, 'video', 43);

    await vi.waitFor(() => {
      expect(bindings).toEqual([43]);
    });
    const dataStreams = recordDataStreams(pair.client);

    await session.openUniStream(43);

    // A reset is a cancellation (§3.3.3): no PUBLISH_DONE, and open data
    // streams are reset (§5.1.1).
    void subscribe.reset();
    await vi.waitFor(() => {
      expect(bindings).toEqual([43, undefined]);
      expect(subscribe.ended()).toBe(true);
    });
    expect(afterSubscribeOk(subscribe, 43)).toEqual([]);
    expect(events.filter((event) => event.stream === dataStreams[0]).map((event) => event.kind)).toEqual(['abort']);
    session.destroy();
    subscriber.destroy();
  });

  it('counts the data streams and closes every one before PUBLISH_DONE', async () => {
    const { pair, session, subscriber, events } = makePublishHarness();

    await session.ready;
    const track = session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video' });

    const subscribe = await rawSubscribe(pair.server, 'video', 61);

    await vi.waitFor(() => {
      expect(subscribe.received).toHaveLength(1);
    });

    const dataStreams = recordDataStreams(pair.client);
    const before = events.length;
    const finished = await session.openUniStream(61);
    const stillOpen = await session.openUniStream(61);
    const [finishedStream, stillOpenStream] = dataStreams;
    const stillOpenWriter = stillOpen.getWriter();

    await finished.close();
    track.end();
    await vi.waitFor(() => {
      expect(subscribe.ended()).toBe(true);
    });
    expect(afterSubscribeOk(subscribe, 61)).toEqual(publishDoneBytes(0x2, 2, 'track ended'));

    // In issue order: the caller's FIN, the session's reset of the stream
    // still open, then PUBLISH_DONE and the request stream's FIN (§10.12:
    // "MUST NOT send PUBLISH_DONE until it has closed all streams").
    const requestStream = events.find((event) => event.bytes[0] === PUBLISH_DONE_TYPE)?.stream;
    const label = new Map([
      [finishedStream, 'finished'],
      [stillOpenStream, 'still open'],
      [requestStream, 'request'],
    ]);
    const steps = events
      .slice(before)
      .filter((event) => label.has(event.stream))
      .map((event) => [label.get(event.stream), event.kind]);

    expect(steps).toEqual([
      ['finished', 'close'],
      ['still open', 'abort'],
      ['request', 'write'],
      ['request', 'close'],
    ]);

    // The stream the session reset is gone for the caller too.
    await expect(stillOpenWriter.write(new Uint8Array([1]))).rejects.toBeDefined();
    session.destroy();
    subscriber.destroy();
  });

  it('surfaces a transport failure on an open data stream to the FIN hand-over, and later ones only to fin()', async () => {
    const { pair, session, subscriber } = makePublishHarness();

    await session.ready;
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video' });
    const subscribe = await rawSubscribe(pair.server, 'video', 71);

    await vi.waitFor(() => {
      expect(subscribe.received).toHaveLength(1);
    });

    // Transport streams that accept writes, settle close only on the peer's ack (never, here), and can be failed as a
    // STOP_SENDING or reset would fail them.
    const transportStreams: { fail(error: unknown): void; close: ReturnType<typeof vi.fn> }[] = [];

    pair.client.createUnidirectionalStream = async () => {
      let controller!: WritableStreamDefaultController;
      let rejectClose: ((error: unknown) => void) | undefined;
      const close = vi.fn(
        () =>
          new Promise<void>((_, reject) => {
            rejectClose = reject;
          })
      );

      transportStreams.push({
        fail(error) {
          controller.error(error);
          rejectClose?.(error);
        },
        close,
      });
      return new WritableStream<Uint8Array>({
        start(c) {
          controller = c;
        },
        close,
      });
    };

    const failure = new Error('STOP_SENDING before the FIN');
    const early = createSubgroupWriter(await session.openUniStream(71), { trackAlias: 71, groupId: 0 });

    await early.writeObject({ objectId: 0, payload: new Uint8Array([1]) });
    transportStreams[0]?.fail(failure);
    await expect(early.handOverFin()).rejects.toBe(failure);
    await expect(early.fin()).rejects.toBe(failure);
    expect(transportStreams[0]?.close).not.toHaveBeenCalled();

    const lateFailure = new Error('reset after the FIN');
    const late = createSubgroupWriter(await session.openUniStream(71), { trackAlias: 71, groupId: 1 });

    await late.writeObject({ objectId: 0, payload: new Uint8Array([2]) });
    await late.handOverFin();
    await vi.waitFor(() => expect(transportStreams[1]?.close).toHaveBeenCalled());
    transportStreams[1]?.fail(lateFailure);
    // Nobody asked about the late failure yet: an unhandled rejection would fail the run.
    await new Promise((resolve) => setTimeout(resolve, 0));
    await expect(late.fin()).rejects.toBe(lateFailure);
    session.destroy();
    subscriber.destroy();
  });

  it('opens data streams only for a live subscription', async () => {
    const { pair, session, subscriber } = makePublishHarness();

    await session.ready;
    const track = session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video' });

    await expect(session.openUniStream(81)).rejects.toThrow('no live subscription');

    const subscribe = await rawSubscribe(pair.server, 'video', 81);

    await vi.waitFor(() => {
      expect(subscribe.received).toHaveLength(1);
    });
    await session.openUniStream(81);

    track.end();
    // Ended: its PUBLISH_DONE already carries the count, so no further
    // stream may join it.
    await expect(session.openUniStream(81)).rejects.toThrow('no live subscription');
    await vi.waitFor(() => {
      expect(subscribe.ended()).toBe(true);
    });
    expect(afterSubscribeOk(subscribe, 81)).toEqual(publishDoneBytes(0x2, 1, 'track ended'));
    session.destroy();
    subscriber.destroy();
  });

  it('resets a data stream that finishes opening after its subscription ended', async () => {
    const { pair, session, subscriber, events } = makePublishHarness();

    await session.ready;
    const track = session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video' });

    const subscribe = await rawSubscribe(pair.server, 'video', 91);

    await vi.waitFor(() => {
      expect(subscribe.received).toHaveLength(1);
    });

    // Hold the open in flight across the end.
    let release!: () => void;
    const released = new Promise<void>((resolve) => {
      release = resolve;
    });
    const create = pair.client.createUnidirectionalStream.bind(pair.client);

    pair.client.createUnidirectionalStream = async () => {
      await released;
      return create();
    };
    const before = events.length;
    const opening = session.openUniStream(91);

    track.end();
    await new Promise((resolve) => setTimeout(resolve, 10));
    // PUBLISH_DONE waits for the open to settle.
    expect(subscribe.ended()).toBe(false);

    release();
    await expect(opening).rejects.toThrow('ended');
    await vi.waitFor(() => {
      expect(subscribe.ended()).toBe(true);
    });
    // The late stream was reset before PUBLISH_DONE and is not counted.
    const kinds = events
      .slice(before)
      .filter((event) => event.kind === 'abort' || event.bytes[0] === PUBLISH_DONE_TYPE)
      .map((event) => event.kind);

    expect(kinds).toEqual(['abort', 'write']);
    expect(afterSubscribeOk(subscribe, 91)).toEqual(publishDoneBytes(0x2, 0, 'track ended'));
    session.destroy();
    subscriber.destroy();
  });

  it('drains a peer-initiated end still waiting on a data-stream open before closing the transport', async () => {
    const bindings: (number | undefined)[] = [];
    const { pair, session, subscriber, events } = makePublishHarness({
      onTrackBinding: ({ trackAlias }) => bindings.push(trackAlias),
    });

    await session.ready;
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video' });

    const subscribe = await rawSubscribe(pair.server, 'video', 95);

    await vi.waitFor(() => {
      expect(bindings).toEqual([95]);
    });

    // Hold a data-stream open in flight, and mark where the transport closes in the event log.
    let release!: () => void;
    const released = new Promise<void>((resolve) => {
      release = resolve;
    });
    const create = pair.client.createUnidirectionalStream.bind(pair.client);
    const closeTransport = pair.client.close.bind(pair.client);
    let transportClosedAt: number | undefined;

    pair.client.createUnidirectionalStream = async () => {
      await released;
      return create();
    };
    pair.client.close = (info) => {
      transportClosedAt ??= events.length;
      closeTransport(info);
    };
    const opening = session.openUniStream(95).catch(() => {});

    // The subscriber FINs while the open is pending: its end waits on the open, and the subscription leaves the track.
    void subscribe.fin();
    await vi.waitFor(() => {
      expect(bindings).toEqual([95, undefined]);
    });

    session.close();
    await new Promise((resolve) => setTimeout(resolve, 10));
    // The close drain waits for that end instead of cutting it off.
    expect(transportClosedAt).toBeUndefined();

    release();
    await opening;
    await vi.waitFor(() => {
      expect(transportClosedAt).toBeDefined();
    });
    expect(subscribe.ended()).toBe(true);
    expect(afterSubscribeOk(subscribe, 95)).toEqual(publishDoneBytes(0x0, 0, 'subscriber closed its request stream'));

    // PUBLISH_DONE and the request stream's FIN both precede the transport close.
    const requestStream = events.find((event) => event.bytes[0] === PUBLISH_DONE_TYPE)?.stream;
    const requestSteps = events
      .map((event, index) => ({ ...event, index }))
      .filter(
        (event) => event.stream === requestStream && (event.kind === 'close' || event.bytes[0] === PUBLISH_DONE_TYPE)
      );

    expect(requestSteps.map((event) => event.kind)).toEqual(['write', 'close']);
    expect(requestSteps.every((event) => event.index < transportClosedAt!)).toBe(true);
    subscriber.destroy();
  });

  it.each([
    ['in a browser, a WebTransportError', true],
    ['elsewhere, a stream error of the SDK', false],
  ])("resets a superseded subscription's data streams with CANCELLED: %s (TA-17, TA-22)", async (_, browser) => {
    // Spec-shaped: WebTransport reads a reset's code only from this native type's `streamErrorCode`.
    class FakeWebTransportError extends Error {
      readonly source: string;
      readonly streamErrorCode: number | null;
      constructor(message = '', options: { source?: string; streamErrorCode?: number } = {}) {
        super(message);
        this.source = options.source ?? 'stream';
        this.streamErrorCode = options.streamErrorCode ?? null;
      }
    }
    if (browser) vi.stubGlobal('WebTransportError', FakeWebTransportError);

    try {
      const { pair, session, subscriber } = makePublishHarness();

      await session.ready;
      session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video' });

      const older = await rawSubscribe(pair.server, 'video', 97);

      await vi.waitFor(() => {
        expect(older.received).toHaveLength(1);
      });

      const reasons: unknown[] = [];
      const create = pair.client.createUnidirectionalStream.bind(pair.client);

      pair.client.createUnidirectionalStream = async () => {
        const inner = (await create()).getWriter();

        return new WritableStream<Uint8Array>({
          write: (chunk) => inner.write(chunk),
          close: () => inner.close(),
          abort: (reason) => {
            reasons.push(reason);
            return inner.abort(reason);
          },
        });
      };
      await session.openUniStream(97);

      // A newer subscription supersedes the older one; the caller never closed its data stream.
      await rawSubscribe(pair.server, 'video', 99);
      await vi.waitFor(() => {
        expect(older.ended()).toBe(true);
      });

      expect(reasons).toHaveLength(1);
      expect(reasons[0]).toBeInstanceOf(browser ? FakeWebTransportError : Error);
      expect(reasons[0]).toMatchObject({ source: 'stream', streamErrorCode: STREAM_RESET_CODE.CANCELLED });
      session.destroy();
      subscriber.destroy();
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it('the subscriber driver reports the PUBLISH_DONE of an ended track', async () => {
    const subscribes: IncomingSubscribe[] = [];
    const { session, subscriber } = makePublishHarness({ onSubscribe: (s) => subscribes.push(s) });

    await session.ready;
    const track = session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video' });

    const done: PublishDone[] = [];
    subscriber.subscribe({ trackNamespace: NAMESPACE, trackName: 'video' }, { onDone: (d) => done.push(d) });

    await vi.waitFor(() => {
      expect(subscribes).toHaveLength(1);
    });
    // The alias is the request ID the relay-side driver put on the wire.
    const data = await session.openUniStream(subscribes[0]!.requestId);

    await data.close();
    track.end();
    await vi.waitFor(() => {
      expect(done).toHaveLength(1);
    });
    expect(done[0]).toMatchObject({
      statusCode: PUBLISH_DONE_STATUS.TRACK_ENDED,
      streamCount: 1,
      reason: 'track ended',
    });
    session.destroy();
    subscriber.destroy();
  });

  it.each([false, true])('drains FINs and NAMESPACE_DONE, including destroy with an error: %s', async (withError) => {
    const pair = createTransportPair();
    // A fully raw peer: complete SETUP and record every frame on the
    // publisher's control stream — it must carry SETUP and nothing else:
    // the publisher sends no GOAWAY (moq-relay 0.14.17 closes the session
    // on a client GOAWAY, code 17; 0.15.1 and later keep it).
    const controlFrames: ControlMessage[] = [];

    void (async () => {
      const control = await pair.server.createUnidirectionalStream();
      const writer = control.getWriter();

      await writer.write(encodeSetup([]));
      const reader = pair.server.incomingUnidirectionalStreams.getReader();
      const { value: publisherControl } = await reader.read();
      const deframer = new ControlMessageDeframer();
      const streamReader = publisherControl!.getReader();

      while (true) {
        const { done, value } = await streamReader.read().catch(() => ({ done: true, value: undefined }) as const);
        if (done) break;

        for (const frame of deframer.push(value!)) controlFrames.push(decodeControlMessage(frame));
      }
    })();
    const onClosed = vi.fn();
    const session = createMoqtPublishSession(pair.client, { callbacks: { onClosed } });

    await session.ready;
    session.announce(NAMESPACE);
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'catalog' });
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video' });

    const solicitation = await solicitNamespace(pair.server, []);
    const subscribe = await rawSubscribe(pair.server, 'video', 51);

    await vi.waitFor(() => {
      expect(solicitation.received).toHaveLength(2);
      expect(subscribe.received).toHaveLength(1);
    });

    const error = new Error('migration requested');
    if (withError) session.destroy(error);
    else session.close();
    await vi.waitFor(() => {
      // The announce is retracted and the subscription ended cleanly
      // (PUBLISH_DONE, then FIN) before the transport goes away.
      expect(solicitation.received.map((m) => m.kind)).toEqual(['request-ok', 'namespace', 'namespace-done']);
      expect(subscribe.ended()).toBe(true);
    });
    expect(afterSubscribeOk(subscribe, 51)).toEqual(publishDoneBytes(0x2, 0, 'track ended'));
    expect(controlFrames.map((m) => m.kind)).toEqual(['setup']);
    // Like moq-relay once it has consumed the PUBLISH_DONE: FIN back, which releases the close.
    await subscribe.fin();
    await expect(pair.client.closed).resolves.toEqual({ closeCode: 0, reason: '' });
    expect(onClosed).toHaveBeenCalledExactlyOnceWith(withError ? { error } : {});
  });

  it.each([false, true])(
    'finishes the publisher drain after GOAWAY and control EOF (local error: %s)',
    async (withError) => {
      const pair = createTransportPair();
      const control = await pair.server.createUnidirectionalStream();
      const writer = control.getWriter();
      const error = new Error('migration requested');
      const onClosed = vi.fn();
      const session = createMoqtPublishSession(pair.client, {
        callbacks: {
          onClosed,
          onGoaway: () => (withError ? session.destroy(error) : session.close()),
        },
      });
      await writer.write(encodeSetup([]));
      await session.ready;
      session.announce(NAMESPACE);
      session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video' });
      const solicitation = await solicitNamespace(pair.server, []);
      const subscribe = await rawSubscribe(pair.server, 'video', 51);
      await vi.waitFor(() => {
        expect(solicitation.received).toHaveLength(2);
        expect(subscribe.received).toHaveLength(1);
      });

      await writer.write(encodeGoaway(1500));
      await writer.close();

      await vi.waitFor(() => {
        expect(solicitation.received.map((message) => message.kind)).toEqual([
          'request-ok',
          'namespace',
          'namespace-done',
        ]);
        expect(subscribe.ended()).toBe(true);
      });
      expect(subscribe.failure()).toBeUndefined();
      await subscribe.fin();

      await expect(pair.client.closed).resolves.toEqual({ closeCode: 0, reason: '' });
      await vi.waitFor(() => expect(onClosed).toHaveBeenCalledExactlyOnceWith(withError ? { error } : {}));
    }
  );

  it.each(['fin', 'reset'] as const)(
    'close() holds the transport until the peer ends its side of each ended request (%s)',
    async (peerEnd) => {
      // moq-relay reads PUBLISH_DONE, finishes the track and only then FINs its side; a transport closed before that
      // aborts the track (viewers see status 0). The PUBLISH_DONE and FIN reaching the wire is not enough, so the close
      // waits past CLOSE_FLUSH_TIMEOUT_MS for the peer's own end.
      const { pair, session, subscriber } = makePublishHarness();
      await session.ready;
      session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video' });
      const subscribe = await rawSubscribe(pair.server, 'video', 21);
      await vi.waitFor(() => expect(subscribe.received).toHaveLength(1));
      let closedAt: number | undefined;
      void pair.client.closed.then(() => {
        closedAt = performance.now();
      });

      session.close();
      await vi.waitFor(() => expect(subscribe.ended()).toBe(true));
      expect(afterSubscribeOk(subscribe, 21)).toEqual(publishDoneBytes(0x2, 0, 'track ended'));

      // A peer slow to read the PUBLISH_DONE: well past the write window, the transport stays up.
      await new Promise((resolve) => setTimeout(resolve, CLOSE_FLUSH_TIMEOUT_MS + 250));
      expect(closedAt).toBeUndefined();

      const started = performance.now();
      if (peerEnd === 'fin') await subscribe.fin();
      else await subscribe.reset();

      await expect(pair.client.closed).resolves.toEqual({ closeCode: 0, reason: '' });
      expect(closedAt! - started).toBeLessThan(500);
      subscriber.destroy();
    }
  );

  it('close() waits on the peer only up to CLOSE_END_ACK_TIMEOUT_MS', async () => {
    // A peer that never ends its side of the request stream must not hold the close.
    const onClosed = vi.fn();
    const { pair, session, subscriber } = makePublishHarness({ onClosed });
    await session.ready;
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video' });
    const subscribe = await rawSubscribe(pair.server, 'video', 21);
    await vi.waitFor(() => expect(subscribe.received).toHaveLength(1));
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });

    try {
      let closed = false;
      void pair.client.closed.then(() => {
        closed = true;
      });

      session.close();
      // The writes land at once, so the bound runs from about the close itself.
      await vi.advanceTimersByTimeAsync(CLOSE_END_ACK_TIMEOUT_MS - 50);
      expect(subscribe.ended()).toBe(true);
      expect(afterSubscribeOk(subscribe, 21)).toEqual(publishDoneBytes(0x2, 0, 'track ended'));
      expect(closed).toBe(false);

      await vi.advanceTimersByTimeAsync(100);
      expect(closed).toBe(true);
      await expect(pair.client.closed).resolves.toEqual({ closeCode: 0, reason: '' });
      expect(onClosed).toHaveBeenCalledExactlyOnceWith({});
    } finally {
      vi.useRealTimers();
    }

    subscriber.destroy();
  });

  it('close() does not wait on a request the peer already ended', async () => {
    // A peer that FINs as soon as it reads the PUBLISH_DONE (or ended the request earlier) owes nothing more: close
    // goes as soon as the writes land.
    const { pair, session, subscriber } = makePublishHarness();
    await session.ready;
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video' });
    const subscribe = await rawSubscribe(pair.server, 'video', 21);
    await vi.waitFor(() => expect(subscribe.received).toHaveLength(1));
    await subscribe.reset();
    await new Promise((resolve) => setTimeout(resolve, 50));

    const started = performance.now();
    session.close();
    await expect(pair.client.closed).resolves.toEqual({ closeCode: 0, reason: '' });
    expect(performance.now() - started).toBeLessThan(CLOSE_FLUSH_TIMEOUT_MS);
    subscriber.destroy();
  });

  it('still treats control EOF as fatal before a local close starts', async () => {
    const pair = createTransportPair();
    const control = await pair.server.createUnidirectionalStream();
    const writer = control.getWriter();
    const onClosed = vi.fn();
    const session = createMoqtPublishSession(pair.client, { callbacks: { onClosed } });
    await writer.write(encodeSetup([]));
    await session.ready;
    await writer.close();
    const info = await pair.client.closed;
    expect(info).toEqual({ closeCode: SESSION_ERROR.PROTOCOL_VIOLATION, reason: 'peer closed its control stream' });
    expect(onClosed).toHaveBeenCalledExactlyOnceWith({
      error: expect.objectContaining({ message: 'peer closed its control stream' }),
    });
  });

  it('preserves a local destruction error when the peer closes during the drain', async () => {
    const pair = createTransportPair();
    void (async () => {
      const control = await pair.server.createUnidirectionalStream();
      await control.getWriter().write(encodeSetup([]));
    })();
    const onClosed = vi.fn();
    const session = createMoqtPublishSession(pair.client, { callbacks: { onClosed } });
    await session.ready;
    const error = new Error('migration requested');
    session.destroy(error);
    pair.server.close();
    await vi.waitFor(() => expect(onClosed).toHaveBeenCalledExactlyOnceWith({ error }));
  });

  it('close() leaves no timer running', async () => {
    vi.useFakeTimers();

    try {
      const pair = createTransportPair();

      void (async () => {
        const control = await pair.server.createUnidirectionalStream();
        const writer = control.getWriter();

        await writer.write(encodeSetup([]));
      })();
      const session = createMoqtPublishSession(pair.client);

      await session.ready;
      session.close();
      // Room for the close drain's own (self-clearing) timers.
      await vi.advanceTimersByTimeAsync(1_000);
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });

  it('registerTrack() after destroy is inert — no track state, end() safe', async () => {
    const { session, subscriber } = makePublishHarness();

    await session.ready;
    session.destroy();

    const handle = session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'late' });

    expect(handle.trackName).toBe('late');
    expect(() => handle.end()).not.toThrow();
    subscriber.destroy();
  });

  it('surfaces a control-stream GOAWAY', async () => {
    const pair = createTransportPair();

    void (async () => {
      const control = await pair.server.createUnidirectionalStream();
      const writer = control.getWriter();

      await writer.write(encodeSetup([]));
      await writer.write(encodeGoaway(1500));
    })();
    const goaways: number[] = [];
    const session = createMoqtPublishSession(pair.client, {
      callbacks: { onGoaway: (goaway) => goaways.push(goaway.timeout) },
    });

    await session.ready;
    await vi.waitFor(() => {
      expect(goaways).toEqual([1500]);
    });
    session.destroy();
  });

  // ---------------------------------------------------------------------------
  // The end-to-end proof: publish session ↔ existing subscribe driver.
  // ---------------------------------------------------------------------------
});
