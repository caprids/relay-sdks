import { describe, expect, it, vi } from 'vitest';

import {
  type ControlMessage,
  ControlMessageDeframer,
  decodeControlMessage,
  encodeGoaway,
  encodePublish,
  encodeRequestUpdate,
  encodeSetup,
  encodeSubscribe,
  REQUEST_ERROR_CODE,
  type TrackNamespace,
} from '../protocol/control-messages.js';
import type { MoqtObject } from '../protocol/object-stream.js';
import {
  createMoqtPublishSession,
  type IncomingSubscribe,
  type MoqtPublishSessionCallbacks,
} from '../protocol/publish-session.js';
import { createMoqtSession, type MoqtSession } from '../protocol/session.js';
import { openRawRequest, rawSubscribe as rawSubscribeTo, solicitNamespace } from './helpers/raw-peer.js';
import { createTransportPair } from './helpers/transport-pair.js';

const NAMESPACE = ['live', 'abc123'];

/** The relay's per-track pull under the test namespace. */
function rawSubscribe(server: Parameters<typeof rawSubscribeTo>[0], trackName: string, requestId: number) {
  return rawSubscribeTo(server, NAMESPACE, trackName, requestId);
}

function makePublishHarness(callbacks: MoqtPublishSessionCallbacks = {}) {
  const pair = createTransportPair();
  const subscriber = createMoqtSession(pair.server, { unknownAliasTimeoutMs: 500 });
  const session = createMoqtPublishSession(pair.client, { callbacks });

  return { pair, session, subscriber };
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

    await vi.waitFor(() => {
      // The peer's request ids are session-unique, so they double as the
      // alias — two live request ids sharing an alias is a session-fatal
      // Duplicate on moq-lite-rs.
      expect(video.aliases).toEqual([video.subscription.requestId]);
      expect(subscribes.map((s) => s.trackName)).toEqual(['video']);
      expect(bindings).toEqual([{ trackName: 'video', trackAlias: video.subscription.requestId }]);
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
      expect(updates).toEqual([{ requestId: updated.requestId, updateRequestId: expect.any(Number) }]);
      expect(updates[0]?.updateRequestId).not.toBe(updated.requestId);
      expect(updateAcks).toHaveLength(1);
    });
    session.destroy();
    subscriber.destroy();
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

  it('ends a track by FINing its subscription streams with no trailing message', async () => {
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
    // A bare FIN is the clean track end. Any byte after
    // SUBSCRIBE_OK — the old PUBLISH_DONE — makes moq-lite-rs abort the
    // track for every downstream viewer instead of finishing it.
    expect(subscribe.received.map((m) => m.kind)).toEqual(['subscribe-ok']);

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
      // The replaced subscription ends cleanly (FIN, no trailing bytes).
      expect(first.ended()).toBe(true);
    });
    expect(first.received.map((m) => m.kind)).toEqual(['subscribe-ok']);
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
    const { pair, session, subscriber } = makePublishHarness({
      onTrackBinding: ({ trackAlias }) => bindings.push(trackAlias),
    });

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
    const { pair, session, subscriber } = makePublishHarness({
      onTrackBinding: ({ trackAlias }) => bindings.push(trackAlias),
      onSubscribe: ({ requestId }) => subscribed.push(requestId),
    });

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
    // indistinguishable — protocol violation, session-fatal.
    await rawSubscribe(pair.server, 'audio', 101);
    await vi.waitFor(() => {
      expect(closes).toHaveLength(1);
    });
    expect((closes[0] as { error?: unknown }).error).toBeInstanceOf(Error);
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
      expect(sub.received.map((m) => m.kind)).toEqual(['subscribe-ok', 'request-error']);
      expect(sub.ended()).toBe(true);
      expect(endedRequests).toEqual([111]);
      expect(bindings.at(-1)).toBeUndefined();
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
    session.destroy();
    subscriber.destroy();
  });

  it.each([false, true])('drains FINs and NAMESPACE_DONE, including destroy with an error: %s', async (withError) => {
    const pair = createTransportPair();
    // A fully raw peer: complete SETUP and record every frame on the
    // publisher's control stream — it must carry SETUP and nothing else
    // (a client GOAWAY closes a moq-lite-rs session, code 17).
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
      // The announce is retracted and the subscription FINed cleanly
      // before the transport goes away.
      expect(solicitation.received.map((m) => m.kind)).toEqual(['request-ok', 'namespace', 'namespace-done']);
      expect(subscribe.ended()).toBe(true);
    });
    expect(subscribe.received.map((m) => m.kind)).toEqual(['subscribe-ok']);
    expect(controlFrames.map((m) => m.kind)).toEqual(['setup']);
    await expect(pair.client.closed).resolves.toEqual({ closeCode: 0, reason: '' });
    expect(onClosed).toHaveBeenCalledExactlyOnceWith(withError ? { error } : {});
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
