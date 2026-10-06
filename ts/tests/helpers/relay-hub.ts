/** Derived from Video.js; Copyright 2025-present Video.js contributors. SPDX-License-Identifier: Apache-2.0 */
/**
 * In-memory MoQ relay hub for cross-engine tests: real publish engine on one side, real playback engine(s) on the
 * other, the negotiated draft's bytes in the middle over `createTransportPair` transports.
 *
 * Publisher side (`connectPublisher`) mirrors moq-relay 0.14.17's announce-and-serve ingest. After the SETUP exchange
 * the hub opens a bidi stream and solicits announces (SUBSCRIBE_NAMESPACE, empty prefix, odd server-side request ids)
 * and reads NAMESPACE / NAMESPACE_DONE entries off that stream for the session's lifetime. Tracks are pulled on demand:
 * one upstream SUBSCRIBE per track (downstream demand dedupes onto it), with the publisher's SUBSCRIBE_OK assigning the
 * alias its subgroup data streams carry. A proactive PUBLISH is answered with REQUEST_ERROR NOT_SUPPORTED (0x3) "PUBLISH
 * is not supported" like moq-relay 0.15.8, so a regression to the old ingest model fails loudly. The publisher ends a
 * track by FINing the hub's SUBSCRIBE stream — no PUBLISH_DONE ever arrives — and retracts a namespace with
 * NAMESPACE_DONE; both land in `trackEnds`, the churn signal the source-switch regression tests assert on.
 *
 * Subscriber side (`connectSubscriber`): answers SUBSCRIBE with SUBSCRIBE_OK (carrying LARGEST_OBJECT once the track
 * has content) and forwards objects one-per-subgroup-stream, resolving the request's Location Filter (draft-22 §3.3.1)
 * using the draft-22 Location Filter rules — `relative-group 1` replays the current group from object 0 then follows live
 * (catalog joins), `relative-group 0` forwards only groups that start after the subscribe (video joins), and
 * `next-object` is strict: nothing already published is replayed (audio joins). FETCH is answered with REQUEST_ERROR;
 * the draft-20 player never sends one. A downstream subscribe registers standing upstream demand for its track; an
 * upstream FIN ends the track's downstream subscriptions with PUBLISH_DONE, the way a real relay ends the track for
 * every viewer.
 *
 * Tracks and demand persist across publisher sessions (a reconnecting publisher is re-solicited and its demanded tracks
 * re-subscribed), so the hub can also observe teardown/reconnect churn instead of crashing on it.
 */
import { StreamReader } from '../../packages/moq-core/src/protocol/bytes.js';
import {
  type ControlMessage,
  decodeControlMessage,
  encodePublishDone,
  encodeRequestError,
  encodeSetup,
  encodeSubscribe,
  encodeSubscribeNamespace,
  encodeSubscribeOk,
  type KeyValuePair,
  type Location,
  type LocationFilter,
  type MessageParameters,
  MOQT_PROTOCOL_ID,
  MOQT_PROTOCOL_IDS,
  type MoqtProtocolId,
  PUBLISH_DONE_STATUS,
  REQUEST_ERROR_CODE,
  type TrackNamespace,
} from '../../packages/moq-core/src/protocol/control-messages.js';
import {
  isSubgroupHeaderType,
  readSubgroupHeader,
  readSubgroupObjects,
  STREAM_TYPE,
} from '../../packages/moq-core/src/protocol/object-stream.js';
import type { BidirectionalStreamLike } from '../../packages/moq-core/src/protocol/request-stream.js';
import type { MoqtTransport } from '../../packages/moq-core/src/protocol/session.js';
import { createSubgroupWriter } from '../../packages/moq-core/src/protocol/subgroup-writer.js';
import { createTransportPair } from '../../packages/moq-core/src/tests/helpers/transport-pair.js';
import type { RelayTransportFactory as ConnectPublishTransport } from '../../src/index.js';

// =============================================================================
// Types
// =============================================================================

interface BufferedObject {
  groupId: number;
  objectId: number;
  properties: KeyValuePair[];
  payload: Uint8Array;
}

interface Subscription {
  trackAlias: number;
  /** First object this subscription may receive (inclusive), resolved from its Location Filter at subscribe time. */
  start: Location;
  deliver(object: BufferedObject): void;
  end(statusCode: number): void;
}

interface TrackRecord {
  name: string;
  /** Insertion-ordered groupId → objects, pruned to the newest few groups. */
  groups: Map<number, BufferedObject[]>;
  /** Largest Object received from the publisher; `undefined` until the first one lands. */
  largest: Location | undefined;
  objectsReceived: number;
  subscribers: Set<Subscription>;
}

/**
 * One upstream end observed from the publisher. Under announce-and-serve there is no PUBLISH_DONE: a bare FIN on the
 * hub's SUBSCRIBE stream is the clean track end, and NAMESPACE_DONE retracts an announce. These replace the old
 * `publishDones` as the source-switch churn signal.
 */
export type ObservedTrackEnd =
  | { kind: 'subscribe-fin'; trackName: string }
  | { kind: 'namespace-done'; namespace: TrackNamespace };

export interface RelayHub {
  /** Transport seam for the publish engine's `connectTransport`. */
  connectPublisher: ConnectPublishTransport;
  /** Transport seam for the playback engine's `createMoqTransport`. */
  connectSubscriber: (url?: string, protocols?: string[]) => { transport: MoqtTransport; ready: Promise<void> };
  /** Upstream track SUBSCRIBEs sent to the publisher, in order (a retry appends again). */
  readonly subscribes: string[];
  /** The draft each connection negotiated, in connection order. */
  readonly negotiated: MoqtProtocolId[];
  /** The parameters of each SUBSCRIBE received from subscribers, as decoded for that connection's draft. */
  readonly downstreamSubscribes: MessageParameters[];
  /** Every upstream end observed — subscribe-stream FINs and NAMESPACE_DONE retractions. */
  readonly trackEnds: ObservedTrackEnd[];
  /**
   * Register standing upstream demand for a track without a downstream viewer — the stand-in for other viewers' pull in
   * late-join scenarios, since an announce-and-serve publisher writes nothing until the relay subscribes. Dedupes onto
   * the single live upstream subscription and carries across publisher reconnects.
   */
  subscribeUpstream(trackName: string): void;
  /** Publisher transports accepted so far (a reconnect increments this). */
  publisherConnections(): number;
  /** Total objects received from the publisher for one track. */
  objectCount(trackName: string): number;
  destroy(): void;
}

const MAX_BUFFERED_GROUPS = 3;

/**
 * moq-relay 0.15.8 declines every proactive PUBLISH as `Error::Unsupported`, which is NOT_SUPPORTED (0x3): see
 * `run_publish_stream` in `rs/moq-net/src/ietf/subscriber.rs` at tag `moq-relay-v0.15.8`. 0.14.17 already sent 0x3,
 * and the literal 400 this fixture used to send dates from 0.14.7.
 */
const PUBLISH_REJECTED_ERROR_CODE = REQUEST_ERROR_CODE.NOT_SUPPORTED;

/**
 * How long the hub waits between retries when a SUBSCRIBE races the publisher's `registerTrack` and comes back
 * DOES_NOT_EXIST.
 */
const SUBSCRIBE_RETRY_DELAY_MS = 25;

// =============================================================================
// Implementation
// =============================================================================

/** Read one length-framed control message (type varint already consumed). */
async function readControlFrame(
  reader: StreamReader,
  type: number,
  protocol: MoqtProtocolId = MOQT_PROTOCOL_ID,
): Promise<ControlMessage> {
  const high = await reader.readUint8();
  const low = await reader.readUint8();
  const body = await reader.readBytes(high * 256 + low);

  return decodeControlMessage({ type, body }, protocol);
}

function locationOf(object: BufferedObject): Location {
  return { group: object.groupId, object: object.objectId };
}

/** `a` is at or after `b` in (group, object) order. */
function isAtOrAfter(a: Location, b: Location): boolean {
  return a.group > b.group || (a.group === b.group && a.object >= b.object);
}

/**
 * Where a new subscription starts, resolved against the track's Largest Object using the
 * draft-22 Location Filter rules (§3.3.1): `next-object` is strict (nothing already published is replayed), `relative-group`
 * counts back from the next group (`0` = the next group, `1` = the current group from object 0), an absolute start is
 * honored as given (the hub never sees an end), and an unfiltered request starts at the current group like moq-lite.
 */
function resolveStart(filter: LocationFilter | undefined, largest: Location | undefined): Location {
  if (filter?.type === 'next-object') {
    return largest ? { group: largest.group, object: largest.object + 1 } : { group: 0, object: 0 };
  }

  if (filter?.type === 'relative-group') {
    const nextGroup = largest ? largest.group + 1 : 0;

    return { group: Math.max(0, nextGroup - filter.groupsBeforeNext), object: 0 };
  }

  if (filter?.type === 'absolute') return filter.start;

  return { group: largest?.group ?? 0, object: 0 };
}

/**
 * `protocols` are the drafts the hub speaks, in its preference order: a connection gets the first one the client offers
 * (TA-7), and the hub encodes and decodes that connection for it.
 */
export function createRelayHub({
  protocols = MOQT_PROTOCOL_IDS,
}: {
  protocols?: readonly MoqtProtocolId[];
} = {}): RelayHub {
  let destroyed = false;
  let nextSubscriberAlias = 1;
  let publisherConnections = 0;

  const subscribes: string[] = [];
  const negotiated: MoqtProtocolId[] = [];
  const downstreamSubscribes: MessageParameters[] = [];
  const select = (offered: readonly string[] = []): MoqtProtocolId => {
    const selected = protocols.find((protocol) => offered.length === 0 || offered.includes(protocol));

    if (selected === undefined) throw new Error(`the hub speaks none of the offered protocols ${offered.join(', ')}`);
    negotiated.push(selected);
    return selected;
  };
  const trackEnds: ObservedTrackEnd[] = [];
  const tracks = new Map<string, TrackRecord>();
  const closers = new Set<() => void>();
  /** Tracks with standing upstream demand (downstream viewers or test priming). */
  const demand = new Set<string>();
  /** The live publisher connection's per-track pull, when one is connected. */
  let pullUpstream: ((trackName: string) => void) | undefined;

  const trackFor = (name: string): TrackRecord => {
    let track = tracks.get(name);

    if (!track) {
      track = { name, groups: new Map(), largest: undefined, objectsReceived: 0, subscribers: new Set() };
      tracks.set(name, track);
    }

    return track;
  };

  const bufferAndForward = (track: TrackRecord, object: BufferedObject): void => {
    let group = track.groups.get(object.groupId);

    if (!group) {
      group = [];
      track.groups.set(object.groupId, group);

      while (track.groups.size > MAX_BUFFERED_GROUPS) {
        const oldest = track.groups.keys().next().value!;

        track.groups.delete(oldest);
      }
    }

    const location = locationOf(object);

    group.push(object);
    track.objectsReceived++;

    if (!track.largest || !isAtOrAfter(track.largest, location)) track.largest = location;

    for (const subscriber of track.subscribers) {
      if (isAtOrAfter(location, subscriber.start)) subscriber.deliver(object);
    }
  };

  /** Standing demand: dedupe onto one live upstream subscription per track. */
  const ensureUpstream = (trackName: string): void => {
    demand.add(trackName);
    pullUpstream?.(trackName);
  };

  // ---------------------------------------------------------------------------
  // Publisher side — announce-and-serve, mirroring moq-relay 0.14.17
  // ---------------------------------------------------------------------------

  const connectPublisher: ConnectPublishTransport = (_url, offered) => {
    publisherConnections++;
    const protocol = select(offered);
    const pair = createTransportPair({ protocol });
    const server = pair.server;
    // Aliases are publisher-assigned in SUBSCRIBE_OK and scoped to one session.
    const aliasToTrack = new Map<number, TrackRecord>();
    /** Odd server-side request ids; 1 goes to the namespace solicitation. */
    let nextRequestId = 3;
    const announced: TrackNamespace[] = [];
    const announceWaiters: ((namespace: TrackNamespace) => void)[] = [];
    /** Tracks with a live (or in-flight) upstream subscription on this session. */
    const upstreamTracks = new Set<string>();
    let connectionClosed = false;

    const announcedNamespace = (): Promise<TrackNamespace> => {
      const first = announced[0];
      if (first) return Promise.resolve(first);

      return new Promise((resolve) => announceWaiters.push(resolve));
    };

    /**
     * The relay's opening move: one SUBSCRIBE_NAMESPACE (empty prefix covers every namespace) right after SETUP. The
     * publisher accepts with REQUEST_OK and announces by writing NAMESPACE entries; the stream stays open for the
     * session's lifetime, carrying NAMESPACE_DONE retractions at the end.
     */
    const solicitNamespaces = async (): Promise<void> => {
      const stream = await server.createBidirectionalStream();
      const writer = stream.writable.getWriter();
      const reader = new StreamReader(stream.readable);

      try {
        await writer.write(
          encodeSubscribeNamespace({ requestId: 1, trackNamespacePrefix: [], parameters: {} }, protocol),
        );

        while (!(await reader.atEnd())) {
          const type = await reader.readVarint();
          const message = await readControlFrame(reader, type, protocol);

          if (message.kind === 'namespace') {
            // Empty solicited prefix ⇒ the suffix IS the full namespace.
            announced.push(message.trackNamespaceSuffix);

            for (const resolve of announceWaiters.splice(0)) resolve(message.trackNamespaceSuffix);
          } else if (message.kind === 'namespace-done') {
            trackEnds.push({ kind: 'namespace-done', namespace: message.trackNamespaceSuffix });
          }
          // 'request-ok' — the publisher accepting the solicitation.
        }
      } catch {
        // Reset or transport teardown.
      }
    };

    /**
     * Pull one announced track: a fresh bidi stream per SUBSCRIBE, alias recorded from the publisher's SUBSCRIBE_OK,
     * held open until one side ends it. A bare FIN from the publisher is the draft-19 clean track end — recorded as
     * churn, and forwarded to the track's downstream subscribers as PUBLISH_DONE the way a real relay ends the track.
     */
    const subscribeUpstream = (trackName: string): void => {
      if (destroyed || connectionClosed || upstreamTracks.has(trackName)) return;

      upstreamTracks.add(trackName);
      void (async () => {
        const trackNamespace = await announcedNamespace();
        const stream = await server.createBidirectionalStream();
        const writer = stream.writable.getWriter();
        const reader = new StreamReader(stream.readable);
        const requestId = nextRequestId;

        nextRequestId += 2;
        const track = trackFor(trackName);
        let accepted = false;

        try {
          await writer.write(
            encodeSubscribe(
              {
                requestId,
                trackNamespace,
                trackName,
                // moq-relay 0.14.17 and 0.15.8 request Next Object plus a current-group fill.
                // Our publisher advertises no cached content, so the fill settles immediately.
                parameters: {
                  forward: 1,
                  subscriberPriority: 0,
                  locationFilter: { type: 'next-object' },
                  fillParameters: { locationFilter: { type: 'relative-group', groupsBeforeNext: 1 } },
                  groupOrder: 'descending',
                },
              },
              protocol,
            ),
          );
          subscribes.push(trackName);

          while (!(await reader.atEnd())) {
            const type = await reader.readVarint();
            const message = await readControlFrame(reader, type, protocol);

            if (message.kind === 'subscribe-ok') {
              accepted = true;
              aliasToTrack.set(message.trackAlias, track);
            } else if (message.kind === 'request-error') {
              // DOES_NOT_EXIST usually means the demand raced the
              // publisher's registerTrack — retry while the demand stands
              // (the `finally` below frees the dedupe slot first).
              if (demand.has(trackName) && !destroyed && !connectionClosed) {
                setTimeout(() => subscribeUpstream(trackName), SUBSCRIBE_RETRY_DELAY_MS);
              }

              return;
            }
          }

          if (accepted) {
            trackEnds.push({ kind: 'subscribe-fin', trackName });

            for (const subscriber of [...track.subscribers]) subscriber.end(PUBLISH_DONE_STATUS.TRACK_ENDED);

            track.subscribers.clear();
          }
        } catch {
          // Reset or transport teardown — not a clean track end.
        } finally {
          // The stream ended one way or another — free the dedupe slot,
          // so `has()` only ever guards a live (or in-flight) upstream
          // subscription and later demand can re-pull a track the
          // publisher ended and re-registered in the same session.
          upstreamTracks.delete(trackName);
          writer.close().catch(() => {});
        }
      })();
    };

    /** Data streams can race our SUBSCRIBE_OK read; wait briefly for the alias. */
    const resolveTrack = async (alias: number): Promise<TrackRecord | undefined> => {
      for (let i = 0; i < 200; i++) {
        const track = aliasToTrack.get(alias);
        if (track) return track;

        await new Promise((resolve) => setTimeout(resolve, 5));
      }

      return undefined;
    };

    const acceptUniStreams = async (): Promise<void> => {
      const streams = server.incomingUnidirectionalStreams.getReader();

      try {
        while (true) {
          const { done, value } = await streams.read();
          if (done) break;

          void (async () => {
            const reader = new StreamReader(value);

            try {
              const streamType = await reader.readVarint();

              if (streamType === STREAM_TYPE.SETUP) {
                // The publisher's control stream: SETUP and nothing else
                // (a client GOAWAY closes a moq-lite-rs session).
                while (!(await reader.atEnd())) await reader.readUint8();

                return;
              }

              if (!isSubgroupHeaderType(streamType)) {
                await reader.cancel();
                return;
              }

              const header = await readSubgroupHeader(reader, streamType);
              const track = await resolveTrack(header.trackAlias);

              if (!track) {
                await reader.cancel();
                return;
              }

              for await (const object of readSubgroupObjects(reader, header)) {
                if (object.status !== 'normal') continue;

                bufferAndForward(track, {
                  groupId: object.groupId,
                  objectId: object.objectId,
                  properties: object.properties,
                  payload: object.payload,
                });
              }
            } catch {
              // Stream reset (drop path or teardown) — buffered objects stay.
            }
          })();
        }
      } catch {
        // Transport went away.
      }
    };

    /**
     * The publisher initiates no request streams under announce-and-serve. Anything arriving here is a regression to
     * the old proactive model — answer it the way moq-relay 0.15.8 does, so the regression fails loudly instead of
     * being silently ingested.
     */
    const rejectRequestStream = async (stream: BidirectionalStreamLike): Promise<void> => {
      const writer = stream.writable.getWriter();
      const reader = new StreamReader(stream.readable);

      try {
        const type = await reader.readVarint();
        const message = await readControlFrame(reader, type, protocol);

        await writer.write(
          message.kind === 'publish'
            ? encodeRequestError(PUBLISH_REJECTED_ERROR_CODE, 'PUBLISH is not supported')
            : encodeRequestError(REQUEST_ERROR_CODE.NOT_SUPPORTED, 'unexpected request stream'),
        );
      } catch {
        // Reset or teardown.
      } finally {
        writer.close().catch(() => {});
        void reader.cancel().catch(() => {});
      }
    };

    const acceptBidiStreams = async (): Promise<void> => {
      const streams = server.incomingBidirectionalStreams.getReader();

      try {
        while (true) {
          const { done, value } = await streams.read();
          if (done) break;

          void rejectRequestStream(value);
        }
      } catch {
        // Transport went away.
      }
    };

    void acceptUniStreams();
    void acceptBidiStreams();
    void (async () => {
      try {
        // Server SETUP rides its own control stream right after connect.
        const control = await server.createUnidirectionalStream();

        await control.getWriter().write(encodeSetup());
      } catch {
        return; // Closed before SETUP could land.
      }

      await solicitNamespaces();
    })();

    // Standing demand carries across publisher sessions: a reconnecting
    // publisher gets its still-wanted tracks re-subscribed once it
    // announces again.
    pullUpstream = subscribeUpstream;

    for (const trackName of demand) subscribeUpstream(trackName);

    const markClosed = (): void => {
      connectionClosed = true;

      if (pullUpstream === subscribeUpstream) pullUpstream = undefined;
    };

    void server.closed.then(markClosed, markClosed);

    closers.add(() => pair.client.close());
    return { transport: pair.client, ready: Promise.resolve() };
  };

  // ---------------------------------------------------------------------------
  // Subscriber side
  // ---------------------------------------------------------------------------

  const connectSubscriber = (_url?: string, offered?: string[]): { transport: MoqtTransport; ready: Promise<void> } => {
    const protocol = select(offered);
    const pair = createTransportPair({ protocol });
    const server = pair.server;
    let open = true;

    /** Forward one object on its own subgroup stream toward the player. */
    const forwardObject = (trackAlias: number, object: BufferedObject): void => {
      if (!open || destroyed) return;

      void (async () => {
        try {
          const stream = await server.createUnidirectionalStream();
          const writer = createSubgroupWriter(stream, {
            trackAlias,
            groupId: object.groupId,
            hasProperties: true,
            endOfGroup: false,
          });

          await writer.writeObject({
            objectId: object.objectId,
            properties: object.properties,
            payload: object.payload,
          });
          await writer.fin();
        } catch {
          // Subscriber went away.
        }
      })();
    };

    const serveRequestStream = async (stream: BidirectionalStreamLike): Promise<void> => {
      const writer = stream.writable.getWriter();
      const reader = new StreamReader(stream.readable);
      let subscription: Subscription | undefined;
      let subscribedTrack: TrackRecord | undefined;

      try {
        while (!(await reader.atEnd())) {
          const type = await reader.readVarint();
          const message = await readControlFrame(reader, type, protocol);

          if (message.kind === 'subscribe') {
            downstreamSubscribes.push(message.parameters);
            // Downstream demand is what makes the hub pull the track from
            // the publisher (announce-and-serve is pull-through).
            ensureUpstream(message.trackName);
            const track = trackFor(message.trackName);
            const trackAlias = nextSubscriberAlias++;

            // Like the relay on draft-20, SUBSCRIBE_OK reports the Largest
            // Object once the track has content (two bare varints, §9.20.17).
            await writer.write(
              encodeSubscribeOk(trackAlias, track.largest ? { largestObject: track.largest } : {}, [], protocol),
            );
            subscription = {
              trackAlias,
              start: resolveStart(message.parameters.locationFilter, track.largest),
              deliver: (object) => forwardObject(trackAlias, object),
              end: (statusCode) => {
                void writer
                  .write(encodePublishDone(statusCode, 0, ''))
                  .then(() => writer.close())
                  .catch(() => {});
              },
            };

            // Replay whatever the filter reaches back for (the current
            // group on a `relative-group 1` join), then follow live.
            for (const groupId of [...track.groups.keys()].sort((a, b) => a - b)) {
              for (const object of track.groups.get(groupId) ?? []) {
                if (isAtOrAfter(locationOf(object), subscription.start)) subscription.deliver(object);
              }
            }

            track.subscribers.add(subscription);
            subscribedTrack = track;
          } else if (message.kind === 'fetch') {
            // The draft-20 player never fetches (its joins are Location
            // Filters); anything asking for history is refused.
            await writer.write(encodeRequestError(REQUEST_ERROR_CODE.INVALID_RANGE, 'no history'));
            await writer.close();
            return;
          }
          // REQUEST_UPDATE / GOAWAY: ignored by the hub.
        }
      } catch {
        // Reset — the subscriber cancelled; ordinary end-of-request.
      } finally {
        if (subscribedTrack && subscription) subscribedTrack.subscribers.delete(subscription);

        writer.close().catch(() => {});
      }
    };

    const acceptBidiStreams = async (): Promise<void> => {
      const streams = server.incomingBidirectionalStreams.getReader();

      try {
        while (true) {
          const { done, value } = await streams.read();
          if (done) break;

          void serveRequestStream(value);
        }
      } catch {
        // Transport went away.
      }
    };

    const drainUniStreams = async (): Promise<void> => {
      // The player's control stream (SETUP + GOAWAY) lands here; drain it.
      const streams = server.incomingUnidirectionalStreams.getReader();

      try {
        while (true) {
          const { done, value } = await streams.read();
          if (done) break;

          void value.getReader().read();
        }
      } catch {
        // Transport went away.
      }
    };

    void acceptBidiStreams();
    void drainUniStreams();
    void (async () => {
      try {
        const control = await server.createUnidirectionalStream();

        await control.getWriter().write(encodeSetup());
      } catch {
        // Closed before SETUP could land.
      }
    })();

    closers.add(() => {
      open = false;
      pair.client.close();
    });
    return { transport: pair.client, ready: Promise.resolve() };
  };

  return {
    connectPublisher,
    connectSubscriber,
    subscribes,
    negotiated,
    downstreamSubscribes,
    trackEnds,
    subscribeUpstream: ensureUpstream,
    publisherConnections: () => publisherConnections,
    objectCount: (trackName) => tracks.get(trackName)?.objectsReceived ?? 0,
    destroy() {
      destroyed = true;

      for (const close of [...closers]) close();

      closers.clear();
    },
  };
}
