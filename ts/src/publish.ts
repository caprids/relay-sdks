import { createMoqtPublishSession, createSubgroupWriter, type ObjectStatus } from '@mux/moq-core';
import { assertTimeoutMs, type ConnectRelayOptions, openRelaySession } from './connect-session.js';
import {
  type DeliveryTimeouts,
  NO_DELIVERY_TIMEOUTS,
  publisherTimeouts,
  subscriberTimeouts,
} from './delivery-timeouts.js';
import { RelayError } from './errors.js';
import { createSettlingGroups } from './settling-groups.js';
import { namespaceFields, trackName, trackPropertyPairs } from './validation.js';
import { createWriteSlot, type WriteSlot } from './write-slot.js';

/**
 * Options for connectRelayPublisher: the shared connection options plus the namespace. There is no
 * request deadline; each track's writeTimeoutMs bounds waiting for demand and writing.
 */
export interface ConnectRelayPublisherOptions extends ConnectRelayOptions {
  /** One namespace per publisher connection. Register any number of tracks within it. */
  namespace: readonly string[];
}

/**
 * One MOQT property (a key-value pair) on a registered track or a written object, encoded as
 * supplied. Invalid properties are a TypeError naming the property index. A byte value over 65535
 * bytes is a RangeError, and so is a list past its bound: on a track, more than 65525 encoded bytes
 * (the room SUBSCRIBE_OK leaves), more than 64 pairs, or a 0x0E above 255 or 0x22 other than 1 or 2;
 * on an object, more than 64 KiB encoded (the relay's limit). Both are raised before a track is
 * registered or a write starts.
 */
export interface PublishProperty {
  /** Non-negative safe integer: even types encode an integer, odd types encode bytes. */
  type: number;
  /** A non-negative safe integer for an even type; an `ArrayBufferView`, copied as its raw bytes, for an odd type. */
  value: number | ArrayBufferView;
}

/**
 * What {@link RelayPublisher.registerTrack} needs. The namespace is the publisher connection's;
 * the generic core's type of the same name spells it out as a field one level down.
 */
export interface RegisterTrackOptions {
  /** Track name, unique within this publisher connection, including after its writer closes. */
  track: string;
  /**
   * Track metadata sent to the relay. The SDK adds no default timescale; the relay may
   * add, drop, or rewrite properties before delivery to subscribers. With moq-relay 0.17.0 every
   * SUBSCRIBE_OK carries the relay's track properties, not these, and a Timescale (`0x08`) here is
   * what keeps supplied object Timestamps unchanged. See "Relay property handling" in
   * spec/behavior.md. The list rides in every SUBSCRIBE_OK, so it must encode to at most 65525
   * bytes, hold at most 64 pairs (the relay's limit) and keep `0x0E` at most 255 and `0x22` at 1
   * or 2 (draft-22); otherwise the relay would refuse every subscriber, so `registerTrack` throws a
   * RangeError instead.
   *
   * An OBJECT_DELIVERY_TIMEOUT (`0x02`) or SUBGROUP_DELIVERY_TIMEOUT (`0x06`) here, in milliseconds,
   * is the publisher's delivery timeout (draft-22 §5.2), which the first object of a group can
   * override for that group. An object still short of the transport when the object timeout
   * expires fails its write with `DELIVERY_TIMEOUT`; a completed group the relay has not
   * acknowledged when the subgroup timeout expires is reset, and nothing is reported. 0 means no
   * timeout.
   */
  trackProperties?: readonly PublishProperty[];
  /** Deadline for relay demand and stream writes. Default 10 seconds, maximum 2147483647. */
  writeTimeoutMs?: number;
}

/**
 * One open group from {@link RelayTrackWriter.openGroup}: a single subgroup stream the application
 * writes objects to in order, then FINs with `close()` or resets with `abort()`. It holds the track's
 * write slot until then, so the track's `write()` and another `openGroup()` fail with `WRITE_PENDING` meanwhile.
 */
export interface RelayGroupWriter {
  /** The group's ID. Group IDs start at 0 on each track and increase by one per group the track opens. */
  readonly groupId: number;
  /** How many objects this group has written; the next `write()` uses it as the object ID. */
  readonly objectCount: number;
  /** True once `close()` completed, or the group was aborted, failed, or lost its track or connection. */
  readonly closed: boolean;
  /**
   * Append one object, starting at ID 0. Each write has its own writeTimeoutMs deadline, and a
   * delivery timeout can end it sooner with `DELIVERY_TIMEOUT`, which resets the whole group.
   * Properties are encoded as supplied; the relay may transform them before delivery. A `0x02` or
   * `0x06` property on object 0 sets the group's delivery timeouts; on a later object it is ignored.
   */
  write(
    payload: ArrayBufferView,
    options?: { properties?: readonly PublishProperty[]; status?: ObjectStatus },
  ): Promise<void>;
  /**
   * FIN the group within writeTimeoutMs. Resolves once the FIN is handed to the transport, not once
   * the relay acknowledges it. Idempotent after successful completion.
   */
  close(): Promise<void>;
  /**
   * Reset this group and free the track for the next random-access point. After a clean `close()`
   * this has no effect: the FIN already ended the stream (draft-22 §11.3.2).
   */
  abort(reason?: unknown): void;
  /** Encode UTF-8 text and write one object with the same options and semantics as write(). */
  writeText(text: string, options?: { properties?: readonly PublishProperty[]; status?: ObjectStatus }): Promise<void>;
  /** Serialize with JSON.stringify and write one object. Serialization errors leave the writer usable. */
  writeJSON(
    value: unknown,
    options?: { properties?: readonly PublishProperty[]; status?: ObjectStatus },
  ): Promise<void>;
}

/**
 * One registered track on a publisher connection, from {@link RelayPublisher.registerTrack}.
 * Registration sends nothing; `write()` and `openGroup()` send, after waiting for relay demand.
 * The behaviour behind this interface is the track's write slot (see CONTEXT.md and the
 * "Write phases and interruptions" table in spec/behavior.md).
 */
export interface RelayTrackWriter {
  /**
   * Whether the relay currently has an upstream subscription for this track, so a write would
   * proceed at once. Demand is the relay's own subscription, shared by every downstream viewer
   * and ended when the relay chooses: it is neither a viewer count nor a liveness signal. Always false once the track is closed.
   */
  readonly hasDemand: boolean;
  /** True once the application closed the track or the publisher connection ended. */
  readonly closed: boolean;
  /**
   * The current demand state first, then one value per relay subscription change: `true` when the
   * relay subscribes or replaces its subscription (so `true` can repeat), `false` when its
   * subscription ends. Ends when the track ends. This is how a catalog re-sends its document on
   * every relay subscription. Values are per relay subscription, never per downstream viewer.
   */
  demandChanges(): AsyncIterableIterator<boolean>;
  /** Hold the track's single write slot until the group closes or aborts. */
  openGroup(options?: { signal?: AbortSignal }): Promise<RelayGroupWriter>;
  /**
   * Send one independent object/group. Waits for relay demand (see hasDemand). Resolves when the
   * transport accepts the complete group, not when a downstream viewer acknowledges it.
   * Only one write may be pending; await it before writing again. Any ArrayBufferView is
   * copied as its raw bytes; anything else is a TypeError that leaves the track usable.
   * Properties are encoded as supplied; the relay may transform them before delivery.
   * Failure resets this group and leaves the track usable while the connection is open.
   * Data may already have reached the relay; choose the next object explicitly (no automatic retry).
   */
  write(
    payload: ArrayBufferView,
    options?: { properties?: readonly PublishProperty[]; signal?: AbortSignal },
  ): Promise<void>;
  /** End the track and cancel an outstanding write. The publisher connection stays open. */
  close(): void;
  /** Encode UTF-8 text and write one object with the same options and semantics as write(). */
  writeText(text: string, options?: { properties?: readonly PublishProperty[]; signal?: AbortSignal }): Promise<void>;
  /** Serialize with JSON.stringify and write one object. Serialization errors leave the writer usable. */
  writeJSON(value: unknown, options?: { properties?: readonly PublishProperty[]; signal?: AbortSignal }): Promise<void>;
}

/**
 * A connection that can only publish, serving one namespace. Create one with
 * {@link connectRelayPublisher}. Subscribing uses a separate connection; see `connectRelaySubscriber`.
 */
export interface RelayPublisher {
  /**
   * Register a track this publisher will serve and return its writer. Nothing is sent: the relay
   * learns of the track when a subscriber asks for it, and bytes move when the writer's `write()`
   * or `openGroup()` runs, after waiting for that relay demand. No capture starts and no media
   * catalog is published. Track names are unique per publisher connection, including after their
   * writer closes.
   */
  registerTrack(options: RegisterTrackOptions): RelayTrackWriter;
  /**
   * Settles once the namespace announce has reached the wire on a solicitation that covers it,
   * which is no earlier than the first registered track. It is the strongest fact the publisher
   * has: the relay acknowledges an announce with nothing and routes subscribes for the namespace
   * about a round trip later, so a subscribe that races it is rejected (request error 16
   * `unroutable` on moq-relay 0.15.8 and later; see spec/behavior.md). It stays pending while no
   * solicitation covers the namespace, where the first write reports `NAMESPACE_NOT_SOLICITED`, and
   * the connection's closure terminates it if that comes first: the closure's error, or a `CLOSED`
   * RelayError after a clean shutdown. Pre-handled, so nobody has to await it.
   */
  readonly announced: Promise<void>;
  /** Ends when the connection shuts down. Errors are returned as data; this never rejects. */
  readonly closed: Promise<{
    /** The failure that ended the connection; absent after a clean shutdown. */
    error?: unknown;
  }>;
  /**
   * End all tracks, retract the namespace, and wait for transport shutdown. First waits, for up to
   * the largest writeTimeoutMs among the tracks, for completed groups the relay has not yet
   * acknowledged; any still unacknowledged after that may be lost. Idempotent.
   */
  close(): Promise<void>;
}

/**
 * Open a dedicated Relay publisher connection. Subscribers use connectRelaySubscriber; this
 * initial publishing API does not share their physical transport. A relay must solicit the
 * namespace and subscribe upstream before data can flow.
 */
export async function connectRelayPublisher(options: ConnectRelayPublisherOptions): Promise<RelayPublisher> {
  // Arguments before anything else: an invalid namespace never reaches the token provider or the
  // transport factory.
  const namespace = namespaceFields(options.namespace);

  // One write slot per registered track; the session's binding callback and the connection's end
  // reach the slots through here.
  const tracks = new Map<string, WriteSlot>();
  // Each relay subscription's delivery timeouts (§5.2), by its request ID, which the core also uses as
  // its track alias: SUBSCRIBE's parameters, as REQUEST_UPDATE changes them.
  const subscriptionTimeouts = new Map<number, DeliveryTimeouts>();
  // Groups whose FIN the transport took but the relay has not acknowledged, shared by every track.
  const settlingGroups = createSettlingGroups();
  // What closing waits for them: the largest write deadline among the tracks.
  let drainMs = 0;
  // Settled by the session's onAnnounced callback; a closure that comes first terminates it.
  let announce: () => void = () => {};
  let abandonAnnounce: (reason: unknown) => void = () => {};
  const announced = new Promise<void>((resolve, reject) => {
    announce = resolve;
    abandonAnnounce = reject;
  });
  // Nobody may await readiness; a rejection must not surface as an unhandled rejection.
  void announced.catch(() => {});
  const lifetime = await openRelaySession(options, (transport, callbacks, setupOptions) =>
    createMoqtPublishSession(transport, {
      implementationName: '@mux/relay publisher',
      setupOptions,
      callbacks: {
        ...callbacks,
        onAnnounced: () => announce(),
        onSubscribe: ({ requestId, parameters }) => subscriptionTimeouts.set(requestId, subscriberTimeouts(parameters)),
        onRequestUpdate: ({ requestId, parameters }) => {
          const previous = subscriptionTimeouts.get(requestId);
          if (previous) subscriptionTimeouts.set(requestId, subscriberTimeouts(parameters, previous));
        },
        onSubscribeEnd: ({ requestId }) => subscriptionTimeouts.delete(requestId),
        onTrackBinding: ({ trackName, trackAlias }) => tracks.get(trackName)?.bind(trackAlias),
        onAnnounceEnded: ({ error }) =>
          callbacks.onClosed?.({
            error: new RelayError('NAMESPACE_ENDED', 'Relay withdrew the namespace', 'connection', { cause: error }),
          }),
      },
    }),
  );
  const { session } = lifetime;
  let closing = false;
  let shutdown: Promise<void> | undefined;

  session.announce(namespace);
  void lifetime.closed.then(({ error }) => {
    closing = true;
    // Only a clean shutdown (no error at all) becomes CLOSED; a closure reason of `null` is preserved.
    abandonAnnounce(
      error === undefined
        ? new RelayError('CLOSED', 'Relay publisher closed before its namespace was announced', 'connection')
        : error,
    );
    for (const slot of tracks.values()) slot.disconnect(error);
  });
  const closed = lifetime.transportClosed.then(
    () => lifetime.closed,
    () => lifetime.closed,
  );

  return {
    announced,
    closed,
    registerTrack({ track, trackProperties, writeTimeoutMs = 10_000 }) {
      // Arguments first, then connection state, then the registry.
      const name = trackName(track);
      const properties = trackPropertyPairs(trackProperties);
      assertTimeoutMs('writeTimeoutMs', writeTimeoutMs);
      lifetime.assertOpen();
      if (closing) throw new RelayError('CLOSED', 'Relay publisher is closed', 'connection');
      if (tracks.has(name)) throw new RangeError('Track name is already registered on this publisher connection');

      const registered = session.registerTrack({
        trackNamespace: namespace,
        trackName: name,
        trackProperties: properties,
      });
      const slot = createWriteSlot({
        openSubgroup: async (trackAlias, groupId) =>
          createSubgroupWriter(await session.openUniStream(trackAlias), { trackAlias, groupId, hasProperties: true }),
        settlingGroups,
        namespaceSolicited: () => session.namespaceSolicited,
        assertOpen: lifetime.assertOpen,
        endTrack: () => registered.end(),
        writeTimeoutMs,
        trackTimeouts: publisherTimeouts(properties),
        subscriberTimeouts: (trackAlias) => subscriptionTimeouts.get(trackAlias) ?? NO_DELIVERY_TIMEOUTS,
      });
      tracks.set(track, slot);
      drainMs = Math.max(drainMs, writeTimeoutMs);
      return slot.writer;
    },
    close() {
      shutdown ??= (async () => {
        closing = true;
        for (const slot of tracks.values()) slot.writer.close();
        // Chromium resets every stream still awaiting its acknowledgment when the session closes,
        // so give the last groups up to one write deadline to settle; after that they may be lost.
        await settlingGroups.drain(drainMs, lifetime.closed);
        lifetime.close();
        await closed;
      })();
      return shutdown;
    },
  };
}
