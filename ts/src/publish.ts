import { createMoqtPublishSession, createSubgroupWriter, type ObjectStatus } from '@mux/moq-core';
import { assertTimeoutMs, type ConnectRelayOptions, openRelaySession } from './connect-session.js';
import { RelayError } from './errors.js';
import { namespaceFields, propertyPairs, trackName } from './validation.js';
import { createWriteSlot, type WriteSlot } from './write-slot.js';

/**
 * Options for connectRelayPublisher: the shared connection options plus the namespace. There is no
 * request deadline; each track's writeTimeoutMs bounds waiting for demand and writing.
 */
export interface ConnectRelayPublisherOptions extends ConnectRelayOptions {
  /** One namespace per publisher connection. Register any number of tracks within it. */
  namespace: readonly string[];
}

export interface PublishProperty {
  /** Non-negative safe integer: even types encode an integer, odd types encode bytes. */
  type: number;
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
   * add, drop, or rewrite properties before delivery to subscribers. See README.
   */
  trackProperties?: readonly PublishProperty[];
  /** Deadline for relay demand and stream writes. Default 10 seconds, maximum 2147483647. */
  writeTimeoutMs?: number;
}

export interface RelayGroupWriter {
  readonly groupId: number;
  readonly objectCount: number;
  readonly closed: boolean;
  /**
   * Append one object, starting at ID 0. Each write has its own writeTimeoutMs deadline.
   * Properties are encoded as supplied; the relay may transform them before delivery.
   */
  write(
    payload: ArrayBufferView,
    options?: { properties?: readonly PublishProperty[]; status?: ObjectStatus },
  ): Promise<void>;
  /** FIN the group within writeTimeoutMs. Idempotent after successful completion. */
  close(): Promise<void>;
  /**
   * Reset this group and free the track for the next random-access point. After a clean `close()`
   * this has no effect: the FIN already ended the stream (draft-20 §11.4.3).
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
   * and kept for a while after the last viewer leaves: it is neither a viewer count nor a
   * liveness signal. Always false once the track is closed.
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
   * `unroutable` on moq-relay 0.15.8; see spec/behavior.md). It stays pending while no
   * solicitation covers the namespace, where the first write reports `NAMESPACE_NOT_SOLICITED`, and
   * the connection's closure terminates it if that comes first: the closure's error, or a `CLOSED`
   * RelayError after a clean shutdown. Pre-handled, so nobody has to await it.
   */
  readonly announced: Promise<void>;
  /** Ends when the connection shuts down. Errors are returned as data; this never rejects. */
  readonly closed: Promise<{ error?: unknown }>;
  /** End all tracks, retract the namespace, and wait for transport shutdown. Idempotent. */
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
  // Settled by the session's onAnnounced callback; a closure that comes first terminates it.
  let announce: () => void = () => {};
  let abandonAnnounce: (reason: unknown) => void = () => {};
  const announced = new Promise<void>((resolve, reject) => {
    announce = resolve;
    abandonAnnounce = reject;
  });
  // Nobody may await readiness; a rejection must not surface as an unhandled rejection.
  void announced.catch(() => {});
  const lifetime = await openRelaySession(options, (transport, callbacks) =>
    createMoqtPublishSession(transport, {
      implementationName: '@mux/relay publisher',
      callbacks: {
        ...callbacks,
        onAnnounced: () => announce(),
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
      const properties = propertyPairs(trackProperties);
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
        namespaceSolicited: () => session.namespaceSolicited,
        assertOpen: lifetime.assertOpen,
        endTrack: () => registered.end(),
        writeTimeoutMs,
      });
      tracks.set(track, slot);
      return slot.writer;
    },
    async close() {
      closing = true;
      for (const slot of tracks.values()) slot.writer.close();
      lifetime.close();
      await closed;
    },
  };
}
