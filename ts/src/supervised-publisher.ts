/**
 * The supervised publisher: one publisher connection at a time, replaced with backoff whenever it ends, each serving
 * the application's root namespace plus a fresh generation field. Track writers outlive the connections: the
 * application registers a track once and keeps writing to the same writer, which binds to each generation's track in
 * turn. Nothing written is replayed onto a new generation; a write made while no generation is up fails at once with
 * `RECONNECTING`. See spec/behavior.md, "Supervised connections".
 */
import type { ConnectRelayOptions, RelaySessionHooks } from './connect-session.js';
import {
  type ConnectRelayPublisherOptions,
  connectRelayPublisher,
  type RegisterTrackOptions,
  type RelayPublisher,
  type RelayTrackWriter,
} from './publish.js';
import {
  backoffOptions,
  newGeneration,
  RECONNECT_BACKOFF,
  rotateBeforeOption,
  type SupervisedStatus,
  type SupervisionBackoff,
} from './supervision.js';
import { createSupervisor } from './supervisor.js';
import { namespaceFields } from './validation.js';

/** Options for `connectSupervisedPublisher`: the shared connection options, the root namespace and the policy. */
export interface ConnectSupervisedPublisherOptions extends ConnectRelayOptions {
  /**
   * The root namespace. Each connection serves the root followed by one generation field the SDK chooses, so the
   * token must grant the root, not just one namespace under it.
   */
  namespace: readonly string[];
  /** Backoff between connection attempts. Default from 500 ms, doubling up to 10 s, with jitter. */
  reconnect?: SupervisionBackoff;
  /**
   * How long before the connection's credential expires (`RelayPublisher.expiresAt`) the next generation is brought
   * up to replace it, make-before-break. Default 60 seconds; never earlier than halfway through the time left.
   */
  rotateBeforeMs?: number;
  /**
   * Called once each generation's connection is up and every registered track is bound to it. On a rotation, once
   * the next generation is up and every track is registered on it; each track moves to it as described in `rotate()`. A throw stops the
   * supervisor with that error, as `onStatus` does.
   */
  onGeneration?(info: PublisherGeneration): void;
  /**
   * Called on every status change; `error` is what ended the previous connection or failed the last attempt. A throw
   * stops the supervisor with that error: a pending `connectSupervisedPublisher` rejects with it and `closed` resolves
   * with it. A throw reporting `closed` rejects the `close()` that caused it; `closed` resolves either way.
   */
  onStatus?(status: SupervisedStatus, error?: unknown): void;
}

/** One publisher generation: a connection and the namespace it serves. */
export interface PublisherGeneration {
  /** The generation field: start time in fixed-width base 36, then four random characters, so it sorts by age. */
  generation: string;
  /** The namespace this generation serves: the root followed by `generation`. */
  namespace: string[];
}

/**
 * A publisher that reconnects. Create one with {@link connectSupervisedPublisher}. Each connection is a new
 * generation with its own namespace; tracks are registered once and rebound to every generation.
 */
export interface SupervisedPublisher {
  /** The live generation, or `undefined` between connections and after closing. */
  readonly generation: PublisherGeneration | undefined;
  /** Where the supervisor stands; see {@link SupervisedStatus}. */
  readonly status: SupervisedStatus;
  /**
   * Register a track every generation serves, and return the writer that outlives the generations. The same rules as
   * `RelayPublisher.registerTrack`, checked at once: track names are unique per supervised publisher, including after
   * their writer closes. Writes wait for demand on the live generation; while none is up they fail with
   * `RECONNECTING`, and an operation the connection's end interrupts fails with `RECONNECTING` too, never `CLOSED`, so
   * a frame writer treats it as an interruption and resumes at the next keyframe.
   */
  registerTrack(options: RegisterTrackOptions): RelayTrackWriter;
  /** Resolves once the supervisor stops: `close()`, the signal, or a failure it does not retry. Never rejects. */
  readonly closed: Promise<{
    /** The failure that stopped the supervisor, or the signal's reason; absent after `close()`. */
    error?: unknown;
  }>;
  /**
   * Replace the live generation make-before-break, as happens on its own `rotateBeforeMs` before the credential
   * expires: the next generation connects, with a fresh token, and every track is registered on it. Each track's
   * writer then moves to it at its next write or group, once the track has demand there or has none on the old one.
   * The old generation closes once every track has moved, after 10 seconds at most. A no-op while no generation is up.
   */
  rotate(): void;
  /** Stop reconnecting and close the live generation as `RelayPublisher.close()` does. Idempotent. */
  close(): Promise<void>;
}

/**
 * Open a publisher that reconnects: each connection serves `namespace` plus a fresh generation field, and the track
 * writers `registerTrack` returns outlive the connections. Resolves once the first generation is up; see
 * spec/behavior.md, "Supervised connections". The primitives (`connectRelayPublisher`) never reconnect.
 */
export function connectSupervisedPublisher(options: ConnectSupervisedPublisherOptions): Promise<SupervisedPublisher> {
  return superviseRelayPublisher(connectRelayPublisher, options);
}

/** How a supervisor opens one generation's connection. */
export type ConnectPublisher = (options: ConnectRelayPublisherOptions & RelaySessionHooks) => Promise<RelayPublisher>;

/**
 * Open a supervised publisher with `connect` (`connectRelayPublisher` in the browser entry point, the Node one in
 * `@caprid/relay/node`). Resolves once the first generation is up. A first attempt that fails on its arguments or the
 * relay refusing the credentials rejects with that failure; any other failure is retried with backoff, bounded only
 * by `signal`, whose abort rejects with its reason.
 */
export async function superviseRelayPublisher(
  connect: ConnectPublisher,
  options: ConnectSupervisedPublisherOptions,
): Promise<SupervisedPublisher> {
  if (typeof options !== 'object' || options === null) {
    throw new TypeError('connectSupervisedPublisher takes an options object');
  }
  const {
    namespace: given,
    reconnect,
    rotateBeforeMs: rotateBefore,
    onGeneration,
    onStatus,
    signal,
    ...connection
  } = options;
  const root = namespaceFields(given);
  // A generation field is always valid; check the whole namespace once so a root at the size limit fails here.
  namespaceFields([...root, newGeneration()]);
  const backoff = backoffOptions('reconnect', reconnect, RECONNECT_BACKOFF);
  const rotateBeforeMs = rotateBeforeOption(rotateBefore);
  signal?.throwIfAborted();

  const supervisor = createSupervisor<ConnectRelayOptions>(
    async (attempt, generation) => {
      // A publisher connection announces one namespace, chosen as it connects: this generation's.
      const namespace = [...root, generation];
      const publisher = await connect({ ...attempt, namespace });
      return {
        publish(announced) {
          if (JSON.stringify(announced) !== JSON.stringify(namespace)) {
            throw new RangeError('A publisher connection serves only the namespace it connected with');
          }
          return {
            registerTrack: (trackOptions) => publisher.registerTrack(trackOptions),
            closed: publisher.closed,
            close: () => publisher.close(),
          };
        },
        closed: publisher.closed,
        get expiresAt() {
          return publisher.expiresAt;
        },
        close: () => publisher.close(),
      };
    },
    {
      what: 'Supervised publisher',
      connection,
      signal,
      backoff,
      rotateBeforeMs,
      onStatus,
      onGeneration: onGeneration
        ? (generation) => onGeneration({ generation, namespace: [...root, generation] })
        : undefined,
    },
  );
  const publication = supervisor.publish(root);
  await supervisor.start();

  return {
    get generation() {
      const generation = supervisor.generation;
      return generation === undefined ? undefined : { generation, namespace: [...root, generation] };
    },
    get status() {
      return supervisor.status;
    },
    closed: supervisor.closed,
    registerTrack: (trackOptions: RegisterTrackOptions): RelayTrackWriter => publication.registerTrack(trackOptions),
    rotate: () => supervisor.rotate(),
    // A throwing onStatus reporting `closed` rejects the promise rather than throwing from the call.
    close: async () => supervisor.stop({}),
  };
}
