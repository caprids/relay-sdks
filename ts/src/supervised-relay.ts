/**
 * The supervised relay: one connection that publishes and subscribes (`connectRelay`), replaced with backoff whenever
 * it ends and make-before-break before its credential expires. Each connection is a generation. Every published root
 * is served as the root followed by that connection's generation field, so all of a client's namespaces change
 * generation together, and every want and publishing-roots view moves with them. See spec/behavior.md, "Supervised
 * connections".
 */
import type { Diagnostics } from './client-diagnostics.js';
import type { RelaySessionHooks } from './connect-session.js';
import { diagnosticsOption } from './diagnostics-report.js';
import type { RegisterTrackOptions, RelayTrackWriter } from './publish.js';
import { type ConnectRelaySessionOptions, connectRelay, type RelaySession } from './relay-session.js';
import { createWantSurface, type WantSurface } from './supervised-subscriber.js';
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

/** Options for `connectSupervisedRelay`: `connectRelay`'s options plus the reconnection policy. */
export interface ConnectSupervisedRelayOptions extends ConnectRelaySessionOptions {
  /** Backoff between connection attempts. Default from 500 ms, doubling up to 10 s, with jitter. */
  reconnect?: SupervisionBackoff;
  /**
   * How long before the connection's credential expires (`RelaySession.expiresAt`) a replacement connection is
   * opened, make-before-break. Default 60 seconds; never earlier than halfway through the time left.
   */
  rotateBeforeMs?: number;
  /**
   * Called once each connection is up and every publication's tracks are registered on it. On a rotation, once the
   * replacement is up; tracks and wants then move to it as described in `rotate()`. A throw stops the supervisor with
   * that error, as `onStatus` does.
   */
  onGeneration?(info: { generation: string }): void;
  /**
   * Called on every status change; `error` is what ended the previous connection or failed the last attempt. A throw
   * stops the supervisor with that error: a pending `connectSupervisedRelay` rejects with it and `closed` resolves with
   * it. A throw reporting `closed` rejects the `close()` that caused it; `closed` resolves either way.
   */
  onStatus?(status: SupervisedStatus, error?: unknown): void;
  /**
   * A recorder, or a scope of one, from `@caprid/relay/diagnostics` (spec/behavior.md, "Client diagnostics"): the
   * supervisor reports under its scope `relay` a `link` event on each status change, a `generation` event for each
   * connection, a `want` event at each turn of a want's life, and the `link` and `wants` sections. Publishing the
   * recorder is `publishDiagnostics` on one of its publications, separately.
   */
  diagnostics?: Diagnostics;
}

/**
 * A root published on a supervised relay. Each connection serves it as the root followed by that connection's
 * generation field; the track writers `registerTrack` returns outlive the connections.
 */
export interface SupervisedPublication {
  /** The root, as published. */
  readonly root: readonly string[];
  /** The namespace the live connection serves: the root followed by its generation. `undefined` between connections. */
  readonly namespace: string[] | undefined;
  /**
   * Register a track every generation serves under this root, and return the writer that outlives the connections.
   * The same rules as `SupervisedPublisher.registerTrack`: names are unique per publication, also after their writer
   * closes; writes wait for demand on the live connection, and fail with `RECONNECTING` while none is up.
   */
  registerTrack(options: RegisterTrackOptions): RelayTrackWriter;
  /**
   * End the publication's tracks, wait for their unacknowledged groups as `RelayPublication.close()` does, and
   * withdraw it. The connection and the other publications carry on. The root stays taken. Idempotent.
   */
  close(): Promise<void>;
}

/**
 * One connection that publishes and subscribes, and reconnects. Create one with {@link connectSupervisedRelay}. It has
 * the supervised subscriber's wants and publishing-roots views, and `publish()` for any number of roots; all of them
 * move to each new connection together.
 *
 * The relay never routes a connection's own announcements back to it: `publishingRoots` does not report this
 * connection's own roots, and a want for one of its own tracks is refused (request error 16 on moq-relay).
 */
export interface SupervisedRelay extends WantSurface {
  /** The live connection's generation field, or `undefined` between connections and after closing. */
  readonly generation: string | undefined;
  /** Where the supervisor stands; see {@link SupervisedStatus}. */
  readonly status: SupervisedStatus;
  /** Resolves once the supervisor stops: `close()`, the signal, or a failure it does not retry. Never rejects. */
  readonly closed: Promise<{
    /** The failure that stopped the supervisor, or the signal's reason; absent after `close()`. */
    error?: unknown;
  }>;
  /**
   * Publish `root` on every connection, as the root followed by the connection's generation field. The token must
   * grant publishing under the root. A root published once stays taken, also after its publication closes. A
   * connection on which the relay stops soliciting one of the roots is replaced: every root changes generation
   * together.
   */
  publish(root: readonly string[]): SupervisedPublication;
  /**
   * Replace the connection make-before-break, as happens on its own `rotateBeforeMs` before the credential expires: a
   * new connection opens with a fresh token and the next generation. Every track is registered on it and moves at its
   * next write or group, once it has demand there or none on the old connection; every want moves its subscription as
   * `SupervisedSubscriber.rotate()` describes. The old connection closes once everything has moved, after 10 seconds
   * at most. A no-op while no connection is up.
   */
  rotate(): void;
  /**
   * Stop reconnecting, cancel every want and view, end every publication's tracks, and close the connection as
   * `RelaySession.close()` does. Idempotent.
   */
  close(): Promise<void>;
}

/**
 * Open one connection that publishes and subscribes, and reconnects: each connection is a generation, and every
 * published root, want and publishing-roots view moves to the next one together. Resolves once the first connection
 * is up; see spec/behavior.md, "Supervised connections". The primitive (`connectRelay`) never reconnects.
 */
export function connectSupervisedRelay(options: ConnectSupervisedRelayOptions): Promise<SupervisedRelay> {
  return superviseRelay(connectRelay, options);
}

/** How a supervised relay opens one connection. */
export type ConnectSession = (options: ConnectRelaySessionOptions & RelaySessionHooks) => Promise<RelaySession>;

/**
 * Open a supervised relay with `connect` (`connectRelay` in the browser entry point, the Node one in
 * `@caprid/relay/node`). Resolves once the first connection is up. A first attempt that fails on its arguments or the
 * relay refusing the credentials rejects with that failure; any other failure is retried with backoff, bounded only
 * by `signal`, whose abort rejects with its reason.
 */
export async function superviseRelay(
  connect: ConnectSession,
  options: ConnectSupervisedRelayOptions,
): Promise<SupervisedRelay> {
  if (typeof options !== 'object' || options === null) {
    throw new TypeError('connectSupervisedRelay takes an options object');
  }
  const {
    reconnect,
    rotateBeforeMs: rotateBefore,
    onGeneration,
    onStatus,
    signal,
    diagnostics,
    ...connection
  } = options;
  const backoff = backoffOptions('reconnect', reconnect, RECONNECT_BACKOFF);
  const reporting = diagnosticsOption(diagnostics)?.scope('relay');
  const rotateBeforeMs = rotateBeforeOption(rotateBefore);
  signal?.throwIfAborted();

  const supervisor = createSupervisor<ConnectRelaySessionOptions>(
    async (attempt) => {
      const session = await connect(attempt);
      return {
        subscriber: session,
        publish: (namespace) => session.publish(namespace),
        closed: session.closed,
        get expiresAt() {
          return session.expiresAt;
        },
        close: () => session.close(),
      };
    },
    {
      what: 'Supervised relay',
      connection,
      signal,
      backoff,
      rotateBeforeMs,
      onStatus,
      onGeneration: onGeneration ? (generation) => onGeneration({ generation }) : undefined,
      diagnostics: reporting,
    },
  );
  const wants = createWantSurface(supervisor, reporting);
  await supervisor.start();

  return {
    get generation() {
      return supervisor.generation;
    },
    get status() {
      return supervisor.status;
    },
    closed: supervisor.closed,
    ...wants,
    publish(given) {
      const root = namespaceFields(given);
      // A generation field is always valid; check the whole namespace once so a root at the size limit fails here.
      namespaceFields([...root, newGeneration()]);
      const publication = supervisor.publish(root);
      return {
        root: [...root],
        get namespace() {
          return publication.namespace;
        },
        registerTrack: (trackOptions) => publication.registerTrack(trackOptions),
        close: () => publication.close(),
      };
    },
    rotate() {
      supervisor.rotate();
    },
    // A throwing onStatus reporting `closed` rejects the promise rather than throwing from the call.
    close: async () => supervisor.stop({}),
  };
}
