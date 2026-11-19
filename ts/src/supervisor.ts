/**
 * The one supervision loop every supervised connection runs: one connection at a time, replaced with backoff whenever
 * it ends, and replaced make-before-break before its credential expires. Each connection is a generation. What it
 * carries moves with it: every publication's tracks (registered once, rebound to each generation's namespace) and, on
 * a connection that subscribes, every want and publishing-roots view. `connectSupervisedRelay`,
 * `connectSupervisedPublisher` and `connectSupervisedSubscriber` are views over this loop that differ only in the
 * connection they open. See spec/behavior.md, "Supervised connections".
 */
import type { RelaySubscriber } from './connect.js';
import type { ConnectRelayOptions, RelaySessionHooks } from './connect-session.js';
import { assertTimeoutMs, reportEndpoint } from './connect-session.js';
import { createDiscoveryIndex, DISCOVERY_CAPACITY } from './discovery.js';
import { RelayError } from './errors.js';
import type { RegisterTrackOptions, RelayPublication, RelayTrackWriter } from './publish.js';
import type { Connection } from './supervised-subscriber.js';
import { idle, SupervisedTrack, type TrackGeneration } from './supervised-track.js';
import {
  backoffDelay,
  isSetupFailure,
  newGeneration,
  ROTATION_OVERLAP_MS,
  rotationDeadline,
  rotationDue,
  type SupervisedStatus,
  sleep,
} from './supervision.js';
import { trackName, trackPropertyPairs } from './validation.js';

/** One connection as a supervisor sees it, whatever opened it. */
export interface SupervisedConnection {
  /** The subscribing side; absent on a connection that only publishes. */
  readonly subscriber?: RelaySubscriber | undefined;
  /** Announce `namespace` on this connection; absent on a connection that only subscribes. */
  publish?(namespace: string[]): Pick<RelayPublication, 'registerTrack' | 'closed' | 'close'>;
  readonly closed: Promise<{ error?: unknown }>;
  readonly expiresAt: Date | undefined;
  /** Close the connection: a promise when it drains publications first. */
  close(): Promise<void> | void;
}

/** How a supervisor opens the connection for `generation`. */
export type ConnectSupervised<O> = (
  options: O & RelaySessionHooks & { signal: AbortSignal },
  generation: string,
) => Promise<SupervisedConnection>;

/** What a supervisor is configured with. */
export interface SupervisorConfig<O extends ConnectRelayOptions> {
  /** Names the supervisor in its errors: `Supervised publisher is closed`. */
  what: string;
  /** The options every connection attempt is made with. */
  connection: O;
  signal?: AbortSignal | undefined;
  backoff: { initialDelayMs: number; maxDelayMs: number };
  rotateBeforeMs: number;
  onStatus?: ((status: SupervisedStatus, error?: unknown) => void) | undefined;
  /** Called once a generation is up and every publication's tracks are registered on it. */
  onGeneration?: ((generation: string) => void) | undefined;
}

/** One publication across generations: its root, its tracks, and its namespace on each connection. */
export interface SupervisedPublicationState {
  readonly root: readonly string[];
  /** The namespace this publication serves on the live generation, or `undefined` between connections. */
  readonly namespace: string[] | undefined;
  registerTrack(options: RegisterTrackOptions): RelayTrackWriter;
  /** End the publication's tracks and withdraw it from every connection, draining as a publication's close does. */
  close(): Promise<void>;
}

/** The loop's handle, which the public views wrap. */
export interface Supervisor {
  readonly status: SupervisedStatus;
  /** The live generation's field, or `undefined` between connections and after closing. */
  readonly generation: string | undefined;
  readonly closed: Promise<{ error?: unknown }>;
  /** Resolves once the first generation is up; rejects as `connectSupervised*` does. Call once. */
  start(): Promise<void>;
  /** The live connection's subscribing state, for wants and views. */
  current(): Connection | undefined;
  /** The live connection, waiting for one while reconnecting; `undefined` once `scope` aborts or the loop stops. */
  liveConnection(scope: AbortSignal): Promise<Connection | undefined>;
  /** Serve `root` plus each generation's field on every connection. Throws once stopped. */
  publish(root: string[]): SupervisedPublicationState;
  rotate(): void;
  /** Stop reconnecting and close every connection; settles once their publications drained. */
  stop(result: { error?: unknown }): Promise<void>;
  /** Whether `namespace` lies under a root this supervisor publishes, or ever did. */
  ownsNamespace(namespace: readonly string[]): boolean;
  /** Whether the loop has stopped. */
  readonly stopped: boolean;
  /** The error a call after stopping throws. */
  closedError(): RelayError;
  /** Stop everything subscribing on a connection: called first when the loop stops. */
  onStop(listener: () => void): void;
}

/** A subscriber stand-in for a connection that only publishes: nothing subscribes on it. */
const NO_SUBSCRIBER = new Proxy({} as RelaySubscriber, {
  get(_target, property) {
    if (property === 'then') return undefined;
    throw new Error(`This supervised connection does not subscribe (${String(property)})`);
  },
});

/** One connection the loop holds: the subscribing state wants use, plus what publishes on it. */
interface Live extends Connection {
  session: SupervisedConnection;
  generation: string;
  /** When it went live, for resetting the backoff. */
  upAt: number;
  ended: AbortController;
  retire: AbortController;
  /** Each publication's state on this connection. */
  publications: Map<Publication, PublicationGeneration>;
  /** Settles once the connection ended, or once a publication was lost under it (its `error`). */
  over: Promise<{ error?: unknown }>;
  fault(error: unknown): void;
  /** Set once the loop closes it on purpose: a publication ending then is no fault. */
  closing: boolean;
  /** Set in the turn the session reports its end, before a publication's end that shares its promise is seen. */
  sessionEnded: boolean;
  /** Set once a lost publication ended it while the session was still open. */
  faulted: boolean;
}

interface PublicationGeneration extends TrackGeneration {
  namespace: string[];
  publisher: Pick<RelayPublication, 'registerTrack' | 'closed' | 'close'>;
}

interface Publication {
  root: string[];
  tracks: Map<string, SupervisedTrack>;
  /** Every name ever registered: a closed track's name stays taken, as on a single connection. */
  names: Set<string>;
  closed: boolean;
}

/**
 * Create the loop. Nothing connects until `start()`; publications made before it are served from the first
 * generation on.
 */
export function createSupervisor<O extends ConnectRelayOptions>(
  connect: ConnectSupervised<O>,
  config: SupervisorConfig<O>,
): Supervisor {
  const { what, connection, signal, backoff, rotateBeforeMs, onStatus, onGeneration } = config;
  const publications = new Set<Publication>();
  /** Every root ever published, by key and as given: a closed publication's root stays taken. */
  const roots = new Set<string>();
  const published: string[][] = [];
  const stopListeners: (() => void)[] = [];
  let current: Live | undefined;
  /** The connection a rotation is replacing, while what it carries moves off it. */
  let retiring: Live | undefined;
  /**
   * Connections ended or given up and still closing: draining groups the relay has not acknowledged. `stop()` waits for
   * them, and a publication's `close()` for those that carry it.
   */
  const draining = new Map<Live, Promise<void>>();
  const connectionWaiters: (() => void)[] = [];
  let status: SupervisedStatus = 'connecting';
  let stopped = false;
  let shutdown: Promise<void> | undefined;
  let resolveClosed: (value: { error?: unknown }) => void = () => {};
  const closed = new Promise<{ error?: unknown }>((resolve) => {
    resolveClosed = resolve;
  });
  // Aborts the attempt in progress and the waits between attempts; a live connection is closed cleanly instead.
  const lifetime = new AbortController();
  /** Aborts the connection attempt in progress only: once connected, a connection is closed, not aborted. */
  let attemptInProgress: AbortController | undefined;
  /**
   * The attempt in progress, until the connection it brings back is taken (made current, or discarded): `stop()` closes
   * a connection that comes back, or was not taken, before it ran.
   */
  let connecting: Promise<SupervisedConnection> | undefined;
  /**
   * Set by `rotate()` while a connection is up, and consumed by the rotation that answers it: the next one to start, or
   * one whose replacement is still connecting or being discarded.
   */
  let rotateRequested = false;
  /** Wakes a serve loop waiting for its rotation; replaced for each wait. */
  let wakeRotation: () => void = () => {};

  const closedError = () => new RelayError('CLOSED', `${what} is closed`, 'connection');
  const allTracks = () => [...publications].flatMap((publication) => [...publication.tracks.values()]);
  const wakeWaiters = () => {
    for (const wake of connectionWaiters.splice(0)) wake();
  };

  const setStatus = (next: SupervisedStatus, error?: unknown) => {
    if (status === next && error === undefined) return;
    status = next;
    onStatus?.(next, error);
  };

  let first: { resolve(): void; reject(error: unknown): void } | undefined;
  const started = new Promise<void>((resolve, reject) => {
    first = { resolve, reject };
  });
  // Observed here: `start()` hands it to the caller, and a stop before then must not go unhandled.
  started.catch(() => {});

  /** Close `live` on purpose: the promise its session's close returns, if it drains. */
  const closeLive = (live: Live): Promise<void> | undefined => {
    live.closing = true;
    for (const generation of live.publications.values()) generation.ended = true;
    let closing: Promise<void> | void;
    try {
      closing = live.session.close();
    } catch {
      closing = undefined;
    }
    return closing ? closing.catch(() => {}) : undefined;
  };

  /**
   * Close a connection ended or given up while the loop runs on, draining in the background; `after` runs once it
   * closed. The connection is closed once: a later call only adds its `after`.
   */
  const drainLive = (live: Live, after?: () => void) => {
    const already = draining.get(live);
    if (already) {
      // Started when the connection ended, or for a lost root, before a rotation gave it up: its clean-up still runs
      // once it closed.
      if (after) void already.then(after);
      return;
    }
    const closing = live.closing ? undefined : closeLive(live);
    if (!closing) {
      after?.();
      return;
    }
    draining.set(
      live,
      closing.then(() => {
        draining.delete(live);
        after?.();
      }),
    );
  };

  /** The connection ended or was given up: release what it ran and stop anything still waiting on it. */
  const settleEnded = (live: Live) => {
    live.ended.abort();
    live.discovery.close();
  };

  const stop = (result: { error?: unknown }): Promise<void> => {
    if (shutdown) return shutdown;
    stopped = true;
    const reason = result.error ?? closedError();
    lifetime.abort(reason);
    attemptInProgress?.abort(reason);
    // An abort while the first connection is still being made rejects the call that is waiting for it.
    first?.reject(reason);
    first = undefined;
    for (const listener of stopListeners.splice(0)) listener();
    const open = [current, retiring].filter((live) => live !== undefined);
    current = undefined;
    retiring = undefined;
    for (const track of allTracks()) track.retire();
    for (const live of open) {
      // Registered as drains, so a publication's `close()` after this one waits for its groups too.
      if (!draining.has(live)) {
        const closing = closeLive(live);
        if (closing) draining.set(live, closing);
      }
      settleEnded(live);
    }
    const pending: Promise<void>[] = [...draining.values()];
    // A connection still on its way back to the loop, or not yet taken: only this stop sees it.
    if (connecting) {
      pending.push(
        connecting.then(
          (session) => Promise.resolve(session.close()).catch(() => {}),
          () => {},
        ),
      );
    }
    wakeWaiters();
    const finish = () => {
      try {
        setStatus('closed');
      } finally {
        resolveClosed(result);
      }
    };
    if (pending.length === 0) {
      // Nothing drains: report `closed` now, so a throwing onStatus reaches the caller of `close()` as it runs.
      // Set first: a `close()` from inside onStatus is then the same stop, and keeps this one's result.
      shutdown = Promise.resolve();
      try {
        finish();
      } catch (error) {
        // Every later `close()` reports the same failure.
        shutdown = Promise.reject(error);
        shutdown.catch(() => {});
        throw error;
      }
      return shutdown;
    }
    shutdown = Promise.all(pending).then(finish);
    // A throwing onStatus fails `close()` for the caller that made it; a stop nobody awaits leaves it observed.
    shutdown.catch(() => {});
    return shutdown;
  };
  /** Stop from a place with no caller to tell: a throwing onStatus reporting `closed` settles `closed` regardless. */
  const stopQuietly = (result: { error?: unknown }) => {
    try {
      void stop(result).catch(() => {});
    } catch {
      // A throwing onStatus while reporting `closed`: `closed` is settled either way.
    }
  };
  const onAbort = () => stopQuietly({ error: signal?.reason });
  signal?.addEventListener('abort', onAbort, { once: true });

  const liveConnection = async (scope: AbortSignal): Promise<Connection | undefined> => {
    while (!scope.aborted && !stopped) {
      if (current && !current.signal.aborted) return current;
      const woken = new Promise<void>((resolve) => connectionWaiters.push(resolve));
      await Promise.race([
        woken,
        new Promise<void>((resolve) => {
          if (scope.aborted) resolve();
          else scope.addEventListener('abort', () => resolve(), { once: true });
        }),
      ]);
    }
    return undefined;
  };

  /** Open one connection, or `undefined` if the loop stopped first. Throws the attempt's failure. */
  const connectOnce = async (): Promise<Live | undefined> => {
    const generation = newGeneration();
    let session: SupervisedConnection;
    try {
      attemptInProgress = new AbortController();
      connecting = connect(
        {
          ...connection,
          signal: signal ? AbortSignal.any([signal, attemptInProgress.signal]) : attemptInProgress.signal,
          [reportEndpoint]: (endpoint: string) => {
            connection.url ??= endpoint;
          },
        },
        generation,
      );
      session = await connecting;
    } catch (error) {
      connecting = undefined;
      throw error;
    } finally {
      attemptInProgress = undefined;
    }
    // `stop()` ran while the attempt was in progress: it closes what the attempt brought back.
    if (stopped) return undefined;
    const ended = new AbortController();
    const retire = new AbortController();
    let fault: (error: unknown) => void = () => {};
    const faulted = new Promise<{ error?: unknown }>((resolve) => {
      fault = (error) => {
        live.faulted = true;
        resolve({ error });
      };
    });
    const subscriber = session.subscriber ?? NO_SUBSCRIBER;
    const live: Live = {
      session,
      generation,
      subscriber,
      signal: ended.signal,
      retiring: retire.signal,
      discovery: createDiscoveryIndex((prefix) => subscriber.namespaceEvents({ prefix, capacity: DISCOVERY_CAPACITY })),
      wakes: new Map(),
      holds: 0,
      rotateAt: rotationDeadline(session.expiresAt, rotateBeforeMs),
      upAt: performance.now(),
      ended,
      retire,
      publications: new Map(),
      over: Promise.race([session.closed, faulted]),
      fault: (error) => fault(error),
      closing: false,
      sessionEnded: false,
      faulted: false,
    };
    void session.closed.then(() => {
      live.sessionEnded = true;
    });
    // Marked in the turn the connection reports its end, before anything awaiting `over` resumes: a rotation in
    // progress sees it and falls back to reconnecting.
    void live.over.then(() => {
      ended.abort();
      if (!stopped) {
        // Given up for a lost publication while the session is still open, whichever path was serving it: close it.
        // Its wants move to the next connection once that is up, as on a rotation (`run`).
        if (live.faulted) drainLive(live, () => settleEnded(live));
        // A session can report its end before it drained: after a GOAWAY a duplex session's publisher still waits for
        // the relay to read its tracks' ends. Closing it waits for that, and `stop()` for the close.
        else drainLive(live);
      }
      for (const publication of live.publications.values()) {
        publication.ended = true;
        // The connection's end closed its groups under the application, which may never close them itself.
        for (const release of [...publication.groupHolds.keys()]) release();
      }
    });
    return live;
  };

  /**
   * Announce `publication` on `live` and register its tracks there: bound at once on a fresh connection, or handed
   * over on a rotation. Returns the hand-overs. A connection that refuses the announce is ending: nothing is bound.
   */
  const attach = (live: Live, publication: Publication, handOver: boolean): Promise<void>[] => {
    if (!live.session.publish) return [];
    const namespace = [...publication.root, live.generation];
    let publisher: PublicationGeneration['publisher'];
    try {
      publisher = live.session.publish(namespace);
    } catch {
      return [];
    }
    const state: PublicationGeneration = { namespace, publisher, ended: false, busy: 0, groupHolds: new Map() };
    live.publications.set(publication, state);
    void publisher.closed.then(({ error }) => {
      state.ended = true;
      for (const release of [...state.groupHolds.keys()]) release();
      // The relay stopped soliciting the namespace while the connection lives: a generation serves every root it
      // carries or none, so the connection ends and the next generation announces them all again.
      if (
        error !== undefined &&
        !live.closing &&
        !live.sessionEnded &&
        !publication.closed &&
        live.publications.get(publication) === state
      )
        live.fault(error);
    });
    return [...publication.tracks.values()].map((track) => {
      if (handOver) return track.handOver(state);
      track.bind(state);
      return Promise.resolve();
    });
  };

  /** Every track bound to `live` loses its binding: writes fail with RECONNECTING until the next generation binds. */
  const unbindAll = (live: Live) => {
    for (const [publication, state] of live.publications) {
      for (const track of publication.tracks.values()) track.unbind(state);
    }
  };

  const report = (live: Live) => {
    onGeneration?.(live.generation);
  };

  /**
   * Bring up the connection that replaces `live` and move what it carries, make-before-break. Returns it, or
   * `undefined` when `live` ended or the loop stopped before a replacement was up, or when an automatic rotation found
   * no fresher credential.
   */
  const rotateFrom = async (live: Live, manual: () => boolean): Promise<Live | undefined> => {
    let failures = 0;
    let next: Live | undefined;
    // The live connection ending while its replacement is still connecting is a reconnect: report it at once.
    void live.over.then(({ error }) => {
      if (stopped || current !== live) return;
      current = undefined;
      unbindAll(live);
      try {
        setStatus('reconnecting', error);
      } catch (thrown) {
        stopQuietly({ error: thrown });
      }
    });
    while (!stopped && !live.signal.aborted) {
      try {
        next = await connectOnce();
        break;
      } catch {
        if (stopped) return undefined;
        // The live connection keeps serving; try again after the backoff unless it ends first.
        await Promise.race([sleep(backoffDelay(failures++, backoff), lifetime.signal).catch(() => {}), live.over]);
      }
    }
    // `stop()` may have run while the helper's result was on its way here: it closed this connection.
    if (!next || stopped) return undefined;
    // Consulted whatever happened to the old connection: a queued `rotate()` is answered by this replacement.
    const asked = manual();
    if (!live.signal.aborted && !asked) {
      // A credential that expires no later than the live one replaces nothing: the relay would end both together.
      const was = live.session.expiresAt?.getTime();
      const now = next.session.expiresAt?.getTime();
      if (was !== undefined && (now === undefined || now <= was)) {
        if (stopped) return undefined;
        connecting = undefined;
        drainLive(next);
        return undefined;
      }
    }
    // Checked again just before taking it: an application callback above may have stopped the loop.
    if (stopped) return undefined;
    connecting = undefined;
    current = next;
    retiring = live;
    const moved = [...publications].flatMap((publication) => attach(next, publication, true));
    setStatus('live');
    report(next);
    wakeWaiters();
    const wantsMoved = new Promise<void>((resolve) => {
      live.onIdle = resolve;
    });
    // Every want on the old connection ends its attempt there and subscribes on the new one.
    live.retire.abort();
    if (live.holds === 0) live.onIdle?.();
    // A group can close without the wrapper hearing of it (its signal, the relay resetting demand): release its hold
    // once it is closed, whatever closed it.
    const sweep = () => {
      for (const state of live.publications.values()) {
        for (const [release, group] of [...state.groupHolds]) if (group.closed) release();
      }
    };
    const sweeper = setInterval(sweep, 50);
    // Every track and want moved and every write or group still on the old connection finished, or the overlap ran
    // out. The replacement ending cuts the overlap short too: what it carries needs the connection after it now. So does
    // its own rotation falling due, for a credential shorter than the overlap.
    const overlap = new AbortController();
    await Promise.race([
      Promise.all([
        wantsMoved,
        Promise.all(moved).then(() => {
          sweep();
          return Promise.all([...live.publications.values()].map(idle));
        }),
      ]),
      sleep(ROTATION_OVERLAP_MS, lifetime.signal).catch(() => {}),
      live.over,
      next.over,
      rotationDue(next.rotateAt, new Promise<void>(() => {}), AbortSignal.any([lifetime.signal, overlap.signal])),
    ]);
    overlap.abort();
    clearInterval(sweeper);
    live.onIdle = undefined;
    for (const state of live.publications.values()) state.onIdle = undefined;
    for (const track of allTracks()) track.settle();
    // A track whose next generation ended before it moved is still bound here: it has no generation now.
    for (const state of live.publications.values()) state.ended = true;
    unbindAll(live);
    if (retiring === live) retiring = undefined;
    // Closing drains the old connection's unacknowledged groups for up to a write deadline; the next one is served
    // meanwhile, and `stop()` waits for the drain.
    drainLive(live, () => {
      for (const [publication, state] of live.publications) {
        for (const track of publication.tracks.values()) track.forget(state);
      }
    });
    settleEnded(live);
    return next;
  };

  /** Serve `live` until it ends, or until a rotation replaced it with the connection returned. */
  const serve = async (live: Live): Promise<{ error?: unknown } | { next: Live }> => {
    const scope = new AbortController();
    /** Cleared once an automatic rotation found no fresher credential: only `rotate()` tries again. */
    let automatic = true;
    try {
      for (;;) {
        const asked = new Promise<void>((resolve) => {
          wakeRotation = resolve;
        });
        if (rotateRequested) wakeRotation();
        const due = rotationDue(
          automatic ? live.rotateAt : undefined,
          asked,
          AbortSignal.any([lifetime.signal, scope.signal]),
        ).then(() => undefined);
        const outcome = await Promise.race([live.over, due]);
        if (outcome !== undefined || stopped) return outcome ?? {};
        // A `rotate()` before the replacement is current makes this rotation a manual one: it takes the replacement.
        let manual = false;
        const isManual = () => {
          if (rotateRequested) {
            rotateRequested = false;
            manual = true;
          }
          return manual;
        };
        isManual();
        const next = await rotateFrom(live, isManual);
        if (next) return { next };
        if (live.signal.aborted || stopped) return await live.over;
        if (!manual) automatic = false;
      }
    } finally {
      scope.abort();
      wakeRotation = () => {};
    }
  };

  const run = async () => {
    let attempt = 0;
    /** A connection a rotation already brought up, reported and moved everything to. */
    let next: Live | undefined;
    /**
     * A connection given up for a lost root, still draining: its wants are still subscribed there, and move to the next
     * connection once it is up, make-before-break, rather than wait out the drain.
     */
    let givenUp: Live | undefined;
    while (!stopped) {
      let live = next;
      next = undefined;
      if (!live) {
        try {
          live = await connectOnce();
        } catch (error) {
          if (stopped) return;
          if (first && isSetupFailure(error)) {
            first.reject(error);
            first = undefined;
            await stop({ error });
            return;
          }
          setStatus('reconnecting', error);
          await sleep(backoffDelay(attempt++, backoff), lifetime.signal).catch(() => {});
          continue;
        }
        // `stop()` may have run while the helper's result was on its way here: it closed this connection.
        if (!live || stopped) return;
        connecting = undefined;
        current = live;
        for (const publication of publications) attach(live, publication, false);
        setStatus('live');
        report(live);
        first?.resolve();
        first = undefined;
        wakeWaiters();
        givenUp?.retire.abort();
        givenUp = undefined;
      }

      const outcome = await serve(live);
      if (stopped) return;
      if ('next' in outcome) {
        next = outcome.next;
        attempt = 0;
        continue;
      }
      current = undefined;
      for (const state of live.publications.values()) state.ended = true;
      unbindAll(live);
      if (live.faulted) givenUp = live;
      else settleEnded(live);
      // Already reported if the connection ended while a rotation's replacement was connecting.
      if (status !== 'reconnecting') setStatus('reconnecting', outcome.error);
      // A connection that stayed up longer than the longest delay was a success: start the backoff over.
      if (performance.now() - live.upAt > backoff.maxDelayMs) attempt = 0;
      await sleep(backoffDelay(attempt++, backoff), lifetime.signal).catch(() => {});
    }
  };

  let startedOnce = false;
  const start = async () => {
    if (startedOnce) throw new Error('A supervisor starts once');
    startedOnce = true;
    void (async () => {
      try {
        await run();
      } catch (error) {
        // An application callback (onStatus, onGeneration) threw: stop rather than leave the loop neither
        // reconnecting nor closed, and fail a caller still waiting to connect.
        stopQuietly({ error });
      }
    })();
    try {
      await started;
    } catch (error) {
      signal?.removeEventListener('abort', onAbort);
      throw error;
    }
    if (stopped) throw signal?.reason ?? closedError();
  };

  const publish = (root: string[]): SupervisedPublicationState => {
    if (stopped) throw closedError();
    const key = JSON.stringify(root);
    if (roots.has(key)) throw new RangeError('Namespace is already published on this supervised connection');
    roots.add(key);
    published.push(root);
    const publication: Publication = { root, tracks: new Map(), names: new Set(), closed: false };
    publications.add(publication);
    if (current) attach(current, publication, false);
    let closing: Promise<void> | undefined;
    return {
      root,
      get namespace() {
        const state = current?.publications.get(publication);
        return state ? [...state.namespace] : undefined;
      },
      registerTrack(trackOptions) {
        if (typeof trackOptions !== 'object' || trackOptions === null) {
          throw new TypeError('registerTrack takes an options object');
        }
        const { track, trackProperties, writeTimeoutMs = 10_000 } = trackOptions;
        const name = trackName(track);
        trackPropertyPairs(trackProperties);
        assertTimeoutMs('writeTimeoutMs', writeTimeoutMs);
        if (stopped || publication.closed) throw closedError();
        if (publication.names.has(name)) {
          throw new RangeError('Track name is already registered on this supervised publication');
        }
        const properties = trackProperties?.map(({ type, value }) => ({
          type,
          // Copied now: every generation registers the track again, after the caller may have reused its buffers.
          value:
            typeof value === 'number'
              ? value
              : new Uint8Array(value.buffer, value.byteOffset, value.byteLength).slice(),
        }));
        const supervised = new SupervisedTrack(
          { track: name, writeTimeoutMs, ...(properties ? { trackProperties: properties } : {}) },
          () => publication.tracks.delete(name),
        );
        publication.tracks.set(name, supervised);
        publication.names.add(name);
        const state = current?.publications.get(publication);
        if (state) supervised.bind(state);
        return supervised.writer;
      },
      close() {
        closing ??= (async () => {
          publication.closed = true;
          for (const track of [...publication.tracks.values()]) track.writer.close();
          publications.delete(publication);
          const ends: Promise<void>[] = [];
          for (const live of [current, retiring]) {
            const state = live?.publications.get(publication);
            if (!live || !state) continue;
            state.ended = true;
            ends.push(state.publisher.close().catch(() => {}));
          }
          // A connection a rotation or a lost root gave up still drains this publication's groups.
          for (const [live, drain] of draining) if (live.publications.has(publication)) ends.push(drain);
          await Promise.all(ends);
        })();
        return closing;
      },
    };
  };

  return {
    get status() {
      return status;
    },
    get generation() {
      return current?.generation;
    },
    get stopped() {
      return stopped;
    },
    closed,
    start,
    current: () => current,
    liveConnection,
    publish,
    rotate() {
      if (stopped || !current) return;
      rotateRequested = true;
      wakeRotation();
    },
    stop: (result) => {
      signal?.removeEventListener('abort', onAbort);
      return stop(result);
    },
    closedError,
    ownsNamespace(namespace) {
      return published.some(
        (root) => root.length < namespace.length && root.every((field, i) => namespace[i] === field),
      );
    },
    onStop(listener) {
      stopListeners.push(listener);
    },
  };
}
