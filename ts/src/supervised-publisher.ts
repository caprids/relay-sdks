/**
 * The supervised publisher: one publisher connection at a time, replaced with backoff whenever it ends, each serving
 * the application's root namespace plus a fresh generation field. Track writers outlive the connections: the
 * application registers a track once and keeps writing to the same writer, which binds to each generation's track in
 * turn. Nothing written is replayed onto a new generation; a write made while no generation is up fails at once with
 * `RECONNECTING`. See spec/behavior.md, "Supervised connections".
 */
import type { ConnectRelayOptions } from './connect-session.js';
import { assertTimeoutMs } from './connect-session.js';
import { RelayError } from './errors.js';
import {
  type ConnectRelayPublisherOptions,
  connectRelayPublisher,
  type PublishProperty,
  type RegisterTrackOptions,
  type RelayGroupWriter,
  type RelayPublisher,
  type RelayTrackWriter,
} from './publish.js';
import { CLOSURE_GRACE_MS } from './subscription.js';
import {
  backoffDelay,
  backoffOptions,
  isSetupFailure,
  newGeneration,
  RECONNECT_BACKOFF,
  ROTATION_OVERLAP_MS,
  rotateBeforeOption,
  rotationDeadline,
  rotationDue,
  type SupervisedStatus,
  type SupervisionBackoff,
  sleep,
} from './supervision.js';
import { namespaceFields, trackName, trackPropertyPairs } from './validation.js';

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
export type ConnectPublisher = (options: ConnectRelayPublisherOptions) => Promise<RelayPublisher>;

/** One generation as the supervisor holds it. */
interface Generation {
  info: PublisherGeneration;
  publisher: RelayPublisher;
  /** Set once the connection ended or the supervisor stopped using it. */
  ended: boolean;
  /** When it went live, for resetting the backoff. */
  upAt: number;
  /** When it is due to rotate, on the `performance.now()` clock; `undefined` for an unknown expiry. */
  rotateAt: number | undefined;
  /** Operations and open groups running on it; a rotation closes it only once none are left. */
  busy: number;
  /** The holds of groups open on it, released when it ends: the connection closed them. */
  groupHolds: Set<() => void>;
  /** Called whenever `busy` drops to zero. */
  onIdle?: (() => void) | undefined;
}

/** Count an operation or open group on `generation` until the returned release runs. */
function use(generation: Generation): () => void {
  generation.busy += 1;
  let released = false;
  return () => {
    if (released) return;
    released = true;
    generation.busy -= 1;
    if (generation.busy === 0) generation.onIdle?.();
  };
}

/** Resolve once nothing runs on `generation`. */
function idle(generation: Generation): Promise<void> {
  if (generation.busy === 0) return Promise.resolve();
  return new Promise((resolve) => {
    generation.onIdle = resolve;
  });
}

/**
 * Open a supervised publisher with `connect` (`connectRelayPublisher` in the browser entry point, the Node one in
 * `@mux/relay/node`). Resolves once the first generation is up. A first attempt that fails on its arguments or the
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

  const tracks = new Map<string, SupervisedTrack>();
  /** Every name ever registered: a closed track's name stays taken, as on a single connection. */
  const names = new Set<string>();
  let current: Generation | undefined;
  /** The generation a rotation is replacing, while its tracks move off it. */
  let retiring: Generation | undefined;
  /** Retired generations still closing: draining groups the relay has not acknowledged. `close()` waits for them. */
  const draining = new Set<Promise<void>>();
  let status: SupervisedStatus = 'connecting';
  let stopped = false;
  let shutdown: Promise<void> | undefined;
  let resolveClosed: (value: { error?: unknown }) => void = () => {};
  const closed = new Promise<{ error?: unknown }>((resolve) => {
    resolveClosed = resolve;
  });
  // Aborts the attempt in progress and the wait between attempts; a live generation is closed gracefully instead.
  const stopping = new AbortController();
  /** Aborts the connection attempt in progress only: once connected, a generation is closed, not aborted. */
  let attemptInProgress: AbortController | undefined;
  /** Asks the live generation to rotate; replaced for each generation served. */
  /**
   * Set by `rotate()` while a connection is up, and consumed by the rotation that answers it: the next one to start, or
   * one whose replacement is still connecting or being discarded.
   */
  let rotateRequested = false;
  /** Wakes a serve loop waiting for its rotation; replaced for each wait. */
  let wakeRotation: () => void = () => {};
  const requestRotation = () => {
    if (stopped || !current) return;
    rotateRequested = true;
    wakeRotation();
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

  const stop = (result: { error?: unknown }): Promise<void> => {
    if (shutdown) return shutdown;
    stopped = true;
    const reason = result.error ?? new RelayError('CLOSED', 'Supervised publisher is closed', 'connection');
    stopping.abort(reason);
    attemptInProgress?.abort(reason);
    // An abort while the first connection is still being made rejects the call that is waiting for it.
    first?.reject(reason);
    first = undefined;
    shutdown = (async () => {
      const open = [current, retiring].filter((generation) => generation !== undefined);
      current = undefined;
      retiring = undefined;
      for (const track of tracks.values()) track.retire();
      await Promise.all([
        ...open.map((generation) => {
          generation.ended = true;
          return generation.publisher.close().catch(() => {});
        }),
        ...draining,
      ]);
      try {
        setStatus('closed');
      } finally {
        resolveClosed(result);
      }
    })();
    // A throwing onStatus fails `close()` for the caller that made it; a stop nobody awaits leaves it observed.
    shutdown.catch(() => {});
    return shutdown;
  };
  const onAbort = () => void stop({ error: signal?.reason });
  signal?.addEventListener('abort', onAbort, { once: true });

  /** Connect one generation, or `undefined` if the supervisor stopped first. Throws the attempt's failure. */
  const connectGeneration = async (): Promise<Generation | undefined> => {
    const generation = newGeneration();
    const namespace = [...root, generation];
    let publisher: RelayPublisher;
    try {
      attemptInProgress = new AbortController();
      publisher = await connect({
        ...connection,
        namespace,
        signal: signal ? AbortSignal.any([signal, attemptInProgress.signal]) : attemptInProgress.signal,
      });
    } finally {
      attemptInProgress = undefined;
    }
    if (stopped) {
      await publisher.close().catch(() => {});
      return undefined;
    }
    const live: Generation = {
      info: { generation, namespace },
      publisher,
      ended: false,
      upAt: performance.now(),
      rotateAt: rotationDeadline(publisher.expiresAt, rotateBeforeMs),
      busy: 0,
      groupHolds: new Set(),
    };
    // Marked in the turn the connection reports its end, before anything awaiting `closed` resumes.
    void publisher.closed.then(() => {
      live.ended = true;
      // The connection's end closed its groups under the application, which may never close them itself.
      for (const release of [...live.groupHolds]) release();
    });
    return live;
  };

  const report = (live: Generation) => {
    onGeneration?.({ generation: live.info.generation, namespace: [...live.info.namespace] });
  };

  /**
   * Bring up the generation that replaces `live` and move the tracks to it, make-before-break. Returns it, or
   * `undefined` when `live` ended or the supervisor stopped before a replacement was up, or when an automatic rotation
   * found no fresher credential.
   */
  const rotateFrom = async (live: Generation, manual: () => boolean): Promise<Generation | undefined> => {
    let failures = 0;
    let next: Generation | undefined;
    // The live generation ending while its replacement is still connecting is a reconnect: report it at once.
    void live.publisher.closed.then(({ error }) => {
      if (stopped || current !== live) return;
      current = undefined;
      for (const track of tracks.values()) track.unbind(live);
      try {
        setStatus('reconnecting', error);
      } catch (thrown) {
        void stop({ error: thrown });
      }
    });
    while (!stopped && !live.ended) {
      try {
        next = await connectGeneration();
        break;
      } catch {
        if (stopped) return undefined;
        // The live generation keeps serving; try again after the backoff unless it ends first.
        await Promise.race([
          sleep(backoffDelay(failures++, backoff), stopping.signal).catch(() => {}),
          live.publisher.closed,
        ]);
      }
    }
    if (!next) return undefined;
    // `close()` may have run while the helper's result was on its way here: it could not see this connection.
    if (stopped) {
      next.ended = true;
      await next.publisher.close().catch(() => {});
      return undefined;
    }
    if (!live.ended && !manual()) {
      // A credential that expires no later than the live one replaces nothing: the relay would end both together.
      const was = live.publisher.expiresAt?.getTime();
      const now = next.publisher.expiresAt?.getTime();
      if (was !== undefined && (now === undefined || now <= was)) {
        next.ended = true;
        await next.publisher.close().catch(() => {});
        return undefined;
      }
    }
    current = next;
    retiring = live;
    const moved = [...tracks.values()].map((track) => track.handOver(next));
    setStatus('live');
    report(next);
    // Every track moved and every write or group still on the old generation finished, or the overlap ran out.
    // The next generation ending cuts the overlap short too: its tracks need the generation after it now. So does its
    // own rotation falling due, for a credential shorter than the overlap.
    const overlap = new AbortController();
    await Promise.race([
      Promise.all(moved).then(() => idle(live)),
      sleep(ROTATION_OVERLAP_MS, stopping.signal).catch(() => {}),
      live.publisher.closed,
      next.publisher.closed,
      rotationDue(next.rotateAt, new Promise<void>(() => {}), AbortSignal.any([stopping.signal, overlap.signal])),
    ]);
    overlap.abort();
    live.onIdle = undefined;
    for (const track of tracks.values()) track.settle();
    live.ended = true;
    // A track whose next generation ended before it moved is still bound here: it has no generation now.
    for (const track of tracks.values()) track.unbind(live);
    if (retiring === live) retiring = undefined;
    // Closing drains the old generation's unacknowledged groups for up to a write deadline; the next generation is
    // served meanwhile, and `close()` waits for the drain.
    const drain: Promise<void> = live.publisher
      .close()
      .catch(() => {})
      .then(() => {
        draining.delete(drain);
        for (const track of tracks.values()) track.forget(live);
      });
    draining.add(drain);
    return next;
  };

  /** Serve `live` until it ends, or until a rotation replaced it with the generation returned. */
  const serve = async (live: Generation): Promise<{ error?: unknown } | { next: Generation }> => {
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
          AbortSignal.any([stopping.signal, scope.signal]),
        ).then(() => undefined);
        const outcome = await Promise.race([live.publisher.closed, due]);
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
        if (live.ended || stopped) return await live.publisher.closed;
        if (!manual) automatic = false;
      }
    } finally {
      scope.abort();
      wakeRotation = () => {};
    }
  };

  const run = async () => {
    let attempt = 0;
    /** A generation a rotation already brought up, reported and moved the tracks to. */
    let next: Generation | undefined;
    while (!stopped) {
      let live = next;
      next = undefined;
      if (!live) {
        try {
          live = await connectGeneration();
        } catch (error) {
          if (stopped) return;
          if (first && isSetupFailure(error)) {
            first.reject(error);
            first = undefined;
            await stop({ error });
            return;
          }
          setStatus('reconnecting', error);
          await sleep(backoffDelay(attempt++, backoff), stopping.signal).catch(() => {});
          continue;
        }
        if (!live) return;
        // `close()` may have run while the helper's result was on its way here: it could not see this connection.
        if (stopped) {
          live.ended = true;
          await live.publisher.close().catch(() => {});
          return;
        }
        current = live;
        for (const track of tracks.values()) track.bind(live);
        setStatus('live');
        report(live);
        first?.resolve();
        first = undefined;
      }

      const outcome = await serve(live);
      if (stopped) return;
      if ('next' in outcome) {
        next = outcome.next;
        attempt = 0;
        continue;
      }
      live.ended = true;
      current = undefined;
      for (const track of tracks.values()) track.unbind(live);
      // Already reported if the generation ended while a rotation's replacement was connecting.
      if (status !== 'reconnecting') setStatus('reconnecting', outcome.error);
      // A generation that stayed up longer than the longest delay was a success: start the backoff over.
      if (performance.now() - live.upAt > backoff.maxDelayMs) attempt = 0;
      await sleep(backoffDelay(attempt++, backoff), stopping.signal).catch(() => {});
    }
  };
  const supervise = async () => {
    try {
      await run();
    } catch (error) {
      // An application callback (onStatus, onGeneration) threw: stop rather than leave the supervisor neither
      // reconnecting nor closed, and fail a caller still waiting to connect.
      void stop({ error });
    }
  };
  void supervise();
  try {
    await started;
  } catch (error) {
    signal?.removeEventListener('abort', onAbort);
    throw error;
  }
  if (stopped) throw signal?.reason ?? new RelayError('CLOSED', 'Supervised publisher is closed', 'connection');

  return {
    get generation() {
      return current ? { ...current.info, namespace: [...current.info.namespace] } : undefined;
    },
    get status() {
      return status;
    },
    closed,
    registerTrack(trackOptions) {
      if (typeof trackOptions !== 'object' || trackOptions === null) {
        throw new TypeError('registerTrack takes an options object');
      }
      const { track, trackProperties, writeTimeoutMs = 10_000 } = trackOptions;
      const name = trackName(track);
      trackPropertyPairs(trackProperties);
      assertTimeoutMs('writeTimeoutMs', writeTimeoutMs);
      if (stopped) throw new RelayError('CLOSED', 'Supervised publisher is closed', 'connection');
      if (names.has(name)) throw new RangeError('Track name is already registered on this supervised publisher');
      const properties = trackProperties?.map(({ type, value }) => ({
        type,
        // Copied now: every generation registers the track again, after the caller may have reused its buffers.
        value:
          typeof value === 'number' ? value : new Uint8Array(value.buffer, value.byteOffset, value.byteLength).slice(),
      }));
      const supervised = new SupervisedTrack(
        { track: name, writeTimeoutMs, ...(properties ? { trackProperties: properties } : {}) },
        () => tracks.delete(name),
      );
      tracks.set(name, supervised);
      names.add(name);
      if (current) supervised.bind(current);
      return supervised.writer;
    },
    rotate: () => requestRotation(),
    close: () => {
      signal?.removeEventListener('abort', onAbort);
      return stop({});
    },
  };
}

/** The failure for a write made while no generation is up, or one the connection's end interrupted. */
function reconnecting(cause?: unknown): RelayError {
  return new RelayError(
    'RECONNECTING',
    'The supervised publisher is between connections; nothing was replayed. Write the next object when it is live',
    'connection',
    cause === undefined ? undefined : { cause },
  );
}

/**
 * Whether a failure from a generation's writer means its connection ended under the operation. Only the generation's
 * end decides, never the code: `CLOSED` also means a write to a finished group or an abort without a reason, while
 * the generation stays live. An operation can fail a little before the connection reports its end, so a RelayError
 * waits up to the closure grace for that report.
 */
async function endedUnder(error: unknown, generation: Generation): Promise<boolean> {
  if (!(error instanceof RelayError)) return false;
  if (!generation.ended) {
    let timer: ReturnType<typeof setTimeout> | undefined;
    await Promise.race([
      generation.publisher.closed,
      new Promise<void>((resolve) => {
        timer = setTimeout(resolve, CLOSURE_GRACE_MS);
      }),
    ]);
    clearTimeout(timer);
  }
  return generation.ended;
}

type WriteOptions = { properties?: readonly PublishProperty[]; signal?: AbortSignal };

/** One registered track across generations: its writer, the binding to the live generation, and demand. */
class SupervisedTrack {
  readonly writer: RelayTrackWriter;
  #binding: { generation: Generation; writer: RelayTrackWriter } | undefined;
  /** The generation a rotation is moving this track to, until it moves. */
  #pending:
    | {
        generation: Generation;
        writer: RelayTrackWriter;
        moved(): void;
        /** The demand its iterator last reported while pending, reported on the move; later values follow it. */
        heard?: boolean;
      }
    | undefined;
  /** The holds of groups open on any generation, released when the track closes and ends them. */
  #groupHolds = new Set<() => void>();
  /** Writers on generations a rotation moved this track off, kept until they close: their open groups end with it. */
  #retired = new Map<Generation, RelayTrackWriter>();
  #closed = false;
  /** The demand the iterators last reported, so a lost generation reports `false` only after `true`. */
  #lastDemand = false;
  #demandListeners = new Set<(value: boolean | undefined) => void>();

  readonly options: RegisterTrackOptions;

  constructor(options: RegisterTrackOptions, release: () => void) {
    this.options = options;
    const track = this;
    /** Run `operation` on the live generation's writer, mapping an interruption by the connection's end. */
    const onLive = async <T>(
      operation: (writer: RelayTrackWriter, generation: Generation, release: () => void) => Promise<T>,
    ): Promise<T> => {
      if (track.#closed) throw new RelayError('CLOSED', 'Published track is closed', 'waiting-for-demand');
      // A rotation moves the track at an operation's start, never under one.
      track.#maybeMove();
      const binding = track.#binding;
      if (!binding) throw reconnecting();
      // A rotation keeps the generation open until the operation (and a group it opens) is done.
      const release = use(binding.generation);
      try {
        return await operation(binding.writer, binding.generation, release);
      } catch (error) {
        if (!track.#closed && (await endedUnder(error, binding.generation))) throw reconnecting(error);
        throw error;
      } finally {
        release();
      }
    };
    this.writer = {
      get hasDemand() {
        return track.#binding?.writer.hasDemand ?? false;
      },
      get closed() {
        return track.#closed;
      },
      demandChanges: () => track.#demandChanges(),
      openGroup: (options) =>
        onLive(async (writer, generation) => {
          const group = await writer.openGroup(options);
          // The generation the group was opened on, read before the await: a disconnect meanwhile clears the binding.
          // The group holds the generation open until it is closed or aborted.
          const hold = use(generation);
          const release = () => {
            track.#groupHolds.delete(release);
            generation.groupHolds.delete(release);
            hold();
          };
          track.#groupHolds.add(release);
          generation.groupHolds.add(release);
          // Opened under a connection that has ended meanwhile: nothing will release it on the end.
          if (generation.ended) release();
          return supervisedGroup(group, () => track.#closed, generation, release);
        }),
      write: (payload, options?: WriteOptions) => onLive((writer) => writer.write(payload, options)),
      writeText: (text, options?: WriteOptions) => onLive((writer) => writer.writeText(text, options)),
      writeJSON: (value, options?: WriteOptions) => onLive((writer) => writer.writeJSON(value, options)),
      close: () => {
        if (track.#closed) return;
        track.#closed = true;
        release();
        track.#binding?.writer.close();
        track.#binding = undefined;
        for (const retired of track.#retired.values()) retired.close();
        track.#retired.clear();
        // Closing the writers ended their groups: none holds a rotating generation open any more.
        for (const release of [...track.#groupHolds]) release();
        track.#dropPending(true);
        track.#endDemand();
      },
    };
  }

  /** Register this track on a new generation. A registration the new connection refuses leaves it unbound. */
  bind(generation: Generation): void {
    if (this.#closed) return;
    let writer: RelayTrackWriter;
    try {
      writer = generation.publisher.registerTrack(this.options);
    } catch {
      // Its arguments were checked when the application registered it, so only the connection's end lands here.
      return;
    }
    this.#binding = { generation, writer };
    void this.#follow(generation, writer);
  }

  /**
   * A rotation brought up `generation`: register this track on it and move there once it has demand there or none on
   * the current generation. Resolves once moved, or once the track closed or could not be registered.
   */
  handOver(generation: Generation): Promise<void> {
    if (this.#closed) return Promise.resolve();
    if (!this.#binding || this.#binding.generation.ended) {
      this.bind(generation);
      return Promise.resolve();
    }
    let writer: RelayTrackWriter;
    try {
      writer = generation.publisher.registerTrack(this.options);
    } catch {
      // As in `bind`: only the new connection's end lands here.
      return Promise.resolve();
    }
    return new Promise((moved) => {
      this.#pending = { generation, writer, moved };
      void this.#follow(generation, writer);
      this.#maybeMove();
    });
  }

  /** The rotation closed `generation`: nothing on it is left to close with the track. */
  forget(generation: Generation): void {
    this.#retired.delete(generation);
  }

  /** The rotation's overlap is over: move to the next generation now, wherever demand is. */
  settle(): void {
    this.#maybeMove(true);
  }

  /** Move to the pending generation once it has demand or the current one has none, or at once when `force`d. */
  #maybeMove(force = false): void {
    const pending = this.#pending;
    if (!pending) return;
    if (pending.generation.ended) {
      this.#dropPending(false);
      return;
    }
    const binding = this.#binding;
    const stay = binding && !binding.generation.ended && binding.writer.hasDemand && !pending.writer.hasDemand;
    if (stay && !force) return;
    this.#pending = undefined;
    if (binding && !binding.generation.ended) this.#retired.set(binding.generation, binding.writer);
    this.#binding = { generation: pending.generation, writer: pending.writer };
    // Reported here only once its iterator caught up; otherwise the iterator reports the values still queued, in order.
    if (pending.heard !== undefined && pending.heard === pending.writer.hasDemand) this.#emitDemand(pending.heard);
    pending.moved();
  }

  /** Give up the pending generation, closing its writer when the track itself closed. */
  #dropPending(close: boolean): void {
    const pending = this.#pending;
    if (!pending) return;
    this.#pending = undefined;
    if (close) pending.writer.close();
    pending.moved();
  }

  /** The generation ended: writes fail with RECONNECTING until the next one binds. */
  unbind(generation: Generation): void {
    if (this.#pending?.generation === generation) this.#dropPending(false);
    if (this.#binding?.generation !== generation) return;
    this.#binding = undefined;
    this.#emitDemand(false);
  }

  /** The supervisor stopped: the track is closed for good. */
  retire(): void {
    for (const release of [...this.#groupHolds]) release();
    if (this.#closed) return;
    this.#closed = true;
    this.#binding = undefined;
    this.#dropPending(false);
    this.#endDemand();
  }

  async #follow(generation: Generation, writer: RelayTrackWriter): Promise<void> {
    const changes = writer.demandChanges();
    let firstValue = true;
    try {
      for await (const value of changes) {
        const bound = this.#binding?.generation === generation;
        const pending = this.#pending?.generation === generation ? this.#pending : undefined;
        if (!bound && !pending) break;
        if (pending) pending.heard = value;
        // The first value is the new connection's current state; `false` repeats what the lost generation reported.
        if (bound && !(firstValue && !value)) this.#emitDemand(value);
        firstValue = false;
        // Demand arriving on the next generation, or leaving the current one, is what moves a rotating track.
        this.#maybeMove();
      }
    } finally {
      await changes.return?.();
    }
  }

  #emitDemand(value: boolean): void {
    // A repeated `true` is kept: a new upstream subscription, or a move to a generation with demand, is when demand-driven
    // state (a catalog) is written again. Only a `false` that changes nothing is dropped.
    if (!value && !this.#lastDemand) return;
    this.#lastDemand = value;
    for (const listener of this.#demandListeners) listener(value);
  }

  #endDemand(): void {
    for (const listener of this.#demandListeners) listener(undefined);
    this.#demandListeners.clear();
  }

  /** The current demand state, then every change across generations; ends when the track closes. */
  #demandChanges(): AsyncIterableIterator<boolean> {
    const queue: boolean[] = [this.writer.hasDemand];
    let done = this.#closed;
    let wake: (() => void) | undefined;
    const listener = (value: boolean | undefined) => {
      if (value === undefined) done = true;
      else queue.push(value);
      wake?.();
    };
    if (!done) this.#demandListeners.add(listener);
    const finish = (): IteratorResult<boolean> => {
      done = true;
      queue.length = 0;
      this.#demandListeners.delete(listener);
      wake?.();
      return { done: true, value: undefined };
    };
    const iterator: AsyncIterableIterator<boolean> = {
      async next() {
        while (queue.length === 0 && !done) await new Promise<void>((resolve) => (wake = resolve));
        wake = undefined;
        const value = queue.shift();
        return value === undefined ? finish() : { done: false, value };
      },
      async return() {
        return finish();
      },
      [Symbol.asyncIterator]() {
        return iterator;
      },
    };
    return iterator;
  }
}

/** A generation's group writer whose failures map the connection's end to RECONNECTING, as the track's do. */
function supervisedGroup(
  group: RelayGroupWriter,
  trackClosed: () => boolean,
  generation: Generation,
  release: () => void,
): RelayGroupWriter {
  let aborted = false;
  const mapped = async <T>(operation: () => Promise<T>): Promise<T> => {
    try {
      return await operation();
    } catch (error) {
      if (!aborted && !trackClosed() && (await endedUnder(error, generation))) throw reconnecting(error);
      throw error;
    } finally {
      // A finished or failed group no longer holds a rotating generation open.
      if (group.closed) release();
    }
  };
  return {
    get groupId() {
      return group.groupId;
    },
    get objectCount() {
      return group.objectCount;
    },
    get closed() {
      return group.closed;
    },
    write: (payload, options) => mapped(() => group.write(payload, options)),
    writeText: (text, options) => mapped(() => group.writeText(text, options)),
    writeJSON: (value, options) => mapped(() => group.writeJSON(value, options)),
    // Released by `mapped` only once the group is closed: a refused close (WRITE_PENDING) keeps it holding.
    close: () => mapped(() => group.close()),
    abort: (reason) => {
      aborted = true;
      try {
        group.abort(reason);
      } finally {
        release();
      }
    },
  };
}
