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
import {
  backoffDelay,
  backoffOptions,
  isSetupFailure,
  newGeneration,
  RECONNECT_BACKOFF,
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
  /** Called once each generation's connection is up and every registered track is bound to it. */
  onGeneration?(info: PublisherGeneration): void;
  /** Called on every status change; `error` is what ended the previous connection or failed the last attempt. */
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
  const { namespace: given, reconnect, onGeneration, onStatus, signal, ...connection } = options;
  const root = namespaceFields(given);
  // A generation field is always valid; check the whole namespace once so a root at the size limit fails here.
  namespaceFields([...root, newGeneration()]);
  const backoff = backoffOptions('reconnect', reconnect, RECONNECT_BACKOFF);
  signal?.throwIfAborted();

  const tracks = new Map<string, SupervisedTrack>();
  /** Every name ever registered: a closed track's name stays taken, as on a single connection. */
  const names = new Set<string>();
  let current: Generation | undefined;
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
      const live = current;
      current = undefined;
      for (const track of tracks.values()) track.retire();
      if (live) {
        live.ended = true;
        await live.publisher.close().catch(() => {});
      }
      setStatus('closed');
      resolveClosed(result);
    })();
    return shutdown;
  };
  const onAbort = () => void stop({ error: signal?.reason });
  signal?.addEventListener('abort', onAbort, { once: true });

  const run = async () => {
    let attempt = 0;
    while (!stopped) {
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
      } catch (error) {
        attemptInProgress = undefined;
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
      attemptInProgress = undefined;
      if (stopped) {
        await publisher.close().catch(() => {});
        return;
      }
      const live: Generation = { info: { generation, namespace }, publisher, ended: false };
      current = live;
      const upAt = performance.now();
      for (const track of tracks.values()) track.bind(live);
      setStatus('live');
      first?.resolve();
      first = undefined;
      onGeneration?.({ generation, namespace: [...namespace] });

      const { error } = await publisher.closed;
      live.ended = true;
      if (stopped) return;
      current = undefined;
      for (const track of tracks.values()) track.unbind(live);
      setStatus('reconnecting', error);
      // A generation that stayed up longer than the longest delay was a success: start the backoff over.
      if (performance.now() - upAt > backoff.maxDelayMs) attempt = 0;
      await sleep(backoffDelay(attempt++, backoff), stopping.signal).catch(() => {});
    }
  };
  void run();
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

/** Whether a failure from a generation's writer means its connection ended under the operation. */
function endedUnder(error: unknown, generation: Generation): boolean {
  if (!(error instanceof RelayError)) return false;
  return generation.ended || error.code === 'CLOSED' || error.phase === 'connection';
}

type WriteOptions = { properties?: readonly PublishProperty[]; signal?: AbortSignal };

/** One registered track across generations: its writer, the binding to the live generation, and demand. */
class SupervisedTrack {
  readonly writer: RelayTrackWriter;
  #binding: { generation: Generation; writer: RelayTrackWriter } | undefined;
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
      operation: (writer: RelayTrackWriter, generation: Generation) => Promise<T>,
    ): Promise<T> => {
      if (track.#closed) throw new RelayError('CLOSED', 'Published track is closed', 'waiting-for-demand');
      const binding = track.#binding;
      if (!binding) throw reconnecting();
      try {
        return await operation(binding.writer, binding.generation);
      } catch (error) {
        if (!track.#closed && endedUnder(error, binding.generation)) throw reconnecting(error);
        throw error;
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
          return supervisedGroup(group, () => track.#closed, generation);
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

  /** The generation ended: writes fail with RECONNECTING until the next one binds. */
  unbind(generation: Generation): void {
    if (this.#binding?.generation !== generation) return;
    this.#binding = undefined;
    this.#emitDemand(false);
  }

  /** The supervisor stopped: the track is closed for good. */
  retire(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#binding = undefined;
    this.#endDemand();
  }

  async #follow(generation: Generation, writer: RelayTrackWriter): Promise<void> {
    const changes = writer.demandChanges();
    let firstValue = true;
    try {
      for await (const value of changes) {
        if (this.#binding?.generation !== generation) break;
        // The first value is the new connection's current state; `false` repeats what the lost generation reported.
        if (!(firstValue && !value)) this.#emitDemand(value);
        firstValue = false;
      }
    } finally {
      await changes.return?.();
    }
  }

  #emitDemand(value: boolean): void {
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
): RelayGroupWriter {
  let aborted = false;
  const mapped = async <T>(operation: () => Promise<T>): Promise<T> => {
    try {
      return await operation();
    } catch (error) {
      if (!aborted && !trackClosed() && endedUnder(error, generation)) throw reconnecting(error);
      throw error;
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
    close: () => mapped(() => group.close()),
    abort: (reason) => {
      aborted = true;
      group.abort(reason);
    },
  };
}
