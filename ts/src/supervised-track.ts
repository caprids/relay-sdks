/**
 * A track a supervisor serves across connections: the writer the application keeps, bound in turn to each
 * generation's track, and moved make-before-break on a rotation. Shared by every supervisor that publishes.
 */
import { RelayError } from './errors.js';
import type { PublishProperty, RegisterTrackOptions, RelayGroupWriter, RelayTrackWriter } from './publish.js';
import { CLOSURE_GRACE_MS } from './subscription.js';
import { objectPropertyPairs, payloadBytes } from './validation.js';
import { CALL_STARTED_AT, type CallStarted, type MaybeMovedOff, MOVED_OFF, now } from './write-slot.js';

/** One publication on one connection, as the tracks it serves see it. */
export interface TrackGeneration {
  /** Where the tracks are registered on this connection. */
  publisher: {
    registerTrack(options: RegisterTrackOptions): RelayTrackWriter;
    readonly closed: Promise<{ error?: unknown }>;
  };
  /** Set once the publication ended or the supervisor stopped using it. */
  ended: boolean;
  /** Operations and open groups running on it; a rotation closes it only once none are left. */
  busy: number;
  /** The holds of groups open on it, with their groups, released when it ends: the connection closed them. */
  groupHolds: Map<() => void, RelayGroupWriter>;
  /** Called whenever `busy` drops to zero. */
  onIdle?: (() => void) | undefined;
}

/** Count an operation or open group on `generation` until the returned release runs. */
export function use(generation: TrackGeneration): () => void {
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
export function idle(generation: TrackGeneration): Promise<void> {
  if (generation.busy === 0) return Promise.resolve();
  return new Promise((resolve) => {
    generation.onIdle = resolve;
  });
}

/** The failure for a write made while no generation is up, or one the connection's end interrupted. */
export function reconnecting(cause?: unknown): RelayError {
  return new RelayError(
    'RECONNECTING',
    'The supervisor is between connections; nothing was replayed. Write the next object when it is live',
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
export async function endedUnder(error: unknown, generation: TrackGeneration): Promise<boolean> {
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
export class SupervisedTrack {
  readonly writer: RelayTrackWriter;
  /**
   * The live generation's writer. `covered` is set when a move reported the writer's `true`: its iterator's next `true`
   * is that same demand, not a new one, unless a `false` came first.
   */
  #binding: { generation: TrackGeneration; writer: RelayTrackWriter; covered?: boolean } | undefined;
  /** The generation a rotation is moving this track to, until it moves. */
  #pending:
    | {
        generation: TrackGeneration;
        writer: RelayTrackWriter;
        moved(): void;
        /** The value its demand iterator yielded last, while pending. */
        seen?: boolean;
      }
    | undefined;
  /** The holds of groups open on any generation, released when the track closes and ends them. */
  #groupHolds = new Set<() => void>();
  /** Writers on generations a rotation moved this track off, kept until they close: their open groups end with it. */
  #retired = new Map<TrackGeneration, RelayTrackWriter>();
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
      operation: (writer: RelayTrackWriter, generation: TrackGeneration, release: () => void) => Promise<T>,
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
      openGroup: async (options) => {
        // The first object's clock starts at this call, so checking its arguments here counts (§5.2).
        const startedAt = now();
        // Arguments before connection or slot state (spec/behavior.md, "Arguments"), and the checked copies are what
        // the live writer gets: the caller's own objects are read once.
        const signal = options?.signal;
        let opening: Parameters<RelayTrackWriter['openGroup']>[0] & CallStarted = { [CALL_STARTED_AT]: startedAt };
        if (options?.payload !== undefined) {
          opening = { ...opening, payload: payloadBytes(options.payload) };
          if (options.properties !== undefined)
            opening = { ...opening, properties: objectPropertyPairs(options.properties) };
        } else if (options?.properties !== undefined)
          throw new TypeError('Group properties belong to a first object: pass a payload with them');
        if (signal !== undefined) opening = { ...opening, signal };
        return onLive(async (writer, generation) => {
          const group = await writer.openGroup(opening);
          // The generation the group was opened on, read before the await: a disconnect meanwhile clears the binding.
          // The group holds the generation open until it is closed or aborted.
          const hold = use(generation);
          const onAbort = () => {
            if (group.closed) release();
          };
          const release = () => {
            track.#groupHolds.delete(release);
            generation.groupHolds.delete(release);
            signal?.removeEventListener('abort', onAbort);
            hold();
          };
          track.#groupHolds.add(release);
          generation.groupHolds.set(release, group);
          // Opened under a connection that has ended meanwhile: nothing will release it on the end.
          if (generation.ended) release();
          // A group its signal aborted is closed without the wrapper hearing of it: release then, or now if already so.
          if (group.closed) release();
          else signal?.addEventListener('abort', onAbort, { once: true });
          return supervisedGroup(
            group,
            () => track.#closed,
            generation,
            release,
            () => track.#binding?.generation !== generation,
          );
        });
      },
      write: async (payload, options?: WriteOptions) => {
        // The object's clock starts at this call, so checking its arguments here counts (§5.2).
        const startedAt = now();
        // Arguments before connection or slot state (spec/behavior.md, "Arguments"), and the checked copies are what
        // the live writer gets: the caller's own objects are read once.
        const bytes = payloadBytes(payload);
        let writing: WriteOptions & CallStarted = { [CALL_STARTED_AT]: startedAt };
        if (options?.properties !== undefined)
          writing = { ...writing, properties: objectPropertyPairs(options.properties) };
        if (options?.signal !== undefined) writing = { ...writing, signal: options.signal };
        return onLive((writer) => writer.write(bytes, writing));
      },
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
  bind(generation: TrackGeneration): void {
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
  handOver(generation: TrackGeneration): Promise<void> {
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
  forget(generation: TrackGeneration): void {
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
    // The move reports `true` for demand the iterator has not yielded yet; one it already yielded is not covered, so the
    // next `true` it yields is new demand.
    const covered = pending.writer.hasDemand && pending.seen !== true;
    this.#binding = { generation: pending.generation, writer: pending.writer, covered };
    // The move reports the next generation's demand as it is now: `true` again if it has demand, so demand-driven state
    // reaches it, or the loss of demand. Values its iterator had queued before the move are stale and not reported.
    this.#emitDemand(pending.writer.hasDemand);
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
  unbind(generation: TrackGeneration): void {
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

  async #follow(generation: TrackGeneration, writer: RelayTrackWriter): Promise<void> {
    const changes = writer.demandChanges();
    /** The value this iterator yielded last: a `true` after `true` is the relay replacing its upstream subscription. */
    let previous: boolean | undefined;
    try {
      for await (const value of changes) {
        const bound = this.#binding?.generation === generation;
        const pending = this.#pending?.generation === generation ? this.#pending : undefined;
        if (!bound && !pending) break;
        if (pending) pending.seen = value;
        // Any `true` after this iterator's first value is new demand: a repeat, or demand back after a `false`.
        const renewed = value && previous !== undefined;
        previous = value;
        // Demand leaving a generation a rotation is moving off: move first if the next one has demand, so the track
        // reports no `false` it did not have. The move reports the next generation's own demand.
        if (bound && !value && this.#pending) {
          this.#maybeMove();
          if (this.#binding?.generation !== generation) continue;
        }
        const binding = this.#binding;
        if (bound && binding) {
          // A `false` is not reported when the writer has demand again by now, but the `true` after it is renewed
          // demand all the same, which demand-driven state (a catalog) is written again on. Only the move's own `true`
          // is not repeated.
          if (!value) binding.covered = false;
          if (value && binding.covered) binding.covered = false;
          else if (value === writer.hasDemand && (value !== this.#lastDemand || renewed)) this.#emitDemand(value);
        }
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
export function supervisedGroup(
  group: RelayGroupWriter,
  trackClosed: () => boolean,
  generation: TrackGeneration,
  release: () => void,
  movedOff: () => boolean,
): RelayGroupWriter & MaybeMovedOff {
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
    get [MOVED_OFF]() {
      return movedOff();
    },
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
