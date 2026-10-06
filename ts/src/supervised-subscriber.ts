/**
 * The supervised subscriber: one subscriber connection at a time, replaced with backoff whenever it ends, and one
 * supervisor per wanted track that keeps a subscription to it open across connections, publisher restarts and
 * generations. Each want retries the announce race, backs off after a publisher's done or a rejection, resubscribes a
 * subscription that went stale, and follows a supervised publisher's newest generation through namespace discovery.
 * Objects are never fetched to fill a gap: a replacement subscription starts live, and `onRestarted` tells the
 * application to drop state that assumed continuity. See spec/behavior.md, "Supervised connections".
 */
import type { MessageParameters, MoqtObject, PublishDone, SubscribeOk } from '@mux/moq-core';
import { type ConnectRelaySubscriberOptions, connectRelaySubscriber, type RelaySubscriber } from './connect.js';
import { assertTimeoutMs } from './connect-session.js';
import { RelayError } from './errors.js';
import type { NamespaceEvents } from './namespace-events.js';
import type { RelaySubscription, SubscriptionEnd } from './subscription.js';
import { SubscriptionConnectionClosedError, SubscriptionRequestError } from './subscription.js';
import {
  backoffDelay,
  backoffOptions,
  isFreshNamespaceRejection,
  isPermanentRequestError,
  isSetupFailure,
  RECONNECT_BACKOFF,
  RESUBSCRIBE_BACKOFF,
  type SupervisedStatus,
  type SupervisionBackoff,
  sleep,
  whenAborted,
} from './supervision.js';
import { namespaceFields, trackName } from './validation.js';

/** Options for `connectSupervisedSubscriber`: the subscriber connection options and the reconnection policy. */
export interface ConnectSupervisedSubscriberOptions extends ConnectRelaySubscriberOptions {
  /** Backoff between connection attempts. Default from 500 ms, doubling up to 10 s, with jitter. */
  reconnect?: SupervisionBackoff;
  /** Called on every status change; `error` is what ended the previous connection or failed the last attempt. */
  onStatus?(status: SupervisedStatus, error?: unknown): void;
}

/** What a want asks for. */
export interface WantOptions {
  /**
   * The track's namespace. With `generations`, a supervised publisher's root instead: the want follows the newest
   * generation announced under it.
   */
  namespace: readonly string[];
  /** Track name within the namespace. */
  track: string;
  /** Message parameters sent on every SUBSCRIBE, such as a location filter or a delivery timeout. */
  parameters?: MessageParameters;
  /**
   * Follow a supervised publisher: discover the namespaces one field below `namespace` and keep the subscription on
   * the newest generation announced there, moving when a newer one is announced or the current one is withdrawn.
   * The subscriber's token must allow discovering the root.
   */
  generations?: boolean;
  /**
   * Resubscribe when no object arrived for this long, from acceptance or the last object. Only for tracks that are
   * never silent while live: a muted microphone or a camera that is off is silent on purpose. After a publisher
   * crash moq-relay can keep an accepted subscription on the dead publisher's cached object for about 11 s.
   */
  staleAfterMs?: number;
  /** Backoff between resubscriptions. Default from 250 ms, doubling up to 8 s, with jitter. */
  retry?: SupervisionBackoff;
}

/** Why a want replaced its subscription. */
export type RestartReason =
  /** The relay sent PUBLISH_DONE: the publisher ended the track or went away. */
  | 'publisher-done'
  /** No object arrived for `staleAfterMs`. */
  | 'stale'
  /** A newer generation was announced, or the current one was withdrawn. */
  | 'generation'
  /** The subscriber connection ended and was replaced. */
  | 'connection'
  /** The relay ended the subscription with REQUEST_ERROR after accepting it. */
  | 'terminated';

/** The callbacks a want delivers to. Like `RelaySubscriptionHandlers`, they must be synchronous. */
export interface WantHandlers {
  /** A subscription was accepted; `namespace` is the one it was made on, the generation's when following one. */
  onSubscribed?(info: SubscribeOk, context: { namespace: string[] }): void;
  /** One object, from whichever subscription is current. */
  onObject?(object: MoqtObject): void;
  /** A subgroup stream ended cleanly. */
  onSubgroupEnd?(info: { groupId: number; subgroupId: number; endOfGroup: boolean }): void;
  /** A subgroup stream was reset. */
  onSubgroupReset?(info: { groupId: number; error: unknown }): void;
  /** The relay sent PUBLISH_DONE on the current subscription; the want resubscribes after its backoff. */
  onPublisherDone?(info: PublishDone): void;
  /**
   * An accepted subscription ended and the want will replace it: drop decoder state that assumed continuity. Called
   * before any object of the replacement; the gap is never filled.
   */
  onRestarted?(info: { reason: RestartReason; namespace: string[] }): void;
  /**
   * A failure the want does not retry, after which it stops and is removed: a REQUEST_ERROR no wait fixes
   * (credentials, an unsupported or malformed request), or discovery refusing the root.
   */
  onError?(error: unknown): void;
}

/**
 * A subscriber that reconnects and keeps wanted tracks subscribed. Create one with
 * {@link connectSupervisedSubscriber}.
 */
export interface SupervisedSubscriber {
  /**
   * Keep a subscription to a track open until `unwant(key)`. Wanting a key again with the same namespace, track,
   * parameters and `generations` only swaps the handlers; with anything else it replaces the want. Arguments are
   * checked at once.
   */
  want(key: string, options: WantOptions, handlers?: WantHandlers): void;
  /** Cancel a want's subscription and forget it. Unknown keys are ignored. */
  unwant(key: string): void;
  /** Where the supervisor stands; see {@link SupervisedStatus}. */
  readonly status: SupervisedStatus;
  /** Resolves once the supervisor stops: `close()`, the signal, or a failure it does not retry. Never rejects. */
  readonly closed: Promise<{
    /** The failure that stopped the supervisor, or the signal's reason; absent after `close()`. */
    error?: unknown;
  }>;
  /** Stop reconnecting, cancel every want and close the connection. Idempotent. */
  close(): void;
}

/**
 * Open a subscriber that reconnects and keeps every wanted track subscribed across connections, publisher restarts
 * and generations. Resolves once the first connection is up; see spec/behavior.md, "Supervised connections". The
 * primitives (`connectRelaySubscriber`) never reconnect or resubscribe.
 */
export function connectSupervisedSubscriber(
  options: ConnectSupervisedSubscriberOptions,
): Promise<SupervisedSubscriber> {
  return superviseRelaySubscriber(connectRelaySubscriber, options);
}

/** How a supervisor opens one subscriber connection. */
export type ConnectSubscriber = (options: ConnectRelaySubscriberOptions) => Promise<RelaySubscriber>;

/** One live subscriber connection and the discovery it runs for followed roots. */
interface Connection {
  subscriber: RelaySubscriber;
  /** Aborts when this connection ends. */
  signal: AbortSignal;
  /** One discovery per followed root, shared by every want on it. */
  roots: Map<string, RootWatch>;
}

/** One want as the supervisor holds it. */
interface Want {
  options: {
    namespace: string[];
    track: string;
    parameters: MessageParameters | undefined;
    generations: boolean;
    staleAfterMs: number | undefined;
  };
  identity: string;
  handlers: WantHandlers;
  retry: Required<SupervisionBackoff>;
  controller: AbortController;
  /** The namespace the latest subscription was made on: the generation's when following one. */
  namespace?: string[];
  /** The generation the latest subscription was made on, when following one. */
  generation?: string | undefined;
}

/**
 * Open a supervised subscriber with `connect`. Resolves once the first connection is up. A first attempt that fails
 * on its arguments or the relay refusing the credentials rejects with that failure; any other failure is retried with
 * backoff, bounded only by `signal`, whose abort rejects with its reason.
 */
export async function superviseRelaySubscriber(
  connect: ConnectSubscriber,
  options: ConnectSupervisedSubscriberOptions,
): Promise<SupervisedSubscriber> {
  if (typeof options !== 'object' || options === null) {
    throw new TypeError('connectSupervisedSubscriber takes an options object');
  }
  const { reconnect, onStatus, signal, ...connection } = options;
  const backoff = backoffOptions('reconnect', reconnect, RECONNECT_BACKOFF);
  signal?.throwIfAborted();

  const wants = new Map<string, Want>();
  let status: SupervisedStatus = 'connecting';
  let stopped = false;
  let current: Connection | undefined;
  const connectionWaiters: (() => void)[] = [];
  let resolveClosed: (value: { error?: unknown }) => void = () => {};
  const closed = new Promise<{ error?: unknown }>((resolve) => {
    resolveClosed = resolve;
  });
  const lifetime = new AbortController();
  /** Aborts the connection attempt in progress only: a live connection is closed cleanly on `close()`. */
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

  const stop = (result: { error?: unknown }) => {
    if (stopped) return;
    stopped = true;
    const reason = result.error ?? new RelayError('CLOSED', 'Supervised subscriber is closed', 'connection');
    lifetime.abort(reason);
    attemptInProgress?.abort(reason);
    // An abort while the first connection is still being made rejects the call that is waiting for it.
    first?.reject(reason);
    first = undefined;
    for (const want of wants.values()) want.controller.abort();
    wants.clear();
    current?.subscriber.close();
    current = undefined;
    for (const wake of connectionWaiters.splice(0)) wake();
    setStatus('closed');
    resolveClosed(result);
  };
  const onAbort = () => stop({ error: signal?.reason });
  signal?.addEventListener('abort', onAbort, { once: true });

  /** The live connection, waiting for one while reconnecting; `undefined` once `scope` aborts. */
  const liveConnection = async (scope: AbortSignal): Promise<Connection | undefined> => {
    while (!scope.aborted && !stopped) {
      if (current && !current.signal.aborted) return current;
      const woken = new Promise<void>((resolve) => connectionWaiters.push(resolve));
      await Promise.race([woken, whenAborted(scope)]);
    }
    return undefined;
  };

  const run = async () => {
    let attempt = 0;
    while (!stopped) {
      let subscriber: RelaySubscriber;
      try {
        attemptInProgress = new AbortController();
        subscriber = await connect({
          ...connection,
          signal: signal ? AbortSignal.any([signal, attemptInProgress.signal]) : attemptInProgress.signal,
        });
      } catch (error) {
        attemptInProgress = undefined;
        if (stopped) return;
        if (first && isSetupFailure(error)) {
          first.reject(error);
          first = undefined;
          stop({ error });
          return;
        }
        setStatus('reconnecting', error);
        await sleep(backoffDelay(attempt++, backoff), lifetime.signal).catch(() => {});
        continue;
      }
      attemptInProgress = undefined;
      if (stopped) {
        subscriber.close();
        return;
      }
      const ended = new AbortController();
      const live: Connection = { subscriber, signal: ended.signal, roots: new Map() };
      current = live;
      const upAt = performance.now();
      setStatus('live');
      first?.resolve();
      first = undefined;
      for (const wake of connectionWaiters.splice(0)) wake();

      const { error } = await subscriber.closed;
      ended.abort();
      for (const watch of live.roots.values()) watch.close();
      if (stopped) return;
      current = undefined;
      setStatus('reconnecting', error);
      if (performance.now() - upAt > backoff.maxDelayMs) attempt = 0;
      await sleep(backoffDelay(attempt++, backoff), lifetime.signal).catch(() => {});
    }
  };
  void run();
  try {
    await started;
  } catch (error) {
    signal?.removeEventListener('abort', onAbort);
    throw error;
  }
  if (stopped) throw signal?.reason ?? new RelayError('CLOSED', 'Supervised subscriber is closed', 'connection');

  const forget = (key: string, want: Want) => {
    want.controller.abort();
    if (wants.get(key) === want) wants.delete(key);
  };

  return {
    get status() {
      return status;
    },
    closed,
    want(key, wantOptions, handlers = {}) {
      if (typeof key !== 'string') throw new TypeError('want key must be a string');
      if (typeof wantOptions !== 'object' || wantOptions === null) {
        throw new TypeError('want takes an options object');
      }
      if (typeof handlers !== 'object' || handlers === null) throw new TypeError('want handlers must be an object');
      for (const [name, callback] of Object.entries(handlers)) {
        if (callback !== undefined && typeof callback !== 'function') {
          throw new TypeError(`${name} must be a function`);
        }
        if (Object.prototype.toString.call(callback) === '[object AsyncFunction]') {
          throw new TypeError(`${name} must be synchronous`);
        }
      }
      const { parameters, generations = false, staleAfterMs, retry } = wantOptions;
      const namespace = namespaceFields(wantOptions.namespace);
      if (generations) namespaceFields([...namespace, 'x']);
      const track = trackName(wantOptions.track);
      if (staleAfterMs !== undefined) assertTimeoutMs('staleAfterMs', staleAfterMs);
      const retryBackoff = backoffOptions('retry', retry, RESUBSCRIBE_BACKOFF);
      if (stopped) throw new RelayError('CLOSED', 'Supervised subscriber is closed', 'connection');

      const identity = JSON.stringify([namespace, track, generations, parameters ?? null], (_, value) =>
        typeof value === 'bigint' ? `${value}n` : value,
      );
      const existing = wants.get(key);
      if (existing && existing.identity === identity) {
        existing.handlers = handlers;
        existing.options = { ...existing.options, staleAfterMs };
        existing.retry = retryBackoff;
        return;
      }
      if (existing) forget(key, existing);
      const want: Want = {
        options: { namespace, track, parameters, generations, staleAfterMs },
        identity,
        handlers,
        retry: retryBackoff,
        controller: new AbortController(),
      };
      wants.set(key, want);
      void superviseWant(want, liveConnection, () => forget(key, want));
    },
    unwant(key) {
      const want = wants.get(key);
      if (want) forget(key, want);
    },
    close() {
      signal?.removeEventListener('abort', onAbort);
      stop({});
    },
  };
}

/**
 * Whether a request refused at the call is the want's own fault, which no connection fixes: an argument, or a root
 * overlapping another followed root. Anything else thrown there (CLOSED, the connection's TRANSPORT_ERROR or GOAWAY)
 * is the connection ending before the supervisor has heard of it.
 */
function refusedForGood(error: unknown): boolean {
  if (error instanceof TypeError || error instanceof RangeError) return true;
  return error instanceof RelayError && error.code === 'PREFIX_OVERLAP';
}

/** What ended one attempt, and what the want does next. */
type AttemptOutcome =
  /** The want or the supervisor was cancelled. */
  | { kind: 'cancelled' }
  /** The connection ended; move to the next one without delay. */
  | { kind: 'connection'; accepted: boolean }
  /** A newer generation, or none; resubscribe without delay. */
  | { kind: 'generation'; accepted: boolean }
  /** A failure no wait fixes; surface it and stop. */
  | { kind: 'fatal'; error: unknown }
  /** Anything else; resubscribe after the backoff. */
  | { kind: 'retry'; accepted: boolean; progressed: boolean; reason?: RestartReason | undefined };

/** Keep `want` subscribed until it is cancelled or fails for good. */
async function superviseWant(
  want: Want,
  liveConnection: (scope: AbortSignal) => Promise<Connection | undefined>,
  forget: () => void,
): Promise<void> {
  const scope = want.controller.signal;
  let failures = 0;
  while (!scope.aborted) {
    const connection = await liveConnection(scope);
    if (!connection) return;
    const outcome = await attempt(want, connection);
    if (scope.aborted) return;
    if (outcome.kind === 'cancelled') return;
    if (outcome.kind === 'fatal') {
      forget();
      want.handlers.onError?.(outcome.error);
      return;
    }
    const reason: RestartReason | undefined =
      outcome.kind === 'connection' ? 'connection' : outcome.kind === 'generation' ? 'generation' : outcome.reason;
    if (outcome.accepted && reason) want.handlers.onRestarted?.({ reason, namespace: [...(want.namespace ?? [])] });
    if (outcome.kind === 'connection') {
      // The request can see the end before the connection reports it; wait for that rather than spin on it.
      await Promise.race([whenAborted(connection.signal), sleep(1_000, scope).catch(() => {})]);
      continue;
    }
    if (outcome.kind !== 'retry') continue;
    // Cached objects followed by a done are not progress: only a later group resets the backoff.
    if (outcome.progressed) failures = 0;
    await backoff(want, connection, backoffDelay(failures++, want.retry));
  }
}

/**
 * Wait out a retry delay, cut short when a fresh attempt can do better at once: the connection ended, or (following
 * generations) a generation other than the last one tried became the newest.
 */
async function backoff(want: Want, connection: Connection, delayMs: number): Promise<void> {
  const wake = new AbortController();
  const scope = AbortSignal.any([want.controller.signal, connection.signal, wake.signal]);
  let held: { watch: RootWatch; release(): void } | undefined;
  if (want.options.generations && !connection.signal.aborted) {
    try {
      held = rootWatch(connection, want.options.namespace);
    } catch {
      // The attempt itself reports a discovery it cannot open.
    }
  }
  // Released in the turn the want is cancelled, as in an attempt, so a replacement can claim an overlapping root.
  if (held) want.controller.signal.addEventListener('abort', held.release, { once: true });
  // Only a newer generation wakes the wait. A discovery that fails again does not: the next attempt reports it after
  // the delay, so failing discovery is retried no faster than the backoff.
  const changed = () => {
    if (!held || held.watch.failure()) return;
    if (held.watch.current() !== want.generation) wake.abort();
  };
  const unwatch = held?.watch.onChange(changed);
  try {
    changed();
    await sleep(delayMs, scope).catch(() => {});
  } finally {
    unwatch?.();
    held?.release();
  }
}

/** One subscription for `want` on `connection`, from SUBSCRIBE to whatever ends it. */
async function attempt(want: Want, connection: Connection): Promise<AttemptOutcome> {
  const { options } = want;
  const scope = AbortSignal.any([want.controller.signal, connection.signal]);
  const cancelledOrConnection = (accepted: boolean): AttemptOutcome =>
    want.controller.signal.aborted ? { kind: 'cancelled' } : { kind: 'connection', accepted };

  let namespace = options.namespace;
  let watch: RootWatch | undefined;
  let release = () => {};
  if (options.generations) {
    try {
      ({ watch, release } = rootWatch(connection, options.namespace));
      // Released in the turn the want is cancelled, so a replacement want can claim an overlapping root at once.
      scope.addEventListener('abort', release, { once: true });
    } catch (error) {
      // As for a subscribe: a connection ending under the call refuses the request with its own error.
      if (scope.aborted || !refusedForGood(error)) return cancelledOrConnection(false);
      return { kind: 'fatal', error };
    }
  }
  try {
    let generation: string | undefined;
    if (watch) {
      const found = await watch.newest(scope);
      if (found.kind === 'failed') return discoveryFailed(found.error, scope, cancelledOrConnection, false);
      if (found.kind !== 'found') return cancelledOrConnection(false);
      generation = found.generation;
      namespace = [...options.namespace, generation];
    }
    want.namespace = namespace;
    want.generation = generation;
    return await subscribeOnce(want, connection, namespace, scope, watch, generation, cancelledOrConnection);
  } finally {
    release();
  }
}

async function subscribeOnce(
  want: Want,
  connection: Connection,
  namespace: string[],
  scope: AbortSignal,
  watch: RootWatch | undefined,
  generation: string | undefined,
  cancelledOrConnection: (accepted: boolean) => AttemptOutcome,
): Promise<AttemptOutcome> {
  const { options } = want;
  let accepted = false;
  let firstGroup: number | undefined;
  let progressed = false;
  let lastObjectAt = 0;
  let publisherDone = false;
  const finish = new AbortController();
  const handlers = () => want.handlers;

  let subscription: RelaySubscription;
  try {
    subscription = connection.subscriber.subscribe(
      { namespace, track: options.track, ...(options.parameters ? { parameters: options.parameters } : {}) },
      {
        onSubscribed: (info) => {
          accepted = true;
          lastObjectAt = performance.now();
          handlers().onSubscribed?.(info, { namespace: [...namespace] });
        },
        onObject: (object) => {
          lastObjectAt = performance.now();
          if (firstGroup === undefined) firstGroup = object.groupId;
          else if (object.groupId !== firstGroup) progressed = true;
          handlers().onObject?.(object);
        },
        onSubgroupEnd: (info) => handlers().onSubgroupEnd?.(info),
        onSubgroupReset: (info) => handlers().onSubgroupReset?.(info),
        onPublisherDone: (info) => {
          publisherDone = true;
          handlers().onPublisherDone?.(info);
          finish.abort();
        },
      },
    );
  } catch (error) {
    // An argument the relay can never take is the want's fault; anything else is the connection ending under the call.
    if (scope.aborted || !refusedForGood(error)) return cancelledOrConnection(false);
    return { kind: 'fatal', error };
  }
  void subscription.subscribed.catch(() => {});

  let stale: ReturnType<typeof setInterval> | undefined;
  const staleAfterMs = options.staleAfterMs;
  let wentStale = false;
  if (staleAfterMs !== undefined) {
    stale = setInterval(
      () => {
        if (accepted && performance.now() - lastObjectAt >= staleAfterMs) {
          wentStale = true;
          finish.abort();
        }
      },
      Math.max(10, Math.min(1_000, Math.floor(staleAfterMs / 4))),
    );
  }
  let moved = false;
  let discoveryEnded: { error: unknown } | undefined;
  const checkWatch = () => {
    if (!watch) return;
    discoveryEnded = watch.failure();
    if (discoveryEnded) finish.abort();
    else if (watch.current() !== generation) {
      moved = true;
      finish.abort();
    }
  };
  const unwatch = watch?.onChange(checkWatch);
  // A change that landed between choosing the generation and attaching the listener is not lost.
  checkWatch();
  try {
    const end = await Promise.race([
      subscription.ended,
      whenAborted(AbortSignal.any([scope, finish.signal])).then(() => undefined),
    ]);
    if (end === undefined) {
      if (scope.aborted) return cancelledOrConnection(accepted);
      if (discoveryEnded) return discoveryFailed(discoveryEnded.error, scope, cancelledOrConnection, accepted);
      if (moved) return { kind: 'generation', accepted };
      return {
        kind: 'retry',
        accepted,
        progressed,
        reason: publisherDone ? 'publisher-done' : wentStale ? 'stale' : undefined,
      };
    }
    return classify(end, accepted, progressed, cancelledOrConnection);
  } finally {
    clearInterval(stale);
    unwatch?.();
    subscription.cancel();
  }
}

/**
 * What a root's discovery ending means for the want: the connection's end moves it on, the relay ending the request
 * or a REQUEST_ERROR a wait may fix is retried with backoff on a fresh request, and anything else is surfaced.
 */
function discoveryFailed(
  error: unknown,
  scope: AbortSignal,
  cancelledOrConnection: (accepted: boolean) => AttemptOutcome,
  accepted: boolean,
): AttemptOutcome {
  if (scope.aborted || error instanceof SubscriptionConnectionClosedError) return cancelledOrConnection(accepted);
  const retry: AttemptOutcome = { kind: 'retry', accepted, progressed: false, reason: 'generation' };
  if (error === undefined) return retry;
  if (error instanceof SubscriptionRequestError && !isPermanentRequestError(error.requestError)) return retry;
  return { kind: 'fatal', error };
}

/** What a subscription's own ending means for the want. */
function classify(
  end: SubscriptionEnd,
  accepted: boolean,
  progressed: boolean,
  cancelledOrConnection: (accepted: boolean) => AttemptOutcome,
): AttemptOutcome {
  switch (end.cause) {
    case 'connection-closed':
    case 'cancelled':
      return cancelledOrConnection(accepted);
    case 'request-error':
      if (!end.accepted) {
        if (isFreshNamespaceRejection(end.error)) return { kind: 'retry', accepted: false, progressed: false };
        if (isPermanentRequestError(end.error)) {
          return { kind: 'fatal', error: new SubscriptionRequestError(end.error) };
        }
        return { kind: 'retry', accepted: false, progressed: false };
      }
      return { kind: 'retry', accepted, progressed, reason: 'terminated' };
  }
}

/** One root's discovery on a connection: the generations announced one field below it. */
interface RootWatch {
  /** The newest generation announced now, or `undefined`. */
  current(): string | undefined;
  /** Wait for a generation to be announced. */
  newest(
    scope: AbortSignal,
  ): Promise<{ kind: 'found'; generation: string } | { kind: 'failed'; error: unknown } | { kind: 'aborted' }>;
  /** How discovery ended, once it has: `error` is undefined when the relay ended it cleanly. */
  failure(): { error: unknown } | undefined;
  /** Observe every change, discovery's end included; returns the removal. */
  onChange(listener: () => void): () => void;
  close(): void;
}

/** Whether two namespace prefixes overlap field by field: equal, or one contains the other. */
function overlaps(a: readonly string[], b: readonly string[]): boolean {
  const [shorter, longer] = a.length <= b.length ? [a, b] : [b, a];
  return shorter.every((field, i) => field === longer[i]);
}

/** The connection's watch for `root`, opened on first use and closed when its last user releases it. */
function rootWatch(connection: Connection, root: string[]): { watch: RootWatch; release(): void } {
  const key = JSON.stringify(root);
  let entry = connection.roots.get(key) as (RootWatch & { users: number }) | undefined;
  if (!entry) {
    // A watch kept for reuse but unused would hold its prefix and refuse an overlapping root: close it first.
    for (const [heldKey, held] of connection.roots) {
      if ((held as RootWatch & { users: number }).users === 0 && overlaps(JSON.parse(heldKey), root)) {
        connection.roots.delete(heldKey);
        held.close();
      }
    }
    // An ended discovery leaves the map at once, so the next attempt asks the relay again.
    const forgetEnded = () => {
      if (connection.roots.get(key) === entry) connection.roots.delete(key);
    };
    entry = Object.assign(openRootWatch(connection.subscriber, root, forgetEnded), { users: 0 });
    connection.roots.set(key, entry);
  }
  const watch = entry;
  watch.users += 1;
  let released = false;
  return {
    watch,
    release() {
      if (released) return;
      released = true;
      watch.users -= 1;
      if (watch.users === 0) {
        // Kept a moment so a want that resubscribes at once reuses the snapshot instead of asking again.
        setTimeout(() => {
          if (watch.users === 0 && connection.roots.get(key) === watch) {
            connection.roots.delete(key);
            watch.close();
          }
        }, 5_000);
      }
    },
  };
}

function openRootWatch(subscriber: RelaySubscriber, root: string[], onEnd: () => void): RootWatch {
  const generations = new Set<string>();
  const listeners = new Set<() => void>();
  let failure: { error: unknown } | undefined;
  // Throws PREFIX_OVERLAP synchronously when another followed root overlaps this one.
  const events: NamespaceEvents = subscriber.namespaceEvents({ prefix: root });
  const notify = () => {
    for (const listener of [...listeners]) listener();
  };
  void (async () => {
    try {
      for await (const event of events) {
        // Only one field below the root is a generation; anything deeper belongs to someone else.
        if (event.namespace.length !== root.length + 1) continue;
        const generation = event.namespace[root.length] as string;
        if (event.type === 'announced') generations.add(generation);
        else generations.delete(generation);
        notify();
      }
      // The relay ended the request, or the watch was closed: neither is a failure of the want.
      failure = { error: undefined };
    } catch (error) {
      failure = { error };
    }
    onEnd();
    notify();
  })();
  const current = () => {
    let newest: string | undefined;
    for (const generation of generations) if (newest === undefined || generation > newest) newest = generation;
    return newest;
  };
  return {
    current,
    failure: () => failure,
    async newest(scope) {
      for (;;) {
        if (failure) return { kind: 'failed', error: failure.error };
        const found = current();
        if (found !== undefined) return { kind: 'found', generation: found };
        if (scope.aborted) return { kind: 'aborted' };
        await new Promise<void>((resolve) => {
          const remove = () => {
            listeners.delete(wake);
            scope.removeEventListener('abort', wake);
          };
          const wake = () => {
            remove();
            resolve();
          };
          listeners.add(wake);
          scope.addEventListener('abort', wake, { once: true });
        });
      }
    },
    onChange(listener) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    close() {
      void events.return();
    },
  };
}
