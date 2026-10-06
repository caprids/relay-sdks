/**
 * The supervised subscriber: one subscriber connection at a time, replaced with backoff whenever it ends, and one
 * supervisor per wanted track that keeps a subscription to it open across connections, publisher restarts and
 * generations. Each want retries the announce race, backs off after a publisher's done or a rejection, resubscribes a
 * subscription that went stale, and follows a supervised publisher's newest generation through namespace discovery.
 * Objects are never fetched to fill a gap: a replacement subscription starts live, and `onRestarted` tells the
 * application to drop state that assumed continuity. See spec/behavior.md, "Supervised connections".
 */
import {
  isMoqtProtocolError,
  type MessageParameters,
  type MoqtObject,
  type PublishDone,
  type SubscribeOk,
} from '@mux/moq-core';
import { type ConnectRelaySubscriberOptions, connectRelaySubscriber, type RelaySubscriber } from './connect.js';
import { assertTimeoutMs } from './connect-session.js';
import { RelayError } from './errors.js';
import type { NamespaceEvents } from './namespace-events.js';
import type { RelaySubscription, SubscriptionEnd } from './subscription.js';
import { SubscriptionConnectionClosedError, SubscriptionRequestError, synchronousCallback } from './subscription.js';
import {
  backoffDelay,
  backoffOptions,
  isFreshNamespaceRejection,
  isPermanentRequestError,
  isSetupFailure,
  RECONNECT_BACKOFF,
  RESUBSCRIBE_BACKOFF,
  ROTATION_OVERLAP_MS,
  rotateBeforeOption,
  rotationDue,
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
  /**
   * How long before the connection's credential expires (`RelaySubscriber.expiresAt`) a replacement connection is
   * opened, make-before-break. Default 60 seconds; never earlier than halfway through the time left.
   */
  rotateBeforeMs?: number;
  /**
   * Called on every status change; `error` is what ended the previous connection or failed the last attempt. A throw
   * stops the supervisor with that error: a pending `connectSupervisedSubscriber` rejects with it and `closed` resolves
   * with it. A throw reporting `closed` is rethrown from the `close()` that caused it; `closed` resolves either way.
   */
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
  /** The subscriber connection was replaced before its credential expired, and the want moved to the new one. */
  | 'rotation'
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
   * before any object of the replacement; the gap is never filled. On a move to a newer generation or a rotated
   * connection, the old subscription keeps delivering until the replacement's first object, so this is called then.
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
   * parameters, `generations` and `staleAfterMs` only swaps the handlers and the retry backoff; with anything else
   * it replaces the want. Arguments are
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
  /**
   * Replace the connection make-before-break, as happens on its own `rotateBeforeMs` before the credential expires: a
   * new connection opens, with a fresh token, and every want subscribes on it. Each want's old subscription keeps
   * delivering until the replacement's first object arrives. The old connection closes once every want has moved,
   * after 10 seconds at most. A no-op while no connection is up.
   */
  rotate(): void;
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
  /** Aborts when a rotation replaced this connection: its wants move to the new one. */
  retiring: AbortSignal;
  /** One discovery per followed root, shared by every want on it. */
  roots: Map<string, RootWatch>;
  /** Attempts and handoffs still using this connection; a rotation closes it once none are left. */
  holds: number;
  /** Called whenever `holds` drops to zero. */
  onIdle?: (() => void) | undefined;
}

/** Count a use of `connection` until the returned release runs. */
function hold(connection: Connection): () => void {
  connection.holds += 1;
  let released = false;
  return () => {
    if (released) return;
    released = true;
    connection.holds -= 1;
    if (connection.holds === 0) connection.onIdle?.();
  };
}

/**
 * A want's previous subscription, still delivering while its replacement starts: on a move to a newer generation or a
 * rotated connection. It ends at the replacement's first object, or when it ends on its own.
 */
interface Handoff {
  /** Stop delivering the previous subscription, cancel it and report `onRestarted`, then run `held`. Idempotent. */
  complete(): void;
  /** Stop delivering and cancel it without reporting anything: the want itself was cancelled. */
  drop(): void;
  /** The replacement's accepted `onSubscribed`, held back until the handoff completes. */
  held?: (() => void) | undefined;
}

/**
 * What a rotated-away subscription delivered, so its replacement on the same namespace does not deliver it again: the
 * replacement starts at the latest group, which the previous one may have delivered.
 */
interface Cutoff {
  /** The subscription that delivered it; its own objects are never cut. */
  owner: RelaySubscription;
  namespace: readonly string[];
  /** The latest location it delivered, read live while it still delivers. */
  latest(): { groupId: number; objectId: number } | undefined;
}

/** Whether `object` on `namespace` is at or before what `cutoff` already delivered. */
function alreadyDelivered(cutoff: Cutoff, object: MoqtObject, namespace: readonly string[]): boolean {
  const last = cutoff.latest();
  if (last === undefined || namespace.length !== cutoff.namespace.length) return false;
  if (namespace.some((field, i) => field !== cutoff.namespace[i])) return false;
  return object.groupId < last.groupId || (object.groupId === last.groupId && object.objectId <= last.objectId);
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
  /** A discovery failure no wait fixes, seen while backing off, for the loop to surface. */
  deferred?: { error: unknown };
  /** The previous subscription, while it still delivers during a move. */
  handoff?: Handoff | undefined;
  /** What a rotation's previous subscription delivered, kept until a replacement that was accepted ends. */
  cutoff?: Cutoff | undefined;
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
  const { reconnect, rotateBeforeMs: rotateBefore, onStatus, signal, ...connection } = options;
  const backoff = backoffOptions('reconnect', reconnect, RECONNECT_BACKOFF);
  const rotateBeforeMs = rotateBeforeOption(rotateBefore);
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
  /** The connection a rotation is replacing, while its wants move off it. */
  let retiring: Connection | undefined;
  /** Asks the live connection to rotate; replaced for each connection served. */
  let requestRotation: () => void = () => {};

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
    for (const want of wants.values()) {
      want.controller.abort();
      want.handoff?.drop();
    }
    wants.clear();
    current?.subscriber.close();
    current = undefined;
    retiring?.subscriber.close();
    retiring = undefined;
    for (const wake of connectionWaiters.splice(0)) wake();
    try {
      setStatus('closed');
    } finally {
      resolveClosed(result);
    }
  };
  const onAbort = () => {
    try {
      stop({ error: signal?.reason });
    } catch {
      // A throwing onStatus while reporting `closed`: `closed` is settled, and an abort listener has no caller to tell.
    }
  };
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

  /** Open one connection, or `undefined` if the supervisor stopped first. Throws the attempt's failure. */
  const connectOnce = async (): Promise<
    { live: Connection; ended: AbortController; retire: AbortController } | undefined
  > => {
    let subscriber: RelaySubscriber;
    try {
      attemptInProgress = new AbortController();
      subscriber = await connect({
        ...connection,
        signal: signal ? AbortSignal.any([signal, attemptInProgress.signal]) : attemptInProgress.signal,
      });
    } finally {
      attemptInProgress = undefined;
    }
    if (stopped) {
      subscriber.close();
      return undefined;
    }
    const ended = new AbortController();
    // Aborted the moment the connection ends, so a rotation in progress sees it and falls back to reconnecting.
    void subscriber.closed.then(() => ended.abort());
    const retire = new AbortController();
    const live: Connection = {
      subscriber,
      signal: ended.signal,
      retiring: retire.signal,
      roots: new Map(),
      holds: 0,
    };
    return { live, ended, retire };
  };

  type Opened = NonNullable<Awaited<ReturnType<typeof connectOnce>>>;

  /** The connection ended: release what it ran and stop anything still waiting on it. */
  const settleEnded = (opened: Opened) => {
    opened.ended.abort();
    for (const watch of opened.live.roots.values()) watch.close();
  };

  /**
   * Open the connection that replaces `opened`, move every want to it, and close the old one once they have moved.
   * Returns the new connection, or `undefined` when the old one ended or the supervisor stopped before a replacement
   * was up, or when an automatic rotation found no fresher credential.
   */
  const rotateFrom = async (opened: Opened, manual: boolean): Promise<Opened | undefined> => {
    const old = opened.live;
    let failures = 0;
    let next: Opened | undefined;
    // The live connection ending while its replacement is still connecting is a reconnect: report it at once.
    void old.subscriber.closed.then(({ error }) => {
      if (stopped || current !== old) return;
      current = undefined;
      try {
        setStatus('reconnecting', error);
      } catch (thrown) {
        try {
          stop({ error: thrown });
        } catch {
          // A throwing onStatus while reporting `closed`: `closed` is settled either way.
        }
      }
    });
    while (!stopped && !old.signal.aborted) {
      try {
        next = await connectOnce();
        break;
      } catch {
        if (stopped) return undefined;
        // The live connection keeps serving; try again after the backoff unless it ends first.
        await Promise.race([
          sleep(backoffDelay(failures++, backoff), lifetime.signal).catch(() => {}),
          whenAborted(old.signal),
        ]);
      }
    }
    if (!next) return undefined;
    if (!old.signal.aborted && !manual) {
      // A credential that expires no later than the live one replaces nothing: the relay would end both together.
      const was = old.subscriber.expiresAt?.getTime();
      const now = next.live.subscriber.expiresAt?.getTime();
      if (was !== undefined && (now === undefined || now <= was)) {
        next.live.subscriber.close();
        return undefined;
      }
    }
    current = next.live;
    retiring = old;
    setStatus('live');
    for (const wake of connectionWaiters.splice(0)) wake();
    const idle = new Promise<void>((resolve) => {
      old.onIdle = resolve;
    });
    // Every want on the old connection ends its attempt there and subscribes on the new one.
    opened.retire.abort();
    if (old.holds === 0) old.onIdle?.();
    // The replacement ending cuts the overlap short too: the wants on it need the next connection now.
    await Promise.race([
      idle,
      sleep(ROTATION_OVERLAP_MS, lifetime.signal).catch(() => {}),
      whenAborted(old.signal),
      whenAborted(next.live.signal),
    ]);
    old.onIdle = undefined;
    if (retiring === old) retiring = undefined;
    old.subscriber.close();
    settleEnded(opened);
    return next;
  };

  /** Serve `opened` until it ends, or until a rotation replaced it with the connection returned. */
  const serve = async (opened: Opened): Promise<{ error?: unknown } | { next: Opened }> => {
    const { subscriber } = opened.live;
    const scope = new AbortController();
    /** Cleared once an automatic rotation found no fresher credential: only `rotate()` tries again. */
    let automatic = true;
    try {
      for (;;) {
        let manual = false;
        const asked = new Promise<void>((resolve) => {
          requestRotation = () => {
            manual = true;
            resolve();
          };
        });
        const due = rotationDue(
          automatic ? subscriber.expiresAt : undefined,
          rotateBeforeMs,
          asked,
          AbortSignal.any([lifetime.signal, scope.signal]),
        ).then(() => undefined);
        const outcome = await Promise.race([subscriber.closed, due]);
        if (outcome !== undefined || stopped) return outcome ?? {};
        requestRotation = () => {};
        const next = await rotateFrom(opened, manual);
        if (next) return { next };
        if (opened.live.signal.aborted || stopped) return await subscriber.closed;
        if (!manual) automatic = false;
      }
    } finally {
      scope.abort();
      requestRotation = () => {};
    }
  };

  const run = async () => {
    let attempt = 0;
    /** A connection a rotation already opened and moved the wants to. */
    let next: Opened | undefined;
    while (!stopped) {
      let opened = next;
      next = undefined;
      if (!opened) {
        try {
          opened = await connectOnce();
        } catch (error) {
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
        if (!opened) return;
        current = opened.live;
        setStatus('live');
        first?.resolve();
        first = undefined;
        for (const wake of connectionWaiters.splice(0)) wake();
      }
      const upAt = performance.now();

      const outcome = await serve(opened);
      if ('next' in outcome) {
        if (stopped) return;
        next = outcome.next;
        attempt = 0;
        continue;
      }
      settleEnded(opened);
      if (stopped) return;
      current = undefined;
      // Already reported if the connection ended while a rotation's replacement was connecting.
      if (status !== 'reconnecting') setStatus('reconnecting', outcome.error);
      if (performance.now() - upAt > backoff.maxDelayMs) attempt = 0;
      await sleep(backoffDelay(attempt++, backoff), lifetime.signal).catch(() => {});
    }
  };
  const supervise = async () => {
    try {
      await run();
    } catch (error) {
      // An application callback (onStatus) threw: stop rather than leave the supervisor neither reconnecting nor
      // closed, and fail a caller still waiting to connect.
      stop({ error });
    }
  };
  // A second throw, from onStatus reporting `closed`, has nowhere to go: `closed` is settled either way.
  void supervise().catch(() => {});
  try {
    await started;
  } catch (error) {
    signal?.removeEventListener('abort', onAbort);
    throw error;
  }
  if (stopped) throw signal?.reason ?? new RelayError('CLOSED', 'Supervised subscriber is closed', 'connection');

  const forget = (key: string, want: Want) => {
    want.controller.abort();
    want.handoff?.drop();
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
      // The primitive's guard: a declared-async callback is refused, a returned promise is observed, never left unhandled.
      const guarded: Record<string, (...args: unknown[]) => void> = {};
      for (const [name, callback] of Object.entries(handlers)) {
        if (callback === undefined) continue;
        if (typeof callback !== 'function') throw new TypeError(`${name} must be a function`);
        guarded[name] = synchronousCallback(name, callback, handlers);
      }
      const safeHandlers = guarded as WantHandlers;
      const { parameters, generations = false, staleAfterMs, retry } = wantOptions;
      const namespace = namespaceFields(wantOptions.namespace);
      if (generations) namespaceFields([...namespace, 'x']);
      const track = trackName(wantOptions.track);
      if (staleAfterMs !== undefined) assertTimeoutMs('staleAfterMs', staleAfterMs);
      const retryBackoff = backoffOptions('retry', retry, RESUBSCRIBE_BACKOFF);
      if (stopped) throw new RelayError('CLOSED', 'Supervised subscriber is closed', 'connection');

      const identity = JSON.stringify(
        [namespace, track, generations, staleAfterMs ?? null, parameters ?? null],
        (_, value) => (typeof value === 'bigint' ? `${value}n` : value),
      );
      const existing = wants.get(key);
      if (existing && existing.identity === identity) {
        existing.handlers = safeHandlers;
        existing.retry = retryBackoff;
        return;
      }
      if (existing) forget(key, existing);
      const want: Want = {
        options: { namespace, track, parameters, generations, staleAfterMs },
        identity,
        handlers: safeHandlers,
        retry: retryBackoff,
        controller: new AbortController(),
      };
      wants.set(key, want);
      void runWant(want, liveConnection, () => forget(key, want));
    },
    unwant(key) {
      const want = wants.get(key);
      if (want) forget(key, want);
    },
    rotate() {
      requestRotation();
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
  // The core's encoders refuse a request the wire cannot carry (a field too long to encode) the same way every time.
  if (error instanceof TypeError || error instanceof RangeError || isMoqtProtocolError(error)) return true;
  return error instanceof RelayError && error.code === 'PREFIX_OVERLAP';
}

/** What ended one attempt, and what the want does next. */
type AttemptOutcome =
  /** The want or the supervisor was cancelled. */
  | { kind: 'cancelled' }
  /** The connection ended; move to the next one without delay. */
  | { kind: 'connection'; accepted: boolean }
  /** A newer generation, or none; resubscribe without delay. `handedOff`: the old subscription still delivers. */
  | { kind: 'generation'; accepted: boolean; handedOff?: boolean }
  /** A rotation replaced the connection; resubscribe on the new one without delay. */
  | { kind: 'rotation'; accepted: boolean; handedOff?: boolean }
  /** A failure no wait fixes; surface it and stop. */
  | { kind: 'fatal'; error: unknown }
  /** Anything else; resubscribe after the backoff. */
  | { kind: 'retry'; accepted: boolean; progressed: boolean; reason?: RestartReason | undefined };

/** Supervise `want`; a failure its loop did not expect forgets it rather than leave it registered but dead. */
async function runWant(
  want: Want,
  liveConnection: (scope: AbortSignal) => Promise<Connection | undefined>,
  forget: () => void,
): Promise<void> {
  try {
    await superviseWant(want, liveConnection, forget);
  } catch (error) {
    forget();
    // biome-ignore lint/suspicious/noConsole: the want has no caller left to report its failure to
    console.error(new Error('A want stopped on an unexpected failure and was forgotten', { cause: error }));
  }
}

/** Keep `want` subscribed until it is cancelled or fails for good. */
async function superviseWant(
  want: Want,
  liveConnection: (scope: AbortSignal) => Promise<Connection | undefined>,
  forget: () => void,
): Promise<void> {
  const scope = want.controller.signal;
  let failures = 0;
  while (!scope.aborted) {
    if (want.deferred) {
      // A discovery refusal seen while backing off is reported as one seen during an attempt would be.
      forget();
      want.handlers.onError?.(want.deferred.error);
      return;
    }
    const connection = await liveConnection(scope);
    // A want replaced while waiting must not open anything: its replacement may already hold an overlapping root.
    if (!connection || scope.aborted) return;
    // A connection a rotation is replacing takes nothing new; the replacement is already live.
    if (connection.retiring.aborted) {
      await Promise.resolve();
      continue;
    }
    const release = hold(connection);
    let outcome: AttemptOutcome;
    try {
      outcome = await attempt(want, connection);
    } finally {
      release();
    }
    if (scope.aborted) return;
    if (outcome.kind === 'cancelled') return;
    if (outcome.kind === 'fatal') {
      forget();
      want.handlers.onError?.(outcome.error);
      return;
    }
    const reason: RestartReason | undefined =
      outcome.kind === 'connection' || outcome.kind === 'generation' || outcome.kind === 'rotation'
        ? outcome.kind
        : outcome.reason;
    // A handed-off subscription reports its restart when the handoff completes.
    const handedOff = (outcome.kind === 'generation' || outcome.kind === 'rotation') && outcome.handedOff;
    if (outcome.accepted && reason && !handedOff) {
      try {
        want.handlers.onRestarted?.({ reason, namespace: [...(want.namespace ?? [])] });
      } catch (error) {
        // A delivery callback's failure is the application's; the want keeps its track subscribed.
        // biome-ignore lint/suspicious/noConsole: the application's callback failed and has no caller to report to
        console.error(new Error('onRestarted threw; the want resubscribes regardless', { cause: error }));
      }
    }
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
  const scope = AbortSignal.any([want.controller.signal, connection.signal, connection.retiring, wake.signal]);
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
    if (!held) return;
    const failure = held.watch.failure();
    if (failure) {
      // A failure no wait fixes is kept for the next turn of the loop, which surfaces it; any other waits out the delay.
      const outcome = discoveryFailed(
        failure.error,
        new AbortController().signal,
        () => ({ kind: 'cancelled' }),
        false,
      );
      if (outcome.kind === 'fatal') {
        want.deferred = { error: failure.error };
        wake.abort();
      }
      return;
    }
    // A fresh discovery starts empty; no generation yet is not a newer one.
    const newest = held.watch.current();
    if (newest !== undefined && newest !== want.generation) wake.abort();
  };
  const unwatch = held?.watch.onChange(changed);
  try {
    changed();
    await sleep(delayMs, scope).catch(() => {});
  } finally {
    unwatch?.();
    if (held) want.controller.signal.removeEventListener('abort', held.release);
    held?.release();
  }
}

/** One subscription for `want` on `connection`, from SUBSCRIBE to whatever ends it. */
async function attempt(want: Want, connection: Connection): Promise<AttemptOutcome> {
  const { options } = want;
  const scope = AbortSignal.any([want.controller.signal, connection.signal]);
  const cancelledOrConnection = (accepted: boolean): AttemptOutcome =>
    want.controller.signal.aborted
      ? { kind: 'cancelled' }
      : connection.retiring.aborted && !connection.signal.aborted
        ? { kind: 'rotation', accepted }
        : { kind: 'connection', accepted };

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
      const found = await watch.newest(AbortSignal.any([scope, connection.retiring]));
      if (found.kind === 'failed') return discoveryFailed(found.error, scope, cancelledOrConnection, false);
      if (found.kind !== 'found') return cancelledOrConnection(false);
      if (connection.retiring.aborted) return cancelledOrConnection(false);
      generation = found.generation;
      namespace = [...options.namespace, generation];
    }
    if (connection.retiring.aborted) return cancelledOrConnection(false);
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
  /** Whether the application heard of this subscription: its `onSubscribed` ran. A handoff can hold that back. */
  let reported = false;
  /** Whether this subscription's events reach the handlers; a completed handoff turns it off. */
  let delivering = true;
  let firstGroup: number | undefined;
  /** The latest object delivered, by location, for a rotation's replacement to skip what was already delivered. */
  let latest: { groupId: number; objectId: number } | undefined;
  let progressed = false;
  let lastObjectAt = 0;
  let publisherDone = false;
  /** Set when this subscription stays open, delivering, while its replacement starts. */
  let handedOff: Handoff | undefined;
  const finish = new AbortController();
  const handlers = () => want.handlers;
  /** Run the pending handoff's restart, which also reports this subscription's held-back `onSubscribed`. */
  const takeOver = () => want.handoff?.complete();
  const report = (info: SubscribeOk) => {
    reported = true;
    want.namespace = namespace;
    handlers().onSubscribed?.(info, { namespace: [...namespace] });
  };

  let subscription: RelaySubscription;
  try {
    subscription = connection.subscriber.subscribe(
      { namespace, track: options.track, ...(options.parameters ? { parameters: options.parameters } : {}) },
      {
        onSubscribed: (info) => {
          accepted = true;
          lastObjectAt = performance.now();
          const handoff = want.handoff;
          // The previous subscription keeps delivering until this one's first object: report this one then.
          if (handoff) handoff.held = () => report(info);
          else report(info);
        },
        onObject: (object) => {
          if (!delivering) return;
          lastObjectAt = performance.now();
          if (firstGroup === undefined) firstGroup = object.groupId;
          else if (object.groupId > firstGroup) progressed = true;
          const cutoff = want.cutoff;
          if (cutoff && cutoff.owner !== subscription && alreadyDelivered(cutoff, object, namespace)) return;
          if (!reported) {
            takeOver();
            // The restart callback may have cancelled the want or stopped the supervisor.
            if (!delivering || want.controller.signal.aborted) return;
          }
          if (
            latest === undefined ||
            object.groupId > latest.groupId ||
            (object.groupId === latest.groupId && object.objectId > latest.objectId)
          ) {
            latest = { groupId: object.groupId, objectId: object.objectId };
          }
          handlers().onObject?.(object);
        },
        onSubgroupEnd: (info) => {
          if (delivering && reported) handlers().onSubgroupEnd?.(info);
        },
        onSubgroupReset: (info) => {
          if (delivering && reported) handlers().onSubgroupReset?.(info);
        },
        onPublisherDone: (info) => {
          if (!delivering) return;
          publisherDone = true;
          if (reported) handlers().onPublisherDone?.(info);
          finish.abort();
          handedOff?.complete();
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
  let rotating = false;
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
  const onRetiring = () => {
    rotating = true;
    finish.abort();
  };
  if (connection.retiring.aborted) onRetiring();
  else connection.retiring.addEventListener('abort', onRetiring, { once: true });
  try {
    const end = await Promise.race([
      subscription.ended,
      whenAborted(AbortSignal.any([scope, finish.signal])).then(() => undefined),
    ]);
    if (end === undefined) {
      if (scope.aborted) return cancelledOrConnection(reported);
      if (discoveryEnded) return discoveryFailed(discoveryEnded.error, scope, cancelledOrConnection, reported);
      if ((moved || rotating) && !publisherDone && !wentStale) {
        const reason = moved ? 'generation' : 'rotation';
        // Only a subscription the application is hearing from is worth keeping; one move at a time.
        if (reported && !want.handoff) {
          // A subscription that delivered nothing new itself inherits what its predecessor delivered.
          const inherited = want.cutoff;
          want.cutoff =
            reason === 'rotation'
              ? {
                  owner: subscription,
                  namespace: [...namespace],
                  latest: () =>
                    latest ??
                    (inherited && inherited.namespace.join('/') === namespace.join('/')
                      ? inherited.latest()
                      : undefined),
                }
              : undefined;
          handedOff = handOff(want, connection, subscription, reason, namespace, () => {
            delivering = false;
          });
          return { kind: reason, accepted: true, handedOff: true };
        }
        return { kind: reason, accepted: reported };
      }
      return {
        kind: 'retry',
        accepted: reported,
        progressed,
        reason: publisherDone ? 'publisher-done' : wentStale ? 'stale' : undefined,
      };
    }
    return classify(end, reported, progressed, cancelledOrConnection);
  } finally {
    clearInterval(stale);
    unwatch?.();
    connection.retiring.removeEventListener('abort', onRetiring);
    // Settles the losing waiter, so a subscription that ended on its own leaves no listener behind.
    finish.abort();
    // A replacement that ends before it took over never reports its held-back onSubscribed.
    if (!reported && want.handoff) want.handoff.held = undefined;
    // A replacement that took over and ended has had its turn: the next subscription may restart group numbers.
    if (reported && !handedOff && want.cutoff && want.cutoff.owner !== subscription) want.cutoff = undefined;
    if (!handedOff) {
      delivering = false;
      subscription.cancel();
    }
  }
}

/**
 * Keep `subscription` delivering while the want's replacement starts. It ends at the replacement's first object, when
 * it ends on its own, or when the want is cancelled; the first two report `onRestarted` with `reason`.
 */
function handOff(
  want: Want,
  connection: Connection,
  subscription: RelaySubscription,
  reason: RestartReason,
  namespace: string[],
  stopDelivering: () => void,
): Handoff {
  const release = hold(connection);
  let over = false;
  const end = (): boolean => {
    if (over) return false;
    over = true;
    stopDelivering();
    subscription.cancel();
    if (want.handoff === handoff) want.handoff = undefined;
    release();
    return true;
  };
  const handoff: Handoff = {
    complete() {
      const held = handoff.held;
      if (!end()) return;
      // A want cancelled meanwhile, or a supervisor that stopped, reports nothing more.
      if (want.controller.signal.aborted) return;
      try {
        want.handlers.onRestarted?.({ reason, namespace: [...namespace] });
      } catch (error) {
        // biome-ignore lint/suspicious/noConsole: the application's callback failed and has no caller to report to
        console.error(new Error('onRestarted threw; the want resubscribes regardless', { cause: error }));
      }
      if (want.controller.signal.aborted) return;
      try {
        held?.();
      } catch (error) {
        // biome-ignore lint/suspicious/noConsole: the application's callback failed and has no caller to report to
        console.error(new Error('onSubscribed threw; the want stays subscribed', { cause: error }));
      }
    },
    drop() {
      end();
    },
  };
  want.handoff = handoff;
  void subscription.ended.then(() => handoff.complete()).catch(() => {});
  return handoff;
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
