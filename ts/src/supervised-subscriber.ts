/**
 * The supervised subscriber: one subscriber connection at a time, replaced with backoff whenever it ends, and one
 * supervisor per wanted track that keeps a subscription to it open across connections, publisher restarts and
 * generations. Each want retries the announce race, backs off after a publisher's done or a rejection, resubscribes a
 * subscription that went stale, and follows a supervised publisher's newest generation through namespace discovery.
 * Objects are never fetched to fill a gap: a replacement subscription starts live, and `onRestarted` tells the
 * application to drop state that assumed continuity. A move to a newer generation, or to another track or parameters
 * on a re-want, is made before the break instead, as `switchTo` makes one (issue #282): the replacement opens while the
 * current subscription keeps delivering, and `onSwitched` marks the cutover. Before the connection's credential
 * expires, the supervisor rotates to a new connection the same way (issue #261): every want's subscription moves to it
 * make-before-break, and a want that keeps its target sees no break and no repeat. See spec/behavior.md, "Supervised
 * connections".
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
import type { ProductSubscription, RelaySubscription, SubscriptionEnd } from './subscription.js';
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
  rotationDeadline,
  rotationDue,
  type SupervisedStatus,
  type SupervisionBackoff,
  sleep,
  whenAborted,
} from './supervision.js';
import {
  assertDrainMs,
  type HeldEvent,
  heldAfter,
  holdEvent,
  type SwitchOptions,
  untilPredicate,
} from './switchable.js';
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
  /**
   * When a move cuts over to its replacement subscription, as on `switchTo`: `'first-object'` (the default) or a
   * synchronous predicate on the replacement's objects. A move is a newer generation, or a re-want with another track
   * or parameters; the current subscription keeps delivering until then. Compare the object's own timeline with what
   * was delivered, never group IDs: each generation and track numbers its own groups.
   */
  until?: SwitchOptions['until'];
  /**
   * How long the old subscription keeps delivering its in-flight groups after a cutover, in milliseconds, as on
   * `switchTo`: 0 (the default) to 2147483647.
   */
  drainMs?: number;
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

/** Where a want's subscription was made: the namespace (the generation's, when following one) and the track. */
export interface WantTarget {
  /** The namespace the subscription was made on: the generation's, when following one. */
  namespace: string[];
  /** The track name. */
  track: string;
}

/** What `onSwitched` receives at a want's cutover. */
export interface WantSwitchedInfo {
  /** The subscription the want delivered from before the cutover. */
  from: WantTarget;
  /** The subscription it delivers from now. */
  to: WantTarget;
  /** The new subscription's acceptance info. */
  subscribed: SubscribeOk;
}

/** The callbacks a want delivers to. Like `RelaySubscriptionHandlers`, they must be synchronous. */
export interface WantHandlers {
  /**
   * A subscription was accepted; `namespace` is the one it was made on, the generation's when following one. A move's
   * replacement reports its acceptance in `onSwitched` instead.
   */
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
   * A move's cutover: the want now delivers from `to`, and `from` was cancelled or drains its in-flight groups. Runs
   * before the replacement's first object, the one that passed `until`. Decoders drop state that assumed continuity
   * here; no `onRestarted` or `onSubscribed` runs for a move.
   */
  onSwitched?(info: WantSwitchedInfo): void;
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
   * Keep a subscription to a track open until `unwant(key)`. Wanting a key again with the same namespace and
   * `generations` updates the want in place: the handlers, `retry`, `staleAfterMs`, `until` and `drainMs` are swapped,
   * and another track or parameters moves the subscription make-before-break. Another namespace or `generations`
   * replaces the want. Arguments are checked at once.
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
   * new connection opens, with a fresh token, and every want moves its subscription to it as a move would, keeping the
   * old one delivering until the cutover. A want whose target is unchanged cuts over on the first object the old one did
   * not deliver, with no callback: nothing is lost or repeated. The old connection closes once every want has moved,
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
  /** Wants with a subscription still on this connection; a rotation closes it once none are left. */
  holds: number;
  /** Called whenever `holds` drops to zero. */
  onIdle?: (() => void) | undefined;
  /** When it is due to rotate, on the `performance.now()` clock; `undefined` for an unknown expiry. */
  rotateAt: number | undefined;
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

/** One want as the supervisor holds it. */
interface Want {
  options: {
    namespace: string[];
    track: string;
    parameters: MessageParameters | undefined;
    generations: boolean;
    staleAfterMs: number | undefined;
  };
  /** The namespace and `generations`: wanting the key again with others replaces the want. */
  source: string;
  /** The track and parameters a subscription is made with, compared to tell a move from no change. */
  target: string;
  handlers: WantHandlers;
  retry: Required<SupervisionBackoff>;
  until: (object: MoqtObject) => boolean;
  drainMs: number;
  /** The target when the latest attempt settled: a later re-want supersedes that attempt's outcome. */
  settledTarget?: string | undefined;
  /** Told when a re-want changes the want in place, by whichever attempt or backoff is running. */
  changes: Set<() => void>;
  controller: AbortController;
  /** The namespace the latest subscription was made on: the generation's when following one. */
  namespace?: string[];
  /** The generation the latest subscription was made on, when following one. */
  generation?: string | undefined;
  /** The connection the latest subscription was made on: a rotation moves it to the new one. */
  connection?: Connection | undefined;
  /** A discovery failure no wait fixes, seen while backing off, for the loop to surface. */
  deferred?: { error: unknown };
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
      rotateAt: rotationDeadline(subscriber.expiresAt, rotateBeforeMs),
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
  const rotateFrom = async (opened: Opened, manual: () => boolean): Promise<Opened | undefined> => {
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
    // `close()` may have run while the helper's result was on its way here: it could not see this connection.
    if (stopped) {
      next.live.subscriber.close();
      return undefined;
    }
    // Consulted whatever happened to the old connection: a queued `rotate()` is answered by this replacement.
    const asked = manual();
    if (!old.signal.aborted && !asked) {
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
    // The replacement ending cuts the overlap short too: the wants on it need the next connection now. So does its own
    // rotation falling due, for a credential shorter than the overlap.
    const overlap = new AbortController();
    await Promise.race([
      idle,
      sleep(ROTATION_OVERLAP_MS, lifetime.signal).catch(() => {}),
      whenAborted(old.signal),
      whenAborted(next.live.signal),
      rotationDue(next.live.rotateAt, new Promise<void>(() => {}), AbortSignal.any([lifetime.signal, overlap.signal])),
    ]);
    overlap.abort();
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
        const asked = new Promise<void>((resolve) => {
          wakeRotation = resolve;
        });
        if (rotateRequested) wakeRotation();
        const due = rotationDue(
          automatic ? opened.live.rotateAt : undefined,
          asked,
          AbortSignal.any([lifetime.signal, scope.signal]),
        ).then(() => undefined);
        const outcome = await Promise.race([subscriber.closed, due]);
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
        const next = await rotateFrom(opened, isManual);
        if (next) return { next };
        if (opened.live.signal.aborted || stopped) return await subscriber.closed;
        if (!manual) automatic = false;
      }
    } finally {
      scope.abort();
      wakeRotation = () => {};
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
        // `close()` may have run while the helper's result was on its way here: it could not see this connection.
        if (stopped) {
          opened.live.subscriber.close();
          return;
        }
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
      const { parameters, generations = false, staleAfterMs, retry, until, drainMs = 0 } = wantOptions;
      const namespace = namespaceFields(wantOptions.namespace);
      if (generations) namespaceFields([...namespace, 'x']);
      const track = trackName(wantOptions.track);
      if (staleAfterMs !== undefined) assertTimeoutMs('staleAfterMs', staleAfterMs);
      const retryBackoff = backoffOptions('retry', retry, RESUBSCRIBE_BACKOFF);
      const predicate = untilPredicate(until);
      assertDrainMs(drainMs);
      if (stopped) throw new RelayError('CLOSED', 'Supervised subscriber is closed', 'connection');

      const source = JSON.stringify([namespace, generations]);
      const target = targetKey([], track, parameters);
      const existing = wants.get(key);
      if (existing && existing.source === source) {
        existing.handlers = safeHandlers;
        existing.retry = retryBackoff;
        existing.until = predicate;
        existing.drainMs = drainMs;
        const moved = existing.target !== target;
        if (!moved && existing.options.staleAfterMs === staleAfterMs) return;
        existing.options = { ...existing.options, track, parameters, staleAfterMs };
        existing.target = target;
        for (const changed of [...existing.changes]) changed();
        return;
      }
      if (existing) forget(key, existing);
      const want: Want = {
        options: { namespace, track, parameters, generations, staleAfterMs },
        source,
        target,
        handlers: safeHandlers,
        retry: retryBackoff,
        until: predicate,
        drainMs,
        changes: new Set(),
        controller: new AbortController(),
      };
      wants.set(key, want);
      void runWant(
        want,
        liveConnection,
        () => current,
        () => forget(key, want),
      );
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
  /**
   * The followed generation was withdrawn with none newer, or the want moved before its subscription was accepted;
   * resubscribe without delay.
   */
  | { kind: 'moved'; accepted: boolean }
  /** A failure no wait fixes; surface it and stop. */
  | { kind: 'fatal'; error: unknown; accepted?: boolean; reason?: RestartReason | undefined }
  /** Anything else; resubscribe after the backoff. */
  | { kind: 'retry'; accepted: boolean; progressed: boolean; reason?: RestartReason | undefined };

/** Supervise `want`; a failure its loop did not expect forgets it rather than leave it registered but dead. */
async function runWant(
  want: Want,
  liveConnection: (scope: AbortSignal) => Promise<Connection | undefined>,
  nextConnection: () => Connection | undefined,
  forget: () => void,
): Promise<void> {
  try {
    await superviseWant(want, liveConnection, nextConnection, forget);
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
  nextConnection: () => Connection | undefined,
  forget: () => void,
): Promise<void> {
  const scope = want.controller.signal;
  let failures = 0;
  // A re-want asks for something new: its first failure waits the shortest delay again.
  const changed = () => {
    failures = 0;
  };
  want.changes.add(changed);
  try {
    await keepSubscribed();
  } finally {
    want.changes.delete(changed);
  }
  async function keepSubscribed() {
    while (!scope.aborted) {
      if (want.deferred) {
        // A discovery refusal seen while backing off is reported as one seen during an attempt would be.
        forget();
        want.handlers.onError?.(want.deferred.error);
        return;
      }
      const started = await liveConnection(scope);
      // A want replaced while waiting must not open anything: its replacement may already hold an overlapping root.
      if (!started || scope.aborted) return;
      want.settledTarget = undefined;
      want.connection = started;
      let outcome = await attempt(want, started, nextConnection);
      // A rotation may have moved the attempt to another connection: what follows waits on the one it ended on.
      const connection = want.connection ?? started;
      const asked = want.settledTarget ?? want.target;
      if (scope.aborted) return;
      if (outcome.kind === 'cancelled') return;
      // A re-want that came after the attempt settled asks for another track or parameters: the old target's failure
      // is not the new one's, so try the new one at once. A subscription the failed move cancelled was a restart.
      if (outcome.kind === 'fatal' && want.target !== asked)
        outcome = {
          kind: 'retry',
          accepted: outcome.accepted ?? false,
          progressed: false,
          reason: outcome.reason ?? 'terminated',
        };
      if (outcome.kind === 'fatal') {
        forget();
        want.handlers.onError?.(outcome.error);
        return;
      }
      const reason: RestartReason | undefined =
        outcome.kind === 'connection' ? 'connection' : outcome.kind === 'moved' ? 'generation' : outcome.reason;
      if (outcome.accepted && reason) {
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
      if (outcome.kind !== 'retry' || want.target !== asked) continue;
      // Cached objects followed by a done are not progress: only a later group resets the backoff.
      if (outcome.progressed) failures = 0;
      await backoff(want, connection, backoffDelay(failures++, want.retry));
    }
  }
}

/**
 * Wait out a retry delay, cut short when a fresh attempt can do better at once: the connection ended, or (following
 * generations) a generation other than the last one tried became the newest.
 */
async function backoff(want: Want, connection: Connection, delayMs: number): Promise<void> {
  const wake = new AbortController();
  // A rotation replacing the connection wakes it too: the want's next attempt belongs on the new one.
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
  // A re-want asks for something the failed attempt did not: try it at once.
  const rewanted = () => wake.abort();
  want.changes.add(rewanted);
  try {
    changed();
    await sleep(delayMs, scope).catch(() => {});
  } finally {
    want.changes.delete(rewanted);
    unwatch?.();
    if (held) want.controller.signal.removeEventListener('abort', held.release);
    held?.release();
  }
}

/** One attempt for `want` on `connection`: its discovery, if it follows generations, and its subscriptions. */
async function attempt(
  want: Want,
  connection: Connection,
  nextConnection: () => Connection | undefined,
): Promise<AttemptOutcome> {
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
      const found = await watch.newest(AbortSignal.any([scope, connection.retiring]));
      if (found.kind === 'failed') return discoveryFailed(found.error, scope, cancelledOrConnection, false);
      // Rotated away before a generation was found: start over on the new connection at once.
      if (found.kind !== 'found' && connection.retiring.aborted && !scope.aborted)
        return { kind: 'moved', accepted: false };
      if (found.kind !== 'found') return cancelledOrConnection(false);
      generation = found.generation;
      namespace = [...options.namespace, generation];
    }
    want.namespace = namespace;
    want.generation = generation;
    // The want's own signal, not the connection's: a rotation moves the attempt to the next connection.
    return await follow(
      want,
      connection,
      namespace,
      generation,
      want.controller.signal,
      watch,
      cancelledOrConnection,
      nextConnection,
    );
  } finally {
    release();
  }
}

/** One subscription an attempt holds: the active one, a move's pending replacement, or an old one draining. */
interface Slot {
  /** The namespace it was made on: the generation's when following one. */
  namespace: string[];
  generation: string | undefined;
  track: string;
  /** Its namespace, track and parameters, compared with what the want asks for now. */
  key: string;
  /** The connection it was made on: a rotation's replacement is on the next one. */
  connection: Connection;
  subscription: RelaySubscription;
  accepted?: SubscribeOk;
  firstGroup?: number;
  largestGroup?: number;
  /** The latest object it delivered, by location: a rotation's replacement skips what is at or before it. */
  latest?: { groupId: number; objectId: number };
  /**
   * After a rotation's silent cutover, the latest object its predecessor delivered: an earlier object arriving late on
   * another stream was delivered already, and is skipped.
   */
  skipThrough?: { groupId: number; objectId: number } | undefined;
  /** Whether it delivered an object from a later group than its first. */
  progressed: boolean;
  /** While draining after a cutover: the largest group it had delivered, through which its events still reach the handlers. */
  drainThrough?: number;
  /** Why the active subscription ended while a move was pending; the move carries on without it. */
  ended?: RestartReason;
  /** While pending: its events held until the cutover, as a switchable holds them. */
  held: HeldEvent[];
}

/** Whether `object` is at or before `latest`, by group and then object ID. */
function atOrBefore(object: MoqtObject, latest: { groupId: number; objectId: number } | undefined): boolean {
  if (latest === undefined) return false;
  return object.groupId < latest.groupId || (object.groupId === latest.groupId && object.objectId <= latest.objectId);
}

type Location = { groupId: number; objectId: number };

/** The later of two locations, either of which may be unknown. */
function later(a: Location | undefined, b: Location | undefined): Location | undefined {
  if (a === undefined) return b;
  if (b === undefined) return a;
  return a.groupId > b.groupId || (a.groupId === b.groupId && a.objectId >= b.objectId) ? a : b;
}

/** Record `object` as the latest `slot` delivered, if it is later than what it had. */
function delivered(slot: Slot, object: MoqtObject): void {
  if (!atOrBefore(object, slot.latest)) slot.latest = { groupId: object.groupId, objectId: object.objectId };
}

/** What a subscription is made with, as a comparable key. */
function targetKey(namespace: readonly string[], track: string, parameters: MessageParameters | undefined): string {
  return JSON.stringify([namespace, track, parameters ?? null], (_, value) =>
    typeof value === 'bigint' ? `${value}n` : value,
  );
}

/**
 * The want's subscriptions from the first SUBSCRIBE on `connection` to whatever ends the attempt. A move (a newer
 * generation, or a re-want with another track or parameters) is made before the break: the replacement opens pending
 * while the active subscription keeps delivering, and takes over once one of its objects passes `until`. A rotation is
 * a move to the next connection: with the target unchanged, the replacement takes over at the first object the active
 * one had not delivered, and no callback marks it.
 */
async function follow(
  want: Want,
  connection: Connection,
  namespace: string[],
  generation: string | undefined,
  scope: AbortSignal,
  initialWatch: RootWatch | undefined,
  cancelledOrConnection: (accepted: boolean) => AttemptOutcome,
  nextConnection: () => Connection | undefined,
): Promise<AttemptOutcome> {
  /** The followed root's discovery on the active subscription's connection. */
  let watch = initialWatch;
  const handlers = () => want.handlers;
  const settled = new AbortController();
  let outcome: AttemptOutcome | undefined;
  const settle = (next: AttemptOutcome) => {
    if (outcome) return;
    // A failure no wait fixes still ends an accepted subscription, which a superseding re-want reports as a restart.
    outcome = next.kind === 'fatal' ? { ...next, accepted: accepted(), reason: active.ended } : next;
    want.settledTarget = want.target;
    settled.abort();
  };
  const over = () => outcome !== undefined || scope.aborted;
  let active!: Slot;
  let pending: Slot | undefined;
  /** Old subscriptions draining after a cutover, each with the timer that cancels it. */
  const draining = new Map<Slot, ReturnType<typeof setTimeout>>();
  /** A move whose replacement failed, retried after the backoff unless the want asks for something else first. */
  let retryMove:
    | { key: string; connection: Connection; timer: ReturnType<typeof setTimeout>; unobserve(): void }
    | undefined;
  let moveFailures = 0;
  /** The target of the latest move, whose failures `moveFailures` counts. */
  let moveTarget: string | undefined;
  let lastObjectAt = 0;
  const accepted = () => active.accepted !== undefined;
  /** A rotation under way: the connection the subscriptions move to, and its discovery of the root. */
  let rotationTo: { connection: Connection; watch: RootWatch | undefined; release(): void } | undefined;
  /** The discovery a rotation swapped in, released when the attempt ends; the first is the attempt's to release. */
  let releaseWatch = () => {};
  /** The connections a subscription of this attempt is on, each held so a rotation closes it only once none are. */
  const holds = new Map<Connection, () => void>();
  /** How to stop observing each connection the attempt observes; dropped with its hold. */
  const observed = new Map<Connection, () => void>();
  const observe = (on: Connection) => {
    if (observed.has(on)) return;
    // Runs inside the supervisor's abort of the retiring signal: an unexpected failure restarts this want alone.
    const onRetiring = () => {
      try {
        rotate(on);
      } catch (error) {
        // biome-ignore lint/suspicious/noConsole: the want's own failure, with no caller to report it to
        console.error(
          new Error('A want failed to start its rotation; it restarts on the new connection', { cause: error }),
        );
        settle(cancelledOrConnection(accepted()));
      }
    };
    on.retiring.addEventListener('abort', onRetiring, { once: true });
    observed.set(on, () => on.retiring.removeEventListener('abort', onRetiring));
    if (on.retiring.aborted) queueMicrotask(onRetiring);
  };

  const holdOn = (on: Connection) => {
    if (holds.has(on)) return;
    holds.set(on, hold(on));
    observe(on);
  };
  const releaseUnused = () => {
    for (const [on, release] of [...holds]) {
      const used =
        active.connection === on ||
        pending?.connection === on ||
        rotationTo?.connection === on ||
        [...draining.keys()].some((slot) => slot.connection === on);
      if (used) continue;
      holds.delete(on);
      release();
      observed.get(on)?.();
      observed.delete(on);
    }
  };
  /**
   * The targets a pending replacement heard PUBLISH_DONE on, until the next cutover: a restarted publisher numbers its
   * groups from 0, so a rotation's replacement for that target no longer skips what the active subscription delivered.
   * Another target's publisher ending says nothing about the active one's.
   */
  const endedTargets = new Set<string>();
  /** Whether `slot` replaces the active subscription on the next connection, with the same target. */
  const rotating = (slot: Slot) => slot.connection !== active.connection && slot.key === active.key;

  const clearRetry = () => {
    clearTimeout(retryMove?.timer);
    retryMove?.unobserve();
    retryMove = undefined;
  };
  const stopDrain = (slot: Slot) => {
    clearTimeout(draining.get(slot));
    draining.delete(slot);
    slot.subscription.cancel();
    releaseUnused();
  };
  /** Whether a subgroup event of `slot` reaches the handlers. */
  const reaches = (slot: Slot | undefined, groupId: number) =>
    slot !== undefined &&
    !over() &&
    ((slot === active && !slot.ended) || (draining.has(slot) && groupId <= (slot.drainThrough as number)));

  /** The active subscription ended without the connection: with a move pending, the move carries on. */
  const activeEnded = (reason: RestartReason) => {
    if (pending) {
      active.ended = reason;
      active.subscription.cancel();
      return;
    }
    settle({ kind: 'retry', accepted: accepted(), progressed: active.progressed, reason });
  };

  /** A move's replacement ended before its cutover. */
  const pendingFailed = (slot: Slot, end: SubscriptionEnd | undefined) => {
    pending = undefined;
    slot.subscription.cancel();
    // The connection's end reaches the active subscription and the attempt's scope as well.
    if (end?.cause === 'connection-closed') {
      settle(cancelledOrConnection(accepted()));
      return;
    }
    const failed = end ? classify(end, false, false, cancelledOrConnection) : undefined;
    if (failed?.kind === 'fatal') {
      settle(failed);
      return;
    }
    if (active.ended) {
      settle({ kind: 'retry', accepted: true, progressed: active.progressed, reason: active.ended });
      return;
    }
    // The active subscription keeps delivering; the move is retried after the backoff.
    const key = slot.key;
    const on = slot.connection;
    // The connection the move is to be retried on closing leaves nothing to retry on: restart now, not after the delay.
    const closed = () => {
      if (retryMove?.connection !== on || over()) return;
      clearRetry();
      settle(cancelledOrConnection(accepted()));
    };
    on.signal.addEventListener('abort', closed, { once: true });
    retryMove = {
      key,
      connection: on,
      timer: setTimeout(
        () => {
          if (retryMove?.key !== key) return;
          clearRetry();
          reconcile();
        },
        backoffDelay(moveFailures++, want.retry),
      ),
      unobserve: () => on.signal.removeEventListener('abort', closed),
    };
    if (on.signal.aborted) closed();
  };

  const cutover = (slot: Slot, object: MoqtObject) => {
    const from = active;
    // A rotation that keeps the target continues the same stream: nothing to mark, and no drain to repeat objects.
    const silent = rotating(slot);
    // Same stream, same numbering: what the old subscription delivered stays delivered, even arriving late here.
    if (silent && !endedTargets.has(slot.key) && from.ended !== 'publisher-done') {
      slot.skipThrough = later(from.latest, from.skipThrough);
    }
    endedTargets.clear();
    pending = undefined;
    clearRetry();
    moveFailures = 0;
    active = slot;
    want.namespace = slot.namespace;
    want.generation = slot.generation;
    want.connection = slot.connection;
    if (rotationTo && rotationTo.connection === slot.connection) {
      // The followed root's discovery moves to the new connection with the subscription.
      unwatch?.();
      unwatchNext?.();
      unwatchNext = undefined;
      watch = rotationTo.watch;
      releaseWatch = rotationTo.release;
      rotationTo = undefined;
      unwatch = watch?.onChange(reconcile);
    }
    if (!silent && !from.ended && want.drainMs > 0 && from.largestGroup !== undefined) {
      from.drainThrough = from.largestGroup;
      draining.set(
        from,
        setTimeout(() => stopDrain(from), want.drainMs),
      );
    } else from.subscription.cancel();
    releaseUnused();
    slot.firstGroup = object.groupId;
    slot.largestGroup = object.groupId;
    lastObjectAt = performance.now();
    if (!silent) {
      try {
        handlers().onSwitched?.({
          from: { namespace: [...from.namespace], track: from.track },
          to: { namespace: [...slot.namespace], track: slot.track },
          // A subscription that delivered an object was accepted.
          subscribed: slot.accepted as SubscribeOk,
        });
      } catch (error) {
        // A delivery callback's failure is the application's; the want keeps delivering.
        // biome-ignore lint/suspicious/noConsole: the application's callback failed and has no caller to report to
        console.error(new Error('onSwitched threw; the want delivers regardless', { cause: error }));
      }
    }
    // Later groups that reached the replacement early, out of order, follow the object that cut over. Each delivery
    // is isolated, so one throwing callback loses neither the rest nor the held events.
    const replay = heldAfter(slot.held, object);
    slot.held = [];
    const deliver = (call: () => void) => {
      try {
        call();
      } catch (error) {
        // biome-ignore lint/suspicious/noConsole: the application's callback failed and has no caller to report to
        console.error(new Error('A want handler threw at a cutover; the want delivers regardless', { cause: error }));
      }
    };
    // A handler may have moved the want again or cancelled it; the object still belongs to this cutover.
    delivered(slot, object);
    if (!over() && active === slot) deliver(() => handlers().onObject?.(object));
    for (const event of replay) {
      if (over() || active !== slot) break;
      if (event.type === 'object') {
        delivered(slot, event.value);
        if (event.value.groupId > (slot.firstGroup as number)) slot.progressed = true;
        if (event.value.groupId > (slot.largestGroup as number)) slot.largestGroup = event.value.groupId;
        deliver(() => handlers().onObject?.(event.value));
      } else if (event.type === 'subgroupEnd') deliver(() => handlers().onSubgroupEnd?.(event.value));
      else deliver(() => handlers().onSubgroupReset?.(event.value));
    }
  };

  const onObject = (slot: Slot | undefined, object: MoqtObject) => {
    if (!slot || over()) return;
    if (slot === active) {
      if (slot.ended) return;
      if (atOrBefore(object, slot.skipThrough)) return;
      lastObjectAt = performance.now();
      if (slot.firstGroup === undefined) slot.firstGroup = object.groupId;
      else if (object.groupId > slot.firstGroup) slot.progressed = true;
      if (slot.largestGroup === undefined || object.groupId > slot.largestGroup) slot.largestGroup = object.groupId;
      delivered(slot, object);
      handlers().onObject?.(object);
      return;
    }
    if (draining.has(slot)) {
      if (object.groupId <= (slot.drainThrough as number)) handlers().onObject?.(object);
      return;
    }
    if (slot !== pending) return;
    lastObjectAt = performance.now();
    // The active subscription's ending reaches `ended` a turn later; one already recorded counts now.
    const activeEnd = (active.subscription as Partial<ProductSubscription>).end;
    if (!active.ended && activeEnd) {
      onEnded(active, activeEnd);
      if (over() || pending !== slot) return;
    }
    // A rotation's replacement starts at the latest group, which the active subscription may have delivered: skip it.
    // Not once the publisher ended or the subscription was ended by it: a restarted publisher numbers from 0 again.
    const rotation = rotating(slot);
    // Only PUBLISH_DONE says the publisher may number from 0 again; a stale, terminated or disconnected subscription
    // does not.
    const continues = !endedTargets.has(slot.key) && active.ended !== 'publisher-done';
    if (rotation && continues && atOrBefore(object, active.latest)) return;
    // With the active subscription gone there is nothing left to compare with: the next object cuts over. A rotation's
    // replacement continues the same stream, so its first new object does too.
    let passed = true;
    if (!active.ended && !rotation) {
      try {
        passed = want.until(object);
      } catch (error) {
        // biome-ignore lint/suspicious/noConsole: the application's predicate failed and has no caller to report to
        console.error(new Error('until threw; the want cuts over on this object', { cause: error }));
      }
    }
    // The predicate is application code: it may have moved the want again or cancelled it.
    if (!passed) holdEvent(slot.held, { type: 'object', value: object });
    else if (!over() && pending === slot) cutover(slot, object);
  };

  /** One subscription's ending; safe to run again for the same slot. */
  const onEnded = (slot: Slot, end: SubscriptionEnd) => {
    if (over()) return;
    if (draining.has(slot)) stopDrain(slot);
    else if (slot === pending) pendingFailed(slot, end);
    else if (slot === active && !slot.ended) {
      // The retiring connection closed under the active subscription: the rotation's replacement carries on, or is
      // tried again on the next connection after its backoff.
      if (end.cause === 'connection-closed' && pending && pending.connection !== slot.connection) {
        activeEnded('connection');
        return;
      }
      const elsewhere =
        (retryMove && retryMove.connection !== slot.connection) ||
        (rotationTo && rotationTo.connection !== slot.connection);
      if (end.cause === 'connection-closed' && !pending && elsewhere) {
        slot.ended = 'connection';
        slot.subscription.cancel();
        return;
      }
      const next = classify(end, accepted(), slot.progressed, cancelledOrConnection);
      if (next.kind === 'retry' && next.reason) activeEnded(next.reason);
      else settle(next);
    }
  };

  const open = (target: string[], targetGeneration: string | undefined, on: Connection): Slot => {
    const { track, parameters } = want.options;
    let slot: Slot | undefined;
    const subscription = on.subscriber.subscribe(
      { namespace: target, track, ...(parameters ? { parameters } : {}) },
      {
        onSubscribed: (info) => {
          if (!slot || over()) return;
          slot.accepted = info;
          if (slot !== active) return;
          lastObjectAt = performance.now();
          handlers().onSubscribed?.(info, { namespace: [...slot.namespace] });
        },
        onObject: (object) => onObject(slot, object),
        onSubgroupEnd: (info) => {
          if (slot && slot === pending && !over()) holdEvent(slot.held, { type: 'subgroupEnd', value: info });
          else if (reaches(slot, info.groupId)) handlers().onSubgroupEnd?.(info);
        },
        onSubgroupReset: (info) => {
          if (slot && slot === pending && !over()) holdEvent(slot.held, { type: 'subgroupReset', value: info });
          else if (reaches(slot, info.groupId)) handlers().onSubgroupReset?.(info);
        },
        onPublisherDone: (info) => {
          if (!slot || over()) return;
          // Heard on the replacement: the publisher behind both ended. An old subscription a move left behind hearing
          // its own publisher end says nothing about the current one.
          if (slot === pending) endedTargets.add(slot.key);
          if (slot === pending) pendingFailed(slot, undefined);
          else if (slot === active && !slot.ended) {
            activeEnded('publisher-done');
            handlers().onPublisherDone?.(info);
          }
        },
      },
    );
    const opened: Slot = {
      namespace: target,
      generation: targetGeneration,
      track,
      key: targetKey(target, track, parameters),
      connection: on,
      subscription,
      progressed: false,
      held: [],
    };
    slot = opened;
    holdOn(on);
    void subscription.subscribed.catch(() => {});
    void subscription.ended.then((end) => onEnded(opened, end));
    return opened;
  };

  /** Bring the subscriptions in line with what the want asks for now: its options and the newest generation. */
  const reconcile = () => {
    if (over()) return;
    // During a rotation the subscription belongs on the next connection, following that connection's discovery.
    const on = rotationTo?.connection ?? active.connection;
    const discovery = rotationTo ? rotationTo.watch : watch;
    let target = want.options.namespace;
    let targetGeneration: string | undefined;
    if (discovery) {
      const failure = discovery.failure();
      if (failure) {
        settle(discoveryFailed(failure.error, scope, cancelledOrConnection, accepted()));
        return;
      }
      targetGeneration = discovery.current();
      // A rotation's discovery on the next connection has found nothing yet: wait for it, the old one still delivers.
      if (targetGeneration === undefined && rotationTo) return;
      // None announced: nothing to move to, so the want waits for one, as after a withdrawal.
      if (targetGeneration === undefined) {
        settle({ kind: 'moved', accepted: accepted() });
        return;
      }
      target = [...target, targetGeneration];
    }
    const key = targetKey(target, want.options.track, want.options.parameters);
    if (key === active.key && on === active.connection) {
      // Asked for what is active again: a pending move is no longer wanted.
      clearRetry();
      moveTarget = undefined;
      if (pending) {
        pending.subscription.cancel();
        pending = undefined;
      }
      if (active.ended) settle({ kind: 'retry', accepted: true, progressed: active.progressed, reason: active.ended });
      return;
    }
    if ((pending?.key === key && pending.connection === on) || (retryMove?.key === key && retryMove.connection === on))
      return;
    clearRetry();
    // An unanswered subscription delivers nothing to overlap: replace it at once.
    if (!accepted()) {
      settle({ kind: 'moved', accepted: false });
      return;
    }
    if (pending) {
      pending.subscription.cancel();
      pending = undefined;
    }
    // A new target starts its own backoff; a retry of the same one keeps counting.
    if (moveTarget !== key) moveFailures = 0;
    moveTarget = key;
    try {
      pending = open(target, targetGeneration, on);
    } catch (error) {
      settle(scope.aborted || !refusedForGood(error) ? cancelledOrConnection(true) : { kind: 'fatal', error });
    }
  };

  try {
    active = open(namespace, generation, connection);
  } catch (error) {
    want.settledTarget = want.target;
    // An argument the relay can never take is the want's fault; anything else is the connection ending under the call.
    if (scope.aborted || !refusedForGood(error)) return cancelledOrConnection(false);
    return { kind: 'fatal', error };
  }

  /** A rotation replaced the active subscription's connection: move the subscription to the next one. */
  const rotate = (from: Connection) => {
    if (over()) return;
    // The active subscription's connection, or the one a rotation is already moving to, if that one retires in turn.
    if (rotationTo ? rotationTo.connection !== from : active.connection !== from) return;
    const next = nextConnection();
    if (!next || next === from || next.signal.aborted) return;
    // Nothing accepted yet to overlap: start over on the new connection at once.
    if (!accepted()) {
      settle({ kind: 'moved', accepted: false });
      return;
    }
    holdOn(next);
    let nextWatch: RootWatch | undefined;
    let release = () => {};
    if (watch) {
      try {
        ({ watch: nextWatch, release } = rootWatch(next, want.options.namespace));
      } catch {
        // Discovery the new connection refuses cannot follow there: restart on it as after a connection's end.
        releaseUnused();
        settle(cancelledOrConnection(accepted()));
        return;
      }
    }
    // Recorded at once, before the new connection's discovery has found the generation: reconcile waits for it, and
    // the rotation is in flight meanwhile, so the old connection closing or this one retiring in turn keeps continuity.
    // A rotation to a connection that retired in turn gives way to this one.
    unwatchNext?.();
    rotationTo?.release();
    rotationTo = { connection: next, watch: nextWatch, release };
    unwatchNext = nextWatch?.onChange(reconcile);
    reconcile();
    releaseUnused();
  };
  let unwatchNext: (() => void) | undefined;
  let stale: ReturnType<typeof setInterval> | undefined;
  const watchStale = () => {
    clearInterval(stale);
    const staleAfterMs = want.options.staleAfterMs;
    if (staleAfterMs === undefined) return;
    stale = setInterval(
      () => {
        if (over() || !accepted() || performance.now() - lastObjectAt < staleAfterMs) return;
        // During a move only the old subscription went stale: the replacement gets a window of its own.
        if (pending && !active.ended) {
          activeEnded('stale');
          lastObjectAt = performance.now();
        } else settle({ kind: 'retry', accepted: true, progressed: active.progressed, reason: 'stale' });
      },
      Math.max(10, Math.min(1_000, Math.floor(staleAfterMs / 4))),
    );
  };
  const changed = () => {
    watchStale();
    reconcile();
  };
  watchStale();
  want.changes.add(changed);
  let unwatch = watch?.onChange(reconcile);
  // A change that landed between choosing the subscription and attaching the listeners is not lost.
  reconcile();
  try {
    await whenAborted(AbortSignal.any([scope, settled.signal]));
    return outcome ?? cancelledOrConnection(accepted());
  } finally {
    // Settles the attempt, so nothing reaches the handlers from here on.
    settle({ kind: 'cancelled' });
    clearInterval(stale);
    clearRetry();
    unwatch?.();
    unwatchNext?.();
    for (const stop of observed.values()) stop();
    observed.clear();
    want.changes.delete(changed);
    for (const slot of [...draining.keys()]) stopDrain(slot);
    pending?.subscription.cancel();
    active.subscription.cancel();
    rotationTo?.release();
    rotationTo = undefined;
    releaseWatch();
    for (const release of holds.values()) release();
    holds.clear();
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
