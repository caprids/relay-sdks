/**
 * The switchable subscription: one subscription at a time delivered to one set of handlers, moved to
 * another track (or the same track with other parameters) make-before-break (issue #264).
 *
 * Draft-22 REQUEST_UPDATE names no track, and moq-relay ends a subscription on any update, so changing
 * what a viewer receives means a new SUBSCRIBE. `switchTo` opens it while the active subscription keeps
 * delivering, holds it pending until one of its objects passes `until`, then cancels the old one and
 * hands the handlers the new one. Each subscription underneath is an ordinary product subscription
 * (`subscription.ts`); this module only routes their events and owns the switchable's own ending.
 */
import type { Goaway, MessageParameters, MoqtObject, PublishDone, RequestError, SubscribeOk } from '@mux/moq-core';
import { RelayError } from './errors.js';
import {
  endError,
  type ProductSubscription,
  type SubscriptionConsumer,
  type SubscriptionEnd,
  synchronousCallback,
} from './subscription.js';
import { trackName } from './validation.js';

/**
 * Callbacks for a switchable subscription. Each runs synchronously, for the active subscription's
 * events only, and receives the active track's name as its last argument. A pending subscription
 * reaches no handler before its cutover.
 */
export interface SwitchableHandlers {
  /**
   * The first subscription accepted (SUBSCRIBE_OK). A later subscription's acceptance arrives with
   * its cutover, in `onSwitched`.
   */
  onSubscribed?(info: SubscribeOk, track: string): void;
  /**
   * The active subscription ended with REQUEST_ERROR: the first one was rejected, or an accepted one
   * was refused later. The switchable has ended. A pending subscription's rejection rejects its
   * `switchTo` instead.
   */
  onError?(error: RequestError, track: string): void;
  /** One object from the active subscription. Payloads are uninterpreted bytes. */
  onObject?(object: MoqtObject, track: string): void;
  /** One of the active subscription's subgroup streams finished cleanly (FIN). */
  onSubgroupEnd?(info: { groupId: number; subgroupId: number; endOfGroup: boolean }, track: string): void;
  /** One of the active subscription's subgroup streams was reset; objects from it may be missing. */
  onSubgroupReset?(info: { groupId: number; error: unknown }, track: string): void;
  /**
   * The active track's publisher finished with it (PUBLISH_DONE). Not an ending, as on
   * `RelaySubscriptionHandlers.onPublisherDone`; a pending switch carries on.
   */
  onPublisherDone?(info: PublishDone, track: string): void;
  /** The publisher changed the active subscription's state (PUBLISH_STATE_NOTIFY). */
  onStateNotify?(parameters: MessageParameters, track: string): void;
  /** GOAWAY on the active subscription's request stream. Informational; nothing is retried. */
  onGoaway?(goaway: Goaway, track: string): void;
  /**
   * A cutover: `to` is now active and `from` has been cancelled. Runs before the first object of
   * `to`, which is the one that passed `until`. Decoders drop state that assumed continuity here.
   */
  onSwitched?(info: SwitchedInfo): void;
}

/** What `onSwitched` receives and `switchTo` resolves with. */
export interface SwitchedInfo {
  /** The track that was active before the cutover. */
  from: string;
  /** The track now active; the same as `from` when only the parameters changed. */
  to: string;
  /** The new subscription's acceptance info. */
  subscribed: SubscribeOk;
}

/** How `switchTo` opens and promotes the new subscription. */
export interface SwitchOptions {
  /**
   * The new subscription's parameters (location filter, priority, delivery timeout). Not inherited
   * from the subscription being replaced: omitted means none.
   */
  parameters?: MessageParameters;
  /**
   * When to cut over; the new subscription's objects before the cutover are discarded.
   * `'first-object'` (the default) promotes on its first object. A synchronous predicate promotes
   * on the first object it accepts: compare the object's own timeline (a timestamp property, say)
   * with what the active subscription has delivered, and accept once the new track has caught up,
   * so the cutover repeats media rather than skipping it. Group IDs are no such timeline: each
   * track numbers its own groups, and the SDK's publisher counts them from the track's first sent
   * group. Against the Mux relay a subscription without a location filter starts at the beginning
   * of the latest group, so its first object begins a group; a subscriber on another relay can
   * lose that first object (issue #243), which a timeline predicate absorbs.
   */
  until?: 'first-object' | ((object: MoqtObject) => boolean);
  /**
   * How long the old subscription stays open after the cutover, in milliseconds; default 0, which
   * cancels it at once. Groups arrive out of order across streams, most of all through another
   * relay, so at the cutover an older group of the old track can still be in flight, or the rest of
   * one it began. While it drains, its objects, subgroup ends and resets from groups no later than
   * the largest it had delivered still reach the handlers, with the old track's name, after
   * `onSwitched`; its later groups and everything else are discarded.
   */
  drainMs?: number;
  /** Abandons this switch while it is pending: the new subscription is cancelled, the active one stays. */
  signal?: AbortSignal;
}

/** The handle `RelaySubscriber.subscribeSwitchable()` returns. */
export interface SwitchableSubscription {
  /** The active subscription's track name. */
  readonly track: string;
  /** The pending subscription's track name, while a switch is pending. */
  readonly pending: string | undefined;
  /**
   * Resolves with the acceptance info of the first subscription to be active and accepted: the first
   * one, or the one a switch made before the first one was answered. Rejects as
   * `RelaySubscription.subscribed` does if the switchable ends before that.
   */
  readonly subscribed: Promise<SubscribeOk>;
  /**
   * Settles once, with the active subscription's ending, when it ends other than by a cutover, or
   * when `cancel()` runs. Any pending switch is cancelled with it. Never rejects.
   */
  readonly ended: Promise<SubscriptionEnd>;
  /**
   * Subscribe to `track` in the same namespace, keep delivering the active subscription until the new
   * one's object passes `until`, then cancel the old one. The track may be the active one, to change
   * parameters. A newer `switchTo` cancels a pending one, which rejects with `CLOSED`. Rejects as
   * `RelaySubscription.subscribed` does if the new subscription ends before its cutover, with the
   * signal's reason when the signal aborts it, or with the thrown error when `until` throws; the
   * active subscription carries on. Throws at once for an invalid track or options, a closed
   * connection, or an ended switchable.
   */
  switchTo(track: string, options?: SwitchOptions): Promise<SwitchedInfo>;
  /** Cancel the active subscription and any pending one. Idempotent. */
  cancel(reason?: unknown): void;
}

/** What the switchable borrows from the connection. */
export interface SwitchableDependencies {
  /**
   * Open one product subscription to `track` in the switchable's namespace with `consumer` attached.
   * Checks its arguments and the connection first, and throws for either.
   */
  subscribe(
    track: string,
    parameters: MessageParameters | undefined,
    consumer: SubscriptionConsumer,
  ): ProductSubscription;
}

type Handler = (...args: unknown[]) => void;

const HANDLER_NAMES = [
  'onSubscribed',
  'onError',
  'onObject',
  'onSubgroupEnd',
  'onSubgroupReset',
  'onPublisherDone',
  'onStateNotify',
  'onGoaway',
  'onSwitched',
] as const satisfies readonly (keyof SwitchableHandlers)[];

/** The handler each product event reaches; `updateOk` has none, since a switchable has no `update()`. */
const EVENT_HANDLERS = {
  subscribed: 'onSubscribed',
  object: 'onObject',
  publisherDone: 'onPublisherDone',
  subgroupEnd: 'onSubgroupEnd',
  subgroupReset: 'onSubgroupReset',
  stateNotify: 'onStateNotify',
  goaway: 'onGoaway',
  updateOk: undefined,
} as const;

/** One subscription the switchable holds, active or pending. */
interface Slot {
  track: string;
  subscription: ProductSubscription;
  /** The acceptance info, once it arrived. */
  accepted?: SubscribeOk;
  /** While draining after a cutover: the largest group it had delivered, through which its events still reach the handlers. */
  drainThrough?: number;
}

/** A pending switch: its slot plus the promise `switchTo` returned. */
interface PendingSwitch extends Slot {
  /** Whether this object may be the cutover. */
  until: (object: MoqtObject) => boolean;
  drainMs: number;
  resolve(info: SwitchedInfo): void;
  reject(error: unknown): void;
  removeAbort(): void;
}

function untilPredicate(until: SwitchOptions['until']): (object: MoqtObject) => boolean {
  if (until === undefined || until === 'first-object') return () => true;
  if (typeof until !== 'function') throw new TypeError("until must be 'first-object' or a function");
  if (Object.prototype.toString.call(until) === '[object AsyncFunction]')
    throw new TypeError('until must be synchronous');
  return (object) => {
    const passed: unknown = until(object);
    if (typeof passed !== 'boolean') {
      // A promise is never awaited; observe its rejection so refusing it cannot surface as unhandled.
      if (
        passed &&
        (typeof passed === 'object' || typeof passed === 'function') &&
        'then' in passed &&
        typeof passed.then === 'function'
      )
        void Promise.resolve(passed).catch(() => {});
      throw new TypeError('until must return a boolean');
    }
    return passed;
  };
}

/** Handlers checked and snapshotted, held to the synchronous callback policy. */
export type SwitchableHandlerSnapshot = ReadonlyMap<keyof SwitchableHandlers, Handler>;

/** Check and snapshot the handlers, before connection state is read. */
export function snapshotSwitchableHandlers(handlers: SwitchableHandlers = {}): SwitchableHandlerSnapshot {
  if (typeof handlers !== 'object' || handlers === null) throw new TypeError('handlers must be an object');
  const snapshot = new Map<keyof SwitchableHandlers, Handler>();
  for (const name of HANDLER_NAMES) {
    const callback = handlers[name];
    if (callback === undefined) continue;
    if (typeof callback !== 'function') throw new TypeError(`${name} must be a function`);
    snapshot.set(name, synchronousCallback(name, callback, handlers));
  }
  return snapshot;
}

export function createSwitchable(
  deps: SwitchableDependencies,
  initialTrack: string,
  parameters: MessageParameters | undefined,
  snapshot: SwitchableHandlerSnapshot,
): SwitchableSubscription {
  const call = (name: keyof SwitchableHandlers, ...args: unknown[]) => snapshot.get(name)?.(...args);

  let active!: Slot;
  /** The largest group ID among the objects the active subscription delivered. */
  let activeLargestGroup: number | undefined;
  let pending: PendingSwitch | undefined;
  /** Old subscriptions draining after a cutover, each with the timer that cancels it. */
  const draining = new Map<Slot, ReturnType<typeof setTimeout>>();
  let end: SubscriptionEnd | undefined;
  let hasSubscribed = false;
  let resolveSubscribed!: (info: SubscribeOk) => void;
  let rejectSubscribed!: (error: unknown) => void;
  let resolveEnded!: (end: SubscriptionEnd) => void;
  const subscribed = new Promise<SubscribeOk>((resolve, reject) => {
    resolveSubscribed = resolve;
    rejectSubscribed = reject;
  });
  void subscribed.catch(() => {});
  const ended = new Promise<SubscriptionEnd>((resolve) => {
    resolveEnded = resolve;
  });

  const markSubscribed = (info: SubscribeOk) => {
    if (hasSubscribed) return;
    hasSubscribed = true;
    resolveSubscribed(info);
  };

  const stopDrain = (slot: Slot) => {
    clearTimeout(draining.get(slot));
    draining.delete(slot);
    slot.subscription.cancel();
  };

  /** Settle a pending switch that did not cut over, cancelling its subscription if it still runs. */
  const abandon = (switching: PendingSwitch, error: unknown) => {
    if (pending === switching) pending = undefined;
    switching.removeAbort();
    switching.reject(error);
    switching.subscription.cancel(error);
  };

  const finish = (next: SubscriptionEnd) => {
    if (end) return;
    end = next;
    if (!hasSubscribed) rejectSubscribed(endError(next));
    // A closed connection ends both subscriptions; report it the same way on the pending switch.
    if (pending)
      abandon(
        pending,
        next.cause === 'connection-closed'
          ? endError(next)
          : new RelayError('CLOSED', 'The switchable subscription ended before the switch', 'subscription'),
      );
    for (const slot of [...draining.keys()]) stopDrain(slot);
    resolveEnded(next);
    if (next.cause === 'request-error') call('onError', next.error, active.track);
  };

  const cutover = (switching: PendingSwitch, object: MoqtObject) => {
    const from = active;
    pending = undefined;
    switching.removeAbort();
    active = switching;
    // The old subscription's consumer checks `active` first, so its events from here on reach the
    // handlers only while it drains, and its ending reaches nobody.
    if (switching.drainMs > 0 && activeLargestGroup !== undefined) {
      from.drainThrough = activeLargestGroup;
      draining.set(
        from,
        setTimeout(() => stopDrain(from), switching.drainMs),
      );
    } else from.subscription.cancel();
    activeLargestGroup = object.groupId;
    // A subscription that delivered an object was accepted.
    const info = switching.accepted as SubscribeOk;
    markSubscribed(info);
    const switched = { from: from.track, to: switching.track, subscribed: info };
    switching.resolve(switched);
    call('onSwitched', switched);
    // A handler may have switched again or cancelled; the object still belongs to this cutover.
    if (!end && active === switching) call('onObject', object, switching.track);
  };

  /** The consumer for one slot: active events go to the handlers, pending ones to the cutover check. */
  const consumerFor = (slot: () => Slot): SubscriptionConsumer => ({
    event(event) {
      const self = slot();
      // Nothing is delivered while the subscription is being opened, but stay safe if it were.
      if (end || !self) return;
      if (event.type === 'subscribed') self.accepted = event.value;
      if (self === active) {
        if (event.type === 'subscribed') {
          if (hasSubscribed) return;
          markSubscribed(event.value);
        }
        if (event.type === 'object' && (activeLargestGroup === undefined || event.value.groupId > activeLargestGroup))
          activeLargestGroup = event.value.groupId;
        const name = EVENT_HANDLERS[event.type];
        if (name) call(name, event.value, self.track);
        return;
      }
      if (draining.has(self)) {
        if (
          (event.type === 'object' || event.type === 'subgroupEnd' || event.type === 'subgroupReset') &&
          event.value.groupId <= (self.drainThrough as number)
        )
          call(EVENT_HANDLERS[event.type], event.value, self.track);
        return;
      }
      if (self !== pending || event.type !== 'object') return;
      const switching = pending;
      let passed: boolean;
      try {
        passed = switching.until(event.value);
      } catch (error) {
        if (!end && pending === switching) abandon(switching, error);
        return;
      }
      // The predicate is application code: it may have switched again or cancelled.
      if (passed && !end && pending === switching) cutover(switching, event.value);
    },
    ended(subscriptionEnd) {
      const self = slot();
      // An ending recorded while the subscription was being opened is read back by the opener.
      if (!self) return;
      if (self === active) finish(subscriptionEnd);
      else if (draining.has(self)) stopDrain(self);
      else if (self === pending) abandon(pending, endError(subscriptionEnd));
    },
  });

  const open = (track: string, params: MessageParameters | undefined, slot: () => Slot) =>
    deps.subscribe(track, params, consumerFor(slot));

  {
    let initial: Slot | undefined;
    const subscription = open(initialTrack, parameters, () => initial as Slot);
    initial = { track: initialTrack, subscription };
    active = initial;
    // An ending recorded while the subscription was being opened found no slot yet.
    if (subscription.end) finish(subscription.end);
  }

  return {
    get track() {
      return active.track;
    },
    get pending() {
      return pending?.track;
    },
    subscribed,
    ended,
    switchTo(track, options = {}) {
      if (typeof options !== 'object' || options === null) throw new TypeError('switchTo options must be an object');
      const { parameters: params, until, signal, drainMs = 0 } = options;
      // Arguments before the switchable's or the connection's state (spec/behavior.md, Arguments).
      trackName(track);
      const predicate = untilPredicate(until);
      if (typeof drainMs !== 'number' || !Number.isFinite(drainMs) || drainMs < 0)
        throw new TypeError('drainMs must be a finite number of milliseconds, 0 or more');
      if (signal !== undefined && !(signal instanceof AbortSignal))
        throw new TypeError('signal must be an AbortSignal');
      if (end) {
        throw new RelayError('CLOSED', 'The switchable subscription has ended; subscribe again', 'subscription');
      }
      signal?.throwIfAborted();
      let resolve!: (info: SwitchedInfo) => void;
      let reject!: (error: unknown) => void;
      const promise = new Promise<SwitchedInfo>((res, rej) => {
        resolve = res;
        reject = rej;
      });
      void promise.catch(() => {});
      let switching: PendingSwitch | undefined;
      // Open first: an invalid track or a closed connection throws here and leaves any pending switch alone.
      const subscription = open(track, params, () => switching as Slot);
      const previous = pending;
      switching = { track, subscription, until: predicate, drainMs, resolve, reject, removeAbort: () => {} };
      pending = switching;
      if (previous) abandon(previous, new RelayError('CLOSED', `Superseded by a switch to ${track}`, 'subscription'));
      if (subscription.end) {
        abandon(switching, endError(subscription.end));
        return promise;
      }
      if (signal) {
        const pendingSwitch = switching;
        const onAbort = () => {
          if (pending === pendingSwitch) abandon(pendingSwitch, signal.reason);
        };
        signal.addEventListener('abort', onAbort, { once: true });
        pendingSwitch.removeAbort = () => signal.removeEventListener('abort', onAbort);
      }
      return promise;
    },
    cancel(reason) {
      if (end) return;
      if (pending)
        abandon(
          pending,
          reason ?? new RelayError('CLOSED', 'The switchable subscription was cancelled', 'subscription'),
        );
      active.subscription.cancel(reason);
    },
  };
}
