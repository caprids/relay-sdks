/**
 * The application's subscription (see CONTEXT.md): one SUBSCRIBE request from creation to its
 * terminal state, as distinct from demand, the relay's own subscription to a publisher.
 *
 * The module owns the core handle, the connection's closure observation and the terminal state, and
 * translates the core's callbacks into product events once. Its consumers are the callback form
 * (`RelaySubscriber.subscribe` with handlers) and the bounded events iterator; both attach through
 * {@link SubscriptionConsumer} and see the same events and the same ending. Its public face is
 * {@link RelaySubscription}: the core handle plus `subscribed` and `ended`, the readiness and
 * termination awaitables. The pre-acceptance `update()` queue lives here too.
 */
import {
  encodeRequestUpdate,
  type Goaway,
  type MessageParameters,
  type MoqtObject,
  type PublishDone,
  type RequestError,
  type SubscribeOk,
  type Subscription,
  type SubscriptionHandlers,
} from '@mux/moq-core';
import { RelayError } from './errors.js';

/** Delivery order is callback arrival order, with no ordering guarantee across streams. */
export type SubscriptionEvent =
  | { type: 'subscribed'; value: SubscribeOk }
  | { type: 'object'; value: MoqtObject }
  | { type: 'publisherDone'; value: PublishDone }
  | { type: 'subgroupEnd'; value: { groupId: number; subgroupId: number; endOfGroup: boolean } }
  | { type: 'subgroupReset'; value: { groupId: number; error: unknown } }
  | { type: 'stateNotify'; value: MessageParameters }
  | { type: 'goaway'; value: Goaway }
  | { type: 'updateOk'; value: undefined };

/**
 * The lifecycle name for how a subscription ended: `rejected` when the relay answered the request
 * with REQUEST_ERROR before accepting it (including the request timeout), `terminated` when an
 * accepted subscription can no longer deliver (the connection ended, or a REQUEST_ERROR arrived
 * after acceptance, which is how a refused `update()` is answered, issue #94) or when the connection
 * ended before any answer, and `cancelled` when the application cancelled it.
 */
export type SubscriptionEndKind = 'rejected' | 'terminated' | 'cancelled';

/**
 * How a subscription ended. `kind` is the lifecycle name; `cause` is what happened on the wire or
 * locally; `accepted` says whether SUBSCRIBE_OK had arrived; `error` is the REQUEST_ERROR, the
 * connection's failure (undefined after a clean shutdown), or the cancel reason.
 */
export type SubscriptionEnd =
  | { kind: 'rejected' | 'terminated'; cause: 'request-error'; accepted: boolean; error: RequestError }
  | { kind: 'terminated'; cause: 'connection-closed'; accepted: boolean; error: unknown }
  | { kind: 'cancelled'; cause: 'cancelled'; accepted: boolean; error: unknown };

/** The handle `RelaySubscriber.subscribe()` returns: the core handle plus readiness and termination. */
export interface RelaySubscription extends Subscription {
  /**
   * Settles when the relay answers. Resolves with the acceptance info `onSubscribed` receives.
   * Rejects with {@link SubscriptionRequestError} when the relay rejected the request or it timed
   * out, {@link SubscriptionConnectionClosedError} when the connection ended first, or the reason
   * given to `cancel()` (a `CLOSED` RelayError when none was given). Never rejects after acceptance.
   */
  readonly subscribed: Promise<SubscribeOk>;
  /**
   * Settles once, when the subscription can no longer deliver, with how it ended. Never rejects.
   * Publisher completion (`onPublisherDone`) is not an ending: trailing objects can still arrive,
   * and the request stays open until `cancel()` or the connection ends. Neither awaitable implies
   * exactly-once or gap-free delivery.
   */
  readonly ended: Promise<SubscriptionEnd>;
}

/** One consumer of a subscription: the handler object or an events iterator. */
export interface SubscriptionConsumer {
  /** One product event, delivered synchronously as the core delivers it. */
  event(event: SubscriptionEvent): void;
  /** The subscription ended; no event follows. Delivered once. */
  ended(end: SubscriptionEnd): void;
}

/** What the subscription borrows from the connection. */
export interface SubscriptionDependencies {
  /** The core session's subscribe, already bound to the track options. */
  subscribe(handlers: SubscriptionHandlers): Subscription;
  /**
   * Observe the connection's closure in the turn it happens, ahead of the request failures the
   * core synthesizes while tearing down. The returned function removes the observer.
   */
  observeClosed(observer: (error?: unknown) => void): () => void;
}

/** The product subscription as its consumers see it: the public handle plus the attach point. */
export interface ProductSubscription extends RelaySubscription {
  /** Attach a consumer; one attached after the end is told the end at once. Returns the detach. */
  attach(consumer: SubscriptionConsumer): () => void;
  /** Whether SUBSCRIBE_OK has arrived. */
  readonly accepted: boolean;
  /** How the subscription ended, once it has. */
  readonly end: SubscriptionEnd | undefined;
}

export class SubscriptionOverflowError extends RelayError {
  override readonly name = 'SubscriptionOverflowError';
  constructor() {
    super('SUBSCRIPTION_OVERFLOW', 'Subscription event buffer overflow', 'subscription');
  }
}

export class SubscriptionRequestError extends RelayError {
  override readonly name = 'SubscriptionRequestError';
  readonly requestError: RequestError;

  constructor(requestError: RequestError) {
    super('SUBSCRIPTION_REQUEST_FAILED', requestError.reason, 'subscription');
    this.requestError = requestError;
  }
}

export class SubscriptionConnectionClosedError extends RelayError {
  override readonly name = 'SubscriptionConnectionClosedError';
  constructor(cause?: unknown) {
    super('CLOSED', 'Connection closed; subscription delivery ended without a drain guarantee', 'connection', {
      cause,
    });
  }
}

/** The error `subscribed` rejects with for an ending that came before acceptance. */
export function endError(end: SubscriptionEnd): unknown {
  switch (end.cause) {
    case 'request-error':
      return new SubscriptionRequestError(end.error);
    case 'connection-closed':
      return new SubscriptionConnectionClosedError(end.error);
    case 'cancelled':
      // Any reason the caller gave stands, null included; only an omitted one gets the default.
      return end.error === undefined
        ? new RelayError('CLOSED', 'Subscription was cancelled before the relay answered', 'subscription')
        : end.error;
  }
}

/**
 * Snapshot one product callback and hold it to the synchronous callback policy: an async function
 * is refused before any request is sent, and a returned promise is observed and diagnosed once,
 * never awaited by delivery. Mirrors the core's own guard, which now only sees this module's lambdas.
 */
export function synchronousCallback(
  name: string,
  callback: (...args: never[]) => unknown,
  receiver: unknown,
): (...args: unknown[]) => void {
  if (Object.prototype.toString.call(callback) === '[object AsyncFunction]') {
    throw new TypeError(`${name} must be synchronous; use Relay subscribeEvents() for async consumption`);
  }
  let reported = false;
  return (...args) => {
    const returned: unknown = Reflect.apply(callback, receiver, args);
    if (
      returned &&
      (typeof returned === 'object' || typeof returned === 'function') &&
      'then' in returned &&
      typeof returned.then === 'function'
    ) {
      // JS promises cannot be cancelled. Observe failures without serializing delivery.
      void Promise.resolve(returned).catch(() => {});
      if (!reported) {
        reported = true;
        // biome-ignore lint/suspicious/noConsole: public diagnostic for unsupported async callbacks
        console.error(
          new TypeError(`${name} returned a promise; callbacks must be synchronous; use Relay subscribeEvents()`),
        );
      }
    }
  };
}

/**
 * How many `update()` calls may wait for SUBSCRIBE_OK. REQUEST_UPDATE is legal only on an accepted
 * subscription (draft-ietf-moq-transport-20 §5.1), so earlier updates queue here in order.
 */
export const UPDATE_QUEUE_BOUND = 8;

export function createSubscription(
  deps: SubscriptionDependencies,
  initial: readonly SubscriptionConsumer[] = [],
): ProductSubscription {
  const consumers = new Set(initial);
  let accepted = false;
  let end: SubscriptionEnd | undefined;
  let core: Subscription | undefined;
  let removeClosed: () => void = () => {};
  const queuedUpdates: MessageParameters[] = [];

  let resolveSubscribed!: (info: SubscribeOk) => void;
  let rejectSubscribed!: (error: unknown) => void;
  let resolveEnded!: (end: SubscriptionEnd) => void;
  const subscribed = new Promise<SubscribeOk>((resolve, reject) => {
    resolveSubscribed = resolve;
    rejectSubscribed = reject;
  });
  // Nobody may await readiness; a rejection must not surface as an unhandled rejection.
  void subscribed.catch(() => {});
  const ended = new Promise<SubscriptionEnd>((resolve) => {
    resolveEnded = resolve;
  });

  const finish = (next: SubscriptionEnd) => {
    if (end) return;
    end = next;
    queuedUpdates.length = 0;
    removeClosed();
    if (!accepted) rejectSubscribed(endError(next));
    resolveEnded(next);
    // Every ending cancels the core handle: after a REQUEST_ERROR or closure the core has already
    // dropped the request, and cancelling then is harmless; after a local cancel it is the point.
    core?.cancel(next.error);
    const consumersToTell = [...consumers];
    consumers.clear();
    for (const consumer of consumersToTell) consumer.ended(next);
  };
  const failRequest = (error: RequestError) => {
    if (accepted) {
      finish({ kind: 'terminated', cause: 'request-error', accepted: true, error });
      return;
    }
    // The core fails a pending request before it reports the closure that killed it, in the same
    // turn. Hold the failure until the turn ends, so a closure that follows is the ending this
    // subscription describes; a genuine rejection is recorded one microtask later.
    queueMicrotask(() => {
      if (!ending()) finish({ kind: 'rejected', cause: 'request-error', accepted: false, error });
    });
  };
  const connectionClosed = (error: unknown) =>
    finish({ kind: 'terminated', cause: 'connection-closed', accepted, error });
  const cancelWith = (error: unknown) => finish({ kind: 'cancelled', cause: 'cancelled', accepted, error });

  const deliver = (event: SubscriptionEvent) => {
    if (end) return;
    if (event.type === 'subscribed') {
      accepted = true;
      resolveSubscribed(event.value);
      // Now Established: the updates that waited go out in order, before consumers hear of it.
      try {
        for (const parameters of queuedUpdates.splice(0)) core?.update(parameters);
      } catch (error) {
        // Enqueue-time validation encoded each update with this request's id, so a throw here is
        // a core fault. Letting it escape into the core's request reader would disable the
        // subscription silently; end it loudly instead. Readiness settled first, so a caller
        // awaiting it sees the acceptance and then this ending.
        cancelWith(error);
        return;
      }
    }
    for (const consumer of [...consumers]) consumer.event(event);
  };
  /** Read through a call: the core's callbacks can end the subscription inside deps.subscribe(). */
  const ending = (): SubscriptionEnd | undefined => end;
  const handle = (): Subscription => {
    if (core) return core;
    const now = ending();
    throw now?.error instanceof Error
      ? now.error
      : new RelayError('CLOSED', 'Subscription ended before its request was sent', 'subscription');
  };

  removeClosed = deps.observeClosed(connectionClosed);
  if (!end) {
    try {
      core = deps.subscribe({
        onOk: (value) => deliver({ type: 'subscribed', value }),
        onObject: (value) => deliver({ type: 'object', value }),
        onDone: (value) => deliver({ type: 'publisherDone', value }),
        onSubgroupEnd: (value) => deliver({ type: 'subgroupEnd', value }),
        onSubgroupReset: (value) => deliver({ type: 'subgroupReset', value }),
        onStateNotify: (value) => deliver({ type: 'stateNotify', value }),
        onGoaway: (value) => deliver({ type: 'goaway', value }),
        onUpdateOk: () => deliver({ type: 'updateOk', value: undefined }),
        onError: failRequest,
      });
    } catch (error) {
      cancelWith(error);
      throw error;
    }
    // A source may end the subscription synchronously during subscribe(), through a closure
    // observer it invokes at once; the handle did not exist when the ending was recorded, so
    // cancel it now. A request failure delivered that early is held until the turn ends and
    // cancels the handle then.
    const endedDuringSubscribe = ending();
    if (endedDuringSubscribe) core.cancel(endedDuringSubscribe.error);
  }

  return {
    subscribed,
    ended,
    get requestId() {
      return handle().requestId;
    },
    update(parameters) {
      if (ending()) {
        throw new RelayError(
          'CLOSED',
          'Subscription has ended; open a new one to change what you receive',
          'subscription',
        );
      }
      if (accepted) {
        handle().update(parameters);
        return;
      }
      if (queuedUpdates.length >= UPDATE_QUEUE_BOUND) {
        throw new RelayError(
          'UPDATE_QUEUE_FULL',
          `More than ${UPDATE_QUEUE_BOUND} updates are waiting for the subscription to be accepted; await onSubscribed before updating again`,
          'subscription',
        );
      }
      // A malformed or oversize update must fail here, where the caller can see it, not during the
      // flush on SUBSCRIBE_OK, so it is encoded exactly as the flush will encode it. The queue
      // holds a snapshot, so later mutation of the caller's object cannot change what is sent, as
      // an immediate send would have serialized it at once.
      encodeRequestUpdate(handle().requestId, parameters);
      queuedUpdates.push(structuredClone(parameters));
    },
    cancel(reason) {
      cancelWith(reason);
    },
    attach(consumer) {
      if (end) {
        consumer.ended(end);
        return () => {};
      }
      consumers.add(consumer);
      return () => {
        consumers.delete(consumer);
      };
    },
    get accepted() {
      return accepted;
    },
    get end() {
      return end;
    },
  };
}
