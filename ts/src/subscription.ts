/**
 * The application's subscription (see CONTEXT.md): one SUBSCRIBE request from creation to its
 * terminal state, as distinct from demand, the relay's own subscription to a publisher.
 *
 * The module owns the core handle, the connection's closure observation and the terminal state, and
 * translates the core's callbacks into product events once. Its consumers are the callback form
 * (`RelaySubscriber.subscribe` with handlers) and the bounded events iterator; both attach through
 * {@link SubscriptionConsumer} and see the same events and the same ending. Nothing here is exported
 * from the package: the public handle keeps the core `Subscription` shape, and readiness and
 * termination (issue #68) and the pre-acceptance `update()` queue (issue #69) land here later.
 */
import type {
  Goaway,
  MessageParameters,
  MoqtObject,
  PublishDone,
  RequestError,
  SubscribeOk,
  Subscription,
  SubscriptionHandlers,
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
 * How a subscription ended. `accepted` says whether SUBSCRIBE_OK had arrived first, which is what
 * separates a rejection from a termination. A REQUEST_ERROR can arrive after acceptance: the core
 * answers a refused `update()` that way and ends the request (issue #94).
 */
export type SubscriptionEnd =
  | { cause: 'request-error'; accepted: boolean; error: RequestError }
  | { cause: 'connection-closed'; accepted: boolean; error: unknown }
  | { cause: 'cancelled'; accepted: boolean; error: unknown };

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
  /** Observe the connection's closure; the returned function removes the observer. */
  observeClosed(observer: (error?: unknown) => void): () => void;
}

/** The product subscription: the core handle plus the lifecycle every consumer shares. */
export interface ProductSubscription extends Subscription {
  /** Attach a consumer; one attached after the end is told the end at once. Returns the detach. */
  attach(consumer: SubscriptionConsumer): () => void;
  /** Whether SUBSCRIBE_OK has arrived. */
  readonly accepted: boolean;
  /** How the subscription ended, once it has. */
  readonly end: SubscriptionEnd | undefined;
}

/** One promise reaction per connection; individual observers can be removed on early exit. */
export function closureObservers(closed: Promise<{ error?: unknown }>) {
  const observers = new Set<(error?: unknown) => void>();
  let ended = false;
  let reason: unknown;
  void closed.then(({ error }) => {
    ended = true;
    reason = error;
    for (const observer of observers) observer(error);
    observers.clear();
  });
  return (observer: (error?: unknown) => void): (() => void) => {
    if (ended) observer(reason);
    else observers.add(observer);
    return () => {
      observers.delete(observer);
    };
  };
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

export function createSubscription(
  deps: SubscriptionDependencies,
  initial: readonly SubscriptionConsumer[] = [],
): ProductSubscription {
  const consumers = new Set(initial);
  let accepted = false;
  let end: SubscriptionEnd | undefined;
  let core: Subscription | undefined;
  let removeClosed: () => void = () => {};

  const finish = (next: SubscriptionEnd) => {
    if (end) return;
    end = next;
    removeClosed();
    // Every ending cancels the core handle: after a REQUEST_ERROR or closure the core has already
    // dropped the request, and cancelling then is harmless; after a local cancel it is the point.
    core?.cancel(next.error);
    const ended = [...consumers];
    consumers.clear();
    for (const consumer of ended) consumer.ended(next);
  };
  const deliver = (event: SubscriptionEvent) => {
    if (end) return;
    if (event.type === 'subscribed') accepted = true;
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

  removeClosed = deps.observeClosed((error) => finish({ cause: 'connection-closed', accepted, error }));
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
        onError: (error) => finish({ cause: 'request-error', accepted, error }),
      });
    } catch (error) {
      finish({ cause: 'cancelled', accepted, error });
      throw error;
    }
    // A source may deliver callbacks synchronously during subscribe(); the handle did not exist
    // when the ending was recorded, so cancel it now.
    const endedDuringSubscribe = ending();
    if (endedDuringSubscribe) core.cancel(endedDuringSubscribe.error);
  }

  return {
    get requestId() {
      return handle().requestId;
    },
    update(parameters) {
      handle().update(parameters);
    },
    cancel(reason) {
      finish({ cause: 'cancelled', accepted, error: reason });
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
