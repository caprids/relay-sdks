/**
 * The bounded events iterator: a single consumer that queues events with a capacity and owns one
 * request, never the connection. `subscribeEvents` binds it to a subscription (see
 * `subscription.ts`) and turns the subscription's ending into the three subscription error classes;
 * `namespaceEvents` binds it to a namespace request (see `namespace-events.ts`).
 */
import {
  type ProductSubscription,
  SubscriptionConnectionClosedError,
  type SubscriptionConsumer,
  type SubscriptionEvent,
  SubscriptionOverflowError,
  SubscriptionRequestError,
} from './subscription.js';

export {
  SubscriptionConnectionClosedError,
  type SubscriptionEvent,
  SubscriptionOverflowError,
  SubscriptionRequestError,
} from './subscription.js';

/** Options for `subscribeEvents`: the iterator's queue bound and its own cancellation. */
export interface SubscriptionEventOptions {
  /** Maximum queued events (including lifecycle events). Positive integer; default 64. */
  capacity?: number;
  /** Cancels only this subscription. */
  signal?: AbortSignal;
}

/** Single-consumer iterator owning one subscription, never the shared connection. */
export interface SubscriptionEvents extends AsyncIterableIterator<SubscriptionEvent> {
  /** Discard queued events and settle pending reads as EOF. Idempotent. */
  close(): void;
  /** Called by `for await` on `break` or a throw: discards queued events, cancels the subscription, resolves done. */
  return(): Promise<IteratorResult<SubscriptionEvent>>;
}

export function createSubscriptionEvents(
  subscribe: (consumer: SubscriptionConsumer) => ProductSubscription,
  options: SubscriptionEventOptions = {},
): SubscriptionEvents {
  return createBoundedEvents<SubscriptionEvent>(
    (sink) =>
      subscribe({
        event: (event) => sink.event(event),
        ended(end) {
          if (end.cause === 'request-error') sink.ended(new SubscriptionRequestError(end.error));
          else if (end.cause === 'connection-closed') sink.ended(new SubscriptionConnectionClosedError(end.error));
          else sink.ended(end.error);
        },
      }),
    options,
  );
}

/** Where a producer delivers into a bounded iterator. */
export interface BoundedEventSink<E> {
  /** One event; past the capacity the iterator ends with `SubscriptionOverflowError`. */
  event(event: E): void;
  /**
   * The producer ended: `undefined` finishes the iterator cleanly, anything else is thrown from `next()`. Either way
   * the queue is discarded.
   */
  ended(error?: unknown): void;
  /** The producer finished cleanly: queued events are still read, then the iterator is done. */
  complete(): void;
}

/** The single-consumer iterator both events forms share: an owned request, a queue bound and a signal. */
export interface BoundedEvents<E> extends AsyncIterableIterator<E> {
  /** Discard queued events and settle pending reads as EOF. Idempotent. */
  close(): void;
  /** Called by `for await` on `break` or a throw: discards queued events, cancels the request, resolves done. */
  return(): Promise<IteratorResult<E>>;
}

/**
 * The bounded iterator over one owned request. `start` opens the request and returns its cancel; an
 * ending this iterator starts (close, return, abort, overflow) cancels it, and the echo of that
 * cancel through the sink is ignored.
 */
export function createBoundedEvents<E>(
  start: (sink: BoundedEventSink<E>) => { cancel(reason?: unknown): void },
  { capacity = 64, signal }: SubscriptionEventOptions = {},
): BoundedEvents<E> {
  if (typeof capacity !== 'number') throw new TypeError('capacity must be a number');
  if (!Number.isSafeInteger(capacity) || capacity <= 0) throw new RangeError('capacity must be a positive integer');
  let queue: E[] = [];
  let pending: { resolve: (result: IteratorResult<E>) => void; reject: (error: unknown) => void } | undefined;
  let ended = false;
  let failure: unknown;
  let request: { cancel(reason?: unknown): void } | undefined;
  const finish = (error?: unknown) => {
    if (ended) return;
    ended = true;
    failure = error;
    queue = [];
    signal?.removeEventListener('abort', abort);
    request?.cancel(error);
    const read = pending;
    pending = undefined;
    if (error !== undefined) read?.reject(error);
    else read?.resolve({ done: true, value: undefined });
  };
  const abort = () => finish(signal?.reason ?? new DOMException('Subscription aborted', 'AbortError'));
  const sink: BoundedEventSink<E> = {
    event(event) {
      if (ended) return;
      if (pending) {
        const read = pending;
        pending = undefined;
        read.resolve({ done: false, value: event });
      } else if (queue.length === capacity) finish(new SubscriptionOverflowError());
      else queue.push(event);
    },
    ended(error) {
      // An ending this iterator started (close, return, abort, overflow) echoes back here.
      if (ended) return;
      finish(error);
    },
    complete() {
      if (ended) return;
      // A pending read only exists while the queue is empty, so finishing settles it as done.
      const rest = queue;
      finish();
      queue = rest;
    },
  };
  if (signal?.aborted) abort();
  else {
    signal?.addEventListener('abort', abort, { once: true });
    try {
      request = start(sink);
    } catch (error) {
      finish(error);
      throw error;
    }
    // Events can overflow the queue before start() returns. The ending recorded then had no
    // request to cancel, so cancel it now; the echo of that cancel is ignored above.
    if (ended) request.cancel(failure);
  }
  return {
    [Symbol.asyncIterator]() {
      return this;
    },
    next() {
      if (pending) return Promise.reject(new Error('Only one pending subscription read is supported'));
      // Events left by a clean completion are read before the end.
      if (queue.length > 0) return Promise.resolve({ done: false, value: queue.shift() as E });
      if (ended)
        return failure === undefined ? Promise.resolve({ done: true, value: undefined }) : Promise.reject(failure);
      return new Promise((resolve, reject) => {
        pending = { resolve, reject };
      });
    },
    close: () => {
      finish();
      queue = [];
    },
    async return() {
      finish();
      queue = [];
      return { done: true, value: undefined };
    },
  };
}
