/**
 * The bounded events iterator: one consumer of a subscription (see `subscription.ts`) that queues
 * product events with a capacity and turns the subscription's ending into the three subscription
 * error classes. It owns its subscription, never the connection.
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
  { capacity = 64, signal }: SubscriptionEventOptions = {},
): SubscriptionEvents {
  if (typeof capacity !== 'number') throw new TypeError('capacity must be a number');
  if (!Number.isSafeInteger(capacity) || capacity <= 0) throw new RangeError('capacity must be a positive integer');
  let queue: SubscriptionEvent[] = [];
  let pending:
    | { resolve: (result: IteratorResult<SubscriptionEvent>) => void; reject: (error: unknown) => void }
    | undefined;
  let ended = false;
  let failure: unknown;
  let subscription: ProductSubscription | undefined;
  const finish = (error?: unknown) => {
    if (ended) return;
    ended = true;
    failure = error;
    queue = [];
    signal?.removeEventListener('abort', abort);
    subscription?.cancel(error);
    const read = pending;
    pending = undefined;
    if (error !== undefined) read?.reject(error);
    else read?.resolve({ done: true, value: undefined });
  };
  const abort = () => finish(signal?.reason ?? new DOMException('Subscription aborted', 'AbortError'));
  const consumer: SubscriptionConsumer = {
    event(event) {
      if (ended) return;
      if (pending) {
        const read = pending;
        pending = undefined;
        read.resolve({ done: false, value: event });
      } else if (queue.length === capacity) finish(new SubscriptionOverflowError());
      else queue.push(event);
    },
    ended(end) {
      // An ending this iterator started (close, return, abort, overflow) echoes back here.
      if (ended) return;
      if (end.cause === 'request-error') finish(new SubscriptionRequestError(end.error));
      else if (end.cause === 'connection-closed') finish(new SubscriptionConnectionClosedError(end.error));
      else finish(end.error);
    },
  };
  if (signal?.aborted) abort();
  else {
    signal?.addEventListener('abort', abort, { once: true });
    try {
      subscription = subscribe(consumer);
    } catch (error) {
      finish(error);
      throw error;
    }
    // Events can overflow the queue before subscribe() returns. The ending recorded then had no
    // subscription to cancel, so cancel it now; the echo of that cancel is ignored above.
    if (ended) subscription.cancel(failure);
  }
  return {
    [Symbol.asyncIterator]() {
      return this;
    },
    next() {
      if (pending) return Promise.reject(new Error('Only one pending subscription read is supported'));
      if (ended)
        return failure === undefined ? Promise.resolve({ done: true, value: undefined }) : Promise.reject(failure);
      const value = queue.shift();
      if (value) return Promise.resolve({ done: false, value });
      return new Promise((resolve, reject) => {
        pending = { resolve, reject };
      });
    },
    close: () => finish(),
    async return() {
      finish();
      return { done: true, value: undefined };
    },
  };
}
