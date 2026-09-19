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

export interface SubscriptionEventOptions {
  /** Maximum queued events (including lifecycle events). Positive integer; default 64. */
  capacity?: number;
  /** Cancels only this subscription. */
  signal?: AbortSignal;
}

export class SubscriptionOverflowError extends Error {
  constructor() {
    super('Subscription event buffer overflow');
    this.name = 'SubscriptionOverflowError';
  }
}

export class SubscriptionRequestError extends Error {
  readonly requestError: RequestError;

  constructor(requestError: RequestError) {
    super(requestError.reason);
    this.name = 'SubscriptionRequestError';
    this.requestError = requestError;
  }
}

export class SubscriptionConnectionClosedError extends Error {
  constructor(cause?: unknown) {
    super('Connection closed; subscription delivery ended without a drain guarantee', { cause });
    this.name = 'SubscriptionConnectionClosedError';
  }
}

/** Single-consumer iterator owning one subscription, never the shared connection. */
export interface SubscriptionEvents extends AsyncIterableIterator<SubscriptionEvent> {
  /** Discard queued events and settle pending reads as EOF. Idempotent. */
  close(): void;
  return(): Promise<IteratorResult<SubscriptionEvent>>;
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

export function createSubscriptionEvents(
  subscribe: (handlers: SubscriptionHandlers) => Subscription,
  observeClosed: (observer: (error?: unknown) => void) => () => void,
  { capacity = 64, signal }: SubscriptionEventOptions = {},
): SubscriptionEvents {
  if (!Number.isSafeInteger(capacity) || capacity <= 0)
    throw new RangeError('capacity must be a positive safe integer');
  let queue: SubscriptionEvent[] = [];
  let pending:
    | { resolve: (result: IteratorResult<SubscriptionEvent>) => void; reject: (error: unknown) => void }
    | undefined;
  let ended = false;
  let failure: unknown;
  let subscription: Subscription | undefined;
  let removeClosed = () => {};
  const finish = (error?: unknown) => {
    if (ended) return;
    ended = true;
    failure = error;
    queue = [];
    removeClosed();
    signal?.removeEventListener('abort', abort);
    subscription?.cancel(error);
    const read = pending;
    pending = undefined;
    if (error !== undefined) read?.reject(error);
    else read?.resolve({ done: true, value: undefined });
  };
  const abort = () => finish(signal?.reason ?? new DOMException('Subscription aborted', 'AbortError'));
  const push = (event: SubscriptionEvent) => {
    if (ended) return;
    if (pending) {
      const read = pending;
      pending = undefined;
      read.resolve({ done: false, value: event });
    } else if (queue.length === capacity) finish(new SubscriptionOverflowError());
    else queue.push(event);
  };
  if (signal?.aborted) abort();
  else {
    signal?.addEventListener('abort', abort, { once: true });
    removeClosed = observeClosed((error) => finish(new SubscriptionConnectionClosedError(error)));
    try {
      if (!ended)
        subscription = subscribe({
          onOk: (value) => push({ type: 'subscribed', value }),
          onObject: (value) => push({ type: 'object', value }),
          onDone: (value) => push({ type: 'publisherDone', value }),
          onSubgroupEnd: (value) => push({ type: 'subgroupEnd', value }),
          onSubgroupReset: (value) => push({ type: 'subgroupReset', value }),
          onStateNotify: (value) => push({ type: 'stateNotify', value }),
          onGoaway: (value) => push({ type: 'goaway', value }),
          onUpdateOk: () => push({ type: 'updateOk', value: undefined }),
          onError: (error) => finish(new SubscriptionRequestError(error)),
        });
      // Also supports a source delivering callbacks synchronously during subscribe().
      if (ended) {
        removeClosed();
        subscription?.cancel(failure);
      }
    } catch (error) {
      finish(error);
      throw error;
    }
  }
  return {
    [Symbol.asyncIterator]() {
      return this;
    },
    next() {
      if (pending) return Promise.reject(new TypeError('Only one pending subscription read is supported'));
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
