/**
 * The announce race (see CONTEXT.md), and the application's side of it.
 *
 * A subscribe that reaches the relay before it has processed the publisher's announce of the
 * namespace is rejected with request error 404, moq-relay's literal for a namespace it has not seen
 * announced (not MOQT's DOES_NOT_EXIST); the relay parks nothing. The SDK reports that rejection like
 * any other and never retries a subscribe, so an application that may start before its publisher
 * retries within its own budget. This module is that policy, shared by the mux-global demo and the
 * live smoke test (scripts/test-relay.mjs), both of which serve it to the browser next to the SDK.
 *
 * A page that also owns the publisher awaits `publisher.announced` first: that removes most of the
 * race, and the retry covers the round trip the relay still needs after the announce reached the
 * wire (measured in spec/relay-interop.md).
 */
import { SubscriptionRequestError } from '@mux/relay';

/** moq-relay's request error for a namespace it has not seen announced ("Broadcast not found"). */
export const FRESH_NAMESPACE_ERROR_CODE = 404;
/** Milliseconds before the second attempt; the delay doubles from here. */
export const INITIAL_DELAY_MS = 500;
/** The longest wait between attempts, in milliseconds. */
export const MAX_DELAY_MS = 2000;

/**
 * Subscribe, retrying only the fresh-namespace 404 until the relay answers otherwise.
 *
 * Returns the first subscription whose `subscribed` is not that rejection: accepted, or rejected for
 * another reason, which the caller reads from `subscribed` or `handlers.onError` exactly as after a
 * plain `subscribe()`. The 404s this retries never reach `handlers.onError`; every other event
 * reaches the handlers untouched. The delay starts at `initialDelayMs` and doubles up to
 * `maxDelayMs`; `onRetry(attempt, delayMs)` runs before each wait. Only `signal` bounds the
 * retries: aborting it rejects with the abort reason, and cancels a request the relay has not
 * answered yet so it cannot be accepted after the budget ran out.
 *
 * @template {{ subscribed: Promise<unknown> }} T
 * @param {{ subscribe(options: object, handlers?: object): T }} subscriber
 * @param {object} options The subscribe request: `namespace`, `track` and optional `parameters`.
 * @param {object | undefined} handlers
 * @param {{ signal?: AbortSignal, initialDelayMs?: number, maxDelayMs?: number, onRetry?: (attempt: number, delayMs: number) => void }} [retry]
 * @returns {Promise<T>}
 */
export async function subscribeWithRetry(
  subscriber,
  options,
  handlers,
  { signal, initialDelayMs = INITIAL_DELAY_MS, maxDelayMs = MAX_DELAY_MS, onRetry } = {},
) {
  let delayMs = initialDelayMs;
  let attempt = 0;
  for (;;) {
    attempt += 1;
    signal?.throwIfAborted();
    const subscription = subscriber.subscribe(options, mutingRetriedRejections(handlers));
    try {
      await unlessAborted(subscription.subscribed, signal);
      return subscription;
    } catch (error) {
      if (signal?.aborted) {
        // The budget ran out while the relay had not answered: a request left pending could still be
        // accepted and deliver objects to handlers nobody expects to run.
        subscription.cancel(signal.reason);
        throw signal.reason;
      }
      // The connection ended first, or the caller cancelled the handle: it carries the reason.
      if (!isFreshNamespaceRejection(error)) return subscription;
    }
    onRetry?.(attempt, delayMs);
    await sleep(delayMs, signal);
    delayMs = Math.min(delayMs * 2, maxDelayMs);
  }
}

/** `promise`, unless `signal` aborts first, which rejects with the abort reason. */
async function unlessAborted(promise, signal) {
  if (!signal) return promise;
  signal.throwIfAborted();
  let abort = () => {};
  const aborted = new Promise((_, reject) => {
    abort = () => reject(signal.reason);
    signal.addEventListener('abort', abort, { once: true });
  });
  try {
    return await Promise.race([promise, aborted]);
  } finally {
    signal.removeEventListener('abort', abort);
  }
}

function isFreshNamespaceRejection(error) {
  return error instanceof SubscriptionRequestError && error.requestError.errorCode === FRESH_NAMESPACE_ERROR_CODE;
}

/**
 * `handlers` whose `onError` skips the fresh-namespace 404 until the relay has accepted the request.
 * The retry reports the outcome through the returned handle, so the callback would otherwise see
 * every attempt fail. A 404 after acceptance is a different event and passes through.
 */
function mutingRetriedRejections(handlers) {
  if (!handlers?.onError) return handlers;
  const { onError, onSubscribed } = handlers;
  let accepted = false;
  return {
    ...handlers,
    onSubscribed(info) {
      accepted = true;
      onSubscribed?.(info);
    },
    onError(error) {
      if (error.errorCode === FRESH_NAMESPACE_ERROR_CODE && !accepted) return;
      onError(error);
    },
  };
}

function sleep(ms, signal) {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(signal.reason);
      return;
    }
    const abort = () => {
      clearTimeout(timer);
      reject(signal?.reason);
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', abort);
      resolve();
    }, ms);
    signal?.addEventListener('abort', abort, { once: true });
  });
}
