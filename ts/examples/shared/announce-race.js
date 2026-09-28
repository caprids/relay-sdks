/**
 * The announce race (see CONTEXT.md), and the application's side of it.
 *
 * A subscribe that reaches the relay before it has processed the publisher's announce of the
 * namespace is rejected, and the relay parks nothing. moq-relay 0.15.8 answers request error 16
 * (0x10, MOQT's DOES_NOT_EXIST) with reason "unroutable"; moq-relay through 0.15.1 answered its
 * literal 404 ("Broadcast not found"). The SDK reports that rejection like any other and never
 * retries a subscribe, so an application that may start before its publisher retries within its own
 * budget. This module is that policy, shared by the mux-global demo and the live smoke test
 * (scripts/test-relay.mjs), both of which serve it to the browser next to the SDK.
 *
 * A page that also owns the publisher awaits `publisher.announced` first: that removes most of the
 * race, and the retry covers the round trip the relay still needs after the announce reached the
 * wire (measured in spec/relay-interop.md).
 */
import { SubscriptionRequestError } from '@mux/relay';

/** MOQT's DOES_NOT_EXIST: moq-relay 0.15.8's code for a namespace it has not seen announced. */
export const FRESH_NAMESPACE_ERROR_CODE = 0x10;
/**
 * The reason that singles the fresh namespace out of DOES_NOT_EXIST. moq-relay 0.15.8 sends the same
 * code with reason "not found" for an unknown track in an announced namespace, which no wait fixes;
 * retrying it would spin until the caller's budget ran out.
 */
export const FRESH_NAMESPACE_REASON = 'unroutable';
/** moq-relay's literal through 0.15.1 for a namespace it has not seen announced ("Broadcast not found"). */
export const LEGACY_FRESH_NAMESPACE_ERROR_CODE = 404;
/** Milliseconds before the second attempt; the delay doubles from here. */
export const INITIAL_DELAY_MS = 500;
/** The longest wait between attempts, in milliseconds. */
export const MAX_DELAY_MS = 2000;

/**
 * Subscribe, retrying only the fresh-namespace rejection until the relay answers otherwise.
 *
 * Returns the first subscription whose `subscribed` is not that rejection: accepted, or rejected for
 * another reason, which the caller reads from `subscribed` or `handlers.onError` exactly as after a
 * plain `subscribe()`. The rejections this retries never reach `handlers.onError`; every other event
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
      if (!(error instanceof SubscriptionRequestError && isFreshNamespaceRejection(error.requestError))) {
        return subscription;
      }
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

/**
 * Whether `requestError` is the relay refusing a namespace it has not seen announced (yet): either
 * 0.15.8's DOES_NOT_EXIST with reason "unroutable" or the older relays' 404. Any other DOES_NOT_EXIST
 * ("not found": the namespace is there, the track is not) is not the race.
 *
 * @param {{ errorCode: number, reason: string }} requestError
 * @returns {boolean}
 */
export function isFreshNamespaceRejection(requestError) {
  if (requestError.errorCode === LEGACY_FRESH_NAMESPACE_ERROR_CODE) return true;
  return requestError.errorCode === FRESH_NAMESPACE_ERROR_CODE && requestError.reason === FRESH_NAMESPACE_REASON;
}

/**
 * `handlers` whose `onError` skips the fresh-namespace rejection until the relay has accepted the
 * request. The retry reports the outcome through the returned handle, so the callback would
 * otherwise see every attempt fail. The same rejection after acceptance is a different event and
 * passes through. The wrappers call the caller's callbacks on the caller's object and return what
 * they return, so a method that uses `this` still works and the SDK's synchronous-callback guard
 * still sees a promise-returning handler and observes it.
 */
function mutingRetriedRejections(handlers) {
  if (!handlers?.onError) return handlers;
  // The SDK refuses a declared-async callback before it sends anything; a synchronous wrapper would
  // hide one from that check, so apply it here first.
  for (const name of ['onSubscribed', 'onError']) {
    if (Object.prototype.toString.call(handlers[name]) === '[object AsyncFunction]') {
      throw new TypeError(`${name} must be synchronous; use Relay subscribeEvents() for async consumption`);
    }
  }
  let accepted = false;
  return {
    ...handlers,
    onSubscribed(info) {
      accepted = true;
      return handlers.onSubscribed?.(info);
    },
    onError(error) {
      if (isFreshNamespaceRejection(error) && !accepted) return undefined;
      return handlers.onError(error);
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
