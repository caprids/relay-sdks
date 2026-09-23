/** `subscribeWithRetry` over a scripted subscriber: what it retries, what it returns, what the handlers see. */
import { expect, it, vi } from 'vitest';
import { subscribeWithRetry } from '../examples/shared/announce-race.js';
import { SubscriptionRequestError } from '../src/index.js';

// The policy imports the SDK by its package name, as the demo does; everything is real.
vi.mock('@mux/relay', () => vi.importActual('../src/index.js'));

const options = { namespace: ['example', 'race'], track: 'ticker' };

/** One attempt's handle: the handlers it was given and a `subscribed` the script settles. */
class FakeSubscription {
  #resolve;
  #reject;

  constructor(handlers) {
    this.handlers = handlers;
    this.cancelled = [];
    this.subscribed = new Promise((resolve, reject) => {
      this.#resolve = resolve;
      this.#reject = reject;
    });
    void this.subscribed.catch(() => {});
  }

  accept() {
    const info = { trackAlias: 7, parameters: {}, trackProperties: [] };
    this.handlers?.onSubscribed?.(info);
    this.#resolve(info);
  }

  /** The relay's REQUEST_ERROR: the callback and the promise both see it, as in the SDK. */
  reject(errorCode, reason) {
    const error = { errorCode, retryInterval: 0, reason };
    this.handlers?.onError?.(error);
    this.#reject(new SubscriptionRequestError(error));
  }

  fail(error) {
    this.#reject(error);
  }

  cancel(reason) {
    this.cancelled.push(reason);
  }
}

/** Answers each `subscribe()` with the next scripted answer on a later microtask. */
class FakeSubscriber {
  constructor(...answers) {
    this.answers = answers;
    this.attempts = [];
    this.subscriptions = [];
  }

  subscribe(options, handlers) {
    const subscription = new FakeSubscription(handlers);
    this.attempts.push({ options, handlers });
    this.subscriptions.push(subscription);
    const answer = this.answers.shift();
    if (answer) queueMicrotask(() => answer(subscription));
    return subscription;
  }
}

const notFound = (subscription) => subscription.reject(404, 'Broadcast not found');
const accepted = (subscription) => subscription.accept();

/** The policy over the fake, with delays short enough for a test. */
function retry(subscriber, handlers, { initialDelayMs = 1, maxDelayMs = 4, ...rest } = {}) {
  return subscribeWithRetry(subscriber, options, handlers, { initialDelayMs, maxDelayMs, ...rest });
}

it('returns the accepted subscription after retrying a fresh-namespace 404', async () => {
  const subscriber = new FakeSubscriber(notFound, accepted);
  const seen = [];
  const retries = [];

  const subscription = await retry(
    subscriber,
    { onError: (error) => seen.push(error) },
    { onRetry: (attempt, delayMs) => retries.push([attempt, delayMs]) },
  );

  expect(subscription).toBe(subscriber.subscriptions[1]);
  await expect(subscription.subscribed).resolves.toMatchObject({ trackAlias: 7 });
  expect(subscriber.attempts.map((attempt) => attempt.options)).toEqual([options, options]);
  expect(retries).toEqual([[1, 1]]);
  expect(seen).toEqual([]);
});

it('returns a subscription rejected for another reason without retrying', async () => {
  const subscriber = new FakeSubscriber((subscription) => subscription.reject(500, 'internal error'));
  const seen = [];
  const onRetry = vi.fn();

  const subscription = await retry(subscriber, { onError: (error) => seen.push(error) }, { onRetry });

  expect(subscription).toBe(subscriber.subscriptions[0]);
  expect(seen.map((error) => error.errorCode)).toEqual([500]);
  expect(onRetry).not.toHaveBeenCalled();
  await expect(subscription.subscribed).rejects.toMatchObject({ requestError: { errorCode: 500 } });
});

it('doubles the delay up to the cap', async () => {
  const subscriber = new FakeSubscriber(notFound, notFound, notFound, notFound, accepted);
  const retries = [];

  await retry(subscriber, undefined, { onRetry: (attempt, delayMs) => retries.push([attempt, delayMs]) });

  expect(retries).toEqual([
    [1, 1],
    [2, 2],
    [3, 4],
    [4, 4],
  ]);
  expect(subscriber.attempts).toHaveLength(5);
});

it('rejects with the abort reason while waiting and subscribes no further', async () => {
  const subscriber = new FakeSubscriber(notFound);
  const owner = new AbortController();
  const reason = new Error('run deadline');

  const pending = retry(subscriber, undefined, { signal: owner.signal, initialDelayMs: 10_000, maxDelayMs: 10_000 });
  await new Promise((resolve) => setTimeout(resolve, 5));
  owner.abort(reason);

  await expect(pending).rejects.toBe(reason);
  await new Promise((resolve) => setTimeout(resolve, 5));
  expect(subscriber.attempts).toHaveLength(1);
});

it("rejects with the abort reason while awaiting the relay's answer and cancels the pending request", async () => {
  const subscriber = new FakeSubscriber(); // never answers
  const owner = new AbortController();
  const reason = new Error('run deadline');

  const pending = retry(subscriber, undefined, { signal: owner.signal });
  await new Promise((resolve) => setTimeout(resolve, 5));
  owner.abort(reason);

  await expect(pending).rejects.toBe(reason);
  expect(subscriber.attempts).toHaveLength(1);
  expect(subscriber.subscriptions[0].cancelled).toEqual([reason]);
});

it('returns the handle when the connection ends first', async () => {
  const closed = new Error('connection closed');
  const subscriber = new FakeSubscriber((subscription) => subscription.fail(closed));

  const subscription = await retry(subscriber, undefined);

  expect(subscription).toBe(subscriber.subscriptions[0]);
  await expect(subscription.subscribed).rejects.toBe(closed);
});

it('lets a 404 after acceptance reach onError', async () => {
  const subscriber = new FakeSubscriber(accepted);
  const seen = [];

  const subscription = await retry(subscriber, { onError: (error) => seen.push(error) });
  // A rejection after acceptance is a different event (a retract, for instance), not the race.
  subscription.handlers.onError({ errorCode: 404, retryInterval: 0, reason: 'dropped' });

  expect(seen.map((error) => error.errorCode)).toEqual([404]);
});

it('forwards the other handlers and leaves handlers without onError alone', async () => {
  const subscriber = new FakeSubscriber(notFound, accepted);
  const infos = [];
  const onObject = vi.fn();
  const handlers = { onSubscribed: (info) => infos.push(info), onObject, onError: () => {} };

  const subscription = await retry(subscriber, handlers);

  expect(infos.map((info) => info.trackAlias)).toEqual([7]);
  expect(subscription.handlers.onObject).toBe(onObject);

  const plain = { onObject };
  const accepting = new FakeSubscriber(accepted);
  const direct = await retry(accepting, plain);
  expect(direct.handlers).toBe(plain);
});
