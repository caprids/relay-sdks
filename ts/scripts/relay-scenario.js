/**
 * The live-relay scenario, runtime-neutral: it uses only APIs that browsers and Node both provide, and
 * gets everything runtime-specific from its launcher. `scripts/test-relay.mjs` is the Chromium launcher
 * and loads this module in the page; a Node launcher passes its own `createTransport`.
 *
 * It resolves the result object on success and rejects with the first failure. Reporting the outcome,
 * redacting the token and choosing the exit code are the launcher's business.
 */
import { connectRelayPublisher, connectRelaySubscriber } from '@mux/relay';
import { subscribeWithRetry } from '../examples/shared/announce-race.js';

/**
 * @param {object} options
 * @param {string} options.endpoint The relay's `https://` URL, without a token.
 * @param {string} [options.subEndpoint] The subscribers' relay, for a cross-relay run; defaults to `endpoint`.
 * @param {string | undefined} options.token The Relay token, or undefined to connect anonymously.
 * @param {string[]} options.namespace The fresh namespace the scenario publishes under.
 * @param {((url: string, protocols: string[]) => unknown) | undefined} options.createTransport The
 *   transport factory both connections use, or undefined for the SDK's default.
 * @param {(value: object) => unknown} options.report Receives each phase as it passes; may return a promise.
 */
export async function runRelayScenario({
  endpoint,
  subEndpoint = endpoint,
  token,
  namespace,
  createTransport,
  report,
}) {
  let publisher;
  let subscriber;
  let lateSubscriber;
  let lateLargestObject;
  const lifetime = new AbortController();
  const deadline = setTimeout(() => lifetime.abort(new Error('Live relay test exceeded 45 seconds')), 45_000);
  const received = { events: [], status: [], lateStatus: [] };
  const errors = [];
  const expectedEvents = [[0, 255, 128, 1], Array.from(new TextEncoder().encode('hello from Relay SDK')), []];
  let closing = false;
  const observeClosed = (label, connection) => {
    void connection.closed.then(({ error }) => {
      if (!closing) errors.push(`${label} closed: ${String(error ?? 'peer closure')}`);
    });
  };
  const waitFor = async (predicate, label) => {
    const until = performance.now() + 10_000;
    while (!predicate()) {
      if (errors.length > 0) throw new Error(errors.join('; '));
      lifetime.signal.throwIfAborted();
      if (performance.now() >= until) throw new Error(`Timed out waiting for ${label}`);
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
  };
  const waitForCount = (track, count) => waitFor(() => received[track].length >= count, `${track}: ${count} objects`);
  const warmUp = new TextEncoder().encode('relay-sdk-smoke warm-up');
  const isWarmUp = (payload) => payload.length === warmUp.length && payload.every((byte, i) => byte === warmUp[i]);
  const warmed = new Set();
  // SUBSCRIBE_OK says the subscriber's relay took the request, not that the path to the publisher is
  // ready. A relay on another hop first subscribes only to learn the track's info and cancels that within
  // about 50 ms (spec/relay-interop.md), so the writes it takes are lost. Repeat a marker the handlers
  // drop until one arrives; the assertion payloads are then written once.
  const warmUpTrack = async (track, writer) => {
    const until = performance.now() + 10_000;
    let writes = 0;
    while (!warmed.has(track)) {
      if (errors.length > 0) throw new Error(errors.join('; '));
      if (performance.now() >= until) throw new Error(`Timed out waiting for ${track}: warm-up object`);
      try {
        await writer.write(warmUp);
      } catch (error) {
        if (error?.code !== 'SUBSCRIPTION_CHANGED') throw error;
      }
      writes += 1;
      const sent = performance.now();
      while (!warmed.has(track) && performance.now() - sent < 250) {
        lifetime.signal.throwIfAborted();
        await new Promise((resolve) => setTimeout(resolve, 20));
      }
    }
    return writes;
  };
  try {
    publisher = await connectRelayPublisher({
      url: endpoint,
      createTransport,
      namespace,
      getToken: () => token,
      signal: lifetime.signal,
    });
    observeClosed('publisher', publisher);
    await report({ phase: 'publisher connected', namespace });
    const events = publisher.registerTrack({ track: 'events', writeTimeoutMs: 20_000 });
    const status = publisher.registerTrack({ track: 'status', writeTimeoutMs: 20_000 });
    subscriber = await connectRelaySubscriber({
      url: subEndpoint,
      getToken: () => token,
      signal: lifetime.signal,
      createTransport,
    });
    observeClosed('subscriber', subscriber);
    await report({ phase: 'subscriber connected' });
    // Assertion 2. The announce left with the first registered track; the relay routes subscribes to
    // the namespace about a round trip later, so the subscribe retries the rejection it may still answer.
    await publisher.announced;
    await report({ phase: 'announced' });
    const retries = { events: 0, status: 0 };
    for (const track of ['events', 'status']) {
      await subscribeWithRetry(
        subscriber,
        { namespace, track },
        {
          onSubscribed: () => {
            void report({ phase: 'subscribed', track });
          },
          onObject: (object) => {
            if (object.status !== 'normal') return;
            if (isWarmUp(object.payload)) warmed.add(track);
            else received[track].push(Array.from(object.payload));
          },
          onError: (error) => errors.push(`${track}: ${error.errorCode} ${error.reason}`),
          onSubgroupReset: ({ error }) => errors.push(`${track} reset: ${String(error)}`),
        },
        {
          signal: lifetime.signal,
          onRetry: (attempt, delayMs) => {
            retries[track] += 1;
            void report({ phase: 'subscribe retried', track, attempt, delayMs });
          },
        },
      );
    }
    if (errors.length > 0) throw new Error(errors.join('; '));
    const warmUpWrites = { events: await warmUpTrack('events', events), status: await warmUpTrack('status', status) };
    await report({ phase: 'warmed up', writes: warmUpWrites });
    for (const payload of expectedEvents) await events.write(new Uint8Array(payload));
    await status.write(new Uint8Array([42]));
    await waitForCount('events', 3);
    await waitForCount('status', 1);
    await report({
      phase: 'received initial payloads',
      events: received.events.length,
      status: received.status.length,
    });
    // A fresh subscriber must decode the relay's SUBSCRIBE_OK for a track with content.
    lateSubscriber = await connectRelaySubscriber({
      url: subEndpoint,
      getToken: () => token,
      signal: lifetime.signal,
      createTransport,
    });
    observeClosed('late subscriber', lateSubscriber);
    lateSubscriber.subscribe(
      { namespace, track: 'status', parameters: { locationFilter: { type: 'next-object' } } },
      {
        onSubscribed: (ok) => {
          lateLargestObject = ok.parameters.largestObject;
          if (!lateLargestObject) errors.push('late SUBSCRIBE_OK omitted LARGEST_OBJECT');
        },
        onObject: (object) => {
          if (object.status === 'normal' && !isWarmUp(object.payload))
            received.lateStatus.push(Array.from(object.payload));
        },
        onError: (error) => errors.push(`late status: ${error.errorCode} ${error.reason}`),
      },
    );
    await waitFor(() => lateLargestObject !== undefined, 'late SUBSCRIBE_OK with LARGEST_OBJECT');
    events.close();
    await status.write(new Uint8Array([43]));
    await waitForCount('status', 2);
    await waitForCount('lateStatus', 1);
    if (JSON.stringify(received.lateStatus) !== '[[43]]') throw new Error('Late subscriber payload mismatch');
    const samePayloads = (actual, expected) =>
      JSON.stringify(actual.map(JSON.stringify).sort()) === JSON.stringify(expected.map(JSON.stringify).sort());
    if (!samePayloads(received.events, expectedEvents) || !samePayloads(received.status, [[42], [43]])) {
      throw new Error(`Payload mismatch: ${JSON.stringify(received)}`);
    }
    if (errors.length > 0) throw new Error(errors.join('; '));
    return {
      endpoint,
      subscriberEndpoint: subEndpoint,
      namespace,
      events: received.events.length,
      status: received.status.length,
      announceRaceRetries: retries,
      warmUpWrites,
      binaryAndEmptyPayloads: 'passed',
      independentTrackClose: 'passed',
      lateSubscriberLargestObject: lateLargestObject,
    };
  } finally {
    closing = true;
    clearTimeout(deadline);
    try {
      await publisher?.close();
    } finally {
      subscriber?.close();
      lateSubscriber?.close();
    }
  }
}
