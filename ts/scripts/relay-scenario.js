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
 * @param {string | undefined} options.token The Relay token, or undefined to connect anonymously.
 * @param {string[]} options.namespace The fresh namespace the scenario publishes under.
 * @param {((url: string, protocols: string[]) => unknown) | undefined} options.createTransport The
 *   transport factory both connections use, or undefined for the SDK's default.
 * @param {(value: object) => unknown} options.report Receives each phase as it passes; may return a promise.
 */
export async function runRelayScenario({ endpoint, token, namespace, createTransport, report }) {
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
      url: endpoint,
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
            if (object.status === 'normal') received[track].push(Array.from(object.payload));
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
      url: endpoint,
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
          if (object.status === 'normal') received.lateStatus.push(Array.from(object.payload));
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
      namespace,
      events: received.events.length,
      status: received.status.length,
      announceRaceRetries: retries,
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
