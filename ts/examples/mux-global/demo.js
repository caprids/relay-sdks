/**
 * Publish JSON ticks to a Relay namespace and subscribe to them again.
 *
 * This is the part of the example that uses the SDK. It runs in any browser with WebTransport;
 * run.mjs serves it next to the built SDK and drives it in headless Chromium because Node has no
 * WebTransport. `log` receives one plain object per event so the runner can print it.
 */
import { connectRelayPublisher, connectRelaySubscriber } from '@mux/relay';
import { subscribeWithRetry } from '../shared/announce-race.js';

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/**
 * @param {object} config
 * @param {string} config.url Relay WebTransport endpoint, e.g. https://sjc.relay.mux.global
 * @param {string} config.token Relay JWT authorized for `namespace`
 * @param {string[]} config.namespace Absolute namespace tuple, including the token's root prefix
 * @param {string} config.track Track name inside the namespace
 * @param {'both' | 'publish' | 'subscribe'} config.mode Run both sides in this page, or only one
 * @param {number} config.count Objects to publish and/or wait for
 * @param {number} config.intervalMs Delay between published objects
 * @param {number} config.timeoutMs Deadline for the whole run
 * @param {(entry: Record<string, unknown>) => void} log
 */
export async function runDemo({ url, token, namespace, track, mode, count, intervalMs, timeoutMs }, log) {
  const lifetime = new AbortController();
  const deadline = setTimeout(() => lifetime.abort(new Error(`Run exceeded ${timeoutMs} ms`)), timeoutMs);
  const connections = [];
  const problems = [];
  let received = 0;
  let closing = false;

  // Both connection kinds expose `closed`; a connection ending before we close it is a failure.
  const watch = async (label, connecting) => {
    let connection;
    try {
      connection = await connecting;
    } catch (error) {
      throw new Error(`${label} failed to connect (check the token and endpoint): ${describe(error)}`, {
        cause: error,
      });
    }
    connections.push(connection);
    void connection.closed.then(({ error }) => {
      if (!closing) problems.push(`${label} connection closed: ${describe(error)}`);
    });
    return connection;
  };
  const failIfBroken = () => {
    if (problems.length > 0) throw new Error(problems.join('; '));
    lifetime.signal.throwIfAborted();
  };

  try {
    let writer;
    let publisher;
    if (mode !== 'subscribe') {
      // One publisher connection serves one namespace; register as many tracks on it as needed.
      publisher = await watch(
        'publisher',
        connectRelayPublisher({ url, namespace, getToken: () => token, signal: lifetime.signal }),
      );
      log({ event: 'publisher connected', url, namespace: namespace.join('/') });
      writer = publisher.registerTrack({ track, writeTimeoutMs: 15_000 });
      // The announce leaves with the first registered track; the relay routes subscribes about a
      // round trip later. Logged so a two-process run shows the publisher's side.
      void publisher.announced.then(
        () => log({ event: 'announced', namespace: namespace.join('/') }),
        () => {},
      );
    }

    if (mode !== 'publish') {
      const subscriber = await watch(
        'subscriber',
        connectRelaySubscriber({ url, getToken: () => token, signal: lifetime.signal }),
      );
      log({ event: 'subscriber connected', url });
      // Same page as the publisher: subscribing before its announce reached the wire would be
      // rejected for certain; after it, the shared retry covers the round trip the relay still needs.
      if (publisher) await publisher.announced;
      // subscribeWithRetry runs in the background (nothing below depends on its result), but
      // subscribe() can itself throw synchronously (e.g. a namespace too long to encode); catch that
      // here instead of leaving it an unhandled rejection, so the run fails promptly on the real
      // error rather than idling until the deadline. Our own abort ends its wait; that is not a failure.
      void subscribeWithRetry(
        subscriber,
        { namespace, track },
        {
          onSubscribed: (ok) =>
            log({ event: 'subscribed', namespace: namespace.join('/'), track, alias: ok.trackAlias }),
          onObject: (object) => {
            // Status-only objects (end-of-group and end-of-track markers) carry no payload.
            if (object.status !== 'normal') return;
            received += 1;
            log({
              event: 'received',
              group: object.groupId,
              bytes: object.payload.byteLength,
              ...inspect(object.payload),
            });
          },
          onError: (error) => problems.push(`subscribe rejected: code ${error.errorCode} ${error.reason}`),
          onSubgroupReset: ({ groupId, error }) => problems.push(`group ${groupId} reset: ${describe(error)}`),
          onPublisherDone: (done) => log({ event: 'publisher done', status: done.statusCode, reason: done.reason }),
        },
        {
          signal: lifetime.signal,
          onRetry: () => log({ event: 'waiting', detail: 'namespace not announced yet; retrying the subscribe' }),
        },
      ).catch((error) => {
        if (lifetime.signal.aborted) return;
        problems.push(`subscribe failed: ${describe(error)}`);
        lifetime.abort(error);
      });
    }

    if (writer) {
      for (let seq = 1; seq <= count; seq += 1) {
        const bytes = encoder.encode(
          JSON.stringify({ seq, sentAt: new Date().toISOString(), message: `tick ${seq} from the @mux/relay example` }),
        );
        await writeWhenDemanded(writer, bytes, failIfBroken, lifetime.signal, log);
        log({ event: 'published', seq, bytes: bytes.byteLength });
        if (seq < count) await sleep(intervalMs, lifetime.signal);
      }
    }

    while (mode !== 'publish' && received < count) {
      failIfBroken();
      await sleep(100, lifetime.signal);
    }
    failIfBroken();
    return { published: writer ? count : 0, received };
  } finally {
    closing = true;
    clearTimeout(deadline);
    // publisher.close() retracts the namespace and awaits shutdown; subscriber.close() is synchronous.
    try {
      await Promise.all(connections.map((connection) => connection.close()));
    } finally {
      // Closed connections have removed their abort listeners; only retry sleeps need waking now.
      lifetime.abort();
    }
  }
}

/** Wait without sending; once write() starts, failure may mean partial delivery. Never replay it. */
async function writeWhenDemanded(writer, bytes, failIfBroken, signal, log) {
  if (!writer.hasDemand) log({ event: 'waiting', detail: 'no relay demand yet; waiting before writing' });
  while (!writer.hasDemand) {
    failIfBroken();
    if (writer.closed) throw new Error('Published track closed while waiting for demand');
    await sleep(100, signal);
  }
  failIfBroken();
  // Demand can disappear after the check. The SDK bounds that race with its write deadline.
  // Even a timeout is propagated: this API cannot prove that no bytes were sent.
  await writer.write(bytes);
}

/** The SDK delivers raw bytes. This expects the JSON written above and shows anything else as text. */
function inspect(payload) {
  const text = decoder.decode(payload);
  try {
    const { seq, sentAt } = JSON.parse(text);
    // Only a real one-way latency when the same page published it; across machines this includes clock skew.
    return { seq, ageMs: sentAt ? Math.max(0, Date.now() - Date.parse(sentAt)) : undefined };
  } catch {
    return { text: text.slice(0, 60) };
  }
}

function describe(error) {
  if (!(error instanceof Error)) return String(error ?? 'no error');
  return error.name && error.name !== 'Error' ? `${error.name}: ${error.message}` : error.message;
}

function sleep(ms, signal) {
  return new Promise((resolve, reject) => {
    signal.throwIfAborted();
    const abort = () => {
      clearTimeout(timer);
      reject(signal.reason);
    };
    const timer = setTimeout(() => {
      signal.removeEventListener('abort', abort);
      resolve();
    }, ms);
    signal.addEventListener('abort', abort, { once: true });
  });
}
