/**
 * Publish JSON ticks to a Relay namespace and subscribe to them again.
 *
 * This is the part of the example that uses the SDK, and it is runtime-neutral: run.mjs runs it in
 * Node with @mux/relay-quic's `createTransport`, and in a browser with WebTransport it runs as is,
 * on the SDK's default transport. `log` receives one plain object per event so the runner can print it.
 */
import { connectRelayPublisher, connectSupervisedSubscriber } from '@mux/relay';
import { writeWithRetry } from '../shared/demand-timeout.js';

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/**
 * @param {object} config
 * @param {string} config.url Relay endpoint, e.g. https://sjc.relay.mux.global/acme/rooms/42 (the token's root as its path)
 * @param {string} config.token Relay JWT authorized for `namespace`
 * @param {string[]} config.namespace Namespace tuple, relative to the endpoint's path (on the bare host, including the
 *   token's root prefix)
 * @param {string} config.track Track name inside the namespace
 * @param {'both' | 'publish' | 'subscribe'} config.mode Run both sides in this process, or only one
 * @param {number} config.count Objects to publish and/or wait for
 * @param {number} config.intervalMs Delay between published objects
 * @param {number} config.timeoutMs Deadline for the whole run
 * @param {((url: string, protocols: string[]) => unknown) | undefined} [config.createTransport] The
 *   transport factory both connections use, or undefined for the SDK's default (browser WebTransport)
 * @param {(entry: Record<string, unknown>) => void} log
 */
export async function runDemo(
  { url, token, namespace, track, mode, count, intervalMs, timeoutMs, createTransport },
  log,
) {
  const lifetime = new AbortController();
  const deadline = setTimeout(() => lifetime.abort(new Error(`Run exceeded ${timeoutMs} ms`)), timeoutMs);
  const connections = [];
  const problems = [];
  // Aborted by the first failure seen in a callback or background task, which wakes a write waiting for demand.
  const broken = new AbortController();
  const report = (problem) => {
    problems.push(problem);
    broken.abort(new Error(problem));
  };
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
      if (!closing) report(`${label} connection closed: ${describe(error)}`);
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
        connectRelayPublisher({ url, createTransport, namespace, getToken: () => token, signal: lifetime.signal }),
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
      // The supervised subscriber reconnects on its own, so a lost connection is logged, not a failure.
      const subscriber = await watch(
        'subscriber',
        connectSupervisedSubscriber({
          url,
          createTransport,
          getToken: () => token,
          signal: lifetime.signal,
          onStatus: (status, error) => {
            if (status === 'reconnecting') log({ event: 'subscriber reconnecting', detail: describe(error) });
          },
        }),
      );
      log({ event: 'subscriber connected', url });
      // The want keeps the track subscribed until the run ends: it retries the announce race (a subscribe
      // that reaches the relay before the publisher's announce), resubscribes after a publisher restart,
      // and moves to a new connection. Only a rejection no wait fixes reaches onError, and ends the run.
      subscriber.want(
        'ticks',
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
          onError: (error) =>
            report(`subscribe rejected: code ${error.requestError?.errorCode ?? error.code} ${error.message}`),
          onSubgroupReset: ({ groupId, error }) => report(`group ${groupId} reset: ${describe(error)}`),
          onPublisherDone: (done) => log({ event: 'publisher done', status: done.statusCode, reason: done.reason }),
          onRestarted: ({ reason }) => log({ event: 'resubscribing', reason }),
        },
      );
    }

    if (writer) {
      // The SDK waits for demand inside write(), up to the track's write deadline. Only a timeout in
      // that wait proves nothing was sent, so only it writes the same object again.
      // The SDK can wait for demand for a whole write deadline. A failure recorded meanwhile (a rejected
      // subscribe, a closed connection) aborts that wait and becomes the run's error, rather than
      // surfacing a deadline later or hiding behind the run's own timeout.
      const retry = {
        signal: AbortSignal.any([lifetime.signal, broken.signal]),
        onRetry: () => {
          failIfBroken();
          log({
            event: 'waiting',
            detail: 'no relay demand within the write deadline; nothing was sent, writing again',
          });
        },
        onUncertain: (error) =>
          log({
            event: 'write failed',
            code: error.code,
            phase: error.phase,
            detail: 'sending had started; the relay may hold the object, so it is not resent',
          }),
      };
      for (let seq = 1; seq <= count; seq += 1) {
        const bytes = encoder.encode(
          JSON.stringify({ seq, sentAt: new Date().toISOString(), message: `tick ${seq} from the @mux/relay example` }),
        );
        failIfBroken();
        if (!writer.hasDemand) log({ event: 'waiting', detail: 'no relay demand yet; write() waits for a subscriber' });
        try {
          await writeWithRetry(writer, bytes, retry);
        } catch (error) {
          if (broken.signal.aborted && error === broken.signal.reason) failIfBroken();
          throw error;
        }
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

/** The SDK delivers raw bytes. This expects the JSON written above and shows anything else as text. */
function inspect(payload) {
  const text = decoder.decode(payload);
  try {
    const { seq, sentAt } = JSON.parse(text);
    // Only a real one-way latency when the same process published it; across machines this includes clock skew.
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
