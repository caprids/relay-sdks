/**
 * Namespace discovery: one SUBSCRIBE_NAMESPACE request from creation to its end, read through the
 * bounded events iterator `subscribeEvents` uses. The relay answers with the namespaces under the
 * prefix that are announced now, then each later announce and withdrawal; this module turns the
 * wire's prefix-relative entries into full namespaces, keeps the events edge-triggered, and maps the
 * request's ending onto the subscription error classes.
 */
import { isStreamFailure, type MoqtSession, REQUEST_ERROR_CODE, type RequestError } from '@caprid/moq-core';
import { RelayError } from './errors.js';
import { CLOSURE_GRACE_MS, SubscriptionConnectionClosedError, SubscriptionRequestError } from './subscription.js';
import { type BoundedEventSink, createBoundedEvents, type SubscriptionEventOptions } from './subscription-events.js';

/** One change in which namespaces under the prefix are announced, or the end of the relay's initial snapshot. */
export type NamespaceEvent =
  | {
      /**
       * `announced`: a publisher announced the namespace, or it was already announced when the request
       * was accepted. `unannounced`: its announce was withdrawn. Each namespace alternates between the
       * two; the relay's repeats are not passed on.
       */
      type: 'announced' | 'unannounced';
      /** The full namespace: the prefix's fields followed by the rest, without splitting or joining. */
      namespace: string[];
    }
  | {
      /**
       * Every namespace announced when the relay accepted the request has been yielded: an application that tracks
       * the announced set now holds the relay's view, and an empty set means nothing is announced. Yielded once, before
       * any later change, and never once the request has ended. With MoQ Active Count negotiated it follows the
       * NAMESPACE messages the relay counted (at once on a count of 0). Otherwise it is the SDK's estimate: the relay
       * accepted the request, then {@link SNAPSHOT_QUIET_MS} passed without an entry, and never later than
       * {@link SNAPSHOT_LIMIT_MS} after the relay accepted it.
       */
      type: 'loaded';
    };

/**
 * Without MoQ Active Count, how long the request's stream stays quiet after the relay accepts it before the relay's
 * snapshot counts as loaded. Each entry restarts it, so an empty prefix loads this long after acceptance. Provisional:
 * moq-relay 0.17.x wrote 60 entries within 0.94 ms of REQUEST_OK, cross-relay, a 50x margin (2026-11-03,
 * https://github.com/muxinc/relay-sdks/issues/422#issuecomment-6599550554).
 */
export const SNAPSHOT_QUIET_MS = 50;

/** Without MoQ Active Count, the latest `loaded` comes after the relay accepts, so a busy prefix still loads. */
export const SNAPSHOT_LIMIT_MS = 1_000;

const withdrawnByEnd = new WeakSet<NamespaceEvent>();

/**
 * Whether `event` is one of the withdrawals a clean end yields, not one the relay sent. The supervised subscriber's
 * discovery reads them as the request's end, so a narrower root it moves keeps what it saw.
 */
export function isWithdrawnByEnd(event: NamespaceEvent): boolean {
  return withdrawnByEnd.has(event);
}

const stopped = new WeakSet<NamespaceEvents>();

/**
 * Whether the request's stream has failed and its ending waits out the closure grace: nothing more arrives. The
 * supervised subscriber's discovery reads it so a member's 100 ms does not run out on a stream that is gone.
 */
export function isStopped(events: NamespaceEvents): boolean {
  return stopped.has(events);
}

type Snapshot =
  | { kind: 'unanswered' }
  | { kind: 'counted' }
  | { kind: 'quiet'; quiet: ReturnType<typeof setTimeout>; limit: ReturnType<typeof setTimeout> }
  | { kind: 'loaded' };

/** Options for `namespaceEvents`: the prefix, the iterator's queue bound and its own cancellation. */
export interface NamespaceEventOptions extends SubscriptionEventOptions {
  /** Namespace prefix to discover, matched field by field. Same rules as a namespace. */
  prefix: readonly string[];
}

/** Single-consumer iterator owning one namespace request, never the shared connection. */
export interface NamespaceEvents extends AsyncIterableIterator<NamespaceEvent> {
  /** Discard queued events and settle pending reads as EOF. Idempotent. */
  close(): void;
  /** Called by `for await` on `break` or a throw: discards queued events, cancels the request, resolves done. */
  return(): Promise<IteratorResult<NamespaceEvent>>;
}

/** What a namespace request borrows from the connection. */
export interface NamespaceRequestDependencies {
  /** The core session's SUBSCRIBE_NAMESPACE. */
  subscribeNamespace: MoqtSession['subscribeNamespace'];
  /** Observe the connection's closure in the turn it happens; returns the removal. */
  observeClosed(observer: (error?: unknown) => void): () => void;
  /** The connection's prefixes with a namespace request open; see {@link claimPrefix}. */
  prefixes: Set<readonly string[]>;
}

/**
 * Hold `prefix` for one request, or refuse it when it overlaps one already held: draft-22 §9.15 has a relay answer an
 * overlapping SUBSCRIBE_NAMESPACE on the same session with PREFIX_OVERLAP. moq-relay 0.17.0 does not, so the SDK
 * refuses it itself, before anything is sent, and behaves the same against every relay. Returns the release.
 */
function claimPrefix(prefixes: Set<readonly string[]>, prefix: readonly string[]): () => void {
  for (const held of prefixes) {
    const [shorter, longer] = held.length <= prefix.length ? [held, prefix] : [prefix, held];
    if (shorter.every((field, i) => field === longer[i])) {
      throw new RelayError(
        'PREFIX_OVERLAP',
        `This connection already discovers ${JSON.stringify(held)}, which overlaps ${JSON.stringify(prefix)}; one request per prefix tree`,
        'subscription',
      );
    }
  }
  prefixes.add(prefix);
  return () => prefixes.delete(prefix);
}

/** Open the iterator over `prefix`, whose fields the caller has already admitted. */
export function createNamespaceEvents(
  deps: NamespaceRequestDependencies,
  prefix: string[],
  options: SubscriptionEventOptions,
): NamespaceEvents {
  const events = createBoundedEvents<NamespaceEvent>(
    (sink) => openNamespaceRequest(deps, prefix, sink, () => stopped.add(events)),
    options,
  );
  return events;
}

function openNamespaceRequest(
  deps: NamespaceRequestDependencies,
  prefix: string[],
  sink: BoundedEventSink<NamespaceEvent>,
  onStopped: () => void,
): { cancel(reason?: unknown): void } {
  let ended = false;
  let core: { cancel(reason?: unknown): void } | undefined;
  let removeClosed: () => void = () => {};
  let cancelGrace: () => void = () => {};
  const releasePrefix = claimPrefix(deps.prefixes, prefix);
  // Keyed by the suffix's JSON: a field may hold any character, a separator could collide.
  const announced = new Set<string>();
  let snapshot: Snapshot = { kind: 'unanswered' };

  const stopTimers = () => {
    if (snapshot.kind !== 'quiet') return;
    clearTimeout(snapshot.quiet);
    clearTimeout(snapshot.limit);
  };
  const load = () => {
    if (ended || snapshot.kind === 'loaded') return;
    stopTimers();
    snapshot = { kind: 'loaded' };
    sink.event({ type: 'loaded' });
  };
  const heard = () => {
    if (snapshot.kind !== 'quiet') return;
    clearTimeout(snapshot.quiet);
    snapshot.quiet = setTimeout(load, SNAPSHOT_QUIET_MS);
  };

  const finish = (error?: unknown) => {
    if (ended) return;
    ended = true;
    releasePrefix();
    removeClosed();
    cancelGrace();
    stopTimers();
    core?.cancel(error);
    // A clean end keeps what is queued: the withdrawals the core reports for it come just before.
    if (error === undefined) sink.complete();
    else sink.ended(error);
  };
  /**
   * The same rule a subscription follows: a REQUEST_ERROR ends the request one microtask later, so a
   * closure reported in the same turn wins; a stream that failed without one may be the first sign of
   * a lost transport, so it waits for the closure up to the grace.
   */
  const fail = (error: RequestError, viaStream: boolean) => {
    const reject = () => finish(new SubscriptionRequestError(error));
    if (!viaStream) {
      queueMicrotask(reject);
      return;
    }
    onStopped();
    const grace = setTimeout(reject, CLOSURE_GRACE_MS);
    cancelGrace = () => clearTimeout(grace);
  };
  const change = (type: 'announced' | 'unannounced', suffix: string[], byEnd = false) => {
    if (ended) return;
    const key = JSON.stringify(suffix);
    if (type === 'announced' ? announced.has(key) : !announced.delete(key)) return;
    if (type === 'announced') announced.add(key);
    const event: NamespaceEvent = { type, namespace: [...prefix, ...suffix] };
    if (byEnd) withdrawnByEnd.add(event);
    sink.event(event);
  };

  removeClosed = deps.observeClosed((error) => finish(new SubscriptionConnectionClosedError(error)));
  if (!ended) {
    try {
      core = deps.subscribeNamespace(
        { trackNamespacePrefix: prefix },
        {
          onOk: ({ activeCount }) => {
            snapshot =
              activeCount === undefined
                ? {
                    kind: 'quiet',
                    quiet: setTimeout(load, SNAPSHOT_QUIET_MS),
                    limit: setTimeout(load, SNAPSHOT_LIMIT_MS),
                  }
                : { kind: 'counted' };
          },
          onSynced: load,
          onNamespace: (suffix) => {
            change('announced', suffix);
            heard();
          },
          onNamespaceDone: (suffix, { requestEnded }) => {
            if (requestEnded) return;
            change('unannounced', suffix);
            heard();
          },
          onError: (error) => fail(error, isStreamFailure(error) && error.errorCode !== REQUEST_ERROR_CODE.TIMEOUT),
          onEnd: ({ error }) => {
            stopTimers();
            // A finished stream is the relay ending the request on purpose; a failed one may be a lost connection.
            if (error === undefined) {
              for (const key of [...announced]) change('unannounced', JSON.parse(key), true);
              finish();
            } else {
              const reason = error instanceof Error ? error.message : 'namespace request stream failed';
              fail({ errorCode: REQUEST_ERROR_CODE.INTERNAL_ERROR, retryInterval: 0, reason }, true);
            }
          },
        },
      );
    } catch (error) {
      // Encoding can refuse the request (a field too long for the wire); leave nothing observing.
      ended = true;
      releasePrefix();
      removeClosed();
      throw error;
    }
    if (ended) core.cancel();
  }
  return { cancel: (reason) => finish(reason) };
}
