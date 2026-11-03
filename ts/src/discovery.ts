/**
 * One connection's namespace discovery, shared by every want on it. Draft-22 §9.15 allows one SUBSCRIBE_NAMESPACE per
 * prefix tree on a session, so the index keeps open requests whose prefixes never overlap: a watch under an open
 * request is served by filtering it, and a watch over open requests replaces them with one request on its own prefix.
 * A replaced request's view is held until the new one has loaded, so a watch it moved hears only the differences.
 */
import { isMoqtProtocolError } from '@caprid/moq-core';
import { RelayError } from './errors.js';
import type { NamespaceEvent, NamespaceEvents } from './namespace-events.js';
import {
  SubscriptionConnectionClosedError,
  SubscriptionOverflowError,
  SubscriptionRequestError,
} from './subscription.js';
import { isPermanentRequestError } from './supervision.js';

/**
 * The queue bound each request is opened with. The index reads every event as it arrives, so the queue only ever holds
 * one stream read's burst, and any finite bound would only move where a large snapshot overflows.
 */
export const DISCOVERY_CAPACITY = Number.MAX_SAFE_INTEGER;
/** How long a request stays open after its last watch is released, so a want that resubscribes at once reuses it. */
export const DISCOVERY_LINGER_MS = 5_000;

/** What a watch's owner hears, synchronously and in the order its request saw the events. */
export interface DiscoveryWatchHandlers {
  /** A namespace under the watch's prefix was announced. */
  onAnnounced?(namespace: string[]): void;
  /** A namespace under the watch's prefix was withdrawn. */
  onUnannounced?(namespace: string[]): void;
  /** Everything announced when the serving request was accepted has been reported. At most once per watch. */
  onLoaded?(): void;
  /** The serving request ended; nothing follows. `error` is undefined for the relay's clean end. */
  onEnded?(error: unknown): void;
}

/** One owner's view of a prefix on one connection. */
export interface DiscoveryWatch {
  /**
   * Whether the watch's view is complete. Stays true when a covering request replaces the one that served it: the
   * owner then hears only the differences.
   */
  readonly loaded: boolean;
  /** How the serving request ended, once it has. */
  readonly ended: { error: unknown } | undefined;
  /** The namespaces under the prefix as this connection sees them now, to seed the owner's own state. */
  namespaces(): string[][];
  /** Stop hearing about the prefix. Idempotent. */
  release(): void;
}

export interface DiscoveryIndex {
  /**
   * Watch `prefix`, whose fields the caller has admitted. Throws what opening a request throws, and CLOSED once the
   * index is closed. No handler runs inside this call.
   */
  watch(prefix: readonly string[], handlers: DiscoveryWatchHandlers): DiscoveryWatch;
  /** The connection ended: every watch ends with `SubscriptionConnectionClosedError`. Idempotent. */
  close(): void;
}

/** One open SUBSCRIBE_NAMESPACE. */
interface DiscoveryRequest {
  readonly prefix: readonly string[];
  /** The prefix's JSON: a field may hold any character, so a separator could collide. */
  readonly key: string;
  readonly events: NamespaceEvents;
  loaded: boolean;
  /** What this request announced, by the namespace's JSON. */
  readonly announced: Map<string, string[]>;
  /**
   * What the requests it replaced showed and it has not confirmed yet. Its view is `announced` plus `held`; at its
   * `loaded`, whatever is still held is withdrawn.
   */
  readonly held: Map<string, string[]>;
  /** Watches by their prefix's JSON, so a namespace reaches its watches with one lookup per field. */
  readonly watches: Map<string, Set<WatchRecord>>;
  linger: ReturnType<typeof setTimeout> | undefined;
}

type WatchState =
  | { kind: 'served'; request: DiscoveryRequest }
  | { kind: 'ended'; error: unknown }
  | { kind: 'released' };

interface WatchRecord {
  readonly prefix: readonly string[];
  readonly key: string;
  readonly handlers: DiscoveryWatchHandlers;
  state: WatchState;
  /** Kept across a move to a covering request. */
  loaded: boolean;
}

/** Whether `prefix` covers `namespace` field by field: equal, or a leading part of it. */
export function covers(prefix: readonly string[], namespace: readonly string[]): boolean {
  return prefix.length <= namespace.length && prefix.every((field, i) => field === namespace[i]);
}

/**
 * Where a watch on `prefix` goes among requests whose prefixes never overlap: under the one request that covers it, or
 * into a new request that replaces every request it covers (none for a disjoint prefix). Both cannot hold, since a
 * request under `prefix` and one over it would overlap each other.
 */
export function placeWatch<R extends { readonly prefix: readonly string[] }>(
  open: Iterable<R>,
  prefix: readonly string[],
): { kind: 'covered'; by: R } | { kind: 'open'; replacing: R[] } {
  const replacing: R[] = [];
  for (const request of open) {
    if (covers(request.prefix, prefix)) return { kind: 'covered', by: request };
    if (covers(prefix, request.prefix)) replacing.push(request);
  }
  return { kind: 'open', replacing };
}

/**
 * What a discovery ending means for its owner: the connection's end waits for the next connection; the relay finishing
 * the request, a REQUEST_ERROR a wait may fix, or a queue overflow is retried after a backoff; anything else, such as a
 * refusal of the credentials, is surfaced.
 */
export function discoveryEnding(error: unknown): 'connection' | 'retry' | 'fatal' {
  if (error instanceof SubscriptionConnectionClosedError) return 'connection';
  if (error === undefined || error instanceof SubscriptionOverflowError) return 'retry';
  if (error instanceof SubscriptionRequestError && !isPermanentRequestError(error.requestError)) return 'retry';
  return 'fatal';
}

/**
 * Whether a request refused at the call, a subscribe or a discovery watch, is the caller's own fault, which no
 * connection fixes. Anything else thrown there (CLOSED, the connection's TRANSPORT_ERROR or GOAWAY) is the connection
 * ending before the supervisor has heard of it.
 */
export function refusedForGood(error: unknown): boolean {
  // The core's encoders refuse a request the wire cannot carry (a field too long to encode) the same way every time.
  return error instanceof TypeError || error instanceof RangeError || isMoqtProtocolError(error);
}

/** Open the index for one connection; `open` starts one namespace request. */
export function createDiscoveryIndex(open: (prefix: string[]) => NamespaceEvents): DiscoveryIndex {
  const requests = new Map<string, DiscoveryRequest>();
  let closed = false;

  const records = (request: DiscoveryRequest): WatchRecord[] =>
    [...request.watches.values()].flatMap((set) => [...set]);
  const serves = (request: DiscoveryRequest, record: WatchRecord) =>
    record.state.kind === 'served' && record.state.request === request;

  const attach = (request: DiscoveryRequest, record: WatchRecord) => {
    record.state = { kind: 'served', request };
    const set = request.watches.get(record.key);
    if (set) set.add(record);
    else request.watches.set(record.key, new Set([record]));
  };

  const call = (record: WatchRecord, run: (handlers: DiscoveryWatchHandlers) => void) => {
    try {
      run(record.handlers);
    } catch (error) {
      // biome-ignore lint/suspicious/noConsole: the owner's handler failed and has no caller to report to
      console.error(new Error('A discovery handler threw; discovery carries on', { cause: error }));
    }
  };

  const notify = (request: DiscoveryRequest, namespace: string[], type: 'announced' | 'unannounced') => {
    for (let length = request.prefix.length; length <= namespace.length; length++) {
      const set = request.watches.get(JSON.stringify(namespace.slice(0, length)));
      // A handler may release a watch or open another: each one is checked as it comes up.
      for (const record of [...(set ?? [])]) {
        if (!serves(request, record)) continue;
        call(record, (handlers) =>
          type === 'announced' ? handlers.onAnnounced?.([...namespace]) : handlers.onUnannounced?.([...namespace]),
        );
      }
    }
  };

  /** Tell the owner its watch ended, unless it released the watch first. */
  const announceEnd = (record: WatchRecord) => {
    const { state } = record;
    if (state.kind === 'ended') call(record, (handlers) => handlers.onEnded?.(state.error));
  };
  const end = (ending: WatchRecord[], error: unknown) => {
    for (const record of ending) record.state = { kind: 'ended', error };
    for (const record of ending) announceEnd(record);
  };

  const dispatch = (request: DiscoveryRequest, event: NamespaceEvent) => {
    if (event.type === 'loaded') {
      if (request.loaded) return;
      request.loaded = true;
      const gone = [...request.held.values()];
      request.held.clear();
      // Withdrawn while the swap could not see them.
      for (const namespace of gone) notify(request, namespace, 'unannounced');
      for (const record of records(request)) {
        if (!serves(request, record) || record.loaded) continue;
        record.loaded = true;
        call(record, (handlers) => handlers.onLoaded?.());
      }
      return;
    }
    const key = JSON.stringify(event.namespace);
    if (event.type === 'announced') {
      // Every view already shows a held namespace: confirming it is not news.
      if (request.held.delete(key)) {
        request.announced.set(key, event.namespace);
        return;
      }
      if (request.announced.has(key)) return;
      request.announced.set(key, event.namespace);
      notify(request, event.namespace, 'announced');
      return;
    }
    if (request.announced.delete(key)) notify(request, event.namespace, 'unannounced');
  };

  const consume = async (request: DiscoveryRequest) => {
    let error: unknown;
    try {
      for await (const event of request.events) {
        // Replaced or closed: its end is not news to the watches, which moved or ended already.
        if (requests.get(request.key) !== request) return;
        dispatch(request, event);
      }
    } catch (thrown) {
      error = thrown;
    }
    if (requests.get(request.key) !== request) return;
    requests.delete(request.key);
    clearTimeout(request.linger);
    end(records(request), error);
  };

  const release = (record: WatchRecord) => {
    const { state } = record;
    record.state = { kind: 'released' };
    if (state.kind !== 'served') return;
    const { request } = state;
    const set = request.watches.get(record.key);
    set?.delete(record);
    if (set?.size === 0) request.watches.delete(record.key);
    if (request.watches.size > 0 || requests.get(request.key) !== request) return;
    // Never narrowed while a watch uses it; closed only once nothing has watched it for a while.
    request.linger = setTimeout(() => {
      if (request.watches.size > 0 || requests.get(request.key) !== request) return;
      requests.delete(request.key);
      request.events.close();
    }, DISCOVERY_LINGER_MS);
  };

  const handle = (record: WatchRecord): DiscoveryWatch => ({
    get loaded() {
      return record.loaded;
    },
    get ended() {
      return record.state.kind === 'ended' ? { error: record.state.error } : undefined;
    },
    namespaces() {
      if (record.state.kind !== 'served') return [];
      const { request } = record.state;
      return [...request.announced.values(), ...request.held.values()]
        .filter((namespace) => covers(record.prefix, namespace))
        .map((namespace) => [...namespace]);
    },
    release: () => release(record),
  });

  return {
    watch(prefix, handlers) {
      if (closed) throw new RelayError('CLOSED', 'Relay connection is closed', 'connection');
      const record: WatchRecord = {
        prefix: [...prefix],
        key: JSON.stringify(prefix),
        handlers,
        state: { kind: 'released' },
        loaded: false,
      };
      const placed = placeWatch(requests.values(), prefix);
      if (placed.kind === 'covered') {
        const request = placed.by;
        clearTimeout(request.linger);
        request.linger = undefined;
        record.loaded = request.loaded;
        attach(request, record);
        return handle(record);
      }
      // Break before make: §9.15 forbids the overlap, and the namespace request refuses it before sending.
      const held = new Map<string, string[]>();
      const moved: WatchRecord[] = [];
      for (const replaced of placed.replacing) {
        requests.delete(replaced.key);
        clearTimeout(replaced.linger);
        for (const [key, namespace] of [...replaced.announced, ...replaced.held]) held.set(key, namespace);
        moved.push(...records(replaced));
        replaced.events.close();
      }
      let events: NamespaceEvents;
      try {
        events = open([...prefix]);
      } catch (error) {
        // Only the caller's own prefix can be refused for its arguments: a moved watch lost its request to the call.
        const ending =
          error instanceof SubscriptionConnectionClosedError ? error : new SubscriptionConnectionClosedError(error);
        for (const record of moved) record.state = { kind: 'ended', error: ending };
        queueMicrotask(() => {
          for (const record of moved) announceEnd(record);
        });
        throw error;
      }
      const request: DiscoveryRequest = {
        prefix: [...prefix],
        key: record.key,
        events,
        loaded: false,
        announced: new Map(),
        held,
        watches: new Map(),
        linger: undefined,
      };
      for (const movedRecord of moved) attach(request, movedRecord);
      attach(request, record);
      requests.set(request.key, request);
      void consume(request);
      return handle(record);
    },
    close() {
      if (closed) return;
      closed = true;
      const ending = [...requests.values()];
      requests.clear();
      for (const request of ending) {
        clearTimeout(request.linger);
        request.events.close();
      }
      end(
        ending.flatMap((request) => records(request)),
        new SubscriptionConnectionClosedError(),
      );
    },
  };
}
