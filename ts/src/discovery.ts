/**
 * Draft-22 §9.15 allows one SUBSCRIBE_NAMESPACE per prefix tree on a session, so the index keeps open requests whose
 * prefixes never overlap.
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

export const DISCOVERY_CAPACITY = Number.MAX_SAFE_INTEGER;
export const DISCOVERY_LINGER_MS = 5_000;

export interface DiscoveryWatchHandlers {
  onAnnounced?(namespace: string[]): void;
  onUnannounced?(namespace: string[]): void;
  onLoaded?(): void;
  onEnded?(error: unknown): void;
}

export interface DiscoveryWatch {
  readonly loaded: boolean;
  readonly ended: { error: unknown } | undefined;
  namespaces(): string[][];
  release(): void;
}

export interface DiscoveryIndex {
  watch(prefix: readonly string[], handlers: DiscoveryWatchHandlers): DiscoveryWatch;
  close(): void;
}

interface DiscoveryRequest {
  readonly prefix: readonly string[];
  readonly key: string;
  readonly events: NamespaceEvents;
  loaded: boolean;
  readonly announced: Map<string, string[]>;
  readonly held: Map<string, string[]>;
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
  loaded: boolean;
}

export function covers(prefix: readonly string[], namespace: readonly string[]): boolean {
  return prefix.length <= namespace.length && prefix.every((field, i) => field === namespace[i]);
}

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

export function discoveryEnding(error: unknown): 'connection' | 'retry' | 'fatal' {
  if (error instanceof SubscriptionConnectionClosedError) return 'connection';
  if (error === undefined || error instanceof SubscriptionOverflowError) return 'retry';
  if (error instanceof SubscriptionRequestError && !isPermanentRequestError(error.requestError)) return 'retry';
  return 'fatal';
}

export function refusedForGood(error: unknown): boolean {
  return error instanceof TypeError || error instanceof RangeError || isMoqtProtocolError(error);
}

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
      for (const record of [...(set ?? [])]) {
        if (!serves(request, record)) continue;
        call(record, (handlers) =>
          type === 'announced' ? handlers.onAnnounced?.([...namespace]) : handlers.onUnannounced?.([...namespace]),
        );
      }
    }
  };

  const announceEnd = (record: WatchRecord) => {
    const { state } = record;
    if (state.kind === 'ended') call(record, (handlers) => handlers.onEnded?.(state.error));
  };
  const lostTo = (error: unknown) =>
    error instanceof SubscriptionConnectionClosedError ? error : new SubscriptionConnectionClosedError(error);
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

  const place = (record: WatchRecord, ended?: DiscoveryRequest) => {
    const placed = placeWatch(requests.values(), record.prefix);
    if (placed.kind === 'covered') {
      const request = placed.by;
      clearTimeout(request.linger);
      request.linger = undefined;
      record.loaded ||= request.loaded;
      attach(request, record);
      return;
    }
    // Break before make: §9.15 forbids the overlap, and the namespace request refuses it before sending.
    const held = new Map<string, string[]>();
    const moved: WatchRecord[] = [];
    for (const replaced of placed.replacing) {
      requests.delete(replaced.key);
      clearTimeout(replaced.linger);
      moved.push(...records(replaced));
      replaced.events.close();
    }
    for (const shown of [...placed.replacing, ...(ended ? [ended] : [])]) {
      for (const [key, namespace] of [...shown.announced, ...shown.held]) {
        if (covers(record.prefix, namespace)) held.set(key, namespace);
      }
    }
    let events: NamespaceEvents;
    try {
      events = open([...record.prefix]);
    } catch (error) {
      for (const movedRecord of moved) movedRecord.state = { kind: 'ended', error: lostTo(error) };
      queueMicrotask(() => {
        for (const movedRecord of moved) announceEnd(movedRecord);
      });
      throw error;
    }
    const request: DiscoveryRequest = {
      prefix: [...record.prefix],
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
  };

  const consume = async (request: DiscoveryRequest) => {
    let error: unknown;
    try {
      for await (const event of request.events) {
        if (requests.get(request.key) !== request) return;
        dispatch(request, event);
      }
    } catch (thrown) {
      error = thrown;
    }
    if (requests.get(request.key) !== request) return;
    requests.delete(request.key);
    clearTimeout(request.linger);
    if (error instanceof SubscriptionConnectionClosedError) {
      end(records(request), error);
      return;
    }
    const rehomed = records(request)
      .filter((record) => record.key !== request.key)
      .sort((a, b) => a.prefix.length - b.prefix.length);
    for (const record of rehomed) {
      try {
        place(record, request);
      } catch (thrown) {
        record.state = { kind: 'ended', error: lostTo(thrown) };
      }
    }
    end([...(request.watches.get(request.key) ?? [])], error);
    for (const record of rehomed) announceEnd(record);
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
      place(record);
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
