/**
 * Scripted connections for the supervisor tests: each fake stands in for one `connectRelayPublisher` or
 * `connectRelaySubscriber` result, and the test drives what the relay would do to it.
 */
import type { MoqtObject, RequestError, SubscribeOk } from '../../packages/moq-core/src/index.js';
import type { RelaySubscriber, RelaySubscriptionHandlers, RelayTrackOptions } from '../../src/connect.js';
import { RelayError } from '../../src/errors.js';
import type { NamespaceEvent } from '../../src/namespace-events.js';
import type { RegisterTrackOptions, RelayPublisher, RelayTrackWriter } from '../../src/publish.js';
import {
  type RelaySubscription,
  SubscriptionConnectionClosedError,
  type SubscriptionEnd,
} from '../../src/subscription.js';

export function deferred<T = void>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

/** Poll until `predicate` holds, failing after a second. */
export async function until(predicate: () => boolean, label = 'condition'): Promise<void> {
  const deadline = Date.now() + 1_000;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${label}`);
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
}

export interface FakeTrack {
  options: RegisterTrackOptions;
  writer: RelayTrackWriter;
  writes: Uint8Array[];
  /** Make the next write fail with this error instead of succeeding. */
  failNext?: unknown;
  setDemand(value: boolean): void;
  closed: boolean;
}

export interface FakePublisher extends RelayPublisher {
  namespace: string[];
  tracks: Map<string, FakeTrack>;
  /** The connection ends, with `error` or cleanly. */
  end(error?: unknown): void;
  closeCalls: number;
}

export function fakePublisher(namespace: string[]): FakePublisher {
  const ended = deferred<{ error?: unknown }>();
  let isEnded = false;
  const tracks = new Map<string, FakeTrack>();
  const publisher: FakePublisher = {
    namespace,
    tracks,
    closeCalls: 0,
    announced: Promise.resolve(),
    closed: ended.promise,
    end(error) {
      if (isEnded) return;
      isEnded = true;
      for (const track of tracks.values()) track.closed = true;
      ended.resolve(error === undefined ? {} : { error });
    },
    async close() {
      publisher.closeCalls += 1;
      publisher.end();
    },
    registerTrack(options) {
      if (isEnded) throw new RelayError('CLOSED', 'Relay publisher is closed', 'connection');
      if (tracks.has(options.track)) throw new RangeError('Track name is already registered');
      const listeners = new Set<(value: boolean | undefined) => void>();
      let demand = false;
      const track: FakeTrack = {
        options,
        writes: [],
        closed: false,
        setDemand(value) {
          demand = value;
          for (const listener of listeners) listener(value);
        },
        writer: {
          get hasDemand() {
            return demand && !track.closed;
          },
          get closed() {
            return track.closed;
          },
          demandChanges() {
            const queue: boolean[] = [demand];
            let wake: (() => void) | undefined;
            let done = false;
            const listener = (value: boolean | undefined) => {
              if (value === undefined) done = true;
              else queue.push(value);
              wake?.();
            };
            listeners.add(listener);
            void ended.promise.then(() => listener(undefined));
            const iterator: AsyncIterableIterator<boolean> = {
              async next() {
                while (queue.length === 0 && !done) await new Promise<void>((resolve) => (wake = resolve));
                const value = queue.shift();
                if (value === undefined) return { done: true, value: undefined };
                return { done: false, value };
              },
              async return() {
                done = true;
                listeners.delete(listener);
                return { done: true, value: undefined };
              },
              [Symbol.asyncIterator]: () => iterator,
            };
            return iterator;
          },
          async write(payload) {
            if (track.closed) throw new RelayError('CLOSED', 'Published track is closed', 'waiting-for-demand');
            if (track.failNext !== undefined) {
              const error = track.failNext;
              track.failNext = undefined;
              throw error;
            }
            track.writes.push(new Uint8Array(payload.buffer, payload.byteOffset, payload.byteLength).slice());
          },
          async writeText(text) {
            await track.writer.write(new TextEncoder().encode(text));
          },
          async writeJSON(value) {
            await track.writer.writeText(JSON.stringify(value));
          },
          async openGroup() {
            throw new Error('not used');
          },
          close() {
            track.closed = true;
            for (const listener of listeners) listener(undefined);
          },
        },
      };
      tracks.set(options.track, track);
      return track.writer;
    },
  };
  return publisher;
}

export interface FakeSubscription {
  options: RelayTrackOptions;
  handlers: RelaySubscriptionHandlers;
  cancelled: boolean;
  accept(info?: Partial<SubscribeOk>): void;
  reject(error: RequestError): void;
  object(groupId: number, objectId?: number): void;
  done(): void;
  /** The relay ends the accepted subscription with REQUEST_ERROR. */
  terminate(error: RequestError): void;
}

export interface FakeSubscriber extends RelaySubscriber {
  subscriptions: FakeSubscription[];
  discoveries: { prefix: string[]; push(event: NamespaceEvent): void; fail(error: unknown): void; closed: boolean }[];
  end(error?: unknown): void;
}

export function fakeSubscriber(): FakeSubscriber {
  const ended = deferred<{ error?: unknown }>();
  let isEnded = false;
  const closers = new Set<(error?: unknown) => void>();
  const fake: FakeSubscriber = {
    subscriptions: [],
    discoveries: [],
    closed: ended.promise,
    trackSource: undefined as never,
    end(error) {
      if (isEnded) return;
      isEnded = true;
      for (const close of [...closers]) close(error);
      ended.resolve(error === undefined ? {} : { error });
    },
    close() {
      fake.end();
    },
    subscribe(options, handlers = {}) {
      if (isEnded) throw new RelayError('CLOSED', 'Relay connection is closed', 'connection');
      const subscribed = deferred<SubscribeOk>();
      const endedSub = deferred<SubscriptionEnd>();
      void subscribed.promise.catch(() => {});
      let accepted = false;
      let over = false;
      const finish = (end: SubscriptionEnd) => {
        if (over) return;
        over = true;
        closers.delete(onClose);
        endedSub.resolve(end);
      };
      const onClose = (error?: unknown) => {
        if (!accepted) subscribed.reject(new RelayError('CLOSED', 'closed', 'connection'));
        finish({ kind: 'terminated', cause: 'connection-closed', accepted, error });
      };
      closers.add(onClose);
      const record: FakeSubscription = {
        options,
        handlers,
        cancelled: false,
        accept(info = {}) {
          accepted = true;
          const ok = { trackAlias: 1, parameters: {}, trackProperties: [], ...info } as SubscribeOk;
          subscribed.resolve(ok);
          handlers.onSubscribed?.(ok);
        },
        reject(error) {
          handlers.onError?.(error);
          subscribed.reject(Object.assign(new Error(error.reason), { requestError: error }));
          finish({ kind: 'rejected', cause: 'request-error', accepted: false, error });
        },
        object(groupId, objectId = 0) {
          if (over) return;
          handlers.onObject?.({
            groupId,
            objectId,
            status: 'normal',
            payload: new Uint8Array([groupId]),
          } as MoqtObject);
        },
        done() {
          handlers.onPublisherDone?.({ statusCode: 2, streamCount: 0, reason: 'track ended' } as never);
        },
        terminate(error) {
          finish({ kind: 'terminated', cause: 'request-error', accepted: true, error });
        },
      };
      fake.subscriptions.push(record);
      const subscription = {
        subscribed: subscribed.promise,
        ended: endedSub.promise,
        cancel(reason?: unknown) {
          record.cancelled = true;
          finish({ kind: 'cancelled', cause: 'cancelled', accepted, error: reason });
        },
        update() {},
      } as unknown as RelaySubscription;
      return subscription;
    },
    subscribeEvents() {
      throw new Error('not used');
    },
    namespaceEvents({ prefix }) {
      if (isEnded) throw new RelayError('CLOSED', 'Relay connection is closed', 'connection');
      const queue: NamespaceEvent[] = [];
      let wake: (() => void) | undefined;
      let failure: { error: unknown } | undefined;
      let done = false;
      const discovery = {
        prefix: [...prefix],
        closed: false,
        push(event: NamespaceEvent) {
          queue.push(event);
          wake?.();
        },
        fail(error: unknown) {
          failure = { error };
          wake?.();
        },
      };
      const onClose = (error?: unknown) => discovery.fail(new SubscriptionConnectionClosedError(error));
      closers.add(onClose);
      fake.discoveries.push(discovery);
      const iterator = {
        async next(): Promise<IteratorResult<NamespaceEvent>> {
          while (queue.length === 0 && !failure && !done) await new Promise<void>((resolve) => (wake = resolve));
          if (done) return { done: true, value: undefined };
          const event = queue.shift();
          if (event) return { done: false, value: event };
          closers.delete(onClose);
          throw failure?.error;
        },
        async return(): Promise<IteratorResult<NamespaceEvent>> {
          done = true;
          discovery.closed = true;
          closers.delete(onClose);
          wake?.();
          return { done: true, value: undefined };
        },
        close() {
          void iterator.return();
        },
        [Symbol.asyncIterator]: () => iterator,
      };
      return iterator;
    },
  };
  return fake;
}
