import {
  createMoqtPublishSession,
  createSubgroupWriter,
  type KeyValuePair,
  MAX_NAMESPACE_FIELDS,
  MAX_OBJECT_PAYLOAD_LENGTH,
  type ObjectStatus,
  type SubgroupWriter,
} from '@mux/moq-core';
import { assertTimeoutMs, type ConnectRelayOptions, openRelaySession } from './connect-session.js';
import { encodeJSON, encodeText } from './payload.js';

export interface ConnectRelayPublisherOptions extends ConnectRelayOptions {
  /** One namespace per publisher connection. Register any number of tracks within it. */
  namespace: readonly string[];
}

export interface PublishProperty {
  /** Non-negative safe integer: even types encode an integer, odd types encode bytes. */
  type: number;
  value: number | ArrayBufferView;
}

export interface PublishTrackOptions {
  track: string;
  /**
   * Track metadata sent to the relay. The SDK adds no default timescale; the relay may
   * add, drop, or rewrite properties before delivery to subscribers. See README.
   */
  trackProperties?: readonly PublishProperty[];
  /** Deadline for relay demand and stream writes. Default 10 seconds, maximum 2147483647. */
  writeTimeoutMs?: number;
}

export interface RelayGroupWriter {
  readonly groupId: number;
  readonly objectCount: number;
  readonly closed: boolean;
  /**
   * Append one object, starting at ID 0. Each write has its own writeTimeoutMs deadline.
   * Properties are encoded as supplied; the relay may transform them before delivery.
   */
  write(
    payload: ArrayBufferView,
    options?: { properties?: readonly PublishProperty[]; status?: ObjectStatus },
  ): Promise<void>;
  /** FIN the group within writeTimeoutMs. Idempotent after successful completion. */
  close(): Promise<void>;
  /** Reset this group and free the track for the next random-access point. */
  abort(reason?: unknown): void;
  /** Encode UTF-8 text and write one object with the same options and semantics as write(). */
  writeText(text: string, options?: { properties?: readonly PublishProperty[]; status?: ObjectStatus }): Promise<void>;
  /** Serialize with JSON.stringify and write one object. Serialization errors leave the writer usable. */
  writeJSON(
    value: unknown,
    options?: { properties?: readonly PublishProperty[]; status?: ObjectStatus },
  ): Promise<void>;
}

export interface RelayTrackWriter {
  readonly bound: boolean;
  readonly closed: boolean;
  /** Current demand first, then each change/replacement; completes when the track ends. */
  bindings(): AsyncIterableIterator<boolean>;
  /** Hold the track's single write slot until the group closes or aborts. */
  openGroup(options?: { signal?: AbortSignal }): Promise<RelayGroupWriter>;
  /**
   * Write one independent object/group. Waits for relay demand. Resolves when the
   * transport accepts the complete group, not when a downstream viewer acknowledges it.
   * Only one write may be pending; await it before writing again. Any ArrayBufferView is
   * copied as its raw bytes; anything else is a TypeError that leaves the track usable.
   * Properties are encoded as supplied; the relay may transform them before delivery.
   * Failure resets this group and leaves the track usable while the connection is open.
   * Data may already have reached the relay; choose the next object explicitly (no automatic retry).
   */
  write(
    payload: ArrayBufferView,
    options?: { properties?: readonly PublishProperty[]; signal?: AbortSignal },
  ): Promise<void>;
  /** End the track and cancel an outstanding write. The publisher connection stays open. */
  close(): void;
  /** Encode UTF-8 text and write one object with the same options and semantics as write(). */
  writeText(text: string, options?: { properties?: readonly PublishProperty[]; signal?: AbortSignal }): Promise<void>;
  /** Serialize with JSON.stringify and write one object. Serialization errors leave the writer usable. */
  writeJSON(value: unknown, options?: { properties?: readonly PublishProperty[]; signal?: AbortSignal }): Promise<void>;
}

export interface RelayPublisher {
  /** Register a data track without starting capture or publishing a media catalog. */
  publish(options: PublishTrackOptions): RelayTrackWriter;
  /** Ends when the connection shuts down. Errors are returned as data; this never rejects. */
  readonly closed: Promise<{ error?: unknown }>;
  /** End all tracks, retract the namespace, and wait for transport shutdown. Idempotent. */
  close(): Promise<void>;
}

function copyProperties(properties: readonly PublishProperty[] = []): KeyValuePair[] {
  return Array.from(properties, (property, index) => {
    const { type, value } = property ?? {};
    if (!Number.isSafeInteger(type) || type < 0)
      throw new TypeError(`Property ${index} type must be a non-negative safe integer`);
    if (type % 2 === 0) {
      if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0)
        throw new TypeError(`Property ${index} value must be a non-negative safe integer for an even type`);
      return { type, value };
    }
    if (!ArrayBuffer.isView(value))
      throw new TypeError(`Property ${index} value must be an ArrayBufferView for an odd type`);
    return { type, value: new Uint8Array(value.buffer, value.byteOffset, value.byteLength).slice() };
  });
}

/**
 * Open a dedicated Relay publisher connection. Subscribe clients use connectRelay;
 * this initial publishing API does not share their physical transport. A relay must
 * solicit the namespace and subscribe upstream before data can flow.
 */
export async function connectRelayPublisher(options: ConnectRelayPublisherOptions): Promise<RelayPublisher> {
  const namespace = [...options.namespace];
  if (namespace.length === 0 || namespace.some((part) => typeof part !== 'string' || part.length === 0)) {
    throw new TypeError('A publisher namespace must contain non-empty string fields');
  }
  if (namespace.length > MAX_NAMESPACE_FIELDS) {
    throw new RangeError(`A publisher namespace must contain at most ${MAX_NAMESPACE_FIELDS} fields`);
  }

  const tracks = new Map<string, { bind(alias: number | undefined): void; close(error?: unknown): void }>();
  const lifetime = await openRelaySession(options, (transport, callbacks) =>
    createMoqtPublishSession(transport, {
      implementationName: '@mux/relay publisher',
      callbacks: {
        ...callbacks,
        onTrackBinding: ({ trackName, trackAlias }) => tracks.get(trackName)?.bind(trackAlias),
        onAnnounceEnded: ({ error }) =>
          callbacks.onClosed?.({ error: error ?? new Error('Relay withdrew the namespace') }),
      },
    }),
  );
  const { session } = lifetime;
  let closing = false;

  session.announce(namespace);
  void lifetime.closed.then(({ error }) => {
    closing = true;
    for (const track of tracks.values()) track.close(error);
  });
  const closed = lifetime.transportClosed.then(
    () => lifetime.closed,
    () => lifetime.closed,
  );

  return {
    closed,
    publish({ track, trackProperties, writeTimeoutMs = 10_000 }) {
      lifetime.assertOpen();
      if (closing) throw new Error('Relay publisher is closed');
      if (typeof track !== 'string') throw new TypeError('Track name must be a string');
      if (track.length === 0) throw new TypeError('Track name must be non-empty');
      if (tracks.has(track)) throw new Error('Track name is already registered on this publisher connection');
      assertTimeoutMs('writeTimeoutMs', writeTimeoutMs);

      const registered = session.registerTrack({
        trackNamespace: namespace,
        trackName: track,
        trackProperties: copyProperties(trackProperties),
      });
      let alias: number | undefined;
      let ended = false;
      let nextGroupId = 0;
      interface Operation {
        kind: 'write' | 'group';
        alias: number | undefined;
        writer: SubgroupWriter | undefined;
        cancelled: boolean;
        failure: unknown;
        demand: Promise<void>;
        interrupted: Promise<never>;
        wake(): void;
        cancel(error: unknown): void;
        cleanup(): void;
      }
      let active: Operation | undefined;
      const watchers = new Set<(value: boolean | undefined) => void>();
      const notify = (value: boolean | undefined) => {
        for (const watcher of watchers) watcher(value);
      };
      const end = () => {
        if (ended) return;
        ended = true;
        registered.end();
        notify(undefined);
        watchers.clear();
      };
      const close = (error: unknown = new Error('Published track is closed')): void => {
        if (ended) return;
        active?.cancel(error);
        end();
      };
      const assertWritable = () => {
        lifetime.assertOpen();
        if (ended || closing) throw new Error('Published track is closed');
        if (active) throw new Error('A write or group is already pending; await it before writing another object');
      };
      const copyPayload = (payload: ArrayBufferView) => {
        if (!ArrayBuffer.isView(payload))
          throw new TypeError('Payload must be a Uint8Array or other ArrayBufferView; wrap an ArrayBuffer in one');
        if (payload.byteLength > MAX_OBJECT_PAYLOAD_LENGTH)
          throw new RangeError('Payload exceeds the 16 MiB object limit');
        return new Uint8Array(payload.buffer, payload.byteOffset, payload.byteLength).slice();
      };
      const begin = (kind: Operation['kind'], signal?: AbortSignal): Operation => {
        let wake!: () => void;
        let reject!: (error: unknown) => void;
        const demand = new Promise<void>((resolve) => {
          wake = resolve;
        });
        const interrupted = new Promise<never>((_, rejectPromise) => {
          reject = rejectPromise;
        });
        // An open group can be aborted between calls, when nobody is awaiting this promise.
        void interrupted.catch(() => {});
        const abort = () => operation.cancel(signal?.reason ?? new Error('Publish operation aborted'));
        const operation: Operation = {
          kind,
          alias: undefined,
          writer: undefined,
          cancelled: false,
          failure: undefined,
          demand,
          interrupted,
          wake,
          cleanup() {
            signal?.removeEventListener('abort', abort);
          },
          cancel(error) {
            if (operation.cancelled) return;
            operation.cancelled = true;
            operation.failure = error;
            operation.writer?.abort(error);
            if (active === operation) active = undefined;
            operation.cleanup();
            wake();
            reject(error);
          },
        };
        active = operation;
        signal?.addEventListener('abort', abort, { once: true });
        if (signal?.aborted) abort();
        return operation;
      };
      const check = (operation: Operation) => {
        if (operation.cancelled) throw operation.failure;
        lifetime.assertOpen();
      };
      const run = async <T>(operation: Operation, send: () => Promise<T>): Promise<T> => {
        const timer = setTimeout(
          () =>
            operation.cancel(
              new Error(
                operation.alias === undefined && session.namespaceSolicited === false
                  ? 'Publisher namespace is not covered by the relay namespace solicitations; check the token and namespace'
                  : 'Publish write timed out waiting for relay demand or transport',
              ),
            ),
          writeTimeoutMs,
        );
        try {
          check(operation);
          const result = await Promise.race([send(), operation.interrupted]);
          check(operation);
          return result;
        } catch (error) {
          operation.cancel(error);
          throw error;
        } finally {
          clearTimeout(timer);
        }
      };
      const openStream = async (operation: Operation) => {
        if (alias === undefined) {
          await operation.demand;
          check(operation);
          if (alias === undefined) throw new Error('Relay subscription ended before the write started');
        }
        check(operation);
        operation.alias = alias;
        const groupId = nextGroupId++;
        const stream = await session.openUniStream();
        if (operation.cancelled) {
          void stream.abort(operation.failure).catch(() => {});
          check(operation);
        }
        const writer = createSubgroupWriter(stream, { trackAlias: operation.alias, groupId, hasProperties: true });
        operation.writer = writer;
        return writer;
      };
      const release = (operation: Operation) => {
        if (active === operation) active = undefined;
        operation.cleanup();
      };
      tracks.set(track, {
        close,
        bind(nextAlias) {
          if (active?.alias !== undefined && active.alias !== nextAlias) {
            active.cancel(new Error('Relay subscription changed during a write or group; objects may be incomplete'));
          }
          const changed = alias !== nextAlias;
          alias = nextAlias;
          if (alias !== undefined) active?.wake();
          if (changed && !ended) notify(alias !== undefined);
        },
      });

      const trackWriter: RelayTrackWriter = {
        get bound() {
          return alias !== undefined && !ended;
        },
        get closed() {
          return ended;
        },
        bindings() {
          const queue: boolean[] = ended ? [] : [alias !== undefined];
          const waiting: ((result: IteratorResult<boolean>) => void)[] = [];
          let done = ended;
          const push = (value: boolean | undefined) => {
            if (done) return;
            if (value === undefined) {
              done = true;
              for (const resolve of waiting.splice(0)) resolve({ done: true, value: undefined });
              watchers.delete(push);
            } else {
              const resolve = waiting.shift();
              if (resolve) resolve({ done: false, value });
              else queue.push(value);
            }
          };
          if (!done) watchers.add(push);
          const iterator: AsyncIterableIterator<boolean> = {
            [Symbol.asyncIterator]() {
              return iterator;
            },
            next() {
              const value = queue.shift();
              if (value !== undefined) return Promise.resolve({ done: false, value });
              if (done) return Promise.resolve({ done: true, value: undefined });
              return new Promise((resolve) => {
                waiting.push(resolve);
              });
            },
            return() {
              queue.length = 0;
              push(undefined);
              return Promise.resolve({ done: true, value: undefined });
            },
          };
          return iterator;
        },
        close: () => close(),
        async writeText(text, writeOptions) {
          await trackWriter.write(encodeText(text), writeOptions);
        },
        async writeJSON(value, writeOptions) {
          await trackWriter.write(encodeJSON(value), writeOptions);
        },
        async write(payload, writeOptions = {}) {
          assertWritable();
          const bytes = copyPayload(payload);
          const properties = copyProperties(writeOptions.properties);
          const operation = begin('write', writeOptions.signal);
          try {
            await run(operation, async () => {
              const writer = await openStream(operation);
              await writer.writeObject({ objectId: 0, properties, payload: bytes });
              check(operation);
              await writer.fin();
            });
          } finally {
            release(operation);
          }
        },
        async openGroup(groupOptions = {}) {
          assertWritable();
          const operation = begin('group', groupOptions.signal);
          const writer = await run(operation, () => openStream(operation));
          let objectCount = 0;
          let pending = false;
          let complete = false;
          const assertGroup = () => {
            check(operation);
            if (complete) throw new Error('Group is closed');
            if (pending) throw new Error('A group write or close is already pending');
          };
          const groupWriter: RelayGroupWriter = {
            groupId: writer.groupId,
            get objectCount() {
              return objectCount;
            },
            get closed() {
              return complete || operation.cancelled;
            },
            async writeText(text, writeOptions) {
              await groupWriter.write(encodeText(text), writeOptions);
            },
            async writeJSON(value, writeOptions) {
              await groupWriter.write(encodeJSON(value), writeOptions);
            },
            async write(payload, writeOptions = {}) {
              assertGroup();
              const bytes = copyPayload(payload);
              const properties = copyProperties(writeOptions.properties);
              const status = writeOptions.status ?? 'normal';
              if (!['normal', 'end-of-group', 'end-of-track'].includes(status))
                throw new TypeError('Unknown object status');
              pending = true;
              try {
                await run(operation, () =>
                  writer.writeObject({ objectId: objectCount, properties, payload: bytes, status }),
                );
                objectCount++;
              } finally {
                pending = false;
              }
            },
            async close() {
              if (complete) return;
              assertGroup();
              pending = true;
              try {
                await run(operation, () => writer.fin());
                complete = true;
                release(operation);
              } finally {
                pending = false;
              }
            },
            abort(reason = new Error('Group was aborted')) {
              if (!complete) operation.cancel(reason);
            },
          };
          return groupWriter;
        },
      };
      return trackWriter;
    },
    async close() {
      closing = true;
      for (const track of tracks.values()) track.close();
      lifetime.close();
      await closed;
    },
  };
}
