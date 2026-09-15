import {
  createMoqtPublishSession,
  createSubgroupWriter,
  type KeyValuePair,
  MAX_OBJECT_PAYLOAD_LENGTH,
  type SubgroupWriter,
} from '@mux/moq-core';
import { assertTimeoutMs, type ConnectRelayOptions, openRelaySession } from './connect-session.js';

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
  /** Explicit track metadata; raw data has no implied timestamp unit. */
  trackProperties?: readonly PublishProperty[];
  /** Deadline for relay demand and stream writes. Default 10 seconds, maximum 2147483647. */
  writeTimeoutMs?: number;
}

export interface RelayTrackWriter {
  /**
   * Write one independent object/group. Waits for relay demand. Resolves when the
   * transport accepts the complete group, not when a downstream viewer acknowledges it.
   * Only one write may be pending; await it before writing again. Any ArrayBufferView is
   * copied as its raw bytes; anything else is a TypeError that leaves the track usable.
   * A failed bound write ends this track; a timeout waiting for demand permits another write.
   */
  write(payload: ArrayBufferView, options?: { properties?: readonly PublishProperty[] }): Promise<void>;
  /** End the track and cancel an outstanding write. The publisher connection stays open. */
  close(): void;
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
  return properties.map((property, index) => {
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
      if (typeof track !== 'string' || track.length === 0) throw new TypeError('Track name must be non-empty');
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
      let active: { alias?: number | undefined; wake(): void; cancel(error: unknown): void } | undefined;

      const close = (error: unknown = new Error('Published track is closed')): void => {
        if (ended) return;
        ended = true;
        active?.cancel(error);
        registered.end();
      };
      tracks.set(track, {
        close,
        bind(nextAlias) {
          if (active?.alias !== undefined && active.alias !== nextAlias) {
            active.cancel(new Error('Relay subscription changed during a write; the object may be incomplete'));
          }
          alias = nextAlias;
          if (alias !== undefined) active?.wake();
        },
      });

      return {
        close: () => close(),
        async write(payload, writeOptions = {}) {
          lifetime.assertOpen();
          if (ended || closing) throw new Error('Published track is closed');
          if (active) throw new Error('A write is already pending; await it before writing another object');
          // Check the payload before the write slot and alias are taken, so a caller's type error
          // stays local and leaves the track usable instead of retiring it.
          if (!ArrayBuffer.isView(payload))
            throw new TypeError('Payload must be a Uint8Array or other ArrayBufferView; wrap an ArrayBuffer in one');
          if (payload.byteLength > MAX_OBJECT_PAYLOAD_LENGTH)
            throw new RangeError('Payload exceeds the 16 MiB object limit');

          // Any view is published as the bytes it spans, which is what Python's bytes-like payloads do.
          const bytes = new Uint8Array(payload.buffer, payload.byteOffset, payload.byteLength).slice();
          const properties = copyProperties(writeOptions.properties);
          let writer: SubgroupWriter | undefined;
          let cancelled = false;
          let failure: unknown;
          let rejectWrite!: (error: unknown) => void;
          let wake!: () => void;
          const interrupted = new Promise<never>((_, reject) => {
            rejectWrite = reject;
          });
          const demand = new Promise<void>((resolve) => {
            wake = resolve;
          });
          const operation = {
            alias: undefined as number | undefined,
            wake,
            cancel(error: unknown) {
              if (cancelled) return;
              cancelled = true;
              failure = error;
              writer?.abort(error);
              // A failed bound write may leave an incomplete group. Retire this track
              // so retries cannot build up abandoned streams behind a stalled transport.
              if (operation.alias !== undefined) {
                ended = true;
                registered.end();
              }
              // Release an outstanding wait for relay demand as well as the public promise.
              wake();
              rejectWrite(error);
            },
          };
          active = operation;
          const timer = setTimeout(
            () => operation.cancel(new Error('Publish write timed out waiting for relay demand or transport')),
            writeTimeoutMs,
          );

          const check = () => {
            if (cancelled) throw failure;
            lifetime.assertOpen();
          };
          const send = async () => {
            // Demand can disappear between its notification and this continuation.
            while (alias === undefined) {
              await demand;
              check();
              if (alias === undefined) throw new Error('Relay subscription ended before the write started');
            }
            check();
            operation.alias = alias;
            const groupId = nextGroupId++;
            const stream = await session.openUniStream();

            if (cancelled) {
              void stream.abort(failure).catch(() => {});
              check();
            }
            writer = createSubgroupWriter(stream, { trackAlias: operation.alias, groupId, hasProperties: true });
            await writer.writeObject({ objectId: 0, properties, payload: bytes });
            check();
            await writer.fin();
            check();
          };

          try {
            await Promise.race([send(), interrupted]);
          } catch (error) {
            operation.cancel(error);
            throw error;
          } finally {
            clearTimeout(timer);
            if (active === operation) active = undefined;
          }
        },
      };
    },
    async close() {
      closing = true;
      for (const track of tracks.values()) track.close();
      lifetime.close();
      await closed;
    },
  };
}
