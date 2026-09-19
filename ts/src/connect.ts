import {
  createMoqtSession,
  MAX_NAMESPACE_FIELDS,
  type MessageParameters,
  type MoqTrackSource,
  type MoqtSession,
  type Subscription,
  type SubscriptionHandlers,
} from '@mux/moq-core';
import { type ConnectRelayOptions, openRelaySession } from './connect-session.js';
import {
  closureObservers,
  createSubscriptionEvents,
  type SubscriptionEventOptions,
  type SubscriptionEvents,
} from './subscription-events.js';

export type { ConnectRelayOptions, RelayTransportFactory } from './connect-session.js';

export interface RelayTrackOptions {
  /** Namespace tuple; fields are preserved without splitting or joining on slashes. */
  namespace: readonly string[];
  track: string;
  /** Protocol subscription controls, including the location filter and priority. */
  parameters?: MessageParameters;
}

export interface RelayConnection {
  /** Subscribe to any track payload format without discovering or loading a catalog. */
  subscribe(options: RelayTrackOptions, handlers?: SubscriptionHandlers): Subscription;
  /** Bounded async events. Owns its subscription; break/return cancels it. */
  subscribeEvents(options: RelayTrackOptions, buffer?: SubscriptionEventOptions): SubscriptionEvents;
  /**
   * Generic borrowed track access for integrations. This object exposes no connection
   * shutdown operation and has no dependency on Relay resource identifiers or Video.js.
   */
  readonly tracks: MoqTrackSource;
  /** Resolves once for local shutdown, peer closure, or failure. It never rejects. */
  readonly closed: Promise<{ error?: unknown }>;
  /** Close this connection and all requests on it. Idempotent. */
  close(): void;
}

/** Connect to Relay for arbitrary track subscriptions. The caller owns connection shutdown. */
export async function connectRelay(options: ConnectRelayOptions): Promise<RelayConnection> {
  const {
    session: connected,
    closed,
    assertOpen,
    close,
  } = await openRelaySession(options, (transport, callbacks) =>
    createMoqtSession(transport, {
      implementationName: '@mux/relay',
      requestTimeoutMs: options.requestTimeoutMs,
      callbacks,
    }),
  );

  const tracks: MoqTrackSource = Object.freeze({
    closed,
    subscribe: (...args: Parameters<MoqtSession['subscribe']>) => {
      assertOpen();
      validateNamespace(args[0].trackNamespace);
      return connected.subscribe(...args);
    },
    fetch: (...args: Parameters<MoqtSession['fetch']>) => {
      assertOpen();
      return connected.fetch(...args);
    },
    trackStatus: (...args: Parameters<MoqtSession['trackStatus']>) => {
      assertOpen();
      return connected.trackStatus(...args);
    },
  });

  const observeClosed = closureObservers(closed);

  return {
    closed,
    tracks,
    subscribe: ({ namespace, track, parameters }, handlers) => {
      validateNamespace(namespace);
      return tracks.subscribe({ trackNamespace: [...namespace], trackName: track, parameters }, handlers);
    },
    subscribeEvents: ({ namespace, track, parameters }, buffer) => {
      assertOpen();
      validateNamespace(namespace);
      return createSubscriptionEvents(
        (handlers) => tracks.subscribe({ trackNamespace: [...namespace], trackName: track, parameters }, handlers),
        observeClosed,
        buffer,
      );
    },
    close,
  };
}

function validateNamespace(namespace: readonly string[]): void {
  if (
    !Array.isArray(namespace) ||
    namespace.length === 0 ||
    Array.from(namespace).some((field) => typeof field !== 'string' || field.length === 0)
  ) {
    throw new TypeError('A subscription namespace must contain non-empty string fields');
  }
  if (namespace.length > MAX_NAMESPACE_FIELDS) {
    throw new RangeError(`A subscription namespace must contain at most ${MAX_NAMESPACE_FIELDS} fields`);
  }
}
