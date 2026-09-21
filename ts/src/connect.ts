import {
  createMoqtSession,
  type Goaway,
  MAX_NAMESPACE_FIELDS,
  type MessageParameters,
  type MoqTrackSource,
  type MoqtObject,
  type MoqtSession,
  type PublishDone,
  type RequestError,
  type SubscribeOk,
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

/**
 * Callbacks for one subscription made through {@link RelaySubscriber.subscribe}. Each is optional
 * and runs synchronously as the protocol layer delivers the event; the SDK adds no queue. The
 * names say what happened between this connection and the relay. The borrowed
 * {@link RelaySubscriber.trackSource} keeps the core's protocol names (`onOk`, `onDone`) instead.
 */
export interface RelaySubscriptionHandlers {
  /**
   * The relay accepted the subscription (SUBSCRIBE_OK) and will forward objects. `info` carries
   * the track alias, the relay's parameters (LARGEST_OBJECT when the track already has content),
   * and the track properties as the relay sends them, which can differ from what the publisher
   * declared.
   */
  onSubscribed?(info: SubscribeOk): void;
  /**
   * The relay rejected the subscription, or it failed before acceptance: request timeout,
   * connection shutdown, or GOAWAY. Not called after acceptance; later failures surface as
   * subgroup resets and connection closure.
   */
  onError?(error: RequestError): void;
  /** One object arrived. Payloads are uninterpreted bytes. Objects can arrive after onPublisherDone. */
  onObject?(object: MoqtObject): void;
  /** A subgroup stream finished cleanly (FIN). */
  onSubgroupEnd?(info: { groupId: number; subgroupId: number; endOfGroup: boolean }): void;
  /** A subgroup stream was reset; objects from it may be missing. */
  onSubgroupReset?(info: { groupId: number; error: unknown }): void;
  /**
   * The publisher finished with the track (PUBLISH_DONE). This is a control notification, not the
   * end of the subscription: objects still in transit can arrive after it, and the request stays
   * open until `cancel()` or connection shutdown. The SDK does not resubscribe; doing so can replay
   * cached objects and another done notification while the publisher is absent.
   */
  onPublisherDone?(info: PublishDone): void;
  /** The relay acknowledged an `update()` on this subscription (REQUEST_OK). */
  onUpdateOk?(): void;
  /**
   * The publisher changed the subscription's state on its own (PUBLISH_STATE_NOTIFY); `parameters`
   * carries only what changed. Informational.
   */
  onStateNotify?(parameters: MessageParameters): void;
  /**
   * GOAWAY on this request stream: the relay wants this request re-issued, possibly elsewhere. A
   * GOAWAY on the connection itself ends the connection instead; observe `closed` for that.
   */
  onGoaway?(goaway: Goaway): void;
}

/**
 * A connection that can only subscribe. Create one with {@link connectRelaySubscriber}. Publishing
 * uses a separate connection and token; see `connectRelayPublisher`.
 */
export interface RelaySubscriber {
  /**
   * Ask the relay for a track's objects, in any payload format, without discovering or loading a
   * catalog. Returns at once with a cancellable handle; `handlers.onSubscribed` or
   * `handlers.onError` reports the relay's answer.
   */
  subscribe(options: RelayTrackOptions, handlers?: RelaySubscriptionHandlers): Subscription;
  /** Bounded async events. Owns its subscription; break/return cancels it. */
  subscribeEvents(options: RelayTrackOptions, buffer?: SubscriptionEventOptions): SubscriptionEvents;
  /**
   * This connection seen through the generic `MoqTrackSource` contract, for integrations such as
   * players: `subscribe`, `fetch`, and `trackStatus` in protocol terms, plus `closed`. It lists no
   * tracks and cannot close the connection; the application that opened the connection owns
   * shutdown.
   */
  readonly trackSource: MoqTrackSource;
  /** Resolves once for local shutdown, peer closure, or failure. It never rejects. */
  readonly closed: Promise<{ error?: unknown }>;
  /** Close this connection and all requests on it. Idempotent. */
  close(): void;
}

/** Open a Relay connection for subscribing to tracks. The caller owns connection shutdown. */
export async function connectRelaySubscriber(options: ConnectRelayOptions): Promise<RelaySubscriber> {
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

  const trackSource: MoqTrackSource = Object.freeze({
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
    trackSource,
    subscribe: ({ namespace, track, parameters }, handlers) => {
      validateNamespace(namespace);
      return trackSource.subscribe(
        { trackNamespace: [...namespace], trackName: track, parameters },
        handlers && toSubscriptionHandlers(handlers),
      );
    },
    subscribeEvents: ({ namespace, track, parameters }, buffer) => {
      assertOpen();
      validateNamespace(namespace);
      return createSubscriptionEvents(
        (handlers) => trackSource.subscribe({ trackNamespace: [...namespace], trackName: track, parameters }, handlers),
        observeClosed,
        buffer,
      );
    },
    close,
  };
}

/** Snapshot product callbacks without hiding async functions or their return values from the core. */
function toSubscriptionHandlers(handlers: RelaySubscriptionHandlers): SubscriptionHandlers {
  const result: SubscriptionHandlers = {};
  const names = [
    ['onOk', 'onSubscribed'],
    ['onError', 'onError'],
    ['onObject', 'onObject'],
    ['onSubgroupEnd', 'onSubgroupEnd'],
    ['onSubgroupReset', 'onSubgroupReset'],
    ['onDone', 'onPublisherDone'],
    ['onUpdateOk', 'onUpdateOk'],
    ['onStateNotify', 'onStateNotify'],
    ['onGoaway', 'onGoaway'],
  ] as const;
  for (const [coreName, productName] of names) {
    const callback = handlers[productName];
    if (callback) Object.assign(result, { [coreName]: callback.bind(handlers) });
  }
  return result;
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
