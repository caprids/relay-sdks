import {
  createMoqtSession,
  type Goaway,
  type MessageParameters,
  type MoqTrackSource,
  type MoqtObject,
  type MoqtSession,
  type MoqtSubscribeOptions,
  type PublishDone,
  type RequestError,
  type SubscribeOk,
  type SubscriptionHandlers,
} from '@mux/moq-core';
import { assertTimeoutMs, type ConnectRelaySubscriberOptions, openRelaySession } from './connect-session.js';
import {
  createSubscription,
  type RelaySubscription,
  type SubscriptionConsumer,
  type SubscriptionEvent,
  synchronousCallback,
} from './subscription.js';
import {
  createSubscriptionEvents,
  type SubscriptionEventOptions,
  type SubscriptionEvents,
} from './subscription-events.js';
import { namespaceFields, trackName } from './validation.js';

export type {
  ConnectRelayOptions,
  ConnectRelaySubscriberOptions,
  RelayTransportFactory,
} from './connect-session.js';

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
   * The relay answered the request with REQUEST_ERROR: it rejected the subscription, or the request
   * failed before acceptance (request timeout, or the request stream failing). A REQUEST_ERROR can
   * also arrive after acceptance, for example when the relay refuses an `update()`, and it ends the
   * subscription then too (issue #94). Connection shutdown and GOAWAY never arrive here: a pending
   * subscription ends through `subscribed` and `ended`, and an accepted one surfaces them as
   * subgroup resets and connection closure.
   */
  onError?(error: RequestError): void;
  /** One object arrived. Payloads are uninterpreted bytes. Objects can arrive after onPublisherDone. */
  onObject?(object: MoqtObject): void;
  /** A subgroup stream finished cleanly (FIN). */
  onSubgroupEnd?(info: { groupId: number; subgroupId: number; endOfGroup: boolean }): void;
  /**
   * A subgroup stream was reset; objects from it may be missing. Called for every reset the relay
   * forwards. The relay may drop a reset for a subgroup it had not begun forwarding, so an abandoned
   * group can arrive as nothing at all, only a gap in group IDs.
   */
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
   * catalog. Returns at once with a cancellable handle whose `subscribed` settles when the relay
   * answers and whose `ended` settles once the subscription can no longer deliver;
   * `handlers.onSubscribed` and `handlers.onError` report the same answer as callbacks.
   */
  subscribe(options: RelayTrackOptions, handlers?: RelaySubscriptionHandlers): RelaySubscription;
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
export async function connectRelaySubscriber(options: ConnectRelaySubscriberOptions): Promise<RelaySubscriber> {
  // The request deadline is subscriber-only, so the subscriber checks it; the shared opener checks
  // the rest before anything connects.
  assertTimeoutMs('requestTimeoutMs', options.requestTimeoutMs ?? 10_000);
  const {
    session: connected,
    closed,
    observeClosed,
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
    subscribe: (options: MoqtSubscribeOptions, handlers: SubscriptionHandlers) => {
      // Arguments before connection state, as on the product entry points.
      const trackNamespace = namespaceFields(options.trackNamespace);
      const name = trackName(options.trackName);
      assertOpen();
      return connected.subscribe({ ...options, trackNamespace, trackName: name }, handlers);
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

  /**
   * Admit the arguments of one public call: checked and copied first, then the connection state.
   * Both entry points do this synchronously, before anything (an iterator, a subscription) exists.
   */
  const admit = ({ namespace, track, parameters }: RelayTrackOptions): MoqtSubscribeOptions => {
    const trackNamespace = namespaceFields(namespace);
    const name = trackName(track);
    assertOpen();
    return { trackNamespace, trackName: name, parameters };
  };

  /** One product subscription per public call; both entry points bind the core the same way. */
  const subscription = (admitted: MoqtSubscribeOptions, consumers: SubscriptionConsumer[]) =>
    createSubscription({ subscribe: (handlers) => connected.subscribe(admitted, handlers), observeClosed }, consumers);

  return {
    closed,
    trackSource,
    subscribe: (options, handlers) => {
      // Every argument (the handlers' shape included) before connection state.
      const consumers = handlers ? [handlersConsumer(handlers)] : [];
      return subscription(admit(options), consumers);
    },
    subscribeEvents: (options, buffer) => {
      // Admit before building the iterator: an already-aborted signal never runs its callback.
      const admitted = admit(options);
      return createSubscriptionEvents((consumer) => subscription(admitted, [consumer]), buffer);
    },
    close,
  };
}

/** Which product handler each product event reaches. */
const EVENT_HANDLERS = {
  subscribed: 'onSubscribed',
  object: 'onObject',
  publisherDone: 'onPublisherDone',
  subgroupEnd: 'onSubgroupEnd',
  subgroupReset: 'onSubgroupReset',
  stateNotify: 'onStateNotify',
  goaway: 'onGoaway',
  updateOk: 'onUpdateOk',
} as const satisfies Record<SubscriptionEvent['type'], keyof RelaySubscriptionHandlers>;

/**
 * The callback form as one consumer: snapshot the product callbacks at subscribe time, hold them to
 * the synchronous callback policy, and dispatch each product event to the handler of the same
 * meaning. A REQUEST_ERROR ending reaches `onError`; other endings reach nothing until issue #68.
 */
function handlersConsumer(handlers: RelaySubscriptionHandlers): SubscriptionConsumer {
  const snapshot = new Map<keyof RelaySubscriptionHandlers, (...args: unknown[]) => void>();
  for (const name of [...Object.values(EVENT_HANDLERS), 'onError'] as const) {
    const callback = handlers[name];
    if (callback) snapshot.set(name, synchronousCallback(name, callback, handlers));
  }
  return {
    event(event) {
      const callback = snapshot.get(EVENT_HANDLERS[event.type]);
      if (!callback) return;
      if (event.type === 'updateOk') callback();
      else callback(event.value);
    },
    ended(end) {
      if (end.cause === 'request-error') snapshot.get('onError')?.(end.error);
    },
  };
}
