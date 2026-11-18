/**
 * One MOQT session that publishes and subscribes: the subscribe driver (`session.ts`) and the publish driver
 * (`publish-session.ts`) share one transport, as draft-22 allows (a subscription "can be established by either
 * endpoint"; roles belong to entities, not sessions).
 *
 * The subscribe driver owns the session: it sends and reads SETUP, starts every request this endpoint makes (SUBSCRIBE,
 * SUBSCRIBE_NAMESPACE, FETCH, TRACK_STATUS), and reads every incoming unidirectional stream, the control stream and
 * subgroup data alike. The publish driver serves the peer: each request stream the peer opens is routed by its first
 * message type, so SUBSCRIBE_NAMESPACE (the relay's solicitation, which carries announces), SUBSCRIBE, TRACK_STATUS
 * and FETCH reach the publish driver, and anything else (PUBLISH, PUBLISH_NAMESPACE) the subscribe driver. The publish
 * driver's own control stream is swallowed, and it is handed the peer's SETUP options and any control-stream GOAWAY
 * the subscribe driver reads, so its readiness and migration silence follow the real session.
 *
 * This endpoint's Request IDs are allocated by the subscribe driver alone. The peer's are consumed session-wide
 * (§6.4.2.1), so both drivers claim them in one shared set: an ID one driver consumed is a reuse to the other.
 *
 * Measured against moq-relay on 2026-11-18 (spec/behavior.md, "One connection that publishes and subscribes"): one
 * session announced, served the
 * relay's SUBSCRIBE and subscribed to another session's track at once, with the relay soliciting its announces even
 * though SETUP carried SOLICIT. The relay never routes a session's own announcements back to it.
 */

import { encodeGoaway, encodeSetup, type KeyValuePair, MESSAGE_TYPE } from './control-messages.js';
import { createMoqtPublishSession, type MoqtPublishSession, type MoqtPublishSessionConfig } from './publish-session.js';
import type { BidirectionalStreamLike } from './request-stream.js';
import { createMoqtSession, type MoqtSession, type MoqtSessionConfig, type MoqtTransport } from './session.js';
import { decodeVarint, varintLengthFromFirstByte } from './varint.js';

/** Peer-initiated requests the publish driver serves; every other request type goes to the subscribe driver. */
const PUBLISHER_REQUESTS: ReadonlySet<number> = new Set([
  MESSAGE_TYPE.SUBSCRIBE_NAMESPACE,
  MESSAGE_TYPE.SUBSCRIBE,
  MESSAGE_TYPE.TRACK_STATUS,
  MESSAGE_TYPE.FETCH,
]);

/** How to configure the two drivers that share the transport. */
export interface MoqtDuplexSessionConfig {
  /** The subscribe driver, which sends SETUP: its `setupOptions` and `implementationName` are the session's. */
  subscriber?: Omit<MoqtSessionConfig, 'perspective' | 'peerRequestIds'>;
  /** The publish driver. It sends no SETUP of its own, so setup options and the implementation name do not apply. */
  publisher?: Omit<MoqtPublishSessionConfig, 'setupOptions' | 'implementationName' | 'peerRequestIds'>;
}

/** One session, seen through each driver. Created by {@link createMoqtDuplexSession}. */
export interface MoqtDuplexSession {
  /** Starts this endpoint's requests and receives their data. Its `ready`, `close` and `destroy` are the session's. */
  readonly subscriber: MoqtSession;
  /**
   * Announces namespaces and serves the peer's requests for their tracks. Its `ready` follows the subscriber's; its
   * `close()` ends its subscriptions and announces and then closes the whole session.
   */
  readonly publisher: MoqtPublishSession;
  /**
   * End the session. The subscriber ends at once, its requests with it, but leaves the transport open; the publisher
   * ends its subscriptions and retracts its announces within its bounded drain, then closes the transport. Call this
   * rather than the subscriber's own `close()` or `destroy()`, which would cut the publisher's drain short. `error`, if
   * given, is the local closure error both drivers report.
   */
  destroy(error?: unknown): void;
}

const concat = (a: Uint8Array, b: Uint8Array): Uint8Array => {
  const out = new Uint8Array(a.length + b.length);
  out.set(a);
  out.set(b, a.length);
  return out;
};

/**
 * Read a request stream's first message type without consuming it: the type, or undefined if the stream ended or
 * failed first, and a stream that replays every byte read.
 */
async function peekRequestType(
  stream: BidirectionalStreamLike
): Promise<{ type: number | undefined; stream: BidirectionalStreamLike }> {
  const reader = stream.readable.getReader();
  let buffered: Uint8Array = new Uint8Array(0);
  let type: number | undefined;
  let failure: { error: unknown } | undefined;

  for (;;) {
    // The first byte gives the varint's length (MOQT's leading-ones prefix, up to nine bytes); once a whole one is
    // in, the router has what it needs.
    const first = buffered[0];
    if (first !== undefined && buffered.length >= varintLengthFromFirstByte(first)) {
      try {
        type = decodeVarint(buffered).value;
      } catch {
        // A varint this endpoint cannot represent: no driver serves it, and the subscriber's reader refuses it.
      }
      break;
    }

    try {
      const { done, value } = await reader.read();
      if (done) break;
      buffered = concat(buffered, value);
    } catch (error) {
      failure = { error };
      break;
    }
  }

  const replay = buffered;
  const readable = new ReadableStream<Uint8Array>({
    start(controller) {
      if (replay.length > 0) controller.enqueue(replay);
      if (failure) controller.error(failure.error);
    },
    async pull(controller) {
      try {
        const { done, value } = await reader.read();
        if (done) controller.close();
        else controller.enqueue(value);
      } catch (error) {
        controller.error(error);
      }
    },
    cancel(reason) {
      return reader.cancel(reason);
    },
  });

  return { type, stream: { readable, writable: stream.writable } };
}

/**
 * Run a subscribe driver and a publish driver over one established transport. The transport must have negotiated its
 * MOQT version already, as for either driver alone.
 *
 * @example
 *   ```ts
 *   const { subscriber, publisher } = createMoqtDuplexSession(transport, {
 *     subscriber: { setupOptions: [{ type: SETUP_OPTION.SOLICIT, value: 1 }] },
 *   });
 *   publisher.announce(['room', 'p', 'alice']);
 *   publisher.registerTrack({ trackNamespace: ['room', 'p', 'alice'], trackName: 'video' });
 *   subscriber.subscribe({ trackNamespace: ['room', 'p', 'bob'], trackName: 'video' }, handlers);
 *   ```;
 */
export function createMoqtDuplexSession(
  transport: MoqtTransport,
  config: MoqtDuplexSessionConfig = {}
): MoqtDuplexSession {
  let toSubscriber!: ReadableStreamDefaultController<BidirectionalStreamLike>;
  let toPublisher!: ReadableStreamDefaultController<BidirectionalStreamLike>;
  let toPublisherControl!: ReadableStreamDefaultController<Uint8Array>;
  const subscriberRequests = new ReadableStream<BidirectionalStreamLike>({
    start: (controller) => {
      toSubscriber = controller;
    },
  });
  const publisherRequests = new ReadableStream<BidirectionalStreamLike>({
    start: (controller) => {
      toPublisher = controller;
    },
  });
  // The publish driver's control stream from the peer: the peer's SETUP and any GOAWAY, copied from the real one. It
  // never closes: the publish driver reads a closed control stream as the end of the session, and the transport's
  // `closed` tells it that first.
  const publisherControl = new ReadableStream<Uint8Array>({
    start: (controller) => {
      toPublisherControl = controller;
    },
  });

  /** Set by `destroy()`: the subscriber's own close must not cut the publisher's drain short. */
  let deferSubscriberClose = false;
  const peerRequestIds = new Set<number>();
  let controlTaken = false;
  const publisherView: MoqtTransport = {
    get protocol() {
      return transport.protocol;
    },
    incomingUnidirectionalStreams: new ReadableStream<ReadableStream<Uint8Array>>({
      start: (controller) => controller.enqueue(publisherControl),
    }),
    incomingBidirectionalStreams: publisherRequests,
    createUnidirectionalStream(options) {
      // The driver's first stream is its control stream, opened before anything else; the subscribe driver's is the
      // session's.
      if (!controlTaken) {
        controlTaken = true;
        return Promise.resolve(new WritableStream<Uint8Array>());
      }
      return transport.createUnidirectionalStream(options);
    },
    createBidirectionalStream: () =>
      Promise.reject(new Error('the publish side of a duplex session starts no requests')),
    close: (closeInfo) => transport.close(closeInfo),
    get closed() {
      return transport.closed;
    },
  };

  const subscriberView: MoqtTransport = {
    get protocol() {
      return transport.protocol;
    },
    get incomingUnidirectionalStreams() {
      return transport.incomingUnidirectionalStreams;
    },
    incomingBidirectionalStreams: subscriberRequests,
    createUnidirectionalStream: (options) => transport.createUnidirectionalStream(options),
    createBidirectionalStream: () => transport.createBidirectionalStream(),
    close: (closeInfo) => {
      if (!deferSubscriberClose) transport.close(closeInfo);
    },
    get closed() {
      return transport.closed;
    },
  };

  const callbacks = config.subscriber?.callbacks ?? {};
  const subscriber = createMoqtSession(subscriberView, {
    ...config.subscriber,
    peerRequestIds,
    callbacks: {
      ...callbacks,
      onReady: (serverOptions: KeyValuePair[]) => {
        toPublisherControl.enqueue(encodeSetup(serverOptions));
        callbacks.onReady?.(serverOptions);
      },
      onGoaway: (goaway) => {
        try {
          toPublisherControl.enqueue(encodeGoaway(goaway.timeout));
        } catch {
          // The publish driver's control stream is gone with the session.
        }
        callbacks.onGoaway?.(goaway);
      },
    },
  });
  const publisher = createMoqtPublishSession(publisherView, { ...config.publisher, peerRequestIds });

  void (async () => {
    const reader = transport.incomingBidirectionalStreams.getReader();
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        void peekRequestType(value).then(({ type, stream }) => {
          const target = type !== undefined && PUBLISHER_REQUESTS.has(type) ? toPublisher : toSubscriber;
          try {
            target.enqueue(stream);
          } catch {
            void stream.readable.cancel().catch(() => {});
          }
        });
      }
      toSubscriber.close();
      toPublisher.close();
    } catch (error) {
      toSubscriber.error(error);
      toPublisher.error(error);
    } finally {
      reader.releaseLock();
    }
  })();

  let destroyed = false;
  return {
    subscriber,
    publisher,
    destroy(error) {
      if (destroyed) return;
      destroyed = true;
      // The subscriber ends now, with `error`, but leaves the transport to the publisher, which closes it after its drain.
      deferSubscriberClose = true;
      subscriber.destroy(error);
      publisher.destroy(error);
    },
  };
}
