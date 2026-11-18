/**
 * One Relay connection that publishes and subscribes: the subscriber's surface plus `publish()` for any number of
 * namespaces, over one MOQT session (`createMoqtDuplexSession`). See spec/behavior.md, "One connection that publishes and
 * subscribes".
 */
import { createMoqtDuplexSession } from '@caprid/moq-core';
import { type RelaySubscriber, relaySubscriberOf, SUBSCRIBER_SETUP_OPTIONS } from './connect.js';
import { assertTimeoutMs, type ConnectRelaySubscriberOptions, openRelaySession } from './connect-session.js';
import { RelayError } from './errors.js';
import { createPublishHost, type RelayPublication } from './publish.js';
import { namespaceFields } from './validation.js';

/** Options for {@link connectRelay}: the subscriber's, since publishing adds no connection option. */
export type ConnectRelaySessionOptions = ConnectRelaySubscriberOptions;

/**
 * A connection that publishes and subscribes. Create one with {@link connectRelay}. Everything a
 * {@link RelaySubscriber} does works here unchanged; `publish()` adds namespaces this connection announces and serves.
 *
 * The relay never routes a connection's own announcements back to it: `namespaceEvents` does not report this
 * connection's publications, and subscribing to one of its own tracks is refused (request error 16 on moq-relay).
 */
export interface RelaySession extends RelaySubscriber {
  /**
   * Announce `namespace` and return the publication that serves its tracks. Nothing is sent until its first track is
   * registered. A namespace published once stays taken on this connection, also after its publication closes. The
   * token must grant publishing it.
   */
  publish(namespace: readonly string[]): RelayPublication;
  /**
   * End every publication's tracks, wait for their unacknowledged groups as `RelayPublisher.close()` does, retract the
   * announces, close every subscription and request, and wait for the transport to close. Idempotent.
   */
  close(): Promise<void>;
}

/**
 * Open one Relay connection that publishes and subscribes. The caller owns connection shutdown. A namespace the relay
 * stops soliciting ends only its publication (`NAMESPACE_ENDED` on its `closed`); the connection stays open.
 */
export async function connectRelay(options: ConnectRelaySessionOptions): Promise<RelaySession> {
  assertTimeoutMs('requestTimeoutMs', options.requestTimeoutMs ?? 10_000);
  const host = createPublishHost((namespace, error) =>
    host.disconnect(
      namespace,
      new RelayError('NAMESPACE_ENDED', 'Relay withdrew the namespace', 'connection', { cause: error }),
    ),
  );

  const lifetime = await openRelaySession(options, (transport, callbacks, setupOptions) => {
    const duplex = createMoqtDuplexSession(transport, {
      subscriber: {
        implementationName: '@caprid/relay',
        setupOptions: [...setupOptions, ...SUBSCRIBER_SETUP_OPTIONS],
        requestTimeoutMs: options.requestTimeoutMs,
        callbacks,
      },
      // A protocol failure on the publishing side ends the connection like one on the subscribing side.
      publisher: {
        // A GOAWAY on one of the relay's requests to it ends the connection, as on a publisher connection.
        callbacks: {
          ...host.callbacks,
          ...(callbacks.onClosed ? { onClosed: callbacks.onClosed } : {}),
          ...(callbacks.onGoaway ? { onGoaway: callbacks.onGoaway } : {}),
        },
      },
    });
    return {
      ready: duplex.subscriber.ready,
      destroy: (error?: unknown) => duplex.destroy(error),
      subscriber: duplex.subscriber,
      publisher: duplex.publisher,
    };
  });
  host.bind(lifetime.session.publisher, lifetime);

  const subscriber = relaySubscriberOf({ ...lifetime, session: lifetime.session.subscriber });
  const transportClosed = lifetime.transportClosed.then(
    () => lifetime.closed,
    () => lifetime.closed,
  );
  let shutdown: Promise<void> | undefined;

  return {
    ...subscriber,
    publish(namespace) {
      // Arguments before connection state, as on the other entry points.
      return host.publish(namespaceFields(namespace));
    },
    close() {
      shutdown ??= (async () => {
        await host.endAll();
        lifetime.close();
        await transportClosed;
      })();
      return shutdown;
    },
  };
}
