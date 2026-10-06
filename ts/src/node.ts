/**
 * `@mux/relay/node`: the SDK for Node. It exports everything `@mux/relay` does, and its connect functions default
 * `createTransport` to `@mux/relay-quic`, the native QUIC transport adapter, an optional peer dependency installed next
 * to this package. It uses that adapter even when a global `WebTransport` exists, and warns once when the global
 * could replace it.
 *
 * ```ts
 * import { connectRelayPublisher } from '@mux/relay/node';
 *
 * const publisher = await connectRelayPublisher({ url, namespace, getToken });
 * ```
 *
 * @packageDocumentation
 */
import {
  type ConnectRelayOptions,
  type ConnectRelaySubscriberOptions,
  connectRelaySubscriber as connectSubscriber,
  type RelaySubscriber,
  type RelayTransportFactory,
} from './connect.js';
import { resolveTransport } from './connect-session.js';
import { RelayError } from './errors.js';
import {
  type ConnectRelayPublisherOptions,
  connectRelayPublisher as connectPublisher,
  type RelayPublisher,
} from './publish.js';

import {
  type ConnectSupervisedPublisherOptions,
  type SupervisedPublisher,
  superviseRelayPublisher,
} from './supervised-publisher.js';
import {
  type ConnectSupervisedSubscriberOptions,
  type SupervisedSubscriber,
  superviseRelaySubscriber,
} from './supervised-subscriber.js';

export * from './index.js';

/** A certificate hash the server's end-entity certificate must match, in the W3C WebTransport shape. */
export interface RelayCertificateHash {
  /** The hash algorithm; only `'sha-256'` is supported. */
  algorithm: 'sha-256';
  /** The SHA-256 of the certificate's DER encoding. */
  value: BufferSource;
}

/**
 * What a Node connection trusts, passed to `@mux/relay-quic`'s `nativeQuicTransport`. With neither option, the
 * platform's root certificates. Declared here, not imported, so these declarations need no `@mux/relay-quic` installed.
 */
export interface RelayNodeTrustOptions {
  /** Extra trust anchors, PEM encoded, added to the platform's roots: one string or buffer may hold several. */
  ca?: string | Uint8Array | readonly (string | Uint8Array)[];
  /** Accept exactly the end-entity certificates with these hashes instead of validating a chain. */
  serverCertificateHashes?: readonly RelayCertificateHash[];
}

/**
 * A role's connection options in Node: either the native QUIC adapter's trust options (`ca`,
 * `serverCertificateHashes`), or a `createTransport` of the caller's own, which replaces the adapter. Both at once is a
 * TypeError, since the trust options would go unused.
 */
export type RelayNodeOptions<T extends ConnectRelayOptions> =
  | (Omit<T, 'createTransport'> &
      RelayNodeTrustOptions & {
        /** Absent: the connection dials through `@mux/relay-quic`. */
        createTransport?: undefined;
      })
  | (T & {
      /** The caller's own transport, used instead of `@mux/relay-quic`. */
      createTransport: RelayTransportFactory;
      /** Not taken beside `createTransport`: pass it to the factory instead. */
      ca?: undefined;
      /** Not taken beside `createTransport`: pass it to the factory instead. */
      serverCertificateHashes?: undefined;
    });

/** The adapter's module and the factory it makes with no options, shared by every connection that passes none. */
interface Adapter {
  nativeQuicTransport(options?: RelayNodeTrustOptions): RelayTransportFactory;
  shared: RelayTransportFactory;
}

let loading: Promise<Adapter> | undefined;
let warned = false;

/**
 * Load `@mux/relay-quic` and its native addon on first use; concurrent first calls share the one load. Either one
 * missing is `TRANSPORT_UNAVAILABLE`; a failure is not kept, so a later call tries again.
 */
function loadAdapter(): Promise<Adapter> {
  loading ??= (async () => {
    try {
      const { nativeQuicTransport } = await import('@mux/relay-quic');
      // With no options the only thing that can fail is loading the addon.
      return { nativeQuicTransport, shared: nativeQuicTransport() };
    } catch (error) {
      loading = undefined;
      throw new RelayError(
        'TRANSPORT_UNAVAILABLE',
        '@mux/relay/node needs @mux/relay-quic and its native binary for this platform; install it next to ' +
          '@mux/relay (see its README)',
        'connecting',
        { cause: error },
      );
    }
  })();
  return loading;
}

/** Whether this runtime has a global `WebTransport` the core's default factory would use: one that negotiates protocols. */
function hasNativeWebTransport(): boolean {
  return typeof WebTransport !== 'undefined' && 'protocol' in WebTransport.prototype;
}

/** Once per process, a deprecation warning when Node's own `WebTransport` could replace this entry point. */
function warnIfNativeWebTransport(): void {
  if (warned || !hasNativeWebTransport()) return;
  warned = true;
  // Through Node's warning channel, not the console: `--no-deprecation` silences it, and the code can be filtered.
  const { process } = globalThis as {
    process?: { emitWarning?(message: string, options: { type: string; code: string }): void };
  };
  process?.emitWarning?.(
    'native WebTransport detected; you can drop this import and use @mux/relay instead of @mux/relay/node',
    { type: 'DeprecationWarning', code: 'MUX_RELAY_NODE_NATIVE_WEBTRANSPORT' },
  );
}

/**
 * The role's options for the root connect function: the caller's own `createTransport`, or the adapter, loaded inside
 * the connection's deadline and signal, after its arguments are checked.
 */
function withTransport<T extends ConnectRelayOptions>(options: RelayNodeOptions<T>): T {
  warnIfNativeWebTransport();
  const { ca, serverCertificateHashes, ...rest } = options;
  const trusted = ca !== undefined || serverCertificateHashes !== undefined;
  if (rest.createTransport !== undefined) {
    if (trusted) {
      throw new TypeError('pass ca and serverCertificateHashes to nativeQuicTransport, not beside createTransport');
    }
    return rest as T;
  }
  return {
    ...rest,
    [resolveTransport]: async () => {
      const { nativeQuicTransport, shared } = await loadAdapter();
      if (!trusted) return shared;
      // Invalid trust options throw here, as they do from nativeQuicTransport itself.
      return nativeQuicTransport({
        ...(ca === undefined ? {} : { ca }),
        ...(serverCertificateHashes === undefined ? {} : { serverCertificateHashes }),
      });
    },
  } as T;
}

/**
 * Open a Relay connection for subscribing to tracks, as `@mux/relay`'s `connectRelaySubscriber` does, over native QUIC
 * through `@mux/relay-quic` unless `createTransport` is given. The caller owns connection shutdown; an open connection
 * keeps the process alive until it closes.
 */
export async function connectRelaySubscriber(
  options: RelayNodeOptions<ConnectRelaySubscriberOptions>,
): Promise<RelaySubscriber> {
  return connectSubscriber(withTransport(options));
}

/**
 * Open a Relay connection that publishes one namespace, as `@mux/relay`'s `connectRelayPublisher` does, over native
 * QUIC through `@mux/relay-quic` unless `createTransport` is given. An open connection keeps the process alive until
 * it closes.
 */
export async function connectRelayPublisher(
  options: RelayNodeOptions<ConnectRelayPublisherOptions>,
): Promise<RelayPublisher> {
  return connectPublisher(withTransport(options));
}

/**
 * Open a subscriber that reconnects, as `@mux/relay`'s `connectSupervisedSubscriber` does, with every connection over
 * native QUIC through `@mux/relay-quic` unless `createTransport` is given.
 */
export async function connectSupervisedSubscriber(
  options: RelayNodeOptions<ConnectSupervisedSubscriberOptions>,
): Promise<SupervisedSubscriber> {
  return superviseRelaySubscriber(connectSubscriber, withTransport(options));
}

/**
 * Open a publisher that reconnects, as `@mux/relay`'s `connectSupervisedPublisher` does, with every generation over
 * native QUIC through `@mux/relay-quic` unless `createTransport` is given.
 */
export async function connectSupervisedPublisher(
  options: RelayNodeOptions<ConnectSupervisedPublisherOptions>,
): Promise<SupervisedPublisher> {
  return superviseRelayPublisher(connectPublisher, withTransport(options));
}
