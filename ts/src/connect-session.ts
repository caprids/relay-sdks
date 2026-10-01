import {
  type KeyValuePair,
  MOQT_PROTOCOL_ID,
  type MoqtSessionCallbacks,
  type MoqtTransport,
  SETUP_OPTION,
} from '@mux/moq-core';

import { RelayError, transportError } from './errors.js';

/** setTimeout stores its delay in a signed 32-bit integer; a larger one wraps and fires at once. */
const MAX_TIMEOUT_MS = 2_147_483_647;

/**
 * Reject a deadline a timer cannot hold, so a larger number never means a shorter wait. A value
 * that is not a number is a TypeError; a number outside (0, 2147483647] is a RangeError.
 */
export function assertTimeoutMs(name: string, value: unknown): void {
  if (typeof value !== 'number') throw new TypeError(`${name} must be a number of milliseconds`);
  if (!Number.isFinite(value) || value <= 0 || value > MAX_TIMEOUT_MS) {
    throw new RangeError(`${name} must be between 0 (exclusive) and ${MAX_TIMEOUT_MS} milliseconds`);
  }
}

/**
 * Supply a browser transport or a compatible implementation in another runtime. Called once per
 * connection with the endpoint URL (the token already in its `jwt` query parameter) and the MOQT
 * protocol IDs to offer, today only `moqt-20`.
 */
export type RelayTransportFactory = (
  url: string,
  protocols: string[],
) => {
  /** The transport the MOQT session runs on. */
  transport: MoqtTransport;
  /** Resolves once the transport can open streams; a rejection fails the connect call. */
  ready: Promise<void>;
  /**
   * What the transport carries. `'webtransport'` (the default) is a WebTransport session, whose
   * CONNECT request carries the URL. `'native-quic'` is MOQT directly on a QUIC connection: the
   * adapter dials only the URL's host and port, and the core sends the path, query and authority
   * in SETUP.
   */
  framing?: 'webtransport' | 'native-quic';
};

/**
 * Connection options shared by connectRelaySubscriber and connectRelayPublisher: endpoint, token,
 * lifetime signal, connection deadline, and transport injection. Each role's options extend this
 * with what only that role uses: {@link ConnectRelaySubscriberOptions} and
 * `ConnectRelayPublisherOptions`.
 */
export interface ConnectRelayOptions {
  /** Relay's HTTPS WebTransport endpoint, including any endpoint path. */
  url: string;
  /**
   * Resolve a connection token. Relay carries it in the endpoint's jwt query parameter.
   * An explicit jwt in the URL wins, and the provider is not called in that case. An empty jwt
   * in the URL is a RangeError, raised before the provider or the transport is used.
   */
  getToken?: () => string | undefined | Promise<string | undefined>;
  /** Aborting closes the connection, including after the connect call has resolved. */
  signal?: AbortSignal;
  /** Deadline for token resolution, transport connection, and protocol SETUP. Default 10 seconds, maximum 2147483647. */
  connectTimeoutMs?: number;
  /**
   * Create the transport. Defaults to the browser's WebTransport; a runtime without it, or whose
   * WebTransport cannot negotiate protocols, fails with `TRANSPORT_UNAVAILABLE` unless this is
   * supplied.
   */
  createTransport?: RelayTransportFactory;
}

/** Options for connectRelaySubscriber: the shared connection options plus the request deadline. */
export interface ConnectRelaySubscriberOptions extends ConnectRelayOptions {
  /** Deadline for an initial subscription response, not for silence on an accepted track. Default 10 seconds, maximum 2147483647. */
  requestTimeoutMs?: number;
}

interface Session {
  ready: Promise<void>;
  destroy(error?: unknown): void;
}

/** A connected session together with the connection lifetime its public handle exposes. */
interface RelaySessionLifetime<T extends Session> {
  session: T;
  /** Settles once the connection has ended, carrying the error that ended it if any. */
  closed: Promise<{ error?: unknown }>;
  /**
   * Observe the closure in the turn it happens, before the session tears its requests down; an
   * observer added after the end is told at once. The returned function removes the observer.
   */
  observeClosed(observer: (error?: unknown) => void): () => void;
  transportClosed: MoqtTransport['closed'];
  /** Throws the closing error once the connection has ended. */
  assertOpen(): void;
  /** Close the connection. Idempotent. */
  close(): void;
}

const createBrowserTransport: RelayTransportFactory = (url, protocols) => {
  if (typeof WebTransport === 'undefined') {
    throw new RelayError(
      'TRANSPORT_UNAVAILABLE',
      'WebTransport is unavailable; in Node, import from @mux/relay/node (needs @mux/relay-quic), or supply createTransport with a transport adapter for this runtime',
      'connecting',
    );
  }
  // Deno 2.x has a global that ignores protocols and has no protocol getter, so it could never
  // agree on moqt-20; refuse it here rather than hang waiting for a SETUP that cannot come.
  if (!('protocol' in WebTransport.prototype)) {
    throw new RelayError(
      'TRANSPORT_UNAVAILABLE',
      'WebTransport in this runtime cannot negotiate protocols; supply createTransport with a transport adapter for this runtime',
      'connecting',
    );
  }

  const transport = new WebTransport(url, { protocols });

  return { transport, ready: transport.ready, framing: 'webtransport' };
};

const utf8 = new TextEncoder();

/**
 * The setup options the endpoint URL needs in CLIENT_SETUP for a framing (TA-6). On native QUIC,
 * PATH (the path-abempty and `?query`, the resolved jwt included) and AUTHORITY take the place of
 * the CONNECT request a WebTransport session sends; on a WebTransport session neither may be sent
 * (draft-20 §10.3.1.2, §10.3.1.1).
 */
function endpointSetupOptions(framing: unknown, url: URL): KeyValuePair[] {
  if (framing === undefined || framing === 'webtransport') return [];
  if (framing !== 'native-quic')
    throw new RelayError(
      'TRANSPORT_ERROR',
      `createTransport returned an unknown framing ${JSON.stringify(framing)}; expected 'webtransport' or 'native-quic'`,
      'connecting',
    );
  // URL.search is empty for both an absent and an empty query, but `?` with nothing after it is a
  // present, empty query that PATH keeps. A path cannot contain `?` and the endpoint has no
  // fragment, so the query is everything in href from the first `?`.
  const queryStart = url.href.indexOf('?');
  const path = url.pathname + (queryStart === -1 ? '' : url.href.slice(queryStart));
  return [
    { type: SETUP_OPTION.PATH, value: utf8.encode(path) },
    { type: SETUP_OPTION.AUTHORITY, value: utf8.encode(url.host) },
  ];
}

/**
 * Internal to the SDK's entry points, never part of the public options: resolves the transport factory in place of
 * `createTransport`, inside the connection's deadline and signal and after the token. `@mux/relay/node` loads its
 * adapter here. A failure is passed through as it is thrown.
 */
export const resolveTransport = Symbol('resolveTransport');

/** The options {@link openRelaySession} reads: the public ones, plus an entry point's {@link resolveTransport}. */
export type RelaySessionOptions = ConnectRelayOptions & { [resolveTransport]?: () => Promise<RelayTransportFactory> };

/**
 * Shared transport/auth lifetime for the subscribe and publish session drivers. It validates only
 * the shared options; each role checks its own options before calling it.
 */
export async function openRelaySession<T extends Session>(
  options: RelaySessionOptions,
  createSession: (
    transport: MoqtTransport,
    callbacks: Pick<MoqtSessionCallbacks, 'onClosed' | 'onGoaway'>,
    setupOptions: KeyValuePair[],
  ) => T,
): Promise<RelaySessionLifetime<T>> {
  // Not a string, or not a URL at all (including an https URL with no host), is a TypeError: the
  // wrong shape. A URL that parses but is not one Relay accepts is a RangeError: a disallowed value.
  if (typeof options.url !== 'string') throw new TypeError('Relay endpoints must be a URL string');
  let url: URL;
  try {
    url = new URL(options.url);
  } catch (error) {
    throw new TypeError('Relay endpoints must be a valid absolute URL', { cause: error });
  }
  if (url.protocol !== 'https:') throw new RangeError('Relay endpoints must use HTTPS');
  if (url.hash || url.username || url.password)
    throw new RangeError('Relay endpoints cannot contain fragments or userinfo');
  // A present jwt wins over getToken, so an empty one would connect with no credential at all.
  if (url.searchParams.getAll('jwt').some((value) => value.length === 0))
    throw new RangeError(
      'Relay endpoint has an empty jwt query parameter; put a token in it, or remove it and supply getToken',
    );

  const timeoutMs = options.connectTimeoutMs ?? 10_000;
  assertTimeoutMs('connectTimeoutMs', timeoutMs);

  options.signal?.throwIfAborted();

  let transport: MoqtTransport | undefined;
  let session: T | undefined;
  let stopped = false;
  let stopError: unknown;
  let rejectConnect!: (error: unknown) => void;
  let resolveClosed!: (info: { error?: unknown }) => void;
  const interrupted = new Promise<never>((_, reject) => {
    rejectConnect = reject;
  });
  const closed = new Promise<{ error?: unknown }>((resolve) => {
    resolveClosed = resolve;
  });
  const closureObservers = new Set<(error?: unknown) => void>();
  let closedInfo: { error?: unknown } = {};

  const finish = (info: { error?: unknown } = {}): void => {
    if (stopped) return;

    stopped = true;
    closedInfo = info;
    stopError =
      info.error !== undefined ? info.error : new RelayError('CLOSED', 'Relay connection is closed', 'connection');
    clearTimeout(timer);
    options.signal?.removeEventListener('abort', onAbort);

    // Settle before synchronous user callbacks run during session teardown.
    resolveClosed(info);
    rejectConnect(stopError);
    // Subscriptions hear the closure in this turn. The session fails a pending request before it
    // reports an unexpected closure, so this is how a lost connection is kept from reading as a
    // rejection.
    for (const observer of [...closureObservers]) observer(info.error);
    closureObservers.clear();

    if (session) {
      session.destroy(info.error);
    } else {
      try {
        transport?.close();
      } catch {
        // A failed handshake may leave a transport that already refuses close().
      }
    }
  };

  const onAbort = (): void => finish({ error: options.signal?.reason });
  const timer = setTimeout(
    () => finish({ error: new RelayError('CONNECTION_TIMEOUT', 'Relay connection timed out', 'connecting') }),
    timeoutMs,
  );

  options.signal?.addEventListener('abort', onAbort, { once: true });

  const assertOpen = (): void => {
    if (stopped) throw stopError;
  };

  let established = false;
  let usingTransport = false;
  const establish = async (): Promise<{ session: T; transport: MoqtTransport }> => {
    if (!url.searchParams.has('jwt') && options.getToken) {
      const token = await options.getToken();

      assertOpen();
      if (token !== undefined && typeof token !== 'string')
        throw new TypeError('getToken must return a string or undefined');
      if (token !== undefined && token.length > 0) url.searchParams.set('jwt', token);
    }

    let createTransport = options.createTransport ?? createBrowserTransport;
    const resolve = options[resolveTransport];
    if (resolve) {
      assertOpen();
      createTransport = await resolve();
    }

    assertOpen();
    usingTransport = true;
    const created = createTransport(url.href, [MOQT_PROTOCOL_ID]);

    transport = created.transport;
    // WebTransport.closed may reject alongside ready, before a session can observe it.
    void transport.closed.catch(() => {});
    // A custom factory can synchronously abort before returning its transport.
    if (stopped) {
      void created.ready.catch(() => {});
      try {
        transport.close();
      } catch {
        // The factory may have closed it while aborting.
      }
      assertOpen();
    }

    await created.ready;
    assertOpen();
    const setupOptions = endpointSetupOptions(created.framing, url);

    session = createSession(
      transport,
      {
        onClosed: (info) =>
          finish(
            info.error === undefined
              ? info
              : { error: transportError(info.error, established ? 'connection' : 'connecting') },
          ),
        onGoaway: () =>
          finish({
            error: new RelayError(
              'GOAWAY',
              'Relay requested session migration; open a new connection',
              established ? 'connection' : 'connecting',
            ),
          }),
      },
      setupOptions,
    );

    await session.ready;
    assertOpen();
    return { session, transport };
  };

  let connected: { session: T; transport: MoqtTransport };

  try {
    connected = await Promise.race([establish(), interrupted]);
    assertOpen();
  } catch (error) {
    const failure = stopped ? stopError : usingTransport ? transportError(error, 'connecting') : error;
    finish({ error: failure });
    throw failure;
  }

  clearTimeout(timer);
  established = true;

  return {
    session: connected.session,
    closed,
    observeClosed: (observer) => {
      if (stopped) {
        observer(closedInfo.error);
        return () => {};
      }
      closureObservers.add(observer);
      return () => {
        closureObservers.delete(observer);
      };
    },
    transportClosed: connected.transport.closed,
    assertOpen,
    close: () => finish(),
  };
}
