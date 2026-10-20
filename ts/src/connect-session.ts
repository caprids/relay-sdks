import {
  type KeyValuePair,
  MOQT_PROTOCOL_IDS,
  type MoqtProtocolId,
  type MoqtSessionCallbacks,
  type MoqtTransport,
  SETUP_OPTION,
} from '@mux/moq-core';

import { connectedError, connectingError, isCredentialRefusal, RelayError, transportError } from './errors.js';

/** setTimeout stores its delay in a signed 32-bit integer; a larger one wraps and fires at once. */
export const MAX_TIMEOUT_MS = 2_147_483_647;

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
 * protocol IDs to offer, in preference order: `moqt-22`, then `moqt-20` as a fallback.
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
   * in SETUP. `'websocket'` is QMux over a WebSocket (`@mux/relay-websocket`), whose upgrade
   * request carries the URL as a CONNECT does. Read once `ready` has resolved.
   */
  framing?: 'webtransport' | 'native-quic' | 'websocket';
};

/**
 * A token together with when the relay stops honoring it. moq-relay ends a session at its grant's deadline, which is
 * the token's expiry, and a live session cannot take a new token; supervised connections replace theirs before then.
 */
export interface RelayCredential {
  /** The token, carried in the endpoint's jwt query parameter. An empty one sends no token. */
  token: string;
  /** When the relay ends a session the token admitted: the JWT's `exp`, or the grant's deadline if it is earlier. */
  expiresAt: Date;
}

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
   * in the URL is a RangeError, raised before the provider or the transport is used. Return a
   * {@link RelayCredential} to say when the token expires; for a bare token the connection reads
   * the JWT's `exp` claim, without verifying it. The provider runs once per connection: a live
   * session keeps the token it connected with until the relay ends it at that expiry.
   */
  getToken?: () => string | RelayCredential | undefined | Promise<string | RelayCredential | undefined>;
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
  /** The MOQT version the transport negotiated, which selects the core's parameter codec. */
  protocol: MoqtProtocolId;
  /** When the relay ends the session for the credential's expiry, if known. */
  expiresAt: Date | undefined;
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
  // Deno 2.x and Firefox before 155 have a global that ignores protocols and has no protocol getter, so
  // it could never agree on a MOQT version; refuse it here rather than hang waiting for a SETUP that cannot come.
  if (!('protocol' in WebTransport.prototype)) {
    throw new RelayError(
      'TRANSPORT_UNAVAILABLE',
      'WebTransport in this runtime cannot negotiate protocols (Firefox needs 155 or later); supply createTransport with a transport adapter for this runtime',
      'connecting',
    );
  }

  // TypeScript's DOM lib does not declare `protocol` yet; the prototype check above found it.
  const transport = new WebTransport(url, { protocols }) as WebTransport & { readonly protocol: string };

  return { transport, ready: transport.ready, framing: 'webtransport' };
};

const utf8 = new TextEncoder();

/**
 * The setup options the endpoint URL needs in CLIENT_SETUP for a framing (TA-6). On native QUIC,
 * PATH (the path-abempty and `?query`, the resolved jwt included) and AUTHORITY take the place of
 * the CONNECT request a WebTransport session sends; on a WebTransport session, or a WebSocket whose
 * upgrade request carries the URL as a CONNECT does, neither may be sent (draft-22 §9.1.2, §9.1.1).
 */
function endpointSetupOptions(framing: unknown, url: URL): KeyValuePair[] {
  if (framing === undefined || framing === 'webtransport' || framing === 'websocket') return [];
  if (framing !== 'native-quic')
    throw new RelayError(
      'TRANSPORT_ERROR',
      `createTransport returned an unknown framing ${JSON.stringify(framing)}; expected 'webtransport', 'native-quic' or 'websocket'`,
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
 * Fail the connection unless the transport negotiated one of the offered protocols, `moqt-22` or `moqt-20` (TA-8). It
 * backs up TA-7, which makes `ready` reject on no selection or an unoffered one; Chromium resolves `ready` anyway, with
 * an empty `protocol`.
 */
function assertNegotiatedProtocol(protocol: unknown): void {
  if ((MOQT_PROTOCOL_IDS as readonly unknown[]).includes(protocol)) return;
  const received = typeof protocol === 'string' && protocol.length > 0 ? JSON.stringify(protocol) : 'none';
  const offered = MOQT_PROTOCOL_IDS.map((id) => `"${id}"`).join(', ');
  throw new RelayError(
    'TRANSPORT_ERROR',
    `Relay transport negotiated ${received}, not one of the offered protocols ${offered}`,
    'connecting',
  );
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
 * Check an endpoint by the Endpoint rule in spec/behavior.md and return it parsed. Not a string, or
 * not a URL at all (including an https URL with no host), is a TypeError: the wrong shape. A URL
 * that parses but is not one Relay accepts is a RangeError: a disallowed value.
 */
export function endpointUrl(endpoint: unknown): URL {
  if (typeof endpoint !== 'string') throw new TypeError('Relay endpoints must be a URL string');
  let url: URL;
  try {
    url = new URL(endpoint);
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
  return url;
}

/**
 * Shared transport/auth lifetime for the subscribe and publish session drivers. It validates only
 * the shared options; each role checks its own options before calling it.
 */
export async function openRelaySession<T extends Session>(
  options: RelaySessionOptions,
  createSession: (
    transport: MoqtTransport,
    callbacks: Pick<MoqtSessionCallbacks, 'onReady' | 'onClosed' | 'onGoaway'>,
    setupOptions: KeyValuePair[],
  ) => T,
): Promise<RelaySessionLifetime<T>> {
  const url = endpointUrl(options.url);

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
  let expiresAt: Date | undefined;
  let peerClose: unknown;
  // Set in the turn server SETUP arrives, ahead of `established`: a failure in between is no credentials refusal.
  let setupReceived = false;
  const connectFailure = (error: unknown): RelayError =>
    setupReceived ? transportError(error, 'connecting') : connectingError(error, peerClose);
  const establish = async (): Promise<{ session: T; transport: MoqtTransport }> => {
    if (!url.searchParams.has('jwt') && options.getToken) {
      const resolved = await options.getToken();

      assertOpen();
      const { token, expiresAt: given } = credential(resolved);
      if (token.length > 0) url.searchParams.set('jwt', token);
      expiresAt = given ?? jwtExpiry(token);
    } else {
      expiresAt = jwtExpiry(url.searchParams.get('jwt') ?? '');
    }

    let createTransport = options.createTransport ?? createBrowserTransport;
    const resolve = options[resolveTransport];
    if (resolve) {
      assertOpen();
      createTransport = await resolve();
    }

    assertOpen();
    usingTransport = true;
    const created = createTransport(url.href, [...MOQT_PROTOCOL_IDS]);

    transport = created.transport;
    // WebTransport.closed may reject alongside ready, before a session can observe it. Registered before the
    // session's own handler, so a peer close is recorded by the time the session reports it.
    void transport.closed.then(
      (info) => {
        peerClose = info;
      },
      () => {},
    );
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
    assertNegotiatedProtocol(transport.protocol);
    const setupOptions = endpointSetupOptions(created.framing, url);

    session = createSession(
      transport,
      {
        onReady: () => {
          setupReceived = true;
        },
        onClosed: (info) => {
          if (info.error !== undefined) {
            finish({ error: established ? connectedError(info.error, peerClose) : connectFailure(info.error) });
          } else if (established && isCredentialRefusal(peerClose)) {
            // The session reports the peer's close without its code; a credentials code ends a live session too.
            finish({ error: connectedError(peerClose) });
          } else {
            finish(info);
          }
        },
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
    const failure = stopped ? stopError : usingTransport ? connectFailure(error) : error;
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
    // assertNegotiatedProtocol admitted it before the session started.
    protocol: connected.transport.protocol as MoqtProtocolId,
    expiresAt,
    assertOpen,
    close: () => finish(),
  };
}

/** Check what `getToken` returned and split it into the token and its expiry, if given. */
function credential(resolved: unknown): { token: string; expiresAt?: Date } {
  if (resolved === undefined) return { token: '' };
  if (typeof resolved === 'string') return { token: resolved };
  if (typeof resolved !== 'object' || resolved === null)
    throw new TypeError('getToken must return a string, a RelayCredential or undefined');
  const { token, expiresAt } = resolved as { token?: unknown; expiresAt?: unknown };
  if (typeof token !== 'string') throw new TypeError('RelayCredential.token must be a string');
  if (!(expiresAt instanceof Date)) throw new TypeError('RelayCredential.expiresAt must be a Date');
  if (Number.isNaN(expiresAt.getTime())) throw new RangeError('RelayCredential.expiresAt is an invalid Date');
  return { token, expiresAt: new Date(expiresAt.getTime()) };
}

/**
 * The `exp` claim of a JWT, read without verifying the token: the relay verifies it, and the SDK only needs to know
 * when the session will end. Anything that is not a JWT with a numeric `exp` has no known expiry.
 */
export function jwtExpiry(token: string): Date | undefined {
  const parts = token.split('.');
  if (parts.length !== 3 || !parts[1]) return undefined;
  try {
    const base64 = parts[1].replace(/-/g, '+').replace(/_/g, '/');
    const json = new TextDecoder('utf-8', { fatal: true }).decode(
      Uint8Array.from(atob(base64.padEnd(base64.length + ((4 - (base64.length % 4)) % 4), '=')), (c) =>
        c.charCodeAt(0),
      ),
    );
    const claims: unknown = JSON.parse(json);
    if (typeof claims !== 'object' || claims === null) return undefined;
    const { exp } = claims as { exp?: unknown };
    if (typeof exp !== 'number' || !Number.isFinite(exp)) return undefined;
    const expiresAt = new Date(exp * 1000);
    return Number.isNaN(expiresAt.getTime()) ? undefined : expiresAt;
  } catch {
    return undefined;
  }
}
