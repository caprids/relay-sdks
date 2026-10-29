import {
  type KeyValuePair,
  MOQT_PROTOCOL_IDS,
  type MoqtProtocolId,
  type MoqtSessionCallbacks,
  type MoqtTransport,
  SETUP_OPTION,
} from '@caprid/moq-core';

import { connectedError, connectingError, isCredentialRefusal, RelayError, transportError } from './errors.js';
import { DEFAULT_ENDPOINT, endpointUrl, roomEndpoint } from './room-endpoint.js';

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
   * in SETUP. `'websocket'` is QMux over a WebSocket (`@caprid/relay-websocket`), whose upgrade
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
  /**
   * Relay's HTTPS WebTransport endpoint, including any endpoint path. Left out, the connection dials
   * {@link DEFAULT_ENDPOINT} rooted at the token's `root` claim, read without verifying the token: the URL `roomEndpoint`
   * builds from that root. `getToken` is then required, and a token that names no usable root is a TypeError or
   * RangeError before the transport is used. Pass `url` for another relay, region or room.
   */
  url?: string;
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
      'WebTransport is unavailable; in Node, import from @caprid/relay/node (needs @caprid/relay-quic), or supply createTransport with a transport adapter for this runtime',
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
 * `createTransport`, inside the connection's deadline and signal and after the token. `@caprid/relay/node` loads its
 * adapter here. A failure is passed through as it is thrown.
 */
export const resolveTransport = Symbol('resolveTransport');

/**
 * Internal to the supervisors, never part of the public options: told the endpoint a connection dialed, before the
 * provider's token was added, once the connection is up. A supervisor given no `url` dials it from then on.
 */
export const reportEndpoint = Symbol('reportEndpoint');

/** The options only the SDK's own entry points and supervisors pass: see {@link resolveTransport}, {@link reportEndpoint}. */
export interface RelaySessionHooks {
  [resolveTransport]?: () => Promise<RelayTransportFactory>;
  [reportEndpoint]?: (endpoint: string) => void;
}

/** The options {@link openRelaySession} reads: the public ones, plus the internal hooks. */
export type RelaySessionOptions = ConnectRelayOptions & RelaySessionHooks;

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
  const given = options.url === undefined ? undefined : endpointUrl(options.url);
  if (!given && !options.getToken) throw new TypeError(noRoot('there is no getToken'));

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
  const establish = async (): Promise<{ session: T; transport: MoqtTransport; endpoint: string }> => {
    let issued: Issued | undefined;
    if (!given?.searchParams.has('jwt') && options.getToken) {
      const resolved = await options.getToken();

      assertOpen();
      issued = credential(resolved);
    }
    const { url, endpoint, expiresAt: expiry } = dialTarget(given, issued);
    expiresAt = expiry;

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
    return { session, transport, endpoint };
  };

  let connected: Awaited<ReturnType<typeof establish>>;

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
  options[reportEndpoint]?.(connected.endpoint);

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

/** A token the provider returned, and its expiry if the provider gave one. */
interface Issued {
  token: string;
  expiresAt?: Date;
}

/** Check what `getToken` returned and split it into the token and its expiry, if given. */
function credential(resolved: unknown): Issued {
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
 * Where a connection dials: the endpoint as given, or with none DEFAULT_ENDPOINT rooted at the token's `root` claim
 * ("Token-rooted endpoint" in spec/behavior.md). `url` carries the provider's token in `jwt`, and `endpoint` is the URL
 * before it was added. Without `issued`, the token is the `jwt` already on the endpoint, if any.
 */
function dialTarget(
  given: URL | undefined,
  issued: Issued | undefined,
): { url: URL; endpoint: string; expiresAt: Date | undefined } {
  const token = issued ? issued.token : (given?.searchParams.get('jwt') ?? '');
  const claims = jwtClaims(token);
  const url = given ?? tokenRootedEndpoint(token, claims);
  const endpoint = url.href;
  if (issued && token.length > 0) url.searchParams.set('jwt', token);
  return { url, endpoint, expiresAt: issued?.expiresAt ?? claimsExpiry(claims) };
}

/** Why a connection given no endpoint has none to dial, and what to do instead. */
const noRoot = (reason: string): string =>
  `With no url, the endpoint is rooted at the token's root claim, but ${reason}; pass url to choose the endpoint yourself`;

/**
 * DEFAULT_ENDPOINT rooted at the token's `root` claim, for a connection given no endpoint. The root's fields, without
 * its leading and trailing `/`, follow roomEndpoint's rule, and an empty root is the whole relay.
 */
function tokenRootedEndpoint(token: string, claims: JwtClaims | undefined): URL {
  if (token.length === 0) throw new TypeError(noRoot('getToken returned no token'));
  if (!claims) throw new TypeError(noRoot('the token is not a JWT with a JSON object payload'));
  const { root } = claims;
  if (typeof root !== 'string') throw new TypeError(noRoot('the token has no string root claim'));
  const room = root.replace(/^\/+|\/+$/g, '');
  if (room.length === 0) return new URL(DEFAULT_ENDPOINT);
  try {
    return new URL(roomEndpoint(DEFAULT_ENDPOINT, room.split('/')));
  } catch (error) {
    const Refusal = error instanceof RangeError ? RangeError : TypeError;
    throw new Refusal(noRoot(`its root cannot be an endpoint path: ${(error as Error).message}`), { cause: error });
  }
}

/** A JWT part: unpadded base64url (RFC 7515 §2). */
const BASE64URL = /^[A-Za-z0-9_-]+$/;

/** The claims the SDK reads, unchecked. */
interface JwtClaims {
  exp?: unknown;
  root?: unknown;
}

/**
 * A JWT's claims, read without verifying the token: the relay verifies it, and the SDK only reads when the session will
 * end and, with no endpoint, where to dial. Anything that is not a JWT with a JSON object payload has none.
 */
function jwtClaims(token: string): JwtClaims | undefined {
  const parts = token.split('.');
  const payload = parts[1];
  if (parts.length !== 3 || payload === undefined || !BASE64URL.test(payload)) return undefined;
  try {
    const base64 = payload.replace(/-/g, '+').replace(/_/g, '/');
    const json = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(
      Uint8Array.from(atob(base64.padEnd(base64.length + ((4 - (base64.length % 4)) % 4), '=')), (c) =>
        c.charCodeAt(0),
      ),
    );
    const claims: unknown = JSON.parse(json);
    return typeof claims === 'object' && claims !== null && !Array.isArray(claims) ? (claims as JwtClaims) : undefined;
  } catch {
    return undefined;
  }
}

/** When the token expires: its numeric `exp` claim. Anything else has no known expiry. */
function claimsExpiry(claims: JwtClaims | undefined): Date | undefined {
  const exp = claims?.exp;
  if (typeof exp !== 'number' || !Number.isFinite(exp)) return undefined;
  const expiresAt = new Date(exp * 1000);
  return Number.isNaN(expiresAt.getTime()) ? undefined : expiresAt;
}
