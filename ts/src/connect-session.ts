import { MOQT_PROTOCOL_ID, type MoqtSessionCallbacks, type MoqtTransport } from '@mux/moq-core';

import { RelayError, transportError } from './errors.js';

/** setTimeout stores its delay in a signed 32-bit integer; a larger one wraps and fires at once. */
const MAX_TIMEOUT_MS = 2_147_483_647;

/** Reject a deadline a timer cannot hold, so a larger number never means a shorter wait. */
export function assertTimeoutMs(name: string, value: number): void {
  if (!Number.isFinite(value) || value <= 0 || value > MAX_TIMEOUT_MS) {
    throw new RangeError(`${name} must be between 0 (exclusive) and ${MAX_TIMEOUT_MS}`);
  }
}

/** Supply a browser transport or a compatible implementation in another runtime. */
export type RelayTransportFactory = (
  url: string,
  protocols: string[],
) => { transport: MoqtTransport; ready: Promise<void> };

export interface ConnectRelayOptions {
  /** Relay's HTTPS WebTransport endpoint, including any endpoint path. */
  url: string;
  /**
   * Resolve a connection token. Relay carries it in the endpoint's jwt query parameter.
   * An explicit jwt in the URL wins, and the provider is not called in that case.
   */
  getToken?: () => string | undefined | Promise<string | undefined>;
  /** Aborting closes the connection, including after connectRelay has resolved. */
  signal?: AbortSignal;
  /** Deadline for token resolution, transport connection, and protocol SETUP. Default 10 seconds, maximum 2147483647. */
  connectTimeoutMs?: number;
  /** Deadline for an initial subscription response, not for silence on an accepted track. Default 10 seconds, maximum 2147483647. */
  requestTimeoutMs?: number;
  createTransport?: RelayTransportFactory;
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
  transportClosed: MoqtTransport['closed'];
  /** Throws the closing error once the connection has ended. */
  assertOpen(): void;
  /** Close the connection. Idempotent. */
  close(): void;
}

function createBrowserTransport(url: string, protocols: string[]) {
  if (typeof WebTransport === 'undefined') {
    throw new RelayError(
      'TRANSPORT_UNAVAILABLE',
      'WebTransport is unavailable; supply createTransport for this runtime',
      'connecting',
    );
  }

  const transport = new WebTransport(url, { protocols });

  return { transport, ready: transport.ready };
}

/** Shared transport/auth lifetime for the subscribe and publish session drivers. */
export async function openRelaySession<T extends Session>(
  options: ConnectRelayOptions,
  createSession: (transport: MoqtTransport, callbacks: Pick<MoqtSessionCallbacks, 'onClosed' | 'onGoaway'>) => T,
): Promise<RelaySessionLifetime<T>> {
  const url = new URL(options.url);
  if (url.protocol !== 'https:') throw new TypeError('Relay endpoints must use HTTPS');
  if (url.hash || url.username || url.password)
    throw new TypeError('Relay endpoints cannot contain fragments or userinfo');

  const timeoutMs = options.connectTimeoutMs ?? 10_000;
  assertTimeoutMs('connectTimeoutMs', timeoutMs);
  assertTimeoutMs('requestTimeoutMs', options.requestTimeoutMs ?? 10_000);

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

  const finish = (info: { error?: unknown } = {}): void => {
    if (stopped) return;

    stopped = true;
    stopError =
      info.error !== undefined ? info.error : new RelayError('CLOSED', 'Relay connection is closed', 'connection');
    clearTimeout(timer);
    options.signal?.removeEventListener('abort', onAbort);

    // Settle before synchronous user callbacks run during session teardown.
    resolveClosed(info);
    rejectConnect(stopError);

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

    assertOpen();
    usingTransport = true;
    const created = (options.createTransport ?? createBrowserTransport)(url.href, [MOQT_PROTOCOL_ID]);

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

    session = createSession(transport, {
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
    });

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
    transportClosed: connected.transport.closed,
    assertOpen,
    close: () => finish(),
  };
}
