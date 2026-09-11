import { MOQT_PROTOCOL_ID, type MoqtTransport, type MoqtSessionCallbacks } from '@mux/moq-core';

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
  /** Deadline for token resolution, transport connection, and protocol SETUP. Default 10 seconds. */
  connectTimeoutMs?: number;
  /** Deadline for an initial subscription response, not for silence on an accepted track. Default 10 seconds. */
  requestTimeoutMs?: number;
  createTransport?: RelayTransportFactory;
}

interface Session {
  ready: Promise<void>;
  destroy(): void;
}

function createBrowserTransport(url: string, protocols: string[]) {
  if (typeof WebTransport === 'undefined') {
    throw new Error('WebTransport is unavailable; supply createTransport for this runtime');
  }

  const transport = new WebTransport(url, { protocols });

  return { transport, ready: transport.ready };
}

/** Shared transport/auth lifetime for the subscribe and publish session drivers. */
export async function openRelaySession<T extends Session>(
  options: ConnectRelayOptions,
  createSession: (transport: MoqtTransport, callbacks: Pick<MoqtSessionCallbacks, 'onClosed' | 'onGoaway'>) => T,
) {
  const url = new URL(options.url);
  if (url.protocol !== 'https:') throw new TypeError('Relay endpoints must use HTTPS');
  if (url.hash || url.username || url.password)
    throw new TypeError('Relay endpoints cannot contain fragments or userinfo');

  const timeoutMs = options.connectTimeoutMs ?? 10_000;
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0)
    throw new RangeError('connectTimeoutMs must be a positive finite number');

  const requestTimeoutMs = options.requestTimeoutMs ?? 10_000;
  if (!Number.isFinite(requestTimeoutMs) || requestTimeoutMs <= 0)
    throw new RangeError('requestTimeoutMs must be a positive finite number');

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
    stopError = info.error ?? new Error('Relay connection is closed');
    clearTimeout(timer);
    options.signal?.removeEventListener('abort', onAbort);

    if (session) {
      session.destroy();
    } else {
      try {
        transport?.close();
      } catch {
        // A failed handshake may leave a transport that already refuses close().
      }
    }

    resolveClosed(info);
    rejectConnect(stopError);
  };

  const onAbort = () => finish({ error: options.signal!.reason });
  const timer = setTimeout(() => finish({ error: new Error('Relay connection timed out') }), timeoutMs);

  options.signal?.addEventListener('abort', onAbort, { once: true });

  const assertOpen = (): void => {
    if (stopped) throw stopError;
  };

  const establish = async (): Promise<T> => {
    if (!url.searchParams.has('jwt') && options.getToken) {
      const token = await options.getToken();

      assertOpen();
      if (token !== undefined && typeof token !== 'string')
        throw new TypeError('getToken must return a string or undefined');
      if (token !== undefined && token.length > 0) url.searchParams.set('jwt', token);
    }

    assertOpen();
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
      onClosed: finish,
      onGoaway: () => finish({ error: new Error('Relay requested session migration; open a new connection') }),
    });

    await session.ready;
    assertOpen();
    return session;
  };

  let connected: T;

  try {
    connected = await Promise.race([establish(), interrupted]);
    assertOpen();
  } catch (error) {
    finish({ error });
    throw error;
  }

  clearTimeout(timer);

  return { session: connected, closed, transportClosed: transport!.closed, assertOpen, close: () => finish() };
}
