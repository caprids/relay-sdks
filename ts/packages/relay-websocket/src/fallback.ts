/**
 * WebTransport first, then QMux over WebSocket racing it: the fallback policy (spec/behavior.md, "Transport
 * fallback"). One transport stands for whichever dial wins, so the SDK's connect call sees a single factory.
 */
import type { BidirectionalStreamLike, MoqtSendStreamOptions, MoqtTransport } from '@caprid/moq-core';
import { type CloseInfo, WebSocketSessionError } from './errors.js';
import { WebSocketTransport } from './transport.js';

/** The transport one dial runs on: the runtime's `WebTransport`, or the WebSocket adapter. */
type Carrier = MoqtTransport & { readonly protocol: string; readonly ready: Promise<void> };

/** Which transport a {@link FallbackTransport} runs on. */
export type FallbackCarrier = 'webtransport' | 'websocket';

/** Options for {@link withWebSocketFallback}. */
export interface WebSocketFallbackOptions {
  /**
   * The WebTransport dial's head start, in milliseconds: if its handshake has not completed by then, the WebSocket dial
   * starts too and the first to be ready carries the session. Default 250, the Connection Attempt Delay of Happy
   * Eyeballs (RFC 8305). 0 starts both dials at once. A WebTransport handshake that fails sooner starts the WebSocket
   * dial straight away.
   */
  webSocketDelayMs?: number;
  /**
   * Called once when a connection ends up on WebSocket, with why WebTransport was not used: its handshake failed, the
   * WebSocket was ready first (both an error), or the runtime has no usable `WebTransport` (`undefined`). For logging;
   * it cannot change the outcome.
   */
  onFallback?: (reason: unknown) => void;
}

/** The runtime's WebTransport, when it can negotiate protocols; Deno 2.x has one that cannot. */
function usableWebTransport(): typeof WebTransport | undefined {
  if (typeof WebTransport === 'undefined') return undefined;
  return 'protocol' in WebTransport.prototype ? WebTransport : undefined;
}

/**
 * The WebSocket delay the options give, 250 ms when unset; a TypeError or RangeError for one a timer cannot hold, or
 * for `handshakeTimeoutMs`, which it replaced.
 */
export function assertFallbackOptions(options: WebSocketFallbackOptions): number {
  if ('handshakeTimeoutMs' in options)
    throw new TypeError(
      'handshakeTimeoutMs was replaced by webSocketDelayMs: the WebSocket dial now races WebTransport',
    );
  const value: unknown = options.webSocketDelayMs;
  if (value === undefined) return 250;
  if (typeof value !== 'number') throw new TypeError('webSocketDelayMs must be a number of milliseconds');
  if (!Number.isFinite(value) || value < 0 || value > 2_147_483_647)
    throw new RangeError('webSocketDelayMs must be between 0 and 2147483647 milliseconds');
  return value;
}

class WebTransportOutpaced extends Error {
  override readonly name = 'WebTransportOutpaced';
}

function closeQuietly(transport: Carrier, info?: { closeCode?: number; reason?: string }): void {
  try {
    transport.close(info);
  } catch {
    // A failed handshake may leave a transport that already refuses close().
  }
}

/**
 * A transport that dials WebTransport and, unless that is ready within the WebSocket delay, QMux over WebSocket
 * alongside it: the first ready carries the session and the other is closed. Before `ready` it stands in for the dials
 * in progress; after, it is the winner's.
 */
export class FallbackTransport implements MoqtTransport {
  /** Resolves once one of the dials is ready; rejects with the WebSocket dial's failure, or when closed first. */
  readonly ready: Promise<void>;
  /** The winner's `closed`; rejects with the dial's failure when neither dial came up (TA-9). */
  readonly closed: Promise<CloseInfo>;
  /** The winner's incoming unidirectional streams. */
  readonly incomingUnidirectionalStreams: ReadableStream<ReadableStream<Uint8Array>>;
  /** The winner's incoming bidirectional streams. */
  readonly incomingBidirectionalStreams: ReadableStream<BidirectionalStreamLike>;

  /** The dials still in progress. */
  readonly #dials = new Set<Carrier>();
  #winner: Carrier | undefined;
  #carrier: FallbackCarrier | undefined;
  /** Ends the race without a winner; unset once it has ended. */
  #cancel: ((info?: { closeCode?: number; reason?: string }) => void) | undefined;
  readonly #won: Promise<Carrier>;

  constructor(url: string, protocols: readonly string[], options: WebSocketFallbackOptions = {}) {
    const delayMs = assertFallbackOptions(options);

    this.#won = this.#race(url, protocols, delayMs, options.onFallback);
    this.#won.catch(() => {});
    this.ready = this.#won.then(() => undefined);
    this.closed = this.#won.then((winner) => winner.closed as Promise<CloseInfo>);
    this.closed.catch(() => {});
    this.incomingUnidirectionalStreams = this.#incoming((winner) => winner.incomingUnidirectionalStreams);
    this.incomingBidirectionalStreams = this.#incoming((winner) => winner.incomingBidirectionalStreams);
  }

  /** The transport the session runs on; undefined until `ready`. */
  get carrier(): FallbackCarrier | undefined {
    return this.#carrier;
  }

  /** The protocol the winner's peer selected (TA-8); empty until `ready` resolves. */
  get protocol(): string {
    return this.#winner?.protocol ?? '';
  }

  async createUnidirectionalStream(options?: MoqtSendStreamOptions): Promise<WritableStream<Uint8Array>> {
    return (await this.#won).createUnidirectionalStream(options);
  }

  async createBidirectionalStream(): Promise<BidirectionalStreamLike> {
    return (await this.#won).createBidirectionalStream();
  }

  /** Close the winner (TA-10); before `ready`, cancel every dial in progress (TA-11). */
  close(info: { closeCode?: number; reason?: string } = {}): void {
    if (this.#winner) closeQuietly(this.#winner, info);
    else this.#cancel?.(info);
  }

  #race(
    url: string,
    protocols: readonly string[],
    delayMs: number,
    onFallback: ((reason: unknown) => void) | undefined,
  ): Promise<Carrier> {
    return new Promise<Carrier>((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      let webTransportPending = false;
      let webTransportFailure: unknown;
      let webSocketStarted = false;
      let webSocketFailure: { error: unknown } | undefined;

      const end = (info?: { closeCode?: number; reason?: string }) => {
        this.#cancel = undefined;
        clearTimeout(timer);
        for (const dial of this.#dials) closeQuietly(dial, info);
        this.#dials.clear();
      };
      this.#cancel = (info) => {
        end(info);
        reject(new WebSocketSessionError('the dial was cancelled'));
      };
      const win = (winner: Carrier, carrier: FallbackCarrier) => {
        // A dial ready after the race ended lost it, or was cancelled.
        if (!this.#cancel) return closeQuietly(winner);
        this.#dials.delete(winner);
        end();
        this.#winner = winner;
        this.#carrier = carrier;
        // Settled before the callback, so a close() from it closes the winner rather than cancelling a dial.
        resolve(winner);
        if (carrier === 'websocket') {
          const reason = webTransportPending
            ? new WebTransportOutpaced('the WebSocket was ready before the WebTransport handshake completed')
            : webTransportFailure;
          try {
            onFallback?.(reason);
          } catch {}
        }
      };
      // Neither dial can still win: the connection fails with the WebSocket dial's failure.
      const failIfLost = () => {
        if (!this.#cancel || webTransportPending || !webSocketFailure) return;
        end();
        reject(webSocketFailure.error);
      };
      const dialWebSocket = () => {
        if (!this.#cancel || webSocketStarted) return;
        webSocketStarted = true;
        clearTimeout(timer);
        let webSocket: WebSocketTransport;
        try {
          webSocket = new WebSocketTransport(toWebSocketUrl(url), protocols);
        } catch (error) {
          webSocketFailure = { error };
          return failIfLost();
        }
        this.#dials.add(webSocket);
        webSocket.ready.then(
          () => win(webSocket, 'websocket'),
          (error: unknown) => {
            this.#dials.delete(webSocket);
            webSocketFailure = { error };
            failIfLost();
          },
        );
      };

      const NativeWebTransport = usableWebTransport();
      let webTransport: Carrier | undefined;
      if (NativeWebTransport) {
        try {
          webTransport = new NativeWebTransport(url, { protocols: [...protocols] }) as unknown as Carrier;
        } catch (error) {
          // A runtime that refuses the dial outright (a policy, a URL it will not take) falls back too.
          webTransportFailure = error;
        }
      }
      if (!webTransport) return dialWebSocket();
      const dial = webTransport;
      dial.closed.catch(() => {});
      this.#dials.add(dial);
      webTransportPending = true;
      timer = setTimeout(dialWebSocket, delayMs);
      dial.ready.then(
        () => win(dial, 'webtransport'),
        (error: unknown) => {
          webTransportPending = false;
          webTransportFailure = error;
          if (this.#dials.delete(dial)) closeQuietly(dial);
          dialWebSocket();
          failIfLost();
        },
      );
    });
  }

  /** The winner's incoming streams, one at a time; the readable ends with the winner's (TA-12). */
  #incoming<S>(select: (winner: Carrier) => ReadableStream<S>): ReadableStream<S> {
    let reader: ReadableStreamDefaultReader<S> | undefined;
    let cancelled: { reason: unknown } | undefined;
    return new ReadableStream<S>(
      {
        start: (controller) => {
          this.#won.then(
            (winner) => {
              // Cancelled before the dial was won: the winner's streams are refused, not left unread.
              if (cancelled) {
                select(winner)
                  .cancel(cancelled.reason)
                  .catch(() => {});
                return;
              }
              reader = select(winner).getReader();
              reader.closed.then(
                () => {
                  try {
                    controller.close();
                  } catch {}
                },
                (error: unknown) => {
                  try {
                    controller.error(error);
                  } catch {}
                },
              );
            },
            (error: unknown) => {
              try {
                controller.error(error);
              } catch {}
            },
          );
        },
        pull: async (controller) => {
          try {
            await this.#won;
          } catch {
            return;
          }
          // `start` took the reader in the turn the dial was won, ahead of any pull.
          const result = await (reader as ReadableStreamDefaultReader<S>).read().catch(() => undefined);
          if (!result || result.done) return;
          try {
            controller.enqueue(result.value);
          } catch {}
        },
        cancel: async (reason) => {
          if (reader) await reader.cancel(reason);
          else cancelled = { reason };
        },
      },
      { highWaterMark: 0 },
    );
  }
}

/** The `wss:` URL for an `https:` endpoint (and `ws:` for `http:`), path and query unchanged. */
export function toWebSocketUrl(url: string): string {
  const parsed = new URL(url);
  switch (parsed.protocol) {
    case 'https:':
    case 'wss:':
      return `wss:${url.slice(parsed.protocol.length)}`;
    case 'http:':
    case 'ws:':
      return `ws:${url.slice(parsed.protocol.length)}`;
    default:
      throw new TypeError(`a WebSocket transport dials an https: URL, not ${parsed.protocol}`);
  }
}
