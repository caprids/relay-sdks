/**
 * WebTransport first, then QMux over WebSocket: the fallback policy (spec/behavior.md, "Transport fallback"). One
 * transport stands for whichever dial wins, so the SDK's connect call sees a single factory.
 */
import type { BidirectionalStreamLike, MoqtTransport } from '@mux/moq-core';
import { type CloseInfo, WebSocketSessionError } from './errors.js';
import { WebSocketTransport } from './transport.js';

/** The transport one dial runs on: the runtime's `WebTransport`, or the WebSocket adapter. */
type Carrier = MoqtTransport & { readonly protocol: string; readonly ready: Promise<void> };

/** Which transport a {@link FallbackTransport} runs on. */
export type FallbackCarrier = 'webtransport' | 'websocket';

/** Options for {@link withWebSocketFallback}. */
export interface WebSocketFallbackOptions {
  /**
   * How long the WebTransport handshake may take before the dial gives up on it and opens a WebSocket instead, in
   * milliseconds. Default 4000: Chromium itself gives up on an unanswered handshake after about 4 seconds. Keep it
   * well inside the connection's `connectTimeoutMs`, which covers both handshakes and SETUP.
   */
  handshakeTimeoutMs?: number;
  /**
   * Called once when a dial falls back to WebSocket, with why WebTransport was not used: its handshake failed or timed
   * out (the error), or the runtime has no usable `WebTransport` (`undefined`). For logging; it cannot stop the
   * fallback.
   */
  onFallback?: (reason: unknown) => void;
}

/** The runtime's WebTransport, when it can negotiate protocols; Deno 2.x has one that cannot. */
function usableWebTransport(): typeof WebTransport | undefined {
  if (typeof WebTransport === 'undefined') return undefined;
  return 'protocol' in WebTransport.prototype ? WebTransport : undefined;
}

/** The handshake timeout `value` gives, 4 s when it is undefined; a TypeError or RangeError for one a timer cannot hold. */
export function assertHandshakeTimeout(value: unknown): number {
  if (value === undefined) return 4_000;
  if (typeof value !== 'number') throw new TypeError('handshakeTimeoutMs must be a number of milliseconds');
  if (!Number.isFinite(value) || value <= 0 || value > 2_147_483_647)
    throw new RangeError('handshakeTimeoutMs must be between 0 (exclusive) and 2147483647 milliseconds');
  return value;
}

class HandshakeTimeout extends Error {
  override readonly name = 'HandshakeTimeout';
}

/**
 * A transport that dials WebTransport and falls back to QMux over WebSocket when that handshake fails or takes longer
 * than the timeout. Before `ready` it stands in for whichever dial is in progress; after, it is the winner's.
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

  #current: Carrier | undefined;
  #carrier: FallbackCarrier | undefined;
  #winner: Carrier | undefined;
  #cancelled = false;
  readonly #won: Promise<Carrier>;

  constructor(url: string, protocols: readonly string[], options: WebSocketFallbackOptions = {}) {
    const timeoutMs = assertHandshakeTimeout(options.handshakeTimeoutMs);

    this.#won = this.#dial(url, protocols, timeoutMs, options.onFallback);
    this.#won.catch(() => {});
    this.ready = this.#won.then(() => undefined);
    this.closed = this.#won.then((winner) => winner.closed as Promise<CloseInfo>);
    this.closed.catch(() => {});
    this.incomingUnidirectionalStreams = this.#incoming((winner) => winner.incomingUnidirectionalStreams);
    this.incomingBidirectionalStreams = this.#incoming((winner) => winner.incomingBidirectionalStreams);
  }

  /** The transport the session runs on: the one in progress before `ready`, the winner after. */
  get carrier(): FallbackCarrier | undefined {
    return this.#carrier;
  }

  /** The protocol the winner's peer selected (TA-8); empty until `ready` resolves. */
  get protocol(): string {
    return this.#winner?.protocol ?? '';
  }

  async createUnidirectionalStream(): Promise<WritableStream<Uint8Array>> {
    return (await this.#won).createUnidirectionalStream();
  }

  async createBidirectionalStream(): Promise<BidirectionalStreamLike> {
    return (await this.#won).createBidirectionalStream();
  }

  /** Close the winner (TA-10); before `ready`, cancel whichever dial is in progress (TA-11). */
  close(info: { closeCode?: number; reason?: string } = {}): void {
    if (!this.#winner) this.#cancelled = true;
    try {
      this.#current?.close(info);
    } catch {
      // A failed handshake may leave a transport that already refuses close().
    }
  }

  async #dial(
    url: string,
    protocols: readonly string[],
    timeoutMs: number,
    onFallback: ((reason: unknown) => void) | undefined,
  ): Promise<Carrier> {
    const NativeWebTransport = usableWebTransport();
    let fallbackReason: unknown;
    let webTransport: (WebTransport & { readonly protocol: string }) | undefined;
    if (NativeWebTransport) {
      try {
        webTransport = new NativeWebTransport(url, { protocols: [...protocols] }) as WebTransport & {
          readonly protocol: string;
        };
      } catch (error) {
        // A runtime that refuses the dial outright (a policy, a URL it will not take) falls back too.
        fallbackReason = error;
      }
    }
    if (webTransport) {
      webTransport.closed.catch(() => {});
      this.#current = webTransport as Carrier;
      this.#carrier = 'webtransport';
      let timer: ReturnType<typeof setTimeout> | undefined;
      const timeout = new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new HandshakeTimeout(`the WebTransport handshake took longer than ${timeoutMs} ms`)),
          timeoutMs,
        );
      });
      try {
        await Promise.race([webTransport.ready, timeout]);
        this.#cancelIfClosed();
        this.#winner = webTransport as Carrier;
        return this.#winner;
      } catch (error) {
        this.#cancelIfClosed();
        fallbackReason = error;
        try {
          webTransport.close();
        } catch {}
      } finally {
        clearTimeout(timer);
      }
    }

    try {
      onFallback?.(fallbackReason);
    } catch {}
    const webSocket = new WebSocketTransport(toWebSocketUrl(url), protocols);
    this.#current = webSocket;
    this.#carrier = 'websocket';
    await webSocket.ready;
    this.#cancelIfClosed();
    this.#winner = webSocket;
    return webSocket;
  }

  /** Throws once close() came while a dial was in progress (TA-11). */
  #cancelIfClosed(): void {
    if (!this.#cancelled) return;
    try {
      this.#current?.close();
    } catch {}
    throw new WebSocketSessionError('the dial was cancelled');
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
