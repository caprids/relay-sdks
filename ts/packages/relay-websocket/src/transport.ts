/** A `MoqtTransport` over one QMux-over-WebSocket dial (spec/transport-adapter.md, "Session lifetime"). */

import type { BidirectionalStreamLike, MoqtSendStreamOptions, MoqtTransport } from '@caprid/moq-core';
import Session from '@moq/qmux';
import { type CloseInfo, isStreamError, WebSocketSessionError } from './errors.js';
import { bidirectional, type MapFailure, recvReadable, type SessionEnd, sendWritable } from './streams.js';

/**
 * The QMux wire format every MOQT protocol is offered on. moq-relay pairs `moqt-18` and newer only with `qmux-01`
 * (`QMUX01_ONLY_ALPNS` in `rs/moq-relay/src/websocket.rs`), so each protocol goes out as `qmux-01.<protocol>`.
 */
export const QMUX_VERSION = 'qmux-01';

/**
 * The longest close reason sent, in UTF-8 bytes. `@moq/qmux` refuses to close at all when the APPLICATION_CLOSE frame
 * would not fit one QMux record (16382 bytes by default), so a longer reason is cut short here, as QUIC's own close
 * cuts it to a packet.
 */
const MAX_REASON_BYTES = 1024;

/** `reason`, cut at a character boundary to at most {@link MAX_REASON_BYTES} UTF-8 bytes. */
function boundedReason(reason: string): string {
  const bytes = new TextEncoder().encode(reason);
  if (bytes.byteLength <= MAX_REASON_BYTES) return reason;
  // A cut inside a character leaves a replacement character, which the trailing slice drops.
  const cut = new TextDecoder().decode(bytes.subarray(0, MAX_REASON_BYTES));
  return cut.endsWith('\uFFFD') ? cut.slice(0, -1) : cut;
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/** The transport for one dial. `ready` resolves once the peer selected a protocol; closing first cancels the dial. */
export class WebSocketTransport implements MoqtTransport {
  readonly incomingUnidirectionalStreams: ReadableStream<ReadableStream<Uint8Array>>;
  readonly incomingBidirectionalStreams: ReadableStream<BidirectionalStreamLike>;
  /** Resolves on an application close from either end and rejects with a session error otherwise (TA-9). */
  readonly closed: Promise<CloseInfo>;
  /** Resolves once the session is established and the peer selected an offered protocol (TA-7). */
  readonly ready: Promise<void>;

  readonly #session: Session;
  #protocol = '';
  #established = false;
  #closeRequested: CloseInfo | undefined;
  /** Set once the session is over, by either end: a stream failure after it is a session error (TA-12). */
  #ended = false;
  /** The peer's application close, when it closed the session (TA-24). */
  #peerClose: CloseInfo | undefined;
  /** Set once the session ended by an application close from either end (TA-9). */
  #cleanEnd = false;
  readonly #fail: (error: WebSocketSessionError) => void;
  /** Settles once the session is over, by either end, with the error a receive stream still open reports (TA-12). */
  readonly #sessionEnd: SessionEnd;

  /**
   * @param url The `wss:` (or `ws:`) URL, path and query included: the relay reads both from the upgrade request.
   * @param protocols The MOQT protocols to offer, in preference order, each as `qmux-01.<protocol>`.
   */
  constructor(url: string, protocols: readonly string[]) {
    const offered = [...protocols];
    this.#session = new Session(url, {
      protocols: offered,
      versions: Object.fromEntries(offered.map((protocol) => [protocol, QMUX_VERSION])),
      // Offer only the MOQT protocols: a bare `qmux-01` or `webtransport` would let the relay pick moq-lite in SETUP.
      requireProtocol: true,
    });
    const session = this.#session;

    let resolveReady!: () => void;
    let rejectReady!: (error: unknown) => void;
    this.ready = new Promise<void>((resolve, reject) => {
      resolveReady = resolve;
      rejectReady = reject;
    });
    this.ready.catch(() => {});
    let resolveClosed!: (info: CloseInfo) => void;
    let rejectClosed!: (error: unknown) => void;
    this.closed = new Promise<CloseInfo>((resolve, reject) => {
      resolveClosed = resolve;
      rejectClosed = reject;
    });
    this.closed.catch(() => {});
    this.#fail = (error) => {
      rejectReady(error);
      rejectClosed(error);
    };

    // Registered before any stream exists, so the session's end is known by the time a stream fails because of it.
    this.#sessionEnd = session.closed.then(
      (info) => {
        this.#ended = true;
        this.#cleanEnd = true;
        if (!this.#closeRequested) this.#peerClose = { closeCode: info.closeCode ?? 0, reason: info.reason ?? '' };
        return this.#sessionError(new Error('the session closed'));
      },
      (error: unknown) => {
        this.#ended = true;
        return this.#sessionError(error);
      },
    );

    session.ready.then(
      () => {
        if (this.#closeRequested) return; // close() already failed ready and closed (TA-11).
        // The WebSocket handshake fails on a selection that was not offered; one that selects nothing still opens.
        const selected = session.protocol;
        if (!offered.includes(selected)) {
          session.close({ closeCode: 0, reason: '' });
          this.#fail(
            new WebSocketSessionError(
              selected
                ? `the peer selected ${JSON.stringify(selected)}, which was not offered`
                : 'the peer selected no protocol',
            ),
          );
          return;
        }
        this.#protocol = selected;
        this.#established = true;
        resolveReady();
        session.closed.then(
          (info) =>
            resolveClosed(this.#closeRequested ?? { closeCode: info.closeCode ?? 0, reason: info.reason ?? '' }),
          (error: unknown) => rejectClosed(this.#sessionError(error)),
        );
      },
      (error: unknown) => {
        if (this.#closeRequested) return;
        this.#fail(new WebSocketSessionError(`the WebSocket dial failed: ${messageOf(error)}`, { cause: error }));
      },
    );

    this.incomingUnidirectionalStreams = this.#incoming(session.incomingUnidirectionalStreams, (stream) =>
      recvReadable(stream, this.#mapFailure, this.#sessionEnd),
    );
    this.incomingBidirectionalStreams = this.#incoming(session.incomingBidirectionalStreams, (stream) =>
      bidirectional(stream, this.#mapFailure, this.#sessionEnd),
    );
  }

  /** The protocol the peer selected, without its `qmux-01.` prefix (TA-8); empty until `ready` resolves. */
  get protocol(): string {
    return this.#protocol;
  }

  /** QMux schedules stream data by `sendOrder` as WebTransport does: the highest first, round-robin among equals. */
  async createUnidirectionalStream(options?: MoqtSendStreamOptions): Promise<WritableStream<Uint8Array>> {
    await this.ready;
    try {
      return sendWritable(await this.#session.createUnidirectionalStream(options), this.#mapFailure);
    } catch (error) {
      throw this.#mapFailure(error);
    }
  }

  async createBidirectionalStream(): Promise<BidirectionalStreamLike> {
    await this.ready;
    try {
      return bidirectional(await this.#session.createBidirectionalStream(), this.#mapFailure, this.#sessionEnd);
    } catch (error) {
      throw this.#mapFailure(error);
    }
  }

  /** Close with a QMux APPLICATION_CLOSE (TA-10); before `ready`, cancel the dial instead (TA-11). */
  close({ closeCode = 0, reason = '' }: { closeCode?: number; reason?: string } = {}): void {
    if (this.#closeRequested) return;
    // An application close code is a 32-bit value, as on a WebTransport session.
    const code = Number.isInteger(closeCode) && closeCode >= 0 && closeCode <= 0xffff_ffff ? closeCode : 0;
    const bounded = boundedReason(String(reason));
    this.#closeRequested = { closeCode: code, reason: bounded };
    if (!this.#established) this.#fail(new WebSocketSessionError('the dial was cancelled'));
    try {
      this.#session.close({ closeCode: code, reason: bounded });
    } catch {
      // The session refuses nothing once it has ended, but close() must never throw (TA-10).
    }
  }

  /** A stream failure as the contract reports it: the stream's own as it is, anything else after the end as a session error. */
  readonly #mapFailure: MapFailure = (error) => {
    if (isStreamError(error) || !this.#ended) return error;
    return this.#sessionError(error);
  };

  #sessionError(error: unknown): WebSocketSessionError {
    if (error instanceof WebSocketSessionError) return error;
    const close = this.#peerClose;
    const message = close
      ? `the peer closed the session (code ${close.closeCode}${close.reason ? `, ${close.reason}` : ''})`
      : `the WebSocket session ended: ${messageOf(error)}`;
    return new WebSocketSessionError(message, { cause: error, close });
  }

  /** Streams the peer opens. The readable closes after a clean end and errors otherwise (TA-12), with no read pending. */
  #incoming<Inner, Outer>(inner: ReadableStream<Inner>, wrap: (stream: Inner) => Outer): ReadableStream<Outer> {
    const reader = inner.getReader();
    return new ReadableStream<Outer>(
      {
        start: (controller) => {
          // Either throws once the readable already ended, which needs nothing more.
          reader.closed.then(
            () => {
              try {
                controller.close();
              } catch {}
            },
            (error: unknown) => {
              try {
                // `@moq/qmux` errors its incoming queues after a peer's application close too.
                if (this.#cleanEnd) controller.close();
                else controller.error(this.#sessionError(error));
              } catch {}
            },
          );
        },
        pull: async (controller) => {
          let result: ReadableStreamReadResult<Inner>;
          try {
            result = await reader.read();
          } catch {
            return; // `start` errors the readable as the inner one errors.
          }
          if (result.done) return;
          try {
            controller.enqueue(wrap(result.value));
          } catch {
            // The readable was cancelled while the stream arrived; the session is going with it.
          }
        },
        cancel: async (reason) => {
          await reader.cancel(reason);
        },
      },
      { highWaterMark: 0 },
    );
  }
}
