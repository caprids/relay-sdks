/** A `MoqtTransport` over one native QUIC dial (spec/transport-adapter.md, "Session lifetime"). */
import type { BidirectionalStreamLike, MoqtTransport } from '@mux/moq-core';
import type { Dial, Failure, Session } from './addon.js';
import { NativeQuicSessionError, safeCode, toError } from './errors.js';
import { bidirectional, recvReadable, sendWritable } from './streams.js';

/** How `closed` resolves after an application close from either end (TA-9). */
export interface CloseInfo {
  /** The application close code. */
  closeCode: number;
  /** The close reason. */
  reason: string;
}

function isFailure(value: object): value is Failure {
  return 'source' in value;
}

/** The transport for one dial. `ready` resolves once the peer selected a protocol; closing first cancels the dial. */
export class NativeQuicTransport implements MoqtTransport {
  readonly incomingUnidirectionalStreams: ReadableStream<ReadableStream<Uint8Array>>;
  readonly incomingBidirectionalStreams: ReadableStream<BidirectionalStreamLike>;
  /** Resolves on an application close from either end and rejects with a session error otherwise (TA-9). */
  readonly closed: Promise<CloseInfo>;
  /** Resolves once the session is established (TA-7); rejects with a session error when the dial fails. */
  readonly ready: Promise<void>;

  readonly #dial: Dial;
  readonly #session: Promise<Session>;
  #established: Session | undefined;
  #closeRequested: CloseInfo | undefined;

  constructor(dial: Dial) {
    this.#dial = dial;
    this.#session = dial.ready().then((result) => {
      if (isFailure(result)) throw toError(result);
      if (this.#closeRequested) {
        // close() came while the handshake finished (TA-11): no session may outlive the cancelled dial.
        result.close(this.#closeRequested.closeCode, this.#closeRequested.reason);
        void result.closed();
        throw cancelled();
      }
      this.#established = result;
      return result;
    });
    this.#session.catch(() => {});
    this.ready = this.#session.then(() => undefined);
    this.closed = this.#session.then(async (session) => this.#closeInfo(await session.closed()));
    this.incomingUnidirectionalStreams = this.#incoming((session) => session.acceptUni(), recvReadable);
    this.incomingBidirectionalStreams = this.#incoming((session) => session.acceptBi(), bidirectional);
  }

  /** The protocol the peer selected (TA-8); empty until `ready` resolves. */
  get protocol(): string {
    return this.#established?.protocol ?? '';
  }

  async createUnidirectionalStream(): Promise<WritableStream<Uint8Array>> {
    const opened = (await this.#session).openUni();
    if (isFailure(opened)) throw toError(opened);
    return sendWritable(opened);
  }

  async createBidirectionalStream(): Promise<BidirectionalStreamLike> {
    const opened = (await this.#session).openBi();
    if (isFailure(opened)) throw toError(opened);
    return bidirectional(opened);
  }

  /** Close with an application CONNECTION_CLOSE (TA-10); before `ready`, cancel the dial instead (TA-11). */
  close({ closeCode = 0, reason = '' }: { closeCode?: number; reason?: string } = {}): void {
    if (this.#closeRequested) return;
    // An application close code is a 32-bit value, as on a WebTransport session.
    const code = Number.isInteger(closeCode) && closeCode >= 0 && closeCode <= 0xffff_ffff ? closeCode : 0;
    this.#closeRequested = { closeCode: code, reason };
    if (this.#established) this.#established.close(code, reason);
    else this.#dial.cancel();
  }

  #closeInfo(ended: Failure): CloseInfo {
    if (ended.kind === 'local') return this.#closeRequested ?? { closeCode: 0, reason: '' };
    if (ended.kind === 'application') return { closeCode: safeCode(ended.code) ?? 0, reason: ended.reason };
    throw toError(ended);
  }

  /** Streams the peer opens, until the session ends: the readable closes after a clean end and errors otherwise (TA-12). */
  #incoming<Native, Stream>(
    accept: (session: Session) => Promise<Native | null>,
    wrap: (stream: Native) => Stream,
  ): ReadableStream<Stream> {
    return new ReadableStream<Stream>(
      {
        pull: async (controller) => {
          let session: Session;
          try {
            session = await this.#session;
          } catch (error) {
            controller.error(error);
            return;
          }
          const stream = await accept(session);
          if (stream !== null) {
            controller.enqueue(wrap(stream));
            return;
          }
          try {
            await this.closed;
            controller.close();
          } catch (error) {
            controller.error(error);
          }
        },
      },
      { highWaterMark: 0 },
    );
  }
}

function cancelled(): NativeQuicSessionError {
  return new NativeQuicSessionError('the dial was cancelled', 'cancelled');
}
