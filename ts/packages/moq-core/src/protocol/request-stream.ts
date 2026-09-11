/**
 * Per-request bidirectional-stream lifecycle (moq-transport draft-20 §3.3).
 *
 * The control model is request-stream based: each request (SUBSCRIBE, FETCH, TRACK_STATUS, …) opens its own
 * bidirectional stream, the first message on it is the request, and responses (SUBSCRIBE_OK / REQUEST_OK /
 * REQUEST_ERROR / PUBLISH_DONE / GOAWAY) arrive as control messages on the same stream. There is no UNSUBSCRIBE message
 * — cancellation IS the stream lifecycle: abort our sending direction (RESET_STREAM) and cancel the receiving direction
 * (STOP_SENDING), per §3.3.3.
 *
 * Callback-shaped, no signals — the session driver binds this to its own bookkeeping; SPF actors bind signals at the
 * `playback/` layer.
 */
import { type ControlMessage, ControlMessageDeframer, decodeControlMessage } from './control-messages.js';
import { MoqtProtocolError } from './errors.js';

/** Structural subset of `WebTransportBidirectionalStream`. */
export interface BidirectionalStreamLike {
  readable: ReadableStream<Uint8Array>;
  writable: WritableStream<Uint8Array>;
}

export interface RequestStreamHandlers {
  onMessage(message: ControlMessage): void;
  /**
   * The peer FINed its direction. Per §3.3.2 this signals the request is complete; an endpoint that receives a FIN
   * before all required messages treats the request as failed — that judgement is the caller's (it knows which messages
   * arrived).
   */
  onFin?(): void;
  /** Read failure, stream reset, or a decode error (`MoqtProtocolError`). */
  onError?(error: unknown): void;
}

export interface RequestStream {
  /** Send a follow-up message (e.g. REQUEST_UPDATE) on the request stream. */
  send(bytes: Uint8Array): Promise<void>;
  /** Gracefully close our sending direction (FIN). Not a cancellation. */
  finWrite(): Promise<void>;
  /**
   * Cancel the request (§3.3.3): abort the sending direction and stop reading the receiving direction. `reason` is
   * surfaced to the transport — a DOM caller passes a `WebTransportError` carrying the stream error code; tests pass
   * anything.
   */
  cancel(reason?: unknown): void;
  readonly cancelled: boolean;
}

/**
 * Open a request over an established bidirectional stream: write the encoded request message, then pump incoming
 * control messages to `handlers.onMessage` until FIN, reset, or cancellation.
 */
export function openRequestStream(
  stream: BidirectionalStreamLike,
  requestMessage: Uint8Array,
  handlers: RequestStreamHandlers
): RequestStream {
  const writer = stream.writable.getWriter();
  // Acquired here (not inside the loop) so `cancel()` can cancel through
  // the reader — cancelling a locked stream directly throws.
  const reader = stream.readable.getReader();
  let cancelled = false;
  let finished = false;

  const readLoop = async () => {
    const deframer = new ControlMessageDeframer();

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        for (const frame of deframer.push(value)) {
          handlers.onMessage(decodeControlMessage(frame));
        }
      }

      if (!cancelled) {
        // A FIN that lands mid-frame is a truncated response, not a clean
        // completion — surfacing it as FIN would leave the request pending
        // with neither response nor error.
        if (deframer.pendingBytes > 0) {
          handlers.onError?.(new MoqtProtocolError('request stream ended mid-control-message'));
        } else {
          handlers.onFin?.();
        }
      }
    } catch (error) {
      if (!cancelled) handlers.onError?.(error);
    } finally {
      reader.releaseLock();
    }
  };

  const firstWrite = writer.write(requestMessage).catch((error) => {
    if (!cancelled) handlers.onError?.(error);
  });

  void readLoop();

  return {
    async send(bytes: Uint8Array): Promise<void> {
      await firstWrite;
      await writer.write(bytes);
    },

    async finWrite(): Promise<void> {
      if (finished || cancelled) return;

      finished = true;
      await firstWrite;
      await writer.close();
    },

    cancel(reason?: unknown): void {
      if (cancelled) return;

      cancelled = true;

      if (!finished) {
        finished = true;
        writer.abort(reason).catch(() => {});
      }

      // STOP_SENDING on the receiving direction. The read loop's pending
      // read resolves as done; `cancelled` suppresses the FIN callback.
      reader.cancel(reason).catch(() => {});
    },

    get cancelled(): boolean {
      return cancelled;
    },
  };
}
