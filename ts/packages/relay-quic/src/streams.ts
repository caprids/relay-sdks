/**
 * WHATWG streams over the addon's stream halves (spec/transport-adapter.md, "Streams").
 *
 * A send stream resets from `controller.signal`, not from the sink's `abort()`: WHATWG holds `abort()` until a pending
 * write or close settles, and TA-16 needs the reset while a write is blocked on flow control or a close waits for
 * acknowledgement. It watches for STOP_SENDING from the start, so `writer.closed` rejects with the peer's code even
 * when nothing is in flight (TA-20).
 */
import type { BiStream, Failure, RecvStream, SendStream } from './addon.js';
import { resetCode, toError } from './errors.js';

/** The most a single read takes from quinn. */
const READ_CHUNK_BYTES = 64 * 1024;

/** A writable over `send`. */
export function sendWritable(send: SendStream): WritableStream<Uint8Array> {
  let reset = false;
  const resetOnce = (reason: unknown) => {
    if (reset) return;
    reset = true;
    void send.reset(resetCode(reason));
  };
  let signal: AbortSignal | undefined;
  // The abort reason, when the stream was aborted: a pending step it interrupted rejects with that, not the addon's own.
  const failed = (error: Error) => (signal?.aborted ? signal.reason : error);

  return new WritableStream<Uint8Array>({
    start(controller) {
      signal = controller.signal;
      controller.signal.addEventListener('abort', () => resetOnce(controller.signal.reason), { once: true });
      void send.stopped().then((failure) => {
        if (failure) controller.error(toError(failure));
      });
    },
    async write(chunk) {
      if (!(chunk instanceof Uint8Array)) throw new TypeError('a native QUIC stream takes Uint8Array chunks');
      // The addon reads the bytes off the JS thread while the write waits, so it gets a copy of its own: the caller may
      // reuse, transfer or resize its buffer as soon as it likes. Not `slice()`, which a Node Buffer shares.
      const failure = await send.write(new Uint8Array(chunk));
      if (failure) throw failed(toError(failure));
    },
    async close() {
      const failure = await send.finish();
      if (failure) throw failed(toError(failure));
    },
    abort: resetOnce,
  });
}

/**
 * A readable over `recv`. It reads only on demand; between reads a native watch waits for a peer reset (TA-19) or the
 * session's end (TA-12) without reading, so either settles the readable even while nothing reads it. Cancelling sends
 * STOP_SENDING with the reason's code and ends the readable (TA-18).
 */
export function recvReadable(recv: RecvStream): ReadableStream<Uint8Array> {
  let ended = false;
  const end = (controller: ReadableStreamDefaultController<Uint8Array>, failure: Failure | undefined) => {
    if (ended) return;
    ended = true;
    if (failure) controller.error(toError(failure));
    else controller.close();
  };
  const watch = (controller: ReadableStreamDefaultController<Uint8Array>) => {
    void recv.watch().then((failure) => {
      // Bytes not yet read may be discarded on a reset (RFC 9000 §3.2).
      if (failure) end(controller, failure);
    });
  };

  return new ReadableStream<Uint8Array>(
    {
      start: watch,
      async pull(controller) {
        const { chunk, fin, failure } = await recv.read(READ_CHUNK_BYTES);
        if (ended) return;
        if (failure || fin) {
          end(controller, failure);
          return;
        }
        if (chunk) controller.enqueue(new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength));
        watch(controller);
      },
      async cancel(reason) {
        ended = true;
        await recv.stop(resetCode(reason));
      },
    },
    { highWaterMark: 0 },
  );
}

/** Both halves of `stream`. */
export function bidirectional(stream: BiStream): {
  readable: ReadableStream<Uint8Array>;
  writable: WritableStream<Uint8Array>;
} {
  return { writable: sendWritable(stream.takeSend()), readable: recvReadable(stream.takeRecv()) };
}
