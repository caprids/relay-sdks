/**
 * WHATWG streams over `@moq/qmux`'s stream halves (spec/transport-adapter.md, "Streams").
 *
 * Each half is wrapped rather than handed over as it is, for three rules:
 *
 * - A reset or a stop carries the code TA-17 gives, checked here: `@moq/qmux` wraps any number to 32 bits.
 * - A send stream's pending write or close settles as soon as the stream is aborted or fails (TA-16, TA-20).
 *   WHATWG holds a sink's `abort()` until a pending write settles, so a write blocked on flow control would otherwise
 *   hold the abort, and the write slot above it, until the peer granted credit.
 * - A failure that is not the stream's own is reported as a session error once the session is over (TA-12, TA-22):
 *   `@moq/qmux` fails a stream's pending operations with a plain `Error` when the session ends.
 */
import { isStreamError, resetReason } from './errors.js';

/** Turns an error `@moq/qmux` raised on a stream into the one the contract wants (TA-22). */
export type MapFailure = (error: unknown) => unknown;

/** Settles, with the error to report, once the session is over (TA-12). */
export type SessionEnd = Promise<unknown>;

/** A writable over `inner`, one of `@moq/qmux`'s send streams. */
export function sendWritable(inner: WritableStream<Uint8Array>, mapFailure: MapFailure): WritableStream<Uint8Array> {
  const writer = inner.getWriter();
  let fail!: (error: unknown) => void;
  // Settles the pending step at once when the stream is aborted or fails; never resolves.
  const failed = new Promise<never>((_, reject) => {
    fail = reject;
  });
  failed.catch(() => {});
  let aborted = false;
  const abortOnce = (reason: unknown): void => {
    if (aborted) return;
    aborted = true;
    // The reset goes out once `@moq/qmux` has finished a write it already started (Known deviations).
    writer.abort(resetReason(reason)).catch(() => {});
  };
  const pending = <T>(step: Promise<T>): Promise<T> =>
    Promise.race([
      step.catch(async (error: unknown) => {
        // A peer stop fails a write blocked on credit with a plain error just before the stream reports the stop.
        if (!isStreamError(error)) {
          const stored = await errorOf(writer.closed);
          if (isStreamError(stored)) throw stored;
        }
        throw mapFailure(error);
      }),
      failed,
    ]);

  return new WritableStream<Uint8Array>({
    start(controller) {
      controller.signal.addEventListener(
        'abort',
        () => {
          fail(controller.signal.reason);
          abortOnce(controller.signal.reason);
        },
        { once: true },
      );
      // A peer STOP_SENDING, or the session's end, errors the stream even when nothing is in flight (TA-20, TA-12).
      writer.closed.catch((error: unknown) => {
        if (aborted) return;
        const mapped = mapFailure(error);
        fail(mapped);
        controller.error(mapped);
      });
    },
    write(chunk) {
      if (!(chunk instanceof Uint8Array)) throw new TypeError('a WebSocket transport stream takes Uint8Array chunks');
      return pending(writer.write(chunk));
    },
    close() {
      return pending(writer.close());
    },
    abort: abortOnce,
  });
}

/** The error `promise` rejected with, if it did within a macrotask; `undefined` otherwise. */
function errorOf(promise: Promise<unknown>): Promise<unknown> {
  return Promise.race([
    promise.then(
      () => undefined,
      (error: unknown) => error,
    ),
    new Promise<undefined>((resolve) => setTimeout(resolve, 0)),
  ]);
}

/**
 * A readable over `inner`, one of `@moq/qmux`'s receive streams. It keeps one read of the inner stream outstanding, so a
 * peer reset (TA-19) lands without a read pending: `@moq/qmux` reports one only to a read, after the bytes before it.
 * That holds at most one chunk ahead of the application, which the flow control window already counts as delivered.
 * The session's end errors the readable at once, a chunk held ahead or not (TA-12). Cancelling sends STOP_SENDING with
 * the reason's code and ends the readable (TA-18).
 */
export function recvReadable(
  inner: ReadableStream<Uint8Array>,
  mapFailure: MapFailure,
  sessionEnd: SessionEnd,
): ReadableStream<Uint8Array> {
  const reader = inner.getReader();
  let ended = false;
  let buffered: Uint8Array | undefined;
  let demand: (() => void) | undefined;
  let controller!: ReadableStreamDefaultController<Uint8Array>;

  const settleDemand = (): void => {
    const waiting = demand;
    demand = undefined;
    waiting?.();
  };
  const fail = (error: unknown): void => {
    if (ended) return;
    ended = true;
    // Bytes not yet delivered may be discarded on a reset (RFC 9000 §3.2).
    buffered = undefined;
    try {
      controller.error(mapFailure(error));
    } catch {}
    settleDemand();
  };
  const deliver = (): void => {
    if (!buffered || !demand) return;
    const chunk = buffered;
    buffered = undefined;
    controller.enqueue(chunk);
    settleDemand();
    readAhead();
  };
  const readAhead = (): void => {
    reader.read().then((result) => {
      if (ended) return;
      if (result.done) {
        ended = true;
        controller.close();
        settleDemand();
        return;
      }
      buffered = result.value;
      deliver();
    }, fail);
  };

  return new ReadableStream<Uint8Array>(
    {
      start(started) {
        controller = started;
        readAhead();
        sessionEnd.then(fail);
      },
      pull() {
        if (ended) return;
        return new Promise<void>((resolve) => {
          demand = resolve;
          deliver();
        });
      },
      async cancel(reason) {
        ended = true;
        buffered = undefined;
        settleDemand();
        await reader.cancel(resetReason(reason));
      },
    },
    { highWaterMark: 0 },
  );
}

/** Both halves of one of `@moq/qmux`'s bidirectional streams. */
export function bidirectional(
  stream: { readable: ReadableStream<Uint8Array>; writable: WritableStream<Uint8Array> },
  mapFailure: MapFailure,
  sessionEnd: SessionEnd,
): { readable: ReadableStream<Uint8Array>; writable: WritableStream<Uint8Array> } {
  return {
    readable: recvReadable(stream.readable, mapFailure, sessionEnd),
    writable: sendWritable(stream.writable, mapFailure),
  };
}
