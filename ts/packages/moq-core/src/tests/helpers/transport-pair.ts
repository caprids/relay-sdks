/**
 * Symmetric in-memory `MoqtTransport` pair for loopback tests: two transports whose stream-creation methods surface on
 * the peer's incoming stream queues, so a publish session on one side can talk to a subscribe session on the other over
 * real (in-process) WHATWG streams — the same structural seam a `WebTransport` satisfies in production.
 */
import type { BidirectionalStreamLike } from '../../protocol/request-stream.js';
import type { MoqtTransport } from '../../protocol/session.js';

interface PushChannel<T> {
  stream: ReadableStream<T>;
  push(value: T): void;
}

function pushChannel<T>(): PushChannel<T> {
  let controller!: ReadableStreamDefaultController<T>;
  const stream = new ReadableStream<T>({
    start(c) {
      controller = c;
    },
  });

  return { stream, push: (value) => controller.enqueue(value) };
}

export interface TransportPair {
  client: MoqtTransport;
  server: MoqtTransport;
}

/** One writer-side step as issued: the stream, `write`/`close`/`abort`, and the bytes written. */
export interface PipeEvent {
  stream: WritableStream<Uint8Array>;
  kind: 'write' | 'close' | 'abort';
  bytes: Uint8Array;
}

/** A pipe's writable that records each step into `events` as it is issued; an abort is recorded (and forwarded) at once. */
function recordedWritable(pipe: WritableStream<Uint8Array>, events: PipeEvent[]): WritableStream<Uint8Array> {
  const inner = pipe.getWriter();
  const stream: WritableStream<Uint8Array> = new WritableStream<Uint8Array>({
    start(controller) {
      controller.signal.addEventListener('abort', () => {
        events.push({ stream, kind: 'abort', bytes: new Uint8Array() });
        inner.abort(controller.signal.reason).catch(() => {});
      });
    },
    write(chunk) {
      events.push({ stream, kind: 'write', bytes: chunk });
      return inner.write(chunk);
    },
    close() {
      events.push({ stream, kind: 'close', bytes: new Uint8Array() });
      return inner.close();
    },
  });

  return stream;
}

/**
 * Create two transports wired back-to-back. With `events`, every writable either side creates records its steps there,
 * in issue order.
 */
export function createTransportPair({ events }: { events?: PipeEvent[] } = {}): TransportPair {
  const writable = (pipe: TransformStream<Uint8Array, Uint8Array>) =>
    events ? recordedWritable(pipe.writable, events) : pipe.writable;
  const makeSide = () => ({
    incomingUni: pushChannel<ReadableStream<Uint8Array>>(),
    incomingBidi: pushChannel<BidirectionalStreamLike>(),
    resolveClosed: undefined as ((info: unknown) => void) | undefined,
  });
  const a = makeSide();
  const b = makeSide();

  let closed = false;
  const closedPromises = [a, b].map(
    (side) =>
      new Promise<unknown>((resolve) => {
        side.resolveClosed = resolve;
      })
  );

  const closeBoth = (info?: { closeCode?: number; reason?: string }): void => {
    if (closed) return;

    closed = true;
    a.resolveClosed?.(info);
    b.resolveClosed?.(info);
  };

  const makeTransport = (local: typeof a, remote: typeof b, closedPromise: Promise<unknown>): MoqtTransport => ({
    incomingUnidirectionalStreams: local.incomingUni.stream,
    incomingBidirectionalStreams: local.incomingBidi.stream,
    async createUnidirectionalStream() {
      if (closed) throw new Error('transport closed');

      const pipe = new TransformStream<Uint8Array, Uint8Array>();

      remote.incomingUni.push(pipe.readable);
      return writable(pipe);
    },
    async createBidirectionalStream() {
      if (closed) throw new Error('transport closed');

      const localToRemote = new TransformStream<Uint8Array, Uint8Array>();
      const remoteToLocal = new TransformStream<Uint8Array, Uint8Array>();

      remote.incomingBidi.push({ readable: localToRemote.readable, writable: writable(remoteToLocal) });
      return { readable: remoteToLocal.readable, writable: writable(localToRemote) };
    },
    close(closeInfo) {
      closeBoth(closeInfo);
    },
    closed: closedPromise,
  });

  return {
    client: makeTransport(a, b, closedPromises[0]!),
    server: makeTransport(b, a, closedPromises[1]!),
  };
}
