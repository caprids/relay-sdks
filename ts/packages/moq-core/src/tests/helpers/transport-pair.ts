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

/** Create two transports wired back-to-back. */
export function createTransportPair(): TransportPair {
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
      return pipe.writable;
    },
    async createBidirectionalStream() {
      if (closed) throw new Error('transport closed');

      const localToRemote = new TransformStream<Uint8Array, Uint8Array>();
      const remoteToLocal = new TransformStream<Uint8Array, Uint8Array>();

      remote.incomingBidi.push({ readable: localToRemote.readable, writable: remoteToLocal.writable });
      return { readable: remoteToLocal.readable, writable: localToRemote.writable };
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
