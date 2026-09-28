/**
 * Symmetric in-memory `MoqtTransport` pair for loopback tests: two transports whose stream-creation methods surface on
 * the peer's incoming stream queues, so a publish session on one side can talk to a subscribe session on the other over
 * real (in-process) WHATWG streams — the same structural seam a `WebTransport` satisfies in production.
 */
import { MESSAGE_TYPE } from '../../protocol/control-messages.js';
import type { BidirectionalStreamLike } from '../../protocol/request-stream.js';
import type { MoqtTransport } from '../../protocol/session.js';
import { decodeVarint, encodeVarintInto, varintByteLength } from '../../protocol/varint.js';

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

/** Request-initiating messages whose body opens with the sender's Request ID (§10.1). */
const REQUEST_ID_MESSAGE_TYPES: ReadonlySet<number> = new Set([
  MESSAGE_TYPE.SUBSCRIBE,
  MESSAGE_TYPE.FETCH,
  MESSAGE_TYPE.TRACK_STATUS,
  MESSAGE_TYPE.REQUEST_UPDATE,
  MESSAGE_TYPE.SUBSCRIBE_NAMESPACE,
  MESSAGE_TYPE.PUBLISH_NAMESPACE,
  MESSAGE_TYPE.PUBLISH,
]);

/**
 * Copy `chunk` with the least significant bit of every request-initiating frame's Request ID set. Uses the core's MOQT
 * varint codec. Every length limit of that encoding is a power of two (2^7, 2^14, ...), so an even value and its odd
 * successor always encode to the same length and the field is re-encoded in place.
 */
export function setRequestIdParity(chunk: Uint8Array): Uint8Array {
  const bytes = new Uint8Array(chunk);
  let offset = 0;

  while (offset < bytes.length) {
    const type = decodeVarint(bytes, offset);
    const lengthAt = offset + type.byteLength;
    const bodyStart = lengthAt + 2;

    if (REQUEST_ID_MESSAGE_TYPES.has(type.value)) {
      const requestId = decodeVarint(bytes, bodyStart);
      // Arithmetic, not `| 1`: bitwise operators truncate to 32 bits, and request IDs go up to 2^53-1.
      const odd = requestId.value % 2 === 0 ? requestId.value + 1 : requestId.value;

      if (varintByteLength(odd) !== requestId.byteLength) {
        throw new Error(`request id ${odd} would change the encoded length`);
      }

      encodeVarintInto(bytes, bodyStart, odd);
    }

    offset = bodyStart + ((bytes[lengthAt]! << 8) | bytes[lengthAt + 1]!);
  }

  return bytes;
}

/**
 * Give the subscribe driver a server's request ids. The driver always allocates client (even) ids, and a publish
 * session is always the client too, so it requires the peer's ids to be odd (§10.1). A test that stands the driver in
 * for the relay wraps the driver's transport with this: every request stream the driver opens has each Request ID's
 * least significant bit set on the wire (0 becomes 1, 2 becomes 3). Assumes each write carries whole frames, which is
 * how the driver writes them.
 */
export function withServerRequestIds(transport: MoqtTransport): MoqtTransport {
  return {
    incomingUnidirectionalStreams: transport.incomingUnidirectionalStreams,
    incomingBidirectionalStreams: transport.incomingBidirectionalStreams,
    createUnidirectionalStream: () => transport.createUnidirectionalStream(),
    async createBidirectionalStream() {
      const stream = await transport.createBidirectionalStream();
      const rewrite = new TransformStream<Uint8Array, Uint8Array>({
        transform: (chunk, controller) => controller.enqueue(setRequestIdParity(chunk)),
      });

      void rewrite.readable.pipeTo(stream.writable).catch(() => {});
      return { readable: stream.readable, writable: rewrite.writable };
    },
    close: (closeInfo) => transport.close(closeInfo),
    closed: transport.closed,
  };
}
