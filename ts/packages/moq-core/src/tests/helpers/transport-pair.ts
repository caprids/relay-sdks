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
  end(error?: unknown): void;
}

function pushChannel<T>(): PushChannel<T> {
  let controller!: ReadableStreamDefaultController<T>;
  let ended = false;
  const stream = new ReadableStream<T>({
    start(c) {
      controller = c;
    },
    cancel() {
      // The reader cancelled the queue, which closed it: nothing more is pushed and teardown leaves it be.
      ended = true;
    },
  });

  return {
    stream,
    push: (value) => {
      if (!ended) controller.enqueue(value);
    },
    end: (error) => {
      if (ended) return;

      ended = true;
      if (error === undefined) controller.close();
      else controller.error(error);
    },
  };
}

/**
 * A stream or session failure, in the shape the transport adapter contract gives every adapter's errors (TA-22):
 * `source` says whether one stream or the whole session failed, and a stream failure carries the peer's code.
 */
export class PairTransportError extends Error {
  override readonly name = 'PairTransportError';
  /** `'stream'` when the peer reset or stopped one stream, `'session'` when the session has ended. */
  readonly source: 'stream' | 'session';
  /** The peer's reset or stop code on a stream failure, `null` on a session failure. */
  readonly streamErrorCode: number | null;
  /** The application close code, on a session failure that reports a close. */
  readonly closeCode: number | undefined;
  /** The application close reason, on a session failure that reports a close. */
  readonly reason: string | undefined;

  constructor(
    message: string,
    init: { source: 'stream' | 'session'; streamErrorCode?: number; closeCode?: number; reason?: string }
  ) {
    super(message);
    this.source = init.source;
    this.streamErrorCode = init.streamErrorCode ?? null;
    this.closeCode = init.closeCode;
    this.reason = init.reason;
  }
}

/** The code a reset or stop carries for `reason` (TA-17): its `streamErrorCode` when that is a 32-bit unsigned integer, else 0. */
export function streamErrorCodeOf(reason: unknown): number {
  const code =
    typeof reason === 'object' && reason !== null
      ? (reason as { streamErrorCode?: unknown }).streamErrorCode
      : undefined;

  return typeof code === 'number' && Number.isInteger(code) && code >= 0 && code <= 0xffff_ffff ? code : 0;
}

/** One direction of one stream: what the sender writes, the receiver reads, with reset and stop carrying codes. */
interface Pipe {
  readable: ReadableStream<Uint8Array>;
  writable: WritableStream<Uint8Array>;
  /** The session has ended: every pending step on both halves fails with `error`. */
  end(error: PairTransportError): void;
}

function createPipe(bufferBytes: number): Pipe {
  let readController!: ReadableStreamDefaultController<Uint8Array>;
  let writeController!: WritableStreamDefaultController;
  let waiting: { resolve(): void; reject(error: unknown): void } | undefined;
  // Set while the receiver wants a chunk (a read is waiting, or the buffer has room); settled by the next enqueue.
  let demand: (() => void) | undefined;
  let writerError: unknown;
  const failWriter = (error: unknown) => {
    writerError ??= error;
    writeController.error(error);
    waiting?.reject(error);
    waiting = undefined;
  };

  // The receive buffer: a write resolves once its bytes are in it, and waits while it is full (TA-14).
  const readable = new ReadableStream<Uint8Array>(
    {
      start(c) {
        readController = c;
      },
      pull() {
        return new Promise<void>((resolve) => {
          demand = resolve;
          waiting?.resolve();
          waiting = undefined;
        });
      },
      cancel(reason) {
        failWriter(
          new PairTransportError('Received STOP_SENDING.', {
            source: 'stream',
            streamErrorCode: streamErrorCodeOf(reason),
          })
        );
      },
    },
    new ByteLengthQueuingStrategy({ highWaterMark: bufferBytes })
  );
  const writable = new WritableStream<Uint8Array>({
    start(c) {
      writeController = c;
      // The signal fires as abort() is called, ahead of any blocked write, so the reset goes out at once (TA-16).
      c.signal.addEventListener('abort', () => {
        readController.error(
          new PairTransportError('Received RESET_STREAM.', {
            source: 'stream',
            streamErrorCode: streamErrorCodeOf(c.signal.reason),
          })
        );
        waiting?.reject(c.signal.reason);
        waiting = undefined;
      });
    },
    async write(chunk) {
      if (writerError !== undefined) throw writerError;

      // A full buffer holds the write until the receiver reads; with no buffer, until a read is waiting.
      if (demand === undefined && (readController.desiredSize ?? 0) <= 0) {
        await new Promise<void>((resolve, reject) => {
          waiting = { resolve, reject };
        });
        // The wait may have been released in the same turn the stream ended: a stop, a reset or the session's end.
        if (writerError !== undefined) throw writerError;
        if (writeController.signal.aborted) throw writeController.signal.reason;
      }

      readController.enqueue(chunk.slice());
      demand?.();
      demand = undefined;
    },
    close() {
      if (writerError !== undefined) throw writerError;

      readController.close();
    },
  });

  return {
    readable,
    writable,
    end(error) {
      readController.error(error);
      failWriter(error);
    },
  };
}

export interface TransportPair {
  client: MoqtTransport;
  server: MoqtTransport;
  /**
   * End the session as a QUIC-layer close or a lost connection would: both ends' `closed` reject with a session
   * error, and so does every pending step on every stream (TA-9, TA-12). A no-op once the session has ended.
   */
  fail(message?: string): void;
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
      // A stop or the session's end reaches this writer between writes too (TA-20).
      inner.closed.catch((error: unknown) => controller.error(error));
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

/** The receive buffer each stream direction gets by default. */
const DEFAULT_BUFFER_BYTES = 64 * 1024;

/**
 * Create two transports wired back-to-back. With `events`, every writable either side creates records its steps there,
 * in issue order. `bufferBytes` is each stream direction's receive buffer: writes resolve until it is full and then
 * wait for the peer to read (TA-14).
 *
 * The pair follows the transport adapter contract except where `spec/transport-adapter.md` ("Test doubles") lists a
 * gap: it has no stream credit (TA-21), and a send stream's close resolves when its FIN is in the peer's buffer rather
 * than on acknowledgement (TA-15).
 */
export function createTransportPair({
  events,
  bufferBytes = DEFAULT_BUFFER_BYTES,
}: {
  events?: PipeEvent[];
  bufferBytes?: number;
} = {}): TransportPair {
  const pipes = new Set<Pipe>();
  const newPipe = (): Pipe => {
    const pipe = createPipe(bufferBytes);

    pipes.add(pipe);
    return pipe;
  };
  const writable = (pipe: Pipe) => (events ? recordedWritable(pipe.writable, events) : pipe.writable);
  const makeSide = () => ({
    incomingUni: pushChannel<ReadableStream<Uint8Array>>(),
    incomingBidi: pushChannel<BidirectionalStreamLike>(),
    settle: undefined as
      | ((outcome: { info: { closeCode: number; reason: string } } | { error: unknown }) => void)
      | undefined,
  });
  const a = makeSide();
  const b = makeSide();

  let ended = false;
  const closedPromises = [a, b].map((side) => {
    const closed = new Promise<unknown>((resolve, reject) => {
      side.settle = (outcome) => ('info' in outcome ? resolve(outcome.info) : reject(outcome.error));
    });

    // A rejection is the session's outcome, not an unhandled failure of whoever made the pair.
    closed.catch(() => {});
    return closed;
  });

  const end = (error: PairTransportError, info?: { closeCode: number; reason: string }): void => {
    if (ended) return;

    ended = true;
    for (const pipe of pipes) pipe.end(error);
    for (const side of [a, b]) {
      side.incomingUni.end(info ? undefined : error);
      side.incomingBidi.end(info ? undefined : error);
      side.settle?.(info ? { info } : { error });
    }
  };

  const closeBoth = (closeInfo?: { closeCode?: number; reason?: string }): void => {
    const info = { closeCode: closeInfo?.closeCode ?? 0, reason: closeInfo?.reason ?? '' };

    end(new PairTransportError('The session is closed.', { source: 'session', ...info }), info);
  };

  const makeTransport = (local: typeof a, remote: typeof b, closedPromise: Promise<unknown>): MoqtTransport => ({
    incomingUnidirectionalStreams: local.incomingUni.stream,
    incomingBidirectionalStreams: local.incomingBidi.stream,
    async createUnidirectionalStream() {
      if (ended) throw new PairTransportError('The session is closed.', { source: 'session' });

      const pipe = newPipe();

      remote.incomingUni.push(pipe.readable);
      return writable(pipe);
    },
    async createBidirectionalStream() {
      if (ended) throw new PairTransportError('The session is closed.', { source: 'session' });

      const localToRemote = newPipe();
      const remoteToLocal = newPipe();

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
    fail(message = 'The connection was lost.') {
      end(new PairTransportError(message, { source: 'session' }));
    },
  };
}

/** How the pair's accepting end answers a dial. */
export interface TransportPairDialOptions {
  /** Answer the handshake at once (default), hold it until `releaseHandshake()`, or refuse it. */
  handshake?: 'accept' | 'hold' | 'refuse';
  /** Select the first offered protocol (default), none, or one that was not offered (TA-7). */
  select?: 'first' | 'none' | 'unoffered';
  /** Passed to {@link createTransportPair}. */
  bufferBytes?: number;
}

/** What the accepting end saw of a dial: the URL and the protocols offered. */
export interface TransportPairDial {
  url: string;
  protocols: string[];
  pair: TransportPair;
}

/** A dial factory over a fresh pair, with the accepting end's view of each dial. */
export interface TransportPairFactory {
  /** Shaped like the SDK's `RelayTransportFactory`: `{ transport, ready, framing }`, the transport with `protocol`. */
  createTransport(
    url: string,
    protocols: string[]
  ): { transport: MoqtTransport & { readonly protocol: string }; ready: Promise<void>; framing: 'webtransport' };
  /** Resolves with the first dial once it is made. */
  readonly dialled: Promise<TransportPairDial>;
  /** Complete a handshake held by `handshake: 'hold'`. A no-op once the dial was cancelled. */
  releaseHandshake(): void;
}

/** The identifier `select: 'unoffered'` answers with. */
const UNOFFERED_PROTOCOL = 'moqt-unoffered';

/**
 * A factory whose dial is answered by a fresh pair's server end, standing in for the factory a real adapter has
 * (TA-1, TA-7, TA-8, TA-11). `ready` resolves on a later task, once the accepting end selected a protocol; a `close()`
 * before then cancels the dial, and both `ready` and `closed` reject with a session error.
 */
export function createTransportPairFactory({
  handshake = 'accept',
  select = 'first',
  bufferBytes,
}: TransportPairDialOptions = {}): TransportPairFactory {
  let resolveDialled!: (dial: TransportPairDial) => void;
  const dialled = new Promise<TransportPairDial>((resolve) => {
    resolveDialled = resolve;
  });
  let release: (() => void) | undefined;

  return {
    dialled,
    releaseHandshake: () => release?.(),
    createTransport(url, protocols) {
      const pair = createTransportPair(bufferBytes === undefined ? {} : { bufferBytes });
      const offered = [...protocols];
      let protocol = '';
      let state: 'dialling' | 'open' | 'cancelled' = 'dialling';
      let settleReady!: { resolve(): void; reject(error: unknown): void };
      const ready = new Promise<void>((resolve, reject) => {
        settleReady = { resolve, reject };
      });
      const failDial = (message: string) => {
        state = 'cancelled';
        pair.fail(message);
        settleReady.reject(new PairTransportError(message, { source: 'session' }));
      };
      const answer = () => {
        if (state !== 'dialling') return;
        if (handshake === 'refuse') return failDial('The accepting end refused the session.');

        const selected = select === 'first' ? offered[0] : select === 'unoffered' ? UNOFFERED_PROTOCOL : undefined;

        if (selected === undefined || !offered.includes(selected)) {
          return failDial(
            selected === undefined
              ? 'The peer selected no protocol.'
              : `The peer selected ${selected}, which was not offered.`
          );
        }

        state = 'open';
        protocol = selected;
        settleReady.resolve();
      };
      const client = pair.client;
      const transport = {
        incomingUnidirectionalStreams: client.incomingUnidirectionalStreams,
        incomingBidirectionalStreams: client.incomingBidirectionalStreams,
        createUnidirectionalStream: () => client.createUnidirectionalStream(),
        createBidirectionalStream: () => client.createBidirectionalStream(),
        close(closeInfo?: { closeCode?: number; reason?: string }) {
          if (state === 'dialling') failDial('The session was closed before it was established.');
          else client.close(closeInfo);
        },
        closed: client.closed,
        get protocol() {
          return protocol;
        },
      };

      // The session can end while the dial is held: the peer closing or the connection failing rejects `ready` too.
      const endedWhileDialling = () => {
        if (state !== 'dialling') return;

        state = 'cancelled';
        settleReady.reject(
          new PairTransportError('The session ended before it was established.', { source: 'session' })
        );
      };
      client.closed.then(endedWhileDialling, endedWhileDialling);
      release = answer;
      if (handshake !== 'hold') setTimeout(answer, 0);
      resolveDialled({ url, protocols: offered, pair });
      return { transport, ready, framing: 'webtransport' };
    },
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
