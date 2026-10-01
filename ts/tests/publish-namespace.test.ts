import { type BidirectionalStreamLike, createMoqtSession, type MoqtObject, type MoqtTransport } from '@mux/moq-core';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { solicitNamespace } from '../packages/moq-core/src/tests/helpers/raw-peer.js';
import { createTransportPair, withServerRequestIds } from '../packages/moq-core/src/tests/helpers/transport-pair.js';
import { connectRelayPublisher, RelayError } from '../src/index.js';

const namespace = ['live', 'example'];
const cleanup: (() => void | Promise<void>)[] = [];

afterEach(async () => {
  for (const dispose of cleanup.splice(0).reverse()) await dispose();
});

async function connectPublisher() {
  const pair = createTransportPair();
  const subscriber = createMoqtSession(withServerRequestIds(pair.server));
  const publisher = await connectRelayPublisher({
    url: 'https://relay.example.com/moq',
    namespace,
    createTransport: () => ({ transport: pair.client, ready: Promise.resolve() }),
  });
  cleanup.push(() => subscriber.destroy());
  cleanup.push(() => publisher.close());
  const track = publisher.registerTrack({ track: 'events', writeTimeoutMs: 100 });
  return { pair, subscriber, publisher, track };
}

type EndOrder =
  /** `closed` rejects, then the streams fail, in one turn. */
  | 'closed-first'
  /** The streams fail, then `closed` rejects, in one turn: the WebTransport cleanup order. */
  | 'streams-first'
  /** As `streams-first`, but handlers attached to `closed` before the end hear of it only a timer later. */
  | 'closed-handlers-late'
  /** The streams fail, and `closed` rejects only a timer later. */
  | 'closed-later';

/**
 * `inner` with its end under the test's control, as a QUIC idle timeout ends it: every incoming stream's readable fails
 * with `streamError` and `closed` rejects with `closedError`, in `order`.
 */
function terminableTransport(inner: MoqtTransport, order: EndOrder) {
  const readables: TransformStreamDefaultController<Uint8Array>[] = [];
  let rejectClosed!: (error: unknown) => void;
  const closed = new Promise<never>((_, reject) => {
    rejectClosed = reject;
  });
  const late = closed.catch((error: unknown) => new Promise<never>((_, reject) => setTimeout(() => reject(error), 20)));
  late.catch(() => {});
  let ended = false;

  const transport: MoqtTransport = {
    get protocol() {
      return inner.protocol;
    },
    incomingUnidirectionalStreams: inner.incomingUnidirectionalStreams,
    incomingBidirectionalStreams: inner.incomingBidirectionalStreams.pipeThrough(
      new TransformStream<BidirectionalStreamLike, BidirectionalStreamLike>({
        transform(stream, controller) {
          const gate = new TransformStream<Uint8Array, Uint8Array>({
            start: (readable) => void readables.push(readable),
          });
          void stream.readable.pipeTo(gate.writable).catch(() => {});
          controller.enqueue({ readable: gate.readable, writable: stream.writable });
        },
      }),
    ),
    createUnidirectionalStream: () => inner.createUnidirectionalStream(),
    createBidirectionalStream: () => inner.createBidirectionalStream(),
    close: (closeInfo) => inner.close(closeInfo),
    get closed() {
      return order === 'closed-handlers-late' && !ended ? late : closed;
    },
  };

  const failStreams = (error: unknown) => {
    for (const readable of readables) readable.error(error);
  };

  return {
    transport,
    terminate(streamError: unknown, closedError: unknown) {
      ended = true;
      if (order === 'closed-first') {
        rejectClosed(closedError);
        failStreams(streamError);
      } else if (order === 'closed-later') {
        failStreams(streamError);
        setTimeout(() => rejectClosed(closedError), 20);
      } else {
        failStreams(streamError);
        rejectClosed(closedError);
      }
    },
  };
}

describe('publisher transport end with a live announce', () => {
  // Issue #159: a transport end fails the solicitation stream too. That is connection loss, not the relay withdrawing
  // the namespace, whichever of the stream failure and the `closed` handler the session sees first.
  it.each([
    ['closed-first', 'a plain stream error'],
    ['streams-first', 'a plain stream error'],
    ['closed-handlers-late', 'a plain stream error'],
    ['closed-later', "a WebTransportError with source 'session'"],
    ['closed-later', "a shape-only error with source 'session' (TA-22, TA-23)"],
  ] as const)('reports TRANSPORT_ERROR when %s, with %s', async (order, streamErrorKind) => {
    const pair = createTransportPair();
    const terminable = terminableTransport(pair.client, order);
    const subscriber = createMoqtSession(withServerRequestIds(pair.server));
    const publisher = await connectRelayPublisher({
      url: 'https://relay.example.com/moq',
      namespace,
      createTransport: () => ({ transport: terminable.transport, ready: Promise.resolve() }),
    });
    cleanup.push(() => subscriber.destroy());
    cleanup.push(() => publisher.close());
    publisher.registerTrack({ track: 'events' });
    const solicitation = await solicitNamespace(pair.server, ['live']);
    await vi.waitFor(() => expect(solicitation.received).toHaveLength(2));
    await publisher.announced;

    const idle = new Error('Connection lost: idle timeout');
    const streamError =
      streamErrorKind === 'a plain stream error'
        ? idle
        : streamErrorKind === "a WebTransportError with source 'session'"
          ? Object.assign(new Error('The session is closed.'), { name: 'WebTransportError', source: 'session' })
          : Object.assign(new Error('The session is closed.'), { source: 'session' });
    terminable.terminate(streamError, idle);

    const { error } = await publisher.closed;
    expect(error).toBeInstanceOf(RelayError);
    expect(error).toMatchObject({ code: 'TRANSPORT_ERROR', phase: 'connection', cause: idle });
  });
});

describe('publisher namespace coverage', () => {
  it('shares namespace withdrawal with closure and pending operations', async () => {
    const { pair, publisher, track } = await connectPublisher();
    const solicitation = await solicitNamespace(pair.server, ['live']);
    await vi.waitFor(() => expect(solicitation.received).toHaveLength(2));
    const pending = track.write(new Uint8Array([1])).catch((error: unknown) => error);
    const reset = new Error('peer reset');
    await solicitation.reset(reset);
    const { error } = await publisher.closed;
    expect(error).toBeInstanceOf(RelayError);
    // The withdrawal carries the peer's reset as its cause, as a stream error with the reset's code (TA-19).
    expect(error).toMatchObject({
      code: 'NAMESPACE_ENDED',
      phase: 'connection',
      cause: { cause: { source: 'stream', streamErrorCode: 0 } },
    });
    expect(await pending).toBe(error);
    await expect(track.write(new Uint8Array([2]))).rejects.toBe(error);
    expect(() => publisher.registerTrack({ track: 'another' })).toThrow(error as RelayError);
  });

  it.each(['write', 'openGroup'] as const)(
    '%s distinguishes an uncovered namespace and recovers after a later matching solicitation',
    async (method) => {
      const { pair, subscriber, track } = await connectPublisher();
      // Raw ids clear of the subscriber's (1, 3, ...): one session, one request id space (§10.1).
      const unrelated = await solicitNamespace(pair.server, ['other'], 101);
      await vi.waitFor(() => expect(unrelated.received.map((message) => message.kind)).toEqual(['request-ok']));

      const pending = method === 'write' ? track.write(new Uint8Array([1])) : track.openGroup();
      await expect(pending).rejects.toMatchObject({ code: 'NAMESPACE_NOT_SOLICITED', phase: 'waiting-for-demand' });
      expect(track.closed).toBe(false);

      const matching = await solicitNamespace(pair.server, ['live'], 103);
      await vi.waitFor(() =>
        expect(matching.received.map((message) => message.kind)).toEqual(['request-ok', 'namespace']),
      );
      const objects: MoqtObject[] = [];
      subscriber.subscribe(
        { trackNamespace: namespace, trackName: 'events' },
        { onObject: (object) => objects.push(object) },
      );
      if (method === 'write') {
        await track.write(new Uint8Array([2]));
      } else {
        const group = await track.openGroup();
        await group.write(new Uint8Array([2]));
        await group.close();
      }
      await vi.waitFor(() => expect(objects).toHaveLength(1));
      expect(objects[0]?.payload).toEqual(new Uint8Array([2]));
    },
  );

  it('settles announced only once a solicitation covers the namespace', async () => {
    const { pair, publisher } = await connectPublisher();
    const unrelated = await solicitNamespace(pair.server, ['other']);
    await vi.waitFor(() => expect(unrelated.received.map((message) => message.kind)).toEqual(['request-ok']));
    let settled = false;
    void publisher.announced.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );
    await new Promise((resolve) => setTimeout(resolve, 20));
    // Solicited, but not for us: the announce has nowhere to go and the fact has not happened.
    expect(settled).toBe(false);

    const matching = await solicitNamespace(pair.server, ['live'], 3);
    await vi.waitFor(() =>
      expect(matching.received.map((message) => message.kind)).toEqual(['request-ok', 'namespace']),
    );
    await expect(publisher.announced).resolves.toBeUndefined();
  });

  it('keeps announced pending for an uncovered namespace until the connection closes', async () => {
    const { pair, publisher } = await connectPublisher();
    const unrelated = await solicitNamespace(pair.server, ['other']);
    await vi.waitFor(() => expect(unrelated.received.map((message) => message.kind)).toEqual(['request-ok']));

    await publisher.close();

    await expect(publisher.announced).rejects.toMatchObject({ code: 'CLOSED' });
  });

  it.each([false, true])(
    'keeps an ordinary demand timeout when a matching solicitation exists: %s',
    async (solicit) => {
      const { pair, track } = await connectPublisher();
      if (solicit) {
        const matching = await solicitNamespace(pair.server, ['live']);
        await vi.waitFor(() => expect(matching.received).toHaveLength(2));
      }
      await expect(track.write(new Uint8Array([1]))).rejects.toMatchObject({
        code: 'WRITE_TIMEOUT',
        phase: 'waiting-for-demand',
      });
      expect(track.closed).toBe(false);
    },
  );
});
