/**
 * Derived from Video.js MOQT session tests; Copyright 2025-present Video.js contributors.
 * SPDX-License-Identifier: Apache-2.0
 * See packages/moq-core/LICENSE and packages/moq-core/UPSTREAM.json.
 */

import type { BidirectionalStreamLike, MoqtTransport } from '@mux/moq-core';
import { vi } from 'vitest';
import { ByteWriter, StreamReader } from '../../packages/moq-core/src/protocol/bytes.js';
import {
  type ControlMessage,
  ControlMessageDeframer,
  decodeControlMessage,
  encodeSetup,
} from '../../packages/moq-core/src/protocol/control-messages.js';

interface PushChannel<T> {
  stream: ReadableStream<T>;
  push(value: T): void;
  close(): void;
}

function pushChannel<T>(): PushChannel<T> {
  let controller!: ReadableStreamDefaultController<T>;
  const stream = new ReadableStream<T>({
    start(c) {
      controller = c;
    },
  });

  return {
    stream,
    push: (value) => controller.enqueue(value),
    close: () => controller.close(),
  };
}

/** Server-side view of one client-initiated bidirectional stream. */
interface ServerRequestStream {
  /** Decoded control messages received from the client, in order. */
  messages: ControlMessage[];
  /** Resolves when the first client message has been decoded. */
  firstMessage: Promise<ControlMessage>;
  send(bytes: Uint8Array): Promise<void>;
  fin(): Promise<void>;
  reset(reason?: unknown): void;
}

/** A scripted server-side transport: the test plays the relay while the SDK plays the client. */
export interface FakeTransport {
  transport: MoqtTransport;
  /** The SETUP control stream the client opened (first client uni stream). */
  clientControlStream(): Promise<StreamReader>;
  /** Wait for the next client-initiated request stream. */
  nextRequestStream(): Promise<ServerRequestStream>;
  /** Open the server's control stream and send its SETUP. */
  sendServerSetup(): void;
  /** Open a unidirectional data stream to the client. */
  openDataStream(bytes: Uint8Array): void;
  /** Open a unidirectional data stream the test writes to (and may reset). */
  openControlledDataStream(): { write(bytes: Uint8Array): Promise<void>; reset(error: unknown): void };
  openIncomingBidi(bytes: Uint8Array): { responses: Promise<Uint8Array[]> };
  getCloseInfo(): { closeCode?: number; reason?: string } | undefined;
  /** Simulate a transport-side drop (peer/network close, not a local `close()`). */
  dropTransport(): void;
}

export function createFakeTransport(): FakeTransport {
  const incomingUni = pushChannel<ReadableStream<Uint8Array>>();
  const incomingBidi = pushChannel<BidirectionalStreamLike>();
  const clientUniStreams: ReadableStream<Uint8Array>[] = [];
  const requestStreams: ServerRequestStream[] = [];
  const requestWaiters: ((stream: ServerRequestStream) => void)[] = [];
  let closeInfo: { closeCode?: number; reason?: string } | undefined;
  let resolveClosed!: (info: unknown) => void;
  const closed = new Promise<unknown>((resolve) => {
    resolveClosed = resolve;
  });

  const transport: MoqtTransport = {
    incomingUnidirectionalStreams: incomingUni.stream,
    incomingBidirectionalStreams: incomingBidi.stream,
    async createUnidirectionalStream() {
      const pipe = new TransformStream<Uint8Array, Uint8Array>();

      clientUniStreams.push(pipe.readable);
      return pipe.writable;
    },
    async createBidirectionalStream() {
      const clientToServer = new TransformStream<Uint8Array, Uint8Array>();
      const serverToClient = new TransformStream<Uint8Array, Uint8Array>();
      const writer = serverToClient.writable.getWriter();

      const messages: ControlMessage[] = [];
      let resolveFirst!: (message: ControlMessage) => void;
      const firstMessage = new Promise<ControlMessage>((resolve) => {
        resolveFirst = resolve;
      });

      // Server-side read pump for the client's messages.
      void (async () => {
        const reader = clientToServer.readable.getReader();
        const deframer = new ControlMessageDeframer();

        try {
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            for (const frame of deframer.push(value)) {
              const message = decodeControlMessage(frame);

              messages.push(message);

              if (messages.length === 1) resolveFirst(message);
            }
          }
        } catch {
          // client reset its sending direction
        }
      })();

      const serverStream: ServerRequestStream = {
        messages,
        firstMessage,
        send: (bytes) => writer.write(bytes),
        fin: () => writer.close(),
        reset: (reason) => {
          writer.abort(reason).catch(() => {});
        },
      };

      requestStreams.push(serverStream);
      requestWaiters.shift()?.(serverStream);
      return { readable: serverToClient.readable, writable: clientToServer.writable };
    },
    close(info) {
      closeInfo = info;
      resolveClosed(info);
    },
    closed,
  };

  return {
    transport,
    /** The SETUP control stream the client opened (first client uni stream). */
    async clientControlStream(): Promise<StreamReader> {
      await vi.waitFor(() => {
        if (clientUniStreams.length === 0) throw new Error('no client uni stream yet');
      });
      return new StreamReader(clientUniStreams[0]!);
    },
    /** Wait for the next client-initiated request stream. */
    nextRequestStream(): Promise<ServerRequestStream> {
      const existing = requestStreams.shift();
      if (existing) return Promise.resolve(existing);

      return new Promise((resolve) => requestWaiters.push(resolve));
    },
    /** Open the server's control stream and send its SETUP. */
    sendServerSetup(): void {
      const pipe = new TransformStream<Uint8Array, Uint8Array>();
      const writer = pipe.writable.getWriter();

      void writer.write(encodeSetup([]));
      incomingUni.push(pipe.readable);
    },
    /** Open a unidirectional data stream to the client. */
    openDataStream(bytes: Uint8Array): void {
      const pipe = new TransformStream<Uint8Array, Uint8Array>();
      const writer = pipe.writable.getWriter();

      void writer.write(bytes).then(() => writer.close());
      incomingUni.push(pipe.readable);
    },
    /** Open a unidirectional data stream the test writes to (and may reset). */
    openControlledDataStream(): { write(bytes: Uint8Array): Promise<void>; reset(error: unknown): void } {
      const pipe = new TransformStream<Uint8Array, Uint8Array>();
      const writer = pipe.writable.getWriter();

      incomingUni.push(pipe.readable);
      return {
        write: (bytes) => writer.write(bytes),
        reset: (error) => {
          writer.abort(error).catch(() => {});
        },
      };
    },
    openIncomingBidi(bytes: Uint8Array): { responses: Promise<Uint8Array[]> } {
      const serverToClient = new TransformStream<Uint8Array, Uint8Array>();
      const clientToServer = new TransformStream<Uint8Array, Uint8Array>();
      const writer = serverToClient.writable.getWriter();

      void writer.write(bytes).then(() => writer.close());
      incomingBidi.push({ readable: serverToClient.readable, writable: clientToServer.writable });
      const responses = (async () => {
        const chunks: Uint8Array[] = [];
        const reader = clientToServer.readable.getReader();

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;

          chunks.push(value);
        }

        return chunks;
      })();

      return { responses };
    },
    getCloseInfo: () => closeInfo,
    /** Simulate a transport-side drop (peer/network close, not a local `close()`). */
    dropTransport: () => resolveClosed(undefined),
  };
}

/** Encode a one-object subgroup stream for `trackAlias` (type 0x38: subgroup 0, default priority, end-of-group). */
export function encodeSubgroup(trackAlias: number, groupId: number, payload: Uint8Array, objectId = 0): Uint8Array {
  const writer = new ByteWriter();

  writer.writeVarint(0x38);
  writer.writeVarint(trackAlias);
  writer.writeVarint(groupId);
  writer.writeVarint(objectId); // first object id (absolute)
  writer.writeVarint(payload.length);
  writer.writeBytes(payload);
  return writer.toBytes();
}
