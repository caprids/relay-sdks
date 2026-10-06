/**
 * Raw peer-initiated request streams for publish-side tests — the relay's half of the announce-and-serve flow, which
 * the existing subscribe driver does not initiate. `openRawRequest` writes one request message and then records every
 * control frame the publisher sends back, plus whether the publisher FINed its side; `solicitNamespace` and
 * `rawSubscribe` wrap it with the two messages moq-lite-rs sends (SUBSCRIBE_NAMESPACE right after SETUP, SUBSCRIBE per
 * pulled track).
 */
import {
  type ControlMessage,
  ControlMessageDeframer,
  decodeControlMessage,
  encodeSubscribe,
  encodeSubscribeNamespace,
  MOQT_PROTOCOL_ID,
  type MoqtProtocolId,
  type TrackNamespace,
} from '../../protocol/control-messages.js';
import type { MoqtTransport } from '../../protocol/session.js';

export interface RawRequest {
  /** Control frames the publisher wrote back, in order. */
  received: ControlMessage[];
  /** The exact bytes the publisher wrote back, before its FIN. */
  raw: () => Uint8Array;
  /** True once the publisher FINed (or reset) its side. */
  ended: () => boolean;
  /**
   * The decode/stream error that ended the read loop, if any — the helper exists for byte-precise assertions, so a
   * malformed frame must surface here instead of masquerading as a clean FIN.
   */
  failure: () => unknown;
  /** Write a follow-up frame on the request stream (e.g. a GOAWAY). */
  send: (bytes: Uint8Array) => Promise<void>;
  /**
   * Reset only the response direction (stop reading) while keeping the request half open — the half-broken peer a
   * response-side write failure test needs.
   */
  abandonReads: () => Promise<void>;
  /** FIN the peer's side — half-closure, NOT a withdrawal (§6.4.2.2). */
  fin: () => Promise<void>;
  /** Reset the peer's side — the actual withdrawal signal. The reader sees `reason` as its stream error. */
  reset: (reason?: unknown) => Promise<void>;
}

export async function openRawRequest(server: MoqtTransport, message: Uint8Array): Promise<RawRequest> {
  const stream = await server.createBidirectionalStream();
  const writer = stream.writable.getWriter();

  await writer.write(message);
  const received: ControlMessage[] = [];
  const chunks: Uint8Array[] = [];
  let ended = false;
  let failure: unknown;
  const reader = stream.readable.getReader();

  void (async () => {
    const deframer = new ControlMessageDeframer();

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      chunks.push(value);
      for (const frame of deframer.push(value)) received.push(decodeControlMessage(frame));
    }
  })().then(
    () => {
      ended = true;
    },
    (error) => {
      ended = true;
      failure = error;
    }
  );
  return {
    received,
    raw: () => {
      const bytes = new Uint8Array(chunks.reduce((length, chunk) => length + chunk.length, 0));
      let offset = 0;

      for (const chunk of chunks) {
        bytes.set(chunk, offset);
        offset += chunk.length;
      }

      return bytes;
    },
    ended: () => ended,
    failure: () => failure,
    send: (bytes) => writer.write(bytes).catch(() => {}),
    abandonReads: () => reader.cancel().catch(() => {}),
    fin: () => writer.close().catch(() => {}),
    reset: (reason) => writer.abort(reason).catch(() => {}),
  };
}

/** The relay's opening move: solicit announces for a namespace prefix. */
export function solicitNamespace(server: MoqtTransport, prefix: TrackNamespace, requestId = 1): Promise<RawRequest> {
  return openRawRequest(server, encodeSubscribeNamespace({ requestId, trackNamespacePrefix: prefix }));
}

/** The relay's per-track pull, with the parameters moq-relay 0.14.17 and 0.15.8 send on draft-20 (Next Object plus current-group fill). */
export function rawSubscribe(
  server: MoqtTransport,
  trackNamespace: TrackNamespace,
  trackName: string,
  requestId: number,
  protocol: MoqtProtocolId = MOQT_PROTOCOL_ID
): Promise<RawRequest> {
  return openRawRequest(
    server,
    encodeSubscribe(
      {
        requestId,
        trackNamespace,
        trackName,
        parameters: {
          forward: 1,
          subscriberPriority: 0,
          locationFilter: { type: 'next-object' },
          fillParameters: { locationFilter: { type: 'relative-group', groupsBeforeNext: 1 } },
          groupOrder: 'descending',
        },
      },
      protocol
    )
  );
}
