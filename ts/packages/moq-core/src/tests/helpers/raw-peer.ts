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
  type TrackNamespace,
} from '../../protocol/control-messages.js';
import type { MoqtTransport } from '../../protocol/session.js';

export interface RawRequest {
  /** Control frames the publisher wrote back, in order. */
  received: ControlMessage[];
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
  /** FIN the peer's side — half-closure, NOT a withdrawal (§3.3.2). */
  fin: () => Promise<void>;
  /** Reset the peer's side — the actual withdrawal signal. */
  reset: () => Promise<void>;
}

export async function openRawRequest(server: MoqtTransport, message: Uint8Array): Promise<RawRequest> {
  const stream = await server.createBidirectionalStream();
  const writer = stream.writable.getWriter();

  await writer.write(message);
  const received: ControlMessage[] = [];
  let ended = false;
  let failure: unknown;
  const reader = stream.readable.getReader();

  void (async () => {
    const deframer = new ControlMessageDeframer();

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

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
    ended: () => ended,
    failure: () => failure,
    send: (bytes) => writer.write(bytes).catch(() => {}),
    abandonReads: () => reader.cancel().catch(() => {}),
    fin: () => writer.close().catch(() => {}),
    reset: () => writer.abort().catch(() => {}),
  };
}

/** The relay's opening move: solicit announces for a namespace prefix. */
export function solicitNamespace(server: MoqtTransport, prefix: TrackNamespace, requestId = 1): Promise<RawRequest> {
  return openRawRequest(server, encodeSubscribeNamespace({ requestId, trackNamespacePrefix: prefix }));
}

/** The relay's per-track pull, with the parameters moq-relay 0.14.17 sends on draft-20 (Next Object plus current-group fill). */
export function rawSubscribe(
  server: MoqtTransport,
  trackNamespace: TrackNamespace,
  trackName: string,
  requestId: number
): Promise<RawRequest> {
  return openRawRequest(
    server,
    encodeSubscribe({
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
    })
  );
}
