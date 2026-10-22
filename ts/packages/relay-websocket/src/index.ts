/**
 * `@caprid/relay-websocket`: the Relay SDK's transport adapter for QMux over WebSocket. It carries MOQT on QMux
 * (draft-ietf-quic-qmux, wire format `qmux-01`) inside one WebSocket connection over TCP, the relay's fallback for
 * networks that block UDP and browsers without a usable `WebTransport`. It follows the transport adapter contract
 * (`spec/transport-adapter.md`), with the deviations the contract records for it.
 *
 * - {@link createWebSocketTransport}: always WebSocket.
 * - {@link withWebSocketFallback}: WebTransport first, WebSocket when its handshake fails or times out.
 *
 * Pass either factory as the SDK's `createTransport`.
 *
 * @packageDocumentation
 */
import type { MoqtTransport } from '@caprid/moq-core';
import {
  assertHandshakeTimeout,
  type FallbackCarrier,
  FallbackTransport,
  toWebSocketUrl,
  type WebSocketFallbackOptions,
} from './fallback.js';
import { WebSocketTransport } from './transport.js';

export type { CloseInfo } from './errors.js';
export { WebSocketSessionError } from './errors.js';
export type { FallbackCarrier, FallbackTransport, WebSocketFallbackOptions } from './fallback.js';

/** The transport a {@link WebSocketConnection} carries: a `MoqtTransport` that reports the selected protocol. */
export interface WebSocketMoqtTransport extends MoqtTransport {
  /** The MOQT protocol the peer selected, without its QMux prefix; empty until `ready` resolves. */
  readonly protocol: string;
}

/** What one call of a {@link WebSocketTransportFactory} returns: the shape the SDK's `createTransport` expects. */
export interface WebSocketConnection {
  /** The transport the MOQT session runs on. */
  transport: WebSocketMoqtTransport;
  /** Resolves once the WebSocket is open and the peer selected an offered protocol. */
  ready: Promise<void>;
  /** Always `'websocket'`: the upgrade request carries the URL's path and query, as a WebTransport CONNECT does. */
  framing: 'websocket';
}

/**
 * Dials `url` as a WebSocket (`https:` becomes `wss:`, path and query unchanged), offering each of `protocols` as the
 * subprotocol `qmux-01.<protocol>`.
 */
export type WebSocketTransportFactory = (url: string, protocols: string[]) => WebSocketConnection;

/**
 * A transport factory for the SDK's `createTransport` option that always connects over QMux over WebSocket, in a
 * browser or in Node:
 *
 * ```ts
 * import { connectRelaySubscriber } from '@caprid/relay';
 * import { createWebSocketTransport } from '@caprid/relay-websocket';
 *
 * const subscriber = await connectRelaySubscriber({ url, getToken, createTransport: createWebSocketTransport() });
 * ```
 *
 * The streams share one TCP connection, so a lost packet holds up every stream behind it (head-of-line blocking), and
 * a relay's credentials refusal arrives as a failed WebSocket handshake, which the SDK reports as `TRANSPORT_ERROR`.
 */
export function createWebSocketTransport(): WebSocketTransportFactory {
  return (url, protocols) => {
    const transport = new WebSocketTransport(toWebSocketUrl(url), protocols);
    return { transport, ready: transport.ready, framing: 'websocket' };
  };
}

/** What one call of a {@link FallbackTransportFactory} returns. */
export interface FallbackConnection {
  /** The transport the MOQT session runs on, whichever dial won. */
  transport: FallbackTransport;
  /** Resolves once one of the dials is ready and its peer selected a protocol. */
  ready: Promise<void>;
  /**
   * `'webtransport'` while the WebTransport dial is in progress or once it won, `'websocket'` once the dial fell back.
   * Both carry the path and token in the URL, so the SDK's SETUP is the same either way.
   */
  readonly framing: FallbackCarrier;
}

/** Dials WebTransport, then QMux over WebSocket when that fails; see {@link withWebSocketFallback}. */
export type FallbackTransportFactory = (url: string, protocols: string[]) => FallbackConnection;

/**
 * A transport factory for the SDK's `createTransport` option that dials the runtime's `WebTransport` first, and falls
 * back to QMux over WebSocket when that handshake fails or takes longer than `handshakeTimeoutMs` (default 4 s). Where
 * the runtime has no usable `WebTransport` (Safari before WebTransport, Node), it dials WebSocket at once. The dials
 * run one after the other, never both at once, and each connection decides afresh.
 *
 * ```ts
 * import { connectRelayPublisher } from '@caprid/relay';
 * import { withWebSocketFallback } from '@caprid/relay-websocket';
 *
 * const publisher = await connectRelayPublisher({ url, getToken, createTransport: withWebSocketFallback() });
 * ```
 */
export function withWebSocketFallback(options: WebSocketFallbackOptions = {}): FallbackTransportFactory {
  assertHandshakeTimeout(options.handshakeTimeoutMs);
  return (url, protocols) => {
    const transport = new FallbackTransport(url, protocols, options);
    return {
      transport,
      ready: transport.ready,
      get framing() {
        return transport.carrier ?? 'webtransport';
      },
    };
  };
}
