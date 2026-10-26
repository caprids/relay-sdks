# @caprid/relay-websocket

The Relay SDK's transport adapter for QMux over WebSocket. It carries MOQT on
[QMux](https://datatracker.ietf.org/doc/draft-ietf-quic-qmux/) (QUIC's streams over a reliable byte stream, wire
format `qmux-01`) inside one WebSocket connection, the way moq-relay serves it on TCP 443
([draft-lcurley-qmux-websocket](https://github.com/moq-dev/moq/blob/main/drafts/draft-lcurley-qmux-websocket.md)). It
reaches the relay where WebTransport cannot: networks that block UDP, and browsers without a WebTransport that can
negotiate protocols. It works in browsers and in Node 24.

It follows the transport adapter contract in [`spec/transport-adapter.md`](../../../spec/transport-adapter.md), with
the deviations recorded there, and passes its conformance suite in Node, Chromium, Firefox and WebKit. QMux comes from
[`@moq/qmux`](https://www.npmjs.com/package/@moq/qmux), by the author of moq-relay's own QMux, at an exact version;
[ADR 0005](../../../docs/adr/0005-websocket-adapter-wraps-moq-qmux.md) records why.

The package is unpublished, like the rest of the SDK.

## Use

Pass one of its factories as the SDK's `createTransport`. Build it once and reuse it for every connection.

```ts
import { connectRelaySubscriber } from '@caprid/relay';
import { createWebSocketTransport, withWebSocketFallback } from '@caprid/relay-websocket';

// WebTransport first; WebSocket when its handshake fails or takes longer than 4 s.
const subscriber = await connectRelaySubscriber({ url, getToken, createTransport: withWebSocketFallback() });

// Always WebSocket.
const other = await connectRelaySubscriber({ url, getToken, createTransport: createWebSocketTransport() });
```

- `createWebSocketTransport()` dials the endpoint's `wss:` URL (`https:` becomes `wss:`, path and query unchanged, so
  the token travels in the upgrade request), offering `qmux-01.moqt-22`, then `qmux-01.moqt-20`. Its factory declares
  `framing: 'websocket'`.
- `withWebSocketFallback({ handshakeTimeoutMs, onFallback })` dials the runtime's `WebTransport`, and falls back to
  WebSocket when that handshake fails or has not completed within `handshakeTimeoutMs` (default 4000). Where the runtime
  has no usable `WebTransport` (Node, Safari without it) it dials WebSocket at once. `onFallback(reason)` is told each
  time a connection falls back. The dials run one after the other, and every connection decides afresh. The factory's
  `framing` and the transport's `carrier` say which transport is in use.

## What differs from WebTransport

- Every stream shares one TCP connection: a lost packet holds up every stream behind it (head-of-line blocking).
- No datagrams.
- A relay that refuses the token answers the WebSocket upgrade with HTTP 401, which no WebSocket API exposes, so the
  SDK reports `TRANSPORT_ERROR` in phase `connecting`, not `UNAUTHORIZED`.
- A send stream's close resolves once the FIN is handed to the socket, not once the relay has it.

The relay's side, with measurements, is in [QMux over WebSocket](../../../spec/relay-interop.md#qmux-over-websocket).

## Trust

The adapter uses the runtime's WebSocket and its certificate checks. A browser offers no way to pin a WebSocket's
certificate. Node trusts the platform's roots, plus any in `NODE_EXTRA_CA_CERTS`, which Node reads only at start-up.

## Tests

- `pnpm test` runs the unit tests in `src/tests/` against an in-process accepting end over plain `ws://`.
- `pnpm test:conformance:websocket` runs the conformance suite in Node against `scripts/websocket-peer.js`, the
  accepting end for QMux over WebSocket; `pnpm test:conformance:websocket:browsers` runs it in Chromium, Firefox and
  WebKit (`BROWSERS=chromium` for one).
- `RELAY_RUNTIME=node-websocket,chromium-websocket pnpm test:relay` runs the live scenario over WebSocket.

## Stability

Before 1.0, a minor version can break the API. This package releases together with `@caprid/relay` at the same version;
see the [changelog](../../CHANGELOG.md).
