# @caprid/relay-websocket

The Relay SDK's transport adapter for QMux over WebSocket. It carries MOQT on
[QMux](https://datatracker.ietf.org/doc/draft-ietf-quic-qmux/), which runs QUIC's streams over a reliable byte stream
(wire format `qmux-01`), inside one WebSocket connection. moq-relay serves it on TCP 443
([draft-lcurley-qmux-websocket](https://github.com/moq-dev/moq/blob/main/drafts/draft-lcurley-qmux-websocket.md)). The
adapter reaches the relay where WebTransport cannot: on networks that block UDP, and in browsers whose WebTransport
cannot negotiate protocols. It works in browsers and in Node 24.

It follows the transport adapter contract in [`spec/transport-adapter.md`](../../../spec/transport-adapter.md), with
the deviations recorded there, and passes its conformance suite in Node, Chromium, Firefox and WebKit. QMux comes from
[`@moq/qmux`](https://www.npmjs.com/package/@moq/qmux), by the author of moq-relay's own QMux, at an exact version;
[ADR 0005](../../../docs/adr/0005-websocket-adapter-wraps-moq-qmux.md) records why.

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

- `createWebSocketTransport()` dials the endpoint's `wss:` URL and offers `qmux-01.moqt-22`, then `qmux-01.moqt-20`.
  `https:` becomes `wss:` with the path and query unchanged, so the token travels in the upgrade request. The factory
  declares `framing: 'websocket'`.
- `withWebSocketFallback({ handshakeTimeoutMs, onFallback })` dials the runtime's `WebTransport` first. It falls back
  to WebSocket when that handshake fails or has not completed within `handshakeTimeoutMs` (default 4000). Where the
  runtime has no usable `WebTransport`, as in Node or a Safari without it, it dials WebSocket at once. The adapter calls
  `onFallback(reason)` each time a connection falls back. The dials run one after the other, and every connection
  decides afresh. The factory's `framing` and the transport's `carrier` say which transport is in use.

## What differs from WebTransport

- Every stream shares one TCP connection, so a lost packet holds up every stream behind it (head-of-line blocking).
- No datagrams.
- A relay that refuses the token answers the WebSocket upgrade with HTTP 401, which no WebSocket API exposes, so the
  SDK reports `TRANSPORT_ERROR` in phase `connecting`, not `UNAUTHORIZED`.
- A send stream's close resolves once the FIN is handed to the socket, not once the relay has it.

The relay's side, with measurements, is in [QMux over WebSocket](../../../spec/relay-interop.md#qmux-over-websocket).

## Trust

The adapter uses the runtime's WebSocket and its certificate checks. A browser offers no way to pin a WebSocket's
certificate. Node trusts the platform's roots, plus any in `NODE_EXTRA_CA_CERTS`, which Node reads only at start-up.

## Tests

Run these from `ts/` in the repository.

- `pnpm test` runs the unit tests in `src/tests/` against an in-process accepting end over plain `ws://`.
- `pnpm test:conformance:websocket` runs the conformance suite in Node against `scripts/websocket-peer.js`, the
  accepting end for QMux over WebSocket.
- `pnpm test:conformance:websocket:browsers` runs the suite in Chromium, Firefox, and WebKit. Set `BROWSERS=chromium`
  to run one.
- `RELAY_RUNTIME=node-websocket,chromium-websocket pnpm test:relay` runs the live scenario over WebSocket.

## Stability

Before 1.0, a minor version can break the API. This package releases together with `@caprid/relay` at the same version;
see the [changelog](../../CHANGELOG.md).
