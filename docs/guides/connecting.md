# Connecting and authentication

Live applications should use the supervised connect functions. They open a connection to Relay, reconnect with backoff after network loss, rotate credentials before expiry, and keep tracks registered and subscribed across reconnects.

- **Publish only**: `connectSupervisedPublisher` / `connect_supervised_publisher`
- **Subscribe only**: `connectSupervisedSubscriber` / `connect_supervised_subscriber`
- **Publish and subscribe on one connection**: `connectSupervisedRelay` / `connect_supervised_relay`

If you need a single non-reconnecting connection for a short script or a custom retry loop, see [Low-level connections](low-level.md).

## Connect with a Client token

The SDK consumes short-lived Relay JWTs (**Client tokens**); it does not issue them. Mint a Client token through Switchboard's API or sign one in your backend with a signing key. Never put an API key in a client.

When you omit `url`, the SDK dials the hosted Relay (`https://relay.mux.global`) using the token's `root` claim as the path. Name namespaces relative to that root (for example `['rooms', '42']`, not `['org', 'rooms', '42']`).

<!-- tabs -->
<!-- tab: TypeScript -->

```ts
import { connectSupervisedRelay } from '@caprid/relay'; // or '@caprid/relay/node' in Node 24+

const relay = await connectSupervisedRelay({
  getToken: async () => fetchClientToken(),
  onStatus: (status, error) => console.log('relay status:', status, error),
});
```

<!-- tab: Python -->

```python
from caprid_relay import connect_supervised_relay

relay = await connect_supervised_relay(
    get_token=fetch_client_token,
    on_status=lambda status, error: print("relay status:", status, getattr(error, "code", None)),
)
```

<!-- /tabs -->

`getToken` / `get_token` is called on the initial connect, on every reconnect, and on every proactive token rotation. Return a fresh short-lived Client token each time rather than caching a single token forever.

## Choosing an endpoint

Caprid relays propagate announcements between regions, so publishers and subscribers can each dial their nearest region. Pass `url` to pick a regional relay, a room sub-path below the token's root, or a Self-hosted relay.

Use `roomEndpoint` / `room_endpoint` to build a room-rooted URL without hand-concatenating paths. It validates path segments (alphanumeric plus `-._~!$&'()+,;=:@`, never `.` or `..`) and leaves them unencoded because the relay matches path bytes verbatim.

<!-- tabs -->
<!-- tab: TypeScript -->

```ts
import { roomEndpoint } from '@caprid/relay';

const hosted = roomEndpoint(['acme', 'rooms', '42']);
// 'https://relay.mux.global/acme/rooms/42'

const regional = roomEndpoint('https://ord.relay.mux.global', ['acme', 'rooms', '42']);
// 'https://ord.relay.mux.global/acme/rooms/42'
```

<!-- tab: Python -->

```python
from caprid_relay import room_endpoint

hosted = room_endpoint(["acme", "rooms", "42"])
# 'https://relay.mux.global/acme/rooms/42'

regional = room_endpoint("https://ord.relay.mux.global", ["acme", "rooms", "42"])
# 'https://ord.relay.mux.global/acme/rooms/42'
```

<!-- /tabs -->

A path outside the token's grant fails at connect with `RelayError` code `UNAUTHORIZED`. To span multiple rooms under one grant, connect to the bare host (`https://relay.mux.global`) and include the token's root in each namespace. See [Token-rooted endpoints](../../spec/behavior.md#token-rooted-endpoint) and [Room-rooted endpoints](../../spec/behavior.md#room-rooted-endpoints).

## Token expiry and proactive rotation

The relay enforces a token's `exp` claim on a live connection and terminates the session with `UNAUTHORIZED` (`0x2`) if the token expires. Supervised connections read `exp` from each JWT and proactively open a replacement connection before expiry (`rotateBeforeMs` / `rotate_before`, default 60 s before `exp`), cutting over before closing the old connection. Call `rotate()` to trigger a rotation manually.

- **Supervised publishers** open each connection as a new **generation** (appending a unique generation segment to the published namespace root). During rotation, the new generation announces before the old connection closes, so subscribers following `generations: true` / `generations=True` switch without a gap.
- **Supervised subscribers** re-establish active `want()` subscriptions on the replacement connection before tearing down the old one.
- **Status callbacks** (`onStatus` / `on_status`) report `'connecting'`, `'live'`, `'reconnecting'`, and `'closed'`. Reconnect backoff is configurable with `reconnect` (`initialDelayMs`/`maxDelayMs` in TS, `SupervisionBackoff(initial_delay=..., max_delay=...)` in Python).

If `getToken` returns a token that the relay rejects with `UNAUTHORIZED`, the supervisor still calls `onStatus` with the error; make sure your token provider fetches a fresh token rather than replaying an expired one.

## Transports and TLS trust

By default:
- **Browsers** use WebTransport over HTTP/3 (UDP).
- **Node 24+** (`@caprid/relay/node`) uses native QUIC via `@caprid/relay-quic`.
- **Python** (`caprid_relay`) uses native QUIC via `aioquic` (`caprid_relay.quic.connect_quic`).

### Networks without UDP (TypeScript)

Where UDP is blocked or the browser lacks WebTransport protocol negotiation (such as Safari), opt into QMux over WebSocket (TCP 443) with [`@caprid/relay-websocket`](../../ts/packages/relay-websocket/README.md). Keep the `https://` endpoint URL; the adapter dials `wss://` on the same host and port.

```ts
import { connectSupervisedSubscriber } from '@caprid/relay';
import { createWebSocketTransport, withWebSocketFallback } from '@caprid/relay-websocket';

// Race WebSocket if WebTransport has not connected after 250 ms (Happy Eyeballs)
const subscriber = await connectSupervisedSubscriber({
  getToken,
  createTransport: withWebSocketFallback({
    webSocketDelayMs: 250,
    onFallback: () => console.log('connected over WebSocket'),
  }),
});

// Or force WebSocket (browser or Node)
const wsOnly = await connectSupervisedSubscriber({
  getToken,
  createTransport: createWebSocketTransport(),
});
```

WebSocket runs over TCP, so all tracks share one ordered byte stream and one lost packet stalls all tracks until TCP retransmits it. See [Unreliable networks](../networks.md#retransmission). Python does not have a WebSocket transport.

### Custom CAs and pinned certificates

For a Self-hosted relay using a private CA or self-signed certificate:

<!-- tabs -->
<!-- tab: TypeScript -->

```ts
import { readFile } from 'node:fs/promises';
import { connectSupervisedSubscriber } from '@caprid/relay/node';

// Node: add a private CA alongside system roots (or pass serverCertificateHashes)
const subscriber = await connectSupervisedSubscriber({
  url: 'https://relay.internal/org',
  getToken,
  ca: await readFile('relay-ca.pem'),
});
```

<!-- tab: Python -->

```python
from aioquic.quic.configuration import QuicConfiguration
from caprid_relay import connect_supervised_subscriber
from caprid_relay.quic import connect_quic

# Python trusts certifi's CA bundle by default, not the OS store.
config = QuicConfiguration(is_client=True, cafile="/path/to/relay-ca.pem")
subscriber = await connect_supervised_subscriber(
    "https://relay.internal/org",
    get_token=get_token,
    create_transport=lambda url, protocols: connect_quic(url, protocols, configuration=config),
)
```

<!-- /tabs -->

## Browser security

### Browser token exposure

Browser WebTransport passes the token as `?jwt=<token>` on the connection URL. On a failed handshake, browsers may log the full URL—including the token—to the browser console. The SDK cannot suppress browser-generated console messages.

- Always use short-lived Client tokens scoped to the minimum required namespaces and permissions.
- Scrub `jwt` query parameters and token strings from browser console breadcrumbs, network traces, and error reporting tools before forwarding diagnostics.

### Content Security Policy

If your web app enforces a Content Security Policy (CSP), allow the origins and worker/Wasm capabilities the SDK needs:

```text
connect-src 'self' https://relay.mux.global wss://relay.mux.global;
script-src 'self' blob: 'wasm-unsafe-eval';
worker-src 'self' blob:
```

- `connect-src`: list every relay `https:` origin you dial, plus its `wss:` origin if you use `@caprid/relay-websocket`.
- `script-src` and `worker-src`: `blob:` is required for `@caprid/moq-media`'s `AudioWorklet` module; `'wasm-unsafe-eval'` is required for libopus WebAssembly loss concealment (without it, `createAudioPlayer` silently falls back to WebCodecs without loss concealment).

## Closing and process lifetime

Always close connections when done: `await relay.close()` / `await publisher.close()` drains in-flight groups and withdraws announcements cleanly.

- In **Node**, an open connection keeps the event loop alive until closed.
- In **Python**, `await publisher.close()` and `await relay.close()` wait through QUIC's closing period (~0.3 s on a 50 ms RTT) so exiting immediately leaves no open UDP sockets or `ResourceWarning`.
