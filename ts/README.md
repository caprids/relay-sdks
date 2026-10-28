# @caprid/relay

The Relay TypeScript SDK. `connectRelaySubscriber` subscribes to track objects and
`connectRelayPublisher` publishes them, through Relay, Mux's Media over QUIC (MoQ) relay service.
Payloads are bytes the SDK does not interpret; no player, media catalog, or Video.js is needed.

## Requirements

- Browser WebTransport in a secure context (HTTPS or localhost). This is the default transport.
  It needs WebTransport protocol negotiation, which the live tests verify against the Relay in
  Chromium and in Firefox 155 or later (Playwright's builds). WebKit cannot open a WebTransport
  session to the Relay yet.
- Node 24+: import from `@caprid/relay/node`, which connects over native QUIC through
  `@caprid/relay-quic` ([Node](#node)). Other runtimes must pass a compatible `createTransport`
  factory.
- Where UDP is blocked, or the browser has no usable WebTransport (Safari): pass
  [`@caprid/relay-websocket`](packages/relay-websocket/README.md)'s `withWebSocketFallback()` or
  `createWebSocketTransport()` as `createTransport` to connect over QMux over WebSocket on TCP 443
  ([networks without UDP](GUIDE.md#networks-without-udp-qmux-over-websocket)).
- A Relay endpoint and a short-lived Relay JWT authorized for the namespace you use. The SDK
  consumes tokens; it does not issue them.

## Install

The package is unpublished and its name is provisional. Build it from a checkout of this repository
and serve it with its `@caprid/moq-core` dependency, as described in
[Use the local packages in your application](GUIDE.md#use-the-local-packages-in-your-application).

## Publish and subscribe

```ts
import { connectRelayPublisher, connectRelaySubscriber, readText, roomEndpoint } from '@caprid/relay';

const url = roomEndpoint(['team']); // your token's root as the path, on https://relay.mux.global
const namespace = ['quickstart', 'unique-run']; // relative to that root
const getToken = () => token; // a Relay JWT allowed to publish and subscribe there

const publisher = await connectRelayPublisher({ url, namespace, getToken });
const ticker = publisher.registerTrack({ track: 'ticker' });
await publisher.announced;

const subscriber = await connectRelaySubscriber({ url, getToken });
let received: (text: string) => void;
const arrived = new Promise<string>((resolve) => { received = resolve; });
const subscription = subscriber.subscribe(
  { namespace, track: 'ticker' },
  { onObject: (object) => { if (object.status === 'normal') received(readText(object)); } },
);
await subscription.subscribed; // a fresh namespace can briefly reject this with error 16; retry it

await ticker.writeText('hello from @caprid/relay'); // waits for the subscriber's demand
console.log(await arrived);
subscriber.close();
await publisher.close();
```

The [guide's first round trip](GUIDE.md#first-round-trip) runs the complete version of this in a
browser, with credential handling, the subscribe retry, a deadline, and cleanup on failure.

These connections never reconnect. For a live application, `connectSupervisedPublisher` and
`connectSupervisedSubscriber` reconnect with backoff, and keep tracks registered and subscribed
across connections and publisher restarts ([stay connected](GUIDE.md#stay-connected-supervised-connections)).

## Node

`@caprid/relay/node` exports everything `@caprid/relay` does. Its `connectRelayPublisher`,
`connectRelaySubscriber` and both `connectSupervised*` functions connect over native QUIC (MOQT on QUIC, ALPN `moqt-22`, falling back to `moqt-20`) through
[`@caprid/relay-quic`](packages/relay-quic/README.md), Mux's transport adapter for Node. Pass the same
`https://` endpoint you pass in a browser.

### Install

Add `@caprid/relay-quic` next to `@caprid/relay`. It is an optional peer dependency, needed only by
`@caprid/relay/node`, and it needs a native binary for your platform:
[its README](packages/relay-quic/README.md) says how to get one. Without the package or the
binary, a connect call fails with `RelayError` code `TRANSPORT_UNAVAILABLE`.

### Publish and subscribe

The code is the browser example's, with a different import:

```ts
import { connectRelayPublisher, connectRelaySubscriber, readText, roomEndpoint } from '@caprid/relay/node';

const url = roomEndpoint(['team']);
const namespace = ['quickstart', 'unique-run'];
const getToken = () => process.env.RELAY_TOKEN;

const publisher = await connectRelayPublisher({ url, namespace, getToken });
const ticker = publisher.registerTrack({ track: 'ticker' });
await publisher.announced;

const subscriber = await connectRelaySubscriber({ url, getToken });
const subscription = subscriber.subscribe(
  { namespace, track: 'ticker' },
  { onObject: (object) => { if (object.status === 'normal') console.log(readText(object)); } },
);
await subscription.subscribed;

await ticker.writeText('hello from Node');
```

### Trust

By default a connection trusts the platform's root certificates. Two options change that, on
either connect function:

- `ca`: extra trust anchors, PEM, as a string, bytes, or an array of either, added to the
  platform's roots. Use it for a relay behind a private certificate authority.
- `serverCertificateHashes`: `[{ algorithm: 'sha-256', value }]`, in the W3C WebTransport shape.
  The server's certificate must match one hash, and its chain is not validated.

```ts
import { readFile } from 'node:fs/promises';

const subscriber = await connectRelaySubscriber({ url, getToken, ca: await readFile('relay-ca.pem') });
```

Connections without trust options share one adapter. Passing `createTransport` replaces the adapter
entirely; passing it together with `ca` or `serverCertificateHashes` is a `TypeError`. To combine
them, build the factory yourself with `nativeQuicTransport({ ca })` from `@caprid/relay-quic`.

### Process lifetime

An open connection keeps the Node process alive, from the start of the dial until the connection
has closed. After that the adapter holds nothing, so a script exits by itself once it has closed
its publisher and subscriber ([TA-25, TA-26](../spec/transport-adapter.md)). A script that stops
early without closing them keeps running.

### Native WebTransport

`@caprid/relay/node` uses `@caprid/relay-quic` even where a global `WebTransport` exists. If Node gains
a `WebTransport` that can negotiate protocols, the first connect emits a `DeprecationWarning`
(code `CAPRID_RELAY_NODE_NATIVE_WEBTRANSPORT`) through `process.emitWarning`, once per process: at
that point `@caprid/relay` connects on its own, and this entry point will be retired.

### Deno

Best effort, and untested: Deno can load the entry as `npm:@caprid/relay/node` and the addon through
its npm compatibility layer. Deno's own `WebTransport` cannot negotiate protocols, so plain
`@caprid/relay` fails there with `TRANSPORT_UNAVAILABLE`.

## Stability

Before 1.0, a minor version can break the API, and [CHANGELOG.md](CHANGELOG.md) says what changed.
`@caprid/relay`, `@caprid/moq-core`, `@caprid/moq-media`, `@caprid/moq-mediabunny` and `@caprid/relay-quic` always
release together at the same version, so install matching versions. A release supports:

- browsers with WebTransport, and Node 24 or later on the platforms `@caprid/relay-quic` ships
  binaries for (Linux x64 and arm64 with glibc or musl, macOS arm64 and x64, Windows x64);
- MOQT draft 22 (`moqt-22`), with draft 20 (`moqt-20`) as an offered fallback, against moq-relay
  0.15.4 or later.

## Documentation

- [TypeScript guide](GUIDE.md): the quickstart, subscription and publishing lifecycles, async
  consumption, and publishing media.
- API reference: run `mise run ts:docs` to build it from the source.
- [Errors and recovery](GUIDE.md#errors-and-recovery): stable `RelayError` codes and phases, and
  recovering without assuming delivery or safe replay.
- [Shared specification](../spec/README.md): the protocol pins, behavior contract, and test vectors
  every Relay SDK implements.
