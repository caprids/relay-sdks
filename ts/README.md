# @caprid/relay

`@caprid/relay` is the Relay TypeScript SDK for browsers and Node. With `@caprid/moq-media` it also
plays live video and audio in a browser page, with no UI.

`connectSupervisedPublisher` and `connectSupervisedSubscriber` publish and subscribe to track objects
through Relay, Caprid's Media over QUIC (MoQ) relay service, with automatic reconnection and
credential rotation. Payloads are raw bytes, so a data track needs no media catalog or player. For
live media, [`@caprid/moq-media`](https://github.com/caprids/relay-sdks/blob/main/ts/packages/moq-media/README.md)
maps WebCodecs chunks to LOC, reads and writes MSF catalogs, and provides headless audio and video
players ([browser audio & video](https://www.caprid.com/docs/media/browser/)).

## Requirements

- **Browsers**: WebTransport in a secure context (HTTPS or localhost) is the default transport
  (verified in Chromium and Firefox 155+).
- **Node 24+**: import from `@caprid/relay/node`, which connects over native QUIC through
  `@caprid/relay-quic` ([Node](#node)).
- **Networks without UDP / Safari**: pass
  [`@caprid/relay-websocket`](https://github.com/caprids/relay-sdks/blob/main/ts/packages/relay-websocket/README.md)'s
  `withWebSocketFallback()` or `createWebSocketTransport()` as `createTransport` to run QMux over
  WebSocket on TCP 443 ([networks without UDP](https://www.caprid.com/docs/relay/connecting/#networks-without-udp-typescript)).
- **Credentials**: a short-lived Relay JWT authorized for the namespace you use (and an explicit
  `url` only for a relay other than the hosted one). The SDK consumes tokens; it does not issue them.

## Install

```sh
npm install @caprid/relay
```

Optional companion packages (install matching versions):

| Package | When to add it |
| --- | --- |
| `@caprid/relay-quic` | Node 24+ native QUIC transport (`@caprid/relay/node`) |
| `@caprid/relay-websocket` | Fallback over TCP 443 for Safari or UDP-blocked networks |
| `@caprid/moq-media` | Live browser audio/video players, LOC framing, MSF catalogs |
| `@caprid/moq-mediabunny` | Publish existing media files with Mediabunny |

## Publish and subscribe

```ts
import { connectSupervisedPublisher, connectSupervisedSubscriber, readText } from '@caprid/relay';

const getToken = () => token; // short-lived Relay JWT
const namespace = ['quickstart', 'unique-run']; // relative to the token's root

const publisher = await connectSupervisedPublisher({ namespace, getToken });
const ticker = publisher.registerTrack({ track: 'ticker' });

const subscriber = await connectSupervisedSubscriber({ getToken });
const arrived = new Promise<string>((resolve) => {
  subscriber.want(
    'ticker',
    { namespace, track: 'ticker', generations: true },
    { onObject: (object) => { if (object.status === 'normal') resolve(readText(object)); } },
  );
});

await ticker.writeText('hello from @caprid/relay');
console.log(await arrived);
subscriber.close();
await publisher.close();
```

With no `url`, each connection dials the hosted Relay (`https://relay.mux.global`) rooted at the
token's root, so namespaces are relative to that root. Pass `url` (or build one with
`roomEndpoint`) for another relay, region, or room. See
[Connecting & authentication](https://www.caprid.com/docs/relay/connecting/) and
[Low-level connections](https://www.caprid.com/docs/relay/low-level/) for single-shot
`connectRelayPublisher` / `connectRelaySubscriber` usage.

## Node

`@caprid/relay/node` exports everything `@caprid/relay` does. Its `connectRelayPublisher`,
`connectRelaySubscriber`, and both `connectSupervised*` functions connect over native QUIC through
[`@caprid/relay-quic`](https://github.com/caprids/relay-sdks/blob/main/ts/packages/relay-quic/README.md), Caprid's transport adapter for Node. Native QUIC
means MOQT directly on QUIC, with ALPN `moqt-22` and a fallback to `moqt-20`. Pass the same
`https://` endpoint you pass in a browser.

### Install

```sh
npm install @caprid/relay @caprid/relay-quic
```

`@caprid/relay-quic` is an optional peer dependency that only `@caprid/relay/node` needs. It lists
a prebuilt native binary for each supported platform as an optional dependency, and your package
manager installs the one that matches ([its README](https://github.com/caprids/relay-sdks/blob/main/ts/packages/relay-quic/README.md#prebuilt-binaries)).
Without the package or the binary, a connect call fails with `RelayError` code
`TRANSPORT_UNAVAILABLE`.

### Publish and subscribe

The code is the browser example's, with a different import:

```ts
import { connectRelayPublisher, connectRelaySubscriber, readText } from '@caprid/relay/node';

const namespace = ['quickstart', 'unique-run'];
const getToken = () => process.env.RELAY_TOKEN;

const publisher = await connectRelayPublisher({ namespace, getToken });
const ticker = publisher.registerTrack({ track: 'ticker' });
await publisher.announced;

const subscriber = await connectRelaySubscriber({ getToken });
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

- `ca` adds extra trust anchors to the platform's roots. Pass PEM as a string, as bytes, or as an
  array of either. Use it for a relay behind a private certificate authority.
- `serverCertificateHashes` takes `[{ algorithm: 'sha-256', value }]`, in the W3C WebTransport
  shape. The server's certificate must match one hash, and the adapter does not validate its chain.

```ts
import { readFile } from 'node:fs/promises';

const subscriber = await connectRelaySubscriber({ url, getToken, ca: await readFile('relay-ca.pem') });
```

Connections without trust options share one adapter. Passing `createTransport` replaces the adapter
entirely. Passing it together with `ca` or `serverCertificateHashes` throws a `TypeError`. To combine
them, build the factory yourself with `nativeQuicTransport({ ca })` from `@caprid/relay-quic`.

### Process lifetime

An open connection keeps the Node process alive from the start of the dial until the connection
has closed. After that the adapter holds nothing, so a script exits by itself once it has closed
its publisher and subscriber ([TA-25, TA-26](https://github.com/caprids/relay-sdks/blob/main/spec/transport-adapter.md)). A script that stops
early without closing them keeps running.

### Native WebTransport

`@caprid/relay/node` uses `@caprid/relay-quic` even where a global `WebTransport` exists. If Node gains
a `WebTransport` that can negotiate protocols, the first connect emits a `DeprecationWarning`
(code `CAPRID_RELAY_NODE_NATIVE_WEBTRANSPORT`) through `process.emitWarning`, once per process. At
that point `@caprid/relay` connects on its own, and this entry point will be retired.

### Deno

Deno support is best effort and untested. Deno can load the entry as `npm:@caprid/relay/node` and
the addon through its npm compatibility layer. Deno's own `WebTransport` cannot negotiate protocols,
so plain `@caprid/relay` fails there with `TRANSPORT_UNAVAILABLE`.

## Stability

Before 1.0, a minor version can break the API, and [CHANGELOG.md](https://github.com/caprids/relay-sdks/blob/main/ts/CHANGELOG.md) says what changed.
`@caprid/relay`, `@caprid/moq-core`, `@caprid/moq-media`, `@caprid/moq-mediabunny`,
`@caprid/relay-quic`, and `@caprid/relay-websocket` always release together at the same version, so
install matching versions. A release supports:

- browsers with WebTransport, and Node 24 or later on the platforms `@caprid/relay-quic` ships
  binaries for (Linux x64 and arm64 with glibc or musl, macOS arm64 and x64, Windows x64).
- MOQT draft 22 (`moqt-22`), with draft 20 (`moqt-20`) as an offered fallback, against moq-relay
  0.15.4 or later.

## Documentation

- [Quickstart](https://www.caprid.com/docs/getting-started/quickstart/) and
  [Relay guides](https://www.caprid.com/docs/relay/connecting/): connecting, pub/sub, discovery,
  switching tracks, control & telemetry, errors & recovery, and browser media.
- [TypeScript API reference](https://www.caprid.com/docs/reference/typescript/): generated from the
  source with TypeDoc.
- [Shared specification](https://www.caprid.com/docs/spec/): protocol pins, behavior contract, and
  test vectors every Relay SDK implements.
- **AI agents**: machine-readable docs at [llms.txt](https://www.caprid.com/llms.txt) and the
  installable Caprid skill at [SKILL.md](https://www.caprid.com/docs/agents/SKILL.md).
