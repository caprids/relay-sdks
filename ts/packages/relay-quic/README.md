# @mux/relay-quic

The Relay SDK's transport adapter for Node. It carries MOQT straight on a QUIC connection (native QUIC, ALPN
`moqt-20`), through a napi-rs addon over [quinn](https://github.com/quinn-rs/quinn), so a plain Node script can publish
and subscribe without a browser. It follows the transport adapter contract in
[`spec/transport-adapter.md`](../../../spec/transport-adapter.md) and passes its conformance suite. The design is
[ADR 0002](../../../docs/adr/0002-node-adapter-speaks-native-quic.md).

The package is private for now: prebuilt binaries are still to come (#195).

## Use

Most scripts need no code from this package: `@mux/relay/node` uses it by default, and takes the trust options below
(see the [SDK README](../../README.md#node)). Install it next to `@mux/relay`.

To use it with `@mux/relay` directly, pass the factory as `createTransport`. The URL stays the same `https://` endpoint the browser uses: the adapter dials
only its host and port, and the core sends the path, the token and the authority in SETUP.

```ts
import { connectRelayPublisher } from '@mux/relay';
import { nativeQuicTransport } from '@mux/relay-quic';

const publisher = await connectRelayPublisher({
  url: 'https://sjc.relay.mux.global',
  getToken: () => process.env.RELAY_TOKEN,
  createTransport: nativeQuicTransport(),
});
```

`nativeQuicTransport()` throws at once if the addon is missing for the platform or an option is invalid. Build it once
and reuse it for every connection.

## Trust

By default a dial trusts the platform's root certificates.

- `ca`: extra trust anchors, PEM, as a string, bytes, or an array of either. They are added to the platform's roots.
- `serverCertificateHashes`: `[{ algorithm: 'sha-256', value }]`, in the W3C WebTransport shape. The server's
  end-entity certificate must match one hash, and its chain is not validated. The handshake signature is still
  checked. Unlike a browser, the adapter does not require the certificate to be valid for 14 days or less.

## Process lifetime

An open session keeps the Node process alive, from the start of the dial until `closed` settles. After that the
adapter holds nothing: a script exits once it has closed its connections. Closing before the dial completes cancels
the dial and releases its socket.

## Building the addon

There are no prebuilt binaries yet, so the addon is built locally, with Rust. The mise tasks bring the toolchain
`mise.toml` pins; nothing else in the repo installs one.

```sh
mise run ts:test:conformance:node   # builds the addon, then runs the conformance suite against it
mise run ts:check:native            # cargo fmt --check, clippy and the unit tests
```

With a Rust toolchain of your own, `cd ts && pnpm --filter @mux/relay-quic build:native` writes
`native/relay-quic.<platform>.node`.

`build:native` also regenerates `src/native.d.ts`, the addon's typings, from the Rust. Commit it with any change to
the crate. CI checks that it matches.

`pnpm check` builds and checks the TypeScript only, so the rest of the workspace needs no Rust.

## Layout

| Path | What it is |
| --- | --- |
| `src/index.ts` | `nativeQuicTransport` and its option and result types. |
| `src/transport.ts` | The `MoqtTransport`: the dial, `ready`, `closed`, `close()` and the incoming streams. |
| `src/streams.ts` | WHATWG streams over the addon's stream halves. |
| `src/errors.ts` | The addon's failures as the contract's error shapes (TA-22). |
| `src/addon.ts` | Finds and loads the addon for this platform. |
| `native/` | The Rust crate: trust (`tls.rs`), stream halves (`stream.rs`), failures as data (`failure.rs`), the dial and the session (`lib.rs`). |
