# @caprid/relay-quic

The Relay SDK's transport adapter for Node. It carries MOQT straight on a QUIC connection (native QUIC), offering the
ALPNs `moqt-22`, then `moqt-20`. A napi-rs addon over [quinn](https://github.com/quinn-rs/quinn) does the QUIC work, so
a plain Node script can publish and subscribe without a browser. The adapter follows the transport adapter contract in
[`spec/transport-adapter.md`](https://github.com/caprids/relay-sdks/blob/main/spec/transport-adapter.md) and passes its conformance suite. The design is
[ADR 0002](https://github.com/caprids/relay-sdks/blob/main/docs/adr/0002-node-adapter-speaks-native-quic.md).

## Use

Install it next to `@caprid/relay`. Most scripts need no code from this package: `@caprid/relay/node` uses it by
default and takes the trust options below (see the [SDK README](https://github.com/caprids/relay-sdks/blob/main/ts/README.md#node)).

To use it with `@caprid/relay` directly, pass the factory as `createTransport`. The URL is the same `https://` endpoint
the browser uses. The adapter dials only its host and port, and the core sends the path, the token, and the authority in
SETUP.

```ts
import { connectRelayPublisher } from '@caprid/relay';
import { nativeQuicTransport } from '@caprid/relay-quic';

const publisher = await connectRelayPublisher({
  url: 'https://relay.mux.global',
  namespace: ['acme', 'rooms', '42'],
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
  end-entity certificate must match one hash, and the adapter does not validate its chain. It still checks the
  handshake signature. Unlike a browser, the adapter does not require the certificate to be valid for 14 days or less.

## Stream credit

`anticipatedConcurrentIncomingUnidirectionalStreams` is how many unidirectional streams the relay may have open to the
connection at once: the MAX_STREAMS credit this end grants (RFC 9000 §4.6). It is an integer from 1 to 65535, and the
default is 1000. quinn's own default is 100. The relay sends each group on a stream of its own, so a voice track at 50
frames a second takes 50 streams a second, and the credit for each closed stream returns over your uplink. In relay-dev's
voice benchmark (finding P-06), a subscriber whose own upload queued its uplink for about 2 s ran out of credit at 100:
locally, the median arrival delay was 2865 ms, and 21.5 ms with more credit. 1000 covers a 20 s stall of the uplink on
one such track.

quinn sets aside an entry per allowed stream up front, so a higher value costs memory on every connection. Unlike the
W3C WebTransport option of the same name, which is a hint that the browser may round up, this is the exact limit. A
value that is not an integer from 1 to 65535 throws a `RangeError` at once, before the addon loads. Bidirectional
streams keep quinn's default of 100, because the relay opens one per request, not per object. `@caprid/relay/node` uses
the default and takes no such option. To change it, build the factory yourself:

```ts
import { connectRelaySubscriber } from '@caprid/relay';
import { nativeQuicTransport } from '@caprid/relay-quic';

const subscriber = await connectRelaySubscriber({
  url: 'https://relay.mux.global',
  getToken: () => process.env.RELAY_TOKEN,
  createTransport: nativeQuicTransport({ anticipatedConcurrentIncomingUnidirectionalStreams: 5000 }),
});
```

## Process lifetime

An open session keeps the Node process alive, from the start of the dial until `closed` settles. After that the
adapter holds nothing, so a script exits once it has closed its connections. Closing before the dial completes cancels
the dial and releases its socket.

## Prebuilt binaries

Each platform's binary ships in its own package, which `@caprid/relay-quic` lists as an optional dependency, as napi-rs
packages do. A package manager installs only the one whose `os`, `cpu`, and `libc` match, so you need no Rust.

| Platform package | Built on | Packed | Installed |
| --- | --- | --- | --- |
| `@caprid/relay-quic-darwin-arm64` | macos-14 | 1.14 MiB | 2.62 MiB |
| `@caprid/relay-quic-darwin-x64` | macos-14, cross | 1.21 MiB | 2.85 MiB |
| `@caprid/relay-quic-linux-arm64-gnu` | ubuntu-latest, zig | 1.19 MiB | 2.67 MiB |
| `@caprid/relay-quic-linux-arm64-musl` | ubuntu-latest, zig | 1.19 MiB | 2.67 MiB |
| `@caprid/relay-quic-linux-x64-gnu` | ubuntu-latest, zig | 1.26 MiB | 3.01 MiB |
| `@caprid/relay-quic-linux-x64-musl` | ubuntu-latest, zig | 1.26 MiB | 3.00 MiB |
| `@caprid/relay-quic-win32-x64-msvc` | windows-latest | 1.26 MiB | 3.33 MiB |

Installed is what an install puts on disk on that platform: this package, `@caprid/moq-core` and the one binary. The
sizes are from the 0.0.0 packages CI packed on 2026-10-01. The glibc builds need glibc 2.28 or later, which Node 24
needs too.

`src/addon.ts` loads a local build, `native/relay-quic.<platform>.node`, first, so a build from source always wins.
Without one it loads `@caprid/relay-quic-<platform>`. If neither is there, the error names both. A platform package that is
installed but cannot load, such as one built for the wrong libc, throws its own error. The loader does not skip it.

The `relay-quic binaries` workflow (`.github/workflows/relay-quic-binaries.yml`) runs on each PR that touches the crate,
the loader or the packaging. It builds every target, and `scripts/pack.mjs` packs the main package, the platform
packages and `@caprid/moq-core`, with the sizes in the job summary. `scripts/smoke.mjs` then installs those tarballs with
npm on each platform (musl in Alpine, arm64 Linux under QEMU, darwin-x64 under Rosetta). It checks that npm picked only
the matching binary, that the loader finds it, and that a dial sends a QUIC Initial and can be cancelled. The
`Release TypeScript SDK` workflow (`release-ts.yml`) calls this one and publishes the tarballs it packs, with the rest of
the TypeScript packages ([releasing](https://github.com/caprids/relay-sdks/blob/main/docs/releasing.md)).

The platform packages live in `npm/<platform>/`. `napi create-npm-dirs` generates them from `napi.targets` in
`package.json`. `scripts/pack.mjs` adds the optional dependencies on them only when it packs, so the lockfile never
names them. `pnpm check:boundaries`, run from `ts/`, checks that `npm/` and `napi.targets` agree.

## Stability

Before 1.0, a minor version can break the API. This package and its platform packages release
together with `@caprid/relay` at the same version; see the [changelog](https://github.com/caprids/relay-sdks/blob/main/ts/CHANGELOG.md).

## Building the addon

To work on the crate, build the addon locally with Rust. The mise tasks bring the toolchain that `mise.toml` pins.
Nothing else in the repo installs one.

```sh
mise run ts:test:conformance:node   # builds the addon, then runs the conformance suite against it
mise run ts:check:native            # cargo fmt --check, clippy and the unit tests
mise run ts:build:native -- --target aarch64-unknown-linux-musl -x   # cross-compiles with zig, as CI does
```

With a Rust toolchain of your own, `cd ts && pnpm --filter @caprid/relay-quic build:native` writes
`native/relay-quic.<platform>.node`. Cross-compiling with `-x` also needs `cargo-zigbuild` and the target's
`rustup target add`.

`build:native` also regenerates `src/native.d.ts`, the addon's typings, from the Rust. Commit it with any change to
the crate. CI checks that it matches.

`pnpm check`, run from `ts/`, builds and checks the TypeScript only, so the rest of the workspace needs no Rust.

## Layout

| Path | What it is |
| --- | --- |
| `src/index.ts` | `nativeQuicTransport` and its option and result types. |
| `src/transport.ts` | The `MoqtTransport`: the dial, `ready`, `protocol`, `closed`, `close()` and the incoming streams. |
| `src/streams.ts` | WHATWG streams over the addon's stream halves. |
| `src/errors.ts` | The addon's failures as the contract's error shapes (TA-22). |
| `src/addon.ts` | Finds and loads the addon for this platform: a local build, else the platform package. |
| `native/` | The Rust crate: trust (`tls.rs`), stream halves (`stream.rs`), failures as data (`failure.rs`), the dial and the session (`lib.rs`). |
| `npm/<platform>/` | The platform packages, one per `napi.targets` entry; CI puts each binary in its own. |
| `scripts/pack.mjs`, `scripts/smoke.mjs` | Pack the release tarballs with their sizes; install and smoke-load them. |
