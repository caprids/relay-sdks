---
status: accepted
date: 2026-10-01
---

# The Node transport adapter speaks native QUIC through our own quinn addon

> The package names below predate the rename in [#222](https://github.com/muxinc/relay-sdks/issues/222): `@mux/*` is now `@caprid/*` on npm, `mux-*` is now `caprid-*` on PyPI, and `mux_relay` is now `caprid_relay`.

The TypeScript SDK needs a transport adapter for runtimes with no usable `WebTransport`, Node above
all. We decided that `@mux/relay-quic` carries MOQT over native QUIC (ALPN `moqt-20`, path and
`?jwt=` token in the SETUP PATH parameter), using a small napi-rs addon over quinn that we build and
ship ourselves, rather than reimplementing WebTransport over HTTP/3. The relay accepts native QUIC
(moq-relay 0.15.8 offers its MOQT ALPNs ahead of `h3`, and a live probe of mux.global on
2026-09-30 got SERVER_SETUP back), so the HTTP/3 and WebTransport layers would be work with no payoff
on this path. Owning the addon also gives us what the transport adapter contract needs that no
existing package exposes: stream-credit rejection, FIN acknowledgement, reset and stop-sending codes,
a custom CA, and musl builds.

## Considered options

- **`@moq/web-transport`** (napi-rs over web-transport-quinn, by the moq-relay author). This was the
  closest existing package and the fallback if quinn surprised us. We rejected it as the base because
  it has no custom CA, no musl builds and no stream-credit visibility, and because it adds a
  W3C-shaped layer the SDK's seam does not need.
- **`node:quic`**. Its API covers everything above, but no official Node binary ships it (as of
  v24.21 and v26.10 it is compile-time gated behind `--experimental-quic`). It is the intended long-term
  replacement for the addon, and because it supports native-ALPN apps, moving to it needs no HTTP/3.
- **`@fails-components/webtransport`**. It has an open Node 24 failure and its author calls it a
  stopgap.
- **Writing HTTP/3 and WebTransport ourselves over a QUIC binding**, as the Python SDK does over
  aioquic. No maintained Node QUIC binding exposes per-stream reset codes and stream credit, and on
  native QUIC that whole layer is unnecessary.

## Consequences

- Node and browsers use different wire paths to the same relay. The live-relay scenarios run
  unchanged on both (Chromium and Node launchers), and the browser path keeps its own Chromium
  coverage.
- The core builds SETUP with PATH and AUTHORITY when the factory declares native QUIC, and classifies
  transport errors by shape (`source`, `streamErrorCode`) instead of by the `WebTransportError`
  name.
- The repo takes on a Rust toolchain and a per-platform prebuilt-binary matrix (linux x64/arm64 gnu
  and musl, darwin arm64; win-x64 if cheap).
- Native QUIC is a supported transport in moq-relay, the relay mux.global runs, not something
  that merely happens to work (confirmed 2026-10-01, #188). If that changes, revisit with
  `@moq/web-transport` as the fallback.
- Revisit when `node:quic` ships in an official Node LTS binary.
