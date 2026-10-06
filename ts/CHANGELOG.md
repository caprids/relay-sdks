# Changelog

The TypeScript packages release together at one version: `@mux/relay`, `@mux/moq-core`, `@mux/moq-media`,
`@mux/moq-mediabunny`, `@mux/relay-quic` and its platform packages. Before 1.0, a minor version can break the API;
each section says what changed and what to do about it. How to cut a release is in
[docs/releasing.md](../docs/releasing.md).

## Unreleased

The first release. Rename this heading to the version (`## 0.1.0`) in the release PR.

- `@mux/relay`: `connectRelayPublisher` and `connectRelaySubscriber` for browsers over WebTransport, and
  `@mux/relay/node` for Node 24+ over native QUIC.
- `@mux/relay`: `subscriber.namespaceEvents({ prefix })` discovers which namespaces under a prefix are announced
  ([#259](https://github.com/muxinc/relay-sdks/issues/259)). The subscriber's SETUP now carries the SOLICIT option,
  so the relay advertises namespaces only when asked. `@mux/moq-core` gains `session.subscribeNamespace` and
  `SETUP_OPTION.SOLICIT`.
- `@mux/relay-quic`: the native QUIC transport adapter for Node, with prebuilt binaries for Linux (x64 and arm64,
  glibc and musl), macOS (arm64 and x64) and Windows (x64).
- `@mux/moq-core`: the transport-independent MOQT draft-20 sessions the SDK is built on.
- `@mux/moq-media` and `@mux/moq-mediabunny`: LOC and MSF format helpers, and an optional Mediabunny adapter.
