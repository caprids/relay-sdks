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
  so the relay advertises namespaces only when asked. A prefix overlapping one the connection already discovers is
  refused with the new `PREFIX_OVERLAP` code (draft-20 §10.19). `@mux/moq-core` gains `session.subscribeNamespace` and
  `SETUP_OPTION.SOLICIT`.
- `@mux/relay`: opt-in supervised connections ([#260](https://github.com/muxinc/relay-sdks/issues/260)).
  `connectSupervisedPublisher` reconnects with backoff, serving the root namespace plus a fresh generation field per
  connection, and rebinds the same track writers to every generation; a write between generations fails with the new
  `RECONNECTING` code and is never replayed. `connectSupervisedSubscriber` keeps tracks subscribed through
  `want(key, options, handlers)`: it retries the announce race, resubscribes after a done, a rejection or a stale
  track, moves to replacement connections, follows a supervised publisher's newest generation through discovery, and
  reports `onRestarted`. An `onStatus` or `onGeneration` that throws stops the supervisor with that error
  ([#272](https://github.com/muxinc/relay-sdks/issues/272)); an `onRestarted` that throws is logged and the want stays
  subscribed. Both are in `@mux/relay/node` too. The primitives are unchanged. The examples' shared
  `announce-race.js` is retired: the demo and the live smoke test subscribe through `want`.
- `@mux/relay-quic`: the native QUIC transport adapter for Node, with prebuilt binaries for Linux (x64 and arm64,
  glibc and musl), macOS (arm64 and x64) and Windows (x64).
- `@mux/moq-core`: the transport-independent MOQT draft-20 sessions the SDK is built on.
- `@mux/moq-media` and `@mux/moq-mediabunny`: LOC and MSF format helpers, and an optional Mediabunny adapter.
