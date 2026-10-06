# Changelog

The Python packages release together at one version: `mux-relay`, `mux-moq-core` and `mux-moq-media`. Before 1.0, a
minor version can break the API; each section says what changed and what to do about it. How to cut a release is in
[docs/releasing.md](../docs/releasing.md).

## Unreleased

The first release. Rename this heading to the version (`## 0.1.0`) in the release PR.

- `mux-relay`: `connect_relay_publisher` and `connect_relay_subscriber` over native QUIC (ALPN `moqt-20`) through aioquic,
  and the `mux_relay.media` frame writer and catalog service.
- `mux-moq-core` (imported as `moq_core`): the transport-independent MOQT draft-20 sessions the SDK is built on.
- `mux-moq-media` (imported as `moq_media`): LOC and MSF format helpers.
- `mux-relay`: `RelaySubscriber.namespace_events(prefix, capacity=..., cancellation=...)` discovers which namespaces
  under a prefix are announced, as a bounded iterator of `NamespaceEvent`s (`announced` / `unannounced`). The
  subscriber's SETUP now sends the MoQ Solicit option, so the relay advertises namespaces only on that request.
- `mux-moq-core`: `MoqtSession.subscribe_namespace` (SUBSCRIBE_NAMESPACE with NAMESPACE / NAMESPACE_DONE) and
  `SETUP_OPTION.SOLICIT`.
