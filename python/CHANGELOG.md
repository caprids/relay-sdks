# Changelog

The Python packages release together at one version: `mux-relay`, `mux-moq-core` and `mux-moq-media`. Before 1.0, a
minor version can break the API; each section says what changed and what to do about it. How to cut a release is in
[docs/releasing.md](../docs/releasing.md).

## Unreleased

The first release. Rename this heading to the version (`## 0.1.0`) in the release PR.

- `mux-relay`: `connect_relay_publisher` and `connect_relay_subscriber` over native QUIC through aioquic,
  and the `mux_relay.media` frame writer and catalog service.
- `mux-moq-core` (imported as `moq_core`): the transport-independent MOQT sessions the SDK is built on.
- MOQT draft-22 by default ([#274](https://github.com/muxinc/relay-sdks/issues/274)): connections offer `moqt-22`,
  then `moqt-20` as a fallback, and speak whichever the relay selects. The drafts differ on the wire only in the
  LOCATION_FILTER parameter, which `moq_core` now encodes for the negotiated draft: its encoders and decoders that carry
  message parameters take a trailing `protocol` argument (default `moqt-22`), and it exports `MOQT_PROTOCOL_IDS` and
  `MoqtProtocolId`. `MOQT_PROTOCOL_ID` is now `moqt-22`. moq-relay 0.17.0 frames LOCATION_FILTER the draft-20 way on
  `moqt-22`, so a Next Object filter closes the session there; use a relative start of 0
  ([#275](https://github.com/muxinc/relay-sdks/issues/275)).
- `mux-moq-media` (imported as `moq_media`): LOC and MSF format helpers.
- `mux-relay`: `RelaySubscriber.namespace_events(prefix, capacity=..., cancellation=...)` discovers which namespaces
  under a prefix are announced, as a bounded iterator of `NamespaceEvent`s (`announced` / `unannounced`). The
  subscriber's SETUP now sends the MoQ Solicit option, so the relay advertises namespaces only on that request.
- `mux-moq-core`: `MoqtSession.subscribe_namespace` (SUBSCRIBE_NAMESPACE with NAMESPACE / NAMESPACE_DONE) and
  `SETUP_OPTION.SOLICIT`.
