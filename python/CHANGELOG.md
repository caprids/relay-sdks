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
- `mux-relay`: `subscriber.subscribe_switchable(namespace, track, handlers)` moves one set of handlers between tracks
  make-before-break ([#264](https://github.com/muxinc/relay-sdks/issues/264)). `switch_to(track, parameters=...,
  until=..., drain=..., cancellation=...)` subscribes to the new track while the old one keeps delivering, cuts over on
  the first object `until` accepts, reports `on_switched`, and can keep the old subscription open briefly for groups
  still in flight. It is how to change a track, filter or priority, which `update()` cannot do against moq-relay.
- `mux-relay`: a supervised want moves make-before-break ([#282](https://github.com/muxinc/relay-sdks/issues/282)).
  A newer generation, or wanting the key again with another track or parameters, opens the replacement while the
  current subscription keeps delivering, and cuts over on the first object `until` accepts. `want` gains `until=` and
  `drain=` (seconds), as on `switch_to`, and `WantHandlers` gains `on_switched`, which receives a `WantSwitchedInfo`
  (`from_namespace`, `from_track`, `to_namespace`, `to_track`, `subscribed`). A generation move no longer reports
  `on_restarted("generation", ...)`; handle `on_switched` instead, where decoders reset. Wanting a key again with only
  `stale_after` changed now swaps it without resubscribing. Another namespace or `generations` still replaces the want.
- `mux-moq-media` (imported as `moq_media`): LOC and MSF format helpers. `build_msf_catalog` leaves `isComplete`
  out, since a live broadcast is not complete (msf-01 §5.1.3), and `build_msf_end_catalog` builds the §11.3 catalog
  that ends a broadcast: `isComplete: true` and no tracks ([#284](https://github.com/muxinc/relay-sdks/issues/284)).
- `mux-moq-media`: one timeline per broadcast, and a playout clock for audio/video sync
  ([#285](https://github.com/muxinc/relay-sdks/issues/285)). `BroadcastClock` stamps every track from one origin
  (`from_source_us`, a calibrated `source_timeline` for a clock with its own origin and rate, and
  `sample_timeline` for sample counters). `PlayoutClock` takes audio reports and answers
  `due(timestamp_us)` with a `PlayoutDecision` to wait, show, or drop, falling back to wall-clock pacing without audio.
  `build_msf_catalog` puts video and audio in one render group (`renderGroup: 1`, msf-01 §5.2.11), and
  `parse_msf_catalog` returns `render_group`.
- `mux-moq-media`: format helpers for msf-01's Media Timeline track, which maps media time to wall-clock time
  ([#291](https://github.com/muxinc/relay-sdks/issues/291)). `BroadcastClock` records its origin as Unix-epoch
  microseconds (`origin_epoch_us`). `media_timeline_entry` turns a timeline timestamp and a group into a
  `MediaTimelineEntry`, `build_media_timeline` and `parse_media_timeline` write and read a timeline object, and
  `build_msf_catalog` announces the track through `media_timeline=MediaTimelineTrackInfo()` (`packaging` and `role`
  `mediatimeline`, `depends` on the video track by default). `parse_msf_catalog` returns `depends` and `mimetype`.
  Publishing the track is not wired up yet.
- `mux-relay`: `RelaySubscriber.namespace_events(prefix, capacity=..., cancellation=...)` discovers which namespaces
  under a prefix are announced, as a bounded iterator of `NamespaceEvent`s (`announced` / `unannounced`). The
  subscriber's SETUP now sends the MoQ Solicit option, so the relay advertises namespaces only on that request.
- `mux-moq-core`: `MoqtSession.subscribe_namespace` (SUBSCRIBE_NAMESPACE with NAMESPACE / NAMESPACE_DONE) and
  `SETUP_OPTION.SOLICIT`.
- `mux-relay`: opt-in supervised connections, as in the TypeScript SDK
  ([#260](https://github.com/muxinc/relay-sdks/issues/260)). `connect_supervised_publisher` reconnects with backoff,
  serving the root namespace plus a fresh generation field per connection, and rebinds the same track writers to every
  generation; a write between generations raises the new `RECONNECTING` code and is never replayed.
  `connect_supervised_subscriber` keeps tracks subscribed through `want(key, namespace, track, WantHandlers(...))`: it
  retries the announce race, resubscribes after a done, a rejection or a stale track, moves to replacement
  connections, follows a supervised publisher's newest generation through discovery, and reports `on_restarted`. The
  primitives are unchanged. The examples' shared `announce_race.py` is retired: the demo, the Video.js example's
  `--verify` viewer and the live smoke test subscribe through `want`.
- `mux-relay`: sessions end at the token's expiry, and the supervisors now rotate before it, as in the TypeScript SDK
  ([#261](https://github.com/muxinc/relay-sdks/issues/261)). `get_token` may return a `RelayCredential`
  (`token`, an aware `expires_at`); every connection exposes `expires_at`, from that or the JWT's `exp` claim. Both
  supervisors take `rotate_before` (seconds, default 60) and `rotate()`, and replace their connection
  make-before-break: the publisher moves each track to the next generation once it has demand there, and every want
  moves its subscription to the new connection as a move does. A want whose target is unchanged cuts over on the first
  object the old subscription did not deliver, with no callback, so nothing is lost or repeated.
