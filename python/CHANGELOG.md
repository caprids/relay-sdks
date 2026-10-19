# Changelog

The Python packages release together at one version: `mux-relay`, `mux-moq-core` and `mux-moq-media`. Before 1.0, a
minor version can break the API; each section says what changed and what to do about it. How to cut a release is in
[docs/releasing.md](../docs/releasing.md).

## Unreleased

The first release. Rename this heading to the version (`## 0.1.0`) in the release PR.

- `mux-relay`: `connect_relay_publisher` and `connect_relay_subscriber` over native QUIC through aioquic,
  and the `mux_relay.media` frame writer and catalog service.
- `mux_relay.media.media_track_properties("audio" | "video", ...)` gives a media track a default subgroup delivery
  timeout, so a stall costs stale frames instead of lasting latency
  ([#302](https://github.com/muxinc/relay-sdks/issues/302)): 500 ms for audio, and for video the target latency less
  the keyframe interval, never less than one interval (one interval by default). Pass its result to `register_track`
  beside `loc_track_properties()`; video needs `keyframe_interval_us`. The Video.js streaming example uses it.
- `open_group(payload, properties=...)` opens a group with its first object, so that object's delivery timeout counts
  the wait for demand, settling room and the stream ([#306](https://github.com/muxinc/relay-sdks/issues/306)); a bare
  `open_group()` has no object to time. The frame writer opens every group this way, so a frame whose open fails
  reports no `group_id`, and `groups` counts only opens that returned a group.
- Transcripts ([#320](https://github.com/muxinc/relay-sdks/issues/320)): `moq_media.build_transcript_segment` and
  `parse_transcript_segment` write and read the transcript track's segments (`TRANSCRIPT_TRACK`, `"transcript"`): the
  text of a talker's audio from `from_us`, sent in interim and final versions that share a `seq`. `Transcript` keeps
  a listener's segments, retracts what an interrupt cut at the playout position, and gives the lines heard by a
  position. The voice bot example publishes the text of its replies: `Respond` may yield `str` before the audio it
  is the text of.
- Interrupts ([#320](https://github.com/muxinc/relay-sdks/issues/320)): `moq_media.build_interrupt` and
  `parse_interrupt` write and read the interrupt track's messages (`INTERRUPT_TRACK`, `"interrupt"`), which tell a
  listener to drop audio a talker already sent, up to `until_us`. A new parrot voice bot example
  (`examples/voice_bot`) decodes a browser user's microphone with `OpusDecoder`, answers with `OpusEncoder` after a
  pause, and sends an interrupt when talked over; its `Respond` function is where STT, a model and TTS plug in.
- Opus for voice bots ([#320](https://github.com/muxinc/relay-sdks/issues/320)): `moq_media.opus.OpusEncoder` and
  `OpusDecoder` on PyAV's libopus, installed with the `opus` extra (`mux-moq-media[opus]`). The encoder turns s16 PCM
  into 20 ms packets stamped on the broadcast timeline, in runs that `flush()` ends; the decoder returns s16 PCM at the
  rate you ask for. `moq_media.opus_packet_duration_us` reads a packet's duration from its TOC byte. The Video.js
  streaming example's audio encoder is built on `OpusEncoder`.
- `mux-moq-core` (imported as `moq_core`): the transport-independent MOQT sessions the SDK is built on.
- `ConnectingTransport.framing` accepts `"websocket"`, a factory carrying MOQT on QMux over WebSocket, which sends the
  same SETUP as `"webtransport"` ([#321](https://github.com/muxinc/relay-sdks/issues/321)). The package ships no such
  factory.
- Reading frames in decode order within a latency budget ([#303](https://github.com/muxinc/relay-sdks/issues/303)).
  `moq_media.FrameSequencer(clock, grouping=)` orders one track's objects across group streams, waits for a late group
  only while it can still matter, skips to the next keyframe past the deadline, drops late audio, and marks each `gap`,
  `resync` or `restart` where a decoder resets. `mux_relay.media.FrameReader(sequencer)` drives it from a
  subscription's or a want's handlers and is iterated for frames. The playout clock holds the budget:
  `target_latency_us`, by default twice the longest recent keyframe interval (`report_keyframe_interval`), 2 s before
  one is known; wall pacing now re-anchors on a late frame only beyond it, instead of beyond `max_video_delay_us`. The
  Video.js streaming example's keyframe interval (`--gop`) now defaults to 1 s.
- Frame grouping plays every frame of a group that holds several, one per object, as `open_group()` writes them: the
  sequencer's position is a group and an object, so a group's later objects are no longer dropped as behind its first,
  and an object skipped inside a group is a `gap`. Groups of one frame, as the frame writer sends audio, behave as
  before.
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
- `mux-moq-media`: one timeline, and a playout clock for audio/video sync
  ([#285](https://github.com/muxinc/relay-sdks/issues/285)). `BroadcastClock` stamps every track in microseconds since
  the Unix epoch, so broadcasts line up with each other ([#290](https://github.com/muxinc/relay-sdks/issues/290))
  (`from_source_us`, a calibrated `source_timeline` for a clock with its own origin and rate, and `sample_timeline` for
  sample counters). `PlayoutClock` takes audio reports and answers `due(timestamp_us)` with a `PlayoutDecision` to wait,
  show, or drop, falling back to wall-clock pacing without audio. `build_msf_catalog` puts video and audio in one render
  group (`renderGroup: 1`, msf-01 §5.2.11), and `parse_msf_catalog` returns `render_group`.
- `mux-moq-media`: format helpers for msf-01's Media Timeline track, which maps media time to wall-clock time
  ([#291](https://github.com/muxinc/relay-sdks/issues/291)). `media_timeline_entry` turns a timeline timestamp and a
  group into a `MediaTimelineEntry`, `build_media_timeline` and `parse_media_timeline` write and read a timeline object,
  and `build_msf_catalog` announces the track through `media_timeline=MediaTimelineTrackInfo()` (`packaging` and `role`
  `mediatimeline`, `depends` on the video and audio tracks by default). `parse_msf_catalog` returns `depends` and
  `mimetype`. `MediaTimeline(max_entries=512)` keeps one broadcast's records (oldest dropped) and returns the document
  to send for each new entry ([#292](https://github.com/muxinc/relay-sdks/issues/292)).
- `mux-relay`: `mux_relay.media.DocumentService` serves a state-shaped track whose document the application replaces,
  such as the media timeline: the latest document on every relay subscription and on every `update()` while the relay
  is subscribed ([#292](https://github.com/muxinc/relay-sdks/issues/292)). The Video.js streaming example publishes
  the media timeline with it, and `--verify` checks the records against the video received.
- `mux-relay`: `mux_relay.media.FrameWriter` drops rather than queues, and says when it needs a keyframe
  ([#263](https://github.com/muxinc/relay-sdks/issues/263)). Concurrent `write()` calls are now served one at a time in
  call order, and the frames waiting are a backlog of at most `max_backlog` (default 8, or 25 with `grouping="frame"`;
  `writer.backlog` counts them). A frame arriving at a full backlog drops the backlog and the group in progress under
  keyframe grouping, and the oldest waiting frame under frame grouping; such frames report the new `dropped` outcome.
  Every relay demand change now resets the open group before the next frame. `writer.keyframe_needed` and the
  `on_keyframe_needed` callback say when the next frame must be a keyframe, so an encoder can force one. The writer
  watches demand from its creation, so create it with an event loop running; `close()` reports waiting frames as
  `dropped`. A caller that awaits each write sees no difference beyond the demand-change reset.
- `mux-moq-media`: capture times on one epoch, and a Timescale-aware reader
  ([#290](https://github.com/muxinc/relay-sdks/issues/290)). Source timelines take a settable `capture_latency_us`,
  subtracted from every timestamp, for the input delay no device reports. `loc_timestamp_us` reads an object's LOC
  Timestamp in microseconds, applying the object's or the track's Timescale.
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
