# Changelog

The Python packages release together at one version: `caprid-relay`, `caprid-moq-core` and `caprid-moq-media`. Before
1.0, a minor version can break the API; each section says what changed and what to do about it. How to cut a release
is in [docs/releasing.md](../docs/releasing.md).

## Unreleased

- `caprid-relay`, `caprid-moq-media`: track replacement for barge-in, as in the TypeScript SDK.
  `caprid_relay.media.ReplaceableTrack(publisher, track=..., catalog_service=..., build_catalog=...)` publishes a track
  whose `replace()` registers `audio-2`, `audio-3`, ..., lists it in the catalog in the old one's place, drops what the
  old track has not sent and closes it; frames written to a replacement wait for the relay's first subscription
  (`demand_wait`, 2 s). `moq_media.find_replacement_track(current, tracks)` finds the replacement in a newer catalog.
- `caprid-relay`, `caprid-moq-core`: `MessageParameters.max_age` (milliseconds, 0 to 2^53-1) is the MoQ Timestamp
  extension's subscriber max age (parameter `0x915C6`, as in the TypeScript SDK): a publisher that honors it skips a
  group once a newer group has superseded it for that long. A session sends it on SUBSCRIBE and REQUEST_UPDATE only to
  a relay whose SETUP declared the extension's `SETUP_OPTION.MAX_AGE` (`0x915C4`), never on FETCH, TRACK_STATUS or
  SUBSCRIBE_NAMESPACE, and leaves it off otherwise: moq-dev main declares it, moq-relay 0.17.x does not.
  `RelaySubscriber.max_age_supported` and `MoqtSession.max_age_supported` report whether the relay did. Nothing changes
  for an application that sets no `max_age`.

## 0.2.0

- `caprid-relay`: `room_endpoint(room)` roots `DEFAULT_ENDPOINT`, the hosted Relay `https://relay.mux.global`, so a
  quickstart passes only the token's root. `room_endpoint(endpoint, room)` works as before. Both arguments are now
  positional-only: a call that names them, such as `room_endpoint(endpoint=..., room=...)`, must pass them in order
  instead.
- `caprid-relay`, `caprid-moq-core`: a publisher connection answers the relay's TRACK_STATUS (draft-22 §9.13) for its
  tracks instead of refusing it with `NOT_SUPPORTED`: TRACK_STATUS_OK with the track's properties and, once the track
  has written an object, LARGEST_OBJECT; an unknown or closed track gets `DOES_NOT_EXIST`, as a SUBSCRIBE does.
  moq-relay `main` asks a publisher for a track's status before it subscribes on behalf of another relay, and a track
  whose publisher refuses does not cross two relays there. Nothing changes for an application. In `caprid-moq-core`,
  `RegisteredTrack.object_published(location)` records what a TRACK_STATUS_OK reports as LARGEST_OBJECT, and
  `encode_request_ok` takes `track_properties`.
- `caprid-moq-core`: a REQUEST_UPDATE carrying `FILL_PARAMETERS` is acknowledged instead of ending the subscription
  with `UPDATE_FAILED`, as in TypeScript 0.1.1. The publisher reports no Largest Object, so the fill is always empty
  and no fill fetch stream opens. moq-relay sends no REQUEST_UPDATE, so the relay path is unchanged.

## 0.1.0

- `caprid-relay`: `connect_relay_publisher` and `connect_relay_subscriber` over native QUIC through aioquic,
  and the `caprid_relay.media` frame writer and catalog service.
- `caprid_relay.room_endpoint(endpoint, room)` builds the recommended room-rooted endpoint, the endpoint with the
  token's root appended to its path. It keeps the query and encodes nothing: moq-relay reads the path without
  percent-decoding it, so a field that would need encoding (a space, `%`, `/`, `*`, non-ASCII, `.` or `..`) raises
  `ValueError`, and that room is reached from the bare host.
- `caprid_relay.media.media_track_properties("audio" | "video", ...)` gives a media track a default subgroup delivery
  timeout, so a stall costs stale frames instead of lasting latency: 500 ms for audio, and for video the target
  latency less the keyframe interval, never less than one interval (one interval by default). Pass its result to
  `register_track` beside `loc_track_properties()`; video needs `keyframe_interval_us`. The Video.js streaming example
  uses it. It also sets a default publisher priority (`0x0E`) that puts audio ahead of video, 16 for audio and 96 for
  video, which `publisher_priority` replaces. moq-relay 0.17 passes it to viewers but orders delivery by their
  subscriber priority alone, so it changes no delivery there yet.
- `caprid_relay.media.media_subscribe_parameters("audio" | "video", overrides=None)` gives a viewer's subscription the
  matching subscriber priority, 16 for audio and 96 for video, the one moq-relay 0.17 orders delivery by. Pass it as
  `parameters=` to `subscribe`, `want` or `switch_to`; `overrides` adds other parameters, and its
  `subscriber_priority` replaces the default. The Video.js example's `--verify` viewer uses it.
- `open_group(payload, properties=...)` opens a group with its first object, so that object's delivery timeout counts
  the wait for demand, settling room and the stream; a bare `open_group()` has no object to time. The frame writer
  opens every group this way, so a frame whose open fails reports no `group_id`, and `groups` counts only opens that
  returned a group.
- `moq_media.AUDIO_ONLY_TARGET_LATENCY_US` (500 ms), the same constant as TypeScript's: pass it as
  `PlayoutClock(target_latency_us=AUDIO_ONLY_TARGET_LATENCY_US)` for a broadcast with no video. Without it the clock
  keeps its 2 s default, since no keyframe interval is ever reported, and a frame sequencer waits 2 s of silence
  before it resyncs. The clock's behaviour is unchanged.
- Transcripts: `moq_media.build_transcript_segment` and `parse_transcript_segment` write and read the transcript
  track's segments (`TRANSCRIPT_TRACK`, `"transcript"`): the text of a talker's audio from `from_us`, sent in interim
  and final versions that share a `seq`. `Transcript` keeps a listener's segments, retracts what an interrupt cut at
  the playout position, and gives the lines heard by a position. The voice bot example publishes the text of its
  replies: `Respond` may yield `str` before the audio it is the text of.
- Interrupts: `moq_media.build_interrupt` and `parse_interrupt` write and read the interrupt track's messages
  (`INTERRUPT_TRACK`, `"interrupt"`), which tell a listener to drop audio a talker already sent, up to `until_us`. A
  new parrot voice bot example (`examples/voice_bot`) decodes a browser user's microphone with `OpusDecoder`, answers
  with `OpusEncoder` after a pause, and sends an interrupt when talked over; its `Respond` function is where STT, a
  model and TTS plug in.
- Opus for voice bots: `moq_media.opus.OpusEncoder` and `OpusDecoder` on PyAV's libopus, installed with the `opus`
  extra (`caprid-moq-media[opus]`). The encoder turns s16 PCM into 20 ms packets stamped on the broadcast timeline, in
  runs that `flush()` ends; the decoder returns s16 PCM at the rate you ask for. `moq_media.opus_packet_duration_us`
  reads a packet's duration from its TOC byte. The Video.js streaming example's audio encoder is built on
  `OpusEncoder`.
- `caprid-moq-core` (imported as `moq_core`): the transport-independent MOQT sessions the SDK is built on.
- `ConnectingTransport.framing` accepts `"websocket"`, a factory carrying MOQT on QMux over WebSocket, which sends the
  same SETUP as `"webtransport"`. The package ships no such factory.
- Reading frames in decode order within a latency budget. `moq_media.FrameSequencer(clock, grouping=)` orders one
  track's objects across group streams, waits for a late group only while it can still matter, skips to the next
  keyframe past the deadline, drops late audio, and marks each `gap`, `resync` or `restart` where a decoder resets.
  `caprid_relay.media.FrameReader(sequencer)` drives it from a subscription's or a want's handlers and is iterated for
  frames. The playout clock holds the budget: `target_latency_us`, by default twice the longest recent keyframe
  interval (`report_keyframe_interval`), 2 s before one is known; wall pacing now re-anchors on a late frame only
  beyond it, instead of beyond `max_video_delay_us`. The Video.js streaming example's keyframe interval (`--gop`) now
  defaults to 1 s.
- Frame grouping plays every frame of a group that holds several, one per object, as `open_group()` writes them: the
  sequencer's position is a group and an object, so a group's later objects are no longer dropped as behind its first,
  and an object skipped inside a group is a `gap`. Groups of one frame, as the frame writer sends audio, behave as
  before.
- MOQT draft-22 by default: connections offer `moqt-22`, then `moqt-20` as a fallback, and speak whichever the relay
  selects. The drafts differ on the wire only in the LOCATION_FILTER parameter, which `moq_core` now encodes for the
  negotiated draft: its encoders and decoders that carry message parameters take a trailing `protocol` argument
  (default `moqt-22`), and it exports `MOQT_PROTOCOL_IDS` and `MoqtProtocolId`. `MOQT_PROTOCOL_ID` is now `moqt-22`.
  moq-relay 0.17.0 frames LOCATION_FILTER the draft-20 way on `moqt-22`, so a Next Object filter closes the session
  there; use a relative start of 0.
- `caprid-relay`: `subscriber.subscribe_switchable(namespace, track, handlers)` moves one set of handlers between
  tracks make-before-break. `switch_to(track, parameters=..., until=..., drain=..., cancellation=...)` subscribes to
  the new track while the old one keeps delivering, cuts over on the first object `until` accepts, reports
  `on_switched`, and can keep the old subscription open briefly for groups still in flight. It is how to change a
  track, filter or priority, which `update()` cannot do against moq-relay.
- `caprid-relay`: a supervised want moves make-before-break. A newer generation, or wanting the key again with another
  track or parameters, opens the replacement while the current subscription keeps delivering, and cuts over on the
  first object `until` accepts. `want` gains `until=` and `drain=` (seconds), as on `switch_to`, and `WantHandlers`
  gains `on_switched`, which receives a `WantSwitchedInfo` (`from_namespace`, `from_track`, `to_namespace`,
  `to_track`, `subscribed`). A generation move no longer reports `on_restarted("generation", ...)`; handle
  `on_switched` instead, where decoders reset. Wanting a key again with only `stale_after` changed now swaps it
  without resubscribing. Another namespace or `generations` still replaces the want.
- `caprid-moq-media` (imported as `moq_media`): LOC and MSF format helpers. `build_msf_catalog` leaves `isComplete`
  out, since a live broadcast is not complete (msf-01 §5.1.3), and `build_msf_end_catalog` builds the §11.3 catalog
  that ends a broadcast: `isComplete: true` and no tracks.
- `caprid-moq-media`: one timeline, and a playout clock for audio/video sync. `BroadcastClock` stamps every track in
  microseconds since the Unix epoch, so broadcasts line up with each other (`from_source_us`, a calibrated
  `source_timeline` for a clock with its own origin and rate, and `sample_timeline` for sample counters).
  `PlayoutClock` takes audio reports and answers `due(timestamp_us)` with a `PlayoutDecision` to wait, show, or drop,
  falling back to wall-clock pacing without audio. `build_msf_catalog` puts video and audio in one render group
  (`renderGroup: 1`, msf-01 §5.2.11), and `parse_msf_catalog` returns `render_group`.
- `caprid-moq-media`: `build_msf_catalog` describes any number of video and audio tracks. `media` takes
  `VideoTrackInfo` and `AudioTrackInfo` entries, which gain `render_group` (default 1) and `alt_group` (msf-01
  §5.2.12) for simulcast layers; members of an alternate group must share a role and a render group. `video` and
  `audio` are unchanged. `parse_msf_catalog` returns `alt_group`, and `group_msf_tracks` lists the sets of tracks a
  viewer picks one of. The module-level `moq_media.catalog.MEDIA_RENDER_GROUP` constant is gone; the default is
  `render_group`'s.
- `caprid-moq-media`: format helpers for msf-01's Media Timeline track, which maps media time to wall-clock time.
  `media_timeline_entry` turns a timeline timestamp and a group into a `MediaTimelineEntry`, `build_media_timeline`
  and `parse_media_timeline` write and read a timeline object, and `build_msf_catalog` announces the track through
  `media_timeline=MediaTimelineTrackInfo()` (`packaging` and `role` `mediatimeline`, `depends` on the video and audio
  tracks by default). `parse_msf_catalog` returns `depends` and `mimetype`. `MediaTimeline(max_entries=512)` keeps one
  broadcast's records (oldest dropped) and returns the document to send for each new entry.
- `caprid-relay`: `caprid_relay.media.DocumentService` serves a state-shaped track whose document the application
  replaces, such as the media timeline: the latest document on every relay subscription and on every `update()` while
  the relay is subscribed. The Video.js streaming example publishes the media timeline with it, and `--verify` checks
  the records against the video received.
- `caprid-relay`: `caprid_relay.media.FrameWriter` drops rather than queues, and says when it needs a keyframe.
  Concurrent `write()` calls are now served one at a time in call order, and the frames waiting are a backlog of at
  most `max_backlog` (default 8, or 25 with `grouping="frame"`; `writer.backlog` counts them). A frame arriving at a
  full backlog drops the backlog and the group in progress under keyframe grouping, and the oldest waiting frame under
  frame grouping; such frames report the new `dropped` outcome. Every relay demand change now resets the open group
  before the next frame. `writer.keyframe_needed` and the `on_keyframe_needed` callback say when the next frame must
  be a keyframe, so an encoder can force one. The writer watches demand from its creation, so create it with an event
  loop running; `close()` reports waiting frames as `dropped`. A caller that awaits each write sees no difference
  beyond the demand-change reset.
- `caprid-moq-media`: capture times on one epoch, and a Timescale-aware reader. Source timelines take a settable
  `capture_latency_us`, subtracted from every timestamp, for the input delay no device reports. `loc_timestamp_us`
  reads an object's LOC Timestamp in microseconds, applying the object's or the track's Timescale.
- `caprid-relay`: `RelaySubscriber.namespace_events(prefix, capacity=..., cancellation=...)` discovers which namespaces
  under a prefix are announced, as a bounded iterator of `NamespaceEvent`s (`announced` / `unannounced`). The
  subscriber's SETUP now sends the MoQ Solicit option, so the relay advertises namespaces only on that request.
- `caprid-moq-core`: `MoqtSession.subscribe_namespace` (SUBSCRIBE_NAMESPACE with NAMESPACE / NAMESPACE_DONE) and
  `SETUP_OPTION.SOLICIT`.
- `caprid-relay`: opt-in supervised connections, as in the TypeScript SDK. `connect_supervised_publisher` reconnects
  with backoff, serving the root namespace plus a fresh generation field per connection, and rebinds the same track
  writers to every generation; a write between generations raises the new `RECONNECTING` code and is never replayed.
  `connect_supervised_subscriber` keeps tracks subscribed through `want(key, namespace, track, WantHandlers(...))`: it
  retries the announce race, resubscribes after a done, a rejection or a stale track, moves to replacement
  connections, follows a supervised publisher's newest generation through discovery, and reports `on_restarted`. The
  primitives are unchanged. The examples' shared `announce_race.py` is retired: the demo, the Video.js example's
  `--verify` viewer and the live smoke test subscribe through `want`.
- `caprid-relay`: sessions end at the token's expiry, and the supervisors now rotate before it, as in the TypeScript
  SDK. `get_token` may return a `RelayCredential` (`token`, an aware `expires_at`); every connection exposes
  `expires_at`, from that or the JWT's `exp` claim. Both supervisors take `rotate_before` (seconds, default 60) and
  `rotate()`, and replace their connection make-before-break: the publisher moves each track to the next generation
  once it has demand there, and every want moves its subscription to the new connection as a move does. A want whose
  target is unchanged cuts over on the first object the old subscription did not deliver, with no callback, so nothing
  is lost or repeated.
