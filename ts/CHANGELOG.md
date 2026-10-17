# Changelog

The TypeScript packages release together at one version: `@mux/relay`, `@mux/moq-core`, `@mux/moq-media`,
`@mux/moq-mediabunny`, `@mux/relay-quic` and its platform packages. Before 1.0, a minor version can break the API;
each section says what changed and what to do about it. How to cut a release is in
[docs/releasing.md](../docs/releasing.md).

## Unreleased

The first release. Rename this heading to the version (`## 0.1.0`) in the release PR.

- `@mux/relay`: `connectRelayPublisher` and `connectRelaySubscriber` for browsers over WebTransport, and
  `@mux/relay/node` for Node 24+ over native QUIC.
- `@mux/relay/media`: the media publishing policy Python has as `mux_relay.media`
  ([#263](https://github.com/muxinc/relay-sdks/issues/263)). `createFrameWriter(track, options)` maps encoded frames
  onto groups (a group per keyframe, or per frame) and can be called from a WebCodecs output callback without
  awaiting: frames wait in a bounded backlog (`maxBacklog`), and when it overflows the writer drops rather than queues.
  Every frame gets one `FrameOutcome`, and `keyframeNeeded` / `onKeyframeNeeded` say when the encoder should force a
  keyframe. `createCatalogService` and `createDocumentService` serve the MSF catalog and the media timeline on every
  relay subscription. The WebCodecs example publishes through all three.
- `@mux/relay/media`: `mediaTrackProperties('audio' | 'video', options)` gives a media track a default subgroup
  delivery timeout, so a stall costs stale frames instead of lasting latency
  ([#302](https://github.com/muxinc/relay-sdks/issues/302)): 500 ms for audio, and for video the target latency less
  the keyframe interval, never less than one interval (one interval by default). Pass its result to `registerTrack`
  beside `locTrackProperties()`; video needs `keyframeIntervalUs`. The WebCodecs example uses it.
- `openGroup({ payload, properties })` opens a group with its first object, so that object's delivery timeout
  counts the wait for demand, settling room and the stream ([#306](https://github.com/muxinc/relay-sdks/issues/306));
  a bare `openGroup()` has no object to time. The frame writer opens every group this way, so a frame whose open
  fails reports no `groupId`, and `groups` counts only opens that returned a group.
- `@mux/moq-media`: a headless audio player ([#314](https://github.com/muxinc/relay-sdks/issues/314)).
  `createAudioPlayer({ context })` mixes any number of inputs into one `output` node; each
  `player.addInput({ clock, decoderConfig })` decodes with WebCodecs, resets its decoder at discontinuities, holds
  audio in an adaptive `AudioWorklet` jitter buffer (60 ms to start, 40–400 ms, under the clock's target latency), and
  reports what is audible to the playout clock. `input.push` is a frame reader's `onFrame`; `input.cut({ fromUs,
  untilUs })` drops received audio in a timeline range. Also `createEchoCancellationLoopback` for Chromium's echo
  canceller, `audioDecoderConfigFromTrack`, `opusPacketDurationUs`, and `AUDIO_ONLY_TARGET_LATENCY_US` (500 ms) for
  an audio-only clock, whose 2 s default otherwise waits for keyframes. `createRenderClock` moved into the package
  from the WebCodecs example, which gains an audio-only listener, `listen-audio.ts`.
- Reading frames in decode order within a latency budget ([#303](https://github.com/muxinc/relay-sdks/issues/303)).
  `@mux/moq-media`'s `createFrameSequencer({ clock, grouping })` orders one track's objects across group streams, waits
  for a late group only while it can still matter, skips to the next keyframe past the deadline, drops late audio, and
  marks each `gap`, `resync` or `restart` where a decoder resets. `@mux/relay/media`'s `createFrameReader(sequencer,
  { onFrame })` is the subscription's or want's handlers that drive it. The playout clock holds the budget:
  `targetLatencyUs`, by default twice the longest recent keyframe interval (`reportKeyframeInterval`), 2 s before one
  is known; wall pacing now re-anchors on a late frame only beyond it, instead of beyond `maxVideoDelayUs`. Both
  examples now force a keyframe at least every second.
- Frame grouping plays every frame of a group that holds several, one per object, as `openGroup()` writes them: the
  sequencer's position is a group and an object, so a group's later objects are no longer dropped as behind its first,
  and an object skipped inside a group is a `gap`. Groups of one frame, as the frame writer sends audio, behave as
  before.
- `@mux/relay`: `subscriber.namespaceEvents({ prefix })` discovers which namespaces under a prefix are announced
  ([#259](https://github.com/muxinc/relay-sdks/issues/259)). The subscriber's SETUP now carries the SOLICIT option,
  so the relay advertises namespaces only when asked. A prefix overlapping one the connection already discovers is
  refused with the new `PREFIX_OVERLAP` code (draft-22 §9.15). `@mux/moq-core` gains `session.subscribeNamespace` and
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
- `@mux/relay`: `subscriber.subscribeSwitchable(options, handlers)` moves one set of handlers between tracks
  make-before-break ([#264](https://github.com/muxinc/relay-sdks/issues/264)). `switchTo(track, { parameters, until,
  drainMs, signal })` subscribes to the new track while the old one keeps delivering, cuts over on the first object
  `until` accepts, reports `onSwitched`, and can keep the old subscription open briefly for groups still in flight.
  It is how to change a track, filter or priority, which `update()` cannot do against moq-relay.
- `@mux/relay`: a supervised want moves make-before-break ([#282](https://github.com/muxinc/relay-sdks/issues/282)).
  A newer generation, or wanting the key again with another track or parameters, opens the replacement while the
  current subscription keeps delivering, and cuts over on the first object `until` accepts. The new `WantOptions`
  fields `until` and `drainMs` work as they do on `switchTo`, and the new `onSwitched` handler receives a
  `WantSwitchedInfo` (`from`, `to`, `subscribed`). A generation move no longer reports `onRestarted({ reason:
  'generation' })`; handle `onSwitched` instead, where decoders reset. Wanting a key again with only `staleAfterMs`
  changed now swaps it without resubscribing. Another namespace or `generations` still replaces the want.
- `@mux/relay`: sessions end at the token's expiry, and the supervisors now rotate before it
  ([#261](https://github.com/muxinc/relay-sdks/issues/261)). `getToken` may return a `RelayCredential`
  (`{ token, expiresAt }`); every connection exposes `expiresAt`, from that or the JWT's `exp` claim. Both supervisors
  take `rotateBeforeMs` (default 60 s) and `rotate()`, and replace their connection make-before-break: the publisher
  moves each track to the next generation once it has demand there, and every want moves its subscription to the new
  connection as a move does. A want whose target is unchanged cuts over on the first object the old subscription did
  not deliver, with no callback, so nothing is lost or repeated. A session closed with `UNAUTHORIZED` (0x2) after
  SETUP, which is how moq-relay ends an expired or revoked session, is now `UNAUTHORIZED` in phase `connection` rather
  than `TRANSPORT_ERROR` or a clean close ([#227](https://github.com/muxinc/relay-sdks/issues/227)).
- `@mux/relay-quic`: the native QUIC transport adapter for Node, with prebuilt binaries for Linux (x64 and arm64,
  glibc and musl), macOS (arm64 and x64) and Windows (x64).
- `@mux/moq-core`: the transport-independent MOQT sessions the SDK is built on.
- MOQT draft-22 by default ([#274](https://github.com/muxinc/relay-sdks/issues/274)): connections offer `moqt-22`,
  then `moqt-20` as a fallback, and speak whichever the relay selects. The drafts differ on the wire only in the
  LOCATION_FILTER parameter, which `@mux/moq-core` now encodes for the negotiated draft: its encoders and decoders that
  carry message parameters take a trailing `protocol` argument (default `moqt-22`), and it exports `MOQT_PROTOCOL_IDS`.
  `MOQT_PROTOCOL_ID` is now `moqt-22`. moq-relay 0.17.0 frames LOCATION_FILTER the draft-20 way on `moqt-22`, so a
  Next Object filter closes the session there; use a relative start of 0
  ([#275](https://github.com/muxinc/relay-sdks/issues/275)).
- `@mux/moq-media` and `@mux/moq-mediabunny`: LOC and MSF format helpers, and an optional Mediabunny adapter.
  `buildMsfCatalog` leaves `isComplete` out, since a live broadcast is not complete (msf-01 §5.1.3), and
  `buildMsfEndCatalog` builds the §11.3 catalog that ends a broadcast: `isComplete: true` and no tracks
  ([#284](https://github.com/muxinc/relay-sdks/issues/284)).
- `@mux/moq-media`: one timeline, and a playout clock for audio/video sync
  ([#285](https://github.com/muxinc/relay-sdks/issues/285)). `createBroadcastClock` stamps every track in microseconds
  since the Unix epoch, so broadcasts line up with each other ([#290](https://github.com/muxinc/relay-sdks/issues/290))
  (`sourceTimeline` with `calibrate` for an `AudioContext` clock or `MediaStreamTrackProcessor` timestamps, so the
  publisher path works in Chromium, Firefox and Safari; `sampleTimeline` for sample counters). `createPlayoutClock`
  takes audio reports and answers `due(timestampUs)` with wait, show, or drop, falling back to wall-clock pacing without
  audio. Both are DOM-free. `buildMsfCatalog` puts video and audio in one render group (`renderGroup: 1`, msf-01
  §5.2.11), and `parseMsfCatalog` returns `renderGroup`.
- `@mux/moq-media`: format helpers for msf-01's Media Timeline track, which maps media time to wall-clock time
  ([#291](https://github.com/muxinc/relay-sdks/issues/291)). `mediaTimelineEntry` turns a timeline timestamp and a group
  into a record, `buildMediaTimeline` and `parseMediaTimeline` write and read a timeline object, and `buildMsfCatalog`
  announces the track through `mediaTimeline` (`packaging` and `role` `mediatimeline`, `depends` on the video and audio
  tracks by default). `parseMsfCatalog` returns `depends` and `mimetype`. `createMediaTimeline({ maxEntries })` keeps
  one broadcast's records (512 by default, oldest dropped) and returns the document to send for each new entry, and the
  WebCodecs example publishes the track ([#292](https://github.com/muxinc/relay-sdks/issues/292)).
- `@mux/moq-media`: capture times on one epoch, and a Timescale-aware reader
  ([#290](https://github.com/muxinc/relay-sdks/issues/290)). Source timelines take a settable `captureLatencyUs`,
  subtracted from every timestamp, for the input delay no browser reports. `locTimestampUs` reads an object's LOC
  Timestamp in microseconds, applying the object's or the track's Timescale.
