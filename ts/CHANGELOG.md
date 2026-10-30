# Changelog

The TypeScript packages release together at one version: `@caprid/relay`, `@caprid/moq-core`, `@caprid/moq-media`,
`@caprid/moq-mediabunny`, `@caprid/relay-quic` and its platform packages, and `@caprid/relay-websocket`. Before 1.0, a minor version can break the API;
each section says what changed and what to do about it. How to cut a release is in
[docs/releasing.md](../docs/releasing.md).

## Unreleased

- `@caprid/relay-quic`: `nativeQuicTransport({ qlogDir })`, or the `RELAY_QUIC_QLOG_DIR` environment variable, writes a
  qlog trace of every connection attempt into that directory, one `relay-quic-<ms>-<pid>-<n>.sqlog` file each, from
  quinn's `qlog` feature. Off by default. A trace holds packet numbers and types, sent packets' sizes, losses, and RTT and
  congestion metrics, but no frames, stream data or token. A file that cannot be created is skipped and the dial goes on; a
  directory that cannot be created throws at once. The linux-x64-gnu addon grew from 2,766,896 to 2,912,848 bytes
  (+5.3%; gzip -9: 1,306,675 to 1,372,739 bytes). Nothing to change in an application.
- `@caprid/relay-quic`: a native QUIC connection now lets the relay have 1000 unidirectional streams open at once, up
  from quinn's default of 100, and the new `nativeQuicTransport` option
  `anticipatedConcurrentIncomingUnidirectionalStreams` sets it. The relay sends each group on a stream of its own, and
  the credit for a closed stream returns over the client's uplink: in relay-dev's voice benchmark (P-06), a subscriber
  whose upload queued its uplink for about 2 s starved of stream credit at 100, and with more credit the median delay
  went from 2865 ms to 21.5 ms locally. The option is the exact limit, an integer from 1 to 65535; any other value
  throws a `RangeError` at once. Nothing to change in an application.
- `@caprid/moq-media`: fixes to the Opus loss concealment that 0.1.1 turned on. A decoder reset clears the
  record that the last packet was CELT-only, so the next gap can use in-band FEC again (it stayed off until a
  non-CELT packet decoded). The audio worklet, not the main thread's report of the buffer, which is up to about 20 ms
  old, decides whether a fill is held: a loss while the buffer fills to its target is now concealed (it was left as
  silence), and a fill that came after the buffer ran dry is dropped (it played late, adding its length to the delay).
  A gap that spans a cut resets the decoder, as a gap over 1 s already did, so the frame after it is not decoded from
  libopus state that belongs before the gap. `synthesizedUs` and `recoveredUs` no longer count fills the buffer
  dropped. Nothing to change in an application.
- `@caprid/moq-media`: parts for a quicker join. `warmAudioPlayer(context)` loads the jitter buffer's worklet module and
  compiles libopus at once, ahead of `createAudioPlayer`, which then loads neither again (libopus's base64 is decoded
  with `Uint8Array.fromBase64` where the engine has it: `loadWasmOpus({ nativeBase64: true })`). `createFrameHold()`
  keeps a frame reader's frames until the player's input exists, so a listener can subscribe to the audio before the
  player is built. `input.joinPhases` and `player.joinPhases` record when each step of the start happened. Nothing
  changes for a player that uses none of them. The WebCodecs example's `listenAudio` now joins this way: it warms the
  player at the start, subscribes to the audio with the catalog and holds the frames until the input exists. In
  relay-dev's voice benchmark, time to first audio on a clean path went from 627 to 249 ms (WebRTC 419; spec/media-layer.md,
  "Joining sooner").
- `@caprid/moq-media`: timestamp playout, off by default. The frame sequencer's
  `timestampPlayout: { leadUs: () => input.playoutLeadUs }` holds a frame that arrives ahead of its timestamp, as a
  text-to-speech agent's bursts do, and releases it once its timestamp is within the lead of the playout clock; only a
  frame more than `horizonUs` (30 s) ahead resyncs forward, where without it one more than the target latency does.
  Delay that underruns added while a queue built is given back once frames come early again (held frames go out early
  by it, less 40 ms, and the jitter buffer trims it). `stats.early` counts the frames held, and the audio input's
  `playoutLeadUs` is the lead that keeps its jitter buffer at its target. In relay-dev's voice benchmark, bursty TTS
  played 100% with it against 5% without (WebRTC fed the same bursts: 39%); spec/media-layer.md has the numbers.
- `@caprid/relay`, `@caprid/moq-media`: track replacement for barge-in. `createReplaceableTrack(publisher, { track,
  catalog: { service, build } })` in `@caprid/relay/media` publishes a track whose `replace()` registers `audio-2`,
  `audio-3`, ..., lists it in the catalog in the old one's place, drops what the old track has not sent and closes it;
  frames written to a replacement wait for the relay's first subscription (`demandWaitMs`, 2 s).
  `findReplacementTrack(current, tracks)` finds the replacement in a newer catalog. The audio input's
  `stats.cutHeardAtUs` and `onCutHeard(atUs)` say when a cut reaches the speaker (the worklet's report after a cut comes
  at once and gains `cutId`). The WebCodecs example's `listenAudio` follows a replacement (`onTrackReplaced`, with the
  phases `seen`, `stopped` and `started`, and `listener.track`). spec/media-layer.md has the local and bench
  measurements.
- `@caprid/moq-core`, `@caprid/relay`: `MessageParameters.maxAge` (milliseconds, 0 to 2^53−1) is the MoQ Timestamp
  extension's subscriber max age (parameter `0x915C6`): a publisher that honors it skips a group once a newer group has
  superseded it for that long. A session sends it on SUBSCRIBE and REQUEST_UPDATE only to a relay whose SETUP declared
  the extension's `SETUP_OPTION.MAX_AGE` (`0x915C4`), never on FETCH, TRACK_STATUS or SUBSCRIBE_NAMESPACE, and leaves it
  off otherwise: moq-dev main declares it, moq-relay 0.17.x does not. `RelaySubscriber.maxAgeSupported` and
  `MoqtSession.maxAgeSupported` report whether the relay did. Nothing changes for an application that sets no `maxAge`.

## 0.2.0

- `@caprid/relay`: `roomEndpoint(room)` roots the hosted Relay, `https://relay.mux.global`, so a quickstart passes
  only the token's root. `roomEndpoint(endpoint, room)` works as before. The endpoint is exported as
  `DEFAULT_ENDPOINT`, as the Python SDK already did.
- `@caprid/moq-core`: relay-side test support (#367). `createMoqtSession` takes `perspective: 'client' | 'server'`
  (default `'client'`, unchanged): a `'server'` session allocates odd Request IDs from 1 and closes with
  `INVALID_REQUEST_ID` on an odd one from its peer, so it can stand in for the relay against a publish session over an
  in-memory pair without rewriting Request IDs on the wire. A new `@caprid/moq-core/wire` entry point exports the
  codec such a test drives the session with (control-message encoders and decoder, the deframer, subgroup and fetch
  stream readers, varints, `StreamReader`); import from it instead of `dist/protocol/*`. Nothing changes for
  `@caprid/relay`.
- `@caprid/relay`, `@caprid/moq-core`: a publisher connection answers the relay's TRACK_STATUS (draft-22 §9.13) for its
  tracks instead of refusing it with `NOT_SUPPORTED`: TRACK_STATUS_OK with the track's properties and, once the track
  has written an object, LARGEST_OBJECT; an unknown or closed track gets `DOES_NOT_EXIST`, as a SUBSCRIBE does.
  moq-relay `main` asks a publisher for a track's status before it subscribes on behalf of another relay, and a track
  whose publisher refuses does not cross two relays there. Nothing changes for an application. In `@caprid/moq-core`,
  `RegisteredTrack.objectPublished(location)` records what a TRACK_STATUS_OK reports as LARGEST_OBJECT (a track with
  `getLargestObject` reports that instead, as its SUBSCRIBE_OK does), and `encodeRequestOk` takes the Track Properties
  as a third argument.
- `@caprid/moq-media`: the audio player's jitter buffer gives back what a delay spike added in seconds rather than a
  minute: +40 ms per underrun, −10 ms after each 2 s of smooth playback, a 20 ms floor (was +20 ms, −10 ms per 8 s,
  40 ms), and trims of 5 ms or more. On stage-2 netbench arrivals replayed offline it held 9–74 ms less (clean 9, busy
  Wi-Fi 28, LEO 42, LTE 74) at about the same drops, and 1.5 drops a minute more under a bulk download's queue
  sawtooth; pass `minJitterTargetUs: 40_000` for the old floor.

## 0.1.1

- `@caprid/moq-media`: lost Opus frames are filled rather than played as silence. Opus inputs decode with libopus
  compiled to WebAssembly (116 KB, embedded), which conceals a missing frame (packet-loss concealment, up to 120 ms a
  gap) and rebuilds it from the next frame's in-band FEC when the publisher's encoder sent one. Stats `synthesizedUs`
  and `recoveredUs` count them. `createAudioPlayer({ context, lossConcealment: false })` decodes with WebCodecs as
  before, as does any input where WebAssembly is unavailable or the codec is not Opus.
- `@caprid/moq-core`: publish-session hooks for embedders such as Video.js. A registered track's `getLargestObject`
  puts `LARGEST_OBJECT` in SUBSCRIBE_OK and REQUEST_OK; with it, a nonempty fill request gets a fill fetch stream
  that is reset after its FETCH_HEADER and counted in PUBLISH_DONE. `RegisteredTrack.end({ after })` holds the
  subscriptions' end until a last group is written, also during an orderly close. Nothing changes for `@caprid/relay`,
  which uses neither.
- `@caprid/moq-core`: a REQUEST_UPDATE carrying `FILL_PARAMETERS` is acknowledged instead of ending the subscription
  with `UPDATE_FAILED`. moq-relay sends no REQUEST_UPDATE, so the relay path is unchanged.
- `@caprid/moq-core`: `MoqtTransport.protocol` is optional; a transport without it gets the `moqt-22` codec. The
  index also exports `TRACK_PROPERTY`, `compareLocations`, `isEmptyFetchRange`, `encodeFetchHeader`,
  `isRetryableRequestErrorCode`, `isRetryablePublishDoneStatus`, `utf8Decode`, `CLOSE_FLUSH_TIMEOUT_MS` and
  `CLOSE_END_ACK_TIMEOUT_MS`.

## 0.1.0

- `@caprid/relay`: `connectRelayPublisher` and `connectRelaySubscriber` for browsers over WebTransport (Chromium, and
  Firefox 155 or later), and `@caprid/relay/node` for Node 24+ over native QUIC.
- `@caprid/relay`: `roomEndpoint(endpoint, room)` builds the recommended room-rooted endpoint, the endpoint with the
  token's root appended to its path. It keeps the query and encodes nothing: moq-relay reads the path without
  percent-decoding it, so a field that would need encoding (a space, `%`, `/`, `*`, non-ASCII, `.` or `..`) throws a
  `RangeError`, and that room is reached from the bare host.
- `@caprid/relay-websocket`: a transport adapter for QMux over WebSocket, the relay's TCP 443 path for networks that
  block UDP and browsers without a usable WebTransport. Pass `createWebSocketTransport()` as `createTransport` to
  always use it, or `withWebSocketFallback()` to dial WebTransport first and WebSocket when that handshake fails or
  takes longer than `handshakeTimeoutMs` (4 s). Nothing falls back unless you pass one. Over WebSocket a refused token
  is `TRANSPORT_ERROR`, not `UNAUTHORIZED`, and every stream shares one TCP connection. `RelayTransportFactory`'s
  `framing` takes `'websocket'`, which sends the same SETUP as `'webtransport'`.
- `@caprid/relay/media`: the media publishing policy Python has as `caprid_relay.media`.
  `createFrameWriter(track, options)` maps encoded frames onto groups (a group per keyframe, or per frame) and can be
  called from a WebCodecs output callback without awaiting: frames wait in a bounded backlog (`maxBacklog`), and when
  it overflows the writer drops rather than queues. Every frame gets one `FrameOutcome`, and `keyframeNeeded` /
  `onKeyframeNeeded` say when the encoder should force a keyframe. `createCatalogService` and `createDocumentService`
  serve the MSF catalog and the media timeline on every relay subscription. The WebCodecs example publishes through
  all three.
- `@caprid/relay/media`: `mediaTrackProperties('audio' | 'video', options)` gives a media track a default subgroup
  delivery timeout, so a stall costs stale frames instead of lasting latency: 500 ms for audio, and for video the
  target latency less the keyframe interval, never less than one interval (one interval by default). Pass its result
  to `registerTrack` beside `locTrackProperties()`; video needs `keyframeIntervalUs`. The WebCodecs example uses it.
  It also sets a default publisher priority (`0x0E`) that puts audio ahead of video, 16 for audio and 96 for video,
  which `publisherPriority` replaces. moq-relay 0.17 passes it to viewers but orders delivery by their subscriber
  priority alone, so it changes no delivery there yet.
- `@caprid/relay/media`: `mediaSubscribeParameters('audio' | 'video', overrides?)` gives a viewer's subscription the
  matching subscriber priority, 16 for audio and 96 for video, the one moq-relay 0.17 orders delivery by. Pass it as
  `parameters` to `subscribe`, `want` or `switchTo`; `overrides` adds other parameters, and its `subscriberPriority`
  replaces the default.
- The publisher opens each data stream of a track with a publisher priority (`0x0E`) with a WebTransport send order
  from it, so a congested uplink sends audio before video, and both after the control streams. Through an 800 kbit/s
  uplink cap it brought a Firefox publisher's audio p99 from 264–302 ms to 108–109 ms. Firefox 155 schedules by it;
  Chromium 153 ignores it. `@caprid/relay-websocket` and `@caprid/relay-quic` schedule by it too. A transport
  adapter's `createUnidirectionalStream` now takes `{ sendOrder }` (`MoqtSendStreamOptions`, TA-28); an adapter that
  cannot schedule ignores it.
- `openGroup({ payload, properties })` opens a group with its first object, so that object's delivery timeout counts
  the wait for demand, settling room and the stream; a bare `openGroup()` has no object to time. The frame writer
  opens every group this way, so a frame whose open fails reports no `groupId`, and `groups` counts only opens that
  returned a group.
- `@caprid/moq-media`: transcripts. `buildTranscriptSegment` and `parseTranscriptSegment` write and read the
  transcript track's segments (`TRANSCRIPT_TRACK`, `'transcript'`): the text of a talker's audio from `fromUs`, sent
  in interim and final versions that share a `seq`. `createTranscript()` keeps a listener's segments, retracts what an
  interrupt cut at the playout position, and gives the lines heard by a position. The WebCodecs example's
  `listenAudio` keeps one as `listener.transcript` when the catalog lists the track, and the voice bot page shows the
  bot's words as its audio plays.
- `@caprid/moq-media`: interrupts. `buildInterrupt` and `parseInterrupt` write and read the interrupt track's messages
  (`INTERRUPT_TRACK`, `'interrupt'`), which tell a listener to drop audio a talker already sent; a parsed `Interrupt`
  is an `AudioCutRange`, so `input.cut(interrupt)` applies it. The WebCodecs example's `listenAudio` applies them when
  the catalog lists the track, and `publishCanvas` takes `data` tracks to list. A new voice bot page
  (`examples/voice-bot`) talks to the Python bot through them, on the WebCodecs example's new audio-only
  `publishMicrophone`.
- `@caprid/moq-media`: a headless audio player. `createAudioPlayer({ context })` mixes any number of inputs into one
  `output` node; each `player.addInput({ clock, decoderConfig })` decodes with WebCodecs, resets its decoder at
  discontinuities, holds audio in an adaptive `AudioWorklet` jitter buffer (60 ms to start, 40–400 ms, under the
  clock's target latency), and reports what is audible to the playout clock. `input.push` is a frame reader's
  `onFrame`; `input.cut({ fromUs, untilUs })` drops received audio in a timeline range. Also
  `createEchoCancellationLoopback` for Chromium's echo canceller, `audioDecoderConfigFromTrack`,
  `opusPacketDurationUs`, and `AUDIO_ONLY_TARGET_LATENCY_US` (500 ms) for an audio-only clock, whose 2 s default
  otherwise waits for keyframes. `createRenderClock` moved into the package from the WebCodecs example, which gains an
  audio-only listener, `listen-audio.ts`. `input.playbackRate` (0.5 to 2) nudges latency without cuts or silence, and
  an input's `clock` is any `AudioInputClock`, which each report hands the input's stats, now with `playStarts`,
  `heldUntilUs` and `dryAtUs`.
- Reading frames in decode order within a latency budget. `@caprid/moq-media`'s
  `createFrameSequencer({ clock, grouping })` orders one track's objects across group streams, waits for a late group
  only while it can still matter, skips to the next keyframe past the deadline, drops late audio, and marks each
  `gap`, `resync` or `restart` where a decoder resets. `@caprid/relay/media`'s
  `createFrameReader(sequencer, { onFrame })` is the subscription's or want's handlers that drive it. The playout
  clock holds the budget: `targetLatencyUs`, by default twice the longest recent keyframe interval
  (`reportKeyframeInterval`), 2 s before one is known; wall pacing now re-anchors on a late frame only beyond it,
  instead of beyond `maxVideoDelayUs`. Both examples now force a keyframe at least every second.
- Frame grouping plays every frame of a group that holds several, one per object, as `openGroup()` writes them: the
  sequencer's position is a group and an object, so a group's later objects are no longer dropped as behind its first,
  and an object skipped inside a group is a `gap`. Groups of one frame, as the frame writer sends audio, behave as
  before.
- Voice delivery per direction (relay-3cnp, the voice benchmark). `createFrameSequencer` takes three per-track
  options, each off by default: `lateDropUs` (frame grouping) replaces the clock's late-drop limit for one track,
  `Infinity` dropping nothing; `reorderWindowUs` (frame grouping) holds newer audio that long for a missing earlier
  group instead of dropping the latecomer; and `catchUpAfterUs` makes `poll()` return `catchUp: true` once audio has
  kept arriving late that long. `createFrameReader`'s `onCatchUp` receives it, and the supervised subscriber's new
  `catchUp(key)` replaces the want's subscription at the live edge, which drops the relay's backlog; `onRestarted`
  reports the new reason `'catch-up'`. A delivered frame after skipped groups carries `missingGroups`
  (`MissingGroups`: `firstGroupId`, `lastGroupId`). moq-relay 0.17.5 and 0.17.7 ignore a subscriber's delivery timeout,
  so on those releases these are how a subscriber bounds its delay; they work with a relay that honours it too
  ([spec/relay-interop.md](../spec/relay-interop.md)). TypeScript only.
- `@caprid/relay`: `subscriber.namespaceEvents({ prefix })` discovers which namespaces under a prefix are announced.
  The subscriber's SETUP now carries the SOLICIT option, so the relay advertises namespaces only when asked. A prefix
  overlapping one the connection already discovers is refused with the new `PREFIX_OVERLAP` code (draft-22 §9.15).
  `@caprid/moq-core` gains `session.subscribeNamespace` and `SETUP_OPTION.SOLICIT`.
- `@caprid/relay`: opt-in supervised connections. `connectSupervisedPublisher` reconnects with backoff, serving the
  root namespace plus a fresh generation field per connection, and rebinds the same track writers to every generation;
  a write between generations fails with the new `RECONNECTING` code and is never replayed.
  `connectSupervisedSubscriber` keeps tracks subscribed through `want(key, options, handlers)`: it retries the
  announce race, resubscribes after a done, a rejection or a stale track, moves to replacement connections, follows a
  supervised publisher's newest generation through discovery, and reports `onRestarted`. An `onStatus` or
  `onGeneration` that throws stops the supervisor with that error; an `onRestarted` that throws is logged and the want
  stays subscribed. Both are in `@caprid/relay/node` too. The primitives are unchanged. The examples' shared
  `announce-race.js` is retired: the demo and the live smoke test subscribe through `want`.
- `@caprid/relay`: `subscriber.subscribeSwitchable(options, handlers)` moves one set of handlers between tracks
  make-before-break. `switchTo(track, { parameters, until, drainMs, signal })` subscribes to the new track while the
  old one keeps delivering, cuts over on the first object `until` accepts, reports `onSwitched`, and can keep the old
  subscription open briefly for groups still in flight. It is how to change a track, filter or priority, which
  `update()` cannot do against moq-relay.
- `@caprid/relay`: a supervised want moves make-before-break. A newer generation, or wanting the key again with
  another track or parameters, opens the replacement while the current subscription keeps delivering, and cuts over on
  the first object `until` accepts. The new `WantOptions` fields `until` and `drainMs` work as they do on `switchTo`,
  and the new `onSwitched` handler receives a `WantSwitchedInfo` (`from`, `to`, `subscribed`). A generation move no
  longer reports `onRestarted({ reason: 'generation' })`; handle `onSwitched` instead, where decoders reset. Wanting a
  key again with only `staleAfterMs` changed now swaps it without resubscribing. Another namespace or `generations`
  still replaces the want.
- `@caprid/relay`: sessions end at the token's expiry, and the supervisors now rotate before it. `getToken` may return
  a `RelayCredential` (`{ token, expiresAt }`); every connection exposes `expiresAt`, from that or the JWT's `exp`
  claim. Both supervisors take `rotateBeforeMs` (default 60 s) and `rotate()`, and replace their connection
  make-before-break: the publisher moves each track to the next generation once it has demand there, and every want
  moves its subscription to the new connection as a move does. A want whose target is unchanged cuts over on the first
  object the old subscription did not deliver, with no callback, so nothing is lost or repeated. A session closed with
  `UNAUTHORIZED` (0x2) after SETUP, which is how moq-relay ends an expired or revoked session, is now `UNAUTHORIZED`
  in phase `connection` rather than `TRANSPORT_ERROR` or a clean close.
- `@caprid/relay-quic`: the native QUIC transport adapter for Node, with prebuilt binaries for Linux (x64 and arm64,
  glibc and musl), macOS (arm64 and x64) and Windows (x64).
- `@caprid/moq-core`: the transport-independent MOQT sessions the SDK is built on.
- MOQT draft-22 by default: connections offer `moqt-22`, then `moqt-20` as a fallback, and speak whichever the relay
  selects. The drafts differ on the wire only in the LOCATION_FILTER parameter, which `@caprid/moq-core` now encodes
  for the negotiated draft: its encoders and decoders that carry message parameters take a trailing `protocol`
  argument (default `moqt-22`), and it exports `MOQT_PROTOCOL_IDS`. `MOQT_PROTOCOL_ID` is now `moqt-22`. moq-relay
  0.17.0 frames LOCATION_FILTER the draft-20 way on `moqt-22`, so a Next Object filter closes the session there; use a
  relative start of 0.
- `@caprid/moq-media` and `@caprid/moq-mediabunny`: LOC and MSF format helpers, and an optional Mediabunny adapter.
  `buildMsfCatalog` leaves `isComplete` out, since a live broadcast is not complete (msf-01 §5.1.3), and
  `buildMsfEndCatalog` builds the §11.3 catalog that ends a broadcast: `isComplete: true` and no tracks.
- `@caprid/moq-media`: one timeline, and a playout clock for audio/video sync. `createBroadcastClock` stamps every
  track in microseconds since the Unix epoch, so broadcasts line up with each other (`sourceTimeline` with `calibrate`
  for an `AudioContext` clock or `MediaStreamTrackProcessor` timestamps, so the publisher path works in Chromium,
  Firefox and Safari; `sampleTimeline` for sample counters). `createPlayoutClock` takes audio reports and answers
  `due(timestampUs)` with wait, show, or drop, falling back to wall-clock pacing without audio. Both are DOM-free.
  `buildMsfCatalog` puts video and audio in one render group (`renderGroup: 1`, msf-01 §5.2.11), and `parseMsfCatalog`
  returns `renderGroup`.
- `@caprid/moq-media`: `buildMsfCatalog` describes any number of video and audio tracks. `media` takes
  `MediaTrackInfo` entries (`{ role: 'video' | 'audio', ... }`), and every video or audio description takes
  `renderGroup` (default 1) and `altGroup` (msf-01 §5.2.12) for simulcast layers; members of an alternate group must
  share a role and a render group. `video` and `audio` are unchanged. `parseMsfCatalog` returns `altGroup`, and
  `groupMsfTracks` lists the sets of tracks a viewer picks one of.
- `@caprid/moq-media`: format helpers for msf-01's Media Timeline track, which maps media time to wall-clock time.
  `mediaTimelineEntry` turns a timeline timestamp and a group into a record, `buildMediaTimeline` and
  `parseMediaTimeline` write and read a timeline object, and `buildMsfCatalog` announces the track through
  `mediaTimeline` (`packaging` and `role` `mediatimeline`, `depends` on the video and audio tracks by default).
  `parseMsfCatalog` returns `depends` and `mimetype`. `createMediaTimeline({ maxEntries })` keeps one broadcast's
  records (512 by default, oldest dropped) and returns the document to send for each new entry, and the WebCodecs
  example publishes the track.
- `@caprid/moq-media`: capture times on one epoch, and a Timescale-aware reader. Source timelines take a settable
  `captureLatencyUs`, subtracted from every timestamp, for the input delay no browser reports. `locTimestampUs` reads
  an object's LOC Timestamp in microseconds, applying the object's or the track's Timescale.
