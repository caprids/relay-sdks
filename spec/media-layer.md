# Media layer proposal: LOC packaging and MSF catalogs

Status: stage 1 implemented; stage 2 partly implemented in Python. The format packages
(`@mux/moq-media`, `moq-media`), shared vectors, protocol pins, and Python example migration are
present, with [behavior.md](behavior.md#media-helpers) as their contract. Of the "Media publishing"
piece below, the catalog service and the frame writer exist as `mux_relay.media` (`CatalogService`,
`FrameWriter`), with [behavior.md](behavior.md#media-publishing) as their contract and the Video.js
example as their first caller; the `register_media` handle that would compose them with the catalog
builder and `update()` of a served document remain proposals. Of the readers (stage 3), the
catalog reader ships in both format packages as `parseMsfCatalog` / `parse_msf_catalog`
([issue #164](https://github.com/muxinc/relay-sdks/issues/164)), and the Python example's
`--verify` logs its track list; the object-to-frame reader remains a proposal. The
[shared timeline and playout clock](#shared-timeline-and-playout-clock) ship in both format
packages ([issue #285](https://github.com/muxinc/relay-sdks/issues/285)), and so do the
[Media Timeline track](#media-timeline-track)'s format helpers
([issue #291](https://github.com/muxinc/relay-sdks/issues/291)). The WebCodecs example publishes
audio on the shared timeline, measured live in browsers
([issue #288](https://github.com/muxinc/relay-sdks/issues/288)). Public names for the
implemented stages are recorded in [vocabulary.md](vocabulary.md).

This design was written 2026-09-18. Its before/after sketches describe the eventual API; they are
not runnable stage 1 examples. See the format packages' READMEs for current usage. Stage 1 retains
`VideoTrackInfo` / `AudioTrackInfo`, adds `DataTrackInfo`, and uses `build_msf_catalog` /
`buildMsfCatalog` for UTF-8 bytes. The TypeScript catalog reader is the Video.js parser's
independent-catalog path, promoted to `parseMsfCatalog`; player presentation projection and delta
updates are not part of this extraction.

## TypeScript input integration

Stage 1 accepts native WebCodecs chunks and decoder configs through `@mux/moq-media`:
`chunkToLocFrame`, `videoTrackFromDecoderConfig`, and `audioTrackFromDecoderConfig`. The
[direct browser example](../ts/examples/webcodecs/README.md) connects `VideoEncoder` output to
LOC/MSF and Relay without a media engine. The pure helpers copy chunk bytes without converting
the bitstream; encoding, callback buffering, and capture remain application concerns.

The optional `@mux/moq-mediabunny` adapter handles file-input packets, demonstrated by the
[MP4 example](../ts/examples/mediabunny/README.md). Mediabunny owns demuxing. Standard decoder
config mapping, LOC properties, MSF catalogs, and source URLs belong to `@mux/moq-media`, with
no Mediabunny dependency. The adapter retains compatibility re-exports of the config mappers.

TypeScript consumes supplied AVC configuration and four-byte-length packets; Annex B splitting,
SPS/PPS reconstruction, and conversion remain Python-only for the PyAV example. Config mappers
currently support AVC and Opus. Grouping, catalog serving, demand, and pacing remain TypeScript
example code; this does not implement the stage 2 publishing handle.

## What a media publisher has to know today

The product SDKs stop at bytes, groups, and demand. `behavior.md` says so on purpose: "The SDK adds
no default timescale, timestamp, or LOC media packaging." Everything a Video.js MoQ player needs
above that line is rebuilt inside the example:

| Step | What the example does | Where the knowledge comes from |
| --- | --- | --- |
| Timing | Declares Timescale 1,000,000 as a track property on every track and puts Timestamp and Timescale on every object, with Video Config on keyframes | draft-ietf-moq-loc-04 registry; [issue #23](https://github.com/muxinc/relay-sdks/issues/23) for why the track property is mandatory against the relay |
| Decoder configuration | Opens the encoder with global headers, splits the Annex B extradata into NAL units, keeps SPS and PPS, builds an avcC, and reads `avc1.PPCCLL` out of its bytes 1 to 3 | ISO/IEC 14496-15, RFC 6381 §3.3 |
| Bitstream contract | Re-frames every access unit from Annex B start codes to 4-byte length prefixes and drops SPS, PPS, and AUD, because `avc1` promises parameter sets out of band | ISO/IEC 14496-15 |
| Catalog document | Assembles `version`, a slash-joined `namespace` per track, `packaging: "loc"`, `isLive`, `role`, codec and dimensions, and `initDataList` with base64 inline entries and per-track `initRef`; leaves out `isComplete`, which would declare the live broadcast over ([#284](https://github.com/muxinc/relay-sdks/issues/284)) | draft-ietf-moq-msf-01 §5, read against the Video.js parser for the exact dialect |
| Catalog track | Registers a track named `catalog` and re-sends the document on every relay subscription, retrying transient failures while demand remains | Video.js `parse-source.ts`; the demand semantics in `behavior.md` |
| Group mapping | Opens a video group on each keyframe and appends deltas; sends audio as one group per frame; after any failure or loss of demand, discards deltas until the next keyframe | MSF group mapping; Video.js `track-publisher.ts` |
| Source URL | Composes `moqt://host/path#msf:<ns>--catalog&c4m=<token>`, escaping every byte outside `[A-Za-z0-9_]` in each namespace field as `.xx` | msf-01 §11.1; Video.js `parse-source.ts` |
| Relay reality | Carries the avcC in the catalog even though LOC puts it on keyframes, because the relay drops odd-typed object properties | Issue #23, observed against relay 0.14.17 |

That is 347 lines in `moq_media.py`, none of it encoder or network code, plus about 250 lines of
catalog serving and group mapping in `stream.py`, before a single frame is encoded. None of it is
specific to this example. A GStreamer, ffmpeg, or WebCodecs publisher writes the same code, and the
Video.js publisher branch has in fact written it again in TypeScript (`build-catalog.ts`,
`loc-packaging.ts`, `avc-codec.ts`, `composeMoqSource` in `parse-source.ts`).
[Issue #29](https://github.com/muxinc/relay-sdks/issues/29) is the symptom: the Python README's own
media snippet calls `loc_properties()`, a function that exists only in the example.

## Where the line goes

Three tiers. The SDK owns the bottom two.

| Tier | Owner | Contents |
| --- | --- | --- |
| Encoding | Application | Capture, encoder configuration, pacing, time-based buffering and drop policy, reconnection. PyAV, GStreamer, ffmpeg, WebCodecs. |
| Media packaging | SDK, proposed | LOC object properties, the MSF catalog and its serving loop, the source URL, decoder configuration records, keyframe-to-group mapping |
| Transport | SDK, exists | Connections, tracks, groups, demand, deadlines, cancellation |

The boundary is the encoded frame: bytes, a timestamp, and a keyframe flag. That is what a WebCodecs
`EncodedVideoChunk` carries and what Video.js's `loc-packaging.ts` consumes. The SDK never configures
an encoder and never decides that a frame is too old to send; the example keeps its `LiveQueue`.
The SDK does turn an encoder's output into something a LOC/MSF player can play, and the one place
the tiers touch, decoder configuration, gets helpers in the packaging tier because an `avc1` catalog
entry is meaningless without an avcC and PyAV hands back Annex B.

## What the example would look like

The Python example, with the encoder, test pattern, pacing, and `LiveQueue` unchanged:

```python
from mux_relay import connect_relay_publisher
from mux_relay.media import AudioTrack, VideoTrack, register_media  # provisional
from moq_media.h264 import H264Config  # provisional

publisher = await connect_relay_publisher(url, namespace=namespace, get_token=lambda: token)

# libx264's extradata is Annex B SPS/PPS. This yields the avcC and the codec string it declares.
h264 = H264Config.from_annexb(encoder.extradata)

media = register_media(
    publisher,
    video=VideoTrack(codec=h264.codec, width=640, height=360, framerate=30, bitrate=1_500_000, init_data=h264.avcc),
    audio=AudioTrack(codec="opus", bitrate=96_000),  # 48 kHz and channel_config "2" are the Opus defaults
)
print(media.source_url(url, token=token))

for sample in encoded_samples():  # the application's encoder loop, however it is built
    if sample.kind == "video":
        await media.video.write_frame(h264.to_avcc(sample.payload), timestamp_us=sample.pts, key=sample.key)
    else:
        await media.audio.write_frame(sample.payload, timestamp_us=sample.pts)

media.close()
await publisher.close()
```

Behind those calls: three tracks registered with a microsecond timescale; a catalog built with
`packaging: "loc"`, the `initDataList` entry and `initRef`, and the `avc1` entry withheld until an
avcC exists; a catalog service that re-sends the document on every relay subscription; frame
writers that open a group on each keyframe, append deltas, drop frames while the relay has no
demand, and resume at the next keyframe after any failure; and a source URL with the namespace
escaped the way the player decodes it.

The same in a browser, where WebCodecs reports the avcC with the first encoded chunk rather than up
front, which is why the catalog has to be updatable:

```ts
import { connectRelayPublisher } from '@mux/relay';
import { registerMedia } from '@mux/relay/media'; // provisional

const publisher = await connectRelayPublisher({ url, namespace, getToken });
const video = { codec: 'avc1.42E01F', width: 640, height: 360, framerate: 30, bitrate: 1_500_000 };
// The video entry is advertised once initData is known; the audio entry is advertised now.
const media = registerMedia(publisher, { video, audio: { codec: 'opus', bitrate: 96_000 } });
console.log(media.sourceUrl(url, { token }));

const encoder = new VideoEncoder({
  output: (chunk, metadata) => {
    const description = metadata?.decoderConfig?.description;
    if (description) media.update({ video: { ...video, initData: new Uint8Array(description as ArrayBuffer) } });
    void media.video.writeChunk(chunk); // key: new group; delta: append; LOC properties attached
  },
  error: console.error,
});
encoder.configure({ ...video, avc: { format: 'avc' } });
```

## The pieces

### Format helpers

Pure functions and small records. No I/O, no writer types, usable from a test or a script that
never opens a connection. Each row names what it replaces in the example and the Video.js module it
mirrors, so the TypeScript side can start as an attributed snapshot the way `@mux/moq-core` did.

| Helper | Replaces in the example | Video.js module |
| --- | --- | --- |
| LOC property registry (`0x10` Timestamp, `0x08` Timescale, `0x0D` Video Config, `0x0F` Audio Config) and the microsecond default | The constants block in `moq_media.py` | `loc.ts` |
| Track properties for a LOC track (declared timescale) and object properties for one frame (timestamp, timescale, config on keyframes) | `TRACK_TIMESCALE_PROPERTIES`, `loc_properties()` | `loc-packaging.ts` |
| Catalog builder taking video, audio, and data track descriptions and a namespace, returning the MSF `draft-01` JSON bytes | `VideoTrackInfo`, `AudioTrackInfo`, `build_msf_catalog()` | `build-catalog.ts` |
| Source URL composer, and the namespace and track-name escaping it depends on | `compose_player_url()`, `encode_namespace_name()` | `composeMoqSource`, `encodeNamespaceName` in `parse-source.ts` |
| H.264 configuration: SPS/PPS from Annex B, avcC from SPS/PPS, `avc1.PPCCLL` from an avcC, and Annex B to AVCC re-framing for each access unit | `extract_parameter_sets()`, `build_avcc()`, `avc_codec_string()`, `annexb_to_avcc()` | `avc-codec.ts` (the codec string; the browser does the re-framing) |

The catalog builder is where most of the value is, because it can refuse to build what a player
cannot play. The rules come straight from what the example and the Video.js publisher learned:

- An `avc1` or `avc3` entry with no init data is not emitted. WebCodecs never puts parameter sets in
  band for `avc` format, and the relay drops the per-keyframe Video Config property, so a track
  advertised without `initRef` is one a player configures from once and never recovers from.
- The H.264 codec string is derived from the avcC when one is supplied, not copied from the
  requested profile: an encoder may honor the profile and still choose its own constraint flags and
  level, and the string is the only place the player learns the bitstream contract.
- Opus declares `samplerate: 48000` whatever the encoder was fed, because that is the rate an Opus
  stream decodes at (RFC 7845 §5.1), and `channelConfig` is a string.
- Data tracks are emitted with name and role only. A media role on an entry with no codec would
  round-trip into an undecodable renderable track.
- Fields the caller left out are omitted, never nulled, and the document is compact JSON, so the
  bytes are stable enough to compare and to put in `spec/vectors/`.

### Media publishing

The stateful part, which wraps `RelayTrackWriter` and therefore lives beside the product API. One
call registers the catalog track and every media track with the LOC timescale declared, builds
the first catalog, and starts serving it. It returns a handle with one frame writer per media
track, the raw catalog writer for anyone who wants it, and a `close()` that ends the catalog
service and the tracks. A deliberate `close()` follows msf-01 §11.3: it ends the media tracks,
then sends the end-of-broadcast catalog (`isComplete: true`, no tracks) before closing the catalog
track. A failure does not send it.

The catalog service is the loop `serve_catalog()` runs today, generalized:

- On every `True` from the catalog track's demand iterator, write the current document as one
  independent group. The repeated `True` for a replacement relay subscription is the point; it is
  what a viewer joining after a relay reconnect needs.
- `update()` rebuilds the document from new track descriptions. Identical bytes send nothing.
  Changed bytes go out as a new group at once if demand is present, and on the next subscription
  otherwise. Every catalog object is a random-access point, which is what the MSF independent
  catalog form requires.
- A transient write failure is retried after a pause while demand remains, keeping only the latest
  document. A closed track or connection ends the service and is reported once.
- The catalog advertises only tracks the handle has registered. Adding a track through `update()`
  registers it first; removing one from the catalog does not free its name on this connection,
  which `behavior.md` already forbids, so the handle says so rather than letting a later `update()`
  fail on the relay.

The frame writer is the mechanism in `publish_media()` today, without the example's time-based
staleness policy:

- Video: a keyframe closes the open group and opens a new one; a delta appends to the open group.
  While the relay has no demand, frames are counted and not sent, without waiting on the write
  deadline, because an encoder loop cannot block for ten seconds. When demand returns, and after any
  group failure, deltas are discarded until the next keyframe.
- Audio, and any track marked as one group per frame: every frame is its own single-object group,
  the LOC mapping for independently decodable frames.
- LOC object properties are attached from the frame's timestamp and the track's declared timescale.
  The track's current init data rides keyframes as Video Config or Audio Config, for consumers
  that receive odd-typed properties, and the catalog stays the copy players rely on.
- Calls on one writer are serialized in arrival order, so a callback-driven encoder can call it
  without tracking a pending write. Backpressure is bounded in whole groups: when more than a
  small number of groups (Video.js uses three) are still queued at a group boundary, the oldest
  are reset and publishing resumes with the boundary keyframe. Whether to discard a frame because
  it is late against a clock stays with the application.
- Counters mirror Video.js's track publisher: groups opened and finished, objects and bytes
  written, frames skipped for no demand, frames skipped awaiting a keyframe, groups dropped. The
  example's `finished` line would read them instead of keeping its own.
- Transient failures are absorbed and counted. A closed track surfaces as `closed` on the writer
  and as an error on the next call, so an application that wants the example's stop-the-run policy
  can still have it.

### Reading side

The example's `--verify` mode and any non-browser consumer do the inverse by hand. Two readers,
symmetric with `read_text` and `read_json`:

- Parse a catalog object into track descriptions (the independent catalog form; delta updates can
  wait until a publisher in this repository emits them). Shipped as `parseMsfCatalog` /
  `parse_msf_catalog`; see [behavior.md](behavior.md#media-helpers).
- Turn an object plus the track's received timescale into a frame: payload, timestamp in
  microseconds, keyframe flag from object ID 0, and any config property that survived the relay.
  Video.js's `toLocFrame` is the reference, including its acceptance of the draft-03 timestamp ID.
  Its timestamp part ships as `locTimestampUs` / `loc_timestamp_us`
  ([issue #290](https://github.com/muxinc/relay-sdks/issues/290)): an object's Timestamp in
  microseconds, scaled by the first Timescale it finds on the object, then in the track properties
  (`SUBSCRIBE_OK`'s, as the relay sends them), then in the catalog. With none, the Timestamp already
  is microseconds since the epoch (LOC §2.3.1.1). Reading `0x10` as raw microseconds works against
  moq-relay only because it rescales every timestamp to microseconds and declares Timescale
  1,000,000; a relay that passes the publisher's Timescale through needs the scaling.

## Shared timeline and playout clock

[Issue #285](https://github.com/muxinc/relay-sdks/issues/285). Implemented in both format packages;
[behavior.md](behavior.md#media-helpers) holds the testable rules.

### One timeline, anchored at the Unix epoch

A broadcast is the set of media tracks one MSF catalog lists. Its publisher stamps every one of
them from one broadcast clock:

- The LOC Timestamp of each object is the presentation time of its first sample: its capture time,
  in microseconds since the Unix epoch on the publisher's system clock. Each track declares
  Timescale 1,000,000 (`locTrackProperties` / `loc_track_properties`). That is LOC's own default
  unit and zero (draft-04 §2.3.1.1: without a Timescale, the Timestamp is wall-clock microseconds
  since the epoch). The Timescale is declared anyway because moq-relay replaces the Timestamp of a
  track without one with its own clock (see below). With a Timescale, LOC §2.3.1.2 leaves the
  anchor to the application, and this convention fixes it at the epoch.
- Audio and video share that anchor. A track never starts its own count at zero: audio stamped from
  a sample counter is anchored at the broadcast time of its first sample, and video stamped from
  capture timestamps is mapped onto the same epoch as everything else.
- Every broadcast on this convention shares one unit and one zero, so timestamps line up across
  catalogs as well as within one: huddle participants, a third-party data track, several viewers
  playing in lockstep. That is the only binding between catalogs that the drafts define (msf-01
  §5.2.8 `targetLatency`, §7 Media Timeline, §8 Event Timeline, all in wall-clock time). How well
  two broadcasts line up is bounded by how far apart their publishers' system clocks are, and by
  each publisher's capture-latency error (see [Capture latency](#capture-latency)).
- [Issue #290](https://github.com/muxinc/relay-sdks/issues/290) chose this over #285's original
  convention, which zeroed each broadcast at its own monotonic origin. Under that convention a
  player could reach wall-clock time only through the millisecond-resolution Media Timeline, which
  rules out honouring `targetLatency` per frame and leaves syncing between broadcasts undefined.
  The cost is that a publisher with a wrong system clock now produces wrong wall-clock times, and
  that nothing in msf-01 signals which anchor a broadcast uses: a player that reads these
  timestamps as wall-clock time relies on this convention.
- The catalog puts the video and audio entries in one render group: `renderGroup: 1` (msf-01
  §5.2.11, "tracks with the same group number SHOULD be rendered simultaneously"). That is
  msf-01's way to say tracks play together, and moq-rs catalogs already send it. There is no
  `timeline` field, because msf-01 defines none and a player ignores fields it does not know. The
  builder sets no target latency or buffer, which §5.2.8 and §5.2.9 require to be identical within
  a render group. The catalog reader returns `renderGroup` as declared. The Video.js MoQ player
  parses it but does not act on it yet.
- A supervised publisher's next generation is a new broadcast with a new broadcast clock. Its
  timestamps continue on the same epoch, less any step in the system clock between the two
  clocks' creation. A player still resets its playout clock wherever it resets its decoders: at a
  Restart, at a Move, and on a new catalog.
- The playout clock below syncs the tracks of one broadcast. Playing several broadcasts in lockstep
  (msf-01 §5.2.9: "Players with identical buffer lengths are likely to be synchronized") needs no
  more than a shared target latency on the viewer's own wall clock, and is left to the player.

What the relay does to timestamps is in
[Relay property handling](behavior.md#relay-property-handling). A track with a declared timescale
keeps the publisher's timestamps, rescaled to microseconds. A track without one, or an object
without a Timestamp, gets the relay's own clock instead. A received Timestamp cannot tell you which
of these happened, so a player must not use relay-clock stamps to sync, and a publisher must declare
the timescale on every media track. The format helpers do this by default.

### Broadcast clock (publisher)

`createBroadcastClock()` / `BroadcastClock()` pins one moment on a monotonic source to the system
clock, and maps the common timestamp sources onto the epoch timeline from there. It reads an
injected monotonic microsecond source. The default source is `performance.now()` in TypeScript and
`time.monotonic_ns()` in Python. The pinned moment is the origin: `originUs` on the source and
`originEpochUs` on the epoch (see [The origin's wall-clock time](#the-origins-wall-clock-time) for
how the system clock is read). A time on the source maps to `originEpochUs + (t − originUs)`, so
the timeline advances at the monotonic source's rate and an NTP step during the broadcast does not
move it.

Every other clock a publisher reads has its own origin, and often its own rate. A source timeline
maps one of them onto the broadcast timeline. The application calibrates it with measured pairs:
"source time `s` happened at `at` on the broadcast clock's source". A pair replaces the current
anchor when there is none, or when the anchor predicts it more than `toleranceUs` off (default
5 ms): the source drifted, or it jumped. Between anchors, timestamps keep the source's own spacing.
A timeline never calibrated anchors its first timestamp at the moment it arrives.

| Operation | TypeScript | Python | Result |
| --- | --- | --- | --- |
| Origin, on the source clock | `originUs` | `origin_us` | Integer µs; defaults to the source's time at creation, rounded |
| Origin, as wall-clock time | `originEpochUs` | `origin_epoch_us` | Integer µs since the Unix epoch; defaults to the system clock at creation less the time since the origin |
| Time on the timeline now | `now()` | `now_us()` | `originEpochUs + source − originUs`, integer µs |
| Map a time on the clock's own source | `fromSourceUs(t)` | `from_source_us(t)` | `originEpochUs + t − originUs`, rounded to integer µs, ties upward |
| Map a clock with another origin | `sourceTimeline({ toleranceUs, captureLatencyUs })` | `source_timeline(tolerance_us=, capture_latency_us=)` | A mapper; tolerance defaults to 5,000, capture latency to 0 |
| Measured pair | `timeline.calibrate(s, at?)` | `timeline.calibrate(s, at_us=)` | Re-anchors (returns true) when unanchored or off by more than the tolerance |
| Source time to timestamp | `timeline.timestampUs(s)` | `timeline.timestamp_us(s)` | `anchor + round(s − anchor s) − captureLatencyUs`; unanchored, the first call anchors at `now()` |
| Change the capture latency | `timeline.captureLatencyUs = n` | `timeline.capture_latency_us = n` | Integer µs, at least 0; applies to later timestamps, not to calibration |
| Anchor a sample counter | `sampleTimeline(rate, { firstSampleUs })` | `sample_timeline(rate, first_sample_us=)` | A mapper; the anchor defaults to `now()` |
| Sample index to timestamp | `timeline.timestampUs(n)` | `timeline.timestamp_us(n)` | `anchor + floor(n × 1,000,000 / rate)` |

#### Browser capture

The recipe has to work in Chromium, Firefox and Safari. `MediaStreamTrackProcessor` cannot be the
default, because Firefox has none and Safari's handles video only, in workers (MDN compat data,
2026-10). The portable path:

- **Video.** Draw the camera into a canvas, or wrap a `<video>` element in a `VideoFrame`, and stamp
  each frame with `clock.now()` when it is captured. That clock is `performance.now()`, which
  every browser has.
- **Audio.** Route the microphone through an `AudioContext` (`MediaStreamAudioSourceNode` into an
  `AudioWorkletNode`), and stamp each chunk with the context time of its first frame, in
  microseconds, through a source timeline calibrated from the context's render clock.
- **Calibrating for capture.** The worklet sees fresh microphone input when the context renders.
  So the calibration pair is a context time and the `performance.now()` time at which it was
  rendered. The worklet posts each block with the context frame rendered so far (`currentFrame`
  plus the quantum). The main thread times each block's arrival, and over a sliding one-second
  window the block that arrived soonest after its render gives the pair:
  `calibrate(renderedUs, arrivalUs)`. Messages only ever arrive late, so the earliest is the best
  estimate. Calibrating on every block costs nothing, because a pair re-anchors only when it
  disagrees by more than the tolerance. The example's `render-clock.ts` and `capture-microphone.ts`
  implement this.
- **Not `getOutputTimestamp()`.** The engines disagree on what it returns. Chromium (147) follows
  the Web Audio spec: `contextTime` is reaching the *speaker* at `performanceTime`, so render time
  is `contextTime + baseLatency + outputLatency`. Firefox (148) returns
  `currentTime − outputLatency` and `performance.now() − outputLatency`, which is the render pair
  shifted back by the output delay, and it steps with each audio callback. The formula
  `calibrate((contextTime + baseLatency + outputLatency) × 1e6, performanceTime × 1000)` agrees with
  the render clock to within about 15 ms in Chromium (median 5 ms). In Firefox it stamps audio early
  by the output delay: 40–50 ms, measured on a PulseAudio null sink.
- **Contiguous blocks.** Start a new block wherever `currentFrame` is not the frame expected next.
  On a new node's first `process()` call, Chromium (147) reports `currentFrame` 0 and only later the
  real frame. A block stamped from that 0 starts tens of milliseconds before its samples.
- **Why the AudioContext.** The context runs at one fixed sample rate whatever the microphone is,
  because the browser resamples. When the user switches devices, say from a laptop microphone at
  48 kHz to Bluetooth earbuds at 16 kHz, the count does not change rate. The context also keeps
  running through the gap while the device switches, emitting silence, so the count never stalls.
- **Drift.** The context clock follows the output device, so it drifts against
  `performance.now()`, and it changes when the output device does. Regular calibration absorbs
  both. A new `AudioContext` restarts its time at zero: give it its own render clock and source
  timeline.
- **Encoders.** Do not trust an audio encoder to carry input timestamps through. Chromium's Opus
  encoder counts samples from its first input and ignores later gaps. Firefox's stamps each chunk
  6.5 ms (its lookahead) before the input it came from. Feed the encoder one contiguous sample
  count, and map each chunk back to context time through the block its timestamp falls in. Encode
  every block, with or without demand, so the count never skips. A new context needs a new
  encoder.
- **The Chromium alternative.** Where `MediaStreamTrackProcessor` exists, its `VideoFrame` and
  `AudioData` timestamps count from the system's monotonic zero, not from `performance.now()`'s
  ([Chromium](https://chromium.googlesource.com/chromium/src/+/211cf780d86979929937ef73b6a27f13b9ea8e70%5E!/)).
  Map them through an uncalibrated source timeline. The first frame's capture-to-callback delay
  becomes a constant offset of a few milliseconds.

#### Capture latency

No browser API reports a microphone's own input latency. A timeline built from when samples reach
the page cannot see the time before that, so audio is stamped late by it, a player plays it late
against video, and on the shared epoch it is late against every other participant too. Chromium's
`MediaStreamTrack.getSettings().latency` does not help: it read 2.7 ms for the built-in microphone
and 5.3 ms for AirPods below, against real delays of about 30 ms and 190 ms.

A source timeline subtracts a capture latency the application supplies, `captureLatencyUs` /
`capture_latency_us`, from every timestamp it returns, so the Timestamp keeps LOC's meaning: the
presentation time of the first sample. Calibration pairs are unaffected. Set it when the source
timeline is created and again whenever the input device changes; a change applies from the next
timestamp. The helpers ship no device table. An application that knows its devices sets the
latency per device. The WebCodecs example estimates from the microphone's label: 190 ms for a
Bluetooth headset (a label containing "AirPods" or "Bluetooth"), 30 ms otherwise.

Measured with the [device latency lab](../tools/device-latency/README.md) on macOS 26.6 (MacBook
Air, Chrome and Firefox, 2026-10-13; raw results on
[issue #290](https://github.com/muxinc/relay-sdks/issues/290)). The figure is how late the capture
recipe stamps a click, net of what the browser reports for the output path:

| Input | Output | Chrome | Firefox |
| --- | --- | --- | --- |
| Built-in microphone | Built-in speakers | 30 ms | 35–38 ms |
| AirPods microphone | Built-in speakers | 162–192 ms | — (no clean run) |
| Built-in microphone, AirPods mic open (call mode) | AirPods | 470 ms | 117 ms, with the output misreported |
| AirPods microphone, call mode | AirPods | 600 ms | 511 ms |

The AirPods microphone delivers about 160 ms later than the built-in one; a reference-microphone
run (161.8 ms) and a clap run (157.8 ms) agree. Call mode, which macOS enters whenever an
application opens the AirPods microphone while playing to them, is 470–600 ms off in both browsers,
and its output delay moves too, so no constant corrects it well.

The same runs show the output side, which the playout clock reads from `outputLatency`. On AirPods
in music mode, Chrome reports 171 ms and misses about 30 ms more. Firefox keeps reporting 34 ms and
misses about 320 ms. After a switch back to the speakers, Firefox's `outputLatency` stays at the
AirPods value (165 ms), and Chrome keeps about 44 ms of extra error. Both are player-side errors:
the playout clock plays audio that much late, or early, and no publisher correction reaches them.

#### Stamping

Stamp the encoder's input (`new VideoFrame(frame, { timestamp })`, the `AudioData` timestamp) and
the encoded chunks carry the timeline, so `chunkToLocFrame` passes it through unchanged. WebCodecs
timestamps are 64-bit microseconds, so epoch values fit. If a chunk still carries the clock's own
source time, `timestampOffsetUs: clock.originEpochUs - clock.originUs` maps it; both are whole
microseconds, so that offset is valid. A camera has a capture latency too, typically a frame or
two; subtract it from the video timestamps if you know it.
A sample timeline suits sources with no clock of their own to calibrate against, such as a file
or a synthetic tone. A live device's counter drifts against the monotonic source by up to about
100 ppm, and it changes rate and stalls when the device changes. Use a calibrated source timeline
for live capture instead.

### Playout clock (player)

`createPlayoutClock()` / `PlayoutClock()` answers one question for the video renderer: when should
the frame with this timestamp be shown? It is DOM-free and transport-agnostic, with the same
injected time source as the broadcast clock, so a Python player or a test drives it with numbers.

Audio is the master. The audio path calls `reportAudio(timestampUs, atUs?)` /
`report_audio(timestamp_us, at_us=None)` with the timeline timestamp that is audible at local time
`at` (default: now). That is the timestamp leaving the speaker. In a browser, take the context
time audible now from the render clock: `currentTime` sampled against `performance.now()`,
min-filtered as for capture, minus `baseLatency + outputLatency`. The player maps that context
time to the media timestamp it scheduled there, then reports `reportAudio(mediaUs, nowUs)`.
`getOutputTimestamp()` gives the same answer in Chromium, but in Firefox (148) its pair is the
render pair, and reporting from it plays audio late by the output delay (33–36 ms measured). Read
the latencies fresh for every report: the output delay changes with the device. Switching to
Bluetooth earbuds can add 150 ms or more, and a value cached at startup leaves video that far
ahead. Where an application reports at handover time instead, it subtracts the jitter-buffer
depth, the current `outputLatency`, and any extra path, such as a WebRTC loopback for echo
cancellation. A
step in the output delay moves the position once. Video then holds frames, or drops them, by that
step and stays in sync after it. Between reports the clock extrapolates at real-time rate:
`position = reported + (now − at)`. Reports re-anchor it, so the audio device's drift never builds
up. Audio silence is measured from when the latest report arrived, not from its `at`, so a report
that describes a moment slightly in the past still counts as fresh.

The clock has three modes:

| Mode | When | Position |
| --- | --- | --- |
| `idle` | Created, or after `reset()`, with nothing reported or asked yet | None; the next `due()` anchors wall pacing |
| `audio` | A report arrived within `audioTimeoutUs` | Extrapolated from the latest report |
| `wall` | No audio has ever been reported, after `stopAudio()` / `stop_audio()`, or once reports have been silent for `audioTimeoutUs` | Wall clock from an anchor: the first frame asked about from `idle`, or the audio position when audio went quiet, so nothing jumps |

The next audio report returns the clock to `audio`.

`due(timestampUs)` compares the frame with the position: `offset = timestamp − position`, where
positive means the frame is early. The decision is one of three:

| Decision | When | The renderer |
| --- | --- | --- |
| `wait`, with `waitUs = ceil(offset)` | `0 < offset ≤ maxVideoDelayUs` | Holds the frame and asks again after the wait |
| `show` | `−lateDropUs ≤ offset ≤ 0`, or a re-sync (below) | Shows it now |
| `drop` | `offset < −lateDropUs` | Discards it; audio has already moved past it |

Every decision carries `offsetUs`, rounded to integer µs, so the application can log its sync error.
From `idle`, the first `due()` anchors wall pacing on that frame and returns `show` with offset 0.

Re-sync with `maxVideoDelayUs`. In `audio` mode, a frame more than `maxVideoDelayUs` early is
shown now and not held: video that far ahead of audio means the two tracks are not on one timeline,
and freezing the picture would not fix that. The offset reports the gap. In `wall` mode, a frame
more than `maxVideoDelayUs` early or late re-anchors the clock on itself and is shown. This covers
a timeline jump and recovery after a network stall, which would otherwise make every later frame
late. A late frame in `audio` mode is always judged against audio and dropped. If video is late
consistently, the audio path needs a deeper buffer, and the clock cannot add one.

| Option | TypeScript | Python | Default | Why |
| --- | --- | --- | --- | --- |
| Late-drop threshold | `lateDropUs` | `late_drop_us` | 40,000 | A late frame makes audio effectively early; EBU R37 puts the audio-early limit at 40 ms |
| Lead before re-sync | `maxVideoDelayUs` | `max_video_delay_us` | 1,000,000 | Above a deep audio jitter buffer plus output latency |
| Audio silence before wall pacing | `audioTimeoutUs` | `audio_timeout_us` | 500,000 | Reports come every 10–20 ms; half a second without one is a stall or a mute |
| Time source | `now` | `now_us` | `performance.now()` µs / `time.monotonic_ns() // 1000` | Injected so tests are exact |

The clock does not buffer, decode, or render frames, and it never changes how much audio is buffered.
The target it is built for is the broadcast guidance of ITU-R BT.1359 and EBU R37: audio within
about ±45 ms of video, and not audibly early.

A player's decoders do not carry timestamps through either. After a gap in its input, Chromium's
Opus decoder (147) keeps counting from before the gap, so reset the audio decoder wherever the next
timestamp is not the one expected. Chromium does not trim Opus's 6.5 ms pre-skip, so its decoded
audio plays that much late.

#### Measured in browsers

[Issue #288](https://github.com/muxinc/relay-sdks/issues/288). `mise run ts:test:av-sync` publishes
the WebCodecs example's canvas and microphone and plays them back in the same page, through the
production Relay in Chromium and through the in-memory relay hub in Firefox. Every 600 ms the canvas
flashes white and a synthetic microphone beeps. The A/V offset is how much later the beep reached
the player's output after it was emitted than the white frame was shown after it was captured.
Frame quantization does not count, because the white frame's own capture time is the video
reference. The figures are net of the beep's path to the capture worklet. That path stands in for
a real microphone's input latency, which no browser reports (#290), and it was 0–55 ms here.
"Settled" leaves out events emitted or played within 2 s of any switch. Results from 2026-10-07, in
ms, positive when audio is late:

| Phase | Chromium 147, Relay: settled mean [min, max] | Firefox 148, hub: settled mean [min, max] |
| --- | --- | --- |
| Baseline | 3.9 [−6.1, 22.3] | −2.4 [−13.6, 0.4] |
| Second microphone (16 kHz) | 2.0 [−13.9, 10.0] | −0.4 [−1.6, 0.8] |
| Replaced capture context | 2.5 [−6.7, 10.3] | −0.5 [−2.2, 2.4] |
| Output delay +200 ms | 8.4 [−4.1, 28.9] | −0.2 [−6.9, 4.1] |
| Audio resumed after a 3 s gap | 8.3 [−6.0, 27.1] | −4.3 [−6.2, −0.9] |

Across all phases, Chromium's mean was 4.7 ms (p95 17.5, worst 28.9) and Firefox's was −1.5 ms
(p95 5.6, worst −13.6). Transients stayed within the same bounds: the worst event in the 2 s after
any switch was −14.1 ms in Chromium and −8.2 ms in Firefox. The output-delay step held video once
for about 230 ms, as designed. During the audio gap the clock paced video by the wall clock, with no
dropped frame and no pause between frames longer than 44 ms. With the defaults (`lateDropUs` 40 ms,
`maxVideoDelayUs` 1 s, `audioTimeoutUs` 500 ms), no frame was dropped in any phase. The one
outlier came at startup: the first flash, emitted before any audio had reached the player, was off
by −246 ms while the clock was still pacing by the wall clock.

A re-run on 2026-10-14, after #290 moved every timestamp onto the Unix epoch, gave the same
picture with 8 s phases: settled means between 2.2 and 5.3 ms in Chromium through the Relay and
between −3.6 and 0.8 ms in Firefox through the hub, with no frame dropped in either.

The same runs show how far `getOutputTimestamp()`'s capture formula lands from the render clock.
In Chromium it was +5 ms (median; p5 −1, p95 16). In Firefox it was −48 ms (p5 −50, p95 −42),
which would stamp audio that much early. With players reporting from `getOutputTimestamp()`, Firefox
played audio 33–36 ms late; Chromium's settled offsets stayed within ±17 ms.

Firefox cannot run against the Relay: its WebTransport (148) offers no protocols and has no
`protocol` attribute, and the SDK refuses a session that negotiates none
([transport-adapter.md](transport-adapter.md), TA-7). Firefox also needs an audio server: without
one, its `AudioContext` stays suspended. Its H.264 `VideoEncoder` (148, Linux) holds about 41 frames,
1.4 s, even with `latencyMode: 'realtime'`, while VP8, VP9 and AV1 return each frame within
milliseconds. So its video arrives that much after its audio. The playout clock keeps audio the
master and would drop video that late, so the Firefox run gives the player a 1.6 s audio jitter
buffer. Playwright's WebKit (Safari 26.4's engine, on Linux) runs the hub scenario with short phases
(settled offsets 4–7 ms), but at full length it stops responding partway through. It cannot open a
WebTransport session to the Relay, and its `getOutputTimestamp()` is seconds off. It is opt-in
(`AV_SYNC_RUNS=webkit:hub`) and says nothing about Safari on macOS.

## Media Timeline track

[Issue #291](https://github.com/muxinc/relay-sdks/issues/291). Format helpers implemented in both
format packages; [behavior.md](behavior.md#media-helpers) holds the testable rules. Publishing the
track over a connection is not implemented yet.

msf-01's Media Timeline track (§7) is an optional track, announced in the catalog, whose records
map media time and an MOQT location to wall-clock time. It is MSF's counterpart of HLS
`PROGRAM-DATE-TIME`. Since #290 the broadcast timeline is itself wall-clock time, so a player that
knows this convention does not need the track to learn when a frame was captured; one that does
not can still read it here. What the track also gives is the map from time to location:
seeking behind the live edge, and generating HLS or DASH playlists, need to know which group to
fetch for a given time, and a LOC timestamp is only visible once the object has arrived.

### Entries

A timeline object's payload is a JSON array of records, each `[mediaTimeMs, [group, object],
wallclockMs]` (msf-01 §7.1.1):

- `mediaTimeMs` is the media time of the first sample in the referenced object, floored to whole
  milliseconds: `floor(timestampUs / 1000)` of its LOC timestamp on the broadcast timeline.
- `[group, object]` is that object's location in the track the timeline indexes.
- `wallclockMs` is milliseconds since the Unix epoch when that media was captured, floored. On the
  epoch-anchored broadcast timeline that is the timestamp itself, so the two numbers are equal. An
  entry helper caller with another anchor passes the wall-clock time explicitly, or 0 for unknown,
  as §7.1.1 allows.

The wall-clock time is the capture time of the indexed frame on the publisher's system clock. It
is not when the encoder produced the frame, which §7.1.1 names, and the difference is the encoder's
latency. Capture time is what lines broadcasts up with each other and what a packager writes as
`PROGRAM-DATE-TIME`. This departs from §7.1.1's wording; see [Spec gaps](#spec-gaps).

### Cadence

One entry per group of the indexed track, for the group's first object. Under keyframe grouping
that is one entry per video keyframe group, so there is an entry at every random-access point and
nowhere else. That is what a seek needs, at one record per GOP, typically every one or two seconds.

The entries' locations point into video, the first track in `depends`. Audio is one group per
frame, 50 or more a second, and an entry for each would multiply the document for no gain: every
track in the render group is stamped on the same broadcast timeline, so a player that has found the
video location for a media time finds the audio for it by timestamp. msf-01 §7.2 still requires
`depends` to list every track the timeline applies to, so audio is listed after video. An
audio-only broadcast indexes its audio track at most once a second.

The §7.4 template, which states the mapping as a start and a constant stride, is not used. A live
encoder's GOPs vary in length (scene cuts, keyframe requests), and §7.4 requires the explicit track
whenever durations vary.

### Catalog entry

`buildMsfCatalog` / `build_msf_catalog` takes an optional `mediaTimeline` / `media_timeline`
description and lists the track after video and audio, before data tracks:

```json
{"namespace":"room/live","packaging":"mediatimeline","isLive":true,"name":"timeline","role":"mediatimeline","mimetype":"application/json","depends":["video","audio"]}
```

- §7.2 requires a "type" of `mediatimeline`, but msf-01 has no field called `type`. Both the
  packaging table (Table 3) and the role table (Table 4) list `mediatimeline`, so the entry sets
  both. The MIME type is the `application/json` §7.2 requires.
- `depends` (§5.2.14, required by §7.2) lists every track the timeline applies to, as msf-01's
  example (§5.6.9) does. It defaults to the video and then the audio track, whichever the catalog
  has, and every name must be a media track in the same catalog. A location does not say which
  track it is in, so the entries' locations point into the first track listed.
- While every track it depends on is withheld (an `avc1` entry waiting for its avcC), the
  timeline entry is withheld too.
- The default track name is `timeline` (`MEDIA_TIMELINE_TRACK`).

The Video.js MoQ player (`mmcc/moq`, checked 2026-10-07) does not subscribe to a media timeline
track. Its catalog parser keeps `depends` and treats any track not packaged as `loc` as
non-renderable, so announcing the track changes nothing for it. Its `timeline.ts` has helpers for
the §7.4 template that nothing calls. Reading the explicit track for seeking is player work.

### The origin's wall-clock time

The broadcast clock records its origin as Unix-epoch microseconds, `originEpochUs` /
`origin_epoch_us`, and every timestamp it hands out is counted from there. A caller may set it, for
example from a capture device's own wall-clock stamps. Otherwise the clock reads the system clock
once, at creation, and subtracts the time between the origin and that moment on its own source:

- TypeScript reads `Date.now()`. `performance.timeOrigin + performance.now()` is finer, but it
  can fall behind the system clock, by however long the page has slept, where `performance.now()`
  does not advance while the machine sleeps. `Date.now()` has millisecond resolution, well inside
  the error of a system clock synced over the network.
- Python reads `time.time_ns()`.

Since it is read once, timestamps advance at the monotonic clock's rate from then on, not the
system clock's. The two can drift apart by tens of parts per million, a fraction of a second in an
hour, and an NTP step during the broadcast does not move the timeline. A step mid-broadcast would
otherwise move every track at once and break the playout clock's pacing. A long broadcast that has
to follow the system clock can be re-anchored at a new generation.

### Publishing the track

Format helpers only for now. The rules a publisher follows, and which a later publishing helper
will implement:

- msf-01 §7.3 requires the first object of every timeline group to be an independent document:
  every record accumulated and still accessible. Each new entry opens a new timeline group whose
  only object is the independent document of the retained entries. A viewer that joins at the
  relay's current group gets the whole history in one object. The incremental objects §7.3 allows
  after the first are not used.
- How many entries to retain is the publisher's choice: as far back as its viewers can still fetch,
  and well under the object size limit. One entry is about 35 bytes, so an hour of two-second
  GOPs is about 65 KB.
- A supervised publisher's next generation is a new broadcast with a new namespace, catalog,
  broadcast clock and origin. Its timeline starts empty. The old entries point at locations in a
  namespace the new catalog does not list, so they are not carried over. Wall-clock time is what
  joins two generations, and both stamp their media with it directly.

### Reading

`parseMediaTimeline` / `parse_media_timeline` reads one timeline object's payload into entries, in
document order. It rejects a payload that is not a UTF-8 JSON array. Within the array it is
lenient, as msf-01 is for catalogs: a record is read when its first three items are a number, an
array whose first two items are numbers, and a number, all non-negative safe integers. Extra items are ignored, and
anything else, a §7.4 template array among them, is skipped. Whether the payload is independent or
incremental is the caller's to know from the object's position in its group. MSF_COMPRESSION
payloads are not decompressed.

## Spec gaps

Questions the drafts leave open, found while settling #290. Each is worth raising on the MSF or LOC
draft; until then, this repository's answer is its convention.

- **No anchor for a render group.** msf-01 §5.2.11 says tracks in a render group "SHOULD be
  rendered simultaneously", and §4.2 aligns alternates, but nothing says the tracks of a group
  share a timestamp anchor, and nothing signals which anchor a track with a declared Timescale
  uses (LOC §2.3.1.2: "application-defined"). This repository anchors every track at the Unix
  epoch. draft-frindell-moq-timestamp-00 proposes a clock identifier and an origin, which would
  let a publisher say so.
- **`targetLatency` needs wall-clock time per frame.** msf-01 §5.2.8 defines it from the time a
  frame was encoded, in UTC. With an application-defined anchor, the only route to that time is
  the Media Timeline, at millisecond resolution and only at the indexed locations.
- **Encoded or captured.** msf-01 §5.2.8 and §7.1.1 both say "encoded". Lip sync, syncing across
  participants and `PROGRAM-DATE-TIME` all need capture time, which is what this repository
  stamps and indexes. draft-nandakumar-moq-tempo-00 carries an explicit capture timestamp next to a
  playout delay.
- **Two timescales.** msf-01 §5.2.21's catalog `timescale` and LOC's Timescale property (`0x08`)
  have no rule for how they relate or which wins. The readers here take the object's property
  first, then the track's, then the catalog's, as the Video.js MoQ player does.
- **Capture latency has no field.** No draft has a place to declare it, so it is folded into the
  Timestamp (see [Capture latency](#capture-latency)).

## Package placement

Recommended: two homes, matching the existing boundary rules.

- A format package per language, `@mux/moq-media` and `moq-media` (module `moq_media`), holding
  the format helpers and the readers. It depends on the core only (for `KeyValuePair` and
  `MoqtObject`), has no runtime dependencies, and knows nothing about Relay. The TypeScript package
  starts as an attributed snapshot of the Video.js `media/moq` modules with its own `UPSTREAM.json`,
  intended to migrate back upstream rather than fork, exactly as the core did; Python ports it.
  The boundary checks gain a rule: the format package imports nothing from the product package and
  no transport library, and the core imports nothing from it.
- The media publishing handle in the product package, as `@mux/relay/media` (a second package
  entry point) and `mux_relay.media`, because it is built from `RelayTrackWriter`, demand, and the
  write deadline, and because it is where Relay-specific observations such as the dropped
  Video Config property belong.

The alternative is one home, everything under `mux_relay.media` and `@mux/relay/media`. It is one
package fewer per language and the code in the samples above is identical apart from the import
lines. It gives up the snapshot-and-migrate story, and it puts a catalog builder that any MoQ
publisher could use behind a Relay dependency. If the package count is the concern, start there
and split later; the boundary between the two tiers is the same either way.

Encoders stay out of both. An `H264Config` record that turns Annex B into an avcC is a format
helper; a class that opens libx264 is not.

## Keeping the languages in lockstep

The same mechanisms the repository already uses:

- `protocol.json` gains pins for the LOC draft (`draft-ietf-moq-loc-04`), the MSF draft and
  catalog version (`draft-ietf-moq-msf-01`, `"draft-01"`), the microsecond default timescale, and
  the default track names (`catalog`, `video`, `audio`), each asserted by a test in every SDK.
- `behavior.md` gains a "Media helpers" section beside "Text and JSON helpers": what the catalog
  builder refuses, how the catalog service treats demand, what the frame writer does without
  demand and after a failure, and the statement that time-based drop policy is the application's.
- `vectors/` gains catalog vectors (track descriptions in, exact JSON bytes out), LOC property
  vectors (timestamp and config in, key-value pairs out), source URL vectors including the `.xx`
  escaping and a token, and avcC vectors (Annex B in, avcC and codec string out). The example's
  `test_videojs_example_helpers.py` already has most of these as unit tests; they move to the spec
  so the TypeScript side is held to the same bytes.
- The TypeScript snapshot brings `parse-catalog.ts` with it, so the acceptance test the Video.js
  builder names for itself, "every emitted document must round-trip through the parser", runs in
  this repository too.
- `vocabulary.md` gains rows before any of these names become public.

## Sequencing

Each step leaves the example runnable against the live relay with `--verify`, which is how the
change is checked without a browser, and each step deletes example code.

1. Format helpers in both languages, with the vectors, pins, and the `behavior.md` section. Move
   the example's `moq_media.py` onto them, which removes about 300 lines and fixes #29 for real
   because the README snippet can then import what it calls.
2. The media publishing handle. The example's `serve_catalog()` and `publish_media()` go; its
   `LiveQueue`, encoders, pacing, and `finished` accounting stay. The example's tests for catalog
   serving and track-end handling become the SDK's tests.
3. The readers, with `--verify` rewritten on them, and the TypeScript example that the browser
   sample above sketches.

## Open questions

- Names. `register_media` and `MediaTracks` are placeholders. The format tier could instead keep
  the Video.js names verbatim (`buildMsfCatalog`, `packageLocFrame`, `composeMoqSource`) so the
  snapshot stays diffable against upstream, at the cost of longer Python names.
- A subscription that arrives mid-GOP. The example waits for the next keyframe. Video.js retains
  the open group and replays it from object 0 so the viewer decodes at once. Replay is better for
  viewers and costs one GOP of memory per track; it can be a later option on the frame writer.
- Queue semantics on the frame writer: whether `write_frame` resolves when the transport accepted
  the frame, as `write()` does, or when the writer queued it, with backpressure read from the
  counters. The browser sample needs the second; the Python example is written for the first.
- Codec scope at the start. H.264 is what the example needs and what the relay has been tested
  with. HEVC (`hvcC`), AV1 (`av1C`), and AAC (AudioSpecificConfig) records are the same shape of
  helper and can follow demand.
