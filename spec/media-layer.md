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
packages ([issue #285](https://github.com/muxinc/relay-sdks/issues/285)). Public names for the
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

## Shared timeline and playout clock

[Issue #285](https://github.com/muxinc/relay-sdks/issues/285). Implemented in both format packages;
[behavior.md](behavior.md#media-helpers) holds the testable rules.

### One timeline per broadcast

A broadcast is the set of media tracks one MSF catalog lists. Its publisher stamps every one of
them from one broadcast clock:

- The LOC Timestamp of each object is the presentation time of its first sample, in microseconds
  since the broadcast's origin. The origin is a moment at or before the first capture on any track,
  so no track needs a negative timestamp. Each track declares Timescale 1,000,000
  (`locTrackProperties` / `loc_track_properties`).
- Audio and video share that origin. A track never starts its own count at zero: audio stamped from
  a sample counter is anchored at the broadcast time of its first sample, and video stamped from
  capture timestamps is shifted by the same origin as everything else.
- The catalog states nothing extra. Every LOC media track in one catalog is on that catalog's
  timeline. There is no `timeline` field, because msf-01 defines none and a player ignores fields
  it does not know. msf-01's `renderGroup` (§5.2.11) is the standard way to say tracks are meant to
  play together; the builder can emit it if a publisher ever lists tracks that are not.
- A timeline belongs to one catalog namespace. A supervised publisher's next generation is a new
  broadcast with a new origin. A player resets its playout clock wherever it resets its decoders:
  at a Restart, at a Move, and on a new catalog.
- Timelines of different broadcasts are unrelated. Syncing between broadcasts or participants is
  out of scope.

What the relay does to timestamps is in
[Relay property handling](behavior.md#relay-property-handling). A track with a declared timescale
keeps the publisher's timestamps, rescaled to microseconds. A track without one, or an object
without a Timestamp, gets the relay's own clock instead. A received Timestamp cannot tell you which
of these happened, so a player must not use relay-clock stamps to sync, and a publisher must declare
the timescale on every media track. The format helpers do this by default.

### Broadcast clock (publisher)

`createBroadcastClock()` / `BroadcastClock()` fixes the origin and maps the common timestamp
sources onto the timeline. It reads an injected monotonic microsecond source. The default source
is `performance.now()` in TypeScript and `time.monotonic_ns()` in Python.

Every other clock a publisher reads has its own origin, and often its own rate. A source timeline
maps one of them onto the broadcast timeline. The application calibrates it with measured pairs:
"source time `s` happened at `at` on the broadcast clock's source". A pair replaces the current
anchor when there is none, or when the anchor predicts it more than `toleranceUs` off (default
5 ms): the source drifted, or it jumped. Between anchors, timestamps keep the source's own spacing.
A timeline never calibrated anchors its first timestamp at the moment it arrives.

| Operation | TypeScript | Python | Result |
| --- | --- | --- | --- |
| Origin, on the source clock | `originUs` | `origin_us` | Integer µs; defaults to the source's time at creation, rounded |
| Time on the timeline now | `now()` | `now_us()` | `source − origin`, integer µs |
| Map a time on the clock's own source | `fromSourceUs(t)` | `from_source_us(t)` | `t − origin`, rounded to integer µs, ties upward |
| Map a clock with another origin | `sourceTimeline({ toleranceUs })` | `source_timeline(tolerance_us=)` | A mapper; tolerance defaults to 5,000 |
| Measured pair | `timeline.calibrate(s, at?)` | `timeline.calibrate(s, at_us=)` | Re-anchors (returns true) when unanchored or off by more than the tolerance |
| Source time to timestamp | `timeline.timestampUs(s)` | `timeline.timestamp_us(s)` | `anchor + round(s − anchor s)`; unanchored, the first call anchors at `now()` |
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
  microseconds, through a source timeline. About once a second, call
  `calibrate(contextTime × 1e6, performanceTime × 1000)` with the pair from
  `AudioContext.getOutputTimestamp()` (Chrome 57, Firefox 70, Safari 14.1).
- **Why the AudioContext.** The context runs at one fixed sample rate whatever the microphone is,
  because the browser resamples. When the user switches devices, say from a laptop microphone at
  48 kHz to Bluetooth earbuds at 16 kHz, the count does not change rate. The context also keeps
  running through the gap while the device switches, emitting silence, so the count never stalls.
- **Drift.** The context clock follows the output device, so it drifts against
  `performance.now()`, and it changes when the output device does. Regular calibration absorbs
  both. A new `AudioContext` restarts its time at zero, and the next calibration is far enough
  off to re-anchor.
- **The Chromium alternative.** Where `MediaStreamTrackProcessor` exists, its `VideoFrame` and
  `AudioData` timestamps count from the system's monotonic zero, not from `performance.now()`'s
  ([Chromium](https://chromium.googlesource.com/chromium/src/+/211cf780d86979929937ef73b6a27f13b9ea8e70%5E!/)).
  Map them through an uncalibrated source timeline. The first frame's capture-to-callback delay
  becomes a constant offset of a few milliseconds.

No browser API reports a microphone's own input latency. Bluetooth input can arrive 100–200 ms
later than a built-in microphone, and a timeline built from when samples reach the page cannot see
that. Audio from such a device is stamped late by that amount, so a player plays it late against
video. Subtract a known capture latency from the audio timestamps if you have one. Measuring it on
real devices is [issue #290](https://github.com/muxinc/relay-sdks/issues/290).

#### Stamping

A source time before the origin maps to a negative number, which the LOC helpers refuse. That frame
was captured before the broadcast started: create the clock before you start capturing, or pass the
earliest capture time as the origin. Stamp the encoder's input (`new VideoFrame(frame, { timestamp })`,
the `AudioData` timestamp) and the encoded chunks carry the timeline, so `chunkToLocFrame` passes it
through unchanged. If a chunk still carries the clock's own source time, `timestampOffsetUs:
-clock.originUs` maps it; the origin is a whole number of microseconds so that offset is valid.
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
`at` (default: now). That is the timestamp leaving the speaker. In a browser,
`AudioContext.getOutputTimestamp()` gives this in every engine: the context time reaching the
output (`contextTime`) and when (`performanceTime`). The player maps that context time to the
media timestamp it scheduled there, then reports `reportAudio(mediaUs, performanceTime × 1000)`.
Read it fresh for every report: the output delay changes with the device. Switching to Bluetooth
earbuds can add 150 ms or more, and a value cached at startup leaves video that far ahead. Where
an application reports at handover time instead, it subtracts the jitter-buffer depth, the
current `outputLatency`, and any extra path, such as a WebRTC loopback for echo cancellation. A
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
