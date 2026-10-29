# Media layer: LOC packaging and MSF catalogs

This document started on 2026-09-18 as a proposal for the media helpers both SDKs would take over
from the Video.js example. Most of it is now implemented:

- Stage 1, the format helpers, is implemented: the format packages (`@caprid/moq-media`,
  `moq-media`), the shared vectors, the protocol pins, and the Python example's migration.
  [behavior.md](behavior.md#media-helpers) is their contract.
- Stage 2, [media publishing](#media-publishing), is partly implemented. The catalog service, a
  document service for replaceable documents, and the frame writer exist in both product packages:
  `caprid_relay.media`, and `@caprid/relay/media` as `createCatalogService`,
  `createDocumentService` and `createFrameWriter`. [behavior.md](behavior.md#media-publishing) is
  their contract, and the Video.js and WebCodecs examples are their first callers. The
  `register_media` handle that would compose them with the catalog builder is still a proposal.
- Stage 3, the [readers](#reading-side), is implemented. The catalog reader ships in both format
  packages as `parseMsfCatalog` and `parse_msf_catalog`
  ([issue #164](https://github.com/muxinc/relay-sdks/issues/164)), and the Python example's
  `--verify` logs its track list. The object-to-frame reader ships as the frame sequencer in both
  format packages and as the frame reader in both product packages
  ([issue #303](https://github.com/muxinc/relay-sdks/issues/303), [Frame reader](#frame-reader)).

Both format packages also ship the [shared timeline and playout
clock](#shared-timeline-and-playout-clock) ([issue #285](https://github.com/muxinc/relay-sdks/issues/285)),
the [interrupt track](#interrupt-track)'s format helpers
([issue #320](https://github.com/muxinc/relay-sdks/issues/320)), and the
[Media Timeline track](#media-timeline-track)'s format helpers and timeline accumulator
([issues #291](https://github.com/muxinc/relay-sdks/issues/291) and
[#292](https://github.com/muxinc/relay-sdks/issues/292)), which both examples publish. The WebCodecs
example publishes audio on the shared timeline, measured live in browsers
([issue #288](https://github.com/muxinc/relay-sdks/issues/288)). [vocabulary.md](vocabulary.md)
records the public names of the implemented stages.

The before and after sketches below describe the eventual API, `register_media` included. They are
not runnable. See the format packages' READMEs for current usage. Stage 1 keeps `VideoTrackInfo`
and `AudioTrackInfo`, adds `DataTrackInfo`, and uses `build_msf_catalog` and `buildMsfCatalog` for
UTF-8 bytes. The TypeScript catalog reader is the Video.js parser's independent-catalog path,
promoted to `parseMsfCatalog`. Player presentation projection and delta updates are not part of
this extraction.

## TypeScript input integration

Stage 1 accepts native WebCodecs chunks and decoder configs through `@caprid/moq-media`:
`chunkToLocFrame`, `videoTrackFromDecoderConfig`, and `audioTrackFromDecoderConfig`. The
[direct browser example](../ts/examples/webcodecs/README.md) connects `VideoEncoder` output to
LOC/MSF and Relay without a media engine. The pure helpers copy chunk bytes without converting
the bitstream. Encoding, callback buffering, and capture remain application concerns.

The optional `@caprid/moq-mediabunny` adapter handles file-input packets, demonstrated by the
[MP4 example](../ts/examples/mediabunny/README.md). Mediabunny owns demuxing. Standard decoder
config mapping, LOC properties, MSF catalogs, and source URLs belong to `@caprid/moq-media`, with
no Mediabunny dependency. The adapter retains compatibility re-exports of the config mappers.

TypeScript consumes supplied AVC configuration and four-byte-length packets. Annex B splitting,
SPS/PPS reconstruction, and conversion are Python-only, for the PyAV example. The config mappers
support AVC and Opus. Grouping, demand, backlog bounding and catalog serving are SDK
code in `@caprid/relay/media` (`createFrameWriter`, `createCatalogService`, `createDocumentService`),
which both TypeScript examples call. Pacing remains example code: the WebCodecs example skips
capture while the encoder is busy, the frame writer has a backlog, or video has no demand, and the
Mediabunny example paces file packets in real time. The stage 2 `register_media` handle that would
compose these is not implemented.

## What a media publisher has to know today

This section records the Python Video.js example as it stood on 2026-09-18, before stage 1 moved
its helpers into the format packages. The product SDKs stop at bytes, groups, and demand. `behavior.md` says so on purpose: "The SDK adds
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
| Media packaging | SDK, exists except the `register_media` handle | LOC object properties, the MSF catalog and its serving loop, the source URL, decoder configuration records, keyframe-to-group mapping |
| Transport | SDK, exists | Connections, tracks, groups, demand, deadlines, cancellation |

The boundary is the encoded frame: bytes, a timestamp, and a keyframe flag. That is what a WebCodecs
`EncodedVideoChunk` carries and what Video.js's `loc-packaging.ts` consumes. The SDK never configures
an encoder and never decides that a frame is too old to send. The example keeps its `LiveQueue`.
The SDK does turn an encoder's output into something a LOC/MSF player can play, and the one place
the tiers touch, decoder configuration, gets helpers in the packaging tier because an `avc1` catalog
entry is meaningless without an avcC and PyAV hands back Annex B.

## What the example would look like

The Python example, with the encoder, test pattern, pacing, and `LiveQueue` unchanged:

```python
from caprid_relay import connect_relay_publisher
from caprid_relay.media import AudioTrack, VideoTrack, register_media  # provisional
from moq_media.h264 import H264Config

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

Behind those calls:

- three tracks registered with a microsecond timescale
- a catalog built with `packaging: "loc"`, the `initDataList` entry and `initRef`, and the `avc1`
  entry withheld until an avcC exists
- a catalog service that re-sends the document on every relay subscription
- frame writers that open a group on each keyframe, append deltas, drop frames while the relay has
  no demand, and resume at the next keyframe after any failure
- a source URL with the namespace escaped the way the player decodes it

The same in a browser, where WebCodecs reports the avcC with the first encoded chunk rather than up
front, which is why the catalog has to be updatable:

```ts
import { connectRelayPublisher } from '@caprid/relay';
import { registerMedia } from '@caprid/relay/media'; // provisional

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
mirrors, so the TypeScript side could start as an attributed snapshot, as `@caprid/moq-core` did.

| Helper | Replaces in the example | Video.js module |
| --- | --- | --- |
| LOC property registry (`0x10` Timestamp, `0x08` Timescale, `0x0D` Video Config, `0x0F` Audio Config) and the microsecond default | The constants block in `moq_media.py` | `loc.ts` |
| Track properties for a LOC track (declared timescale) and object properties for one frame (timestamp, timescale, config on keyframes) | `TRACK_TIMESCALE_PROPERTIES`, `loc_properties()` | `loc-packaging.ts` |
| Catalog builder taking video, audio, and data track descriptions and a namespace, returning the MSF `draft-01` JSON bytes; any number of video and audio tracks, in render and alternate groups ([several media tracks](#several-media-tracks)) | `VideoTrackInfo`, `AudioTrackInfo`, `build_msf_catalog()` | `build-catalog.ts` |
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
- Members of one alternate group share a role and a render group (see
  [several media tracks](#several-media-tracks)).
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

Each media track is registered with its default delivery timeout and publisher priority from
`mediaTrackProperties` / `media_track_properties` ([issue #302](https://github.com/muxinc/relay-sdks/issues/302),
[issue #316](https://github.com/muxinc/relay-sdks/issues/316)), which the handle would compute from
the track's kind and keyframe interval. A viewer subscribes to each media track with
`mediaSubscribeParameters` / `media_subscribe_parameters` for the matching subscriber priority
([issue #337](https://github.com/muxinc/relay-sdks/issues/337)), the only one moq-relay 0.17 orders delivery by.

The catalog service is the loop the example's `serve_catalog()` ran on 2026-09-18, generalized:

- On every `True` from the catalog track's demand iterator, write the current document as one
  independent group. The repeated `True` for a replacement relay subscription matters most,
  because a viewer joining after a relay reconnect needs it.
- `update()` rebuilds the document from new track descriptions. Identical bytes send nothing.
  Changed bytes go out as a new group at once if demand is present, and on the next subscription
  otherwise. Every catalog object is a random-access point, which is what the MSF independent
  catalog form requires.
- A transient write failure is retried after a pause while demand remains, keeping only the latest
  document. A closed track or connection ends the service and is reported once.
- The catalog advertises only tracks the handle has registered. Adding a track through `update()`
  registers it first. Removing one from the catalog does not free its name on this connection,
  which `behavior.md` already forbids, so the handle says so rather than letting a later `update()`
  fail on the relay.

The frame writer is the mechanism the example's `publish_media()` had on 2026-09-18, without the
example's time-based staleness policy:

- Video: a keyframe closes the open group and opens a new one. A delta appends to the open group.
  While the relay has no demand, frames are counted and not sent, without waiting on the write
  deadline, because an encoder loop cannot block for ten seconds. When demand returns, and after any
  group failure, deltas are discarded until the next keyframe.
- Audio, and any track marked as one group per frame: every frame is its own single-object group,
  the LOC mapping for independently decodable frames.
- LOC object properties are attached from the frame's timestamp and the track's declared timescale.
  The track's current init data rides keyframes as Video Config or Audio Config, for consumers
  that receive odd-typed properties, and the catalog stays the copy players rely on.
- Calls on one writer are serialized in arrival order, so a callback-driven encoder can call it
  without tracking a pending write. Backpressure is bounded in frames, not groups: Video.js's three
  queued groups are seconds of video, too much for live. When the backlog is full, keyframe grouping
  drops it with the open group and resumes at the next keyframe, and frame grouping drops the oldest
  frame. The writer says when it needs a keyframe so the encoder can force one
  ([issue #263](https://github.com/muxinc/relay-sdks/issues/263)). Whether to discard a frame
  because it is late against a clock stays with the application.
- Each `write` reports what became of its frame: `sent`, `skipped`, `stale`, `dropped`,
  `interrupted`, or `ended` (`FrameOutcome.kind` in both SDKs). The writer keeps only
  `groups`, `objects`, `backlog`, and `keyframeNeeded` / `keyframe_needed`. Video.js's track
  publisher keeps a fixed set of counters, but the writer does not. An application counts the
  outcomes it cares about, grouped the way it wants, and adds `frame.payload`'s length if it wants
  bytes. The Python example's `finished` line does this
  ([issue #263](https://github.com/muxinc/relay-sdks/issues/263)).
- An interruption resets the group and leaves the track open. A track that ends on its own is an
  `ended` outcome, for that frame and every later one, and the writer's `ended` holds the error.
  It is not an exception, because the encoder keeps producing frames. Only a `write` after the
  application's own `close()` raises `CLOSED`. An application that wants the example's
  stop-the-run policy stops on the first `ended` outcome.

### Reading side

Before stage 3, the example's `--verify` mode and any non-browser consumer did the inverse by
hand. Three readers, symmetric with `read_text` and `read_json`:

- Parse a catalog object into track descriptions, in the independent catalog form. Delta updates
  can wait until a publisher in this repository emits them. Shipped as `parseMsfCatalog` /
  `parse_msf_catalog` (see [behavior.md](behavior.md#media-helpers)).
- Turn a track's objects into frames in decode order within a latency budget: the
  [frame reader](#frame-reader) below.
- Turn an object plus the track's received timescale into a frame: payload, timestamp in
  microseconds, keyframe flag from object ID 0, and any config property that survived the relay.
  Video.js's `toLocFrame` is the reference, including its acceptance of the draft-03 timestamp ID.
  Its timestamp part ships as `locTimestampUs` / `loc_timestamp_us`
  ([issue #290](https://github.com/muxinc/relay-sdks/issues/290)): an object's Timestamp in
  microseconds, scaled by the first Timescale it finds on the object, then in the track properties
  (`SUBSCRIBE_OK`'s, as the relay sends them). It does not read the catalog's `timescale`: msf-01
  §5.2.21 never ties that field to the LOC Timestamp, and a catalog that set it for another purpose
  would silently rescale a Timestamp LOC defines as microseconds. With no Timescale, the Timestamp
  already is microseconds since the epoch (LOC §2.3.1.1). Reading `0x10` as raw microseconds works against
  moq-relay only because it rescales every timestamp to microseconds and declares Timescale
  1,000,000. A relay that passes the publisher's Timescale through needs the scaling.

## Several media tracks

[Issue #351](https://github.com/muxinc/relay-sdks/issues/351). A broadcast can carry simulcast
layers and more than one source, such as a camera and a screen share. msf-01 describes both with
two integers on a track entry:

- `renderGroup` (§5.2.11): tracks "designed to be rendered together", such as a camera and its
  microphone.
- `altGroup` (§5.2.12): "alternate versions of one-another", such as the same camera at three
  bitrates. "A subscriber typically subscribes to one track" of the group, and alternates "MUST
  have matching media time sequences".

### Building

`buildMsfCatalog` / `build_msf_catalog` takes a `media` list of video and audio track
descriptions, each with an optional `renderGroup` / `render_group` (default 1) and `altGroup` /
`alt_group`. In TypeScript each entry names its role (`{ role: 'video', ... }`). In Python it is a
`VideoTrackInfo` or an `AudioTrackInfo`. The single `video` and `audio` inputs remain, as shorthand
for one-entry lists listed before `media`, so existing callers build the same bytes. A group
participant publishing a camera in three layers, a microphone, and a screen share with its audio
lists:

| Track | Role | `renderGroup` | `altGroup` |
| --- | --- | --- | --- |
| `video-hi`, `video-mid`, `video-lo` | video | 1 | 1 |
| `audio` | audio | 1 | — |
| `screen` | video | 2 | — |
| `screen-audio` | audio | 2 | — |

The builder refuses:

- a duplicate track name, across media, data and the media timeline, as before
- a render or alternate group that is not a non-negative safe integer
- an alternate group whose members differ in role or render group. Alternates are one content, so
  they play with the same companions. A viewer picks one track per alternate group within a
  render group, which is only well defined when the alternate group sits inside one render group.

Each H.264 entry is still withheld until it has init data, so an alternate group can list fewer
members than the caller passed. The media timeline's default `depends` is the first video track
then the first audio track. Name the tracks to index others. Alternates are time-aligned but their
group IDs are not (see [switching tracks](behavior.md#switching-tracks)), so a timeline's
locations point into its first dependency only.

### Reading and choosing

The catalog reader returns `altGroup` / `alt_group` beside `renderGroup` / `render_group`, as
declared. `groupMsfTracks` / `group_msf_tracks` turns a parsed track list into the sets a viewer
chooses from, in order of each set's first track: tracks sharing a render group and an alternate
group form one set, and every other track is a set of its own, alternate group or not. A viewer
plays one track from each set of the render group it wants, and moves between alternates with
`want(..., { until })` (`want(..., until=...)`), which already holds the old track until the new
one has started.

The helper picks nothing: bandwidth estimation, layout and which render group is on screen belong
to the player. It skips no track either: a track with no alternate group, a data track among them, comes back
as a set of one. Tracks sharing an alternate group but declaring no render group form one set.

### Keeping the catalog current

A publisher's encoder configuration can change mid-broadcast: a camera switch, a resized shared
window, a codec fallback from H.264 to VP8. Each change is a new catalog. Serve the catalog with
`createDocumentService` / `DocumentService` instead of the fixed catalog service, and `update()` it
with the rebuilt bytes when the configuration changes. The new document goes out as a new group at
once while the relay is subscribed, and identical bytes send nothing. Adding a track this way
registers it first. A track removed from the catalog keeps its name on the connection.

msf-01 §5.3 fixes a track's attributes to its namespace and name: "To modify any attribute, a new
track with a different Namespace|Name tuple is created". The rule sits among the delta-update
rules, but its wording is general, so the conforming way to change a codec, its init data or its
dimensions is a new track: register `video-2`, list it in place of `video` in the next catalog,
and let viewers move to it with `want(..., { until })`. Rewriting a listed track's entry in place
works only with viewers that reconfigure their decoder on every catalog, and is outside msf-01.

Either way, a viewer can receive a key frame before the catalog that describes it, because the catalog and the
media are separate tracks. Until relays forward object properties
([issue #262](https://github.com/muxinc/relay-sdks/issues/262)), the catalog is the only copy of
the decoder configuration a viewer can rely on, so a viewer that sees a key frame it cannot decode
with its current configuration waits for the next catalog and drops frames until then. Once relays
forward them, the LOC Video Config property on each key frame (`0x0D`) is the earlier copy, and a
viewer can prefer it. To shorten the wait, a publisher updates the catalog before it sends the
first key frame of the new configuration.

Not covered: `temporalId` / `spatialId` (§5.2.16, §5.2.17, the temporal and spatial layer a
scalable-coded track carries) and delta catalog updates.

## Shared timeline and playout clock

[Issue #285](https://github.com/muxinc/relay-sdks/issues/285). Implemented in both format packages.
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
  that nothing on the wire signals which anchor a broadcast uses. The epoch anchor is a convention
  between the SDKs and players that follow this document. A receiver that does not know it sees
  only an application-defined anchor (LOC §2.3.1.2).
- The catalog puts the video and audio entries in one render group unless the caller names
  another: `renderGroup: 1` (msf-01 §5.2.11, "tracks with the same group number SHOULD be rendered
  simultaneously"). That is msf-01's way to say tracks play together, and moq-rs catalogs already
  send it. A second source, such as a screen share, takes its own render group (see
  [several media tracks](#several-media-tracks)). There is no
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
the timescale on every media track. The format helpers do this by default. None of these rewrites
is permitted by MOQT-22 §8.4, which says a relay must forward properties unmodified (see
[Spec gaps](#spec-gaps)).

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
timeline is created and again whenever the input device changes. A change applies from the next
timestamp. The helpers ship no device table. An application that knows its devices sets the
latency per device. The WebCodecs example estimates from the microphone's label: 190 ms for a
Bluetooth headset (a label containing "AirPods" or "Bluetooth"), 30 ms otherwise.

Folding the latency into the Timestamp is allowed (LOC draft-04 defines it only as the "Timestamp of
the encoded media frame") and no draft has a separate field for it, but it has costs:

- Audio is stamped at its corrected capture time, video at the time the frame reached the page,
  unless the application also subtracts a camera latency. The two are equally "capture time" only
  as far as both corrections are right.
- A relay or player that judges an object's age against its own wall clock sees corrected audio as
  that much older: 30 ms for a laptop microphone, 190 ms for AirPods. draft-lcurley-moq-timestamp-01
  §4.1 measures age against the newest object on the same track instead, where a constant
  correction cancels out. Only a change in the correction, at a device switch, moves it.
- The label heuristic under-corrects AirPods in call mode (470–600 ms measured, below) by about
  280–410 ms. Only an application that can tell call mode apart can do better.

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

The AirPods microphone delivers about 160 ms later than the built-in one. A reference-microphone
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
source time, `timestampOffsetUs: clock.originEpochUs - clock.originUs` maps it. Both are whole
microseconds, so that offset is valid. A camera has a capture latency too, typically a frame or
two. Subtract it from the video timestamps if you know it.
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
more than `maxVideoDelayUs` early, or more than the target latency late, re-anchors the clock on
itself and is shown. This covers a timeline jump and recovery after a network stall, which would
otherwise make every later frame late. A frame reader never passes a frame that late, so behind one
the late re-anchor happens only at its resync. A late frame in `audio` mode is always judged against audio and dropped. If video is late
consistently, the audio path needs a deeper buffer, and the clock cannot add one.

| Option | TypeScript | Python | Default | Why |
| --- | --- | --- | --- | --- |
| Late-drop threshold | `lateDropUs` | `late_drop_us` | 40,000 | A late frame makes audio effectively early; EBU R37 puts the audio-early limit at 40 ms |
| Lead before re-sync | `maxVideoDelayUs` | `max_video_delay_us` | 1,000,000 | Above a deep audio jitter buffer plus output latency |
| Audio silence before wall pacing | `audioTimeoutUs` | `audio_timeout_us` | 500,000 | Reports come every 10–20 ms; half a second without one is a stall or a mute |
| Target latency | `targetLatencyUs` | `target_latency_us` | 2 × the longest of the last four reported keyframe intervals; 2,000,000 before any | Two GOPs: room to wait for a late group and still decode up to the next keyframe; see [Frame reader](#frame-reader). For a broadcast with no video, pass `AUDIO_ONLY_TARGET_LATENCY_US` (500,000); see [Audio player](#audio-player) |
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
production Relay in Chromium and through the in-memory relay hub in Firefox (and, since Firefox 155,
through the Relay too, as described below). Every 600 ms the canvas
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
played audio 33–36 ms late, and Chromium's settled offsets stayed within ±17 ms.

Firefox reaches the Relay over WebTransport from 155 (stable 2026-09-01), which added the
`protocols` option and the `protocol` attribute. Firefox 148 has neither, and the SDK refuses a
session that negotiates no protocol ([transport-adapter.md](transport-adapter.md), TA-7). A run on
2026-10-19 ([issue #322](https://github.com/muxinc/relay-sdks/issues/322)), with Playwright's
Chromium 153 and Firefox 155 both through the production Relay (sjc), passed every phase, in ms:

| Phase | Chromium 153, Relay: settled mean [min, max] | Firefox 155, Relay: settled mean [min, max] |
| --- | --- | --- |
| Baseline | 6.1 [1.1, 15.6] | −1.8 [−3.1, 0.5] |
| Second microphone (16 kHz) | 4.2 [−2.3, 11.2] | −2.5 [−4.8, 0.5] |
| Replaced capture context | 3.2 [−4.9, 13.3] | −4.3 [−6.1, 1.2] |
| Output delay +200 ms | 5.0 [−12.3, 20.8] | −3.5 [−5.5, −0.5] |
| Audio resumed after a 3 s gap | 4.8 [−19.3, 29.4] | −2.0 [−5.1, 1.2] |

Each dropped at most one frame in the whole run. The same day's Firefox run through the hub failed,
in 148 and in 155 alike: its player has judged nearly every video frame late since the frame reader
landed ([issue #339](https://github.com/muxinc/relay-sdks/issues/339)).

Over QMux over WebSocket (`@caprid/relay-websocket`), Firefox 148 and Playwright's WebKit both passed
the live data scenario against a local moq-relay 0.17.5 on 2026-10-17
([QMux over WebSocket](relay-interop.md#qmux-over-websocket)). The A/V sync run has not been
repeated over WebSocket. Firefox also needs an audio server: without one, its `AudioContext` stays
suspended. Its H.264 `VideoEncoder` (148, Linux) holds about 41 frames, 1.4 s, even with `latencyMode: 'realtime'`, while VP8, VP9 and AV1 return each frame within
milliseconds. So its video arrives that much after its audio. The playout clock keeps audio the
master and would drop video that late, so the Firefox run gives the player a 1.6 s audio jitter
buffer. Firefox 155 still needs it: with a 150 ms buffer it showed no video through the Relay.
Playwright's WebKit (Safari 26.4's engine, on Linux) runs the hub scenario with short phases
(settled offsets 4–7 ms), but at full length it stops responding partway through. It cannot open a
WebTransport session to the Relay (it can reach it over WebSocket, above), and its
`getOutputTimestamp()` is seconds off. It is opt-in
(`AV_SYNC_RUNS=webkit:hub`) and says nothing about Safari on macOS.

### Frame reader

[Issue #303](https://github.com/muxinc/relay-sdks/issues/303). Implemented in both SDKs.
[behavior.md](behavior.md#media-subscribing) holds the testable rules.

Each group travels on its own stream, and streams are not ordered relative to each other. After a
2 s stall through the impairment proxy, the live A/V test's player, which fed every video object to
one decoder in arrival order, crashed its decoder on `… g10/o0, g6/o1 … g6/o4, g9/o0, g9/o1,
g10/o1`: the tail of group 6's stalled stream arrived after group 10's keyframe. Nothing was lost.
The same stall left about 2.3 s of audio scheduled ahead, against about 0.14 s normally, because the
player re-anchored its jitter buffer on the first late frame.

The frame sequencer is the decision engine, a pure state machine in the format package like the
playout clock: events in, frames out, and how long until a wait can end. The frame reader in the
product package is the glue. It is a subscription's or a want's handlers, feeds every event to the
sequencer, and polls again when the wait ends. The product package cannot import the format
package, so the reader takes the sequencer as an argument.

- **One budget.** Lateness is measured against the playout clock's position, the media time that
  should be playing now. It advances in real time through a stall, so a burst that arrives after one
  is already behind it. Nothing depends on the publisher's and viewer's system clocks agreeing. The
  budget is the clock's `targetLatencyUs`, so all tracks of a broadcast and the clock's own late
  re-anchor share one number.
- **The default follows the stream.** The video sequencer reports the interval between the
  keyframes of consecutive groups. The clock keeps the last four and takes twice the longest, so a
  forced keyframe (`onKeyframeNeeded`) does not shrink the budget. Before two consecutive keyframes
  have arrived it is 2 s, twice the examples' one-second keyframe interval. msf-01 has no catalog
  field for keyframe spacing, and its `targetLatency` (§5.2.8) means end-to-end wall-clock latency,
  so neither is read.
- **Video waits only while it is worth it.** In-order frames go out at once. A gap, or a still-open
  older group, holds newer frames until the last delivered frame's timestamp plus the target
  latency, because the missing frame is later than that one. A group that has not been seen may
  never come, since a failed group's ID is not reused, so the next keyframe waits for it only while
  it is not yet due. Past the deadline the sequencer skips to the next keyframe within the budget and
  marks a `gap`, and the player resets its decoder there.
- **Audio is not held.** No audio frame depends on another, so a frame more than the clock's
  `lateDropUs` behind the position is dropped at once, and nothing waits for a missing one. A budget
  of two GOPs for audio would keep almost all of a 2 s blackout's burst, and the player would
  re-anchor on it and stay that far behind.
- **Resync.** If frames keep arriving but none within the budget for one whole target latency,
  and their lateness holds steady, the path delay has grown for good. Lateness that keeps growing
  is a stall, and lateness that shrinks is its backlog draining. Neither resyncs, because the frames
  the clock would anchor on are about to be caught up with. The sequencer delivers the newest late keyframe group (or audio
  frame), marks it `resync`, and resets the clock so it re-anchors there, rather than leave the
  player black or silent.
- **Resync forward.** A clock anchored on old frames, by a resync onto a delay that later
  recovered, sees fresher frames arrive more than the target latency ahead of its position. That is
  a resync too, forward, so the old delay does not become lasting latency. The player drops the
  audio and video it had scheduled on the old anchor.
- **Restart and Move.** A want's Restart or Move restarts the sequencer, since group IDs start over,
  and marks the next frame `restart`.

Measured live on 2026-10-15: the A/V test (`mise run ts:test:av-sync`) in Chromium 147 against
`sjc.relay.mux.global`, through a UDP impairment proxy that, 4 s into playback, either blacked out
both directions for 2 s or capped the uplink at 80 kbit/s (1 s queue) for 4 s. The publisher uses
[#302](https://github.com/muxinc/relay-sdks/issues/302)'s default delivery timeouts. The player reads
both tracks through frame readers, keeps a 150 ms audio jitter buffer, drops audio whose slot has
passed instead of re-anchoring on it, and re-anchors at a resync, a restart, or a schedule more than
100 ms beyond its depth. "Ahead" is the audio scheduled ahead of the context's time at each later
phase boundary, the first about 6 s after the impairment:

| Impairment | Before (the player decoding in arrival order) | With the frame reader, final round |
| --- | --- | --- |
| None | Passes; 130–185 ms ahead | 3 of 3 passed; 130–158 ms ahead |
| 2 s blackout | 0 of 2: `EncodingError` from an old group's tail after a newer keyframe, or about 2.6 s ahead for the rest of the run | 3 of 3 passed; 120–170 ms ahead |
| Uplink capped for 4 s | 0 of 2: `EncodingError`, or about 3.5 s ahead for the rest of the run | 3 of 3 passed; 126–197 ms ahead |

Over the rounds before the final one, after the rebase onto #302: 14 of 15 unimpaired runs, 11 of 12
blackouts and 11 of 11 capped runs passed. Unimpaired, the readers' counters stayed at zero skips and
resyncs, with at most two audio frames dropped as late. The two failures were an unimpaired video
stall of 3.2 s, before the player's last fixes and not seen again, and the WebCodecs example's own
encoder-queue guard ("The audio encoder cannot keep up") tripping after one blackout.

Getting here took three rules found in these runs: a resync only on a steady delay, a resync
forward when the clock is behind what arrives, and the player's bounded jitter buffer.

The publisher side of the same stall is [issue #302](https://github.com/muxinc/relay-sdks/issues/302)'s
delivery timeouts. They derive from the same keyframe interval, about one interval for video, under
the reader's two, so a publisher stops spending bandwidth on frames the reader would drop. Nothing
links the two on the wire.

#### Timestamp playout

TypeScript only (the frame sequencer's `timestampPlayout`), off by default. A text-to-speech agent
generates its reply faster than real time, and MoQ lets it send each frame as it is generated,
stamped with its slot on the timeline, rather than pace it as WebRTC must. A player then has to hold
what arrives early and play each frame at its timestamp. The sequencer without it does not: a frame
more than the clock's target latency ahead of the position (`AUDIO_ONLY_TARGET_LATENCY_US`, 500 ms,
for audio) resyncs it forward, as after a stall, which drops what the player holds and re-anchors on
the newest frame, and the jitter buffer trims what it holds beyond its target. relay-dev's offline
replay of bursty TTS traces through it discarded about 90% of the clip (moq-netbench
`plans/README.md`, "Stage 4 wave 2's agent cells").

```ts
const input = player.addInput({ clock, decoderConfig });
const sequencer = createFrameSequencer({
  clock,
  grouping: 'frame',
  timestampPlayout: { leadUs: () => input.playoutLeadUs },
});
```

- A frame, and every frame after it, waits until its timestamp is within `leadUs` of the clock's
  position. The poll's `waitUs` says when, so a frame reader releases each frame on time.
- The lead is what the player holds beyond what is audible: `input.playoutLeadUs`, the output delay
  (`baseLatency + outputLatency + player.outputDelayUs`) plus the jitter target now, keeps the
  jitter buffer at its target. A longer constant lead leaves more held than the target, and the
  buffer's trim drops the excess every second; a shorter one leaves the buffer short of its target.
  Default 100 ms, the player's default target and a 40 ms output delay.
- While the clock has no position (nothing has played yet, or after a reset), the first frame
  anchors a position of the sequencer's own on the clock's local time. The clock's takes over as
  soon as the player reports.
- Only a frame more than `horizonUs` (default 30 s) ahead resyncs forward, as a timeline jump. The
  sequencer holds up to a horizon of frames (at 2.5 ms each), beyond the 1,024 it holds otherwise.
- A live talker's frames are never early, so they play as without it: late drop, resync and
  catch-up are unchanged. `stats.early` counts the frames held. A restart (a want's Restart or Move)
  drops every frame held.
- **Delay added by underruns is given back.** Each underrun leaves the clock further behind local
  time, by the silence it played. Without timestamp playout, frames that then arrive early fill the
  jitter buffer past its target and its trim gives that delay back. With it, frames reach the buffer
  only a lead ahead, so the buffer never holds more than its target and never trims: a queue that
  built and drained would leave its delay for good. So the sequencer keeps the least delay seen from
  the clock's position to its local time, less the lead, since the clock was last reset, and releases
  held frames early by however much the delay now exceeds it beyond 40 ms (a frame or two of report
  noise). The buffer then holds that excess and trims it, as it does without timestamp playout. A TTS
  talker whose frames came early all along has added nothing, and plays at its timestamps as before.

**Measured on relay-dev's voice benchmark.** Stage 4ai-tts (relay-sdks build tag
`netbench/stage4-ai`, 5a4bd33, the same code without the give-back above; moq-relay 0.17.9; runs
run-20261029-054413-95db41, run-20261029-102627-5276c7, run-20261029-105904-7de8ee and
run-20261029-112630-e14a13; finding C-3 in relay-dev `moq-netbench/results/voice/FINDINGS.md`): the
agent's TTS fed at four times real time, each frame stamped with its slot. With timestamp playout
the listener played 100% of the audio on a clean path, 99.7% with 60 ms loss bursts and 98.1% on
LTE with a download (R 89.2, 87.9 and 71.6). Without it, the same player played 5% (R 11.2 clean):
it resyncs on early frames. WebRTC fed the same bursts played 39.2% clean (38.3% on LTE) and dropped
the rest, whatever its jitter buffer target; paced, it played 100%. Wave 2's agent cells (finding
R-AG-01) had shown the player without it failing the same feed live: every clean round suspect, and
on LTE 2,497 and 2,159 ms mouth-to-ear with 41.4 and 24.1 drops a minute, R 0.

The same runs found the give-back's reason (finding A-16): on LTE with a download, a paced talker
through timestamp playout sat at 960 ms mouth-to-ear (R 43.5), against 378 ms for the bursty talker
and 540 ms for paced WebRTC; with 60 ms loss bursts, 252 against 146 ms. A simulation of the
sequencer and the playout clock with a model of the jitter buffer (its grow, shrink, trim and hard
bound; a paced 20 ms talker; a queue growing to 700 ms over 30 s, then draining) reproduces it: the
median mouth-to-ear 10 to 30 s after the drain was 120 ms without timestamp playout and 840 ms with
it, as the clock never caught up. With the give-back it is 140 ms. The bursty talker through the
same queue played 98.1% at 300 ms after the drain without the give-back, 97.4% at 120 ms with it;
on a path without a queue neither talker changed (bursty 100% played, paced 100 ms). The bench has
not run the give-back yet.

Measured locally on 2026-10-29 (relay-3cnp.399, before the give-back): moq-relay 0.17.9
(`30fd9d450`) built from source and the talker in one network namespace of relay-dev's moq-netbench
rig (`rig local`), the listener in the other behind the rig's profile (`rig apply`, the downlink
shaped on the peer), both on this SDK over native QUIC. The listener is relay-sdks' own receive chain
composed as the example listener (frame reader, frame sequencer, audio input, its worklet and the
WASM Opus decoder), with Web Audio stood in for in real time (558-frame render callbacks, a 20 ms
output delay); a frame counts as played when its decoded audio reaches the output. A TTS agent sends
six 8 s turns, 2 s apart, each handed over at four times real time from its first frame's slot
(2,400 frames of 20 ms), with and without `timestampPlayout` (`leadUs: () => input.playoutLeadUs`).
Without it the player discarded about 91% of the clip on every link: the sequencer dropped most
frames as late and the jitter buffer trimmed about 6 s, and what it did play came up to 5.9 s before
its slot. With it, every frame that arrived played, at a constant delay from its slot within each
turn (the listener's own delay plus the lead), with no trim and no resync; QUIC's resends on the
lossy link arrived while their frames were still held. The one frame lost in a second lossy run was
100 ms into the first turn, sent 75 ms ahead of its slot: too little lead for a resend over a 50 ms
round trip, as for any frame a live talker sends.

| Link (rig profile) | Played without it | Its losses | Played with it | Mouth-to-ear from the slot, with it |
| --- | --- | --- | --- | --- |
| Clean | 203 of 2,400 (91.5% dropped) | 1,938 late, 6.18 s trimmed | 2,400 of 2,400 | 40 ms (53 ms max) |
| `loss-random-2` (50 ms RTT, 2% loss) | 206 (91.4%) | 1,942 late, 6.20 s trimmed | 2,400; 2,399 in a second run | 64 ms (77 ms) |
| `narrow` (30 ms RTT, 2/1 Mbit/s, 500 ms FIFO) | 210 (91.2%) | 1,935 late, 6.16 s trimmed | 2,400 | 47 ms (60 ms) |
| `narrow`, a bulk CUBIC download filling the FIFO | 215 (91.0%) | 1,921 late, 6.61 s trimmed, 12 resyncs | 2,398 (2 late, 1 underrun) | 473 ms (513 ms) |

### Audio player

[Issue #314](https://github.com/muxinc/relay-sdks/issues/314). TypeScript only, in
`@caprid/moq-media`: `createAudioPlayer({ context })` plays any number of inputs, usually one per
speaker, into one `AudioContext`. Each `player.addInput({ clock, decoderConfig })` takes frames in
decode order from a frame reader (`onFrame: input.push`), decodes them with WebCodecs, holds them in
a jitter buffer in an `AudioWorklet`, and reports what is audible to its broadcast's playout clock.
Every input goes through its own gain into `player.output`, which the application connects. The
player has no UI and no transport, so the Video.js renderer can use the same core behind its own
frames-in, clock-out seam.

**Decoding.** The decoder is built from the catalog entry (`audioDecoderConfigFromTrack`), or
defaults to Opus at 48 kHz. Decoders count output timestamps on from their first input (see
[Playout clock](#playout-clock-player)), so the input resets its decoder wherever a frame does not
start where the previous one ended: at a `gap`, a `resync` or a `restart`, and at any jump of more
than 1 ms. For Opus the previous frame's length is read from its TOC byte (RFC 6716 §3.1), so the
check does not wait for decoder output. At a `gap` or a jump the old decoder is flushed, not closed:
a frame reader can deliver frames and the gap after them in one poll, before any of them is decoded,
so the old decoder first outputs what it was given, and the frames after the gap wait for the new
decoder until it has. A frame that starts before the previous one ended is behind it and is dropped
as late. A `resync` or `restart` closes the decoder, drops its pending output, and flushes what the
buffer holds, all of which belongs to the replaced timeline. A `restart` also forgets earlier cuts.
A `resync` keeps them, because audio a talker interrupted can still arrive behind one. A context at another rate than the decoder's gets linearly resampled
audio.

**Loss concealment.** WebCodecs' `AudioDecoder` has no packet-loss concealment and cannot decode
the in-band FEC (LBRR) an Opus encoder puts in the next packet, so a frame that never came would
play as silence. With `lossConcealment` on (the default), an Opus input decodes instead with libopus
1.5.2 compiled to WebAssembly (decoder only, 116 KB, imports nothing; built by
`ts/packages/moq-media/scripts/build-opus-wasm.sh` from pinned sources and committed as base64,
with its SHA-256 checked by a test). It decodes synchronously on the main thread with one decoder
for the input's life. At a `gap` (frames skipped: lost, or too late for the sequencer) the next
frame's FEC rebuilds the frame just before it when that packet carries LBRR, and libopus's
concealment synthesizes the rest of the gap, up to 120 ms, from the decoder's own state. Past that,
the worklet plays silence, as for any gap. The filled audio carries the missing timestamps, so the
buffer plays on without a hole and no decoder is reset. Only while the buffer plays: once it has run dry
the worklet skips the missing audio rather than playing it late (as its own silence fill works only
inside held audio), so a gap then is not filled, which would add its length to the delay. A gap longer than 1 s, a `resync` or a
`restart` resets the decoder instead. `synthesizedUs` and `recoveredUs` count what was filled.
`lossConcealment: false`, a codec other than Opus, or a realm without WebAssembly decodes with
WebCodecs as described above. Measured offline on stage-2 netbench arrivals at 2% random loss
(relay-dev `moq-netbench/docs/moq-optimisation.md`): drops from about 56 a minute to 16 with
concealment alone and 3 with the publisher's in-band FEC, at no added delay.

**Jitter buffer.** A ring in the worklet, in the context's frames, starting from the numbers
Huddle's player worklet runs with (`demos/huddle`):

| Rule | Default | Why |
| --- | --- | --- |
| Hold the target before playing, at the start and after running dry | 60 ms | Three 20 ms Opus frames: room for one late frame and a burst of two. Huddle runs on the Relay with this start. The A/V test's fixed 150 ms was sized for video and Firefox's encoder, not for voice |
| Grow the target after an underrun | +40 ms | Two Opus frames: react hard, since the shrink below gives it back within seconds when the spike was a one-off (relay-dev relay-3cnp.170; Huddle grew 20 ms and gave it back in 16 s) |
| Count an underrun only when audio comes back within the ceiling, and not after a flush | | A longer dry spell is the talker going quiet, which no buffer would cover; audio after a flush starts a new timeline |
| Shrink the target after 2 s with no underrun | −10 ms, to 20 ms | The jitter is measured where it shows, in what the buffer never used (the trim below), so the floor only has to cover a render callback and timing noise. Measured on network arrivals replayed offline, not on a busy page: a main thread that stalls delays decoded audio on its way to the worklet, and a lower floor leaves less room for that before an underrun (which then grows the target). Huddle's 40 ms floor and 8 s cadence held audio 20–60 ms longer than WebRTC's NetEQ on every stage-2 netbench path, from the same arrivals (relay-dev `moq-netbench/docs/moq-optimisation.md`) |
| Ceiling | 400 ms, and never more than the clock's target latency less the output delay and 40 ms | Huddle's ceiling; a frame further ahead of the clock than its target latency makes the frame sequencer resync forward, which would flush the buffer |
| Trim, each second, held audio that never dipped to the target | when ≥ 5 ms, and not while the playback rate is below 1 | A burst after a stall would otherwise stay as lasting latency, since audio arrives as fast as it plays; a rate below 1 gains that delay on purpose |
| Hard bound | 2 × target + 60 ms, under the ceiling | Huddle's backlog bound |
| A frame that never came, inside held audio | silence for its length, up to 1 s | Keeps later audio on time. With loss concealment the main thread fills an Opus gap before it reaches the worklet (above), so this covers what concealment leaves and the WebCodecs path |

**Clock.** About every 20 ms the worklet posts the context frame it is rendering and the timeline
timestamp of the sample there. The main thread times those posts with a [render clock](#browser-capture),
on the playout clock's own time source, and reports `reportAudio(timestamp, renderedAt +
baseLatency + outputLatency + player.outputDelayUs, input.stats)`, reading the latencies fresh each
time. While the buffer is dry it reports nothing, so the clock runs on by itself and frames that
missed their slot are dropped as late instead of played late.

The input calls four members of its clock, `targetLatencyUs`, `localNow`, `reportAudio` and
`stopAudio` (`AudioInputClock`), so a player can pass its own clock instead of a `PlayoutClock`
([issue #329](https://github.com/muxinc/relay-sdks/issues/329)). The Video.js renderer does: its
clock holds where the buffer ran dry rather than handing over to wall pacing, which would step the
position back when audio resumes. Such a clock needs to know what the worklet has played, which it
cannot rebuild from the frames pushed and the timing of reports, so each report carries the input's
stats, refreshed with it:

| Stat | Meaning |
| --- | --- |
| `heldUntilUs` | Timeline timestamp just past the newest held sample: the end of decoded audio, or `null` when nothing is held |
| `dryAtUs` | Timeline timestamp of the read position where the buffer last ran dry while playing, until it plays again; `null` otherwise, and after a flush or a cut that empties it |
| `playStarts` | Bumped each time playback starts: at the start, after running dry, after a flush or a cut that emptied it |

A report whose `playStarts` is newer than the one before is of audio played after the buffer last
ran dry. A report with the same count, still in flight when it ran dry, is the tail before it. A clock
can hold at `dryAtUs` while it is set and release on the first report of a newer `playStarts`.
`PlayoutClock` ignores the third argument: its behavior is pinned by the shared vectors, and it
paces by wall clock while the buffer is dry.

**Playback rate.** `input.playbackRate` (and the `playbackRate` option), from 0.5 to 2, default 1,
is how much held audio each output frame plays, from the next render quantum. A player that owns
its latency, as the Video.js renderer's controller does with ±5 % for seconds at a time, nudges
it: above 1 sheds latency, below 1 gains it, where trimming would cut audibly and running dry
would play silence. The worklet keeps a fractional read position and reads between held samples
by linear interpolation, as the resampler does, carrying the fraction across quanta and rate
changes, so a change does not click. Pitch shifts with the rate (about 0.85 semitone at 5 %), and there
is no time-stretching. Held audio, the jitter target, trims, the hard bound and underruns stay in
held audio, and `bufferedUs` stays timeline time. Below rate 1 the input does not trim: held
audio that never dips to the target is the latency being gained, and trimming it would cut out what
the slower rate added. Only the hard bound limits it then, and back at rate 1 the usual trim
applies. A player that keeps more latency than that holds it upstream and meters frames in, as the
Video.js renderer does with its subscriber buffer. Reports carry the timestamp at the fractional
read position of the reporting quantum's first sample, so a `PlayoutClock` extrapolating at rate 1
between reports is off by at most |rate − 1| × 20 ms, 1 ms at 5 %. Rate 0 is not a rate: pausing
is `context.suspend()`.

**Audio-only target latency.** The clock's 2 s default waits for keyframe intervals that an
audio-only broadcast never reports. `AUDIO_ONLY_TARGET_LATENCY_US`, 500 ms, is the smallest budget
that still holds the 400 ms jitter-buffer ceiling plus a wired output delay (about 50–60 ms in
Chromium's headless runs below), and it cuts the silence before a resync from 2 s to half a
second. A Bluetooth output delay lowers the ceiling instead: to 260 ms at a 200 ms delay.
ITU-T G.114 puts the limit for most conversation at 400 ms one way, so a jitter buffer near the
ceiling is already the network's failure, not a setting to plan for. The default stays a constant
the application passes, rather than a clock preset or an automatic default, because the clock's
behavior is pinned by shared vectors in both SDKs.

**Cut.** `input.cut({ fromUs, untilUs })` drops received audio in a timeline range: what the
buffer holds, output still in the decoder, and, when the range has an `untilUs`, frames that arrive
later inside it, until a `restart`. The
first audio from the cut's start on is joined on without silence for the range cut out. Audio from
before the cut's start that was still decoding can land first and plays as usual, and a frame lost
outside the range is still filled with silence. With no range it drops
everything held. A talker's interrupt, read from the [interrupt track](#interrupt-track), is a cut
range as it is.

**Echo cancellation.** Chromium's echo canceller takes only audio that WebRTC plays as its
reference. `createEchoCancellationLoopback(context, player.output)` sends the output through two
local `RTCPeerConnection`s into a media element. Firefox and Safari cancel echo from all page output
and need none. The loopback adds its receiver's buffering, which no API reports: set
`player.outputDelayUs` if video must stay in sync with it.

Measured live on 2026-10-16 with `mise run ts:test:audio-player`: headless Chromium plays the
WebCodecs example's synthetic microphone (a steady tone) through the example's audio-only listener,
8 s steady, then a cut of everything held and the next 300 ms of the timeline. "Delay" is the
broadcast clock's now less the playout clock's position: capture to audible, with no microphone
latency. Medians [p95]:

| Path | Frames | Underruns | Buffer, ms | Delay, ms | Output latency, ms |
| --- | --- | --- | --- | --- | --- |
| In-memory hub | 399 | 0 | 55 [70] | 124 [132] | 59.6 |
| Relay (`sjc`) | 401 | 0 | 64 [84] | 183 [191] | 51.6 |

In both runs the target shrank from 60 to 50 ms during the steady phase, playback resumed after the
cut with no underrun, and the frame sequencer resynced nothing.

Rerun on 2026-10-19 with two phases between the steady phase and the cut: playback rate 0.95 for a
second, then 1.05 for a second ([issue #329](https://github.com/muxinc/relay-sdks/issues/329)).
"Delay moved" is the playout delay at the end of the phase less at its start. A second at ±5 %
should move it 50 ms. "Largest step" is the largest difference between two consecutive output
samples, read through an `AnalyserNode` every 20 ms (a rerun after the fix for running dry at a
fractional rate, which none of these phases did, moved the delay by +49.2 / −49.1 ms on the hub and
+49.0 / −49.2 ms on the Relay): the 0.3 tone's own steepest is 0.0174 at rate
1 and 0.0181 at 1.05, and a click would be far larger. Neither phase trimmed or concealed audio,
and the clock's position never stepped back between 20 ms ticks:

| Path | Phase | Underruns | Delay moved, ms | Buffer p50 [p95], ms | Largest step |
| --- | --- | --- | --- | --- | --- |
| In-memory hub | 0.95 | 0 | +49.1 | 76 [111] | 0.0174 |
| In-memory hub | 1.05 | 0 | −49.0 | 80 [105] | 0.0183 |
| Relay (`sjc`) | 0.95 | 0 | +49.1 | 74 [92] | 0.0174 |
| Relay (`sjc`) | 1.05 | 0 | −49.4 | 68 [96] | 0.0183 |

The steady phases in the same runs: hub 399 frames, buffer 46 [56] ms, delay 101 ms, output
latency 42 ms; Relay 400 frames, buffer 43 [53] ms, delay 142 ms. The hub's steady phase concealed
25 ms of frames that came late, which no rate phase did.

## Interrupt track

[Issue #320](https://github.com/muxinc/relay-sdks/issues/320). A talker that stops mid-utterance, a
voice bot the user talks over, has already sent audio its listeners hold in their jitter buffers and
have in flight. Stopping its writes does not stop that audio. The interrupt track tells listeners to
drop it.

**The track.** A data track in the broadcast's namespace, named `interrupt` by default
(`INTERRUPT_TRACK`), listed in the catalog by name with no role, so a listener subscribes only to a
talker that can interrupt. Each interrupt is one object in a group of its own, its payload compact
UTF-8 JSON (`buildInterrupt` / `build_interrupt`):

```json
{"track":"audio","fromUs":1760000000000000,"untilUs":1760000000300000}
```

| Field | Meaning |
| --- | --- |
| `track` | The audio track in the same namespace whose audio to drop. Required, a non-empty string. |
| `fromUs` | Optional. Drop from this timestamp on, on the broadcast timeline. Absent: from the oldest audio the listener holds. |
| `untilUs` | Required. Drop before this timestamp: the end of the last audio the talker sent. |

Timestamps are non-negative safe integers in microseconds on the broadcast timeline the audio is
stamped on, and `fromUs`, when present, is before `untilUs`. A reader (`parseInterrupt` /
`parse_interrupt`) refuses a payload that breaks those rules and ignores fields it does not know, so
later versions can add some.

**The talker** sends an interrupt when it stops before the end of what it has sent, with `untilUs`
the end of the last audio frame it wrote: for a Python talker on `OpusEncoder`, the last packet's
`timestamp_us` plus its `duration_us`. It drops any audio it has encoded and not sent, and starts
its next audio at or after `untilUs`. On a live clock that comes on its own. It leaves `fromUs` out
to drop everything the listener has not played yet, which is what barge-in wants: the talker cannot
know how far behind each listener plays. With `fromUs` set, a talker can let a listener finish
audio up to a point, such as the end of a word.

**The listener** applies an interrupt for an audio track it plays as a cut,
`input.cut(interrupt)`: it drops what the buffer holds and what is still decoding in the range,
and frames that arrive later inside the range, which covers audio still in flight on the audio
track when the interrupt overtakes it. Audio from before a `fromUs` still plays, wherever it is. The listener ignores an interrupt for a track it does
not play and reports one that does not parse.

**No sequence number.** A cut can only drop audio before its `untilUs`, and the talker's next audio
starts at or after it, so an interrupt that arrives late, twice, or out of order with a newer one
drops nothing it should not. Group IDs give the order anyway. For the same reason a listener that
joins and is handed the track's latest object (a relay cache) drops only audio older than what it
will play. Nothing about an interrupt needs the relay to deliver it before the audio it cuts.

**What it does not do.** Detecting that the user is talking (VAD, turn detection) is the talker's
business, as is cancelling whatever produced the audio. The convention cuts audio only. A listener
applies the same interrupt to the [transcript](#transcript-track) of the audio it cuts.

Measured live on 2026-10-19 with `mise run ts:test:audio-player`, which now sends its cut as an
interrupt (everything held and the next 300 ms of the timeline) from the publisher page to the
listener: the listener applied it 0.4 ms after the write through the in-memory hub, and 50.7 ms after
it through the Relay (`sjc`). It cut 60 ms and 39 ms of held audio, and playback resumed with no underrun.

The [voice bot example](../python/examples/voice_bot/README.md) is a talker built this way: a
Python bot answers a browser page and interrupts itself when talked over, leaving `fromUs` out. Its
test runs it over the in-memory hub. Against `sjc` on 2026-10-19, with the page in headless Chromium
on a scripted microphone, over four runs of three turns each, every interrupt reached the page's
player 32 to 50 ms after the bot's last audio, and each run cut 117 to 140 ms of held reply audio, with
no underrun.

## Transcript track

[Issue #320](https://github.com/muxinc/relay-sdks/issues/320). The text of what a talker says, such
as a voice bot's reply from its TTS, for a listener to show as the audio plays. A data track written
one group per write has no order across groups and no tie to the audio, and a transcript of audio
an interrupt cut would still claim the cut words were said. The transcript track places each piece
of text on the audio's timeline and lets an interrupt retract it.

**The track.** A data track in the broadcast's namespace, named `transcript` by default
(`TRANSCRIPT_TRACK`), listed in the catalog by name with no role. Each object is one version of one
segment, in a group of its own, its payload compact UTF-8 JSON (`buildTranscriptSegment` /
`build_transcript_segment`):

```json
{"seq":12,"track":"audio","fromUs":1760000000000000,"untilUs":1760000000480000,"text":"Hello there.","final":true}
```

| Field | Meaning |
| --- | --- |
| `seq` | Required. The segment's number. A later version of the same segment repeats it; a talker numbers its segments in timeline order. |
| `track` | Required. The audio track in the same namespace whose audio this is the text of. |
| `fromUs` | Required. Where the segment's audio starts, on the broadcast timeline. |
| `untilUs` | Optional. Where it ends, after `fromUs`. Absent while the talker does not know yet. |
| `text` | Required, possibly empty. A later version replaces it whole. |
| `final` | Required. `true`: no later version follows. A final segment has `untilUs`. |

Numbers are non-negative safe integers, timestamps in microseconds on the timeline the audio is
stamped on. A reader (`parseTranscriptSegment` / `parse_transcript_segment`) refuses a payload
that breaks those rules and ignores fields it does not know.

**Versions.** A segment can be sent more than once: interim versions while its text or its end is
not known, then a final one. An STT talker sends its partial hypotheses this way. A TTS talker sends
an interim version just before the segment's first audio, so that a listener has it when the audio
plays, and the final one when its last audio is sent. A listener keeps a segment's final version
against any later one, and otherwise the last version it read. Groups can arrive out of order, so an
interim version read after a newer interim one shows until the final one replaces it.

**Interrupts retract.** A talker that interrupts its audio sends no final version for the segment
it cut. The interrupt ends it. A listener applies the interrupt to its transcript at its playout
position: audio before the position was heard, so the cut starts at the later of the interrupt's
`fromUs` and the position. It drops the segments that start inside the cut, and ends the one playing
at the cut's start there and marks it cut. It remembers the cut, so a version that arrives later is
treated as if it had arrived first, as the audio player treats audio that arrives late: a segment
that starts inside the cut is dropped, one that runs into it is ended at its start, and an interim
version of a cut segment changes nothing. The talker's final version of a cut segment, delayed past
the interrupt, is clipped the same way, so a segment whose audio ended before the cut is no longer
marked cut.

**Showing it.** A listener shows a segment once its playout clock reaches `fromUs`: the text
appears as the audio it belongs to plays, not when it arrives, and a segment the interrupt dropped
is never shown.

`Transcript` (`createTranscript()` / `Transcript()`) is that listener's state for one audio track:
`apply(segment)` keeps a version and says whether it changed anything, `cut(range, positionUs)`
applies an interrupt (a parsed `Interrupt` is the range), and `linesAt(positionUs)` /
`lines_at(position_us)` gives the segments whose audio has started, in timeline order, each a
`TranscriptLine` with a `cut` flag. It is pure, so both SDKs hold it to the same vectors. A segment
is known by its `seq` within one run of the talker's track, so a listener uses a new `Transcript`
when the talker starts over, and a talker that starts over should not reuse its last run's numbers
(the example bot starts at the clock's millisecond). The transcript keeps every segment it is given.
A long-lived listener that wants a bound starts a new one.

**No ordering track or sequence across segments.** The timeline gives the order (`fromUs`), and
`seq` only names a segment so that its versions find each other. A relay that hands a late joiner
the track's latest object hands it the latest version of one segment, which is all the transcript
it can have: earlier segments are history the talker would publish another way.

The [voice bot example](../python/examples/voice_bot/README.md) publishes the text of each part of
its replies, and its page shows each line as the audio reaches it, marked where it was talked over.
Measured against `sjc` on 2026-10-19, with the page in headless Chromium on a scripted microphone,
over four runs of three turns each: each interim segment reached the page 65 to 72 ms before its
audio played (14 segments; the first of a run is not measured, while the clock is idle), every line
appeared with its audio, and every reply the user talked over was shown marked cut, with no
underrun. The margin is about the jitter buffer's depth, since the bot sends the text just before
the audio: a talker that knows its text earlier, as a TTS does, can send it sooner.

## Media Timeline track

[Issue #291](https://github.com/muxinc/relay-sdks/issues/291). Format helpers implemented in both
format packages. [behavior.md](behavior.md#media-helpers) holds the testable rules. Both product
packages publish the track with their document service, and both examples do
([issue #292](https://github.com/muxinc/relay-sdks/issues/292)).

msf-01's Media Timeline track (§7) is an optional track, announced in the catalog, whose records
map media time and an MOQT location to wall-clock time. It is MSF's counterpart of HLS
`PROGRAM-DATE-TIME`. Since #290 the broadcast timeline is itself wall-clock time, so a player that
knows this convention does not need the track to learn when a frame was captured. One that does
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
`PROGRAM-DATE-TIME`. This departs from §7.1.1's wording (see [Spec gaps](#spec-gaps)).

### Cadence

One entry per group of the indexed track, for the group's first object. Under keyframe grouping
that is one entry per video keyframe group, so there is an entry at every random-access point and
nowhere else. That is what a seek needs, at one record per GOP, typically every one or two seconds.

The entries' locations point into video, the first track in `depends`. Audio is one group per
frame, 50 or more a second, and an entry for each would multiply the document for no gain: every
track in the render group is stamped on the same broadcast timeline, so a player that has found the
video location for a media time finds the audio for it by timestamp. A location cannot address
audio anyway: it is a bare `[group, object]`, and audio's group numbers (one per frame) do not line
up with video's (one per GOP). msf-01's example 5.6.10 gives each track its own timeline for that.
A player here seeks audio by timestamp instead. msf-01 §7.2 still requires
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

The rules a publisher follows:

- An entry is added only when a group's first object was sent: under keyframe grouping, a keyframe
  that opened a group on the relay. A keyframe the relay had no subscription for, or that was stale
  or interrupted, adds nothing, since its location would point at a group that does not exist.
- msf-01 §7.3 requires the first object of every timeline group to be an independent document:
  every record accumulated and still accessible. Each new entry opens a new timeline group whose
  only object is the independent document of the retained entries. A viewer that joins at the
  relay's current group gets the whole history in one object. The incremental objects §7.3 allows
  after the first are not used.
- How many entries to retain is the publisher's choice: as far back as its viewers can still fetch,
  and well under the object size limit. One entry is about 35 bytes, so an hour of the examples'
  one-second GOPs is about 126 KB. The accumulator keeps 512 by default, about 18 KB or 8.5 minutes
  of one-second GOPs, and drops the oldest.
- The timeline is a state-shaped track whose document changes. The document service
  (`createDocumentService` / `DocumentService`) sends the latest document on every relay
  subscription, as the catalog service does, and on every new entry while the relay is subscribed.
  Entries made without demand accumulate and go out with the next subscription.
- A supervised publisher's next generation is a new broadcast with a new namespace, catalog,
  broadcast clock and origin. It creates a new timeline and service, which start empty. The old
  entries point at locations in a namespace the new catalog does not list, so they are not carried
  over. Wall-clock time is what joins two generations, and both stamp their media with it directly.

The [WebCodecs example](../ts/examples/webcodecs/README.md) and the Python
[Video.js streaming example](../python/examples/videojs_stream/README.md) publish the track. The
Mediabunny example does not: a file's media has no capture time to report. Against
`sjc.relay.mux.global` (2026-10-14), every record the Python example's `--verify` viewer received
named a received keyframe's group and timestamp, and a subscriber that joined 12 s in got all seven
records so far in its first object.

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
draft. Until then, this repository's answer is its convention.

- **No anchor for a render group.** msf-01 §5.2.11 says tracks in a render group "SHOULD be
  rendered simultaneously", and §4.2 aligns alternates, but nothing says the tracks of a group
  share a timestamp anchor, and nothing signals which anchor a track with a declared Timescale
  uses (LOC §2.3.1.2: "application-defined"). This repository anchors every track at the Unix
  epoch. draft-frindell-moq-timestamp-00 proposes a clock identifier and an origin, which would
  let a publisher say so.
- **`targetLatency` needs wall-clock time per frame.** msf-01 §5.2.8 defines it from the time a
  frame was encoded, in UTC. With an application-defined anchor, the only route to that time is
  the Media Timeline, at millisecond resolution and only at the indexed locations.
- **Encoded or captured.** msf-01 §5.2.8 and §7.1.1 both define the wall-clock time as when the
  media was encoded. Stamping capture time departs from that definition by the encoder's latency.
  The sentence has no RFC 2119 keyword, but normative text needs none (RFC 8174 §2). Lip sync,
  syncing across participants and `PROGRAM-DATE-TIME` all need capture time, which is what this
  repository stamps and indexes. draft-nandakumar-moq-tempo-00 carries an
  explicit capture timestamp next to a playout delay.
- **Two timescales.** msf-01 §5.2.21's catalog `timescale` and LOC's Timescale property (`0x08`)
  have no rule for how they relate or which wins. The readers here take the object's property,
  then the track's, and never the catalog's. LOC has no rule for object versus track precedence
  either.
- **A missing Timescale reads two ways.** LOC draft-04 §2.3.1.1 reads a Timestamp without one as
  wall-clock microseconds since the epoch. draft-lcurley-moq-timestamp-01 §3 says such a track has
  no timeline and falls back to arrival time for age decisions, though a receiver that implements
  LOC may read it as LOC does. Neither tells a relay to overwrite the Timestamp. moq-relay's
  restamping (below) is its own.
- **The relay modifies properties.** MOQT-22 §8.4 says a relay must forward properties unmodified
  and cache them. moq-relay (read at moq-dev/moq 5152ec74) rescales `0x10`, writes its own `0x08`,
  restamps tracks without a Timescale, and drops properties it does not know, among them `0x0D`
  and the publisher's other track properties in `SUBSCRIBE_OK`. That is a relay bug rather than a
  draft gap, listed here because it is why the readers scale by whatever Timescale arrives.
- **Capture latency has no field.** No draft has a place to declare it, so it is folded into the
  Timestamp (see [Capture latency](#capture-latency)).

## Package placement

The media layer has two homes, matching the existing boundary rules.

- A format package per language, `@caprid/moq-media` and `moq-media` (module `moq_media`), holds
  the format helpers and the readers. It depends on the core only (for `KeyValuePair` and
  `MoqtObject`), has no other runtime dependencies, and knows nothing about Relay. The TypeScript
  package started as an attributed snapshot of the Video.js `media/moq` modules with its own
  `UPSTREAM.json`, intended to migrate back upstream rather than fork, as the core did. The Python
  package ports it. The boundary checks hold that the format package imports nothing from the
  product package and no transport library, and that the core imports nothing from it.
- Media publishing lives in the product package, as `@caprid/relay/media` (a second package
  entry point) and `caprid_relay.media`, because it is built from `RelayTrackWriter`, demand, and the
  write deadline, and because Relay-specific observations such as the dropped Video Config property
  belong there.

The alternative was one home, everything under `caprid_relay.media` and `@caprid/relay/media`. It
is one package fewer per language, and the code in the samples above is identical apart from the
import lines. It gives up the snapshot-and-migrate story, and it puts a catalog builder that any
MoQ publisher could use behind a Relay dependency.

Encoders stay out of the product packages, and out of the format packages' roots. An `H264Config`
record that turns Annex B into an avcC is a format helper. A class that opens libx264 is not. The
exception is Python's Opus encoder and decoder ([issue #320](https://github.com/muxinc/relay-sdks/issues/320)),
which a voice bot needs both ways: `moq_media.opus`, on PyAV, behind the `opus` extra, and never
imported by the package root ([ADR 0006](../docs/adr/0006-opus-codec-behind-an-extra.md)).

## Keeping the languages in lockstep

The media layer uses the mechanisms the rest of the repository uses:

- `protocol.json` pins the LOC draft (`draft-ietf-moq-loc-04`), the MSF draft and catalog version
  (`draft-ietf-moq-msf-01`, `"draft-01"`), the microsecond default timescale, the LOC property
  types, and the default track names, each asserted by a test in every SDK.
- `behavior.md` has [Media helpers](behavior.md#media-helpers),
  [Media publishing](behavior.md#media-publishing) and
  [Media subscribing](behavior.md#media-subscribing) sections: what the catalog builder refuses,
  how the catalog service treats demand, what the frame writer does without demand and after a
  failure, and the statement that time-based drop policy is the application's.
- `vectors/media.json` holds catalog vectors (track descriptions in, exact JSON bytes out), LOC
  property vectors (timestamp and config in, key-value pairs out), source URL vectors including the
  `.xx` escaping and a token, and avcC vectors (Annex B in, avcC and codec string out), so both
  SDKs are held to the same bytes.
- The TypeScript snapshot brought `parse-catalog.ts` with it, so the acceptance test the Video.js
  builder names for itself, "every emitted document must round-trip through the parser", runs in
  this repository too.
- `vocabulary.md` gets rows before any of these names become public.

## Sequencing

Each step leaves the example runnable against the live relay with `--verify`, which is how the
change is checked without a browser, and each step deletes example code. The status at the top of
this document says how far each step has gone.

1. Format helpers in both languages, with the vectors, pins, and the `behavior.md` section. Move
   the example's `moq_media.py` onto them, which removes about 300 lines and fixes #29 for real
   because the README snippet can then import what it calls.
2. The media publishing handle. The example's catalog serving and frame writing move into the SDK.
   Its `LiveQueue`, encoders, pacing, and `finished` accounting stay. The example's tests for
   catalog serving and track-end handling become the SDK's tests.
3. The readers, with `--verify` rewritten on them, and the TypeScript example that the browser
   sample above sketches.

## Open questions

- Names. `register_media` and `MediaTracks` are placeholders. The format tier kept the Video.js
  names `buildMsfCatalog` and `composeMoqSource`, with Python spelling them `build_msf_catalog`
  and `compose_player_url`.
- A subscription that arrives mid-GOP. The example waits for the next keyframe. Video.js retains
  the open group and replays it from object 0 so the viewer decodes at once. Replay is better for
  viewers and costs one GOP of memory per track. It can be a later option on the frame writer.
- Queue semantics on the frame writer: settled by
  [issue #263](https://github.com/muxinc/relay-sdks/issues/263). `write` resolves with the frame's
  outcome once the transport accepted it, as `write()` does, and may be called without awaiting.
  The frames waiting meanwhile are a bounded backlog that drops rather than queues.
- Codec scope at the start. H.264 is what the example needs and what the relay has been tested
  with. HEVC (`hvcC`), AV1 (`av1C`), and AAC (AudioSpecificConfig) records are the same shape of
  helper and can follow demand.
