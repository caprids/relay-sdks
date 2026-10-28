# MoQ media formats

`@caprid/moq-media` holds pure format helpers for LOC (`draft-ietf-moq-loc-04`) and independent MSF
(`draft-ietf-moq-msf-01`) catalogs, plus the clocks, frame sequencer, and audio player that sit on top of them. It
depends only on `@caprid/moq-core`. It needs no encoder, transport library, or Relay connection.

```ts
import { buildMsfCatalog, composeMoqSource, locProperties, locTrackProperties, parseMsfCatalog } from '@caprid/moq-media';

const catalog = buildMsfCatalog({
  namespace: ['room', 'live'],
  video: { codec: 'avc1.42E01F', width: 640, height: 360, initData: avcc },
  audio: { codec: 'opus' },
});
const video = publisher.registerTrack({ track: 'video', trackProperties: locTrackProperties() });
await group.write(encodedFrame, { properties: locProperties(timestampUs, { videoConfig: avcc }) });
const source = composeMoqSource('https://relay.example', ['room', 'live'], { token });

const { tracks } = parseMsfCatalog(catalogObject.payload, { catalogNamespace: ['room', 'live'] });
```

Shared test vectors live in `spec/vectors/media.json` and `spec/vectors/clock.json`. `UPSTREAM.json` records the
Video.js provenance.

## Catalogs

`buildMsfCatalog` emits compact UTF-8 JSON bytes. It follows these rules:

- It withholds `avc1` and `avc3` until non-empty init data exists. It derives the profile, constraint, and level suffix
  from the avcC, and keeps `avc3` as `avc3`.
- It forces Opus to 48 kHz.
- It rejects empty or duplicate track names, and media roles on data tracks.
- Catalog namespaces use the player's slash-joined dialect, so it rejects a field that is empty or contains `/`. Source
  URLs can escape a literal slash in a tuple field.
- Omitted fields never become JSON nulls. Empty init data is left out for every codec.
- Catalog strings must be valid Unicode, with no lone surrogates.

`buildMsfCatalog` describes a live broadcast and leaves `isComplete` out. msf-01 §5.1.3 forbids the field while it is
false, and once a catalog carries it, it can never be removed. To end a broadcast on purpose, follow msf-01 §11.3:

1. Close the media tracks. Each close tells subscribers that the track ended.
2. If the catalog track has demand, write `buildMsfEndCatalog({ generatedAt })` to it.
3. Close the catalog track.

Do not send the end catalog after a crash or a lost connection.

`parseMsfCatalog` reads an independent catalog object's payload back into track descriptions. Each description has the
namespace tuple, name, packaging, live status, role, codec, dimensions, rates, bitrate, render group, and init data,
resolved from `initRef` to bytes. The parser rejects unsupported versions and delta updates, and ignores unknown fields.
It returns a packaging other than `loc`, such as moq-rs `cmaf`, as declared.

For simulcast layers or a second source, pass `media`, a list of `{ role: 'video', ... }` and `{ role: 'audio', ... }`
entries. Each entry takes an optional `renderGroup` and `altGroup`:

- `renderGroup` (default 1) names tracks that play together.
- `altGroup` names alternate versions of one content, of which a viewer plays one. Members of an alternate group share a
  role and a render group.

`parseMsfCatalog` returns both fields, and `groupMsfTracks(catalog.tracks)` lists the sets that a viewer picks one track
from. See [several media tracks](https://github.com/caprids/relay-sdks/blob/main/spec/media-layer.md#several-media-tracks).

```ts
const catalog = buildMsfCatalog({
  namespace,
  media: [
    { role: 'video', name: 'video-hi', codec: 'vp8', width: 1280, height: 720, altGroup: 1 },
    { role: 'video', name: 'video-lo', codec: 'vp8', width: 320, height: 180, altGroup: 1 },
    { role: 'audio', name: 'audio' },
    { role: 'video', name: 'screen', codec: 'vp9', renderGroup: 2 },
  ],
});
```

## LOC frames from WebCodecs

LOC helpers take nonnegative microsecond timestamps and a positive timescale, both safe integers (up to 2^53−1). A custom
timescale rounds to the nearest tick, with ties rounding up. Pass decoder configs only on keyframes, declare the same
timescale on the track, and keep the init data in the catalog too.

The primary browser input is native WebCodecs. The helpers use WebCodecs types but need no browser globals at import
time.

- `videoTrackFromDecoderConfig` maps an AVC decoder config with four-byte NAL lengths. A decoder config carries no
  bitrate or framerate, so a caller that knows them spreads them into the returned `VideoTrackInfo`.
- `audioTrackFromDecoderConfig` maps an Opus decoder config.
- Both copy the description buffer, including the exact range of a view.
- `chunkToLocFrame` takes an `EncodedVideoChunk` or `EncodedAudioChunk` and returns
  `{ payload, timestampUs, key, properties }`. It copies the encoded bytes unchanged and attaches supplied configs only
  on key chunks. It takes the same `timescale` and config options as `locProperties`, plus an integer
  `timestampOffsetUs` for a shared track timeline. The final timestamp must be a nonnegative safe integer. Keep chunks
  in encoder output order, even when presentation timestamps are reordered.

TypeScript takes the decoder config and length-prefixed H.264 packets that an encoder or demuxer provides.
`avcCodecString` inspects the avcC header. It does not parse the SPS or validate a bitstream. TypeScript has no Annex B
parsing, SPS or PPS reconstruction, or access-unit conversion. Python keeps its Annex B helpers for the PyAV publisher.

See the [direct WebCodecs publishing example](https://github.com/caprids/relay-sdks/blob/main/ts/examples/webcodecs/README.md) for live browser video, catalog
serving, groups, bounded buffering, and cleanup. For files that need demuxing, use the optional
[Mediabunny packet adapter](https://github.com/caprids/relay-sdks/blob/main/ts/packages/moq-mediabunny/README.md).

## Shared timeline and playout clock

Every media track shares one timeline. LOC timestamps are capture times in microseconds since the Unix epoch, with
Timescale 1,000,000 declared on each track. `createBroadcastClock()` pins the `performance.now()` clock to `Date.now()`
once and counts from there, so broadcasts from different publishers line up as closely as their system clocks do.

The publisher path below works in Chromium, Firefox, and Safari:

- **Video.** Stamp frames drawn from the camera with `clock.now()`.
- **Audio.** Route the microphone through an `AudioContext` and an `AudioWorklet`. Stamp audio from the context clock
  through `clock.sourceTimeline()`. Calibrate the source timeline with the context's render clock, a pair of a context
  time and when it was rendered, taken from the worklet block that reached the main thread soonest after its render. Do
  not calibrate from `getOutputTimestamp()`. Firefox returns that render pair shifted back by the output delay, which
  stamps audio that much early. The WebCodecs example's `capture-microphone.ts` calibrates this way.
- **Device changes.** The context resamples whatever device is connected and keeps running while devices change, so
  switching microphones does not shift the timeline.
- **Chromium alternative.** Where `MediaStreamTrackProcessor` exists, its timestamps count from the system's monotonic
  zero, so map them through an uncalibrated `sourceTimeline()`.
- **Microphone latency.** No browser reports a microphone's own input latency. Measured on macOS, it is about 30 ms for
  a MacBook's built-in microphone and 190 ms for AirPods. Set it as `captureLatencyUs` on the source timeline, at
  creation or whenever the device changes, and the timeline subtracts it from every timestamp. See
  [capture latency](https://github.com/caprids/relay-sdks/blob/main/spec/media-layer.md#capture-latency).

On the player, `createPlayoutClock()` makes audio the master. Report what is audible with `reportAudio(mediaUs, atUs)`:
the context time rendered now, less `baseLatency + outputLatency`. Read those two fresh on each report, because the
output delay changes with the device. Video calls `due(timestampUs)` and gets a `wait`, `show`, or `drop` decision. With
no audio, the clock paces by wall clock. Neither clock needs the DOM. See
[shared timeline and playout clock](https://github.com/caprids/relay-sdks/blob/main/spec/media-layer.md#shared-timeline-and-playout-clock).

```ts
const clock = createBroadcastClock();
const video = new VideoFrame(canvas, { timestamp: clock.now() });

// AudioWorklet blocks carry their context time. `renderedUs` and `arrivedUs` are the best pair of the last second.
const context = clock.sourceTimeline({ captureLatencyUs: 30_000 });
context.calibrate(renderedUs, arrivedUs);
const audioTimestampUs = context.timestampUs(blockContextUs);

const playout = createPlayoutClock();
// The context time audible now: rendered now, less the output delay read fresh on every report.
const latencyUs = (audioContext.baseLatency + audioContext.outputLatency) * 1e6;
playout.reportAudio(mediaTimestampAt(nowRenderedUs - latencyUs), performance.now() * 1000);
const decision = playout.due(frame.timestamp);
if (decision.action === 'wait') setTimeout(retry, decision.waitUs / 1000);
```

## Reading subscribed media

A subscriber reads an object's timestamp with `locTimestampUs(object.properties, { trackProperties })`, passing the
track properties from `subscription.subscribed`. It applies whichever Timescale the object or the track declares, rather
than assuming that the relay rescaled to microseconds.

`createFrameSequencer({ clock })` turns one subscribed track's objects into frames in decode order. Objects can arrive
out of order across group streams, and the sequencer drops what is too late against the playout clock.
`createFrameReader` in `@caprid/relay/media` drives it from a subscription (see
[subscribing to media](https://github.com/caprids/relay-sdks/blob/main/ts/GUIDE.md#subscribing-to-media)). Per track, `lateDropUs`, `reorderWindowUs`, and
`catchUpAfterUs` tune audio delivery for a listener or for speech-to-text. `missingGroups` on a frame names the groups
skipped before it (see [voice delivery](https://github.com/caprids/relay-sdks/blob/main/ts/GUIDE.md#voice-delivery-per-direction)).

The combined `register_media` publishing handle is still a proposal in `spec/media-layer.md`. `@caprid/relay/media`
ships the frame writer and catalog service that it would compose.

## Media timeline

msf-01's Media Timeline track maps media time to group locations, for seeking and playlists, with one record per video
group. On this timeline, media time is already wall-clock time, so a record's two times agree. Announce the track with
`mediaTimeline: {}` in `buildMsfCatalog`, which lists the video and audio tracks in its `depends`.

`createMediaTimeline` keeps one broadcast's records, 512 by default. Each time a keyframe has opened a video group,
`add` returns the document to send. Write it as a new timeline group, and again whenever the relay subscribes. The
[WebCodecs example](https://github.com/caprids/relay-sdks/blob/main/ts/examples/webcodecs/README.md) does this. See
[media timeline track](https://github.com/caprids/relay-sdks/blob/main/spec/media-layer.md#media-timeline-track).

```ts
const timeline = createMediaTimeline();
// Once the keyframe that opened video group `groupId` is written:
const document = timeline.add({ timestampUs, group: groupId });
if (timelineTrack.hasDemand) await timelineTrack.write(document); // one object, one group
const read = parseMediaTimeline(timelineObject.payload);
```

## Audio playout

`createAudioPlayer({ context })` plays decoded audio with no UI. Each `player.addInput({ clock, decoderConfig })` takes
the frames it is pushed and does the following:

- It decodes them. Opus decodes with libopus compiled to WebAssembly, which fills a lost frame by packet-loss
  concealment, or rebuilds it from the next frame's in-band FEC. Other codecs decode with WebCodecs, as does every input
  when you pass `lossConcealment: false` or WebAssembly is unavailable. `audioDecoderConfigFromTrack` reads the decoder
  config from a catalog track.
- It holds them in an adaptive jitter buffer in an `AudioWorklet`: 60 ms to start, adapting between 20 ms and 400 ms.
- It reports what is audible to the broadcast's playout clock.
- It plays them through its own gain into `player.output`, the one mix that the application routes.

Inputs and helpers:

- `input.cut({ fromUs, untilUs })` drops received audio in a timeline range.
- A talker's interrupt track (`INTERRUPT_TRACK`) carries cut ranges. Read one with `parseInterrupt`, and apply it with
  `input.cut(parseInterrupt(object.payload))`. `buildInterrupt({ untilUs })` writes one.
- A talker's transcript track (`TRANSCRIPT_TRACK`) carries the text of its audio. Read each segment with
  `parseTranscriptSegment` and keep the segments in `createTranscript()`. Cut the transcript on each interrupt with
  `transcript.cut(interrupt, clock.position())`, and show `transcript.linesAt(clock.position())`.
  `buildTranscriptSegment` writes a segment.
- `createEchoCancellationLoopback(context, node)` plays a node through a local WebRTC loopback, so Chromium's echo
  canceller hears it.
- `input.playbackRate`, from 0.5 to 2, lets a player that owns its latency nudge it.
- An input's clock can be the player's own `AudioInputClock`. Each report passes it the input's stats, including
  `playStarts`, `heldUntilUs`, and `dryAtUs`.
- `createRenderClock` times an `AudioContext`'s renders on the `performance.now()` clock, for capture and playout alike.

Give an audio-only broadcast's playout clock `AUDIO_ONLY_TARGET_LATENCY_US` (500 ms) as its `targetLatencyUs`. The 2 s
default waits for keyframe intervals.

The player takes structural Web Audio types, so it imports no DOM library. Its tests run under a fake context that runs
the real worklet. See [playing audio](https://github.com/caprids/relay-sdks/blob/main/ts/GUIDE.md#playing-audio) and
[audio player](https://github.com/caprids/relay-sdks/blob/main/spec/media-layer.md#audio-player).

## Stability

Before 1.0, a minor version can break the API. This package releases together with `@caprid/relay` at the same version.
See the [changelog](https://github.com/caprids/relay-sdks/blob/main/ts/CHANGELOG.md).
