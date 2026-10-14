# MoQ media formats

Pure LOC (`draft-ietf-moq-loc-04`) and independent MSF (`draft-ietf-moq-msf-01`) format helpers.
Depends only on the generic MoQ core. No encoders, transport libraries, or Relay connection required.

The catalog builder emits compact UTF-8 JSON bytes. It withholds `avc1`/`avc3` until non-empty init
data exists, derives their profile/constraint/level suffix from avcC (preserving `avc3`), forces Opus
to 48 kHz, and rejects duplicate names and media roles on data tracks. Catalog namespaces use the
player's slash-joined dialect, so fields containing `/` or empty fields are rejected. Source URLs
can escape literal slashes in tuple fields. Omitted fields never become JSON nulls. Empty init
data is omitted for every codec, and catalog strings must be valid Unicode (no lone surrogates).

LOC helpers accept nonnegative microsecond timestamps and a positive timescale, both safe integers
(up to 2^53−1). A custom timescale rounds to the nearest tick, ties upward. Pass decoder configs only
on keyframes, declare the same timescale on the track, and keep init data in the catalog too.

TypeScript consumes encoder/demuxer-provided decoder configuration and length-prefixed H.264
packets. `avcCodecString` inspects the avcC header; it does not parse SPS or validate a bitstream.
Annex B parsing, SPS/PPS reconstruction, and access-unit conversion are not exposed in TypeScript.
The primary browser input is native WebCodecs. `videoTrackFromDecoderConfig` maps AVC decoder
configuration (four-byte NAL lengths). A decoder config carries no bitrate or framerate, so a caller
that knows them spreads them into the returned `VideoTrackInfo`. `audioTrackFromDecoderConfig` maps Opus
configuration.
Both copy description buffers, including the exact range of a view. `chunkToLocFrame` accepts
`EncodedVideoChunk` or `EncodedAudioChunk` and returns `{ payload, timestampUs, key, properties }`.
It copies encoded bytes unchanged, attaches supplied configs only on key chunks, and accepts the
same timescale/config options as `locProperties`, plus an integer `timestampOffsetUs` for a shared
track timeline. The final timestamp must be a nonnegative safe integer. Preserve encoder output
order, even when presentation timestamps are reordered. These functions use WebCodecs types but
do not require browser globals at import time.

See the [direct WebCodecs publishing example](../../examples/webcodecs/README.md) for live browser
video, catalog serving, groups, bounded buffering, and cleanup. For files that need demuxing, use
the optional [Mediabunny packet adapter](../moq-mediabunny/README.md). Python retains its Annex B
helpers for the PyAV publisher.

`buildMsfCatalog` describes a live broadcast and leaves `isComplete` out: msf-01 §5.1.3 forbids the
field while false, and once a catalog carries it, it can never be removed. When a broadcast ends on
purpose, close the media tracks first (each close tells subscribers the track ended), then write
`buildMsfEndCatalog({ generatedAt })` to the catalog track if it has demand, then close it
(msf-01 §11.3). Do not send it after a crash or a lost connection.

`parseMsfCatalog` reads an independent catalog object's payload back into track descriptions:
namespace tuple, name, packaging, live status, role, codec, dimensions, rates, bitrate, render
group, and init data resolved from `initRef` to bytes. It rejects unsupported versions and delta
updates, ignores unknown fields, and returns non-`loc` packagings (such as moq-rs `cmaf`) as
declared.

Every media track shares one timeline: LOC timestamps are capture times in microseconds since the
Unix epoch, with Timescale 1,000,000 declared on each track. `createBroadcastClock()` pins the
`performance.now()` clock to `Date.now()` once and counts from there, so broadcasts from different
publishers line up as closely as their system clocks do.

The publisher path below works in Chromium, Firefox and Safari:

- **Video.** Stamp frames drawn from the camera with `clock.now()`.
- **Audio.** Route the microphone through an `AudioContext` and an `AudioWorklet`. Stamp audio
  from the context clock through `clock.sourceTimeline()`, calibrated with the context's render
  clock: a context time and when it was rendered, from the worklet block that reached the main
  thread soonest after its render. Do not calibrate from `getOutputTimestamp()`: Firefox returns
  the render pair shifted back by the output delay, which stamps audio that much early. The
  WebCodecs example's `capture-microphone.ts` does this.
- **Device changes.** The context resamples whatever device is connected and keeps running while
  devices change, so switching microphones does not shift the timeline.
- **Chromium alternative.** Where `MediaStreamTrackProcessor` exists, its timestamps count from the
  system's monotonic zero, so map them through an uncalibrated `sourceTimeline()`.
- **Microphone latency.** No browser reports a microphone's own input latency: about 30 ms for a
  MacBook's built-in microphone and 190 ms for AirPods, measured on macOS. Set it as
  `captureLatencyUs` on the source timeline, at creation or whenever the device changes, and it is
  subtracted from every timestamp. See
  [spec/media-layer.md](../../../spec/media-layer.md#capture-latency).

On the player, `createPlayoutClock()` makes audio the master. Report what is audible with
`reportAudio(mediaUs, atUs)`: the context time rendered now, less `baseLatency + outputLatency`
read fresh each time, because the output delay changes with the device. Video asks `due(timestampUs)` for a `wait`, `show`, or `drop`
decision. With no audio, the clock paces by wall clock. Neither clock needs the DOM; see
[spec/media-layer.md](../../../spec/media-layer.md#shared-timeline-and-playout-clock).

```ts
const clock = createBroadcastClock();
const video = new VideoFrame(canvas, { timestamp: clock.now() });

// AudioWorklet blocks carry their context time. `renderedUs` / `arrivedUs`: the best pair of the last second.
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

A subscriber reads an object's timestamp with `locTimestampUs(object.properties, { trackProperties })`,
passing the track properties from `subscription.subscribed`. It applies whichever Timescale the
object or the track declares, rather than assuming the relay rescaled to microseconds.

msf-01's Media Timeline track maps media time to group locations, for seeking and playlists: one
record per video group. On this timeline media time already is wall-clock time, so a record's two
times agree. Announce it with `mediaTimeline: {}` in `buildMsfCatalog` (it lists the video and audio
tracks in `depends`), and on each new video group open a timeline group holding every retained
record. The track is not published for you yet; see
[spec/media-layer.md](../../../spec/media-layer.md#media-timeline-track).

```ts
const entries: MediaTimelineEntry[] = [];
// On each keyframe that opens video group `groupId`:
entries.push(mediaTimelineEntry({ timestampUs, group: groupId }));
await timelineTrack.write(buildMediaTimeline(entries)); // one object, one group
const read = parseMediaTimeline(timelineObject.payload);
```

Shared vectors live in `spec/vectors/media.json` and `spec/vectors/clock.json`. See `UPSTREAM.json` for Video.js provenance.
Publishing handles and the object-to-frame reader are later stages of `spec/media-layer.md`.

```ts
import { buildMsfCatalog, composeMoqSource, locProperties, locTrackProperties, parseMsfCatalog } from '@mux/moq-media';

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

## Stability

Before 1.0, a minor version can break the API. This package releases together with `@mux/relay`
at the same version; see the [changelog](../../CHANGELOG.md).
