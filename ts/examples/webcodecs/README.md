# Publish live video and audio with WebCodecs

The primary browser publishing path runs from capture, through WebCodecs and LOC with an MSF catalog, to MoQ over
WebTransport. LOC carries the encoder's chunk bytes directly, with no MP4 or WebM muxing and no Mediabunny dependency.

[`publish-canvas.ts`](publish-canvas.ts) samples a canvas at approximately 30 fps and publishes
AVC/H.264 with native `VideoEncoder` output. Given a microphone from
[`capture-microphone.ts`](capture-microphone.ts), it also publishes mono Opus on the same broadcast
timeline. It uses only `@caprid/relay`, `@caprid/moq-media`, and browser APIs. Copy the files into a
browser application whose bundler resolves those packages. Run it in a secure context with WebCodecs,
AVC encoding, and WebTransport support.

The encoders' output goes through `@caprid/relay/media`: a frame writer per media track maps chunks
onto groups (a group per keyframe for video, per frame for audio), and a catalog service and a
document service serve the catalog and the media timeline on every relay subscription. The output
callbacks never await a write. When the transport falls behind, the video writer drops its backlog
and asks for a keyframe, which the capture loop forces on the next frame; it does the same for every
new relay subscription. Only a track that ends stops the example.

```ts
import { composeMoqSource, createBroadcastClock } from '@caprid/moq-media';
import { connectRelayPublisher } from '@caprid/relay';
import { captureMicrophone } from './capture-microphone.js';
import { publishCanvas } from './publish-canvas.js';

// A canvas your application draws into; keep its dimensions fixed during publishing.
const canvas = document.querySelector('canvas');
if (!canvas) throw new Error('Missing canvas');
canvas.width = 640;
canvas.height = 360;
const namespace = ['demo', 'live'];
const url = 'https://relay.mux.global';
const publisher = await connectRelayPublisher({ url, namespace, getToken: () => publisherToken });
const stop = new AbortController();
// Wire your stop button to stop.abort(). Your application keeps owning capture and drawing.
console.log(composeMoqSource(url, namespace)); // supply authorized subscriber credentials in the player
// Optional audio. Create the context in a user gesture.
const clock = createBroadcastClock();
const context = new AudioContext({ sampleRate: 48_000 });
const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
const microphone = await captureMicrophone(clock, stream, { context });
try {
  await publishCanvas(canvas, publisher, namespace, { signal: stop.signal, microphone });
} catch (error) {
  if (!stop.signal.aborted) throw error;
} finally {
  microphone.close();
  await context.close();
  await publisher.close();
}
```

For a voice participant with no video, [`publish-microphone.ts`](publish-microphone.ts)'s
`publishMicrophone(publisher, namespace, { signal, microphone, data })` publishes a catalog with the
audio track alone; `publishCanvas` encodes its microphone through the same code. The
[voice bot page](../voice-bot/README.md) uses it.

## Listen to the audio

[`listen-audio.ts`](listen-audio.ts) plays a broadcast's `audio` track with no video, as a voice
listener would: it reads the catalog for the decoder config, gives the playout clock the audio-only
target latency, and plays the frames a frame reader delivers through `@caprid/moq-media`'s audio player.
Pass `echoCancellation: true` in Chromium when the page also captures a microphone. When the
catalog lists the interrupt track (`INTERRUPT_TRACK`), the listener subscribes to it too and cuts
the audio each interrupt names; `onInterrupt` hears about each one. When it lists the transcript
track (`TRANSCRIPT_TRACK`), the listener keeps the audio track's transcript as `listener.transcript`,
which interrupts cut too; `onTranscript` hears about each segment. `publishCanvas`'s `data` option
lists extra data tracks, such as that one, in the catalog it serves.

The listener joins quickly. The audio side builds while the handshake and the catalog are on their
way: `warmAudioPlayer(context)` loads the player's worklet module and libopus at once, so start it in
the click that connects, right after creating the context (a context created outside a gesture stays
suspended, and `listenAudio` also warms it, which costs nothing when the page already has). The
audio track is subscribed together with the catalog, by its conventional name, and the frames are held
until the player exists, then passed on in order. On relay-dev's benchmark this took the first audio
from 627 to 249 ms after the connect started ([spec](../../../spec/media-layer.md), "Joining sooner").

```ts
import { warmAudioPlayer } from '@caprid/moq-media';
import { connectRelaySubscriber } from '@caprid/relay';
import { listenAudio } from './listen-audio.js';

// In the user's click: create the context there, and start the player's parts loading before the connect.
const context = new AudioContext({ sampleRate: 48_000, latencyHint: 'interactive' });
warmAudioPlayer(context).catch(() => {});
const subscriber = await connectRelaySubscriber({ url, getToken: () => subscriberToken });
const listener = await listenAudio(subscriber, ['demo', 'live'], { context, onError: console.error });
console.log(listener.input.stats); // underruns, buffer depth and target, frames dropped as late
await listener.stop();
```

`mise run ts:test:audio-player` publishes this example's microphone and plays it back through
`listen-audio.ts` in headless Chromium, through an in-memory relay hub, or the Relay with
`AUDIO_PLAYER_RUNS=relay`, then interrupts it over the interrupt track.

## Audio and the shared timeline

Both tracks are stamped from one broadcast clock, in microseconds since the Unix epoch
([spec/media-layer.md](../../../spec/media-layer.md#shared-timeline-and-playout-clock)). Video frames
get `clock.now()` when they are captured. The microphone runs through an `AudioContext` and an
`AudioWorklet`, which works in Chromium, Firefox and Safari: no `MediaStreamTrackProcessor`. The
worklet cuts the input into 20 ms blocks stamped with context time. A source timeline maps them onto
the broadcast clock. It is calibrated on every block from the context's render clock
([`createRenderClock`](render-clock.ts) from `@caprid/moq-media`): the block that reached the main thread soonest after its
render, over the last second. The catalog lists the audio beside the video in one render group, and
every Opus frame is its own group.

- **Switching microphones.** `microphone.useStream(otherStream)` reconnects the context. The context
  resamples whatever rate the device has, and its count runs on, so the timeline does not move.
  `useStream(undefined)` captures silence.
- **Replacing the context.** `await microphone.useContext(newContext)` moves capture to it. The new
  context's time starts at zero, so it gets its own render clock, timeline and encoder. Close the old
  context yourself.
- **What the example does not trust.** It does not trust `getOutputTimestamp()`, because Firefox
  returns the render pair shifted back by the output delay, which would stamp audio 40–50 ms early.
  It does not trust the encoder's timestamps either. Chromium's Opus encoder counts samples from its
  first input, and Firefox's subtracts its lookahead. Instead, the example feeds the encoder one
  contiguous count and maps each chunk back to context time through the block it came from. Last, it
  does not trust the worklet's first `currentFrame`, because Chromium reports 0 on a new node's first
  call. A block restarts wherever the frame count jumps.
- **What no browser reports.** A microphone's own input latency reaches no browser API. The example
  estimates it from the device label with `estimateCaptureLatencyUs`: 190 ms for a label that names
  AirPods or Bluetooth, and 30 ms otherwise (measured on macOS,
  [spec/media-layer.md](../../../spec/media-layer.md#capture-latency)). It subtracts the estimate from
  every audio timestamp and asks again on every `useStream`. Pass `captureLatencyUs` to
  `captureMicrophone` with your own figures.

`mise run ts:test:av-sync` runs this example end to end and measures the A/V offset a player gets,
with device switches; see [spec/media-layer.md](../../../spec/media-layer.md#measured-in-browsers)
for the results. Firefox's H.264 encoder (148, Linux; 155 too) holds about 1.4 s of video, so a
player needs an audio buffer that deep to keep video from being dropped as late.

## How `publishCanvas` encodes and sends

The example requests length-prefixed AVC (`avc: { format: 'avc' }`) with a fixed encoder
configuration. It keeps the latest decoder configuration when later output callbacks omit it. It
starts serving the catalog once the first configuration arrives, and re-sends it on each catalog
subscription. Video groups begin with keyframes, and the configuration appears on keyframes and in
the catalog. A changed decoder configuration stops the example. To change it, start a fresh
publisher. Encoded output order and presentation timestamps are preserved.

The example encodes enough to obtain the initial configuration, then skips capture while the video
track has no demand. It also skips capture while two frames wait in the encoder or any encoded chunk
waits in the frame writer's backlog. It requests a keyframe at least every second of active capture.
Demand loss resets the group, and new or replacement demand requests a keyframe.

Encoder output goes straight to `createFrameWriter`, which serves writes in call order. When more
than eight chunks wait behind the one being written, as in a burst of internally buffered encoder
output, the writer drops them with the open group rather than queue deltas without bound. It resumes
at the next keyframe and asks the encoder for one.

A `timeline` track (msf-01 §7, announced in the catalog) maps each video group to the capture time of
its keyframe. Once a keyframe has opened a group, `createMediaTimeline` adds a record. The document
of every retained record goes out as a new group, and again on each new timeline subscription. A
keyframe skipped for lack of demand adds nothing.

Audio is encoded whether or not it has demand, and sent only with demand. A frame stamped before the
Unix epoch, possible only on a clock pinned near zero, is not sent.

An `ended` frame outcome, an encoder error, or cancellation closes the example's tracks and
encoders. An `interrupted` outcome does not: the writer drops that group and resumes at the next
keyframe. The caller
closes the connection and the microphone. The example has no automatic reconnection and no adaptive
bitrate policy. After the connection ends, for example after a network outage longer than about 5 s
in Chromium, nothing reconnects. See [publisher recovery](../../../spec/publisher-recovery.md).

## Use your own encoder

For your own encoder callback, import `videoTrackFromDecoderConfig`, `audioTrackFromDecoderConfig`,
and `chunkToLocFrame` from `@caprid/moq-media`. `chunkToLocFrame` accepts native video or audio
chunks, copies the encoded payload without changing its bytes, keeps microsecond timestamps, and adds
LOC properties. It performs no I/O or grouping. Keep the decoder configuration across callbacks, and
hand each frame to a `createFrameWriter` from `@caprid/relay/media`, as this example does. The writer
groups frames, skips them without demand, bounds the backlog, and says when the next frame must be a
keyframe. See [multi-object groups and relay demand](../../GUIDE.md#multi-object-groups-and-relay-demand).

To publish an existing MP4, use the optional [Mediabunny file example](../mediabunny/README.md) to
demux encoded packets, then pass them to the same media and transport layers.

References: [WebCodecs](https://www.w3.org/TR/webcodecs/),
[AVC configuration](https://www.w3.org/TR/webcodecs-avc-codec-registration/), and
[LOC draft-04](https://datatracker.ietf.org/doc/html/draft-ietf-moq-loc-04).
