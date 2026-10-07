# Publish live video and audio with WebCodecs

The primary browser publishing path is capture → WebCodecs → LOC/MSF → MoQ over WebTransport.
LOC carries the encoder's chunk bytes directly: no MP4/WebM muxing and no Mediabunny dependency.

[`publish-canvas.ts`](publish-canvas.ts) samples a canvas at approximately 30 fps and publishes
AVC/H.264 with native `VideoEncoder` output. Given a microphone from
[`capture-microphone.ts`](capture-microphone.ts), it also publishes mono Opus on the same broadcast
timeline. It uses only `@mux/relay`, `@mux/moq-media`, and browser APIs. Copy the files into a
browser application whose bundler resolves those workspace packages. Run in a secure context with
WebCodecs, AVC encoding, and WebTransport support.

```ts
import { composeMoqSource, createBroadcastClock } from '@mux/moq-media';
import { connectRelayPublisher } from '@mux/relay';
import { captureMicrophone } from './capture-microphone.js';
import { publishCanvas } from './publish-canvas.js';

// A canvas your application draws into; keep its dimensions fixed during publishing.
const canvas = document.querySelector('canvas');
if (!canvas) throw new Error('Missing canvas');
canvas.width = 640;
canvas.height = 360;
const namespace = ['demo', 'live'];
const url = 'https://sjc.relay.mux.global';
const publisher = await connectRelayPublisher({ url, namespace, getToken: () => publisherToken });
const stop = new AbortController();
// Wire your stop button to stop.abort(). Capture/drawing remains owned by your application.
console.log(composeMoqSource(url, namespace)); // supply authorized subscriber credentials in the player
// Optional audio. Create the clock before capturing anything; create the context in a user gesture.
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

## Audio and the shared timeline

Both tracks are stamped from one broadcast clock
([spec/media-layer.md](../../../spec/media-layer.md#shared-timeline-and-playout-clock)). Video frames
get `clock.now()` when they are captured. The microphone runs through an `AudioContext` and an
`AudioWorklet`, which works in Chromium, Firefox and Safari: no `MediaStreamTrackProcessor`. The
worklet cuts the input into 20 ms blocks stamped with context time. A source timeline maps them onto
the broadcast clock. It is calibrated on every block from the context's render clock
([`render-clock.ts`](render-clock.ts)): the block that reached the main thread soonest after its
render, over the last second. The catalog lists the audio beside the video in one render group, and
every Opus frame is its own group.

- **Switching microphones.** `microphone.useStream(otherStream)` reconnects the context. The context
  resamples whatever rate the device has, and its count runs on, so the timeline does not move.
  `useStream(undefined)` captures silence.
- **Replacing the context.** `await microphone.useContext(newContext)` moves capture to it. The new
  context's time starts at zero, so it gets its own render clock, timeline and encoder. Close the old
  context yourself.
- **What the example does not trust.** Not `getOutputTimestamp()`: Firefox returns the render pair
  shifted back by the output delay, which would stamp audio 40–50 ms early. Not the encoder's
  timestamps: Chromium's Opus encoder counts samples from its first input, and Firefox's subtracts
  its lookahead. The example feeds the encoder one contiguous count and maps each chunk back to
  context time through the block it came from. Not the worklet's first `currentFrame`: Chromium
  reports 0 on a new node's first call, so a block restarts wherever the frame count jumps.
- **What nobody can see.** A microphone's own input latency, such as Bluetooth's 100–200 ms, reaches
  no browser API. Audio from such a device is stamped late by that much
  ([#290](https://github.com/muxinc/relay-sdks/issues/290)).

`mise run ts:test:av-sync` runs this example end to end and measures the A/V offset a player gets,
with device switches; see [spec/media-layer.md](../../../spec/media-layer.md#measured-in-browsers)
for the results. Firefox's H.264 encoder (148, Linux) holds about 1.4 s of video, so a player needs
an audio buffer that deep to keep video from being dropped as late.

The function keeps the latest decoder configuration when subsequent output callbacks omit it. It
starts serving the catalog after the first configuration arrives and re-sends it on each catalog
subscription. Video groups begin with keyframes; configuration appears on keyframes and in the
catalog. It encodes enough to obtain the initial configuration, then skips capture without video demand.
Demand loss resets the group; new or replacement demand requests a keyframe.
Encoded output order and presentation timestamps are preserved.

The example requests length-prefixed AVC (`avc: { format: 'avc' }`) with a fixed encoder
configuration. A changed decoder configuration stops the example; start a fresh publisher/session
to change it. It requests keyframes at least every two seconds of active capture. It skips capture
when the encoder input queue is busy or two encoded chunks await transport, serializes output
writes, and keeps an eight-chunk hard cap for bursts of internally buffered encoder output. It does
not silently drop encoded deltas to relieve transport backpressure.
Subscription changes reset video to the next keyframe and re-serve the catalog on replacement
demand. Audio is encoded whether or not it has demand, and sent only with it; a frame stamped before
the broadcast's origin is not sent. Other write errors, encoder errors, and cancellation close its
tracks and encoders; the caller closes the connection and the microphone. There is no automatic
reconnection or adaptive bitrate policy in this example. After the
connection ends, for example after a network outage longer than about 5 s in Chromium, nothing
reconnects; see [publisher recovery](../../../spec/publisher-recovery.md).

For your own encoder callback, import `videoTrackFromDecoderConfig`, `audioTrackFromDecoderConfig`,
and `chunkToLocFrame` from `@mux/moq-media`. `chunkToLocFrame` accepts native video or audio chunks,
copies the encoded payload without changing its bytes, preserves microsecond timestamps, and adds
LOC properties. Retain decoder configuration across callbacks; await/serialize group writes and
bound callback buffering, as this example does. The helper itself performs no I/O or grouping.

For publishing an existing MP4, use the optional [Mediabunny file example](../mediabunny/README.md)
to demux encoded packets before passing them to the same media/transport layers.

References: [WebCodecs](https://www.w3.org/TR/webcodecs/),
[AVC configuration](https://www.w3.org/TR/webcodecs-avc-codec-registration/), and
[LOC draft-04](https://datatracker.ietf.org/doc/html/draft-ietf-moq-loc-04).
