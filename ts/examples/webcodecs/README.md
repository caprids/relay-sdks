# Publish live video with WebCodecs

The primary browser publishing path is capture → WebCodecs → LOC/MSF → MoQ over WebTransport.
LOC carries the encoder's chunk bytes directly: no MP4/WebM muxing and no Mediabunny dependency.

[`publish-canvas.ts`](publish-canvas.ts) samples a canvas at approximately 30 fps and publishes
AVC/H.264 with native `VideoEncoder` output. It uses only `@mux/relay`, `@mux/moq-media`, and browser
APIs. Copy the function into a browser application whose bundler resolves those workspace packages.
Run in a secure context with WebCodecs, AVC encoding, and WebTransport support.

```ts
import { composeMoqSource } from '@mux/moq-media';
import { connectRelayPublisher } from '@mux/relay';
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
try {
  await publishCanvas(canvas, publisher, namespace, { signal: stop.signal });
} catch (error) {
  if (!stop.signal.aborted) throw error;
} finally {
  await publisher.close();
}
```

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

This example does not reconnect after the connection ends, for example after a network outage longer than about 5 s in Chromium. See [publisher recovery](../../../spec/publisher-recovery.md) for what a replacement publisher can and cannot restore.
Subscription changes reset video to the next keyframe and re-serve the catalog on replacement
demand. Other write errors, encoder errors, and cancellation close its tracks and encoder; the caller
closes the connection. There is no audio, automatic reconnection, or adaptive bitrate policy in this example.

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
