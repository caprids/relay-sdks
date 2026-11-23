# Browser audio and video

With `@caprid/relay/media` and `@caprid/moq-media`, a browser page can publish and play live video and audio using WebCodecs and headless players—no Video.js, media server, or WebRTC required.

```sh
npm install @caprid/relay @caprid/moq-media
```

For existing media files rather than live camera/mic capture, add [`@caprid/moq-mediabunny`](../../ts/packages/moq-mediabunny/README.md).

## Publishing media with WebCodecs

A media broadcast consists of:
1. A **catalog track** describing available tracks and their decoder configurations (MSF format).
2. One or more **media tracks** (`video`, `audio`, or alternate renditions) carrying LOC-framed encoded chunks.

Use `createCatalogService` to re-send the catalog whenever relay demand starts, and `createFrameWriter` to map encoded frames onto MOQT groups (one group per GOP for video; one group per frame with `grouping: 'frame'` for audio).

```ts
import { buildMsfCatalog, chunkToLocFrame, locTrackProperties } from '@caprid/moq-media';
import { connectSupervisedPublisher } from '@caprid/relay';
import { createCatalogService, createFrameWriter, mediaTrackProperties } from '@caprid/relay/media';

const publisher = await connectSupervisedPublisher({ namespace, getToken });
const catalogTrack = publisher.registerTrack({ track: 'catalog' });
const videoTrack = publisher.registerTrack({
  track: 'video',
  trackProperties: [...locTrackProperties(), ...mediaTrackProperties('video')],
});

let forceKeyframe = false;
const writer = createFrameWriter(videoTrack, {
  onKeyframeNeeded: () => {
    forceKeyframe = true;
  },
});

// Inside your VideoEncoder output callback:
void writer.write(chunkToLocFrame(chunk, { videoConfig })).then((outcome) => {
  if (outcome.kind === 'ended') stop(outcome.error);
});
```

### Avoid the catalog-demand deadlock

Viewers subscribe to the catalog first, read the track list, and only then subscribe to `video`—which is what creates demand on `video`. If your catalog waits for the encoder's first decoder config, and your encoder waits for `videoTrack.hasDemand`, neither ever starts.

Either build the catalog up front from known encoder settings, or encode unconditionally until the first decoder config arrives and publish the catalog before gating on demand:

```ts
// Encode until the catalog has been published, then encode only while someone is watching.
if (!catalogPublished || videoTrack.hasDemand) {
  encoder.encode(frame, { keyFrame: forceKeyframe });
  forceKeyframe = false;
}
```

`writer.write(frame)` resolves to an outcome (`'sent'`, `'skipped'`, `'stale'`, `'dropped'`, `'interrupted'`, or `'ended'`) rather than rejecting on transient delivery issues, so you can call it from a WebCodecs output callback without awaiting. When more than `maxBacklog` frames wait behind the active write (default `8` for keyframe grouping; unbounded by default for `'frame'` grouping), the writer drops waiting frames and calls `onKeyframeNeeded`.

See the complete [WebCodecs example](../../ts/examples/webcodecs/README.md) for camera, microphone, and catalog wiring.

## Subscribing and sequencing frames

Objects on different group streams can arrive out of order after packet loss. Combine three pieces per broadcast:

- **`createPlayoutClock()`** (`@caprid/moq-media`): shared timeline across all tracks of a broadcast. Audio is the master clock; without audio, it paces by wall clock. For audio-only broadcasts, pass `{ targetLatencyUs: AUDIO_ONLY_TARGET_LATENCY_US }` (500 ms) so a delay increase resyncs in 500 ms instead of the 2 s video default.
- **`createFrameSequencer({ clock })`** (`@caprid/moq-media`): reorders objects into decode order and drops late frames against `clock`. Use `grouping: 'frame'` for audio.
- **`createFrameReader(sequencer, { onFrame })`** (`@caprid/relay/media`): returns subscription handlers you spread straight into `subscriber.want(...)` or `subscriber.subscribe(...)` (keep `drainMs` at `0`).

Always pass `mediaSubscribeParameters('audio' | 'video')` on media subscriptions so the relay prioritizes audio (`16`) ahead of video (`96`) on a congested downlink.

```ts
import { createFrameSequencer, createPlayoutClock } from '@caprid/moq-media';
import { createFrameReader, mediaSubscribeParameters } from '@caprid/relay/media';

const clock = createPlayoutClock();

const videoReader = createFrameReader(createFrameSequencer({ clock }), {
  onFrame: videoPlayer.push,
});
const audioReader = createFrameReader(createFrameSequencer({ clock, grouping: 'frame' }), {
  onFrame: audioInput.push,
});

subscriber.want(
  'video',
  { namespace, track: 'video', generations: true, parameters: mediaSubscribeParameters('video') },
  { ...videoReader, onError },
);
subscriber.want(
  'audio',
  { namespace, track: 'audio', generations: true, parameters: mediaSubscribeParameters('audio') },
  { ...audioReader, onError },
);
```

## Playing audio

`createAudioPlayer({ context })` decodes Opus frames in an `AudioWorklet` with an adaptive jitter buffer, libopus WebAssembly loss concealment (and in-band FEC recovery when the publisher enables `opus: { useinbandfec: true, packetlossperc: 10 }`), and automatic `clock.reportAudio(...)` updates.

Create one `AudioPlayer` per `AudioContext`, and add one `input` per speaker:

```ts
import {
  AUDIO_ONLY_TARGET_LATENCY_US,
  audioDecoderConfigFromTrack,
  createAudioPlayer,
  createFrameHold,
  createFrameSequencer,
  createPlayoutClock,
  type SequencedFrame,
  warmAudioPlayer,
} from '@caprid/moq-media';
import { createFrameReader, mediaSubscribeParameters } from '@caprid/relay/media';

// Inside the user's click handler (an AudioContext created outside a gesture stays suspended):
const context = new AudioContext({ sampleRate: 48_000, latencyHint: 'interactive' });
const warm = warmAudioPlayer(context); // preloads AudioWorklet + libopus Wasm during connect
warm.catch(() => {});

const clock = createPlayoutClock({ targetLatencyUs: AUDIO_ONLY_TARGET_LATENCY_US });
const hold = createFrameHold<SequencedFrame>();
const reader = createFrameReader(createFrameSequencer({ clock, grouping: 'frame' }), {
  onFrame: hold.push,
});

// Subscribe to audio immediately in parallel with fetching the catalog:
subscriber.want(
  'audio',
  { namespace, track: 'audio', generations: true, parameters: mediaSubscribeParameters('audio') },
  { ...reader, onError },
);
const audioTrack = await readCatalogAudioTrack(subscriber);

await warm;
const player = await createAudioPlayer({ context });
player.output.connect(context.destination);

const input = player.addInput({
  clock,
  decoderConfig: audioDecoderConfigFromTrack(audioTrack),
  // Raise the jitter floor (default 20 ms) if main-thread stalls or network jitter cause frequent underruns:
  // minJitterTargetUs: 80_000,
});
hold.release(input.push);
```

Check your [Content Security Policy](connecting.md#content-security-policy): `script-src` needs `blob:` (for the worklet) and `'wasm-unsafe-eval'` (for libopus Wasm). Without `'wasm-unsafe-eval'`, the player silently falls back to WebCodecs without loss concealment (`player.joinPhases.opusLoadedUs === null`).

## Playing video

`createVideoPlayer({ clock, decoderConfig, render })` decodes video frames with `VideoDecoder` and calls `render(frame)` when `clock.due(...)` says each frame is due. Pass `canvasRenderer(canvas)` for an `HTMLCanvasElement` or `OffscreenCanvas`:

```ts
import {
  canvasRenderer,
  createFrameSequencer,
  createPlayoutClock,
  createVideoPlayer,
  videoDecoderConfigFromTrack,
} from '@caprid/moq-media';
import { createFrameReader, mediaSubscribeParameters } from '@caprid/relay/media';

const player = createVideoPlayer({
  clock, // shared with audioInput for A/V sync
  decoderConfig: videoDecoderConfigFromTrack(videoTrack),
  render: canvasRenderer(canvas),
});

const reader = createFrameReader(createFrameSequencer({ clock }), { onFrame: player.push });
subscriber.want(
  'video',
  { namespace, track: 'video', generations: true, parameters: mediaSubscribeParameters('video') },
  { ...reader, onRestarted: () => player.reset(), onError },
);
```

- **Frame ownership**: `render` borrows the `VideoFrame`; the player closes it immediately when `render` returns. If you keep a frame, call `frame.clone()` and close the clone yourself.
- **Switching renditions**: call `player.configure(newDecoderConfig)` before the new track's cutover keyframe arrives (see [Switching tracks & layers](switching-tracks.md)).
- **Dedicated workers**: `createVideoPlayer` uses only WebCodecs and timers, so it runs inside a Web Worker rendering to a transferred `OffscreenCanvas` (using its own wall-clock `PlayoutClock`).

See [Media layer specification](../../spec/media-layer.md) for the exact sequencer, audio player, and video player contracts.
