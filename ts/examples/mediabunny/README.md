# Optional file input: publish an MP4's video with Mediabunny

For live browser capture, start with the [direct WebCodecs example](../webcodecs/README.md).

[publish-video.ts](publish-video.ts) is a type-checked browser integration example. It reads a local
MP4 with Mediabunny, takes its AVC decoder configuration and encoded packets, and publishes a paced
LOC video track plus an independent MSF catalog. It does not decode or re-encode the file.

After the [workspace setup](../../README.md#development), run `mise run ts:build`. Import the function
into a browser application built with a TypeScript-capable bundler, alongside the workspace packages
`@mux/relay`, `@mux/moq-media`, `@mux/moq-mediabunny`, and `mediabunny`:

```ts
import { connectRelayPublisher } from '@mux/relay';
import { composeMoqSource } from '@mux/moq-media';
import { publishVideoFile } from './publish-video.js';

// file: a File from <input type="file">; namespace: authorized string[]; getToken: your auth callback.
const publisher = await connectRelayPublisher({ url, namespace, getToken });
const stop = new AbortController(); // Wire a Stop button to stop.abort().
try {
  const source = composeMoqSource(url, namespace);
  // Open source in a LOC/MSF-capable player, supplying its authorized subscriber token separately.
  await publishVideoFile(file, publisher, namespace, { signal: stop.signal });
} finally {
  await publisher.close();
}
```

Use a browser with WebTransport support on localhost or HTTPS and a fresh authorized namespace.
The caller owns credentials and connection shutdown. This example contains no UI, server, or player;
see the [existing browser launcher](../mux-global/README.md) for the repository's WebTransport setup.

The file must contain AVC video with decoder configuration and four-byte NAL lengths. The example
publishes its primary video track once and ignores audio. Timestamps must be nonnegative. Each
keyframe opens a group; deltas append to it. Packets stay in decode order and are paced by their
durations; LOC properties retain presentation timestamps, including B-frame reordering. It waits
for initial demand (the SDK's default 10-second
write deadline applies), starts pacing when the first keyframe can be sent, and re-sends the catalog
on every relay subscription. At EOF it stops accepting catalog demand and drains any active catalog
write before closing that track; genuine catalog failures still fail the run. After loss of demand
it keeps pacing and skips packets, including keyframes, until demand returns;
publication resumes at the next keyframe. Writes are
serial and any write failure stops the run. Demand can disappear between a notification and a
write, so even an EOF catalog drain can wait for the SDK write deadline and fail; this remains a
reported write failure. There is no reconnect or retry policy. Abort closes the
tracks, cancels pacing, and immediately disposes the file input, including during metadata and packet
reads. It does not close the caller's connection.

The reusable adapter also supports Opus and explicit shared timestamp offsets. This example keeps
multi-track scheduling, codec changes, and live encoder queues out of its scope; those are application
policy and the later media-publishing stage in [the proposal](../../../spec/media-layer.md).
