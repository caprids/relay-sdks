# Optional file input: publish an MP4's video with Mediabunny

For live browser capture, start with the [direct WebCodecs example](../webcodecs/README.md).

[publish-video.ts](publish-video.ts) is a type-checked browser integration example. It reads a local
MP4 with Mediabunny, takes its AVC decoder configuration and encoded packets, and publishes a paced
LOC video track plus an independent MSF catalog. It does not decode or re-encode the file.

The packets go through `@mux/relay/media`, as in the [WebCodecs example](../webcodecs/README.md): a
frame writer maps them onto groups (a group per keyframe), and a catalog service serves the catalog
on every relay subscription. The example keeps only its own pacing. Each write is awaited before the
next packet is read, because file playback has no encoder callback to keep up with, so the frame
writer's backlog never fills. Only a track that ends stops the example.

After the [workspace setup](../../../README.md#development), run `mise run ts:build`. Import the function
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

This example does not reconnect after the connection ends, for example after a network outage
longer than about 5 s in Chromium. See [publisher recovery](../../../spec/publisher-recovery.md)
for what a replacement publisher can and cannot restore.

The file must contain AVC video with decoder configuration and four-byte NAL lengths. The example
publishes its primary video track once and ignores audio. Timestamps must be nonnegative. Each
keyframe opens a group; deltas append to it. Packets stay in decode order and are paced by their
durations; LOC properties retain presentation timestamps, including B-frame reordering. Catalog
objects carry no LOC properties.

Pacing starts when the relay first subscribes to the video track, so the first viewer sees the file
from its start; until then the example waits, for as long as it takes. After that the clock keeps
moving: a packet the relay has no subscription for is skipped at once, keyframes included, and
publication resumes at the next keyframe once demand returns. A write that fails while the track
stays open costs the rest of its group, and publication resumes at the next keyframe too. The catalog
service re-sends the catalog on every relay subscription and retries a write that fails while the
catalog track stays open, after a one-second pause, for as long as demand remains.

When the file runs out, the example ends the broadcast as msf-01 §11.3 describes: the frame writer
FINs the last group, the video track closes (PUBLISH_DONE Track Ended), and the catalog service is
stopped, cancelling any catalog write still in progress. Then, if the catalog track has demand, it
writes the end-of-broadcast catalog (`buildMsfEndCatalog`: `isComplete: true`, no tracks) and closes
the catalog track. A failure of that last write fails the run. A failed or aborted run never sends
the end-of-broadcast catalog.

A track that ends under the example (closed, or its connection gone) fails the run, as does a catalog
failure that is not a `RelayError`. Abort closes the tracks, cancels pacing, and immediately disposes
the file input, including during metadata and packet reads. It does not close the caller's
connection.

The reusable adapter also supports Opus and explicit shared timestamp offsets. This example keeps
multi-track scheduling, codec changes, and live encoder queues out of its scope; those are application
policy and the later media-publishing stage in [the proposal](../../../spec/media-layer.md).
