# Optional file input: Mediabunny to LOC and MSF

`@caprid/moq-mediabunny` turns encoded packets that [Mediabunny](https://mediabunny.dev/) demuxes from a file into LOC
frames. Install it alongside `@caprid/moq-media` and Mediabunny 1.58.1 or later. Neither `@caprid/moq-core`,
`@caprid/relay`, nor `@caprid/moq-media` needs Mediabunny.

Mediabunny parses the file, and this adapter maps its encoded packets. The decoder config mappers in
`@caprid/moq-media` support length-prefixed AVC (H.264) with four-byte NAL lengths, and Opus. The adapter uses only
public Mediabunny APIs. It never rebuilds SPS or PPS, converts Annex B, decodes, or re-encodes media. For other codecs,
use `@caprid/moq-media` directly. For live capture, `@caprid/moq-media` also provides the
[direct WebCodecs publishing path](https://github.com/caprids/relay-sdks/blob/main/ts/examples/webcodecs/README.md).

```ts
import { packetToLocFrame } from '@caprid/moq-mediabunny';
import { buildMsfCatalog, videoTrackFromDecoderConfig } from '@caprid/moq-media';
import { BlobSource, EncodedPacketSink, Input, MP4 } from 'mediabunny';

const input = new Input({ source: new BlobSource(file), formats: [MP4] });
try {
  const track = await input.getPrimaryVideoTrack();
  const config = await track?.getDecoderConfig();
  if (!track || !config) throw new Error('Missing video track or decoder config');
  const video = videoTrackFromDecoderConfig(config);
  const catalog = buildMsfCatalog({ namespace, video });
  // Serve catalog with createCatalogService from @caprid/relay/media.
  for await (const packet of new EncodedPacketSink(track).packets()) {
    const frame = packetToLocFrame(packet, video.initData ? { videoConfig: video.initData } : {});
    // Hand frame to a frame writer from @caprid/relay/media: frame.key starts a group, deltas join it.
  }
} finally {
  input.dispose();
}
```

See the [complete publishing function](https://github.com/caprids/relay-sdks/blob/main/ts/examples/mediabunny/publish-video.ts) and its
[usage instructions](https://github.com/caprids/relay-sdks/blob/main/ts/examples/mediabunny/README.md) for demand, pacing, groups, and cleanup.

## Contract

The config mappers and the `LocFrame` type belong to `@caprid/moq-media`. This package still re-exports
`videoTrackFromDecoderConfig`, `audioTrackFromDecoderConfig`, and `LocFrame` for compatibility. Import them from
`@caprid/moq-media` in new code.

See the [media package](https://github.com/caprids/relay-sdks/blob/main/ts/packages/moq-media/README.md) for the AVC/Opus config-mapper contract.

- `packetToLocFrame(packet, options?)` returns `{ payload, timestampUs, key, properties }`. The payload borrows the
  packet's bytes unchanged, so keep them stable until the write completes. A metadata-only packet throws. The timestamp
  is Mediabunny's integer microsecond presentation timestamp. Keep packets in decode order, and do not sort B-frames by
  presentation time.
- The options are `timescale`, `videoConfig`, `audioConfig`, and `timestampOffsetUs`. Configs go on key packets only.
  The default timescale is 1,000,000. Declare the same timescale on the track. A file with negative timestamps needs an
  explicit `timestampOffsetUs`, shared across the audio and video tracks. The resulting timestamp must be a nonnegative
  safe integer.

For live browser encoding, use `chunkToLocFrame` and the config mappers from `@caprid/moq-media` directly, with no
Mediabunny packet in between.

Public upstream APIs: [encoded packets](https://mediabunny.dev/api/EncodedPacket),
[reading files](https://mediabunny.dev/guide/reading-media-files),
[media sources](https://mediabunny.dev/guide/media-sources),
[AVC packet contract](https://mediabunny.dev/codec-registry/avc).

## Stability

Before 1.0, a minor version can break the API. This package releases together with `@caprid/relay` at the same version.
See the [changelog](https://github.com/caprids/relay-sdks/blob/main/ts/CHANGELOG.md).
