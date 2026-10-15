# Optional file input: Mediabunny → LOC/MSF

Optional adapter for encoded packets demuxed by [Mediabunny](https://mediabunny.dev/). The unpublished
workspace package is `@mux/moq-mediabunny`; consumers install it alongside
`@mux/moq-media` and Mediabunny (tested with 1.58.1). Neither the core, Relay SDK, nor format package
requires Mediabunny.

The initial adapter supports length-prefixed AVC/H.264 with four-byte NAL lengths and Opus.
Mediabunny owns file parsing; this adapter maps its encoded packets. Standard WebCodecs decoder
config mapping lives in `@mux/moq-media`, which also provides the
[direct browser publishing path](../../examples/webcodecs/README.md).
It uses only public APIs and never rebuilds SPS/PPS, converts Annex B, decodes, or re-encodes media.
Other codecs can still use the generic format package directly.

```ts
import { packetToLocFrame } from '@mux/moq-mediabunny';
import { buildMsfCatalog, videoTrackFromDecoderConfig } from '@mux/moq-media';
import { BlobSource, EncodedPacketSink, Input, MP4 } from 'mediabunny';

const input = new Input({ source: new BlobSource(file), formats: [MP4] });
try {
  const track = await input.getPrimaryVideoTrack();
  const config = await track?.getDecoderConfig();
  if (!track || !config) throw new Error('Missing video track/config');
  const video = videoTrackFromDecoderConfig(config);
  const catalog = buildMsfCatalog({ namespace, video });
  // Serve catalog with createCatalogService from @mux/relay/media.
  for await (const packet of new EncodedPacketSink(track).packets()) {
    const frame = packetToLocFrame(packet, video.initData ? { videoConfig: video.initData } : {});
    // Hand frame to a frame writer from @mux/relay/media: frame.key starts a group, deltas join it.
  }
} finally {
  input.dispose();
}
```

See the [complete publishing function](../../examples/mediabunny/publish-video.ts) and its
[usage instructions](../../examples/mediabunny/README.md) for demand, pacing, groups, and cleanup.

## Contract

The config mappers and `LocFrame` type belong to `@mux/moq-media`. This adapter retains their
original exports for compatibility; new code should import them from the media package.

See the [media package](../moq-media/README.md) for the AVC/Opus config-mapper contract.

- `packetToLocFrame(packet, options?)` returns `{ payload, timestampUs, key, properties }`. Payload
  bytes are borrowed unchanged; keep them stable until the write completes. Metadata-only packets
  are rejected. Timestamps use Mediabunny's integer microsecond presentation timestamp, and input
  decode order must be preserved (do not sort B-frames by presentation time).
- Options include `timescale`, `videoConfig`, `audioConfig`, and `timestampOffsetUs`. Configs are
  attached only to key packets. The default timescale is 1,000,000; declare the same timescale on
  the track. Negative file timestamps need an explicit offset shared across audio/video; the
  resulting timestamp must fit the format package's nonnegative safe-integer contract.

For live browser encoding, use `chunkToLocFrame` and the config mappers from `@mux/moq-media`
directly. No conversion to a Mediabunny packet is needed.

Public upstream APIs: [encoded packets](https://mediabunny.dev/api/EncodedPacket),
[reading files](https://mediabunny.dev/guide/reading-media-files),
[media sources](https://mediabunny.dev/guide/media-sources),
[AVC packet contract](https://mediabunny.dev/codec-registry/avc).

## Stability

Before 1.0, a minor version can break the API. This package releases together with `@mux/relay`
at the same version; see the [changelog](../../CHANGELOG.md).
