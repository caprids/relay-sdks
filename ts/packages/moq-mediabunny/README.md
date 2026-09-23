# Mediabunny → LOC/MSF

Optional adapter for [Mediabunny](https://mediabunny.dev/) encoded packets and WebCodecs decoder
configs. The unpublished workspace package is `@mux/moq-mediabunny`; consumers install it alongside
`@mux/moq-media` and Mediabunny (tested with 1.58.1). Neither the core, Relay SDK, nor format package
requires Mediabunny.

The initial adapter supports length-prefixed AVC/H.264 with four-byte NAL lengths and Opus.
Mediabunny owns file parsing and encoding; this adapter maps the resulting metadata and packets.
It uses only public APIs and never rebuilds SPS/PPS, converts Annex B, decodes, or re-encodes media.
Other codecs can still use the generic format package directly.

```ts
import { packetToLocFrame, videoTrackFromDecoderConfig } from '@mux/moq-mediabunny';
import { buildMsfCatalog } from '@mux/moq-media';
import { BlobSource, EncodedPacketSink, Input, MP4 } from 'mediabunny';

const input = new Input({ source: new BlobSource(file), formats: [MP4] });
try {
  const track = await input.getPrimaryVideoTrack();
  const config = await track?.getDecoderConfig();
  if (!track || !config) throw new Error('Missing video track/config');
  const video = videoTrackFromDecoderConfig(config);
  const catalog = buildMsfCatalog({ namespace, video });
  // Serve catalog on the catalog track whenever relay demand changes to true.
  for await (const packet of new EncodedPacketSink(track).packets()) {
    const frame = packetToLocFrame(packet, video.initData ? { videoConfig: video.initData } : {});
    // frame.key starts a group; frame.payload and frame.properties go to group.write().
  }
} finally {
  input.dispose();
}
```

See the [complete publishing function](../../examples/mediabunny/publish-video.ts) and its
[usage instructions](../../examples/mediabunny/README.md) for demand, pacing, groups, and cleanup.

## Contract

- `videoTrackFromDecoderConfig(config)` returns a `VideoTrackInfo`. It requires AVC init data,
  keeps the `avc1`/`avc3` prefix, derives the codec suffix from avcC, and copies the exact description
  buffer/view range. Encoder-supplied High-profile records are supported without interpreting SPS.
  This is header inspection, not full bitstream validation.
- `audioTrackFromDecoderConfig(config)` returns an Opus `AudioTrackInfo`, with 48 kHz and the
  supplied channel count. Non-empty decoder description bytes are copied when present.
- `packetToLocFrame(packet, options?)` returns `{ payload, timestampUs, key, properties }`. Payload
  bytes are borrowed unchanged; keep them stable until the write completes. Metadata-only packets
  are rejected. Timestamps use Mediabunny's integer microsecond presentation timestamp, and input
  decode order must be preserved (do not sort B-frames by presentation time).
- Options include `timescale`, `videoConfig`, `audioConfig`, and `timestampOffsetUs`. Configs are
  attached only to key packets. The default timescale is 1,000,000; declare the same timescale on
  the track. Negative file timestamps need an explicit offset shared across audio/video; the
  resulting timestamp must fit the format package's nonnegative safe-integer contract.

For browser encoding, pass `onEncodedPacket` metadata's `decoderConfig` through the corresponding
track mapper. Keep the latest config when metadata is absent on later callbacks, and update the
catalog before publishing packets for a changed config. The application owns callback serialization,
bounded buffering, demand, and group recovery; the adapter performs no I/O.

Public upstream APIs: [encoded packets](https://mediabunny.dev/api/EncodedPacket),
[reading files](https://mediabunny.dev/guide/reading-media-files),
[media sources](https://mediabunny.dev/guide/media-sources),
[AVC packet contract](https://mediabunny.dev/codec-registry/avc).
