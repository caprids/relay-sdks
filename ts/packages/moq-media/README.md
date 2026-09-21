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

H.264 helpers split Annex B, build avcC from SPS/PPS, and convert access units to four-byte NAL
length prefixes. Conversion removes SPS, PPS, and AUD by default, as `avc1` requires. Codec-string
inspection checks the avcC version/header; it does not validate SPS semantics or decode a bitstream.
Construction from SPS/PPS supports Baseline, Main, and Extended profiles. Other profiles require
extension fields and are rejected; callers can still supply an encoder-generated High-profile avcC
to the catalog builder.

Shared vectors live in `spec/vectors/media.json`. See `UPSTREAM.json` for Video.js provenance.
Publishing handles and public readers are later stages of `spec/media-layer.md`.

```ts
import { buildMsfCatalog, composeMoqSource, locProperties, locTrackProperties } from '@mux/moq-media';

const catalog = buildMsfCatalog({
  namespace: ['room', 'live'],
  video: { codec: 'avc1.42E01F', width: 640, height: 360, initData: avcc },
  audio: { codec: 'opus' },
});
const video = publisher.registerTrack({ track: 'video', trackProperties: locTrackProperties() });
await group.write(encodedFrame, { properties: locProperties(timestampUs, { videoConfig: avcc }) });
const source = composeMoqSource('https://relay.example', ['room', 'live'], { token });
```

The internal `parse-catalog.ts` extraction is used to check builder compatibility with Video.js;
it is not exported as a public reader API.
