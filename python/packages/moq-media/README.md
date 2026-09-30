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

`parse_msf_catalog` reads an independent catalog object's payload back into an `MsfCatalog` of
`MsfCatalogTrack` descriptions: namespace tuple, name, packaging, live status, role, codec,
dimensions, rates, bitrate, and init data resolved from `initRef` to bytes. It raises `ValueError`
on unsupported versions and delta updates, ignores unknown fields, and returns non-`loc` packagings
(such as moq-rs `cmaf`) as declared.

Shared vectors live in `spec/vectors/media.json`. See `UPSTREAM.json` for Video.js provenance.
Publishing handles and the object-to-frame reader are later stages of `spec/media-layer.md`.

```python
from moq_media import (
    AudioTrackInfo,
    VideoTrackInfo,
    build_msf_catalog,
    loc_properties,
    loc_track_properties,
    parse_msf_catalog,
)
from moq_media.h264 import H264Config

h264 = H264Config.from_annexb(encoder.extradata)
catalog = build_msf_catalog(
    ["room", "live"],
    video=VideoTrackInfo(codec=h264.codec, width=640, height=360, init_data=h264.avcc),
    audio=AudioTrackInfo(),
)
video = publisher.register_track("video", track_properties=loc_track_properties())
# The application opens a group at each keyframe and handles demand/recovery.
await group.write(h264.to_avcc(access_unit), properties=loc_properties(timestamp_us, video_config=h264.avcc))

tracks = parse_msf_catalog(catalog_object.payload, catalog_namespace=["room", "live"]).tracks
```
