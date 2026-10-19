# MoQ media formats

Pure LOC (`draft-ietf-moq-loc-04`) and independent MSF (`draft-ietf-moq-msf-01`) format helpers.
Depends only on the generic MoQ core. No transport libraries or Relay connection required. The one
encoder, `moq_media.opus`, needs the `opus` extra (see [Opus](#opus)); `import moq_media` never loads it.

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

`build_msf_catalog` describes a live broadcast and leaves `isComplete` out: msf-01 §5.1.3 forbids
the field while false, and once a catalog carries it, it can never be removed. When a broadcast
ends on purpose, close the media tracks first (each close tells subscribers the track ended), then
write `build_msf_end_catalog(generated_at_ms=...)` to the catalog track if it has demand, then
close it (msf-01 §11.3). Do not send it after a crash or a lost connection.

`parse_msf_catalog` reads an independent catalog object's payload back into an `MsfCatalog` of
`MsfCatalogTrack` descriptions: namespace tuple, name, packaging, live status, role, codec,
dimensions, rates, bitrate, render group, and init data resolved from `initRef` to bytes. It raises `ValueError`
on unsupported versions and delta updates, ignores unknown fields, and returns non-`loc` packagings
(such as moq-rs `cmaf`) as declared.

Every media track shares one timeline: LOC timestamps are capture times in microseconds since the
Unix epoch, with Timescale 1,000,000 declared on each track. `BroadcastClock()` pins a moment on the
monotonic clock to the system clock (`origin_us`, `origin_epoch_us`) and counts from there. Use
`from_source_us` for capture timestamps on that clock. Use `source_timeline()` for a clock with
its own origin and rate, kept in line with `calibrate(source_us, at_us=...)` pairs. No browser or OS
reports a microphone's input delay, so pass the one you know as `capture_latency_us=` and set
`timeline.capture_latency_us` again when the device changes (about 30 ms for a laptop microphone,
190 ms for AirPods). Use
`sample_timeline(48_000)` for a sample counter with nothing to calibrate against, such as a file. `PlayoutClock()` is the player side. Audio is the master: report what is
audible with `report_audio(timestamp_us)`, and `due(timestamp_us)` returns a `PlayoutDecision` to
wait, show, or drop a video frame. With no audio it paces by wall clock. See
[spec/media-layer.md](../../../spec/media-layer.md#shared-timeline-and-playout-clock).

On the subscriber side, `loc_timestamp_us(obj.properties, track_properties=ok.track_properties)`
reads an object's Timestamp in microseconds, applying whichever Timescale the object or the
`SUBSCRIBE_OK` track properties declare.

msf-01's Media Timeline track holds one record per video group, mapping media time and the group's
location to wall-clock time, for seeking and playlists. On the epoch timeline the two times are the
same, so an entry needs only the timestamp and the group. Announce it with
`media_timeline=MediaTimelineTrackInfo()` in `build_msf_catalog`. `MediaTimeline` keeps one
broadcast's records, 512 by default, and returns the document to send each time a keyframe has
opened a video group; `mux_relay.media.DocumentService` sends it as a new timeline group, and again
whenever the relay subscribes. See
[spec/media-layer.md](../../../spec/media-layer.md#media-timeline-track).

```python
timeline = MediaTimeline()
service = DocumentService(timeline_track)  # from mux_relay.media; run service.serve() as a task
# Once the frame writer reports the keyframe that opened video group `group_id` as sent:
service.update(timeline.add(timestamp_us, group_id))
read = parse_media_timeline(timeline_object.payload)
```

Shared vectors live in `spec/vectors/media.json` and `spec/vectors/clock.json`. See `UPSTREAM.json` for Video.js provenance.
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

## Opus

`pip install "mux-moq-media[opus]"` adds PyAV and `moq_media.opus`: Opus encoding and decoding for a
server-side talker such as a voice bot. `OpusEncoder` takes interleaved s16 PCM (or a PyAV
`AudioFrame`) at any rate and returns 20 ms `OpusPacket`s stamped on the broadcast timeline. Its
defaults are for voice: mono, 32 kb/s, libopus's `voip` application. Audio comes in runs. The first
`encode` of a run gives the timestamp of its first sample, later calls continue by sample count, and
`flush()` ends the run (an utterance) by padding the last partial frame with silence.
`OpusDecoder(rate=16_000)` turns packets back into s16 PCM at the rate speech recognition wants. Call
`reset()` at a frame's `discontinuity`.

```python
from moq_media import AudioTrackInfo
from moq_media.opus import OpusDecoder, OpusEncoder

encoder = OpusEncoder(input_rate=24_000)  # the TTS engine's rate
audio_info = AudioTrackInfo(channel_config="1", bitrate=encoder.bitrate)  # for build_msf_catalog
# An OpusPacket carries its LOC properties, so mux_relay.media.FrameWriter takes it as a frame.
for packet in encoder.encode(tts_pcm, timestamp_us=clock.now_us()) + encoder.flush():
    await frame_writer.write(packet)

decoder = OpusDecoder(rate=16_000)
async for frame in frame_reader:
    if frame.discontinuity:
        decoder.reset()
    stt.feed(decoder.decode(frame.payload))
```

Packet *k* of a run is stamped at the run's timestamp plus 20 ms per packet before it, as WebCodecs
stamps its output; decoded audio trails the stamps by libopus's 6.5 ms look-ahead. A flushed run has
one packet more than the frames it was given, which carries the look-ahead's tail.
`opus_packet_duration_us(payload)` reads a packet's duration from its TOC byte without PyAV, held to
the same vectors as TypeScript's `opusPacketDurationUs`. See [ADR 0006](../../../docs/adr/0006-opus-codec-behind-an-extra.md).

The distribution on PyPI is `mux-moq-media`; the import name is `moq_media`.

## Stability

Before 1.0, a minor version can break the API. This package releases together with `mux-relay` at
the same version; see the [changelog](../../CHANGELOG.md).
