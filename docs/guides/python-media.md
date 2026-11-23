# Python media and bots

Python services—such as live video ingest pipelines, media bridges, and conversational AI voice bots—can publish and subscribe to LOC/MSF media tracks using `caprid-relay` and `caprid-moq-media`.

```sh
uv add caprid-relay caprid-moq-media
# For Opus encoding/decoding (adds PyAV):
uv add "caprid-moq-media[opus]"
```

## Publishing media

`caprid_relay.media` provides the publishing policies, while `moq_media` builds LOC properties and MSF catalogs:

- **`CatalogService(track, document)`**: re-sends the MSF catalog on every relay subscription and retries failed sends while demand persists.
- **`DocumentService(track)`**: re-sends a mutable document (such as an MSF `MediaTimeline` or telemetry snapshot) on each `update()` and on every new relay subscription.
- **`FrameWriter(track)`**: maps encoded frames onto MOQT groups (one group per GOP by default, or `grouping="frame"` for audio), skips frames when there is no relay demand, drops waiting frames past `max_backlog`, and signals `keyframe_needed` / `on_keyframe_needed`.

```python
import asyncio
from caprid_relay import connect_supervised_publisher
from caprid_relay.media import CatalogService, FrameWriter, media_track_properties
from moq_media import AudioTrackInfo, VideoTrackInfo, build_msf_catalog, loc_properties, loc_track_properties

publisher = await connect_supervised_publisher(namespace=namespace, get_token=get_token)
catalog_track = publisher.register_track("catalog")
video_track = publisher.register_track(
    "video",
    track_properties=[*loc_track_properties(), *media_track_properties("video")],
)

catalog_bytes = build_msf_catalog(
    namespace,
    video=VideoTrackInfo(codec=h264.codec, width=640, height=360, init_data=h264.avcc),
    audio=AudioTrackInfo(),
)
catalog_service = CatalogService(catalog_track, catalog_bytes)
catalog_task = asyncio.create_task(catalog_service.serve())

writer = FrameWriter(video_track, on_keyframe_needed=encoder.request_keyframe)

# For each encoded frame (any object with payload, key, properties, discontinuity):
outcome = await writer.write(encoded_frame)
```

Serve the catalog before gating encoding on `video_track.has_demand`: viewers subscribe to the catalog first, and only then subscribe to `video`. For manual GOP management without `FrameWriter`, see [Multi-object groups](pub-sub.md#multi-object-groups).

## Subscribing to media

Combine `PlayoutClock`, `FrameSequencer` (from `moq_media`), and `FrameReader` (from `caprid_relay.media`) to turn incoming track objects into ordered frames:

```python
from caprid_relay import connect_supervised_subscriber
from caprid_relay.media import FrameReader, media_subscribe_parameters
from moq_media import AUDIO_ONLY_TARGET_LATENCY_US, FrameSequencer, PlayoutClock

clock = PlayoutClock(target_latency_us=AUDIO_ONLY_TARGET_LATENCY_US)
reader = FrameReader(
    FrameSequencer(clock=clock, grouping="frame"),
    on_frame=handle_audio_frame,
)

subscriber = await connect_supervised_subscriber(get_token=get_token)
subscriber.want(
    "audio",
    namespace,
    "audio",
    reader.want_handlers(on_error=on_error),
    generations=True,
    parameters=media_subscribe_parameters("audio"),
)
```

A bot that only decodes audio without anchoring `clock` via `report_audio()` treats no frame as late.

## Voice bots with Opus, interrupts, and transcripts

With `caprid-moq-media[opus]`, `moq_media.opus` provides `OpusEncoder` and `OpusDecoder` for 20 ms voice packets stamped on the shared `BroadcastClock`:

- **Decode user speech**: feed received `audio` frames into `OpusDecoder` to get PCM audio for VAD and STT.
- **Encode agent speech**: pass TTS PCM audio into `OpusEncoder`, publish each packet with `FrameWriter(audio_track, grouping="frame")`, and send timestamped text segments on `TRANSCRIPT_TRACK` with `build_transcript_segment(...)`.
- **Handle barge-in**: when the user speaks over the bot, publish `build_interrupt(until_us=last_end_us)` on `INTERRUPT_TRACK` so browser listeners call `input.cut(interrupt)` and stop playback immediately.

See the runnable examples in the repository:
- [voice_bot](../../python/examples/voice_bot/README.md): end-to-end Python parrot voice bot paired with `ts/examples/voice-bot` in the browser.
- [videojs_stream](../../python/examples/videojs_stream/README.md): live H.264 + Opus publisher consumable by a browser player.

## Python performance and memory notes

`caprid-relay` uses pure-Python `aioquic` on the asyncio event loop:

- **Receive ceiling**: an idle core receives roughly **40 MB/s** of large objects (~25 ms CPU per MB). Media bitrates sit far below this ceiling, but heavy CPU work on the same event loop reduces throughput—offload encoding, decoding, and ML inference to worker threads or processes.
- **Long-lived publisher memory**: `aioquic` retains every finished stream ID in `QuicConnection._streams_finished` for the lifetime of a connection (~28 bytes per sent group, or ~2.4 MB/day at 1 group/s). Supervised connections rotate on token expiry, which naturally resets connection state.
- **Feature parity**: `OpusEncoder` does not expose an in-band FEC toggle yet, and the Python `FrameSequencer` does not yet support `lateDropUs`, `reorderWindowUs`, or `catchUpAfterUs` (though `subscriber.catch_up(key)` is available to call directly).
