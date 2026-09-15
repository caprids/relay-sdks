#!/usr/bin/env python3
"""Stream live video to a Relay namespace and watch it in a Video.js MoQ player.

Encodes a generated test pattern (or transcodes a media file) to H.264 with PyAV's bundled libx264,
optionally with Opus audio, and publishes it the way the Video.js v10 MoQ engine expects: an MSF
catalog on the ``catalog`` track and LOC-packaged frames on ``video`` (one group per GOP, object 0
the keyframe) and ``audio`` (one group per frame). Prints the ``moqt://`` source URL for the player.

    RELAY_TOKEN=... mise run python:example:videojs -- --namespace mattypoo/relay-sdk-example/tv

Media work runs on a worker thread; the asyncio side owns the Relay connection.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import math
import os
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from fractions import Fraction
from getpass import getpass
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit

import av
import numpy as np
from av.audio.fifo import AudioFifo
from av.audio.resampler import AudioResampler
from av.codec.context import Flags

from mux_relay import (
    DEFAULT_ENDPOINT,
    CancellationToken,
    Cancelled,
    Closed,
    MoqtObject,
    RelayClosedError,
    RelayError,
    RelayGroupWriter,
    RelayPublisher,
    RelayTrackWriter,
    SubscriptionHandlers,
    connect_relay,
    connect_relay_publisher,
)

# Runnable both as `python examples/videojs_stream/stream.py` and as a module: put `examples/` on the
# path when started as a script so the sibling helper module resolves.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from videojs_stream.moq_media import (
    AUDIO_TRACK,
    CATALOG_TRACK,
    LOC_TIMESTAMP,
    LOC_VIDEO_CONFIG,
    MICROSECONDS_PER_SECOND,
    TRACK_TIMESCALE_PROPERTIES,
    VIDEO_TRACK,
    AudioTrackInfo,
    VideoTrackInfo,
    annexb_to_avcc,
    avc_codec_string,
    build_avcc,
    build_msf_catalog,
    compose_player_url,
    extract_parameter_sets,
    loc_properties,
)

Log = Callable[[str, dict[str, Any]], None]

MICROSECOND = Fraction(1, MICROSECONDS_PER_SECOND)
OPUS_RATE = 48_000
OPUS_FRAME_SAMPLES = 960  # 20 ms


# ---------------------------------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class Config:
    url: str
    token: str
    namespace: list[str]
    input: Path | None
    loop: bool
    width: int
    height: int
    fps: int
    bitrate: int
    gop_seconds: float
    audio: Literal["auto", "tone", "off"]
    duration: float | None
    verify: bool
    show_token: bool
    on_track_end: Literal["stop", "continue"]


@dataclass(frozen=True, slots=True)
class EncodedSample:
    kind: Literal["video", "audio"]
    timestamp_us: int
    payload: bytes
    key: bool


# ---------------------------------------------------------------------------------------------------
# Encoders (worker thread)
# ---------------------------------------------------------------------------------------------------


class VideoEncoder:
    """libx264 configured for LOC: no B-frames, fixed GOP, parameter sets out of band (avcC)."""

    def __init__(self, width: int, height: int, fps: int, bitrate: int, gop_seconds: float) -> None:
        keyint = max(1, round(fps * gop_seconds))
        context = av.CodecContext.create("libx264", "w")
        context.width = width
        context.height = height
        context.pix_fmt = "yuv420p"
        context.time_base = MICROSECOND
        context.framerate = Fraction(fps, 1)
        context.bit_rate = bitrate
        context.options = {
            "preset": "veryfast",
            "tune": "zerolatency",
            "profile": "baseline",
            "x264-params": f"keyint={keyint}:min-keyint={keyint}:scenecut=0:bframes=0",
        }
        context.flags |= Flags.global_header
        context.open()
        sps, pps = extract_parameter_sets(bytes(context.extradata or b""))
        self.avcc = build_avcc(sps, pps)
        self.codec = avc_codec_string(self.avcc)
        self._context = context
        self.width = width
        self.height = height

    def encode(self, frame: av.VideoFrame) -> list[EncodedSample]:
        return [self._sample(packet) for packet in self._context.encode(frame)]

    def flush(self) -> list[EncodedSample]:
        return [self._sample(packet) for packet in self._context.encode(None)]

    def _sample(self, packet: av.Packet) -> EncodedSample:
        return EncodedSample(
            kind="video",
            timestamp_us=int(packet.pts or 0),
            payload=annexb_to_avcc(bytes(packet)),
            key=bool(packet.is_keyframe),
        )


class AudioEncoder:
    """libopus at 48 kHz stereo; input frames of any rate/layout are resampled and re-chunked to 20 ms."""

    def __init__(self, bitrate: int = 96_000) -> None:
        context = av.CodecContext.create("libopus", "w")
        context.sample_rate = OPUS_RATE
        context.layout = "stereo"
        context.format = "s16"
        context.bit_rate = bitrate
        context.time_base = Fraction(1, OPUS_RATE)
        context.open()
        self._context = context
        self._resampler = AudioResampler(format="s16", layout="stereo", rate=OPUS_RATE)
        self._fifo = AudioFifo()
        self._origin_us: int | None = None
        self._samples_out = 0

    def encode(self, frame: av.AudioFrame, timestamp_us: int) -> list[EncodedSample]:
        """Encode one decoded frame whose first sample plays at ``timestamp_us`` on the shared timeline."""
        if self._origin_us is None:
            self._origin_us = timestamp_us

        for resampled in self._resampler.resample(frame):
            resampled.pts = None
            self._fifo.write(resampled)

        samples: list[EncodedSample] = []

        while (chunk := self._fifo.read(OPUS_FRAME_SAMPLES)) is not None:
            chunk.pts = self._samples_out
            chunk.time_base = Fraction(1, OPUS_RATE)
            chunk.sample_rate = OPUS_RATE
            timestamp = self._origin_us + self._samples_out * MICROSECONDS_PER_SECOND // OPUS_RATE
            self._samples_out += OPUS_FRAME_SAMPLES

            for packet in self._context.encode(chunk):
                samples.append(EncodedSample(kind="audio", timestamp_us=timestamp, payload=bytes(packet), key=True))

        return samples


# ---------------------------------------------------------------------------------------------------
# Sources (worker thread)
# ---------------------------------------------------------------------------------------------------

# A 5x7 bitmap font for the on-screen clock: digits and a colon.
_GLYPHS: dict[str, tuple[str, ...]] = {
    "0": ("01110", "10001", "10011", "10101", "11001", "10001", "01110"),
    "1": ("00100", "01100", "00100", "00100", "00100", "00100", "01110"),
    "2": ("01110", "10001", "00001", "00010", "00100", "01000", "11111"),
    "3": ("11111", "00010", "00100", "00010", "00001", "10001", "01110"),
    "4": ("00010", "00110", "01010", "10010", "11111", "00010", "00010"),
    "5": ("11111", "10000", "11110", "00001", "00001", "10001", "01110"),
    "6": ("00110", "01000", "10000", "11110", "10001", "10001", "01110"),
    "7": ("11111", "00001", "00010", "00100", "01000", "01000", "01000"),
    "8": ("01110", "10001", "10001", "01110", "10001", "10001", "01110"),
    "9": ("01110", "10001", "10001", "01111", "00001", "00010", "01100"),
    ":": ("00000", "00100", "00100", "00000", "00100", "00100", "00000"),
}

_BARS = np.array(
    [[192, 192, 192], [192, 192, 0], [0, 192, 192], [0, 192, 0], [192, 0, 192], [192, 0, 0], [0, 0, 192]],
    dtype=np.uint8,
)


def _draw_text(canvas: np.ndarray, text: str, x: int, y: int, scale: int, color: tuple[int, int, int]) -> None:
    for index, char in enumerate(text):
        glyph = _GLYPHS.get(char)
        if glyph is None:
            continue

        left = x + index * 6 * scale

        for row, bits in enumerate(glyph):
            for column, bit in enumerate(bits):
                if bit == "1":
                    top = y + row * scale
                    canvas[top : top + scale, left + column * scale : left + (column + 1) * scale] = color


def pattern_frames(width: int, height: int, fps: int) -> Iterator[tuple[av.VideoFrame, int]]:
    """Color bars, a bouncing square, and a MM:SS clock, forever. Yields (frame, timestamp_us)."""
    bars = np.repeat(_BARS[np.linspace(0, 6, width, endpoint=False).astype(int)][np.newaxis, :, :], height, axis=0)
    square = max(16, min(width, height) // 6)
    scale = max(2, height // 60)
    index = 0

    while True:
        canvas = bars.copy()
        seconds = index / fps
        # A bouncing square makes motion (and dropped frames) obvious.
        travel_x = width - square
        travel_y = height - square
        phase_x = (seconds * 0.35) % 2.0
        phase_y = (seconds * 0.5) % 2.0
        x = int(travel_x * (phase_x if phase_x <= 1 else 2 - phase_x))
        y = int(travel_y * (phase_y if phase_y <= 1 else 2 - phase_y))
        canvas[y : y + square, x : x + square] = (255, 255, 255)
        # The clock proves liveness and lets a viewer read the glass-to-glass delay.
        clock = time.strftime("%M:%S", time.gmtime(seconds))
        canvas[height // 2 - 7 * scale : height // 2 + 8 * scale, :] //= 3
        _draw_text(
            canvas, clock, (width - len(clock) * 6 * scale) // 2, height // 2 - 3 * scale, scale, (255, 255, 255)
        )

        frame = av.VideoFrame.from_ndarray(canvas, format="rgb24").reformat(format="yuv420p")
        timestamp_us = index * MICROSECONDS_PER_SECOND // fps
        frame.pts = timestamp_us
        frame.time_base = MICROSECOND
        yield frame, timestamp_us
        index += 1


def tone_frames(frequency: float = 440.0, level: float = 0.1) -> Iterator[tuple[av.AudioFrame, int]]:
    """A quiet sine tone in 20 ms stereo frames, forever. Yields (frame, timestamp_us)."""
    index = 0

    while True:
        offset = index * OPUS_FRAME_SAMPLES
        t = (np.arange(OPUS_FRAME_SAMPLES) + offset) / OPUS_RATE
        mono = (np.sin(2 * math.pi * frequency * t) * level * 32767).astype(np.int16)
        interleaved = np.empty((1, OPUS_FRAME_SAMPLES * 2), dtype=np.int16)
        interleaved[0, 0::2] = mono
        interleaved[0, 1::2] = mono
        frame = av.AudioFrame.from_ndarray(interleaved, format="s16", layout="stereo")
        frame.sample_rate = OPUS_RATE
        yield frame, offset * MICROSECONDS_PER_SECOND // OPUS_RATE
        index += 1


def file_frames(
    path: Path, width: int, height: int, *, loop: bool, with_audio: bool
) -> Iterator[tuple[av.VideoFrame | av.AudioFrame, int]]:
    """Decode a media file's video (scaled to the target size) and, optionally, audio on one timeline.

    Yields frames in decode order with their timestamps in microseconds; looping restarts the
    timeline where the previous pass ended.
    """
    offset_us = 0

    while True:
        with av.open(str(path)) as container:
            video_stream = container.streams.video[0]
            audio_stream = container.streams.audio[0] if with_audio and container.streams.audio else None
            streams = [video_stream, *([audio_stream] if audio_stream else [])]
            start_us: int | None = None
            last_us = offset_us

            for frame in container.decode(*streams):
                if frame.pts is None or frame.time_base is None:
                    continue

                pts_us = int(frame.pts * frame.time_base * MICROSECONDS_PER_SECOND)

                if start_us is None:
                    start_us = pts_us

                timestamp_us = offset_us + pts_us - start_us
                last_us = max(last_us, timestamp_us)

                if isinstance(frame, av.VideoFrame):
                    scaled = frame.reformat(width=width, height=height, format="yuv420p")
                    scaled.pts = timestamp_us
                    scaled.time_base = MICROSECOND
                    yield scaled, timestamp_us
                else:
                    yield frame, timestamp_us

            offset_us = last_us + MICROSECONDS_PER_SECOND // 30

        if not loop:
            return


# ---------------------------------------------------------------------------------------------------
# Producer thread: source -> encoders -> asyncio queue, paced to media time
# ---------------------------------------------------------------------------------------------------


class Producer:
    def __init__(self, config: Config, loop: asyncio.AbstractEventLoop, queue: asyncio.Queue[EncodedSample | None]):
        self._config = config
        self._loop = loop
        self._queue = queue
        self._stop = threading.Event()
        self.video = VideoEncoder(config.width, config.height, config.fps, config.bitrate, config.gop_seconds)
        self.audio: AudioEncoder | None = None
        self.error: BaseException | None = None
        self._thread = threading.Thread(target=self._run, name="media-producer", daemon=True)

        if config.audio == "tone" or (config.audio == "auto" and config.input is not None and self._input_has_audio()):
            self.audio = AudioEncoder()

    def _input_has_audio(self) -> bool:
        assert self._config.input is not None

        with av.open(str(self._config.input)) as container:
            return len(container.streams.audio) > 0

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _emit(self, samples: list[EncodedSample]) -> None:
        for sample in samples:
            self._loop.call_soon_threadsafe(self._queue.put_nowait, sample)

    def _run(self) -> None:
        config = self._config
        started = time.monotonic()

        try:
            if config.input is not None:
                frames: Iterator[tuple[Any, int]] = file_frames(
                    config.input, config.width, config.height, loop=config.loop, with_audio=self.audio is not None
                )
            else:
                video = pattern_frames(config.width, config.height, config.fps)
                audio = tone_frames() if self.audio is not None else None
                frames = _interleave(video, audio)

            for frame, timestamp_us in frames:
                if self._stop.is_set():
                    break

                # Pace to media time so the relay sees a live stream, not a burst.
                due = started + timestamp_us / MICROSECONDS_PER_SECOND
                delay = due - time.monotonic()

                if delay > 0 and self._stop.wait(delay):
                    break

                if isinstance(frame, av.VideoFrame):
                    self._emit(self.video.encode(frame))
                elif self.audio is not None:
                    self._emit(self.audio.encode(frame, timestamp_us))

            self._emit(self.video.flush())
        except BaseException as error:
            self.error = error
        finally:
            self._loop.call_soon_threadsafe(self._queue.put_nowait, None)


def _interleave(
    video: Iterator[tuple[av.VideoFrame, int]], audio: Iterator[tuple[av.AudioFrame, int]] | None
) -> Iterator[tuple[Any, int]]:
    """Merge two timestamp-ordered generators into one timeline."""
    if audio is None:
        yield from video
        return

    next_video = next(video)
    next_audio = next(audio)

    while True:
        if next_audio[1] <= next_video[1]:
            yield next_audio
            next_audio = next(audio)
        else:
            yield next_video
            next_video = next(video)


# ---------------------------------------------------------------------------------------------------
# Publisher side (asyncio)
# ---------------------------------------------------------------------------------------------------


class TrackEnded(RuntimeError):
    """A published track was retired mid-run. Raised through the lifetime so the run exits non-zero."""


def failure_ends_track(error: RelayError, *, operation: Literal["write", "group"], bound: bool) -> bool:
    """Decide whether a failed publish left the track retired or only cost this one object.

    `python/README.md` states the rule the SDK implements: "A `RelayTimeoutError` waiting for demand
    can be retried. A failure once a write is bound to a relay subscription ends that track". So a
    single-object `write()` that had reached a relay subscription is terminal whatever it raised --
    a timeout included, which is the case #19 describes -- while an unbound one, demand that came
    and went or a deadline spent waiting for it, is not. A group is different: the SDK resets its
    stream and keeps the track open for the next random-access point, so only a closed track ends it.

    `RelayClosedError` is always terminal: it is what the next write raises once the track is
    retired, which is the backstop for a write that bound after `bound` was sampled. #31 adds a
    dedicated track-ended error as a subclass of it, so this check keeps working unchanged.
    """
    if isinstance(error, RelayClosedError):
        return True

    return operation == "write" and bound


async def track_has_ended(track: RelayTrackWriter) -> bool:
    """Ask the writer whether it is retired, instead of reading that out of `bound`.

    `bound` cannot answer: False is equally what "nobody is subscribed right now" looks like. #31
    adds a `closed` property that says it outright; until that lands, the same fact is readable from
    a fresh `bindings()` iterator, which replays the track's current demand state and "stops once
    the track is closed", so one that stops before yielding anything means the track is gone.
    """
    closed = getattr(track, "closed", None)

    if isinstance(closed, bool):
        return closed

    # `anext` drops the iterator as soon as it answers, which releases the watcher it registered.
    return await anext(track.bindings(), None) is None


async def write_ended_track(track: RelayTrackWriter, error: RelayError, *, bound: bool) -> bool:
    """Whether a failed `write()` left this track retired, confirmed against the writer itself.

    `bound` is sampled before the write, so it can be stale by one turn of the loop: demand that
    disappears in that window makes a retryable failure look terminal. Confirming with the writer
    keeps a viewer leaving at the wrong moment from ending an otherwise healthy run.
    """
    if not failure_ends_track(error, operation="write", bound=bound):
        return False

    # A closed track, publisher, or connection needs no confirmation, and its writer may never have
    # been retired in the first place -- the connection under it went away instead.
    return isinstance(error, RelayClosedError) or await track_has_ended(track)


@dataclass(slots=True)
class Stats:
    video_groups: int = 0
    video_objects: int = 0
    audio_objects: int = 0
    skipped_video: int = 0
    skipped_audio: int = 0
    dropped_video: int = 0
    dropped_audio: int = 0
    catalog_sends: int = 0
    catalog_failures: int = 0
    ended_tracks: dict[str, str] = field(default_factory=dict)


# Called once per track that the relay retired under us, with the failure that ended it.
TrackEndHandler = Callable[[str, RelayError], None]


def make_track_end_handler(
    policy: Literal["stop", "continue"], stats: Stats, log: Log, stop: Callable[[BaseException], None]
) -> TrackEndHandler:
    """Record a retired track once, loudly, and by default fail the run rather than stream on without it.

    The catalog keeps advertising every track, and the name cannot be reused on this connection, so
    carrying on means viewers get a stream that silently no longer matches its own catalog. Only
    ``--on-track-end continue`` accepts that, and then the lost samples are counted as dropped.
    """

    def on_track_end(track: str, error: RelayError) -> None:
        if track in stats.ended_tracks:
            return

        stats.ended_tracks[track] = describe(error)
        log(
            f"{track} track ended",
            {
                "error": describe(error),
                "note": "the track name cannot be reused on this connection; the catalog still advertises it",
                "policy": policy,
            },
        )

        if policy == "stop":
            stop(TrackEnded(f"the {track} track ended mid-run: {describe(error)}"))

    return on_track_end


async def serve_catalog(
    catalog: RelayTrackWriter, document: bytes, stats: Stats, log: Log, on_track_end: TrackEndHandler
) -> None:
    """Send the catalog as a fresh group every time the relay (re)subscribes: catalog frames flow on change."""
    async for bound in catalog.bindings():
        if not bound:
            continue

        try:
            await catalog.write(document)
            stats.catalog_sends += 1
            log("catalog sent", {"bytes": len(document)})
        except RelayError as error:
            # A retired catalog track is not recoverable on this connection: stop re-sending rather
            # than spin on a writer that can only keep failing.
            if await write_ended_track(catalog, error, bound=True):
                on_track_end(CATALOG_TRACK, error)
                return

            stats.catalog_failures += 1
            log("catalog write failed", {"error": describe(error)})


async def publish_media(
    queue: asyncio.Queue[EncodedSample | None],
    video: RelayTrackWriter,
    audio: RelayTrackWriter | None,
    encoder: VideoEncoder,
    stats: Stats,
    log: Log,
    on_track_end: TrackEndHandler,
) -> None:
    """Route encoded samples: video as one group per GOP while the relay wants the track, audio one group per frame."""
    group: RelayGroupWriter | None = None
    video_ended = False

    async def end_group() -> None:
        nonlocal group

        if group is not None:
            with contextlib.suppress(RelayError, ValueError):
                await group.close()

            group = None

    try:
        while True:
            sample = await queue.get()

            if sample is None:
                break

            if sample.kind == "audio":
                if audio is None:
                    # The track was retired earlier in the run; these frames are gone, not deferred.
                    stats.dropped_audio += 1
                    continue

                # Sampled before the write, because a failure once the write reached this
                # subscription retires the track while an unbound one only costs the frame.
                bound = audio.bound

                if not bound:
                    stats.skipped_audio += 1
                    continue

                try:
                    await audio.write(sample.payload, properties=loc_properties(sample.timestamp_us))
                    stats.audio_objects += 1
                except RelayError as error:
                    if not await write_ended_track(audio, error, bound=bound):
                        # Demand came and went before the write started: the track is still open, so
                        # this frame is skipped and the next one tries again.
                        stats.skipped_audio += 1
                        continue

                    audio = None
                    stats.dropped_audio += 1
                    on_track_end(AUDIO_TRACK, error)

                continue

            if sample.key:
                await end_group()

                if not video_ended and video.bound:
                    try:
                        opened = await video.open_group()
                    except RelayError as error:
                        if failure_ends_track(error, operation="group", bound=True):
                            video_ended = True
                            on_track_end(VIDEO_TRACK, error)
                        else:
                            log("group open failed", {"error": describe(error)})
                    else:
                        group = opened
                        stats.video_groups += 1

                        if stats.video_groups == 1:
                            log("video flowing", {"group": opened.group_id})

            if group is None:
                if video_ended:
                    stats.dropped_video += 1
                else:
                    stats.skipped_video += 1

                continue

            try:
                await group.write(
                    sample.payload,
                    properties=loc_properties(sample.timestamp_us, video_config=encoder.avcc if sample.key else None),
                )
                stats.video_objects += 1
            except RelayError as error:
                # This sample went nowhere either, so it belongs in a counter like every other one.
                group_id, group = group.group_id, None

                if failure_ends_track(error, operation="group", bound=True):
                    video_ended = True
                    stats.dropped_video += 1
                    on_track_end(VIDEO_TRACK, error)
                else:
                    stats.skipped_video += 1
                    log("group interrupted", {"group": group_id, "error": describe(error)})
    finally:
        await end_group()


# ---------------------------------------------------------------------------------------------------
# Optional in-process viewer (--verify)
# ---------------------------------------------------------------------------------------------------


@dataclass(slots=True)
class Verified:
    catalog: bytes | None = None
    video_objects: int = 0
    video_keyframes: int = 0
    keyframes_with_config: int = 0
    video_groups: set[int] | None = None
    audio_objects: int = 0
    first_timestamp_us: int | None = None
    last_timestamp_us: int | None = None
    problems: list[str] | None = None


async def verify(config: Config, lifetime: CancellationToken, log: Log) -> tuple[Any, Verified]:
    """Subscribe like a player would and record what arrives, so the wire can be checked without a browser."""
    result = Verified(video_groups=set(), problems=[])
    subscriber = await connect_relay(config.url, get_token=lambda: config.token, cancellation=lifetime)
    assert result.problems is not None
    assert result.video_groups is not None
    problems = result.problems
    groups = result.video_groups

    def on_catalog(obj: MoqtObject) -> None:
        if obj.status == "normal" and obj.payload:
            result.catalog = obj.payload
            log("verify: catalog received", {"bytes": len(obj.payload), "group": obj.group_id})

    def on_video(obj: MoqtObject) -> None:
        if obj.status != "normal":
            return

        result.video_objects += 1
        groups.add(obj.group_id)
        timestamp = next((p.value for p in obj.properties if p.type == LOC_TIMESTAMP), None)

        if not isinstance(timestamp, int):
            problems.append("video object without a LOC timestamp")
            return

        if obj.object_id == 0:
            result.video_keyframes += 1

            # Relays drop odd-typed object properties they do not know, so the per-keyframe Video
            # Config usually does not survive the hop; the catalog's initDataList is the copy players use.
            if any(p.type == LOC_VIDEO_CONFIG for p in obj.properties):
                result.keyframes_with_config += 1

        if result.first_timestamp_us is None:
            result.first_timestamp_us = timestamp

        result.last_timestamp_us = timestamp

    def on_audio(obj: MoqtObject) -> None:
        if obj.status == "normal":
            result.audio_objects += 1

    def on_error(track: str) -> Callable[[Any], None]:
        return lambda error: problems.append(f"{track}: subscribe error {error.error_code} {error.reason}")

    subscriber.subscribe(
        config.namespace, CATALOG_TRACK, SubscriptionHandlers(on_object=on_catalog, on_error=on_error("catalog"))
    )
    subscriber.subscribe(
        config.namespace, VIDEO_TRACK, SubscriptionHandlers(on_object=on_video, on_error=on_error("video"))
    )

    if config.audio != "off":
        subscriber.subscribe(
            config.namespace, AUDIO_TRACK, SubscriptionHandlers(on_object=on_audio, on_error=on_error("audio"))
        )

    return subscriber, result


# ---------------------------------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------------------------------


async def run(config: Config, stats: Stats, log: Log) -> None:
    lifetime = CancellationToken()
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[EncodedSample | None] = asyncio.Queue()
    producer = Producer(config, loop, queue)
    closing = False
    stopped: asyncio.Future[None] = loop.create_future()

    on_track_end = make_track_end_handler(config.on_track_end, stats, log, lifetime.cancel)

    # Registered before anything can cancel the lifetime: a publisher connection that drops during
    # setup cancels it, and a callback added afterwards would never run.
    def on_cancelled(reason: BaseException) -> None:
        if not stopped.done():
            stopped.set_result(None)

    lifetime.add_callback(on_cancelled)

    def watch(label: str, closed: asyncio.Future[Closed]) -> None:
        def done(future: asyncio.Future[Closed]) -> None:
            if not closing:
                log(f"{label} connection closed", {"error": describe(future.result().error)})
                lifetime.cancel(future.result().error or RelayClosedError(f"{label} connection closed"))

        closed.add_done_callback(done)

    publisher: RelayPublisher = await connect_relay_publisher(
        config.url, namespace=config.namespace, get_token=lambda: config.token, cancellation=lifetime
    )
    watch("publisher", publisher.closed)
    log("publisher connected", {"url": config.url, "namespace": "/".join(config.namespace)})

    video_info = VideoTrackInfo(
        codec=producer.video.codec,
        width=config.width,
        height=config.height,
        framerate=config.fps,
        bitrate=config.bitrate,
        init_data=producer.video.avcc,
    )
    audio_info = AudioTrackInfo(bitrate=96_000) if producer.audio is not None else None
    document = build_msf_catalog(
        config.namespace, video=video_info, audio=audio_info, generated_at_ms=int(time.time() * 1000)
    )
    log(
        "catalog",
        {"codec": video_info.codec, "size": f"{config.width}x{config.height}", "audio": audio_info is not None},
    )

    catalog = publisher.publish(CATALOG_TRACK, track_properties=TRACK_TIMESCALE_PROPERTIES, write_timeout=5)
    video = publisher.publish(VIDEO_TRACK, track_properties=TRACK_TIMESCALE_PROPERTIES, write_timeout=5)
    audio = (
        publisher.publish(AUDIO_TRACK, track_properties=TRACK_TIMESCALE_PROPERTIES, write_timeout=5)
        if producer.audio is not None
        else None
    )

    print_player_url(config, log)

    tasks = [
        asyncio.ensure_future(serve_catalog(catalog, document, stats, log, on_track_end)),
        asyncio.ensure_future(publish_media(queue, video, audio, producer.video, stats, log, on_track_end)),
    ]
    verifier: Any = None
    verified: Verified | None = None

    if config.verify:
        verifier, verified = await verify(config, lifetime, log)

    producer.start()
    deadline = loop.call_later(config.duration, lambda: lifetime.cancel()) if config.duration else None
    media_task = tasks[1]

    try:
        await asyncio.wait([stopped, media_task], return_when=asyncio.FIRST_COMPLETED)

        if producer.error is not None:
            raise RuntimeError(f"media pipeline failed: {describe(producer.error)}") from producer.error

        # The media loop ending on its own is either the source running out (a file without --loop)
        # or a failure; a failure must not read as a clean finish.
        if media_task.done() and not media_task.cancelled() and media_task.exception() is not None:
            raise RuntimeError(f"publishing failed: {describe(media_task.exception())}") from media_task.exception()

        # A deadline cancels with the default reason; anything else (a connection loss) is a failure.
        if lifetime.cancelled and lifetime.reason is not None and not isinstance(lifetime.reason, Cancelled):
            raise lifetime.reason
    finally:
        closing = True

        if deadline is not None:
            deadline.cancel()

        producer.stop()

        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)

        if verified is not None:
            report_verification(verified, log)

        if verifier is not None:
            verifier.close()

        video.close()
        catalog.close()

        if audio is not None:
            audio.close()

        await publisher.close()


def summarize(stats: Stats) -> dict[str, Any]:
    """The `finished` line: every sample the run produced is in exactly one of these counters."""
    return {
        "videoGroups": stats.video_groups,
        "videoObjects": stats.video_objects,
        "audioObjects": stats.audio_objects,
        "skippedVideo": stats.skipped_video,
        "skippedAudio": stats.skipped_audio,
        "droppedVideo": stats.dropped_video,
        "droppedAudio": stats.dropped_audio,
        "catalogSends": stats.catalog_sends,
        "catalogFailures": stats.catalog_failures,
        "endedTracks": ",".join(stats.ended_tracks) or None,
    }


def report_verification(verified: Verified, log: Log) -> None:
    span = (
        (verified.last_timestamp_us - verified.first_timestamp_us) / MICROSECONDS_PER_SECOND
        if verified.first_timestamp_us is not None and verified.last_timestamp_us is not None
        else 0.0
    )
    log(
        "verify: summary",
        {
            "catalog": "received" if verified.catalog else "MISSING",
            "videoGroups": len(verified.video_groups or ()),
            "videoObjects": verified.video_objects,
            "keyframes": verified.video_keyframes,
            "keyframesWithConfig": verified.keyframes_with_config,
            "note": "relays may strip the per-keyframe Video Config; the catalog carries the avcC",
            "audioObjects": verified.audio_objects,
            "spanSeconds": round(span, 2),
            "problems": verified.problems or "none",
        },
    )


def print_player_url(config: Config, log: Log) -> None:
    redacted = compose_player_url(config.url, config.namespace, token="RELAY_TOKEN")
    log("player url", {"src": redacted.replace("c4m=RELAY_TOKEN", "c4m=<RELAY_TOKEN>")})

    if config.show_token:
        print(compose_player_url(config.url, config.namespace, token=config.token), flush=True)
    else:
        log("player url", {"hint": "pass --show-token to print the URL with your token filled in"})


def describe(error: BaseException | None) -> str:
    if error is None:
        return "no error"

    name = type(error).__name__
    return f"{name}: {error}" if str(error) else name


# ---------------------------------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------------------------------


def read_token() -> str:
    token = os.environ.get("RELAY_TOKEN")

    if not token and sys.stdin.isatty():
        token = getpass("Relay token (input hidden): ").strip()
    elif not token:
        token = sys.stdin.readline().strip()

    return token or ""


def parse_args(argv: list[str]) -> Config:
    parser = argparse.ArgumentParser(description="Stream live video to a Relay namespace for a Video.js MoQ player.")
    parser.add_argument("--url", default=os.environ.get("RELAY_URL", DEFAULT_ENDPOINT), help="Relay endpoint")
    parser.add_argument(
        "--namespace",
        help="Namespace, slash-separated, including the token's root prefix "
        "(default RELAY_NAMESPACE_PREFIX or relay-sdk-example, plus a random field)",
    )
    parser.add_argument("--input", type=Path, help="Media file to transcode instead of the generated test pattern")
    parser.add_argument("--loop", action="store_true", help="Loop the input file")
    parser.add_argument("--width", type=int, default=640)
    parser.add_argument("--height", type=int, default=360)
    parser.add_argument("--fps", type=int, default=30, help="Frame rate of the test pattern (default 30)")
    parser.add_argument("--bitrate", type=int, default=1_500_000, help="Video bitrate in bits/s (default 1500000)")
    parser.add_argument("--gop", type=float, default=2.0, help="Keyframe interval in seconds (default 2)")
    parser.add_argument(
        "--audio",
        choices=["auto", "tone", "off"],
        default="auto",
        help="auto: the input file's audio as Opus, none for the pattern; tone: a 440 Hz sine; off: video only",
    )
    parser.add_argument("--duration", type=float, help="Stop after this many seconds (default: run until Ctrl-C)")
    parser.add_argument(
        "--on-track-end",
        choices=["stop", "continue"],
        default="stop",
        help="What to do when the relay retires a track mid-run: stop: fail the run (default); "
        "continue: keep publishing the surviving tracks and count the lost samples",
    )
    parser.add_argument("--verify", action="store_true", help="Also subscribe in-process and report what arrives")
    parser.add_argument("--show-token", action="store_true", help="Print the player URL with the token filled in")
    args = parser.parse_args(argv)

    parts = urlsplit(args.url)
    if parts.scheme != "https":
        parser.error("--url must be an https URL")
    if "jwt=" in parts.query:
        parser.error("Supply the token via RELAY_TOKEN or the prompt, not inside --url")

    for name in ("width", "height", "fps", "bitrate"):
        if getattr(args, name) <= 0:
            parser.error(f"--{name} must be a positive integer")

    if args.width % 2 or args.height % 2:
        parser.error("--width and --height must be even (yuv420p)")

    if args.gop <= 0:
        parser.error("--gop must be positive")

    if args.input is not None and not args.input.is_file():
        parser.error(f"--input {args.input} is not a file")

    namespace_text = (
        args.namespace or f"{os.environ.get('RELAY_NAMESPACE_PREFIX', 'relay-sdk-example')}/{uuid.uuid4().hex[:8]}"
    )
    namespace = [field for field in namespace_text.split("/") if field]
    if not namespace:
        parser.error("--namespace must contain at least one field")

    token = read_token()
    if not token:
        parser.error("A Relay token is required (RELAY_TOKEN or the prompt)")

    return Config(
        url=args.url,
        token=token,
        namespace=namespace,
        input=args.input,
        loop=args.loop,
        width=args.width,
        height=args.height,
        fps=args.fps,
        bitrate=args.bitrate,
        gop_seconds=args.gop,
        audio=args.audio,
        duration=args.duration,
        verify=args.verify,
        show_token=args.show_token,
        on_track_end=args.on_track_end,
    )


def main(argv: list[str] | None = None) -> int:
    config = parse_args(sys.argv[1:] if argv is None else argv)

    def redact(text: str) -> str:
        return text.replace(config.token, "[redacted]")

    def log(event: str, fields: dict[str, Any]) -> None:
        rendered = " ".join(f"{key}={value}" for key, value in fields.items() if value is not None)
        print(redact(f"{event:<22} {rendered}".rstrip()), flush=True)

    log(
        "starting",
        {"namespace": "/".join(config.namespace), "input": config.input or "test pattern", "audio": config.audio},
    )

    # Stats live out here so the counters are reported even when the run ends in a failure: a track
    # the relay retired is exactly the case where the summary matters most.
    stats = Stats()

    try:
        asyncio.run(run(config, stats, log))
    except KeyboardInterrupt:
        log("finished", summarize(stats))
        print(redact("interrupted"), file=sys.stderr)
        return 130
    except BaseException as error:
        log("finished", summarize(stats))
        print(redact(f"{'failed':<22} error={describe(error)}"), file=sys.stderr)
        return 1

    log("finished", summarize(stats))
    return 0


if __name__ == "__main__":
    sys.exit(main())
