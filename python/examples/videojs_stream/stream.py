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
import itertools
import math
import os
import sys
import threading
import time
import uuid
from collections.abc import AsyncIterator, Callable, Iterator, Sequence
from dataclasses import dataclass, field, replace
from fractions import Fraction
from getpass import getpass
from pathlib import Path
from typing import Any, Literal, Protocol
from urllib.parse import urlsplit

import av
import numpy as np
from av.audio.fifo import AudioFifo
from av.audio.resampler import AudioResampler
from av.codec.context import Flags
from av.video.frame import PictureType

from moq_media import (
    AUDIO_TRACK,
    CATALOG_TRACK,
    LOC_VIDEO_CONFIG,
    MEDIA_TIMELINE_TRACK,
    MICROSECONDS_PER_SECOND,
    TRACK_TIMESCALE_PROPERTIES,
    VIDEO_TRACK,
    AudioTrackInfo,
    H264Config,
    MediaTimeline,
    MediaTimelineEntry,
    MediaTimelineTrackInfo,
    VideoTrackInfo,
    annexb_to_avcc,
    build_msf_catalog,
    build_msf_end_catalog,
    compose_player_url,
    loc_properties,
    loc_timestamp_us,
    parse_media_timeline,
    parse_msf_catalog,
)
from mux_relay import (
    DEFAULT_ENDPOINT,
    CancellationToken,
    Cancelled,
    Closed,
    KeyValuePair,
    MoqtObject,
    RelayAwaitable,
    RelayError,
    RelayPublisher,
    RelayTrackWriter,
    SubscriptionRequestError,
    WantHandlers,
    connect_relay_publisher,
    connect_supervised_subscriber,
)
from mux_relay.media import CatalogService, DocumentService, FrameOutcome, FrameWriter, media_track_properties

Log = Callable[[str, dict[str, Any]], None]

MICROSECOND = Fraction(1, MICROSECONDS_PER_SECOND)
OPUS_RATE = 48_000
OPUS_FRAME_SAMPLES = 960  # 20 ms
BOUNCE_RATES = (Fraction(7, 20), Fraction(1, 2))  # Wall-to-wall trips per second (x, y).
CATALOG_RETRY_DELAY = 1.0  # seconds between failed attempts while demand remains
TIMELINE_MAX_ENTRIES = 512  # media timeline entries retained: about 18 KB, 8.5 minutes of one-second GOPs
TIMELINE_SKEW_LIMIT_MS = 5_000  # how far --verify lets the newest entry's wall-clock time sit from its arrival
VERIFY_STARTUP_TIMEOUT = 10.0  # seconds the in-process viewer may wait for the announce and its subscribes


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
    rate_control: Literal["abr", "cbr"] = "abr"
    gop_seconds: float
    audio: Literal["auto", "tone", "off"]
    duration: float | None
    verify: bool
    show_token: bool
    on_track_end: Literal["stop", "continue"]


@dataclass(frozen=True, slots=True)
class EncodedSample:
    """One encoded frame with its LOC properties attached: what the SDK's frame writer takes as a frame."""

    kind: Literal["video", "audio"]
    timestamp_us: int
    payload: bytes
    key: bool
    discontinuity: bool = False
    properties: Sequence[KeyValuePair] = ()


class LiveQueue(asyncio.Queue[EncodedSample | None]):
    """A bounded track buffer. Mutations happen on the event loop, including producer handoff.

    Overflow clears the buffered video GOP: its dependent frames cannot be decoded after a loss.
    A discontinuity on the next sample tells the writer to reset the open group as well. EOF has
    its own reserved slot, so it never evicts the last frame or gets lost behind a full queue.
    """

    def __init__(
        self, kind: Literal["video", "audio"], stats: Stats, *, max_samples: int, max_bytes: int, max_lag: float
    ):
        super().__init__()
        self.kind = kind
        self.stats = stats
        self.max_samples = max_samples
        self.max_bytes = max_bytes
        self.max_lag = max_lag
        self.started: float | None = None
        self.buffered_bytes = 0
        self.discontinuity = False
        self.latest_lag = 0.0
        self.pending: EncodedSample | None = None
        self._source_finished = False

    def put_nowait(self, item: EncodedSample | None) -> None:
        if item is None:
            if not self._source_finished:
                self._source_finished = True
                super().put_nowait(None)
            return
        if item.kind == "video":
            self.stats.encoded_video_bytes += len(item.payload)
        else:
            self.stats.encoded_audio_bytes += len(item.payload)
        if self._source_finished:
            discard_sample(item, self.stats)
            return
        while self.qsize() and (
            self.qsize() >= self.max_samples or self.buffered_bytes + len(item.payload) > self.max_bytes
        ):
            self._drop_oldest()
            if self.kind == "video":
                while not self.empty():
                    self._drop_oldest()
        if len(item.payload) > self.max_bytes:
            self.discontinuity = True
            stale_sample(item, self.stats)
            return
        self.buffered_bytes += len(item.payload)
        super().put_nowait(item)

    def _drop_oldest(self) -> None:
        sample = self.get_nowait()
        assert sample is not None
        stale_sample(sample, self.stats)
        self.discontinuity = True

    def get_nowait(self) -> EncodedSample | None:
        sample = super().get_nowait()
        self.task_done()
        if sample is not None:
            self.buffered_bytes -= len(sample.payload)
            if self.discontinuity:
                sample = replace(sample, discontinuity=True)
                self.discontinuity = False
        return sample

    def current_lag(self) -> float:
        return self.lag(self.pending) if self.pending is not None else 0.0

    def lag(self, sample: EncodedSample) -> float:
        self.latest_lag = (
            max(0.0, time.monotonic() - self.started - sample.timestamp_us / MICROSECONDS_PER_SECOND)
            if self.started is not None
            else 0.0
        )
        self.stats.max_lag_seconds = max(self.stats.max_lag_seconds, self.latest_lag)
        return self.latest_lag


# ---------------------------------------------------------------------------------------------------
# Encoders (worker thread)
# ---------------------------------------------------------------------------------------------------


def keyframe_interval_frames(fps: Fraction | int, gop_seconds: float) -> int:
    """The encoder's GOP in frames: ``gop_seconds`` rounded to whole frames, at least one."""
    return max(1, round(fps * Fraction(gop_seconds)))


def input_frame_rate(path: Path, fallback: Fraction) -> Fraction:
    """A media file's average video frame rate, exactly as it declares it, or ``fallback`` when it declares none.

    A variable-rate file's GOP still counts frames, so its keyframe interval is the average one.
    """
    with av.open(str(path)) as container:
        rate = container.streams.video[0].average_rate if container.streams.video else None
    return Fraction(rate) if rate else fallback


def keyframe_interval_us(fps: Fraction | int, gop_seconds: float) -> int:
    """The encoder's actual keyframe interval in microseconds, after rounding the GOP to whole frames."""
    return round(keyframe_interval_frames(fps, gop_seconds) * 1_000_000 / fps)


class VideoEncoder:
    """libx264 configured for LOC: no B-frames, fixed GOP, parameter sets out of band (avcC).

    ``encode(frame, keyframe=True)`` forces an IDR on demand; x264 counts the next GOP from it.
    """

    def __init__(
        self, width: int, height: int, fps: Fraction | int, bitrate: int, gop_seconds: float, rate_control: str = "abr"
    ) -> None:
        fps = Fraction(fps)
        keyint = keyframe_interval_frames(fps, gop_seconds)
        self.fps = fps
        # The interval the encoder actually produces, which the video track's delivery timeout follows.
        self.keyframe_interval_us = keyframe_interval_us(fps, gop_seconds)
        context = av.CodecContext.create("libx264", "w")
        context.width = width
        context.height = height
        context.pix_fmt = "yuv420p"
        context.time_base = MICROSECOND
        context.framerate = fps
        context.bit_rate = bitrate
        context.options = {
            "preset": "veryfast",
            "tune": "zerolatency",
            "profile": "baseline",
            "x264-params": f"keyint={keyint}:min-keyint={keyint}:scenecut=0:bframes=0",
            # A frame marked I becomes an IDR, a point a new subscription can start decoding from.
            "forced-idr": "1",
        }
        if rate_control == "cbr":
            # x264's VBV units are kbit/s and kbit. HRD CBR includes filler NALs even for
            # simple color bars; the AVCC conversion preserves these bytes on the wire.
            rate_kbps = max(1, bitrate // 1000)
            context.bit_rate = rate_kbps * 1000
            context.options["x264-params"] += f":vbv-maxrate={rate_kbps}:vbv-bufsize={rate_kbps}:nal-hrd=cbr:filler=1"
        context.flags |= Flags.global_header
        context.open()
        h264 = H264Config.from_annexb(bytes(context.extradata or b""))
        self.avcc = h264.avcc
        self.codec = h264.codec
        self._context = context
        self.width = width
        self.height = height
        # Added to media time in the LOC Timestamp only: the wire carries Unix-epoch microseconds (media-layer.md).
        self.epoch_us = 0

    def encode(self, frame: av.VideoFrame, *, keyframe: bool = False) -> list[EncodedSample]:
        # Always set: a frame decoded from a file carries its source's picture type, and with
        # forced-idr every source I-frame would otherwise restart the GOP.
        frame.pict_type = PictureType.I if keyframe else PictureType.NONE
        return [self._sample(packet) for packet in self._context.encode(frame)]

    def flush(self) -> list[EncodedSample]:
        return [self._sample(packet) for packet in self._context.encode(None)]

    def _sample(self, packet: av.Packet) -> EncodedSample:
        timestamp_us = int(packet.pts or 0)
        key = bool(packet.is_keyframe)
        return EncodedSample(
            kind="video",
            timestamp_us=timestamp_us,
            payload=annexb_to_avcc(bytes(packet)),
            key=key,
            properties=loc_properties(self.epoch_us + timestamp_us, video_config=self.avcc if key else None),
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
        self.epoch_us = 0  # as VideoEncoder.epoch_us

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
                samples.append(
                    EncodedSample(
                        kind="audio",
                        timestamp_us=timestamp,
                        payload=bytes(packet),
                        key=True,
                        properties=loc_properties(self.epoch_us + timestamp),
                    )
                )

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
        phase_x, phase_y = (float(Fraction(index, fps) * rate % 2) for rate in BOUNCE_RATES)
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


def bounce_frames() -> Iterator[tuple[av.AudioFrame, int]]:
    """An 80 ms beep at each wall hit, with silence between hits, on the pattern's timeline."""
    offset = 0

    while True:
        positions = np.arange(OPUS_FRAME_SAMPLES) + offset
        # Integer phase arithmetic keeps even fractional collision times aligned indefinitely.
        # The initial corner is a starting position, not a collision. At a corner hit, play once.
        age = np.full(OPUS_FRAME_SAMPLES, np.inf)
        for rate in BOUNCE_RATES:
            phase = positions * rate.numerator
            period = OPUS_RATE * rate.denominator
            since_hit = (phase % period) / (OPUS_RATE * rate.numerator)
            age = np.minimum(age, np.where(phase >= period, since_hit, np.inf))

        active = age < 0.08
        t = np.where(active, age, 0.0)
        # Short attack/release ramps avoid clicks while preserving a clear onset.
        envelope = np.minimum(np.clip(t / 0.003, 0, 1), np.clip((0.08 - t) / 0.005, 0, 1))
        mono = (np.sin(2 * math.pi * 880 * t) * envelope * active * 0.2 * 32767).astype(np.int16)
        interleaved = np.repeat(mono, 2).reshape(1, -1)
        frame = av.AudioFrame.from_ndarray(interleaved, format="s16", layout="stereo")
        frame.sample_rate = OPUS_RATE
        yield frame, offset * MICROSECONDS_PER_SECOND // OPUS_RATE
        offset += OPUS_FRAME_SAMPLES


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
    def __init__(self, config: Config, loop: asyncio.AbstractEventLoop, queues: dict[str, LiveQueue]):
        self._config = config
        self._loop = loop
        self._queues = queues
        self._stop = threading.Event()
        # Set from the event loop by request_keyframes when a relay subscription starts.
        self.keyframe_requested = threading.Event()
        # A file plays at its own frame rate, so the GOP is counted in its frames, not --fps's.
        fps = Fraction(config.fps) if config.input is None else input_frame_rate(config.input, Fraction(config.fps))
        self.video = VideoEncoder(
            config.width, config.height, fps, config.bitrate, config.gop_seconds, config.rate_control
        )
        self.audio: AudioEncoder | None = None
        self.error: BaseException | None = None
        self._thread = threading.Thread(target=self._run, name="media-producer", daemon=True)

        if config.audio == "tone" or (config.audio == "auto" and (config.input is None or self._input_has_audio())):
            self.audio = AudioEncoder()

    def _input_has_audio(self) -> bool:
        assert self._config.input is not None

        with av.open(str(self._config.input)) as container:
            return len(container.streams.audio) > 0

    def start(self) -> None:
        # Media time 0 is due now: pin it to the system clock so every track is stamped in epoch microseconds.
        epoch_us = time.time_ns() // 1000
        self.video.epoch_us = epoch_us
        if self.audio is not None:
            self.audio.epoch_us = epoch_us
        for queue in self._queues.values():
            queue.started = time.monotonic()
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    async def join(self) -> None:
        await asyncio.to_thread(self._thread.join)

    def _handoff(self, samples: list[EncodedSample] | None) -> None:
        # Wait for this callback before scheduling another. Merely bounding asyncio.Queue leaves
        # call_soon_threadsafe's callback queue unbounded when encoding outpaces the event loop.
        delivered = threading.Event()

        def deliver() -> None:
            try:
                if samples is None:
                    for queue in self._queues.values():
                        queue.put_nowait(None)
                else:
                    for sample in samples:
                        self._queues[sample.kind].put_nowait(sample)
            finally:
                delivered.set()

        self._loop.call_soon_threadsafe(deliver)
        delivered.wait()

    def _emit(self, samples: list[EncodedSample]) -> None:
        if samples:
            self._handoff(samples)

    def _run(self) -> None:
        config = self._config
        started = self._queues["video"].started
        assert started is not None

        try:
            if config.input is not None:
                frames: Iterator[tuple[Any, int]] = file_frames(
                    config.input, config.width, config.height, loop=config.loop, with_audio=self.audio is not None
                )
            else:
                video = pattern_frames(config.width, config.height, config.fps)
                audio = None
                if self.audio is not None:
                    audio = tone_frames() if config.audio == "tone" else bounce_frames()
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
                    # Frames already queued still go first; the writer skips the deltas ahead of
                    # this keyframe, so the wait stays within the video queue's max_lag.
                    keyframe = self.keyframe_requested.is_set()

                    if keyframe:
                        self.keyframe_requested.clear()

                    self._emit(self.video.encode(frame, keyframe=keyframe))
                elif self.audio is not None:
                    self._emit(self.audio.encode(frame, timestamp_us))

            self._emit(self.video.flush())
        except BaseException as error:
            self.error = error
        finally:
            self._handoff(None)


class DemandSource(Protocol):
    def demand_changes(self) -> AsyncIterator[bool]: ...


async def request_keyframes(track: DemandSource, requested: threading.Event) -> None:
    """Ask the producer for a keyframe whenever the relay subscribes to ``track``, as the TS webcodecs example does.

    The video writer opens a group only at a keyframe, so without this a new subscription waits for
    the encoder's next scheduled one: up to a whole GOP. The initial snapshot needs none (the
    encoder starts with a keyframe), and ``False`` needs none; a repeated ``True`` is a replacement
    subscription and gets its own.
    """
    changes = track.demand_changes()
    await anext(changes, None)

    async for demanded in changes:
        if demanded:
            requested.set()


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
#
# The catalog service and the frame writer come from the SDK (`mux_relay.media`). What stays here is
# this program's own policy: the live queue's time-based dropping, what to do when a track ends, how
# the run's exit code is chosen, and the counters the `finished` line reports.


class TrackEnded(RuntimeError):
    """A published track was retired mid-run. Raised through the lifetime so the run exits non-zero."""


@dataclass(slots=True)
class Stats:
    started: float | None = None
    finished: float | None = None
    encoded_video_bytes: int = 0
    encoded_audio_bytes: int = 0
    published_video_bytes: int = 0
    published_audio_bytes: int = 0
    video_groups: int = 0
    video_objects: int = 0
    audio_objects: int = 0
    skipped_video: int = 0
    skipped_audio: int = 0
    dropped_video: int = 0
    dropped_audio: int = 0
    discarded_video: int = 0
    discarded_audio: int = 0
    stale_video: int = 0
    stale_audio: int = 0
    max_lag_seconds: float = 0.0
    catalog_sends: int = 0
    catalog_failures: int = 0
    timeline_entries: int = 0
    timeline_sends: int = 0
    timeline_failures: int = 0
    ended_tracks: dict[str, str] = field(default_factory=dict)


# Called once per track that the relay retired under us, with the failure that ended it.
TrackEndHandler = Callable[[str, RelayError], None]


def discard_sample(sample: EncodedSample, stats: Stats) -> None:
    """Count a sample the run abandoned: not skipped for want of demand, not lost with a track."""
    if sample.kind == "audio":
        stats.discarded_audio += 1
    else:
        stats.discarded_video += 1


def stale_sample(sample: EncodedSample, stats: Stats) -> None:
    """Count media abandoned to keep the publisher near the live edge."""
    if sample.kind == "audio":
        stats.stale_audio += 1
    else:
        stats.stale_video += 1


def discard_queue(queue: asyncio.Queue[EncodedSample | None], stats: Stats) -> None:
    """Count everything the encoder handed over that a stopping run will never publish."""
    while not queue.empty():
        sample = queue.get_nowait()

        if sample is not None:
            discard_sample(sample, stats)


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


def run_failure(
    *,
    producer_error: BaseException | None,
    media_failure: BaseException | None,
    cancel_reason: BaseException | None,
    track_end_reason: BaseException | None,
) -> BaseException | None:
    """The one exception that explains why the run is over, or None for a clean finish.

    Cancelling the lifetime for a retired track makes the SDK re-raise that very object out of
    every write in flight, so the media task usually fails with it too. Reporting that as a separate
    publishing failure would bury the track that ended. Likewise, a normal deadline's own
    cancellation is a clean stop. Match by identity so an unrelated failure still fails the run.
    """
    if producer_error is not None:
        return _caused_by(RuntimeError(f"media pipeline failed: {describe(producer_error)}"), producer_error)

    # The media loop ending on its own is either the source running out (a file without --loop) or
    # a failure; a failure must not read as a clean finish.
    normal_stop = isinstance(cancel_reason, Cancelled) and media_failure is cancel_reason
    if media_failure is not None and media_failure is not track_end_reason and not normal_stop:
        return _caused_by(RuntimeError(f"publishing failed: {describe(media_failure)}"), media_failure)

    # A deadline cancels with the default reason; anything else (a connection loss, a retired track)
    # is a failure.
    if cancel_reason is not None and not isinstance(cancel_reason, Cancelled):
        return cancel_reason

    return None


def _caused_by(error: BaseException, cause: BaseException) -> BaseException:
    """What ``raise error from cause`` records, for a failure chosen before it is raised."""
    error.__cause__ = cause
    return error


def stale_for(queue: LiveQueue) -> Callable[[EncodedSample], bool]:
    """This program's time policy as the frame writer's ``stale`` predicate.

    A sample older than one GOP (video) or 250 ms (audio) against the producer clock is not worth
    sending. The frame writer asks before opening a group and again before each write, so a write
    that blocked for a while drops what went stale behind it.
    """
    return lambda sample: queue.lag(sample) > queue.max_lag


async def end_broadcast(track: RelayTrackWriter, log: Log) -> None:
    """Tell the catalog's current viewers the broadcast is over (msf-01 §11.3).

    Only for a deliberate stop, after the media tracks closed (each close sends PUBLISH_DONE Track
    Ended). Without demand nobody is listening, and a write would wait for a subscription.
    """
    if not track.has_demand:
        return

    try:
        await track.write(build_msf_end_catalog(generated_at_ms=int(time.time() * 1000)))
    except RelayError as error:
        log("end-of-broadcast catalog not sent", {"error": describe(error)})
    else:
        log("end-of-broadcast catalog sent", {})


async def serve_catalog(catalog: CatalogService, stats: Stats, on_track_end: TrackEndHandler) -> None:
    """Run the SDK's catalog service, and fold its end and its counters into the run's accounting."""
    try:
        await catalog.serve()
    except RelayError as error:
        on_track_end(CATALOG_TRACK, error)
    finally:
        stats.catalog_sends = catalog.sends
        stats.catalog_failures = catalog.failures


async def serve_timeline(timeline: DocumentService, stats: Stats, on_track_end: TrackEndHandler) -> None:
    """Run the SDK's document service for the media timeline, folding its end and counters in like the catalog's."""
    try:
        await timeline.serve()
    except RelayError as error:
        on_track_end(MEDIA_TIMELINE_TRACK, error)
    finally:
        stats.timeline_sends = timeline.sends
        stats.timeline_failures = timeline.failures


# Called with each video keyframe the frame writer sent and the group it opened: the media timeline's cadence.
GroupHandler = Callable[[EncodedSample, int], None]


def account(
    sample: EncodedSample, outcome: FrameOutcome, writer: FrameWriter[EncodedSample], stats: Stats, log: Log
) -> None:
    """Put one sample in exactly one counter, from what the frame writer said became of it."""
    video = sample.kind == "video"

    if outcome.kind == "sent":
        if video:
            if stats.video_objects == 0:
                log("video flowing", {"group": outcome.group_id})

            stats.video_objects += 1
            stats.published_video_bytes += len(sample.payload)
        else:
            stats.audio_objects += 1
            stats.published_audio_bytes += len(sample.payload)
    elif outcome.kind == "stale":
        stale_sample(sample, stats)
    elif outcome.kind == "ended":
        # The track is gone for good; these samples are lost, not deferred.
        if video:
            stats.dropped_video += 1
        else:
            stats.dropped_audio += 1
    else:
        # Skipped for want of demand, or interrupted: the track is fine, and the next keyframe (or
        # the next audio frame) tries again.
        if video:
            stats.skipped_video += 1
        else:
            stats.skipped_audio += 1

        if outcome.kind == "interrupted":
            log(f"{sample.kind} group interrupted", {"group": outcome.group_id, "error": describe(outcome.error)})

    if video:
        stats.video_groups = writer.groups


async def publish_media(
    queue: LiveQueue,
    writer: FrameWriter[EncodedSample],
    stats: Stats,
    log: Log,
    on_track_end: TrackEndHandler,
    lifetime: CancellationToken,
    on_group: GroupHandler | None = None,
) -> None:
    """Feed one track's queue to its frame writer until the encoder is done, counting every sample once.

    ``on_group`` hears of every keyframe that opened a group on the relay; skipped, stale and
    interrupted keyframes opened nothing a viewer could seek to.
    """
    track = VIDEO_TRACK if queue.kind == "video" else AUDIO_TRACK
    reported = False

    # The sample in hand that has not landed in a counter yet. Nothing awaits between counting a
    # sample and taking the next, so whatever is here when the run is torn down really did go
    # nowhere, whether the lifetime's cancel reason arrived or the task itself was cancelled.
    pending: EncodedSample | None = None

    try:
        while True:
            queue.pending = None
            sample = await queue.get()

            if sample is None:
                break

            pending = queue.pending = sample
            outcome = await writer.write(sample)
            account(sample, outcome, writer, stats, log)
            pending = None

            if on_group is not None and outcome.kind == "sent" and sample.key and outcome.group_id is not None:
                on_group(sample, outcome.group_id)

            if outcome.kind == "ended" and not reported:
                # Every later sample is `ended` too; the track-end policy runs once.
                reported = True
                assert outcome.error is not None
                on_track_end(track, outcome.error)
    except BaseException:
        if pending is not None:
            discard_sample(pending, stats)

        raise
    finally:
        queue.pending = None

        if queue.kind == "video":
            # A keyframe write cancelled by the shutdown may have opened a group `account` never saw.
            stats.video_groups = writer.groups

        try:
            await writer.close()
        except BaseException as error:
            # Cancelling the lifetime makes the SDK re-raise that same reason out of everything in
            # flight, this FIN included. `run` reports it once; raising it again from a teardown
            # would only replace the exception already on its way out. Anything else is a real bug.
            if error is not lifetime.reason:
                raise


async def publish_live_media(
    queues: dict[str, LiveQueue],
    writers: dict[str, FrameWriter[EncodedSample]],
    catalog: CatalogService,
    stats: Stats,
    log: Log,
    on_track_end: TrackEndHandler,
    lifetime: CancellationToken,
    timeline: DocumentService | None = None,
    on_video_group: GroupHandler | None = None,
) -> None:
    """Each track gets its own task so an audio FIN cannot hold up the video timeline."""

    async def report() -> None:
        while True:
            await asyncio.sleep(1)
            stats.catalog_sends, stats.catalog_failures = catalog.sends, catalog.failures
            if timeline is not None:
                stats.timeline_sends, stats.timeline_failures = timeline.sends, timeline.failures
            log(
                "media progress",
                {
                    "queuedVideo": queues["video"].qsize(),
                    "queuedAudio": queues["audio"].qsize(),
                    "videoLagSeconds": round(queues["video"].current_lag(), 3),
                    "audioLagSeconds": round(queues["audio"].current_lag(), 3),
                    **summarize(stats),
                },
            )

    publishing = [
        asyncio.create_task(
            publish_media(
                queues[kind],
                writer,
                stats,
                log,
                on_track_end,
                lifetime,
                on_group=on_video_group if kind == "video" else None,
            )
        )
        for kind, writer in writers.items()
    ]
    reporter = asyncio.create_task(report())
    try:
        # gather preserves the original failure object used by run_failure (TaskGroup wraps it).
        await asyncio.gather(*publishing)
    finally:
        for task in [*publishing, reporter]:
            task.cancel()
        try:
            await asyncio.gather(*publishing, reporter, return_exceptions=True)
        finally:
            # gather raises the first failure. A normal deadline may win that race while
            # another track has failed independently. Inspect the tasks even if run() also
            # cancels this parent during its drain, so that failure cannot disappear.
            for task in publishing:
                error = task.exception() if task.done() and not task.cancelled() else None
                if error is not None and error is not lifetime.reason:
                    raise error


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
    keyframe_times_ms: dict[int, int] = field(default_factory=dict)
    """Each video group's first-object timestamp, floored to milliseconds as a timeline entry states it."""
    timeline_objects: int = 0
    timeline: tuple[MediaTimelineEntry, ...] = ()
    """The newest media timeline document received."""
    timeline_max_skew_ms: int | None = None
    """The largest distance between a document's newest wall-clock time and when it arrived."""
    problems: list[str] | None = None


async def verify(
    config: Config, publisher: RelayPublisher, lifetime: CancellationToken, log: Log
) -> tuple[Any, Verified]:
    """Subscribe like a player would and record what arrives, so the wire can be checked without a browser.

    The viewer is a supervised subscriber whose wants keep each track subscribed: they retry the
    announce race (a subscribe that reaches the relay before it routes the namespace, about a round
    trip after the announce reached the wire) and resubscribe after a publisher's done, as a player's
    would.
    """
    result = Verified(video_groups=set(), problems=[])
    assert result.problems is not None
    # Bounded, connection included: the run's own duration timer starts only after this returns, and a
    # supervised connection that keeps failing, an announce that no solicitation covers, or a relay that
    # keeps refusing the namespace would otherwise hold the media back forever.
    deadline = asyncio.get_running_loop().time() + VERIFY_STARTUP_TIMEOUT
    try:
        async with asyncio.timeout_at(deadline):
            subscriber = await connect_supervised_subscriber(
                config.url, get_token=lambda: config.token, cancellation=lifetime
            )
    except TimeoutError:
        # Verification is diagnostic: record it and let the media start.
        result.problems.append(f"verify: subscriber not connected within {VERIFY_STARTUP_TIMEOUT:g} s")
        return None, result

    assert result.video_groups is not None
    problems = result.problems
    groups = result.video_groups

    def on_catalog(obj: MoqtObject) -> None:
        if obj.status == "normal" and obj.payload:
            result.catalog = obj.payload
            try:
                catalog = parse_msf_catalog(obj.payload, catalog_namespace=config.namespace)
            except ValueError as error:
                problems.append(f"catalog: {error}")
                return

            tracks = [
                {
                    key: value
                    for key, value in {
                        "name": track.name,
                        "packaging": track.packaging,
                        "codec": track.codec,
                        "size": f"{track.width}x{track.height}" if track.width and track.height else None,
                        "samplerate": track.samplerate,
                        "initBytes": len(track.init_data) if track.init_data else None,
                    }.items()
                    if value is not None
                }
                for track in catalog.tracks
            ]
            log("verify: catalog received", {"bytes": len(obj.payload), "group": obj.group_id, "tracks": tracks})

    def on_video(obj: MoqtObject) -> None:
        if obj.status != "normal":
            return

        result.video_objects += 1
        groups.add(obj.group_id)
        # The relay may rescale timestamps; its SUBSCRIBE_OK declares the Timescale it delivers.
        timestamp = loc_timestamp_us(obj.properties, track_properties=track_properties.get(VIDEO_TRACK, ()))

        if timestamp is None:
            problems.append("video object without a LOC timestamp")
            return

        if obj.object_id == 0:
            result.video_keyframes += 1
            result.keyframe_times_ms[obj.group_id] = timestamp // 1000

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

    def on_timeline(obj: MoqtObject) -> None:
        if obj.status != "normal":
            return

        arrived_ms = time.time_ns() // 1_000_000
        result.timeline_objects += 1

        try:
            entries = parse_media_timeline(obj.payload)
        except ValueError as error:
            problems.append(f"timeline: {error}")
            return

        problems.extend(f"timeline: {problem}" for problem in timeline_problems(entries))

        if entries:
            skew = abs(arrived_ms - entries[-1].wallclock_ms)
            result.timeline_max_skew_ms = max(skew, result.timeline_max_skew_ms or 0)

            if skew > TIMELINE_SKEW_LIMIT_MS:
                problems.append(f"timeline: newest wall-clock time {skew} ms from its arrival")

        result.timeline = entries

    subscribed: set[str] = set()
    track_properties: dict[str, list[KeyValuePair]] = {}
    all_subscribed = asyncio.Event()

    def on_error(track: str) -> Callable[[BaseException], None]:
        def failed(error: BaseException) -> None:
            detail = (
                f"{error.request_error.error_code} {error.request_error.reason}"
                if isinstance(error, SubscriptionRequestError)
                else describe(error)
            )
            problems.append(f"{track}: subscribe error {detail}")

        return failed

    def on_subscribed(track: str) -> Callable[[Any, tuple[str, ...]], None]:
        def accepted(info: Any, _namespace: tuple[str, ...]) -> None:
            track_properties[track] = list(getattr(info, "track_properties", ()))
            subscribed.add(track)
            if len(subscribed) == len(tracks):
                all_subscribed.set()

        return accepted

    def on_restarted(track: str) -> Callable[[str, tuple[str, ...]], None]:
        def restarted(reason: str, _namespace: tuple[str, ...]) -> None:
            log("verify: resubscribing", {"track": track, "reason": reason})

        return restarted

    tracks = [(CATALOG_TRACK, on_catalog), (VIDEO_TRACK, on_video), (MEDIA_TIMELINE_TRACK, on_timeline)]

    if config.audio != "off":
        tracks.append((AUDIO_TRACK, on_audio))

    for track, on_object in tracks:
        subscriber.want(
            track,
            config.namespace,
            track,
            WantHandlers(
                on_subscribed=on_subscribed(track),
                on_object=on_object,
                on_error=on_error(track),
                on_restarted=on_restarted(track),
            ),
        )

    try:
        async with asyncio.timeout_at(deadline):
            await publisher.announced
            log("verify: namespace announced", {"namespace": "/".join(config.namespace)})
            await all_subscribed.wait()
    except TimeoutError:
        # Verification is diagnostic: record it and let the media start.
        problems.append(
            f"verify: subscribes not accepted within {VERIFY_STARTUP_TIMEOUT:g} s of starting "
            "(announce pending, or the namespace still unknown to the relay)"
        )

    return subscriber, result


def timeline_problems(entries: Sequence[MediaTimelineEntry]) -> list[str]:
    """What is wrong with one media timeline document on its own: its size, and its order."""
    problems: list[str] = []

    if len(entries) > TIMELINE_MAX_ENTRIES:
        problems.append(f"{len(entries)} entries, more than the {TIMELINE_MAX_ENTRIES} retained")

    for earlier, later in itertools.pairwise(entries):
        if later.group <= earlier.group or later.media_time_ms < earlier.media_time_ms:
            problems.append(f"entry for group {later.group} out of order after group {earlier.group}")
            break

    return problems


def timeline_mismatches(verified: Verified) -> list[str]:
    """Entries that disagree with the video the viewer received: an unseen group, or another media time."""
    problems: list[str] = []
    # The newest entry can go out just before its keyframe arrives; only a gap behind the viewer counts.
    newest = max(verified.keyframe_times_ms, default=-1)

    for entry in verified.timeline:
        seen = verified.keyframe_times_ms.get(entry.group)

        if seen is None and entry.group > newest:
            continue

        if seen is None:
            problems.append(f"timeline: group {entry.group} has no video keyframe received")
        elif entry.media_time_ms != seen or entry.object != 0:
            problems.append(
                f"timeline: group {entry.group} says {entry.media_time_ms} ms at object {entry.object}, "
                f"its keyframe is {seen} ms"
            )

    return problems


# ---------------------------------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------------------------------


async def run(config: Config, stats: Stats, log: Log) -> None:
    lifetime = CancellationToken()
    loop = asyncio.get_running_loop()
    queues = {
        "video": LiveQueue("video", stats, max_samples=120, max_bytes=16 * 1024 * 1024, max_lag=config.gop_seconds),
        "audio": LiveQueue("audio", stats, max_samples=100, max_bytes=1024 * 1024, max_lag=0.25),
    }
    producer = Producer(config, loop, queues)
    closing = False
    stopped: asyncio.Future[None] = loop.create_future()

    # Kept so a write killed by this very cancellation is reported as the track end it was, rather
    # than wrapped as a second, unrelated-looking publishing failure.
    track_end_reason: BaseException | None = None

    def stop_for_track_end(reason: BaseException) -> None:
        nonlocal track_end_reason
        track_end_reason = reason
        lifetime.cancel(reason)

    on_track_end = make_track_end_handler(config.on_track_end, stats, log, stop_for_track_end)

    # Registered before anything can cancel the lifetime: a publisher connection that drops during
    # setup cancels it, and a callback added afterwards would never run.
    def on_cancelled(reason: BaseException) -> None:
        if not stopped.done():
            stopped.set_result(None)

    lifetime.add_callback(on_cancelled)

    # The deadline is a deliberate stop: it ends the media but leaves the publisher connection open,
    # so the run can end the broadcast with the msf-01 §11.3 catalog. Anything else that ends the
    # lifetime ends the connection with it.
    deadline_reached = Cancelled("duration reached")
    connection = CancellationToken()

    def end_connection(reason: BaseException) -> None:
        if reason is not deadline_reached:
            connection.cancel(reason)

    lifetime.add_callback(end_connection)

    def watch(label: str, closed: RelayAwaitable[Closed]) -> None:
        def done(future: asyncio.Future[Closed]) -> None:
            if not closing:
                log(f"{label} connection closed", {"error": describe(future.result().error)})
                lifetime.cancel(future.result().error or RelayError(f"{label} connection closed", code="CLOSED"))

        closed.add_done_callback(done)

    publisher: RelayPublisher = await connect_relay_publisher(
        config.url, namespace=config.namespace, get_token=lambda: config.token, cancellation=connection
    )
    watch("publisher", publisher.closed)
    log("publisher connected", {"url": config.url, "namespace": "/".join(config.namespace)})

    video_info = VideoTrackInfo(
        codec=producer.video.codec,
        width=config.width,
        height=config.height,
        framerate=float(producer.video.fps),
        bitrate=config.bitrate,
        init_data=producer.video.avcc,
    )
    audio_info = AudioTrackInfo(bitrate=96_000) if producer.audio is not None else None
    document = build_msf_catalog(
        config.namespace,
        video=video_info,
        audio=audio_info,
        media_timeline=MediaTimelineTrackInfo(),
        generated_at_ms=int(time.time() * 1000),
    )
    log(
        "catalog",
        {"codec": video_info.codec, "size": f"{config.width}x{config.height}", "audio": audio_info is not None},
    )

    catalog_track = publisher.register_track(
        CATALOG_TRACK, track_properties=TRACK_TIMESCALE_PROPERTIES, write_timeout=5
    )
    # Media tracks get the media tier's delivery timeouts, so a stall costs stale frames rather than lasting latency.
    # The interval is the encoder's own, after rounding the GOP to whole frames.
    video_track = publisher.register_track(
        VIDEO_TRACK,
        track_properties=[
            *TRACK_TIMESCALE_PROPERTIES,
            *media_track_properties("video", keyframe_interval_us=producer.video.keyframe_interval_us),
        ],
        write_timeout=5,
    )
    timeline_track = publisher.register_track(
        MEDIA_TIMELINE_TRACK, track_properties=TRACK_TIMESCALE_PROPERTIES, write_timeout=5
    )
    audio_track = (
        publisher.register_track(
            AUDIO_TRACK,
            track_properties=[*TRACK_TIMESCALE_PROPERTIES, *media_track_properties("audio")],
            write_timeout=5,
        )
        if producer.audio is not None
        else None
    )

    print_player_url(config, log)

    # The SDK's media policy over the registered tracks; the queues' time policy rides in as `stale`.
    catalog = CatalogService(catalog_track, document, retry_delay=CATALOG_RETRY_DELAY)
    # The timeline's document waits for the first video group; the service sends nothing until then.
    timeline_service = DocumentService(timeline_track, retry_delay=CATALOG_RETRY_DELAY)
    writers: dict[str, FrameWriter[EncodedSample]] = {
        "video": FrameWriter(video_track, stale=stale_for(queues["video"])),
    }

    if audio_track is not None:
        writers["audio"] = FrameWriter(audio_track, grouping="frame", stale=stale_for(queues["audio"]))

    timeline = MediaTimeline(max_entries=TIMELINE_MAX_ENTRIES)

    def index_video_group(sample: EncodedSample, group_id: int) -> None:
        # The entry's media time is the keyframe's LOC timestamp, which on the epoch timeline is its capture time too.
        timestamp_us = loc_timestamp_us(sample.properties)
        assert timestamp_us is not None, "every sample carries its LOC timestamp"
        timeline_service.update(timeline.add(timestamp_us, group_id))
        stats.timeline_entries += 1

    tasks = [
        asyncio.ensure_future(serve_catalog(catalog, stats, on_track_end)),
        asyncio.ensure_future(serve_timeline(timeline_service, stats, on_track_end)),
        asyncio.ensure_future(request_keyframes(video_track, producer.keyframe_requested)),
        asyncio.ensure_future(
            publish_live_media(
                queues,
                writers,
                catalog,
                stats,
                log,
                on_track_end,
                lifetime,
                timeline=timeline_service,
                on_video_group=index_video_group,
            )
        ),
    ]
    verifier: Any = None
    verified: Verified | None = None

    if config.verify:
        verifier, verified = await verify(config, publisher, lifetime, log)

    stats.started = time.monotonic()
    producer.start()
    deadline = loop.call_later(config.duration, lambda: lifetime.cancel(deadline_reached)) if config.duration else None
    media_task = tasks[-1]
    stopped_normally = False
    failure: BaseException | None = None

    try:
        await asyncio.wait([stopped, media_task], return_when=asyncio.FIRST_COMPLETED)
        stopped_normally = True
    finally:
        closing = True

        if deadline is not None:
            deadline.cancel()

        producer.stop()
        await producer.join()

        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)

        # The producer is joined before accounting so even its final flush is included.
        for queue in queues.values():
            discard_queue(queue, stats)

        if verified is not None:
            report_verification(verified, log)

        if verifier is not None:
            verifier.close()

        # Inspect after joining/draining: failures that arrive in the same turn as a deadline
        # must not depend on which callback woke asyncio.wait first.
        if stopped_normally:
            failure = run_failure(
                producer_error=producer.error,
                media_failure=media_task.exception() if not media_task.cancelled() else None,
                cancel_reason=lifetime.reason,
                track_end_reason=track_end_reason,
            )

        video_track.close()
        timeline_track.close()

        if audio_track is not None:
            audio_track.close()

        # A clean finish (the deadline, or a file without --loop running out) ends the broadcast.
        # A failure did not end it on purpose, and Ctrl-C cancels the run before it could write.
        try:
            if stopped_normally and failure is None:
                await end_broadcast(catalog_track, log)
        finally:
            # Ctrl-C during that write must not skip the connection's shutdown.
            catalog_track.close()
            await publisher.close()
            stats.finished = time.monotonic()

    if failure is not None:
        raise failure


def summarize(stats: Stats) -> dict[str, Any]:
    """The `finished` line: every sample the run produced is in exactly one of these counters."""
    elapsed = (stats.finished or time.monotonic()) - stats.started if stats.started is not None else 0

    def kbps(size: int) -> float:
        return round(size * 8 / elapsed / 1000, 1) if elapsed > 0 else 0.0

    return {
        "encodedVideoKbps": kbps(stats.encoded_video_bytes),
        "encodedAudioKbps": kbps(stats.encoded_audio_bytes),
        "publishedVideoKbps": kbps(stats.published_video_bytes),
        "publishedAudioKbps": kbps(stats.published_audio_bytes),
        "videoGroups": stats.video_groups,
        "videoObjects": stats.video_objects,
        "audioObjects": stats.audio_objects,
        "skippedVideo": stats.skipped_video,
        "skippedAudio": stats.skipped_audio,
        "droppedVideo": stats.dropped_video,
        "droppedAudio": stats.dropped_audio,
        "discardedVideo": stats.discarded_video,
        "discardedAudio": stats.discarded_audio,
        "staleVideo": stats.stale_video,
        "staleAudio": stats.stale_audio,
        "maxLagSeconds": round(stats.max_lag_seconds, 3),
        "catalogSends": stats.catalog_sends,
        "catalogFailures": stats.catalog_failures,
        "timelineEntries": stats.timeline_entries,
        "timelineSends": stats.timeline_sends,
        "timelineFailures": stats.timeline_failures,
        "endedTracks": ",".join(stats.ended_tracks) or None,
    }


def report_verification(verified: Verified, log: Log) -> None:
    span = (
        (verified.last_timestamp_us - verified.first_timestamp_us) / MICROSECONDS_PER_SECOND
        if verified.first_timestamp_us is not None and verified.last_timestamp_us is not None
        else 0.0
    )
    problems = [*(verified.problems or ()), *timeline_mismatches(verified)]

    if verified.video_keyframes and not verified.timeline_objects:
        problems.append("timeline: no document received")
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
            "timelineObjects": verified.timeline_objects,
            "timelineEntries": len(verified.timeline),
            "timelineMaxSkewMs": verified.timeline_max_skew_ms,
            "problems": problems or "none",
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
    parser.add_argument(
        "--bitrate",
        type=int,
        default=1_500_000,
        help="Video bitrate target in bits/s (default 1500000; ABR can undershoot)",
    )
    parser.add_argument(
        "--rate-control",
        choices=["abr", "cbr"],
        default="abr",
        help="abr: content-dependent target (default); cbr: pad toward --bitrate for bandwidth tests",
    )
    parser.add_argument("--gop", type=float, default=1.0, help="Keyframe interval in seconds (default 1)")
    parser.add_argument(
        "--audio",
        choices=["auto", "tone", "off"],
        default="auto",
        help="auto: the input file's audio as Opus, wall-hit beeps for the pattern; "
        "tone: a 440 Hz sine; off: video only",
    )
    parser.add_argument("--duration", type=float, help="Stop after this many seconds (default: run until Ctrl-C)")
    parser.add_argument(
        "--on-track-end",
        choices=["stop", "continue"],
        default="stop",
        help="What to do when a track closes mid-run: stop: fail the run (default); "
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
        rate_control=args.rate_control,
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
        {
            "namespace": "/".join(config.namespace),
            "input": config.input or "test pattern",
            "audio": config.audio,
            "bitrate": config.bitrate,
            "rateControl": config.rate_control,
        },
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
