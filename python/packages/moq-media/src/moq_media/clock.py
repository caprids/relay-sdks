"""One timeline per broadcast, and a playout clock for audio/video sync.

Neither clock touches a transport or an output device: both read an injected monotonic microsecond source. See
``spec/media-layer.md``, "Shared timeline and playout clock".
"""

import math
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

from .loc import MICROSECONDS_PER_SECOND

PlayoutAction = Literal["wait", "show", "drop"]
"""What a video renderer does with one frame."""
PlayoutMode = Literal["idle", "audio", "wall"]
"""What a playout clock is paced by."""
_SAFE_INTEGER = 2**53 - 1


def _monotonic_us() -> int:
    return time.monotonic_ns() // 1000


def _finite(value: float, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"{name} must be a finite number")
    return value


def _integer(value: int, name: str, minimum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= _SAFE_INTEGER:
        raise ValueError(f"{name} must be a safe integer of at least {minimum}")
    return value


def _round(value: float) -> int:
    """Round to the nearest integer, ties upward, as JavaScript's ``Math.round`` does."""
    return value if isinstance(value, int) else math.floor(value + 0.5)


@dataclass(frozen=True, slots=True)
class SampleTimeline:
    """Maps an audio sample counter that starts at zero onto the broadcast timeline."""

    sample_rate: int
    first_sample_us: int

    def timestamp_us(self, sample_index: int) -> int:
        """Return the broadcast time of the sample at this index: whole microseconds after the first sample."""
        _integer(sample_index, "sample_index", 0)
        return self.first_sample_us + sample_index * MICROSECONDS_PER_SECOND // self.sample_rate


class SourceTimeline:
    """Maps timestamps from a clock with its own origin and rate onto the broadcast timeline.

    Calibrate it with measured pairs of source time and time on the broadcast clock's own source; without a
    calibration, the first timestamp mapped is anchored at the clock's ``now_us()``.
    """

    def __init__(self, clock: "BroadcastClock", tolerance_us: int) -> None:
        self._clock = clock
        self._tolerance_us = tolerance_us
        self._anchor: tuple[float, int] | None = None

    @property
    def anchor_source_us(self) -> float | None:
        """The source time of the current anchor; ``None`` until the first calibration or timestamp."""
        return None if self._anchor is None else self._anchor[0]

    def calibrate(self, source_us: int, *, at_us: int | None = None) -> bool:
        """Record that source time ``source_us`` happened at ``at_us`` on the clock's own source (default: now).

        The pair becomes the anchor when there is none, or when the current anchor predicts it more than the
        tolerance off: the source drifted, or jumped. Returns whether the anchor changed.
        """
        _finite(source_us, "source_us")
        measured = self._clock.now_us() if at_us is None else self._clock.from_source_us(at_us)
        anchor = self._anchor
        if anchor is not None and abs(anchor[1] + _round(source_us - anchor[0]) - measured) <= self._tolerance_us:
            return False
        self._anchor = (source_us, measured)
        return True

    def timestamp_us(self, source_us: int) -> int:
        """Return the broadcast time of this source timestamp, rounded to whole microseconds.

        Without an anchor, the first call anchors this timestamp at the clock's ``now_us()``.
        """
        _finite(source_us, "source_us")
        if self._anchor is None:
            self._anchor = (source_us, self._clock.now_us())
        anchor_source, anchor_timestamp = self._anchor
        return anchor_timestamp + _round(source_us - anchor_source)


class BroadcastClock:
    """A publisher's single clock: every track in the broadcast is stamped from it, in microseconds since its origin.

    ``now_us`` is the time source in microseconds, ``time.monotonic_ns() // 1000`` by default. ``origin_us`` is the
    broadcast's zero on that source in integer microseconds, at or before the first capture; it defaults to the
    source's time now, rounded to whole microseconds. ``origin_epoch_us`` is that origin as Unix-epoch microseconds;
    it defaults to ``time.time_ns()`` at creation less the source time elapsed since the origin, read once.
    """

    def __init__(
        self,
        *,
        now_us: Callable[[], int] | None = None,
        origin_us: int | None = None,
        origin_epoch_us: int | None = None,
    ) -> None:
        self._source = now_us or _monotonic_us
        created = _finite(self._source(), "now_us")
        origin = _round(created) if origin_us is None else origin_us
        self._origin_us = _integer(origin, "origin_us", -_SAFE_INTEGER)
        if origin_epoch_us is None:
            origin_epoch_us = _round(time.time_ns() // 1000 - (created - self._origin_us))
        self._origin_epoch_us = _integer(origin_epoch_us, "origin_epoch_us", 0)

    @property
    def origin_us(self) -> int:
        """The broadcast's zero, on the source clock."""
        return self._origin_us

    @property
    def origin_epoch_us(self) -> int:
        """The broadcast's zero as microseconds since the Unix epoch: the wall-clock time media timeline entries use."""
        return self._origin_epoch_us

    def now_us(self) -> int:
        """Return the microseconds since the broadcast started."""
        return self.from_source_us(self._source())

    def from_source_us(self, source_us: int) -> int:
        """Map a timestamp on this clock's own source to the broadcast timeline, rounded to whole microseconds.

        A time before the origin maps to a negative number, which the LOC helpers refuse. For a capture clock with
        another origin, use :meth:`source_timeline`.
        """
        return _round(_finite(source_us, "source_us") - self._origin_us)

    def source_timeline(self, *, tolerance_us: int = 5_000) -> SourceTimeline:
        """Map a clock with its own origin and rate, kept on the broadcast timeline by calibration.

        An ``AudioContext``-style clock, or capture timestamps from another origin. ``tolerance_us`` is how far a
        calibration may disagree with the current anchor before it replaces it.
        """
        if _finite(tolerance_us, "tolerance_us") < 0:
            raise ValueError("tolerance_us must not be negative")
        return SourceTimeline(self, tolerance_us)

    def sample_timeline(self, sample_rate: int, *, first_sample_us: int | None = None) -> SampleTimeline:
        """Anchor a sample counter at ``first_sample_us`` on the timeline (default: now).

        The device clock that drives the counter drifts against the source, so re-anchor with a fresh timeline
        when the two disagree by more than a frame.
        """
        _integer(sample_rate, "sample_rate", 1)
        first = self.now_us() if first_sample_us is None else first_sample_us
        return SampleTimeline(sample_rate, _integer(first, "first_sample_us", -_SAFE_INTEGER))


@dataclass(frozen=True, slots=True)
class PlayoutDecision:
    """What to do with one video frame.

    ``offset_us`` is the frame's timestamp minus the clock's position (positive: early). ``wait_us`` is how long to
    hold the frame before asking again, and is 0 unless ``action`` is ``"wait"``.
    """

    action: PlayoutAction
    offset_us: int
    wait_us: int = 0


@dataclass(frozen=True, slots=True)
class _Anchor:
    media_us: float
    at_us: float
    received_us: float | None = None  # when an audio report arrived; silence is measured from here, not at_us


class PlayoutClock:
    """A player's clock for one broadcast's timeline. Audio is the master; video asks it when each frame is due.

    ``late_drop_us`` (default 40 ms, EBU R37's audio-early limit) is how late a frame may be shown;
    ``max_video_delay_us`` (default 1 s) is how far a frame may lead before the clock re-syncs;
    ``audio_timeout_us`` (default 500 ms) is how long audio may stay silent before wall-clock pacing takes over.
    ``now_us`` is the time source, ``time.monotonic_ns() // 1000`` by default.
    """

    def __init__(
        self,
        *,
        now_us: Callable[[], int] | None = None,
        late_drop_us: int = 40_000,
        max_video_delay_us: int = 1_000_000,
        audio_timeout_us: int = 500_000,
    ) -> None:
        if _finite(late_drop_us, "late_drop_us") < 0:
            raise ValueError("late_drop_us must not be negative")
        if _finite(max_video_delay_us, "max_video_delay_us") <= 0:
            raise ValueError("max_video_delay_us must be positive")
        if _finite(audio_timeout_us, "audio_timeout_us") <= 0:
            raise ValueError("audio_timeout_us must be positive")
        self._source = now_us or _monotonic_us
        self._late_drop_us = late_drop_us
        self._max_video_delay_us = max_video_delay_us
        self._audio_timeout_us = audio_timeout_us
        self._anchor: _Anchor | None = None

    def _mode_at(self, now: float) -> PlayoutMode:
        anchor = self._anchor
        if anchor is None:
            return "idle"
        received = anchor.received_us
        return "audio" if received is not None and now - received <= self._audio_timeout_us else "wall"

    def _position_at(self, now: float) -> float | None:
        anchor = self._anchor
        return None if anchor is None else anchor.media_us + (now - anchor.at_us)

    @property
    def mode(self) -> PlayoutMode:
        """``idle`` before anything anchors it, ``audio`` while audio reports arrive, ``wall`` otherwise."""
        return self._mode_at(self._source())

    def report_audio(self, timestamp_us: int, *, at_us: int | None = None) -> None:
        """Report that the audio with this timeline timestamp is audible at local time ``at_us`` (default: now).

        That is what was handed to the output, less the jitter-buffer depth, the output latency, and any extra path.
        """
        media = _finite(timestamp_us, "timestamp_us")
        received = self._source()
        at = received if at_us is None else _finite(at_us, "at_us")
        self._anchor = _Anchor(media, at, received)

    def stop_audio(self) -> None:
        """Audio is muted or ended: pace by wall clock from the current position."""
        now = self._source()
        position = self._position_at(now)
        if self._mode_at(now) == "audio" and position is not None:
            self._anchor = _Anchor(position, now)

    def due(self, timestamp_us: int) -> PlayoutDecision:
        """Decide whether the frame with this timeline timestamp should wait, be shown now, or be dropped."""
        _finite(timestamp_us, "timestamp_us")
        now = self._source()
        mode = self._mode_at(now)
        position = self._position_at(now)
        if mode == "idle" or position is None:
            self._anchor = _Anchor(timestamp_us, now)
            return PlayoutDecision("show", 0)
        offset = timestamp_us - position
        offset_us = _round(offset)
        if mode == "wall" and abs(offset) > self._max_video_delay_us:
            # A timeline jump or a stall: follow the video rather than drop or hold every later frame.
            self._anchor = _Anchor(timestamp_us, now)
            return PlayoutDecision("show", offset_us)
        if offset > self._max_video_delay_us:
            # Audio stays the master: a frame that far ahead is not on its timeline, and holding it freezes the picture.
            return PlayoutDecision("show", offset_us)
        if offset > 0:
            return PlayoutDecision("wait", offset_us, math.ceil(offset))
        if offset >= -self._late_drop_us:
            return PlayoutDecision("show", offset_us)
        return PlayoutDecision("drop", offset_us)

    def position_us(self) -> float | None:
        """Return the timeline position now, or ``None`` while idle."""
        return self._position_at(self._source())

    def reset(self) -> None:
        """Forget every anchor, at a Restart, a Move, or a new catalog: the next frame anchors wall pacing."""
        self._anchor = None
