"""LOC draft-04 property helpers. No transport or encoder dependencies."""

from moq_core import KeyValuePair

LOC_TIMESTAMP = 0x10
LOC_TIMESCALE = 0x08
LOC_VIDEO_CONFIG = 0x0D
LOC_AUDIO_CONFIG = 0x0F
MICROSECONDS_PER_SECOND = 1_000_000

CATALOG_TRACK = "catalog"
VIDEO_TRACK = "video"
AUDIO_TRACK = "audio"
MEDIA_TIMELINE_TRACK = "timeline"

LOC_DOCUMENT = "draft-ietf-moq-loc-04"
MSF_DOCUMENT = "draft-ietf-moq-msf-01"


def loc_track_properties(timescale: int = MICROSECONDS_PER_SECOND) -> list[KeyValuePair]:
    """Declare timestamp units on a LOC track; returns fresh properties on every call."""
    _integer(timescale, "timescale", positive=True)
    return [KeyValuePair(LOC_TIMESCALE, timescale)]


def _integer(value: int, name: str, *, positive: bool = False) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not (int(positive) <= value <= 2**53 - 1):
        raise ValueError(f"{name} must be a {'positive' if positive else 'nonnegative'} safe integer")


TRACK_TIMESCALE_PROPERTIES = tuple(loc_track_properties())
"""Track properties declared on every media track: timestamps are microseconds."""


def loc_properties(
    timestamp_us: int,
    *,
    timescale: int = MICROSECONDS_PER_SECOND,
    video_config: bytes | None = None,
    audio_config: bytes | None = None,
) -> list[KeyValuePair]:
    """Package a microsecond timestamp; pass decoder configuration only on keyframes.

    Other timescales round to the nearest tick, with ties rounded upward.
    """
    _integer(timestamp_us, "timestamp_us")
    _integer(timescale, "timescale", positive=True)
    timestamp = (timestamp_us * timescale + MICROSECONDS_PER_SECOND // 2) // MICROSECONDS_PER_SECOND
    _integer(timestamp, "timestamp")
    properties = [KeyValuePair(LOC_TIMESTAMP, timestamp), KeyValuePair(LOC_TIMESCALE, timescale)]

    if video_config is not None:
        properties.append(KeyValuePair(LOC_VIDEO_CONFIG, bytes(video_config)))

    if audio_config is not None:
        properties.append(KeyValuePair(LOC_AUDIO_CONFIG, bytes(audio_config)))

    return properties
