"""The MSF Media Timeline track (msf-01 §7): records that map media time and a location to wall-clock time.

See ``spec/media-layer.md``, "Media Timeline track". Format only: the publisher decides which groups get an entry,
how many to retain, and when to send a document.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass

_SAFE_INTEGER = 2**53 - 1


@dataclass(frozen=True, slots=True, kw_only=True)
class MediaTimelineEntry:
    """One media timeline record, ``[media_time_ms, [group, object], wallclock_ms]`` on the wire (msf-01 §7.1.1)."""

    media_time_ms: int
    """Media time of the first sample in the referenced object, floored to whole milliseconds."""
    group: int
    object: int
    """The referenced object's location in the track the timeline indexes."""
    wallclock_ms: int
    """Milliseconds since the Unix epoch when that media was captured; 0 when unknown."""


def _whole(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= _SAFE_INTEGER:
        raise ValueError(f"{name} must be a nonnegative safe integer")
    return value


def media_timeline_entry(
    timestamp_us: int, group: int, *, object: int = 0, wallclock_us: int | None = None
) -> MediaTimelineEntry:
    """Return the record for an object whose LOC timestamp on the broadcast timeline is ``timestamp_us``.

    ``wallclock_us`` is when that media was captured, as Unix-epoch microseconds. It defaults to ``timestamp_us``,
    since the broadcast timeline is anchored at the epoch; pass 0 for unknown, as msf-01 allows.
    """
    _whole(timestamp_us, "timestamp_us")
    wallclock = timestamp_us if wallclock_us is None else _whole(wallclock_us, "wallclock_us")
    return MediaTimelineEntry(
        media_time_ms=timestamp_us // 1000,
        group=_whole(group, "group"),
        object=_whole(object, "object"),
        wallclock_ms=wallclock // 1000,
    )


def build_media_timeline(entries: Iterable[MediaTimelineEntry]) -> bytes:
    """Return a timeline object's payload: the records in the order given, as compact JSON.

    The same form serves an independent document (every retained record) and an incremental one (only new ones).
    """
    records = [
        [
            _whole(entry.media_time_ms, "media_time_ms"),
            [_whole(entry.group, "group"), _whole(entry.object, "object")],
            _whole(entry.wallclock_ms, "wallclock_ms"),
        ]
        for entry in entries
    ]
    return json.dumps(records, separators=(",", ":")).encode("utf-8")


def parse_media_timeline(payload: bytes) -> tuple[MediaTimelineEntry, ...]:
    """Read a timeline object's payload into its records, in document order.

    Raises ``ValueError`` on a payload that is not a UTF-8 JSON array. Records that do not read as a nonnegative
    safe-integer ``[number, [number, number], number]`` are skipped, a §7.4 template among them; extra items are
    ignored. MSF_COMPRESSION payloads are not decompressed.
    """
    raw = json.loads(payload.decode("utf-8"), parse_constant=_reject_constant)
    if not isinstance(raw, list):
        raise ValueError("media timeline is not a JSON array")
    return tuple(entry for record in raw if (entry := _parse_record(record)) is not None)


def _reject_constant(name: str) -> object:
    # JSON.parse rejects NaN and Infinity; so does this reader.
    raise ValueError(f"media timeline is not valid JSON: {name}")


def _count(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if isinstance(value, float) and not value.is_integer():
        return None
    return int(value) if 0 <= value <= _SAFE_INTEGER else None


def _parse_record(record: object) -> MediaTimelineEntry | None:
    if not isinstance(record, list) or len(record) < 3 or not isinstance(record[1], list) or len(record[1]) < 2:
        return None
    media_time, (group, object_id), wallclock = _count(record[0]), map(_count, record[1][:2]), _count(record[2])
    if media_time is None or group is None or object_id is None or wallclock is None:
        return None
    return MediaTimelineEntry(media_time_ms=media_time, group=group, object=object_id, wallclock_ms=wallclock)
