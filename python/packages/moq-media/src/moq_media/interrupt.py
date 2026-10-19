"""The interrupt track: how a talker that stops mid-utterance tells listeners to drop the audio they hold.

See ``spec/media-layer.md``, "Interrupt track". Format only: the talker registers and writes the track, and a
listener applies what it reads to its audio player.
"""

import json
from dataclasses import dataclass

from .loc import AUDIO_TRACK

INTERRUPT_TRACK = "interrupt"
"""The interrupt track's default name, listed in the catalog as a data track."""

_SAFE_INTEGER = 2**53 - 1


@dataclass(frozen=True, slots=True, kw_only=True)
class Interrupt:
    """One interrupt: drop the named audio track's audio in a timeline range, in microseconds."""

    track: str = AUDIO_TRACK
    """The audio track in the same namespace whose audio to drop."""
    until_us: int
    """Drop before this timestamp: the end of the last audio the talker sent.

    Frames that arrive later with an earlier timestamp are dropped too, so the talker's next audio must start at or
    after it.
    """
    from_us: int | None = None
    """Drop from this timestamp on; ``None`` drops from the oldest audio held."""


def build_interrupt(*, until_us: int, from_us: int | None = None, track: str = AUDIO_TRACK) -> bytes:
    """An interrupt object's payload: compact JSON, ``{"track":…,"fromUs":…,"untilUs":…}``, ``fromUs`` only if given.

    Raises ``ValueError`` on an empty track, a timestamp that is not a nonnegative safe integer, or ``from_us`` not
    before ``until_us``.
    """
    interrupt = _checked(track, from_us, until_us, from_present=from_us is not None, read=False)
    document: dict[str, object] = {"track": interrupt.track}
    if interrupt.from_us is not None:
        document["fromUs"] = interrupt.from_us
    document["untilUs"] = interrupt.until_us
    return json.dumps(document, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def parse_interrupt(payload: bytes) -> Interrupt:
    """Read an interrupt object's payload.

    Raises ``ValueError`` on a payload that is not a UTF-8 JSON object with a non-empty string ``track`` and a
    nonnegative safe-integer ``untilUs``, or whose ``fromUs``, when present, is not one before ``untilUs``. Other
    fields are ignored.
    """
    raw = json.loads(payload.decode("utf-8"), parse_constant=_reject_constant)
    if not isinstance(raw, dict):
        raise ValueError("interrupt is not a JSON object")
    return _checked(raw.get("track"), raw.get("fromUs"), raw.get("untilUs"), from_present="fromUs" in raw, read=True)


def _reject_constant(name: str) -> object:
    # JSON.parse rejects NaN and Infinity; so does this reader.
    raise ValueError(f"interrupt is not valid JSON: {name}")


def _timestamp(value: object, name: str, *, read: bool) -> int:
    # A payload's integral float (2e3) reads as an integer, as JavaScript's JSON.parse sees it.
    if read and isinstance(value, float) and value.is_integer():
        value = int(value)
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= _SAFE_INTEGER:
        raise ValueError(f"{name} must be a nonnegative safe integer")
    return value


def _checked(track: object, from_us: object, until_us: object, *, from_present: bool, read: bool) -> Interrupt:
    if not isinstance(track, str) or not track:
        raise ValueError("track must be a non-empty string")
    try:
        track.encode("utf-8")
    except UnicodeEncodeError:
        raise ValueError("track must be valid Unicode") from None
    until = _timestamp(until_us, "untilUs", read=read)
    if not from_present:
        return Interrupt(track=track, until_us=until)
    start = _timestamp(from_us, "fromUs", read=read)
    if start >= until:
        raise ValueError("fromUs must be before untilUs")
    return Interrupt(track=track, from_us=start, until_us=until)
