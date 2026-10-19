"""The transcript track: the text of a talker's audio, in segments placed on its timeline.

See ``spec/media-layer.md``, "Transcript track". Format only: the talker registers and writes the track, and a
listener keeps what it reads in a ``Transcript``, which drops what an interrupt cuts.
"""

import json
import math
from dataclasses import dataclass

from .loc import AUDIO_TRACK

TRANSCRIPT_TRACK = "transcript"
"""The transcript track's default name, listed in the catalog as a data track."""

_SAFE_INTEGER = 2**53 - 1


@dataclass(frozen=True, slots=True, kw_only=True)
class TranscriptSegment:
    """One version of a transcript segment: the text of the named audio track's audio from ``from_us`` on."""

    seq: int
    """The segment's number; a later version of the same segment repeats it."""
    track: str = AUDIO_TRACK
    """The audio track in the same namespace whose audio this is the text of."""
    from_us: int
    """Where the segment's audio starts, in microseconds on the broadcast timeline."""
    until_us: int | None = None
    """Where it ends; ``None`` while the talker does not know yet. A final segment has it."""
    text: str
    """The text, which a later version replaces whole."""
    final: bool = False
    """No later version follows."""


@dataclass(frozen=True, slots=True, kw_only=True)
class TranscriptLine:
    """A segment as a ``Transcript`` holds it: its latest version, and whether an interrupt cut it short."""

    seq: int
    track: str
    from_us: int
    until_us: int | None
    text: str
    final: bool
    cut: bool = False
    """An interrupt cut the segment's audio short at ``until_us``: the listener heard only the start of the text."""


def build_transcript_segment(
    *,
    seq: int,
    from_us: int,
    text: str,
    until_us: int | None = None,
    final: bool = False,
    track: str = AUDIO_TRACK,
) -> bytes:
    """A transcript object's payload: compact JSON, ``{"seq":…,"track":…,"fromUs":…,"untilUs":…,"text":…,"final":…}``.

    ``untilUs`` only if given. Raises ``ValueError`` on a number that is not a nonnegative safe integer, an empty
    track, text that is not valid Unicode, ``until_us`` not after ``from_us``, or a final segment without ``until_us``.
    """
    segment = _checked(
        {"seq": seq, "track": track, "fromUs": from_us, "untilUs": until_us, "text": text, "final": final},
        until_present=until_us is not None,
        read=False,
    )
    document: dict[str, object] = {"seq": segment.seq, "track": segment.track, "fromUs": segment.from_us}
    if segment.until_us is not None:
        document["untilUs"] = segment.until_us
    document |= {"text": segment.text, "final": segment.final}
    return json.dumps(document, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def parse_transcript_segment(payload: bytes) -> TranscriptSegment:
    """Read a transcript object's payload.

    Raises ``ValueError`` on a payload that is not a UTF-8 JSON object with a nonnegative safe-integer ``seq`` and
    ``fromUs``, a non-empty string ``track``, a string ``text`` and a boolean ``final``, or whose ``untilUs``, when
    present, is not one after ``fromUs``, or that is final without ``untilUs``. Other fields are ignored.
    """
    raw = json.loads(payload.decode("utf-8"), parse_constant=_reject_constant)
    if not isinstance(raw, dict):
        raise ValueError("transcript segment is not a JSON object")
    return _checked(raw, until_present="untilUs" in raw, read=True)


class Transcript:
    """What a listener has read of one audio track's transcript: each segment's latest version, less what was cut.

    ``apply`` each segment read for the track and ``cut`` on each interrupt for it; ``lines_at`` gives what to show as
    the audio plays. A segment's final version is kept against later ones. Cuts are remembered, so a version that
    arrives after one is clipped as if it had arrived before. A segment is known by its ``seq`` within one run of the
    talker's track, so use a new ``Transcript`` when the talker starts over.
    """

    def __init__(self) -> None:
        self._lines: dict[int, TranscriptLine] = {}
        self._cuts: list[tuple[int, int]] = []
        # The segments the talker sent final: a line a cut ended is final too, but takes the talker's final version.
        self._finals: set[int] = set()

    def apply(self, segment: TranscriptSegment) -> bool:
        """Keep a segment's version, clipped by the cuts so far; ``False`` if it changed nothing.

        Ignored: a version of a segment the talker already sent final, a segment that starts inside a range cut
        earlier (as audio arriving there late is), and an interim version of a segment a cut ended. A cut segment
        takes the talker's final version, clipped: one whose audio ended before the cut is no longer cut.
        """
        held = self._lines.get(segment.seq)
        if segment.seq in self._finals or (held is not None and held.cut and not segment.final):
            return False
        if segment.final:
            # Final even if a cut drops it below: no later version may bring the segment back.
            self._finals.add(segment.seq)
        line = TranscriptLine(
            seq=segment.seq,
            track=segment.track,
            from_us=segment.from_us,
            until_us=segment.until_us,
            text=segment.text,
            final=segment.final,
        )
        for start, until in sorted(self._cuts):
            if start <= line.from_us < until:
                # Starts inside a cut: never heard. A final version says an earlier one held is wrong.
                return segment.final and self._lines.pop(segment.seq, None) is not None
            if line.from_us < start and (line.until_us is None or line.until_us > start):
                line = _clipped(line, start)
                break
        if line == held:
            return False
        self._lines[segment.seq] = line
        return True

    def cut(self, *, until_us: int, from_us: int | None = None, position_us: float | None = None) -> None:
        """Apply an interrupt's range, with the playout position now (``PlayoutClock.position_us``).

        Audio before the position has been heard, so the cut starts at the later of ``from_us`` and the position.
        It drops the segments that start in the cut, and ends the one playing there at its start, final and cut.
        Raises ``ValueError`` on a range an interrupt could not have, or a negative or non-finite position.
        """
        until = _timestamp(until_us, "untilUs", read=False)
        start = 0 if from_us is None else _timestamp(from_us, "fromUs", read=False)
        if from_us is not None and start >= until:
            raise ValueError("fromUs must be before untilUs")
        if position_us is not None:
            if isinstance(position_us, bool) or not math.isfinite(position_us) or position_us < 0:
                raise ValueError("position_us must be a nonnegative number")
            start = max(start, math.floor(position_us))
        if start >= until:
            return
        self._cuts.append((start, until))
        for seq, line in list(self._lines.items()):
            if line.from_us >= until:
                continue
            if line.from_us >= start:
                del self._lines[seq]
            elif line.until_us is None or line.until_us > start:
                self._lines[seq] = _clipped(line, start)

    @property
    def lines(self) -> list[TranscriptLine]:
        """Every segment held, in timeline order."""
        return sorted(self._lines.values(), key=lambda line: (line.from_us, line.seq))

    def lines_at(self, position_us: float | None) -> list[TranscriptLine]:
        """The segments whose audio has started playing at this position, in timeline order; none while idle."""
        if position_us is None:
            return []
        return [line for line in self.lines if line.from_us <= position_us]


def _clipped(line: TranscriptLine, start_us: int) -> TranscriptLine:
    # Ended where a cut starts: final, since nothing after it plays, and cut.
    return TranscriptLine(
        seq=line.seq,
        track=line.track,
        from_us=line.from_us,
        until_us=start_us,
        text=line.text,
        final=True,
        cut=True,
    )


def _reject_constant(name: str) -> object:
    # JSON.parse rejects NaN and Infinity; so does this reader.
    raise ValueError(f"transcript segment is not valid JSON: {name}")


def _timestamp(value: object, name: str, *, read: bool) -> int:
    # A payload's integral float (2e3) reads as an integer, as JavaScript's JSON.parse sees it.
    if read and isinstance(value, float) and value.is_integer():
        value = int(value)
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= _SAFE_INTEGER:
        raise ValueError(f"{name} must be a nonnegative safe integer")
    return value


def _checked(raw: dict[str, object], *, until_present: bool, read: bool) -> TranscriptSegment:
    seq = _timestamp(raw.get("seq"), "seq", read=read)
    track = raw.get("track")
    if not isinstance(track, str) or not track:
        raise ValueError("track must be a non-empty string")
    text = raw.get("text")
    if not isinstance(text, str):
        raise ValueError("text must be a string")
    for value, name in ((track, "track"), (text, "text")):
        try:
            value.encode("utf-8")
        except UnicodeEncodeError:
            raise ValueError(f"{name} must be valid Unicode") from None
    final = raw.get("final")
    if not isinstance(final, bool):
        raise ValueError("final must be a boolean")
    start = _timestamp(raw.get("fromUs"), "fromUs", read=read)
    until = None
    if until_present:
        until = _timestamp(raw.get("untilUs"), "untilUs", read=read)
        if until <= start:
            raise ValueError("untilUs must be after fromUs")
    elif final:
        raise ValueError("a final segment needs untilUs")
    return TranscriptSegment(seq=seq, track=track, from_us=start, until_us=until, text=text, final=final)
