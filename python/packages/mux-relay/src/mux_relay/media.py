"""Media publishing policy over registered tracks: the catalog service and the frame writer.

This is the part of stage 2 in ``spec/media-layer.md`` that the Video.js streaming example proved
against the relay. A catalog is a state-shaped track whose document is re-sent on every relay
subscription; a media track maps encoded frames onto groups (one GOP per group, or one frame per
group), skips the frames the relay has no subscription for, and resumes at the next keyframe after
any failure. The module knows nothing about LOC or MSF: a frame carries its own object properties
and the catalog is bytes. Encoding, pacing and time-based frame dropping stay with the application;
``stale`` is how it tells the frame writer that a frame is too old to send.

A failure is one of two things (see ``CONTEXT.md``). A *retirement* ends the track under the
writer: the error code is ``CLOSED``, or the track reports ``closed`` afterwards. Everything else is
an *interruption*: the group is reset, the track stays open, and publishing resumes at the next
keyframe. The "Media publishing" section of ``spec/behavior.md`` is the contract these classes
implement.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Sequence
from dataclasses import dataclass
from typing import Generic, Literal, Protocol, TypeVar

from moq_core import KeyValuePair, ObjectStatus

from ._validation import payload_bytes
from .errors import RelayError

FrameOutcomeKind = Literal["sent", "skipped", "stale", "interrupted", "ended"]
"""What became of a frame: sent, or one of the four ways it was not."""

Grouping = Literal["keyframe", "frame"]


class Frame(Protocol):
    """One encoded frame as the frame writer sees it. Any object with these attributes qualifies.

    ``properties`` are the object properties sent with the frame, already built by the caller (for
    LOC media, ``moq_media.loc_properties``). ``key`` marks a random-access point: under keyframe
    grouping it starts a new group. ``discontinuity`` says frames before this one were dropped, so
    an open group can no longer be completed and is reset.
    """

    @property
    def payload(self) -> bytes: ...

    @property
    def key(self) -> bool: ...

    @property
    def properties(self) -> Sequence[KeyValuePair]: ...

    @property
    def discontinuity(self) -> bool: ...


class _GroupWriter(Protocol):
    """What the frame writer needs from one open group: the shape of ``RelayGroupWriter``."""

    @property
    def group_id(self) -> int: ...

    async def write(
        self,
        payload: bytes,
        *,
        properties: Sequence[KeyValuePair] | None = None,
        status: ObjectStatus = "normal",
    ) -> None: ...

    async def close(self) -> None: ...

    def abort(self, reason: BaseException | None = None) -> None: ...


class _TrackWriter(Protocol):
    """What this module needs from a registered track: the shape of ``RelayTrackWriter``."""

    @property
    def has_demand(self) -> bool: ...

    @property
    def closed(self) -> bool: ...

    def demand_changes(self) -> AsyncIterator[bool]: ...

    async def write(self, payload: bytes, *, properties: Sequence[KeyValuePair] | None = None) -> None: ...

    async def open_group(self) -> _GroupWriter: ...


@dataclass(frozen=True, slots=True)
class FrameOutcome:
    """What became of one frame handed to :meth:`FrameWriter.write`."""

    kind: FrameOutcomeKind
    error: RelayError | None = None
    """The failure behind ``interrupted`` or ``ended``."""
    group_id: int | None = None
    """The group the frame was written to, or the one opened or reset on its behalf."""


def _retired(track: _TrackWriter, error: RelayError) -> bool:
    """A failure ends the track exactly when the SDK says so: ``CLOSED``, or the track is closed afterwards.

    ``has_demand`` cannot answer this: ``False`` is also what a track nobody subscribes to looks like.
    """
    return error.code == "CLOSED" or track.closed


def _never(frame: object) -> bool:
    return False


F = TypeVar("F", bound=Frame)


class FrameWriter(Generic[F]):
    """Map encoded frames onto a registered track's groups, one frame at a time.

    ``grouping="keyframe"`` is the LOC mapping for video: a keyframe FINs the open group and opens a
    new one, deltas append to it. ``grouping="frame"`` sends every frame as its own single-object
    group, the mapping for audio and any other independently decodable frames.

    :meth:`write` never waits for relay demand. A frame the relay has no subscription for is
    ``skipped`` at once, because an encoder loop cannot block on the write deadline; when demand
    returns, publishing resumes at the next keyframe. A frame the caller's ``stale`` predicate
    rejects is ``stale``, and so are the deltas that depended on a dropped keyframe. A group that
    fails is reset and the frame is ``interrupted``, with the error; the next keyframe opens a new
    group. A failure that ends the track (see ``CONTEXT.md``: retirement) is ``ended``, with the
    error, and every later frame is ``ended`` too, as is the first frame after the track closed
    with nothing in flight.

    Calls on one writer are made in order by one task; the writer holds no queue.
    """

    __slots__ = ("_closed", "_ended", "_group", "_grouping", "_groups", "_objects", "_recovering", "_stale", "_track")

    def __init__(
        self,
        track: _TrackWriter,
        *,
        grouping: Grouping = "keyframe",
        stale: Callable[[F], bool] | None = None,
    ) -> None:
        if not isinstance(grouping, str):
            raise TypeError("grouping must be 'keyframe' or 'frame'")

        if grouping not in ("keyframe", "frame"):
            raise ValueError("grouping must be 'keyframe' or 'frame'")

        self._track = track
        self._grouping: Grouping = grouping
        self._stale: Callable[[F], bool] = stale if stale is not None else _never
        self._group: _GroupWriter | None = None
        self._recovering = False
        self._closed = False
        self._ended: RelayError | None = None
        self._groups = 0
        self._objects = 0

    @property
    def closed(self) -> bool:
        """True once :meth:`close` ran or the track ended under the writer."""
        return self._closed or self._ended is not None

    @property
    def ended(self) -> RelayError | None:
        """The failure that ended the track, once a write has seen it; ``None`` while the track is open."""
        return self._ended

    @property
    def groups(self) -> int:
        """Groups opened so far."""
        return self._groups

    @property
    def objects(self) -> int:
        """Objects the transport accepted so far."""
        return self._objects

    async def write(self, frame: F) -> FrameOutcome:
        """Send one frame if the relay wants it, and say what happened.

        Returns when the transport accepted the object, or as soon as the frame is known not to be
        sent. Raises ``RelayError`` with code ``CLOSED`` after :meth:`close`; a track that ended on
        its own is reported as an ``ended`` outcome instead, because the encoder keeps producing.
        """
        if self._ended is not None:
            return FrameOutcome("ended", error=self._ended)

        if self._closed:
            raise RelayError("Frame writer is closed", code="CLOSED", phase="writing")

        if self._track.closed:
            # The track ended while nothing was in flight (the connection closed, or the application
            # closed the track itself). No operation failed, so the state is named instead.
            self._ended = RelayError("Published track is closed", code="CLOSED", phase="writing")
            self._group = None
            return FrameOutcome("ended", error=self._ended)

        if self._grouping == "frame":
            return await self._write_own_group(frame)

        return await self._write_into_gop(frame)

    async def close(self) -> None:
        """FIN the open group, if any, and refuse further frames. Idempotent.

        A ``RelayError`` from that FIN is swallowed: the group was abandoned either way. Anything
        else propagates. The registered track stays the application's to close.
        """
        if self._closed:
            return

        self._closed = True
        await self._end_group()

    # ------------------------------------------------------------------ keyframe grouping

    async def _write_into_gop(self, frame: F) -> FrameOutcome:
        stale = self._stale(frame)
        reset: int | None = None

        if stale or frame.discontinuity:
            # Whatever group is open depended on frames that will never be sent.
            reset = self._reset_group()
            self._recovering = True

        if stale or (self._recovering and not frame.key):
            return FrameOutcome("stale", group_id=reset)

        if frame.key:
            self._recovering = False
            await self._end_group()

            if self._stale(frame):
                # The FIN took long enough that this keyframe is too old to start a group with.
                self._recovering = True
                return FrameOutcome("stale")

            if not self._track.has_demand:
                return FrameOutcome("skipped")

            try:
                self._group = await self._track.open_group()
            except RelayError as error:
                return self._failed(error, group_id=None)

            self._groups += 1

        if self._group is None:
            # No demand when the GOP started, or the group was lost: wait for the next keyframe.
            return FrameOutcome("skipped")

        if self._stale(frame):
            self._recovering = True
            return FrameOutcome("stale", group_id=self._reset_group())

        group = self._group

        try:
            await group.write(frame.payload, properties=frame.properties)
        except RelayError as error:
            # The SDK reset the group's stream already; the next keyframe opens a fresh one.
            self._group = None
            return self._failed(error, group_id=group.group_id)
        except BaseException:
            # Cancelled from outside (the task, the caller's deadline, the lifetime's reason), or a
            # frame the SDK rejected as an argument before anything was sent. Reset the group (a
            # no-op when the SDK already did) so the track's write slot is free, and let it go: kept,
            # it would replay this failure from the next FIN. The next keyframe opens a fresh one.
            group.abort()
            self._group = None
            raise

        self._objects += 1
        return FrameOutcome("sent", group_id=group.group_id)

    def _reset_group(self) -> int | None:
        """Abort the open group, if any, and say which one it was."""
        group, self._group = self._group, None

        if group is None:
            return None

        group.abort()
        return group.group_id

    async def _end_group(self) -> None:
        group, self._group = self._group, None

        if group is None:
            return

        try:
            await group.close()
        except RelayError:
            # The group was already reset, or the track is gone: nothing left to complete.
            pass
        except BaseException:
            # Cancelled mid-FIN: make sure the slot is free before the failure propagates.
            group.abort()
            raise

    # ------------------------------------------------------------------ one group per frame

    async def _write_own_group(self, frame: F) -> FrameOutcome:
        if self._stale(frame):
            return FrameOutcome("stale")

        if not self._track.has_demand:
            return FrameOutcome("skipped")

        try:
            group = await self._track.open_group()
        except RelayError as error:
            return self._failed(error, group_id=None)

        self._groups += 1

        # From here on the group holds the track's write slot: whatever happens, including the
        # caller's own predicate raising, it is reset before this method is left.
        try:
            if self._stale(frame):
                # Opening the stream took long enough that the frame is too old; a reset costs less
                # than a late frame.
                group.abort()
                return FrameOutcome("stale", group_id=group.group_id)

            await group.write(frame.payload, properties=frame.properties)
            self._objects += 1
            await group.close()
        except RelayError as error:
            group.abort()
            return self._failed(error, group_id=group.group_id)
        except BaseException:
            group.abort()
            raise

        return FrameOutcome("sent", group_id=group.group_id)

    # ------------------------------------------------------------------ failures

    def _failed(self, error: RelayError, *, group_id: int | None) -> FrameOutcome:
        if _retired(self._track, error):
            self._ended = error
            self._group = None
            return FrameOutcome("ended", error=error, group_id=group_id)

        return FrameOutcome("interrupted", error=error, group_id=group_id)


class CatalogService:
    """Serve a state-shaped track: send its document on every relay subscription.

    Relay demand is the trigger. ``demand_changes()`` yields the current state first and then every
    new or replacement relay subscription, so a viewer who joins after the relay reconnected gets
    the document again (the repeated ``True`` the vocabulary describes). A write that fails while
    the track stays open is retried after ``retry_delay`` seconds for as long as demand remains,
    keeping only this document. A failure that ends the track ends the service.
    """

    __slots__ = ("_document", "_failures", "_retry_delay", "_sends", "_track")

    def __init__(self, track: _TrackWriter, document: bytes, *, retry_delay: float = 1.0) -> None:
        # Arguments first, as everywhere in the SDK: a bad document never reaches the track.
        self._document = payload_bytes(document)

        if isinstance(retry_delay, bool) or not isinstance(retry_delay, int | float):
            raise TypeError("retry_delay must be a number of seconds")

        if retry_delay != retry_delay or retry_delay < 0:
            raise ValueError("retry_delay must be zero or more seconds")

        self._track = track
        self._retry_delay = retry_delay
        self._sends = 0
        self._failures = 0

    @property
    def sends(self) -> int:
        """Documents the transport accepted."""
        return self._sends

    @property
    def failures(self) -> int:
        """Writes that failed while the track stayed open, each followed by a retry or by lost demand."""
        return self._failures

    async def serve(self) -> None:
        """Run until cancelled or the track ends.

        Returns when the track ends without a write having failed. Raises the ``RelayError`` that
        ended the track when a write exposed it, so a retired catalog cannot go unnoticed.
        """
        async for demanded in self._track.demand_changes():
            if not demanded:
                continue

            while True:
                try:
                    await self._track.write(self._document)
                except RelayError as error:
                    if _retired(self._track, error):
                        raise

                    self._failures += 1

                    # The document must eventually arrive even if the relay keeps its subscription.
                    # Pause between attempts; losing demand returns to the demand iterator, and a
                    # cancelled task ends the pause.
                    if not self._track.has_demand:
                        break

                    await asyncio.sleep(self._retry_delay)

                    if not self._track.has_demand:
                        break
                else:
                    self._sends += 1
                    break


__all__ = [
    "CatalogService",
    "Frame",
    "FrameOutcome",
    "FrameOutcomeKind",
    "FrameWriter",
]
