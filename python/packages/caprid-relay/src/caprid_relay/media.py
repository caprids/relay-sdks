"""Media policy over registered tracks and subscriptions: the catalog and document services, the frame writer, the
track properties that give a media track its default delivery timeout and publisher priority, the subscription
parameters that give a viewer's subscription to it the matching subscriber priority, and the frame reader.

This is the part of stage 2 in ``spec/media-layer.md`` that the Video.js streaming example proved
against the relay. A catalog is a state-shaped track whose document is re-sent on every relay
subscription, and a document service does the same for a document the application replaces, such as
a media timeline; a media track maps encoded frames onto groups (one GOP per group, or one frame per
group), skips the frames the relay has no subscription for, resumes at the next keyframe after
any failure, drops rather than queues when the transport falls behind, and says when it needs a
keyframe. The module knows nothing about LOC or MSF: a frame carries its own object properties
and the catalog is bytes. Encoding, pacing and time-based frame dropping stay with the application;
``stale`` is how it tells the frame writer that a frame is too old to send.

A failure is one of two things (see ``CONTEXT.md``). A *retirement* ends the track under the
writer: the error code is ``CLOSED``, or the track reports ``closed`` afterwards. Everything else is
an *interruption*: the group is reset, the track stays open, and publishing resumes at the next
keyframe. The "Media publishing" section of ``spec/behavior.md`` is the contract these classes
implement.

On the subscribing side, :class:`FrameReader` reads a subscription or a want as frames in decode order. It drives a
sequencer it is given (``moq_media.FrameSequencer``), which knows the format and the deadlines; the "Media
subscribing" section of ``spec/behavior.md`` is its contract.
"""

from __future__ import annotations

import asyncio
import dataclasses
from collections import deque
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Generic, Literal, Protocol, TypeVar

from moq_core import KeyValuePair, MessageParameters, ObjectStatus

from ._frame_reader import FrameReader, FrameReaderPoll, FrameReaderSequencer
from ._validation import DEFAULT_PUBLISHER_PRIORITY, MAX_DEFAULT_PUBLISHER_PRIORITY, payload_bytes
from ._write_slot import SUBGROUP_DELIVERY_TIMEOUT
from .errors import RelayError

FrameOutcomeKind = Literal["sent", "skipped", "stale", "dropped", "interrupted", "ended"]
"""What became of a frame: sent, or one of the five ways it was not."""

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

    @property
    def closed(self) -> bool: ...

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

    async def open_group(
        self, payload: bytes | None = None, *, properties: Sequence[KeyValuePair] | None = None
    ) -> _GroupWriter: ...


@dataclass(frozen=True, slots=True)
class FrameOutcome:
    """What became of one frame handed to :meth:`FrameWriter.write`."""

    kind: FrameOutcomeKind
    error: RelayError | None = None
    """The failure behind ``interrupted`` or ``ended``."""
    group_id: int | None = None
    """The group the frame was written to, or the one opened or reset on its behalf."""


def _first_payload(frame: Frame) -> bytes:
    """A frame's payload as its group's first object; ``None`` would open a bare group and send nothing."""
    if frame.payload is None:
        raise TypeError("frame.payload must be bytes-like")

    return frame.payload


def _cancelling() -> bool:
    """Whether the running task itself is being cancelled, as opposed to a task it awaits."""
    task = asyncio.current_task()
    return task is not None and task.cancelling() > 0


def _retired(track: _TrackWriter, error: RelayError) -> bool:
    """A failure ends the track exactly when the SDK says so: ``CLOSED``, or the track is closed afterwards.

    ``has_demand`` cannot answer this: ``False`` is also what a track nobody subscribes to looks like.
    """
    return error.code == "CLOSED" or track.closed


def _never(frame: object) -> bool:
    return False


def _settle(turn: asyncio.Future[FrameOutcome | None], outcome: FrameOutcome) -> None:
    """Give a waiting frame its outcome, unless its caller was cancelled and the turn is already done."""
    if not turn.done():
        turn.set_result(outcome)


F = TypeVar("F", bound=Frame)

_DEFAULT_BACKLOG: dict[Grouping, int] = {"keyframe": 8, "frame": 25}


class _BacklogDropped(Exception):
    """The reason a group is reset when the backlog overflows; the frame in it was dropped, nothing failed."""


class FrameWriter(Generic[F]):
    """Map encoded frames onto a registered track's groups, one frame at a time.

    ``grouping="keyframe"`` is the LOC mapping for video: a keyframe FINs the open group and opens a
    new one, deltas append to it. ``grouping="frame"`` sends every frame as its own single-object
    group, the mapping for audio and any other independently decodable frames.

    :meth:`write` may be called again before the previous call returned, from any task: calls are
    served one at a time in call order, and the frames waiting meanwhile are the writer's backlog,
    at most ``max_backlog`` of them (default 8 under keyframe grouping, 25 under frame grouping). A
    frame arriving at a full backlog means the transport fell behind, and frames are ``dropped``
    rather than queued: under keyframe grouping the backlog and the open group (with the frame being
    written into it) go, and deltas are dropped until the next keyframe; under frame grouping the
    oldest waiting frame goes.

    :meth:`write` never waits for relay demand. A frame the relay has no subscription for is
    ``skipped`` at once, because an encoder loop cannot block on the write deadline. Every relay
    demand change resets the open group before the next frame, so publishing resumes at the next
    keyframe. A frame the caller's ``stale`` predicate rejects is ``stale``, and so are the deltas
    that depended on a dropped keyframe. A group that fails is reset and the frame is
    ``interrupted``, with the error; the next keyframe opens a new group. A failure that ends the
    track (see ``CONTEXT.md``: retirement) is ``ended``, with the error, and every later frame is
    ``ended`` too, as is the first frame after the track closed with nothing in flight.

    Under keyframe grouping, :attr:`keyframe_needed` says the next frame the writer can send must be a
    keyframe, and ``on_keyframe_needed`` is called each time it becomes true after the start, so an
    encoder can force one instead of waiting out its interval. The writer watches the track's demand
    from its creation, so it must be created with an event loop running.
    """

    __slots__ = (
        "_backlog",
        "_busy",
        "_closed",
        "_closing",
        "_drop_epoch",
        "_dropped_group",
        "_dropping",
        "_ended",
        "_finishing",
        "_gap_next",
        "_gap_turns",
        "_group",
        "_grouping",
        "_groups",
        "_idle",
        "_key_opening",
        "_keyframe_needed",
        "_loop",
        "_max_backlog",
        "_objects",
        "_on_keyframe_needed",
        "_opening",
        "_recovering",
        "_stale",
        "_track",
        "_watch",
    )

    def __init__(
        self,
        track: _TrackWriter,
        *,
        grouping: Grouping = "keyframe",
        stale: Callable[[F], bool] | None = None,
        max_backlog: int | None = None,
        on_keyframe_needed: Callable[[], object] | None = None,
    ) -> None:
        if not isinstance(grouping, str):
            raise TypeError("grouping must be 'keyframe' or 'frame'")

        if grouping not in ("keyframe", "frame"):
            raise ValueError("grouping must be 'keyframe' or 'frame'")

        if max_backlog is not None:
            if isinstance(max_backlog, bool) or not isinstance(max_backlog, int):
                raise TypeError("max_backlog must be a positive integer")

            if max_backlog < 1:
                raise ValueError("max_backlog must be a positive integer")

        if on_keyframe_needed is not None and not callable(on_keyframe_needed):
            raise TypeError("on_keyframe_needed must be callable")

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            raise RuntimeError("FrameWriter watches relay demand: create it with an event loop running") from None

        self._track = track
        self._grouping: Grouping = grouping
        self._stale: Callable[[F], bool] = stale if stale is not None else _never
        self._max_backlog = max_backlog if max_backlog is not None else _DEFAULT_BACKLOG[grouping]
        self._on_keyframe_needed = on_keyframe_needed
        self._loop = loop
        self._group: _GroupWriter | None = None
        self._recovering = False
        self._dropping = False
        # Frames that follow a cancelled one, which left a gap in the GOP: by their turn, or the next to arrive.
        self._gap_turns: set[asyncio.Future[FrameOutcome | None]] = set()
        self._gap_next = False
        self._closing: asyncio.Task[None] | None = None
        # A keyframe is being served and may still open a group: a demand change meanwhile is recovered from.
        self._key_opening = False
        # The group the last overflow reset, for the outcome of the frame that was being written into it.
        self._dropped_group: int | None = None
        self._drop_epoch = 0
        self._finishing: _GroupWriter | None = None
        # The open_group a keyframe is opening its group with: an overflow cancels it, which resets the group.
        self._opening: asyncio.Task[_GroupWriter] | None = None
        self._keyframe_needed = grouping == "keyframe"
        self._closed = False
        self._ended: RelayError | None = None
        self._groups = 0
        self._objects = 0
        self._backlog: deque[tuple[F, asyncio.Future[FrameOutcome | None]]] = deque()
        self._busy = False
        self._idle = asyncio.Event()
        self._idle.set()
        # Only GOPs care about demand changes; an independent frame needs nothing reset.
        self._watch: asyncio.Task[None] | None = (
            loop.create_task(self._watch_demand(track.demand_changes())) if grouping == "keyframe" else None
        )

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

    @property
    def backlog(self) -> int:
        """Frames waiting behind the one being written."""
        return len(self._backlog)

    @property
    def keyframe_needed(self) -> bool:
        """True while the next frame this writer can send must be a keyframe; always False under frame grouping."""
        return self._keyframe_needed

    async def write(self, frame: F) -> FrameOutcome:
        """Send one frame if the relay wants it, and say what happened.

        Returns when the transport accepted the object, or as soon as the frame is known not to be
        sent. Raises ``RelayError`` with code ``CLOSED`` after :meth:`close`; a track that ended on
        its own is reported as an ``ended`` outcome instead, because the encoder keeps producing.
        """
        if self._closed:
            # The application's own close wins over everything, a retirement seen earlier included.
            raise RelayError("Frame writer is closed", code="CLOSED", phase="waiting-for-demand")

        if self._grouping == "keyframe" and frame.key:
            # A keyframe has arrived, whether or not it is sent later.
            self._keyframe_needed = False

        if self._busy:
            if len(self._backlog) >= self._max_backlog:
                dropped = self._overflow(frame)

                if dropped is not None:
                    return dropped

            turn: asyncio.Future[FrameOutcome | None] = self._loop.create_future()
            entry = (frame, turn)
            self._backlog.append(entry)

            if self._gap_next:
                self._gap_next = False
                self._gap_turns.add(turn)

            try:
                outcome = await turn
            except BaseException:
                if turn.done() and not turn.cancelled() and turn.result() is None:
                    # Cancelled after the turn was handed over: the frame after it follows a gap; hand it on.
                    self._gap_turns.discard(turn)
                    self._mark_gap(0)
                    self._next_turn()
                else:
                    # Still waiting: leave the backlog. Frames compare by value, so find this entry by identity.
                    for index, waiting in enumerate(self._backlog):
                        if waiting is entry:
                            del self._backlog[index]

                            self._gap_turns.discard(turn)
                            self._mark_gap(index)
                            break

                raise

            gap = turn in self._gap_turns
            self._gap_turns.discard(turn)

            if outcome is not None:
                return outcome
        else:
            self._busy = True
            self._idle.clear()
            gap, self._gap_next = self._gap_next, False

        try:
            return await self._take_turn(frame, gap=gap)
        finally:
            self._next_turn()

    async def close(self) -> None:
        """Drop the backlog, FIN the open group, if any, and refuse further frames. Idempotent.

        A frame being written finishes first. A ``RelayError`` from the FIN is swallowed: the group
        was abandoned either way. Anything else propagates. The registered track stays the
        application's to close.
        """
        if self._closing is None:
            self._drop_backlog()

            # One cleanup for every caller, which a cancelled caller cannot cut short.
            self._closing = self._loop.create_task(self._finish_closing())

        await asyncio.shield(self._closing)

    def _drop_backlog(self) -> None:
        """Refuse further frames and settle every waiting one as ``dropped``, all before anything else runs."""
        self._closed = True

        while self._backlog:
            _, turn = self._backlog.popleft()
            self._gap_turns.discard(turn)
            _settle(turn, FrameOutcome("dropped"))

    async def _finish_closing(self) -> None:
        if self._watch is not None:
            self._watch.cancel()
            await asyncio.wait([self._watch])

        await self._idle.wait()
        await self._end_group()

    # ------------------------------------------------------------------ the backlog

    def _mark_gap(self, index: int) -> None:
        """A waiting frame was cancelled: the frame now at ``index`` (or the next to arrive) follows a gap."""
        if self._grouping != "keyframe":
            return

        if index < len(self._backlog):
            self._gap_turns.add(self._backlog[index][1])
        else:
            self._gap_next = True

        self._need_keyframe()

    def _next_turn(self) -> None:
        """The frame being written is done: hand the turn to the oldest waiting frame."""
        while self._backlog:
            _, turn = self._backlog.popleft()

            if turn.cancelled():
                # Its caller was cancelled before the handler could leave the backlog: the gap is here.
                self._gap_turns.discard(turn)
                self._mark_gap(0)
                continue

            if not turn.done():
                turn.set_result(None)
                return

        self._busy = False
        self._idle.set()

    def _overflow(self, frame: F) -> FrameOutcome | None:
        """A frame arrived at a full backlog. Returns its outcome when it is dropped at once."""
        if self._grouping == "frame":
            _, turn = self._backlog.popleft()
            self._gap_turns.discard(turn)
            _settle(turn, FrameOutcome("dropped"))
            return None

        # The transport fell behind: everything behind it goes, and the GOP in progress with it.
        while self._backlog:
            _, turn = self._backlog.popleft()
            self._gap_turns.discard(turn)
            _settle(turn, FrameOutcome("dropped"))

        self._drop_epoch += 1
        reset = self._reset_group(_BacklogDropped("the frame writer's backlog overflowed"))
        self._dropped_group = reset

        if frame.key:
            # The arriving keyframe opens the next group; nothing needs asking for.
            self._dropping = False
            return None

        self._dropping = True
        self._need_keyframe()
        return FrameOutcome("dropped", group_id=reset)

    # ------------------------------------------------------------------ keyframe needed

    def _need_keyframe(self) -> None:
        if self._grouping != "keyframe" or self._keyframe_needed or self.closed:
            return

        self._keyframe_needed = True
        callback = self._on_keyframe_needed

        if callback is None:
            return

        try:
            callback()
        except Exception as error:
            # The application's callback must not break the writer; report it like an unhandled task error.
            self._loop.call_exception_handler(
                {"message": "FrameWriter on_keyframe_needed callback raised", "exception": error}
            )

    async def _watch_demand(self, changes: AsyncIterator[bool]) -> None:
        first = True

        async for _ in changes:
            if first:
                # The state the writer started in: it already needs a keyframe.
                first = False
                continue

            # A new or replacement relay subscription, or its end. The write slot has already ended the
            # open group, which the next frame finds closed; a keyframe is needed for the new subscription.
            # Unless it has one: the change closed every group open at the time, so a group still open
            # came after it, and a keyframe waiting or in progress opens one.
            recovered = (
                (self._group is not None and not self._group.closed)
                or self._key_opening
                or any(frame.key for frame, _ in self._backlog)
            )

            if not recovered:
                self._need_keyframe()

    # ------------------------------------------------------------------ one frame's turn

    async def _take_turn(self, frame: F, *, gap: bool = False) -> FrameOutcome:
        if self._closed:
            # The turn was handed over before close() ran but this frame had not started: it is part of the backlog.
            return FrameOutcome("dropped")

        if self._ended is not None:
            return FrameOutcome("ended", error=self._ended)

        if self._track.closed:
            # The track ended while nothing was in flight (the connection closed, or the application
            # closed the track itself). No operation failed, so the state is named instead.
            self._ended = RelayError("Published track is closed", code="CLOSED", phase="waiting-for-demand")
            self._group = None
            return FrameOutcome("ended", error=self._ended)

        if self._grouping == "frame":
            return await self._write_own_group(frame)

        self._key_opening = frame.key

        try:
            return await self._write_into_gop(frame, gap)
        finally:
            self._key_opening = False

    # ------------------------------------------------------------------ keyframe grouping

    async def _write_into_gop(self, frame: F, gap: bool) -> FrameOutcome:
        epoch = self._drop_epoch

        try:
            return await self._map_into_gop(frame, epoch, gap)
        except BaseException as error:
            if self._drop_epoch != epoch and not isinstance(error, asyncio.CancelledError):
                # The backlog overflowed while this frame was in flight and its group was reset for it.
                return FrameOutcome("dropped", group_id=self._dropped_group)

            # Whatever escapes (a cancelled write, a frame the SDK rejected as an argument, the
            # caller's own predicate raising) leaves the GOP in progress with a gap, or a group that
            # never got its keyframe. Reset it (a no-op when the SDK already did) so the track's
            # write slot is free and the next delta is skipped rather than sent as object 0, and let
            # the failure through. The next keyframe opens a fresh group.
            self._reset_group()
            self._need_keyframe()
            raise

    async def _map_into_gop(self, frame: F, epoch: int, gap: bool) -> FrameOutcome:
        if self._group is not None and self._group.closed:
            # The write slot ended the group when relay demand changed (before the demand watch may
            # have run): the GOP in progress cannot continue. A keyframe here is the recovery.
            self._group = None

            if not frame.key:
                self._need_keyframe()

        stale = self._stale(frame)
        reset: int | None = None

        # A cancelled frame before this one is a discontinuity the caller did not flag.
        if stale or frame.discontinuity or gap:
            # Whatever group is open depended on frames that will never be sent.
            reset = self._reset_group()
            self._recovering = True

            if stale or not frame.key:
                # A keyframe after the discontinuity is the recovery itself.
                self._need_keyframe()

        if not stale and self._dropping and not frame.key:
            return FrameOutcome("dropped")

        if stale or (self._recovering and not frame.key):
            return FrameOutcome("stale", group_id=reset)

        if frame.key:
            self._recovering = False
            self._dropping = False
            previous = self._group.group_id if self._group is not None else None
            failure = await self._end_group()

            if self._drop_epoch != epoch:
                # An overflow reset the group during its FIN: whatever the FIN reports, that is the drop.
                return FrameOutcome("dropped")

            if failure is not None and _retired(self._track, failure):
                # The track ended under the FIN of the group before this keyframe: that is the
                # retirement, with its own error, not a keyframe nobody wanted.
                self._ended = failure
                return FrameOutcome("ended", error=failure, group_id=previous)

            if self._stale(frame):
                # The FIN took long enough that this keyframe is too old to start a group with.
                self._recovering = True
                self._need_keyframe()
                return FrameOutcome("stale")

            if not self._track.has_demand:
                # This keyframe opens nothing, so it cannot recover from a demand change still to be reported.
                self._key_opening = False
                return FrameOutcome("skipped")

            # The keyframe opens its group as object 0, so its delivery timeout counts the wait for the stream (§5.2).
            payload = _first_payload(frame)
            opening = self._loop.create_task(self._track.open_group(payload, properties=frame.properties))
            self._opening = opening

            try:
                group = await opening
            except RelayError as error:
                if self._drop_epoch != epoch:
                    return FrameOutcome("dropped")

                return self._failed(error, group_id=None)
            except asyncio.CancelledError:
                if opening.done() and not opening.cancelled() and opening.exception() is None:
                    # The caller was cancelled as the open completed: the group holds the track's slot, so free it.
                    opening.result().abort()

                if self._drop_epoch != epoch and opening.cancelled() and not _cancelling():
                    # The overflow cancelled the opening, which reset the group: it has no writer to name.
                    return FrameOutcome("dropped")

                raise
            finally:
                if self._opening is opening:
                    self._opening = None

            self._groups += 1
            self._objects += 1
            # From here the open group itself says whether a demand change was recovered from.
            self._key_opening = False

            if self._drop_epoch != epoch:
                # The overflow came as the open completed: the transport took the keyframe, but it will not arrive.
                group.abort()
                return FrameOutcome("dropped", group_id=group.group_id)

            # Held before the predicate runs, so a predicate that raises still has the group reset.
            self._group = group

            if self._stale(frame):
                # Opening the group took long enough that its keyframe is too old: a reset costs less than a late GOP.
                self._recovering = True
                reset = self._reset_group()
                self._need_keyframe()
                return FrameOutcome("stale", group_id=reset)

            return FrameOutcome("sent", group_id=group.group_id)

        if self._group is None:
            # No demand when the GOP started, or the group was lost: wait for the next keyframe.
            return FrameOutcome("skipped")

        if self._stale(frame):
            self._recovering = True
            reset = self._reset_group()
            self._need_keyframe()
            return FrameOutcome("stale", group_id=reset)

        group = self._group

        try:
            await group.write(frame.payload, properties=frame.properties)
        except RelayError as error:
            if self._drop_epoch != epoch:
                return FrameOutcome("dropped", group_id=group.group_id)

            # The SDK reset the group's stream already; the next keyframe opens a fresh one.
            self._group = None
            return self._failed(error, group_id=group.group_id)

        self._objects += 1

        if self._drop_epoch != epoch:
            # Accepted, but the overflow reset the group around it.
            return FrameOutcome("dropped", group_id=group.group_id)

        return FrameOutcome("sent", group_id=group.group_id)

    def _reset_group(self, reason: BaseException | None = None) -> int | None:
        """Abort the open group, or the one a keyframe's write is FINning, and say which one it was."""
        group = self._group or self._finishing
        self._group = None
        self._finishing = None

        if self._opening is not None:
            self._opening.cancel()

        if group is None:
            return None

        group.abort(reason)
        return group.group_id

    async def _end_group(self) -> RelayError | None:
        """FIN the open group, if any. Returns the failure if the FIN did not complete.

        A failed FIN leaves nothing to complete: the SDK reset the group, and what was delivered
        stands. Whether the failure also ended the track is the caller's question; :meth:`close`
        does not ask it.
        """
        group, self._group = self._group, None

        if group is None:
            return None

        # While the FIN is pending, an overflow can still reset the group.
        self._finishing = group

        try:
            await group.close()
        except RelayError as error:
            return error
        except BaseException:
            # Cancelled mid-FIN: make sure the slot is free before the failure propagates.
            group.abort()
            raise
        finally:
            if self._finishing is group:
                self._finishing = None

        return None

    # ------------------------------------------------------------------ one group per frame

    async def _write_own_group(self, frame: F) -> FrameOutcome:
        if self._stale(frame):
            return FrameOutcome("stale")

        if not self._track.has_demand:
            return FrameOutcome("skipped")

        # The frame opens its group as object 0, so its delivery timeout counts the wait for the stream (§5.2).
        try:
            group = await self._track.open_group(_first_payload(frame), properties=frame.properties)
        except RelayError as error:
            return self._failed(error, group_id=None)

        self._groups += 1
        self._objects += 1

        # From here on the group holds the track's write slot: whatever happens, including the
        # caller's own predicate raising, it is reset before this method is left.
        try:
            if self._stale(frame):
                # Opening the group took long enough that the frame is too old; a reset costs less
                # than a late frame.
                group.abort()
                return FrameOutcome("stale", group_id=group.group_id)

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

        self._need_keyframe()
        return FrameOutcome("interrupted", error=error, group_id=group_id)


class CatalogService:
    """Serve a state-shaped track: send its document on every relay subscription.

    Relay demand is the trigger. ``demand_changes()`` yields the current state first and then every
    new or replacement relay subscription, so a viewer who joins after the relay reconnected gets
    the document again (the repeated ``True`` the vocabulary describes). Viewers who join during one
    relay subscription trigger nothing and get the relay's cached copy, so the document must be safe
    to serve from cache. A write that fails while the track stays open is retried after
    ``retry_delay`` seconds for as long as demand remains, keeping only this document.
    A failure that ends the track ends the service.
    """

    __slots__ = ("_service",)

    def __init__(self, track: _TrackWriter, document: bytes, *, retry_delay: float = 1.0) -> None:
        # The document service's loop with a document that never changes, as in the TypeScript SDK.
        self._service = DocumentService(track, payload_bytes(document), retry_delay=retry_delay)

    @property
    def sends(self) -> int:
        """Documents the transport accepted."""
        return self._service.sends

    @property
    def failures(self) -> int:
        """Writes that failed while the track stayed open, each followed by a retry or by lost demand."""
        return self._service.failures

    async def serve(self) -> None:
        """Run until cancelled or the track ends.

        Returns when the track ends without a write having failed, even during a retry pause. Raises
        the ``RelayError`` that ended the track when a write exposed it, so a retired catalog cannot
        go unnoticed.
        """
        await self._service.serve()


class DocumentService:
    """Serve a state-shaped track whose document the application replaces.

    The latest document is sent as a new group on every relay subscription, as :class:`CatalogService`
    does, and again on every :meth:`update` while the relay is subscribed. Nothing is sent until there
    is a document. Updates made while the relay has no subscription only replace the document. A write
    that fails while the track stays open is retried after ``retry_delay`` seconds with the latest
    document, for as long as demand remains. A failure that ends the track ends the service.

    The msf-01 media timeline is this shape: each new entry is a new independent document. One service
    serves one broadcast; a supervised publisher's next generation registers a new track and a new
    service.
    """

    __slots__ = ("_document", "_due", "_failures", "_retry_delay", "_sends", "_track", "_wake")

    def __init__(self, track: _TrackWriter, document: bytes | None = None, *, retry_delay: float = 1.0) -> None:
        # Arguments first, as everywhere in the SDK: a bad document never reaches the track.
        self._document = None if document is None else payload_bytes(document)

        if isinstance(retry_delay, bool) or not isinstance(retry_delay, int | float):
            raise TypeError("retry_delay must be a number of seconds")

        if retry_delay != retry_delay or retry_delay < 0:
            raise ValueError("retry_delay must be zero or more seconds")

        self._track = track
        self._retry_delay = retry_delay
        self._sends = 0
        self._failures = 0
        self._due = False
        self._wake = asyncio.Event()

    @property
    def document(self) -> bytes | None:
        """The latest document, or ``None`` before the first."""
        return self._document

    @property
    def sends(self) -> int:
        """Documents the transport accepted."""
        return self._sends

    @property
    def failures(self) -> int:
        """Writes that failed while the track stayed open, each followed by a retry or by lost demand."""
        return self._failures

    def update(self, document: bytes) -> None:
        """Replace the document, and send it if the relay is subscribed. Returns at once."""
        self._document = payload_bytes(document)
        self._due = True
        self._wake.set()

    async def serve(self) -> None:
        """Run until cancelled or the track ends.

        Returns when the track ends without a write having failed, even during a retry pause. Raises
        the ``RelayError`` that ended the track when a write exposed it, so a retired track cannot go
        unnoticed.
        """
        watching = asyncio.ensure_future(self._watch_demand())

        try:
            while True:
                await self._wake.wait()
                self._wake.clear()

                if watching.done():
                    # The track ended (or the demand iterator failed, which is raised here).
                    watching.result()
                    return

                await self._send_due(watching)
        finally:
            watching.cancel()
            await asyncio.gather(watching, return_exceptions=True)

    async def _watch_demand(self) -> None:
        try:
            async for demanded in self._track.demand_changes():
                if demanded:
                    self._due = True
                    self._wake.set()
        finally:
            self._wake.set()

    async def _send_due(self, watching: asyncio.Future[None]) -> None:
        while self._due and self._document is not None and self._track.has_demand:
            self._due = False
            document = self._document

            try:
                await self._track.write(document)
            except RelayError as error:
                if _retired(self._track, error):
                    raise

                self._failures += 1

                # Losing demand ends the retries; the next relay subscription makes the document due again.
                if not self._track.has_demand:
                    return

                self._due = True

                # The track ending cuts the pause short: serve() then returns rather than wait out the
                # delay. asyncio.wait detaches its callback from the watcher when the pause ends.
                await asyncio.wait([watching], timeout=self._retry_delay)

                if watching.done():
                    return
            else:
                self._sends += 1


class _RegisteredTrack(_TrackWriter, Protocol):
    """What a replaceable track needs from a track it registered: a writer the application can close."""

    def close(self) -> None: ...


class _TrackPublisher(Protocol):
    """What a replaceable track needs from the publisher: the shape of ``RelayPublisher.register_track``."""

    def register_track(
        self,
        track: str,
        *,
        track_properties: Sequence[KeyValuePair] | None = None,
        write_timeout: float = ...,
    ) -> _RegisteredTrack: ...


class _CatalogService(Protocol):
    """Where the catalog is served: the shape of :class:`DocumentService`."""

    def update(self, document: bytes) -> None: ...


@dataclass(frozen=True, slots=True)
class TrackReplacement:
    """What :meth:`ReplaceableTrack.replace` did."""

    from_track: str
    """The track replaced, now closed."""
    to_track: str
    """The new track, listed in the catalog in its place."""
    replacements: int
    """How many times the track has been replaced, this time included: 1 for the first replacement."""


@dataclass(slots=True)
class _Incarnation(Generic[F]):
    """One track of a replaceable track, with its writer and the frames waiting for the relay's first subscription."""

    name: str
    replacements: int
    track: _RegisteredTrack
    writer: FrameWriter[F]
    waiting: list[tuple[F, asyncio.Future[FrameOutcome]]] | None = None
    """Frames that wait for the relay's first subscription, in order; ``None`` once they go to the writer."""
    watcher: asyncio.Task[None] | None = None
    pump: asyncio.Task[None] | None = None
    retired: bool = False


class ReplaceableTrack(Generic[F]):
    """A track the publisher can replace mid-stream, for barge-in.

    A voice agent that is talked over replaces its reply track, and listeners drop the old reply's
    audio and play the new track. msf-01 §5.3 makes a changed track a new name, so :meth:`replace`
    registers the next track (``{track}-{n + 1}`` for replacement ``n`` from 1, so ``audio-2`` first, or
    ``name(n)``) and lists it in the catalog
    in the current track's place; a listener that sees that (``moq_media.find_replacement_track``)
    plays the new track. The old track's frames that have not been sent are dropped, and the track is
    closed. The catalog is whatever ``build_catalog(name)`` returns for the track that is current,
    handed to ``catalog_service.update`` at creation and at each replacement.

    Frames go through a :class:`FrameWriter` per track, built with ``writer_options`` (default
    ``grouping="frame"``, the mapping for audio). A listener subscribes to a replacement only once the
    new catalog has reached it, about a round trip, and a frame writer skips what it is handed before
    then: the first words of the new reply. So frames written to a replacement before the relay
    subscribes wait, in order, and go out once it does; any still waiting after ``demand_wait`` seconds
    go to the writer, which skips them. ``demand_wait=0`` waits for nothing. The first track never
    waits.

    Create one with an event loop running: the frame writers need it. The first track is registered at
    once, and the publisher's track name rules apply to every name: they are unique per publisher
    connection, including after a track closes.
    """

    __slots__ = (
        "_attempts",
        "_build_catalog",
        "_catalog_service",
        "_closed",
        "_closing",
        "_current",
        "_demand_wait",
        "_name",
        "_publisher",
        "_retiring",
        "_track_properties",
        "_write_timeout",
        "_writer_options",
    )

    def __init__(
        self,
        publisher: _TrackPublisher,
        *,
        track: str,
        catalog_service: _CatalogService,
        build_catalog: Callable[[str], bytes],
        name: Callable[[int], str] | None = None,
        track_properties: Sequence[KeyValuePair] | None = None,
        write_timeout: float | None = None,
        writer_options: Mapping[str, Any] | None = None,
        demand_wait: float = 2.0,
    ) -> None:
        if not isinstance(track, str) or not track:
            raise TypeError("track must be a non-empty string")

        if name is not None and not callable(name):
            raise TypeError("name must be callable")

        if not callable(getattr(catalog_service, "update", None)) or not callable(build_catalog):
            raise TypeError("catalog_service must have update() and build_catalog must be callable")

        if isinstance(demand_wait, bool) or not isinstance(demand_wait, int | float):
            raise TypeError("demand_wait must be a number of seconds")

        if demand_wait != demand_wait or demand_wait < 0:
            raise ValueError("demand_wait must be zero or more seconds")

        self._publisher = publisher
        self._name: Callable[[int], str] = (
            name if name is not None else (lambda replacement: f"{track}-{replacement + 1}")
        )
        self._track_properties = track_properties
        self._write_timeout = write_timeout
        self._writer_options: dict[str, Any] = {"grouping": "frame", **(writer_options or {})}
        self._catalog_service = catalog_service
        self._build_catalog = build_catalog
        self._demand_wait = demand_wait
        self._closed = False
        # Names asked for so far: a failed attempt uses its name up, since the publisher may hold it.
        self._attempts = 0
        self._closing: asyncio.Task[None] | None = None
        self._retiring: set[asyncio.Task[None]] = set()
        self._current = self._register(track, 0, wait=False)

        try:
            self._catalog_service.update(self._build_catalog(track))
        except BaseException:
            self._current.track.close()
            raise

    @property
    def track(self) -> str:
        """The current track's name."""
        return self._current.name

    @property
    def replacements(self) -> int:
        """How many times the track has been replaced: 0 while the first track plays."""
        return self._current.replacements

    @property
    def closed(self) -> bool:
        """True once :meth:`close` ran."""
        return self._closed

    async def write(self, frame: F) -> FrameOutcome:
        """Send one frame on the current track and say what happened, as :meth:`FrameWriter.write` does.

        A frame written to a replacement before the relay has subscribed to it waits (``demand_wait``).
        A frame of a track that is replaced before it went out is ``dropped``. Raises ``RelayError``
        with code ``CLOSED`` after :meth:`close`.
        """
        if self._closed:
            raise RelayError("Replaceable track is closed", code="CLOSED", phase="waiting-for-demand")

        target = self._current

        if target.waiting is None:
            return await target.writer.write(frame)

        turn: asyncio.Future[FrameOutcome] = asyncio.get_running_loop().create_future()
        target.waiting.append((frame, turn))
        return await turn

    async def replace(self) -> TrackReplacement:
        """Replace the track: register the next track, list it in the catalog, then drop and close the old one.

        What the current track has not sent is dropped. Returns once the old track is closed. Frames
        written from the call on go to the new track. A failure to register the new track or to update
        the catalog is raised and leaves the current track in place. Raises ``RelayError`` with code
        ``CLOSED`` after :meth:`close`.
        """
        if self._closed:
            raise RelayError("Replaceable track is closed", code="CLOSED", phase="waiting-for-demand")

        old = self._current
        replacements = old.replacements + 1
        self._attempts += 1
        name = self._name(self._attempts)

        if not isinstance(name, str) or not name:
            raise TypeError("name() must return a non-empty string")

        replacement = self._register(name, replacements, wait=True)

        try:
            self._catalog_service.update(self._build_catalog(name))
        except BaseException:
            await self._discard(replacement)
            raise

        # The old track's unsent frames are dropped before anything else runs, so none goes out after the switch.
        self._begin_retire(old)
        self._current = replacement
        retiring = asyncio.ensure_future(self._retire(old))
        self._retiring.add(retiring)
        retiring.add_done_callback(self._retiring.discard)

        # A cancelled caller does not stop the old track's closing.
        await asyncio.shield(retiring)
        return TrackReplacement(old.name, name, replacements)

    async def close(self) -> None:
        """Drop what has not been sent and close the current track. Idempotent.

        :meth:`write` and :meth:`replace` raise after it.
        """
        if self._closing is None:
            self._closed = True
            self._begin_retire(self._current)
            self._closing = asyncio.ensure_future(self._finish_closing())

        await asyncio.shield(self._closing)

    async def _finish_closing(self) -> None:
        # The current track retires at once, so nothing waiting on it goes out, beside every old track still closing.
        await asyncio.gather(self._retire(self._current), *self._retiring, return_exceptions=True)

    def _register(self, name: str, replacements: int, *, wait: bool) -> _Incarnation[F]:
        options: dict[str, Any] = {}

        if self._track_properties is not None:
            options["track_properties"] = self._track_properties

        if self._write_timeout is not None:
            options["write_timeout"] = self._write_timeout

        track = self._publisher.register_track(name, **options)

        try:
            writer: FrameWriter[F] = FrameWriter(track, **self._writer_options)
        except BaseException:
            track.close()
            raise

        incarnation = _Incarnation(name, replacements, track, writer)

        if wait and self._demand_wait > 0 and not track.has_demand:
            incarnation.waiting = []
            incarnation.watcher = asyncio.get_running_loop().create_task(self._release_at_demand(incarnation))

        return incarnation

    async def _release_at_demand(self, incarnation: _Incarnation[F]) -> None:
        """Wait for the relay's first subscription to the track, or for ``demand_wait`` to run out, then release."""
        changes = incarnation.track.demand_changes()

        try:
            async with asyncio.timeout(self._demand_wait):
                async for demand in changes:
                    if demand:
                        break
        except TimeoutError:
            pass
        finally:
            # An async generator, as the SDK's is, releases its demand watcher when closed.
            if (aclose := getattr(changes, "aclose", None)) is not None:
                await aclose()

        # The track ending ends the iterator too: the writer reports it for each frame.
        incarnation.pump = asyncio.get_running_loop().create_task(self._pump(incarnation))

    async def _pump(self, incarnation: _Incarnation[F]) -> None:
        """Hand the waiting frames to the writer one at a time, in order, so its backlog never overflows.

        Frames written meanwhile join the end. Then later frames go straight to the writer.
        """
        while incarnation.waiting:
            frame, turn = incarnation.waiting.pop(0)

            if turn.done():
                continue

            if incarnation.retired:
                turn.set_result(FrameOutcome("dropped"))
                continue

            try:
                outcome = await incarnation.writer.write(frame)
            except Exception as error:
                if not turn.done():
                    turn.set_exception(error)
            else:
                if not turn.done():
                    turn.set_result(outcome)

        incarnation.waiting = None

    def _begin_retire(self, incarnation: _Incarnation[F]) -> None:
        """The synchronous start of retiring a track: what it has not sent is dropped, with no await in between.

        A frame waiting for demand or in the writer's backlog could otherwise go out after the switch.
        Safe to repeat.
        """
        incarnation.retired = True

        if incarnation.watcher is not None:
            incarnation.watcher.cancel()

        for _, turn in incarnation.waiting or ():
            if not turn.done():
                turn.set_result(FrameOutcome("dropped"))

        if incarnation.waiting is not None:
            incarnation.waiting.clear()

        incarnation.writer._drop_backlog()

    async def _retire(self, incarnation: _Incarnation[F]) -> None:
        self._begin_retire(incarnation)

        if incarnation.watcher is not None:
            await asyncio.wait([incarnation.watcher])

        try:
            await incarnation.writer.close()
        finally:
            incarnation.track.close()

    async def _discard(self, incarnation: _Incarnation[F]) -> None:
        """Close a track that never became current."""
        incarnation.retired = True

        if incarnation.watcher is not None:
            incarnation.watcher.cancel()
            await asyncio.wait([incarnation.watcher])

        incarnation.track.close()
        await incarnation.writer.close()


MediaKind = Literal["audio", "video"]
"""What a media track carries, which decides its default delivery timeout and priorities."""

_AUDIO_SUBGROUP_TIMEOUT_US = 500_000
"""Audio's default subgroup timeout: measured under a 2 s uplink stall to sjc (issue #302)."""

_PUBLISHER_PRIORITY: dict[str, int] = {"audio": 16, "video": 96}
"""Default Publisher Priority per kind, lower being more urgent: audio ahead of video, as Huddle sets them by hand."""

_SUBSCRIBER_PRIORITY: dict[str, int] = {"audio": 16, "video": 96}
"""SUBSCRIBER_PRIORITY per kind, lower being more urgent: the same values as the publisher priorities."""

_MAX_SUBSCRIBER_PRIORITY = 255
"""The largest SUBSCRIBER_PRIORITY, a single byte."""

_MAX_TIMEOUT_US = 2_147_483_647_000
"""The largest value whose whole milliseconds the TypeScript SDK's timers hold, the bound in both SDKs."""


def media_track_properties(
    kind: MediaKind,
    *,
    keyframe_interval_us: int | None = None,
    target_latency_us: int | None = None,
    subgroup_timeout_us: int | None = None,
    publisher_priority: int | None = None,
) -> list[KeyValuePair]:
    """The track properties that give a media track its default delivery timeout and publisher priority.

    Pass them to ``register_track`` with the track's format properties, so a stall costs stale
    frames rather than lasting latency::

        video = publisher.register_track(
            "video",
            track_properties=[
                *loc_track_properties(),
                *media_track_properties("video", keyframe_interval_us=1_000_000),
            ],
        )

    The result is a SUBGROUP_DELIVERY_TIMEOUT (0x06) in whole milliseconds, rounded up: 500 ms for
    audio, and for video the target latency less the keyframe interval, but never less than the
    interval. Every duration is a positive integer number of microseconds. ``keyframe_interval_us``
    is required for video; ``target_latency_us`` (see ``CONTEXT.md``) defaults to twice it and may
    not be smaller; audio ignores both. ``subgroup_timeout_us`` replaces the default for either kind.
    Then a Default Publisher Priority (0x0E) that puts audio ahead of video: 16 for audio, 96 for
    video, lower being more urgent; ``publisher_priority``, an integer from 0 to 255, replaces it.
    moq-relay 0.17 passes it on to viewers in SUBSCRIBE_OK but orders delivery by subscriber
    priority alone, so on that relay it changes nothing yet.
    No object timeout is set, in either SDK: through a frame writer it would never fire in a browser,
    because a group's wait for settling room is not counted and, once its stream is open, the
    browser's transport takes each object at once even during a stall.
    """
    if not isinstance(kind, str):
        raise TypeError("kind must be 'audio' or 'video'")
    if kind not in ("audio", "video"):
        raise ValueError("kind must be 'audio' or 'video'")
    _check_microseconds(keyframe_interval_us, "keyframe_interval_us")
    _check_microseconds(target_latency_us, "target_latency_us")
    _check_microseconds(subgroup_timeout_us, "subgroup_timeout_us")
    if publisher_priority is not None:
        if isinstance(publisher_priority, bool) or not isinstance(publisher_priority, int):
            raise TypeError("publisher_priority must be an integer")
        if not 0 <= publisher_priority <= MAX_DEFAULT_PUBLISHER_PRIORITY:
            raise ValueError(f"publisher_priority must be an integer from 0 to {MAX_DEFAULT_PUBLISHER_PRIORITY}")
    if kind == "video" and keyframe_interval_us is None:
        raise TypeError("keyframe_interval_us is required for video")
    if keyframe_interval_us is not None and target_latency_us is not None and target_latency_us < keyframe_interval_us:
        raise ValueError("target_latency_us must be at least keyframe_interval_us")

    if subgroup_timeout_us is not None:
        timeout_us = subgroup_timeout_us
    elif kind == "audio":
        timeout_us = _AUDIO_SUBGROUP_TIMEOUT_US
    else:
        assert keyframe_interval_us is not None
        target = 2 * keyframe_interval_us if target_latency_us is None else target_latency_us
        timeout_us = max(keyframe_interval_us, target - keyframe_interval_us)
    priority = _PUBLISHER_PRIORITY[kind] if publisher_priority is None else publisher_priority
    return [
        KeyValuePair(SUBGROUP_DELIVERY_TIMEOUT, -(-timeout_us // 1000)),
        KeyValuePair(DEFAULT_PUBLISHER_PRIORITY, priority),
    ]


def media_subscribe_parameters(kind: MediaKind, overrides: MessageParameters | None = None) -> MessageParameters:
    """The subscription parameters a viewer gives a media track, so audio goes ahead of video on a congested downlink.

    Pass them to ``subscribe``, ``want`` or ``switch_to`` (``spec/behavior.md``, "Media subscribing")::

        audio = subscriber.subscribe(namespace, "audio", handlers, parameters=media_subscribe_parameters("audio"))
        video = subscriber.subscribe(namespace, "video", handlers, parameters=media_subscribe_parameters("video"))

    The result is a new ``MessageParameters`` with a SUBSCRIBER_PRIORITY of 16 for audio and 96 for
    video, lower being more urgent: the same values ``media_track_properties`` gives as publisher
    priorities. moq-relay 0.17 orders what it sends a viewer by subscriber priority alone and ignores
    the publisher's, so this is the priority that decides delivery there. It cannot be changed later:
    on that relay any ``update()`` ends the subscription. ``overrides`` are copied onto the result,
    such as a ``location_filter``; an explicit ``subscriber_priority``, an integer from 0 to 255,
    replaces the default.
    """
    if not isinstance(kind, str):
        raise TypeError("kind must be 'audio' or 'video'")
    if kind not in ("audio", "video"):
        raise ValueError("kind must be 'audio' or 'video'")
    if overrides is None:
        overrides = MessageParameters()
    elif not isinstance(overrides, MessageParameters):
        raise TypeError("overrides must be MessageParameters")
    priority = overrides.subscriber_priority
    if priority is None:
        priority = _SUBSCRIBER_PRIORITY[kind]
    elif isinstance(priority, bool) or not isinstance(priority, int):
        raise TypeError("subscriber_priority must be an integer")
    elif not 0 <= priority <= _MAX_SUBSCRIBER_PRIORITY:
        raise ValueError(f"subscriber_priority must be an integer from 0 to {_MAX_SUBSCRIBER_PRIORITY}")
    return dataclasses.replace(overrides, subscriber_priority=priority)


def _check_microseconds(value: object, name: str) -> None:
    """Accept ``None`` or a positive integer of microseconds whose whole milliseconds fit the timer bound."""
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer number of microseconds")
    if not 0 < value <= _MAX_TIMEOUT_US:
        raise ValueError(f"{name} must be a positive integer of at most {_MAX_TIMEOUT_US} microseconds")


__all__ = [
    "CatalogService",
    "DocumentService",
    "Frame",
    "FrameOutcome",
    "FrameOutcomeKind",
    "FrameReader",
    "FrameReaderPoll",
    "FrameReaderSequencer",
    "FrameWriter",
    "MediaKind",
    "ReplaceableTrack",
    "TrackReplacement",
    "media_subscribe_parameters",
    "media_track_properties",
]
