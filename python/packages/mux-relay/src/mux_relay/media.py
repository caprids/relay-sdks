"""Media publishing policy over registered tracks: the catalog and document services and the frame writer.

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
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator, Callable, Sequence
from dataclasses import dataclass
from typing import Generic, Literal, Protocol, TypeVar

from moq_core import KeyValuePair, ObjectStatus

from ._validation import payload_bytes
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
        "_drop_epoch",
        "_dropping",
        "_ended",
        "_finishing",
        "_group",
        "_grouping",
        "_groups",
        "_idle",
        "_keyframe_needed",
        "_loop",
        "_max_backlog",
        "_objects",
        "_on_keyframe_needed",
        "_recovering",
        "_reset_pending",
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
        self._reset_pending = False
        self._drop_epoch = 0
        self._finishing: _GroupWriter | None = None
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

            try:
                outcome = await turn
            except BaseException:
                if turn.done() and not turn.cancelled() and turn.result() is None:
                    # Cancelled after the turn was handed over: hand it on.
                    self._next_turn()
                else:
                    # Still waiting: leave the backlog. Frames compare by value, so find this entry by identity.
                    for index, waiting in enumerate(self._backlog):
                        if waiting is entry:
                            del self._backlog[index]
                            break

                raise

            if outcome is not None:
                return outcome
        else:
            self._busy = True
            self._idle.clear()

        try:
            return await self._take_turn(frame)
        finally:
            self._next_turn()

    async def close(self) -> None:
        """Drop the backlog, FIN the open group, if any, and refuse further frames. Idempotent.

        A frame being written finishes first. A ``RelayError`` from the FIN is swallowed: the group
        was abandoned either way. Anything else propagates. The registered track stays the
        application's to close.
        """
        if self._closed:
            return

        self._closed = True

        while self._backlog:
            _, turn = self._backlog.popleft()
            _settle(turn, FrameOutcome("dropped"))

        if self._watch is not None:
            self._watch.cancel()
            await asyncio.wait([self._watch])

        await self._idle.wait()
        await self._end_group()

    # ------------------------------------------------------------------ the backlog

    def _next_turn(self) -> None:
        """The frame being written is done: hand the turn to the oldest waiting frame."""
        while self._backlog:
            _, turn = self._backlog.popleft()

            if not turn.done():
                turn.set_result(None)
                return

        self._busy = False
        self._idle.set()

    def _overflow(self, frame: F) -> FrameOutcome | None:
        """A frame arrived at a full backlog. Returns its outcome when it is dropped at once."""
        if self._grouping == "frame":
            _, turn = self._backlog.popleft()
            _settle(turn, FrameOutcome("dropped"))
            return None

        # The transport fell behind: everything behind it goes, and the GOP in progress with it.
        while self._backlog:
            _, turn = self._backlog.popleft()
            _settle(turn, FrameOutcome("dropped"))

        self._drop_epoch += 1
        reset = self._reset_group(_BacklogDropped("the frame writer's backlog overflowed"))

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

            # A new or replacement relay subscription, or its end: the open group cannot continue.
            self._reset_pending = True
            self._need_keyframe()

    # ------------------------------------------------------------------ one frame's turn

    async def _take_turn(self, frame: F) -> FrameOutcome:
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

        return await self._write_into_gop(frame)

    # ------------------------------------------------------------------ keyframe grouping

    async def _write_into_gop(self, frame: F) -> FrameOutcome:
        epoch = self._drop_epoch

        try:
            return await self._map_into_gop(frame, epoch)
        except BaseException as error:
            if self._drop_epoch != epoch and not isinstance(error, asyncio.CancelledError):
                # The backlog overflowed while this frame was in flight and its group was reset for it.
                return FrameOutcome("dropped")

            # Whatever escapes (a cancelled write, a frame the SDK rejected as an argument, the
            # caller's own predicate raising) leaves the GOP in progress with a gap, or a group that
            # never got its keyframe. Reset it (a no-op when the SDK already did) so the track's
            # write slot is free and the next delta is skipped rather than sent as object 0, and let
            # the failure through. The next keyframe opens a fresh group.
            self._reset_group()
            self._need_keyframe()
            raise

    async def _map_into_gop(self, frame: F, epoch: int) -> FrameOutcome:
        if self._reset_pending:
            # Relay demand changed since the last frame: the GOP in progress cannot continue.
            self._reset_pending = False
            self._reset_group()

        stale = self._stale(frame)
        reset: int | None = None

        if stale or frame.discontinuity:
            # Whatever group is open depended on frames that will never be sent.
            reset = self._reset_group()
            self._recovering = True
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
                return FrameOutcome("skipped")

            try:
                group = await self._track.open_group()
            except RelayError as error:
                if self._drop_epoch != epoch:
                    return FrameOutcome("dropped")

                return self._failed(error, group_id=None)

            self._groups += 1

            if self._drop_epoch != epoch:
                # Opened for a frame the overflow dropped meanwhile; it never gets its keyframe.
                group.abort()
                return FrameOutcome("dropped", group_id=group.group_id)

            self._group = group

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

        Returns when the track ends without a write having failed. Raises the ``RelayError`` that
        ended the track when a write exposed it, so a retired track cannot go unnoticed.
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

                await self._send_due()
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

    async def _send_due(self) -> None:
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
                await asyncio.sleep(self._retry_delay)
            else:
                self._sends += 1


__all__ = [
    "CatalogService",
    "DocumentService",
    "Frame",
    "FrameOutcome",
    "FrameOutcomeKind",
    "FrameWriter",
]
