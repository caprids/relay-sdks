"""A track a supervisor serves across connections: the writer the application keeps, bound in turn to
each generation's track, and moved make-before-break on a rotation. A write made while no generation
is up, or one the connection's end interrupts, fails with ``RECONNECTING``; nothing is replayed onto
the next generation. The counterpart of ``ts/src/supervised-track.ts``.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import Protocol, TypeVar

from moq_core import Closed, KeyValuePair, ObjectStatus

from ._awaitable import RelayAwaitable
from ._supervision import CLOSURE_GRACE, wait_any
from ._validation import object_property_pairs, payload_bytes
from ._write_slot import RelayGroupWriter, RelayTrackWriter, _observe
from .errors import RelayError
from .payload import _encode_json, _encode_text

T = TypeVar("T")


class TrackPublisher(Protocol):
    """Where a generation's tracks are registered: a publisher connection, or one publication on a connection."""

    @property
    def closed(self) -> RelayAwaitable[Closed]: ...

    def register_track(
        self, track: str, *, track_properties: Sequence[KeyValuePair] | None = None, write_timeout: float = ...
    ) -> RelayTrackWriter: ...


@dataclass(slots=True, eq=False)
class TrackGeneration:
    """One generation as the tracks it serves see it."""

    publisher: TrackPublisher
    ended: bool = False
    """Set once the connection ended or the supervisor stopped using it."""
    busy: int = 0
    """Operations and open groups running on it; a rotation closes it only once none are left."""
    group_holds: dict[Callable[[], None], RelayGroupWriter] = field(default_factory=dict)
    """The holds of groups open on it, released when it ends: the connection closed them."""
    idle: asyncio.Future[None] | None = None
    """Set by a rotation waiting for it; resolved whenever ``busy`` drops to zero."""

    def use(self) -> Callable[[], None]:
        """Count an operation or open group on this generation until the returned release runs."""
        self.busy += 1
        released = False

        def release() -> None:
            nonlocal released
            if released:
                return
            released = True
            self.busy -= 1
            if self.busy == 0 and self.idle is not None and not self.idle.done():
                self.idle.set_result(None)

        return release

    async def wait_idle(self) -> None:
        """Return once nothing runs on this generation."""
        if self.busy == 0:
            return
        self.idle = asyncio.get_running_loop().create_future()
        await self.idle


def _reconnecting(cause: BaseException | None = None) -> RelayError:
    """The failure for a write made while no generation is up, or one the connection's end interrupted."""
    error = RelayError(
        "The supervised publisher is between connections; nothing was replayed. Write the next object when it is live",
        code="RECONNECTING",
        phase="connection",
    )
    error.__cause__ = cause
    return error


async def _ended_under(error: BaseException, generation: TrackGeneration, writer: RelayTrackWriter) -> bool:
    """Whether a failure from a generation's writer means its connection ended under the operation.

    Only the generation's end decides, never the code: ``CLOSED`` also means a write to a finished
    group, while the generation stays live. A track the connection's end retired reports ``closed``
    at once; a stream can fail a little before the connection reports its end, so a RelayError waits
    up to the closure grace for that report.
    """
    if not isinstance(error, RelayError):
        return False
    loop = asyncio.get_running_loop()
    deadline = loop.time() + CLOSURE_GRACE
    while not (generation.ended or writer.closed):
        remaining = deadline - loop.time()
        if remaining <= 0:
            break
        await wait_any(settles=[generation.publisher.closed], limit=min(remaining, 0.01))
    return generation.ended or writer.closed


@dataclass(slots=True, eq=False)
class _Binding:
    """The live generation's writer."""

    generation: TrackGeneration
    writer: RelayTrackWriter
    covered: bool = False
    """Set when a move reported the writer's True: its iterator's next True is that same demand, not a new one,
    unless a False came first."""


@dataclass(slots=True, eq=False)
class _Pending:
    """The generation a rotation is moving a track to, until it moves."""

    generation: TrackGeneration
    writer: RelayTrackWriter
    moved: asyncio.Future[None]
    seen: bool | None = None
    """The value its demand iterator yielded last, while pending."""


class SupervisedTrackWriter(RelayTrackWriter):
    """One registered track across generations: the writer, the binding to the live generation, and demand.

    A :class:`RelayTrackWriter` to the application. Every public member is overridden and the base's
    own slots are never set, as ``RelaySubscription`` does over the core handle.
    """

    __slots__ = (
        "_binding",
        "_demand_queues",
        "_group_holds",
        "_last_demand",
        "_pending",
        "_release",
        "_retired",
        "_retired_writers",
        "_track_properties",
        "_wt",
    )

    def __init__(
        self, name: str, track_properties: list[KeyValuePair], write_timeout: float, release: Callable[[], None]
    ) -> None:
        # Deliberately no super().__init__(): this writer owns no write slot of its own.
        self.name = name
        self._track_properties = track_properties
        self._wt = write_timeout
        self._release = release
        self._binding: _Binding | None = None
        self._pending: _Pending | None = None
        # Writers on generations a rotation moved this track off, kept until they close: their open groups end with it.
        self._retired_writers: dict[TrackGeneration, RelayTrackWriter] = {}
        # The holds of groups open on any generation, released when the track closes and ends them.
        self._group_holds: set[Callable[[], None]] = set()
        self._retired = False
        # The demand the iterators last reported, so a lost generation reports False only after True.
        self._last_demand = False
        self._demand_queues: set[asyncio.Queue[bool | None]] = set()

    # ------------------------------------------------------------------ the public writer

    @property
    def has_demand(self) -> bool:
        """Whether the live generation's relay subscription wants this track. ``False`` between generations."""
        return self._binding.writer.has_demand if self._binding is not None else False

    @property
    def closed(self) -> bool:
        """True only once the application closed the track or the supervisor stopped."""
        return self._retired

    def demand_changes(self) -> AsyncIterator[bool]:
        """The current demand, then every change across generations: ``False`` when a generation that
        had demand is lost, then the next one's changes. Ends when the track closes."""
        queue: asyncio.Queue[bool | None] = asyncio.Queue()
        queue.put_nowait(None if self._retired else self.has_demand)
        if not self._retired:
            self._demand_queues.add(queue)
        return self._iterate_demand(queue)

    async def _iterate_demand(self, queue: asyncio.Queue[bool | None]) -> AsyncIterator[bool]:
        try:
            while True:
                item = await queue.get()
                if item is None:
                    return
                yield item
        finally:
            self._demand_queues.discard(queue)

    async def write(self, payload: bytes, *, properties: Sequence[KeyValuePair] | None = None) -> None:
        """Write one object on the live generation; see :meth:`RelayTrackWriter.write`.

        Raises ``RECONNECTING`` at once while no generation is up, and when the connection's end
        interrupts the write, never ``CLOSED``. Nothing is replayed onto the next generation.
        """
        # The object's clock starts at this call, so checking its arguments here counts (section 5.2). The SDK's
        # writers time with the event loop's clock.
        started = asyncio.get_running_loop().time()
        data = payload_bytes(payload)
        object_properties = object_property_pairs(properties)
        await self._on_live(lambda writer, _: writer._write_from(started, data, object_properties))

    async def write_text(self, text: str, *, properties: Sequence[KeyValuePair] | None = None) -> None:
        """Encode UTF-8 text and write one object with the same semantics as ``write``."""
        await self.write(_encode_text(text), properties=properties)

    async def write_json(self, value: object, *, properties: Sequence[KeyValuePair] | None = None) -> None:
        """Serialize JSON and write one object with the same semantics as ``write``."""
        await self.write(_encode_json(value), properties=properties)

    async def open_group(
        self, payload: bytes | None = None, *, properties: Sequence[KeyValuePair] | None = None
    ) -> RelayGroupWriter:
        """Open a multi-object group on the live generation; see :meth:`RelayTrackWriter.open_group`."""
        # The first object's clock starts at this call, so checking its arguments here counts (section 5.2).
        started = asyncio.get_running_loop().time()
        # Arguments before connection or slot state (spec/behavior.md, "Arguments").
        data: bytes | None = None
        object_properties: list[KeyValuePair] | None = None

        if payload is not None:
            data = payload_bytes(payload)
            object_properties = object_property_pairs(properties)
        elif properties is not None:
            raise TypeError("Group properties belong to a first object: pass a payload with them")

        async def open_on(writer: RelayTrackWriter, generation: TrackGeneration) -> RelayGroupWriter:
            group = await writer._open_group_from(started, data, object_properties)
            # The generation the group was opened on, read before the await: a disconnect clears the binding.
            # The group holds the generation open until it is closed or aborted.
            hold = generation.use()

            def release() -> None:
                self._group_holds.discard(release)
                generation.group_holds.pop(release, None)
                hold()

            self._group_holds.add(release)
            generation.group_holds[release] = group
            # Opened under a connection that has ended meanwhile: nothing will release it on the end.
            if generation.ended:
                release()
            return _SupervisedGroupWriter(group, self, generation, writer, release)

        return await self._on_live(open_on)

    def close(self) -> None:
        """End the track on every generation. Its name stays taken on this supervised publisher."""
        if self._retired:
            return
        self._retired = True
        self._release()
        binding, self._binding = self._binding, None
        if binding is not None:
            binding.writer.close()
        retired, self._retired_writers = list(self._retired_writers.values()), {}
        for writer in retired:
            writer.close()
        # Closing the writers ended their groups: none holds a rotating generation open any more.
        for release in list(self._group_holds):
            release()
        self._drop_pending(close=True)
        self._end_demand()

    # ------------------------------------------------------------------ the supervisor's side

    async def _on_live(self, operation: Callable[[RelayTrackWriter, TrackGeneration], Awaitable[T]]) -> T:
        """Run ``operation`` on the live generation's writer, mapping an interruption by the connection's end."""
        if self._retired:
            raise RelayError("Published track is closed", code="CLOSED", phase="waiting-for-demand")
        # A rotation moves the track at an operation's start, never under one.
        self._maybe_move()
        binding = self._binding
        if binding is None:
            raise _reconnecting()
        # A rotation keeps the generation open until the operation (and a group it opens) is done.
        release = binding.generation.use()
        try:
            return await operation(binding.writer, binding.generation)
        except Exception as error:
            if not self._retired and await _ended_under(error, binding.generation, binding.writer):
                raise _reconnecting(error) from error
            raise
        finally:
            release()

    def _bind_generation(self, generation: TrackGeneration) -> None:
        """Register this track on a new generation. A registration the new connection refuses leaves it unbound."""
        if self._retired:
            return
        try:
            writer = generation.publisher.register_track(
                self.name, track_properties=self._track_properties, write_timeout=self._wt
            )
        except Exception:
            # Its arguments were checked when the application registered it, so only the connection's end lands here.
            return
        self._binding = _Binding(generation, writer)
        follower = asyncio.ensure_future(self._follow(generation, writer))
        _observe(follower)

    def _hand_over(self, generation: TrackGeneration) -> asyncio.Future[None]:
        """A rotation brought up ``generation``: register this track on it and move there once it has
        demand there or none on the current generation. Settles once moved, or once the track closed or
        could not be registered."""
        moved: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        if self._retired:
            moved.set_result(None)
            return moved
        if self._binding is None or self._binding.generation.ended:
            self._bind_generation(generation)
            moved.set_result(None)
            return moved
        try:
            writer = generation.publisher.register_track(
                self.name, track_properties=self._track_properties, write_timeout=self._wt
            )
        except Exception:
            # As in `_bind_generation`: only the new connection's end lands here.
            moved.set_result(None)
            return moved
        self._pending = _Pending(generation, writer, moved)
        follower = asyncio.ensure_future(self._follow(generation, writer))
        _observe(follower)
        self._maybe_move()
        return moved

    def _forget(self, generation: TrackGeneration) -> None:
        """The rotation closed ``generation``: nothing on it is left to close with the track."""
        self._retired_writers.pop(generation, None)

    def _settle(self) -> None:
        """The rotation's overlap is over: move to the next generation now, wherever demand is."""
        self._maybe_move(force=True)

    def _maybe_move(self, *, force: bool = False) -> None:
        """Move to the pending generation once it has demand or the current one has none, or at once when forced."""
        pending = self._pending
        if pending is None:
            return
        if pending.generation.ended:
            self._drop_pending(close=False)
            return
        binding = self._binding
        stay = (
            binding is not None
            and not binding.generation.ended
            and binding.writer.has_demand
            and not pending.writer.has_demand
        )
        if stay and not force:
            return
        self._pending = None
        if binding is not None and not binding.generation.ended:
            self._retired_writers[binding.generation] = binding.writer
        # The move reports True for demand the iterator has not yielded yet; one it already yielded is not covered,
        # so the next True it yields is new demand.
        covered = pending.writer.has_demand and pending.seen is not True
        self._binding = _Binding(pending.generation, pending.writer, covered=covered)
        # The move reports the next generation's demand as it is now: True again if it has demand, so
        # demand-driven state reaches it, or the loss of demand. Values its iterator had queued before the move
        # are stale and not reported.
        self._emit_demand(pending.writer.has_demand)
        if not pending.moved.done():
            pending.moved.set_result(None)

    def _drop_pending(self, *, close: bool) -> None:
        """Give up the pending generation, closing its writer when the track itself closed."""
        pending, self._pending = self._pending, None
        if pending is None:
            return
        if close:
            pending.writer.close()
        if not pending.moved.done():
            pending.moved.set_result(None)

    def _unbind_generation(self, generation: TrackGeneration) -> None:
        """The generation ended: writes fail with RECONNECTING until the next one binds."""
        if self._pending is not None and self._pending.generation is generation:
            self._drop_pending(close=False)
        if self._binding is None or self._binding.generation is not generation:
            return
        self._binding = None
        self._emit_demand(False)

    def _retire(self) -> None:
        """The supervisor stopped: the track is closed for good."""
        for release in list(self._group_holds):
            release()
        if self._retired:
            return
        self._retired = True
        self._binding = None
        self._drop_pending(close=False)
        self._end_demand()

    async def _follow(self, generation: TrackGeneration, writer: RelayTrackWriter) -> None:
        # The value this iterator yielded last: a True after True is the relay replacing its upstream subscription.
        previous: bool | None = None
        changes = writer.demand_changes()
        try:
            async for value in changes:
                bound = self._binding is not None and self._binding.generation is generation
                pending = (
                    self._pending if self._pending is not None and self._pending.generation is generation else None
                )
                if not bound and pending is None:
                    break
                if pending is not None:
                    pending.seen = value
                # Any True after this iterator's first value is new demand: a repeat, or demand back after a False.
                renewed = value and previous is not None
                previous = value
                # Demand leaving a generation a rotation is moving off: move first if the next one has demand, so
                # the track reports no False it did not have. The move reports the next generation's own demand.
                if bound and not value and self._pending is not None:
                    self._maybe_move()
                    if self._binding is None or self._binding.generation is not generation:
                        continue
                binding = self._binding
                if bound and binding is not None:
                    # A False is not reported when the writer has demand again by now, but the True after it is
                    # renewed demand all the same, which demand-driven state (a catalog) is written again on. Only
                    # the move's own True is not repeated.
                    if not value:
                        binding.covered = False
                    if value and binding.covered:
                        binding.covered = False
                    elif value == writer.has_demand and (value != self._last_demand or renewed):
                        self._emit_demand(value)
                # Demand arriving on the next generation, or leaving the current one, moves a rotating track.
                self._maybe_move()
        finally:
            close = getattr(changes, "aclose", None)
            if close is not None:
                await close()

    def _emit_demand(self, value: bool) -> None:
        # A repeated True is kept: a new upstream subscription, or a move to a generation with demand, is when
        # demand-driven state (a catalog) is written again. Only a False that changes nothing is dropped.
        if not value and not self._last_demand:
            return
        self._last_demand = value
        for queue in list(self._demand_queues):
            queue.put_nowait(value)

    def _end_demand(self) -> None:
        for queue in list(self._demand_queues):
            queue.put_nowait(None)
        self._demand_queues.clear()


class _SupervisedGroupWriter(RelayGroupWriter):
    """A generation's group writer whose failures map the connection's end to RECONNECTING, as the track's do."""

    __slots__ = ("_aborted", "_generation", "_group", "_release", "_track", "_track_writer")

    def __init__(
        self,
        group: RelayGroupWriter,
        track: SupervisedTrackWriter,
        generation: TrackGeneration,
        track_writer: RelayTrackWriter,
        release: Callable[[], None],
    ) -> None:
        # Deliberately no super().__init__(): every member forwards to the generation's group.
        self._group = group
        self._release = release
        self._track = track
        self._generation = generation
        self._track_writer = track_writer
        self._aborted = False
        self.group_id = group.group_id

    @property
    def closed(self) -> bool:
        return self._group.closed

    @property
    def _moved_off(self) -> bool:
        """True once a rotation moved the track to another generation: the group finishes, but no longer
        reaches the track's subscribers. Read by the frame writer."""
        binding = self._track._binding
        return binding is None or binding.generation is not self._generation

    @property
    def object_count(self) -> int:
        return self._group.object_count

    async def _mapped(self, operation: Callable[[], Awaitable[None]]) -> None:
        try:
            await operation()
        except Exception as error:
            if (
                not self._aborted
                and not self._track.closed
                and await _ended_under(error, self._generation, self._track_writer)
            ):
                raise _reconnecting(error) from error
            raise
        finally:
            # A finished or failed group no longer holds a rotating generation open.
            if self._group.closed:
                self._release()

    async def write(
        self, payload: bytes, *, properties: Sequence[KeyValuePair] | None = None, status: ObjectStatus = "normal"
    ) -> None:
        await self._mapped(lambda: self._group.write(payload, properties=properties, status=status))

    async def write_text(
        self, text: str, *, properties: Sequence[KeyValuePair] | None = None, status: ObjectStatus = "normal"
    ) -> None:
        await self.write(_encode_text(text), properties=properties, status=status)

    async def write_json(
        self, value: object, *, properties: Sequence[KeyValuePair] | None = None, status: ObjectStatus = "normal"
    ) -> None:
        await self.write(_encode_json(value), properties=properties, status=status)

    async def close(self) -> None:
        # Released by `_mapped` only once the group is closed: a refused close (WRITE_PENDING) keeps it holding.
        await self._mapped(self._group.close)

    def abort(self, reason: BaseException | None = None) -> None:
        self._aborted = True
        try:
            self._group.abort(reason)
        finally:
            self._release()


__all__ = ["SupervisedTrackWriter", "TrackGeneration", "TrackPublisher"]
