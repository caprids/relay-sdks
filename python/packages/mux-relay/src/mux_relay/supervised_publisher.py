"""The supervised publisher: one publisher connection at a time, replaced with backoff whenever it
ends, each serving the application's root namespace plus a fresh generation field.

Track writers outlive the connections: the application registers a track once and keeps writing to
the same writer, which binds to each generation's track in turn. Nothing written is replayed onto a
new generation; a write made while no generation is up fails at once with ``RECONNECTING``. See
``spec/behavior.md``, "Supervised connections". The counterpart of ``ts/src/supervised-publisher.ts``.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from types import TracebackType
from typing import Self, TypeVar

from moq_core import Closed, KeyValuePair, ObjectStatus

from ._awaitable import RelayAwaitable
from ._supervision import (
    CLOSURE_GRACE,
    RECONNECT_BACKOFF,
    ROTATION_OVERLAP,
    SupervisedStatus,
    SupervisionBackoff,
    backoff_delay,
    backoff_options,
    is_setup_failure,
    new_generation,
    rotate_before_option,
    rotation_deadline,
    rotation_due,
    supervisor_closed,
    wait_any,
)
from ._validation import namespace_fields, object_property_pairs, payload_bytes, track_name, track_property_pairs
from ._write_slot import RelayGroupWriter, RelayTrackWriter, _observe
from .cancellation import CancellationToken
from .connect_session import DEFAULT_CONNECT_TIMEOUT, RelayTransportFactory, TokenProvider, validate_timeout
from .errors import RelayError
from .payload import _encode_json, _encode_text
from .publish import DEFAULT_WRITE_TIMEOUT, RelayPublisher, connect_relay_publisher

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class PublisherGeneration:
    """One publisher generation: a connection and the namespace it serves."""

    generation: str
    """The generation field: start time in fixed-width base 36, then four random characters, so it sorts by age."""
    namespace: tuple[str, ...]
    """The namespace this generation serves: the root followed by ``generation``."""


ConnectPublisher = Callable[[tuple[str, ...], CancellationToken], Awaitable[RelayPublisher]]
"""How a supervisor opens one generation's connection: the namespace, and the attempt's cancellation."""


@dataclass(slots=True, eq=False)
class _Generation:
    """One generation as the supervisor holds it."""

    info: PublisherGeneration
    publisher: RelayPublisher
    ended: bool = False
    """Set once the connection ended or the supervisor stopped using it."""
    up_at: float = 0.0
    """When it went live, on the loop's clock, for resetting the backoff."""
    rotate_at: float | None = None
    """When it is due to rotate, on the loop's clock; ``None`` for an unknown expiry."""
    busy: int = 0
    """Operations and open groups running on it; a rotation closes it only once none are left."""
    group_holds: set[Callable[[], None]] = field(default_factory=set)
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


async def _ended_under(error: BaseException, generation: _Generation, writer: RelayTrackWriter) -> bool:
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
    generation: _Generation
    writer: RelayTrackWriter


@dataclass(slots=True, eq=False)
class _Pending:
    """The generation a rotation is moving a track to, until it moves."""

    generation: _Generation
    writer: RelayTrackWriter
    moved: asyncio.Future[None]
    heard: bool | None = None
    """The demand its iterator last reported while pending, reported on the move; later values follow it."""


class _SupervisedTrackWriter(RelayTrackWriter):
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
        self._retired_writers: dict[_Generation, RelayTrackWriter] = {}
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
        data = payload_bytes(payload)
        object_properties = object_property_pairs(properties)
        await self._on_live(lambda writer, _: writer.write(data, properties=object_properties))

    async def write_text(self, text: str, *, properties: Sequence[KeyValuePair] | None = None) -> None:
        """Encode UTF-8 text and write one object with the same semantics as ``write``."""
        await self.write(_encode_text(text), properties=properties)

    async def write_json(self, value: object, *, properties: Sequence[KeyValuePair] | None = None) -> None:
        """Serialize JSON and write one object with the same semantics as ``write``."""
        await self.write(_encode_json(value), properties=properties)

    async def open_group(self) -> RelayGroupWriter:
        """Open a multi-object group on the live generation; see :meth:`RelayTrackWriter.open_group`."""

        async def open_on(writer: RelayTrackWriter, generation: _Generation) -> RelayGroupWriter:
            group = await writer.open_group()
            # The generation the group was opened on, read before the await: a disconnect clears the binding.
            # The group holds the generation open until it is closed or aborted.
            hold = generation.use()

            def release() -> None:
                self._group_holds.discard(release)
                generation.group_holds.discard(release)
                hold()

            self._group_holds.add(release)
            generation.group_holds.add(release)
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

    async def _on_live(self, operation: Callable[[RelayTrackWriter, _Generation], Awaitable[T]]) -> T:
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

    def _bind_generation(self, generation: _Generation) -> None:
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

    def _hand_over(self, generation: _Generation) -> asyncio.Future[None]:
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

    def _forget(self, generation: _Generation) -> None:
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
        self._binding = _Binding(pending.generation, pending.writer)
        # Reported here only once its iterator caught up; otherwise the iterator reports the values still
        # queued, in order.
        if pending.heard is not None and pending.heard == pending.writer.has_demand:
            self._emit_demand(pending.heard)
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

    def _unbind_generation(self, generation: _Generation) -> None:
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

    async def _follow(self, generation: _Generation, writer: RelayTrackWriter) -> None:
        first = True
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
                    pending.heard = value
                # The first value is the new connection's current state; False repeats the lost generation's.
                if bound and not (first and not value):
                    self._emit_demand(value)
                first = False
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


def _discard(tracks: dict[str, _SupervisedTrackWriter], name: str) -> None:
    tracks.pop(name, None)


class _SupervisedGroupWriter(RelayGroupWriter):
    """A generation's group writer whose failures map the connection's end to RECONNECTING, as the track's do."""

    __slots__ = ("_aborted", "_generation", "_group", "_release", "_track", "_track_writer")

    def __init__(
        self,
        group: RelayGroupWriter,
        track: _SupervisedTrackWriter,
        generation: _Generation,
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


@dataclass(slots=True, eq=False)
class _State:
    """The supervisor's mutable state, shared by its run loop and the handle."""

    tracks: dict[str, _SupervisedTrackWriter] = field(default_factory=dict)
    names: set[str] = field(default_factory=set)
    """Every name ever registered: a closed track's name stays taken, as on a single connection."""
    current: _Generation | None = None
    retiring: _Generation | None = None
    """The generation a rotation is replacing, while its tracks move off it."""
    status: SupervisedStatus = "connecting"
    stopped: bool = False
    rotate_requested: bool = False
    """Set by ``rotate()`` while a connection is up, and consumed by the rotation that answers it: the next one
    to start, or one whose replacement is still connecting or being discarded."""
    wake: asyncio.Future[None] | None = None
    """Wakes a serve loop waiting for its rotation; replaced for each wait."""
    draining: set[asyncio.Future[None]] = field(default_factory=set)
    """Retired generations still closing: draining groups the relay has not acknowledged. ``close()`` waits for them."""


class SupervisedPublisher:
    """A publisher that reconnects. Create one with :func:`connect_supervised_publisher`.

    Each connection is a new generation with its own namespace; tracks are registered once and
    rebound to every generation.
    """

    __slots__ = ("_close", "_closed", "_state")

    def __init__(
        self,
        state: _State,
        closed: asyncio.Future[Closed],
        close: Callable[[], Awaitable[None]],
    ) -> None:
        self._state = state
        self._closed = RelayAwaitable(closed)
        self._close = close

    @property
    def generation(self) -> PublisherGeneration | None:
        """The live generation, or ``None`` between connections and after closing."""
        return self._state.current.info if self._state.current is not None else None

    @property
    def status(self) -> SupervisedStatus:
        """Where the supervisor stands; see :data:`SupervisedStatus`."""
        return self._state.status

    @property
    def closed(self) -> RelayAwaitable[Closed]:
        """Resolves once the supervisor stops, with the failure that stopped it or the cancellation's
        reason as ``error`` (``None`` after :meth:`close`). Never raises."""
        return self._closed

    def register_track(
        self,
        track: str,
        *,
        track_properties: Sequence[KeyValuePair] | None = None,
        write_timeout: float = DEFAULT_WRITE_TIMEOUT,
    ) -> RelayTrackWriter:
        """Register a track every generation serves, and return the writer that outlives the generations.

        The same rules as :meth:`RelayPublisher.register_track`, checked at once: track names are
        unique per supervised publisher, including after their writer closes. Writes wait for demand
        on the live generation; while none is up they raise ``RECONNECTING``, and an operation the
        connection's end interrupts raises ``RECONNECTING`` too, never ``CLOSED``, so a frame writer
        treats it as an interruption and resumes at the next keyframe.
        """
        name = track_name(track)
        properties = track_property_pairs(track_properties)
        validate_timeout("write_timeout", write_timeout)
        state = self._state
        if state.stopped:
            raise supervisor_closed("publisher")
        if name in state.names:
            raise ValueError("Track name is already registered on this supervised publisher")
        writer = _SupervisedTrackWriter(name, properties, write_timeout, lambda: _discard(state.tracks, name))
        state.tracks[name] = writer
        state.names.add(name)
        if state.current is not None:
            writer._bind_generation(state.current)
        return writer

    def rotate(self) -> None:
        """Replace the live generation make-before-break, as happens on its own ``rotate_before`` before the
        credential expires.

        The next generation connects, with a fresh token, and every track is registered on it. Each
        track's writer then moves to it at its next write or group, once the track has demand there or
        has none on the old one. The old generation closes once every track has moved, after 10 seconds
        at most. A no-op while no generation is up.
        """
        state = self._state
        if state.stopped or state.current is None:
            return
        state.rotate_requested = True
        if state.wake is not None and not state.wake.done():
            state.wake.set_result(None)

    async def close(self) -> None:
        """Stop reconnecting and close the live generation as :meth:`RelayPublisher.close` does. Idempotent."""
        await self._close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        await self.close()


async def connect_supervised_publisher(
    url: str,
    *,
    namespace: Sequence[str],
    get_token: TokenProvider | None = None,
    cancellation: CancellationToken | None = None,
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
    create_transport: RelayTransportFactory | None = None,
    reconnect: SupervisionBackoff | None = None,
    rotate_before: float | None = None,
    on_generation: Callable[[PublisherGeneration], None] | None = None,
    on_status: Callable[[SupervisedStatus, BaseException | None], None] | None = None,
) -> SupervisedPublisher:
    """Open a publisher that reconnects: each connection serves ``namespace`` plus a fresh generation
    field, and the writers :meth:`SupervisedPublisher.register_track` returns outlive the connections.

    Returns once the first generation is up. A first attempt that fails on its arguments, or because
    the relay refused the credentials (``UNAUTHORIZED``), raises that failure; any other failure is
    retried with backoff (``reconnect``, from 0.5 s doubling up to 10 s), bounded only by
    ``cancellation``, whose reason is raised. The token must grant ``namespace``, the root, since every
    generation publishes one field below it. ``on_generation`` runs once each generation is up and
    every track is bound to it; ``on_status`` on every status change, with what ended the previous
    connection or failed the last attempt. ``rotate_before`` (seconds, default 60) is how long before
    the connection's credential expires (:attr:`RelayPublisher.expires_at`) the next generation is
    brought up to replace it, make-before-break; never earlier than halfway through the time left.
    See ``spec/behavior.md``, "Supervised connections"; :func:`connect_relay_publisher` never reconnects.
    """

    def connect(fields: tuple[str, ...], attempt: CancellationToken) -> Awaitable[RelayPublisher]:
        return connect_relay_publisher(
            url,
            namespace=fields,
            get_token=get_token,
            cancellation=attempt,
            connect_timeout=connect_timeout,
            create_transport=create_transport,
        )

    return await _supervise_relay_publisher(
        connect,
        namespace,
        cancellation=cancellation,
        reconnect=reconnect,
        rotate_before=rotate_before,
        on_generation=on_generation,
        on_status=on_status,
    )


async def _supervise_relay_publisher(
    connect: ConnectPublisher,
    namespace: Sequence[str],
    *,
    cancellation: CancellationToken | None = None,
    reconnect: SupervisionBackoff | None = None,
    rotate_before: float | None = None,
    on_generation: Callable[[PublisherGeneration], None] | None = None,
    on_status: Callable[[SupervisedStatus, BaseException | None], None] | None = None,
) -> SupervisedPublisher:
    """Open a supervised publisher with ``connect``; the tests drive it with scripted connections."""
    root = namespace_fields(namespace)
    # A generation field is always valid; check the whole namespace once so a root at the size limit fails here.
    namespace_fields((*root, new_generation()))
    backoff = backoff_options("reconnect", reconnect, RECONNECT_BACKOFF)
    rotate_margin = rotate_before_option(rotate_before)
    if cancellation is not None:
        cancellation.throw_if_cancelled()

    loop = asyncio.get_running_loop()
    state = _State()
    closed: asyncio.Future[Closed] = loop.create_future()
    # Cancelled when the supervisor stops: ends the wait between attempts.
    stopping = CancellationToken()
    # The connection attempt in progress only: once connected, a generation is closed, not cancelled.
    attempt_in_progress: CancellationToken | None = None
    first: asyncio.Future[None] | None = loop.create_future()
    shutdown: asyncio.Future[None] | None = None

    def set_status(status: SupervisedStatus, error: BaseException | None = None) -> None:
        if state.status == status and error is None:
            return
        state.status = status
        if on_status is not None:
            on_status(status, error)

    def stop(error: BaseException | None) -> asyncio.Future[None]:
        nonlocal first, shutdown
        if shutdown is not None:
            return shutdown
        state.stopped = True
        reason = error if error is not None else supervisor_closed("publisher")
        stopping.cancel(reason)
        if attempt_in_progress is not None:
            attempt_in_progress.cancel(reason)
        # A stop while the first connection is still being made fails the call that is waiting for it.
        if first is not None and not first.done():
            first.set_exception(reason)
        first = None

        async def finish() -> None:
            open_generations = [g for g in (state.current, state.retiring) if g is not None]
            state.current = state.retiring = None
            for track in list(state.tracks.values()):
                track._retire()
            for live in open_generations:
                live.ended = True
            for live in open_generations:
                with contextlib.suppress(Exception):
                    await live.publisher.close()
            if state.draining:
                await asyncio.gather(*state.draining, return_exceptions=True)
            try:
                set_status("closed")
            finally:
                if not closed.done():
                    closed.set_result(Closed(error))

        shutdown = asyncio.ensure_future(finish())
        _observe(shutdown)
        return shutdown

    def on_cancelled(reason: BaseException) -> None:
        stop(reason)

    if cancellation is not None:
        cancellation.add_callback(on_cancelled)

    async def connect_generation() -> _Generation | None:
        """Connect one generation, or ``None`` if the supervisor stopped first. Raises the attempt's failure."""
        nonlocal attempt_in_progress
        generation = new_generation()
        fields = (*root, generation)
        attempt_in_progress = CancellationToken()
        try:
            publisher = await connect(fields, attempt_in_progress)
        finally:
            attempt_in_progress = None
        if state.stopped:
            with contextlib.suppress(Exception):
                await publisher.close()
            return None
        live = _Generation(
            PublisherGeneration(generation, fields),
            publisher,
            up_at=loop.time(),
            rotate_at=rotation_deadline(publisher.expires_at, rotate_margin),
        )

        # Marked in the turn the connection reports its end, before anything awaiting `closed` resumes.
        def mark_ended(_: object, live: _Generation = live) -> None:
            live.ended = True
            # The connection's end closed its groups under the application, which may never close them itself.
            for release in list(live.group_holds):
                release()

        publisher.closed.add_done_callback(mark_ended)
        return live

    def report(live: _Generation) -> None:
        if on_generation is not None:
            on_generation(live.info)

    async def rotate_from(live: _Generation, manual: Callable[[], bool]) -> _Generation | None:
        """Bring up the generation that replaces ``live`` and move the tracks to it, make-before-break.

        ``None`` when ``live`` ended or the supervisor stopped before a replacement was up, or when an
        automatic rotation found no fresher credential.
        """
        failures = 0
        nxt: _Generation | None = None

        # The live generation ending while its replacement is still connecting is a reconnect: report it at once.
        def on_live_closed(done: asyncio.Future[Closed]) -> None:
            if state.stopped or state.current is not live:
                return
            state.current = None
            for track in list(state.tracks.values()):
                track._unbind_generation(live)
            try:
                set_status("reconnecting", done.result().error)
            except Exception as thrown:
                stop(thrown)

        live.publisher.closed.add_done_callback(on_live_closed)
        while not state.stopped and not live.ended:
            try:
                nxt = await connect_generation()
                break
            except Exception:
                if state.stopped:
                    return None
                # The live generation keeps serving; try again after the backoff unless it ends first.
                await wait_any([stopping], settles=[live.publisher.closed], limit=backoff_delay(failures, backoff))
                failures += 1
        if nxt is None:
            return None
        # close() may have run while the helper's result was on its way here: it could not see this connection.
        if state.stopped:
            nxt.ended = True
            with contextlib.suppress(Exception):
                await nxt.publisher.close()
            return None
        if not live.ended and not manual():
            # A credential that expires no later than the live one replaces nothing: the relay would end both together.
            was, now = live.publisher.expires_at, nxt.publisher.expires_at
            if was is not None and (now is None or now <= was):
                nxt.ended = True
                with contextlib.suppress(Exception):
                    await nxt.publisher.close()
                return None
        state.current = nxt
        state.retiring = live
        moved = [track._hand_over(nxt) for track in list(state.tracks.values())]
        set_status("live")
        report(nxt)

        async def moved_and_idle() -> None:
            await asyncio.gather(*moved)
            await live.wait_idle()

        # Every track moved and every write or group still on the old generation finished, or the overlap ran out.
        settled = asyncio.ensure_future(moved_and_idle())
        _observe(settled)
        # So does the next generation's own rotation falling due, for a credential shorter than the overlap.
        next_due = asyncio.ensure_future(rotation_due(nxt.rotate_at, loop.create_future()))
        _observe(next_due)
        try:
            # The next generation ending cuts the overlap short too: its tracks need the generation after it now.
            await wait_any(
                [stopping],
                settles=[settled, live.publisher.closed, nxt.publisher.closed, next_due],
                limit=ROTATION_OVERLAP,
            )
        finally:
            settled.cancel()
            next_due.cancel()
            live.idle = None
        for track in list(state.tracks.values()):
            track._settle()
        live.ended = True
        # A track whose next generation ended before it moved is still bound here: it has no generation now.
        for track in list(state.tracks.values()):
            track._unbind_generation(live)
        if state.retiring is live:
            state.retiring = None

        # Closing drains the old generation's unacknowledged groups for up to a write deadline; the next
        # generation is served meanwhile, and close() waits for the drain.
        async def drain_old() -> None:
            with contextlib.suppress(Exception):
                await live.publisher.close()
            for track in list(state.tracks.values()):
                track._forget(live)

        drain = asyncio.ensure_future(drain_old())
        _observe(drain)
        state.draining.add(drain)
        drain.add_done_callback(state.draining.discard)
        return nxt

    async def serve(live: _Generation) -> Closed | _Generation:
        """Serve ``live`` until it ends (its ``Closed``), or until a rotation replaced it (the new generation)."""
        # Cleared once an automatic rotation found no fresher credential: only rotate() tries again.
        automatic = True
        while True:
            wake: asyncio.Future[None] = loop.create_future()
            state.wake = wake
            if state.rotate_requested:
                wake.set_result(None)
            due = asyncio.ensure_future(rotation_due(live.rotate_at if automatic else None, wake))
            _observe(due)
            try:
                await wait_any([stopping], settles=[live.publisher.closed, due])
                if live.publisher.closed.done() or state.stopped:
                    return await live.publisher.closed
                # A rotate() before the replacement is current makes this rotation a manual one: it takes the
                # replacement.
                manual = False

                def is_manual() -> bool:
                    nonlocal manual
                    if state.rotate_requested:
                        state.rotate_requested = False
                        manual = True
                    return manual

                is_manual()
                nxt = await rotate_from(live, is_manual)
                if nxt is not None:
                    return nxt
                if live.ended or state.stopped:
                    return await live.publisher.closed
                if not manual:
                    automatic = False
            finally:
                due.cancel()
                if state.wake is wake:
                    state.wake = None

    async def run() -> None:
        nonlocal first
        attempt = 0
        # A generation a rotation already brought up, reported and moved the tracks to.
        nxt: _Generation | None = None
        while not state.stopped:
            live, nxt = nxt, None
            if live is None:
                try:
                    live = await connect_generation()
                except Exception as error:
                    if state.stopped:
                        return
                    if first is not None and is_setup_failure(error):
                        first.set_exception(error)
                        first = None
                        await stop(error)
                        return
                    set_status("reconnecting", error)
                    await wait_any([stopping], limit=backoff_delay(attempt, backoff))
                    attempt += 1
                    continue
                if live is None:
                    return
                # close() may have run while the helper's result was on its way here: it could not see this connection.
                if state.stopped:
                    live.ended = True
                    with contextlib.suppress(Exception):
                        await live.publisher.close()
                    return
                state.current = live
                for track in list(state.tracks.values()):
                    track._bind_generation(live)
                set_status("live")
                if first is not None:
                    first.set_result(None)
                    first = None
                report(live)

            outcome = await serve(live)
            if state.stopped:
                return
            if isinstance(outcome, _Generation):
                nxt = outcome
                attempt = 0
                continue
            live.ended = True
            state.current = None
            for track in list(state.tracks.values()):
                track._unbind_generation(live)
            # Already reported if the generation ended while a rotation's replacement was connecting.
            if state.status != "reconnecting":
                set_status("reconnecting", outcome.error)
            # A generation that stayed up longer than the longest delay was a success: start the backoff over.
            if loop.time() - live.up_at > backoff.max_delay:
                attempt = 0
            await wait_any([stopping], limit=backoff_delay(attempt, backoff))
            attempt += 1

    async def supervise() -> None:
        try:
            await run()
        except Exception as error:
            # An application callback (on_status, on_generation) raised: stop rather than leave the
            # supervisor neither reconnecting nor closed, and fail a caller still waiting to connect.
            await stop(error)

    runner = asyncio.ensure_future(supervise())
    _observe(runner)
    started = first
    assert started is not None
    # A caller that stops waiting detaches from the shield, which then no longer observes the result.
    _observe(started)
    try:
        await asyncio.shield(started)
    except asyncio.CancelledError:
        # The caller gave up waiting: nothing will hold the supervisor, so stop it.
        if cancellation is not None:
            cancellation.remove_callback(on_cancelled)
        stop(None)
        raise
    except BaseException:
        if cancellation is not None:
            cancellation.remove_callback(on_cancelled)
        raise

    async def close() -> None:
        if cancellation is not None:
            cancellation.remove_callback(on_cancelled)
        await asyncio.shield(stop(None))

    return SupervisedPublisher(state, closed, close)


__all__ = ["PublisherGeneration", "SupervisedPublisher", "connect_supervised_publisher"]
