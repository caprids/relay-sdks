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
    SupervisedStatus,
    SupervisionBackoff,
    backoff_delay,
    backoff_options,
    is_setup_failure,
    new_generation,
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


class _SupervisedTrackWriter(RelayTrackWriter):
    """One registered track across generations: the writer, the binding to the live generation, and demand.

    A :class:`RelayTrackWriter` to the application. Every public member is overridden and the base's
    own slots are never set, as ``RelaySubscription`` does over the core handle.
    """

    __slots__ = ("_binding", "_demand_queues", "_last_demand", "_release", "_retired", "_track_properties", "_wt")

    def __init__(
        self, name: str, track_properties: list[KeyValuePair], write_timeout: float, release: Callable[[], None]
    ) -> None:
        # Deliberately no super().__init__(): this writer owns no write slot of its own.
        self.name = name
        self._track_properties = track_properties
        self._wt = write_timeout
        self._release = release
        self._binding: _Binding | None = None
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
            return _SupervisedGroupWriter(group, self, generation, writer)

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
        self._end_demand()

    # ------------------------------------------------------------------ the supervisor's side

    async def _on_live(self, operation: Callable[[RelayTrackWriter, _Generation], Awaitable[T]]) -> T:
        """Run ``operation`` on the live generation's writer, mapping an interruption by the connection's end."""
        if self._retired:
            raise RelayError("Published track is closed", code="CLOSED", phase="waiting-for-demand")
        binding = self._binding
        if binding is None:
            raise _reconnecting()
        try:
            return await operation(binding.writer, binding.generation)
        except Exception as error:
            if not self._retired and await _ended_under(error, binding.generation, binding.writer):
                raise _reconnecting(error) from error
            raise

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

    def _unbind_generation(self, generation: _Generation) -> None:
        """The generation ended: writes fail with RECONNECTING until the next one binds."""
        if self._binding is None or self._binding.generation is not generation:
            return
        self._binding = None
        self._emit_demand(False)

    def _retire(self) -> None:
        """The supervisor stopped: the track is closed for good."""
        if self._retired:
            return
        self._retired = True
        self._binding = None
        self._end_demand()

    async def _follow(self, generation: _Generation, writer: RelayTrackWriter) -> None:
        first = True
        changes = writer.demand_changes()
        try:
            async for value in changes:
                if self._binding is None or self._binding.generation is not generation:
                    break
                # The first value is the new connection's current state; False repeats the lost generation's.
                if not (first and not value):
                    self._emit_demand(value)
                first = False
        finally:
            close = getattr(changes, "aclose", None)
            if close is not None:
                await close()

    def _emit_demand(self, value: bool) -> None:
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

    __slots__ = ("_aborted", "_generation", "_group", "_track", "_track_writer")

    def __init__(
        self,
        group: RelayGroupWriter,
        track: _SupervisedTrackWriter,
        generation: _Generation,
        track_writer: RelayTrackWriter,
    ) -> None:
        # Deliberately no super().__init__(): every member forwards to the generation's group.
        self._group = group
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
        await self._mapped(self._group.close)

    def abort(self, reason: BaseException | None = None) -> None:
        self._aborted = True
        self._group.abort(reason)


@dataclass(slots=True, eq=False)
class _State:
    """The supervisor's mutable state, shared by its run loop and the handle."""

    tracks: dict[str, _SupervisedTrackWriter] = field(default_factory=dict)
    names: set[str] = field(default_factory=set)
    """Every name ever registered: a closed track's name stays taken, as on a single connection."""
    current: _Generation | None = None
    status: SupervisedStatus = "connecting"
    stopped: bool = False


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
    connection or failed the last attempt. See ``spec/behavior.md``, "Supervised connections";
    :func:`connect_relay_publisher` never reconnects.
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
        on_generation=on_generation,
        on_status=on_status,
    )


async def _supervise_relay_publisher(
    connect: ConnectPublisher,
    namespace: Sequence[str],
    *,
    cancellation: CancellationToken | None = None,
    reconnect: SupervisionBackoff | None = None,
    on_generation: Callable[[PublisherGeneration], None] | None = None,
    on_status: Callable[[SupervisedStatus, BaseException | None], None] | None = None,
) -> SupervisedPublisher:
    """Open a supervised publisher with ``connect``; the tests drive it with scripted connections."""
    root = namespace_fields(namespace)
    # A generation field is always valid; check the whole namespace once so a root at the size limit fails here.
    namespace_fields((*root, new_generation()))
    backoff = backoff_options("reconnect", reconnect, RECONNECT_BACKOFF)
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
            live, state.current = state.current, None
            for track in list(state.tracks.values()):
                track._retire()
            if live is not None:
                live.ended = True
                with contextlib.suppress(Exception):
                    await live.publisher.close()
            set_status("closed")
            if not closed.done():
                closed.set_result(Closed(error))

        shutdown = asyncio.ensure_future(finish())
        _observe(shutdown)
        return shutdown

    def on_cancelled(reason: BaseException) -> None:
        stop(reason)

    if cancellation is not None:
        cancellation.add_callback(on_cancelled)

    async def run() -> None:
        nonlocal attempt_in_progress, first
        attempt = 0
        while not state.stopped:
            generation = new_generation()
            fields = (*root, generation)
            attempt_in_progress = CancellationToken()
            try:
                publisher = await connect(fields, attempt_in_progress)
            except Exception as error:
                attempt_in_progress = None
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
            attempt_in_progress = None
            if state.stopped:
                with contextlib.suppress(Exception):
                    await publisher.close()
                return
            live = _Generation(PublisherGeneration(generation, fields), publisher)

            # Marked in the turn the connection reports its end, before anything awaiting `closed` resumes.
            def mark_ended(_: object, live: _Generation = live) -> None:
                live.ended = True

            publisher.closed.add_done_callback(mark_ended)
            state.current = live
            up_at = loop.time()
            for track in list(state.tracks.values()):
                track._bind_generation(live)
            set_status("live")
            if first is not None:
                first.set_result(None)
                first = None
            if on_generation is not None:
                on_generation(live.info)

            ended = await publisher.closed
            live.ended = True
            if state.stopped:
                return
            state.current = None
            for track in list(state.tracks.values()):
                track._unbind_generation(live)
            set_status("reconnecting", ended.error)
            # A generation that stayed up longer than the longest delay was a success: start the backoff over.
            if loop.time() - up_at > backoff.max_delay:
                attempt = 0
            await wait_any([stopping], limit=backoff_delay(attempt, backoff))
            attempt += 1

    runner = asyncio.ensure_future(run())
    _observe(runner)
    started = first
    assert started is not None
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
