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
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from types import TracebackType
from typing import Self, TypeVar

from moq_core import Closed, KeyValuePair

from ._awaitable import RelayAwaitable
from ._supervised_track import SupervisedTrackWriter, TrackGeneration
from ._supervision import (
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
from ._validation import namespace_fields, track_name, track_property_pairs
from ._write_slot import RelayTrackWriter, _observe
from .cancellation import CancellationToken
from .connect_session import DEFAULT_CONNECT_TIMEOUT, RelayTransportFactory, TokenProvider, validate_timeout
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


@dataclass(slots=True, eq=False, kw_only=True)
class _Generation(TrackGeneration):
    """One generation as the supervisor holds it."""

    info: PublisherGeneration
    connection: RelayPublisher
    """The connection itself; ``publisher`` is the same one, as the tracks see it."""
    up_at: float = 0.0
    """When it went live, on the loop's clock, for resetting the backoff."""
    rotate_at: float | None = None
    """When it is due to rotate, on the loop's clock; ``None`` for an unknown expiry."""


def _discard(tracks: dict[str, SupervisedTrackWriter], name: str) -> None:
    tracks.pop(name, None)


@dataclass(slots=True, eq=False)
class _State:
    """The supervisor's mutable state, shared by its run loop and the handle."""

    tracks: dict[str, SupervisedTrackWriter] = field(default_factory=dict)
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
        writer = SupervisedTrackWriter(name, properties, write_timeout, lambda: _discard(state.tracks, name))
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
    url: str | None = None,
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
    With no ``url``, every generation dials the endpoint the first one rooted at its token's root. See
    ``spec/behavior.md``, "Supervised connections"; :func:`connect_relay_publisher` never reconnects.
    """

    async def connect(fields: tuple[str, ...], attempt: CancellationToken) -> RelayPublisher:
        nonlocal url
        publisher = await connect_relay_publisher(
            url,
            namespace=fields,
            get_token=get_token,
            cancellation=attempt,
            connect_timeout=connect_timeout,
            create_transport=create_transport,
        )
        if url is None:
            url = publisher._lifetime.endpoint
        return publisher

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
                    await live.connection.close()
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
            info=PublisherGeneration(generation, fields),
            publisher=publisher,
            connection=publisher,
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

        live.connection.closed.add_done_callback(on_live_closed)
        # Checked on the future itself too: the callback that marks `ended` runs a turn later, and a connect that
        # fails without suspending would never let it.
        while not state.stopped and not live.ended and not live.connection.closed.done():
            try:
                nxt = await connect_generation()
                break
            except Exception:
                if state.stopped:
                    return None
                # The live generation keeps serving; try again after the backoff unless it ends first.
                await wait_any([stopping], settles=[live.connection.closed], limit=backoff_delay(failures, backoff))
                failures += 1
        if nxt is None:
            return None
        # close() may have run while the helper's result was on its way here: it could not see this connection.
        if state.stopped:
            nxt.ended = True
            with contextlib.suppress(Exception):
                await nxt.connection.close()
            return None
        # Consulted whatever happened to the old generation: a queued rotate() is answered by this replacement.
        asked = manual()
        if not live.ended and not live.connection.closed.done() and not asked:
            # A credential that expires no later than the live one replaces nothing: the relay would end both together.
            was, now = live.connection.expires_at, nxt.connection.expires_at
            if was is not None and (now is None or now <= was):
                nxt.ended = True
                with contextlib.suppress(Exception):
                    await nxt.connection.close()
                return None
        state.current = nxt
        state.retiring = live
        moved = [track._hand_over(nxt) for track in list(state.tracks.values())]
        set_status("live")
        report(nxt)

        # A group can close without the wrapper hearing of it (the relay resetting demand): release its hold
        # once it is closed, whatever closed it.
        def sweep() -> None:
            for release, group in list(live.group_holds.items()):
                if group.closed:
                    release()

        async def keep_sweeping() -> None:
            while True:
                await asyncio.sleep(0.05)
                sweep()

        async def moved_and_idle() -> None:
            await asyncio.gather(*moved)
            sweep()
            await live.wait_idle()

        # Every track moved and every write or group still on the old generation finished, or the overlap ran out.
        settled = asyncio.ensure_future(moved_and_idle())
        _observe(settled)
        sweeper = asyncio.ensure_future(keep_sweeping())
        _observe(sweeper)
        # So does the next generation's own rotation falling due, for a credential shorter than the overlap.
        next_due = asyncio.ensure_future(rotation_due(nxt.rotate_at, loop.create_future()))
        _observe(next_due)
        try:
            # The next generation ending cuts the overlap short too: its tracks need the generation after it now.
            await wait_any(
                [stopping],
                settles=[settled, live.connection.closed, nxt.connection.closed, next_due],
                limit=ROTATION_OVERLAP,
            )
        finally:
            settled.cancel()
            sweeper.cancel()
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
                await live.connection.close()
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
                await wait_any([stopping], settles=[live.connection.closed, due])
                if live.connection.closed.done() or state.stopped:
                    return await live.connection.closed
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
                    return await live.connection.closed
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
                        await live.connection.close()
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
