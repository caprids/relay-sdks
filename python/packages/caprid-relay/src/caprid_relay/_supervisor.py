"""The one supervision loop every supervised connection runs.

It holds one connection at a time, replaces it with backoff whenever it ends, and replaces it
make-before-break before its credential expires. Each connection is a generation. What it carries
moves with it: every publication's tracks (registered once, rebound to each generation's namespace)
and, on a connection that subscribes, every want and publishing-roots view. The supervised publisher,
subscriber and relay are views over this loop that differ only in the connection they open. See
``spec/behavior.md``, "Supervised connections". The counterpart of ``ts/src/supervisor.ts``.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, ClassVar, Literal, NoReturn, TypeAlias, cast

from moq_core import Closed, KeyValuePair

from ._awaitable import RelayAwaitable
from ._discovery import DISCOVERY_CAPACITY, DiscoveryIndex
from ._supervised_track import SupervisedTrackWriter, TrackGeneration, TrackPublisher
from ._supervision import (
    ROTATION_OVERLAP,
    SupervisedStatus,
    _Backoff,
    backoff_delay,
    is_setup_failure,
    new_generation,
    rotation_deadline,
    rotation_due,
    supervisor_closed,
    wait_any,
)
from ._validation import track_name, track_property_pairs
from ._write_slot import RelayTrackWriter, _observe
from .cancellation import CancellationToken
from .connect import _Subscribing
from .connect_session import validate_timeout
from .errors import RelayError
from .publish import DEFAULT_WRITE_TIMEOUT

if TYPE_CHECKING:
    from .supervised_subscriber import _NamespaceWake


@dataclass(frozen=True, slots=True, kw_only=True)
class SupervisedConnection:
    """One connection as a supervisor sees it, whatever opened it."""

    closed: RelayAwaitable[Closed]
    expires_at: datetime | None
    close: Callable[[], Awaitable[None] | None]
    """Close the connection. It returns an awaitable when it drains its publications first."""
    subscriber: _Subscribing | None = None
    """The subscribing side; ``None`` on a connection that only publishes."""
    publish: Callable[[tuple[str, ...]], TrackPublisher] | None = None
    """Announce a namespace on this connection; ``None`` on a connection that only subscribes."""


ConnectSupervised: TypeAlias = Callable[[CancellationToken, str], Awaitable[SupervisedConnection]]
"""How a supervisor opens the connection for a generation, given the attempt's cancellation and the generation field."""


class _NotSubscribing:
    """The subscribing side of a connection that only publishes. Every use raises."""

    def __getattr__(self, name: str) -> NoReturn:
        raise RuntimeError(f"This supervised connection does not subscribe ({name})")


_NO_SUBSCRIBER = cast(_Subscribing, _NotSubscribing())


@dataclass(slots=True, eq=False)
class _Publication:
    """One publication across generations: its root and its tracks."""

    root: tuple[str, ...]
    tracks: dict[str, SupervisedTrackWriter] = field(default_factory=dict)
    names: set[str] = field(default_factory=set)
    """Every name ever registered: a closed track's name stays taken, as on a single connection."""
    closed: bool = False


@dataclass(slots=True, eq=False, kw_only=True)
class _PublicationGeneration(TrackGeneration):
    """One publication on one connection: the namespace it is announced as there, and its tracks' state."""

    namespace: tuple[str, ...]


@dataclass(slots=True, eq=False, kw_only=True)
class _Connection:
    """One connection the loop holds: the subscribing state wants and views use, and what publishes on it."""

    session: SupervisedConnection
    generation: str
    subscriber: _Subscribing
    """The subscribing side; on a connection that only publishes, a stand-in that refuses every use."""
    discovery: DiscoveryIndex
    up_at: float
    """When it went live, on the loop's clock, for resetting the backoff."""
    rotate_at: float | None
    """When it is due to rotate, on the loop's clock; ``None`` for an unknown expiry."""
    over: asyncio.Future[Closed]
    """Settles once the connection ended, or once a publication was lost under it (its ``error``)."""
    end: Literal["session", "fault"] | None = None
    """What ended it: the session reporting its end, or a publication lost while the session stayed open."""
    closing: bool = False
    """Set once the loop closes it on purpose, so a publication ending then is no fault."""
    ended: CancellationToken = field(default_factory=CancellationToken)
    """Cancelled when this connection ends, or once the loop has given it up."""
    retiring: CancellationToken = field(default_factory=CancellationToken)
    """Cancelled when a rotation replaced this connection: its wants move to the new one."""
    publications: dict[_Publication, _PublicationGeneration] = field(default_factory=dict)
    """Each publication's state on this connection."""
    wakes: dict[tuple[str, ...], _NamespaceWake] = field(default_factory=dict)
    """One watch per fixed namespace a want waits on to be announced again, shared by every want on it."""
    holds: int = 0
    """Wants with a subscription and publishing-roots views with a discovery still on this connection; a
    rotation closes it once none are left."""
    idle: asyncio.Future[None] | None = None
    """Set by a rotation; resolved whenever ``holds`` drops to zero."""

    def hold(self) -> Callable[[], None]:
        """Count a use of this connection until the returned release runs."""
        self.holds += 1
        released = False

        def release() -> None:
            nonlocal released
            if released:
                return
            released = True
            self.holds -= 1
            if self.holds == 0 and self.idle is not None and not self.idle.done():
                self.idle.set_result(None)

        return release


# Where the loop stands. Its status and its connections change together, in `Supervisor._enter`.


@dataclass(frozen=True, slots=True)
class _Connecting:
    """No connection has been up yet, and no attempt has failed."""

    status: ClassVar[SupervisedStatus] = "connecting"


@dataclass(frozen=True, slots=True)
class _Serving:
    """``current`` serves; during a rotation's overlap, ``retiring`` is the connection it replaced."""

    current: _Connection
    retiring: _Connection | None = None
    status: ClassVar[SupervisedStatus] = "live"


@dataclass(frozen=True, slots=True)
class _Reconnecting:
    """No connection serves, because the last one ended or every attempt so far failed."""

    status: ClassVar[SupervisedStatus] = "reconnecting"


@dataclass(frozen=True, slots=True)
class _Stopped:
    """Nothing connects or serves any more.

    ``status`` keeps its earlier value until the connections that were open have drained. Then it reads
    ``closed``, and ``shutdown`` settles.
    """

    status: SupervisedStatus
    shutdown: asyncio.Future[None]


_Phase: TypeAlias = _Connecting | _Serving | _Reconnecting | _Stopped


def _close_session(session: SupervisedConnection) -> asyncio.Future[None] | None:
    """Close ``session`` and return the drain to wait for, or ``None`` when it closed at once. Never raises."""
    try:
        closing = session.close()
    except Exception:
        return None
    return None if closing is None else asyncio.ensure_future(_quietly(closing))


async def _close_arriving(connecting: asyncio.Future[SupervisedConnection]) -> None:
    """Close the connection an attempt brings back after the loop stopped, if it brings one back."""
    await asyncio.wait([connecting])
    if connecting.cancelled():
        return
    closing = _close_session(connecting.result())
    if closing is not None:
        await closing


async def _quietly(operation: Awaitable[object]) -> None:
    with contextlib.suppress(Exception):
        await operation


class Supervisor:
    """The loop's handle, which the public views wrap.

    Nothing connects until :meth:`start`; publications made before it are served from the first
    generation on. ``name`` names the supervisor in its errors, as in ``Supervised publisher is closed``.
    """

    def __init__(
        self,
        connect: ConnectSupervised,
        *,
        name: str,
        backoff: _Backoff,
        rotate_before: float,
        cancellation: CancellationToken | None = None,
        on_status: Callable[[SupervisedStatus, BaseException | None], None] | None = None,
        on_generation: Callable[[str], None] | None = None,
    ) -> None:
        loop = asyncio.get_running_loop()
        self.name = name
        self._connect = connect
        self._backoff = backoff
        self._rotate_before = rotate_before
        self._cancellation = cancellation
        self._on_status = on_status
        self._on_generation = on_generation
        self._phase: _Phase = _Connecting()
        self._closed: asyncio.Future[Closed] = loop.create_future()
        # Settles once the first generation is up, or fails as `start()` does. Observed here, because a stop
        # before `start()` hands it to its caller must not leave it unretrieved.
        self._first: asyncio.Future[None] | None = loop.create_future()
        _observe(self._first)
        self._started = False
        # Cancelled when the loop stops. It ends the attempt in progress and the waits between attempts. The
        # loop closes a live connection cleanly instead.
        self._lifetime = CancellationToken()
        # The connection attempt in progress only. Once connected, a connection is closed, not cancelled.
        self._attempt: CancellationToken | None = None
        # The attempt in progress, until the connection it brings back is taken (made current, or discarded):
        # stop() closes a connection that comes back, or was not taken, before it ran.
        self._connecting: asyncio.Future[SupervisedConnection] | None = None
        self._publications: list[_Publication] = []
        # Every root ever published. A closed publication's root stays taken.
        self._roots: set[tuple[str, ...]] = set()
        self._stop_listeners: list[Callable[[], None]] = []
        # Connections ended or given up and still closing, while they drain groups the relay has not acknowledged.
        # `stop()` waits for them, and a publication's `close()` for those that carry it.
        self._draining: dict[_Connection, asyncio.Future[None]] = {}
        self._waiters: list[asyncio.Future[None]] = []
        # Set by `rotate()` while a connection is up, and consumed by the rotation that answers it: the next one
        # to start, or one whose replacement is still connecting or being discarded.
        self._rotate_requested = False
        # Wakes a serve loop waiting for its rotation; replaced for each wait.
        self._wake_rotation: asyncio.Future[None] | None = None
        if cancellation is not None:
            cancellation.add_callback(self._on_cancelled)

    # ------------------------------------------------------------------ what the views read

    @property
    def status(self) -> SupervisedStatus:
        """Where the loop stands; see :data:`SupervisedStatus`."""
        return self._phase.status

    @property
    def current(self) -> _Connection | None:
        """The connection that serves, or ``None`` between connections and after stopping."""
        phase = self._phase
        return phase.current if isinstance(phase, _Serving) else None

    @property
    def generation(self) -> str | None:
        """The live connection's generation field, or ``None`` between connections and after stopping."""
        current = self.current
        return None if current is None else current.generation

    @property
    def stopped(self) -> bool:
        """Whether the loop has stopped."""
        return isinstance(self._phase, _Stopped)

    @property
    def closed(self) -> asyncio.Future[Closed]:
        """Settles once the loop stopped and what was open drained, with what stopped it."""
        return self._closed

    def closed_error(self) -> RelayError:
        """The error a call after stopping raises."""
        return supervisor_closed(self.name)

    def owns_namespace(self, namespace: Sequence[str]) -> bool:
        """Whether ``namespace`` lies under a root this supervisor publishes, or ever did."""
        return any(len(root) < len(namespace) and tuple(namespace[: len(root)]) == root for root in self._roots)

    def on_stop(self, listener: Callable[[], None]) -> None:
        """Add a listener that stops everything subscribing on a connection. The loop calls it first when it stops."""
        self._stop_listeners.append(listener)

    async def live_connection(self, scope: CancellationToken) -> _Connection | None:
        """The live connection, waiting for one while reconnecting; ``None`` once ``scope`` is cancelled or the
        loop stopped."""
        while not scope.cancelled and not self.stopped:
            current = self.current
            if current is not None and not current.ended.cancelled:
                return current
            waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            self._waiters.append(waiter)
            try:
                await wait_any([scope], settles=[waiter])
            finally:
                if waiter in self._waiters:
                    self._waiters.remove(waiter)
        return None

    # ------------------------------------------------------------------ what the views call

    def publish(self, root: tuple[str, ...]) -> SupervisedPublication:
        """Serve ``root`` plus each generation's field on every connection. Raises ``CLOSED`` once stopped."""
        if self.stopped:
            raise self.closed_error()
        if root in self._roots:
            raise ValueError("Namespace is already published on this supervised connection")
        self._roots.add(root)
        publication = _Publication(root)
        self._publications.append(publication)
        current = self.current
        if current is not None:
            self._attach(current, publication, hand_over=False)
        return SupervisedPublication(self, publication)

    async def start(self) -> None:
        """Return once the first generation is up; raise what failed it, or the cancellation's reason."""
        if self._started:
            raise RuntimeError("A supervisor starts once")
        self._started = True
        first = self._first
        assert first is not None
        runner = asyncio.ensure_future(self._supervise())
        _observe(runner)
        try:
            await asyncio.shield(first)
        except asyncio.CancelledError:
            # The caller gave up waiting: nothing will hold the supervisor, so stop it.
            self._forget_cancellation()
            self._stop_quietly(None)
            raise
        except BaseException:
            self._forget_cancellation()
            raise

    def rotate(self) -> None:
        """Replace the live connection make-before-break. A no-op while no connection is up."""
        if self.stopped or self.current is None:
            return
        self._rotate_requested = True
        wake = self._wake_rotation
        if wake is not None and not wake.done():
            wake.set_result(None)

    def stop(self, error: BaseException | None = None) -> asyncio.Future[None]:
        """Stop reconnecting and close every connection. The result settles once their publications drained.

        Raises what ``on_status`` raises reporting ``closed`` when nothing drains; otherwise the result
        carries it. Idempotent.
        """
        self._forget_cancellation()
        return self._stop(error)

    # ------------------------------------------------------------------ the loop

    def _enter(self, phase: _Phase, error: BaseException | None = None) -> None:
        """Move to ``phase``, reporting its status when it changed or comes with what caused it."""
        was = self._phase.status
        self._phase = phase
        if (phase.status != was or error is not None) and self._on_status is not None:
            self._on_status(phase.status, error)

    def _open(self) -> list[_Connection]:
        phase = self._phase
        if not isinstance(phase, _Serving):
            return []
        return [phase.current] if phase.retiring is None else [phase.current, phase.retiring]

    def _all_tracks(self) -> list[SupervisedTrackWriter]:
        return [track for publication in self._publications for track in publication.tracks.values()]

    def _wake_waiters(self) -> None:
        waiters, self._waiters = self._waiters, []
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(None)

    def _report(self, live: _Connection) -> None:
        if self._on_generation is not None:
            self._on_generation(live.generation)

    def _on_cancelled(self, reason: BaseException) -> None:
        self._stop_quietly(reason)

    def _forget_cancellation(self) -> None:
        if self._cancellation is not None:
            self._cancellation.remove_callback(self._on_cancelled)

    def _stop(self, error: BaseException | None) -> asyncio.Future[None]:
        phase = self._phase
        if isinstance(phase, _Stopped):
            return phase.shutdown
        shutdown: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        # A failure reporting `closed` reaches the caller that stopped; a stop nobody awaits leaves it observed.
        _observe(shutdown)
        open_ = self._open()
        self._enter(_Stopped(phase.status, shutdown))
        reason = error if error is not None else self.closed_error()
        self._lifetime.cancel(reason)
        if self._attempt is not None:
            self._attempt.cancel(reason)
        # A stop while the first connection is still being made fails the call that is waiting for it.
        first, self._first = self._first, None
        if first is not None and not first.done():
            first.set_exception(reason)
        listeners, self._stop_listeners = self._stop_listeners, []
        for listener in listeners:
            listener()
        for track in self._all_tracks():
            track._retire()
        for live in open_:
            # Registered as drains, so a publication's `close()` after this one waits for its groups too.
            if live not in self._draining:
                closing = self._close_live(live)
                if closing is not None:
                    self._draining[live] = closing
            self._settle_ended(live)
        pending = list(self._draining.values())
        # A connection still on its way back to the loop, or not yet taken: only this stop sees it.
        if self._connecting is not None:
            pending.append(asyncio.ensure_future(_close_arriving(self._connecting)))
        self._wake_waiters()

        def finish() -> None:
            try:
                self._enter(_Stopped("closed", shutdown))
            finally:
                if not self._closed.done():
                    self._closed.set_result(Closed(error))

        if not pending:
            # Nothing drains: report `closed` now, so a raising on_status reaches the caller as it runs.
            try:
                finish()
            except Exception as raised:
                # Every later stop reports the same failure.
                shutdown.set_exception(raised)
                raise
            shutdown.set_result(None)
            return shutdown

        async def drain() -> None:
            await asyncio.gather(*pending, return_exceptions=True)
            finish()

        def settle(done: asyncio.Future[None]) -> None:
            if done.cancelled():
                shutdown.cancel()
            elif (failure := done.exception()) is not None:
                shutdown.set_exception(failure)
            else:
                shutdown.set_result(None)

        asyncio.ensure_future(drain()).add_done_callback(settle)
        return shutdown

    def _stop_quietly(self, error: BaseException | None) -> None:
        """Stop from a place with no caller to tell. If on_status raises while reporting ``closed``,
        ``closed`` settles anyway."""
        with contextlib.suppress(Exception):
            self._stop(error)

    def _close_live(self, live: _Connection) -> asyncio.Future[None] | None:
        """Close ``live`` on purpose and return the drain to wait for, if its close drains."""
        live.closing = True
        for generation in live.publications.values():
            generation.ended = True
        return _close_session(live.session)

    def _drain_live(self, live: _Connection, after: Callable[[], None] | None = None) -> None:
        """Close a connection ended or given up while the loop runs on, and drain it in the background.

        ``after`` runs once it closed. The connection is closed once, and a later call only adds its ``after``.
        """
        already = self._draining.get(live)
        if already is not None:
            # Started when the connection ended, or for a lost publication, before a rotation gave it up. Its
            # clean-up still runs once it closed.
            if after is not None:
                already.add_done_callback(lambda _: after())
            return
        closing = None if live.closing else self._close_live(live)
        if closing is None:
            if after is not None:
                after()
            return

        async def drain() -> None:
            await closing
            self._draining.pop(live, None)
            if after is not None:
                after()

        drained = asyncio.ensure_future(drain())
        _observe(drained)
        self._draining[live] = drained

    def _settle_ended(self, live: _Connection) -> None:
        """Release what an ended or given-up connection ran, and stop anything still waiting on it."""
        live.ended.cancel()
        live.discovery.close()

    def _end(self, live: _Connection, cause: Literal["session", "fault"], closed: Closed) -> None:
        """Mark ``live`` ended, by its session or by a lost publication.

        The mark lands in the turn the end happened, before anything waiting on the connection resumes, so a
        rotation in progress sees it and falls back to reconnecting.
        """
        if live.end is not None:
            return
        live.end = cause
        live.over.set_result(closed)
        live.ended.cancel()
        if not self.stopped:
            if cause == "fault":
                # Given up for a lost publication while the session is still open, so close it. Its wants move to
                # the next connection once that is up, as on a rotation (`_run`).
                self._drain_live(live, lambda: self._settle_ended(live))
            else:
                # A session can report its end before it drained. After a GOAWAY, a duplex session's publisher
                # still waits for the relay to read its tracks' ends. Closing it waits for that, and stop() for the
                # close.
                self._drain_live(live)
        for generation in live.publications.values():
            generation.ended = True
            # The connection's end closed its groups under the application, which may never close them itself.
            for release in list(generation.group_holds):
                release()

    async def _connect_once(self) -> _Connection | None:
        """Open one connection, or ``None`` if the loop stopped first. Raises the attempt's failure."""
        generation = new_generation()
        self._attempt = attempt = CancellationToken()
        self._connecting = connecting = asyncio.get_running_loop().create_future()
        try:
            session = await self._connect(attempt, generation)
        except BaseException:
            connecting.cancel()
            self._connecting = None
            raise
        finally:
            self._attempt = None
        connecting.set_result(session)
        # stop() ran while the attempt was in progress: it closes what the attempt brought back.
        if self.stopped:
            return None
        loop = asyncio.get_running_loop()
        subscriber = session.subscriber if session.subscriber is not None else _NO_SUBSCRIBER
        live = _Connection(
            session=session,
            generation=generation,
            subscriber=subscriber,
            discovery=DiscoveryIndex(lambda prefix: subscriber.namespace_events(prefix, capacity=DISCOVERY_CAPACITY)),
            up_at=loop.time(),
            rotate_at=rotation_deadline(session.expires_at, self._rotate_before),
            over=loop.create_future(),
        )
        session.closed.add_done_callback(lambda done: self._end(live, "session", done.result()))
        return live

    def _attach(self, live: _Connection, publication: _Publication, *, hand_over: bool) -> list[asyncio.Future[None]]:
        """Announce ``publication`` on ``live`` and register its tracks there.

        The tracks bind at once on a fresh connection, or are handed over on a rotation. Returns the
        hand-overs. A connection that refuses the announce is ending, and nothing is bound.
        """
        publish = live.session.publish
        if publish is None:
            return []
        namespace = (*publication.root, live.generation)
        try:
            announced = publish(namespace)
        except Exception:
            return []
        state = _PublicationGeneration(announced, namespace=namespace)
        live.publications[publication] = state

        def on_closed(done: asyncio.Future[Closed]) -> None:
            state.ended = True
            for release in list(state.group_holds):
                release()
            error = done.result().error
            # The relay stopped soliciting the namespace while the connection is open. A generation serves every
            # root it carries or none, so the connection ends and the next generation announces them all again.
            if (
                error is not None
                and not live.closing
                and live.end is None
                and not publication.closed
                and live.publications.get(publication) is state
            ):
                self._end(live, "fault", Closed(error))

        announced.closed.add_done_callback(on_closed)
        tracks = list(publication.tracks.values())
        if hand_over:
            return [track._hand_over(state) for track in tracks]
        for track in tracks:
            track._bind_generation(state)
        return []

    def _unbind_all(self, live: _Connection) -> None:
        """Unbind every track bound to ``live``. Writes fail with RECONNECTING until the next generation binds."""
        for publication, state in live.publications.items():
            for track in list(publication.tracks.values()):
                track._unbind_generation(state)

    async def _rotate_from(self, live: _Connection, manual: Callable[[], bool]) -> _Connection | None:
        """Bring up the connection that replaces ``live`` and move what it carries, make-before-break.

        ``None`` when ``live`` ended or the loop stopped before a replacement was up, or when an automatic
        rotation found no fresher credential.
        """
        loop = asyncio.get_running_loop()
        failures = 0
        nxt: _Connection | None = None

        # The live connection ending while its replacement is still connecting is a reconnect: report it at once.
        def on_over(done: asyncio.Future[Closed]) -> None:
            if self.stopped or self.current is not live:
                return
            self._unbind_all(live)
            try:
                self._enter(_Reconnecting(), done.result().error)
            except Exception as thrown:
                self._stop_quietly(thrown)

        live.over.add_done_callback(on_over)
        # Also checked on the session's future, because `ended` is cancelled in a callback a turn later, and a
        # connect that fails without suspending would never let it run.
        while not self.stopped and not live.ended.cancelled and not live.session.closed.done():
            try:
                nxt = await self._connect_once()
                break
            except Exception:
                if self.stopped:
                    return None
                # The live connection keeps serving; try again after the backoff unless it ends first.
                await wait_any(
                    [self._lifetime, live.ended],
                    settles=[live.session.closed],
                    limit=backoff_delay(failures, self._backoff),
                )
                failures += 1
        # stop() may have run while the helper's result was on its way here: it closed this connection.
        if nxt is None or self.stopped:
            return None
        # Consulted whatever happened to the old connection: a queued rotate() is answered by this replacement.
        asked = manual()
        if not live.ended.cancelled and not live.session.closed.done() and not asked:
            # A credential that expires no later than the live one replaces nothing: the relay would end both together.
            was, now = live.session.expires_at, nxt.session.expires_at
            if was is not None and (now is None or now <= was):
                if self.stopped:
                    return None
                self._connecting = None
                self._drain_live(nxt)
                return None
        # Checked again just before taking it: an application callback above may have stopped the loop.
        if self.stopped:
            return None
        self._connecting = None
        moved = [
            handed
            for publication in list(self._publications)
            for handed in self._attach(nxt, publication, hand_over=True)
        ]
        self._enter(_Serving(nxt, retiring=live))
        self._report(nxt)
        self._wake_waiters()
        wants_moved: asyncio.Future[None] = loop.create_future()
        live.idle = wants_moved
        # Every want on the old connection ends its attempt there and subscribes on the new one.
        live.retiring.cancel()
        # A want's rotation runs in the turn `retiring` is cancelled and may already have released the last hold.
        if live.holds == 0 and not wants_moved.done():
            wants_moved.set_result(None)

        # A group can close without the wrapper hearing of it, as when the relay resets demand. Release its hold
        # once it is closed, whatever closed it.
        def sweep() -> None:
            for state in live.publications.values():
                for release, group in list(state.group_holds.items()):
                    if group.closed:
                        release()

        async def keep_sweeping() -> None:
            while True:
                await asyncio.sleep(0.05)
                sweep()

        async def tracks_moved_and_idle() -> None:
            await asyncio.gather(*moved)
            sweep()
            for state in list(live.publications.values()):
                await state.wait_idle()

        # Every track and want moved and every write or group still on the old connection finished, or the overlap
        # ran out.
        settled = asyncio.gather(wants_moved, tracks_moved_and_idle())
        _observe(settled)
        sweeper = asyncio.ensure_future(keep_sweeping())
        _observe(sweeper)
        # So does the replacement's own rotation falling due, for a credential shorter than the overlap.
        next_due = asyncio.ensure_future(rotation_due(nxt.rotate_at, loop.create_future()))
        _observe(next_due)
        try:
            # The replacement ending cuts the overlap short too: what it carries needs the connection after it now.
            await wait_any([self._lifetime], settles=[settled, live.over, nxt.over, next_due], limit=ROTATION_OVERLAP)
        finally:
            settled.cancel()
            sweeper.cancel()
            next_due.cancel()
            live.idle = None
            for state in live.publications.values():
                state.idle = None
        for track in self._all_tracks():
            track._settle()
        # A track whose next generation ended before it moved is still bound here: it has no generation now.
        for state in live.publications.values():
            state.ended = True
        self._unbind_all(live)
        phase = self._phase
        if isinstance(phase, _Serving) and phase.retiring is live:
            self._enter(_Serving(phase.current))

        def forget() -> None:
            for publication, state in live.publications.items():
                for track in list(publication.tracks.values()):
                    track._forget(state)

        # Closing drains the old connection's unacknowledged groups for up to a write deadline; the next one is
        # served meanwhile, and stop() waits for the drain.
        self._drain_live(live, forget)
        self._settle_ended(live)
        return nxt

    async def _serve(self, live: _Connection) -> Closed | _Connection:
        """Serve ``live`` until it ends (its ``Closed``), or until a rotation replaced it (the replacement)."""
        loop = asyncio.get_running_loop()
        # Cleared once an automatic rotation found no fresher credential: only rotate() tries again.
        automatic = True
        while True:
            wake: asyncio.Future[None] = loop.create_future()
            self._wake_rotation = wake
            if self._rotate_requested:
                wake.set_result(None)
            due = asyncio.ensure_future(rotation_due(live.rotate_at if automatic else None, wake))
            _observe(due)
            try:
                await wait_any([self._lifetime], settles=[live.over, due])
                if live.over.done():
                    return live.over.result()
                if self.stopped:
                    return Closed(None)
                # A rotate() before the replacement is current makes this rotation a manual one: it takes the
                # replacement.
                manual = False

                def is_manual() -> bool:
                    nonlocal manual
                    if self._rotate_requested:
                        self._rotate_requested = False
                        manual = True
                    return manual

                is_manual()
                nxt = await self._rotate_from(live, is_manual)
                if nxt is not None:
                    return nxt
                if self.stopped:
                    return Closed(None)
                if live.ended.cancelled:
                    return await asyncio.shield(live.over)
                if not manual:
                    automatic = False
            finally:
                due.cancel()
                if self._wake_rotation is wake:
                    self._wake_rotation = None

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        attempt = 0
        # A connection a rotation already brought up, reported and moved everything to.
        nxt: _Connection | None = None
        # A connection given up for a lost publication and still draining. Its wants are still subscribed there,
        # and move to the next connection once it is up, make-before-break, rather than wait out the drain.
        given_up: _Connection | None = None
        while not self.stopped:
            live, nxt = nxt, None
            if live is None:
                try:
                    live = await self._connect_once()
                except Exception as error:
                    if self.stopped:
                        return
                    first = self._first
                    if first is not None and is_setup_failure(error):
                        self._first = None
                        first.set_exception(error)
                        await asyncio.shield(self._stop(error))
                        return
                    self._enter(_Reconnecting(), error)
                    await wait_any([self._lifetime], limit=backoff_delay(attempt, self._backoff))
                    attempt += 1
                    continue
                # stop() may have run while the helper's result was on its way here: it closed this connection.
                if live is None or self.stopped:
                    return
                self._connecting = None
                for publication in list(self._publications):
                    self._attach(live, publication, hand_over=False)
                self._enter(_Serving(live))
                first, self._first = self._first, None
                if first is not None:
                    first.set_result(None)
                self._report(live)
                self._wake_waiters()
                if given_up is not None:
                    given_up.retiring.cancel()
                    given_up = None

            outcome = await self._serve(live)
            if self.stopped:
                return
            if isinstance(outcome, _Connection):
                nxt = outcome
                attempt = 0
                continue
            for state in live.publications.values():
                state.ended = True
            self._unbind_all(live)
            if live.end == "fault":
                given_up = live
            else:
                self._settle_ended(live)
            # Already reported if the connection ended while a rotation's replacement was connecting.
            if not isinstance(self._phase, _Reconnecting):
                self._enter(_Reconnecting(), outcome.error)
            # A connection that stayed up longer than the longest delay was a success: start the backoff over.
            if loop.time() - live.up_at > self._backoff.max_delay:
                attempt = 0
            await wait_any([self._lifetime], limit=backoff_delay(attempt, self._backoff))
            attempt += 1

    async def _supervise(self) -> None:
        try:
            await self._run()
        except Exception as error:
            # An application callback (on_status, on_generation) raised: stop rather than leave the loop neither
            # reconnecting nor closed, and fail a caller still waiting to connect.
            self._stop_quietly(error)

    # ------------------------------------------------------------------ publications

    async def _close_publication(self, publication: _Publication) -> None:
        publication.closed = True
        for track in list(publication.tracks.values()):
            track.close()
        if publication in self._publications:
            self._publications.remove(publication)
        ends: list[Awaitable[None]] = []
        for live in self._open():
            state = live.publications.get(publication)
            if state is None:
                continue
            state.ended = True
            ends.append(_quietly(state.publisher.close()))
        # A connection a rotation or a lost publication gave up still drains this publication's groups.
        ends += [drain for live, drain in self._draining.items() if publication in live.publications]
        await asyncio.gather(*ends)


class SupervisedPublication:
    """One root a supervisor publishes on every connection, as the root followed by the connection's
    generation field. The track writers :meth:`register_track` returns outlive the connections."""

    __slots__ = ("_closing", "_publication", "_supervisor")

    def __init__(self, supervisor: Supervisor, publication: _Publication) -> None:
        self._supervisor = supervisor
        self._publication = publication
        self._closing: asyncio.Future[None] | None = None

    @property
    def root(self) -> tuple[str, ...]:
        """The root, as published."""
        return self._publication.root

    @property
    def namespace(self) -> tuple[str, ...] | None:
        """The namespace the live connection serves, which is the root followed by its generation. ``None``
        between connections."""
        current = self._supervisor.current
        state = None if current is None else current.publications.get(self._publication)
        return None if state is None else state.namespace

    def register_track(
        self,
        track: str,
        *,
        track_properties: Sequence[KeyValuePair] | None = None,
        write_timeout: float = DEFAULT_WRITE_TIMEOUT,
    ) -> RelayTrackWriter:
        """Register a track every generation serves under this root, and return the writer that outlives them.

        The rules of :meth:`SupervisedPublisher.register_track` apply, checked at once. Names are unique per
        publication, also after their writer closes. Writes wait for demand on the live connection, and
        raise ``RECONNECTING`` while none is up.
        """
        name = track_name(track)
        properties = track_property_pairs(track_properties)
        validate_timeout("write_timeout", write_timeout)
        supervisor, publication = self._supervisor, self._publication
        if supervisor.stopped or publication.closed:
            raise supervisor.closed_error()
        if name in publication.names:
            raise ValueError("Track name is already registered on this supervised publication")
        tracks = publication.tracks

        def forget() -> None:
            tracks.pop(name, None)

        writer = SupervisedTrackWriter(name, properties, write_timeout, forget)
        tracks[name] = writer
        publication.names.add(name)
        current = supervisor.current
        state = None if current is None else current.publications.get(publication)
        if state is not None:
            writer._bind_generation(state)
        return writer

    async def close(self) -> None:
        """End the publication's tracks, wait for their unacknowledged groups, and withdraw it from every
        connection.

        The connection and the other publications stay open, and the root stays taken. Idempotent.
        """
        if self._closing is None:
            self._closing = asyncio.ensure_future(self._supervisor._close_publication(self._publication))
        await asyncio.shield(self._closing)


__all__ = ["ConnectSupervised", "SupervisedConnection", "SupervisedPublication", "Supervisor"]
