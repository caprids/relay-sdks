"""The supervised subscriber: one subscriber connection at a time, replaced with backoff whenever it
ends, and one supervisor per wanted track that keeps a subscription to it open across connections,
publisher restarts and generations.

Each want retries the announce race, backs off after a publisher's done or a rejection, resubscribes
a subscription that went stale, and follows a supervised publisher's newest generation through
namespace discovery. Objects are never fetched to fill a gap: a replacement subscription starts
live, and ``on_restarted`` tells the application to drop state that assumed continuity. A move to a
newer generation, or to another track or parameters on a re-want, is made before the break instead,
as ``switch_to`` makes one (issue #282): the replacement opens while the current subscription keeps
delivering, and ``on_switched`` marks the cutover. Before the connection's credential expires, the
supervisor rotates to a new connection the same way (issue #261): every want's subscription moves to
it make-before-break, and a want that keeps its target sees no break and no repeat. See
``spec/behavior.md``, "Supervised connections". The counterpart of ``ts/src/supervised-subscriber.ts``.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable, Sequence
from copy import deepcopy
from dataclasses import dataclass, field, fields, replace
from types import TracebackType
from typing import Literal, Self, TypeAlias

from moq_core import (
    Closed,
    MessageParameters,
    MoqtObject,
    PublishDone,
    RequestError,
    SubgroupEnd,
    SubgroupReset,
    SubscribeOk,
)
from moq_core.errors import is_moqt_protocol_error

from ._awaitable import RelayAwaitable
from ._subscription import (
    ObjectEvent,
    RelaySubscription,
    SubgroupEndEvent,
    SubgroupResetEvent,
    SubscriptionConnectionClosedError,
    SubscriptionEnd,
    SubscriptionRequestError,
    synchronous_callback,
)
from ._supervision import (
    RECONNECT_BACKOFF,
    RESUBSCRIBE_BACKOFF,
    ROTATION_OVERLAP,
    SupervisedStatus,
    SupervisionBackoff,
    _Backoff,
    backoff_delay,
    backoff_options,
    is_fresh_namespace_rejection,
    is_permanent_request_error,
    is_setup_failure,
    on_cancel,
    rotate_before_option,
    rotation_deadline,
    rotation_due,
    supervisor_closed,
    wait_any,
)
from ._validation import namespace_fields, track_name
from ._write_slot import _observe
from .cancellation import CancellationToken
from .connect import RelaySubscriber, RelaySubscriptionHandlers, connect_relay_subscriber
from .connect_session import (
    DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_REQUEST_TIMEOUT,
    RelayTransportFactory,
    TokenProvider,
    validate_timeout,
)
from .errors import RelayError
from .namespace_events import NamespaceEvents
from .switchable import Until, _check_drain, _held_after, _HeldEvent, _hold, _until_predicate

_log = logging.getLogger("mux_relay")

RestartReason: TypeAlias = Literal["publisher-done", "stale", "generation", "connection", "terminated"]
"""Why a want replaced its subscription.

``publisher-done``: the relay sent PUBLISH_DONE. ``stale``: no object arrived for ``stale_after``.
``generation``: the current generation was withdrawn with none newer, or a move to a newer one
could not be made before the break. ``connection``:
the subscriber connection ended and was replaced. ``terminated``: the relay ended the subscription
with REQUEST_ERROR after accepting it.
"""

_DISCOVERY_CAPACITY = 1024
"""Events a root's discovery may queue: its first answer lists every namespace under the root."""

_ROOT_LINGER = 5.0
"""How long an unused root's discovery is kept, so a want that resubscribes at once reuses its snapshot."""


@dataclass(frozen=True, slots=True)
class WantSwitchedInfo:
    """What ``on_switched`` receives at a want's cutover."""

    from_namespace: tuple[str, ...]
    """The namespace the want delivered from before the cutover: the generation's, when following one."""
    from_track: str
    """The track the want delivered from before the cutover."""
    to_namespace: tuple[str, ...]
    """The namespace it delivers from now."""
    to_track: str
    """The track it delivers from now."""
    subscribed: SubscribeOk
    """The new subscription's acceptance info."""


@dataclass(slots=True, kw_only=True)
class WantHandlers:
    """The callbacks a want delivers to. Like :class:`RelaySubscriptionHandlers`, they must be synchronous."""

    on_subscribed: Callable[[SubscribeOk, tuple[str, ...]], None] | None = None
    """A subscription was accepted. The second argument is the namespace it was made on: the
    generation's, when following one. A move's replacement reports its acceptance in
    ``on_switched`` instead."""
    on_object: Callable[[MoqtObject], None] | None = None
    """One object, from whichever subscription is current."""
    on_subgroup_end: Callable[[SubgroupEnd], None] | None = None
    """A subgroup stream ended cleanly."""
    on_subgroup_reset: Callable[[SubgroupReset], None] | None = None
    """A subgroup stream was reset."""
    on_publisher_done: Callable[[PublishDone], None] | None = None
    """The relay sent PUBLISH_DONE on the current subscription; the want resubscribes after its backoff."""
    on_restarted: Callable[[RestartReason, tuple[str, ...]], None] | None = None
    """An accepted subscription ended and the want will replace it: drop decoder state that assumed
    continuity. Receives the reason and the namespace the ended subscription was made on. Called
    before any object of the replacement; the gap is never filled."""
    on_switched: Callable[[WantSwitchedInfo], None] | None = None
    """A move's cutover: the want now delivers from the ``to_`` subscription, and the ``from_`` one
    was cancelled or drains its in-flight groups. Runs before the replacement's first object, the one
    that passed ``until``. Decoders drop state that assumed continuity here; no ``on_restarted`` or
    ``on_subscribed`` runs for a move."""
    on_error: Callable[[BaseException], None] | None = None
    """A failure the want does not retry, after which it stops and is removed: a REQUEST_ERROR no
    wait fixes (credentials, an unsupported or malformed request) as a
    :class:`SubscriptionRequestError`, or discovery refusing the root."""


@dataclass(slots=True, eq=False)
class _Connection:
    """One live subscriber connection and the discovery it runs for followed roots."""

    subscriber: RelaySubscriber
    ended: CancellationToken
    """Cancelled when this connection ends."""
    retiring: CancellationToken = field(default_factory=CancellationToken)
    """Cancelled when a rotation replaced this connection: its wants move to the new one."""
    roots: dict[tuple[str, ...], _RootWatch] = field(default_factory=dict)
    """One discovery per followed root, shared by every want on it."""
    holds: int = 0
    """Wants with a subscription still on this connection; a rotation closes it once none are left."""
    idle: asyncio.Future[None] | None = None
    """Set by a rotation; resolved whenever ``holds`` drops to zero."""
    rotate_at: float | None = None
    """When it is due to rotate, on the loop's clock; ``None`` for an unknown expiry."""

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


@dataclass(frozen=True, slots=True)
class _WantOptions:
    namespace: tuple[str, ...]
    track: str
    generations: bool
    stale_after: float | None
    parameters: MessageParameters | None


@dataclass(slots=True, eq=False)
class _Want:
    """One want as the supervisor holds it."""

    options: _WantOptions
    handlers: dict[str, Callable[..., None]]
    retry: _Backoff
    until: Callable[[MoqtObject], bool]
    drain: float
    changes: list[Callable[[], None]] = field(default_factory=list)
    """Told when a re-want changes the want in place, by whichever attempt or backoff is running."""
    controller: CancellationToken = field(default_factory=CancellationToken)
    namespace: tuple[str, ...] = ()
    """The namespace the latest subscription was made on: the generation's when following one."""
    generation: str | None = None
    """The generation the latest subscription was made on, when following one."""
    connection: _Connection | None = None
    """The connection the latest subscription was made on: a rotation moves it to the new one."""
    deferred: BaseException | None = None
    settled_target: tuple[str, MessageParameters | None] | None = None
    """The track and parameters when the latest attempt settled: a later re-want supersedes its outcome."""
    """A discovery failure no wait fixes, seen while backing off, for the loop to surface."""

    def call(self, name: str, *args: object) -> None:
        """Deliver to one handler; nothing reaches a want after it was cancelled, except its own ``on_error``."""
        if self.controller.cancelled and name != "on_error":
            return
        callback = self.handlers.get(name)
        if callback is not None:
            callback(*args)

    def changed(self) -> None:
        for listener in list(self.changes):
            listener()


def _guarded(handlers: WantHandlers) -> dict[str, Callable[..., None]]:
    """The primitive's guard on every handler: a declared-async callback is refused, a returned awaitable observed."""
    guarded: dict[str, Callable[..., None]] = {}
    for handler_field in fields(handlers):
        callback = getattr(handlers, handler_field.name)
        if callback is None:
            continue
        if not callable(callback):
            raise TypeError(f"{handler_field.name} must be callable")
        guarded[handler_field.name] = synchronous_callback(handler_field.name, callback)
    return guarded


@dataclass(slots=True, eq=False)
class _State:
    wants: dict[str, _Want] = field(default_factory=dict)
    status: SupervisedStatus = "connecting"
    stopped: bool = False
    current: _Connection | None = None
    retiring: _Connection | None = None
    """The connection a rotation is replacing, while its wants move off it."""
    rotate_requested: bool = False
    """Set by ``rotate()`` while a connection is up, and consumed by the rotation that answers it: the next one
    to start, or one whose replacement is still connecting or being discarded."""
    wake: asyncio.Future[None] | None = None
    """Wakes a serve loop waiting for its rotation; replaced for each wait."""
    waiters: list[asyncio.Future[None]] = field(default_factory=list)

    def wake_waiters(self) -> None:
        waiters, self.waiters = self.waiters, []
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(None)


class SupervisedSubscriber:
    """A subscriber that reconnects and keeps wanted tracks subscribed. Create one with
    :func:`connect_supervised_subscriber`."""

    __slots__ = ("_close", "_closed", "_state")

    def __init__(self, state: _State, closed: asyncio.Future[Closed], close: Callable[[], None]) -> None:
        self._state = state
        self._closed = RelayAwaitable(closed)
        self._close = close

    @property
    def status(self) -> SupervisedStatus:
        """Where the supervisor stands; see :data:`SupervisedStatus`."""
        return self._state.status

    @property
    def closed(self) -> RelayAwaitable[Closed]:
        """Resolves once the supervisor stops, with the failure that stopped it or the cancellation's
        reason as ``error`` (``None`` after :meth:`close`). Never raises."""
        return self._closed

    def want(
        self,
        key: str,
        namespace: Sequence[str],
        track: str,
        handlers: WantHandlers | None = None,
        *,
        parameters: MessageParameters | None = None,
        generations: bool = False,
        stale_after: float | None = None,
        retry: SupervisionBackoff | None = None,
        until: Until = "first-object",
        drain: float = 0.0,
    ) -> None:
        """Keep a subscription to a track open until ``unwant(key)``.

        ``generations=True`` makes ``namespace`` a supervised publisher's root: the want follows the
        newest generation announced one field below it, and the subscriber's token must allow
        discovering the root. ``stale_after`` (seconds) resubscribes when no object arrived for that
        long, for tracks that are never silent while live. ``parameters`` is sent on every SUBSCRIBE;
        ``retry`` is the backoff between resubscriptions (from 0.25 s, doubling up to 8 s).

        A move (a newer generation, or a re-want with another track or parameters) is made before the
        break, as ``switch_to`` makes one: the current subscription keeps delivering until an object
        of the replacement passes ``until`` (``"first-object"``, the default, or a synchronous
        predicate), and ``on_switched`` reports the cutover. Compare the object's own timeline with
        what was delivered, never group IDs: each generation and track numbers its own groups.
        ``drain`` (seconds, default 0) keeps the old subscription delivering its in-flight groups
        that long after the cutover.

        Wanting a key again with the same namespace and ``generations`` updates the want in place:
        the handlers, ``retry``, ``stale_after``, ``until`` and ``drain`` are swapped, and another
        track or parameters moves the subscription. Another namespace or ``generations`` replaces the
        want. Arguments are checked at once.
        """
        if not isinstance(key, str):
            raise TypeError("want key must be a string")
        if handlers is None:
            handlers = WantHandlers()
        if not isinstance(handlers, WantHandlers):
            raise TypeError("want handlers must be WantHandlers")
        safe_handlers = _guarded(handlers)
        fields_ = namespace_fields(namespace)
        if not isinstance(generations, bool):
            raise TypeError("generations must be a bool")
        if generations:
            namespace_fields((*fields_, "x"))
        name = track_name(track)
        if parameters is not None and not isinstance(parameters, MessageParameters):
            raise TypeError("parameters must be MessageParameters")
        if stale_after is not None:
            validate_timeout("stale_after", stale_after)
        retry_backoff = backoff_options("retry", retry, RESUBSCRIBE_BACKOFF)
        predicate = _until_predicate(until)
        _check_drain(drain)
        state = self._state
        if state.stopped:
            raise supervisor_closed("subscriber")

        # A snapshot, so the caller's later changes to the parameters neither reach the wire nor the identity.
        options = _WantOptions(fields_, name, generations, stale_after, deepcopy(parameters))
        existing = state.wants.get(key)
        if existing is not None and (existing.options.namespace, existing.options.generations) == (
            fields_,
            generations,
        ):
            existing.handlers = safe_handlers
            existing.retry = retry_backoff
            existing.until = predicate
            existing.drain = float(drain)
            if existing.options == options:
                return
            existing.options = replace(
                existing.options, track=name, parameters=options.parameters, stale_after=stale_after
            )
            existing.changed()
            return
        if existing is not None:
            self._forget(key, existing)
        want = _Want(options, safe_handlers, retry_backoff, predicate, float(drain))
        state.wants[key] = want
        task = asyncio.ensure_future(
            _run_want(want, self._live_connection, lambda: state.current, lambda: self._forget(key, want))
        )
        _observe(task)

    def unwant(self, key: str) -> None:
        """Cancel a want's subscription and forget it. Unknown keys are ignored."""
        want = self._state.wants.get(key)
        if want is not None:
            self._forget(key, want)

    def rotate(self) -> None:
        """Replace the connection make-before-break, as happens on its own ``rotate_before`` before the
        credential expires.

        A new connection opens, with a fresh token, and every want moves its subscription to it as a
        move would, keeping the old one delivering until the cutover. A want whose target is unchanged
        cuts over on the first object the old one did not deliver, with no callback: nothing is lost
        or repeated. The old connection closes once every want has moved, after 10 seconds at most. A
        no-op while no connection is up.
        """
        state = self._state
        if state.stopped or state.current is None:
            return
        state.rotate_requested = True
        if state.wake is not None and not state.wake.done():
            state.wake.set_result(None)

    def close(self) -> None:
        """Stop reconnecting, cancel every want and close the connection. Idempotent."""
        self._close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        self.close()

    def _forget(self, key: str, want: _Want) -> None:
        want.controller.cancel()
        if self._state.wants.get(key) is want:
            del self._state.wants[key]

    async def _live_connection(self, scope: CancellationToken) -> _Connection | None:
        """The live connection, waiting for one while reconnecting; ``None`` once ``scope`` is cancelled."""
        state = self._state
        while not scope.cancelled and not state.stopped:
            if state.current is not None and not state.current.ended.cancelled:
                return state.current
            waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            state.waiters.append(waiter)
            try:
                await wait_any([scope], settles=[waiter])
            finally:
                if waiter in state.waiters:
                    state.waiters.remove(waiter)
        return None


async def connect_supervised_subscriber(
    url: str,
    *,
    get_token: TokenProvider | None = None,
    cancellation: CancellationToken | None = None,
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
    request_timeout: float = DEFAULT_REQUEST_TIMEOUT,
    create_transport: RelayTransportFactory | None = None,
    reconnect: SupervisionBackoff | None = None,
    rotate_before: float | None = None,
    on_status: Callable[[SupervisedStatus, BaseException | None], None] | None = None,
) -> SupervisedSubscriber:
    """Open a subscriber that reconnects and keeps every wanted track subscribed across connections,
    publisher restarts and generations.

    Returns once the first connection is up. A first attempt that fails on its arguments, or because
    the relay refused the credentials (``UNAUTHORIZED``), raises that failure; any other failure is
    retried with backoff (``reconnect``, from 0.5 s doubling up to 10 s), bounded only by
    ``cancellation``, whose reason is raised. ``on_status`` runs on every status change, with what
    ended the previous connection or failed the last attempt. ``rotate_before`` (seconds, default 60)
    is how long before the connection's credential expires (:attr:`RelaySubscriber.expires_at`) a
    replacement connection is opened, make-before-break; never earlier than halfway through the time
    left. See ``spec/behavior.md``, "Supervised connections"; :func:`connect_relay_subscriber` never
    reconnects or resubscribes.
    """

    def connect(attempt: CancellationToken) -> Awaitable[RelaySubscriber]:
        return connect_relay_subscriber(
            url,
            get_token=get_token,
            cancellation=attempt,
            connect_timeout=connect_timeout,
            request_timeout=request_timeout,
            create_transport=create_transport,
        )

    return await _supervise_relay_subscriber(
        connect, cancellation=cancellation, reconnect=reconnect, rotate_before=rotate_before, on_status=on_status
    )


ConnectSubscriber = Callable[[CancellationToken], Awaitable[RelaySubscriber]]
"""How a supervisor opens one subscriber connection, given the attempt's cancellation."""


async def _supervise_relay_subscriber(
    connect: ConnectSubscriber,
    *,
    cancellation: CancellationToken | None = None,
    reconnect: SupervisionBackoff | None = None,
    rotate_before: float | None = None,
    on_status: Callable[[SupervisedStatus, BaseException | None], None] | None = None,
) -> SupervisedSubscriber:
    """Open a supervised subscriber with ``connect``; the tests drive it with scripted connections."""
    backoff = backoff_options("reconnect", reconnect, RECONNECT_BACKOFF)
    rotate_margin = rotate_before_option(rotate_before)
    if cancellation is not None:
        cancellation.throw_if_cancelled()

    loop = asyncio.get_running_loop()
    state = _State()
    closed: asyncio.Future[Closed] = loop.create_future()
    lifetime = CancellationToken()
    # The connection attempt in progress only: a live connection is closed cleanly on close().
    attempt_in_progress: CancellationToken | None = None
    first: asyncio.Future[None] | None = loop.create_future()

    def set_status(status: SupervisedStatus, error: BaseException | None = None) -> None:
        if state.status == status and error is None:
            return
        state.status = status
        if on_status is not None:
            on_status(status, error)

    def stop(error: BaseException | None) -> None:
        nonlocal first
        if state.stopped:
            return
        state.stopped = True
        reason = error if error is not None else supervisor_closed("subscriber")
        lifetime.cancel(reason)
        if attempt_in_progress is not None:
            attempt_in_progress.cancel(reason)
        # A stop while the first connection is still being made fails the call that is waiting for it.
        if first is not None and not first.done():
            first.set_exception(reason)
        first = None
        wants = list(state.wants.values())
        state.wants.clear()
        for want in wants:
            want.controller.cancel()
        if state.current is not None:
            state.current.subscriber.close()
        state.current = None
        if state.retiring is not None:
            state.retiring.subscriber.close()
        state.retiring = None
        state.wake_waiters()
        try:
            set_status("closed")
        finally:
            if not closed.done():
                closed.set_result(Closed(error))

    def on_cancelled(reason: BaseException) -> None:
        stop(reason)

    if cancellation is not None:
        cancellation.add_callback(on_cancelled)

    async def connect_once() -> _Connection | None:
        """Open one connection, or ``None`` if the supervisor stopped first. Raises the attempt's failure."""
        nonlocal attempt_in_progress
        attempt_in_progress = CancellationToken()
        try:
            subscriber = await connect(attempt_in_progress)
        finally:
            attempt_in_progress = None
        if state.stopped:
            subscriber.close()
            return None
        live = _Connection(
            subscriber, CancellationToken(), rotate_at=rotation_deadline(subscriber.expires_at, rotate_margin)
        )
        # Cancelled the moment the connection ends, so a rotation in progress sees it and falls back to reconnecting.
        subscriber.closed.add_done_callback(lambda _: live.ended.cancel())
        return live

    def settle_ended(live: _Connection) -> None:
        """The connection ended: release what it ran and stop anything still waiting on it."""
        live.ended.cancel()
        for watch in list(live.roots.values()):
            watch.close()

    async def rotate_from(old: _Connection, manual: Callable[[], bool]) -> _Connection | None:
        """Open the connection that replaces ``old``, move every want to it, and close ``old`` once they moved.

        ``None`` when ``old`` ended or the supervisor stopped before a replacement was up, or when an
        automatic rotation found no fresher credential.
        """
        failures = 0
        nxt: _Connection | None = None

        # The live connection ending while its replacement is still connecting is a reconnect: report it at once.
        def on_old_closed(done: asyncio.Future[Closed]) -> None:
            if state.stopped or state.current is not old:
                return
            state.current = None
            try:
                set_status("reconnecting", done.result().error)
            except Exception as thrown:
                with contextlib.suppress(Exception):
                    # A raising on_status while reporting `closed`: `closed` is settled either way.
                    stop(thrown)

        old.subscriber.closed.add_done_callback(on_old_closed)
        while not state.stopped and not old.ended.cancelled:
            try:
                nxt = await connect_once()
                break
            except Exception:
                if state.stopped:
                    return None
                # The live connection keeps serving; try again after the backoff unless it ends first.
                await wait_any([lifetime], settles=[old.subscriber.closed], limit=backoff_delay(failures, backoff))
                failures += 1
        if nxt is None:
            return None
        # close() may have run while the helper's result was on its way here: it could not see this connection.
        if state.stopped:
            nxt.subscriber.close()
            return None
        # Consulted whatever happened to the old connection: a queued rotate() is answered by this replacement.
        asked = manual()
        if not old.ended.cancelled and not asked:
            # A credential that expires no later than the live one replaces nothing: the relay would end both together.
            was, now = old.subscriber.expires_at, nxt.subscriber.expires_at
            if was is not None and (now is None or now <= was):
                nxt.subscriber.close()
                return None
        state.current = nxt
        state.retiring = old
        set_status("live")
        state.wake_waiters()
        old.idle = loop.create_future()
        # Every want on the old connection ends its attempt there and subscribes on the new one.
        old.retiring.cancel()
        if old.holds == 0:
            old.idle.set_result(None)
        # The replacement ending cuts the overlap short too: the wants on it need the next connection now. So
        # does its own rotation falling due, for a credential shorter than the overlap.
        next_due = asyncio.ensure_future(rotation_due(nxt.rotate_at, loop.create_future()))
        _observe(next_due)
        try:
            await wait_any(
                [lifetime, old.ended, nxt.ended],
                settles=[old.idle, old.subscriber.closed, next_due],
                limit=ROTATION_OVERLAP,
            )
        finally:
            next_due.cancel()
        if state.retiring is old:
            state.retiring = None
        old.subscriber.close()
        settle_ended(old)
        return nxt

    async def serve(live: _Connection) -> Closed | _Connection:
        """Serve ``live`` until it ends (its ``Closed``), or until a rotation replaced it (the new connection)."""
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
                await wait_any([lifetime], settles=[live.subscriber.closed, due])
                if live.subscriber.closed.done() or state.stopped:
                    return await live.subscriber.closed
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
                if live.ended.cancelled or state.stopped:
                    return await live.subscriber.closed
                if not manual:
                    automatic = False
            finally:
                due.cancel()
                if state.wake is wake:
                    state.wake = None

    async def run() -> None:
        nonlocal first
        attempt = 0
        # A connection a rotation already opened and moved the wants to.
        nxt: _Connection | None = None
        while not state.stopped:
            live, nxt = nxt, None
            if live is None:
                try:
                    live = await connect_once()
                except Exception as error:
                    if state.stopped:
                        return
                    if first is not None and is_setup_failure(error):
                        first.set_exception(error)
                        first = None
                        stop(error)
                        return
                    set_status("reconnecting", error)
                    await wait_any([lifetime], limit=backoff_delay(attempt, backoff))
                    attempt += 1
                    continue
                if live is None:
                    return
                # close() may have run while the helper's result was on its way here: it could not see this connection.
                if state.stopped:
                    live.subscriber.close()
                    return
                state.current = live
                set_status("live")
                if first is not None:
                    first.set_result(None)
                    first = None
                state.wake_waiters()
            up_at = loop.time()

            outcome = await serve(live)
            if isinstance(outcome, _Connection):
                if state.stopped:
                    return
                nxt = outcome
                attempt = 0
                continue
            settle_ended(live)
            if state.stopped:
                return
            state.current = None
            # Already reported if the connection ended while a rotation's replacement was connecting.
            if state.status != "reconnecting":
                set_status("reconnecting", outcome.error)
            if loop.time() - up_at > backoff.max_delay:
                attempt = 0
            await wait_any([lifetime], limit=backoff_delay(attempt, backoff))
            attempt += 1

    async def supervise() -> None:
        try:
            await run()
        except Exception as error:
            # An application callback (on_status, on_generation) raised: stop rather than leave the
            # supervisor neither reconnecting nor closed, and fail a caller still waiting to connect.
            stop(error)

    runner = asyncio.ensure_future(supervise())
    _observe(runner)
    started = first
    assert started is not None
    # A caller that stops waiting detaches from the shield, which then no longer observes the result.
    _observe(started)
    try:
        await asyncio.shield(started)
    except asyncio.CancelledError:
        if cancellation is not None:
            cancellation.remove_callback(on_cancelled)
        stop(None)
        raise
    except BaseException:
        if cancellation is not None:
            cancellation.remove_callback(on_cancelled)
        raise

    def close() -> None:
        if cancellation is not None:
            cancellation.remove_callback(on_cancelled)
        stop(None)

    return SupervisedSubscriber(state, closed, close)


def _refused_for_good(error: BaseException) -> bool:
    """Whether a request refused at the call is the want's own fault, which no connection fixes.

    An argument, a request the wire cannot carry, or a root overlapping another followed root.
    Anything else raised there (``CLOSED``, the connection's ``TRANSPORT_ERROR`` or ``GOAWAY``) is the
    connection ending before the supervisor has heard of it.
    """
    if isinstance(error, TypeError | ValueError) or is_moqt_protocol_error(error):
        return True
    return isinstance(error, RelayError) and error.code == "PREFIX_OVERLAP"


@dataclass(frozen=True, slots=True)
class _Outcome:
    """What ended one attempt, and what the want does next.

    ``cancelled``: the want or the supervisor was cancelled. ``connection``: the connection ended;
    move to the next one without delay. ``moved``: the followed generation was withdrawn with none
    newer, or the want moved before its subscription was accepted; resubscribe without delay.
    ``fatal``: a failure no wait fixes; surface it and stop. ``retry``: anything else; resubscribe
    after the backoff.
    """

    kind: Literal["cancelled", "connection", "moved", "fatal", "retry"]
    accepted: bool = False
    progressed: bool = False
    reason: RestartReason | None = None
    error: BaseException | None = None


_CANCELLED = _Outcome("cancelled")


async def _run_want(
    want: _Want,
    live_connection: Callable[[CancellationToken], Awaitable[_Connection | None]],
    next_connection: Callable[[], _Connection | None],
    forget: Callable[[], None],
) -> None:
    """Supervise ``want``; a failure the loop did not expect forgets it rather than leave it registered but dead."""
    try:
        await _supervise_want(want, live_connection, next_connection, forget)
    except Exception:
        forget()
        _log.exception("a want stopped on an unexpected failure and was forgotten")


async def _supervise_want(
    want: _Want,
    live_connection: Callable[[CancellationToken], Awaitable[_Connection | None]],
    next_connection: Callable[[], _Connection | None],
    forget: Callable[[], None],
) -> None:
    """Keep ``want`` subscribed until it is cancelled or fails for good."""
    scope = want.controller
    failures = 0

    def rewanted() -> None:
        # A re-want asks for something new: its first failure waits the shortest delay again.
        nonlocal failures
        failures = 0

    want.changes.append(rewanted)
    try:
        while not scope.cancelled:
            if want.deferred is not None:
                # A discovery refusal seen while backing off is reported as one seen during an attempt would be.
                forget()
                want.call("on_error", want.deferred)
                return
            started = await live_connection(scope)
            # A want replaced while waiting must not open anything: its replacement may already hold an
            # overlapping root.
            if started is None or scope.cancelled:
                return
            want.settled_target = None
            want.connection = started
            outcome = await _attempt(want, started, next_connection)
            # A rotation may have moved the attempt to another connection: what follows waits on the one it ended on.
            connection = want.connection or started
            asked = want.settled_target or (want.options.track, want.options.parameters)
            if scope.cancelled or outcome.kind == "cancelled":
                return
            # A re-want that came after the attempt settled asks for another track or parameters: the old
            # target's failure is not the new one's, so try the new one at once. A subscription the failed
            # move cancelled was a restart.
            superseded = (want.options.track, want.options.parameters) != asked
            if outcome.kind == "fatal" and superseded:
                outcome = _Outcome("retry", accepted=outcome.accepted, reason=outcome.reason or "terminated")
            if outcome.kind == "fatal":
                forget()
                assert outcome.error is not None
                want.call("on_error", outcome.error)
                return
            reason: RestartReason | None = outcome.reason
            if outcome.kind == "connection":
                reason = "connection"
            elif outcome.kind == "moved":
                reason = "generation"
            if outcome.accepted and reason is not None:
                try:
                    want.call("on_restarted", reason, want.namespace)
                except Exception:
                    # A delivery callback's failure is the application's; the want keeps its track subscribed.
                    _log.exception("on_restarted raised; the want resubscribes regardless")
            if outcome.kind == "connection":
                # The request can see the end before the connection reports it; wait for that rather than spin on it.
                await wait_any([connection.ended, scope], limit=1.0)
                continue
            # on_restarted is application code: it may have wanted another target.
            if outcome.kind != "retry" or (want.options.track, want.options.parameters) != asked:
                continue
            # Cached objects followed by a done are not progress: only a later group resets the backoff.
            if outcome.progressed:
                failures = 0
            delay = backoff_delay(failures, want.retry)
            failures += 1
            await _backoff(want, connection, delay)
    finally:
        want.changes.remove(rewanted)


async def _backoff(want: _Want, connection: _Connection, delay: float) -> None:
    """Wait out a retry delay, cut short when a fresh attempt can do better at once: the connection
    ended, or (following generations) a generation other than the last one tried became the newest."""
    wake = CancellationToken()
    held: tuple[_RootWatch, Callable[[], None]] | None = None
    if want.options.generations and not connection.ended.cancelled:
        with contextlib.suppress(Exception):
            # The attempt itself reports a discovery it cannot open.
            held = _root_watch(connection, want.options.namespace)
    # Released in the turn the want is cancelled, as in an attempt, so a replacement can claim an overlapping root.
    remove_release = on_cancel([want.controller], held[1]) if held is not None else None

    def changed() -> None:
        if held is None:
            return
        watch = held[0]
        failure = watch.failure()
        if failure is not None:
            # Only a failure no wait fixes is kept, for the loop to surface; any other waits out the delay.
            outcome = _discovery_failed(failure[0], CancellationToken(), lambda _accepted: _CANCELLED, accepted=False)
            if outcome.kind == "fatal":
                want.deferred = failure[0]
                wake.cancel()
            return
        # A fresh discovery starts empty; no generation yet is not a newer one.
        newest = watch.current()
        if newest is not None and newest != want.generation:
            wake.cancel()

    unwatch = held[0].on_change(changed) if held is not None else None
    # A re-want asks for something the failed attempt did not: try it at once.
    want.changes.append(wake.cancel)
    try:
        changed()
        # A rotation replacing the connection wakes it too: the want's next attempt belongs on the new one.
        await wait_any([want.controller, connection.ended, connection.retiring, wake], limit=delay)
    finally:
        want.changes.remove(wake.cancel)
        if unwatch is not None:
            unwatch()
        if remove_release is not None:
            remove_release()
        if held is not None:
            held[1]()


async def _attempt(want: _Want, connection: _Connection, next_connection: Callable[[], _Connection | None]) -> _Outcome:
    """One attempt for ``want`` on ``connection``: its discovery, if it follows generations, and its subscriptions."""
    options = want.options
    scope = [want.controller, connection.ended]

    def cancelled_or_connection(accepted: bool) -> _Outcome:
        return _CANCELLED if want.controller.cancelled else _Outcome("connection", accepted=accepted)

    namespace = options.namespace
    watch: _RootWatch | None = None
    release: Callable[[], None] = _nothing
    remove_release: Callable[[], None] = _nothing
    if options.generations:
        try:
            watch, release = _root_watch(connection, options.namespace)
        except Exception as error:
            # As for a subscribe: a connection ending under the call refuses the request with its own error.
            if any(token.cancelled for token in scope) or not _refused_for_good(error):
                return cancelled_or_connection(False)
            return _Outcome("fatal", error=error)
        # Released in the turn the want is cancelled, so a replacement want can claim an overlapping root at once.
        remove_release = on_cancel(scope, release)
    try:
        generation: str | None = None
        if watch is not None:
            found = await watch.newest([*scope, connection.retiring])
            if isinstance(found, _Failed):
                return _discovery_failed(found.error, _any_cancelled(scope), cancelled_or_connection, accepted=False)
            # Rotated away before a generation was found: start over on the new connection at once.
            if found is None and connection.retiring.cancelled and not any(token.cancelled for token in scope):
                return _Outcome("moved")
            if found is None:
                return cancelled_or_connection(False)
            generation = found
            namespace = (*options.namespace, generation)
        want.namespace = namespace
        want.generation = generation
        # The want's own cancellation, not the connection's: a rotation moves the attempt to the next connection.
        return await _follow(
            want,
            connection,
            namespace,
            generation,
            [want.controller],
            watch,
            cancelled_or_connection,
            next_connection,
        )
    finally:
        remove_release()
        release()


def _nothing() -> None:
    pass


def _any_cancelled(tokens: Sequence[CancellationToken]) -> CancellationToken:
    """A token already cancelled when any of ``tokens`` is, for the classifiers' one check."""
    token = CancellationToken()
    if any(item.cancelled for item in tokens):
        token.cancel()
    return token


@dataclass(slots=True, eq=False)
class _Slot:
    """One subscription an attempt holds: the active one, a move's pending replacement, or an old one draining."""

    namespace: tuple[str, ...]
    """The namespace it was made on: the generation's when following one."""
    generation: str | None
    track: str
    parameters: MessageParameters | None
    connection: _Connection
    """The connection it was made on: a rotation's replacement is on the next one."""
    subscription: RelaySubscription | None = None
    accepted: SubscribeOk | None = None
    first_group: int | None = None
    largest_group: int | None = None
    latest: tuple[int, int] | None = None
    """The latest object it delivered, by location: a rotation's replacement skips what is at or before it."""
    progressed: bool = False
    """Whether it delivered an object from a later group than its first."""
    drain_through: int | None = None
    """While draining after a cutover: the largest group it had delivered, through which its events
    still reach the handlers."""
    ended: RestartReason | None = None
    """Why the active subscription ended while a move was pending; the move carries on without it."""
    held: list[_HeldEvent] = field(default_factory=list)
    """While pending: its events held until the cutover, as a switchable holds them."""

    def made_with(self, namespace: tuple[str, ...], track: str, parameters: MessageParameters | None) -> bool:
        return (self.namespace, self.track, self.parameters) == (namespace, track, parameters)

    def same_target(self, other: _Slot) -> bool:
        return self.made_with(other.namespace, other.track, other.parameters)

    def cancel(self) -> None:
        if self.subscription is not None:
            self.subscription.cancel()

    def delivered(self, obj: MoqtObject) -> None:
        """Record ``obj`` as the latest this slot delivered, if it is later than what it had."""
        if not _at_or_before(obj, self.latest):
            self.latest = (obj.group_id, obj.object_id)


def _at_or_before(obj: MoqtObject, latest: tuple[int, int] | None) -> bool:
    """Whether ``obj`` is at or before ``latest``, by group and then object ID."""
    return latest is not None and (obj.group_id, obj.object_id) <= latest


@dataclass(slots=True, eq=False)
class _RotationTo:
    """A rotation under way: the connection the subscriptions move to, and its discovery of the root."""

    connection: _Connection
    watch: _RootWatch | None
    release: Callable[[], None]


async def _follow(
    want: _Want,
    connection: _Connection,
    namespace: tuple[str, ...],
    generation: str | None,
    scope: list[CancellationToken],
    initial_watch: _RootWatch | None,
    cancelled_or_connection: Callable[[bool], _Outcome],
    next_connection: Callable[[], _Connection | None],
) -> _Outcome:
    """The want's subscriptions from the first SUBSCRIBE on ``connection`` to whatever ends the attempt.

    A move (a newer generation, or a re-want with another track or parameters) is made before the
    break: the replacement opens pending while the active subscription keeps delivering, and takes
    over once one of its objects passes ``until``. A rotation is a move to the next connection: with
    the target unchanged, the replacement takes over at the first object the active one had not
    delivered, and no callback marks it.
    """
    loop = asyncio.get_running_loop()
    # The followed root's discovery on the active subscription's connection.
    watch = initial_watch
    settled = CancellationToken()
    outcome: _Outcome | None = None
    pending: _Slot | None = None
    # Old subscriptions draining after a cutover, each with the timer that cancels it.
    draining: dict[_Slot, asyncio.TimerHandle] = {}
    # A move whose replacement failed, retried after the backoff unless the want asks for something else first.
    retry_move: tuple[_Slot, asyncio.TimerHandle] | None = None
    move_failures = 0
    # The target of the latest move, whose failures ``move_failures`` counts.
    move_target: tuple[tuple[str, ...], str, MessageParameters | None] | None = None
    last_object_at = 0.0
    # A rotation under way: the connection the subscriptions move to, and its discovery of the root.
    rotation_to: _RotationTo | None = None
    # The discovery a rotation swapped in, released when the attempt ends; the first is the attempt's to release.
    release_watch: Callable[[], None] = _nothing
    unwatch: Callable[[], None] | None = None
    unwatch_next: Callable[[], None] | None = None
    # The connections a subscription of this attempt is on, each held so a rotation closes it only once none are.
    holds: dict[_Connection, Callable[[], None]] = {}
    # Stop observing each connection the attempt observed.
    observed: list[Callable[[], None]] = []
    # Set once any subscription of the attempt heard PUBLISH_DONE: a restarted publisher numbers its groups from 0,
    # so a rotation's replacement no longer skips what the active subscription delivered.
    publisher_ended = False

    def observe(on: _Connection) -> None:
        if any(item is on for item in watched):
            return
        watched.append(on)

        def on_retiring(_: object = None) -> None:
            task = asyncio.ensure_future(rotate(on))
            _observe(task)

        if on.retiring.cancelled:
            loop.call_soon(on_retiring)
            return
        on.retiring.add_callback(on_retiring)
        observed.append(lambda: on.retiring.remove_callback(on_retiring))

    watched: list[_Connection] = []

    def hold_on(on: _Connection) -> None:
        if on in holds:
            return
        holds[on] = on.hold()
        observe(on)

    def release_unused() -> None:
        for on, release in list(holds.items()):
            used = (
                active.connection is on
                or (pending is not None and pending.connection is on)
                or (rotation_to is not None and rotation_to.connection is on)
                or any(slot.connection is on for slot in draining)
            )
            if used:
                continue
            del holds[on]
            release()

    def rotating(slot: _Slot) -> bool:
        """Whether ``slot`` replaces the active subscription on the next connection, with the same target."""
        return slot.connection is not active.connection and slot.same_target(active)

    def settle(next_outcome: _Outcome) -> None:
        nonlocal outcome
        if outcome is None:
            # A failure no wait fixes still ends an accepted subscription, which a superseding re-want
            # reports as a restart.
            if next_outcome.kind == "fatal":
                next_outcome = replace(next_outcome, accepted=accepted(), reason=active.ended)
            outcome = next_outcome
            want.settled_target = (want.options.track, want.options.parameters)
            settled.cancel()

    def over() -> bool:
        return outcome is not None or any(token.cancelled for token in scope)

    def accepted() -> bool:
        return active.accepted is not None

    def clear_retry() -> None:
        nonlocal retry_move
        if retry_move is not None:
            retry_move[1].cancel()
            retry_move = None

    def stop_drain(slot: _Slot) -> None:
        timer = draining.pop(slot, None)
        if timer is not None:
            timer.cancel()
        slot.cancel()
        release_unused()

    def reaches(slot: _Slot, group_id: int) -> bool:
        """Whether a subgroup event of ``slot`` reaches the handlers."""
        if over():
            return False
        if slot is active:
            return slot.ended is None
        return slot in draining and slot.drain_through is not None and group_id <= slot.drain_through

    def active_ended(reason: RestartReason) -> None:
        """The active subscription ended without the connection: with a move pending, the move carries on."""
        if pending is not None:
            active.ended = reason
            active.cancel()
            return
        settle(_Outcome("retry", accepted=accepted(), progressed=active.progressed, reason=reason))

    def pending_failed(slot: _Slot, end: SubscriptionEnd | None) -> None:
        """A move's replacement ended before its cutover."""
        nonlocal pending, retry_move, move_failures
        pending = None
        slot.cancel()
        # The connection's end reaches the active subscription and the attempt's scope as well.
        if end is not None and end.cause == "connection-closed":
            settle(cancelled_or_connection(accepted()))
            return
        failed = _classify(end, False, False, cancelled_or_connection) if end is not None else None
        if failed is not None and failed.kind == "fatal":
            settle(failed)
            return
        if active.ended is not None:
            settle(_Outcome("retry", accepted=True, progressed=active.progressed, reason=active.ended))
            return
        # The active subscription keeps delivering; the move is retried after the backoff.

        def retry_now() -> None:
            nonlocal retry_move
            if retry_move is not None and retry_move[0] is slot:
                retry_move = None
                reconcile()

        delay = backoff_delay(move_failures, want.retry)
        move_failures += 1
        retry_move = (slot, loop.call_later(delay, retry_now))

    def cutover(slot: _Slot, obj: MoqtObject) -> None:
        nonlocal active, pending, move_failures, last_object_at, watch, release_watch, rotation_to, unwatch
        nonlocal unwatch_next
        previous = active
        # A rotation that keeps the target continues the same stream: nothing to mark, and no drain to repeat objects.
        silent = rotating(slot)
        pending = None
        clear_retry()
        move_failures = 0
        active = slot
        want.namespace = slot.namespace
        want.generation = slot.generation
        want.connection = slot.connection
        if rotation_to is not None and rotation_to.connection is slot.connection:
            # The followed root's discovery moves to the new connection with the subscription.
            if unwatch is not None:
                unwatch()
            if unwatch_next is not None:
                unwatch_next()
                unwatch_next = None
            watch = rotation_to.watch
            release_watch = rotation_to.release
            rotation_to = None
            unwatch = watch.on_change(reconcile) if watch is not None else None
        if not silent and previous.ended is None and want.drain > 0 and previous.largest_group is not None:
            previous.drain_through = previous.largest_group
            draining[previous] = loop.call_later(want.drain, stop_drain, previous)
        else:
            previous.cancel()
        release_unused()
        slot.first_group = obj.group_id
        slot.largest_group = obj.group_id
        last_object_at = loop.time()
        if not silent:
            # A subscription that delivered an object was accepted.
            assert slot.accepted is not None
            info = WantSwitchedInfo(previous.namespace, previous.track, slot.namespace, slot.track, slot.accepted)
            try:
                want.call("on_switched", info)
            except Exception:
                # A delivery callback's failure is the application's; the want keeps delivering.
                _log.exception("on_switched raised; the want delivers regardless")
        # Later groups that reached the replacement early, out of order, follow the object that cut over.
        # Each delivery is isolated, so one raising callback loses neither the rest nor the held events.
        replay = _held_after(slot.held, obj)
        slot.held = []

        def deliver(name: str, value: object) -> None:
            try:
                want.call(name, value)
            except Exception:
                _log.exception("%s raised at a cutover; the want delivers regardless", name)

        # A handler may have moved the want again or cancelled it; the object still belongs to this cutover.
        slot.delivered(obj)
        if not over() and active is slot:
            deliver("on_object", obj)
        for held in replay:
            if over() or active is not slot:
                break
            if isinstance(held, ObjectEvent):
                slot.delivered(held.value)
                if held.value.group_id > obj.group_id:
                    slot.progressed = True
                slot.largest_group = max(slot.largest_group or held.value.group_id, held.value.group_id)
                deliver("on_object", held.value)
            elif isinstance(held, SubgroupEndEvent):
                deliver("on_subgroup_end", held.value)
            else:
                deliver("on_subgroup_reset", held.value)

    def on_object(slot: _Slot, obj: MoqtObject) -> None:
        nonlocal last_object_at
        if over():
            return
        if slot is active:
            if slot.ended is not None:
                return
            last_object_at = loop.time()
            if slot.first_group is None:
                slot.first_group = obj.group_id
            elif obj.group_id > slot.first_group:
                slot.progressed = True
            if slot.largest_group is None or obj.group_id > slot.largest_group:
                slot.largest_group = obj.group_id
            slot.delivered(obj)
            want.call("on_object", obj)
            return
        if slot in draining:
            if slot.drain_through is not None and obj.group_id <= slot.drain_through:
                want.call("on_object", obj)
            return
        if slot is not pending:
            return
        last_object_at = loop.time()
        # The active subscription's ending reaches on_ended a turn later; one already recorded counts now.
        if active.ended is None and active.subscription is not None and active.subscription.ended.done():
            on_ended(active, active.subscription.ended.result())
            if over() or pending is not slot:
                return
        # A rotation's replacement starts at the latest group, which the active subscription may have delivered:
        # skip it. Not once the publisher ended or the subscription was ended by it: a restarted publisher
        # numbers from 0 again.
        rotation = rotating(slot)
        continues = not publisher_ended and active.ended in (None, "connection")
        if rotation and continues and _at_or_before(obj, active.latest):
            return
        # With the active subscription gone there is nothing left to compare with: the next object cuts over. A
        # rotation's replacement continues the same stream, so its first new object does too.
        passed = True
        if active.ended is None and not rotation:
            try:
                passed = want.until(obj)
            except Exception:
                _log.exception("until raised; the want cuts over on this object")
        # The predicate is application code: it may have moved the want again or cancelled it.
        if not passed:
            _hold(slot.held, ObjectEvent(obj))
        elif not over() and pending is slot:
            cutover(slot, obj)

    def on_subscribed(slot: _Slot, info: SubscribeOk) -> None:
        nonlocal last_object_at
        if over():
            return
        slot.accepted = info
        if slot is not active:
            return
        last_object_at = loop.time()
        want.call("on_subscribed", info, slot.namespace)

    def on_subgroup_event(slot: _Slot, event: SubgroupEndEvent | SubgroupResetEvent) -> None:
        if slot is pending and not over():
            _hold(slot.held, event)
        elif reaches(slot, event.value.group_id):
            want.call("on_subgroup_end" if isinstance(event, SubgroupEndEvent) else "on_subgroup_reset", event.value)

    def on_publisher_done(slot: _Slot, info: PublishDone) -> None:
        nonlocal publisher_ended
        if over():
            return
        publisher_ended = True
        if slot is pending:
            pending_failed(slot, None)
        elif slot is active and slot.ended is None:
            active_ended("publisher-done")
            want.call("on_publisher_done", info)

    def on_ended(slot: _Slot, end: SubscriptionEnd) -> None:
        if over():
            return
        if slot in draining:
            stop_drain(slot)
        elif slot is pending:
            pending_failed(slot, end)
        elif slot is active and slot.ended is None:
            # The retiring connection closed under the active subscription: the rotation's replacement carries
            # on, or is tried again on the next connection after its backoff.
            if end.cause == "connection-closed":
                if pending is not None and pending.connection is not slot.connection:
                    active_ended("connection")
                    return
                if pending is None and retry_move is not None and retry_move[0].connection is not slot.connection:
                    slot.ended = "connection"
                    slot.cancel()
                    return
            ended = _classify(end, accepted(), slot.progressed, cancelled_or_connection)
            if ended.kind == "retry" and ended.reason is not None:
                active_ended(ended.reason)
            else:
                settle(ended)

    def open_slot(target: tuple[str, ...], target_generation: str | None, on: _Connection) -> _Slot:
        options = want.options
        slot = _Slot(target, target_generation, options.track, options.parameters, on)
        subscription = on.subscriber.subscribe(
            target,
            options.track,
            RelaySubscriptionHandlers(
                on_subscribed=lambda info: on_subscribed(slot, info),
                on_object=lambda obj: on_object(slot, obj),
                on_subgroup_end=lambda info: on_subgroup_event(slot, SubgroupEndEvent(info)),
                on_subgroup_reset=lambda info: on_subgroup_event(slot, SubgroupResetEvent(info)),
                on_publisher_done=lambda info: on_publisher_done(slot, info),
            ),
            parameters=deepcopy(options.parameters),
        )
        slot.subscription = subscription
        hold_on(on)
        subscription.ended.add_done_callback(lambda _: on_ended(slot, subscription.ended.result()))
        return slot

    def reconcile() -> None:
        """Bring the subscriptions in line with what the want asks for now: its options and the newest generation."""
        nonlocal pending, move_failures, move_target
        if over():
            return
        # During a rotation the subscription belongs on the next connection, following that connection's discovery.
        on = rotation_to.connection if rotation_to is not None else active.connection
        discovery = rotation_to.watch if rotation_to is not None else watch
        target = want.options.namespace
        target_generation: str | None = None
        if discovery is not None:
            failure = discovery.failure()
            if failure is not None:
                settle(
                    _discovery_failed(failure[0], _any_cancelled(scope), cancelled_or_connection, accepted=accepted())
                )
                return
            target_generation = discovery.current()
            # None announced: nothing to move to, so the want waits for one, as after a withdrawal.
            if target_generation is None:
                settle(_Outcome("moved", accepted=accepted()))
                return
            target = (*target, target_generation)
        track, parameters = want.options.track, want.options.parameters
        if active.made_with(target, track, parameters) and on is active.connection:
            # Asked for what is active again: a pending move is no longer wanted.
            clear_retry()
            move_target = None
            if pending is not None:
                pending.cancel()
                pending = None
            if active.ended is not None:
                settle(_Outcome("retry", accepted=True, progressed=active.progressed, reason=active.ended))
            return
        if pending is not None and pending.made_with(target, track, parameters) and pending.connection is on:
            return
        if (
            retry_move is not None
            and retry_move[0].made_with(target, track, parameters)
            and retry_move[0].connection is on
        ):
            return
        clear_retry()
        # An unanswered subscription delivers nothing to overlap: replace it at once.
        if not accepted():
            settle(_Outcome("moved"))
            return
        if pending is not None:
            pending.cancel()
            pending = None
        # A new target starts its own backoff; a retry of the same one keeps counting.
        if move_target != (target, track, parameters):
            move_failures = 0
        move_target = (target, track, parameters)
        try:
            pending = open_slot(target, target_generation, on)
        except Exception as error:
            if any(token.cancelled for token in scope) or not _refused_for_good(error):
                settle(cancelled_or_connection(True))
            else:
                settle(_Outcome("fatal", error=error))

    try:
        active = open_slot(namespace, generation, connection)
    except Exception as error:
        want.settled_target = (want.options.track, want.options.parameters)
        # An argument the relay can never take is the want's fault; anything else is the connection ending
        # under the call.
        if any(token.cancelled for token in scope) or not _refused_for_good(error):
            return cancelled_or_connection(False)
        return _Outcome("fatal", error=error)

    async def rotate(from_: _Connection) -> None:
        """A rotation replaced the active subscription's connection: move the subscription to the next one."""
        nonlocal rotation_to, unwatch_next

        def current_from() -> bool:
            # The active subscription's connection, or the one a rotation is already moving to, if that one
            # retires in turn.
            return rotation_to.connection is from_ if rotation_to is not None else active.connection is from_

        if over() or not current_from():
            return
        nxt = next_connection()
        if nxt is None or nxt is from_ or nxt.ended.cancelled:
            return
        # Nothing accepted yet to overlap: start over on the new connection at once.
        if not accepted():
            settle(_Outcome("moved"))
            return
        hold_on(nxt)
        next_watch: _RootWatch | None = None
        release: Callable[[], None] = _nothing
        if watch is not None:
            try:
                next_watch, release = _root_watch(nxt, want.options.namespace)
            except Exception:
                # Discovery the new connection refuses cannot follow there: restart on it as after a connection's end.
                release_unused()
                settle(cancelled_or_connection(accepted()))
                return
            found = await next_watch.newest([*scope, settled, nxt.ended])
            if not isinstance(found, str):
                release()
                if not over():
                    settle(cancelled_or_connection(accepted()))
                release_unused()
                return
        if over() or not current_from():
            release()
            release_unused()
            return
        # A rotation to a connection that retired in turn gives way to this one.
        if unwatch_next is not None:
            unwatch_next()
        if rotation_to is not None:
            rotation_to.release()
        rotation_to = _RotationTo(nxt, next_watch, release)
        unwatch_next = next_watch.on_change(reconcile) if next_watch is not None else None
        reconcile()
        release_unused()

    stale_task: asyncio.Future[None] | None = None

    def watch_stale() -> None:
        nonlocal stale_task
        if stale_task is not None:
            stale_task.cancel()
            stale_task = None
        stale_after = want.options.stale_after
        if stale_after is None:
            return
        interval = max(0.01, min(1.0, stale_after / 4))

        async def check() -> None:
            nonlocal last_object_at
            while True:
                await asyncio.sleep(interval)
                if over() or not accepted() or loop.time() - last_object_at < stale_after:
                    continue
                # During a move only the old subscription went stale: the replacement gets a window of its own.
                if pending is not None and active.ended is None:
                    active_ended("stale")
                    last_object_at = loop.time()
                else:
                    settle(_Outcome("retry", accepted=True, progressed=active.progressed, reason="stale"))
                    return

        stale_task = asyncio.ensure_future(check())

    def changed() -> None:
        watch_stale()
        reconcile()

    watch_stale()
    want.changes.append(changed)
    unwatch = watch.on_change(reconcile) if watch is not None else None
    observe(connection)
    # A change that landed between choosing the subscription and attaching the listeners is not lost.
    reconcile()
    try:
        await wait_any([*scope, settled])
        return outcome if outcome is not None else cancelled_or_connection(accepted())
    finally:
        # Settles the attempt, so nothing reaches the handlers from here on.
        settle(_CANCELLED)
        if stale_task is not None:
            stale_task.cancel()
        clear_retry()
        if unwatch is not None:
            unwatch()
        if unwatch_next is not None:
            unwatch_next()
        for stop_observing in observed:
            stop_observing()
        want.changes.remove(changed)
        for slot in list(draining):
            stop_drain(slot)
        if pending is not None:
            pending.cancel()
        active.cancel()
        if rotation_to is not None:
            rotation_to.release()
            rotation_to = None
        release_watch()
        for release_hold in holds.values():
            release_hold()
        holds.clear()


def _discovery_failed(
    error: BaseException | None,
    scope: CancellationToken,
    cancelled_or_connection: Callable[[bool], _Outcome],
    *,
    accepted: bool,
) -> _Outcome:
    """What a root's discovery ending means for the want: the connection's end moves it on, the relay
    ending the request or a REQUEST_ERROR a wait may fix is retried with backoff on a fresh request,
    and anything else is surfaced."""
    if scope.cancelled or isinstance(error, SubscriptionConnectionClosedError):
        return cancelled_or_connection(accepted)
    retry = _Outcome("retry", accepted=accepted, reason="generation")
    if error is None:
        return retry
    if isinstance(error, SubscriptionRequestError) and not is_permanent_request_error(error.request_error):
        return retry
    return _Outcome("fatal", error=error)


def _classify(
    end: SubscriptionEnd, accepted: bool, progressed: bool, cancelled_or_connection: Callable[[bool], _Outcome]
) -> _Outcome:
    """What a subscription's own ending means for the want."""
    if end.cause in ("connection-closed", "cancelled"):
        return cancelled_or_connection(accepted)
    request_error = end.error
    if not end.accepted:
        if isinstance(request_error, RequestError):
            if is_fresh_namespace_rejection(request_error):
                return _Outcome("retry")
            if is_permanent_request_error(request_error):
                return _Outcome("fatal", error=SubscriptionRequestError(request_error))
        return _Outcome("retry")
    return _Outcome("retry", accepted=accepted, progressed=progressed, reason="terminated")


@dataclass(frozen=True, slots=True)
class _Failed:
    error: BaseException | None


class _RootWatch:
    """One root's discovery on a connection: the generations announced one field below it."""

    __slots__ = ("_events", "_failure", "_generations", "_listeners", "_root", "_task", "users")

    def __init__(self, subscriber: RelaySubscriber, root: tuple[str, ...], on_end: Callable[[], None]) -> None:
        self._root = root
        self._generations: set[str] = set()
        self._listeners: list[Callable[[], None]] = []
        self._failure: tuple[BaseException | None] | None = None
        self.users = 0
        # Raises PREFIX_OVERLAP at once when another followed root overlaps this one.
        self._events: NamespaceEvents = subscriber.namespace_events(root, capacity=_DISCOVERY_CAPACITY)
        self._task = asyncio.ensure_future(self._read(on_end))
        _observe(self._task)

    async def _read(self, on_end: Callable[[], None]) -> None:
        depth = len(self._root)
        try:
            async for event in self._events:
                # Only one field below the root is a generation; anything deeper belongs to someone else.
                if len(event.namespace) != depth + 1:
                    continue
                generation = event.namespace[depth]
                if event.type == "announced":
                    self._generations.add(generation)
                else:
                    self._generations.discard(generation)
                self._notify()
            # The relay ended the request, or the watch was closed: neither is a failure of the want.
            self._failure = (None,)
        except asyncio.CancelledError:
            self._failure = (None,)
            raise
        except Exception as error:
            self._failure = (error,)
        finally:
            on_end()
            self._notify()

    def _notify(self) -> None:
        for listener in list(self._listeners):
            listener()

    def current(self) -> str | None:
        """The newest generation announced now, or ``None``."""
        return max(self._generations) if self._generations else None

    def failure(self) -> tuple[BaseException | None] | None:
        """How discovery ended, once it has: the error, ``None`` inside when the relay ended it cleanly."""
        return self._failure

    def on_change(self, listener: Callable[[], None]) -> Callable[[], None]:
        """Observe every change, discovery's end included; returns the removal."""
        self._listeners.append(listener)

        def remove() -> None:
            if listener in self._listeners:
                self._listeners.remove(listener)

        return remove

    async def newest(self, scope: Sequence[CancellationToken]) -> str | _Failed | None:
        """Wait for a generation to be announced; ``None`` once ``scope`` is cancelled."""
        while True:
            if self._failure is not None:
                return _Failed(self._failure[0])
            found = self.current()
            if found is not None:
                return found
            if any(token.cancelled for token in scope):
                return None
            changed = CancellationToken()
            remove = self.on_change(changed.cancel)
            try:
                await wait_any([*scope, changed])
            finally:
                remove()

    def close(self) -> None:
        self._events.close()


def _overlaps(a: tuple[str, ...], b: tuple[str, ...]) -> bool:
    """Whether two namespace prefixes overlap field by field: equal, or one contains the other."""
    shorter, longer = (a, b) if len(a) <= len(b) else (b, a)
    return longer[: len(shorter)] == shorter


def _root_watch(connection: _Connection, root: tuple[str, ...]) -> tuple[_RootWatch, Callable[[], None]]:
    """The connection's watch for ``root``, opened on first use and closed when its last user releases it."""
    entry = connection.roots.get(root)
    if entry is None:
        # A watch kept for reuse but unused would hold its prefix and refuse an overlapping root: close it first.
        for held_root, held in list(connection.roots.items()):
            if held.users == 0 and _overlaps(held_root, root):
                del connection.roots[held_root]
                held.close()
        created: list[_RootWatch] = []

        # An ended discovery leaves the map at once, so the next attempt asks the relay again.
        def forget_ended() -> None:
            if created and connection.roots.get(root) is created[0]:
                del connection.roots[root]

        entry = _RootWatch(connection.subscriber, root, forget_ended)
        created.append(entry)
        connection.roots[root] = entry
    watch = entry
    watch.users += 1
    released = False

    def release() -> None:
        nonlocal released
        if released:
            return
        released = True
        watch.users -= 1
        if watch.users == 0:

            def linger_over() -> None:
                if watch.users == 0 and connection.roots.get(root) is watch:
                    del connection.roots[root]
                    watch.close()

            asyncio.get_running_loop().call_later(_ROOT_LINGER, linger_over)

    return watch, release


__all__ = ["RestartReason", "SupervisedSubscriber", "WantHandlers", "WantSwitchedInfo", "connect_supervised_subscriber"]
