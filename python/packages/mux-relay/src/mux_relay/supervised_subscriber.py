"""The supervised subscriber: one subscriber connection at a time, replaced with backoff whenever it
ends, and one supervisor per wanted track that keeps a subscription to it open across connections,
publisher restarts and generations.

Each want retries the announce race, backs off after a publisher's done or a rejection, resubscribes
a subscription that went stale, and follows a supervised publisher's newest generation through
namespace discovery. Objects are never fetched to fill a gap: a replacement subscription starts
live, and ``on_restarted`` tells the application to drop state that assumed continuity. A move to a
newer generation, or to another track or parameters on a re-want, is made before the break instead,
as ``switch_to`` makes one (issue #282): the replacement opens while the current subscription keeps
delivering, and ``on_switched`` marks the cutover. See ``spec/behavior.md``, "Supervised
connections". The counterpart of ``ts/src/supervised-subscriber.ts``.
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
    SupervisedStatus,
    SupervisionBackoff,
    _Backoff,
    backoff_delay,
    backoff_options,
    is_fresh_namespace_rejection,
    is_permanent_request_error,
    is_setup_failure,
    on_cancel,
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
    roots: dict[tuple[str, ...], _RootWatch] = field(default_factory=dict)
    """One discovery per followed root, shared by every want on it."""


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
        task = asyncio.ensure_future(_run_want(want, self._live_connection, lambda: self._forget(key, want)))
        _observe(task)

    def unwant(self, key: str) -> None:
        """Cancel a want's subscription and forget it. Unknown keys are ignored."""
        want = self._state.wants.get(key)
        if want is not None:
            self._forget(key, want)

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
    on_status: Callable[[SupervisedStatus, BaseException | None], None] | None = None,
) -> SupervisedSubscriber:
    """Open a subscriber that reconnects and keeps every wanted track subscribed across connections,
    publisher restarts and generations.

    Returns once the first connection is up. A first attempt that fails on its arguments, or because
    the relay refused the credentials (``UNAUTHORIZED``), raises that failure; any other failure is
    retried with backoff (``reconnect``, from 0.5 s doubling up to 10 s), bounded only by
    ``cancellation``, whose reason is raised. ``on_status`` runs on every status change, with what
    ended the previous connection or failed the last attempt. See ``spec/behavior.md``, "Supervised
    connections"; :func:`connect_relay_subscriber` never reconnects or resubscribes.
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
        connect, cancellation=cancellation, reconnect=reconnect, on_status=on_status
    )


ConnectSubscriber = Callable[[CancellationToken], Awaitable[RelaySubscriber]]
"""How a supervisor opens one subscriber connection, given the attempt's cancellation."""


async def _supervise_relay_subscriber(
    connect: ConnectSubscriber,
    *,
    cancellation: CancellationToken | None = None,
    reconnect: SupervisionBackoff | None = None,
    on_status: Callable[[SupervisedStatus, BaseException | None], None] | None = None,
) -> SupervisedSubscriber:
    """Open a supervised subscriber with ``connect``; the tests drive it with scripted connections."""
    backoff = backoff_options("reconnect", reconnect, RECONNECT_BACKOFF)
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

    async def run() -> None:
        nonlocal attempt_in_progress, first
        attempt = 0
        while not state.stopped:
            attempt_in_progress = CancellationToken()
            try:
                subscriber = await connect(attempt_in_progress)
            except Exception as error:
                attempt_in_progress = None
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
            attempt_in_progress = None
            if state.stopped:
                subscriber.close()
                return
            live = _Connection(subscriber, CancellationToken())
            state.current = live
            up_at = loop.time()
            set_status("live")
            if first is not None:
                first.set_result(None)
                first = None
            state.wake_waiters()

            ended = await subscriber.closed
            live.ended.cancel()
            for watch in list(live.roots.values()):
                watch.close()
            if state.stopped:
                return
            state.current = None
            set_status("reconnecting", ended.error)
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
    forget: Callable[[], None],
) -> None:
    """Supervise ``want``; a failure the loop did not expect forgets it rather than leave it registered but dead."""
    try:
        await _supervise_want(want, live_connection, forget)
    except Exception:
        forget()
        _log.exception("a want stopped on an unexpected failure and was forgotten")


async def _supervise_want(
    want: _Want,
    live_connection: Callable[[CancellationToken], Awaitable[_Connection | None]],
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
            connection = await live_connection(scope)
            # A want replaced while waiting must not open anything: its replacement may already hold an
            # overlapping root.
            if connection is None or scope.cancelled:
                return
            want.settled_target = None
            outcome = await _attempt(want, connection)
            asked = want.settled_target or (want.options.track, want.options.parameters)
            if scope.cancelled or outcome.kind == "cancelled":
                return
            # A re-want that came after the attempt settled asks for another track or parameters: the old
            # target's failure is not the new one's, so try the new one at once. A subscription the failed
            # move cancelled was a restart.
            superseded = (want.options.track, want.options.parameters) != asked
            if outcome.kind == "fatal" and superseded:
                outcome = _Outcome("retry", accepted=outcome.accepted, reason="terminated")
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
        await wait_any([want.controller, connection.ended, wake], limit=delay)
    finally:
        want.changes.remove(wake.cancel)
        if unwatch is not None:
            unwatch()
        if remove_release is not None:
            remove_release()
        if held is not None:
            held[1]()


async def _attempt(want: _Want, connection: _Connection) -> _Outcome:
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
            found = await watch.newest(scope)
            if isinstance(found, _Failed):
                return _discovery_failed(found.error, _any_cancelled(scope), cancelled_or_connection, accepted=False)
            if found is None:
                return cancelled_or_connection(False)
            generation = found
            namespace = (*options.namespace, generation)
        want.namespace = namespace
        want.generation = generation
        return await _follow(want, connection, namespace, generation, scope, watch, cancelled_or_connection)
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
    subscription: RelaySubscription | None = None
    accepted: SubscribeOk | None = None
    first_group: int | None = None
    largest_group: int | None = None
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

    def cancel(self) -> None:
        if self.subscription is not None:
            self.subscription.cancel()


async def _follow(
    want: _Want,
    connection: _Connection,
    namespace: tuple[str, ...],
    generation: str | None,
    scope: list[CancellationToken],
    watch: _RootWatch | None,
    cancelled_or_connection: Callable[[bool], _Outcome],
) -> _Outcome:
    """The want's subscriptions on ``connection`` from the first SUBSCRIBE to whatever ends the attempt.

    A move (a newer generation, or a re-want with another track or parameters) is made before the
    break: the replacement opens pending while the active subscription keeps delivering, and takes
    over once one of its objects passes ``until``.
    """
    loop = asyncio.get_running_loop()
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

    def settle(next_outcome: _Outcome) -> None:
        nonlocal outcome
        if outcome is None:
            # A failure no wait fixes still ends an accepted subscription, which a superseding re-want
            # reports as a restart.
            outcome = replace(next_outcome, accepted=accepted()) if next_outcome.kind == "fatal" else next_outcome
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
        nonlocal active, pending, move_failures, last_object_at
        previous = active
        pending = None
        clear_retry()
        move_failures = 0
        active = slot
        want.namespace = slot.namespace
        want.generation = slot.generation
        if previous.ended is None and want.drain > 0 and previous.largest_group is not None:
            previous.drain_through = previous.largest_group
            draining[previous] = loop.call_later(want.drain, stop_drain, previous)
        else:
            previous.cancel()
        slot.first_group = obj.group_id
        slot.largest_group = obj.group_id
        last_object_at = loop.time()
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
        if not over() and active is slot:
            deliver("on_object", obj)
        for held in replay:
            if over() or active is not slot:
                break
            if isinstance(held, ObjectEvent):
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
        # With the active subscription gone there is nothing left to compare with: the next object cuts over.
        passed = True
        if active.ended is None:
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
        if over():
            return
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
            ended = _classify(end, accepted(), slot.progressed, cancelled_or_connection)
            if ended.kind == "retry" and ended.reason is not None:
                active_ended(ended.reason)
            else:
                settle(ended)

    def open_slot(target: tuple[str, ...], target_generation: str | None) -> _Slot:
        options = want.options
        slot = _Slot(target, target_generation, options.track, options.parameters)
        subscription = connection.subscriber.subscribe(
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
        subscription.ended.add_done_callback(lambda _: on_ended(slot, subscription.ended.result()))
        return slot

    def reconcile() -> None:
        """Bring the subscriptions in line with what the want asks for now: its options and the newest generation."""
        nonlocal pending, move_failures, move_target
        if over():
            return
        target = want.options.namespace
        target_generation: str | None = None
        if watch is not None:
            failure = watch.failure()
            if failure is not None:
                settle(
                    _discovery_failed(failure[0], _any_cancelled(scope), cancelled_or_connection, accepted=accepted())
                )
                return
            target_generation = watch.current()
            # None announced: nothing to move to, so the want waits for one, as after a withdrawal.
            if target_generation is None:
                settle(_Outcome("moved", accepted=accepted()))
                return
            target = (*target, target_generation)
        track, parameters = want.options.track, want.options.parameters
        if active.made_with(target, track, parameters):
            # Asked for what is active again: a pending move is no longer wanted.
            clear_retry()
            move_target = None
            if pending is not None:
                pending.cancel()
                pending = None
            if active.ended is not None:
                settle(_Outcome("retry", accepted=True, progressed=active.progressed, reason=active.ended))
            return
        if pending is not None and pending.made_with(target, track, parameters):
            return
        if retry_move is not None and retry_move[0].made_with(target, track, parameters):
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
            pending = open_slot(target, target_generation)
        except Exception as error:
            if any(token.cancelled for token in scope) or not _refused_for_good(error):
                settle(cancelled_or_connection(True))
            else:
                settle(_Outcome("fatal", error=error))

    try:
        active = open_slot(namespace, generation)
    except Exception as error:
        want.settled_target = (want.options.track, want.options.parameters)
        # An argument the relay can never take is the want's fault; anything else is the connection ending
        # under the call.
        if any(token.cancelled for token in scope) or not _refused_for_good(error):
            return cancelled_or_connection(False)
        return _Outcome("fatal", error=error)

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
        want.changes.remove(changed)
        for slot in list(draining):
            stop_drain(slot)
        if pending is not None:
            pending.cancel()
        active.cancel()


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
