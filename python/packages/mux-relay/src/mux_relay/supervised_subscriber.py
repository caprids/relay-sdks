"""The supervised subscriber: one subscriber connection at a time, replaced with backoff whenever it
ends, and one supervisor per wanted track that keeps a subscription to it open across connections,
publisher restarts and generations.

Each want retries the announce race, backs off after a publisher's done or a rejection, resubscribes
a subscription that went stale, and follows a supervised publisher's newest generation through
namespace discovery. Objects are never fetched to fill a gap: a replacement subscription starts
live, and ``on_restarted`` tells the application to drop state that assumed continuity. See
``spec/behavior.md``, "Supervised connections". The counterpart of ``ts/src/supervised-subscriber.ts``.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable, Sequence
from copy import deepcopy
from dataclasses import dataclass, field, fields
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
    RelaySubscription,
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

RestartReason: TypeAlias = Literal["publisher-done", "stale", "generation", "connection", "terminated"]
"""Why a want replaced its subscription.

``publisher-done``: the relay sent PUBLISH_DONE. ``stale``: no object arrived for ``stale_after``.
``generation``: a newer generation was announced, or the current one was withdrawn. ``connection``:
the subscriber connection ended and was replaced. ``terminated``: the relay ended the subscription
with REQUEST_ERROR after accepting it.
"""

_DISCOVERY_CAPACITY = 1024
"""Events a root's discovery may queue: its first answer lists every namespace under the root."""

_ROOT_LINGER = 5.0
"""How long an unused root's discovery is kept, so a want that resubscribes at once reuses its snapshot."""


@dataclass(slots=True, kw_only=True)
class WantHandlers:
    """The callbacks a want delivers to. Like :class:`RelaySubscriptionHandlers`, they must be synchronous."""

    on_subscribed: Callable[[SubscribeOk, tuple[str, ...]], None] | None = None
    """A subscription was accepted. The second argument is the namespace it was made on: the
    generation's, when following one."""
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
    controller: CancellationToken = field(default_factory=CancellationToken)
    namespace: tuple[str, ...] = ()
    """The namespace the latest subscription was made on: the generation's when following one."""
    generation: str | None = None
    """The generation the latest subscription was made on, when following one."""
    deferred: BaseException | None = None
    """A discovery failure no wait fixes, seen while backing off, for the loop to surface."""

    def call(self, name: str, *args: object) -> None:
        """Deliver to one handler; nothing reaches a want after it was cancelled, except its own ``on_error``."""
        if self.controller.cancelled and name != "on_error":
            return
        callback = self.handlers.get(name)
        if callback is not None:
            callback(*args)


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
    ) -> None:
        """Keep a subscription to a track open until ``unwant(key)``.

        ``generations=True`` makes ``namespace`` a supervised publisher's root: the want follows the
        newest generation announced one field below it, and the subscriber's token must allow
        discovering the root. ``stale_after`` (seconds) resubscribes when no object arrived for that
        long, for tracks that are never silent while live. ``parameters`` is sent on every SUBSCRIBE;
        ``retry`` is the backoff between resubscriptions (from 0.25 s, doubling up to 8 s). Wanting a
        key again with the same namespace, track, parameters, ``generations`` and ``stale_after``
        only swaps the handlers and the retry backoff; anything else replaces the want. Arguments
        are checked at once.
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
        state = self._state
        if state.stopped:
            raise supervisor_closed("subscriber")

        # A snapshot, so the caller's later changes to the parameters neither reach the wire nor the identity.
        options = _WantOptions(fields_, name, generations, stale_after, deepcopy(parameters))
        existing = state.wants.get(key)
        if existing is not None and existing.options == options:
            existing.handlers = safe_handlers
            existing.retry = retry_backoff
            return
        if existing is not None:
            self._forget(key, existing)
        want = _Want(options, safe_handlers, retry_backoff)
        state.wants[key] = want
        task = asyncio.ensure_future(_supervise_want(want, self._live_connection, lambda: self._forget(key, want)))
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
        set_status("closed")
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

    runner = asyncio.ensure_future(run())
    _observe(runner)
    started = first
    assert started is not None
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
    move to the next one without delay. ``generation``: a newer generation, or none; resubscribe
    without delay. ``fatal``: a failure no wait fixes; surface it and stop. ``retry``: anything else;
    resubscribe after the backoff.
    """

    kind: Literal["cancelled", "connection", "generation", "fatal", "retry"]
    accepted: bool = False
    progressed: bool = False
    reason: RestartReason | None = None
    error: BaseException | None = None


_CANCELLED = _Outcome("cancelled")


async def _supervise_want(
    want: _Want,
    live_connection: Callable[[CancellationToken], Awaitable[_Connection | None]],
    forget: Callable[[], None],
) -> None:
    """Keep ``want`` subscribed until it is cancelled or fails for good."""
    scope = want.controller
    failures = 0
    while not scope.cancelled:
        if want.deferred is not None:
            # A discovery refusal seen while backing off is reported as one seen during an attempt would be.
            forget()
            want.call("on_error", want.deferred)
            return
        connection = await live_connection(scope)
        # A want replaced while waiting must not open anything: its replacement may already hold an overlapping root.
        if connection is None or scope.cancelled:
            return
        outcome = await _attempt(want, connection)
        if scope.cancelled or outcome.kind == "cancelled":
            return
        if outcome.kind == "fatal":
            forget()
            assert outcome.error is not None
            want.call("on_error", outcome.error)
            return
        reason: RestartReason | None = outcome.reason
        if outcome.kind == "connection":
            reason = "connection"
        elif outcome.kind == "generation":
            reason = "generation"
        if outcome.accepted and reason is not None:
            want.call("on_restarted", reason, want.namespace)
        if outcome.kind == "connection":
            # The request can see the end before the connection reports it; wait for that rather than spin on it.
            await wait_any([connection.ended, scope], limit=1.0)
            continue
        if outcome.kind != "retry":
            continue
        # Cached objects followed by a done are not progress: only a later group resets the backoff.
        if outcome.progressed:
            failures = 0
        delay = backoff_delay(failures, want.retry)
        failures += 1
        await _backoff(want, connection, delay)


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
    try:
        changed()
        await wait_any([want.controller, connection.ended, wake], limit=delay)
    finally:
        if unwatch is not None:
            unwatch()
        if remove_release is not None:
            remove_release()
        if held is not None:
            held[1]()


async def _attempt(want: _Want, connection: _Connection) -> _Outcome:
    """One subscription for ``want`` on ``connection``, from SUBSCRIBE to whatever ends it."""
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
        return await _subscribe_once(want, connection, namespace, scope, watch, generation, cancelled_or_connection)
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


async def _subscribe_once(
    want: _Want,
    connection: _Connection,
    namespace: tuple[str, ...],
    scope: list[CancellationToken],
    watch: _RootWatch | None,
    generation: str | None,
    cancelled_or_connection: Callable[[bool], _Outcome],
) -> _Outcome:
    options = want.options
    loop = asyncio.get_running_loop()
    accepted = False
    first_group: int | None = None
    progressed = False
    last_object_at = 0.0
    publisher_done = False
    went_stale = False
    finish = CancellationToken()

    def on_subscribed(info: SubscribeOk) -> None:
        nonlocal accepted, last_object_at
        accepted = True
        last_object_at = loop.time()
        want.call("on_subscribed", info, namespace)

    def on_object(obj: MoqtObject) -> None:
        nonlocal first_group, progressed, last_object_at
        last_object_at = loop.time()
        if first_group is None:
            first_group = obj.group_id
        elif obj.group_id != first_group:
            progressed = True
        want.call("on_object", obj)

    def on_publisher_done(info: PublishDone) -> None:
        nonlocal publisher_done
        publisher_done = True
        want.call("on_publisher_done", info)
        finish.cancel()

    try:
        subscription: RelaySubscription = connection.subscriber.subscribe(
            namespace,
            options.track,
            RelaySubscriptionHandlers(
                on_subscribed=on_subscribed,
                on_object=on_object,
                on_subgroup_end=lambda info: want.call("on_subgroup_end", info),
                on_subgroup_reset=lambda info: want.call("on_subgroup_reset", info),
                on_publisher_done=on_publisher_done,
            ),
            parameters=deepcopy(options.parameters),
        )
    except Exception as error:
        # An argument the relay can never take is the want's fault; anything else is the connection ending
        # under the call.
        if any(token.cancelled for token in scope) or not _refused_for_good(error):
            return cancelled_or_connection(False)
        return _Outcome("fatal", error=error)

    stale_task: asyncio.Future[None] | None = None
    stale_after = options.stale_after
    if stale_after is not None:
        interval = max(0.01, min(1.0, stale_after / 4))

        async def watch_stale() -> None:
            nonlocal went_stale
            while True:
                await asyncio.sleep(interval)
                if accepted and loop.time() - last_object_at >= stale_after:
                    went_stale = True
                    finish.cancel()
                    return

        stale_task = asyncio.ensure_future(watch_stale())

    moved = False
    discovery_ended: tuple[BaseException | None] | None = None

    def check_watch() -> None:
        nonlocal moved, discovery_ended
        if watch is None:
            return
        discovery_ended = watch.failure()
        if discovery_ended is not None:
            finish.cancel()
        elif watch.current() != generation:
            moved = True
            finish.cancel()

    unwatch = watch.on_change(check_watch) if watch is not None else None
    # A change that landed between choosing the generation and attaching the listener is not lost.
    check_watch()
    try:
        await wait_any([*scope, finish], settles=[subscription.ended])
        if not subscription.ended.done():
            if any(token.cancelled for token in scope):
                return cancelled_or_connection(accepted)
            if discovery_ended is not None:
                return _discovery_failed(
                    discovery_ended[0], _any_cancelled(scope), cancelled_or_connection, accepted=accepted
                )
            if moved:
                return _Outcome("generation", accepted=accepted)
            return _Outcome(
                "retry",
                accepted=accepted,
                progressed=progressed,
                reason="publisher-done" if publisher_done else "stale" if went_stale else None,
            )
        return _classify(subscription.ended.result(), accepted, progressed, cancelled_or_connection)
    finally:
        if stale_task is not None:
            stale_task.cancel()
        if unwatch is not None:
            unwatch()
        subscription.cancel()


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


__all__ = ["RestartReason", "SupervisedSubscriber", "WantHandlers", "connect_supervised_subscriber"]
