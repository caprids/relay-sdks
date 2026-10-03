"""Connect to Relay for subscribing to tracks. The caller owns connection shutdown."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from dataclasses import dataclass, fields, replace
from types import TracebackType
from typing import Self

from moq_core import (
    Closed,
    FetchHandle,
    FetchHandlers,
    Goaway,
    KeyValuePair,
    MessageParameters,
    MoqtObject,
    MoqTrackSource,
    MoqtSession,
    MoqtSessionCallbacks,
    MoqtSessionConfig,
    MoqtSubscribeOptions,
    MoqtTransport,
    PublishDone,
    RequestError,
    SubgroupEnd,
    SubgroupReset,
    SubscribeOk,
    Subscription,
    SubscriptionHandlers,
    TrackStatusHandlers,
    create_moqt_session,
)

from ._awaitable import RelayAwaitable
from ._subscription import (
    RelaySubscription,
    SubscriptionConsumer,
    SubscriptionDependencies,
    SubscriptionEnd,
    SubscriptionEvent,
    UpdateOkEvent,
    synchronous_callback,
)
from ._validation import namespace_fields, track_name
from .cancellation import CancellationToken
from .connect_session import (
    DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_REQUEST_TIMEOUT,
    ConnectRelayOptions,
    LifetimeCallbacks,
    RelaySessionLifetime,
    RelayTransportFactory,
    TokenProvider,
    open_relay_session,
    validate_timeout,
)
from .subscription_events import SubscriptionEvents

IMPLEMENTATION_NAME = "mux-relay (Python)"


@dataclass(slots=True, kw_only=True)
class RelaySubscriptionHandlers:
    """Callbacks for one subscription made through :meth:`RelaySubscriber.subscribe`.

    Each is optional and runs synchronously as the protocol layer delivers the event; the SDK adds
    no queue. The names say what happened between this connection and the relay. The borrowed
    :attr:`RelaySubscriber.track_source` keeps the core's protocol names (``on_ok``, ``on_done``).
    """

    on_subscribed: Callable[[SubscribeOk], None] | None = None
    """The relay accepted the subscription (SUBSCRIBE_OK) and will forward objects.

    ``info`` carries the track alias, the relay's parameters (``largest_object`` when the track
    already has content), and the track properties as the relay sends them, which can differ from
    what the publisher declared.
    """
    on_error: Callable[[RequestError], None] | None = None
    """The relay answered the request with REQUEST_ERROR.

    It rejected the subscription, or the request failed before acceptance (request timeout, or the
    request stream failing). A REQUEST_ERROR can also arrive after acceptance, for example when the
    relay refuses an ``update()``, and it ends the subscription then too (issue #94). Connection
    shutdown and GOAWAY never arrive here: a pending subscription ends through ``subscribed`` and
    ``ended``, and an accepted one surfaces them as subgroup resets and connection closure.
    """
    on_object: Callable[[MoqtObject], None] | None = None
    """One object arrived. Payloads are uninterpreted bytes. Objects can arrive after ``on_publisher_done``."""
    on_subgroup_end: Callable[[SubgroupEnd], None] | None = None
    """A subgroup stream finished cleanly (FIN)."""
    on_subgroup_reset: Callable[[SubgroupReset], None] | None = None
    """A subgroup stream was reset; objects from it may be missing.

    Called for every reset the relay forwards. The relay may drop a reset for a subgroup it had not
    begun forwarding, so an abandoned group can arrive as nothing at all, only a gap in group IDs.
    """
    on_publisher_done: Callable[[PublishDone], None] | None = None
    """The publisher finished with the track (PUBLISH_DONE).

    A control notification, not the end of the subscription: objects still in transit can arrive
    after it, and the request stays open until ``cancel()`` or connection shutdown. The SDK does not
    resubscribe, and resubscribing cannot bring back a finished track: against moq-relay, a subscribe
    after the publisher has gone is refused (request error 16 ``unroutable``), and one while its
    namespace stays announced replays the track's cached last group and another done one round trip
    later. Back off, and do not resubscribe to a track the publisher ended. A done that follows this
    subscription's own ``update()`` is different: moq-relay ends the subscription on any update, the
    track is still live, and resubscribing is how to apply the change. After a publisher crash the
    done arrives only at the relay's QUIC idle timeout, about 30 s later. See spec/relay-interop.md,
    "Publisher restarts".
    """
    on_update_ok: Callable[[], None] | None = None
    """The relay acknowledged an ``update()`` on this subscription (REQUEST_OK).

    moq-relay never does: it ends the subscription with PUBLISH_DONE on any update (spec/behavior.md,
    relay interop deviation 4).
    """
    on_state_notify: Callable[[MessageParameters], None] | None = None
    """The publisher changed the subscription's state on its own (PUBLISH_STATE_NOTIFY).

    The parameters carry only what changed. Informational.
    """
    on_goaway: Callable[[Goaway], None] | None = None
    """GOAWAY on this request stream: the relay wants this request re-issued, possibly elsewhere.

    A GOAWAY on the connection itself ends the connection instead; observe ``closed`` for that.
    """


_EVENT_HANDLERS: dict[str, str] = {
    "subscribed": "on_subscribed",
    "object": "on_object",
    "publisher_done": "on_publisher_done",
    "subgroup_end": "on_subgroup_end",
    "subgroup_reset": "on_subgroup_reset",
    "state_notify": "on_state_notify",
    "goaway": "on_goaway",
    "update_ok": "on_update_ok",
}
"""Which product handler each product event reaches."""


class _HandlersConsumer:
    """The callback form as one consumer of a subscription.

    Snapshots the product callbacks at subscribe time, holds them to the synchronous callback policy,
    and dispatches each product event to the handler of the same meaning. A REQUEST_ERROR ending
    reaches ``on_error``; closure and cancellation reach no callback, only ``subscribed`` and ``ended``.
    """

    __slots__ = ("_callbacks",)

    def __init__(self, handlers: RelaySubscriptionHandlers) -> None:
        self._callbacks: dict[str, Callable[..., None]] = {}
        for handler_field in fields(handlers):
            callback = getattr(handlers, handler_field.name)
            if callback is not None:
                self._callbacks[handler_field.name] = synchronous_callback(handler_field.name, callback)

    def event(self, event: SubscriptionEvent) -> None:
        callback = self._callbacks.get(_EVENT_HANDLERS[event.type])
        if callback is None:
            return
        if isinstance(event, UpdateOkEvent):
            callback()
        else:
            callback(event.value)

    def ended(self, end: SubscriptionEnd) -> None:
        if end.cause == "request-error":
            callback = self._callbacks.get("on_error")
            if callback is not None:
                callback(end.error)


class _BorrowedTrackSource:
    """This connection seen through the generic :class:`moq_core.MoqTrackSource` contract.

    Track requests in protocol terms plus closure observation. No connection shutdown operation and
    no dependency on Relay resource identifiers.
    """

    __slots__ = ("_closed", "_lifetime")

    def __init__(self, lifetime: RelaySessionLifetime[MoqtSession]) -> None:
        self._lifetime = lifetime
        self._closed = lifetime.closed

    @property
    def closed(self) -> asyncio.Future[Closed]:
        return self._closed

    def subscribe(self, options: MoqtSubscribeOptions, handlers: SubscriptionHandlers | None = None) -> Subscription:
        # Arguments before connection state, as on the product entry points.
        admitted = replace(
            options,
            track_namespace=namespace_fields(options.track_namespace),
            track_name=track_name(options.track_name),
        )
        self._lifetime.assert_open()
        return self._lifetime.session.subscribe(admitted, handlers)

    def fetch(self, options: MoqtSubscribeOptions, handlers: FetchHandlers | None = None) -> FetchHandle:
        self._lifetime.assert_open()
        return self._lifetime.session.fetch(options, handlers)

    def track_status(self, options: MoqtSubscribeOptions, handlers: TrackStatusHandlers | None = None) -> None:
        self._lifetime.assert_open()
        self._lifetime.session.track_status(options, handlers)


class RelaySubscriber:
    """A connection that can only subscribe. Create one with :func:`connect_relay_subscriber`.

    Publishing uses a separate connection and token; see :func:`connect_relay_publisher`.
    """

    __slots__ = ("_closed", "_lifetime", "_track_source")

    def __init__(self, lifetime: RelaySessionLifetime[MoqtSession]) -> None:
        self._lifetime = lifetime
        self._track_source = _BorrowedTrackSource(lifetime)
        self._closed: RelayAwaitable[Closed] = RelayAwaitable(lifetime.closed)

    @property
    def closed(self) -> RelayAwaitable[Closed]:
        """Resolves once for local shutdown, peer closure, or failure. It never raises.

        Every waiter is isolated: a timed-out ``asyncio.wait_for`` or a cancelled task leaves the
        result intact for every other waiter.
        """
        return self._closed

    @property
    def track_source(self) -> MoqTrackSource:
        """This connection seen through the generic ``MoqTrackSource`` contract, for integrations such as players.

        ``subscribe``, ``fetch``, and ``track_status`` in protocol terms, plus ``closed``. It lists no
        tracks and cannot close the connection; the application that opened the connection owns shutdown.
        """
        return self._track_source

    def _admit(
        self, namespace: Sequence[str], track: str, parameters: MessageParameters | None
    ) -> MoqtSubscribeOptions:
        """Admit the arguments of one public call: checked and copied first, then the connection state.

        Both entry points do this synchronously, before anything (an iterator, a subscription) exists.
        """
        options = MoqtSubscribeOptions(
            track_namespace=namespace_fields(namespace), track_name=track_name(track), parameters=parameters
        )
        self._lifetime.assert_open()
        return options

    def _subscription(
        self, options: MoqtSubscribeOptions, consumers: Sequence[SubscriptionConsumer]
    ) -> RelaySubscription:
        """One product subscription per public call; both entry points bind the core the same way."""
        session = self._lifetime.session
        return RelaySubscription(
            SubscriptionDependencies(
                subscribe=lambda handlers: session.subscribe(options, handlers),
                observe_closed=self._lifetime.observe_closed,
                transport_ended=self._lifetime.transport_ended,
            ),
            consumers,
        )

    def subscribe(
        self,
        namespace: Sequence[str],
        track: str,
        handlers: RelaySubscriptionHandlers | None = None,
        *,
        parameters: MessageParameters | None = None,
    ) -> RelaySubscription:
        """Ask the relay for a track's objects, in any payload format, without discovering or loading a catalog.

        Returns at once with a cancellable handle whose ``subscribed`` settles when the relay answers
        and whose ``ended`` settles once the subscription can no longer deliver;
        ``handlers.on_subscribed`` and ``handlers.on_error`` report the same answer as callbacks.
        ``namespace`` fields are sent verbatim, without splitting or joining on slashes.
        ``parameters`` carries protocol subscription controls such as the location filter and priority.
        """
        consumers = [_HandlersConsumer(handlers)] if handlers is not None else []
        return self._subscription(self._admit(namespace, track, parameters), consumers)

    def subscribe_events(
        self,
        namespace: Sequence[str],
        track: str,
        *,
        parameters: MessageParameters | None = None,
        capacity: int = 64,
        cancellation: CancellationToken | None = None,
    ) -> SubscriptionEvents:
        """Create a bounded event iterator. Use ``async with`` around ``async for``.

        Owns only this request. ``publisher_done`` permits trailing objects; it is not EOF.
        """
        # Admit before building the iterator: an already-cancelled token never runs its callback.
        options = self._admit(namespace, track, parameters)
        return SubscriptionEvents(
            lambda consumer: self._subscription(options, [consumer]), capacity=capacity, cancellation=cancellation
        )

    def close(self) -> None:
        """Close this connection and all requests on it. Idempotent."""
        self._lifetime.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        self.close()


async def connect_relay_subscriber(
    url: str,
    *,
    get_token: TokenProvider | None = None,
    cancellation: CancellationToken | None = None,
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
    request_timeout: float = DEFAULT_REQUEST_TIMEOUT,
    create_transport: RelayTransportFactory | None = None,
) -> RelaySubscriber:
    """Open a Relay connection for subscribing to tracks.

    One deadline (``connect_timeout``) covers token resolution and both handshakes. ``cancellation``
    controls the connection's entire lifetime. An explicit ``jwt`` query parameter on ``url`` takes
    precedence over ``get_token``, matching the Relay authentication convention; an empty one is a
    ``ValueError``. Publishing uses a separate connection; see :func:`connect_relay_publisher`.
    """
    # The request deadline is subscriber-only, so the subscriber checks it; the shared opener checks
    # the rest before anything connects.
    validate_timeout("request_timeout", request_timeout)
    options = ConnectRelayOptions(
        url=url,
        get_token=get_token,
        cancellation=cancellation,
        connect_timeout=connect_timeout,
        create_transport=create_transport,
    )

    def create_session(
        transport: MoqtTransport, callbacks: LifetimeCallbacks, setup_options: Sequence[KeyValuePair]
    ) -> MoqtSession:
        return create_moqt_session(
            transport,
            MoqtSessionConfig(
                implementation_name=IMPLEMENTATION_NAME,
                setup_options=setup_options,
                request_timeout=request_timeout,
                callbacks=MoqtSessionCallbacks(on_closed=callbacks.on_closed, on_goaway=callbacks.on_goaway),
            ),
        )

    lifetime = await open_relay_session(options, create_session)
    return RelaySubscriber(lifetime)


__all__ = ["RelaySubscriber", "RelaySubscriptionHandlers", "connect_relay_subscriber"]
