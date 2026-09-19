"""Connect to Relay for arbitrary track subscriptions. The caller owns connection shutdown."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from types import TracebackType
from typing import Self

from moq_core import (
    MAX_NAMESPACE_FIELDS,
    Closed,
    FetchHandle,
    FetchHandlers,
    MessageParameters,
    MoqTrackSource,
    MoqtSession,
    MoqtSessionCallbacks,
    MoqtSessionConfig,
    MoqtSubscribeOptions,
    MoqtTransport,
    Subscription,
    SubscriptionHandlers,
    TrackStatusHandlers,
    create_moqt_session,
)

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
)
from .subscription_events import SubscriptionEvents

IMPLEMENTATION_NAME = "mux-relay (Python)"


def _validate_namespace(namespace: Sequence[str]) -> None:
    if (
        not isinstance(namespace, Sequence)
        or isinstance(namespace, (str, bytes, bytearray))
        or not namespace
        or any(not isinstance(field, str) or not field for field in namespace)
    ):
        raise TypeError("A subscription namespace must contain non-empty string fields")
    if len(namespace) > MAX_NAMESPACE_FIELDS:
        raise ValueError(f"A subscription namespace must contain at most {MAX_NAMESPACE_FIELDS} fields")


class _BorrowedTracks:
    """Generic borrowed track access for integrations.

    Exposes no connection shutdown operation and has no dependency on Relay resource identifiers.
    Satisfies :class:`moq_core.MoqTrackSource`.
    """

    __slots__ = ("_closed", "_lifetime")

    def __init__(self, lifetime: RelaySessionLifetime[MoqtSession]) -> None:
        self._lifetime = lifetime
        self._closed = lifetime.closed

    @property
    def closed(self) -> asyncio.Future[Closed]:
        return self._closed

    def subscribe(self, options: MoqtSubscribeOptions, handlers: SubscriptionHandlers | None = None) -> Subscription:
        self._lifetime.assert_open()
        _validate_namespace(options.track_namespace)
        return self._lifetime.session.subscribe(options, handlers)

    def fetch(self, options: MoqtSubscribeOptions, handlers: FetchHandlers | None = None) -> FetchHandle:
        self._lifetime.assert_open()
        return self._lifetime.session.fetch(options, handlers)

    def track_status(self, options: MoqtSubscribeOptions, handlers: TrackStatusHandlers | None = None) -> None:
        self._lifetime.assert_open()
        self._lifetime.session.track_status(options, handlers)


class RelayConnection:
    """A subscriber connection. Create one with :func:`connect_relay`."""

    __slots__ = ("_lifetime", "_tracks")

    def __init__(self, lifetime: RelaySessionLifetime[MoqtSession]) -> None:
        self._lifetime = lifetime
        self._tracks = _BorrowedTracks(lifetime)

    @property
    def closed(self) -> asyncio.Future[Closed]:
        """Resolves once for local shutdown, peer closure, or failure. It never raises."""
        return self._lifetime.closed

    @property
    def tracks(self) -> MoqTrackSource:
        """Borrowed generic track access (subscribe, fetch, track status) without connection shutdown."""
        return self._tracks

    def subscribe(
        self,
        namespace: Sequence[str],
        track: str,
        handlers: SubscriptionHandlers | None = None,
        *,
        parameters: MessageParameters | None = None,
    ) -> Subscription:
        """Subscribe to any track payload format without discovering or loading a catalog.

        ``namespace`` fields are sent verbatim, without splitting or joining on slashes.
        ``parameters`` carries protocol subscription controls such as the location filter and priority.
        """
        _validate_namespace(namespace)
        return self._tracks.subscribe(
            MoqtSubscribeOptions(track_namespace=list(namespace), track_name=track, parameters=parameters), handlers
        )

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
        self._lifetime.assert_open()
        _validate_namespace(namespace)
        return SubscriptionEvents(
            lambda handlers: self.subscribe(namespace, track, handlers, parameters=parameters),
            self.closed,
            capacity=capacity,
            cancellation=cancellation,
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


async def connect_relay(
    url: str,
    *,
    get_token: TokenProvider | None = None,
    cancellation: CancellationToken | None = None,
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
    request_timeout: float = DEFAULT_REQUEST_TIMEOUT,
    create_transport: RelayTransportFactory | None = None,
) -> RelayConnection:
    """Connect to Relay for arbitrary track subscriptions.

    One deadline (``connect_timeout``) covers token resolution and both handshakes. ``cancellation``
    controls the connection's entire lifetime. An explicit ``jwt`` query parameter on ``url`` takes
    precedence over ``get_token``, matching the Relay authentication convention.
    """
    options = ConnectRelayOptions(
        url=url,
        get_token=get_token,
        cancellation=cancellation,
        connect_timeout=connect_timeout,
        request_timeout=request_timeout,
        create_transport=create_transport,
    )

    def create_session(transport: MoqtTransport, callbacks: LifetimeCallbacks) -> MoqtSession:
        return create_moqt_session(
            transport,
            MoqtSessionConfig(
                implementation_name=IMPLEMENTATION_NAME,
                request_timeout=options.request_timeout,
                callbacks=MoqtSessionCallbacks(on_closed=callbacks.on_closed, on_goaway=callbacks.on_goaway),
            ),
        )

    lifetime = await open_relay_session(options, create_session)
    return RelayConnection(lifetime)


__all__ = ["RelayConnection", "connect_relay"]
