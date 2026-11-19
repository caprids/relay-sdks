"""The supervised relay: one connection that publishes and subscribes (:func:`connect_relay`), replaced with
backoff whenever it ends and make-before-break before its credential expires.

Each connection is a generation. Every published root is served as the root followed by that connection's
generation field, so all of a client's namespaces change generation together, and every want and
publishing-roots view moves with them. A view over the one supervision loop (``_supervisor.py``) whose
connections both publish and subscribe. See ``spec/behavior.md``, "Supervised relay". The counterpart of
``ts/src/supervised-relay.ts``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from types import TracebackType
from typing import Self

from moq_core import Closed, MessageParameters

from ._awaitable import RelayAwaitable
from ._supervision import (
    RECONNECT_BACKOFF,
    SupervisedStatus,
    SupervisionBackoff,
    backoff_options,
    new_generation,
    rotate_before_option,
)
from ._supervisor import SupervisedConnection, SupervisedPublication, Supervisor
from ._validation import namespace_fields
from .cancellation import CancellationToken
from .connect_session import DEFAULT_CONNECT_TIMEOUT, DEFAULT_REQUEST_TIMEOUT, RelayTransportFactory, TokenProvider
from .publishing_roots import PublishingRoots, PublishingRootsHandlers
from .relay_session import RelaySession, connect_relay
from .supervised_subscriber import WantHandlers, _WantSurface
from .switchable import Until


class SupervisedRelay:
    """One connection that publishes and subscribes, and reconnects. Create one with :func:`connect_supervised_relay`.

    It has the supervised subscriber's wants and publishing-roots views, and :meth:`publish` for any number
    of roots; all of them move to each new connection together. The relay never routes a connection's own
    announcements back to it: :meth:`publishing_roots` does not report this relay's own roots, and a want
    for one of its own tracks is refused (request error 16 on moq-relay).
    """

    __slots__ = ("_closed", "_supervisor", "_wants")

    def __init__(self, supervisor: Supervisor) -> None:
        self._supervisor = supervisor
        self._wants = _WantSurface(supervisor)
        self._closed = RelayAwaitable(supervisor.closed)

    @property
    def generation(self) -> str | None:
        """The live connection's generation field, or ``None`` between connections and after closing."""
        return self._supervisor.generation

    @property
    def status(self) -> SupervisedStatus:
        """Where the supervisor stands; see :data:`SupervisedStatus`."""
        return self._supervisor.status

    @property
    def closed(self) -> RelayAwaitable[Closed]:
        """Resolves once the supervisor stops, with the failure that stopped it or the cancellation's
        reason as ``error`` (``None`` after :meth:`close`). Never raises."""
        return self._closed

    def publish(self, root: Sequence[str]) -> SupervisedPublication:
        """Publish ``root`` on every connection, as the root followed by the connection's generation field.

        The root is checked at once: the root plus one field must satisfy the namespace rules. The token
        must grant publishing under it. A root published once stays taken, also after its publication
        closes: publishing it again raises ``ValueError``. When the relay stops soliciting one of the roots
        while the connection lives, the connection is replaced: every root changes generation together.
        Raises :class:`RelayError` with code ``CLOSED`` once the relay is closed.
        """
        fields = namespace_fields(root)
        # A generation field is always valid; check the whole namespace once so a root at the size limit fails here.
        namespace_fields((*fields, new_generation()))
        return self._supervisor.publish(fields)

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
        """Keep a subscription to a track open until ``unwant(key)``, as :meth:`SupervisedSubscriber.want` does.

        A want for one of this relay's own tracks is refused by the relay, as on :func:`connect_relay`.
        """
        self._wants.want(
            key,
            namespace,
            track,
            handlers,
            parameters=parameters,
            generations=generations,
            stale_after=stale_after,
            retry=retry,
            until=until,
            drain=drain,
        )

    def unwant(self, key: str) -> None:
        """Cancel a want's subscription and forget it. Unknown keys are ignored."""
        self._wants.unwant(key)

    def catch_up(self, key: str) -> None:
        """Replace a want's accepted subscription at once, as :meth:`SupervisedSubscriber.catch_up` does."""
        self._wants.catch_up(key)

    def publishing_roots(self, prefix: Sequence[str], handlers: PublishingRootsHandlers) -> PublishingRoots:
        """Follow the roots publishing now under ``prefix``, as :meth:`SupervisedSubscriber.publishing_roots` does.

        A namespace under one of this relay's own roots is never reported, also when the connection a
        rotation is replacing sees the replacement's announcements.
        """
        return self._wants.publishing_roots(prefix, handlers)

    def rotate(self) -> None:
        """Replace the connection make-before-break, as happens on its own ``rotate_before`` before the
        credential expires.

        A new connection opens with a fresh token and the next generation. Every track is registered on
        it and moves at its next write or group, once it has demand there or none on the old connection;
        every want moves its subscription as :meth:`SupervisedSubscriber.rotate` describes. The old
        connection closes once everything has moved, after 10 seconds at most. A no-op while no
        connection is up.
        """
        self._supervisor.rotate()

    async def close(self) -> None:
        """Stop reconnecting, cancel every want and view, end every publication's tracks, and close the
        connection as :meth:`RelaySession.close` does. Idempotent."""
        await asyncio.shield(self._supervisor.stop())

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        await self.close()


async def connect_supervised_relay(
    url: str | None = None,
    *,
    get_token: TokenProvider | None = None,
    cancellation: CancellationToken | None = None,
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
    request_timeout: float = DEFAULT_REQUEST_TIMEOUT,
    create_transport: RelayTransportFactory | None = None,
    reconnect: SupervisionBackoff | None = None,
    rotate_before: float | None = None,
    on_generation: Callable[[str], None] | None = None,
    on_status: Callable[[SupervisedStatus, BaseException | None], None] | None = None,
) -> SupervisedRelay:
    """Open one connection that publishes, subscribes and reconnects: each connection is a generation, and
    every published root, want and publishing-roots view moves to the next one together.

    It takes :func:`connect_relay`'s options plus the reconnection policy, and returns once the first
    connection is up. A first attempt that fails on its arguments, or because the relay refused the
    credentials (``UNAUTHORIZED``), raises that failure; any other failure is retried with backoff
    (``reconnect``, from 0.5 s doubling up to 10 s), bounded only by ``cancellation``, whose reason is
    raised. ``on_generation`` receives the generation field once each connection is up and every
    publication's tracks are registered on it; ``on_status`` runs on every status change, with what ended
    the previous connection or failed the last attempt. A raise from either stops the supervisor with that
    error. ``rotate_before`` (seconds, default 60) is how long before the connection's credential expires
    (:attr:`RelaySession.expires_at`) a replacement connection is opened, make-before-break; never earlier
    than halfway through the time left. With no ``url``, every connection dials the endpoint the first one
    rooted at its token's root. See ``spec/behavior.md``, "Supervised relay"; :func:`connect_relay` never
    reconnects.
    """

    async def connect(attempt: CancellationToken) -> RelaySession:
        nonlocal url
        session = await connect_relay(
            url,
            get_token=get_token,
            cancellation=attempt,
            connect_timeout=connect_timeout,
            request_timeout=request_timeout,
            create_transport=create_transport,
        )
        if url is None:
            url = session._lifetime.endpoint
        return session

    return await _supervise_relay(
        connect,
        cancellation=cancellation,
        reconnect=reconnect,
        rotate_before=rotate_before,
        on_generation=on_generation,
        on_status=on_status,
    )


ConnectSession = Callable[[CancellationToken], Awaitable[RelaySession]]
"""How a supervised relay opens one connection, given the attempt's cancellation."""


async def _supervise_relay(
    connect: ConnectSession,
    *,
    cancellation: CancellationToken | None = None,
    reconnect: SupervisionBackoff | None = None,
    rotate_before: float | None = None,
    on_generation: Callable[[str], None] | None = None,
    on_status: Callable[[SupervisedStatus, BaseException | None], None] | None = None,
) -> SupervisedRelay:
    """Open a supervised relay with ``connect``; the tests drive it with scripted connections."""
    backoff = backoff_options("reconnect", reconnect, RECONNECT_BACKOFF)
    rotate_margin = rotate_before_option(rotate_before)
    if cancellation is not None:
        cancellation.throw_if_cancelled()

    async def connect_once(attempt: CancellationToken, _generation: str) -> SupervisedConnection:
        session = await connect(attempt)
        return SupervisedConnection(
            closed=session.closed,
            expires_at=session.expires_at,
            close=session.close,
            subscriber=session,
            publish=session.publish,
        )

    supervisor = Supervisor(
        connect_once,
        name="relay",
        backoff=backoff,
        rotate_before=rotate_margin,
        cancellation=cancellation,
        on_status=on_status,
        on_generation=on_generation,
    )
    relay = SupervisedRelay(supervisor)
    await supervisor.start()
    return relay


__all__ = ["SupervisedRelay", "connect_supervised_relay"]
