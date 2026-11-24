"""The supervised publisher: one publisher connection at a time, replaced with backoff whenever it
ends, each serving the application's root namespace plus a fresh generation field.

Track writers outlive the connections: the application registers a track once and keeps writing to
the same writer, which binds to each generation's track in turn. Nothing written is replayed onto a
new generation; a write made while no generation is up fails at once with ``RECONNECTING``. It is a
view over the one supervision loop (``_supervisor.py``) whose connections each publish one namespace. See
``spec/behavior.md``, "Supervised connections". The counterpart of ``ts/src/supervised-publisher.ts``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from types import TracebackType
from typing import Self

from moq_core import Closed, KeyValuePair

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
from ._write_slot import RelayTrackWriter
from .cancellation import CancellationToken
from .connect_session import DEFAULT_CONNECT_TIMEOUT, RelayTransportFactory, TokenProvider
from .publish import DEFAULT_WRITE_TIMEOUT, RelayPublisher, connect_relay_publisher


@dataclass(frozen=True, slots=True)
class PublisherGeneration:
    """One publisher generation: a connection and the namespace it serves."""

    generation: str
    """The generation field: start time in fixed-width base 36, then four random characters, so it sorts by age."""
    namespace: tuple[str, ...]
    """The namespace this generation serves: the root followed by ``generation``."""


ConnectPublisher = Callable[[tuple[str, ...], CancellationToken], Awaitable[RelayPublisher]]
"""How a supervisor opens one generation's connection: the namespace, and the attempt's cancellation."""


class SupervisedPublisher:
    """A publisher that reconnects. Create one with :func:`connect_supervised_publisher`.

    Each connection is a new generation with its own namespace; tracks are registered once and
    rebound to every generation.
    """

    __slots__ = ("_closed", "_publication", "_supervisor")

    def __init__(self, supervisor: Supervisor, publication: SupervisedPublication) -> None:
        self._supervisor = supervisor
        self._publication = publication
        self._closed = RelayAwaitable(supervisor.closed)

    @property
    def generation(self) -> PublisherGeneration | None:
        """The live generation, or ``None`` between connections and after closing."""
        namespace = self._publication.namespace
        return None if namespace is None else PublisherGeneration(namespace[-1], namespace)

    @property
    def status(self) -> SupervisedStatus:
        """Where the supervisor stands; see :data:`SupervisedStatus`."""
        return self._supervisor.status

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
        return self._publication.register_track(track, track_properties=track_properties, write_timeout=write_timeout)

    def rotate(self) -> None:
        """Replace the live generation make-before-break, as happens on its own ``rotate_before`` before the
        credential expires.

        The next generation connects, with a fresh token, and every track is registered on it. Each
        track's writer then moves to it at its next write or group, once the track has demand there or
        has none on the old one. The old generation closes once every track has moved, after 10 seconds
        at most. A no-op while no generation is up.
        """
        self._supervisor.rotate()

    async def close(self) -> None:
        """Stop reconnecting and close the live generation as :meth:`RelayPublisher.close` does. Idempotent."""
        await asyncio.shield(self._supervisor.stop())

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

    async def connect_generation(attempt: CancellationToken, generation: str) -> SupervisedConnection:
        # A publisher connection announces one namespace, chosen as it connects, so this generation's.
        served = (*root, generation)
        publisher = await connect(served, attempt)

        def publish(announced: tuple[str, ...]) -> RelayPublisher:
            if announced != served:
                raise ValueError("A publisher connection serves only the namespace it connected with")
            return publisher

        return SupervisedConnection(
            closed=publisher.closed, expires_at=publisher.expires_at, close=publisher.close, publish=publish
        )

    def report(generation: str) -> None:
        if on_generation is not None:
            on_generation(PublisherGeneration(generation, (*root, generation)))

    supervisor = Supervisor(
        connect_generation,
        name="publisher",
        backoff=backoff,
        rotate_before=rotate_margin,
        cancellation=cancellation,
        on_status=on_status,
        on_generation=report,
    )
    publication = supervisor.publish(root)
    await supervisor.start()
    return SupervisedPublisher(supervisor, publication)


__all__ = ["PublisherGeneration", "SupervisedPublisher", "connect_supervised_publisher"]
