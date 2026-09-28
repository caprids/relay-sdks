"""Publish data on a dedicated Relay connection.

Publishing follows Relay's announce-and-serve ingest flow. :meth:`RelayPublisher.register_track`
registers a track and sends nothing; :meth:`RelayTrackWriter.write` waits for the relay to subscribe
upstream (demand) before sending bytes. Each write copies its bytes and sends one independent group
containing one object.
:meth:`RelayTrackWriter.open_group` sends a multi-object group instead: the LOC/MSF shape for video,
where a group is one GOP whose object 0 is the keyframe.

The behaviour behind a track writer is its write slot (see ``CONTEXT.md``), implemented in
``_write_slot.py`` and bound to the core session here.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from types import TracebackType
from typing import Self

from moq_core import (
    Closed,
    KeyValuePair,
    MoqtPublishSession,
    MoqtPublishSessionCallbacks,
    MoqtPublishSessionConfig,
    MoqtTransport,
    RegisterTrackOptions,
    SubgroupWriter,
    TrackBinding,
    create_moqt_publish_session,
    create_subgroup_writer,
)

from ._awaitable import RelayAwaitable
from ._validation import namespace_fields, property_pairs, track_name
from ._write_slot import RelayGroupWriter, RelayTrackWriter, WriteSlotDependencies, _observe
from .cancellation import CancellationToken
from .connect_session import (
    DEFAULT_CONNECT_TIMEOUT,
    ConnectRelayOptions,
    LifetimeCallbacks,
    RelaySessionLifetime,
    RelayTransportFactory,
    TokenProvider,
    open_relay_session,
    validate_timeout,
)
from .errors import RelayError

IMPLEMENTATION_NAME = "mux-relay (Python) publisher"
DEFAULT_WRITE_TIMEOUT = 10.0


class RelayPublisher:
    """A connection that can only publish, serving one namespace. Create one with :func:`connect_relay_publisher`.

    Subscribing uses a separate connection; see :func:`connect_relay_subscriber`.
    """

    __slots__ = (
        "_announced",
        "_announced_awaitable",
        "_closed",
        "_closed_awaitable",
        "_closing",
        "_lifetime",
        "_namespace",
        "_tracks",
        "_watcher",
    )

    def __init__(
        self,
        lifetime: RelaySessionLifetime[MoqtPublishSession],
        namespace: Sequence[str],
        announced: asyncio.Future[None],
    ) -> None:
        self._lifetime = lifetime
        self._namespace = namespace
        # One write slot per registered track; the session's binding callback and the connection's
        # end reach the slots through here.
        self._tracks: dict[str, RelayTrackWriter] = {}
        self._closing = False
        self._closed: asyncio.Future[Closed] = asyncio.get_running_loop().create_future()
        self._closed_awaitable: RelayAwaitable[Closed] = RelayAwaitable(self._closed)
        # Settled by the session's on_announced callback; a closure that comes first terminates it.
        self._announced = announced
        self._announced_awaitable: RelayAwaitable[None] = RelayAwaitable(announced)
        _observe(announced)

        lifetime.session.announce(namespace)
        lifetime.closed.add_done_callback(self._on_lifetime_closed)
        self._watcher: asyncio.Future[None] = asyncio.ensure_future(self._await_transport_closed())
        _observe(self._watcher)

    def _on_lifetime_closed(self, done: asyncio.Future[Closed]) -> None:
        self._closing = True
        error = done.result().error

        if not self._announced.done():
            self._announced.set_exception(
                error
                if error is not None
                else RelayError("Relay publisher closed before its namespace was announced", code="CLOSED")
            )

        for track in list(self._tracks.values()):
            track._disconnect(error)

    async def _await_transport_closed(self) -> None:
        try:
            await self._lifetime.wait_transport_closed()
        except Exception:
            pass

        result = await self._lifetime.closed

        if not self._closed.done():
            self._closed.set_result(result)

    def _on_track_binding(self, binding: TrackBinding) -> None:
        track = self._tracks.get(binding.track_name)

        if track is not None:
            track._bind(binding.track_alias)

    @property
    def namespace(self) -> list[str]:
        """The namespace every track on this connection is published under."""
        return list(self._namespace)

    @property
    def announced(self) -> RelayAwaitable[None]:
        """Settles once the namespace announce has reached the wire on a solicitation that covers it.

        That is no earlier than the first :meth:`register_track`, and it is the strongest fact the
        publisher has: the relay acknowledges an announce with nothing and routes subscribes for the
        namespace about a round trip later, so a subscribe that races it is rejected (request error
        16 ``unroutable`` on moq-relay 0.15.8; see ``spec/behavior.md``). It stays pending while no
        solicitation covers the namespace, where the first write reports ``NAMESPACE_NOT_SOLICITED``,
        and the connection's closure terminates it if that comes first: it raises the closure's
        error, or a ``CLOSED`` :class:`RelayError` after a clean shutdown. Every waiter is isolated, as for ``closed``.
        """
        return self._announced_awaitable

    @property
    def closed(self) -> RelayAwaitable[Closed]:
        """Resolves when the connection shuts down. Errors are returned as data; this never raises.

        Every waiter is isolated: a timed-out ``asyncio.wait_for`` or a cancelled task leaves the
        result intact for every other waiter.
        """
        return self._closed_awaitable

    def register_track(
        self,
        track: str,
        *,
        track_properties: Sequence[KeyValuePair] | None = None,
        write_timeout: float = DEFAULT_WRITE_TIMEOUT,
    ) -> RelayTrackWriter:
        """Register a track this publisher will serve and return its writer.

        Nothing is sent: the relay learns of the track when a subscriber asks for it, and bytes move
        when the writer's ``write()`` or ``open_group()`` runs, after waiting for that relay demand.
        No capture starts and no media catalog is published. Track names are unique per publisher
        connection, including after their writer closes.
        ``track_properties`` is encoded as supplied; the SDK adds no default timescale.
        The relay may add, drop, or rewrite properties before delivery to subscribers; see README.
        ``write_timeout`` (seconds) bounds relay demand and the transport write together.
        """
        # Arguments first, then connection state, then the registry.
        name = track_name(track)
        properties = property_pairs(track_properties)

        validate_timeout("write_timeout", write_timeout)

        self._lifetime.assert_open()

        if self._closing:
            raise RelayError("Relay publisher is closed", code="CLOSED")

        if name in self._tracks:
            raise ValueError("Track name is already registered on this publisher connection")

        session = self._lifetime.session
        registered = session.register_track(
            RegisterTrackOptions(track_namespace=self._namespace, track_name=name, track_properties=properties)
        )

        async def open_subgroup(track_alias: int, group_id: int) -> SubgroupWriter:
            stream = await session.open_uni_stream(track_alias)
            return create_subgroup_writer(stream, track_alias=track_alias, group_id=group_id, has_properties=True)

        writer = RelayTrackWriter(
            name,
            WriteSlotDependencies(
                open_subgroup=open_subgroup,
                namespace_solicited=lambda: session.namespace_solicited,
                assert_open=self._lifetime.assert_open,
                end_track=registered.end,
                write_timeout=write_timeout,
            ),
        )
        self._tracks[name] = writer
        return writer

    async def close(self) -> None:
        """End all tracks, retract the namespace, and wait for transport shutdown. Idempotent."""
        self._closing = True

        for track in list(self._tracks.values()):
            track.close()

        self._lifetime.close()
        await self._closed

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        await self.close()


def _namespace_ended(cause: BaseException | None) -> RelayError:
    error = RelayError("Relay withdrew the namespace", code="NAMESPACE_ENDED", phase="connection")
    error.__cause__ = cause
    return error


async def connect_relay_publisher(
    url: str,
    *,
    namespace: Sequence[str],
    get_token: TokenProvider | None = None,
    cancellation: CancellationToken | None = None,
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
    create_transport: RelayTransportFactory | None = None,
) -> RelayPublisher:
    """Open a dedicated Relay publisher connection.

    Subscribers use :func:`connect_relay_subscriber`; publishing does not share their transport. One
    publisher connection serves one ``namespace`` and any number of tracks. A relay must solicit the
    namespace and subscribe upstream before data can flow.
    """
    # Arguments before anything else: an invalid namespace never reaches the token provider or the
    # transport factory.
    fields = namespace_fields(namespace)

    options = ConnectRelayOptions(
        url=url,
        get_token=get_token,
        cancellation=cancellation,
        connect_timeout=connect_timeout,
        create_transport=create_transport,
    )
    publisher_ref: list[RelayPublisher] = []
    announced: asyncio.Future[None] = asyncio.get_running_loop().create_future()

    def on_announced(namespace: Sequence[str]) -> None:
        if not announced.done():
            announced.set_result(None)

    def create_session(transport: MoqtTransport, callbacks: LifetimeCallbacks) -> MoqtPublishSession:
        def on_track_binding(binding: TrackBinding) -> None:
            if publisher_ref:
                publisher_ref[0]._on_track_binding(binding)

        return create_moqt_publish_session(
            transport,
            MoqtPublishSessionConfig(
                implementation_name=IMPLEMENTATION_NAME,
                callbacks=MoqtPublishSessionCallbacks(
                    on_closed=callbacks.on_closed,
                    on_goaway=callbacks.on_goaway,
                    on_announced=on_announced,
                    on_track_binding=on_track_binding,
                    on_announce_ended=lambda ended: callbacks.on_closed(Closed(_namespace_ended(ended.error))),
                ),
            ),
        )

    lifetime = await open_relay_session(options, create_session)
    publisher = RelayPublisher(lifetime, fields, announced)
    publisher_ref.append(publisher)
    return publisher


__all__ = [
    "DEFAULT_WRITE_TIMEOUT",
    "RelayGroupWriter",
    "RelayPublisher",
    "RelayTrackWriter",
    "connect_relay_publisher",
]
