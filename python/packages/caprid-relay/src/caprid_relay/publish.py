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
from collections.abc import Awaitable, Callable, Sequence
from datetime import datetime
from types import TracebackType
from typing import Any, Self

from moq_core import (
    Closed,
    KeyValuePair,
    Location,
    MoqtPublishSession,
    MoqtPublishSessionCallbacks,
    MoqtPublishSessionConfig,
    MoqtTransport,
    ObjectStatus,
    RegisteredTrack,
    RegisterTrackOptions,
    SubgroupWriter,
    TrackBinding,
    TrackNamespace,
    create_moqt_publish_session,
    create_subgroup_writer,
)

from ._awaitable import RelayAwaitable
from ._settling_groups import SettlingGroups
from ._validation import namespace_fields, track_name, track_property_pairs
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

IMPLEMENTATION_NAME = "caprid-relay (Python) publisher"
DEFAULT_WRITE_TIMEOUT = 10.0


class RelayPublication:
    """One namespace a connection announces, with the tracks it serves: what :meth:`RelaySession.publish` returns.

    A dedicated publisher connection (:class:`RelayPublisher`) is one of these. Its tracks behave as on
    :class:`RelayPublisher`.
    """

    __slots__ = (
        "_announced",
        "_announced_awaitable",
        "_closed",
        "_closed_awaitable",
        "_closing",
        "_drain_timeout",
        "_groups",
        "_lifetime",
        "_lost",
        "_namespace",
        "_session",
        "_shutdown",
        "_tracks",
    )

    def __init__(
        self,
        session: MoqtPublishSession,
        namespace: tuple[str, ...],
        lifetime: RelaySessionLifetime[Any],
        groups: SettlingGroups,
    ) -> None:
        self._session = session
        self._namespace = namespace
        self._lifetime = lifetime
        # The connection's limit applies to every publication's groups, but closing one waits only for its own.
        self._groups = groups
        # One write slot per registered track; the session's binding callback reaches them through here.
        self._tracks: dict[str, RelayTrackWriter] = {}
        # How long closing waits for the groups: the largest write deadline among the tracks.
        self._drain_timeout = 0.0
        self._closing = False
        # Set when the namespace was lost while the connection lives: every later operation fails with it.
        self._lost: BaseException | None = None
        self._shutdown: asyncio.Future[None] | None = None
        loop = asyncio.get_running_loop()
        self._closed: asyncio.Future[Closed] = loop.create_future()
        self._closed_awaitable: RelayAwaitable[Closed] = RelayAwaitable(self._closed)
        # Settled by the session's on_announced callback; an end that comes first terminates it.
        self._announced: asyncio.Future[None] = loop.create_future()
        self._announced_awaitable: RelayAwaitable[None] = RelayAwaitable(self._announced)
        _observe(self._announced)

        session.announce(namespace)

    @property
    def namespace(self) -> list[str]:
        """The announced namespace."""
        return list(self._namespace)

    @property
    def announced(self) -> RelayAwaitable[None]:
        """Settles once the namespace announce has reached the wire on a solicitation that covers it.

        No earlier than the first :meth:`register_track`; see :attr:`RelayPublisher.announced`. The
        publication or the connection ending first terminates it. Every waiter is isolated.
        """
        return self._announced_awaitable

    @property
    def closed(self) -> RelayAwaitable[Closed]:
        """Resolves once the publication has ended. Errors are returned as data; this never raises.

        That is after :meth:`close`, when the relay withdrew its interest in the namespace (a ``RelayError``
        with code ``NAMESPACE_ENDED``; the connection stays open), or with the connection. The error is
        ``None`` after ``close()`` or a clean connection shutdown.
        """
        return self._closed_awaitable

    def register_track(
        self,
        track: str,
        *,
        track_properties: Sequence[KeyValuePair] | None = None,
        write_timeout: float = DEFAULT_WRITE_TIMEOUT,
    ) -> RelayTrackWriter:
        """Register a track in this namespace and return its writer, as :meth:`RelayPublisher.register_track` does."""
        # Arguments first, then connection state, then the registry.
        name = track_name(track)
        properties = track_property_pairs(track_properties)

        validate_timeout("write_timeout", write_timeout)

        self._assert_published()

        if self._closing:
            raise RelayError("Relay publisher is closed", code="CLOSED")

        if name in self._tracks:
            raise ValueError("Track name is already registered on this publisher connection")

        session = self._session
        namespace = self._namespace
        registered = session.register_track(
            RegisterTrackOptions(track_namespace=namespace, track_name=name, track_properties=properties)
        )

        async def open_subgroup(track_alias: int, group_id: int) -> _ReportingObjects:
            stream = await session.open_uni_stream(track_alias)
            subgroup = create_subgroup_writer(stream, track_alias=track_alias, group_id=group_id, has_properties=True)
            return _ReportingObjects(subgroup, registered)

        writer = RelayTrackWriter(
            name,
            WriteSlotDependencies(
                open_subgroup=open_subgroup,
                settling_groups=self._groups,
                namespace_solicited=lambda: session.is_solicited(namespace),
                assert_open=self._assert_published,
                end_track=registered.end,
                write_timeout=write_timeout,
                track_properties=properties,
            ),
        )
        self._tracks[name] = writer
        self._drain_timeout = max(self._drain_timeout, write_timeout)
        return writer

    async def close(self) -> None:
        """End this namespace's tracks, wait for their groups, then withdraw the namespace. Idempotent.

        It waits, for up to the largest ``write_timeout`` among the tracks, for the relay to acknowledge
        the groups that completed and each track's end, as :meth:`RelayPublisher.close` does, then
        withdraws the namespace (NAMESPACE_DONE). The connection stays open. Cancelling the caller does
        not stop the close.
        """
        if self._shutdown is None:
            # A publication the connection's end already ended has nothing left to do.
            if not self._lifetime.closed.done():
                self._end_tracks()
            self._shutdown = asyncio.ensure_future(self._finish_close())
            _observe(self._shutdown)

        await asyncio.shield(self._shutdown)

    async def _finish_close(self) -> None:
        if self._lifetime.closed.done():
            return

        await self._drain(self._drain_timeout)

        if not self._announced.done():
            self._announced.set_exception(
                RelayError("Relay publication closed before its namespace was announced", code="CLOSED")
            )

        self._session.withdraw(self._namespace)

        if not self._closed.done():
            self._closed.set_result(Closed())

    def _assert_published(self) -> None:
        self._lifetime.assert_open()

        if self._lost is not None:
            raise self._lost

    def _end_tracks(self) -> None:
        """Refuse new tracks and end every track, cancelling what is in flight."""
        self._closing = True

        for writer in list(self._tracks.values()):
            writer.close()

    async def _drain(self, seconds: float) -> None:
        """Wait for this publication's settling groups and its tracks' subscription ends, bounded.

        Closing the session sends the close capsule and then the transport close, and the relay aborts a
        track whose data or PUBLISH_DONE it has not received by then (viewers see status 0). So the last
        groups and the ends get up to one write deadline to be acknowledged first. Every track belongs to
        a publication, so draining them all, as ``end_all`` does, waits for every end on the connection.
        """
        await self._groups.drain(
            max(self._drain_timeout, seconds),
            self._lifetime.closed,
            self._session.wait_ends_acknowledged(self._namespace),
        )

    def _on_announced(self) -> None:
        if not self._announced.done():
            self._announced.set_result(None)

    def _bind(self, binding: TrackBinding) -> None:
        writer = self._tracks.get(binding.track_name)

        if writer is not None:
            writer._bind(binding.track_alias, binding.object_delivery_timeout, binding.subgroup_delivery_timeout)

    def _disconnect(self, error: BaseException | None, *, live: bool = False) -> None:
        """The connection ended with ``error``, or (``live``) the namespace was lost while the connection lives.

        A lost namespace ends its tracks as ``close()`` would, failing what is in flight with ``error``; the
        transport goes on, and so do the delivery timeouts it still needs.
        """
        self._closing = True

        if live:
            self._lost = error

        if not self._announced.done():
            # Only a clean shutdown (no error at all) becomes CLOSED.
            self._announced.set_exception(
                error
                if error is not None
                else RelayError("Relay publisher closed before its namespace was announced", code="CLOSED")
            )

        for writer in list(self._tracks.values()):
            if live and error is not None:
                writer._end(error)
            else:
                writer._disconnect(error)

        # The relay withdrawing the namespace while the application's own close() drains it does not make that
        # close a withdrawal: `closed` reports the close the application asked for.
        withdrawn_while_closing = live and self._shutdown is not None
        if not self._closed.done():
            self._closed.set_result(Closed() if error is None or withdrawn_while_closing else Closed(error))


class _PublishHost:
    """The publishing state of one connection: its publications, and how the connection tells them it has ended."""

    __slots__ = ("_bound", "_groups", "_publications", "_shutting_down")

    def __init__(self) -> None:
        self._publications: dict[tuple[str, ...], RelayPublication] = {}
        # Groups whose FIN the transport took but the relay has not acknowledged, shared by every track on the
        # connection.
        self._groups = SettlingGroups()
        # Set once the connection starts closing: no publication may start after `end_all()` took its snapshot.
        self._shutting_down = False
        self._bound: tuple[MoqtPublishSession, RelaySessionLifetime[Any]] | None = None

    def session_callbacks(
        self,
        connection: LifetimeCallbacks,
        on_namespace_ended: Callable[[TrackNamespace, BaseException | None], None],
    ) -> MoqtPublishSessionCallbacks:
        """The publish session's callbacks: the connection's closure and GOAWAY, and what reaches the publications.

        ``on_namespace_ended`` is told when the relay withdraws its interest in a namespace (no solicitation
        covers it anymore).
        """

        def on_announced(namespace: TrackNamespace) -> None:
            publication = self._publications.get(tuple(namespace))
            if publication is not None:
                publication._on_announced()

        def on_track_binding(binding: TrackBinding) -> None:
            publication = self._publications.get(tuple(binding.track_namespace))
            if publication is not None:
                publication._bind(binding)

        return MoqtPublishSessionCallbacks(
            on_closed=connection.on_closed,
            on_goaway=connection.on_goaway,
            on_announced=on_announced,
            on_track_binding=on_track_binding,
            on_announce_ended=lambda ended: on_namespace_ended(ended.namespace, ended.error),
        )

    def bind(self, session: MoqtPublishSession, lifetime: RelaySessionLifetime[Any]) -> None:
        """Bind the session once it is up. Before this, ``publish()`` cannot run."""
        self._bound = (session, lifetime)
        lifetime.closed.add_done_callback(self._connection_ended)

    def _connection_ended(self, closed: asyncio.Future[Closed]) -> None:
        error = closed.result().error

        for publication in list(self._publications.values()):
            publication._disconnect(error)

    def publish(self, namespace: tuple[str, ...]) -> RelayPublication:
        """Announce ``namespace`` and serve tracks in it. A namespace published once on a connection stays taken."""
        assert self._bound is not None, "publish before the connection is up"
        session, lifetime = self._bound
        lifetime.assert_open()

        if self._shutting_down:
            raise RelayError("Relay connection is closed", code="CLOSED")

        if namespace in self._publications:
            raise ValueError(
                "Namespace is already published on this connection, including after its publication closed"
            )

        publication = RelayPublication(session, namespace, lifetime, self._groups.subset())
        self._publications[namespace] = publication
        return publication

    def end_all(self) -> Awaitable[object]:
        """End every publication's tracks now; the result waits, bounded, for their groups to settle.

        For a connection about to close: the connection's groups get the largest deadline among all its
        tracks, as a publisher connection's do.
        """
        self._shutting_down = True
        publications = list(self._publications.values())
        seconds = max((publication._drain_timeout for publication in publications), default=0.0)

        for publication in publications:
            publication._end_tracks()

        return asyncio.gather(*(publication._drain(seconds) for publication in publications))

    def disconnect(self, namespace: TrackNamespace, error: BaseException) -> None:
        """End one publication while the connection lives: its tracks fail with ``error``, and the namespace is
        withdrawn."""
        publication = self._publications.get(tuple(namespace))

        if publication is None or self._bound is None:
            return

        session, lifetime = self._bound
        if lifetime.closed.done():
            return

        session.withdraw(namespace)
        publication._disconnect(error, live=True)


class RelayPublisher:
    """A connection that can only publish, serving one namespace. Create one with :func:`connect_relay_publisher`.

    Subscribing uses a separate connection; see :func:`connect_relay_subscriber`. :func:`connect_relay` opens one
    connection that does both.
    """

    __slots__ = ("_closed", "_closed_awaitable", "_host", "_lifetime", "_publication", "_shutdown", "_watcher")

    def __init__(
        self,
        lifetime: RelaySessionLifetime[MoqtPublishSession],
        host: _PublishHost,
        publication: RelayPublication,
    ) -> None:
        self._lifetime = lifetime
        self._host = host
        self._publication = publication
        self._shutdown: asyncio.Future[None] | None = None
        self._closed: asyncio.Future[Closed] = asyncio.get_running_loop().create_future()
        self._closed_awaitable: RelayAwaitable[Closed] = RelayAwaitable(self._closed)
        self._watcher: asyncio.Future[None] = asyncio.ensure_future(self._await_transport_closed())
        _observe(self._watcher)

    async def _await_transport_closed(self) -> None:
        try:
            await self._lifetime.wait_transport_closed()
        except Exception:
            pass

        result = await self._lifetime.closed

        if not self._closed.done():
            self._closed.set_result(result)

    @property
    def expires_at(self) -> datetime | None:
        """When the relay will end this connection for its credential's expiry, as an aware UTC datetime.

        The :class:`RelayCredential`'s ``expires_at``, or the JWT's ``exp`` claim, read without
        verifying it; ``None`` when neither is known. The relay closes the session then with
        ``UNAUTHORIZED`` in phase ``connection``; open a replacement before it. Supervised connections do.
        """
        return self._lifetime.expires_at

    @property
    def namespace(self) -> list[str]:
        """The namespace every track on this connection is published under."""
        return self._publication.namespace

    @property
    def announced(self) -> RelayAwaitable[None]:
        """Settles once the namespace announce has reached the wire on a solicitation that covers it.

        That is no earlier than the first :meth:`register_track`, and it is the strongest fact the
        publisher has: the relay acknowledges an announce with nothing and routes subscribes for the
        namespace about a round trip later, so a subscribe that races it is rejected (request error
        16 ``unroutable`` on moq-relay 0.15.8 and later; see ``spec/behavior.md``). It stays pending while no
        solicitation covers the namespace. The first write then reports ``NAMESPACE_NOT_SOLICITED`` if
        solicitations arrived and none covers it, or ``WRITE_TIMEOUT`` if none arrived at all, as for a
        token that can't publish anywhere. The connection's closure terminates it if that comes first:
        it raises the closure's error, or a ``CLOSED`` :class:`RelayError` after a clean shutdown.
        Every waiter is isolated, as for ``closed``.
        """
        return self._publication.announced

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
        ``track_properties`` is encoded as supplied; the SDK adds no default timescale. It rides in
        every SUBSCRIBE_OK (and TRACK_STATUS_OK), so it must encode to at most 65525 bytes, hold at most 64 pairs (the
        relay's limit) and keep 0x0E at most 255 and 0x22 at 1 or 2 (draft-22); otherwise the relay
        would refuse every subscriber, so ``register_track`` raises ``ValueError`` instead.
        The relay may add, drop, or rewrite properties before delivery to subscribers; see README.
        Properties 0x02 (OBJECT_DELIVERY_TIMEOUT) and 0x06 (SUBGROUP_DELIVERY_TIMEOUT), in
        milliseconds, are also the publisher's delivery timeouts (draft-ietf-moq-transport-22
        section 5.2), which the writer enforces; 0 means none, and values above 2147483647 are clamped.
        ``write_timeout`` (seconds) bounds relay demand and the transport write together.
        """
        return self._publication.register_track(track, track_properties=track_properties, write_timeout=write_timeout)

    async def close(self) -> None:
        """End all tracks, retract the namespace, and wait for transport shutdown. Idempotent.

        Before the session ends it waits, for up to the largest ``write_timeout`` among the tracks,
        for the relay to acknowledge the groups that completed and each track's end; any still
        unacknowledged after that may be lost, and the close still succeeds. Cancelling the caller
        does not stop the shutdown.
        """
        if self._shutdown is None:
            self._shutdown = asyncio.ensure_future(self._close_after(self._host.end_all()))
            _observe(self._shutdown)

        await asyncio.shield(self._shutdown)

    async def _close_after(self, drained: Awaitable[object]) -> None:
        await drained
        self._lifetime.close()
        await self._closed

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        await self.close()


class _ReportingObjects:
    """A subgroup writer that tells the core of every object the transport took.

    The core answers the relay's TRACK_STATUS with the track's largest object (draft-22 §9.20.17).
    """

    __slots__ = ("_subgroup", "_track", "group_id")

    def __init__(self, subgroup: SubgroupWriter, track: RegisteredTrack) -> None:
        self._subgroup = subgroup
        self._track = track
        self.group_id = subgroup.group_id

    async def write_object(
        self,
        *,
        object_id: int,
        payload: bytes,
        properties: Sequence[KeyValuePair] | None = None,
        status: ObjectStatus = "normal",
    ) -> None:
        await self._subgroup.write_object(object_id=object_id, payload=payload, properties=properties, status=status)
        self._track.object_published(Location(group=self.group_id, object=object_id))

    async def fin(self) -> None:
        await self._subgroup.fin()

    async def wait_acknowledged(self) -> None:
        await self._subgroup.wait_acknowledged()

    def abort(self, reason: object = None) -> None:
        self._subgroup.abort(reason)


def _namespace_ended(cause: BaseException | None) -> RelayError:
    error = RelayError("Relay withdrew the namespace", code="NAMESPACE_ENDED", phase="connection")
    error.__cause__ = cause
    return error


async def connect_relay_publisher(
    url: str | None = None,
    *,
    namespace: Sequence[str],
    get_token: TokenProvider | None = None,
    cancellation: CancellationToken | None = None,
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
    create_transport: RelayTransportFactory | None = None,
) -> RelayPublisher:
    """Open a dedicated Relay publisher connection.

    Subscribers use :func:`connect_relay_subscriber`, and :func:`connect_relay` opens one connection that
    does both. One publisher connection serves one ``namespace`` and any number of tracks. A relay must
    solicit the namespace and subscribe upstream before data can flow. With no ``url``, the connection dials
    ``DEFAULT_ENDPOINT`` rooted at the token's ``root`` claim, which ``get_token`` must then supply.
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
    host = _PublishHost()

    def create_session(
        transport: MoqtTransport, callbacks: LifetimeCallbacks, setup_options: Sequence[KeyValuePair]
    ) -> MoqtPublishSession:
        # A dedicated publisher's one namespace is the whole connection: losing it ends the connection.
        def on_namespace_ended(_: TrackNamespace, cause: BaseException | None) -> None:
            callbacks.on_closed(Closed(_namespace_ended(cause)))

        return create_moqt_publish_session(
            transport,
            MoqtPublishSessionConfig(
                implementation_name=IMPLEMENTATION_NAME,
                setup_options=setup_options,
                callbacks=host.session_callbacks(callbacks, on_namespace_ended),
            ),
        )

    lifetime = await open_relay_session(options, create_session)
    host.bind(lifetime.session, lifetime)
    return RelayPublisher(lifetime, host, host.publish(fields))


__all__ = [
    "DEFAULT_WRITE_TIMEOUT",
    "RelayGroupWriter",
    "RelayPublication",
    "RelayPublisher",
    "RelayTrackWriter",
    "connect_relay_publisher",
]
