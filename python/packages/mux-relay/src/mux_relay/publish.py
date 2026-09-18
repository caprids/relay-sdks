"""Publish data on a dedicated Relay connection.

Publishing follows Relay's announce-and-serve ingest flow. :meth:`RelayPublisher.publish` registers
a track; :meth:`RelayTrackWriter.write` waits for the relay to subscribe upstream before sending
bytes. Each write copies its bytes and sends one independent group containing one object.
:meth:`RelayTrackWriter.open_group` sends a multi-object group instead: the LOC/MSF shape for video,
where a group is one GOP whose object 0 is the keyframe.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Sequence
from dataclasses import dataclass
from types import TracebackType
from typing import Literal, Self

from moq_core import (
    MAX_NAMESPACE_FIELDS,
    MAX_OBJECT_PAYLOAD_LENGTH,
    Closed,
    KeyValuePair,
    MoqtPublishSession,
    MoqtPublishSessionCallbacks,
    MoqtPublishSessionConfig,
    MoqtTransport,
    ObjectStatus,
    RegisteredTrack,
    RegisterTrackOptions,
    SubgroupWriter,
    TrackBinding,
    create_moqt_publish_session,
    create_subgroup_writer,
)
from moq_core._tasks import call_later, observe, spawn
from moq_core.varint import MAX_VARINT_VALUE

from .cancellation import CancellationToken
from .connect_session import (
    DEFAULT_CONNECT_TIMEOUT,
    ConnectRelayOptions,
    LifetimeCallbacks,
    RelaySessionLifetime,
    RelayTransportFactory,
    TokenProvider,
    open_relay_session,
)
from .errors import (
    RelayClosedError,
    RelayError,
    RelayNamespaceError,
    RelayTimeoutError,
    RelayWritePendingError,
)
from .payload import _encode_json, _encode_text

IMPLEMENTATION_NAME = "mux-relay (Python) publisher"
DEFAULT_WRITE_TIMEOUT = 10.0
MAX_WRITE_TIMEOUT = 2_147_483.647


def _copy_properties(properties: Sequence[KeyValuePair] | None) -> list[KeyValuePair]:
    copied: list[KeyValuePair] = []
    for index, prop in enumerate(properties or ()):
        if not isinstance(prop.type, int) or isinstance(prop.type, bool) or not 0 <= prop.type <= MAX_VARINT_VALUE:
            raise TypeError(f"Property {index} type must be a non-negative 64-bit integer")
        if prop.type % 2 == 0:
            if (
                not isinstance(prop.value, int)
                or isinstance(prop.value, bool)
                or not 0 <= prop.value <= MAX_VARINT_VALUE
            ):
                raise TypeError(f"Property {index} value must be a non-negative 64-bit integer for an even type")
            value = prop.value
        else:
            if isinstance(prop.value, int):
                raise TypeError(f"Property {index} value must be bytes-like for an odd type")
            try:
                value = memoryview(prop.value).tobytes()
            except TypeError as error:
                raise TypeError(f"Property {index} value must be bytes-like for an odd type") from error
        copied.append(KeyValuePair(prop.type, value))
    return copied


@dataclass(slots=True)
class _Operation:
    """One pending write or open group: the unit that holds the track's single in-flight slot."""

    kind: Literal["write", "group"]
    demand: asyncio.Future[None]
    interrupted: asyncio.Future[None]
    alias: int | None = None
    writer: SubgroupWriter | None = None
    cancelled: bool = False
    failure: BaseException | None = None

    def wake(self) -> None:
        if not self.demand.done():
            self.demand.set_result(None)


class RelayGroupWriter:
    """One open multi-object group on a track. Create one with :meth:`RelayTrackWriter.open_group`.

    Objects are appended in order with increasing object IDs starting at 0, so the first object is
    the group's random-access point in LOC/MSF terms. ``close()`` FINs the stream, which is how a
    complete group is announced; ``abort()`` resets it. A group that fails part-way is reset, and the
    track stays open for the next group.
    """

    __slots__ = ("_next_object_id", "_operation", "_track", "_writer", "group_id")

    def __init__(self, track: RelayTrackWriter, operation: _Operation, writer: SubgroupWriter) -> None:
        self._track = track
        self._operation = operation
        self._writer = writer
        self._next_object_id = 0
        self.group_id = writer.group_id

    @property
    def closed(self) -> bool:
        """True once the group was FINed or reset; no further objects can be written."""
        return self._operation.cancelled or self._track._active is not self._operation

    @property
    def object_count(self) -> int:
        """Objects accepted by the transport so far."""
        return self._next_object_id

    def _check(self) -> None:
        if self._operation.cancelled:
            assert self._operation.failure is not None
            raise self._operation.failure

        if self._track._active is not self._operation:
            raise RelayClosedError("Group is closed")

        self._track._lifetime.assert_open()

    async def write_text(
        self, text: str, *, properties: Sequence[KeyValuePair] | None = None, status: ObjectStatus = "normal"
    ) -> None:
        """Encode UTF-8 text and write one object with the same semantics as ``write``."""
        await self.write(_encode_text(text), properties=properties, status=status)

    async def write_json(
        self, value: object, *, properties: Sequence[KeyValuePair] | None = None, status: ObjectStatus = "normal"
    ) -> None:
        """Serialize JSON and write one object; serialization errors leave the writer usable."""
        await self.write(_encode_json(value), properties=properties, status=status)

    async def write(
        self, payload: bytes, *, properties: Sequence[KeyValuePair] | None = None, status: ObjectStatus = "normal"
    ) -> None:
        """Append one object. Returns when the transport accepts it. Bytes are copied.

        Properties are encoded as supplied; the relay may transform them before delivery.
        Each call has its own ``write_timeout`` deadline; expiry resets the group and leaves the
        track open. ``status`` is only meaningful for an empty payload (an end-of-group or end-of-track marker).
        """
        self._check()

        if len(payload) > MAX_OBJECT_PAYLOAD_LENGTH:
            raise ValueError("Payload exceeds the 16 MiB object limit")

        object_id = self._next_object_id
        data = bytes(payload)
        object_properties = _copy_properties(properties)

        await self._await_transport(
            self._writer.write_object(object_id=object_id, payload=data, properties=object_properties, status=status)
        )

        # The binding may have moved while the write was in flight; the object may then be lost.
        self._check()
        self._next_object_id += 1

    async def close(self) -> None:
        """FIN within ``write_timeout``; expiry resets the group. Idempotent once closed cleanly."""
        if self._operation.cancelled:
            assert self._operation.failure is not None
            raise self._operation.failure

        if self._track._active is not self._operation:
            return

        try:
            await self._await_transport(self._writer.fin())
        finally:
            if self._track._active is self._operation:
                self._track._active = None

    async def _await_transport(self, action: Awaitable[None]) -> None:
        timer = self._track._arm_timeout(self._operation)
        sending = asyncio.ensure_future(action)
        observe(sending)
        try:
            await asyncio.wait({sending, self._operation.interrupted}, return_when=asyncio.FIRST_COMPLETED)
            # Preserve the cancellation/re-bind/deadline reason even if abort also woke the send.
            self._check()
            sending.result()
        except BaseException as error:
            self._track._cancel_operation(self._operation, error)
            raise
        finally:
            timer.cancel()
            if not sending.done():
                sending.cancel()

    def abort(self, reason: BaseException | None = None) -> None:
        """Reset the group's stream; objects already sent may be incomplete at the peer."""
        self._track._cancel_operation(self._operation, reason or RelayClosedError("Group was aborted"))


class RelayTrackWriter:
    """One registered track on a publisher connection. Create one with :meth:`RelayPublisher.publish`."""

    __slots__ = (
        "_active",
        "_alias",
        "_ended",
        "_lifetime",
        "_next_group_id",
        "_publisher",
        "_registered",
        "_session",
        "_watchers",
        "_write_timeout",
        "name",
    )

    def __init__(
        self,
        publisher: RelayPublisher,
        lifetime: RelaySessionLifetime[MoqtPublishSession],
        registered: RegisteredTrack,
        write_timeout: float,
    ) -> None:
        self.name = registered.track_name
        self._publisher = publisher
        self._lifetime = lifetime
        self._session = lifetime.session
        self._registered = registered
        self._write_timeout = write_timeout
        self._alias: int | None = None
        self._ended = False
        self._next_group_id = 0
        self._active: _Operation | None = None
        self._watchers: set[asyncio.Queue[bool | None]] = set()

    # ------------------------------------------------------------------ binding

    @property
    def bound(self) -> bool:
        """True while the relay has a live subscription for this track, so writes can flow at once.

        Always ``False`` once the track has ended: no write can flow again, whatever the relay asks
        for. Read ``closed`` to tell a track that ended apart from one nobody is subscribed to.
        """
        return self._alias is not None and not self._ended

    @property
    def closed(self) -> bool:
        """True once the application closed the track or the publisher connection ended.

        Individual write failures reset their stream and leave the track open.
        """
        return self._ended

    def bindings(self) -> AsyncIterator[bool]:
        """Iterate the track's demand state: the current value first, then every change.

        Yields ``True`` when the relay (re)subscribes and ``False`` when its subscription ends, and
        stops once the track ends, however it ended. A replacement subscription yields ``True``
        again, so a publisher that re-sends state on every subscribe (a catalog) can key off each
        ``True``.
        """
        queue: asyncio.Queue[bool | None] = asyncio.Queue()
        queue.put_nowait(None if self._ended else self.bound)
        self._watchers.add(queue)
        return self._iterate_bindings(queue)

    async def _iterate_bindings(self, queue: asyncio.Queue[bool | None]) -> AsyncIterator[bool]:
        try:
            while True:
                item = await queue.get()

                if item is None:
                    return

                yield item
        finally:
            self._watchers.discard(queue)

    def _notify_watchers(self, value: bool | None) -> None:
        for queue in list(self._watchers):
            queue.put_nowait(value)

    def _bind(self, next_alias: int | None) -> None:
        active = self._active

        if active is not None and active.alias is not None and active.alias != next_alias:
            error = RelayError("Relay subscription changed during a write or group; objects may be incomplete")
            self._cancel_operation(active, error)

        changed = self._alias != next_alias
        self._alias = next_alias

        if next_alias is not None and self._active is not None:
            self._active.wake()

        if changed and not self._ended:
            self._notify_watchers(next_alias is not None)

    def _cancel_operation(self, operation: _Operation, error: BaseException) -> None:
        if operation.cancelled:
            return

        operation.cancelled = True
        operation.failure = error

        if operation.writer is not None:
            operation.writer.abort(error)

        # Reset only this group's stream. The next explicit write uses a fresh group ID;
        # bytes already delivered are not withdrawn and the SDK never retries them.

        # Release an outstanding wait for relay demand as well as the public future.
        operation.wake()

        if not operation.interrupted.done():
            operation.interrupted.set_exception(error)

        if self._active is operation:
            self._active = None

    def _end_track(self) -> None:
        """Retire the track once: refuse later writes, end the registration, and tell the watchers.

        Every path that ends a track goes through here, so a ``bindings()`` consumer always learns
        the track is over whether the application closed it or the publisher connection ended.
        """
        if self._ended:
            return

        self._ended = True
        self._registered.end()
        self._notify_watchers(None)

    # ------------------------------------------------------------------ public API

    def close(self, error: BaseException | None = None) -> None:
        """End the track and cancel an outstanding write or group. The publisher connection stays open."""
        if self._ended:
            return

        if self._active is not None:
            self._cancel_operation(self._active, error or RelayClosedError("Published track is closed"))

        self._end_track()

    def _assert_writable(self) -> None:
        """What a new write or group needs: an open connection, a live track, and a free slot."""
        self._lifetime.assert_open()

        if self._ended or self._publisher._closing:
            raise RelayClosedError("Published track is closed")

        if self._active is not None:
            raise RelayWritePendingError("A write or group is already pending; await it before writing another object")

    def _begin(self, kind: Literal["write", "group"]) -> _Operation:
        self._assert_writable()
        loop = asyncio.get_running_loop()
        operation = _Operation(kind=kind, demand=loop.create_future(), interrupted=loop.create_future())
        observe(operation.interrupted)
        self._active = operation
        return operation

    def _arm_timeout(self, operation: _Operation) -> asyncio.TimerHandle:
        def expire() -> None:
            error: RelayError
            if operation.alias is None and self._session.namespace_solicited is False:
                error = RelayNamespaceError(
                    "Publisher namespace is not covered by the relay namespace solicitations; "
                    "check the token and namespace"
                )
            else:
                error = RelayTimeoutError("Publish write timed out waiting for relay demand or transport")
            self._cancel_operation(operation, error)

        return call_later(self._write_timeout, expire)

    async def write_text(self, text: str, *, properties: Sequence[KeyValuePair] | None = None) -> None:
        """Encode UTF-8 text and write one object with the same semantics as ``write``."""
        await self.write(_encode_text(text), properties=properties)

    async def write_json(self, value: object, *, properties: Sequence[KeyValuePair] | None = None) -> None:
        """Serialize JSON and write one object; serialization errors leave the writer usable."""
        await self.write(_encode_json(value), properties=properties)

    async def write(self, payload: bytes, *, properties: Sequence[KeyValuePair] | None = None) -> None:
        """Write one independent object/group.

        Waits for relay demand. Returns when the transport accepts the complete group, not when a
        downstream viewer acknowledges it. Only one write or group may be pending; await it before
        writing again. Bytes are copied. Failure resets this group and leaves the track usable
        while the connection is open. Data may already have reached the relay; choose the next
        object explicitly (no automatic retry).
        Properties are encoded as supplied; the relay may transform them before delivery.
        """
        self._assert_writable()

        if len(payload) > MAX_OBJECT_PAYLOAD_LENGTH:
            raise ValueError("Payload exceeds the 16 MiB object limit")

        # Copy the arguments before taking the track's single in-flight slot: a payload or property
        # that cannot be converted must not leave the slot held for the life of the connection.
        data = bytes(payload)
        object_properties = _copy_properties(properties)

        operation = self._begin("write")
        timer = self._arm_timeout(operation)

        try:
            sending = spawn(self._send(operation, data, object_properties))
            await asyncio.wait({sending, operation.interrupted}, return_when=asyncio.FIRST_COMPLETED)

            # Cancellation wins even when the transport finishes in the same event-loop turn.
            self._check(operation)
            sending.result()
        except BaseException as error:
            self._cancel_operation(operation, error)
            raise
        finally:
            timer.cancel()

            if self._active is operation:
                self._active = None

    async def open_group(self) -> RelayGroupWriter:
        """Open a multi-object group: one stream carrying objects 0, 1, 2, ... until ``close()``.

        Waits for relay demand and the stream open within ``write_timeout``. Only one write or group
        may be pending per track. If the relay's subscription changes or ends while the group is
        open, the group is reset and its writer raises; the track stays open, so start the next group
        at the next random-access point.
        """
        operation = self._begin("group")
        timer = self._arm_timeout(operation)

        try:
            opening = spawn(self._open_group_stream(operation))
            await asyncio.wait({opening, operation.interrupted}, return_when=asyncio.FIRST_COMPLETED)

            if opening.done():
                writer = opening.result()
            else:
                operation.interrupted.result()
                raise AssertionError("unreachable: an interrupted group always carries an error")
        except BaseException as error:
            self._cancel_operation(operation, error)

            if self._active is operation:
                self._active = None

            raise
        finally:
            timer.cancel()

        return RelayGroupWriter(self, operation, writer)

    # ------------------------------------------------------------------ internals

    def _check(self, operation: _Operation) -> None:
        if operation.cancelled:
            assert operation.failure is not None
            raise operation.failure

        self._lifetime.assert_open()

    async def _await_demand(self, operation: _Operation) -> int:
        # Demand can disappear between its notification and this continuation.
        while self._alias is None:
            await operation.demand
            self._check(operation)

            if self._alias is None:
                # Demand came and went before the write could start. The track itself is still open,
                # so this is a retryable failure, not a closed track.
                raise RelayError("Relay subscription ended before the write started")

        self._check(operation)
        return self._alias

    async def _open_stream(self, operation: _Operation) -> SubgroupWriter:
        alias = await self._await_demand(operation)
        operation.alias = alias
        group_id = self._next_group_id
        self._next_group_id += 1
        stream = await self._session.open_uni_stream()

        if operation.cancelled:
            stream.abort(operation.failure)
            self._check(operation)

        writer = create_subgroup_writer(stream, track_alias=alias, group_id=group_id, has_properties=True)
        operation.writer = writer
        return writer

    async def _send(self, operation: _Operation, data: bytes, properties: list[KeyValuePair]) -> None:
        writer = await self._open_stream(operation)
        await writer.write_object(object_id=0, properties=properties, payload=data)
        self._check(operation)
        await writer.fin()
        self._check(operation)

    async def _open_group_stream(self, operation: _Operation) -> SubgroupWriter:
        writer = await self._open_stream(operation)
        self._check(operation)
        return writer


class RelayPublisher:
    """A dedicated publisher connection serving one namespace. Create one with :func:`connect_relay_publisher`."""

    __slots__ = ("_closed", "_closing", "_lifetime", "_namespace", "_tracks")

    def __init__(self, lifetime: RelaySessionLifetime[MoqtPublishSession], namespace: list[str]) -> None:
        self._lifetime = lifetime
        self._namespace = namespace
        self._tracks: dict[str, RelayTrackWriter] = {}
        self._closing = False
        self._closed: asyncio.Future[Closed] = asyncio.get_running_loop().create_future()

        lifetime.session.announce(namespace)
        lifetime.closed.add_done_callback(self._on_lifetime_closed)
        spawn(self._await_transport_closed())

    def _on_lifetime_closed(self, done: asyncio.Future[Closed]) -> None:
        self._closing = True
        error = done.result().error

        for track in list(self._tracks.values()):
            track.close(error)

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
    def closed(self) -> asyncio.Future[Closed]:
        """Resolves when the connection shuts down. Errors are returned as data; this never raises."""
        return self._closed

    def publish(
        self,
        track: str,
        *,
        track_properties: Sequence[KeyValuePair] | None = None,
        write_timeout: float = DEFAULT_WRITE_TIMEOUT,
    ) -> RelayTrackWriter:
        """Register a data track without starting capture or publishing a media catalog.

        ``track_properties`` is encoded as supplied; the SDK adds no default timescale.
        The relay may add, drop, or rewrite properties before delivery to subscribers; see README.
        ``write_timeout`` (seconds) bounds relay demand and the transport write together.
        """
        self._lifetime.assert_open()

        if self._closing:
            raise RelayClosedError("Relay publisher is closed")

        if not isinstance(track, str):
            raise TypeError("Track name must be a string")
        if len(track) == 0:
            raise TypeError("Track name must be non-empty")

        if track in self._tracks:
            raise ValueError("Track name is already registered on this publisher connection")

        if (
            not isinstance(write_timeout, int | float)
            or isinstance(write_timeout, bool)
            or write_timeout != write_timeout
            or write_timeout <= 0
            or write_timeout > MAX_WRITE_TIMEOUT
        ):
            raise ValueError(f"write_timeout must be between 0 (exclusive) and {MAX_WRITE_TIMEOUT} seconds")

        registered = self._lifetime.session.register_track(
            RegisterTrackOptions(
                track_namespace=self._namespace, track_name=track, track_properties=_copy_properties(track_properties)
            )
        )
        writer = RelayTrackWriter(self, self._lifetime, registered, write_timeout)
        self._tracks[track] = writer
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

    Subscribe clients use :func:`connect_relay`; publishing does not share their transport. One
    publisher connection serves one ``namespace`` and any number of tracks. A relay must solicit the
    namespace and subscribe upstream before data can flow.
    """
    fields = list(namespace)

    if len(fields) == 0 or any(not isinstance(part, str) or len(part) == 0 for part in fields):
        raise TypeError("A publisher namespace must contain non-empty string fields")
    if len(fields) > MAX_NAMESPACE_FIELDS:
        raise ValueError(f"A publisher namespace must contain at most {MAX_NAMESPACE_FIELDS} fields")

    options = ConnectRelayOptions(
        url=url,
        get_token=get_token,
        cancellation=cancellation,
        connect_timeout=connect_timeout,
        create_transport=create_transport,
    )
    publisher_ref: list[RelayPublisher] = []

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
                    on_track_binding=on_track_binding,
                    on_announce_ended=lambda ended: callbacks.on_closed(
                        Closed(ended.error if ended.error is not None else RelayError("Relay withdrew the namespace"))
                    ),
                ),
            ),
        )

    lifetime = await open_relay_session(options, create_session)
    publisher = RelayPublisher(lifetime, fields)
    publisher_ref.append(publisher)
    return publisher


__all__ = [
    "DEFAULT_WRITE_TIMEOUT",
    "RelayGroupWriter",
    "RelayPublisher",
    "RelayTrackWriter",
    "connect_relay_publisher",
]
