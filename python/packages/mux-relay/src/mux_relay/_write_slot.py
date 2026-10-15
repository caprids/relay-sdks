"""The write slot: a track's single place for a pending operation (see ``CONTEXT.md``).

One slot per registered track. It owns the relay's demand binding for the track, the one active
operation (a single write or an open group) and its phases, the write deadline, cancellation, and
the demand observers behind ``has_demand`` and ``demand_changes()``. Its public face is
:class:`RelayTrackWriter`, the class the application holds; the publisher connection drives it
through two inbound calls, ``_bind()`` when the relay's binding changes and ``_disconnect()`` when the
connection ends.

An operation completes once the transport has taken its group's FIN, not once the relay has
acknowledged it: the slot frees itself and passes the group on to the connection as a settling group
(see ``CONTEXT.md`` and ``_settling_groups.py``). Only the group's own delivery timeout resets a
settling group, and nothing reports how it ends.

The slot enforces both delivery timeouts of draft-ietf-moq-transport-22 section 5.2 (see
``CONTEXT.md``). Each group's values are the smaller non-zero of the publisher's (the 0x02 or 0x06
property on the group's first object, else the track property) and the subscriber's (the relay
subscription's parameters, as updated by REQUEST_UPDATE). The subscriber's are fixed when the group's
stream opens, the publisher's when its first object is handed over. The object timeout bounds an
object's age from the application's call until the transport has taken it, every wait included:
it is checked just before the object is written and runs on until the stream write returns, which
is when aioquic has transmitted the bytes. Until the subscriber's values are fixed only the
publisher's time the wait; at that point an object already older than the fixed value fails, before
any stream opens. An older object fails its operation with
``DELIVERY_TIMEOUT`` and resets the group with DELIVERY_TIMEOUT (0x2). The subgroup timeout starts
when the FIN is handed over and resets the settling group, silently, if the relay has not
acknowledged the FIN by then.

Everything the slot borrows enters through :class:`WriteSlotDependencies`: how to open a subgroup
stream, the connection's settling groups, whether the namespace is solicited, whether the connection
is still open, how to end the track in the core, and the track's properties. Tests drive a slot with
an in-memory subgroup writer; the "Write phases and interruptions" table in ``spec/behavior.md`` is
the list they cover.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any, Literal, Protocol, TypeVar

from moq_core import KeyValuePair, ObjectStatus
from moq_core.control_messages import STREAM_RESET_CODE

from ._settling_groups import SettlingGroups
from ._validation import object_property_pairs, payload_bytes
from .errors import (
    RelayError,
    RelayErrorPhase,
    _stream_stopped,
    _transport_error,
)
from .payload import _encode_json, _encode_text

T = TypeVar("T")

MAX_DELIVERY_TIMEOUT_MS = 2_147_483_647
"""The largest delivery timeout honoured, in milliseconds; a larger value is clamped to it.

The SDKs' common timer bound (see Deadline in ``spec/behavior.md``)."""

OBJECT_DELIVERY_TIMEOUT = 0x02
"""The OBJECT_DELIVERY_TIMEOUT property type (draft-ietf-moq-transport-22 section 10.2)."""

SUBGROUP_DELIVERY_TIMEOUT = 0x06
"""The SUBGROUP_DELIVERY_TIMEOUT property type (draft-ietf-moq-transport-22 section 10.1)."""


@dataclass(frozen=True, slots=True)
class _Timeouts:
    """One side's delivery timeouts in milliseconds; ``None`` where that side set none."""

    object: int | None = None
    subgroup: int | None = None


_NO_TIMEOUTS = _Timeouts()


def _property_timeout(properties: Sequence[KeyValuePair], kind: int) -> int | None:
    """The first ``kind`` property as a timeout, or ``None``: absent, or not an integer (ignored for timing)."""
    for pair in properties:
        if pair.type == kind:
            value = pair.value
            return value if isinstance(value, int) and not isinstance(value, bool) else None

    return None


def _publisher_timeouts(properties: Sequence[KeyValuePair], fallback: _Timeouts) -> _Timeouts:
    """The publisher's values for a group: its first object's properties, else ``fallback`` (the track's)."""
    object_timeout = _property_timeout(properties, OBJECT_DELIVERY_TIMEOUT)
    subgroup_timeout = _property_timeout(properties, SUBGROUP_DELIVERY_TIMEOUT)
    return _Timeouts(
        object=fallback.object if object_timeout is None else object_timeout,
        subgroup=fallback.subgroup if subgroup_timeout is None else subgroup_timeout,
    )


def _effective_seconds(publisher: int | None, subscriber: int | None) -> float:
    """§5.2: the smaller of the non-zero values, clamped, in seconds; 0 when neither sets a timeout."""
    values = [value for value in (publisher, subscriber) if value]
    return min(*values, MAX_DELIVERY_TIMEOUT_MS) / 1000 if values else 0.0


def _observe(future: asyncio.Future[Any]) -> None:
    """Mark a future's eventual exception as handled without acting on it."""

    def finished(done: asyncio.Future[Any]) -> None:
        if not done.cancelled():
            done.exception()

    future.add_done_callback(finished)


class StreamReset(Exception):
    """The reason handed to a stream reset.

    ``stream_error_code`` is the MOQT reset code the transport sends (draft-ietf-moq-transport-22
    §12.5, registry §16.11.4); the aioquic transport reads it, and in-memory test transports hand the
    reason to the receiver as the reset error. ``__cause__`` is the failure the caller saw.
    """

    def __init__(self, stream_error_code: int, cause: BaseException | None) -> None:
        message = str(cause) if cause is not None and str(cause) else "Publish operation reset"
        super().__init__(message)
        self.stream_error_code = int(stream_error_code)
        self.__cause__ = cause


class SubgroupStream(Protocol):
    """What the slot needs from one subgroup stream: the shape of ``moq_core.SubgroupWriter``."""

    group_id: int

    async def write_object(
        self,
        *,
        object_id: int,
        payload: bytes,
        properties: Sequence[KeyValuePair] | None = None,
        status: ObjectStatus = "normal",
    ) -> None: ...

    async def fin(self) -> None: ...

    async def wait_acknowledged(self) -> None: ...

    def abort(self, reason: object = None) -> None: ...


def _connection_reset_code(error: BaseException | None) -> int:
    """The code for a reset caused by the connection ending."""
    if getattr(error, "code", None) == "GOAWAY":
        return STREAM_RESET_CODE.GOING_AWAY
    return STREAM_RESET_CODE.SESSION_CLOSED


@dataclass(frozen=True, slots=True)
class WriteSlotDependencies:
    """What a track's write slot borrows from the publisher connection and the core session."""

    open_subgroup: Callable[[int, int], Awaitable[SubgroupStream]]
    """Open a subgroup stream for one group of this track, given the alias and group ID."""
    settling_groups: SettlingGroups
    """The connection's settling groups: an operation waits for room before opening its stream."""
    namespace_solicited: Callable[[], bool | None]
    """Whether a current relay solicitation covers the namespace; ``None`` before any arrived."""
    assert_open: Callable[[], None]
    """Raise the connection's closing error once the connection has ended."""
    end_track: Callable[[], None]
    """Tell the core the track is done, so it FINs any live subscription stream."""
    write_timeout: float
    """Deadline in seconds for waiting for demand and for each stream step."""
    track_properties: Sequence[KeyValuePair] = ()
    """The track's properties as registered: its 0x02 and 0x06 are the publisher's delivery timeouts."""
    clock: Callable[[], float] | None = None
    """Seconds on a monotonic clock, for an object's age; the event loop's clock when ``None``."""


class _Operation:
    """One pending write or open group occupying a track's write slot (see ``CONTEXT.md``).

    The operation owns its own deadline, cancellation and step racing, so the group writer that
    holds it never reaches into the track writer.
    """

    __slots__ = (
        "_object_properties",
        "_object_started",
        "_object_timer",
        "_slot",
        "alias",
        "cancelled",
        "code",
        "demand",
        "failure",
        "interrupted",
        "kind",
        "phase",
        "publisher",
        "subscriber",
        "writer",
    )

    def __init__(self, slot: RelayTrackWriter, kind: Literal["write", "group"]) -> None:
        loop = asyncio.get_running_loop()
        self._slot = slot
        self.kind = kind
        self.phase: RelayErrorPhase = "waiting-for-demand"
        self.alias: int | None = None
        self.writer: SubgroupStream | None = None
        self.cancelled = False
        self.failure: BaseException | None = None
        self.code: int = STREAM_RESET_CODE.INTERNAL_ERROR
        self.demand: asyncio.Future[None] = loop.create_future()
        self.interrupted: asyncio.Future[None] = loop.create_future()
        _observe(self.interrupted)
        self.subscriber: _Timeouts | None = None
        """The relay subscription's delivery timeouts, fixed when the group's stream opens."""
        self.publisher: _Timeouts | None = None
        """The publisher's delivery timeouts, fixed when the group's first object is handed over."""
        self._object_started: float | None = None
        """When the application handed over the object now on its way, by the slot's clock."""
        self._object_properties: Sequence[KeyValuePair] = ()
        self._object_timer: asyncio.TimerHandle | None = None

    def wake(self) -> None:
        if not self.demand.done():
            self.demand.set_result(None)

    def cancel(self, error: BaseException, code: int) -> None:
        """End the operation with ``error``; a stream it opened is reset with ``code``."""
        if self.cancelled:
            return

        self.cancelled = True
        self.failure = error
        self.code = code
        self._stop_object_clock()

        if self.writer is not None:
            # Reset only this group's stream. The next explicit write uses a fresh group ID;
            # bytes already delivered are not withdrawn and the SDK never retries them.
            self.writer.abort(StreamReset(code, error))

        self.release()
        # Release an outstanding wait for relay demand as well as the public future.
        self.wake()

        if not self.interrupted.done():
            self.interrupted.set_exception(error)

    def release(self) -> None:
        """Free the slot if this operation still holds it."""
        if self._slot._active is self:
            self._slot._active = None

    def complete(self, writer: SubgroupStream) -> None:
        """The FIN was handed over: the group settles on the connection's account and the slot is free.

        Its SUBGROUP_DELIVERY_TIMEOUT starts now (§5.2). If the relay has not acknowledged the FIN when
        it expires, the stream is reset with DELIVERY_TIMEOUT, and nothing is reported: the group
        leaves the settling set as a late failure.
        """
        publisher = self.publisher if self.publisher is not None else self._slot._track_timeouts
        subscriber = self.subscriber if self.subscriber is not None else _NO_TIMEOUTS
        timeout = _effective_seconds(publisher.subgroup, subscriber.subgroup)

        def expire() -> None:
            writer.abort(StreamReset(STREAM_RESET_CODE.DELIVERY_TIMEOUT, None))

        self._slot._deps.settling_groups.add(
            writer.wait_acknowledged(), timeout=timeout or None, on_timeout=expire if timeout else None
        )
        self.release()

    # ------------------------------------------------------------------ OBJECT_DELIVERY_TIMEOUT (§5.2)

    def start_object(self, started: float, properties: Sequence[KeyValuePair]) -> None:
        """The application handed over an object at ``started``: its age counts from then, every wait included."""
        self._object_started = started
        self._object_properties = properties
        self._arm_object_timer()

    def fix_subscriber(self, subscriber: _Timeouts) -> None:
        """Just before the stream opens: fix the subscriber's values and check the object against the result.

        Until now only the publisher's value timed the wait. An object already older than the fixed
        effective value fails here, in ``opening-stream``, before any stream opens; otherwise its
        timer is re-armed for what remains.
        """
        self.subscriber = subscriber

        if self._object_started is None:
            return

        limit = self._object_limit()

        if limit and self._slot._clock() - self._object_started >= limit:
            self._fail_delivery()
        else:
            self._arm_object_timer()

        self.check()

    def check_object_age(self) -> None:
        """The object is about to go to the transport: the §5.2 check, in case its timer is late.

        The group's first object fixes the publisher's values. An object older than the effective
        OBJECT_DELIVERY_TIMEOUT fails the operation with ``DELIVERY_TIMEOUT`` and resets its stream.
        Its timer keeps running until :meth:`object_taken`.
        """
        if self.publisher is None:
            self.publisher = self._publisher_now()

        started = self._object_started

        if started is not None:
            limit = self._object_limit()

            if limit and self._slot._clock() - started >= limit:
                self._fail_delivery()

        self.check()

    def object_taken(self) -> None:
        """The transport took the object: its stream write returned, so aioquic transmitted it. No longer timed."""
        self._stop_object_clock()

    def _publisher_now(self) -> _Timeouts:
        if self.publisher is not None:
            return self.publisher

        return _publisher_timeouts(self._object_properties, self._slot._track_timeouts)

    def _object_limit(self) -> float:
        """The object's timeout in seconds, 0 for none: the publisher's alone until the subscriber's is fixed."""
        subscriber = self.subscriber if self.subscriber is not None else _NO_TIMEOUTS
        return _effective_seconds(self._publisher_now().object, subscriber.object)

    def _arm_object_timer(self) -> None:
        if self._object_timer is not None:
            self._object_timer.cancel()
            self._object_timer = None

        if self._object_started is None or self.cancelled:
            return

        limit = self._object_limit()

        if limit:
            remaining = self._object_started + limit - self._slot._clock()
            self._object_timer = asyncio.get_running_loop().call_later(max(remaining, 0.0), self._object_expired)

    def _object_expired(self) -> None:
        self._object_timer = None
        started = self._object_started
        limit = self._object_limit()

        if started is not None and limit and self._slot._clock() - started >= limit:
            self._fail_delivery()
        else:
            self._arm_object_timer()

    def _stop_object_clock(self) -> None:
        self._object_started = None

        if self._object_timer is not None:
            self._object_timer.cancel()
            self._object_timer = None

    def _fail_delivery(self) -> None:
        error = RelayError(
            "Object exceeded its delivery timeout before it reached the transport; the data is stale",
            code="DELIVERY_TIMEOUT",
            phase=self.phase,
        )
        self.cancel(error, STREAM_RESET_CODE.DELIVERY_TIMEOUT)

    def check(self) -> None:
        """Raise this operation's failure, or the connection's, before or after a step."""
        if self.cancelled:
            assert self.failure is not None
            raise self.failure

        self._slot._deps.assert_open()

    async def run(self, send: Callable[[], Awaitable[T]]) -> T:
        """Race one step against this operation's deadline and interruptions.

        Cancellation and closure win over a transport result that lands in the same event-loop
        turn, and the failure the caller sees keeps its identity: task cancellation stays a
        ``CancelledError``, a connection failure stays the connection's error, a peer stop of the
        operation's own stream becomes ``SUBSCRIPTION_CHANGED``, and only another bare transport
        exception is wrapped as ``TRANSPORT_ERROR``.
        """
        timer = asyncio.get_running_loop().call_later(self._slot._deps.write_timeout, self._expire)
        sending = asyncio.ensure_future(send())
        _observe(sending)

        # The step is never cancelled from here: a stream open that completes after the failure
        # must still arrive so `_open_stream` can reset it, and an aborted writer fails a pending
        # write or FIN on its own.
        try:
            await asyncio.wait({sending, self.interrupted}, return_when=asyncio.FIRST_COMPLETED)
            self.check()
            return sending.result()
        except BaseException as error:
            failure, code = self._classify(error)
            self.cancel(failure, code)
            if failure is error:
                raise
            raise failure from failure.__cause__
        finally:
            timer.cancel()

    def _classify(self, error: BaseException) -> tuple[BaseException, int]:
        # Preserve asyncio task cancellation even when another failure arrives in the same turn.
        if not isinstance(error, Exception):
            return error, STREAM_RESET_CODE.CANCELLED
        try:
            # Closure can precede the callback that cancels this track. The public operation
            # may resume first, so consult the lifetime before classifying a transport failure.
            self.check()
        except BaseException as interrupted:
            code = self.code if self.cancelled else _connection_reset_code(interrupted)
            return interrupted, code
        if self.writer is not None and _stream_stopped(error):
            # The relay stopped this operation's own stream: the group is no longer of interest
            # (§11.3.2), whether or not its subscription is ending too. The binding alone decides
            # whether the next write waits for demand; a later binding change finds no operation.
            stopped = RelayError(
                "Relay stopped the stream during a write or group; objects may be incomplete",
                code="SUBSCRIPTION_CHANGED",
                phase=self.phase,
            )
            stopped.__cause__ = error
            return stopped, STREAM_RESET_CODE.CANCELLED
        return _transport_error(error, self.phase), STREAM_RESET_CODE.INTERNAL_ERROR

    def _expire(self) -> None:
        error: RelayError
        if self.phase == "waiting-for-demand" and self._slot._deps.namespace_solicited() is False:
            error = RelayError(
                "Publisher namespace is not covered by the relay namespace solicitations; "
                "check the token and namespace",
                code="NAMESPACE_NOT_SOLICITED",
                phase=self.phase,
            )
        else:
            error = RelayError(
                "Publish write timed out waiting for relay demand or transport", code="WRITE_TIMEOUT", phase=self.phase
            )
        self.cancel(error, STREAM_RESET_CODE.DELIVERY_TIMEOUT)


class RelayGroupWriter:
    """One open multi-object group on a track. Create one with :meth:`RelayTrackWriter.open_group`.

    Objects are appended in order with increasing object IDs starting at 0, so the first object is
    the group's random-access point in LOC/MSF terms. ``close()`` FINs the stream, which is how a
    complete group is announced; ``abort()`` resets it unless it already closed cleanly. A group that
    fails part-way is reset, and the track stays open for the next group.
    """

    __slots__ = ("_complete", "_next_object_id", "_operation", "_pending", "_writer", "group_id")

    def __init__(self, operation: _Operation, writer: SubgroupStream, *, object_count: int = 0) -> None:
        self._operation = operation
        self._writer = writer
        self._next_object_id = object_count
        self._pending = False
        self._complete = False
        self.group_id = writer.group_id

    @property
    def closed(self) -> bool:
        """True once the group was FINed or reset; no further objects can be written."""
        return self._complete or self._operation.cancelled

    @property
    def object_count(self) -> int:
        """Objects accepted by the transport so far."""
        return self._next_object_id

    def _check(self) -> None:
        self._operation.check()

        if self._complete:
            # The FIN already went out, so nothing of the refused call is sent.
            raise RelayError("Group is closed", code="CLOSED", phase="waiting-for-demand")

        if self._pending:
            raise RelayError("A group write or close is already pending", code="WRITE_PENDING", phase="writing")

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
        On the group's first object, a 0x02 or 0x06 property sets the group's delivery timeout
        instead of the track's; on a later object it is only sent. An object older than the
        group's OBJECT_DELIVERY_TIMEOUT when it reaches the transport raises ``DELIVERY_TIMEOUT``
        and resets the group.
        """
        started = self._operation._slot._clock()
        # Arguments before group state: a bad payload is a programming error whatever the group holds.
        data = payload_bytes(payload)
        object_properties = object_property_pairs(properties)

        if not isinstance(status, str):
            raise TypeError("Object status must be a string")

        if status not in ("normal", "end-of-group", "end-of-track"):
            raise ValueError("Unknown object status")

        self._check()
        object_id = self._next_object_id
        operation = self._operation

        async def send() -> None:
            operation.check_object_age()
            await self._writer.write_object(
                object_id=object_id, payload=data, properties=object_properties, status=status
            )
            operation.object_taken()

        self._pending = True
        try:
            operation.phase = "writing"
            operation.start_object(started, object_properties)
            await operation.run(send)
            self._next_object_id += 1
        finally:
            self._pending = False

    async def close(self) -> None:
        """FIN within ``write_timeout``; expiry resets the group. Idempotent once closed cleanly."""
        if self._complete:
            return

        self._check()
        self._pending = True
        try:
            self._operation.phase = "closing"
            await self._operation.run(self._writer.fin)
            self._complete = True
            self._operation.complete(self._writer)
        finally:
            self._pending = False

    def abort(self, reason: BaseException | None = None) -> None:
        """Reset the group's stream; objects already sent may be incomplete at the peer.

        After a clean ``close()`` this has no effect: the FIN already ended the stream, and a reset
        after complete delivery is ignorable (draft-ietf-moq-transport-22 section 11.3.2).
        """
        if self._complete:
            return

        self._operation.cancel(
            reason
            if reason is not None
            else RelayError("Group was aborted", code="CLOSED", phase=self._operation.phase),
            STREAM_RESET_CODE.CANCELLED,
        )


class RelayTrackWriter:
    """One registered track on a publisher connection. Create one with :meth:`RelayPublisher.register_track`.

    Registration sends nothing; :meth:`write` and :meth:`open_group` send, after waiting for relay demand.
    The behaviour behind this class is the track's write slot (see ``CONTEXT.md`` and the "Write
    phases and interruptions" table in ``spec/behavior.md``).
    """

    __slots__ = (
        "_active",
        "_alias",
        "_clock",
        "_deps",
        "_ended",
        "_next_group_id",
        "_subscriber_timeouts",
        "_track_timeouts",
        "_watchers",
        "name",
    )

    def __init__(self, name: str, dependencies: WriteSlotDependencies) -> None:
        self.name = name
        self._deps = dependencies
        loop = asyncio.get_running_loop()
        self._clock: Callable[[], float] = dependencies.clock if dependencies.clock is not None else loop.time
        self._track_timeouts = _publisher_timeouts(dependencies.track_properties, _NO_TIMEOUTS)
        """The publisher's delivery timeouts from the track properties."""
        self._subscriber_timeouts = _NO_TIMEOUTS
        """The current relay subscription's delivery timeouts; none while there is no binding."""
        self._alias: int | None = None
        self._ended = False
        self._next_group_id = 0
        self._active: _Operation | None = None
        self._watchers: set[asyncio.Queue[bool | None]] = set()

    # ------------------------------------------------------------------ demand

    @property
    def has_demand(self) -> bool:
        """Whether the relay currently has an upstream subscription for this track, so a write would proceed at once.

        Demand is the relay's own subscription, shared by every downstream viewer and ended when
        the relay chooses: it is neither a viewer count nor a liveness signal.
        Always ``False`` once the track has ended, whatever the relay asks for; read ``closed`` to
        tell a track that ended apart from one the relay has not subscribed to.
        """
        return self._alias is not None and not self._ended

    @property
    def closed(self) -> bool:
        """True once the application closed the track or the publisher connection ended.

        Individual write failures reset their stream and leave the track open.
        """
        return self._ended

    def demand_changes(self) -> AsyncIterator[bool]:
        """Iterate relay demand: the current state first, then one value per relay subscription change.

        Yields ``True`` when the relay subscribes or replaces its subscription (so ``True`` can
        repeat) and ``False`` when its subscription ends, and stops once the track ends, however it
        ended. This is how a catalog re-sends its document on every relay subscription. Values are
        per relay subscription, never per downstream viewer.
        """
        queue: asyncio.Queue[bool | None] = asyncio.Queue()
        queue.put_nowait(None if self._ended else self.has_demand)
        self._watchers.add(queue)
        return self._iterate_demand(queue)

    async def _iterate_demand(self, queue: asyncio.Queue[bool | None]) -> AsyncIterator[bool]:
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

    # ------------------------------------------------------------------ inbound from the publisher

    def _bind(
        self,
        next_alias: int | None,
        object_delivery_timeout: int | None = None,
        subgroup_delivery_timeout: int | None = None,
    ) -> None:
        """The relay's binding for this track changed: a new alias, or ``None`` when its subscription ended.

        The two timeouts are the bound subscription's values in milliseconds (``None`` where it sent
        none); a REQUEST_UPDATE that changes them arrives as a binding with the same alias. They apply
        to groups whose streams open from now on.
        """
        active = self._active

        if active is not None and active.alias is not None and active.alias != next_alias:
            error = RelayError(
                "Relay subscription changed during a write or group; objects may be incomplete",
                code="SUBSCRIPTION_CHANGED",
                phase=active.phase,
            )
            active.cancel(error, STREAM_RESET_CODE.CANCELLED)

        changed = self._alias != next_alias
        self._alias = next_alias
        self._subscriber_timeouts = (
            _Timeouts(object=object_delivery_timeout, subgroup=subgroup_delivery_timeout)
            if next_alias is not None
            else _NO_TIMEOUTS
        )

        if next_alias is not None and self._active is not None:
            self._active.wake()

        if changed and not self._ended:
            self._notify_watchers(next_alias is not None)

    def _disconnect(self, error: BaseException | None) -> None:
        """The publisher connection ended; ``error`` is its failure, ``None`` after a clean shutdown."""
        self._close(error, _connection_reset_code(error))

    # ------------------------------------------------------------------ public API

    def close(self) -> None:
        """End the track and cancel an outstanding write or group. The publisher connection stays open."""
        self._close(None, STREAM_RESET_CODE.CANCELLED)

    def _close(self, error: BaseException | None, code: int) -> None:
        if self._ended:
            return

        if self._active is not None:
            self._active.cancel(
                error
                if error is not None
                else RelayError("Published track is closed", code="CLOSED", phase=self._active.phase),
                code,
            )

        self._end_track()

    def _end_track(self) -> None:
        """Retire the track once: refuse later writes, end the registration, and tell the watchers.

        Every path that ends a track goes through here, so a ``demand_changes()`` consumer always learns
        the track is over whether the application closed it or the publisher connection ended.
        """
        if self._ended:
            return

        self._ended = True
        self._deps.end_track()
        self._notify_watchers(None)

    def _assert_writable(self) -> None:
        """What a new write or group needs: an open connection, a live track, and a free slot."""
        self._deps.assert_open()

        if self._ended:
            # Refused before an operation starts: nothing of this write was sent (spec/behavior.md).
            raise RelayError("Published track is closed", code="CLOSED", phase="waiting-for-demand")

        if self._active is not None:
            raise RelayError(
                "A write or group is already pending; await it before writing another object",
                code="WRITE_PENDING",
                phase="writing",
            )

    def _begin(self, kind: Literal["write", "group"]) -> _Operation:
        self._assert_writable()
        operation = _Operation(self, kind)
        self._active = operation
        return operation

    async def write_text(self, text: str, *, properties: Sequence[KeyValuePair] | None = None) -> None:
        """Encode UTF-8 text and write one object with the same semantics as ``write``."""
        await self.write(_encode_text(text), properties=properties)

    async def write_json(self, value: object, *, properties: Sequence[KeyValuePair] | None = None) -> None:
        """Serialize JSON and write one object; serialization errors leave the writer usable."""
        await self.write(_encode_json(value), properties=properties)

    async def write(self, payload: bytes, *, properties: Sequence[KeyValuePair] | None = None) -> None:
        """Send one independent object/group.

        Waits for relay demand (see ``has_demand``). Returns when the transport accepts the complete group, not when a
        downstream viewer acknowledges it. Only one write or group may be pending; await it before
        writing again. Bytes are copied. Failure resets this group and leaves the track usable
        while the connection is open. Data may already have reached the relay; choose the next
        object explicitly (no automatic retry).
        Properties are encoded as supplied; the relay may transform them before delivery. A 0x02 or
        0x06 property sets this group's delivery timeout instead of the track's. An object older
        than its OBJECT_DELIVERY_TIMEOUT when it would reach the transport, counted from this call and
        including the wait for demand, raises ``DELIVERY_TIMEOUT``: the data is stale, so write fresh
        data rather than this object again.
        """
        await self._write_from(self._clock(), payload, properties)

    async def _write_from(
        self, started: float, payload: bytes, properties: Sequence[KeyValuePair] | None = None
    ) -> None:
        """``write()`` for an object the application handed over at ``started`` on this writer's clock.

        A wrapper that checks the arguments first (the supervised publisher) calls this, so its own work
        counts toward the object's delivery timeout (draft-ietf-moq-transport-22 section 5.2).
        """
        # Admit the arguments before consulting the track's single write slot: a payload or property
        # that cannot be converted must not leave the slot held, and a programming error is reported
        # the same way whatever the slot holds. `_begin` checks the slot.
        data = payload_bytes(payload)
        object_properties = object_property_pairs(properties)

        operation = self._begin("write")
        operation.start_object(started, object_properties)
        try:
            writer = await operation.run(lambda: self._send(operation, data, object_properties))
            operation.complete(writer)
        finally:
            operation.release()

    async def open_group(
        self, payload: bytes | None = None, *, properties: Sequence[KeyValuePair] | None = None
    ) -> RelayGroupWriter:
        """Open a multi-object group: one stream carrying objects 0, 1, 2, ... until ``close()``.

        Waits for relay demand, for room under the connection's settling-group limit and for the
        stream within ``write_timeout``. Only one write or group may be pending per track. If the
        relay's subscription changes or ends while the group is open, the group is reset and its
        writer raises; the track stays open, so start the next group at the next random-access point.

        With ``payload``, the group opens with it as object 0 and returns once the transport takes it
        (``object_count`` is 1). Its delivery timeout counts from this call, so every wait before the
        stream opens counts (draft-ietf-moq-transport-22 section 5.2), and a 0x02 or 0x06 property in
        ``properties`` sets the group's delivery timeouts. If it expires this raises
        ``DELIVERY_TIMEOUT`` and the group is reset. Without ``payload`` there is no object to time,
        so only ``write_timeout`` bounds the wait; ``properties`` without ``payload`` is a TypeError.
        """
        # With a first object, its age counts from here, waits for demand, settling room and the stream included.
        return await self._open_group_from(self._clock(), payload, properties)

    async def _open_group_from(
        self, started: float, payload: bytes | None = None, properties: Sequence[KeyValuePair] | None = None
    ) -> RelayGroupWriter:
        """``open_group()`` for a first object the application handed over at ``started`` on this writer's clock."""
        if payload is None:
            if properties is not None:
                raise TypeError("Group properties belong to a first object: pass a payload with them")

            operation = self._begin("group")
            writer = await operation.run(lambda: self._open_stream(operation))
            return RelayGroupWriter(operation, writer)

        data = payload_bytes(payload)
        object_properties = object_property_pairs(properties)
        operation = self._begin("group")
        operation.start_object(started, object_properties)
        # A failure cancels the operation, which resets any stream it opened: no writer is returned.
        writer = await operation.run(lambda: self._open_with_first(operation, data, object_properties))
        return RelayGroupWriter(operation, writer, object_count=1)

    # ------------------------------------------------------------------ steps

    async def _await_demand(self, operation: _Operation) -> int:
        # Demand can disappear between its notification and this continuation.
        while self._alias is None:
            await operation.demand
            operation.check()

            if self._alias is None:
                # Demand came and went before the write could start. The track itself is still open,
                # so report the lost binding while leaving the track open.
                raise RelayError(
                    "Relay subscription ended before the write started",
                    code="SUBSCRIPTION_CHANGED",
                    phase=operation.phase,
                )

        operation.check()
        return self._alias

    async def _open_stream(self, operation: _Operation) -> SubgroupStream:
        alias = await self._await_demand(operation)
        operation.alias = alias
        group_id = self._next_group_id
        self._next_group_id += 1
        operation.phase = "opening-stream"
        settling_groups = self._deps.settling_groups

        if not settling_groups.has_room():
            # At the settling-group limit this waits under the write deadline, which reports a stall
            # as a WRITE_TIMEOUT in this phase (or a DELIVERY_TIMEOUT, if the object's comes first).
            await settling_groups.wait_for_room(operation.interrupted)
            operation.check()

        # The group's subscriber values are those of the subscription its stream opens on (§5.2). An
        # object already older than the fixed value fails here, before any stream opens.
        operation.fix_subscriber(self._subscriber_timeouts)
        writer = await self._deps.open_subgroup(alias, group_id)

        if operation.cancelled:
            # The stream opened after the failure: reset it before any object is written (§11.3.2).
            writer.abort(StreamReset(operation.code, operation.failure))
            operation.check()

        operation.writer = writer
        return writer

    async def _open_with_first(
        self, operation: _Operation, data: bytes, properties: list[KeyValuePair]
    ) -> SubgroupStream:
        """Open the group's stream and hand its object 0 to the transport."""
        writer = await self._open_stream(operation)
        operation.phase = "writing"
        operation.check_object_age()
        await writer.write_object(object_id=0, properties=properties, payload=data)
        operation.object_taken()
        operation.check()
        return writer

    async def _send(self, operation: _Operation, data: bytes, properties: list[KeyValuePair]) -> SubgroupStream:
        writer = await self._open_with_first(operation, data, properties)
        operation.phase = "closing"
        await writer.fin()
        operation.check()
        return writer


__all__ = [
    "RelayGroupWriter",
    "RelayTrackWriter",
    "StreamReset",
    "SubgroupStream",
    "WriteSlotDependencies",
]
