"""Pure MOQT session protocol driver (moq-transport draft-20), subscribe side only.

Owns the protocol mechanics over an established transport: the SETUP exchange on paired
unidirectional control streams (§3.3), request-stream bookkeeping keyed by Request ID (§10.1: client
IDs are even, starting at 0), routing of incoming data streams to subscriptions (by Track Alias) and
fetches (by Request ID), GOAWAY handling, and session termination.

Deliberately callback-shaped. The transport is the structural subset of a WebTransport session
declared in :mod:`moq_core.transport`, so tests drive the driver with an in-memory fake and the
product package passes its aioquic adapter unchanged. Callbacks are plain synchronous callables
invoked from the driver's tasks; hand work to your own queue or task if it must not block delivery.

Version negotiation happens at connection time (ALPN / ``WT-Available-Protocols``,
``MOQT_PROTOCOL_ID``), before this driver sees the transport.

Datagram reception is not implemented: every object arrives on its own stream.

The driver must be created inside a running asyncio event loop.
"""

from __future__ import annotations

import asyncio
import inspect
import warnings
from collections.abc import Callable, Sequence
from contextlib import aclosing
from dataclasses import dataclass, field, fields
from typing import TypeAlias

from ._tasks import call_later, observe, spawn
from .bytes import StreamReader, utf8_encode
from .control_messages import (
    MESSAGE_TYPE,
    REQUEST_ERROR_CODE,
    SETUP_OPTION,
    ControlMessage,
    ControlMessageFrame,
    Fetch,
    FetchOk,
    Goaway,
    GroupOrder,
    KeyValuePair,
    MessageParameters,
    Publish,
    PublishDone,
    PublishStateNotify,
    RequestError,
    RequestOk,
    Setup,
    Subscribe,
    SubscribeOk,
    TrackStatus,
    decode_control_message,
    encode_fetch,
    encode_goaway,
    encode_request_error,
    encode_request_ok,
    encode_request_update,
    encode_setup,
    encode_subscribe,
    encode_track_status,
)
from .errors import SESSION_ERROR, MoqtProtocolError, is_moqt_protocol_error
from .object_stream import (
    STREAM_TYPE,
    FetchStreamEntry,
    MoqtObject,
    SubgroupHeader,
    is_subgroup_header_type,
    read_fetch_entries,
    read_fetch_header,
    read_subgroup_header,
    read_subgroup_objects,
)
from .request_stream import RequestStream, RequestStreamHandlers, open_request_stream
from .transport import BidirectionalStream, MoqtTransport, ReceiveStream

# ============================================================================
# Public session surface
# ============================================================================


@dataclass(frozen=True, slots=True)
class Closed:
    """How a session or connection ended. ``error`` is ``None`` for a clean close."""

    error: BaseException | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class SubgroupEnd:
    """A subgroup stream finished cleanly (FIN)."""

    group_id: int
    subgroup_id: int
    end_of_group: bool


@dataclass(frozen=True, slots=True, kw_only=True)
class SubgroupReset:
    """A subgroup stream was reset; objects from it may be missing."""

    group_id: int
    error: BaseException


@dataclass(frozen=True, slots=True, kw_only=True)
class FetchReset:
    """The fetch data stream was reset; delivered entries may be incomplete."""

    error: BaseException


@dataclass(frozen=True, slots=True, kw_only=True)
class TrackStatusOk:
    parameters: MessageParameters
    track_properties: list[KeyValuePair]


@dataclass(slots=True, kw_only=True)
class SubscriptionHandlers:
    on_ok: Callable[[SubscribeOk], None] | None = None
    on_error: Callable[[RequestError], None] | None = None
    on_object: Callable[[MoqtObject], None] | None = None
    on_subgroup_end: Callable[[SubgroupEnd], None] | None = None
    on_subgroup_reset: Callable[[SubgroupReset], None] | None = None
    on_done: Callable[[PublishDone], None] | None = None
    on_update_ok: Callable[[], None] | None = None
    """Response to a REQUEST_UPDATE sent on this subscription."""
    on_state_notify: Callable[[MessageParameters], None] | None = None
    """PUBLISH_STATE_NOTIFY (§10.10): the publisher changed subscription state on its own.

    The parameters carry only what changed, with LARGEST_OBJECT marking where. Informational; the
    session sends no reply.
    """
    on_goaway: Callable[[Goaway], None] | None = None
    """GOAWAY on this request stream: re-issue the request (possibly elsewhere)."""


@dataclass(slots=True, kw_only=True)
class FetchHandlers:
    on_ok: Callable[[FetchOk], None] | None = None
    on_error: Callable[[RequestError], None] | None = None
    on_entry: Callable[[FetchStreamEntry], None] | None = None
    on_end: Callable[[], None] | None = None
    """The fetch data stream finished cleanly (FIN)."""
    on_reset: Callable[[FetchReset], None] | None = None
    on_goaway: Callable[[Goaway], None] | None = None


@dataclass(slots=True, kw_only=True)
class TrackStatusHandlers:
    on_ok: Callable[[TrackStatusOk], None] | None = None
    on_error: Callable[[RequestError], None] | None = None


class PublishResponder:
    """Answer a server-initiated PUBLISH. Call exactly one of the two methods; later calls are ignored."""

    __slots__ = ("_respond", "_responded")

    def __init__(self, respond: Callable[[bytes], object]) -> None:
        self._respond = respond
        self._responded = False

    def accept(self) -> None:
        """Send a bare PUBLISH_OK.

        Since draft-20 the subscription parameters (FORWARD, LOCATION_FILTER, ...) are legal only on
        PUBLISH and REQUEST_UPDATE, and a parameter in the wrong message is a PROTOCOL_VIOLATION
        (§10.2.1).
        """
        if self._responded:
            return

        self._responded = True
        self._respond(encode_request_ok())

    def reject(self, error_code: int = REQUEST_ERROR_CODE.UNINTERESTED, reason: str = "") -> None:
        if self._responded:
            return

        self._responded = True
        self._respond(encode_request_error(error_code, reason))


@dataclass(slots=True, kw_only=True)
class MoqtSessionCallbacks:
    on_ready: Callable[[list[KeyValuePair]], None] | None = None
    """The server's SETUP arrived; the session is fully established."""
    on_goaway: Callable[[Goaway], None] | None = None
    """GOAWAY on the control stream: stop initiating requests, migrate."""
    on_incoming_publish: Callable[[Publish, PublishResponder], None] | None = None
    """A server-initiated PUBLISH arrived. Call exactly one of the responder's methods.

    Absent, the session rejects with UNINTERESTED (a subscribe-only client).
    """
    on_closed: Callable[[Closed], None] | None = None
    """The session ended: transport closed, or a fatal protocol error."""


DEFAULT_UNKNOWN_ALIAS_TIMEOUT = 2.0
DEFAULT_REQUEST_TIMEOUT = 10.0
DEFAULT_IMPLEMENTATION_NAME = "moq-core (Python)"


@dataclass(slots=True, kw_only=True)
class MoqtSessionConfig:
    setup_options: Sequence[KeyValuePair] = ()
    """Extra Setup Options to send (an implementation identifier is added automatically)."""
    implementation_name: str = DEFAULT_IMPLEMENTATION_NAME
    """Identifies this implementation in SETUP (§10.3.1.5)."""
    unknown_alias_timeout: float = DEFAULT_UNKNOWN_ALIAS_TIMEOUT
    """Seconds to hold an incoming data stream whose Track Alias has no registered subscription yet.

    Objects can outrace SUBSCRIBE_OK; §11.4.2 allows brief buffering, and expired streams are dropped.
    """
    request_timeout: float = DEFAULT_REQUEST_TIMEOUT
    """Seconds to wait for a request's first response before failing it; ``0`` disables the bound.

    The draft expects implementations to bound control exchanges (§3.5's CONTROL_MESSAGE_TIMEOUT);
    without this a relay that accepts the stream and then goes quiet leaves the request pending
    forever.
    """
    callbacks: MoqtSessionCallbacks = field(default_factory=MoqtSessionCallbacks)


@dataclass(frozen=True, slots=True, kw_only=True)
class MoqtSubscribeOptions:
    track_namespace: Sequence[str]
    track_name: str
    parameters: MessageParameters | None = None


MoqtFetchOptions: TypeAlias = MoqtSubscribeOptions
"""FETCH (§10.13) takes the SUBSCRIBE shape.

The range is ``parameters.location_filter``, read with fetch rules (§5.1.2): a relative start counts
back from Largest Object, an omitted end stops there, no filter means the whole track.
"""


def _stream_failure(reason: str, error_code: int = REQUEST_ERROR_CODE.INTERNAL_ERROR) -> RequestError:
    """A request failure that arrives as a *stream* event rather than a REQUEST_ERROR message.

    FIN before any response, a transport reset, or the response timeout, shaped as a
    :class:`RequestError` so consumers have one failure path regardless of how the request died.
    """
    return RequestError(error_code=error_code, retry_interval=0, reason=reason)


def _describe(error: BaseException, fallback: str) -> str:
    return str(error) or fallback


def _call(callback: Callable[..., None] | None, *args: object) -> None:
    if callback is not None:
        callback(*args)


def _synchronous_subscription_handlers(handlers: SubscriptionHandlers) -> SubscriptionHandlers:
    def wrap(callback: Callable[..., None], name: str) -> Callable[..., None]:
        if inspect.iscoroutinefunction(callback) or inspect.iscoroutinefunction(callback.__call__):
            raise TypeError(f"{name} must be synchronous; use Relay subscribe_events() for async consumption")

        def invoke(*args: object) -> None:
            result = callback(*args)
            if inspect.isawaitable(result):
                if inspect.iscoroutine(result):
                    result.close()
                elif isinstance(result, asyncio.Future):
                    result.cancel()

                    # Observe an already-failed task as well as a pending task's eventual failure.
                    def consume(future: asyncio.Future[object]) -> None:
                        if not future.cancelled():
                            future.exception()

                    result.add_done_callback(consume)
                warnings.warn(
                    f"{name} returned an awaitable; callbacks must be synchronous; use Relay subscribe_events()",
                    RuntimeWarning,
                    stacklevel=2,
                )

        return invoke

    result = SubscriptionHandlers()
    for handler_field in fields(handlers):
        callback = getattr(handlers, handler_field.name)
        if callback is not None:
            setattr(result, handler_field.name, wrap(callback, handler_field.name))
    return result


# ============================================================================
# Internal records
# ============================================================================


@dataclass(slots=True, kw_only=True)
class _RequestRecord:
    """Bookkeeping ``_open_request`` manages for every request kind."""

    request_id: int
    cancelled: bool = False
    pending_cancel_reason: object = None
    stream: RequestStream | None = None
    settled: bool = False
    """The request's first response (an OK or a REQUEST_ERROR) arrived.

    Until it does, a FIN / reset / timeout on the stream is a failure; after it, those are ordinary
    end-of-request events.
    """
    response_timer: asyncio.TimerHandle | None = None


@dataclass(slots=True, kw_only=True)
class _SubscriptionRecord(_RequestRecord):
    readers: set[StreamReader] = field(default_factory=set)
    handlers: SubscriptionHandlers
    track_alias: int | None = None


@dataclass(slots=True, kw_only=True)
class _FetchRecord(_RequestRecord):
    handlers: FetchHandlers
    group_order: GroupOrder = "ascending"


@dataclass(slots=True, kw_only=True)
class _TrackStatusRecord(_RequestRecord):
    handlers: TrackStatusHandlers


@dataclass(slots=True)
class _AliasWaiter:
    future: asyncio.Future[_SubscriptionRecord | None]
    timer: asyncio.TimerHandle


class Subscription:
    """A live subscription request.

    MOQT has no UNSUBSCRIBE message: teardown is the request stream's lifecycle (§3.3.3).
    """

    __slots__ = ("_record", "_session")

    def __init__(self, session: MoqtSession, record: _SubscriptionRecord) -> None:
        self._session = session
        self._record = record

    @property
    def request_id(self) -> int:
        return self._record.request_id

    def update(self, parameters: MessageParameters) -> None:
        """Modify the subscription (REQUEST_UPDATE, §10.9)."""
        # REQUEST_UPDATE consumes a Request ID of its own (§10.1); the stream it travels on is what
        # names the subscription. Reusing the subscription's id would be a duplicate the peer MUST
        # close on.
        stream = self._record.stream
        if stream is not None:
            spawn(stream.send(encode_request_update(self._session._allocate_request_id(), parameters)))

    def cancel(self, reason: object = None) -> None:
        """Tear the subscription down."""
        self._session._cancel_subscription(self._record, reason)


class FetchHandle:
    __slots__ = ("_record", "_session")

    def __init__(self, session: MoqtSession, record: _FetchRecord) -> None:
        self._session = session
        self._record = record

    @property
    def request_id(self) -> int:
        return self._record.request_id

    def cancel(self, reason: object = None) -> None:
        self._session._cancel_fetch(self._record, reason)


# ============================================================================
# Session driver
# ============================================================================


class MoqtSession:
    """The subscribe-side session driver. Create one with :func:`create_moqt_session`."""

    def __init__(self, transport: MoqtTransport, config: MoqtSessionConfig | None = None) -> None:
        self._transport = transport
        self._config = config or MoqtSessionConfig()
        self._callbacks = self._config.callbacks

        self._next_request_id = 0  # client request IDs: even, starting at 0 (§10.1)
        self._subscriptions: dict[int, _SubscriptionRecord] = {}
        self._fetches: dict[int, _FetchRecord] = {}
        self._track_statuses: dict[int, _TrackStatusRecord] = {}
        self._alias_routes: dict[int, _SubscriptionRecord] = {}
        self._alias_waiters: dict[int, list[_AliasWaiter]] = {}

        loop = asyncio.get_running_loop()
        self.ready: asyncio.Future[None] = loop.create_future()
        """Resolves when the server's SETUP has been received; fails if the session ends first."""
        # Consumers may observe readiness through `on_closed` alone.
        observe(self.ready)

        self._control_writer = None
        self._received_server_setup = False
        self._received_control_goaway = False
        self._destroyed = False

        spawn(self._start())

    # --------------------------------------------------------------------------
    # Session lifecycle
    # --------------------------------------------------------------------------

    async def _start(self) -> None:
        # Accept loops start first: the peer's streams must be processed even while our own SETUP
        # write is still propagating (either peer may send stream data first, §3.3).
        spawn(self._accept_unidirectional_streams())
        spawn(self._accept_bidirectional_streams())
        spawn(self._watch_transport())

        try:
            control = await self._transport.create_unidirectional_stream()
            self._control_writer = control
            options = [
                KeyValuePair(SETUP_OPTION.MOQT_IMPLEMENTATION, utf8_encode(self._config.implementation_name)),
                *self._config.setup_options,
            ]
            await control.write(encode_setup(options))
        except Exception as error:  # any failure to open the control stream ends the session
            self._fatal(error)

    async def _watch_transport(self) -> None:
        try:
            await self._transport.wait_closed()
        except Exception as error:  # the transport's own failure ends the session
            self._handle_closed(error)
        else:
            self._handle_closed(None)

    def _fatal(self, error: BaseException) -> None:
        if self._destroyed:
            return

        close_code = error.code if isinstance(error, MoqtProtocolError) else SESSION_ERROR.INTERNAL_ERROR

        try:
            self._transport.close(close_code, _describe(error, "session error"))
        except Exception:  # transport already gone
            pass

        self._handle_closed(error)

    def _fatal_if_protocol_error(self, error: BaseException) -> None:
        if is_moqt_protocol_error(error):
            self._fatal(error)

    def _handle_closed(self, error: BaseException | None, *, expected: bool = False) -> None:
        if self._destroyed:
            return

        self._destroyed = True
        setup_error = (
            None
            if self._received_server_setup
            else (error if error is not None else MoqtProtocolError("session closed before server SETUP"))
        )

        if setup_error is not None and not self.ready.done():
            self.ready.set_exception(setup_error)

        for waiters in self._alias_waiters.values():
            for waiter in waiters:
                waiter.timer.cancel()
                if not waiter.future.done():
                    waiter.future.set_result(None)

        self._alias_waiters.clear()

        # Requests still awaiting their first response die with the session. An unexpected close
        # (transport drop) is a failure each of them has to hear about; otherwise a dropped transport
        # leaves subscribers pending forever. A deliberate `close()` is ordinary teardown, and
        # cancelled requests already have their answer.
        records: list[_SubscriptionRecord | _FetchRecord | _TrackStatusRecord] = [
            *self._subscriptions.values(),
            *self._fetches.values(),
            *self._track_statuses.values(),
        ]
        pending = [] if expected else [record for record in records if not record.settled and not record.cancelled]

        for record in records:
            self._settle_request(record)
            if record.stream is not None:
                record.stream.cancel(error)

        for record in list(self._subscriptions.values()):
            self._remove_subscription(record)
        self._subscriptions.clear()
        self._fetches.clear()
        self._track_statuses.clear()
        self._alias_routes.clear()
        self._control_writer = None

        failure = _stream_failure(
            _describe(error, "session closed before the request was answered")
            if error is not None
            else "session closed before the request was answered"
        )

        for record in pending:
            _call(record.handlers.on_error, failure)

        # A transport that drops before server SETUP is a session failure even when the close itself
        # was clean: callback-only consumers observing `on_closed` alone must see the error too. A
        # deliberate local `close()` stays a clean close.
        close_error = error if error is not None else (None if expected else setup_error)
        _call(self._callbacks.on_closed, Closed(close_error))

    def close(self, close_code: int = SESSION_ERROR.NO_ERROR, reason: str = "") -> None:
        """Send GOAWAY then close the transport session."""
        self._close(close_code, reason)

    def _close(self, close_code: int, reason: str, error: BaseException | None = None) -> None:
        if self._destroyed:
            return

        # Best-effort GOAWAY so the peer can stop routing to us first.
        if self._control_writer is not None:
            spawn(self._control_writer.write(encode_goaway(0)))

        try:
            self._transport.close(close_code, reason)
        except Exception:  # transport already gone
            pass

        self._handle_closed(error, expected=error is None)

    def destroy(self, error: BaseException | None = None) -> None:
        """End locally; an error also fails requests awaiting their first response."""
        self._close(SESSION_ERROR.NO_ERROR, "", error)

    # --------------------------------------------------------------------------
    # Requests
    # --------------------------------------------------------------------------

    def _allocate_request_id(self) -> int:
        request_id = self._next_request_id
        self._next_request_id += 2
        return request_id

    def subscribe(self, options: MoqtSubscribeOptions, handlers: SubscriptionHandlers | None = None) -> Subscription:
        handlers = _synchronous_subscription_handlers(handlers or SubscriptionHandlers())
        request_id = self._allocate_request_id()
        message = encode_subscribe(
            Subscribe(
                request_id=request_id,
                track_namespace=list(options.track_namespace),
                track_name=options.track_name,
                parameters=options.parameters or MessageParameters(),
            )
        )
        record = _SubscriptionRecord(request_id=request_id, handlers=handlers)
        self._subscriptions[request_id] = record

        def on_failure(error: RequestError) -> None:
            self._remove_subscription(record)
            _call(handlers.on_error, error)

        spawn(
            self._open_request(message, record, lambda msg: self._handle_subscription_message(record, msg), on_failure)
        )

        return Subscription(self, record)

    def fetch(self, options: MoqtSubscribeOptions, handlers: FetchHandlers | None = None) -> FetchHandle:
        handlers = handlers or FetchHandlers()
        request_id = self._allocate_request_id()
        parameters = options.parameters or MessageParameters()
        message = encode_fetch(
            Fetch(
                request_id=request_id,
                track_namespace=list(options.track_namespace),
                track_name=options.track_name,
                parameters=parameters,
            )
        )
        record = _FetchRecord(
            request_id=request_id, handlers=handlers, group_order=parameters.group_order or "ascending"
        )
        self._fetches[request_id] = record

        def on_failure(error: RequestError) -> None:
            self._fetches.pop(request_id, None)
            _call(handlers.on_error, error)

        spawn(self._open_request(message, record, lambda msg: self._handle_fetch_message(record, msg), on_failure))

        return FetchHandle(self, record)

    def track_status(self, options: MoqtSubscribeOptions, handlers: TrackStatusHandlers | None = None) -> None:
        handlers = handlers or TrackStatusHandlers()
        request_id = self._allocate_request_id()
        message = encode_track_status(
            TrackStatus(
                request_id=request_id,
                track_namespace=list(options.track_namespace),
                track_name=options.track_name,
                parameters=options.parameters or MessageParameters(),
            )
        )
        record = _TrackStatusRecord(request_id=request_id, handlers=handlers)

        # Tracked so session teardown fails a still-pending TRACK_STATUS the same way it fails
        # pending subscriptions and fetches. Answered requests leave the map; nothing else arrives on
        # their stream.
        self._track_statuses[request_id] = record

        def on_message(msg: ControlMessage) -> None:
            if isinstance(msg, RequestOk):
                self._settle_request(record)
                self._track_statuses.pop(request_id, None)
                _call(handlers.on_ok, TrackStatusOk(parameters=msg.parameters, track_properties=msg.track_properties))
                if record.stream is not None:
                    spawn(record.stream.fin_write())
            elif isinstance(msg, RequestError):
                self._settle_request(record)
                self._track_statuses.pop(request_id, None)
                _call(handlers.on_error, msg)
                if record.stream is not None:
                    spawn(record.stream.fin_write())

        def on_failure(error: RequestError) -> None:
            self._track_statuses.pop(request_id, None)
            _call(handlers.on_error, error)

        spawn(self._open_request(message, record, on_message, on_failure))

    async def _open_request(
        self,
        message: bytes,
        record: _RequestRecord,
        on_message: Callable[[ControlMessage], None],
        on_failure: Callable[[RequestError], None],
    ) -> None:
        """Open a request stream and bind its lifecycle to ``record``.

        ``on_failure`` is the caller's "this request died without answering me" path; the request
        stream deliberately leaves that judgement here, since only the session knows which messages a
        given request kind requires. It fires for a FIN before any response, a non-protocol stream
        error (transport reset), and the response timeout; a protocol error still kills the whole
        session. Once ``record.settled`` is set the same events are ordinary end-of-request signals
        and are ignored.
        """

        def fail(error: RequestError) -> None:
            if record.settled or record.cancelled or self._destroyed:
                return

            self._settle_request(record)
            on_failure(error)

        try:
            stream = await self._transport.create_bidirectional_stream()
        except Exception as error:  # losing the ability to open streams is a session failure
            # ...but this request also has to hear about it.
            fail(_stream_failure(_describe(error, "failed to open request stream")))

            if not self._destroyed:
                self._fatal(error)

            return

        timeout = self._config.request_timeout

        if timeout > 0 and not record.settled and not record.cancelled:

            def on_timeout() -> None:
                failure = _stream_failure("no response before the request timeout", REQUEST_ERROR_CODE.TIMEOUT)

                # The stream dies with the request: left open, a late SUBSCRIBE_OK would fire on_ok
                # after on_error and re-register the alias route of a subscription already reported
                # dead.
                if record.stream is not None:
                    record.stream.cancel(failure)

                fail(failure)

            record.response_timer = call_later(timeout, on_timeout)

        def on_error(error: BaseException) -> None:
            if is_moqt_protocol_error(error):
                self._fatal(error)
                return

            fail(_stream_failure(_describe(error, "request stream failed")))

        record.stream = open_request_stream(
            stream,
            message,
            RequestStreamHandlers(
                on_message=on_message,
                on_fin=lambda: fail(_stream_failure("request stream closed before a response")),
                on_error=on_error,
            ),
        )

        # A cancel that raced the stream opening lands here.
        if record.cancelled:
            record.stream.cancel(record.pending_cancel_reason)

    def _settle_request(self, record: _RequestRecord) -> None:
        """Mark a request answered (or dead) and disarm its response timeout."""
        record.settled = True

        if record.response_timer is not None:
            record.response_timer.cancel()
            record.response_timer = None

    def _handle_subscription_message(self, record: _SubscriptionRecord, message: ControlMessage) -> None:
        if record.cancelled:
            return
        match message:
            case SubscribeOk(track_alias=track_alias):
                if record.track_alias is not None and record.track_alias != track_alias:
                    self._fatal(MoqtProtocolError("SUBSCRIBE_OK changed track alias"))
                    return

                existing = self._alias_routes.get(track_alias)

                if existing is not None and existing is not record:
                    self._fatal(MoqtProtocolError("duplicate track alias", SESSION_ERROR.DUPLICATE_TRACK_ALIAS))
                    return

                self._settle_request(record)
                record.track_alias = track_alias
                self._alias_routes[track_alias] = record
                waiters = self._alias_waiters.pop(track_alias, None)

                if waiters:
                    for waiter in waiters:
                        waiter.timer.cancel()
                        if not waiter.future.done():
                            waiter.future.set_result(record)

                _call(record.handlers.on_ok, message)
            case RequestOk():
                _call(record.handlers.on_update_ok)
            case RequestError():
                self._settle_request(record)
                self._remove_subscription(record)
                _call(record.handlers.on_error, message)
            case PublishDone():
                # Data streams may still be inbound; the alias route stays until the subscription is
                # cancelled or the session ends, so late subgroups still deliver (§10.12 leaves the
                # timing policy to us).
                _call(record.handlers.on_done, message)
            case PublishStateNotify(parameters=parameters):
                # Unilateral (§10.10): no REQUEST_OK back, and it does not count against
                # MAX_REQUEST_UPDATES.
                _call(record.handlers.on_state_notify, parameters)
            case Goaway():
                _call(record.handlers.on_goaway, message)
            case _:
                self._fatal(MoqtProtocolError(f"unexpected {message.kind} on a subscribe request stream"))

    def _handle_fetch_message(self, record: _FetchRecord, message: ControlMessage) -> None:
        match message:
            case FetchOk():
                self._settle_request(record)
                _call(record.handlers.on_ok, message)
            case RequestOk():
                # Some deployed relays accept a FETCH with the generic REQUEST_OK instead of the
                # FETCH_OK §10.13 mandates. It carries no End Location/End Of Track, so on_ok can't
                # fire; the fetch's actual completion still surfaces via on_entry/on_end on the data
                # stream. It is still the response, so it has to settle the request: leaving the timer
                # armed would fail an answered fetch (and drop its data stream) once the deadline
                # passed.
                self._settle_request(record)
            case RequestError():
                self._settle_request(record)
                self._fetches.pop(record.request_id, None)
                _call(record.handlers.on_error, message)
            case Goaway():
                _call(record.handlers.on_goaway, message)
            case _:
                self._fatal(MoqtProtocolError(f"unexpected {message.kind} on a fetch request stream"))

    def _cancel_subscription(self, record: _SubscriptionRecord, reason: object = None) -> None:
        if record.cancelled:
            return

        record.cancelled = True
        record.pending_cancel_reason = reason
        self._settle_request(record)
        if record.stream is not None:
            record.stream.cancel(reason)
        self._remove_subscription(record)

    def _remove_subscription(self, record: _SubscriptionRecord) -> None:
        record.cancelled = True
        if record.stream is not None:
            record.stream.cancel(record.pending_cancel_reason)
        for reader in record.readers:
            reader.cancel(record.pending_cancel_reason)
        record.readers.clear()
        self._subscriptions.pop(record.request_id, None)

        if record.track_alias is not None and self._alias_routes.get(record.track_alias) is record:
            del self._alias_routes[record.track_alias]

    def _cancel_fetch(self, record: _FetchRecord, reason: object = None) -> None:
        if record.cancelled:
            return

        record.cancelled = True
        record.pending_cancel_reason = reason
        self._settle_request(record)
        if record.stream is not None:
            record.stream.cancel(reason)
        self._fetches.pop(record.request_id, None)

    # --------------------------------------------------------------------------
    # Incoming unidirectional streams (control, subgroup, fetch, padding)
    # --------------------------------------------------------------------------

    async def _accept_unidirectional_streams(self) -> None:
        try:
            async for stream in self._transport.incoming_unidirectional_streams():
                spawn(self._handle_unidirectional_stream(stream), on_error=self._fatal_if_protocol_error)
        except Exception:  # transport went away; `wait_closed` handles the surfacing
            pass

    async def _handle_unidirectional_stream(self, stream: ReceiveStream) -> None:
        reader = StreamReader(stream)
        stream_type = await reader.read_varint()

        if stream_type == STREAM_TYPE.SETUP:
            await self._run_control_stream(reader)
            return

        if is_subgroup_header_type(stream_type):
            header = await read_subgroup_header(reader, stream_type)
            await self._run_subgroup_stream(reader, header)
            return

        if stream_type == STREAM_TYPE.FETCH_HEADER:
            request_id = await read_fetch_header(reader)
            await self._run_fetch_stream(reader, request_id)
            return

        if stream_type == STREAM_TYPE.PADDING:
            reader.cancel()
            return

        reader.cancel()
        raise MoqtProtocolError(f"unknown unidirectional stream type 0x{stream_type:x}")

    async def _read_control_frame(self, reader: StreamReader, message_type: int) -> ControlMessage:
        """Read one control-message frame using the pull-based stream reader."""
        high = await reader.read_uint8()
        low = await reader.read_uint8()
        body = await reader.read_bytes((high << 8) | low)
        return decode_control_message(ControlMessageFrame(message_type, body))

    async def _run_control_stream(self, reader: StreamReader) -> None:
        # The stream-type varint (0x2F00) doubles as the first message's type.
        setup = await self._read_control_frame(reader, MESSAGE_TYPE.SETUP)
        if not isinstance(setup, Setup):
            raise MoqtProtocolError("control stream did not begin with SETUP")

        if self._received_server_setup:
            raise MoqtProtocolError("received a second control stream")

        self._received_server_setup = True
        if not self.ready.done():
            self.ready.set_result(None)
        _call(self._callbacks.on_ready, setup.options)

        # Subsequent control-stream messages: GOAWAY (at most once).
        while not await reader.at_end():
            message_type = await reader.read_varint()
            message = await self._read_control_frame(reader, message_type)
            if not isinstance(message, Goaway):
                raise MoqtProtocolError(f"unexpected {message.kind} on the control stream")

            if self._received_control_goaway:
                raise MoqtProtocolError("received more than one GOAWAY on the control stream")

            self._received_control_goaway = True
            _call(self._callbacks.on_goaway, message)

        # A closed control stream ends the session (§3.3).
        if not self._destroyed:
            self._fatal(MoqtProtocolError("peer closed its control stream"))

    async def _wait_for_alias(self, track_alias: int) -> _SubscriptionRecord | None:
        """Wait briefly for a subscription to claim ``track_alias`` (§11.4.2 buffering)."""
        known = self._alias_routes.get(track_alias)
        if known is not None or self._destroyed:
            return known

        loop = asyncio.get_running_loop()
        future: asyncio.Future[_SubscriptionRecord | None] = loop.create_future()
        waiters = self._alias_waiters.setdefault(track_alias, [])

        def expire() -> None:
            remaining = [w for w in self._alias_waiters.get(track_alias, []) if w is not waiter]

            if remaining:
                self._alias_waiters[track_alias] = remaining
            else:
                self._alias_waiters.pop(track_alias, None)

            if not future.done():
                future.set_result(None)

        waiter = _AliasWaiter(future, call_later(self._config.unknown_alias_timeout, expire))
        waiters.append(waiter)
        return await future

    async def _run_subgroup_stream(self, reader: StreamReader, header: SubgroupHeader) -> None:
        record = await self._wait_for_alias(header.track_alias)

        if record is None or record.cancelled:
            reader.cancel()
            return

        record.readers.add(reader)
        last_subgroup_id = header.subgroup_id if header.subgroup_id_mode == "explicit" and header.subgroup_id else 0

        try:
            async with aclosing(read_subgroup_objects(reader, header)) as objects:
                async for obj in objects:
                    if record.cancelled:
                        reader.cancel()
                        return

                    last_subgroup_id = obj.subgroup_id
                    _call(record.handlers.on_object, obj)

            if record.cancelled:
                return
            _call(
                record.handlers.on_subgroup_end,
                SubgroupEnd(group_id=header.group_id, subgroup_id=last_subgroup_id, end_of_group=header.end_of_group),
            )
        except MoqtProtocolError:
            if not record.cancelled:
                raise
        except Exception as error:  # a reset stream is a delivery event, not a session error
            if not record.cancelled:
                _call(record.handlers.on_subgroup_reset, SubgroupReset(group_id=header.group_id, error=error))
        finally:
            record.readers.discard(reader)

    async def _run_fetch_stream(self, reader: StreamReader, request_id: int) -> None:
        record = self._fetches.get(request_id)

        # A FETCH_HEADER naming no fetch of ours is dropped, not fatal. That includes a fill fetch
        # stream (§5.1.3), keyed by a SUBSCRIBE's Request ID: this subscriber never asks for one, so
        # none is expected.
        if record is None or record.cancelled:
            reader.cancel()
            return

        try:
            async with aclosing(read_fetch_entries(reader, record.group_order)) as entries:
                async for entry in entries:
                    if record.cancelled:
                        reader.cancel()
                        return

                    _call(record.handlers.on_entry, entry)

            _call(record.handlers.on_end)
        except MoqtProtocolError:
            raise
        except Exception as error:  # a reset mid-replay means entries may be missing
            # Never report it as the clean FIN `on_end` promises.
            _call(record.handlers.on_reset, FetchReset(error=error))

    # --------------------------------------------------------------------------
    # Incoming bidirectional streams (server-initiated requests)
    # --------------------------------------------------------------------------

    async def _accept_bidirectional_streams(self) -> None:
        try:
            async for stream in self._transport.incoming_bidirectional_streams():
                spawn(self._handle_incoming_request(stream), on_error=self._fatal_if_protocol_error)
        except Exception:  # transport went away
            pass

    async def _handle_incoming_request(self, stream: BidirectionalStream) -> None:
        reader = StreamReader(stream.readable)
        message_type = await reader.read_varint()
        message = await self._read_control_frame(reader, message_type)

        async def respond(data: bytes) -> None:
            try:
                await stream.writable.write(data)
                await stream.writable.close()
            except Exception:  # peer cancelled; nothing to clean up
                pass

            reader.cancel()

        if isinstance(message, Publish):
            if self._callbacks.on_incoming_publish is not None:
                self._callbacks.on_incoming_publish(message, PublishResponder(lambda data: spawn(respond(data))))
                return

            await respond(encode_request_error(REQUEST_ERROR_CODE.UNINTERESTED, "subscribe-only client"))
            return

        # Any other server-initiated request: this client doesn't serve them.
        await respond(encode_request_error(REQUEST_ERROR_CODE.NOT_SUPPORTED, "subscribe-only client"))


def create_moqt_session(transport: MoqtTransport, config: MoqtSessionConfig | None = None) -> MoqtSession:
    """Create a MOQT session driver over an established transport. Requires a running event loop.

    Example::

        session = create_moqt_session(transport, MoqtSessionConfig(callbacks=MoqtSessionCallbacks(on_goaway=migrate)))
        await session.ready
        subscription = session.subscribe(
            MoqtSubscribeOptions(track_namespace=["live", "stream1"], track_name="catalog"),
            SubscriptionHandlers(on_object=handle_catalog_object),
        )
    """
    return MoqtSession(transport, config)


__all__ = [
    "DEFAULT_IMPLEMENTATION_NAME",
    "DEFAULT_REQUEST_TIMEOUT",
    "DEFAULT_UNKNOWN_ALIAS_TIMEOUT",
    "Closed",
    "FetchHandle",
    "FetchHandlers",
    "FetchReset",
    "MoqtFetchOptions",
    "MoqtSession",
    "MoqtSessionCallbacks",
    "MoqtSessionConfig",
    "MoqtSubscribeOptions",
    "PublishResponder",
    "SubgroupEnd",
    "SubgroupReset",
    "Subscription",
    "SubscriptionHandlers",
    "TrackStatusHandlers",
    "TrackStatusOk",
    "create_moqt_session",
]
