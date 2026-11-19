"""One MOQT session that publishes and subscribes.

The subscribe driver (``session``) and the publish driver (``publish_session``) share one transport, as
draft-22 allows (a subscription "can be established by either endpoint"; roles belong to entities, not
sessions). The counterpart of ``ts/packages/moq-core/src/protocol/duplex-session.ts``.

The subscribe driver owns the session: it sends and reads SETUP, starts every request this endpoint makes
(SUBSCRIBE, SUBSCRIBE_NAMESPACE, FETCH, TRACK_STATUS), and reads every incoming unidirectional stream, the
control stream and subgroup data alike. The publish driver serves the peer: each request stream the peer
opens is routed by its first message type (:data:`_PUBLISHER_REQUESTS`), so SUBSCRIBE_NAMESPACE (the
relay's solicitation, which carries announces), SUBSCRIBE, TRACK_STATUS and FETCH reach the publish driver,
and anything else (PUBLISH, PUBLISH_NAMESPACE) the subscribe driver. The publish driver's own control stream
is swallowed, and it is handed the peer's SETUP options and any control-stream GOAWAY the subscribe driver
reads, so its readiness and migration silence follow the real session.

This endpoint's Request IDs are allocated by the subscribe driver alone. The peer's are consumed
session-wide (§6.4.2.1), so both drivers claim them in one shared set: an ID one driver consumed is a reuse
to the other.

Measured against moq-relay on 2026-11-18 (spec/behavior.md, "One connection that publishes and
subscribes"): one session announced, served the relay's SUBSCRIBE and subscribed to another session's track
at once, with the relay soliciting its announces even though SETUP carried SOLICIT. The relay never routes a
session's own announcements back to it.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field, replace
from typing import Generic, TypeVar

from ._tasks import spawn
from .control_messages import MESSAGE_TYPE, Goaway, KeyValuePair, encode_control_message, encode_setup
from .errors import SESSION_ERROR
from .publish_session import MoqtPublishSession, MoqtPublishSessionConfig, create_moqt_publish_session
from .session import Closed, MoqtSession, MoqtSessionConfig, _call
from .transport import BidirectionalStream, MoqtTransport, ReceiveStream, SendStream, transport_ended
from .varint import decode_varint, varint_length_from_first_byte

_PUBLISHER_REQUESTS = frozenset(
    {MESSAGE_TYPE.SUBSCRIBE_NAMESPACE, MESSAGE_TYPE.SUBSCRIBE, MESSAGE_TYPE.TRACK_STATUS, MESSAGE_TYPE.FETCH}
)
"""Peer-initiated requests the publish driver serves; every other request type goes to the subscribe driver."""


@dataclass(slots=True, kw_only=True)
class MoqtDuplexSessionConfig:
    """How :func:`create_moqt_duplex_session` configures the two drivers that share the transport.

    Each driver's ``peer_request_ids`` is replaced by the session's shared set.
    """

    subscriber: MoqtSessionConfig = field(default_factory=MoqtSessionConfig)
    """The subscribe driver, which sends SETUP: its ``setup_options`` and ``implementation_name`` are the session's."""
    publisher: MoqtPublishSessionConfig = field(default_factory=MoqtPublishSessionConfig)
    """The publish driver. It sends no SETUP of its own, so its ``setup_options`` and ``implementation_name`` do not
    apply."""


_T = TypeVar("_T")


class _Channel(Generic[_T]):
    """An unbounded queue with one reader, which iterates until the channel has ended and run dry."""

    __slots__ = ("_ended", "_items", "_waiter")

    def __init__(self) -> None:
        self._items: deque[_T] = deque()
        self._ended = False
        self._waiter: asyncio.Future[None] | None = None

    def push(self, item: _T) -> bool:
        """Queue ``item``; ``False`` once the channel has ended."""
        if self._ended:
            return False

        self._items.append(item)
        self._wake()
        return True

    def end(self) -> None:
        self._ended = True
        self._wake()

    async def next(self) -> _T | None:
        """The next item, or ``None`` once the channel has ended and run dry."""
        while not self._items:
            if self._ended:
                return None

            self._waiter = asyncio.get_running_loop().create_future()
            await self._waiter

        return self._items.popleft()

    def _wake(self) -> None:
        if self._waiter is not None and not self._waiter.done():
            self._waiter.set_result(None)

        self._waiter = None

    def __aiter__(self) -> _Channel[_T]:
        return self

    async def __anext__(self) -> _T:
        item = await self.next()
        if item is None:
            raise StopAsyncIteration
        return item


class _ControlCopy(_Channel[bytes]):
    """The publish driver's control stream from the peer: the peer's SETUP and any GOAWAY, copied from the real one.

    It ends only once the publish driver has: the driver reads its control stream ending as the end of the session.
    """

    async def read(self) -> bytes | None:
        return await self.next()

    def cancel(self, reason: object = None) -> None:
        self.end()


class _Discard:
    """The publish driver's own control stream: the subscribe driver's is the session's."""

    async def write(self, data: bytes) -> None:
        pass

    async def close(self) -> None:
        pass

    def abort(self, reason: object = None) -> None:
        pass


class _Replayed:
    """A request stream's receiving half that first replays the bytes the router read to find its type."""

    __slots__ = ("_ended", "_failure", "_head", "_inner")

    def __init__(self, inner: ReceiveStream, head: bytes, failure: BaseException | None, ended: bool) -> None:
        self._inner = inner
        self._head = head
        self._failure = failure
        self._ended = ended

    async def read(self) -> bytes | None:
        if self._head:
            head, self._head = self._head, b""
            return head

        if self._failure is not None:
            raise self._failure

        if self._ended:
            return None

        return await self._inner.read()

    def cancel(self, reason: object = None) -> None:
        self._head = b""
        self._inner.cancel(reason)


async def _peek_request_type(stream: BidirectionalStream) -> tuple[int | None, BidirectionalStream]:
    """Read a request stream's first message type without consuming it.

    The type, or ``None`` if the stream ended or failed first, and a stream that replays every byte read.
    """
    head = bytearray()
    failure: BaseException | None = None
    ended = False

    # The first byte gives the varint's length (MOQT's leading-ones prefix, up to nine bytes); once a whole one is
    # in, the router has what it needs.
    while not head or len(head) < varint_length_from_first_byte(head[0]):
        try:
            chunk = await stream.readable.read()
        except Exception as error:  # a reset before the type: the driver it reaches reads the failure
            failure = error
            break

        if chunk is None:
            ended = True
            break

        head += chunk

    request_type = None if failure is not None or ended else decode_varint(head)[0]
    readable = _Replayed(stream.readable, bytes(head), failure, ended)
    return request_type, BidirectionalStream(readable=readable, writable=stream.writable)


class _SharedTransport:
    """What each driver's view passes straight through to the real transport."""

    __slots__ = ("_transport",)

    def __init__(self, transport: MoqtTransport) -> None:
        self._transport = transport

    @property
    def protocol_id(self) -> object:
        """The negotiated MOQT version (TA-8), which selects the driver's parameter codec."""
        return getattr(self._transport, "protocol_id", None)

    @property
    def ended(self) -> bool:
        return transport_ended(self._transport)

    async def wait_closed(self) -> None:
        await self._transport.wait_closed()


class _SubscriberView(_SharedTransport):
    """The subscribe driver's transport: the real one, but only the peer's requests routed to it, and a close that
    waits for the publisher's drain once the session is ending."""

    __slots__ = ("_requests", "_session")

    def __init__(
        self, transport: MoqtTransport, requests: _Channel[BidirectionalStream], session: MoqtDuplexSession
    ) -> None:
        super().__init__(transport)
        self._requests = requests
        self._session = session

    def incoming_unidirectional_streams(self) -> AsyncIterator[ReceiveStream]:
        return self._transport.incoming_unidirectional_streams()

    def incoming_bidirectional_streams(self) -> _Channel[BidirectionalStream]:
        return self._requests

    async def create_unidirectional_stream(self) -> SendStream:
        return await self._transport.create_unidirectional_stream()

    async def create_bidirectional_stream(self) -> BidirectionalStream:
        return await self._transport.create_bidirectional_stream()

    def close(self, close_code: int = 0, reason: str = "") -> None:
        # Once the session is ending the publisher closes the transport, after its drain.
        if not self._session._ending:
            self._transport.close(close_code, reason)


class _PublisherView(_SharedTransport):
    """The publish driver's transport: a copy of the peer's control stream, the peer's requests routed to it, and a
    swallowed control stream of its own."""

    __slots__ = ("_control", "_control_taken", "_requests", "_uni")

    def __init__(
        self, transport: MoqtTransport, requests: _Channel[BidirectionalStream], control: _ControlCopy
    ) -> None:
        super().__init__(transport)
        self._requests = requests
        self._control = control
        self._uni: _Channel[ReceiveStream] = _Channel()
        self._uni.push(control)
        self._control_taken = False

    def incoming_unidirectional_streams(self) -> _Channel[ReceiveStream]:
        return self._uni

    def incoming_bidirectional_streams(self) -> _Channel[BidirectionalStream]:
        return self._requests

    async def create_unidirectional_stream(self) -> SendStream:
        # The driver's first stream is its control stream, opened before anything else.
        if not self._control_taken:
            self._control_taken = True
            return _Discard()

        return await self._transport.create_unidirectional_stream()

    async def create_bidirectional_stream(self) -> BidirectionalStream:
        raise RuntimeError("the publish side of a duplex session starts no requests")

    def close(self, close_code: int = 0, reason: str = "") -> None:
        self._transport.close(close_code, reason)

    def end(self) -> None:
        """The publish driver has ended: end its incoming streams, so none of its reads wait forever."""
        self._control.end()
        self._uni.end()


class _DuplexSubscriber(MoqtSession):
    """The subscribe driver, except that ending it ends the session in order, as :meth:`MoqtDuplexSession.destroy`
    does."""

    def __init__(self, duplex: MoqtDuplexSession, transport: MoqtTransport, config: MoqtSessionConfig) -> None:
        self._duplex = duplex
        super().__init__(transport, config)

    def close(self, close_code: int = SESSION_ERROR.NO_ERROR, reason: str = "") -> None:
        """End the session: the subscriber at once, the transport once the publisher has drained, with this code."""
        publisher = self._duplex.publisher
        self._duplex._end(
            lambda: MoqtSession.close(self, close_code, reason), lambda: publisher.close(close_code, reason)
        )

    def destroy(self, error: BaseException | None = None) -> None:
        """End the session as :meth:`MoqtDuplexSession.destroy` does."""
        self._duplex.destroy(error)


class MoqtDuplexSession:
    """One session, seen through each driver. Create one with :func:`create_moqt_duplex_session`."""

    __slots__ = ("_ending", "publisher", "subscriber")

    def __init__(self, transport: MoqtTransport, config: MoqtDuplexSessionConfig | None = None) -> None:
        config = config or MoqtDuplexSessionConfig()
        self._ending = False
        """Set by ending the session: the subscriber's own close must not cut the publisher's drain short."""
        peer_request_ids: set[int] = set()
        control = _ControlCopy()
        to_subscriber: _Channel[BidirectionalStream] = _Channel()
        to_publisher: _Channel[BidirectionalStream] = _Channel()
        publisher_view = _PublisherView(transport, to_publisher, control)
        subscriber_callbacks = config.subscriber.callbacks
        publisher_callbacks = config.publisher.callbacks

        def on_ready(options: list[KeyValuePair]) -> None:
            control.push(encode_setup(options))
            _call(subscriber_callbacks.on_ready, options)

        def on_goaway(goaway: Goaway) -> None:
            # Once the session is ending the publisher is draining, and a GOAWAY changes nothing for it.
            if not self._ending:
                control.push(encode_control_message(goaway))
            _call(subscriber_callbacks.on_goaway, goaway)

        def on_publisher_closed(info: Closed) -> None:
            try:
                _call(publisher_callbacks.on_closed, info)
            finally:
                publisher_view.end()

        self.subscriber: MoqtSession = _DuplexSubscriber(
            self,
            _SubscriberView(transport, to_subscriber, self),
            replace(
                config.subscriber,
                peer_request_ids=peer_request_ids,
                callbacks=replace(subscriber_callbacks, on_ready=on_ready, on_goaway=on_goaway),
            ),
        )
        """Starts this endpoint's requests and receives their data. Its ``ready`` is the session's.

        Its ``close()`` and ``destroy()`` end the session as :meth:`destroy` does: the subscriber at once, the
        transport once the publisher has drained, with the close code and reason ``close()`` was given.
        """
        self.publisher: MoqtPublishSession = create_moqt_publish_session(
            publisher_view,
            replace(
                config.publisher,
                peer_request_ids=peer_request_ids,
                callbacks=replace(publisher_callbacks, on_closed=on_publisher_closed),
            ),
        )
        """Announces namespaces and serves the peer's requests for their tracks.

        Its ``ready`` follows the subscriber's; its ``close()`` ends its subscriptions and announces and then
        closes the whole session.
        """
        spawn(_route(transport, to_subscriber, to_publisher))

    def destroy(self, error: BaseException | None = None) -> None:
        """End the session.

        The subscriber ends at once, its requests with it, but leaves the transport open; the publisher ends
        its subscriptions and retracts its announces within its bounded drain, then closes the transport.
        ``error``, if given, is the local closure error both drivers report. Each driver's callbacks end at
        its own ``on_closed``: the subscriber's at once, the publisher's (subscription ends, unbindings) once
        its drain is over. A session error before then (a protocol violation on either side) is not a drain:
        it closes the transport at once.
        """
        subscriber = self.subscriber
        self._end(lambda: MoqtSession.destroy(subscriber, error), lambda: self.publisher.destroy(error))

    def _end(self, end_subscriber: Callable[[], None], end_publisher: Callable[[], None]) -> None:
        """End the subscriber now, leaving the transport to the publisher, which closes it after its drain."""
        if self._ending:
            return

        self._ending = True
        # A throwing subscriber callback must not leave the publisher, and with it the transport, open.
        try:
            end_subscriber()
        finally:
            end_publisher()


async def _route(
    transport: MoqtTransport, to_subscriber: _Channel[BidirectionalStream], to_publisher: _Channel[BidirectionalStream]
) -> None:
    """Hand each request stream the peer opens to the driver its first message type names."""

    async def route(stream: BidirectionalStream) -> None:
        request_type, replayed = await _peek_request_type(stream)
        driver = to_publisher if request_type in _PUBLISHER_REQUESTS else to_subscriber

        # A request arriving as the transport ends reaches no driver.
        if not driver.push(replayed):
            replayed.readable.cancel()
            replayed.writable.abort()

    try:
        async for stream in transport.incoming_bidirectional_streams():
            spawn(route(stream))
    except Exception:  # the transport went away; each driver's wait_closed reports it
        pass
    finally:
        to_subscriber.end()
        to_publisher.end()


def create_moqt_duplex_session(
    transport: MoqtTransport, config: MoqtDuplexSessionConfig | None = None
) -> MoqtDuplexSession:
    """Run a subscribe driver and a publish driver over one established transport. Requires a running event loop.

    The transport must have negotiated its MOQT version already, as for either driver alone.

    Example::

        solicit = MoqtSessionConfig(setup_options=[KeyValuePair(SETUP_OPTION.SOLICIT, 1)])
        session = create_moqt_duplex_session(transport, MoqtDuplexSessionConfig(subscriber=solicit))
        alice = ["room", "p", "alice"]
        session.publisher.announce(alice)
        session.publisher.register_track(RegisterTrackOptions(track_namespace=alice, track_name="video"))
        session.subscriber.subscribe(MoqtSubscribeOptions(track_namespace=["room", "p", "bob"], track_name="video"))
    """
    return MoqtDuplexSession(transport, config)


__all__ = ["MoqtDuplexSession", "MoqtDuplexSessionConfig", "create_moqt_duplex_session"]
