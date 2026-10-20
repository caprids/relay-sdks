"""In-memory MoQ relay hub for cross-driver tests. Derived from Video.js (Apache-2.0); see packages/moq-core/NOTICE.

A real publish driver on one side, real subscribe driver(s) on the other, the negotiated draft's
bytes in the middle. The hub is transport-agnostic: :meth:`RelayHub.serve_publisher` and
:meth:`RelayHub.serve_subscriber` run the relay's half over any ``MoqtTransport``, and
:meth:`RelayHub.connect_publisher` / :meth:`RelayHub.connect_subscriber` wrap them in in-memory pairs
shaped like a ``RelayTransportFactory``.

Publisher side mirrors moq-relay 0.14.17's announce-and-serve ingest. After the SETUP exchange the
hub opens a bidi stream and solicits announces (SUBSCRIBE_NAMESPACE, empty prefix, odd server-side
request ids) and reads NAMESPACE / NAMESPACE_DONE entries off that stream for the session's lifetime.
Tracks are pulled on demand: one upstream SUBSCRIBE per track (downstream demand dedupes onto it),
with the publisher's SUBSCRIBE_OK assigning the alias its subgroup data streams carry. A proactive
PUBLISH is answered with REQUEST_ERROR NOT_SUPPORTED (0x3) "PUBLISH is not supported" like moq-relay 0.15.8. The
publisher ends a track by FINing the hub's SUBSCRIBE stream (no PUBLISH_DONE ever arrives) and
retracts a namespace with NAMESPACE_DONE; both land in ``track_ends``.

Subscriber side answers SUBSCRIBE with SUBSCRIBE_OK (carrying LARGEST_OBJECT once the track has
content) and forwards each upstream subgroup on one downstream stream, FINed or reset when the upstream
stream is, the way a relay preserves subgroups (a frame reader takes a subgroup's FIN as its group's end),
resolving the request's Location Filter using the draft-22 §3.3.1 rules. FETCH is answered with
REQUEST_ERROR. An upstream FIN ends the track's downstream subscriptions with PUBLISH_DONE, the way a
real relay ends the track for every viewer.

Tracks and demand persist across publisher sessions, so the hub can also observe teardown/reconnect
churn instead of crashing on it.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine, Sequence
from dataclasses import dataclass, field
from typing import Any, ClassVar, Literal

from core_helpers.transport_pair import create_transport_pair

from moq_core.bytes import StreamReader
from moq_core.control_messages import (
    MOQT_PROTOCOL_ID,
    MOQT_PROTOCOL_IDS,
    PUBLISH_DONE_STATUS,
    REQUEST_ERROR_CODE,
    AbsoluteLocationFilter,
    ControlMessage,
    ControlMessageFrame,
    Fetch,
    KeyValuePair,
    Location,
    LocationFilter,
    MessageParameters,
    MoqtProtocolId,
    Namespace,
    NamespaceDone,
    NextObjectFilter,
    Publish,
    RelativeGroupFilter,
    RequestError,
    RequestOk,
    Subscribe,
    SubscribeNamespace,
    SubscribeOk,
    TrackNamespace,
    decode_control_message,
    encode_publish_done,
    encode_request_error,
    encode_request_update,
    encode_setup,
    encode_subscribe,
    encode_subscribe_namespace,
    encode_subscribe_ok,
    resolve_moqt_protocol,
)
from moq_core.object_stream import STREAM_TYPE, is_subgroup_header_type, read_subgroup_header, read_subgroup_objects
from moq_core.subgroup_writer import SubgroupWriter, create_subgroup_writer
from moq_core.transport import BidirectionalStream, MoqtTransport, ReceiveStream
from mux_relay import ConnectingTransport

# =============================================================================
# Types
# =============================================================================


@dataclass(slots=True)
class BufferedObject:
    group_id: int
    object_id: int
    properties: list[KeyValuePair]
    payload: bytes


GroupEnd = Literal["fin", "reset"]
"""How an upstream subgroup stream ended."""


@dataclass(slots=True)
class _BufferedGroup:
    objects: list[BufferedObject] = field(default_factory=list)
    end: GroupEnd | None = None
    """Set once the upstream stream carrying the group has ended."""


@dataclass(slots=True)
class _GroupStream:
    """One group's downstream stream for one subscription."""

    writer: SubgroupWriter | None = None
    done: bool = False
    """A write failed: nothing more is written."""
    chain: asyncio.Task[None] | None = None
    """The latest queued write; each waits for the one before it."""


@dataclass(slots=True)
class _Subscription:
    track_alias: int
    start: Location
    """First object this subscription may receive (inclusive), resolved from its Location Filter at subscribe time."""
    deliver: Callable[[BufferedObject], None]
    end_group: Callable[[int, GroupEnd], None]
    """The group's upstream stream ended: end its downstream stream the same way."""
    end: Callable[[int], None]


@dataclass(slots=True)
class _TrackRecord:
    name: str
    groups: dict[int, _BufferedGroup] = field(default_factory=dict)
    """Insertion-ordered group id -> objects, pruned to the newest few groups. The publish driver sends one subgroup
    per group (subgroup ID 0), so a group is one upstream stream."""
    largest: Location | None = None
    objects_received: int = 0
    subscribers: set[int] = field(default_factory=set)
    subscriptions: dict[int, _Subscription] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ObservedSubscribeFin:
    """A bare FIN on the hub's SUBSCRIBE stream: the publisher's clean track end."""

    track_name: str
    kind: ClassVar[Literal["subscribe-fin"]] = "subscribe-fin"


@dataclass(frozen=True, slots=True)
class ObservedNamespaceDone:
    """A NAMESPACE_DONE retraction on the solicitation stream."""

    namespace: list[str]
    kind: ClassVar[Literal["namespace-done"]] = "namespace-done"


ObservedTrackEnd = ObservedSubscribeFin | ObservedNamespaceDone

MAX_BUFFERED_GROUPS = 3
PUBLISH_REJECTED_ERROR_CODE = REQUEST_ERROR_CODE.NOT_SUPPORTED
"""moq-relay 0.15.8 declines every proactive PUBLISH as ``Error::Unsupported``, which is NOT_SUPPORTED (0x3).

See ``run_publish_stream`` in ``rs/moq-net/src/ietf/subscriber.rs`` at tag ``moq-relay-v0.15.8``; 0.14.17 already
sent 0x3, and the literal 400 this fixture used to send dates from 0.14.7.
"""
SUBSCRIBE_RETRY_DELAY = 0.025
"""Retry spacing when a SUBSCRIBE races the publisher's register_track and comes back DOES_NOT_EXIST."""


# =============================================================================
# Implementation
# =============================================================================


async def read_control_frame(
    reader: StreamReader, message_type: int, protocol: MoqtProtocolId = MOQT_PROTOCOL_ID
) -> ControlMessage:
    """Read one length-framed control message (type varint already consumed)."""
    high = await reader.read_uint8()
    low = await reader.read_uint8()
    body = await reader.read_bytes((high << 8) | low)
    return decode_control_message(ControlMessageFrame(message_type, body), protocol)


def location_of(obj: BufferedObject) -> Location:
    return Location(obj.group_id, obj.object_id)


def is_at_or_after(a: Location, b: Location) -> bool:
    return a.group > b.group or (a.group == b.group and a.object >= b.object)


def resolve_start(location_filter: LocationFilter | None, largest: Location | None) -> Location:
    """Where a new subscription starts, resolved using the draft-22 Location Filter rules."""
    if isinstance(location_filter, NextObjectFilter):
        return Location(largest.group, largest.object + 1) if largest else Location(0, 0)

    if isinstance(location_filter, RelativeGroupFilter):
        next_group = largest.group + 1 if largest else 0
        return Location(max(0, next_group - location_filter.groups_before_next), 0)

    if isinstance(location_filter, AbsoluteLocationFilter):
        return location_filter.start

    return Location(largest.group if largest else 0, 0)


def _completed() -> asyncio.Future[None]:
    future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    future.set_result(None)
    return future


def _spawn(coroutine: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
    task = asyncio.get_running_loop().create_task(coroutine)
    task.add_done_callback(lambda done: done.exception() if not done.cancelled() else None)
    return task


class RelayHub:
    def __init__(self, protocols: Sequence[MoqtProtocolId] = MOQT_PROTOCOL_IDS) -> None:
        self.protocols = tuple(protocols)
        """The drafts the hub speaks, in its preference order; a connection gets the first one the client offers."""
        self.subscribes: list[str] = []
        """Upstream track SUBSCRIBEs sent to the publisher, in order (a retry appends again)."""
        self.negotiated: list[MoqtProtocolId] = []
        """The draft each connection negotiated, in connection order."""
        self.downstream_subscribes: list[Subscribe] = []
        """SUBSCRIBEs received from subscribers, as decoded for each connection's draft."""
        self.track_ends: list[ObservedTrackEnd] = []
        """Every upstream end observed: subscribe-stream FINs and NAMESPACE_DONE retractions."""
        self._destroyed = False
        self._next_subscriber_alias = 1
        self._publisher_connections = 0
        self._tracks: dict[str, _TrackRecord] = {}
        self._closers: list[Callable[[], None]] = []
        self._demand: set[str] = set()
        """Tracks with standing upstream demand (downstream viewers or test priming)."""
        self._pull_upstream: Callable[[str], None] | None = None
        """The live publisher connection's per-track pull, when one is connected."""
        self.upstream_object_delivery_timeout: int | None = None
        """OBJECT_DELIVERY_TIMEOUT to put on upstream SUBSCRIBEs; moq-relay 0.15.8 sends none (#167)."""
        self.upstream_subgroup_delivery_timeout: int | None = None
        """SUBGROUP_DELIVERY_TIMEOUT to put on upstream SUBSCRIBEs; moq-relay 0.15.8 sends none (#167)."""
        self.updates_acknowledged: list[str] = []
        """Tracks whose upstream REQUEST_UPDATE the publisher answered with REQUEST_OK, in order."""
        self._update_upstream: Callable[[str, MessageParameters], Coroutine[Any, Any, None]] | None = None

    # ------------------------------------------------------------------ bookkeeping

    def _track_for(self, name: str) -> _TrackRecord:
        track = self._tracks.get(name)

        if track is None:
            track = _TrackRecord(name=name)
            self._tracks[name] = track

        return track

    def _buffer_and_forward(self, track: _TrackRecord, obj: BufferedObject) -> None:
        group = track.groups.get(obj.group_id)

        if group is None:
            group = _BufferedGroup()
            track.groups[obj.group_id] = group

            while len(track.groups) > MAX_BUFFERED_GROUPS:
                oldest = next(iter(track.groups))
                del track.groups[oldest]

        location = location_of(obj)
        group.objects.append(obj)
        track.objects_received += 1

        if track.largest is None or not is_at_or_after(track.largest, location):
            track.largest = location

        for subscription in list(track.subscriptions.values()):
            if is_at_or_after(location, subscription.start):
                subscription.deliver(obj)

    def _end_group(self, track: _TrackRecord, group_id: int, how: GroupEnd) -> None:
        group = track.groups.get(group_id)

        if group is not None:
            group.end = how

        for subscription in list(track.subscriptions.values()):
            subscription.end_group(group_id, how)

    def subscribe_upstream(self, track_name: str) -> None:
        """Register standing upstream demand for a track without a downstream viewer.

        The stand-in for other viewers' pull in late-join scenarios: an announce-and-serve publisher
        writes nothing until the relay subscribes. Dedupes onto the single live upstream subscription
        and carries across publisher reconnects.
        """
        self._demand.add(track_name)

        if self._pull_upstream is not None:
            self._pull_upstream(track_name)

    async def update_upstream(self, track_name: str, parameters: MessageParameters) -> None:
        """Send REQUEST_UPDATE on the live upstream subscription for a track (moq-relay never does)."""
        assert self._update_upstream is not None, "no publisher connected"
        await self._update_upstream(track_name, parameters)

    def publisher_connections(self) -> int:
        return self._publisher_connections

    def object_count(self, track_name: str) -> int:
        track = self._tracks.get(track_name)
        return track.objects_received if track else 0

    def destroy(self) -> None:
        self._destroyed = True

        for close in list(self._closers):
            close()

        self._closers.clear()

    # ------------------------------------------------------------------ transport factories

    def _select(self, offered: Sequence[str]) -> MoqtProtocolId:
        """The draft a connection speaks: the first one offered that the hub speaks (TA-7), or the hub's first."""
        for protocol in self.protocols:
            if not offered or protocol in offered:
                self.negotiated.append(protocol)
                return protocol

        raise AssertionError(f"the hub speaks none of the offered protocols {list(offered)}")

    def connect_publisher(self, url: str = "", protocols: Sequence[str] = ()) -> ConnectingTransport:
        pair = create_transport_pair(self._select(protocols))
        self.serve_publisher(pair.server)
        self._closers.append(pair.client.close)
        return ConnectingTransport(transport=pair.client, ready=_completed())

    def connect_subscriber(self, url: str = "", protocols: Sequence[str] = ()) -> ConnectingTransport:
        pair = create_transport_pair(self._select(protocols))
        closer = self.serve_subscriber(pair.server)
        self._closers.append(closer)
        self._closers.append(pair.client.close)
        return ConnectingTransport(transport=pair.client, ready=_completed())

    # ------------------------------------------------------------------ publisher side

    def serve_publisher(self, server: MoqtTransport) -> None:
        """Run the relay's announce-and-serve half against a connected publisher."""
        protocol = resolve_moqt_protocol(getattr(server, "protocol_id", None))
        self._publisher_connections += 1
        alias_to_track: dict[int, _TrackRecord] = {}
        next_request_id = [3]
        announced: list[TrackNamespace] = []
        announce_waiters: list[asyncio.Future[TrackNamespace]] = []
        upstream_tracks: set[str] = set()
        upstream_writers: dict[str, Any] = {}
        connection_closed = [False]

        async def update_upstream(track_name: str, parameters: MessageParameters) -> None:
            request_id = next_request_id[0]
            next_request_id[0] += 2
            await upstream_writers[track_name].write(encode_request_update(request_id, parameters, protocol))

        self._update_upstream = update_upstream

        async def announced_namespace() -> TrackNamespace:
            if announced:
                return announced[0]

            waiter: asyncio.Future[TrackNamespace] = asyncio.get_running_loop().create_future()
            announce_waiters.append(waiter)
            return await waiter

        async def solicit_namespaces() -> None:
            stream = await server.create_bidirectional_stream()
            reader = StreamReader(stream.readable)

            try:
                await stream.writable.write(
                    encode_subscribe_namespace(SubscribeNamespace(request_id=1, track_namespace_prefix=[]), protocol)
                )

                while not await reader.at_end():
                    message_type = await reader.read_varint()
                    message = await read_control_frame(reader, message_type, protocol)

                    if isinstance(message, Namespace):
                        # Empty solicited prefix: the suffix IS the full namespace.
                        announced.append(message.track_namespace_suffix)

                        for waiter in announce_waiters:
                            if not waiter.done():
                                waiter.set_result(message.track_namespace_suffix)

                        announce_waiters.clear()
                    elif isinstance(message, NamespaceDone):
                        self.track_ends.append(ObservedNamespaceDone(message.track_namespace_suffix))
                    # RequestOk: the publisher accepting the solicitation.
            except Exception:
                pass  # Reset or transport teardown.

        def subscribe_upstream(track_name: str) -> None:
            if self._destroyed or connection_closed[0] or track_name in upstream_tracks:
                return

            upstream_tracks.add(track_name)

            async def pull() -> None:
                track_namespace = await announced_namespace()
                stream = await server.create_bidirectional_stream()
                reader = StreamReader(stream.readable)
                request_id = next_request_id[0]
                next_request_id[0] += 2
                track = self._track_for(track_name)
                accepted = False

                try:
                    await stream.writable.write(
                        encode_subscribe(
                            Subscribe(
                                request_id=request_id,
                                track_namespace=track_namespace,
                                track_name=track_name,
                                # The exact parameter set moq-relay 0.14.17 and 0.15.8 send upstream on draft-20.
                                parameters=MessageParameters(
                                    forward=1,
                                    subscriber_priority=0,
                                    location_filter=NextObjectFilter(),
                                    fill_parameters=MessageParameters(
                                        location_filter=RelativeGroupFilter(groups_before_next=1)
                                    ),
                                    group_order="descending",
                                    object_delivery_timeout=self.upstream_object_delivery_timeout,
                                    subgroup_delivery_timeout=self.upstream_subgroup_delivery_timeout,
                                ),
                            ),
                            protocol,
                        )
                    )
                    self.subscribes.append(track_name)
                    upstream_writers[track_name] = stream.writable

                    while not await reader.at_end():
                        message_type = await reader.read_varint()
                        message = await read_control_frame(reader, message_type, protocol)

                        if isinstance(message, SubscribeOk):
                            accepted = True
                            alias_to_track[message.track_alias] = track
                        elif isinstance(message, RequestOk):
                            self.updates_acknowledged.append(track_name)
                        elif isinstance(message, RequestError):
                            # DOES_NOT_EXIST usually means the demand raced the publisher's
                            # register_track: retry while the demand stands.
                            if track_name in self._demand and not self._destroyed and not connection_closed[0]:
                                asyncio.get_running_loop().call_later(
                                    SUBSCRIBE_RETRY_DELAY, lambda: subscribe_upstream(track_name)
                                )

                            return

                    if accepted:
                        self.track_ends.append(ObservedSubscribeFin(track_name))

                        for subscription in list(track.subscriptions.values()):
                            subscription.end(PUBLISH_DONE_STATUS.TRACK_ENDED)

                        track.subscriptions.clear()
                except Exception:
                    pass  # Reset or transport teardown: not a clean track end.
                finally:
                    upstream_tracks.discard(track_name)
                    upstream_writers.pop(track_name, None)

                    try:
                        await stream.writable.close()
                    except Exception:
                        pass

            _spawn(pull())

        async def resolve_track(alias: int) -> _TrackRecord | None:
            for _ in range(200):
                track = alias_to_track.get(alias)
                if track is not None:
                    return track

                await asyncio.sleep(0.005)

            return None

        async def handle_uni_stream(value: ReceiveStream) -> None:
            reader = StreamReader(value)

            try:
                stream_type = await reader.read_varint()

                if stream_type == STREAM_TYPE.SETUP:
                    # The publisher's control stream: SETUP and nothing else.
                    while not await reader.at_end():
                        await reader.read_uint8()

                    return

                if not is_subgroup_header_type(stream_type):
                    reader.cancel()
                    return

                header = await read_subgroup_header(reader, stream_type)
                track = await resolve_track(header.track_alias)

                if track is None:
                    reader.cancel()
                    return

                try:
                    async for obj in read_subgroup_objects(reader, header):
                        if obj.status != "normal":
                            continue

                        self._buffer_and_forward(
                            track, BufferedObject(obj.group_id, obj.object_id, obj.properties, obj.payload)
                        )
                except Exception:
                    # Stream reset (drop path or teardown): buffered objects stay, and downstream streams are reset.
                    self._end_group(track, header.group_id, "reset")
                    raise

                self._end_group(track, header.group_id, "fin")
            except Exception:
                pass  # Stream reset or teardown.

        async def accept_uni_streams() -> None:
            try:
                async for value in server.incoming_unidirectional_streams():
                    _spawn(handle_uni_stream(value))
            except Exception:
                pass

        async def reject_request_stream(stream: BidirectionalStream) -> None:
            reader = StreamReader(stream.readable)

            try:
                message_type = await reader.read_varint()
                message = await read_control_frame(reader, message_type, protocol)

                if isinstance(message, Publish):
                    await stream.writable.write(
                        encode_request_error(PUBLISH_REJECTED_ERROR_CODE, "PUBLISH is not supported")
                    )
                else:
                    await stream.writable.write(
                        encode_request_error(REQUEST_ERROR_CODE.NOT_SUPPORTED, "unexpected request stream")
                    )
            except Exception:
                pass
            finally:
                try:
                    await stream.writable.close()
                except Exception:
                    pass

                reader.cancel()

        async def accept_bidi_streams() -> None:
            try:
                async for stream in server.incoming_bidirectional_streams():
                    _spawn(reject_request_stream(stream))
            except Exception:
                pass

        async def start() -> None:
            try:
                control = await server.create_unidirectional_stream()
                await control.write(encode_setup())
            except Exception:
                return

            await solicit_namespaces()

        async def watch_closed() -> None:
            try:
                await server.wait_closed()
            except Exception:
                pass
            finally:
                connection_closed[0] = True

                if self._pull_upstream is subscribe_upstream:
                    self._pull_upstream = None

        _spawn(accept_uni_streams())
        _spawn(accept_bidi_streams())
        _spawn(start())
        _spawn(watch_closed())

        # Standing demand carries across publisher sessions.
        self._pull_upstream = subscribe_upstream

        for track_name in list(self._demand):
            subscribe_upstream(track_name)

    # ------------------------------------------------------------------ subscriber side

    def serve_subscriber(self, server: MoqtTransport) -> Callable[[], None]:
        """Run the relay's downstream half against a connected subscriber. Returns a function that stops forwarding."""
        protocol = resolve_moqt_protocol(getattr(server, "protocol_id", None))
        open_flag = [True]

        def group_streams(
            track_alias: int,
        ) -> tuple[Callable[[BufferedObject], None], Callable[[int, GroupEnd], None]]:
            """One subscription's downstream subgroup streams, one per group: objects are written in order on the
            group's stream, opened by its first object, and the stream ends as the upstream one did, after the objects
            received before the end: a relay forwards what it received before a reset. A failed write ends the group
            for this subscriber, so later objects never reopen it on a second stream."""
            streams: dict[int, _GroupStream] = {}

            def forward(obj: BufferedObject) -> None:
                if not open_flag[0] or self._destroyed:
                    return

                state = streams.setdefault(obj.group_id, _GroupStream())
                previous = state.chain

                async def send() -> None:
                    if previous is not None:
                        await previous

                    if state.done:
                        return

                    try:
                        if state.writer is None:
                            stream = await server.create_unidirectional_stream()
                            state.writer = create_subgroup_writer(
                                stream,
                                track_alias=track_alias,
                                group_id=obj.group_id,
                                has_properties=True,
                                end_of_group=False,
                            )

                        await state.writer.write_object(
                            object_id=obj.object_id, properties=obj.properties, payload=obj.payload
                        )
                    except Exception:
                        state.done = True  # Subscriber went away, or cancelled this stream.

                state.chain = _spawn(send())

            def end(group_id: int, how: GroupEnd) -> None:
                state = streams.pop(group_id, None)

                if state is None:
                    return

                chain = state.chain

                async def finish() -> None:
                    if chain is not None:
                        await chain

                    try:
                        if state.writer is None:
                            return

                        if how == "reset":
                            state.writer.abort()
                        elif not state.done:
                            await state.writer.fin()
                    except Exception:
                        pass  # Subscriber went away.

                _spawn(finish())

            return forward, end

        async def serve_request_stream(stream: BidirectionalStream) -> None:
            reader = StreamReader(stream.readable)
            subscription_key: int | None = None
            subscribed_track: _TrackRecord | None = None

            try:
                while not await reader.at_end():
                    message_type = await reader.read_varint()
                    message = await read_control_frame(reader, message_type, protocol)

                    if isinstance(message, Subscribe):
                        self.downstream_subscribes.append(message)
                        # Downstream demand is what makes the hub pull the track from the publisher.
                        self.subscribe_upstream(message.track_name)
                        track = self._track_for(message.track_name)
                        track_alias = self._next_subscriber_alias
                        self._next_subscriber_alias += 1

                        # Like the relay on draft-20, SUBSCRIBE_OK reports the Largest Object once the
                        # track has content (two bare varints, §9.20.17).
                        await stream.writable.write(
                            encode_subscribe_ok(
                                track_alias,
                                MessageParameters(largest_object=track.largest)
                                if track.largest
                                else MessageParameters(),
                                (),
                                protocol,
                            )
                        )

                        def end(status_code: int, alias: int = track_alias) -> None:
                            async def finish() -> None:
                                try:
                                    await stream.writable.write(encode_publish_done(status_code, 0, ""))
                                    await stream.writable.close()
                                except Exception:
                                    pass

                            _spawn(finish())

                        forward, end_group = group_streams(track_alias)
                        subscription = _Subscription(
                            track_alias=track_alias,
                            start=resolve_start(message.parameters.location_filter, track.largest),
                            deliver=forward,
                            end_group=end_group,
                            end=end,
                        )

                        # Replay whatever the filter reaches back for, then follow live.
                        for group_id in sorted(track.groups):
                            group = track.groups[group_id]

                            for obj in group.objects:
                                if is_at_or_after(location_of(obj), subscription.start):
                                    subscription.deliver(obj)

                            if group.end is not None:
                                subscription.end_group(group_id, group.end)

                        track.subscriptions[track_alias] = subscription
                        subscription_key = track_alias
                        subscribed_track = track
                    elif isinstance(message, Fetch):
                        # The draft-20 player never fetches; anything asking for history is refused.
                        await stream.writable.write(
                            encode_request_error(REQUEST_ERROR_CODE.INVALID_RANGE, "no history")
                        )
                        await stream.writable.close()
                        return
                    # REQUEST_UPDATE / GOAWAY: ignored by the hub.
            except Exception:
                pass  # Reset: the subscriber cancelled; ordinary end-of-request.
            finally:
                if subscribed_track is not None and subscription_key is not None:
                    subscribed_track.subscriptions.pop(subscription_key, None)

                try:
                    await stream.writable.close()
                except Exception:
                    pass

        async def accept_bidi_streams() -> None:
            try:
                async for stream in server.incoming_bidirectional_streams():
                    _spawn(serve_request_stream(stream))
            except Exception:
                pass

        async def drain(value: ReceiveStream) -> None:
            try:
                while await value.read() is not None:
                    pass
            except Exception:
                pass

        async def drain_uni_streams() -> None:
            # The subscriber's control stream (SETUP + GOAWAY) lands here; drain it.
            try:
                async for value in server.incoming_unidirectional_streams():
                    _spawn(drain(value))
            except Exception:
                pass

        async def start() -> None:
            try:
                control = await server.create_unidirectional_stream()
                await control.write(encode_setup())
            except Exception:
                pass

        _spawn(accept_bidi_streams())
        _spawn(drain_uni_streams())
        _spawn(start())

        def stop() -> None:
            open_flag[0] = False

        return stop


def create_relay_hub() -> RelayHub:
    return RelayHub()
