"""One session that publishes and subscribes, ported from ``duplex-session.test.ts`` and its teardown cases."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import pytest
from core_helpers.raw_peer import raw_subscribe, solicit_namespace
from core_helpers.streams import StreamClosedError
from core_helpers.transport_pair import CloseInfo, PairTransport, TransportPair, create_transport_pair
from core_helpers.waiting import settle, wait_for

from moq_core import (
    Closed,
    MoqtDuplexSession,
    MoqtDuplexSessionConfig,
    MoqtPublishSessionCallbacks,
    MoqtPublishSessionConfig,
    MoqtSessionCallbacks,
    MoqtSessionConfig,
    MoqtSubscribeNamespaceOptions,
    MoqtSubscribeOptions,
    RegisterTrackOptions,
    create_moqt_duplex_session,
    create_subgroup_writer,
)
from moq_core.control_messages import (
    MESSAGE_TYPE,
    SETUP_OPTION,
    ControlMessage,
    ControlMessageDeframer,
    Goaway,
    KeyValuePair,
    Namespace,
    NamespaceDone,
    Publish,
    Subscribe,
    SubscribeNamespace,
    decode_control_message,
    encode_control_message,
    encode_publish,
    encode_setup,
    encode_subscribe,
)
from moq_core.errors import SESSION_ERROR, MoqtProtocolError
from moq_core.session import PublishResponder
from moq_core.transport import ReceiveStream, SendStream
from moq_core.varint import decode_varint

OWN = ["room", "p", "alice"]
OTHER = ["room", "p", "bob"]
SOLICIT = KeyValuePair(SETUP_OPTION.SOLICIT, 1)

_tasks: set[asyncio.Task[Any]] = set()


def _run(coroutine: Any) -> None:
    task = asyncio.get_running_loop().create_task(coroutine)
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)


@dataclass
class ClientStreams:
    """The first message type of each stream the client opens, by direction, as the server sees them."""

    uni: list[int] = field(default_factory=list)
    bidi: list[tuple[int, ControlMessage]] = field(default_factory=list)


def watch_client_streams(server: PairTransport) -> ClientStreams:
    seen = ClientStreams()

    async def read_uni(stream: ReceiveStream) -> None:
        chunk = await stream.read()
        if chunk:
            seen.uni.append(decode_varint(chunk)[0])
        while await stream.read() is not None:
            pass

    async def read_bidi(readable: ReceiveStream) -> None:
        chunk = await readable.read()
        if chunk:
            seen.bidi.append((decode_varint(chunk)[0], decode_control_message(ControlMessageDeframer().push(chunk)[0])))

    async def accept_uni() -> None:
        async for stream in server.incoming_unidirectional_streams():
            _run(read_uni(stream))

    async def accept_bidi() -> None:
        async for stream in server.incoming_bidirectional_streams():
            _run(read_bidi(stream.readable))

    _run(accept_uni())
    _run(accept_bidi())
    return seen


@dataclass
class Harness:
    pair: TransportPair
    session: MoqtDuplexSession
    seen: ClientStreams
    control: SendStream

    @property
    def server(self) -> PairTransport:
        return self.pair.server

    def close_info(self) -> CloseInfo | None:
        return self.pair.client._shared.close_info

    async def end(self) -> None:
        """Destroy the session and close the peer's side, so no drain outlives the test."""
        self.session.destroy()
        self.server.close()
        await settle()

    def announce_own(self) -> None:
        self.session.publisher.announce(OWN)
        self.session.publisher.register_track(RegisterTrackOptions(track_namespace=OWN, track_name="video"))


async def harness(config: MoqtDuplexSessionConfig | None = None) -> Harness:
    pair = create_transport_pair()
    seen = watch_client_streams(pair.server)
    session = create_moqt_duplex_session(pair.client, config)
    control = await pair.server.create_unidirectional_stream()
    await control.write(encode_setup([SOLICIT]))
    await asyncio.gather(session.subscriber.ready, session.publisher.ready)
    return Harness(pair, session, seen, control)


def subscriber_callbacks(**callbacks: Any) -> MoqtDuplexSessionConfig:
    return MoqtDuplexSessionConfig(subscriber=MoqtSessionConfig(callbacks=MoqtSessionCallbacks(**callbacks)))


async def solicited(h: Harness) -> Any:
    """Announce OWN on the peer's solicitation of every namespace and wait for the NAMESPACE entry."""
    h.announce_own()
    solicitation = await solicit_namespace(h.server, [], 1)
    await wait_for(lambda: len(solicitation.received) == 2)
    return solicitation


def namespace_done(solicitation: Any) -> bool:
    return any(isinstance(message, NamespaceDone) for message in solicitation.received)


async def offer(server: PairTransport, data: bytes) -> SendStream:
    stream = await server.create_bidirectional_stream()
    await stream.writable.write(data)
    return stream.writable


PUBLISH = encode_publish(Publish(request_id=5, track_namespace=OTHER, track_name="offered", track_alias=9))


async def test_sends_one_setup_with_the_subscribers_options_and_readies_both_sides_on_the_peers() -> None:
    server_options: list[list[KeyValuePair]] = []
    h = await harness(
        MoqtDuplexSessionConfig(
            subscriber=MoqtSessionConfig(
                setup_options=[SOLICIT], callbacks=MoqtSessionCallbacks(on_ready=server_options.append)
            )
        )
    )

    await wait_for(lambda: h.seen.uni == [MESSAGE_TYPE.SETUP])
    await settle()
    assert h.seen.uni == [MESSAGE_TYPE.SETUP]
    assert server_options == [[SOLICIT]]
    await h.end()


async def test_announces_on_the_peers_solicitation_and_serves_its_subscribe_with_data_on_the_same_transport() -> None:
    h = await harness()
    h.announce_own()

    solicitation = await solicit_namespace(h.server, ["room"], 1)
    await wait_for(lambda: solicitation.kinds() == ["request-ok", "namespace"])
    assert solicitation.received[1] == Namespace(track_namespace_suffix=["p", "alice"])

    subscribe = await raw_subscribe(h.server, OWN, "video", 3)
    await wait_for(lambda: subscribe.kinds() == ["subscribe-ok"])

    writer = create_subgroup_writer(
        await h.session.publisher.open_uni_stream(3), track_alias=3, group_id=0, has_properties=True
    )
    await writer.write_object(object_id=0, payload=b"\x01")
    await writer.fin()
    await wait_for(lambda: len(h.seen.uni) == 2)
    await h.end()


async def test_reports_announces_through_the_publisher_config() -> None:
    announced: list[list[str]] = []
    h = await harness(
        MoqtDuplexSessionConfig(
            publisher=MoqtPublishSessionConfig(callbacks=MoqtPublishSessionCallbacks(on_announced=announced.append))
        )
    )

    await solicited(h)
    await wait_for(lambda: announced == [OWN])
    await h.end()


async def test_sends_this_endpoints_requests_from_the_subscriber_with_its_own_request_ids() -> None:
    h = await harness()

    h.session.subscriber.subscribe_namespace(MoqtSubscribeNamespaceOptions(track_namespace_prefix=["room", "p"]))
    h.session.subscriber.subscribe(MoqtSubscribeOptions(track_namespace=OTHER, track_name="video"))

    await wait_for(lambda: len(h.seen.bidi) == 2)
    assert sorted(request_type for request_type, _ in h.seen.bidi) == sorted(
        [MESSAGE_TYPE.SUBSCRIBE, MESSAGE_TYPE.SUBSCRIBE_NAMESPACE]
    )
    requests = [message for _, message in h.seen.bidi if isinstance(message, Subscribe | SubscribeNamespace)]
    assert sorted(request.request_id for request in requests) == [0, 2]
    await h.end()


async def test_routes_the_peers_publish_to_the_subscriber() -> None:
    offered: list[str] = []

    def on_incoming_publish(publish: Publish, responder: PublishResponder) -> None:
        offered.append(publish.track_name)
        responder.reject()

    h = await harness(subscriber_callbacks(on_incoming_publish=on_incoming_publish))

    await offer(h.server, PUBLISH)
    await wait_for(lambda: offered == ["offered"])
    await h.end()


async def test_hands_the_peers_goaway_to_both_sides() -> None:
    subscriber_goaways: list[Goaway] = []
    publisher_goaways: list[Goaway] = []
    h = await harness(
        MoqtDuplexSessionConfig(
            subscriber=MoqtSessionConfig(callbacks=MoqtSessionCallbacks(on_goaway=subscriber_goaways.append)),
            publisher=MoqtPublishSessionConfig(
                callbacks=MoqtPublishSessionCallbacks(on_goaway=publisher_goaways.append)
            ),
        )
    )

    goaway = Goaway(new_session_uri="https://relay.example.com/next", timeout=1500)
    await h.control.write(encode_control_message(goaway))
    await wait_for(lambda: len(subscriber_goaways) == 1 and len(publisher_goaways) == 1)
    assert subscriber_goaways == publisher_goaways == [goaway]
    await h.end()


async def test_ends_the_whole_session_when_the_publisher_closes_after_retracting_its_announces() -> None:
    closed: list[Closed] = []
    h = await harness(subscriber_callbacks(on_closed=closed.append))
    solicitation = await solicited(h)

    h.session.publisher.close()
    await wait_for(lambda: namespace_done(solicitation) and len(closed) == 1)


async def test_ends_the_subscriber_at_once_with_the_error_and_leaves_the_transport_to_the_publishers_drain() -> None:
    subscriber_closed: list[Closed] = []
    publisher_closed: list[Closed] = []
    h = await harness(
        MoqtDuplexSessionConfig(
            subscriber=MoqtSessionConfig(callbacks=MoqtSessionCallbacks(on_closed=subscriber_closed.append)),
            publisher=MoqtPublishSessionConfig(
                callbacks=MoqtPublishSessionCallbacks(on_closed=publisher_closed.append)
            ),
        )
    )
    solicitation = await solicited(h)

    reason = RuntimeError("going")
    h.session.destroy(reason)
    h.session.destroy()
    assert subscriber_closed == [Closed(reason)]
    # The retraction still reaches the peer: the subscriber's end did not close the transport under the drain.
    await wait_for(lambda: namespace_done(solicitation) and publisher_closed == [Closed(reason)])


@pytest.mark.parametrize(
    "end",
    [
        pytest.param(lambda session: session.subscriber.close(), id="close"),
        pytest.param(lambda session: session.subscriber.destroy(), id="destroy"),
    ],
)
async def test_lets_the_publisher_drain_when_the_subscribers_own_end_ends_the_session(
    end: Callable[[MoqtDuplexSession], None],
) -> None:
    publisher_closed: list[Closed] = []
    h = await harness(
        MoqtDuplexSessionConfig(
            publisher=MoqtPublishSessionConfig(callbacks=MoqtPublishSessionCallbacks(on_closed=publisher_closed.append))
        )
    )
    solicitation = await solicited(h)

    end(h.session)
    await wait_for(lambda: len(publisher_closed) == 1)
    assert namespace_done(solicitation)


async def test_closes_the_transport_with_the_code_and_reason_the_subscribers_close_was_given() -> None:
    h = await harness()
    await solicited(h)

    h.session.subscriber.close(0x10, "done here")
    await wait_for(lambda: h.close_info() is not None)
    assert h.close_info() == CloseInfo(0x10, "done here")


async def test_closes_the_session_on_a_peer_request_id_reused_across_the_two_drivers() -> None:
    closed: list[Closed] = []
    h = await harness(subscriber_callbacks(on_closed=closed.append))

    subscribe = await raw_subscribe(h.server, OWN, "video", 1)
    await wait_for(lambda: len(subscribe.received) == 1)
    await offer(h.server, encode_publish(Publish(request_id=1, track_namespace=OTHER, track_name="x", track_alias=9)))

    await wait_for(lambda: len(closed) == 1)
    assert isinstance(closed[0].error, MoqtProtocolError)
    assert closed[0].error.code == SESSION_ERROR.INVALID_REQUEST_ID


async def test_routes_a_request_stream_with_an_unknown_nine_byte_type_at_once_instead_of_buffering_it() -> None:
    closed: list[Closed] = []
    h = await harness(subscriber_callbacks(on_closed=closed.append))

    writer = await offer(h.server, b"\xff" * 9)
    try:
        await writer.write(bytes(64))
    except StreamClosedError:
        pass

    # The subscriber reads it and refuses the session; nothing waits for more bytes.
    await wait_for(lambda: len(closed) == 1)
    assert closed[0].error is not None


async def test_waits_for_a_whole_nine_byte_request_type_before_routing_it() -> None:
    h = await harness()
    await solicited(h)

    # SUBSCRIBE (0x03) spelled with MOQT's longest prefix, 0xff and eight value bytes, split after eight bytes.
    subscribe = encode_subscribe(Subscribe(request_id=3, track_namespace=OWN, track_name="video"))
    data = bytes([0xFF, 0, 0, 0, 0, 0, 0, 0, 0x03]) + subscribe[1:]
    stream = await h.server.create_bidirectional_stream()
    await stream.writable.write(data[:8])
    await settle(0.01)
    await stream.writable.write(data[8:])

    response = await stream.readable.read()
    assert response is not None
    assert decode_control_message(ControlMessageDeframer().push(response)[0]).kind == "subscribe-ok"
    await h.end()


async def test_reports_a_clean_peer_close_as_clean() -> None:
    pair = create_transport_pair()
    watch_client_streams(pair.server)
    closed: list[Closed] = []
    create_moqt_duplex_session(pair.client, subscriber_callbacks(on_closed=closed.append))
    control = await pair.server.create_unidirectional_stream()
    await control.write(encode_setup([]))
    await settle(0.01)

    pair.server.close(0, "")
    await wait_for(lambda: len(closed) == 1)
    assert closed == [Closed()]


async def test_delivers_nothing_more_to_the_subscriber_once_destroyed_while_the_publisher_drains() -> None:
    offered: list[Publish] = []
    h = await harness(subscriber_callbacks(on_incoming_publish=lambda publish, _: offered.append(publish)))
    await solicited(h)

    h.session.destroy()
    try:
        await offer(h.server, PUBLISH)
    except StreamClosedError:
        pass
    await settle(0.05)
    assert offered == []
    await h.end()


async def test_drops_a_request_to_the_subscriber_still_arriving_when_it_is_destroyed() -> None:
    order: list[str] = []
    h = await harness(
        subscriber_callbacks(
            on_incoming_publish=lambda publish, _: order.append("publish"), on_closed=lambda _: order.append("closed")
        )
    )
    await solicited(h)

    writer = await offer(h.server, PUBLISH[:1])
    await settle()
    h.session.destroy()
    try:
        await writer.write(PUBLISH[1:])
    except StreamClosedError:
        pass
    await settle(0.05)
    assert order == ["closed"]
    await h.end()


async def test_still_drains_and_closes_the_publisher_when_a_subscriber_callback_raises_during_destroy() -> None:
    publisher_closed: list[Closed] = []

    def on_closed(_: Closed) -> None:
        raise RuntimeError("application bug")

    h = await harness(
        MoqtDuplexSessionConfig(
            subscriber=MoqtSessionConfig(callbacks=MoqtSessionCallbacks(on_closed=on_closed)),
            publisher=MoqtPublishSessionConfig(
                callbacks=MoqtPublishSessionCallbacks(on_closed=publisher_closed.append)
            ),
        )
    )
    solicitation = await solicited(h)

    with pytest.raises(RuntimeError, match="application bug"):
        h.session.destroy()
    await wait_for(lambda: namespace_done(solicitation) and len(publisher_closed) == 1)


async def test_ends_the_publisher_cleanly_when_the_transport_closes_before_its_synthetic_streams_end() -> None:
    publisher_closed: list[Closed] = []
    h = await harness(
        MoqtDuplexSessionConfig(
            publisher=MoqtPublishSessionConfig(callbacks=MoqtPublishSessionCallbacks(on_closed=publisher_closed.append))
        )
    )

    h.server.close(0, "")
    await wait_for(lambda: len(publisher_closed) == 1)
    # Not "peer closed its control stream": the driver had ended before its control stream did.
    assert publisher_closed == [Closed()]
