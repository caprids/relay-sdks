"""``connect_relay``: one connection that publishes and subscribes, ported from ``ts/tests/relay-session.test.ts``."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

import pytest
from core_helpers.fake_transport import encode_subgroup
from core_helpers.raw_peer import raw_subscribe, solicit_namespace
from core_helpers.transport_pair import PairTransport, create_transport_pair
from core_helpers.waiting import settle, wait_for
from relay_helpers.acked_transport import AckedTransport, AckGate

from caprid_relay import (
    ConnectingTransport,
    RelayError,
    RelaySession,
    RelaySubscriber,
    RelaySubscriptionHandlers,
    connect_relay,
)
from moq_core import Closed, MoqtObject
from moq_core.bytes import StreamReader
from moq_core.control_messages import (
    SETUP_OPTION,
    ControlMessageDeframer,
    ControlMessageFrame,
    KeyValuePair,
    NamespaceDone,
    Setup,
    Subscribe,
    decode_control_message,
    encode_goaway,
    encode_setup,
    encode_subscribe_ok,
)
from moq_core.object_stream import STREAM_TYPE
from moq_core.transport import BidirectionalStream, ReceiveStream

URL = "https://relay.example.com/room"
OWN = ["room", "p", "alice"]
HOST = ["room", "host"]
OTHER = ["room", "p", "bob"]

_tasks: set[asyncio.Task[Any]] = set()


def _run(coroutine: Any) -> None:
    task = asyncio.get_running_loop().create_task(coroutine)
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)


@dataclass
class ScriptedRelay:
    """What the scripted relay saw of the client."""

    setup_options: list[KeyValuePair] = field(default_factory=list)
    subgroup_streams: int = 0
    subscribes: list[str] = field(default_factory=list)


def scripted_relay(server: PairTransport) -> ScriptedRelay:
    """A relay on one transport.

    It sends SETUP, records the client's SETUP options, counts the client's subgroup streams, and answers each
    client SUBSCRIBE with SUBSCRIBE_OK and one object.
    """
    seen = ScriptedRelay()

    async def read_uni(stream: ReceiveStream) -> None:
        reader = StreamReader(stream)
        stream_type = await reader.read_varint()
        if stream_type == STREAM_TYPE.SETUP:
            length = await reader.read_uint8() << 8 | await reader.read_uint8()
            message = decode_control_message(ControlMessageFrame(stream_type, await reader.read_bytes(length)))
            if isinstance(message, Setup):
                seen.setup_options = list(message.options)
        else:
            seen.subgroup_streams += 1
        while not await reader.at_end():
            await reader.read_uint8()

    async def serve(stream: BidirectionalStream) -> None:
        chunk = await stream.readable.read()
        frames = ControlMessageDeframer().push(chunk) if chunk else []
        message = decode_control_message(frames[0]) if frames else None
        if not isinstance(message, Subscribe):
            return
        seen.subscribes.append(f"{'/'.join(message.track_namespace)}:{message.track_name}")
        alias = 100 + message.request_id
        await stream.writable.write(encode_subscribe_ok(alias))
        data = await server.create_unidirectional_stream()
        await data.write(encode_subgroup(alias, 0, b"hello"))
        await data.close()

    async def accept_uni() -> None:
        async for stream in server.incoming_unidirectional_streams():
            _run(read_uni(stream))

    async def accept_bidi() -> None:
        async for stream in server.incoming_bidirectional_streams():
            _run(serve(stream))

    async def send_setup() -> None:
        control = await server.create_unidirectional_stream()
        await control.write(encode_setup([KeyValuePair(SETUP_OPTION.SOLICIT, 1)]))

    _run(accept_uni())
    _run(accept_bidi())
    _run(send_setup())
    return seen


@dataclass
class Connected:
    session: RelaySession
    server: PairTransport
    seen: ScriptedRelay


@pytest.fixture
async def connections() -> AsyncIterator[list[Connected]]:
    opened: list[Connected] = []
    yield opened

    for connected in opened:
        connected.server.close()
        await connected.session.close()


async def connect(connections: list[Connected], acknowledgements: AckGate | None = None) -> Connected:
    pair = create_transport_pair()
    seen = scripted_relay(pair.server)
    ready: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    ready.set_result(None)
    client = pair.client if acknowledgements is None else AckedTransport(pair.client, acknowledgements)
    session = await connect_relay(
        URL, create_transport=lambda url, protocols: ConnectingTransport(transport=client, ready=ready)
    )
    connected = Connected(session, pair.server, seen)
    connections.append(connected)
    return connected


async def test_publishes_and_subscribes_on_one_connection_with_one_setup(connections: list[Connected]) -> None:
    c = await connect(connections)
    publication = c.session.publish(OWN)
    writer = publication.register_track("video")
    solicitation = await solicit_namespace(c.server, ["room"], 1)
    await publication.announced
    await wait_for(lambda: solicitation.kinds() == ["request-ok", "namespace"])

    upstream = await raw_subscribe(c.server, OWN, "video", 3)
    await wait_for(lambda: upstream.kinds() == ["subscribe-ok"])
    await writer.write(b"\x01\x02\x03")
    await wait_for(lambda: c.seen.subgroup_streams == 1)

    objects: list[MoqtObject] = []
    c.session.subscribe(OTHER, "video", RelaySubscriptionHandlers(on_object=objects.append))
    await wait_for(lambda: [o.payload for o in objects] == [b"hello"])
    assert c.seen.subscribes == ["room/p/bob:video"]
    # SOLICIT, but not ACTIVE_COUNT: the publishing side sends no counts.
    declared = [option.type for option in c.seen.setup_options]
    assert SETUP_OPTION.SOLICIT in declared
    assert SETUP_OPTION.ACTIVE_COUNT not in declared


async def test_announces_several_namespaces_and_closes_one_publication_without_the_other(
    connections: list[Connected],
) -> None:
    c = await connect(connections)
    own = c.session.publish(OWN)
    host = c.session.publish(HOST)
    own.register_track("video")
    host.register_track("control")
    with pytest.raises(ValueError):
        c.session.publish(OWN)
    solicitation = await solicit_namespace(c.server, ["room"], 1)
    await asyncio.gather(own.announced, host.announced)

    await host.close()
    assert await host.closed == Closed()
    await wait_for(lambda: solicitation.received[-1] == NamespaceDone(track_namespace_suffix=["host"]))
    with pytest.raises(RelayError):
        host.register_track("other")
    with pytest.raises(ValueError):
        c.session.publish(HOST)
    own.register_track("audio")


async def test_closing_one_publication_does_not_wait_for_another_publications_unacknowledged_end(
    connections: list[Connected],
) -> None:
    acknowledgements = AckGate()
    c = await connect(connections, acknowledgements)
    own = c.session.publish(OWN)
    host = c.session.publish(HOST)
    video = own.register_track("video")
    host.register_track("control", write_timeout=5)
    solicitation = await solicit_namespace(c.server, ["room"], 1)
    await asyncio.gather(own.announced, host.announced)
    upstream = await raw_subscribe(c.server, OWN, "video", 3)
    await wait_for(lambda: upstream.kinds() == ["subscribe-ok"])

    video.close()
    await wait_for(lambda: upstream.kinds() == ["subscribe-ok", "publish-done"])

    await asyncio.wait_for(host.close(), 1)
    await wait_for(lambda: solicitation.received[-1] == NamespaceDone(track_namespace_suffix=["host"]))

    closing = asyncio.ensure_future(own.close())
    await settle(0.1)
    assert not closing.done(), "a publication's close still waits for its own end"
    acknowledgements.release()
    await asyncio.wait_for(closing, 1)


async def test_ends_only_the_publication_whose_namespace_the_relay_stops_soliciting(
    connections: list[Connected],
) -> None:
    c = await connect(connections)
    own = c.session.publish(OWN)
    host = c.session.publish(HOST)
    own.register_track("video")
    control = host.register_track("control")
    await solicit_namespace(c.server, ["room", "p"], 1)
    hosts = await solicit_namespace(c.server, ["room", "host"], 3)
    await asyncio.gather(own.announced, host.announced)

    hosts.reset()
    error = (await host.closed).error
    assert isinstance(error, RelayError)
    assert error.code == "NAMESPACE_ENDED"
    with pytest.raises(RelayError) as write_failure:
        await control.write(b"\x01")
    assert write_failure.value is error
    with pytest.raises(RelayError) as register_failure:
        host.register_track("another")
    assert register_failure.value is error

    # The connection and the other publication carry on.
    await settle()
    assert not c.session.closed.done()
    own.register_track("audio")


async def test_retracts_every_announce_on_close_and_ends_the_connection(connections: list[Connected]) -> None:
    c = await connect(connections)
    own = c.session.publish(OWN)
    own.register_track("video")
    solicitation = await solicit_namespace(c.server, [], 1)
    await own.announced

    await c.session.close()
    assert await c.session.closed == Closed()
    assert await own.closed == Closed()
    await wait_for(lambda: any(isinstance(message, NamespaceDone) for message in solicitation.received))
    with pytest.raises(RelayError):
        c.session.publish(HOST)


async def test_checks_the_namespace_before_connection_state(connections: list[Connected]) -> None:
    c = await connect(connections)
    with pytest.raises(ValueError):
        c.session.publish(["field"] * 33)

    await c.session.close()
    with pytest.raises(ValueError):
        c.session.publish(["field"] * 33)


async def test_fails_an_open_group_with_the_namespace_loss_not_a_subscription_change(
    connections: list[Connected],
) -> None:
    c = await connect(connections)
    host = c.session.publish(HOST)
    control = host.register_track("control")
    hosts = await solicit_namespace(c.server, ["room", "host"], 1)
    await host.announced
    upstream = await raw_subscribe(c.server, HOST, "control", 3)
    await wait_for(lambda: upstream.kinds() == ["subscribe-ok"])
    group = await control.open_group(b"\x01")

    hosts.reset()
    error = (await host.closed).error
    assert isinstance(error, RelayError)
    assert error.code == "NAMESPACE_ENDED"
    with pytest.raises(RelayError) as failure:
        await group.write(b"\x02")
    assert failure.value is error


async def test_refuses_a_publication_once_closing_has_begun(connections: list[Connected]) -> None:
    c = await connect(connections)
    own = c.session.publish(OWN)
    own.register_track("video")
    await solicit_namespace(c.server, [], 1)
    await own.announced

    closing = asyncio.ensure_future(c.session.close())
    await asyncio.sleep(0)
    with pytest.raises(RelayError):
        c.session.publish(HOST)
    await closing


async def test_ends_the_connection_on_a_goaway_the_relay_sends_on_its_solicitation(
    connections: list[Connected],
) -> None:
    c = await connect(connections)
    own = c.session.publish(OWN)
    own.register_track("video")
    solicitation = await solicit_namespace(c.server, [], 1)
    await own.announced

    await solicitation.send(encode_goaway(1000))
    error = (await c.session.closed).error
    assert isinstance(error, RelayError)
    assert error.code == "GOAWAY"
    assert await own.closed == Closed(error)


async def test_is_a_context_manager_whose_exit_closes_the_connection(connections: list[Connected]) -> None:
    c = await connect(connections)
    own = c.session.publish(OWN)
    own.register_track("video")

    async with c.session:
        pass

    assert await c.session.closed == Closed()
    assert await own.closed == Closed()
    # A session shares the subscriber's surface but not its synchronous close().
    assert not isinstance(c.session, RelaySubscriber)
