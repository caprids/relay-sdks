"""Publisher demand deadlines distinguish incompatible namespace solicitations from no viewers."""

import asyncio

import pytest
from core_helpers.raw_peer import solicit_namespace
from core_helpers.streams import StreamClosedError
from core_helpers.transport_pair import create_transport_pair, with_server_request_ids
from core_helpers.waiting import wait_for

from moq_core import MoqtObject, SubscriptionHandlers
from moq_core.session import MoqtSessionConfig, MoqtSubscribeOptions, create_moqt_session
from mux_relay import ConnectingTransport, RelayError, connect_relay_publisher


@pytest.mark.parametrize("prefix", [None, [], ["other"]])
@pytest.mark.parametrize("operation", ["write", "open_group"])
async def test_namespace_demand_deadline_and_recovery(prefix: list[str] | None, operation: str) -> None:
    pair = create_transport_pair()
    peer = create_moqt_session(with_server_request_ids(pair.server), MoqtSessionConfig())
    ready: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    ready.set_result(None)
    publisher = await connect_relay_publisher(
        "https://relay.example.com",
        namespace=["testing", "data"],
        create_transport=lambda url, protocols: ConnectingTransport(transport=pair.client, ready=ready),
    )
    try:
        track = publisher.register_track("events", write_timeout=0.05)
        if prefix is not None:
            # Raw ids clear of the peer session's (1, 3, ...): one session, one request id space (§10.1).
            request = await solicit_namespace(pair.server, prefix, 101)
            await wait_for(lambda: "request-ok" in request.kinds())

        with pytest.raises(RelayError) as failure:
            if operation == "write":
                await track.write(b"before")
            else:
                await track.open_group()
        assert failure.value.code == ("NAMESPACE_NOT_SOLICITED" if prefix == ["other"] else "WRITE_TIMEOUT")
        assert failure.value.phase == "waiting-for-demand"
        assert not track.closed

        if prefix == ["other"]:
            matching = await solicit_namespace(pair.server, ["testing"], 103)
            await wait_for(lambda: "namespace" in matching.kinds())
        objects: list[MoqtObject] = []
        peer.subscribe(
            MoqtSubscribeOptions(track_namespace=["testing", "data"], track_name="events"),
            SubscriptionHandlers(on_object=objects.append),
        )
        await track.write(b"after")
        await wait_for(lambda: len(objects) == 1)
        assert objects[0].payload == b"after"
    finally:
        await publisher.close()
        peer.destroy()


async def test_announced_waits_for_a_solicitation_that_covers_the_namespace() -> None:
    pair = create_transport_pair()
    peer = create_moqt_session(with_server_request_ids(pair.server), MoqtSessionConfig())
    ready: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    ready.set_result(None)
    publisher = await connect_relay_publisher(
        "https://relay.example.com",
        namespace=["testing", "data"],
        create_transport=lambda url, protocols: ConnectingTransport(transport=pair.client, ready=ready),
    )
    try:
        publisher.register_track("events")
        other = await solicit_namespace(pair.server, ["other"])
        await wait_for(lambda: "request-ok" in other.kinds())
        await asyncio.sleep(0.02)
        # Solicited, but not for us: the announce has nowhere to go and the fact has not happened.
        assert not publisher.announced.done()

        matching = await solicit_namespace(pair.server, ["testing"], 103)
        await wait_for(lambda: "namespace" in matching.kinds())

        assert await asyncio.wait_for(publisher.announced, 1) is None
    finally:
        await publisher.close()
        peer.destroy()


async def test_announced_stays_pending_for_an_uncovered_namespace_until_the_connection_closes() -> None:
    pair = create_transport_pair()
    peer = create_moqt_session(with_server_request_ids(pair.server), MoqtSessionConfig())
    ready: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    ready.set_result(None)
    publisher = await connect_relay_publisher(
        "https://relay.example.com",
        namespace=["testing", "data"],
        create_transport=lambda url, protocols: ConnectingTransport(transport=pair.client, ready=ready),
    )
    try:
        publisher.register_track("events")
        other = await solicit_namespace(pair.server, ["other"])
        await wait_for(lambda: "request-ok" in other.kinds())
        await asyncio.sleep(0.02)
        assert not publisher.announced.done()
    finally:
        await publisher.close()
        peer.destroy()

    with pytest.raises(RelayError) as failure:
        await publisher.announced
    assert failure.value.code == "CLOSED"


async def test_namespace_withdrawal_is_shared_with_closure_and_pending_operations() -> None:
    pair = create_transport_pair()
    peer = create_moqt_session(with_server_request_ids(pair.server), MoqtSessionConfig())
    ready: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    ready.set_result(None)
    publisher = await connect_relay_publisher(
        "https://relay.example.com",
        namespace=["testing", "data"],
        create_transport=lambda url, protocols: ConnectingTransport(transport=pair.client, ready=ready),
    )
    try:
        track = publisher.register_track("events")
        solicitation = await solicit_namespace(pair.server, ["testing"])
        await wait_for(lambda: "namespace" in solicitation.kinds())
        pending = asyncio.create_task(track.write(b"pending"))
        await asyncio.sleep(0)
        solicitation.reset()
        closed = await publisher.closed
        assert isinstance(closed.error, RelayError)
        assert closed.error.code == "NAMESPACE_ENDED"
        assert closed.error.phase == "connection"
        assert isinstance(closed.error.__cause__, Exception)
        # The withdrawal carries the peer's reset as its cause.
        assert isinstance(closed.error.__cause__.__cause__, StreamClosedError)
        with pytest.raises(RelayError) as failure:
            await pending
        assert failure.value is closed.error
        with pytest.raises(RelayError) as failure:
            await track.write(b"later")
        assert failure.value is closed.error
        with pytest.raises(RelayError) as failure:
            publisher.register_track("another")
        assert failure.value is closed.error
    finally:
        await publisher.close()
        peer.destroy()


@pytest.mark.parametrize("release_after_turns", [0, 5])
async def test_transport_end_with_a_live_announce_is_a_transport_error(release_after_turns: int) -> None:
    # Issue #159: an idle timeout fails the solicitation stream with the transport's error. That is
    # connection loss, not the relay withdrawing the namespace, whichever of the stream failure and
    # the closure the session sees first.
    pair = create_transport_pair()
    pair.client.release_after_turns = release_after_turns
    peer = create_moqt_session(with_server_request_ids(pair.server), MoqtSessionConfig())
    ready: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    ready.set_result(None)
    publisher = await connect_relay_publisher(
        "https://relay.example.com",
        namespace=["testing", "data"],
        create_transport=lambda url, protocols: ConnectingTransport(transport=pair.client, ready=ready),
    )
    try:
        publisher.register_track("events")
        solicitation = await solicit_namespace(pair.server, ["testing"])
        await wait_for(lambda: "namespace" in solicitation.kinds())
        await publisher.announced

        idle = ConnectionError("QUIC connection terminated: Idle timeout (code 0x1)")
        pair.client.terminate(idle)
        closed = await asyncio.wait_for(publisher.closed, 1)
        assert isinstance(closed.error, RelayError)
        assert closed.error.code == "TRANSPORT_ERROR"
        assert closed.error.phase == "connection"
        causes: list[BaseException] = []
        cause = closed.error.__cause__
        while cause is not None:
            causes.append(cause)
            cause = cause.__cause__
        assert idle in causes
    finally:
        await publisher.close()
        peer.destroy()
