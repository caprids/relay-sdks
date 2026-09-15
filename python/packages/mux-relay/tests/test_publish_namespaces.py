"""Publisher demand deadlines distinguish incompatible namespace solicitations from no viewers."""

import asyncio

import pytest
from core_helpers.raw_peer import solicit_namespace
from core_helpers.transport_pair import create_transport_pair
from core_helpers.waiting import wait_for

from moq_core import MoqtObject, SubscriptionHandlers
from moq_core.session import MoqtSessionConfig, MoqtSubscribeOptions, create_moqt_session
from mux_relay import ConnectingTransport, RelayNamespaceError, RelayTimeoutError, connect_relay_publisher


@pytest.mark.parametrize("prefix", [None, [], ["other"]])
@pytest.mark.parametrize("operation", ["write", "open_group"])
async def test_namespace_demand_deadline_and_recovery(prefix: list[str] | None, operation: str) -> None:
    pair = create_transport_pair()
    peer = create_moqt_session(pair.server, MoqtSessionConfig())
    ready: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    ready.set_result(None)
    publisher = await connect_relay_publisher(
        "https://relay.example.com",
        namespace=["testing", "data"],
        create_transport=lambda url, protocols: ConnectingTransport(transport=pair.client, ready=ready),
    )
    try:
        track = publisher.publish("events", write_timeout=0.05)
        if prefix is not None:
            request = await solicit_namespace(pair.server, prefix)
            await wait_for(lambda: "request-ok" in request.kinds())

        expected = RelayNamespaceError if prefix == ["other"] else RelayTimeoutError
        with pytest.raises(expected):
            if operation == "write":
                await track.write(b"before")
            else:
                await track.open_group()
        assert not track.closed

        if prefix == ["other"]:
            matching = await solicit_namespace(pair.server, ["testing"], 3)
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
