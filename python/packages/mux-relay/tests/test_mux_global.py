"""Exercise the guide's actual demo, including its timeout and connection ownership."""

import asyncio
from dataclasses import replace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from mux_global import demo
from relay_helpers.relay_hub import create_relay_hub

from mux_relay import RelayTimeoutError, RelayTrackWriter, connect_relay_publisher, connect_relay_subscriber

CONFIG = demo.Config(
    url="https://relay.example.com",
    token="test-token",
    namespace=["quickstart", "test"],
    track="ticker",
    mode="both",
    count=1,
    interval=0.001,
    timeout=2,
)


@pytest.fixture
async def connected(monkeypatch: pytest.MonkeyPatch):
    hub = create_relay_hub()
    connections = []

    async def publisher(url: str, **kwargs: Any):
        result = await connect_relay_publisher(url, **kwargs, create_transport=hub.connect_publisher)
        connections.append(result)
        return result

    async def subscriber(url: str, **kwargs: Any):
        result = await connect_relay_subscriber(url, **kwargs, create_transport=hub.connect_subscriber)
        connections.append(result)
        return result

    monkeypatch.setattr(demo, "connect_relay_publisher", publisher)
    monkeypatch.setattr(demo, "connect_relay_subscriber", subscriber)
    try:
        yield connections
    finally:
        hub.destroy()


async def test_one_object_round_trip_closes_connections(connected):
    entries = []
    assert await demo.run_demo(CONFIG, entries.append) == {"published": 1, "received": 1}
    assert any(entry.get("seq") == 1 and entry["event"] == "received" for entry in entries)
    assert len(connected) == 2
    assert all(connection.closed.done() for connection in connected)
    for connection in connected:
        assert (await connection.closed).error is None


async def test_deadline_waiting_for_demand_never_attempts_write(connected, monkeypatch: pytest.MonkeyPatch):
    write = AsyncMock()
    monkeypatch.setattr(RelayTrackWriter, "write", write)
    with pytest.raises(TimeoutError, match=r"Run exceeded 0\.05 s"):
        await demo.run_demo(replace(CONFIG, mode="publish", timeout=0.05), lambda _: None)
    write.assert_not_awaited()
    assert all(connection.closed.done() for connection in connected)


async def test_partial_delivery_timeout_is_not_replayed(connected, monkeypatch: pytest.MonkeyPatch):
    original = RelayTrackWriter.write
    attempts = 0
    failure = RelayTimeoutError("transport write timed out after partial progress")

    async def partial_write(self, payload, **kwargs):
        nonlocal attempts
        attempts += 1
        await original(self, payload, **kwargs)  # Bytes reached the relay before the reported failure.
        raise failure

    monkeypatch.setattr(RelayTrackWriter, "write", partial_write)
    with pytest.raises(RelayTimeoutError) as caught:
        await demo.run_demo(CONFIG, lambda _: None)
    assert caught.value is failure
    assert attempts == 1
    assert all(connection.closed.done() for connection in connected)


async def test_second_connection_failure_closes_publisher(connected, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(demo, "connect_relay_subscriber", AsyncMock(side_effect=RuntimeError("handshake failed")))
    with pytest.raises(RuntimeError, match="subscriber failed to connect"):
        await demo.run_demo(CONFIG, lambda _: None)
    assert len(connected) == 1
    assert connected[0].closed.done()


async def test_subscriber_first_startup(connected):
    async with asyncio.TaskGroup() as tasks:
        subscriber = tasks.create_task(demo.run_demo(replace(CONFIG, mode="subscribe", timeout=4), lambda _: None))
        await asyncio.sleep(0.1)
        publisher = tasks.create_task(demo.run_demo(replace(CONFIG, mode="publish"), lambda _: None))
    assert subscriber.result() == {"published": 0, "received": 1}
    assert publisher.result() == {"published": 1, "received": 0}
    assert all(connection.closed.done() for connection in connected)


def test_cli_failure_is_nonzero_and_redacts_token(monkeypatch: pytest.MonkeyPatch, capsys):
    monkeypatch.setattr(demo, "parse_args", lambda _: CONFIG)
    monkeypatch.setattr(demo, "run_demo", AsyncMock(side_effect=RuntimeError(f"failure with {CONFIG.token}")))
    assert demo.main([]) == 1
    output = capsys.readouterr()
    assert "failed" in output.err and "[redacted]" in output.err
    assert CONFIG.token not in output.out + output.err


async def test_demo_over_native_aioquic(monkeypatch: pytest.MonkeyPatch):
    from relay_helpers.wt_server import start_webtransport_server

    from mux_relay import connect_webtransport

    hub = create_relay_hub()

    def serve(session):
        if session.path.startswith("/publish"):
            hub.serve_publisher(session)
        else:
            hub.serve_subscriber(session)

    server = await start_webtransport_server(serve)

    def transport(url, protocols):
        return connect_webtransport(url, protocols, configuration=server.client_configuration())

    async def publisher(_url, **kwargs):
        return await connect_relay_publisher(server.url_for("/publish"), **kwargs, create_transport=transport)

    async def subscriber(_url, **kwargs):
        return await connect_relay_subscriber(server.url_for("/subscribe"), **kwargs, create_transport=transport)

    monkeypatch.setattr(demo, "connect_relay_publisher", publisher)
    monkeypatch.setattr(demo, "connect_relay_subscriber", subscriber)
    try:
        assert await demo.run_demo(CONFIG, lambda _: None) == {"published": 1, "received": 1}
    finally:
        hub.destroy()
        server.close()
        await asyncio.sleep(0.05)


async def test_fresh_namespace_rejection_is_retried(connected, monkeypatch: pytest.MonkeyPatch):
    from types import SimpleNamespace

    from mux_relay import RelaySubscriber, RequestError, SubscriptionRequestError

    original = RelaySubscriber.subscribe
    attempts = 0

    def subscribe(self, namespace, track, handlers):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            # The relay's answer arrives through `subscribed`: a fresh namespace is still a 404.
            loop = asyncio.get_running_loop()
            subscribed = loop.create_future()
            subscribed.set_exception(
                SubscriptionRequestError(RequestError(error_code=404, retry_interval=0, reason="Broadcast not found"))
            )
            return SimpleNamespace(subscribed=subscribed, ended=loop.create_future(), cancel=lambda reason=None: None)
        return original(self, namespace, track, handlers)

    monkeypatch.setattr(RelaySubscriber, "subscribe", subscribe)
    entries = []
    assert await demo.run_demo(CONFIG, entries.append) == {"published": 1, "received": 1}
    assert attempts == 2
    assert {"event": "waiting", "detail": "namespace not announced yet; retrying the subscribe"} in entries
    assert all(connection.closed.done() for connection in connected)


async def test_track_closed_while_waiting_for_demand(connected, monkeypatch: pytest.MonkeyPatch):
    from mux_relay import RelayPublisher

    original = RelayPublisher.register_track
    write = AsyncMock()
    monkeypatch.setattr(RelayTrackWriter, "write", write)

    def register_track(self, track, **kwargs):
        writer = original(self, track, **kwargs)
        asyncio.get_running_loop().call_soon(writer.close)
        return writer

    monkeypatch.setattr(RelayPublisher, "register_track", register_track)
    with pytest.raises(RuntimeError, match="Published track closed while waiting for demand"):
        await demo.run_demo(replace(CONFIG, mode="publish"), lambda _: None)
    write.assert_not_awaited()
    assert all(connection.closed.done() for connection in connected)
