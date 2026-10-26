"""Exercise the guide's actual demo, including its timeout and connection ownership."""

import asyncio
from dataclasses import replace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from mux_global import demo
from relay_helpers.relay_hub import create_relay_hub

from caprid_relay import (
    RelayError,
    RelaySubscriber,
    RelayTrackWriter,
    SubscriptionEnd,
    connect_relay_publisher,
    connect_supervised_subscriber,
)
from moq_core import RequestError

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

    # The demo's supervised subscriber connects through the hub, attempt after attempt.
    async def subscriber(url: str, **kwargs: Any):
        result = await connect_supervised_subscriber(url, **kwargs, create_transport=hub.connect_subscriber)
        connections.append(result)
        return result

    monkeypatch.setattr(demo, "connect_relay_publisher", publisher)
    monkeypatch.setattr(demo, "connect_supervised_subscriber", subscriber)
    try:
        yield connections
    finally:
        hub.destroy()


async def test_one_object_round_trip_closes_connections(connected):
    entries = []
    assert await demo.run_demo(CONFIG, entries.append) == {"published": 1, "received": 1}
    # The combined run logs the announce reaching the wire, before the relay accepts the subscription.
    assert entries.index({"event": "announced", "namespace": "quickstart/test"}) < next(
        index for index, entry in enumerate(entries) if entry["event"] == "subscribed"
    )
    assert any(entry.get("seq") == 1 and entry["event"] == "received" for entry in entries)
    assert len(connected) == 2
    assert all(connection.closed.done() for connection in connected)
    for connection in connected:
        assert (await connection.closed).error is None


async def test_deadline_ends_the_wait_for_demand_before_anything_is_sent(connected):
    entries = []
    with pytest.raises(TimeoutError, match=r"Run exceeded 0\.05 s"):
        await demo.run_demo(replace(CONFIG, mode="publish", timeout=0.05), entries.append)
    # The SDK was still waiting for a subscriber inside write() when the run deadline cancelled it.
    assert {"event": "waiting", "detail": "no relay demand yet; write() waits for a subscriber"} in entries
    assert not any(entry["event"] == "published" for entry in entries)
    assert all(connection.closed.done() for connection in connected)


async def test_demand_timeout_writes_the_same_object_again(connected, monkeypatch: pytest.MonkeyPatch):
    original = RelayTrackWriter.write
    payloads = []

    async def write(self, payload, **kwargs):
        payloads.append(payload)
        if len(payloads) == 1:  # The write deadline expired before any subscriber asked for the track.
            raise RelayError("no demand", code="WRITE_TIMEOUT", phase="waiting-for-demand")
        await original(self, payload, **kwargs)

    monkeypatch.setattr(RelayTrackWriter, "write", write)
    entries = []
    assert await demo.run_demo(CONFIG, entries.append) == {"published": 1, "received": 1}
    assert len(payloads) == 2 and payloads[0] == payloads[1]
    assert any("nothing was sent" in entry.get("detail", "") for entry in entries)
    assert not any(entry["event"] == "write failed" for entry in entries)


async def test_partial_delivery_timeout_is_not_replayed(connected, monkeypatch: pytest.MonkeyPatch):
    original = RelayTrackWriter.write
    attempts = 0
    failure = RelayError("transport write timed out after partial progress", code="WRITE_TIMEOUT", phase="writing")

    async def partial_write(self, payload, **kwargs):
        nonlocal attempts
        attempts += 1
        await original(self, payload, **kwargs)  # Bytes reached the relay before the reported failure.
        raise failure

    monkeypatch.setattr(RelayTrackWriter, "write", partial_write)
    entries = []
    with pytest.raises(RelayError) as caught:
        await demo.run_demo(CONFIG, entries.append)
    assert caught.value is failure
    assert attempts == 1
    assert [entry for entry in entries if entry["event"] == "write failed"] == [
        {
            "event": "write failed",
            "code": "WRITE_TIMEOUT",
            "phase": "writing",
            "detail": "sending had started; the relay may hold the object, so it is not resent",
        }
    ]
    assert all(connection.closed.done() for connection in connected)


async def test_second_connection_failure_closes_publisher(connected, monkeypatch: pytest.MonkeyPatch):
    # A refusal on the first attempt is not retried: the supervised subscriber raises it.
    refused = RelayError("refused", code="UNAUTHORIZED", phase="connecting")
    monkeypatch.setattr(demo, "connect_supervised_subscriber", AsyncMock(side_effect=refused))
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


async def test_demo_over_native_quic(monkeypatch: pytest.MonkeyPatch):
    from relay_helpers.quic_hub import start_quic_hub

    hub = create_relay_hub()
    servers = await start_quic_hub(hub)

    async def publisher(_url, **kwargs):
        transport = servers.publishers.factory()
        return await connect_relay_publisher(servers.publishers.url(), **kwargs, create_transport=transport)

    async def subscriber(_url, **kwargs):
        transport = servers.subscribers.factory()
        return await connect_supervised_subscriber(servers.subscribers.url(), **kwargs, create_transport=transport)

    monkeypatch.setattr(demo, "connect_relay_publisher", publisher)
    monkeypatch.setattr(demo, "connect_supervised_subscriber", subscriber)
    try:
        assert await demo.run_demo(CONFIG, lambda _: None) == {"published": 1, "received": 1}
    finally:
        hub.destroy()
        await servers.close()


async def test_track_closed_while_waiting_for_demand(connected, monkeypatch: pytest.MonkeyPatch):
    from caprid_relay import RelayPublisher

    original = RelayPublisher.register_track

    def register_track(self, track, **kwargs):
        writer = original(self, track, **kwargs)
        asyncio.get_running_loop().call_later(0.05, writer.close)  # once write() is waiting for demand
        return writer

    monkeypatch.setattr(RelayPublisher, "register_track", register_track)
    entries = []
    with pytest.raises(RelayError) as caught:
        await demo.run_demo(replace(CONFIG, mode="publish"), entries.append)
    # Nothing was sent, and the closed track is not a demand timeout: no retry, no uncertain delivery.
    assert (caught.value.code, caught.value.phase) == ("CLOSED", "waiting-for-demand")
    assert not any(entry["event"] in ("published", "write failed") for entry in entries)
    assert all(connection.closed.done() for connection in connected)


async def test_a_rejection_no_wait_fixes_interrupts_the_wait_for_demand(connected, monkeypatch: pytest.MonkeyPatch):
    class Rejected:
        """A subscription the relay refuses with UNAUTHORIZED, a rejection the want does not retry."""

        def __init__(self) -> None:
            loop = asyncio.get_running_loop()
            self._ended: asyncio.Future[SubscriptionEnd] = loop.create_future()
            rejection = RequestError(error_code=0x1, retry_interval=0, reason="forbidden")
            end = SubscriptionEnd(kind="rejected", cause="request-error", accepted=False, error=rejection)
            loop.call_later(0.01, self._ended.set_result, end)
            self.ended = self._ended

        def cancel(self, reason: object = None) -> None:
            pass

    monkeypatch.setattr(RelaySubscriber, "subscribe", lambda self, *args, **kwargs: Rejected())
    loop = asyncio.get_running_loop()
    started = loop.time()
    # The write deadline is 15 s and the run's 2 s: the surfaced rejection must end the wait first.
    with pytest.raises(RuntimeError, match="subscribe rejected: code 1 forbidden"):
        await demo.run_demo(CONFIG, lambda _: None)
    assert loop.time() - started < 1
    assert all(connection.closed.done() for connection in connected)
