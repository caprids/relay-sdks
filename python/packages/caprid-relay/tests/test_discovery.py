"""The supervised subscriber's per-connection discovery index."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Sequence
from typing import cast

import pytest
from core_helpers.fake_transport import FakeTransport, create_fake_transport
from relay_helpers.supervision_fakes import FakeDiscovery, FakeSubscriber, until

from caprid_relay import (
    SNAPSHOT_QUIET,
    ConnectingTransport,
    NamespaceEvents,
    RelayError,
    RequestError,
    SubscriptionConnectionClosedError,
    SubscriptionRequestError,
    _discovery,
    connect_relay_subscriber,
)
from caprid_relay._discovery import DiscoveryIndex, DiscoveryWatchHandlers, covers
from moq_core.control_messages import SubscribeNamespace, encode_namespace, encode_request_ok


async def settle() -> None:
    for _ in range(10):
        await asyncio.sleep(0)


def unauthorized() -> SubscriptionRequestError:
    return SubscriptionRequestError(RequestError(error_code=0x1, retry_interval=0, reason="unauthorized"))


class Scripted:
    """An index over a fake connection that refuses an overlapping request the way the real one does."""

    def __init__(self) -> None:
        self.fake = FakeSubscriber()
        self.refuse: set[tuple[str, ...]] = set()
        self.index = DiscoveryIndex(self._open)

    @property
    def discoveries(self) -> list[FakeDiscovery]:
        return self.fake.discoveries

    def _open(self, prefix: tuple[str, ...]) -> NamespaceEvents:
        if prefix in self.refuse:
            raise RelayError("Relay connection is closed", code="CLOSED")
        for held in self.discoveries:
            if not held.closed and not held.failed and (covers(held.prefix, prefix) or covers(prefix, held.prefix)):
                raise RelayError("overlap", code="PREFIX_OVERLAP", phase="subscription")
        return cast(NamespaceEvents, self.fake.namespace_events(prefix))


class Heard:
    def __init__(self) -> None:
        self.seen: list[str] = []
        self.handlers = DiscoveryWatchHandlers(
            on_announced=lambda namespace: self.seen.append("+" + "/".join(namespace)),
            on_unannounced=lambda namespace: self.seen.append("-" + "/".join(namespace)),
            on_loaded=lambda: self.seen.append("loaded"),
            on_ended=lambda error: self.seen.append(f"ended {type(error).__name__ if error else None}"),
        )


async def test_serves_a_watch_under_an_open_request_by_filtering_it() -> None:
    s = Scripted()
    wide = Heard()
    s.index.watch(("r",), wide.handlers)
    s.discoveries[0].push("announced", ["r", "a", "g1"])
    s.discoveries[0].push("announced", ["r", "b", "g1"])
    await settle()
    narrow = Heard()
    watch = s.index.watch(("r", "a"), narrow.handlers)
    assert watch.namespaces() == [("r", "a", "g1")]
    assert not watch.loaded
    s.discoveries[0].push("announced", ["r", "a", "g2"])
    s.discoveries[0].push("announced", ["r", "b", "g2"])
    s.discoveries[0].push("loaded")
    await settle()
    assert len(s.discoveries) == 1
    assert narrow.seen == ["+r/a/g2", "loaded"]
    assert wide.seen == ["+r/a/g1", "+r/b/g1", "+r/a/g2", "+r/b/g2", "loaded"]
    assert watch.loaded


async def test_replaces_narrower_requests_break_before_make_and_reports_only_the_differences_at_its_load() -> None:
    s = Scripted()
    narrow = Heard()
    moved = s.index.watch(("r", "a"), narrow.handlers)
    first = s.discoveries[0]
    first.push("announced", ["r", "a", "g1"])
    first.push("announced", ["r", "a", "g2"])
    first.push("loaded")
    await settle()
    wide = Heard()
    covering = s.index.watch(("r",), wide.handlers)
    assert first.closed
    assert s.discoveries[1].prefix == ("r",)
    assert covering.namespaces() == [("r", "a", "g1"), ("r", "a", "g2")]
    assert moved.loaded
    assert not moved.following
    second = s.discoveries[1]
    second.push("announced", ["r", "a", "g1"])
    second.push("announced", ["r", "a", "g3"])
    second.push("loaded")
    await settle()
    assert narrow.seen == ["+r/a/g1", "+r/a/g2", "loaded", "+r/a/g3", "-r/a/g2", "loaded"]
    assert wide.seen == ["+r/a/g3", "-r/a/g2", "loaded"]
    assert moved.following


async def test_ends_the_watches_it_moved_after_the_call_when_the_covering_request_cannot_open() -> None:
    s = Scripted()
    narrow = Heard()
    moved = s.index.watch(("r", "a"), narrow.handlers)
    s.discoveries[0].push("loaded")
    await settle()
    s.refuse.add(("r",))
    with pytest.raises(RelayError):
        s.index.watch(("r",), DiscoveryWatchHandlers())
    assert narrow.seen == ["loaded"]
    assert moved.ended is not None
    assert isinstance(moved.ended.error, SubscriptionConnectionClosedError)
    await settle()
    assert narrow.seen == ["loaded", "ended SubscriptionConnectionClosedError"]


async def test_reports_a_request_ending_to_its_watches_and_asks_again_for_the_next_watch() -> None:
    s = Scripted()
    first = Heard()
    watch = s.index.watch(("r",), first.handlers)
    refusal = unauthorized()
    s.discoveries[0].fail(refusal)
    await settle()
    assert first.seen == ["ended SubscriptionRequestError"]
    assert watch.ended is not None
    assert watch.ended.error is refusal
    s.index.watch(("r",), DiscoveryWatchHandlers())
    assert len(s.discoveries) == 2


async def test_moves_narrower_watches_to_a_request_of_their_own_when_theirs_ends_holding_their_view() -> None:
    s = Scripted()
    narrow = Heard()
    moved = s.index.watch(("r", "a"), narrow.handlers)
    s.discoveries[0].push("announced", ["r", "a", "g1"])
    s.discoveries[0].push("loaded")
    await settle()
    wide = Heard()
    s.index.watch(("r",), wide.handlers)
    deeper = Heard()
    filtered = s.index.watch(("r", "a", "g2"), deeper.handlers)
    covering = s.discoveries[1]
    covering.push("announced", ["r", "a", "g1"])
    covering.push("announced", ["r", "a", "g2"])
    covering.push("announced", ["r", "b", "g1"])
    await settle()
    covering.fail(unauthorized())
    await settle()
    assert wide.seen == ["+r/a/g2", "+r/b/g1", "ended SubscriptionRequestError"]
    assert [discovery.prefix for discovery in s.discoveries] == [("r", "a"), ("r",), ("r", "a")]
    assert moved.ended is None
    assert moved.loaded
    assert moved.namespaces() == [("r", "a", "g1"), ("r", "a", "g2")]
    assert not filtered.loaded
    rehomed = s.discoveries[2]
    rehomed.push("announced", ["r", "a", "g2"])
    rehomed.push("loaded")
    await settle()
    assert narrow.seen == ["+r/a/g1", "loaded", "+r/a/g2", "-r/a/g1", "loaded"]
    assert deeper.seen == ["+r/a/g2", "loaded"]


def _factory(fake: FakeTransport) -> Callable[[str, Sequence[str]], ConnectingTransport]:
    ready: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    ready.set_result(None)
    return lambda url, protocols: ConnectingTransport(transport=fake, ready=ready)


async def test_moves_a_narrower_watch_holding_its_view_when_the_relay_finishes_the_covering_request() -> None:
    fake = create_fake_transport()
    connecting = asyncio.ensure_future(
        connect_relay_subscriber("https://relay.example.com/moq", create_transport=_factory(fake))
    )
    fake.send_server_setup()
    connection = await connecting
    try:
        index = DiscoveryIndex(lambda prefix: connection.namespace_events(prefix))
        wide = Heard()
        index.watch(("r",), wide.handlers)
        narrow = Heard()
        moved = index.watch(("r", "a"), narrow.handlers)
        covering = await fake.next_request_stream()
        await covering.first_message
        await covering.send(encode_request_ok())
        await covering.send(encode_namespace(["a", "g1"]))
        await until(lambda: "loaded" in narrow.seen, "the covering request loads")
        await covering.fin()
        own = await fake.next_request_stream()
        message = await own.first_message
        assert isinstance(message, SubscribeNamespace)
        assert message.track_namespace_prefix == ["r", "a"]
        assert moved.namespaces() == [("r", "a", "g1")]
        await own.send(encode_request_ok())
        await own.send(encode_namespace(["g1"]))
        await asyncio.sleep(SNAPSHOT_QUIET * 2)
        assert wide.seen == ["+r/a/g1", "loaded", "ended None"]
        assert narrow.seen == ["+r/a/g1", "loaded", "loaded"]
    finally:
        connection.close()


async def test_ends_a_narrower_watch_whose_own_request_cannot_open_after_the_wider_one_ended() -> None:
    s = Scripted()
    wide = Heard()
    s.index.watch(("r",), wide.handlers)
    narrow = Heard()
    watch = s.index.watch(("r", "a"), narrow.handlers)
    s.refuse.add(("r", "a"))
    s.discoveries[0].fail(unauthorized())
    await settle()
    assert wide.seen == ["ended SubscriptionRequestError"]
    assert narrow.seen == ["ended SubscriptionConnectionClosedError"]
    assert watch.ended is not None
    assert isinstance(watch.ended.error, SubscriptionConnectionClosedError)


async def test_keeps_a_request_a_while_after_its_last_watch_for_reuse_then_closes_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_discovery, "DISCOVERY_LINGER", 0.1)
    s = Scripted()
    s.index.watch(("r",), DiscoveryWatchHandlers()).release()
    await asyncio.sleep(0.03)
    again = s.index.watch(("r", "a"), DiscoveryWatchHandlers())
    assert len(s.discoveries) == 1
    again.release()
    again.release()
    await asyncio.sleep(0.06)
    assert not s.discoveries[0].closed
    await asyncio.sleep(0.1)
    assert s.discoveries[0].closed
    s.index.watch(("r",), DiscoveryWatchHandlers())
    assert len(s.discoveries) == 2


async def test_ends_every_watch_when_the_connection_ends_and_refuses_new_ones() -> None:
    s = Scripted()
    one, two = Heard(), Heard()
    s.index.watch(("r",), one.handlers)
    s.index.watch(("s",), two.handlers)
    s.index.close()
    s.index.close()
    assert one.seen == ["ended SubscriptionConnectionClosedError"]
    assert two.seen == ["ended SubscriptionConnectionClosedError"]
    assert all(discovery.closed for discovery in s.discoveries)
    with pytest.raises(RelayError):
        s.index.watch(("r",), DiscoveryWatchHandlers())


async def test_isolates_a_handler_that_raises(caplog: pytest.LogCaptureFixture) -> None:
    s = Scripted()

    def boom(_namespace: tuple[str, ...]) -> None:
        raise RuntimeError("boom")

    s.index.watch(("r",), DiscoveryWatchHandlers(on_announced=boom))
    other = Heard()
    s.index.watch(("r", "a"), other.handlers)
    with caplog.at_level(logging.ERROR, logger="caprid_relay"):
        s.discoveries[0].push("announced", ["r", "a", "g1"])
        await settle()
    assert other.seen == ["+r/a/g1"]
    assert len(caplog.records) == 1
