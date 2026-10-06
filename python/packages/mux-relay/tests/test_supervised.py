"""The supervised publisher and subscriber over scripted connections.

Mirrors ``ts/tests/supervised.test.ts``: each case drives what the relay would do to one connection,
and the review-fix cases keep the regressions the TypeScript review passes found.
"""

from __future__ import annotations

import asyncio
import contextlib
import warnings
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, cast

import pytest
from relay_helpers.supervision_fakes import FakePublisher, FakeSubscriber, until

from moq_core.errors import MoqtProtocolError
from mux_relay import (
    CancellationToken,
    RelayError,
    RequestError,
    SubscriptionConnectionClosedError,
    SubscriptionRequestError,
    SupervisedPublisher,
    SupervisedSubscriber,
    SupervisionBackoff,
    WantHandlers,
)
from mux_relay._supervision import RESUBSCRIBE_BACKOFF, _Backoff, backoff_delay, new_generation
from mux_relay.supervised_publisher import _supervise_relay_publisher
from mux_relay.supervised_subscriber import _supervise_relay_subscriber

FAST = SupervisionBackoff(initial_delay=0, max_delay=0)
SLOW = SupervisionBackoff(initial_delay=60, max_delay=60)


@pytest.fixture
async def cleanup() -> AsyncIterator[list[Callable[[], object]]]:
    closers: list[Callable[[], object]] = []
    yield closers
    for close in closers:
        result = close()
        if isinstance(result, Awaitable):
            await result


def request_error(code: int, reason: str) -> SubscriptionRequestError:
    return SubscriptionRequestError(RequestError(error_code=code, retry_interval=0, reason=reason))


# ------------------------------------------------------------------ helpers


def test_generations_sort_by_start_time() -> None:
    older = new_generation(1_000)
    newer = new_generation(1_001)
    assert len(older) == 13
    assert newer > older
    assert new_generation(1_791_244_800_000) > new_generation(1_791_158_400_000)


def test_backoff_doubles_up_to_the_cap_from_its_upper_half() -> None:
    backoff = _Backoff(0.1, 0.4)
    for attempt, ceiling in ((0, 0.1), (1, 0.2), (2, 0.4), (5, 0.4)):
        for _ in range(20):
            assert ceiling / 2 <= backoff_delay(attempt, backoff) <= ceiling
    assert _Backoff(0.25, 8.0) == RESUBSCRIBE_BACKOFF


# ------------------------------------------------------------------ the publisher


class Publishers:
    def __init__(self) -> None:
        self.connections: list[FakePublisher] = []
        self.statuses: list[tuple[str, BaseException | None]] = []
        self.generations: list[tuple[str, ...]] = []
        self.gate: asyncio.Event | None = None
        self.gate_call = 0
        self.calls = 0

    async def connect(self, namespace: tuple[str, ...], _attempt: CancellationToken) -> Any:
        self.calls += 1
        if self.gate is not None and self.calls == self.gate_call:
            await self.gate.wait()
        fake = FakePublisher(namespace)
        self.connections.append(fake)
        return fake


async def start_publisher(
    cleanup: list[Callable[[], object]], publishers: Publishers | None = None
) -> tuple[SupervisedPublisher, Publishers]:
    publishers = publishers or Publishers()
    publisher = await _supervise_relay_publisher(
        publishers.connect,
        ["room", "alice"],
        reconnect=FAST,
        on_status=lambda status, error: publishers.statuses.append((status, error)),
        on_generation=lambda info: publishers.generations.append(info.namespace),
    )
    cleanup.append(publisher.close)
    return publisher, publishers


async def test_serves_the_root_plus_a_generation_field(cleanup: list[Callable[[], object]]) -> None:
    publisher, p = await start_publisher(cleanup)
    assert len(p.connections) == 1
    root0, root1, generation = p.connections[0].namespace
    assert (root0, root1) == ("room", "alice")
    assert publisher.generation is not None
    assert publisher.generation.generation == generation
    assert publisher.generation.namespace == ("room", "alice", generation)
    assert p.generations == [("room", "alice", generation)]
    assert publisher.status == "live"


async def test_reconnects_as_a_newer_generation_and_rebinds_the_writer(cleanup: list[Callable[[], object]]) -> None:
    publisher, p = await start_publisher(cleanup)
    writer = publisher.register_track("video")
    await writer.write(b"\x01")
    assert p.connections[0].tracks["video"].writes == [b"\x01"]

    lost = RelayError("gone", code="TRANSPORT_ERROR")
    p.connections[0].end(lost)
    await until(lambda: len(p.connections) == 2 and publisher.status == "live", "second generation")
    assert ("reconnecting", lost) in p.statuses
    assert len(p.generations) == 2
    assert p.generations[1][2] > p.generations[0][2]
    await writer.write(b"\x02")
    assert p.connections[1].tracks["video"].writes == [b"\x02"]
    assert not writer.closed


async def test_a_write_between_generations_fails_with_reconnecting(cleanup: list[Callable[[], object]]) -> None:
    gated = Publishers()
    gated.gate = asyncio.Event()
    gated.gate_call = 2
    publisher, p = await start_publisher(cleanup, gated)
    writer = publisher.register_track("audio")
    p.connections[0].end(RuntimeError("lost"))
    await until(lambda: publisher.status == "reconnecting", "reconnecting")
    with pytest.raises(RelayError) as raised:
        await writer.write(b"\x09")
    assert (raised.value.code, raised.value.phase) == ("RECONNECTING", "connection")
    assert not writer.has_demand
    gated.gate.set()
    await until(lambda: publisher.status == "live", "live again")
    assert p.connections[1].tracks["audio"].writes == []


async def test_an_interrupted_operation_is_reconnecting_not_closed(cleanup: list[Callable[[], object]]) -> None:
    publisher, p = await start_publisher(cleanup)
    writer = publisher.register_track("video")
    p.connections[0].tracks["video"].fail_next = RelayError("slow", code="WRITE_TIMEOUT", phase="waiting-for-demand")
    with pytest.raises(RelayError, match="slow"):
        await writer.write(b"\x01")
    # The write fails a moment before the connection reports its end, as it does on a real transport loss.
    p.connections[0].tracks["video"].fail_next = RelayError("Published track is closed", code="CLOSED")
    asyncio.get_running_loop().call_later(0.02, p.connections[0].end, RuntimeError("lost"))
    with pytest.raises(RelayError) as raised:
        await writer.write(b"\x01")
    assert raised.value.code == "RECONNECTING"


async def test_closed_stays_closed_while_the_generation_is_live(cleanup: list[Callable[[], object]]) -> None:
    publisher, p = await start_publisher(cleanup)
    writer = publisher.register_track("v")
    p.connections[0].tracks["v"].fail_next = RelayError("Publish operation aborted", code="CLOSED", phase="writing")
    with pytest.raises(RelayError) as raised:
        await writer.write(b"\x01")
    assert raised.value.code == "CLOSED"
    assert publisher.status == "live"


async def test_a_group_maps_its_own_generation_ending_to_reconnecting(cleanup: list[Callable[[], object]]) -> None:
    publisher, p = await start_publisher(cleanup)
    writer = publisher.register_track("video")
    group = await writer.open_group()
    await group.write(b"key")
    assert group.object_count == 1
    p.connections[0].end(RuntimeError("lost"))
    await until(lambda: len(p.connections) == 2 and publisher.status == "live", "second generation")
    # The group belongs to the generation that ended, even though a new one is live now.
    with pytest.raises(RelayError) as raised:
        await group.write(b"delta")
    assert raised.value.code == "RECONNECTING"
    fresh = await writer.open_group()
    await fresh.write(b"key")
    await fresh.close()
    assert fresh.closed
    # A group the application aborted keeps the primitive's own failure.
    aborted = await writer.open_group()
    aborted.abort()
    with pytest.raises(RelayError) as raised:
        await aborted.write(b"late")
    assert raised.value.code == "CLOSED"


async def test_demand_spans_generations(cleanup: list[Callable[[], object]]) -> None:
    publisher, p = await start_publisher(cleanup)
    writer = publisher.register_track("video")
    changes = writer.demand_changes()
    assert await anext(changes) is False
    p.connections[0].tracks["video"].set_demand(True)
    assert await anext(changes) is True
    p.connections[0].end(RuntimeError("lost"))
    assert await anext(changes) is False
    await until(lambda: len(p.connections) == 2 and publisher.status == "live", "second generation")
    p.connections[1].tracks["video"].set_demand(True)
    assert await anext(changes) is True
    writer.close()
    with pytest.raises(StopAsyncIteration):
        await anext(changes)


async def test_track_names_stay_unique_and_closed_tracks_stay_closed(cleanup: list[Callable[[], object]]) -> None:
    publisher, p = await start_publisher(cleanup)
    writer = publisher.register_track("a")
    with pytest.raises(ValueError, match="already registered"):
        publisher.register_track("a")
    with pytest.raises(TypeError):
        publisher.register_track("")
    writer.close()
    with pytest.raises(ValueError, match="already registered"):
        publisher.register_track("a")
    with pytest.raises(RelayError) as raised:
        await writer.write(b"\x01")
    assert raised.value.code == "CLOSED"
    p.connections[0].end(RuntimeError("lost"))
    await until(lambda: len(p.connections) == 2 and publisher.status == "live", "second generation")
    assert "a" not in p.connections[1].tracks


async def test_a_track_added_between_generations_joins_the_next(cleanup: list[Callable[[], object]]) -> None:
    gated = Publishers()
    gated.gate = asyncio.Event()
    gated.gate_call = 2
    publisher, p = await start_publisher(cleanup, gated)
    p.connections[0].end(RuntimeError("lost"))
    await until(lambda: publisher.status == "reconnecting", "reconnecting")
    publisher.register_track("late")
    gated.gate.set()
    await until(lambda: publisher.status == "live", "live again")
    assert "late" in p.connections[1].tracks


async def test_the_first_attempt_raises_a_credential_refusal_and_retries_the_rest(
    cleanup: list[Callable[[], object]],
) -> None:
    refused = RelayError("no", code="UNAUTHORIZED", phase="connecting")

    async def refuse(_namespace: tuple[str, ...], _attempt: CancellationToken) -> Any:
        raise refused

    with pytest.raises(RelayError) as raised:
        await _supervise_relay_publisher(refuse, ["room"], reconnect=FAST)
    assert raised.value is refused

    calls = 0

    async def flaky(namespace: tuple[str, ...], _attempt: CancellationToken) -> Any:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise RelayError("down", code="TRANSPORT_ERROR", phase="connecting")
        return FakePublisher(namespace)

    publisher = await _supervise_relay_publisher(flaky, ["room"], reconnect=FAST)
    cleanup.append(publisher.close)
    assert calls == 3


async def test_close_stops_the_publisher(cleanup: list[Callable[[], object]]) -> None:
    publisher, p = await start_publisher(cleanup)
    writer = publisher.register_track("video")
    await publisher.close()
    assert p.connections[0].close_calls == 1
    assert writer.closed
    assert publisher.status == "closed"
    assert publisher.generation is None
    assert (await publisher.closed).error is None
    assert p.statuses[-1] == ("closed", None)
    await asyncio.sleep(0.01)
    assert len(p.connections) == 1
    with pytest.raises(RelayError):
        publisher.register_track("more")


async def test_cancellation_stops_it_before_or_after_the_first_generation() -> None:
    owner = CancellationToken()
    reason = RuntimeError("owner done")
    p = Publishers()
    publisher = await _supervise_relay_publisher(p.connect, ["room"], reconnect=FAST, cancellation=owner)
    owner.cancel(reason)
    assert (await publisher.closed).error is reason
    assert p.connections[0].close_calls == 1

    never = CancellationToken()

    async def hang(_namespace: tuple[str, ...], attempt: CancellationToken) -> Any:
        await asyncio.Event().wait()

    pending = asyncio.ensure_future(_supervise_relay_publisher(hang, ["room"], cancellation=never))
    await asyncio.sleep(0)
    never.cancel(reason)
    with pytest.raises(RuntimeError) as raised:
        await pending
    assert raised.value is reason


async def test_the_attempt_in_progress_is_cancelled_on_close() -> None:
    attempts: list[CancellationToken] = []
    owner = CancellationToken()

    async def hang(_namespace: tuple[str, ...], attempt: CancellationToken) -> Any:
        attempts.append(attempt)
        await asyncio.Event().wait()

    pending = asyncio.ensure_future(_supervise_relay_publisher(hang, ["room"], cancellation=owner))
    await until(lambda: len(attempts) == 1, "attempt")
    owner.cancel()
    with contextlib.suppress(Exception):
        await pending
    assert attempts[0].cancelled


async def test_publisher_checks_its_arguments_at_once() -> None:
    p = Publishers()
    with pytest.raises(TypeError):
        await _supervise_relay_publisher(p.connect, [])
    with pytest.raises(ValueError):
        await _supervise_relay_publisher(p.connect, ["room"], reconnect=SupervisionBackoff(initial_delay=-1))
    with pytest.raises(ValueError):
        await _supervise_relay_publisher(
            p.connect, ["room"], reconnect=SupervisionBackoff(initial_delay=2, max_delay=1)
        )
    assert p.calls == 0


# ------------------------------------------------------------------ the subscriber


class Subscribers:
    def __init__(self) -> None:
        self.connections: list[FakeSubscriber] = []
        self.statuses: list[str] = []

    async def connect(self, _attempt: CancellationToken) -> Any:
        fake = FakeSubscriber()
        self.connections.append(fake)
        return fake

    @property
    def first(self) -> FakeSubscriber:
        return self.connections[0]


async def start_subscriber(cleanup: list[Callable[[], object]]) -> tuple[SupervisedSubscriber, Subscribers]:
    s = Subscribers()
    subscriber = await _supervise_relay_subscriber(
        s.connect, reconnect=FAST, on_status=lambda status, _error: s.statuses.append(status)
    )
    cleanup.append(subscriber.close)
    return subscriber, s


class Recorder:
    def __init__(self) -> None:
        self.seen: list[str] = []
        self.errors: list[BaseException] = []
        self.handlers = WantHandlers(
            on_subscribed=lambda _info, namespace: self.seen.append(f"subscribed {'/'.join(namespace)}"),
            on_object=lambda obj: self.seen.append(f"object {obj.group_id}"),
            on_publisher_done=lambda _done: self.seen.append("done"),
            on_restarted=lambda reason, _namespace: self.seen.append(f"restarted {reason}"),
            on_error=self._error,
        )

    def _error(self, error: BaseException) -> None:
        self.errors.append(error)
        self.seen.append(f"error {cast(RelayError, error).code}")


async def test_retries_the_announce_race_silently(cleanup: list[Callable[[], object]]) -> None:
    subscriber, s = await start_subscriber(cleanup)
    r = Recorder()
    subscriber.want("video", ["room"], "video", r.handlers, retry=FAST)
    await until(lambda: len(s.first.subscriptions) == 1, "first subscribe")
    s.first.latest.reject(0x10, "unroutable")
    await until(lambda: len(s.first.subscriptions) == 2, "retry")
    s.first.latest.accept()
    s.first.latest.send_object(0)
    assert r.seen == ["subscribed room", "object 0"]


async def test_resubscribes_after_a_publisher_done(cleanup: list[Callable[[], object]]) -> None:
    subscriber, s = await start_subscriber(cleanup)
    r = Recorder()
    subscriber.want("video", ["room"], "video", r.handlers, retry=FAST)
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    first = s.first.latest
    first.accept()
    first.send_object(3)
    first.done()
    await until(lambda: len(s.first.subscriptions) == 2, "resubscribe")
    assert first.cancelled
    s.first.latest.accept()
    s.first.latest.send_object(4)
    assert r.seen == [
        "subscribed room",
        "object 3",
        "done",
        "restarted publisher-done",
        "subscribed room",
        "object 4",
    ]


async def test_resubscribes_a_subscription_that_went_stale(cleanup: list[Callable[[], object]]) -> None:
    subscriber, s = await start_subscriber(cleanup)
    r = Recorder()
    subscriber.want("video", ["room"], "video", r.handlers, stale_after=0.03, retry=FAST)
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    s.first.latest.accept()
    s.first.latest.send_object(7)  # the dead publisher's cached object, then silence
    await until(lambda: len(s.first.subscriptions) == 2, "stale resubscribe")
    assert "restarted stale" in r.seen


async def test_moves_every_want_to_the_replacement_connection(cleanup: list[Callable[[], object]]) -> None:
    subscriber, s = await start_subscriber(cleanup)
    r = Recorder()
    subscriber.want("video", ["room"], "video", r.handlers, retry=FAST)
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    s.first.latest.accept()
    s.first.end(RelayError("idle", code="TRANSPORT_ERROR"))
    await until(lambda: len(s.connections) == 2 and len(s.connections[1].subscriptions) == 1, "new connection")
    s.connections[1].latest.accept()
    assert r.seen == ["subscribed room", "restarted connection", "subscribed room"]
    assert s.statuses == ["live", "reconnecting", "live"]


async def test_surfaces_a_rejection_no_wait_fixes(cleanup: list[Callable[[], object]]) -> None:
    subscriber, s = await start_subscriber(cleanup)
    r = Recorder()
    subscriber.want("video", ["room"], "video", r.handlers, retry=FAST)
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    s.first.latest.reject(0x1, "unauthorized")
    await until(lambda: len(r.seen) == 1, "error")
    assert r.seen == ["error SUBSCRIPTION_REQUEST_FAILED"]
    assert isinstance(r.errors[0], SubscriptionRequestError)
    await asyncio.sleep(0.01)
    assert len(s.first.subscriptions) == 1


async def test_retries_other_rejections_with_backoff(cleanup: list[Callable[[], object]]) -> None:
    subscriber, s = await start_subscriber(cleanup)
    r = Recorder()
    subscriber.want("video", ["room"], "video", r.handlers, retry=FAST)
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    s.first.latest.reject(0x10, "not found")
    await until(lambda: len(s.first.subscriptions) == 2, "retry")
    assert r.seen == []


async def test_retries_a_termination_after_acceptance(cleanup: list[Callable[[], object]]) -> None:
    subscriber, s = await start_subscriber(cleanup)
    r = Recorder()
    subscriber.want("video", ["room"], "video", r.handlers, retry=FAST)
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    s.first.latest.accept()
    s.first.latest.terminate(0x0, "internal")
    await until(lambda: len(s.first.subscriptions) == 2, "retry")
    assert r.seen == ["subscribed room", "restarted terminated"]


async def test_follows_the_newest_generation(cleanup: list[Callable[[], object]]) -> None:
    subscriber, s = await start_subscriber(cleanup)
    r = Recorder()
    subscriber.want("video", ["room", "alice"], "video", r.handlers, generations=True, retry=FAST)
    await until(lambda: len(s.first.discoveries) == 1, "discovery")
    discovery = s.first.discoveries[0]
    assert discovery.prefix == ("room", "alice")
    old = new_generation(1_000)
    replacement = new_generation(2_000)
    # A deeper namespace under the root is not a generation.
    discovery.push("announced", ["room", "alice", "x", "y"])
    discovery.push("announced", ["room", "alice", old])
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe to the old generation")
    assert s.first.latest.namespace == ("room", "alice", old)
    s.first.latest.accept()
    s.first.latest.send_object(1)

    # The crashed generation is still announced when the replacement arrives: the newer one wins at once.
    discovery.push("announced", ["room", "alice", replacement])
    await until(lambda: len(s.first.subscriptions) == 2, "subscribe to the replacement")
    assert s.first.latest.namespace == ("room", "alice", replacement)
    s.first.latest.accept()
    assert r.seen == [
        f"subscribed room/alice/{old}",
        "object 1",
        "restarted generation",
        f"subscribed room/alice/{replacement}",
    ]

    # Its withdrawal does not move the want back: the old one is gone too.
    discovery.push("unannounced", ["room", "alice", old])
    await asyncio.sleep(0.01)
    assert len(s.first.subscriptions) == 2


async def test_waits_for_a_generation_when_none_is_announced(cleanup: list[Callable[[], object]]) -> None:
    subscriber, s = await start_subscriber(cleanup)
    subscriber.want("video", ["room"], "video", generations=True, retry=FAST)
    await until(lambda: len(s.first.discoveries) == 1, "discovery")
    await asyncio.sleep(0.01)
    assert s.first.subscriptions == []
    s.first.discoveries[0].push("announced", ["room", new_generation()])
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")


async def test_shares_one_discovery_among_the_wants_on_a_root(cleanup: list[Callable[[], object]]) -> None:
    subscriber, s = await start_subscriber(cleanup)
    subscriber.want("video", ["room"], "video", generations=True)
    subscriber.want("audio", ["room"], "audio", generations=True)
    await until(lambda: len(s.first.discoveries) == 1, "discovery")
    s.first.discoveries[0].push("announced", ["room", new_generation()])
    await until(lambda: len(s.first.subscriptions) == 2, "both subscribed")
    assert len(s.first.discoveries) == 1


async def test_swaps_handlers_for_the_same_want_and_replaces_a_different_one(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, s = await start_subscriber(cleanup)
    first, second = Recorder(), Recorder()
    subscriber.want("video", ["room"], "video", first.handlers)
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    s.first.latest.accept()
    subscriber.want("video", ["room"], "video", second.handlers)
    s.first.latest.send_object(1)
    assert first.seen == ["subscribed room"]
    assert second.seen == ["object 1"]
    assert len(s.first.subscriptions) == 1

    subscriber.want("video", ["room"], "screen", second.handlers)
    await until(lambda: len(s.first.subscriptions) == 2, "replacement want")
    assert s.first.subscriptions[0].cancelled
    assert s.first.latest.track == "screen"


async def test_unwant_cancels_and_close_stops_everything(cleanup: list[Callable[[], object]]) -> None:
    subscriber, s = await start_subscriber(cleanup)
    r = Recorder()
    subscriber.want("a", ["room"], "a", r.handlers)
    subscriber.want("b", ["room"], "b")
    await until(lambda: len(s.first.subscriptions) == 2, "subscribe")
    s.first.subscriptions[0].accept()
    subscriber.unwant("a")
    # Nothing reaches an unwanted want, even before its subscription is cancelled.
    s.first.subscriptions[0].send_object(5)
    await until(lambda: s.first.subscriptions[0].cancelled, "a cancelled")
    assert r.seen == ["subscribed room"]
    subscriber.unwant("missing")
    subscriber.close()
    assert (await subscriber.closed).error is None
    assert subscriber.status == "closed"
    await asyncio.sleep(0.01)
    assert len(s.connections) == 1
    with pytest.raises(RelayError):
        subscriber.want("c", ["room"], "c")


async def test_checks_a_want_at_once(cleanup: list[Callable[[], object]]) -> None:
    subscriber, _ = await start_subscriber(cleanup)
    with pytest.raises(TypeError):
        subscriber.want("k", [], "v")
    with pytest.raises(TypeError):
        subscriber.want("k", ["room"], "")
    with pytest.raises(ValueError):
        subscriber.want("k", ["room"], "v", stale_after=-1)

    async def handler(_obj: object) -> None:
        pass

    with pytest.raises(TypeError):
        subscriber.want("k", ["room"], "v", WantHandlers(on_object=handler))  # ty: ignore[invalid-argument-type]


async def test_the_first_subscriber_attempt_raises_a_credential_refusal() -> None:
    refused = RelayError("no", code="UNAUTHORIZED", phase="connecting")

    async def refuse(_attempt: CancellationToken) -> Any:
        raise refused

    with pytest.raises(RelayError) as raised:
        await _supervise_relay_subscriber(refuse, reconnect=FAST)
    assert raised.value is refused


async def test_waits_for_the_connection_end_instead_of_treating_its_refusal_as_fatal(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, s = await start_subscriber(cleanup)
    r = Recorder()
    subscriber.want("v", ["room"], "v", r.handlers, generations=True)
    await until(lambda: len(s.first.discoveries) == 1, "discovery")
    # The discovery fails with the closure, and further requests are refused, a turn before `closed` resolves.
    s.first.refuse_discovery = RelayError("Relay connection is closed", code="CLOSED")
    s.first.discoveries[0].fail(SubscriptionConnectionClosedError(None))
    await asyncio.sleep(0.02)
    s.first.refuse_discovery = None
    s.first.end(RuntimeError("lost"))
    await until(lambda: len(s.connections) == 2 and len(s.connections[1].discoveries) == 1, "rediscovered")
    assert r.errors == []


# ------------------------------------------------------------------ regressions from the TypeScript reviews


async def test_moves_to_a_newer_generation_announced_with_the_older(cleanup: list[Callable[[], object]]) -> None:
    subscriber, s = await start_subscriber(cleanup)
    subscriber.want("v", ["room"], "v", generations=True, retry=FAST)
    await until(lambda: len(s.first.discoveries) == 1, "discovery")
    older, newer = new_generation(1_000), new_generation(2_000)
    s.first.discoveries[0].push("announced", ["room", older])
    s.first.discoveries[0].push("announced", ["room", newer])
    await until(
        lambda: bool(s.first.subscriptions) and s.first.latest.namespace[1] == newer,
        "subscription on the newer generation",
    )


async def test_surfaces_a_discovery_that_fails_for_good_while_subscribed(cleanup: list[Callable[[], object]]) -> None:
    subscriber, s = await start_subscriber(cleanup)
    r = Recorder()
    subscriber.want("v", ["room"], "v", r.handlers, generations=True)
    await until(lambda: len(s.first.discoveries) == 1, "discovery")
    s.first.discoveries[0].push("announced", ["room", new_generation()])
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    s.first.subscriptions[0].accept()
    s.first.discoveries[0].fail(request_error(0x1, "unauthorized"))
    await until(lambda: len(r.errors) == 1, "on_error")
    assert s.first.subscriptions[0].cancelled


async def test_an_overlapping_root_replaces_one_whose_last_want_went_away(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, s = await start_subscriber(cleanup)
    r = Recorder()
    subscriber.want("a", ["room"], "v", r.handlers, generations=True)
    await until(lambda: len(s.first.discoveries) == 1, "first discovery")
    subscriber.want("a", ["room", "alice"], "v", r.handlers, generations=True)
    await until(lambda: len(s.first.discoveries) == 2, "second discovery")
    assert s.first.discoveries[0].closed
    assert r.errors == []


async def test_a_newer_generation_cuts_a_retry_delay_short(cleanup: list[Callable[[], object]]) -> None:
    subscriber, s = await start_subscriber(cleanup)
    subscriber.want("v", ["room"], "v", generations=True, retry=SLOW)
    await until(lambda: len(s.first.discoveries) == 1, "discovery")
    discovery = s.first.discoveries[0]
    discovery.push("announced", ["room", new_generation(1_000)])
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    s.first.subscriptions[0].reject(0x10, "not found")
    await asyncio.sleep(0.02)
    assert len(s.first.subscriptions) == 1  # waiting out a minute-long delay
    discovery.push("announced", ["room", new_generation(2_000)])
    await until(lambda: len(s.first.subscriptions) == 2, "resubscribe at once")


async def test_keeps_a_want_the_closing_connection_refused(cleanup: list[Callable[[], object]]) -> None:
    subscriber, s = await start_subscriber(cleanup)
    r = Recorder()
    s.first.refuse_subscribe = RelayError("lost", code="TRANSPORT_ERROR")
    subscriber.want("v", ["room"], "v", r.handlers)
    await asyncio.sleep(0.02)
    s.first.end(RuntimeError("lost"))
    await until(lambda: len(s.connections) == 2 and len(s.connections[1].subscriptions) == 1, "resubscribed")
    assert r.errors == []


async def test_a_discovery_that_keeps_failing_is_retried_no_faster_than_the_backoff(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, s = await start_subscriber(cleanup)
    subscriber.want("v", ["room"], "v", generations=True, retry=SupervisionBackoff(initial_delay=0.2, max_delay=0.2))
    loop = asyncio.get_running_loop()
    started = loop.time()
    while loop.time() - started < 0.3:
        for discovery in s.first.discoveries:
            if not discovery.closed:
                discovery.fail(request_error(0x0, "internal error"))
        await asyncio.sleep(0.005)
    # One request at the start, one per 100-200 ms delay, and one opened in each delay: far from one per response.
    assert len(s.first.discoveries) <= 8


async def test_replaces_the_want_when_only_stale_after_changes(cleanup: list[Callable[[], object]]) -> None:
    subscriber, s = await start_subscriber(cleanup)
    subscriber.want("v", ["room"], "v")
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    s.first.subscriptions[0].accept()
    subscriber.want("v", ["room"], "v", stale_after=0.02, retry=FAST)
    await until(lambda: len(s.first.subscriptions) == 2, "replacement")
    s.first.subscriptions[1].accept()
    await until(lambda: len(s.first.subscriptions) == 3, "stale resubscribe under the new threshold")


async def test_an_empty_fresh_discovery_does_not_cut_a_backoff_short(cleanup: list[Callable[[], object]]) -> None:
    subscriber, s = await start_subscriber(cleanup)
    subscriber.want("v", ["room"], "v", generations=True, retry=SLOW)
    await until(lambda: len(s.first.discoveries) == 1, "discovery")
    generation = new_generation()
    s.first.discoveries[0].push("announced", ["room", generation])
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    s.first.subscriptions[0].accept()
    s.first.discoveries[0].fail(request_error(0x0, "internal error"))
    await until(lambda: len(s.first.discoveries) == 2, "discovery reopened during the backoff")
    s.first.discoveries[1].push("announced", ["room", generation])
    await asyncio.sleep(0.03)
    assert len(s.first.subscriptions) == 1


async def test_surfaces_a_request_the_core_cannot_encode(cleanup: list[Callable[[], object]]) -> None:
    subscriber, s = await start_subscriber(cleanup)
    s.first.refuse_subscribe = MoqtProtocolError("field too long to encode")
    r = Recorder()
    r.handlers.on_error = r.errors.append
    subscriber.want("v", ["room"], "v", r.handlers)
    await until(lambda: len(r.errors) == 1, "on_error")
    assert isinstance(r.errors[0], MoqtProtocolError)


async def test_a_synchronous_replacement_with_an_overlapping_root_wins(cleanup: list[Callable[[], object]]) -> None:
    subscriber, s = await start_subscriber(cleanup)
    r = Recorder()
    subscriber.want("a", ["room"], "v", r.handlers, generations=True)
    subscriber.want("a", ["room", "alice"], "v", r.handlers, generations=True)
    await until(lambda: len(s.first.discoveries) > 0, "discovery")
    await asyncio.sleep(0.02)
    assert [d.prefix for d in s.first.discoveries] == [("room", "alice")]
    assert r.errors == []


async def test_a_handler_returning_an_awaitable_is_observed_not_left_pending(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, s = await start_subscriber(cleanup)

    async def boom() -> None:
        raise RuntimeError("boom")

    subscriber.want("v", ["room"], "v", WantHandlers(on_object=lambda _obj: boom()))  # ty: ignore[invalid-argument-type]
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    s.first.subscriptions[0].accept()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        s.first.subscriptions[0].send_object(0)
    assert any("on_object returned an awaitable" in str(w.message) for w in caught)


async def test_surfaces_a_permanent_discovery_failure_seen_while_backing_off(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, s = await start_subscriber(cleanup)
    r = Recorder()
    subscriber.want("v", ["room"], "v", r.handlers, generations=True, retry=SLOW)
    await until(lambda: len(s.first.discoveries) == 1, "discovery")
    s.first.discoveries[0].push("announced", ["room", new_generation()])
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    s.first.subscriptions[0].reject(0x10, "not found")
    # The backoff holds the discovery; the relay now refuses it for good.
    await asyncio.sleep(0.01)
    s.first.discoveries[0].fail(request_error(0x1, "unauthorized"))
    await until(lambda: len(r.errors) == 1, "on_error")
