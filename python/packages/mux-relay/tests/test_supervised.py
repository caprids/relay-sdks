"""The supervised publisher and subscriber over scripted connections.

Mirrors ``ts/tests/supervised.test.ts``: each case drives what the relay would do to one connection,
and the review-fix cases keep the regressions the TypeScript review passes found.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import warnings
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest
from relay_helpers.supervision_fakes import FakePublisher, FakeSubscriber, until

from moq_core import MessageParameters, MoqtObject, SubgroupEnd
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
    WantSwitchedInfo,
)
from mux_relay._supervision import RESUBSCRIBE_BACKOFF, _Backoff, backoff_delay, new_generation, wait_any
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

    # The crashed generation is still announced when the replacement arrives: the newer one wins at
    # once, made before the break.
    discovery.push("announced", ["room", "alice", replacement])
    await until(lambda: len(s.first.subscriptions) == 2, "subscribe to the replacement")
    assert s.first.latest.namespace == ("room", "alice", replacement)
    s.first.latest.accept()
    s.first.latest.send_object(0)
    assert s.first.subscriptions[0].cancelled
    assert r.seen == [f"subscribed room/alice/{old}", "object 1", "object 0"]

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

    subscriber.want("video", ["lobby"], "video", second.handlers)
    await until(lambda: len(s.first.subscriptions) == 2, "replacement want")
    assert s.first.subscriptions[0].cancelled
    assert s.first.latest.namespace == ("lobby",)


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


async def test_applies_a_new_stale_after_in_place(cleanup: list[Callable[[], object]]) -> None:
    subscriber, s = await start_subscriber(cleanup)
    subscriber.want("v", ["room"], "v")
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    s.first.subscriptions[0].accept()
    subscriber.want("v", ["room"], "v", stale_after=0.02, retry=FAST)
    assert not s.first.subscriptions[0].cancelled
    await until(lambda: len(s.first.subscriptions) == 2, "stale resubscribe under the new threshold")
    assert s.first.subscriptions[0].cancelled


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


# ------------------------------------------------------------------ regressions from the Python review


async def test_a_raising_status_callback_stops_the_supervisors_instead_of_hanging() -> None:
    boom = RuntimeError("status callback failed")

    def on_status(status: str, _error: BaseException | None) -> None:
        if status == "live":
            raise boom

    p = Publishers()
    with pytest.raises(RuntimeError) as raised:
        await asyncio.wait_for(_supervise_relay_publisher(p.connect, ["room"], on_status=on_status), 1)
    assert raised.value is boom
    assert p.connections[0].close_calls == 1

    s = Subscribers()
    with pytest.raises(RuntimeError) as raised:
        await asyncio.wait_for(_supervise_relay_subscriber(s.connect, on_status=on_status), 1)
    assert raised.value is boom
    assert s.first.closed.done()


async def test_a_raising_generation_callback_closes_the_publisher(cleanup: list[Callable[[], object]]) -> None:
    p = Publishers()
    generations = 0

    def on_generation(_info: object) -> None:
        nonlocal generations
        generations += 1
        if generations == 2:
            raise RuntimeError("second generation callback failed")

    publisher = await _supervise_relay_publisher(p.connect, ["room"], reconnect=FAST, on_generation=on_generation)
    cleanup.append(publisher.close)
    p.connections[0].end(RuntimeError("lost"))
    closed = await asyncio.wait_for(publisher.closed, 1)
    assert isinstance(closed.error, RuntimeError)
    assert publisher.status == "closed"


async def test_a_caller_that_stops_waiting_leaves_no_unretrieved_exception() -> None:
    async def hang(*args: object) -> Any:
        # Like the real connect, the attempt ends with its token's reason once the supervisor stops.
        attempt = cast(CancellationToken, args[-1])
        await wait_any([attempt])
        attempt.throw_if_cancelled()

    loop = asyncio.get_running_loop()
    unretrieved: list[dict[str, Any]] = []
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: unretrieved.append(context))
    try:
        for start in (
            lambda: _supervise_relay_publisher(hang, ["room"]),
            lambda: _supervise_relay_subscriber(hang),
        ):
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(start(), 0.02)
        await asyncio.sleep(0.01)
        import gc

        gc.collect()
        await asyncio.sleep(0)
    finally:
        loop.set_exception_handler(previous)
    assert [context.get("message") for context in unretrieved] == []


async def test_a_raising_restart_callback_does_not_orphan_the_want(cleanup: list[Callable[[], object]]) -> None:
    subscriber, s = await start_subscriber(cleanup)

    def on_restarted(_reason: str, _namespace: tuple[str, ...]) -> None:
        raise RuntimeError("restart callback failed")

    subscriber.want("v", ["room"], "v", WantHandlers(on_restarted=on_restarted), retry=FAST)
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    s.first.latest.accept()
    s.first.latest.done()
    await until(lambda: len(s.first.subscriptions) == 2, "resubscribed despite the raising callback")


async def test_cached_groups_replayed_newest_first_are_not_progress(cleanup: list[Callable[[], object]]) -> None:
    subscriber, s = await start_subscriber(cleanup)
    subscriber.want("v", ["room"], "v", retry=SupervisionBackoff(initial_delay=0.04, max_delay=1))
    loop = asyncio.get_running_loop()
    started: list[float] = []
    for count in range(1, 5):
        await until(lambda count=count: len(s.first.subscriptions) == count, f"subscription {count}")
        started.append(loop.time())
        # A finished track still announced: its cached groups, newest first, then a done.
        s.first.latest.accept()
        s.first.latest.send_object(10)
        s.first.latest.send_object(9)
        s.first.latest.done()
    # The delays keep doubling (0.02-0.04, 0.04-0.08, 0.08-0.16 s): nothing reset the backoff.
    assert started[3] - started[2] >= 0.08


# Make-before-break moves (#282)


class MoveRecorder(Recorder):
    def __init__(self) -> None:
        super().__init__()
        self.switched: list[WantSwitchedInfo] = []
        self.handlers.on_subgroup_end = lambda info: self.seen.append(f"end {info.group_id}")
        self.handlers.on_switched = self._switched

    def _switched(self, info: WantSwitchedInfo) -> None:
        self.switched.append(info)
        source = f"{'/'.join(info.from_namespace)}:{info.from_track}"
        self.seen.append(f"switched {source} > {'/'.join(info.to_namespace)}:{info.to_track}")


class OldGeneration:
    """A want following generations under ``room``, subscribed and delivering on the older one."""

    def __init__(self, subscriber: SupervisedSubscriber, s: Subscribers, r: MoveRecorder) -> None:
        self.subscriber, self.s, self.r = subscriber, s, r
        self.old = new_generation(1_000)
        self.replacement = new_generation(2_000)

    @property
    def subs(self) -> list[Any]:
        return self.s.first.subscriptions

    async def announce_replacement(self) -> Any:
        self.s.first.discoveries[0].push("announced", ["room", self.replacement])
        await until(lambda: len(self.subs) == 2, "replacement opened")
        return self.subs[1]


async def on_old_generation(cleanup: list[Callable[[], object]], **options: Any) -> tuple[OldGeneration, Any]:
    subscriber, s = await start_subscriber(cleanup)
    r = MoveRecorder()
    subscriber.want("v", ["room"], "v", r.handlers, generations=True, retry=FAST, **options)
    await until(lambda: len(s.first.discoveries) == 1, "discovery")
    g = OldGeneration(subscriber, s, r)
    s.first.discoveries[0].push("announced", ["room", g.old])
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe to the old generation")
    first = s.first.subscriptions[0]
    first.accept()
    first.send_object(10)
    return g, first


SWITCHED = "switched"


def switched_at(seen: list[str]) -> list[str]:
    """``seen`` with each switch shortened to its marker."""
    return [SWITCHED if entry.startswith(SWITCHED) else entry for entry in seen]


async def test_a_generation_move_keeps_the_old_one_delivering_and_reports_a_switch(
    cleanup: list[Callable[[], object]],
) -> None:
    g, first = await on_old_generation(cleanup)
    second = await g.announce_replacement()
    assert second.namespace == ("room", g.replacement)
    first.send_object(11)
    second.accept()
    first.send_object(12)
    assert not first.cancelled
    second.send_object(0)
    assert first.cancelled
    first.send_object(13)
    second.send_object(1)
    assert g.r.seen == [
        f"subscribed room/{g.old}",
        "object 10",
        "object 11",
        "object 12",
        f"switched room/{g.old}:v > room/{g.replacement}:v",
        "object 0",
        "object 1",
    ]
    assert g.r.switched[0].subscribed.track_alias == 1


async def test_a_move_cuts_over_on_the_first_object_until_accepts(cleanup: list[Callable[[], object]]) -> None:
    g, first = await on_old_generation(cleanup, until=lambda obj: obj.group_id >= 5)
    second = await g.announce_replacement()
    second.accept()
    second.send_object(3)
    first.send_object(11)
    second.send_object(5)
    assert switched_at(g.r.seen[1:]) == ["object 10", "object 11", SWITCHED, "object 5"]


async def test_a_move_follows_the_cutover_object_with_later_groups_that_arrived_early(
    cleanup: list[Callable[[], object]],
) -> None:
    g, _first = await on_old_generation(cleanup, until=lambda obj: obj.group_id == 5)
    second = await g.announce_replacement()
    second.accept()
    second.send_object(6)  # ahead: refused, held
    second.handlers.on_subgroup_end(SubgroupEnd(group_id=6, subgroup_id=0, end_of_group=True))
    second.send_object(3)  # earlier than the cutover: the old subscription's to deliver
    second.send_object(5)
    assert switched_at(g.r.seen[1:]) == ["object 10", SWITCHED, "object 5", "object 6", "end 6"]


async def test_a_move_drains_the_old_subscription(cleanup: list[Callable[[], object]]) -> None:
    g, first = await on_old_generation(cleanup, drain=0.03)
    second = await g.announce_replacement()
    first.send_object(12)
    second.accept()
    second.send_object(0)
    assert not first.cancelled
    first.send_object(11)  # in flight, older than the largest it delivered
    first.handlers.on_subgroup_end(SubgroupEnd(group_id=11, subgroup_id=0, end_of_group=True))
    first.send_object(13)  # later than the cutover: discarded
    await until(lambda: first.cancelled, "drain over")
    assert switched_at(g.r.seen[1:]) == ["object 10", "object 12", SWITCHED, "object 0", "object 11", "end 11"]


async def test_a_rewant_with_another_track_moves_make_before_break(cleanup: list[Callable[[], object]]) -> None:
    subscriber, s = await start_subscriber(cleanup)
    before, after = MoveRecorder(), MoveRecorder()
    subscriber.want("v", ["room"], "video-lo", before.handlers)
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    lo = s.first.subscriptions[0]
    lo.accept()
    lo.send_object(1)
    subscriber.want("v", ["room"], "video-hi", after.handlers)
    await until(lambda: len(s.first.subscriptions) == 2, "pending move")
    hi = s.first.subscriptions[1]
    assert hi.track == "video-hi"
    lo.send_object(2)
    hi.accept()
    hi.send_object(7)
    assert lo.cancelled
    assert before.seen == ["subscribed room", "object 1"]
    assert after.seen == ["object 2", "switched room:video-lo > room:video-hi", "object 7"]
    assert (after.switched[0].from_track, after.switched[0].to_track) == ("video-lo", "video-hi")


async def test_a_rewant_moves_on_new_parameters_alone(cleanup: list[Callable[[], object]]) -> None:
    subscriber, s = await start_subscriber(cleanup)
    r = MoveRecorder()
    subscriber.want("v", ["room"], "v", r.handlers)
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    s.first.subscriptions[0].accept()
    subscriber.want("v", ["room"], "v", r.handlers, parameters=MessageParameters(subscriber_priority=1))
    await until(lambda: len(s.first.subscriptions) == 2, "pending move")
    assert s.first.subscriptions[1].parameters == MessageParameters(subscriber_priority=1)
    s.first.subscriptions[1].accept()
    s.first.subscriptions[1].send_object(0)
    assert r.seen == ["subscribed room", "switched room:v > room:v", "object 0"]


async def test_a_rewant_replaces_an_unanswered_subscription_at_once(cleanup: list[Callable[[], object]]) -> None:
    subscriber, s = await start_subscriber(cleanup)
    r = MoveRecorder()
    subscriber.want("v", ["room"], "a", r.handlers)
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    subscriber.want("v", ["room"], "b", r.handlers)
    await until(lambda: len(s.first.subscriptions) == 2, "replacement")
    assert s.first.subscriptions[0].cancelled
    s.first.subscriptions[1].accept()
    assert r.seen == ["subscribed room"]


async def test_a_rewant_back_to_the_active_track_cancels_the_pending_move(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, s = await start_subscriber(cleanup)
    r = MoveRecorder()
    subscriber.want("v", ["room"], "a", r.handlers)
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    a = s.first.subscriptions[0]
    a.accept()
    subscriber.want("v", ["room"], "b", r.handlers)
    await until(lambda: len(s.first.subscriptions) == 2, "pending b")
    subscriber.want("v", ["room"], "c", r.handlers)
    await until(lambda: len(s.first.subscriptions) == 3, "pending c")
    assert s.first.subscriptions[1].cancelled
    subscriber.want("v", ["room"], "a", r.handlers)
    assert s.first.subscriptions[2].cancelled
    a.send_object(4)
    assert not a.cancelled
    assert r.seen == ["subscribed room", "object 4"]


async def test_a_refused_replacement_keeps_the_old_one_delivering_and_is_retried(
    cleanup: list[Callable[[], object]],
) -> None:
    g, first = await on_old_generation(cleanup)
    second = await g.announce_replacement()
    second.reject(0x10, "unroutable")
    await until(lambda: len(g.subs) == 3, "move retried")
    assert not first.cancelled
    first.send_object(11)
    g.subs[2].accept()
    g.subs[2].send_object(0)
    assert switched_at(g.r.seen[1:]) == ["object 10", "object 11", SWITCHED, "object 0"]


async def test_a_replacement_refused_for_good_is_surfaced(cleanup: list[Callable[[], object]]) -> None:
    g, first = await on_old_generation(cleanup)
    second = await g.announce_replacement()
    second.reject(0x1, "unauthorized")
    await until(lambda: "error SUBSCRIPTION_REQUEST_FAILED" in g.r.seen, "error")
    assert first.cancelled
    assert "restarted generation" not in g.r.seen


async def test_a_move_cuts_over_once_the_old_subscription_ended_whatever_until_says(
    cleanup: list[Callable[[], object]],
) -> None:
    g, first = await on_old_generation(cleanup, until=lambda _obj: False)
    second = await g.announce_replacement()
    second.accept()
    second.send_object(0)  # refused while the old one still delivers
    first.done()  # the old publisher finished: a supervised publisher's rotation
    assert first.cancelled
    second.send_object(1)
    assert switched_at(g.r.seen[1:]) == ["object 10", "done", SWITCHED, "object 1"]


async def test_a_failed_replacement_after_the_old_one_ended_reports_its_restart(
    cleanup: list[Callable[[], object]],
) -> None:
    g, first = await on_old_generation(cleanup)
    second = await g.announce_replacement()
    first.done()
    second.reject(0x10, "unroutable")
    await until(lambda: "restarted publisher-done" in g.r.seen, "restart")
    await until(lambda: len(g.subs) == 3, "resubscribe")
    assert g.subs[2].namespace == ("room", g.replacement)


async def test_a_raising_until_cuts_over_and_is_logged(
    cleanup: list[Callable[[], object]], caplog: pytest.LogCaptureFixture
) -> None:
    def bad(_obj: MoqtObject) -> bool:
        raise RuntimeError("bad predicate")

    g, _first = await on_old_generation(cleanup, until=bad)
    second = await g.announce_replacement()
    second.accept()
    second.send_object(0)
    assert switched_at(g.r.seen[-2:]) == [SWITCHED, "object 0"]
    assert "until raised" in caplog.text


async def test_a_pending_move_survives_the_old_subscription_going_stale(cleanup: list[Callable[[], object]]) -> None:
    g, first = await on_old_generation(cleanup, stale_after=0.04, until=lambda _obj: False)
    second = await g.announce_replacement()
    second.accept()
    await until(lambda: first.cancelled, "old subscription stale")
    assert not second.cancelled
    second.send_object(0)  # the old one is gone: cuts over whatever until says
    assert switched_at(g.r.seen[1:]) == ["object 10", SWITCHED, "object 0"]
    await until(lambda: "restarted stale" in g.r.seen, "the replacement went stale too")


async def test_held_later_groups_replay_even_when_a_handler_raises_at_the_cutover(
    cleanup: list[Callable[[], object]], caplog: pytest.LogCaptureFixture
) -> None:
    subscriber, s = await start_subscriber(cleanup)
    objects: list[int] = []

    def on_object(obj: MoqtObject) -> None:
        objects.append(obj.group_id)
        if obj.group_id == 5:
            raise RuntimeError("decoder failed")

    handlers = WantHandlers(on_object=on_object)
    subscriber.want("v", ["room"], "a", handlers, until=lambda obj: obj.group_id == 5)
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    s.first.subscriptions[0].accept()
    subscriber.want("v", ["room"], "b", handlers, until=lambda obj: obj.group_id == 5)
    await until(lambda: len(s.first.subscriptions) == 2, "pending move")
    b = s.first.subscriptions[1]
    b.accept()
    b.send_object(6)
    b.send_object(7)
    b.send_object(5)
    assert objects == [5, 6, 7]
    assert "raised at a cutover" in caplog.text


async def test_a_new_move_target_starts_its_own_backoff(cleanup: list[Callable[[], object]]) -> None:
    subscriber, s = await start_subscriber(cleanup)
    retry = SupervisionBackoff(initial_delay=0.06, max_delay=60)
    subscriber.want("v", ["room"], "a", retry=retry)
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    s.first.subscriptions[0].accept()
    subscriber.want("v", ["room"], "b", retry=retry)
    for n in range(2, 5):
        await until(lambda n=n: len(s.first.subscriptions) == n, f"move to b, attempt {n - 1}")
        s.first.subscriptions[n - 1].reject(0x10, "unroutable")
    # b has failed three times: its next retry would wait 0.24 to 0.48 s.
    subscriber.want("v", ["room"], "c", retry=retry)
    await until(lambda: s.first.latest.track == "c", "move to c")
    rejected_at = time.monotonic()
    s.first.latest.reject(0x10, "unroutable")
    count = len(s.first.subscriptions)
    await until(lambda: len(s.first.subscriptions) == count + 1, "c retried")
    assert time.monotonic() - rejected_at < 0.2


async def test_a_rewant_after_the_old_target_failed_for_good_is_kept(cleanup: list[Callable[[], object]]) -> None:
    subscriber, s = await start_subscriber(cleanup)
    r = MoveRecorder()
    subscriber.want("v", ["room"], "a", r.handlers)
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    s.first.subscriptions[0].reject(0x1, "unauthorized")
    await asyncio.sleep(0)
    subscriber.want("v", ["room"], "b", r.handlers)
    await until(lambda: len(s.first.subscriptions) == 2, "subscribe to b")
    assert s.first.subscriptions[1].track == "b"
    assert r.seen == []


async def test_a_replacement_refused_for_good_after_a_rewant_is_surfaced(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, s = await start_subscriber(cleanup)
    r = MoveRecorder()
    subscriber.want("v", ["room"], "a", r.handlers)
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    s.first.subscriptions[0].accept()
    subscriber.want("v", ["room"], "b", r.handlers)
    await until(lambda: len(s.first.subscriptions) == 2, "pending b")
    s.first.subscriptions[1].reject(0x1, "unauthorized")
    await until(lambda: "error SUBSCRIPTION_REQUEST_FAILED" in r.seen, "error")
    await asyncio.sleep(0.01)
    assert len(s.first.subscriptions) == 2


async def test_a_target_wanted_from_on_restarted_is_tried_at_once(cleanup: list[Callable[[], object]]) -> None:
    subscriber, s = await start_subscriber(cleanup)
    retry = SupervisionBackoff(initial_delay=60, max_delay=60)
    rewanted: list[bool] = []

    def on_restarted(_reason: str, _namespace: tuple[str, ...]) -> None:
        if not rewanted:
            rewanted.append(True)
            subscriber.want("v", ["room"], "b", handlers, retry=retry)

    handlers = WantHandlers(on_restarted=on_restarted)
    subscriber.want("v", ["room"], "a", handlers, retry=retry)
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    s.first.subscriptions[0].accept()
    s.first.subscriptions[0].done()
    await until(lambda: len(s.first.subscriptions) == 2, "b subscribed at once")
    assert s.first.subscriptions[1].track == "b"


async def test_a_replacement_object_queued_as_the_old_subscription_ended_cuts_over(
    cleanup: list[Callable[[], object]],
) -> None:
    g, first = await on_old_generation(cleanup, until=lambda _obj: False)
    second = await g.announce_replacement()
    second.accept()
    # The relay ends the old subscription, and the replacement's object arrives before on_ended runs.
    first.terminate(0x0, "internal")
    second.send_object(0)
    await asyncio.sleep(0.01)
    assert switched_at(g.r.seen[1:]) == ["object 10", SWITCHED, "object 0"]


async def test_a_rewant_superseding_a_move_that_failed_for_good_reports_a_restart(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, s = await start_subscriber(cleanup)
    r = MoveRecorder()
    subscriber.want("v", ["room"], "a", r.handlers)
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    s.first.subscriptions[0].accept()
    subscriber.want("v", ["room"], "b", r.handlers)
    await until(lambda: len(s.first.subscriptions) == 2, "pending b")
    s.first.subscriptions[1].reject(0x1, "unauthorized")
    await asyncio.sleep(0)
    subscriber.want("v", ["room"], "c", r.handlers)
    await until(lambda: len(s.first.subscriptions) == 3, "subscribe to c")
    s.first.subscriptions[2].accept()
    assert r.seen == ["subscribed room", "restarted terminated", "subscribed room"]


async def test_a_superseded_failed_move_keeps_the_old_subscriptions_reason(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, s = await start_subscriber(cleanup)
    r = MoveRecorder()
    subscriber.want("v", ["room"], "a", r.handlers)
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    s.first.subscriptions[0].accept()
    subscriber.want("v", ["room"], "b", r.handlers)
    await until(lambda: len(s.first.subscriptions) == 2, "pending b")
    s.first.subscriptions[0].done()
    s.first.subscriptions[1].reject(0x1, "unauthorized")
    await asyncio.sleep(0)
    subscriber.want("v", ["room"], "c", r.handlers)
    await until(lambda: "restarted publisher-done" in r.seen, "restart")
    assert "restarted terminated" not in r.seen


async def test_a_withdrawn_generation_with_none_newer_still_restarts(cleanup: list[Callable[[], object]]) -> None:
    g, first = await on_old_generation(cleanup)
    g.s.first.discoveries[0].push("unannounced", ["room", g.old])
    await until(lambda: "restarted generation" in g.r.seen, "restart")
    assert first.cancelled


async def test_until_and_drain_swap_in_place_and_are_checked_at_once(cleanup: list[Callable[[], object]]) -> None:
    subscriber, s = await start_subscriber(cleanup)
    subscriber.want("v", ["room"], "v")
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    subscriber.want("v", ["room"], "v", until=lambda _obj: True, drain=0.1)
    assert len(s.first.subscriptions) == 1
    assert not s.first.subscriptions[0].cancelled
    with pytest.raises(TypeError):
        subscriber.want("v", ["room"], "v", until=cast(Any, "first"))

    async def asynchronous(_obj: MoqtObject) -> bool:
        return True

    with pytest.raises(TypeError):
        subscriber.want("v", ["room"], "v", until=cast(Any, asynchronous))
    with pytest.raises(ValueError):
        subscriber.want("v", ["room"], "v", drain=-1)
    with pytest.raises(ValueError):
        subscriber.want("v", ["room"], "v", drain=2**31)


async def test_a_connection_end_during_a_pending_move_moves_to_the_next_connection(
    cleanup: list[Callable[[], object]],
) -> None:
    g, _first = await on_old_generation(cleanup)
    await g.announce_replacement()
    g.s.first.end(RelayError("idle", code="TRANSPORT_ERROR"))
    await until(lambda: "restarted connection" in g.r.seen, "restart")
    assert not [entry for entry in g.r.seen if entry.startswith(SWITCHED)]


# ------------------------------------------------------------------ rotation before the credential expires (#261)


def _in(seconds: float) -> datetime:
    return datetime.now(UTC) + timedelta(seconds=seconds)


class ExpiringPublishers(Publishers):
    def __init__(self, expiries: list[datetime | None]) -> None:
        super().__init__()
        self.expiries = expiries

    async def connect(self, namespace: tuple[str, ...], _attempt: CancellationToken) -> Any:
        index = len(self.connections)
        fake = FakePublisher(namespace, self.expiries[index] if index < len(self.expiries) else None)
        self.connections.append(fake)
        return fake


async def start_rotating_publisher(
    cleanup: list[Callable[[], object]], expiries: list[datetime | None], rotate_before: float | None = None
) -> tuple[SupervisedPublisher, ExpiringPublishers]:
    p = ExpiringPublishers(expiries)
    publisher = await _supervise_relay_publisher(
        p.connect,
        ["room"],
        reconnect=FAST,
        rotate_before=rotate_before,
        on_generation=lambda info: p.generations.append(info.namespace),
    )
    cleanup.append(publisher.close)
    return publisher, p


async def test_rotation_moves_a_track_once_it_has_demand_on_the_next_generation(
    cleanup: list[Callable[[], object]],
) -> None:
    publisher, p = await start_rotating_publisher(cleanup, [])
    writer = publisher.register_track("video")
    old = p.connections[0]
    old.tracks["video"].set_demand(True)
    await until(lambda: writer.has_demand, "demand on the first generation")
    # Let the supervisor hear the change before the iterator snapshots the current state.
    await asyncio.sleep(0.005)
    changes = writer.demand_changes()
    assert await anext(changes) is True

    publisher.rotate()
    await until(lambda: len(p.connections) == 2 and "video" in p.connections[1].tracks, "registered on the next")
    assert len(p.generations) == 2
    assert publisher.generation is not None and publisher.generation.namespace == p.generations[1]
    assert publisher.status == "live"
    # The viewers are still on the old generation: writes stay there.
    await writer.write(b"\x01")
    assert old.tracks["video"].writes == [b"\x01"]
    assert old.close_calls == 0

    p.connections[1].tracks["video"].set_demand(True)
    await writer.write(b"\x02")
    assert p.connections[1].tracks["video"].writes == [b"\x02"]
    await until(lambda: old.close_calls == 1, "old generation closed once the track moved")
    assert writer.has_demand
    assert not writer.closed
    # Demand carried over: the move repeats True (so demand-driven state is written to the new generation), and
    # no False comes in between.
    assert await asyncio.wait_for(anext(changes), 1) is True
    p.connections[1].tracks["video"].set_demand(False)
    assert await asyncio.wait_for(anext(changes), 1) is False
    await changes.aclose()  # ty: ignore[unresolved-attribute]


async def test_rotation_moves_a_track_without_demand_at_once(cleanup: list[Callable[[], object]]) -> None:
    publisher, p = await start_rotating_publisher(cleanup, [])
    writer = publisher.register_track("audio")
    publisher.rotate()
    await until(lambda: p.connections[0].close_calls == 1, "old generation closed")
    await writer.write(b"\x09")
    assert p.connections[1].tracks["audio"].writes == [b"\x09"]
    assert p.connections[0].tracks["audio"].writes == []


async def test_publisher_rotates_on_its_own_before_the_credential_expires(
    cleanup: list[Callable[[], object]],
) -> None:
    _, p = await start_rotating_publisher(cleanup, [_in(0.2), _in(3600)], rotate_before=0.15)
    assert len(p.connections) == 1
    await until(lambda: p.connections[0].close_calls == 1, "rotated before the expiry")
    assert len(p.connections) == 2
    assert len(p.generations) == 2


async def test_publisher_keeps_the_live_generation_when_the_next_expires_no_later(
    cleanup: list[Callable[[], object]],
) -> None:
    soon = _in(0.1)
    publisher, p = await start_rotating_publisher(cleanup, [soon, soon], rotate_before=60)
    await until(lambda: len(p.connections) == 2, "replacement attempted")
    await until(lambda: p.connections[1].close_calls == 1, "replacement discarded")
    assert p.connections[0].close_calls == 0
    assert publisher.generation is not None and publisher.generation.namespace == p.connections[0].namespace


@pytest.mark.parametrize(("value", "kind"), [("60", TypeError), (True, TypeError), (-1, ValueError)])
async def test_rotate_before_is_checked_at_once(value: object, kind: type[Exception]) -> None:
    p = Publishers()
    with pytest.raises(kind):
        await _supervise_relay_publisher(p.connect, ["room"], rotate_before=cast(float, value))
    s = Subscribers()
    with pytest.raises(kind):
        await _supervise_relay_subscriber(s.connect, rotate_before=cast(float, value))


class ExpiringSubscribers(Subscribers):
    def __init__(self, expiries: list[datetime | None]) -> None:
        super().__init__()
        self.expiries = expiries

    async def connect(self, _attempt: CancellationToken) -> Any:
        index = len(self.connections)
        fake = FakeSubscriber(self.expiries[index] if index < len(self.expiries) else None)
        self.connections.append(fake)
        return fake


async def start_rotating_subscriber(
    cleanup: list[Callable[[], object]], expiries: list[datetime | None] | None = None
) -> tuple[SupervisedSubscriber, ExpiringSubscribers]:
    s = ExpiringSubscribers(expiries or [])
    subscriber = await _supervise_relay_subscriber(
        s.connect, reconnect=FAST, rotate_before=0.15, on_status=lambda status, _error: s.statuses.append(status)
    )
    cleanup.append(subscriber.close)
    return subscriber, s


async def test_rotation_moves_a_want_to_the_new_connection_with_no_callback_and_nothing_repeated(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, s = await start_rotating_subscriber(cleanup)
    seen: list[str] = []
    subscriber.want(
        "video",
        ["room"],
        "video",
        WantHandlers(
            on_subscribed=lambda _info, _namespace: seen.append("subscribed"),
            on_object=lambda obj: seen.append(f"object {obj.group_id}"),
            on_restarted=lambda reason, _namespace: seen.append(f"restarted {reason}"),
            on_switched=lambda _info: seen.append("switched"),
        ),
        retry=FAST,
    )
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    old = s.first.subscriptions[0]
    old.accept()
    old.send_object(1)

    subscriber.rotate()
    await until(lambda: len(s.connections) == 2 and len(s.connections[1].subscriptions) == 1, "resubscribe")
    replacement = s.connections[1].subscriptions[0]
    replacement.accept()
    old.send_object(2)
    assert not old.cancelled
    assert len(s.first.subscriptions) == 1
    # The replacement starts at the latest group, which the old subscription already delivered: not repeated.
    replacement.send_object(2)
    assert not old.cancelled
    replacement.send_object(3)
    old.send_object(4)
    await until(lambda: old.cancelled, "old subscription cancelled")
    assert seen == ["subscribed", "object 1", "object 2", "object 3"]
    assert s.statuses == ["live"]
    # The old connection closes once every want moved; nothing reconnects after it.
    await until(lambda: s.first.closed.done(), "old connection closed")
    await asyncio.sleep(0.02)
    assert len(s.connections) == 2
    assert subscriber.status == "live"


async def test_rotation_carries_on_to_the_replacement_when_the_old_connection_ends_before_it_delivered(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, s = await start_rotating_subscriber(cleanup)
    seen: list[str] = []
    subscriber.want(
        "video",
        ["room"],
        "video",
        WantHandlers(
            on_subscribed=lambda _info, _namespace: seen.append("subscribed"),
            on_object=lambda obj: seen.append(f"object {obj.group_id}"),
            on_restarted=lambda reason, _namespace: seen.append(f"restarted {reason}"),
        ),
        retry=FAST,
    )
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    s.first.subscriptions[0].accept()
    s.first.subscriptions[0].send_object(4)
    subscriber.rotate()
    await until(lambda: len(s.connections) == 2 and len(s.connections[1].subscriptions) == 1, "resubscribe")
    s.connections[1].subscriptions[0].accept()
    s.first.end()
    await asyncio.sleep(0.01)
    s.connections[1].subscriptions[0].send_object(4)  # already delivered
    s.connections[1].subscriptions[0].send_object(5)
    assert seen == ["subscribed", "object 4", "object 5"]


async def test_subscriber_rotates_on_its_own_before_the_credential_expires(
    cleanup: list[Callable[[], object]],
) -> None:
    _, s = await start_rotating_subscriber(cleanup, [_in(0.2), _in(3600)])
    # No wants: the old connection closes as soon as the new one is up.
    await until(lambda: len(s.connections) == 2 and s.first.closed.done(), "rotated")
    assert s.statuses == ["live"]


# ------------------------------------------------------------------ Codex pass 1 fixes (#261)


async def test_keeps_the_old_generation_open_until_a_group_opened_on_it_is_closed(
    cleanup: list[Callable[[], object]],
) -> None:
    publisher, p = await start_rotating_publisher(cleanup, [])
    writer = publisher.register_track("video")
    p.connections[0].tracks["video"].set_demand(True)
    await until(lambda: writer.has_demand, "demand")
    group = await writer.open_group()
    publisher.rotate()
    await until(lambda: len(p.connections) == 2 and "video" in p.connections[1].tracks, "next generation")
    p.connections[1].tracks["video"].set_demand(True)
    await writer.write(b"\x07")  # moves the track
    await asyncio.sleep(0.02)
    assert p.connections[0].close_calls == 0
    await group.write(b"\x01")
    await group.close()
    await until(lambda: p.connections[0].close_calls == 1, "old generation closed after the group")
    assert p.connections[1].tracks["video"].writes == [b"\x07"]


async def test_still_rotates_on_rotate_after_an_automatic_rotation_was_discarded(
    cleanup: list[Callable[[], object]],
) -> None:
    soon = _in(0.1)
    publisher, p = await start_rotating_publisher(cleanup, [soon, soon, _in(3600)])
    await until(lambda: len(p.connections) == 2 and p.connections[1].close_calls == 1, "automatic one discarded")
    publisher.rotate()
    await until(lambda: p.connections[0].close_calls == 1, "manual rotation replaced the generation")
    assert publisher.generation is not None and publisher.generation.namespace == p.connections[2].namespace


async def test_keeps_cutting_repeats_after_the_old_subscription_ended_before_the_replacement_delivered(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, s = await start_rotating_subscriber(cleanup)
    seen: list[int] = []
    subscriber.want("v", ["room"], "v", WantHandlers(on_object=lambda o: seen.append(o.group_id)), retry=FAST)
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    old = s.first.subscriptions[0]
    old.accept()
    old.send_object(2)
    subscriber.rotate()
    await until(lambda: len(s.connections) == 2 and len(s.connections[1].subscriptions) == 1, "resubscribe")
    replacement = s.connections[1].subscriptions[0]
    replacement.accept()
    s.first.end()
    await asyncio.sleep(0.005)
    replacement.send_object(2)
    replacement.send_object(1)
    replacement.send_object(3)
    assert seen == [2, 3]


async def test_reconnects_when_the_old_connection_ends_while_replacements_keep_failing(
    cleanup: list[Callable[[], object]],
) -> None:
    connections: list[FakeSubscriber] = []
    statuses: list[tuple[str, BaseException | None]] = []
    failing = False

    async def connect(_attempt: CancellationToken) -> Any:
        if failing:
            raise RelayError("down", code="TRANSPORT_ERROR", phase="connecting")
        fake = FakeSubscriber()
        connections.append(fake)
        return fake

    subscriber = await _supervise_relay_subscriber(
        connect,
        reconnect=SupervisionBackoff(initial_delay=0.02, max_delay=0.02),
        on_status=lambda status, error: statuses.append((status, error)),
    )
    cleanup.append(subscriber.close)
    failing = True
    subscriber.rotate()
    await asyncio.sleep(0.03)
    lost = RelayError("expired", code="UNAUTHORIZED", phase="connection")
    connections[0].end(lost)
    await until(lambda: any(status == "reconnecting" for status, _ in statuses), "reconnecting reported")
    assert ("reconnecting", lost) in statuses
    failing = False
    await until(lambda: subscriber.status == "live" and len(connections) == 2, "reconnected")


async def test_reports_nothing_for_a_pending_handoff_after_close(cleanup: list[Callable[[], object]]) -> None:
    subscriber, s = await start_rotating_subscriber(cleanup)
    seen: list[str] = []
    subscriber.want(
        "v",
        ["room"],
        "v",
        WantHandlers(
            on_subscribed=lambda _info, _ns: seen.append("subscribed"),
            on_restarted=lambda _reason, _ns: seen.append("restarted"),
        ),
        retry=FAST,
    )
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    s.first.subscriptions[0].accept()
    subscriber.rotate()
    await until(lambda: len(s.connections) == 2 and len(s.connections[1].subscriptions) == 1, "resubscribe")
    s.connections[1].subscriptions[0].accept()
    subscriber.close()
    await asyncio.sleep(0.02)
    assert seen == ["subscribed"]


# ------------------------------------------------------------------ Codex pass 2 fixes (#261)


async def test_reconnects_at_once_when_the_replacement_connection_ends_during_the_overlap(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, s = await start_rotating_subscriber(cleanup)
    subscriber.want("a", ["room"], "a", retry=FAST)
    subscriber.want("b", ["room"], "b", retry=FAST)
    await until(lambda: len(s.first.subscriptions) == 2, "subscribe both")
    for sub in s.first.subscriptions:
        sub.accept()
    subscriber.rotate()
    await until(lambda: len(s.connections) == 2 and len(s.connections[1].subscriptions) == 2, "resubscribe both")
    s.connections[1].subscriptions[0].accept()
    s.connections[1].subscriptions[0].send_object(1)  # 'a' moved, 'b' still on the old connection
    s.connections[1].end(RelayError("lost", code="TRANSPORT_ERROR", phase="connection"))
    await until(lambda: len(s.connections) == 3, "a third connection well within the overlap", 0.5)
    assert "reconnecting" in s.statuses


async def test_closes_a_group_still_open_on_the_old_generation_when_the_track_closes(
    cleanup: list[Callable[[], object]],
) -> None:
    publisher, p = await start_rotating_publisher(cleanup, [])
    writer = publisher.register_track("video")
    p.connections[0].tracks["video"].set_demand(True)
    await until(lambda: writer.has_demand, "demand")
    group = await writer.open_group()
    publisher.rotate()
    await until(lambda: len(p.connections) == 2 and "video" in p.connections[1].tracks, "next generation")
    p.connections[1].tracks["video"].set_demand(True)
    await writer.write(b"\x07")
    writer.close()
    assert p.connections[0].tracks["video"].closed
    with pytest.raises(RelayError) as failure:
        await group.write(b"\x01")
    assert failure.value.code == "CLOSED"


async def test_keeps_the_cutoff_when_a_replacement_ends_before_taking_over(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, s = await start_rotating_subscriber(cleanup)
    seen: list[int] = []
    subscriber.want("v", ["room"], "v", WantHandlers(on_object=lambda o: seen.append(o.group_id)), retry=FAST)
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    old = s.first.subscriptions[0]
    old.accept()
    old.send_object(2)
    subscriber.rotate()
    await until(lambda: len(s.connections) == 2 and len(s.connections[1].subscriptions) == 1, "resubscribe")
    s.connections[1].subscriptions[0].accept()
    s.connections[1].subscriptions[0].terminate(0x0, "internal error")
    old.send_object(3)
    await until(lambda: len(s.connections[1].subscriptions) == 2, "retry on the new connection")
    retry = s.connections[1].subscriptions[1]
    retry.accept()
    retry.send_object(3)
    retry.send_object(4)
    assert seen == [2, 3, 4]


# ------------------------------------------------------------------ Codex pass 3 fixes (#261)


async def test_passes_a_repeated_true_through_as_demand_driven_state_needs(
    cleanup: list[Callable[[], object]],
) -> None:
    publisher, p = await start_rotating_publisher(cleanup, [])
    writer = publisher.register_track("catalog")
    changes = writer.demand_changes()
    assert await asyncio.wait_for(anext(changes), 1) is False
    p.connections[0].tracks["catalog"].set_demand(True)
    assert await asyncio.wait_for(anext(changes), 1) is True
    p.connections[0].tracks["catalog"].set_demand(True)  # the relay replaced its upstream subscription
    assert await asyncio.wait_for(anext(changes), 1) is True
    await changes.aclose()  # ty: ignore[unresolved-attribute]


async def test_keeps_cutting_what_was_delivered_across_two_rotations_with_nothing_new_in_between(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, s = await start_rotating_subscriber(cleanup)
    seen: list[int] = []
    subscriber.want("v", ["room"], "v", WantHandlers(on_object=lambda o: seen.append(o.group_id)), retry=FAST)
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    s.first.subscriptions[0].accept()
    s.first.subscriptions[0].send_object(2)
    subscriber.rotate()
    await until(lambda: len(s.connections) == 2 and len(s.connections[1].subscriptions) == 1, "first rotation")
    s.connections[1].subscriptions[0].accept()
    s.connections[1].subscriptions[0].send_object(2)  # cached: cut
    s.first.end()
    await asyncio.sleep(0.01)
    subscriber.rotate()
    await until(lambda: len(s.connections) == 3 and len(s.connections[2].subscriptions) == 1, "second rotation")
    s.connections[2].subscriptions[0].accept()
    s.connections[2].subscriptions[0].send_object(2)
    s.connections[2].subscriptions[0].send_object(3)
    assert seen == [2, 3]


async def test_reports_reconnecting_when_the_connection_ends_while_its_replacement_is_still_connecting(
    cleanup: list[Callable[[], object]],
) -> None:
    gate = asyncio.Event()
    connections: list[FakeSubscriber] = []
    statuses: list[tuple[str, BaseException | None]] = []

    async def connect(_attempt: CancellationToken) -> Any:
        if len(connections) == 1:
            await gate.wait()
        fake = FakeSubscriber()
        connections.append(fake)
        return fake

    subscriber = await _supervise_relay_subscriber(
        connect, reconnect=FAST, on_status=lambda status, error: statuses.append((status, error))
    )
    cleanup.append(subscriber.close)
    subscriber.rotate()
    await asyncio.sleep(0.01)
    lost = RelayError("lost", code="TRANSPORT_ERROR", phase="connection")
    connections[0].end(lost)
    await until(lambda: subscriber.status == "reconnecting", "reconnecting while the replacement connects")
    assert ("reconnecting", lost) in statuses
    gate.set()
    await until(lambda: subscriber.status == "live", "live on the replacement")
    assert len(connections) == 2


async def test_unbinds_a_track_left_on_the_old_generation_when_the_next_one_ends_before_it_moved(
    cleanup: list[Callable[[], object]],
) -> None:
    publisher, p = await start_rotating_publisher(cleanup, [])
    writer = publisher.register_track("video")
    p.connections[0].tracks["video"].set_demand(True)
    await until(lambda: writer.has_demand, "demand")
    await asyncio.sleep(0.005)
    changes = writer.demand_changes()
    assert await asyncio.wait_for(anext(changes), 1) is True
    publisher.rotate()
    await until(lambda: len(p.connections) == 2 and "video" in p.connections[1].tracks, "next generation")
    p.connections[1].end(RuntimeError("lost"))
    await until(lambda: p.connections[0].close_calls == 1, "old generation closed")
    assert not writer.has_demand
    assert await asyncio.wait_for(anext(changes), 1) is False
    await changes.aclose()  # ty: ignore[unresolved-attribute]


# ------------------------------------------------------------------ Codex pass 4 fixes (#261)


async def test_closes_the_old_generation_at_once_when_a_track_with_a_group_open_on_it_closes(
    cleanup: list[Callable[[], object]],
) -> None:
    publisher, p = await start_rotating_publisher(cleanup, [])
    writer = publisher.register_track("video")
    p.connections[0].tracks["video"].set_demand(True)
    await until(lambda: writer.has_demand, "demand")
    await writer.open_group()
    publisher.rotate()
    await until(lambda: len(p.connections) == 2 and "video" in p.connections[1].tracks, "next generation")
    p.connections[1].tracks["video"].set_demand(True)
    await writer.write(b"\x07")
    writer.close()
    await until(lambda: p.connections[0].close_calls == 1, "old generation closed well before the overlap ends", 0.5)


# ------------------------------------------------------------------ Codex pass 5 fixes (#261)


async def test_rotates_a_short_lived_replacement_during_the_overlap_instead_of_letting_it_expire(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, s = await start_rotating_subscriber(cleanup, [None, _in(0.3), _in(3600)])
    subscriber.want("silent", ["room"], "silent", retry=FAST)
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    s.first.subscriptions[0].accept()  # silent: no object will take the handoff over
    subscriber.rotate()
    await until(lambda: len(s.connections) == 2 and len(s.connections[1].subscriptions) == 1, "resubscribe")
    s.connections[1].subscriptions[0].accept()
    await until(lambda: len(s.connections) == 3, "the replacement rotated well within the 10 s overlap", 2.0)


# ------------------------------------------------------------------ Codex pass 6 fixes (#261)


async def test_keeps_a_replacements_rotation_deadline_from_when_it_connected_overlap_or_not(
    cleanup: list[Callable[[], object]],
) -> None:
    connections: list[FakeSubscriber] = []
    connected_at: list[float] = []
    lifetimes = [None, 0.6, 3600.0]
    loop = asyncio.get_running_loop()

    async def connect(_attempt: CancellationToken) -> Any:
        lifetime = lifetimes[len(connections)]
        fake = FakeSubscriber(None if lifetime is None else _in(lifetime))
        connections.append(fake)
        connected_at.append(loop.time())
        return fake

    subscriber = await _supervise_relay_subscriber(connect, reconnect=FAST)
    cleanup.append(subscriber.close)
    subscriber.want("silent", ["room"], "silent", retry=FAST)
    await until(lambda: len(connections[0].subscriptions) == 1, "subscribe")
    connections[0].subscriptions[0].accept()  # silent: the overlap lasts until the replacement's own rotation
    subscriber.rotate()
    await until(lambda: len(connections) == 2 and len(connections[1].subscriptions) == 1, "resubscribe")
    connections[1].subscriptions[0].accept()
    await until(lambda: len(connections) == 3, "second rotation", 2.0)
    # Half of the 600 ms credential: 300 ms after the replacement connected, not 300 ms plus half what was left.
    assert connected_at[2] - connected_at[1] < 0.4


# ------------------------------------------------------------------ Codex pass 7 fixes (#261)


async def test_serves_the_next_generation_while_the_old_one_drains_and_close_waits_for_the_drain() -> None:
    drained = asyncio.Event()
    connections: list[FakePublisher] = []

    async def connect(namespace: tuple[str, ...], _attempt: CancellationToken) -> Any:
        fake = FakePublisher(namespace)
        if not connections:
            original = fake.close

            async def slow_close() -> None:
                fake.close_calls += 1
                await drained.wait()
                fake.close_calls -= 1
                await original()

            fake.close = slow_close  # ty: ignore[invalid-assignment]
        connections.append(fake)
        return fake

    publisher = await _supervise_relay_publisher(connect, ["room"], reconnect=FAST)
    try:
        publisher.rotate()
        await until(lambda: connections[0].close_calls == 1, "old generation draining")
        publisher.rotate()  # the next generation is served while the old one drains
        await until(lambda: len(connections) == 3, "a rotation during the drain")
        closing = asyncio.ensure_future(publisher.close())
        await asyncio.sleep(0.02)
        assert not closing.done()
        drained.set()
        await asyncio.wait_for(closing, 1)
    finally:
        drained.set()
        await publisher.close()


async def test_takes_up_a_rotate_made_during_the_overlap_once_the_overlap_ends(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, s = await start_rotating_subscriber(cleanup)
    subscriber.want("silent", ["room"], "silent", retry=FAST)
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    s.first.subscriptions[0].accept()
    subscriber.rotate()
    await until(lambda: len(s.connections) == 2 and len(s.connections[1].subscriptions) == 1, "resubscribe")
    subscriber.rotate()  # during the overlap: the silent old subscription still holds it open
    await asyncio.sleep(0.02)
    assert len(s.connections) == 2
    s.first.end()
    await until(lambda: len(s.connections) == 3, "the rotation asked for during the overlap")


# ------------------------------------------------------------------ Codex pass 8 fixes (#261)


async def test_takes_the_replacement_when_rotate_is_called_while_an_automatic_rotation_connects_it(
    cleanup: list[Callable[[], object]],
) -> None:
    gate = asyncio.Event()
    soon = _in(0.2)
    connections: list[FakePublisher] = []

    async def connect(namespace: tuple[str, ...], _attempt: CancellationToken) -> Any:
        if len(connections) == 1:
            await gate.wait()
        fake = FakePublisher(namespace, soon)  # no fresher credential
        connections.append(fake)
        return fake

    publisher = await _supervise_relay_publisher(connect, ["room"], reconnect=FAST, rotate_before=0.15)
    cleanup.append(publisher.close)
    await asyncio.sleep(0.15)  # the automatic rotation is connecting
    publisher.rotate()
    gate.set()
    await until(lambda: len(connections) == 2 and connections[0].close_calls == 1, "replacement taken")
    assert connections[1].close_calls == 0
    assert publisher.generation is not None and publisher.generation.namespace == connections[1].namespace


# ------------------------------------------------------------------ Codex pass 9 fixes (#261)


async def test_closes_a_replacement_subscriber_that_close_could_not_see_yet() -> None:
    connections: list[FakeSubscriber] = []
    statuses: list[str] = []
    supervised: list[SupervisedSubscriber] = []

    class ClosingSubscriber(FakeSubscriber):
        @property
        def expires_at(self) -> datetime | None:
            # Read once the helper has checked for shutdown and is handing the connection back.
            supervised[0].close()
            return None

        @expires_at.setter
        def expires_at(self, _value: datetime | None) -> None:
            pass

    async def connect(_attempt: CancellationToken) -> Any:
        fake = ClosingSubscriber() if len(connections) == 1 else FakeSubscriber()
        connections.append(fake)
        return fake

    subscriber = await _supervise_relay_subscriber(
        connect, reconnect=FAST, on_status=lambda status, _error: statuses.append(status)
    )
    supervised.append(subscriber)
    subscriber.rotate()
    await until(lambda: "closed" in statuses, "closed")
    await asyncio.sleep(0.02)
    assert statuses == ["live", "closed"]
    await until(lambda: connections[1].closed.done(), "replacement closed")


async def test_closes_a_replacement_generation_that_close_could_not_see_yet() -> None:
    connections: list[FakePublisher] = []
    supervised: list[SupervisedPublisher] = []

    class ClosingPublisher(FakePublisher):
        @property
        def expires_at(self) -> datetime | None:
            _observe_close(supervised[0].close())
            return None

        @expires_at.setter
        def expires_at(self, _value: datetime | None) -> None:
            pass

    async def connect(namespace: tuple[str, ...], _attempt: CancellationToken) -> Any:
        fake = ClosingPublisher(namespace) if len(connections) == 1 else FakePublisher(namespace)
        connections.append(fake)
        return fake

    publisher = await _supervise_relay_publisher(connect, ["room"], reconnect=FAST)
    supervised.append(publisher)
    publisher.rotate()
    await until(lambda: len(connections) == 2 and connections[1].close_calls == 1, "replacement closed")
    await asyncio.wait_for(publisher.closed, 1)
    assert publisher.status == "closed"


def _observe_close(close: Awaitable[None]) -> None:
    task = asyncio.ensure_future(close)
    task.add_done_callback(lambda done: done.exception())


# ------------------------------------------------------------------ Codex pass 10 fixes (#261)


async def test_keeps_the_old_generation_open_when_closing_a_group_there_is_refused(
    cleanup: list[Callable[[], object]],
) -> None:
    publisher, p = await start_rotating_publisher(cleanup, [])
    writer = publisher.register_track("video")
    p.connections[0].tracks["video"].set_demand(True)
    await until(lambda: writer.has_demand, "demand")
    group = await writer.open_group()
    publisher.rotate()
    await until(lambda: len(p.connections) == 2 and "video" in p.connections[1].tracks, "next generation")
    p.connections[1].tracks["video"].set_demand(True)
    await writer.write(b"\x07")
    p.connections[0].tracks["video"].fail_next = RelayError("busy", code="WRITE_PENDING", phase="closing")
    with pytest.raises(RelayError) as refused:
        await group.close()
    assert refused.value.code == "WRITE_PENDING"
    await asyncio.sleep(0.02)
    assert p.connections[0].close_calls == 0
    await group.close()
    await until(lambda: p.connections[0].close_calls == 1, "old generation closed once the group did")


async def test_answers_a_rotate_made_while_a_discarded_replacement_closes(
    cleanup: list[Callable[[], object]],
) -> None:
    soon = _in(0.2)
    gate = asyncio.Event()
    connections: list[FakePublisher] = []

    async def connect(namespace: tuple[str, ...], _attempt: CancellationToken) -> Any:
        fake = FakePublisher(namespace, soon)
        if len(connections) == 1:
            original = fake.close

            async def held_close() -> None:
                await gate.wait()
                await original()

            fake.close = held_close  # ty: ignore[invalid-assignment]
        connections.append(fake)
        return fake

    publisher = await _supervise_relay_publisher(connect, ["room"], reconnect=FAST, rotate_before=0.15)

    async def close() -> None:
        gate.set()
        await publisher.close()

    cleanup.append(close)
    await until(lambda: len(connections) == 2, "automatic replacement connected")
    await asyncio.sleep(0.01)  # being discarded: its close is held
    publisher.rotate()
    gate.set()
    await until(lambda: len(connections) == 3, "the manual rotation")
    await until(lambda: connections[0].close_calls == 1, "the replacement taken")
    assert publisher.generation is not None and publisher.generation.namespace == connections[2].namespace


async def test_answers_a_rotate_called_from_on_generation_after_a_reconnect(
    cleanup: list[Callable[[], object]],
) -> None:
    connections: list[FakePublisher] = []
    rotate_from_callback = [False]
    supervised: list[SupervisedPublisher] = []

    async def connect(namespace: tuple[str, ...], _attempt: CancellationToken) -> Any:
        fake = FakePublisher(namespace)
        connections.append(fake)
        return fake

    def on_generation(_info: object) -> None:
        if rotate_from_callback[0]:
            rotate_from_callback[0] = False
            supervised[0].rotate()

    publisher = await _supervise_relay_publisher(connect, ["room"], reconnect=FAST, on_generation=on_generation)
    supervised.append(publisher)
    cleanup.append(publisher.close)
    rotate_from_callback[0] = True
    connections[0].end(RuntimeError("lost"))
    await until(lambda: len(connections) == 3, "the rotation asked for from on_generation")


# ------------------------------------------------------------------ Codex pass 11 fixes (#261)


async def test_drops_the_cutoff_when_the_outgoing_publisher_ends_so_a_restarted_one_is_delivered_from_group_0(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, s = await start_rotating_subscriber(cleanup)
    seen: list[int] = []
    subscriber.want("v", ["room"], "v", WantHandlers(on_object=lambda o: seen.append(o.group_id)), retry=FAST)
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    old = s.first.subscriptions[0]
    old.accept()
    old.send_object(9)
    subscriber.rotate()
    await until(lambda: len(s.connections) == 2 and len(s.connections[1].subscriptions) == 1, "resubscribe")
    s.connections[1].subscriptions[0].reject(0x10, "unroutable")  # the replacement is not up yet
    old.done()  # the publisher ended
    await until(lambda: len(s.connections[1].subscriptions) == 2, "retry")
    restarted = s.connections[1].subscriptions[1]
    restarted.accept()
    restarted.send_object(0)
    restarted.send_object(1)
    assert seen == [9, 0, 1]


# ------------------------------------------------------------------ Codex pass 12 fixes (#261)


async def test_drops_the_cutoff_when_the_replacement_hears_the_publisher_end_before_taking_over(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, s = await start_rotating_subscriber(cleanup)
    seen: list[int] = []
    subscriber.want("v", ["room"], "v", WantHandlers(on_object=lambda o: seen.append(o.group_id)), retry=FAST)
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    s.first.subscriptions[0].accept()
    s.first.subscriptions[0].send_object(10)
    subscriber.rotate()
    await until(lambda: len(s.connections) == 2 and len(s.connections[1].subscriptions) == 1, "resubscribe")
    s.connections[1].subscriptions[0].accept()
    s.connections[1].subscriptions[0].done()  # the publisher ended, heard on the replacement
    await until(lambda: len(s.connections[1].subscriptions) == 2, "retry")
    restarted = s.connections[1].subscriptions[1]
    restarted.accept()
    restarted.send_object(0)  # the restarted publisher, while the old subscription is still open
    s.first.end()  # the old connection closes without a done
    restarted.send_object(1)
    assert seen == [10, 0, 1]


async def test_answers_a_queued_rotate_with_the_replacement_even_when_the_old_connection_ended_first(
    cleanup: list[Callable[[], object]],
) -> None:
    gate = asyncio.Event()
    connections: list[FakeSubscriber] = []

    async def connect(_attempt: CancellationToken) -> Any:
        if len(connections) == 1:
            await gate.wait()
        fake = FakeSubscriber(_in(0.2) if not connections else None)
        connections.append(fake)
        return fake

    subscriber = await _supervise_relay_subscriber(connect, reconnect=FAST, rotate_before=0.15)
    cleanup.append(subscriber.close)
    await asyncio.sleep(0.15)  # the automatic rotation is connecting
    subscriber.rotate()
    connections[0].end(RuntimeError("lost"))
    gate.set()
    await until(lambda: len(connections) == 2 and subscriber.status == "live", "replacement adopted")
    await asyncio.sleep(0.05)
    assert len(connections) == 2


# ------------------------------------------------------------------ Codex pass 13 fixes (#261)


async def test_does_not_promote_a_replacement_that_ended_in_the_turn_the_old_connection_did(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, s = await start_rotating_subscriber(cleanup)
    seen: list[int] = []
    subscriber.want("v", ["room"], "v", WantHandlers(on_object=lambda o: seen.append(o.group_id)), retry=FAST)
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    s.first.subscriptions[0].accept()
    s.first.subscriptions[0].send_object(7)
    subscriber.rotate()
    await until(lambda: len(s.connections) == 2 and len(s.connections[1].subscriptions) == 1, "resubscribe")
    s.connections[1].subscriptions[0].accept()
    s.connections[1].subscriptions[0].terminate(0x0, "internal error")
    s.first.end()  # the same turn
    await until(lambda: len(s.connections[1].subscriptions) == 2, "retry")
    retry = s.connections[1].subscriptions[1]
    retry.accept()
    retry.send_object(7)
    retry.send_object(8)
    assert seen == [7, 8]


async def test_releases_the_old_generation_when_the_relay_resets_demand_under_an_open_group_there(
    cleanup: list[Callable[[], object]],
) -> None:
    publisher, p = await start_rotating_publisher(cleanup, [])
    writer = publisher.register_track("video")
    p.connections[0].tracks["video"].set_demand(True)
    await until(lambda: writer.has_demand, "demand")
    await writer.open_group()
    publisher.rotate()
    await until(lambda: len(p.connections) == 2 and "video" in p.connections[1].tracks, "next generation")
    p.connections[1].tracks["video"].set_demand(True)
    await writer.write(b"\x07")
    await asyncio.sleep(0.02)
    assert p.connections[0].close_calls == 0
    p.connections[0].tracks["video"].reset_groups()
    await until(lambda: p.connections[0].close_calls == 1, "old generation closed once the group was reset", 0.5)


# ------------------------------------------------------------------ rotation with generations (merged with #282)


async def _following(
    cleanup: list[Callable[[], object]],
) -> tuple[SupervisedSubscriber, ExpiringSubscribers, list[str]]:
    subscriber, s = await start_rotating_subscriber(cleanup)
    seen: list[str] = []
    subscriber.want(
        "v",
        ["room"],
        "v",
        WantHandlers(
            on_object=lambda obj: seen.append(f"object {obj.group_id}"),
            on_restarted=lambda reason, _namespace: seen.append(f"restarted {reason}"),
            on_switched=lambda info: seen.append(f"switched {info.from_namespace[1]} -> {info.to_namespace[1]}"),
        ),
        generations=True,
        retry=FAST,
    )
    await until(lambda: len(s.first.discoveries) == 1, "discovery")
    return subscriber, s, seen


async def test_moves_a_followed_generation_to_the_new_connection_silently(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, s, seen = await _following(cleanup)
    generation = new_generation(1_000)
    s.first.discoveries[0].push("announced", ["room", generation])
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    s.first.subscriptions[0].accept()
    s.first.subscriptions[0].send_object(3)
    subscriber.rotate()
    await until(lambda: len(s.connections) == 2 and len(s.connections[1].discoveries) == 1, "discovery on the next")
    s.connections[1].discoveries[0].push("announced", ["room", generation])
    await until(lambda: len(s.connections[1].subscriptions) == 1, "replacement")
    replacement = s.connections[1].subscriptions[0]
    assert replacement.namespace == ("room", generation)
    replacement.accept()
    replacement.send_object(3)
    replacement.send_object(4)
    assert seen == ["object 3", "object 4"]
    await until(lambda: s.first.subscriptions[0].cancelled, "old subscription cancelled")


async def test_keeps_skipping_repeats_on_a_rotation_after_the_generation_a_move_left_behind_ended(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, s, seen = await _following(cleanup)
    older = new_generation(1_000)
    newer = new_generation(2_000)
    discovery = s.first.discoveries[0]
    discovery.push("announced", ["room", older])
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    first = s.first.subscriptions[0]
    first.accept()
    first.send_object(5)
    discovery.push("announced", ["room", newer])
    await until(lambda: len(s.first.subscriptions) == 2, "move")
    moved = s.first.subscriptions[1]
    moved.accept()
    moved.send_object(0)
    first.done()  # the old generation's publisher closed after the move: not the current publisher
    moved.send_object(1)
    subscriber.rotate()
    await until(lambda: len(s.connections) == 2 and len(s.connections[1].discoveries) == 1, "discovery on the next")
    s.connections[1].discoveries[0].push("announced", ["room", newer])
    await until(lambda: len(s.connections[1].subscriptions) == 1, "replacement")
    replacement = s.connections[1].subscriptions[0]
    replacement.accept()
    replacement.send_object(1)  # already delivered
    replacement.send_object(2)
    assert seen == ["object 5", f"switched {older} -> {newer}", "object 0", "object 1", "object 2"]


async def test_reports_a_newer_generation_found_on_the_new_connection_as_a_move(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, s, seen = await _following(cleanup)
    older = new_generation(1_000)
    newer = new_generation(2_000)
    s.first.discoveries[0].push("announced", ["room", older])
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    s.first.subscriptions[0].accept()
    s.first.subscriptions[0].send_object(3)
    subscriber.rotate()
    await until(lambda: len(s.connections) == 2 and len(s.connections[1].discoveries) == 1, "discovery on the next")
    s.connections[1].discoveries[0].push("announced", ["room", older])
    s.connections[1].discoveries[0].push("announced", ["room", newer])
    await until(
        lambda: bool(s.connections[1].subscriptions) and s.connections[1].latest.namespace[1] == newer,
        "replacement on the newer generation",
    )
    replacement = s.connections[1].latest
    replacement.accept()
    replacement.send_object(0)
    assert seen == ["object 3", f"switched {older} -> {newer}", "object 0"]


# ------------------------------------------------------------------ Codex review of the merge (#261)


async def test_skips_an_object_the_old_subscription_delivered_that_reaches_the_replacement_late(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, s = await start_rotating_subscriber(cleanup)
    seen: list[int] = []
    subscriber.want("v", ["room"], "v", WantHandlers(on_object=lambda o: seen.append(o.group_id)), retry=FAST)
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe")
    s.first.subscriptions[0].accept()
    s.first.subscriptions[0].send_object(4)
    subscriber.rotate()
    await until(lambda: len(s.connections) == 2 and len(s.connections[1].subscriptions) == 1, "resubscribe")
    replacement = s.connections[1].subscriptions[0]
    replacement.accept()
    replacement.send_object(5)  # cuts over
    replacement.send_object(4)  # group 4's stream arrives late
    replacement.send_object(6)
    assert seen == [4, 5, 6]


async def test_keeps_skipping_repeats_when_only_another_targets_publisher_ended(
    cleanup: list[Callable[[], object]],
) -> None:
    subscriber, s = await start_rotating_subscriber(cleanup)
    seen: list[str] = []
    handlers = WantHandlers(on_object=lambda o: seen.append(f"{o.group_id}"))
    subscriber.want("v", ["room"], "a", handlers, retry=FAST)
    await until(lambda: len(s.first.subscriptions) == 1, "subscribe a")
    s.first.subscriptions[0].accept()
    s.first.subscriptions[0].send_object(4)
    subscriber.want("v", ["room"], "b", handlers, retry=FAST)
    await until(lambda: len(s.first.subscriptions) == 2, "pending b")
    s.first.subscriptions[1].done()  # b's publisher ended
    subscriber.want("v", ["room"], "a", handlers, retry=FAST)  # back to a
    await asyncio.sleep(0.01)
    subscriber.rotate()
    await until(lambda: len(s.connections) == 2 and len(s.connections[1].subscriptions) == 1, "replacement for a")
    replacement = s.connections[1].subscriptions[0]
    assert replacement.track == "a"
    replacement.accept()
    replacement.send_object(4)  # a's cached group, already delivered
    replacement.send_object(5)
    assert seen == ["4", "5"]
