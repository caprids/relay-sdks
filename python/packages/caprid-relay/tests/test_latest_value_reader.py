"""The latest-value reader: superseded and late values dropped, one catch-up per late run, restarts start it over."""

from __future__ import annotations

import logging
from typing import Any

import pytest
from relay_helpers.supervision_fakes import FakeSubscriber, FakeSubscription, until

from caprid_relay import CancellationToken, SupervisionBackoff, WantSwitchedInfo
from caprid_relay.media import LatestValueReader, LatestValueStats
from caprid_relay.supervised_subscriber import _supervise_relay_subscriber
from moq_core import MoqtObject, SubscribeOk


def obj(group: int, object_id: int = 0, *, late: bool = False) -> MoqtObject:
    return MoqtObject(group_id=group, subgroup_id=0, object_id=object_id, payload=b"late" if late else b"fresh")


def is_late(o: MoqtObject) -> bool:
    return o.payload == b"late"


class Seen:
    def __init__(self) -> None:
        self.values: list[tuple[int, int]] = []
        self.catch_ups = 0

    def value(self, o: MoqtObject) -> None:
        self.values.append((o.group_id, o.object_id))

    def catch_up(self) -> None:
        self.catch_ups += 1


def switched() -> WantSwitchedInfo:
    return WantSwitchedInfo(
        subscribed=SubscribeOk(track_alias=2),
        from_namespace=("room",),
        from_track="a",
        to_namespace=("room",),
        to_track="b",
    )


def test_delivers_newer_values_and_drops_older_groups_and_objects_as_superseded() -> None:
    seen = Seen()
    reader = LatestValueReader(seen.value)
    on_object = reader.subscription_handlers().on_object
    assert on_object is not None
    for o in (obj(5), obj(3), obj(5), obj(6, 2), obj(6, 1), obj(6, 3), obj(4), obj(7)):
        on_object(o)
    assert seen.values == [(5, 0), (6, 2), (6, 3), (7, 0)]
    assert reader.stats == LatestValueStats(delivered=4, superseded=4, late=0, catch_ups=0)


def test_ignores_status_objects() -> None:
    seen = Seen()
    reader = LatestValueReader(seen.value)
    on_object = reader.subscription_handlers().on_object
    assert on_object is not None
    on_object(MoqtObject(group_id=9, subgroup_id=0, object_id=0, payload=b"", status="end-of-group"))
    on_object(obj(2))
    assert seen.values == [(2, 0)]
    assert reader.stats == LatestValueStats(delivered=1)


def test_a_late_value_is_dropped_and_still_supersedes_older_ones() -> None:
    seen = Seen()
    reader = LatestValueReader(seen.value, late=is_late, on_catch_up=seen.catch_up)
    on_object = reader.subscription_handlers().on_object
    assert on_object is not None
    on_object(obj(1))
    on_object(obj(3, late=True))
    on_object(obj(2))
    assert seen.values == [(1, 0)]
    assert seen.catch_ups == 1
    assert reader.stats == LatestValueStats(delivered=1, superseded=1, late=1, catch_ups=1)


def test_signals_one_catch_up_per_run_of_late_values_until_a_value_is_delivered() -> None:
    seen = Seen()
    reader = LatestValueReader(seen.value, late=is_late, on_catch_up=seen.catch_up)
    on_object = reader.subscription_handlers().on_object
    assert on_object is not None
    for group in (1, 2, 3):
        on_object(obj(group, late=True))
    assert seen.catch_ups == 1
    on_object(obj(4))
    on_object(obj(5, late=True))
    on_object(obj(6, late=True))
    assert seen.values == [(4, 0)]
    assert seen.catch_ups == 2
    assert reader.stats == LatestValueStats(delivered=1, superseded=0, late=5, catch_ups=2)


def test_a_want_restart_starts_the_reader_over_and_then_runs_the_applications_callback() -> None:
    seen = Seen()
    order: list[str] = []
    reader = LatestValueReader(seen.value, late=is_late, on_catch_up=seen.catch_up)
    handlers = reader.want_handlers(on_restarted=lambda reason, _namespace: order.append(reason))
    assert handlers.on_object is not None and handlers.on_restarted is not None
    handlers.on_object(obj(40))
    handlers.on_object(obj(41, late=True))
    handlers.on_restarted("catch-up", ("room",))
    # The replacement numbers its groups afresh, and a late value there does not signal again.
    handlers.on_object(obj(2, late=True))
    handlers.on_object(obj(3))
    assert order == ["catch-up"]
    assert seen.values == [(40, 0), (3, 0)]
    assert seen.catch_ups == 1
    assert reader.stats == LatestValueStats(delivered=2, superseded=0, late=2, catch_ups=1)


def test_a_want_move_starts_the_reader_over_and_then_runs_the_applications_callback() -> None:
    seen = Seen()
    moves: list[str] = []
    reader = LatestValueReader(seen.value)
    handlers = reader.want_handlers(on_switched=lambda info: moves.append(info.to_track))
    assert handlers.on_object is not None and handlers.on_switched is not None
    handlers.on_object(obj(90))
    handlers.on_switched(switched())
    handlers.on_object(obj(1))
    assert moves == ["b"]
    assert seen.values == [(90, 0), (1, 0)]
    assert reader.stats == LatestValueStats(delivered=2)


def test_a_restart_does_not_rearm_the_catch_up_until_a_value_is_delivered() -> None:
    seen = Seen()
    reader = LatestValueReader(seen.value, late=is_late, on_catch_up=seen.catch_up)
    handlers = reader.want_handlers()
    assert handlers.on_object is not None and handlers.on_restarted is not None
    handlers.on_object(obj(1, late=True))
    handlers.on_object(obj(2, late=True))
    handlers.on_restarted("catch-up", ("room",))
    handlers.on_object(obj(0, late=True))
    assert seen.catch_ups == 1
    handlers.on_object(obj(1))
    handlers.on_object(obj(2, late=True))
    assert seen.catch_ups == 2


def test_raising_callbacks_are_logged_and_the_reader_carries_on(caplog: pytest.LogCaptureFixture) -> None:
    calls: list[str] = []

    def on_value(o: MoqtObject) -> None:
        calls.append(f"value {o.group_id}")
        raise RuntimeError("on_value")

    def late(o: MoqtObject) -> bool:
        if o.group_id == 2:
            raise RuntimeError("late")
        return o.group_id == 3

    def on_catch_up() -> None:
        calls.append("catch-up")
        raise RuntimeError("on_catch_up")

    reader = LatestValueReader(on_value, late=late, on_catch_up=on_catch_up)
    on_object = reader.subscription_handlers().on_object
    assert on_object is not None
    with caplog.at_level(logging.ERROR, logger="caprid_relay"):
        on_object(obj(1))
        on_object(obj(2))  # a raising predicate counts as false: delivered
        on_object(obj(3))
        on_object(obj(4))
    assert calls == ["value 1", "value 2", "catch-up", "value 4"]
    assert reader.stats == LatestValueStats(delivered=3, superseded=0, late=1, catch_ups=1)
    messages = [record.getMessage() for record in caplog.records]
    assert sum("on_value raised" in message for message in messages) == 3
    assert sum("late raised" in message for message in messages) == 1
    assert sum("on_catch_up raised" in message for message in messages) == 1


def test_subscription_handlers_pass_the_applications_callbacks_through() -> None:
    errors: list[object] = []
    done: list[object] = []
    handlers = LatestValueReader(lambda _o: None).subscription_handlers(
        on_error=errors.append, on_publisher_done=done.append
    )
    assert handlers.on_error == errors.append
    assert handlers.on_publisher_done == done.append


def test_stats_are_a_snapshot() -> None:
    reader = LatestValueReader(lambda _o: None)
    before = reader.stats
    on_object = reader.subscription_handlers().on_object
    assert on_object is not None
    on_object(obj(1))
    assert before == LatestValueStats()
    assert reader.stats.delivered == 1


async def test_a_late_run_catches_a_want_up_to_the_live_edge() -> None:
    connections: list[FakeSubscriber] = []

    async def connect(_attempt: CancellationToken) -> Any:
        fake = FakeSubscriber()
        connections.append(fake)
        return fake

    subscriber = await _supervise_relay_subscriber(connect, reconnect=SupervisionBackoff(initial_delay=0, max_delay=0))
    try:
        seen = Seen()
        reasons: list[str] = []
        reader = LatestValueReader(seen.value, late=is_late, on_catch_up=lambda: subscriber.catch_up("telemetry"))
        subscriber.want(
            "telemetry",
            ["room"],
            "telemetry",
            reader.want_handlers(on_restarted=lambda reason, _namespace: reasons.append(reason)),
            retry=SupervisionBackoff(initial_delay=60, max_delay=60),
        )
        await until(lambda: len(connections[0].subscriptions) == 1, "subscribe")
        first = connections[0].latest
        first.accept()
        deliver(first, obj(10))
        deliver(first, obj(11, late=True))
        await until(lambda: len(connections[0].subscriptions) == 2, "resubscribe at the live edge")
        assert first.cancelled
        second = connections[0].latest
        second.accept()
        deliver(second, obj(1))
        assert reasons == ["catch-up"]
        assert seen.values == [(10, 0), (1, 0)]
        assert reader.stats == LatestValueStats(delivered=2, superseded=0, late=1, catch_ups=1)
    finally:
        subscriber.close()


def deliver(subscription: FakeSubscription, o: MoqtObject) -> None:
    on_object = subscription.handlers.on_object
    assert on_object is not None
    on_object(o)
