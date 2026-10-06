"""The switchable subscription through its own interface: each subscription underneath is a real
product subscription over a fake core, so the tests drive SUBSCRIBE_OK, objects and endings per
track. The live switch with no gap is in scripts/test_relay.py.
"""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, cast
from unittest.mock import Mock

import pytest
from core_helpers.waiting import wait_for
from relay_helpers.relay_hub import create_relay_hub

from moq_core import MessageParameters, MoqtObject, RequestError, SubgroupEnd, SubgroupReset, SubscribeOk, Subscription
from moq_core import SubscriptionHandlers as CoreHandlers
from mux_relay import (
    RelayError,
    SubscriptionConnectionClosedError,
    SubscriptionRequestError,
    SwitchableHandlers,
    SwitchableSubscription,
    SwitchedInfo,
    connect_relay_publisher,
    connect_relay_subscriber,
)
from mux_relay._subscription import RelaySubscription, SubscriptionConsumer, SubscriptionDependencies
from mux_relay.switchable import snapshot_switchable_handlers

OK = cast(SubscribeOk, Mock(name="subscribe_ok"))


def obj(group_id: int, object_id: int = 0) -> MoqtObject:
    return MoqtObject(group_id=group_id, object_id=object_id, subgroup_id=0, payload=bytes([group_id % 256]))


@dataclass
class Opened:
    track: str
    parameters: MessageParameters | None
    core: CoreHandlers | None = None
    cancel: Mock = field(default_factory=Mock)

    def ok(self, info: SubscribeOk = OK) -> None:
        assert self.core is not None and self.core.on_ok is not None
        self.core.on_ok(info)

    def object(self, value: MoqtObject) -> None:
        assert self.core is not None and self.core.on_object is not None
        self.core.on_object(value)

    def error(self, error: RequestError) -> None:
        assert self.core is not None and self.core.on_error is not None
        self.core.on_error(error)

    def subgroup_end(self, group_id: int) -> None:
        assert self.core is not None and self.core.on_subgroup_end is not None
        self.core.on_subgroup_end(SubgroupEnd(group_id=group_id, subgroup_id=0, end_of_group=True))

    def subgroup_reset(self, group_id: int) -> None:
        assert self.core is not None and self.core.on_subgroup_reset is not None
        self.core.on_subgroup_reset(SubgroupReset(group_id=group_id, error=RuntimeError("reset")))


class Harness:
    def __init__(
        self,
        handlers: SwitchableHandlers | None = None,
        track: str = "video-mid",
        parameters: MessageParameters | None = None,
    ) -> None:
        self.opened: list[Opened] = []
        self.observers: list[Callable[[BaseException | None], None]] = []
        self.closed = False
        self.switchable = SwitchableSubscription(self._open, track, parameters, snapshot_switchable_handlers(handlers))

    def _open(
        self, track: str, parameters: MessageParameters | None, consumer: SubscriptionConsumer
    ) -> RelaySubscription:
        if self.closed:
            raise RelayError("Relay connection is closed", code="CLOSED", phase="connection")
        opened = Opened(track, parameters)
        self.opened.append(opened)

        def subscribe(handlers: CoreHandlers) -> Subscription:
            opened.core = handlers
            core = Mock(spec=Subscription)
            core.request_id = len(self.opened)
            core.cancel = opened.cancel
            return cast(Subscription, core)

        def observe_closed(observer: Callable[[BaseException | None], None]) -> Callable[[], None]:
            self.observers.append(observer)
            return lambda: self.observers.remove(observer) if observer in self.observers else None

        return RelaySubscription(
            SubscriptionDependencies(subscribe=subscribe, observe_closed=observe_closed, transport_ended=lambda: False),
            [consumer],
        )

    def track(self, name: str, nth: int = 0) -> Opened:
        return [entry for entry in self.opened if entry.track == name][nth]

    def close_connection(self, error: BaseException | None = None) -> None:
        self.closed = True
        for observer in list(self.observers):
            observer(error)


def recording() -> tuple[list[tuple[object, ...]], SwitchableHandlers]:
    calls: list[tuple[object, ...]] = []
    handlers = SwitchableHandlers(
        on_subscribed=lambda _info, track: calls.append(("subscribed", track)),
        on_error=lambda error, track: calls.append(("error", error.error_code, track)),
        on_object=lambda value, track: calls.append(("object", value.group_id, value.object_id, track)),
        on_subgroup_end=lambda info, track: calls.append(("subgroup_end", info.group_id, track)),
        on_subgroup_reset=lambda info, track: calls.append(("subgroup_reset", info.group_id, track)),
        on_publisher_done=lambda _info, track: calls.append(("publisher_done", track)),
        on_switched=lambda info: calls.append(("switched", info.from_track, info.to_track)),
    )
    return calls, handlers


async def turn_ends() -> None:
    for _ in range(3):
        await asyncio.sleep(0)


class TestSwitchable:
    async def test_delivers_the_first_subscription_with_its_track_name(self) -> None:
        calls, handlers = recording()
        harness = Harness(handlers)
        mid = harness.track("video-mid")
        mid.ok()
        mid.object(obj(5))
        assert await harness.switchable.subscribed is OK
        assert harness.switchable.track == "video-mid"
        assert harness.switchable.pending is None
        assert calls == [("subscribed", "video-mid"), ("object", 5, 0, "video-mid")]

    async def test_keeps_the_old_track_delivering_until_the_new_one_cuts_over_then_cancels_it(self) -> None:
        calls, handlers = recording()
        harness = Harness(handlers)
        mid = harness.track("video-mid")
        mid.ok()
        mid.object(obj(5))
        switched = harness.switchable.switch_to("video-hi")
        assert harness.switchable.pending == "video-hi"
        hi = harness.track("video-hi")
        hi.ok()
        mid.object(obj(5, 1))
        hi.object(obj(5))
        mid.object(obj(5, 2))
        assert await switched == SwitchedInfo(from_track="video-mid", to_track="video-hi", subscribed=OK)
        mid.cancel.assert_called_once()
        hi.cancel.assert_not_called()
        assert harness.switchable.track == "video-hi"
        assert harness.switchable.pending is None
        hi.object(obj(5, 1))
        assert calls == [
            ("subscribed", "video-mid"),
            ("object", 5, 0, "video-mid"),
            ("object", 5, 1, "video-mid"),
            ("switched", "video-mid", "video-hi"),
            ("object", 5, 0, "video-hi"),
            ("object", 5, 1, "video-hi"),
        ]

    async def test_holds_the_new_track_until_the_predicate_passes_discarding_what_it_refuses(self) -> None:
        calls, handlers = recording()
        harness = Harness(handlers)
        mid = harness.track("video-mid")
        mid.ok()
        switched = harness.switchable.switch_to("video-lo", until=lambda value: value.group_id >= 6)
        lo = harness.track("video-lo")
        lo.ok()
        lo.object(obj(5))
        lo.subgroup_end(5)
        lo.subgroup_reset(5)
        mid.object(obj(6))
        lo.object(obj(6))
        await switched
        assert calls == [
            ("subscribed", "video-mid"),
            ("object", 6, 0, "video-mid"),
            ("switched", "video-mid", "video-lo"),
            ("object", 6, 0, "video-lo"),
        ]

    async def test_delivers_refused_objects_of_later_groups_after_the_cutover_object(self) -> None:
        calls, handlers = recording()
        harness = Harness(handlers)
        harness.track("video-mid").ok()
        switched = harness.switchable.switch_to(
            "video-lo", until=lambda value: value.group_id == 7 and value.object_id == 0
        )
        lo = harness.track("video-lo")
        lo.ok()
        lo.object(obj(8))  # ahead of the timeline: refused, held
        lo.subgroup_end(8)
        lo.object(obj(4))  # stale: refused, and earlier than the cutover
        lo.object(obj(7, 1))  # the cutover group's later object, arrived on another subgroup
        lo.subgroup_end(7)  # and that subgroup's end
        lo.object(obj(7))
        await switched
        assert calls == [
            ("subscribed", "video-mid"),
            ("switched", "video-mid", "video-lo"),
            ("object", 7, 0, "video-lo"),
            ("object", 8, 0, "video-lo"),
            ("subgroup_end", 8, "video-lo"),
            ("object", 7, 1, "video-lo"),
            ("subgroup_end", 7, "video-lo"),
        ]

    async def test_switches_to_the_same_track_with_new_parameters_which_are_not_inherited(self) -> None:
        parameters = MessageParameters(subscriber_priority=3)
        harness = Harness(parameters=parameters)
        harness.track("video-mid").ok()
        switched = harness.switchable.switch_to("video-mid")
        assert [entry.parameters for entry in harness.opened] == [parameters, None]
        harness.track("video-mid", 1).ok()
        harness.track("video-mid", 1).object(obj(1))
        info = await switched
        assert (info.from_track, info.to_track) == ("video-mid", "video-mid")
        harness.track("video-mid", 0).cancel.assert_called_once()

    async def test_drains_the_old_track_through_its_largest_group_after_the_cutover_then_cancels_it(self) -> None:
        calls, handlers = recording()
        harness = Harness(handlers)
        mid = harness.track("video-mid")
        mid.ok()
        mid.object(obj(44))
        mid.object(obj(46))
        switched = harness.switchable.switch_to("video-hi", drain=0.02)
        hi = harness.track("video-hi")
        hi.ok()
        # Each track numbers its own groups.
        hi.object(obj(3))
        await switched
        mid.cancel.assert_not_called()
        mid.object(obj(45))
        mid.object(obj(46, 1))
        mid.subgroup_reset(43)
        mid.object(obj(47))
        mid.subgroup_end(47)
        hi.object(obj(4))
        await wait_for(lambda: mid.cancel.assert_called_once())
        mid.object(obj(45, 1))
        assert calls[1:] == [
            ("object", 44, 0, "video-mid"),
            ("object", 46, 0, "video-mid"),
            ("switched", "video-mid", "video-hi"),
            ("object", 3, 0, "video-hi"),
            ("object", 45, 0, "video-mid"),
            ("object", 46, 1, "video-mid"),
            ("subgroup_reset", 43, "video-mid"),
            ("object", 4, 0, "video-hi"),
        ]

    async def test_drains_nothing_when_the_old_track_delivered_nothing(self) -> None:
        harness = Harness()
        harness.track("video-mid").ok()
        switched = harness.switchable.switch_to("video-hi", drain=60)
        harness.track("video-hi").ok()
        harness.track("video-hi").object(obj(1))
        await switched
        harness.track("video-mid").cancel.assert_called_once()

    async def test_stops_a_drain_when_the_switchable_is_cancelled(self) -> None:
        harness = Harness()
        harness.track("video-mid").ok()
        harness.track("video-mid").object(obj(1))
        switched = harness.switchable.switch_to("video-hi", drain=60)
        harness.track("video-hi").ok()
        harness.track("video-hi").object(obj(1))
        await switched
        harness.switchable.cancel()
        harness.track("video-mid").cancel.assert_called_once()
        harness.track("video-hi").cancel.assert_called_once()

    async def test_raises_from_a_switch_whose_subscription_is_refused_and_keeps_the_active_track(self) -> None:
        calls, handlers = recording()
        harness = Harness(handlers)
        harness.track("video-mid").ok()
        switched = harness.switchable.switch_to("video-hi")
        refusal = RequestError(error_code=0x10, retry_interval=0, reason="not found")
        harness.track("video-hi").error(refusal)
        with pytest.raises(SubscriptionRequestError) as raised:
            await switched
        assert raised.value.request_error is refusal
        assert harness.switchable.pending is None
        harness.track("video-mid").object(obj(9))
        assert calls == [("subscribed", "video-mid"), ("object", 9, 0, "video-mid")]

    async def test_cancels_a_pending_switch_that_a_newer_one_supersedes(self) -> None:
        harness = Harness()
        harness.track("video-mid").ok()
        first = harness.switchable.switch_to("video-hi")
        second = harness.switchable.switch_to("video-lo")
        with pytest.raises(RelayError) as raised:
            await first
        assert raised.value.code == "CLOSED"
        harness.track("video-hi").cancel.assert_called_once()
        assert harness.switchable.pending == "video-lo"
        harness.track("video-lo").ok()
        harness.track("video-lo").object(obj(1))
        assert (await second).to_track == "video-lo"

    async def test_abandons_a_pending_switch_when_its_token_is_cancelled(self) -> None:
        from mux_relay import CancellationToken

        harness = Harness()
        harness.track("video-mid").ok()
        token = CancellationToken()
        switched = harness.switchable.switch_to("video-hi", cancellation=token)
        reason = RuntimeError("probe took too long")
        token.cancel(reason)
        with pytest.raises(RuntimeError) as raised:
            await switched
        assert raised.value is reason
        harness.track("video-hi").cancel.assert_called_once()
        assert harness.switchable.track == "video-mid"

    async def test_abandons_a_switch_whose_predicate_raises(self) -> None:
        harness = Harness()
        harness.track("video-mid").ok()
        failure = ValueError("bad predicate")

        def until(_value: MoqtObject) -> bool:
            raise failure

        switched = harness.switchable.switch_to("video-hi", until=until)
        harness.track("video-hi").ok()
        harness.track("video-hi").object(obj(1))
        with pytest.raises(ValueError) as raised:
            await switched
        assert raised.value is failure
        assert harness.switchable.track == "video-mid"

    async def test_ends_with_the_active_subscription_and_cancels_a_pending_switch(self) -> None:
        calls, handlers = recording()
        harness = Harness(handlers)
        harness.track("video-mid").ok()
        switched = harness.switchable.switch_to("video-hi")
        harness.track("video-mid").error(RequestError(error_code=0, retry_interval=0, reason="refused"))
        end = await harness.switchable.ended
        assert (end.kind, end.cause) == ("terminated", "request-error")
        with pytest.raises(RelayError) as raised:
            await switched
        assert raised.value.code == "CLOSED"
        harness.track("video-hi").cancel.assert_called_once()
        assert calls[-1] == ("error", 0, "video-mid")
        with pytest.raises(RelayError):
            harness.switchable.switch_to("video-lo")

    async def test_raises_subscribed_and_ends_when_the_first_subscription_is_refused(self) -> None:
        calls, handlers = recording()
        harness = Harness(handlers)
        harness.track("video-mid").error(RequestError(error_code=0x10, retry_interval=0, reason="unroutable"))
        with pytest.raises(SubscriptionRequestError):
            await harness.switchable.subscribed
        assert (await harness.switchable.ended).kind == "rejected"
        assert calls == [("error", 0x10, "video-mid")]

    async def test_settles_subscribed_from_a_switch_that_cut_over_before_the_first_answer(self) -> None:
        harness = Harness()
        switched = harness.switchable.switch_to("video-lo")
        other = cast(SubscribeOk, Mock(name="other"))
        harness.track("video-lo").ok(other)
        harness.track("video-lo").object(obj(1))
        await switched
        assert await harness.switchable.subscribed is other
        harness.track("video-mid").cancel.assert_called_once()

    async def test_cancels_both_subscriptions_on_cancel(self) -> None:
        harness = Harness()
        harness.track("video-mid").ok()
        switched = harness.switchable.switch_to("video-hi")
        reason = RuntimeError("viewer left")
        harness.switchable.cancel(reason)
        with pytest.raises(RuntimeError):
            await switched
        end = await harness.switchable.ended
        assert (end.kind, end.accepted, end.error) == ("cancelled", True, reason)
        harness.track("video-mid").cancel.assert_called_once()
        harness.track("video-hi").cancel.assert_called_once()

    async def test_ends_on_connection_closure_raising_from_a_pending_switch(self) -> None:
        harness = Harness()
        harness.track("video-mid").ok()
        switched = harness.switchable.switch_to("video-hi")
        failure = RuntimeError("lost")
        harness.close_connection(failure)
        with pytest.raises(SubscriptionConnectionClosedError):
            await switched
        end = await harness.switchable.ended
        assert (end.cause, end.error) == ("connection-closed", failure)

    async def test_lets_on_switched_switch_again_before_the_cutover_object_is_delivered(self) -> None:
        calls: list[tuple[object, ...]] = []
        again: list[Awaitable[SwitchedInfo]] = []
        holder: dict[str, SwitchableSubscription] = {}

        def on_switched(info: SwitchedInfo) -> None:
            calls.append(("switched", info.to_track))
            if info.to_track == "video-hi":
                again.append(holder["switchable"].switch_to("video-lo"))

        harness = Harness(
            SwitchableHandlers(
                on_object=lambda value, track: calls.append(("object", value.group_id, track)),
                on_switched=on_switched,
            )
        )
        holder["switchable"] = harness.switchable
        harness.track("video-mid").ok()
        harness.switchable.switch_to("video-hi")
        harness.track("video-hi").ok()
        harness.track("video-hi").object(obj(1))
        assert harness.switchable.pending == "video-lo"
        assert calls == [("switched", "video-hi"), ("object", 1, "video-hi")]
        harness.track("video-lo").ok()
        harness.track("video-lo").object(obj(2))
        info = await again[0]
        assert (info.from_track, info.to_track) == ("video-hi", "video-lo")

    async def test_ignores_a_predicate_that_switched_again_and_leaves_the_newer_switch_pending(self) -> None:
        harness = Harness()
        harness.track("video-mid").ok()
        replacement: list[Awaitable[SwitchedInfo]] = []

        def until(_value: MoqtObject) -> bool:
            replacement.append(harness.switchable.switch_to("video-lo"))
            return True

        first = harness.switchable.switch_to("video-hi", until=until)
        harness.track("video-hi").ok()
        harness.track("video-hi").object(obj(1))
        with pytest.raises(RelayError):
            await first
        assert harness.switchable.track == "video-mid"
        assert harness.switchable.pending == "video-lo"
        harness.track("video-lo").ok()
        harness.track("video-lo").object(obj(2))
        info = await replacement[0]
        assert (info.from_track, info.to_track, info.subscribed) == ("video-mid", "video-lo", OK)

    async def test_closes_a_coroutine_a_predicate_returned_before_refusing_it(self) -> None:
        async def later() -> bool:
            return True

        returned: list[Any] = []

        def until(_value: MoqtObject) -> bool:
            coroutine = later()
            returned.append(coroutine)
            return cast(Any, coroutine)

        harness = Harness()
        harness.track("video-mid").ok()
        switched = harness.switchable.switch_to("video-hi", until=until)
        harness.track("video-hi").ok()
        harness.track("video-hi").object(obj(1))
        with pytest.raises(TypeError):
            await switched
        assert returned[0].cr_frame is None

    async def test_checks_the_track_before_its_own_ended_state(self) -> None:
        harness = Harness()
        harness.switchable.cancel()
        with pytest.raises(TypeError):
            harness.switchable.switch_to("")
        with pytest.raises(RelayError):
            harness.switchable.switch_to("video-hi")

    async def test_abandons_a_switch_whose_predicate_raises_cancelled_error(self) -> None:
        harness = Harness()
        harness.track("video-mid").ok()

        def until(_value: MoqtObject) -> bool:
            raise asyncio.CancelledError

        switched = harness.switchable.switch_to("video-hi", until=until)
        harness.track("video-hi").ok()
        harness.track("video-hi").object(obj(1))
        with pytest.raises(asyncio.CancelledError):
            await switched
        harness.track("video-hi").cancel.assert_called_once()
        assert harness.switchable.pending is None

    async def test_checks_its_arguments_at_once_and_leaves_a_pending_switch_alone_when_one_is_bad(self) -> None:
        harness = Harness()
        harness.track("video-mid").ok()
        harness.switchable.switch_to("video-hi")

        async def async_until(_value: MoqtObject) -> bool:
            return True

        with pytest.raises(TypeError):
            harness.switchable.switch_to("video-lo", until=cast(Any, "never"))
        with pytest.raises(TypeError):
            harness.switchable.switch_to("video-lo", until=cast(Any, async_until))
        with pytest.raises(ValueError):
            harness.switchable.switch_to("video-lo", drain=-1)
        with pytest.raises(ValueError):
            harness.switchable.switch_to("video-lo", drain=2_147_483.648)
        with pytest.raises(TypeError):
            harness.switchable.switch_to("video-lo", drain=cast(Any, "5"))
        with pytest.raises(TypeError):
            harness.switchable.switch_to("video-lo", cancellation=cast(Any, object()))
        assert harness.switchable.pending == "video-hi"

        async def on_object(_value: MoqtObject, _track: str) -> None:
            return None

        with pytest.raises(TypeError):
            snapshot_switchable_handlers(SwitchableHandlers(on_object=cast(Any, on_object)))


Cleanup = list[Callable[[], Awaitable[None] | None]]


@pytest.fixture
async def cleanup() -> AsyncIterator[Cleanup]:
    steps: Cleanup = []
    yield steps
    for dispose in reversed(steps):
        result = dispose()
        if result is not None:
            await result


class TestSwitchableOverARelay:
    async def test_switches_layers_on_a_live_connection_with_no_gap_in_frames(self, cleanup: Cleanup) -> None:
        url = "https://relay.example.com/moq"
        namespace = ["room", "layers"]
        hub = create_relay_hub()
        cleanup.append(hub.destroy)
        publisher = await connect_relay_publisher(url, namespace=namespace, create_transport=hub.connect_publisher)
        subscriber = await connect_relay_subscriber(url, create_transport=hub.connect_subscriber)
        cleanup.append(subscriber.close)
        cleanup.append(publisher.close)
        writers = {track: publisher.register_track(track, write_timeout=2.0) for track in ("video-hi", "video-lo")}
        received: list[tuple[str, int]] = []
        switchable = subscriber.subscribe_switchable(
            namespace,
            "video-lo",
            SwitchableHandlers(on_object=lambda value, track: received.append((track, value.payload[0]))),
        )
        await switchable.subscribed
        await wait_for(lambda: writers["video-lo"].has_demand)

        # Every layer carries the same frame numbers, as a simulcast encoder's layers do.
        async def frame(n: int) -> None:
            await asyncio.gather(*(writer.write(bytes([n])) for writer in writers.values() if writer.has_demand))

        n = 0
        await frame(n)
        n += 1
        await wait_for(lambda: len(received) == 1)
        switched = switchable.switch_to("video-hi")
        await wait_for(lambda: writers["video-hi"].has_demand)
        while switchable.track != "video-hi":
            await frame(n)
            n += 1
            await asyncio.sleep(0.005)
        await switched
        await frame(n)
        n += 1
        await wait_for(lambda: received[-1] == ("video-hi", n - 1))
        assert {value for _, value in received} == set(range(n))
        assert [track for track, _ in received].index("video-hi") > 0
