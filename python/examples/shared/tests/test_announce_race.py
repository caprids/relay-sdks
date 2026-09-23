"""``subscribe_with_retry`` over a scripted subscriber: what it retries, what it returns, what the handlers see."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from typing import cast

import pytest
from shared.announce_race import subscribe_with_retry

from mux_relay import (
    MessageParameters,
    RelayAwaitable,
    RelaySubscriber,
    RelaySubscriptionHandlers,
    RequestError,
    SubscribeOk,
    SubscriptionConnectionClosedError,
    SubscriptionRequestError,
)

NAMESPACE = ["example", "race"]


class FakeSubscription:
    """One attempt's handle: the handlers it was given and a ``subscribed`` the script settles."""

    def __init__(self, handlers: RelaySubscriptionHandlers | None) -> None:
        self.handlers = handlers
        self._subscribed: asyncio.Future[SubscribeOk] = asyncio.get_running_loop().create_future()
        self.subscribed: RelayAwaitable[SubscribeOk] = RelayAwaitable(self._subscribed)
        self.cancelled: list[object] = []

    def accept(self) -> None:
        info = SubscribeOk(track_alias=7)

        if self.handlers is not None and self.handlers.on_subscribed is not None:
            self.handlers.on_subscribed(info)

        self._subscribed.set_result(info)

    def reject(self, code: int, reason: str) -> None:
        """The relay's REQUEST_ERROR: the callback and the awaitable both see it, as in the SDK."""
        error = RequestError(error_code=code, retry_interval=0, reason=reason)

        if self.handlers is not None and self.handlers.on_error is not None:
            self.handlers.on_error(error)

        if not self._subscribed.done():
            self._subscribed.set_exception(SubscriptionRequestError(error))

    def fail(self, error: BaseException) -> None:
        self._subscribed.set_exception(error)

    def cancel(self, reason: object = None) -> None:
        self.cancelled.append(reason)


Answer = Callable[[FakeSubscription], None]


class FakeSubscriber:
    """Answers each ``subscribe()`` with the next scripted answer on the following loop turn."""

    def __init__(self, *answers: Answer) -> None:
        self.answers = list(answers)
        self.attempts: list[tuple[list[str], str, MessageParameters | None]] = []
        self.subscriptions: list[FakeSubscription] = []

    def subscribe(
        self,
        namespace: Sequence[str],
        track: str,
        handlers: RelaySubscriptionHandlers | None = None,
        *,
        parameters: MessageParameters | None = None,
    ) -> FakeSubscription:
        subscription = FakeSubscription(handlers)
        self.attempts.append((list(namespace), track, parameters))
        self.subscriptions.append(subscription)

        if self.answers:
            asyncio.get_running_loop().call_soon(self.answers.pop(0), subscription)

        return subscription


def not_found(subscription: FakeSubscription) -> None:
    subscription.reject(404, "Broadcast not found")


def accepted(subscription: FakeSubscription) -> None:
    subscription.accept()


async def retry(
    subscriber: FakeSubscriber,
    handlers: RelaySubscriptionHandlers | None = None,
    *,
    parameters: MessageParameters | None = None,
    initial_delay: float = 0.001,
    max_delay: float = 0.004,
    on_retry: Callable[[int, float], None] | None = None,
) -> FakeSubscription:
    """The helper over the fake, with delays short enough for a test."""
    subscription = await subscribe_with_retry(
        cast(RelaySubscriber, subscriber),
        NAMESPACE,
        "ticker",
        handlers,
        parameters=parameters,
        initial_delay=initial_delay,
        max_delay=max_delay,
        on_retry=on_retry,
    )
    return cast(FakeSubscription, subscription)


async def test_returns_the_accepted_subscription_after_retrying_a_fresh_namespace_404() -> None:
    subscriber = FakeSubscriber(not_found, accepted)
    seen: list[RequestError] = []
    retries: list[tuple[int, float]] = []
    parameters = MessageParameters()

    subscription = await retry(
        subscriber,
        RelaySubscriptionHandlers(on_error=seen.append),
        parameters=parameters,
        on_retry=lambda attempt, delay: retries.append((attempt, delay)),
    )

    assert subscription is subscriber.subscriptions[1]
    assert subscription.subscribed.result().track_alias == 7
    assert subscriber.attempts == [(NAMESPACE, "ticker", parameters)] * 2
    assert retries == [(1, 0.001)]
    assert seen == []


async def test_returns_a_subscription_rejected_for_another_reason_without_retrying() -> None:
    subscriber = FakeSubscriber(lambda subscription: subscription.reject(500, "internal error"))
    seen: list[RequestError] = []
    retries: list[tuple[int, float]] = []

    subscription = await retry(
        subscriber,
        RelaySubscriptionHandlers(on_error=seen.append),
        on_retry=lambda attempt, delay: retries.append((attempt, delay)),
    )

    assert subscription is subscriber.subscriptions[0]
    assert [error.error_code for error in seen] == [500]
    assert retries == []

    with pytest.raises(SubscriptionRequestError) as failure:
        await subscription.subscribed

    assert failure.value.request_error.error_code == 500


async def test_doubles_the_delay_up_to_the_cap() -> None:
    subscriber = FakeSubscriber(not_found, not_found, not_found, not_found, accepted)
    retries: list[tuple[int, float]] = []

    await retry(subscriber, on_retry=lambda attempt, delay: retries.append((attempt, delay)))

    assert retries == [(1, 0.001), (2, 0.002), (3, 0.004), (4, 0.004)]
    assert len(subscriber.attempts) == 5


async def test_cancellation_while_waiting_raises_and_subscribes_no_further() -> None:
    subscriber = FakeSubscriber(not_found)

    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.02):
            await retry(subscriber, initial_delay=10, max_delay=10)

    await asyncio.sleep(0.01)
    assert len(subscriber.attempts) == 1


async def test_cancellation_while_awaiting_the_answer_raises_and_cancels_the_pending_request() -> None:
    subscriber = FakeSubscriber()  # never answers

    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.02):
            await retry(subscriber)

    assert len(subscriber.attempts) == 1
    assert subscriber.subscriptions[0].cancelled == [None]


async def test_returns_the_handle_when_the_connection_ends_first() -> None:
    closed = SubscriptionConnectionClosedError(None)
    subscriber = FakeSubscriber(lambda subscription: subscription.fail(closed))

    subscription = await retry(subscriber)

    assert subscription is subscriber.subscriptions[0]

    with pytest.raises(SubscriptionConnectionClosedError):
        await subscription.subscribed


async def test_a_404_after_acceptance_reaches_on_error() -> None:
    subscriber = FakeSubscriber(accepted)
    seen: list[RequestError] = []

    subscription = await retry(subscriber, RelaySubscriptionHandlers(on_error=seen.append))
    # A rejection after acceptance is a different event (a retract, for instance), not the race.
    subscription.reject(404, "dropped")

    assert [error.error_code for error in seen] == [404]


async def test_returns_what_the_wrapped_callbacks_return_so_the_sdk_guard_still_sees_an_awaitable() -> None:
    subscriber = FakeSubscriber(accepted)
    marker = object()
    handlers = RelaySubscriptionHandlers(
        on_subscribed=cast(Callable[[SubscribeOk], None], lambda info: marker),
        on_error=cast(Callable[[RequestError], None], lambda error: marker),
    )

    subscription = await retry(subscriber, handlers)

    given = subscription.handlers
    assert given is not None and given.on_subscribed is not None and given.on_error is not None
    rejection = RequestError(error_code=500, retry_interval=0, reason="internal error")
    assert cast(object, given.on_subscribed(SubscribeOk(track_alias=7))) is marker
    assert cast(object, given.on_error(rejection)) is marker


async def test_forwards_the_other_handlers_and_leaves_handlers_without_on_error_alone() -> None:
    subscriber = FakeSubscriber(not_found, accepted)
    infos: list[SubscribeOk] = []
    objects: list[object] = []
    on_object = objects.append
    handlers = RelaySubscriptionHandlers(on_subscribed=infos.append, on_object=on_object, on_error=lambda error: None)

    subscription = await retry(subscriber, handlers)

    assert [info.track_alias for info in infos] == [7]
    assert subscription.handlers is not None
    assert subscription.handlers.on_object is on_object

    plain = RelaySubscriptionHandlers(on_object=on_object)
    subscriber = FakeSubscriber(accepted)

    subscription = await retry(subscriber, plain)

    assert subscription.handlers is plain
