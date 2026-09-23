"""The announce race (see ``CONTEXT.md``), and the application's side of it.

A subscribe that reaches the relay before it has processed the publisher's announce of the
namespace is rejected with request error 404, moq-relay's literal for a namespace it has not seen
announced (not MOQT's ``DOES_NOT_EXIST``); the relay parks nothing. The SDK reports that rejection
like any other and never retries a subscribe, so an application that may start before its publisher
retries within its own budget. This module is that policy, shared by the mux-global demo, the live
smoke test and the Video.js example's ``--verify`` viewer.

A process that also owns the publisher awaits ``publisher.announced`` first: that removes most of
the race, and the retry covers the round trip the relay still needs after the announce reached the
wire (measured in ``spec/relay-interop.md``).
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable, Sequence
from dataclasses import replace

from mux_relay import (
    MessageParameters,
    RelaySubscriber,
    RelaySubscription,
    RelaySubscriptionHandlers,
    RequestError,
    SubscribeOk,
    SubscriptionRequestError,
)

FRESH_NAMESPACE_ERROR_CODE = 404
"""moq-relay's request error for a namespace it has not seen announced (``Broadcast not found``)."""

INITIAL_DELAY = 0.5
"""Seconds before the second attempt; the delay doubles from here."""

MAX_DELAY = 2.0
"""The longest wait between attempts, in seconds."""


async def subscribe_with_retry(
    subscriber: RelaySubscriber,
    namespace: Sequence[str],
    track: str,
    handlers: RelaySubscriptionHandlers | None = None,
    *,
    parameters: MessageParameters | None = None,
    initial_delay: float = INITIAL_DELAY,
    max_delay: float = MAX_DELAY,
    on_retry: Callable[[int, float], None] | None = None,
) -> RelaySubscription:
    """Subscribe, retrying only the fresh-namespace 404 until the relay answers otherwise.

    Returns the first subscription whose ``subscribed`` is not that rejection: accepted, or rejected
    for another reason, which the caller reads from ``subscribed`` or ``handlers.on_error`` exactly
    as after a plain ``subscribe()``. The 404s this retries never reach ``handlers.on_error``; every
    other event reaches the handlers untouched. The delay starts at ``initial_delay`` seconds and
    doubles up to ``max_delay``; ``on_retry(attempt, delay)`` runs before each wait. Only
    cancellation bounds the retries: run it under ``asyncio.timeout`` or cancel the task, and the
    cancellation raises out of it, cancelling a request the relay has not answered yet so it cannot
    be accepted after the budget ran out.
    """
    delay = initial_delay
    attempt = 0

    while True:
        attempt += 1
        subscription = subscriber.subscribe(
            namespace, track, _muting_retried_rejections(handlers), parameters=parameters
        )

        try:
            await subscription.subscribed
        except asyncio.CancelledError:
            # The budget ran out while the relay had not answered: a request left pending could still
            # be accepted and deliver objects to handlers nobody expects to run.
            subscription.cancel()
            raise
        except SubscriptionRequestError as error:
            if error.request_error.error_code != FRESH_NAMESPACE_ERROR_CODE:
                return subscription
        except Exception:
            # The connection ended first, or the caller cancelled the handle: it carries the reason.
            return subscription
        else:
            return subscription

        if on_retry is not None:
            on_retry(attempt, delay)

        await asyncio.sleep(delay)
        delay = min(delay * 2, max_delay)


def _muting_retried_rejections(handlers: RelaySubscriptionHandlers | None) -> RelaySubscriptionHandlers | None:
    """``handlers`` whose ``on_error`` skips the fresh-namespace 404 until the relay has accepted the request.

    The retry reports the outcome through the returned handle, so the callback would otherwise see
    every attempt fail. A 404 after acceptance is a different event and passes through. The wrappers
    return what the caller's callbacks return, so the SDK's synchronous-callback guard still sees an
    awaitable-returning handler and applies its policy.
    """
    if handlers is None or handlers.on_error is None:
        return handlers

    on_error = handlers.on_error
    on_subscribed = handlers.on_subscribed

    # The SDK refuses a declared-async callback before it sends anything; a synchronous wrapper would
    # hide one from that check, so apply it here first.
    for name, callback in (("on_subscribed", on_subscribed), ("on_error", on_error)):
        if callback is not None and (
            inspect.iscoroutinefunction(callback) or inspect.iscoroutinefunction(callback.__call__)
        ):
            raise TypeError(f"{name} must be synchronous; use Relay subscribe_events() for async consumption")

    accepted = False

    def accept(info: SubscribeOk) -> None:
        nonlocal accepted
        accepted = True

        if on_subscribed is not None:
            return on_subscribed(info)

        return None

    def fail(error: RequestError) -> None:
        if error.error_code == FRESH_NAMESPACE_ERROR_CODE and not accepted:
            return None

        return on_error(error)

    return replace(handlers, on_subscribed=accept, on_error=fail)


__all__ = ["FRESH_NAMESPACE_ERROR_CODE", "INITIAL_DELAY", "MAX_DELAY", "subscribe_with_retry"]
