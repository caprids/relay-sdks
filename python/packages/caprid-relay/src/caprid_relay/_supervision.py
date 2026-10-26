"""What the supervised publisher and subscriber share: their status names, the backoff between
attempts, the generation field a supervised publisher appends to its root namespace, the request
error classifiers, and the waits both build on. The supervisors are policy the application opts into
(``spec/behavior.md``, "Supervised connections"); everything here is built on the public primitives'
facts. The counterpart of ``ts/src/supervision.ts``.
"""

from __future__ import annotations

import asyncio
import math
import random
import secrets
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Protocol, TypeAlias

from moq_core import RequestError

from .cancellation import CancellationToken
from .errors import RelayError

SupervisedStatus: TypeAlias = Literal["connecting", "live", "reconnecting", "closed"]
"""Where a supervised connection stands: ``connecting`` until the first connection is up, ``live``
while one is, ``reconnecting`` between connections, and ``closed`` once the application closed it,
its cancellation token was cancelled, or a first attempt failed in a way that is not retried."""


@dataclass(frozen=True, slots=True, kw_only=True)
class SupervisionBackoff:
    """The delay before each retry: it starts at ``initial_delay`` and doubles up to ``max_delay``, with jitter.

    Both are seconds; ``None`` takes the default for what is being retried.
    """

    initial_delay: float | None = None
    """The delay before the first retry, in seconds."""
    max_delay: float | None = None
    """The longest delay between retries, in seconds."""


@dataclass(frozen=True, slots=True)
class _Backoff:
    """A checked backoff with its defaults filled in."""

    initial_delay: float
    max_delay: float


RECONNECT_BACKOFF = _Backoff(0.5, 10.0)
"""Reconnection: from half a second, doubling up to ten."""
RESUBSCRIBE_BACKOFF = _Backoff(0.25, 8.0)
"""Resubscription: from a quarter second, doubling up to eight."""

_MAX_DELAY = 2_147_483.647
"""The longest delay accepted, in seconds: the TypeScript SDK's timer limit."""

CLOSURE_GRACE = 0.25
"""How long a failed operation waits for its connection to report an end that may explain it, in seconds."""


def backoff_options(name: str, given: SupervisionBackoff | None, defaults: _Backoff) -> _Backoff:
    """Check a backoff's bounds and fill in the defaults."""
    if given is not None and not isinstance(given, SupervisionBackoff):
        raise TypeError(f"{name} must be a SupervisionBackoff")
    initial = given.initial_delay if given is not None and given.initial_delay is not None else defaults.initial_delay
    _check_delay(f"{name}.initial_delay", initial)
    maximum = given.max_delay if given is not None and given.max_delay is not None else max(defaults.max_delay, initial)
    _check_delay(f"{name}.max_delay", maximum)
    if maximum < initial:
        raise ValueError(f"{name}.max_delay must not be less than initial_delay")
    return _Backoff(float(initial), float(maximum))


def _check_delay(name: str, value: object) -> None:
    if not isinstance(value, int | float) or isinstance(value, bool):
        raise TypeError(f"{name} must be a number of seconds")
    if not (math.isfinite(value) and 0 <= value <= _MAX_DELAY):
        raise ValueError(f"{name} must be between 0 and {_MAX_DELAY} seconds")


ROTATE_BEFORE = 60.0
"""How long before the credential's expiry a supervisor replaces its connection by default, in seconds."""
ROTATION_OVERLAP = 10.0
"""The longest a supervisor keeps the replaced connection open while its tracks or wants move over, in seconds."""


def rotate_before_option(value: object) -> float:
    """Check ``rotate_before``: a number of seconds a timer can hold, ``0`` included."""
    if value is None:
        return ROTATE_BEFORE
    _check_delay("rotate_before", value)
    assert isinstance(value, int | float)
    return float(value)


def rotation_delay(expires_at: datetime | None, rotate_before: float, now: float | None = None) -> float | None:
    """Seconds until a connection whose credential expires at ``expires_at`` is replaced.

    ``rotate_before`` before it, but no earlier than halfway through what is left, so a token
    shorter than the margin still gets a connection's worth of use. ``None`` when the expiry is unknown.
    """
    if expires_at is None:
        return None
    left = expires_at.timestamp() - (time.time() if now is None else now)
    return max(0.0, left - rotate_before, left / 2)


def rotation_deadline(expires_at: datetime | None, rotate_before: float) -> float | None:
    """When, on the event loop's clock, a connection whose credential expires at ``expires_at`` is replaced.

    See :func:`rotation_delay`. Computed once, when it connects, so time spent in a rotation's
    overlap does not push it back. ``None`` for an unknown expiry.
    """
    delay = rotation_delay(expires_at, rotate_before)
    return None if delay is None else asyncio.get_running_loop().time() + delay


async def rotation_due(deadline: float | None, manual: asyncio.Future[None]) -> None:
    """Return once ``deadline`` (from :func:`rotation_deadline`) passed, or ``manual`` resolved.

    With no deadline only ``manual`` ends the wait. Cancel the task to stop waiting.
    """
    limit = None if deadline is None else max(0.0, deadline - asyncio.get_running_loop().time())
    await wait_any(settles=(manual,), limit=limit)


def backoff_delay(attempt: int, backoff: _Backoff) -> float:
    """The delay before retry ``attempt`` (0 first): the doubled delay, capped, then drawn from its
    upper half, so that many clients that lost the same relay do not come back in step."""
    ceiling = min(backoff.max_delay, backoff.initial_delay * 2 ** min(attempt, 30))
    return ceiling / 2 + random.random() * ceiling / 2


class _Settles(Protocol):
    """A future or a ``RelayAwaitable``: something whose settling can be observed."""

    def done(self) -> bool: ...

    def add_done_callback(self, callback: Callable[..., object], /) -> None: ...

    def remove_done_callback(self, callback: Callable[..., object], /) -> int: ...


async def wait_any(
    tokens: Iterable[CancellationToken] = (),
    *,
    settles: Sequence[_Settles] = (),
    limit: float | None = None,
) -> None:
    """Return once any token is cancelled, any of ``settles`` is done, or ``limit`` seconds passed.

    Never raises for any of those, and leaves no callback behind. Task cancellation still raises.
    """
    tokens = list(tokens)
    if any(token.cancelled for token in tokens) or any(item.done() for item in settles):
        return
    loop = asyncio.get_running_loop()
    waiter: asyncio.Future[None] = loop.create_future()

    def wake(*_: object) -> None:
        if not waiter.done():
            waiter.set_result(None)

    for token in tokens:
        token.add_callback(wake)
    for item in settles:
        item.add_done_callback(wake)
    timer = loop.call_later(limit, wake) if limit is not None else None
    try:
        await waiter
    finally:
        for token in tokens:
            token.remove_callback(wake)
        for item in settles:
            item.remove_done_callback(wake)
        if timer is not None:
            timer.cancel()


def on_cancel(tokens: Iterable[CancellationToken], callback: Callable[[], None]) -> Callable[[], None]:
    """Run ``callback`` once, in the turn the first of ``tokens`` is cancelled. Returns the removal.

    Runs at once when one is already cancelled.
    """
    tokens = list(tokens)
    ran = False

    def fire(_: object = None) -> None:
        nonlocal ran
        if ran:
            return
        ran = True
        remove()
        callback()

    def remove() -> None:
        for token in tokens:
            token.remove_callback(fire)

    if any(token.cancelled for token in tokens):
        fire()
        return lambda: None
    for token in tokens:
        token.add_callback(fire)
    return remove


_GENERATION_TIME_DIGITS = 9
"""Digits of base-36 milliseconds: enough until the year 5188."""
_BASE36 = "0123456789abcdefghijklmnopqrstuvwxyz"
_last_generation_ms = 0
"""The start time of this process's latest generation, in milliseconds."""


def new_generation(now_ms: int | None = None) -> str:
    """A fresh generation field: the start time in fixed-width base 36, then four random characters.

    Generations sort by start time as strings, which is how a subscriber following a root picks the
    newest among those announced. The same field the TypeScript SDK chooses. Without ``now_ms``, a
    second generation started in the same millisecond takes the next one, so this process's
    generations are strictly ordered however fast it reconnects.
    """
    global _last_generation_ms
    if now_ms is None:
        now_ms = max(int(time.time() * 1000), _last_generation_ms + 1)
        _last_generation_ms = now_ms
    value = now_ms
    digits = ""
    while value:
        value, digit = divmod(value, 36)
        digits = _BASE36[digit] + digits
    stamp = digits.rjust(_GENERATION_TIME_DIGITS, "0")
    return stamp + "".join(_BASE36[byte % 36] for byte in secrets.token_bytes(4))


_FRESH_NAMESPACE_ERROR_CODE = 0x10
"""MOQT's DOES_NOT_EXIST: moq-relay 0.15.8 and later's code for a namespace it has not seen announced."""
_FRESH_NAMESPACE_REASON = "unroutable"
"""The reason that singles the fresh namespace out of DOES_NOT_EXIST; ``not found`` is an unknown track instead."""
_LEGACY_FRESH_NAMESPACE_ERROR_CODE = 404
"""moq-relay's literal through 0.15.1 for a namespace it has not seen announced."""


def is_fresh_namespace_rejection(request_error: RequestError) -> bool:
    """Whether the relay refused a namespace it has not seen announced (yet): the announce race."""
    if request_error.error_code == _LEGACY_FRESH_NAMESPACE_ERROR_CODE:
        return True
    return request_error.error_code == _FRESH_NAMESPACE_ERROR_CODE and request_error.reason == _FRESH_NAMESPACE_REASON


_PERMANENT_REQUEST_ERRORS = frozenset({0x1, 0x3, 0x4, 0x5, 0x11, 0x12})
"""REQUEST_ERROR codes no wait fixes: credentials (0x1, 0x4, 0x5), a request the relay does not
support (0x3), and a request it cannot serve as made (0x11 INVALID_RANGE, 0x12 MALFORMED_TRACK). A
supervised subscription surfaces these and stops; it retries every other code with backoff."""


def is_permanent_request_error(request_error: RequestError) -> bool:
    """Whether a REQUEST_ERROR is one a supervised subscription surfaces instead of retrying."""
    return request_error.error_code in _PERMANENT_REQUEST_ERRORS


def is_setup_failure(error: BaseException) -> bool:
    """Whether a failed connection attempt is one a supervisor does not retry before its first connection.

    An argument error, or the relay refusing the credentials. Once a connection has been up, a
    refusal is retried, since the token provider may hand out a fresh token on the next attempt.
    """
    if isinstance(error, TypeError | ValueError):
        return True
    return isinstance(error, RelayError) and error.code == "UNAUTHORIZED"


def supervisor_closed(name: str) -> RelayError:
    """The failure for an operation on a supervisor that stopped, and the reason it stopped with by default."""
    return RelayError(f"Supervised {name} is closed", code="CLOSED", phase="connection")


__all__ = [
    "CLOSURE_GRACE",
    "RECONNECT_BACKOFF",
    "RESUBSCRIBE_BACKOFF",
    "ROTATE_BEFORE",
    "ROTATION_OVERLAP",
    "SupervisedStatus",
    "SupervisionBackoff",
    "backoff_delay",
    "backoff_options",
    "is_fresh_namespace_rejection",
    "is_permanent_request_error",
    "is_setup_failure",
    "new_generation",
    "on_cancel",
    "rotate_before_option",
    "rotation_deadline",
    "rotation_delay",
    "rotation_due",
    "supervisor_closed",
    "wait_any",
]
