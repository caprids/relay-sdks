"""A cancellation primitive that governs a connection's whole lifetime.

The behavior contract asks for "an abort signal or a cancellation token". asyncio has neither built
in, so :class:`CancellationToken` plays the role of ``AbortSignal`` + ``AbortController`` in one
object: the owner calls :meth:`cancel`, and every connection given the token closes with the reason.
"""

from __future__ import annotations

from collections.abc import Callable


class Cancelled(Exception):
    """The default reason recorded when a token is cancelled without one."""


class CancellationToken:
    """Cancel every connection that was handed this token, whether still connecting or established."""

    __slots__ = ("_callbacks", "_cancelled", "_reason")

    def __init__(self) -> None:
        self._cancelled = False
        self._reason: BaseException | None = None
        self._callbacks: list[Callable[[BaseException], None]] = []

    @property
    def cancelled(self) -> bool:
        """Whether :meth:`cancel` has been called."""
        return self._cancelled

    @property
    def reason(self) -> BaseException | None:
        """The exception recorded by :meth:`cancel`; ``None`` until then."""
        return self._reason

    def cancel(self, reason: BaseException | None = None) -> None:
        """Cancel once. Later calls are ignored. Registered callbacks run synchronously, in order."""
        if self._cancelled:
            return

        self._cancelled = True
        self._reason = reason if reason is not None else Cancelled("cancelled")
        callbacks, self._callbacks = self._callbacks, []

        for callback in callbacks:
            callback(self._reason)

    def throw_if_cancelled(self) -> None:
        """Raise the recorded :attr:`reason` if the token has been cancelled; return otherwise."""
        if self._cancelled:
            assert self._reason is not None
            raise self._reason

    def add_callback(self, callback: Callable[[BaseException], None]) -> None:
        """Run ``callback`` with the reason when the token is cancelled. No-op once already cancelled."""
        if not self._cancelled:
            self._callbacks.append(callback)

    def remove_callback(self, callback: Callable[[BaseException], None]) -> None:
        """Unregister a callback added with :meth:`add_callback`. No-op if it is not registered."""
        try:
            self._callbacks.remove(callback)
        except ValueError:
            pass


__all__ = ["CancellationToken", "Cancelled"]
