"""The latest-value reader: a subscription's or a want's handlers that deliver only the newest value of a latest-wins
track, such as telemetry or a control. See the "Latest-value reading" section of ``spec/behavior.md``.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass

from moq_core import MoqtObject, PublishDone, RequestError, SubscribeOk

from .connect import RelaySubscriptionHandlers
from .supervised_subscriber import RestartReason, WantHandlers, WantSwitchedInfo

_log = logging.getLogger("caprid_relay")


@dataclass(frozen=True, slots=True)
class LatestValueStats:
    """What a :class:`LatestValueReader` dropped and delivered, cumulative across restarts."""

    delivered: int = 0
    """Values passed to ``on_value``."""
    superseded: int = 0
    """Objects dropped because a newer one had already been judged."""
    late: int = 0
    """Newer values dropped because ``late`` returned true."""
    catch_ups: int = 0
    """Times the reader called ``on_catch_up``."""


class LatestValueReader:
    """Read a latest-wins track: drop each object whose location (group, then object) is not after every one judged
    since the start or the last restart, and each one the ``late`` predicate rejects.

    ``on_value`` receives each value newer than every one judged before it. ``late`` is the application's test that a
    newer value still arrived too late to act on, such as a capture time in the payload against the viewer's clock; a
    late value is dropped and still supersedes older ones. ``on_catch_up`` runs on the first late value since the start
    or since a delivered value, and not again until a value is delivered; a restart does not re-arm it. Its usual body
    is ``SupervisedSubscriber.catch_up(key)``, whose restart restarts the reader. An exception from any of the three
    is logged on the ``caprid_relay`` logger and the reader carries on; a ``late`` that raises counts as false.

    Pass :meth:`subscription_handlers` to ``RelaySubscriber.subscribe`` or :meth:`want_handlers` to
    ``SupervisedSubscriber.want``. A want read through a reader keeps ``drain`` at 0, the default: a drained old
    subscription's groups would arrive after the Move, numbered against the old one.
    """

    def __init__(
        self,
        on_value: Callable[[MoqtObject], None],
        *,
        late: Callable[[MoqtObject], bool] | None = None,
        on_catch_up: Callable[[], None] | None = None,
    ) -> None:
        if not callable(on_value):
            raise TypeError("on_value must be callable")
        if late is not None and not callable(late):
            raise TypeError("late must be callable")
        if on_catch_up is not None and not callable(on_catch_up):
            raise TypeError("on_catch_up must be callable")
        self._on_value = on_value
        self._late = late
        self._on_catch_up = on_catch_up
        self._newest: tuple[int, int] | None = None
        self._catch_up_signalled = False
        self._delivered = 0
        self._superseded = 0
        self._late_count = 0
        self._catch_ups = 0

    @property
    def stats(self) -> LatestValueStats:
        """A snapshot of the counters."""
        return LatestValueStats(self._delivered, self._superseded, self._late_count, self._catch_ups)

    # --- handlers

    def subscription_handlers(
        self,
        *,
        on_error: Callable[[RequestError], None] | None = None,
        on_publisher_done: Callable[[PublishDone], None] | None = None,
    ) -> RelaySubscriptionHandlers:
        """Handlers for one subscription, with the application's own error and completion callbacks."""
        return RelaySubscriptionHandlers(
            on_error=on_error,
            on_object=self._object,
            on_publisher_done=on_publisher_done,
        )

    def want_handlers(
        self,
        *,
        on_subscribed: Callable[[SubscribeOk, tuple[str, ...]], None] | None = None,
        on_publisher_done: Callable[[PublishDone], None] | None = None,
        on_restarted: Callable[[RestartReason, tuple[str, ...]], None] | None = None,
        on_switched: Callable[[WantSwitchedInfo], None] | None = None,
        on_error: Callable[[BaseException], None] | None = None,
    ) -> WantHandlers:
        """Handlers for a want. A Restart and a Move start the reader over, since the new subscription numbers its
        groups afresh; the application's callbacks run after."""

        def restarted(reason: RestartReason, namespace: tuple[str, ...]) -> None:
            self._restart()
            if on_restarted is not None:
                on_restarted(reason, namespace)

        def switched(info: WantSwitchedInfo) -> None:
            self._restart()
            if on_switched is not None:
                on_switched(info)

        return WantHandlers(
            on_subscribed=on_subscribed,
            on_object=self._object,
            on_publisher_done=on_publisher_done,
            on_restarted=restarted,
            on_switched=switched,
            on_error=on_error,
        )

    # --- judging

    def _restart(self) -> None:
        # A restart does not re-arm the catch-up: a replacement late from its first value is still behind the old
        # subscription's bytes on the viewer's link, and catching up again would only repeat the gap.
        self._newest = None

    def _object(self, obj: MoqtObject) -> None:
        # A status object (an end of group or track) carries no value.
        if obj.status != "normal":
            return
        location = (obj.group_id, obj.object_id)
        if self._newest is not None and location <= self._newest:
            self._superseded += 1
            return
        self._newest = location
        if self._is_late(obj):
            self._late_count += 1
            if self._catch_up_signalled:
                return
            self._catch_up_signalled = True
            self._catch_ups += 1
            if self._on_catch_up is not None:
                try:
                    self._on_catch_up()
                except Exception:
                    _log.exception("on_catch_up raised; the latest-value reader carries on")
            return
        self._catch_up_signalled = False
        self._delivered += 1
        try:
            self._on_value(obj)
        except Exception:
            _log.exception("on_value raised; the latest-value reader carries on")

    def _is_late(self, obj: MoqtObject) -> bool:
        if self._late is None:
            return False
        try:
            return self._late(obj) is True
        except Exception:
            _log.exception("late raised; the latest-value reader delivers the value")
            return False
