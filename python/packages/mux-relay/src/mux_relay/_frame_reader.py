"""The frame reader: a subscription's or a want's handlers that read its track as frames in decode order.

It drives a sequencer it is given (``moq_media.FrameSequencer`` qualifies), which owns the ordering, the deadlines
and the LOC format, so this package stays free of the format package. See the "Media subscribing" section of
``spec/behavior.md``.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Callable, Sequence
from typing import Generic, Protocol, TypeVar

from moq_core import KeyValuePair, MoqtObject, PublishDone, RequestError, SubgroupEnd, SubgroupReset, SubscribeOk

from .connect import RelaySubscriptionHandlers
from .supervised_subscriber import RestartReason, WantHandlers, WantSwitchedInfo

_log = logging.getLogger("mux_relay.media")

F = TypeVar("F")
F_co = TypeVar("F_co", covariant=True)


class FrameReaderPoll(Protocol[F_co]):
    """What a sequencer's ``poll()`` returns: the frames ready now, and when to poll again in microseconds."""

    @property
    def frames(self) -> Sequence[F_co]: ...

    @property
    def wait_us(self) -> int | None: ...


class FrameReaderSequencer(Protocol[F_co]):
    """What a :class:`FrameReader` drives: ``moq_media.FrameSequencer`` qualifies."""

    track_properties: Sequence[KeyValuePair]

    def object(self, obj: MoqtObject) -> None: ...

    def group_ended(self, group_id: int) -> None: ...

    def group_reset(self, group_id: int) -> None: ...

    def restart(self) -> None: ...

    def poll(self) -> FrameReaderPoll[F_co]: ...


class FrameReader(Generic[F]):
    """Read a track as frames: feed the sequencer every event, poll it after each, and again when a wait it reports
    ends. Iterate it for the frames, in decode order; :meth:`close` ends the iteration.

    Pass :meth:`subscription_handlers` to ``RelaySubscriber.subscribe`` or :meth:`want_handlers` to
    ``SupervisedSubscriber.want``. Each group is one subgroup, as the frame writer sends it, so a subgroup's end is its
    group's. The handlers must run on the event loop the reader is iterated on.
    """

    def __init__(self, sequencer: FrameReaderSequencer[F]) -> None:
        self._sequencer = sequencer
        self._ready: deque[F] = deque()
        self._waiter: asyncio.Future[None] | None = None
        self._timer: asyncio.TimerHandle | None = None
        self._closed = False

    @property
    def closed(self) -> bool:
        """Whether :meth:`close` was called."""
        return self._closed

    def close(self) -> None:
        """Stop delivering, drop frames not yet read, and cancel the pending poll. Later events are ignored."""
        self._closed = True
        self._ready.clear()
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        self._wake()

    def __aiter__(self) -> FrameReader[F]:
        return self

    async def __anext__(self) -> F:
        while not self._ready:
            if self._closed:
                raise StopAsyncIteration
            self._waiter = asyncio.get_running_loop().create_future()
            try:
                await self._waiter
            finally:
                self._waiter = None
        return self._ready.popleft()

    # --- handlers

    def subscription_handlers(
        self,
        *,
        on_error: Callable[[RequestError], None] | None = None,
        on_publisher_done: Callable[[PublishDone], None] | None = None,
    ) -> RelaySubscriptionHandlers:
        """Handlers for one subscription, with the application's own error and completion callbacks."""
        return RelaySubscriptionHandlers(
            on_subscribed=self._subscribed,
            on_error=on_error,
            on_object=self._object,
            on_subgroup_end=self._subgroup_end,
            on_subgroup_reset=self._subgroup_reset,
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
        """Handlers for a want. A Restart and a Move start the sequencer over; the application's callbacks run after."""

        def subscribed(info: SubscribeOk, namespace: tuple[str, ...]) -> None:
            self._subscribed(info)
            if on_subscribed is not None:
                on_subscribed(info, namespace)

        def restarted(reason: RestartReason, namespace: tuple[str, ...]) -> None:
            self._event(self._sequencer.restart)
            if on_restarted is not None:
                on_restarted(reason, namespace)

        def switched(info: WantSwitchedInfo) -> None:
            def move() -> None:
                self._sequencer.restart()
                self._sequencer.track_properties = info.subscribed.track_properties

            self._event(move)
            if on_switched is not None:
                on_switched(info)

        return WantHandlers(
            on_subscribed=subscribed,
            on_object=self._object,
            on_subgroup_end=self._subgroup_end,
            on_subgroup_reset=self._subgroup_reset,
            on_publisher_done=on_publisher_done,
            on_restarted=restarted,
            on_switched=switched,
            on_error=on_error,
        )

    def _subscribed(self, info: SubscribeOk) -> None:
        if not self._closed:
            self._sequencer.track_properties = info.track_properties

    def _object(self, obj: MoqtObject) -> None:
        self._event(lambda: self._sequencer.object(obj))

    def _subgroup_end(self, end: SubgroupEnd) -> None:
        self._event(lambda: self._sequencer.group_ended(end.group_id))

    def _subgroup_reset(self, reset: SubgroupReset) -> None:
        self._event(lambda: self._sequencer.group_reset(reset.group_id))

    # --- delivery

    def _event(self, apply: Callable[[], None]) -> None:
        if self._closed:
            return
        apply()
        self._drain()

    def _drain(self) -> None:
        if self._closed:
            return
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        result = self._sequencer.poll()
        if result.frames:
            self._ready.extend(result.frames)
            self._wake()
        if result.wait_us is not None:
            self._timer = asyncio.get_running_loop().call_later(result.wait_us / 1_000_000, self._drain)

    def _wake(self) -> None:
        waiter = self._waiter
        if waiter is not None and not waiter.done():
            waiter.set_result(None)
