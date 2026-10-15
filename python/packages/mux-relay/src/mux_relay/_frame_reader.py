"""The frame reader: a subscription's or a want's handlers that read its track as frames in decode order.

It drives a sequencer it is given (``moq_media.FrameSequencer`` qualifies), which owns the ordering, the deadlines
and the LOC format, so this package stays free of the format package. See the "Media subscribing" section of
``spec/behavior.md``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from typing import Generic, Protocol, TypeVar

from moq_core import KeyValuePair, MoqtObject, PublishDone, RequestError, SubgroupEnd, SubgroupReset, SubscribeOk

from .connect import RelaySubscriptionHandlers
from .supervised_subscriber import RestartReason, WantHandlers, WantSwitchedInfo

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

    def poll(self, max_frames: int | None = None) -> FrameReaderPoll[F_co]: ...


class FrameReader(Generic[F]):
    """Read a track as frames: feed the sequencer every event, and poll it as the application reads, or when a wait it
    reports ends. Iterate it for the frames, in decode order; :meth:`close` ends the iteration.

    Pass :meth:`subscription_handlers` to ``RelaySubscriber.subscribe`` or :meth:`want_handlers` to
    ``SupervisedSubscriber.want``. Each group is one subgroup, as the frame writer sends it, so a subgroup's end is its
    group's. A want read through a reader keeps ``drain`` at 0, the default: a drained old subscription's groups would
    arrive after the Move, indistinguishable from the new one's. Frames wait in the sequencer until they are read, and
    lateness is judged then, so a slow reader gets fresh frames rather than a backlog.
    """

    def __init__(self, sequencer: FrameReaderSequencer[F]) -> None:
        self._sequencer = sequencer
        self._changed: asyncio.Event | None = None
        self._closed = False

    @property
    def closed(self) -> bool:
        """Whether :meth:`close` was called."""
        return self._closed

    def close(self) -> None:
        """Stop delivering and drop frames not yet read. Later events are ignored."""
        self._closed = True
        self._wake()

    def __aiter__(self) -> FrameReader[F]:
        return self

    async def __anext__(self) -> F:
        while not self._closed:
            # One frame per read: whatever waits stays in the sequencer, judged late when it is taken.
            result = self._sequencer.poll(1)
            if result.frames:
                return result.frames[0]
            changed = self._changed = self._changed or asyncio.Event()
            changed.clear()
            timeout = None if result.wait_us is None else result.wait_us / 1_000_000
            try:
                await asyncio.wait_for(changed.wait(), timeout)
            except TimeoutError:
                pass
        raise StopAsyncIteration

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
        self._wake()

    def _wake(self) -> None:
        if self._changed is not None:
            self._changed.set()
