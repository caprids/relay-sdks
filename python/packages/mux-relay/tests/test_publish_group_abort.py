"""Cancelling a group whose FIN is stuck behind transport backpressure.

The transport settles a write or a FIN only once its bytes have been handed to the wire, so
``group.close()`` can be parked for as long as the link is blocked. Everything that cancels the
operation in that window -- a re-bind, ``track.close()``, the publisher closing, a cancellation
token -- has to reset the group's stream rather than let the queued FIN deliver it complete.
"""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence

import pytest
from core_helpers.waiting import settle, wait_for
from relay_helpers.relay_hub import create_relay_hub

from moq_core import MoqtObject, SubscriptionHandlers
from moq_core.transport import BidirectionalStream, MoqtTransport, ReceiveStream, SendStream
from mux_relay import (
    ConnectingTransport,
    RelayClosedError,
    RelayConnection,
    RelayTimeoutError,
    connect_relay,
    connect_relay_publisher,
)

NAMESPACE = ["testing", "video"]
URL = "https://relay.example.com/moq"

Cleanup = list[Callable[[], Awaitable[None] | None]]


@pytest.fixture
async def cleanup() -> AsyncIterator[Cleanup]:
    steps: Cleanup = []
    yield steps

    for dispose in reversed(steps):
        result = dispose()

        if result is not None:
            await result


def receive(subscriber: RelayConnection, track: str) -> list[MoqtObject]:
    objects: list[MoqtObject] = []
    subscriber.subscribe(NAMESPACE, track, SubscriptionHandlers(on_object=objects.append))
    return objects


def _retrieve(task: asyncio.Task[None]) -> None:
    """Read a background FIN's outcome so an abandoned one is not reported as unretrieved."""
    if not task.cancelled():
        task.exception()


class _CongestedSendStream:
    """A send half whose FIN stays queued until the link moves, the way a full window holds it.

    Models ``_SendHalf`` in ``mux_relay.webtransport``: a close whose bytes have not been transmitted
    is still abortable, and the abort fails the parked FIN with the caller's reason.
    """

    def __init__(self, inner: SendStream) -> None:
        self._inner = inner
        self._transmitted: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._fin: asyncio.Task[None] | None = None
        self.finned = False
        self.hold_writes = False
        self.reset_with: object = None

    async def write(self, data: bytes) -> None:
        if self.hold_writes:
            await asyncio.shield(self._transmitted)
        await self._inner.write(data)

    async def _deliver_fin(self) -> None:
        await self._transmitted
        await self._inner.close()
        self.finned = True

    async def close(self) -> None:
        if self._fin is None:
            self._fin = asyncio.ensure_future(self._deliver_fin())
            self._fin.add_done_callback(_retrieve)

        # Shielded like ``_SendHalf._send``: a caller that gives up does not withdraw the queued FIN.
        await asyncio.shield(self._fin)

    def release(self) -> None:
        """The link moves again: a FIN still queued is transmitted."""
        if not self._transmitted.done():
            self._transmitted.set_result(None)

    def abort(self, reason: object = None) -> None:
        self.reset_with = reason

        if not self._transmitted.done():
            self._transmitted.set_exception(
                reason if isinstance(reason, BaseException) else RelayClosedError("Stream was reset")
            )

        self._inner.abort(reason)


class _CongestedTransport:
    """The hub transport with every unidirectional stream's FIN held behind a congested link."""

    def __init__(self, inner: MoqtTransport) -> None:
        self._inner = inner
        self.streams: list[_CongestedSendStream] = []

    def incoming_unidirectional_streams(self) -> AsyncIterator[ReceiveStream]:
        return self._inner.incoming_unidirectional_streams()

    def incoming_bidirectional_streams(self) -> AsyncIterator[BidirectionalStream]:
        return self._inner.incoming_bidirectional_streams()

    async def create_unidirectional_stream(self) -> SendStream:
        stream = _CongestedSendStream(await self._inner.create_unidirectional_stream())
        self.streams.append(stream)
        return stream

    async def create_bidirectional_stream(self) -> BidirectionalStream:
        return await self._inner.create_bidirectional_stream()

    def close(self, close_code: int = 0, reason: str = "") -> None:
        self._inner.close(close_code, reason)

    async def wait_closed(self) -> None:
        await self._inner.wait_closed()


class TestRecoveringSingleWrites:
    @pytest.mark.parametrize("stage", ["write", "fin"])
    @pytest.mark.parametrize("interrupt", ["timeout", "cancel"])
    async def test_resets_the_stream_and_keeps_the_track_for_fresh_data(
        self, cleanup: Cleanup, stage: str, interrupt: str
    ) -> None:
        hub = create_relay_hub()
        cleanup.append(hub.destroy)
        transports: list[_CongestedTransport] = []

        class HoldDataTransport(_CongestedTransport):
            async def create_unidirectional_stream(self) -> SendStream:
                stream = await super().create_unidirectional_stream()
                if len(self.streams) == 2 and stage == "write":
                    self.streams[-1].hold_writes = True
                elif len(self.streams) > 2:
                    self.streams[-1].release()
                return stream

        def connect_publisher(url: str = "", protocols: Sequence[str] = ()) -> ConnectingTransport:
            created = hub.connect_publisher(url, protocols)
            transport = HoldDataTransport(created.transport)
            transports.append(transport)
            return ConnectingTransport(transport=transport, ready=created.ready)

        publisher = await connect_relay_publisher(URL, namespace=NAMESPACE, create_transport=connect_publisher)
        subscriber = await connect_relay(URL, create_transport=hub.connect_subscriber)
        cleanup.extend([subscriber.close, publisher.close])
        audio = publisher.publish("audio", write_timeout=0.1)
        objects = receive(subscriber, "audio")
        writing = asyncio.create_task(audio.write(b"stale"))
        await wait_for(lambda: len(transports[0].streams) == 2)
        stream = transports[0].streams[-1]
        if stage == "fin":
            await wait_for(lambda: stream._fin is not None)
        if interrupt == "cancel":
            writing.cancel()
        with pytest.raises(asyncio.CancelledError if interrupt == "cancel" else RelayTimeoutError) as failure:
            await writing
        assert stream.reset_with is failure.value
        assert not audio.closed
        assert audio.bound
        stream.release()
        await settle()
        assert not stream.finned
        await audio.write(b"fresh")
        await wait_for(lambda: any(obj.payload == b"fresh" for obj in objects))
        assert [(obj.group_id, obj.payload) for obj in objects] == (
            [(0, b"stale"), (1, b"fresh")] if stage == "fin" else [(1, b"fresh")]
        )
        assert not hub.track_ends


class TestCancellingAStuckGroup:
    @pytest.mark.parametrize("operation", ["write", "close"])
    async def test_each_group_operation_has_a_deadline_and_releases_the_track(
        self, cleanup: Cleanup, operation: str
    ) -> None:
        hub = create_relay_hub()
        cleanup.append(hub.destroy)
        transports: list[_CongestedTransport] = []

        def connect_publisher(url: str = "", protocols: Sequence[str] = ()) -> ConnectingTransport:
            created = hub.connect_publisher(url, protocols)
            transport = _CongestedTransport(created.transport)
            transports.append(transport)
            return ConnectingTransport(transport=transport, ready=created.ready)

        publisher = await connect_relay_publisher(URL, namespace=NAMESPACE, create_transport=connect_publisher)
        subscriber = await connect_relay(URL, create_transport=hub.connect_subscriber)
        cleanup.extend([subscriber.close, publisher.close])
        video = publisher.publish("video", write_timeout=0.05)
        objects = receive(subscriber, "video")
        group = await video.open_group()
        await group.write(b"key")
        # The deadline applies per operation, not to the total open group lifetime or idle time.
        await asyncio.sleep(0.075)
        assert not group.closed
        await group.write(b"delta")
        stream = transports[0].streams[-1]
        stream.hold_writes = True

        with pytest.raises(RelayTimeoutError, match="timed out") as failure:
            await asyncio.wait_for(group.write(b"stalled") if operation == "write" else group.close(), 1)

        assert stream.reset_with is failure.value
        assert group.closed
        assert not video.closed
        assert video.bound
        assert group.object_count == 2
        stream.release()
        await settle()
        assert not stream.finned

        # A new group can use the released slot and cannot be cancelled by the previous timer.
        next_group = await video.open_group()
        next_stream = transports[0].streams[-1]
        next_stream.release()
        await next_group.write(b"next key")
        await next_group.close()
        await wait_for(lambda: len(objects) == 3)
        assert [obj.payload for obj in objects] == [b"key", b"delta", b"next key"]
        assert next_stream.finned

    async def test_closing_the_track_resets_a_group_whose_fin_is_still_queued(self, cleanup: Cleanup) -> None:
        # A FIN blocked by transport backpressure used to be unreachable: the writer marked itself
        # finished before awaiting the close, so the cancel could not reset the stream. The group was
        # delivered complete although the SDK reported it aborted, and close() waited for the link.
        hub = create_relay_hub()
        cleanup.append(hub.destroy)
        transports: list[_CongestedTransport] = []

        def connect_publisher(url: str = "", protocols: Sequence[str] = ()) -> ConnectingTransport:
            created = hub.connect_publisher(url, protocols)
            transport = _CongestedTransport(created.transport)
            transports.append(transport)
            return ConnectingTransport(transport=transport, ready=created.ready)

        publisher = await connect_relay_publisher(URL, namespace=NAMESPACE, create_transport=connect_publisher)
        subscriber = await connect_relay(URL, create_transport=hub.connect_subscriber)
        cleanup.append(subscriber.close)
        cleanup.append(publisher.close)

        video = publisher.publish("video")
        objects = receive(subscriber, "video")
        group = await video.open_group()
        await group.write(b"key")
        await wait_for(lambda: len(objects) == 1)

        stream = transports[0].streams[-1]
        closing = asyncio.ensure_future(group.close())
        await settle()
        assert not closing.done()  # the FIN is queued: the link has not taken it

        video.close()  # a track close, a re-bind, the publisher closing: all cancel the operation

        with pytest.raises(RelayClosedError, match="closed"):
            await asyncio.wait_for(closing, 1)

        assert isinstance(stream.reset_with, RelayClosedError)  # reset, not a complete group
        assert not stream.finned

        await settle()
        assert len(objects) == 1  # and nothing is delivered after the cancel
        assert hub.object_count("video") == 1

    async def test_a_cancelled_close_resets_the_group_instead_of_leaving_the_fin_queued(self, cleanup: Cleanup) -> None:
        # Giving up on a blocked close (a deadline, a cancelled task) does not withdraw the FIN: the
        # transport holds it behind a shielded waiter. So the writer must reset the stream on its way
        # out, or the group turns up complete once the link heals, after the caller was told it failed.
        hub = create_relay_hub()
        cleanup.append(hub.destroy)
        transports: list[_CongestedTransport] = []

        def connect_publisher(url: str = "", protocols: Sequence[str] = ()) -> ConnectingTransport:
            created = hub.connect_publisher(url, protocols)
            transport = _CongestedTransport(created.transport)
            transports.append(transport)
            return ConnectingTransport(transport=transport, ready=created.ready)

        publisher = await connect_relay_publisher(URL, namespace=NAMESPACE, create_transport=connect_publisher)
        subscriber = await connect_relay(URL, create_transport=hub.connect_subscriber)
        cleanup.append(subscriber.close)
        cleanup.append(publisher.close)

        video = publisher.publish("video")
        objects = receive(subscriber, "video")
        group = await video.open_group()
        await group.write(b"key")
        await wait_for(lambda: len(objects) == 1)

        stream = transports[0].streams[-1]

        with pytest.raises(TimeoutError):
            await asyncio.wait_for(group.close(), 0.05)

        assert isinstance(stream.reset_with, asyncio.CancelledError)  # reset on the way out
        assert not stream.finned

        # The link heals: the abandoned FIN must not turn up as a complete group.
        stream.release()
        await settle()
        assert not stream.finned
        assert len(objects) == 1
        assert hub.object_count("video") == 1
