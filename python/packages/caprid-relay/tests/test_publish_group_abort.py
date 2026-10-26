"""Failure identity when a publisher's closure races a send that has already completed.

The write slot's own phase-by-interruption behaviour (a FIN stuck behind backpressure, deadlines,
task cancellation) is covered in ``test_write_slot.py`` against an in-memory subgroup writer. What
stays here needs a real publisher connection: the owner's cancellation reason, or the connection's
closure, landing in the same event-loop turn as the send's completion.
"""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence

import pytest
from relay_helpers.relay_hub import create_relay_hub

from caprid_relay import (
    CancellationToken,
    ConnectingTransport,
    RelaySubscriber,
    RelaySubscriptionHandlers,
    connect_relay_publisher,
    connect_relay_subscriber,
)
from moq_core import MoqtObject
from moq_core.transport import SendStream

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


def receive(subscriber: RelaySubscriber, track: str) -> list[MoqtObject]:
    objects: list[MoqtObject] = []
    subscriber.subscribe(NAMESPACE, track, RelaySubscriptionHandlers(on_object=objects.append))
    return objects


@pytest.mark.parametrize("operation", ["write", "open_group", "group_write", "group_close"])
@pytest.mark.parametrize("default_reason", [False, True])
async def test_owner_cancellation_after_send_completion_keeps_identity(
    cleanup: Cleanup, operation: str, default_reason: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    hub = create_relay_hub()
    cleanup.append(hub.destroy)
    owner = CancellationToken()
    reason = None if default_reason else RuntimeError("owner finished")

    def cancel_when_task_completes() -> None:
        # The task's wait callback was registered before the transport was called. It wakes the
        # public operation before this callback schedules the publisher's track-close callback.
        task = asyncio.current_task()
        assert task is not None
        task.add_done_callback(lambda _: owner.cancel(reason))

    class CancelOnCompletionStream:
        def __init__(self, inner: SendStream) -> None:
            self.inner = inner
            self.writes = 0

        async def write(self, data: bytes) -> None:
            await self.inner.write(data)
            self.writes += 1
            if operation == "group_write" and self.writes == 2:
                cancel_when_task_completes()

        async def close(self) -> None:
            await self.inner.close()
            if operation in ("write", "group_close"):
                cancel_when_task_completes()

        def abort(self, reason: object = None) -> None:
            self.inner.abort(reason)

    def create_transport(url: str, protocols: Sequence[str]) -> ConnectingTransport:
        created = hub.connect_publisher(url, protocols)
        transport = created.transport
        open_stream = transport.create_unidirectional_stream
        streams = 0

        async def open_wrapped() -> SendStream:
            nonlocal streams
            streams += 1
            stream = await open_stream()
            if streams != 2:
                return stream
            if operation == "open_group":
                cancel_when_task_completes()
            return CancelOnCompletionStream(stream)

        monkeypatch.setattr(transport, "create_unidirectional_stream", open_wrapped)
        return created

    publisher = await connect_relay_publisher(
        URL, namespace=NAMESPACE, create_transport=create_transport, cancellation=owner
    )
    subscriber = await connect_relay_subscriber(URL, create_transport=hub.connect_subscriber)
    cleanup.extend([subscriber.close, publisher.close])
    track = publisher.register_track("events")
    receive(subscriber, "events")
    with pytest.raises(Exception) as failure:
        if operation == "write":
            await track.write(b"value")
        else:
            group = await track.open_group()
            if operation == "group_write":
                await group.write(b"value")
            elif operation == "group_close":
                await group.close()
    assert failure.value is owner.reason
    assert (await publisher.closed).error is owner.reason


@pytest.mark.parametrize("operation", ["write", "open_group"])
async def test_task_cancellation_wins_over_simultaneous_owner_closure(cleanup: Cleanup, operation: str) -> None:
    hub = create_relay_hub()
    cleanup.append(hub.destroy)
    owner = CancellationToken()
    publisher = await connect_relay_publisher(
        URL, namespace=NAMESPACE, create_transport=hub.connect_publisher, cancellation=owner
    )
    cleanup.append(publisher.close)
    track = publisher.register_track("events")
    pending = asyncio.ensure_future(track.write(b"value") if operation == "write" else track.open_group())
    await asyncio.sleep(0)
    # Wake the task first, then close the lifetime before its continuation runs.
    pending.cancel("caller cancelled task")
    owner.cancel()
    with pytest.raises(asyncio.CancelledError, match="caller cancelled task"):
        await pending
    assert pending.cancelled()
    assert (await publisher.closed).error is owner.reason
