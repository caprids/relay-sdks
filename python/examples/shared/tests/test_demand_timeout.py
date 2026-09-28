"""``write_with_retry`` over a fake track writer: what it retries, what it never resends, what it reports."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Never, cast

import pytest
from relay_helpers.fake_track_writer import FakeTrackWriter, closed_error, timeout_error
from shared.demand_timeout import is_demand_timeout, write_with_retry

from mux_relay import KeyValuePair, RelayError, RelayErrorPhase, RelayTrackWriter
from mux_relay._write_slot import WriteSlotDependencies

PAYLOAD = b'{"seq": 1}'


class PartialWriter(FakeTrackWriter):
    """The relay took the object's bytes, then the write deadline expired before the stream closed."""

    def __init__(self, phase: RelayErrorPhase = "closing") -> None:
        super().__init__()
        self.phase = phase

    async def write(self, payload: bytes, *, properties: Sequence[KeyValuePair] | None = None) -> None:
        await super().write(payload, properties=properties)
        raise timeout_error(self.phase)


class DemandlessWriter(FakeTrackWriter):
    """No subscriber ever arrives: every write waits out a short deadline, as the SDK's does."""

    def __init__(self, deadline: float) -> None:
        super().__init__(demand=False)
        self.deadline = deadline

    async def write(self, payload: bytes, *, properties: Sequence[KeyValuePair] | None = None) -> None:
        self.calls["write"] += 1
        await asyncio.sleep(self.deadline)
        raise timeout_error("waiting-for-demand")


class Recorder:
    def __init__(self) -> None:
        self.retries: list[tuple[int, RelayError]] = []
        self.uncertain: list[RelayError] = []

    def on_retry(self, attempt: int, error: RelayError) -> None:
        self.retries.append((attempt, error))

    def on_uncertain(self, error: RelayError) -> None:
        self.uncertain.append(error)


async def write(writer: FakeTrackWriter, recorder: Recorder) -> None:
    await write_with_retry(
        cast(RelayTrackWriter, writer), PAYLOAD, on_retry=recorder.on_retry, on_uncertain=recorder.on_uncertain
    )


async def test_retries_demand_timeouts_until_the_write_goes_through() -> None:
    writer = FakeTrackWriter(demand=False)
    first, second = timeout_error("waiting-for-demand"), timeout_error("waiting-for-demand")
    writer.fail("write", first, on=0)
    writer.fail("write", second, on=1)
    recorder = Recorder()

    await write(writer, recorder)

    assert writer.calls["write"] == 3
    assert writer.writes == [PAYLOAD]
    assert recorder.retries == [(1, first), (2, second)]
    assert recorder.uncertain == []


@pytest.mark.parametrize("phase", ["opening-stream", "writing", "closing"])
async def test_never_resends_after_a_timeout_in_a_sending_phase(phase: RelayErrorPhase) -> None:
    writer = FakeTrackWriter()
    failure = timeout_error(phase)
    writer.fail("write", failure)
    recorder = Recorder()

    with pytest.raises(RelayError) as caught:
        await write(writer, recorder)

    assert caught.value is failure
    assert writer.calls["write"] == 1
    assert recorder.retries == []
    assert recorder.uncertain == [failure]


async def test_a_timeout_after_partial_progress_is_attempted_once_and_raised() -> None:
    writer = PartialWriter()
    recorder = Recorder()

    with pytest.raises(RelayError) as caught:
        await write(writer, recorder)

    assert (caught.value.code, caught.value.phase) == ("WRITE_TIMEOUT", "closing")
    assert writer.writes == [PAYLOAD]  # The relay holds the bytes; they are not sent again.
    assert writer.calls["write"] == 1
    assert recorder.uncertain == [caught.value]
    assert recorder.retries == []


@pytest.mark.parametrize(
    "failure",
    [
        RelayError("Published track is closed", code="CLOSED", phase="waiting-for-demand"),
        RelayError("not covered", code="NAMESPACE_NOT_SOLICITED", phase="waiting-for-demand"),
    ],
)
async def test_other_failures_before_sending_are_raised_without_retrying(failure: RelayError) -> None:
    writer = FakeTrackWriter(demand=False)
    writer.fail("write", failure)
    recorder = Recorder()

    with pytest.raises(RelayError) as caught:
        await write(writer, recorder)

    assert caught.value is failure
    assert writer.calls["write"] == 1
    assert recorder.retries == []
    assert recorder.uncertain == []


async def test_a_write_on_an_ended_track_is_raised_without_retrying_or_uncertain_delivery() -> None:
    # The SDK's own track writer, not a fake: a refusal that sent nothing must not look like a sending phase.
    opened: list[int] = []

    async def open_subgroup(track_alias: int, group_id: int) -> Never:
        opened.append(group_id)
        raise AssertionError("a closed track opens no stream")

    writer = RelayTrackWriter(
        "events",
        WriteSlotDependencies(
            open_subgroup=open_subgroup,
            namespace_solicited=lambda: None,
            assert_open=lambda: None,
            end_track=lambda: None,
            write_timeout=1.0,
        ),
    )
    writer.close()
    recorder = Recorder()

    with pytest.raises(RelayError) as caught:
        await write_with_retry(writer, PAYLOAD, on_retry=recorder.on_retry, on_uncertain=recorder.on_uncertain)

    assert (caught.value.code, caught.value.phase) == ("CLOSED", "waiting-for-demand")
    assert opened == []
    assert recorder.retries == []
    assert recorder.uncertain == []


async def test_branches_on_code_and_phase_never_on_the_message() -> None:
    writer = FakeTrackWriter()
    # The demand-timeout message, but a transport phase: bytes may have left.
    failure = RelayError("Publish write timed out waiting for relay demand", code="WRITE_TIMEOUT", phase="writing")
    writer.fail("write", failure)
    recorder = Recorder()

    with pytest.raises(RelayError):
        await write(writer, recorder)

    assert writer.calls["write"] == 1
    assert not is_demand_timeout(RuntimeError("timed out waiting for relay demand"))
    assert is_demand_timeout(RelayError("anything", code="WRITE_TIMEOUT", phase="waiting-for-demand"))


async def test_the_deadline_bounds_the_retries_and_cancels_the_wait() -> None:
    writer = DemandlessWriter(deadline=0.005)
    recorder = Recorder()

    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.05):
            await write(writer, recorder)

    assert writer.calls["write"] >= 2
    assert len(recorder.retries) == writer.calls["write"] - 1
    assert writer.writes == []


async def test_cancellation_ends_a_wait_promptly() -> None:
    writer = DemandlessWriter(deadline=10)
    loop = asyncio.get_running_loop()
    started = loop.time()

    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.02):
            await write(writer, Recorder())

    assert loop.time() - started < 1
    assert writer.calls["write"] == 1


async def test_a_raising_on_retry_stops_the_retries() -> None:
    writer = FakeTrackWriter(demand=False)
    writer.fail("write", timeout_error("waiting-for-demand"))

    def stop(attempt: int, error: RelayError) -> None:
        raise RuntimeError("subscribe rejected")

    with pytest.raises(RuntimeError, match="subscribe rejected"):
        await write_with_retry(cast(RelayTrackWriter, writer), PAYLOAD, on_retry=stop)

    assert writer.calls["write"] == 1


async def test_raises_unchanged_without_callbacks() -> None:
    writer = FakeTrackWriter(demand=False)
    writer.fail("write", closed_error())

    with pytest.raises(RelayError, match="closed"):
        await write_with_retry(cast(RelayTrackWriter, writer), PAYLOAD)

    assert writer.calls["write"] == 1
