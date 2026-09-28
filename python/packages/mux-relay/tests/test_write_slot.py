"""The write slot through its own interface: the public track writer plus the two inbound calls the
publisher makes, driven with an in-memory subgroup writer. Rows follow the "Write phases and
interruptions" table in ``spec/behavior.md``. Relay behaviour the slot cannot see (demand arriving
through the real binding callback, announce-and-serve) stays in ``test_publish*.py``.
"""

import asyncio
from collections.abc import Sequence
from typing import Any, Literal

import pytest
from core_helpers.waiting import settle, wait_for

from moq_core import KeyValuePair, ObjectStatus
from moq_core.control_messages import STREAM_RESET_CODE
from mux_relay import RelayError
from mux_relay._write_slot import RelayTrackWriter, StreamReset, WriteSlotDependencies

Stall = Literal["none", "write", "second-write", "fin"]
SHORT = 0.05
"""A deadline short enough to expire inside a test and long enough for the loop to settle first."""


class FakeSubgroup:
    """One subgroup stream as the slot sees it: records objects, FIN and abort reasons, and can stall or fail."""

    def __init__(self, track_alias: int, group_id: int, stall: Stall) -> None:
        self.track_alias = track_alias
        self.group_id = group_id
        self.objects: list[dict[str, Any]] = []
        self.aborts: list[object] = []
        self.fin_calls = 0
        self._stall = stall
        self._waiting: asyncio.Future[None] | None = None

    async def write_object(
        self,
        *,
        object_id: int,
        payload: bytes,
        properties: Sequence[KeyValuePair] | None = None,
        status: ObjectStatus = "normal",
    ) -> None:
        self.objects.append({"object_id": object_id, "payload": payload, "properties": properties, "status": status})
        stalled = self._stall == "write" or (self._stall == "second-write" and len(self.objects) == 2)
        await self._settle(stalled)

    async def fin(self) -> None:
        self.fin_calls += 1
        await self._settle(self._stall == "fin")

    def abort(self, reason: object = None) -> None:
        self.aborts.append(reason)
        self._finish(reason if isinstance(reason, BaseException) else RelayError("stream was reset", code="CLOSED"))

    def fail(self, error: BaseException) -> None:
        """The transport rejects the stalled step, as a failed stream write would."""
        self._finish(error)

    def release(self) -> None:
        """The transport accepts the stalled step."""
        if self._waiting is not None and not self._waiting.done():
            self._waiting.set_result(None)
        self._waiting = None

    @property
    def stalled(self) -> bool:
        return self._waiting is not None and not self._waiting.done()

    def _finish(self, error: BaseException) -> None:
        if self._waiting is not None and not self._waiting.done():
            self._waiting.set_exception(error)
        self._waiting = None

    async def _settle(self, stall: bool) -> None:
        if not stall:
            return
        self._waiting = asyncio.get_running_loop().create_future()
        await self._waiting


class Harness:
    """A write slot with fake dependencies, plus the connection-side events a publisher would deliver."""

    def __init__(
        self,
        *,
        write_timeout: float = 1.0,
        solicited: bool | None = None,
        stall: Stall = "none",
        open: Literal["ok", "hold", "reject"] = "ok",
    ) -> None:
        self.opened: list[FakeSubgroup] = []
        self.open_failure = ConnectionError("data stream failed")
        self.end_track_calls = 0
        self._stall = stall
        self._open = open
        self._holds: list[asyncio.Future[FakeSubgroup]] = []
        self._connection_closed = False
        self._connection_error: BaseException | None = None
        self.writer = RelayTrackWriter(
            "events",
            WriteSlotDependencies(
                open_subgroup=self._open_subgroup,
                namespace_solicited=lambda: solicited,
                assert_open=self._assert_open,
                end_track=self._end_track,
                write_timeout=write_timeout,
            ),
        )

    async def _open_subgroup(self, track_alias: int, group_id: int) -> FakeSubgroup:
        if self._open == "reject":
            raise self.open_failure
        # Only the first stream stalls, so the recovery write after a failure can complete.
        subgroup = FakeSubgroup(track_alias, group_id, self._stall if not self.opened else "none")
        self.opened.append(subgroup)
        if self._open == "hold":
            held: asyncio.Future[FakeSubgroup] = asyncio.get_running_loop().create_future()
            self._holds.append(held)
            return await held
        return subgroup

    @property
    def holding(self) -> bool:
        """A stream open is parked behind :meth:`release_open`."""
        return bool(self._holds)

    def release_open(self) -> None:
        """Let a held stream open complete, as a late transport would."""
        held = self._holds.pop(0)
        held.set_result(self.opened[len(self.opened) - len(self._holds) - 1])

    def _assert_open(self) -> None:
        if self._connection_closed:
            raise self._connection_error or RelayError("Relay connection is closed", code="CLOSED", phase="connection")

    def _end_track(self) -> None:
        self.end_track_calls += 1

    def disconnect(self, error: BaseException | None = None) -> None:
        """The connection ends: the lifetime's open check fails, then the publisher tells the slot."""
        self._connection_closed = True
        self._connection_error = error
        self.writer._disconnect(error)

    def fail_connection(self, error: BaseException) -> None:
        """The connection ends before the publisher's callback reaches the slot."""
        self._connection_closed = True
        self._connection_error = error


def reset_code(reason: object) -> int:
    assert isinstance(reason, StreamReset)
    return reason.stream_error_code


async def started(coroutine: Any) -> asyncio.Task[Any]:
    task = asyncio.ensure_future(coroutine)
    await asyncio.sleep(0)
    return task


class TestWaitingForDemand:
    async def test_sends_one_object_as_its_own_group_once_demand_is_bound_copying_the_payload_first(self) -> None:
        h = Harness()
        payload = bytearray(b"\x01\x02")
        mutable: Any = payload  # the slot copies whatever bytes-like it is handed
        writing = await started(h.writer.write(mutable, properties=[KeyValuePair(2, 7)]))
        payload[0] = 9
        assert not h.writer.has_demand
        assert h.opened == []
        h.writer._bind(4)
        assert h.writer.has_demand
        await writing
        assert len(h.opened) == 1
        first = h.opened[0]
        assert (first.track_alias, first.group_id, first.fin_calls, first.aborts) == (4, 0, 1, [])
        assert h.opened[0].objects == [
            {"object_id": 0, "payload": b"\x01\x02", "properties": [KeyValuePair(2, 7)], "status": "normal"}
        ]
        await h.writer.write(b"\x03")
        assert (h.opened[1].track_alias, h.opened[1].group_id) == (4, 1)

    async def test_times_out_with_no_stream_opened_and_no_group_id_consumed_then_writes_after_demand(self) -> None:
        h = Harness(write_timeout=SHORT)
        with pytest.raises(RelayError) as failure:
            await h.writer.write(b"\x01")
        assert (failure.value.code, failure.value.phase) == ("WRITE_TIMEOUT", "waiting-for-demand")
        assert h.opened == []
        assert not h.writer.closed
        h.writer._bind(1)
        await h.writer.write(b"\x02")
        assert h.opened[0].group_id == 0

    @pytest.mark.parametrize(
        ("solicited", "code"),
        [(False, "NAMESPACE_NOT_SOLICITED"), (True, "WRITE_TIMEOUT"), (None, "WRITE_TIMEOUT")],
    )
    async def test_the_deadline_reports_namespace_coverage_when_solicitations_exclude_it(
        self, solicited: bool | None, code: str
    ) -> None:
        h = Harness(write_timeout=SHORT, solicited=solicited)
        with pytest.raises(RelayError) as failure:
            await h.writer.write(b"\x01")
        assert (failure.value.code, failure.value.phase) == (code, "waiting-for-demand")

    async def test_keeps_waiting_when_the_relay_subscription_ends_before_any_demand_was_used(self) -> None:
        h = Harness(write_timeout=SHORT)
        writing = await started(h.writer.write(b"\x01"))
        h.writer._bind(None)
        await asyncio.sleep(SHORT / 2)
        assert not writing.done()
        assert h.opened == []
        with pytest.raises(RelayError) as failure:
            await writing
        assert failure.value.code == "WRITE_TIMEOUT"

    async def test_reports_demand_that_came_and_went_before_the_write_started(self) -> None:
        h = Harness()
        writing = await started(h.writer.write(b"\x01"))
        h.writer._bind(5)
        h.writer._bind(None)
        with pytest.raises(RelayError, match="ended before the write started") as failure:
            await writing
        assert failure.value.code == "SUBSCRIPTION_CHANGED"
        assert not h.writer.closed
        h.writer._bind(6)
        await h.writer.write(b"\x02")

    async def test_rejects_a_second_write_or_group_with_write_pending_and_leaves_the_first_unaffected(self) -> None:
        h = Harness()
        writing = await started(h.writer.write(b"\x01"))
        with pytest.raises(RelayError) as failure:
            await h.writer.write(b"\x02")
        assert (failure.value.code, failure.value.phase) == ("WRITE_PENDING", "writing")
        with pytest.raises(RelayError) as failure:
            await h.writer.open_group()
        assert failure.value.code == "WRITE_PENDING"
        h.writer._bind(1)
        await writing
        assert len(h.opened) == 1

    async def test_task_cancellation_keeps_its_identity_and_opens_nothing(self) -> None:
        h = Harness()
        writing = await started(h.writer.write(b"\x01"))
        writing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await writing
        assert h.opened == []
        assert not h.writer.closed
        h.writer._bind(1)
        await h.writer.write(b"\x02")

    async def test_ends_the_track_on_close_cancelling_the_wait_and_telling_the_core_once(self) -> None:
        h = Harness()
        writing = await started(h.writer.write(b"\x01"))
        h.writer.close()
        h.writer.close()
        with pytest.raises(RelayError) as failure:
            await writing
        assert (failure.value.code, failure.value.phase) == ("CLOSED", "waiting-for-demand")
        assert h.end_track_calls == 1
        assert h.writer.closed
        assert not h.writer.has_demand
        assert h.opened == []
        with pytest.raises(RelayError) as failure:
            await h.writer.write(b"\x02")
        # Refused before any operation started: nothing sent, so not a sending phase.
        assert (failure.value.code, failure.value.phase) == ("CLOSED", "waiting-for-demand")
        with pytest.raises(RelayError) as failure:
            await h.writer.open_group()
        assert (failure.value.code, failure.value.phase) == ("CLOSED", "waiting-for-demand")
        assert h.opened == []

    @pytest.mark.parametrize("error", [RelayError("link dropped", code="TRANSPORT_ERROR", phase="connection"), None])
    async def test_shares_the_connection_end_with_the_pending_write_and_ends_the_track(
        self, error: BaseException | None
    ) -> None:
        h = Harness()
        writing = await started(h.writer.write(b"\x01"))
        h.disconnect(error)
        with pytest.raises(RelayError) as failure:
            await writing
        if error is not None:
            assert failure.value is error
        else:
            assert failure.value.code == "CLOSED"
        assert h.writer.closed
        assert h.end_track_calls == 1


class TestOpeningTheStream:
    async def test_times_out_a_held_open_and_resets_the_late_stream_with_delivery_timeout(self) -> None:
        h = Harness(write_timeout=SHORT, open="hold")
        h.writer._bind(1)
        with pytest.raises(RelayError) as failure:
            await h.writer.write(b"\x01")
        assert (failure.value.code, failure.value.phase) == ("WRITE_TIMEOUT", "opening-stream")
        assert not h.writer.closed
        h.release_open()
        await settle()
        reason = h.opened[0].aborts[0]
        assert isinstance(reason, StreamReset)
        assert (reason.stream_error_code, reason.__cause__) == (STREAM_RESET_CODE.DELIVERY_TIMEOUT, failure.value)
        assert h.opened[0].objects == []
        writing = await started(h.writer.write(b"\x02"))
        await wait_for(lambda: h.holding)
        h.release_open()
        await writing
        assert (h.opened[1].group_id, h.opened[1].fin_calls) == (1, 1)

    async def test_reports_a_refused_stream_as_transport_error_with_the_cause_and_keeps_the_track(self) -> None:
        h = Harness(open="reject")
        h.writer._bind(1)
        with pytest.raises(RelayError) as failure:
            await h.writer.write(b"\x01")
        assert (failure.value.code, failure.value.phase) == ("TRANSPORT_ERROR", "opening-stream")
        assert failure.value.__cause__ is h.open_failure
        assert not h.writer.closed
        assert h.writer.has_demand

    async def test_cancels_a_held_open_and_resets_the_late_stream_with_cancelled(self) -> None:
        h = Harness(open="hold")
        h.writer._bind(1)
        writing = await started(h.writer.write(b"\x01"))
        await wait_for(lambda: h.holding)
        writing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await writing
        h.release_open()
        await settle()
        assert [reset_code(reason) for reason in h.opened[0].aborts] == [STREAM_RESET_CODE.CANCELLED]

    async def test_fails_a_held_open_with_subscription_changed_when_the_relay_rebinds_then_serves_the_new_alias(
        self,
    ) -> None:
        h = Harness(open="hold")
        h.writer._bind(1)
        writing = await started(h.writer.write(b"\x01"))
        await wait_for(lambda: h.holding)
        h.writer._bind(2)
        with pytest.raises(RelayError) as failure:
            await writing
        assert (failure.value.code, failure.value.phase) == ("SUBSCRIPTION_CHANGED", "opening-stream")
        h.release_open()
        await settle()
        assert [reset_code(reason) for reason in h.opened[0].aborts] == [STREAM_RESET_CODE.CANCELLED]
        writing = await started(h.writer.write(b"\x02"))
        await wait_for(lambda: h.holding)
        h.release_open()
        await writing
        assert (h.opened[1].track_alias, h.opened[1].group_id) == (2, 1)

    @pytest.mark.parametrize(
        ("error", "code"),
        [
            (RelayError("gone", code="TRANSPORT_ERROR", phase="connection"), STREAM_RESET_CODE.SESSION_CLOSED),
            (RelayError("migrate", code="GOAWAY", phase="connection"), STREAM_RESET_CODE.GOING_AWAY),
        ],
    )
    async def test_resets_a_late_stream_after_the_connection_ended_with_the_matching_code(
        self, error: RelayError, code: int
    ) -> None:
        h = Harness(open="hold")
        h.writer._bind(1)
        writing = await started(h.writer.write(b"\x01"))
        await wait_for(lambda: h.holding)
        h.disconnect(error)
        with pytest.raises(RelayError) as failure:
            await writing
        assert failure.value is error
        h.release_open()
        await settle()
        assert [reset_code(reason) for reason in h.opened[0].aborts] == [code]
        assert h.writer.closed


class TestWritingAndClosingASingleWrite:
    @pytest.mark.parametrize(("stall", "phase"), [("write", "writing"), ("fin", "closing")])
    async def test_times_out_a_stalled_step_resets_with_delivery_timeout_and_keeps_the_track_and_demand(
        self, stall: Stall, phase: str
    ) -> None:
        h = Harness(write_timeout=SHORT, stall=stall)
        h.writer._bind(1)
        writing = await started(h.writer.write(b"\x01"))
        await wait_for(lambda: h.opened[0].stalled)
        with pytest.raises(RelayError) as failure:
            await h.writer.write(b"\x02")
        assert failure.value.code == "WRITE_PENDING"
        with pytest.raises(RelayError) as failure:
            await writing
        assert (failure.value.code, failure.value.phase) == ("WRITE_TIMEOUT", phase)
        assert [reset_code(reason) for reason in h.opened[0].aborts] == [STREAM_RESET_CODE.DELIVERY_TIMEOUT]
        assert not h.writer.closed
        assert h.writer.has_demand
        await h.writer.write(b"\x03")
        assert (h.opened[1].group_id, h.opened[1].fin_calls) == (1, 1)

    async def test_cancels_a_stalled_write_keeping_the_cancellation_and_resets_with_cancelled(self) -> None:
        h = Harness(stall="write")
        h.writer._bind(1)
        writing = await started(h.writer.write(b"\x01"))
        await wait_for(lambda: h.opened[0].stalled)
        writing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await writing
        assert [reset_code(reason) for reason in h.opened[0].aborts] == [STREAM_RESET_CODE.CANCELLED]
        await h.writer.write(b"\x02")
        assert h.opened[1].group_id == 1

    async def test_wraps_a_failed_stream_write_as_transport_error_with_the_cause_and_resets_with_internal_error(
        self,
    ) -> None:
        h = Harness(stall="write")
        h.writer._bind(1)
        writing = await started(h.writer.write(b"\x01"))
        await wait_for(lambda: h.opened[0].stalled)
        cause = ConnectionError("stream write failed")
        h.opened[0].fail(cause)
        with pytest.raises(RelayError) as failure:
            await writing
        assert (failure.value.code, failure.value.phase) == ("TRANSPORT_ERROR", "writing")
        assert failure.value.__cause__ is cause
        assert [reset_code(reason) for reason in h.opened[0].aborts] == [STREAM_RESET_CODE.INTERNAL_ERROR]
        assert not h.writer.closed

    @pytest.mark.parametrize("next_alias", [None, 2])
    async def test_fails_an_in_flight_write_with_subscription_changed_when_the_relay_subscription_moves(
        self, next_alias: int | None
    ) -> None:
        h = Harness(stall="write")
        h.writer._bind(1)
        writing = await started(h.writer.write(b"\x01"))
        await wait_for(lambda: h.opened[0].stalled)
        h.writer._bind(next_alias)
        with pytest.raises(RelayError) as failure:
            await writing
        assert (failure.value.code, failure.value.phase) == ("SUBSCRIPTION_CHANGED", "writing")
        assert [reset_code(reason) for reason in h.opened[0].aborts] == [STREAM_RESET_CODE.CANCELLED]
        assert not h.writer.closed
        assert h.writer.has_demand is (next_alias is not None)

    async def test_cancels_an_in_flight_write_with_closed_when_the_track_closes_and_resets_with_cancelled(self) -> None:
        h = Harness(stall="write")
        h.writer._bind(1)
        writing = await started(h.writer.write(b"\x01"))
        await wait_for(lambda: h.opened[0].stalled)
        h.writer.close()
        with pytest.raises(RelayError) as failure:
            await writing
        assert (failure.value.code, failure.value.phase) == ("CLOSED", "writing")
        assert [reset_code(reason) for reason in h.opened[0].aborts] == [STREAM_RESET_CODE.CANCELLED]
        assert h.end_track_calls == 1
        assert h.writer.closed

    async def test_reports_the_connection_failure_not_a_wrapped_transport_error_when_closure_precedes_the_callback(
        self,
    ) -> None:
        h = Harness(stall="fin")
        h.writer._bind(1)
        writing = await started(h.writer.write(b"\x01"))
        await wait_for(lambda: h.opened[0].stalled)
        error = RelayError("connection dropped", code="TRANSPORT_ERROR", phase="connection")
        h.fail_connection(error)
        h.opened[0].release()
        with pytest.raises(RelayError) as failure:
            await writing
        assert failure.value is error
        assert [reset_code(reason) for reason in h.opened[0].aborts] == [STREAM_RESET_CODE.SESSION_CLOSED]

    @pytest.mark.parametrize("turns", range(6))
    async def test_settles_with_the_owner_reason_or_success_when_closure_lands_after_the_fin(self, turns: int) -> None:
        h = Harness(stall="fin")
        h.writer._bind(1)
        reason = RelayError("owner finished", code="TRANSPORT_ERROR", phase="connection")
        writing = await started(h.writer.write(b"\x01"))
        await wait_for(lambda: h.opened[0].stalled)

        async def close_later() -> None:
            for _ in range(turns):
                await asyncio.sleep(0)
            h.disconnect(reason)

        h.opened[0].release()
        closing = asyncio.ensure_future(close_later())
        try:
            await writing
        except BaseException as error:
            # Completion may win at later offsets. Any failure must be the owner's exact reason.
            assert error is reason
        await closing
        assert h.writer.closed


class TestGroups:
    async def test_holds_the_slot_appends_objects_with_increasing_ids_and_fins_once_on_close(self) -> None:
        h = Harness()
        h.writer._bind(1)
        group = await h.writer.open_group()
        assert (group.group_id, group.object_count, group.closed) == (0, 0, False)
        with pytest.raises(RelayError) as failure:
            await h.writer.write(b"\x01")
        assert failure.value.code == "WRITE_PENDING"
        with pytest.raises(RelayError) as failure:
            await h.writer.open_group()
        assert failure.value.code == "WRITE_PENDING"
        text: Any = "not bytes"
        unknown_status: Any = "later"
        with pytest.raises(TypeError):
            await group.write(text)
        with pytest.raises(TypeError):
            await group.write(b"\x01", properties=[KeyValuePair(3, 3)])
        with pytest.raises(ValueError, match="Unknown object status"):
            await group.write(b"", status=unknown_status)
        not_a_status: Any = 7
        with pytest.raises(TypeError, match="Object status must be a string"):
            await group.write(b"", status=not_a_status)
        await group.write(b"\x01")
        await group.write(b"\x02", properties=[KeyValuePair(2, 7)])
        await group.write(b"", status="end-of-group")
        assert group.object_count == 3
        assert [(obj["object_id"], obj["status"]) for obj in h.opened[0].objects] == [
            (0, "normal"),
            (1, "normal"),
            (2, "end-of-group"),
        ]
        await group.close()
        await group.close()
        assert group.closed
        assert h.opened[0].fin_calls == 1
        with pytest.raises(RelayError, match="Group is closed") as failure:
            await group.write(b"\x09")
        # The FIN already went out: the refused write sent nothing.
        assert (failure.value.code, failure.value.phase) == ("CLOSED", "waiting-for-demand")
        assert group.object_count == 3
        assert len(h.opened[0].objects) == 3
        await h.writer.write(b"\x03")
        assert h.opened[1].group_id == 1

    async def test_waits_for_demand_before_opening_a_group_and_releases_the_slot_after_a_timeout(self) -> None:
        h = Harness(write_timeout=SHORT)
        with pytest.raises(RelayError) as failure:
            await h.writer.open_group()
        assert (failure.value.code, failure.value.phase) == ("WRITE_TIMEOUT", "waiting-for-demand")
        assert not h.writer.closed
        h.writer._bind(1)
        group = await h.writer.open_group()
        await group.write(b"\x01")
        await group.close()
        assert len(h.opened) == 1

    @pytest.mark.parametrize(
        ("mode", "stall", "phase"), [("write", "second-write", "writing"), ("close", "fin", "closing")]
    )
    async def test_times_out_a_stalled_group_step_resets_with_delivery_timeout_and_permits_the_next_group(
        self, mode: str, stall: Stall, phase: str
    ) -> None:
        h = Harness(write_timeout=SHORT, stall=stall)
        h.writer._bind(1)
        group = await h.writer.open_group()
        await group.write(b"\x01")
        pending = await started(group.write(b"\x02") if mode == "write" else group.close())
        await wait_for(lambda: h.opened[0].stalled)
        with pytest.raises(RelayError) as failure:
            await group.write(b"\x03")
        assert failure.value.code == "WRITE_PENDING"
        with pytest.raises(RelayError) as failure:
            await group.close()
        assert failure.value.code == "WRITE_PENDING"
        with pytest.raises(RelayError) as failure:
            await pending
        assert (failure.value.code, failure.value.phase) == ("WRITE_TIMEOUT", phase)
        assert [reset_code(reason) for reason in h.opened[0].aborts] == [STREAM_RESET_CODE.DELIVERY_TIMEOUT]
        assert group.closed
        assert not h.writer.closed
        following = await h.writer.open_group()
        assert following.group_id == 1
        await following.write(b"\x04")
        await following.close()

    async def test_aborts_an_idle_group_resetting_with_cancelled_and_frees_the_slot(self) -> None:
        h = Harness()
        h.writer._bind(1)
        first = await h.writer.open_group()
        await first.write(b"\x01")
        reason = RelayError("drop incomplete GOP", code="CLOSED", phase="writing")
        first.abort(reason)
        assert first.closed
        with pytest.raises(RelayError) as failure:
            await first.write(b"\x02")
        assert failure.value is reason
        with pytest.raises(RelayError):
            await first.close()
        assert [reset_code(r) for r in h.opened[0].aborts] == [STREAM_RESET_CODE.CANCELLED]
        second = await h.writer.open_group()
        second.abort()
        assert second.closed
        with pytest.raises(RelayError) as failure:
            await second.close()
        assert failure.value.code == "CLOSED"
        third = await h.writer.open_group()
        await third.write(b"\x03")
        await third.close()
        assert third.group_id == 2

    async def test_ignores_abort_after_a_clean_close_no_reset_closed_stays_true_close_stays_idempotent(self) -> None:
        h = Harness()
        h.writer._bind(1)
        group = await h.writer.open_group()
        await group.write(b"\x01")
        await group.close()
        group.abort(RelayError("late abort", code="CLOSED"))
        assert group.closed
        await group.close()
        assert h.opened[0].aborts == []
        following = await h.writer.open_group()
        assert following.group_id == 1

    async def test_ends_an_idle_group_with_subscription_changed_when_the_relay_subscription_ends(self) -> None:
        h = Harness()
        h.writer._bind(1)
        group = await h.writer.open_group()
        await group.write(b"\x01")
        h.writer._bind(None)
        assert group.closed
        assert not h.writer.closed
        with pytest.raises(RelayError) as failure:
            await group.close()
        assert (failure.value.code, failure.value.phase) == ("SUBSCRIPTION_CHANGED", "writing")
        assert [reset_code(r) for r in h.opened[0].aborts] == [STREAM_RESET_CODE.CANCELLED]
        h.writer._bind(2)
        following = await h.writer.open_group()
        assert (following.group_id, h.opened[1].track_alias) == (1, 2)

    async def test_cancels_an_open_group_when_the_connection_ends_and_reports_the_connection_error(self) -> None:
        h = Harness()
        h.writer._bind(1)
        group = await h.writer.open_group()
        await group.write(b"\x01")
        error = RelayError("gone", code="TRANSPORT_ERROR", phase="connection")
        h.disconnect(error)
        assert group.closed
        assert h.writer.closed
        with pytest.raises(RelayError) as failure:
            await group.close()
        assert failure.value is error
        assert [reset_code(r) for r in h.opened[0].aborts] == [STREAM_RESET_CODE.SESSION_CLOSED]


class TestDemandObservers:
    async def test_yields_the_current_state_first_repeats_true_on_replacement_and_ends_with_the_track(self) -> None:
        h = Harness()
        seen: list[bool] = []

        async def watch() -> None:
            async for demanded in h.writer.demand_changes():
                seen.append(demanded)

        watcher = asyncio.ensure_future(watch())
        await wait_for(lambda: seen == [False])
        h.writer._bind(1)
        h.writer._bind(2)
        h.writer._bind(None)
        h.writer._bind(3)
        await wait_for(lambda: seen == [False, True, True, False, True])
        other = h.writer.demand_changes()
        assert await anext(other) is True
        h.writer.close()
        assert not h.writer.has_demand
        await wait_for(lambda: watcher.done())
        with pytest.raises(StopAsyncIteration):
            await anext(other)
        with pytest.raises(StopAsyncIteration):
            await anext(h.writer.demand_changes())

    async def test_does_not_notify_observers_after_the_track_ended_even_if_the_relay_rebinds(self) -> None:
        h = Harness()
        changes = h.writer.demand_changes()
        assert await anext(changes) is False
        h.writer.close()
        h.writer._bind(1)
        assert not h.writer.has_demand
        with pytest.raises(StopAsyncIteration):
            await anext(changes)


def test_stream_reset_carries_the_moqt_code_and_the_cause() -> None:
    cause = RelayError("why", code="WRITE_TIMEOUT", phase="writing")
    reset = StreamReset(STREAM_RESET_CODE.DELIVERY_TIMEOUT, cause)
    assert (reset.stream_error_code, str(reset), reset.__cause__) == (2, "why", cause)
