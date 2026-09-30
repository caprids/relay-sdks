import asyncio

import pytest
from core_helpers.waiting import wait_for

from moq_core.bytes import StreamReader
from moq_core.control_messages import KeyValuePair
from moq_core.errors import MoqtProtocolError
from moq_core.object_stream import (
    MAX_OBJECT_PAYLOAD_LENGTH,
    MAX_OBJECT_PROPERTIES_LENGTH,
    MoqtObject,
    SubgroupHeader,
    read_subgroup_header,
    read_subgroup_objects,
)
from moq_core.subgroup_writer import create_subgroup_writer

# LOC property ids, used numerically so the wire round-trip matches what LOC packaging emits without
# a media import.
TIMESTAMP = 0x06
TIMESCALE = 0x08
VIDEO_CONFIG = 13


class CollectingStream:
    """A ``SendStream`` that records everything written and whether it was closed or aborted."""

    def __init__(self) -> None:
        self.chunks: list[bytes] = []
        self.closed = False
        self.aborted = False
        self.abort_reason: object = None

    async def write(self, data: bytes) -> None:
        self.chunks.append(data)

    async def close(self) -> None:
        self.closed = True

    def abort(self, reason: object = None) -> None:
        self.aborted = True
        self.abort_reason = reason


class ReplayStream:
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = list(chunks)

    async def read(self) -> bytes | None:
        return self._chunks.pop(0) if self._chunks else None

    def cancel(self, reason: object = None) -> None:
        self._chunks.clear()


async def parse_subgroup(chunks: list[bytes]) -> tuple[SubgroupHeader, list[MoqtObject]]:
    """Parse a fully written subgroup stream with the existing reader."""
    reader = StreamReader(ReplayStream(chunks))
    stream_type = await reader.read_varint()
    header = await read_subgroup_header(reader, stream_type)
    objects = [obj async for obj in read_subgroup_objects(reader, header)]
    return header, objects


class TestCreateSubgroupWriter:
    async def test_round_trips_a_multi_object_group_through_the_existing_reader(self) -> None:
        sink = CollectingStream()
        writer = create_subgroup_writer(sink, track_alias=7, group_id=3)

        await writer.write_object(
            object_id=0,
            properties=[
                KeyValuePair(TIMESTAMP, 1_000_000),
                KeyValuePair(TIMESCALE, 1_000_000),
                KeyValuePair(VIDEO_CONFIG, bytes([1, 2, 3])),
            ],
            payload=bytes([0xAA, 0xBB]),
        )
        await writer.write_object(object_id=1, properties=[KeyValuePair(TIMESTAMP, 1_033_333)], payload=bytes([0xCC]))
        await writer.fin()

        assert sink.closed
        header, objects = await parse_subgroup(sink.chunks)

        assert header.track_alias == 7
        assert header.group_id == 3
        assert header.subgroup_id_mode == "zero"
        assert header.has_properties
        assert header.end_of_group
        assert header.first_object
        assert header.priority is None

        assert [o.object_id for o in objects] == [0, 1]
        assert [o.subgroup_id for o in objects] == [0, 0]
        assert objects[0].properties == [
            KeyValuePair(TIMESTAMP, 1_000_000),
            KeyValuePair(TIMESCALE, 1_000_000),
            KeyValuePair(VIDEO_CONFIG, bytes([1, 2, 3])),
        ]
        assert objects[0].payload == bytes([0xAA, 0xBB])
        assert objects[1].properties == [KeyValuePair(TIMESTAMP, 1_033_333)]
        assert objects[1].payload == bytes([0xCC])

    async def test_round_trips_object_id_gaps_via_delta_encoding(self) -> None:
        sink = CollectingStream()
        writer = create_subgroup_writer(sink, track_alias=1, group_id=0, has_properties=False)

        await writer.write_object(object_id=0, payload=bytes([1]))
        await writer.write_object(object_id=5, payload=bytes([2]))
        await writer.write_object(object_id=9, payload=bytes([3]))
        await writer.fin()

        header, objects = await parse_subgroup(sink.chunks)

        assert not header.has_properties
        assert [o.object_id for o in objects] == [0, 5, 9]
        assert all(len(o.properties) == 0 for o in objects)

    async def test_carries_an_explicit_publisher_priority(self) -> None:
        sink = CollectingStream()
        writer = create_subgroup_writer(sink, track_alias=2, group_id=1, priority=42)

        await writer.write_object(object_id=0, properties=[], payload=bytes([1]))
        await writer.fin()

        header, objects = await parse_subgroup(sink.chunks)

        assert header.priority == 42
        assert objects[0].priority == 42

    async def test_round_trips_zero_length_payloads_with_an_explicit_status(self) -> None:
        sink = CollectingStream()
        writer = create_subgroup_writer(sink, track_alias=2, group_id=1)

        await writer.write_object(object_id=0, properties=[], payload=bytes([1]))
        await writer.write_object(object_id=1, properties=[], payload=b"", status="end-of-group")
        await writer.fin()

        _, objects = await parse_subgroup(sink.chunks)

        assert objects[1].status == "end-of-group"
        assert objects[1].payload == b""

    async def test_accepts_a_payload_exactly_at_the_limit_and_rejects_one_past_it(self) -> None:
        sink = CollectingStream()
        writer = create_subgroup_writer(sink, track_alias=1, group_id=0, has_properties=False)

        await writer.write_object(object_id=0, payload=bytes(MAX_OBJECT_PAYLOAD_LENGTH))

        with pytest.raises(MoqtProtocolError):
            await writer.write_object(object_id=1, payload=bytes(MAX_OBJECT_PAYLOAD_LENGTH + 1))

        await writer.fin()

        _, objects = await parse_subgroup(sink.chunks)

        assert len(objects) == 1
        assert len(objects[0].payload) == MAX_OBJECT_PAYLOAD_LENGTH

    async def test_rejects_a_properties_block_past_the_limit(self) -> None:
        sink = CollectingStream()
        writer = create_subgroup_writer(sink, track_alias=1, group_id=0)

        with pytest.raises(MoqtProtocolError):
            await writer.write_object(
                object_id=0,
                properties=[KeyValuePair(VIDEO_CONFIG, bytes(MAX_OBJECT_PROPERTIES_LENGTH + 1))],
                payload=bytes([1]),
            )

    async def test_rejects_non_increasing_object_ids_and_properties_without_the_flag(self) -> None:
        sink = CollectingStream()
        writer = create_subgroup_writer(sink, track_alias=1, group_id=0, has_properties=False)

        await writer.write_object(object_id=3, payload=bytes([1]))

        with pytest.raises(MoqtProtocolError):
            await writer.write_object(object_id=3, payload=bytes([2]))

        with pytest.raises(MoqtProtocolError):
            await writer.write_object(object_id=4, properties=[KeyValuePair(TIMESTAMP, 1)], payload=bytes([2]))

    async def test_rejects_writes_after_fin(self) -> None:
        sink = CollectingStream()
        writer = create_subgroup_writer(sink, track_alias=1, group_id=0)

        await writer.fin()

        with pytest.raises(MoqtProtocolError):
            await writer.write_object(object_id=0, properties=[], payload=bytes([1]))

    async def test_abort_resets_the_stream(self) -> None:
        sink = CollectingStream()
        writer = create_subgroup_writer(sink, track_alias=9, group_id=4)

        await writer.write_object(object_id=0, properties=[], payload=bytes([1, 2, 3]))
        reason = RuntimeError("dropped under backpressure")

        writer.abort(reason)

        await wait_for(lambda: sink.aborted)
        assert sink.abort_reason is reason

    async def test_a_failed_header_write_surfaces_on_the_first_awaited_write(self) -> None:
        class FailingStream(CollectingStream):
            async def write(self, data: bytes) -> None:
                raise ConnectionError("stream gone")

        writer = create_subgroup_writer(FailingStream(), track_alias=1, group_id=0)
        await asyncio.sleep(0)

        with pytest.raises(ConnectionError, match="stream gone"):
            await writer.write_object(object_id=0, properties=[], payload=bytes([1]))


class AcknowledgingStream(CollectingStream):
    """A ``CollectingStream`` whose transport reports the peer's acknowledgement when the test says so."""

    def __init__(self) -> None:
        super().__init__()
        self.acknowledgement: asyncio.Future[None] = asyncio.get_running_loop().create_future()

    async def wait_acknowledged(self) -> None:
        await self.acknowledgement


class TestWaitAcknowledged:
    async def test_counts_a_stream_without_acknowledgement_reporting_as_acknowledged_once_finned(self) -> None:
        writer = create_subgroup_writer(CollectingStream(), track_alias=1, group_id=0)
        await writer.fin()
        await asyncio.wait_for(writer.wait_acknowledged(), 1)

    async def test_waits_for_the_transports_acknowledgement_and_raises_its_failure(self) -> None:
        sink = AcknowledgingStream()
        writer = create_subgroup_writer(sink, track_alias=1, group_id=0)
        await writer.fin()
        waiting = asyncio.ensure_future(writer.wait_acknowledged())
        await asyncio.sleep(0.01)
        assert not waiting.done()
        sink.acknowledgement.set_result(None)
        await asyncio.wait_for(waiting, 1)

        failed = AcknowledgingStream()
        writer = create_subgroup_writer(failed, track_alias=1, group_id=1)
        await writer.fin()
        failed.acknowledgement.set_exception(ConnectionError("reset"))
        with pytest.raises(ConnectionError):
            await writer.wait_acknowledged()
