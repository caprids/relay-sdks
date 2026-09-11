import asyncio
from collections.abc import AsyncIterator, Callable
from typing import Any, TypeVar

import pytest
from core_helpers.streams import Pipe

from moq_core.bytes import ByteWriter, StreamReader, utf8_encode
from moq_core.errors import MoqtProtocolError
from moq_core.object_stream import (
    MAX_OBJECT_PAYLOAD_LENGTH,
    MAX_OBJECT_PROPERTIES_LENGTH,
    FetchEndOfRange,
    FetchObject,
    FetchStreamEntry,
    is_subgroup_header_type,
    read_fetch_entries,
    read_fetch_header,
    read_subgroup_header,
    read_subgroup_objects,
)


class ChunkedStream:
    """A ``ReceiveStream`` that hands out ``data`` in fixed-size chunks then FINs."""

    def __init__(self, data: bytes, chunk_size: int | None = None) -> None:
        size = chunk_size or max(len(data), 1)
        self._chunks = [data[offset : offset + size] for offset in range(0, len(data), size)]

    async def read(self) -> bytes | None:
        if self._chunks:
            return self._chunks.pop(0)

        return None

    def cancel(self, reason: object = None) -> None:
        self._chunks.clear()


def stream_of(data: bytes, chunk_size: int | None = None) -> StreamReader:
    return StreamReader(ChunkedStream(data, chunk_size))


def encode_subgroup_stream(
    *,
    stream_type: int,
    track_alias: int,
    group_id: int,
    objects: list[dict[str, Any]],
    subgroup_id: int | None = None,
    priority: int | None = None,
) -> bytes:
    """Build a subgroup stream: header (with explicit subgroup id + priority) and objects."""
    writer = ByteWriter()
    writer.write_varint(stream_type)
    writer.write_varint(track_alias)
    writer.write_varint(group_id)

    if subgroup_id is not None:
        writer.write_varint(subgroup_id)

    if priority is not None:
        writer.write_uint8(priority)

    for obj in objects:
        writer.write_varint(obj["object_id_delta"])

        properties = obj.get("properties")
        if isinstance(properties, bytes):
            writer.write_varint(len(properties))
            writer.write_bytes(properties)

        payload = obj.get("payload", b"")
        assert isinstance(payload, bytes)
        writer.write_varint(len(payload))

        if len(payload) == 0:
            writer.write_varint(obj.get("status", 0))
        else:
            writer.write_bytes(payload)

    return writer.to_bytes()


T = TypeVar("T")


async def collect(iterable: AsyncIterator[T]) -> list[T]:
    return [item async for item in iterable]


class TestIsSubgroupHeaderType:
    def test_accepts_the_valid_ranges(self) -> None:
        for stream_type in (0x10, 0x15, 0x18, 0x1D, 0x30, 0x3D, 0x50, 0x5D, 0x70, 0x7D):
            assert is_subgroup_header_type(stream_type)

    def test_rejects_reserved_subgroup_id_mode_and_out_of_form_values(self) -> None:
        for stream_type in (0x16, 0x17, 0x1E, 0x1F, 0x36, 0x76, 0x7E):
            assert not is_subgroup_header_type(stream_type)

        for stream_type in (0x00, 0x05, 0x0F, 0x20, 0x2F, 0x40, 0x80, 0x2F00):
            assert not is_subgroup_header_type(stream_type)


class TestReadSubgroupHeader:
    async def test_parses_an_explicit_subgroup_id_header_with_priority(self) -> None:
        # 0x14: bit4 set, mode 0b10 (explicit), no properties, priority present.
        data = encode_subgroup_stream(
            stream_type=0x14, track_alias=3, group_id=41, subgroup_id=2, priority=128, objects=[]
        )
        reader = stream_of(data)
        header = await read_subgroup_header(reader, await reader.read_varint())

        assert header.track_alias == 3
        assert header.group_id == 41
        assert header.subgroup_id_mode == "explicit"
        assert header.subgroup_id == 2
        assert header.priority == 128
        assert not header.has_properties
        assert not header.end_of_group
        assert not header.first_object

    async def test_parses_default_priority_and_end_of_group_flag_bits(self) -> None:
        # 0x38: bit4 set, mode 0b00 (zero), END_OF_GROUP (0x08), DEFAULT_PRIORITY (0x20).
        data = encode_subgroup_stream(stream_type=0x38, track_alias=1, group_id=7, objects=[])
        reader = stream_of(data)
        header = await read_subgroup_header(reader, await reader.read_varint())

        assert header.subgroup_id_mode == "zero"
        assert header.end_of_group
        assert header.priority is None


class TestReadSubgroupObjects:
    async def test_delta_decodes_object_ids(self) -> None:
        # First absolute, then +delta+1.
        data = encode_subgroup_stream(
            stream_type=0x38,
            track_alias=1,
            group_id=7,
            objects=[
                {"object_id_delta": 0, "payload": utf8_encode("a")},
                {"object_id_delta": 0, "payload": utf8_encode("b")},  # 0 + 0 + 1 = 1
                {"object_id_delta": 2, "payload": utf8_encode("c")},  # 1 + 2 + 1 = 4
            ],
        )
        reader = stream_of(data, 3)  # tiny chunks

        await reader.read_varint()
        header = await read_subgroup_header(reader, 0x38)
        objects = await collect(read_subgroup_objects(reader, header))

        assert [o.object_id for o in objects] == [0, 1, 4]
        assert [o.payload.decode() for o in objects] == ["a", "b", "c"]
        assert (objects[0].group_id, objects[0].subgroup_id, objects[0].status) == (7, 0, "normal")

    async def test_resolves_subgroup_id_from_the_first_object_in_first_object_id_mode(self) -> None:
        # 0x32 = 0b0110010: bit4 (0x10) set, mode bits (0x06)>>1 = 0b01, DEFAULT_PRIORITY set.
        data = encode_subgroup_stream(
            stream_type=0x32, track_alias=1, group_id=9, objects=[{"object_id_delta": 5, "payload": utf8_encode("x")}]
        )
        reader = stream_of(data)

        await reader.read_varint()
        header = await read_subgroup_header(reader, 0x32)

        assert header.subgroup_id_mode == "first-object-id"
        objects = await collect(read_subgroup_objects(reader, header))

        assert (objects[0].object_id, objects[0].subgroup_id) == (5, 5)

    async def test_parses_zero_length_objects_with_explicit_status(self) -> None:
        data = encode_subgroup_stream(
            stream_type=0x38,
            track_alias=1,
            group_id=7,
            objects=[
                {"object_id_delta": 0, "payload": utf8_encode("a")},
                {"object_id_delta": 0, "status": 0x3},  # end of group
            ],
        )
        reader = stream_of(data)

        await reader.read_varint()
        header = await read_subgroup_header(reader, 0x38)
        objects = await collect(read_subgroup_objects(reader, header))

        assert (objects[1].object_id, objects[1].status, objects[1].payload) == (1, "end-of-group", b"")

    async def test_raises_on_an_unknown_object_status(self) -> None:
        data = encode_subgroup_stream(
            stream_type=0x38, track_alias=1, group_id=7, objects=[{"object_id_delta": 0, "status": 0x9}]
        )
        reader = stream_of(data)

        await reader.read_varint()
        header = await read_subgroup_header(reader, 0x38)

        with pytest.raises(MoqtProtocolError):
            await collect(read_subgroup_objects(reader, header))

    async def test_raises_when_delta_decoded_object_ids_overflow_the_varint_range(self) -> None:
        data = encode_subgroup_stream(
            stream_type=0x38,
            track_alias=1,
            group_id=7,
            objects=[
                {"object_id_delta": 2**64 - 1, "payload": utf8_encode("a")},
                {"object_id_delta": 0, "payload": utf8_encode("b")},  # 2^64-1 + 0 + 1 overflows
            ],
        )
        reader = stream_of(data)

        await reader.read_varint()
        header = await read_subgroup_header(reader, 0x38)

        with pytest.raises(MoqtProtocolError):
            await collect(read_subgroup_objects(reader, header))

    # The declared lengths below are followed by NO bytes: reaching the read would surface an
    # IncompleteInputError from the stream, so a MoqtProtocolError proves the declaration is rejected
    # before anything is allocated.
    async def test_rejects_a_payload_length_beyond_the_limit_before_allocating(self) -> None:
        writer = ByteWriter()
        writer.write_varint(0x38)
        writer.write_varint(1)  # track alias
        writer.write_varint(7)  # group id
        writer.write_varint(0)  # object id delta
        writer.write_varint(MAX_OBJECT_PAYLOAD_LENGTH + 1)
        reader = stream_of(writer.to_bytes())

        await reader.read_varint()
        header = await read_subgroup_header(reader, 0x38)

        with pytest.raises(MoqtProtocolError, match=r"payload length .* exceeds"):
            await collect(read_subgroup_objects(reader, header))

    async def test_rejects_a_properties_length_beyond_the_limit_before_allocating(self) -> None:
        writer = ByteWriter()
        writer.write_varint(0x39)  # 0x38 | PROPERTIES
        writer.write_varint(1)
        writer.write_varint(7)
        writer.write_varint(0)  # object id delta
        writer.write_varint(MAX_OBJECT_PROPERTIES_LENGTH + 1)
        reader = stream_of(writer.to_bytes())

        await reader.read_varint()
        header = await read_subgroup_header(reader, 0x39)

        with pytest.raises(MoqtProtocolError, match=r"properties length .* exceeds"):
            await collect(read_subgroup_objects(reader, header))

    async def test_accepts_a_payload_exactly_at_the_limit(self) -> None:
        data = encode_subgroup_stream(
            stream_type=0x38,
            track_alias=1,
            group_id=7,
            objects=[{"object_id_delta": 0, "payload": bytes(MAX_OBJECT_PAYLOAD_LENGTH)}],
        )
        reader = stream_of(data)

        await reader.read_varint()
        header = await read_subgroup_header(reader, 0x38)
        objects = await collect(read_subgroup_objects(reader, header))

        assert len(objects[0].payload) == MAX_OBJECT_PAYLOAD_LENGTH

    async def test_accepts_a_properties_block_holding_a_maximum_size_kvp_value(self) -> None:
        # One odd-type KVP whose byte value is the largest the KVP codec permits (2^16-1): the
        # aggregate block bound must not reject what the per-value rule allows.
        props = ByteWriter()
        props.write_varint(0x07)
        props.write_varint(0xFFFF)
        props.write_bytes(bytes([1]) * 0xFFFF)
        data = encode_subgroup_stream(
            stream_type=0x39,  # 0x38 | PROPERTIES
            track_alias=1,
            group_id=7,
            objects=[{"object_id_delta": 0, "payload": utf8_encode("a"), "properties": props.to_bytes()}],
        )
        reader = stream_of(data)

        await reader.read_varint()
        header = await read_subgroup_header(reader, 0x39)
        objects = await collect(read_subgroup_objects(reader, header))

        assert len(objects[0].properties) == 1
        assert objects[0].properties[0].type == 0x07
        value = objects[0].properties[0].value
        assert isinstance(value, bytes)
        assert len(value) == 0xFFFF

    async def test_parses_per_object_properties_when_the_header_properties_bit_is_set(self) -> None:
        # Properties KVP: type 0x06 (Timestamp), varint value 90000.
        props = ByteWriter()
        props.write_varint(0x06)
        props.write_varint(90_000)
        data = encode_subgroup_stream(
            stream_type=0x39,  # 0x38 | PROPERTIES
            track_alias=1,
            group_id=7,
            objects=[{"object_id_delta": 0, "payload": utf8_encode("a"), "properties": props.to_bytes()}],
        )
        reader = stream_of(data)

        await reader.read_varint()
        header = await read_subgroup_header(reader, 0x39)
        objects = await collect(read_subgroup_objects(reader, header))

        assert [(p.type, p.value) for p in objects[0].properties] == [(0x06, 90_000)]

    async def test_surfaces_a_reset_mid_object_as_the_transport_error(self) -> None:
        pipe = Pipe()
        reader = StreamReader(pipe)
        head = encode_subgroup_stream(stream_type=0x38, track_alias=1, group_id=7, objects=[])
        writer = ByteWriter()
        writer.write_varint(0)  # object id
        writer.write_varint(10)  # payload length, then only part of the payload
        writer.write_bytes(b"abc")

        async def feed() -> None:
            await pipe.write(head + writer.to_bytes())
            pipe.abort(RuntimeError("dropped"))

        asyncio.get_running_loop().create_task(feed())
        await reader.read_varint()
        header = await read_subgroup_header(reader, 0x38)

        with pytest.raises(RuntimeError, match="dropped"):
            await collect(read_subgroup_objects(reader, header))


class TestReadFetchEntries:
    @staticmethod
    def encode_fetch_stream(build: Callable[[ByteWriter], None]) -> bytes:
        writer = ByteWriter()
        writer.write_varint(0x05)  # FETCH_HEADER
        writer.write_varint(8)  # request id
        build(writer)
        return writer.to_bytes()

    async def test_parses_the_fetch_header_request_id(self) -> None:
        reader = stream_of(self.encode_fetch_stream(lambda w: None))

        await reader.read_varint()
        assert await read_fetch_header(reader) == 8

    async def test_decodes_absolute_first_then_delta_encoded_objects(self) -> None:
        def build(w: ByteWriter) -> None:
            # First object: flags need GROUP_ID_DELTA (0x08) + OBJECT_ID_DELTA (0x04) + PRIORITY (0x10);
            # subgroup mode 0 (subgroup id zero).
            w.write_varint(0x1C)
            w.write_varint(10)  # group id (absolute)
            w.write_varint(0)  # object id (absolute)
            w.write_uint8(100)  # priority
            w.write_varint(1)  # payload length
            w.write_bytes(utf8_encode("a"))
            # Second object: same group, next object id (no fields at all).
            w.write_varint(0x00)
            w.write_varint(1)
            w.write_bytes(utf8_encode("b"))
            # Third object: next group (delta 0 -> +1), object id resets via delta.
            w.write_varint(0x0C)
            w.write_varint(0)  # group delta -> 10 + 0 + 1 = 11
            w.write_varint(0)  # object id absolute within new group
            w.write_varint(1)
            w.write_bytes(utf8_encode("c"))

        reader = stream_of(self.encode_fetch_stream(build), 4)

        await reader.read_varint()
        await read_fetch_header(reader)
        entries = await collect(read_fetch_entries(reader))

        assert [(e.group_id, e.object_id) if isinstance(e, FetchObject) else None for e in entries] == [
            (10, 0),
            (10, 1),
            (11, 0),
        ]

    async def test_rejects_a_payload_length_beyond_the_limit_before_allocating(self) -> None:
        def build(w: ByteWriter) -> None:
            w.write_varint(0x1C)  # GROUP_ID_DELTA | OBJECT_ID_DELTA | PRIORITY
            w.write_varint(10)  # group id
            w.write_varint(0)  # object id
            w.write_uint8(100)  # priority
            w.write_varint(MAX_OBJECT_PAYLOAD_LENGTH + 1)  # no bytes follow

        reader = stream_of(self.encode_fetch_stream(build))

        await reader.read_varint()
        await read_fetch_header(reader)

        with pytest.raises(MoqtProtocolError, match=r"payload length .* exceeds"):
            await collect(read_fetch_entries(reader))

    async def test_yields_end_of_range_markers_for_all_three_statuses(self) -> None:
        def build(w: ByteWriter) -> None:
            w.write_varint(0x8C)  # End of Non-Existent Range
            w.write_varint(4)
            w.write_varint(20)
            w.write_varint(0x10C)  # End of Unknown Range
            w.write_varint(5)
            w.write_varint(0)
            w.write_varint(0x20C)  # End of Timed-Out Range
            w.write_varint(6)
            w.write_varint(2)

        reader = stream_of(self.encode_fetch_stream(build))

        await reader.read_varint()
        await read_fetch_header(reader)
        entries: list[FetchStreamEntry] = await collect(read_fetch_entries(reader))

        assert entries == [
            FetchEndOfRange(status="non-existent", group_id=4, object_id=20),
            FetchEndOfRange(status="unknown", group_id=5, object_id=0),
            FetchEndOfRange(status="timed-out", group_id=6, object_id=2),
        ]

    async def test_rejects_a_first_object_that_references_the_prior_object(self) -> None:
        reader = stream_of(self.encode_fetch_stream(lambda w: w.write_varint(0x00)))

        await reader.read_varint()
        await read_fetch_header(reader)

        with pytest.raises(MoqtProtocolError):
            await collect(read_fetch_entries(reader))

    async def test_rejects_delta_decoded_locations_that_overflow_the_varint_range(self) -> None:
        def build(w: ByteWriter) -> None:
            # First object: absolute group at the varint ceiling.
            w.write_varint(0x1C)
            w.write_varint(2**64 - 1)  # group id (absolute)
            w.write_varint(0)  # object id (absolute)
            w.write_uint8(100)  # priority
            w.write_varint(1)  # payload length
            w.write_bytes(utf8_encode("a"))
            # Second object: group delta 0 -> 2^64-1 + 0 + 1 overflows.
            w.write_varint(0x0C)
            w.write_varint(0)  # group delta
            w.write_varint(0)  # object id
            w.write_varint(1)
            w.write_bytes(utf8_encode("b"))

        reader = stream_of(self.encode_fetch_stream(build))

        await reader.read_varint()
        await read_fetch_header(reader)

        with pytest.raises(MoqtProtocolError):
            await collect(read_fetch_entries(reader))
