"""MOQT unidirectional data-stream parsing (moq-transport draft-20 §3.4, §11).

Every unidirectional stream begins with a varint stream type:

- ``SUBGROUP_HEADER`` (a Type Flags bitfield with bit 4 set, §11.4.2): objects for one subgroup of
  one group of a subscribed track, identified by Track Alias.
- ``FETCH_HEADER`` (0x05): objects for a FETCH response, identified by Request ID. A fill fetch stream
  (§5.1.3) has the same shape and names the SUBSCRIBE or REQUEST_UPDATE that asked for it.
- ``SETUP`` (0x2F00): the peer's control stream (handled by the session).
- ``PADDING`` (0x132B3E28): bandwidth probing; drained and ignored.

The parsers are async generators over a :class:`~moq_core.bytes.StreamReader`.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from enum import IntEnum
from typing import ClassVar, Literal, TypeAlias

from .bytes import ByteReader, StreamReader
from .control_messages import GroupOrder, KeyValuePair, decode_key_value_pairs
from .errors import MoqtProtocolError
from .varint import MAX_VARINT_VALUE

# ============================================================================
# Stream types
# ============================================================================


class STREAM_TYPE(IntEnum):
    FETCH_HEADER = 0x05
    SETUP = 0x2F00
    PADDING = 0x132B3E28


def is_subgroup_header_type(stream_type: int) -> bool:
    """Whether a stream-type varint is a SUBGROUP_HEADER.

    §11.4.2 describes the type as Type Flags: bit 4 is always set, SUBGROUP_ID_MODE 0b11 is reserved,
    and a value of 128 or more, or any set bit without a specified meaning, is a PROTOCOL_VIOLATION.
    Every bit below 0x80 is assigned, so "bit 7 clear" is that last rule in full.
    """
    if stream_type > 0x7F or (stream_type & 0x10) == 0:
        return False

    return (stream_type & 0x06) >> 1 != 0b11


# ============================================================================
# Object model
# ============================================================================

MAX_OBJECT_PAYLOAD_LENGTH = 16 * 1024 * 1024
"""Ceiling on a single object's payload.

Unlike control messages, bounded by their 16-bit frame length, a data stream's payload length is an
unbounded varint that feeds straight into ``StreamReader.read_bytes``, which buffers until
satisfied. A malformed or hostile relay declaring 2^53 would exhaust memory before the read ever
completed, so the declaration is rejected before any allocation. 16 MiB clears any real LOC frame.
"""

MAX_OBJECT_PROPERTIES_LENGTH = 1024 * 1024
"""Ceiling on an object's Properties block.

Key-Value-Pairs whose individual byte values may legally reach 2^16-1, so the bound leaves room for
several maximum-size values, not just typical frame metadata.
"""


def _check_payload_length(length: int) -> int:
    if length > MAX_OBJECT_PAYLOAD_LENGTH:
        raise MoqtProtocolError(f"object payload length {length} exceeds {MAX_OBJECT_PAYLOAD_LENGTH} bytes")

    return length


ObjectStatus: TypeAlias = Literal["normal", "end-of-group", "end-of-track"]

_OBJECT_STATUS_WIRE: dict[int, ObjectStatus] = {0: "normal", 3: "end-of-group", 4: "end-of-track"}


@dataclass(frozen=True, slots=True, kw_only=True)
class MoqtObject:
    """An object delivered on a subscription's subgroup stream."""

    group_id: int
    object_id: int
    subgroup_id: int
    priority: int | None = None
    """Publisher priority; ``None`` inherits the subscription's priority."""
    status: ObjectStatus = "normal"
    properties: list[KeyValuePair] = field(default_factory=list)
    payload: bytes = b""


# ============================================================================
# Subgroup streams (§11.4.2)
# ============================================================================

_SUBGROUP_FLAG_PROPERTIES = 0x01
_SUBGROUP_FLAG_SUBGROUP_ID_MODE_MASK = 0x06
_SUBGROUP_FLAG_END_OF_GROUP = 0x08
_SUBGROUP_FLAG_DEFAULT_PRIORITY = 0x20
_SUBGROUP_FLAG_FIRST_OBJECT = 0x40

SubgroupIdMode: TypeAlias = Literal["zero", "first-object-id", "explicit"]


@dataclass(frozen=True, slots=True, kw_only=True)
class SubgroupHeader:
    type: int
    track_alias: int
    group_id: int
    subgroup_id_mode: SubgroupIdMode
    subgroup_id: int | None = None
    """Present when ``subgroup_id_mode`` is ``"explicit"``."""
    priority: int | None = None
    """Present when the header carries an explicit priority."""
    has_properties: bool
    """All objects in this subgroup carry a Properties field."""
    end_of_group: bool
    """This subgroup contains the largest object in the group (on FIN)."""
    first_object: bool
    """The first object on this stream is the subgroup's first published object."""


async def read_subgroup_header(reader: StreamReader, stream_type: int) -> SubgroupHeader:
    """Read the SUBGROUP_HEADER fields (the stream-type varint must already be consumed)."""
    if not is_subgroup_header_type(stream_type):
        raise MoqtProtocolError(f"invalid subgroup header type 0x{stream_type:x}")

    mode_bits = (stream_type & _SUBGROUP_FLAG_SUBGROUP_ID_MODE_MASK) >> 1
    subgroup_id_mode: SubgroupIdMode = (
        "zero" if mode_bits == 0b00 else "first-object-id" if mode_bits == 0b01 else "explicit"
    )

    track_alias = await reader.read_varint()
    group_id = await reader.read_varint()
    subgroup_id = await reader.read_varint() if subgroup_id_mode == "explicit" else None
    priority = await reader.read_uint8() if (stream_type & _SUBGROUP_FLAG_DEFAULT_PRIORITY) == 0 else None

    return SubgroupHeader(
        type=stream_type,
        track_alias=track_alias,
        group_id=group_id,
        subgroup_id_mode=subgroup_id_mode,
        subgroup_id=subgroup_id,
        priority=priority,
        has_properties=(stream_type & _SUBGROUP_FLAG_PROPERTIES) != 0,
        end_of_group=(stream_type & _SUBGROUP_FLAG_END_OF_GROUP) != 0,
        first_object=(stream_type & _SUBGROUP_FLAG_FIRST_OBJECT) != 0,
    )


async def _read_object_properties(reader: StreamReader) -> list[KeyValuePair]:
    """Object Properties structure (§11.2.1.2): varint length + Key-Value-Pairs."""
    length = await reader.read_varint()
    if length == 0:
        return []

    if length > MAX_OBJECT_PROPERTIES_LENGTH:
        raise MoqtProtocolError(f"object properties length {length} exceeds {MAX_OBJECT_PROPERTIES_LENGTH} bytes")

    data = await reader.read_bytes(length)
    return decode_key_value_pairs(ByteReader(data), length)


def _read_object_status(status_wire: int) -> ObjectStatus:
    status = _OBJECT_STATUS_WIRE.get(status_wire)
    if status is None:
        raise MoqtProtocolError(f"unknown object status {status_wire}")

    return status


async def read_subgroup_objects(reader: StreamReader, header: SubgroupHeader) -> AsyncGenerator[MoqtObject, None]:
    """Yield the objects of one subgroup stream until FIN.

    Object IDs are delta-decoded (``delta + 1`` from the previous object; absolute for the first).
    The stream's ``subgroup_id`` resolves per the header's mode; for ``"first-object-id"`` it is the
    first object's ID.
    """
    previous_object_id: int | None = None
    subgroup_id = header.subgroup_id if header.subgroup_id_mode == "explicit" and header.subgroup_id is not None else 0

    while not await reader.at_end():
        object_id_delta = await reader.read_varint()

        object_id = object_id_delta if previous_object_id is None else previous_object_id + object_id_delta + 1
        # Each operand fits the varint range, but the delta sum can pass 2^64-1: the exact
        # corruption the varint layer rejects loudly.
        if object_id > MAX_VARINT_VALUE:
            raise MoqtProtocolError("subgroup object ID exceeds supported range")

        if previous_object_id is None and header.subgroup_id_mode == "first-object-id":
            subgroup_id = object_id

        previous_object_id = object_id

        properties = await _read_object_properties(reader) if header.has_properties else []
        payload_length = _check_payload_length(await reader.read_varint())
        status: ObjectStatus = _read_object_status(await reader.read_varint()) if payload_length == 0 else "normal"
        payload = await reader.read_bytes(payload_length) if payload_length > 0 else b""

        yield MoqtObject(
            group_id=header.group_id,
            object_id=object_id,
            subgroup_id=subgroup_id,
            priority=header.priority,
            status=status,
            properties=properties,
            payload=payload,
        )


# ============================================================================
# Fetch streams (§11.4.4)
# ============================================================================

_FETCH_FLAG_SUBGROUP_MODE_MASK = 0x03
_FETCH_FLAG_OBJECT_ID_DELTA_PRESENT = 0x04
_FETCH_FLAG_GROUP_ID_DELTA_PRESENT = 0x08
_FETCH_FLAG_PRIORITY_PRESENT = 0x10
_FETCH_FLAG_PROPERTIES_PRESENT = 0x20
_FETCH_FLAG_DATAGRAM = 0x40

FetchEndOfRangeStatus: TypeAlias = Literal["non-existent", "unknown", "timed-out"]
"""Why a run of Locations was not serialized: they do not exist, their status is unknown to the relay,
or the relay's Fill Timeout expired before upstream supplied them (§10.2.5)."""

# End of Range serialization flags (§11.4.4, Table 7).
_FETCH_END_OF_RANGE_STATUS: dict[int, FetchEndOfRangeStatus] = {
    0x8C: "non-existent",
    0x10C: "unknown",
    0x20C: "timed-out",
}


@dataclass(frozen=True, slots=True, kw_only=True)
class FetchObject:
    kind: ClassVar[Literal["object"]] = "object"
    group_id: int
    object_id: int
    subgroup_id: int | None = None
    """Absent for objects with Forwarding Preference = Datagram."""
    priority: int
    properties: list[KeyValuePair] = field(default_factory=list)
    payload: bytes = b""


@dataclass(frozen=True, slots=True, kw_only=True)
class FetchEndOfRange:
    kind: ClassVar[Literal["end-of-range"]] = "end-of-range"
    status: FetchEndOfRangeStatus
    group_id: int
    object_id: int


FetchStreamEntry: TypeAlias = FetchObject | FetchEndOfRange


async def read_fetch_header(reader: StreamReader) -> int:
    """Read the FETCH_HEADER's Request ID (the stream-type varint must already be consumed)."""
    return await reader.read_varint()


@dataclass(slots=True)
class _PriorLocation:
    group_id: int
    object_id: int
    subgroup_id: int | None
    priority: int | None


async def read_fetch_entries(
    reader: StreamReader, group_order: GroupOrder = "ascending"
) -> AsyncGenerator[FetchStreamEntry, None]:
    """Yield the entries of one FETCH response stream until FIN.

    Group/object IDs are delta-decoded against the prior entry per §11.4.4.1; the first object must
    carry both deltas (as absolute values).
    """
    prior: _PriorLocation | None = None

    while not await reader.at_end():
        flags = await reader.read_varint()
        end_of_range = _FETCH_END_OF_RANGE_STATUS.get(flags)

        if end_of_range is not None:
            group_id = await reader.read_varint()
            object_id = await reader.read_varint()

            # The indicator becomes the prior Location; subgroup/priority carry over from the last
            # actual object (§11.4.4.2).
            prior = _PriorLocation(
                group_id,
                object_id,
                prior.subgroup_id if prior else None,
                prior.priority if prior else None,
            )
            yield FetchEndOfRange(status=end_of_range, group_id=group_id, object_id=object_id)
            continue

        if flags > 0x7F:
            raise MoqtProtocolError(f"invalid fetch serialization flags 0x{flags:x}")

        group_id_delta_present = (flags & _FETCH_FLAG_GROUP_ID_DELTA_PRESENT) != 0
        object_id_delta_present = (flags & _FETCH_FLAG_OBJECT_ID_DELTA_PRESENT) != 0

        if prior is None and (not group_id_delta_present or not object_id_delta_present):
            raise MoqtProtocolError("first fetch object must carry group and object IDs")

        group_id_delta = await reader.read_varint() if group_id_delta_present else None

        # Subgroup encoding: 2 LSBs, ignored when the DATAGRAM bit is set.
        is_datagram = (flags & _FETCH_FLAG_DATAGRAM) != 0
        subgroup_mode = -1 if is_datagram else flags & _FETCH_FLAG_SUBGROUP_MODE_MASK
        subgroup_id: int | None = None

        if subgroup_mode == 0x03:
            subgroup_id = await reader.read_varint()
        elif subgroup_mode == 0x00:
            subgroup_id = 0
        elif subgroup_mode in (0x01, 0x02):
            if prior is None or prior.subgroup_id is None:
                raise MoqtProtocolError("fetch object references prior subgroup ID with no prior object")

            subgroup_id = prior.subgroup_id if subgroup_mode == 0x01 else prior.subgroup_id + 1

        object_id_delta = await reader.read_varint() if object_id_delta_present else None

        if group_id_delta is None:
            assert prior is not None
            group_id = prior.group_id
        elif prior is None:
            group_id = group_id_delta
        elif group_order == "ascending":
            group_id = prior.group_id + group_id_delta + 1
        else:
            group_id = prior.group_id - (group_id_delta + 1)

        if group_id < 0:
            raise MoqtProtocolError("fetch group ID underflow")

        if prior is None:
            assert object_id_delta is not None
            object_id = object_id_delta
        elif group_id_delta is not None:
            object_id = object_id_delta if object_id_delta is not None else prior.object_id + 1
        elif object_id_delta is not None:
            object_id = prior.object_id + object_id_delta
        else:
            object_id = prior.object_id + 1

        # Mirror of the underflow check above: delta sums can pass 2^64-1 and silently collapse
        # later locations.
        if group_id > MAX_VARINT_VALUE or object_id > MAX_VARINT_VALUE:
            raise MoqtProtocolError("fetch location exceeds supported range")

        if (flags & _FETCH_FLAG_PRIORITY_PRESENT) != 0:
            priority = await reader.read_uint8()
        else:
            if prior is None or prior.priority is None:
                raise MoqtProtocolError("fetch object references prior priority with no prior object")

            priority = prior.priority

        properties = await _read_object_properties(reader) if (flags & _FETCH_FLAG_PROPERTIES_PRESENT) != 0 else []
        payload_length = _check_payload_length(await reader.read_varint())
        payload = await reader.read_bytes(payload_length) if payload_length > 0 else b""

        prior = _PriorLocation(
            group_id,
            object_id,
            subgroup_id if subgroup_id is not None else (prior.subgroup_id if prior else None),
            priority,
        )
        yield FetchObject(
            group_id=group_id,
            object_id=object_id,
            subgroup_id=subgroup_id,
            priority=priority,
            properties=properties,
            payload=payload,
        )


__all__ = [
    "MAX_OBJECT_PAYLOAD_LENGTH",
    "MAX_OBJECT_PROPERTIES_LENGTH",
    "STREAM_TYPE",
    "FetchEndOfRange",
    "FetchEndOfRangeStatus",
    "FetchObject",
    "FetchStreamEntry",
    "MoqtObject",
    "ObjectStatus",
    "SubgroupHeader",
    "SubgroupIdMode",
    "is_subgroup_header_type",
    "read_fetch_entries",
    "read_fetch_header",
    "read_subgroup_header",
    "read_subgroup_objects",
]
