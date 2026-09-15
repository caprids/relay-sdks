"""MOQT subgroup data-stream writing (moq-transport draft-20 §11.4.2).

The publish-direction complement to the reader in ``object_stream``.

One writer owns one unidirectional stream carrying one subgroup of one group: the SUBGROUP_HEADER
goes out first, objects follow serially with delta-encoded object IDs, and :meth:`SubgroupWriter.fin`
closes the stream to mark the group complete. :meth:`SubgroupWriter.abort` resets the stream instead:
the drop path for a publisher falling behind transport backpressure.

The shape this SDK publishes is one subgroup per group at subgroup ID 0, so the writer pins
SUBGROUP_ID_MODE to ``0b00`` (no explicit subgroup-id field) and sets END_OF_GROUP: on FIN the subgroup
contains the group's largest object by construction.

Everything written here round-trips through ``read_subgroup_header`` + ``read_subgroup_objects``;
that pairing is the module's contract and its test suite.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

from ._tasks import observe, spawn
from .bytes import ByteWriter
from .control_messages import KeyValuePair, encode_key_value_pairs
from .errors import MoqtProtocolError
from .object_stream import MAX_OBJECT_PAYLOAD_LENGTH, MAX_OBJECT_PROPERTIES_LENGTH, ObjectStatus
from .transport import SendStream

_SUBGROUP_TYPE_BASE = 0x10
_SUBGROUP_TYPE_PROPERTIES = 0x01
_SUBGROUP_TYPE_END_OF_GROUP = 0x08
_SUBGROUP_TYPE_DEFAULT_PRIORITY = 0x20
_SUBGROUP_TYPE_FIRST_OBJECT = 0x40

_OBJECT_STATUS_WIRE: dict[str, int] = {"normal": 0, "end-of-group": 3, "end-of-track": 4}


def _encode_subgroup_header(
    track_alias: int, group_id: int, priority: int | None, has_properties: bool, end_of_group: bool
) -> bytes:
    stream_type = (
        _SUBGROUP_TYPE_BASE
        | _SUBGROUP_TYPE_FIRST_OBJECT
        | (_SUBGROUP_TYPE_PROPERTIES if has_properties else 0)
        | (_SUBGROUP_TYPE_END_OF_GROUP if end_of_group else 0)
        | (_SUBGROUP_TYPE_DEFAULT_PRIORITY if priority is None else 0)
    )

    writer = ByteWriter()
    writer.write_varint(stream_type)
    writer.write_varint(track_alias)
    writer.write_varint(group_id)

    if priority is not None:
        writer.write_uint8(priority)

    return writer.to_bytes()


def _encode_object(
    object_id: int,
    payload: bytes,
    properties: Sequence[KeyValuePair] | None,
    status: ObjectStatus,
    previous_object_id: int | None,
    has_properties: bool,
) -> bytes:
    if previous_object_id is not None and object_id <= previous_object_id:
        raise MoqtProtocolError(f"subgroup object ID {object_id} does not increase past {previous_object_id}")

    if len(payload) > MAX_OBJECT_PAYLOAD_LENGTH:
        raise MoqtProtocolError(f"object payload length {len(payload)} exceeds {MAX_OBJECT_PAYLOAD_LENGTH} bytes")

    writer = ByteWriter()

    # First object carries its absolute ID; the rest are `delta + 1` (§11.4.2).
    writer.write_varint(object_id if previous_object_id is None else object_id - previous_object_id - 1)

    if has_properties:
        encoded_properties = ByteWriter()
        encode_key_value_pairs(encoded_properties, properties or ())

        if encoded_properties.length > MAX_OBJECT_PROPERTIES_LENGTH:
            raise MoqtProtocolError(
                f"object properties length {encoded_properties.length} exceeds {MAX_OBJECT_PROPERTIES_LENGTH} bytes"
            )

        writer.write_length_prefixed(encoded_properties.to_bytes())
    elif properties:
        raise MoqtProtocolError("object carries properties on a subgroup stream without the PROPERTIES flag")

    writer.write_varint(len(payload))

    if len(payload) == 0:
        writer.write_varint(_OBJECT_STATUS_WIRE[status])
    else:
        writer.write_bytes(payload)

    return writer.to_bytes()


class SubgroupWriter:
    """Writer for one subgroup stream. Create one with :func:`create_subgroup_writer`."""

    __slots__ = (
        "_finished",
        "_finishing",
        "_has_properties",
        "_header_write",
        "_previous_object_id",
        "_stream",
        "group_id",
    )

    def __init__(
        self,
        stream: SendStream,
        *,
        track_alias: int,
        group_id: int,
        priority: int | None,
        has_properties: bool,
        end_of_group: bool,
    ) -> None:
        self.group_id = group_id
        self._stream = stream
        self._has_properties = has_properties
        self._previous_object_id: int | None = None
        self._finished = False
        self._finishing = False
        # Header errors surface on the first awaited write/fin: an errored stream rejects
        # everything queued after it.
        header = _encode_subgroup_header(track_alias, group_id, priority, has_properties, end_of_group)
        self._header_write: asyncio.Task[None] = spawn(stream.write(header))
        observe(self._header_write)

    async def write_object(
        self,
        *,
        object_id: int,
        payload: bytes,
        properties: Sequence[KeyValuePair] | None = None,
        status: ObjectStatus = "normal",
    ) -> None:
        """Serialize one object. Returns when the transport accepted the bytes.

        ``object_id`` is absolute and must increase monotonically within the stream. ``status`` is
        only meaningful for zero-length payloads (§11.2.1.1).
        """
        if self._finished or self._finishing:
            raise MoqtProtocolError("subgroup stream already finished")

        encoded = _encode_object(object_id, payload, properties, status, self._previous_object_id, self._has_properties)
        self._previous_object_id = object_id
        await self._header_write
        await self._stream.write(encoded)

    async def fin(self) -> None:
        """FIN the stream: the group is complete.

        The FIN counts as final only once the transport has taken it. Under real backpressure it can
        stay queued for a long time, and the stream has to remain abortable for as long as it does:
        otherwise a group whose operation was cancelled meanwhile is still delivered complete, and
        the caller waiting here is never released.
        """
        if self._finished or self._finishing:
            return

        self._finishing = True

        try:
            await self._header_write
            await self._stream.close()
        except BaseException as error:
            # The close suspends until the transport takes the FIN, and a caller that gives up here
            # (a deadline, a cancelled task) does not withdraw it. Reset the stream, or the group is
            # delivered complete after the caller was told its close failed.
            self._finishing = False
            self.abort(error)
            raise

        self._finishing = False
        self._finished = True

    def abort(self, reason: object = None) -> None:
        """Reset the stream; delivered objects may be incomplete (drop path).

        A FIN the transport has not taken yet is reset too, and the pending :meth:`fin` raises
        ``reason``: the send half turns a closed-but-untransmitted stream into a RESET_STREAM.
        """
        if self._finished:
            return

        self._finished = True
        self._finishing = False
        self._stream.abort(reason)


def create_subgroup_writer(
    stream: SendStream,
    *,
    track_alias: int,
    group_id: int,
    priority: int | None = None,
    has_properties: bool = True,
    end_of_group: bool = True,
) -> SubgroupWriter:
    """Open a subgroup writer over an established unidirectional stream.

    The SUBGROUP_HEADER is queued immediately; objects follow in ``write_object`` call order.
    ``priority`` omitted sets the DEFAULT_PRIORITY flag so subscribers fall back to the
    subscription's priority. ``has_properties`` makes every object carry a Properties block (an
    empty list still encodes as a zero length, so mixed-metadata tracks need no special casing).
    ``end_of_group`` declares that the subgroup contains the group's largest object on FIN.
    """
    return SubgroupWriter(
        stream,
        track_alias=track_alias,
        group_id=group_id,
        priority=priority,
        has_properties=has_properties,
        end_of_group=end_of_group,
    )


__all__ = ["SubgroupWriter", "create_subgroup_writer"]
