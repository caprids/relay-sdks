"""Language-neutral encoding vectors shared by every SDK; see ../../../spec/vectors/README.md."""

import json
from pathlib import Path
from typing import Any

import pytest

from moq_core.bytes import ByteReader, ByteWriter
from moq_core.control_messages import (
    MOQT_PROTOCOL_IDS,
    AbsoluteLocationFilter,
    Location,
    LocationFilter,
    MessageParameters,
    MoqtProtocolId,
    NextObjectFilter,
    NoLocationFilter,
    RelativeGroupFilter,
    decode_message_parameters,
    encode_message_parameters,
)
from moq_core.errors import IncompleteInputError, MoqtProtocolError
from moq_core.varint import decode_varint, encode_varint

REPOSITORY_ROOT = Path(__file__).resolve().parents[4]
VECTORS = json.loads((REPOSITORY_ROOT / "spec" / "vectors" / "varint.json").read_text())


def test_encodes_every_minimal_vector_to_its_bytes_and_decodes_it_back() -> None:
    for vector in VECTORS["minimal"]:
        value, encoded = int(vector["value"]), bytes.fromhex(vector["bytes"])
        assert encode_varint(value) == encoded
        assert decode_varint(encoded) == (value, len(encoded))


def test_decodes_non_minimal_encodings_without_ever_producing_them() -> None:
    for vector in VECTORS["nonMinimal"]:
        value, encoded = int(vector["value"]), bytes.fromhex(vector["bytes"])
        assert decode_varint(encoded)[0] == value
        assert encode_varint(value) != encoded


def test_carries_values_beyond_53_bits_exactly() -> None:
    # These vectors exist to exercise implementations without exact 64-bit integers. Python has
    # them, so the full wire range decodes exactly and round-trips through the encoder instead of
    # being refused. (The vectors are decode vectors: 2^53's 9-byte spelling is not minimal.)
    for vector in VECTORS["beyond53Bits"]:
        value, encoded = int(vector["value"]), bytes.fromhex(vector["bytes"])
        assert decode_varint(encoded) == (value, len(encoded))
        assert decode_varint(encode_varint(value))[0] == value


LOCATION_FILTERS = json.loads((REPOSITORY_ROOT / "spec" / "vectors" / "location-filter.json").read_text())


def _to_filter(vector: dict[str, Any]) -> LocationFilter:
    """The vector's decimal strings as the core's filter classes."""
    match vector["type"]:
        case "none":
            return NoLocationFilter()
        case "next-object":
            return NextObjectFilter()
        case "relative-group":
            return RelativeGroupFilter(groups_before_next=int(vector["groupsBeforeNext"]))
        case _:
            end_group_delta = vector.get("endGroupDelta")
            end_object = vector.get("endObject")
            return AbsoluteLocationFilter(
                start=Location(int(vector["start"]["group"]), int(vector["start"]["object"])),
                end_group_delta=None if end_group_delta is None else int(end_group_delta),
                end_object=None if end_object is None else int(end_object),
            )


def _encode_filter_parameter(location_filter: LocationFilter, protocol: MoqtProtocolId) -> str:
    writer = ByteWriter()
    encode_message_parameters(writer, MessageParameters(location_filter=location_filter), protocol)
    return writer.to_bytes().hex()


def _decode_filter_parameter(data: str, protocol: MoqtProtocolId) -> LocationFilter | None:
    reader = ByteReader(bytes.fromhex(data))
    parameters = decode_message_parameters(reader, protocol)
    assert reader.remaining == 0
    return parameters.location_filter


@pytest.mark.parametrize("protocol", MOQT_PROTOCOL_IDS)
def test_encodes_every_location_filter_to_its_bytes_and_decodes_it_back(protocol: MoqtProtocolId) -> None:
    for vector in LOCATION_FILTERS["filters"]:
        location_filter = _to_filter(vector["filter"])
        assert _encode_filter_parameter(location_filter, protocol) == vector[protocol]
        assert _decode_filter_parameter(vector[protocol], protocol) == location_filter


def test_encodes_an_open_ended_absolute_zero_as_no_filter_on_draft_20_only() -> None:
    for vector in LOCATION_FILTERS["encodeOnly"]:
        location_filter = _to_filter(vector["filter"])
        assert _encode_filter_parameter(location_filter, "moqt-22") == vector["moqt-22"]
        assert _decode_filter_parameter(vector["moqt-22"], "moqt-22") == location_filter
        assert _encode_filter_parameter(location_filter, "moqt-20") == vector["moqt-20"]
        assert _decode_filter_parameter(vector["moqt-20"], "moqt-20") == _to_filter(vector["moqt-20 decodes as"])


def test_rejects_every_rejected_location_filter() -> None:
    for vector in LOCATION_FILTERS["rejected"]:
        with pytest.raises((MoqtProtocolError, IncompleteInputError)):
            decode_message_parameters(ByteReader(bytes.fromhex(vector["bytes"])), vector["protocol"])
