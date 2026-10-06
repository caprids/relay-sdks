from typing import Any, cast

import pytest

from moq_core.errors import IncompleteInputError
from moq_core.varint import (
    MAX_VARINT_VALUE,
    decode_varint,
    encode_varint,
    varint_byte_length,
    varint_length_from_first_byte,
)

# Golden vectors from moq-transport draft-22 §8.1, Table 4.
SPEC_VECTORS: list[tuple[bytes, int]] = [
    (bytes([0x25]), 37),
    (bytes([0x80, 0x25]), 37),
    (bytes([0xBB, 0xBD]), 15_293),
    (bytes([0xED, 0x7F, 0x3E, 0x7D]), 226_442_877),
    (bytes([0xFA, 0xA1, 0xA0, 0xE4, 0x03, 0xD8]), 2_893_212_287_960),
    (bytes([0xFC, 0x89, 0x98, 0xAB, 0xC6, 0x6B, 0xC0]), 151_288_809_941_952),
    (bytes([0xFE, 0xFA, 0x31, 0x8F, 0xA8, 0xE3, 0xCA, 0x11]), 70_423_237_261_249_041),
]


class TestDecodeVarint:
    def test_decodes_the_spec_example_encodings(self) -> None:
        for encoded, value in SPEC_VECTORS:
            assert decode_varint(encoded) == (value, len(encoded))

    def test_accepts_non_minimal_encodings(self) -> None:
        assert decode_varint(bytes([0x00]))[0] == 0
        assert decode_varint(bytes([0x80, 0x00]))[0] == 0
        assert decode_varint(bytes([0xC0, 0x00, 0x00]))[0] == 0

    def test_decodes_at_an_offset(self) -> None:
        assert decode_varint(bytes([0xFF, 0xFF, 0x25]), 2) == (37, 1)

    def test_raises_on_truncated_input(self) -> None:
        with pytest.raises(IncompleteInputError):
            decode_varint(b"")
        with pytest.raises(IncompleteInputError):
            decode_varint(bytes([0xBB]))
        with pytest.raises(IncompleteInputError):
            decode_varint(bytes([0xED, 0x7F, 0x3E]))

    def test_decodes_the_full_64_bit_range_exactly(self) -> None:
        # Python integers are exact, so unlike the TypeScript core nothing above 2^53-1 is refused.
        assert decode_varint(bytes([0xFF] * 9)) == (MAX_VARINT_VALUE, 9)
        assert decode_varint(bytes([0xFF, 0, 0x20, 0, 0, 0, 0, 0, 0])) == (2**53, 9)

    def test_accepts_9_byte_encodings_of_small_values(self) -> None:
        assert decode_varint(bytes([0xFF, 0, 0, 0, 0, 0, 0, 0, 1])) == (1, 9)


class TestEncodeVarint:
    def test_round_trips_boundary_values_at_each_length(self) -> None:
        boundaries = [
            0,
            127,
            128,
            16_383,
            16_384,
            2**21 - 1,
            2**21,
            2**28 - 1,
            2**28,
            2**35 - 1,
            2**35,
            2**42 - 1,
            2**42,
            2**49 - 1,
            2**49,
            2**53 - 1,
            2**53,
            2**56 - 1,
            2**56,
            MAX_VARINT_VALUE,
        ]

        for value in boundaries:
            encoded = encode_varint(value)
            assert len(encoded) == varint_byte_length(value)
            assert decode_varint(encoded) == (value, len(encoded))

    def test_produces_minimal_encodings_matching_the_spec_vectors(self) -> None:
        assert encode_varint(37) == bytes([0x25])
        assert encode_varint(15_293) == bytes([0xBB, 0xBD])
        assert encode_varint(226_442_877) == bytes([0xED, 0x7F, 0x3E, 0x7D])
        assert encode_varint(2_893_212_287_960) == bytes([0xFA, 0xA1, 0xA0, 0xE4, 0x03, 0xD8])

    def test_rejects_negative_and_out_of_range_values(self) -> None:
        with pytest.raises(ValueError, match="out of range"):
            encode_varint(-1)
        with pytest.raises(ValueError, match="out of range"):
            encode_varint(MAX_VARINT_VALUE + 1)
        with pytest.raises(ValueError, match="out of range"):
            encode_varint(cast(Any, 1.5))
        with pytest.raises(ValueError, match="out of range"):
            encode_varint(True)


class TestVarintLengthFromFirstByte:
    def test_maps_prefixes_to_lengths(self) -> None:
        assert varint_length_from_first_byte(0x00) == 1
        assert varint_length_from_first_byte(0x7F) == 1
        assert varint_length_from_first_byte(0x80) == 2
        assert varint_length_from_first_byte(0xBF) == 2
        assert varint_length_from_first_byte(0xC0) == 3
        assert varint_length_from_first_byte(0xE0) == 4
        assert varint_length_from_first_byte(0xF0) == 5
        assert varint_length_from_first_byte(0xF8) == 6
        assert varint_length_from_first_byte(0xFC) == 7
        assert varint_length_from_first_byte(0xFE) == 8
        assert varint_length_from_first_byte(0xFF) == 9
