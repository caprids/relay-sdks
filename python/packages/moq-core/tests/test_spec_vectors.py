"""Language-neutral encoding vectors shared by every SDK; see ../../../spec/vectors/README.md."""

import json
from pathlib import Path

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
