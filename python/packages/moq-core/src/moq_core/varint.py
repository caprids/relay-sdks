"""MOQT variable-length integers (moq-transport draft-20 §1.4.1).

NOT the QUIC RFC 9000 varint: draft-15 replaced it with a leading-ones scheme. The number of leading 1
bits of the first byte gives the encoded length minus one (1-9 bytes); the bits after the first 0,
plus any subsequent bytes, hold the value in network byte order.

Python integers are exact at any width, so this codec covers the full wire range 0..2^64-1. Encoding
rejects negative or wider values with :class:`ValueError`; decoding a truncated buffer raises
:class:`~moq_core.errors.IncompleteInputError`.
"""

from __future__ import annotations

MAX_VARINT_VALUE = 2**64 - 1
"""Largest value the wire format can carry; ``encode_varint`` accepts nothing above it."""

_LENGTH_LIMITS = (2**7, 2**14, 2**21, 2**28, 2**35, 2**42, 2**49, 2**56)


def varint_length_from_first_byte(first_byte: int) -> int:
    """Encoded length in bytes implied by a varint's first byte."""
    leading_ones = 0
    mask = 0x80

    while leading_ones < 8 and first_byte & mask:
        leading_ones += 1
        mask >>= 1

    return leading_ones + 1


def decode_varint(data: bytes | bytearray | memoryview, offset: int = 0) -> tuple[int, int]:
    """Decode a varint at ``offset`` and return ``(value, byte_length)``.

    Raises :class:`IncompleteInputError` when ``data`` ends before the encoding completes.
    """
    from .errors import IncompleteInputError

    if offset >= len(data):
        raise IncompleteInputError("varint: unexpected end of input")

    first_byte = data[offset]
    byte_length = varint_length_from_first_byte(first_byte)

    if offset + byte_length > len(data):
        raise IncompleteInputError("varint: unexpected end of input")

    # Value bits contributed by the first byte: everything after the leading-ones prefix and its
    # terminating 0. For a 9-byte encoding (prefix 0xFF) the first byte contributes nothing.
    value = first_byte & (0xFF >> byte_length) if byte_length <= 8 else 0

    for index in range(1, byte_length):
        value = (value << 8) | data[offset + index]

    return value, byte_length


def varint_byte_length(value: int) -> int:
    """Minimal encoded length in bytes for ``value``."""
    if not isinstance(value, int) or isinstance(value, bool) or value < 0 or value > MAX_VARINT_VALUE:
        raise ValueError(f"varint: value out of range: {value!r}")

    for length, limit in enumerate(_LENGTH_LIMITS, start=1):
        if value < limit:
            return length

    # The 9-byte form carries a full 64 value bits.
    return 9


def encode_varint(value: int) -> bytes:
    """Encode ``value`` using the minimal length."""
    byte_length = varint_byte_length(value)

    if byte_length == 9:
        return b"\xff" + value.to_bytes(8, "big")

    # Leading-ones prefix for (byte_length - 1) ones, then the value in the remaining bits.
    prefix = 0 if byte_length == 1 else (0xFF << (9 - byte_length)) & 0xFF
    encoded = value.to_bytes(byte_length, "big")

    return bytes((prefix | encoded[0],)) + encoded[1:]


__all__ = [
    "MAX_VARINT_VALUE",
    "decode_varint",
    "encode_varint",
    "varint_byte_length",
    "varint_length_from_first_byte",
]
