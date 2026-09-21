"""Opt-in text and JSON encoding and decoding, and the argument checks a write performs before it takes the slot."""

import json
from collections.abc import Sequence
from typing import Any

from moq_core import KeyValuePair, MoqtObject
from moq_core.varint import MAX_VARINT_VALUE


def _encode_text(text: str) -> bytes:
    if not isinstance(text, str):
        raise TypeError("Text must be a string")
    return text.encode("utf-8")


def _encode_json(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")


def _copy_properties(properties: Sequence[KeyValuePair] | None) -> list[KeyValuePair]:
    """Validate track or object properties and copy any byte values, so later mutation cannot leak in."""
    copied: list[KeyValuePair] = []
    for index, prop in enumerate(properties or ()):
        if not isinstance(prop.type, int) or isinstance(prop.type, bool) or not 0 <= prop.type <= MAX_VARINT_VALUE:
            raise TypeError(f"Property {index} type must be a non-negative 64-bit integer")
        if prop.type % 2 == 0:
            if (
                not isinstance(prop.value, int)
                or isinstance(prop.value, bool)
                or not 0 <= prop.value <= MAX_VARINT_VALUE
            ):
                raise TypeError(f"Property {index} value must be a non-negative 64-bit integer for an even type")
            value = prop.value
        else:
            if isinstance(prop.value, int):
                raise TypeError(f"Property {index} value must be bytes-like for an odd type")
            try:
                value = memoryview(prop.value).tobytes()
            except TypeError as error:
                raise TypeError(f"Property {index} value must be bytes-like for an odd type") from error
        copied.append(KeyValuePair(prop.type, value))
    return copied


def read_text(obj: MoqtObject) -> str:
    """Decode one complete object as UTF-8; malformed bytes raise UnicodeDecodeError."""
    return obj.payload.decode("utf-8")


def _reject_constant(value: str) -> Any:
    raise ValueError(f"Invalid JSON constant: {value}")


def read_json(obj: MoqtObject) -> Any:
    """Parse one UTF-8 JSON payload without schema validation; invalid input raises ValueError."""
    return json.loads(read_text(obj), parse_constant=_reject_constant)
