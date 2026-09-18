"""Opt-in text and JSON decoding for complete received objects."""

import json
from typing import Any

from moq_core import MoqtObject


def _encode_text(text: str) -> bytes:
    if not isinstance(text, str):
        raise TypeError("Text must be a string")
    return text.encode("utf-8")


def _encode_json(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")


def read_text(obj: MoqtObject) -> str:
    """Decode one complete object as UTF-8; malformed bytes raise UnicodeDecodeError."""
    return obj.payload.decode("utf-8")


def _reject_constant(value: str) -> Any:
    raise ValueError(f"Invalid JSON constant: {value}")


def read_json(obj: MoqtObject) -> Any:
    """Parse one UTF-8 JSON payload without schema validation; invalid input raises ValueError."""
    return json.loads(read_text(obj), parse_constant=_reject_constant)
