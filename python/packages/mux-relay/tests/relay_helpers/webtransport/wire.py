"""Wire constants and codecs: capsule types, the reserved HTTP/3 error-code range, structured-field lists."""

from __future__ import annotations

from collections.abc import Sequence

from mux_relay.quic._errors import _from_h3_error_code, _to_h3_error_code

CAPSULE_CLOSE_WEBTRANSPORT_SESSION = 0x2843
CAPSULE_DRAIN_WEBTRANSPORT_SESSION = 0x78AE

webtransport_to_h3_error_code = _to_h3_error_code
"""Map a 32-bit WebTransport application error code onto the reserved HTTP/3 range."""

h3_to_webtransport_error_code = _from_h3_error_code
"""Recover the application error code from an HTTP/3 stream error, or ``None`` when it is not one."""


def _encode_uint_var(value: int) -> bytes:
    """RFC 9000 variable-length integer (HTTP/3 framing, distinct from MOQT's varint)."""
    if value < 0x40:
        return value.to_bytes(1, "big")
    if value < 0x4000:
        return (value | 0x4000).to_bytes(2, "big")
    if value < 0x40000000:
        return (value | 0x80000000).to_bytes(4, "big")
    return (value | 0xC000000000000000).to_bytes(8, "big")


def _decode_uint_var(data: bytes, offset: int) -> tuple[int, int] | None:
    if offset >= len(data):
        return None

    length = 1 << (data[offset] >> 6)

    if offset + length > len(data):
        return None

    value = int.from_bytes(data[offset : offset + length], "big") & ((1 << (8 * length - 2)) - 1)
    return value, offset + length


def encode_close_session_capsule(code: int, reason: str) -> bytes:
    payload = code.to_bytes(4, "big") + reason.encode("utf-8")[:1024]
    return _encode_uint_var(CAPSULE_CLOSE_WEBTRANSPORT_SESSION) + _encode_uint_var(len(payload)) + payload


def sf_list_of_strings(items: Sequence[str]) -> bytes:
    """Serialize an RFC 9651 list of strings, the ``WT-Available-Protocols`` value shape."""
    return ", ".join('"' + item.replace("\\", "\\\\").replace('"', '\\"') + '"' for item in items).encode("ascii")


def parse_sf_string(value: bytes) -> str:
    """Parse one RFC 9651 string item, the ``WT-Protocol`` value shape. Tolerates a bare token."""
    text = value.decode("ascii", errors="replace").strip()

    if len(text) >= 2 and text[0] == '"' and text[-1] == '"':
        return text[1:-1].replace('\\"', '"').replace("\\\\", "\\")

    return text


def parse_sf_list_of_strings(value: bytes) -> list[str]:
    return [
        parse_sf_string(item.encode("ascii")) for item in value.decode("ascii", errors="replace").split(",") if item
    ]
