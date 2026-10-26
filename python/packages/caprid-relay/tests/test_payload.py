"""Opt-in readers reject malformed payloads without modifying received objects."""

import json

import pytest

from caprid_relay import MoqtObject, read_json, read_text


def received(payload: bytes) -> MoqtObject:
    return MoqtObject(group_id=0, object_id=0, subgroup_id=0, payload=payload)


def test_text_preserves_bom_and_payload() -> None:
    obj = received("\ufeffhello".encode())
    assert read_text(obj) == "\ufeffhello"
    assert obj.payload == "\ufeffhello".encode()
    assert read_text(received(b"")) == ""


@pytest.mark.parametrize("payload", [b"\xff", b"\xc3", b"\xed\xa0\x80"])
def test_malformed_utf8(payload: bytes) -> None:
    for reader in (read_text, read_json):
        with pytest.raises(UnicodeDecodeError):
            reader(received(payload))


@pytest.mark.parametrize("text", ["", "{", "undefined", "NaN", "Infinity", '{"x":1,}', "\ufeffnull"])
def test_malformed_json(text: str) -> None:
    with pytest.raises(ValueError):
        read_json(received(text.encode()))


@pytest.mark.parametrize("value", [None, False, 0, "", [1, 2], {"text": "世界 🌍"}])
def test_json_values(value: object) -> None:
    assert read_json(received(json.dumps(value).encode())) == value
