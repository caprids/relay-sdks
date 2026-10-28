"""``room_endpoint``: the cases come from ``spec/vectors/room-endpoint.json`` and run in every SDK, per
"Room-rooted endpoints" in ``spec/behavior.md``; the local cases hold what JSON cannot carry.
"""

import json
from pathlib import Path
from typing import Any

import pytest

from caprid_relay import DEFAULT_ENDPOINT, room_endpoint

REPO = Path(__file__).resolve().parents[4]
VECTORS = json.loads((REPO / "spec" / "vectors" / "room-endpoint.json").read_text())
PROTOCOL = json.loads((REPO / "spec" / "protocol.json").read_text())
ERROR_CLASS: dict[str, type[Exception]] = {"type": TypeError, "range": ValueError}
Case = dict[str, Any]


def case_id(case: Case) -> str:
    return case["name"]


@pytest.mark.parametrize("case", [case for case in VECTORS["cases"] if "error" not in case], ids=case_id)
def test_vector_builds_the_expected_url(case: Case) -> None:
    assert room_endpoint(case["endpoint"], case["room"]) == case["expected"]


@pytest.mark.parametrize("case", [case for case in VECTORS["cases"] if "error" in case], ids=case_id)
def test_vector_is_refused_with_the_named_error(case: Case) -> None:
    with pytest.raises(ERROR_CLASS[case["error"]]):
        room_endpoint(case["endpoint"], case["room"])


def test_names_the_refused_field_and_points_at_the_bare_host() -> None:
    with pytest.raises(ValueError, match=r"Room field 1 \('a b'\).*bare host"):
        room_endpoint("https://relay.example", ["acme", "a b"])
    with pytest.raises(ValueError, match=r"Room field 0 is '\.\.'"):
        room_endpoint("https://relay.example", [".."])


def test_admits_a_tuple() -> None:
    assert room_endpoint("https://relay.example", ("acme", "rooms")) == "https://relay.example/acme/rooms"


def test_refuses_a_generator_as_a_type_error() -> None:
    with pytest.raises(TypeError):
        room_endpoint("https://relay.example", (field for field in ["acme"]))  # ty: ignore[invalid-argument-type]


def test_roots_the_default_endpoint_when_given_only_a_room() -> None:
    assert room_endpoint(["acme", "rooms", "42"]) == "https://relay.mux.global/acme/rooms/42"
    assert room_endpoint(["acme"]) == room_endpoint(DEFAULT_ENDPOINT, ["acme"])


def test_checks_a_room_given_alone_as_it_checks_one_given_with_an_endpoint() -> None:
    with pytest.raises(ValueError, match=r"Room field 1"):
        room_endpoint(["acme", "a b"])
    with pytest.raises(TypeError):
        room_endpoint([])
    with pytest.raises(TypeError):
        room_endpoint("https://relay.example")  # a lone string is a string room, not an endpoint


def test_refuses_more_than_two_arguments() -> None:
    with pytest.raises(TypeError):
        room_endpoint("https://relay.example", ["acme"], ["rooms"])  # ty: ignore[no-matching-overload]


def test_default_endpoint_is_the_one_spec_protocol_pins() -> None:
    assert PROTOCOL["relay"]["defaultEndpoint"] == DEFAULT_ENDPOINT
