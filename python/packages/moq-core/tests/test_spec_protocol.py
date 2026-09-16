"""The core's constants must match the repository-wide pins in spec/protocol.json."""

import json
from pathlib import Path

from moq_core import MAX_NAMESPACE_FIELDS, MAX_OBJECT_PAYLOAD_LENGTH, MOQT_PROTOCOL_ID

REPOSITORY_ROOT = Path(__file__).resolve().parents[4]
SPEC = json.loads((REPOSITORY_ROOT / "spec" / "protocol.json").read_text())


def test_moqt_protocol_id_matches_spec() -> None:
    assert SPEC["moqt"]["protocolId"] == MOQT_PROTOCOL_ID


def test_wire_identifier_carries_the_pinned_draft_number() -> None:
    # draft-ietf-moq-transport-20 §1.4: the identifier is "moqt-" followed by the draft number.
    draft_number = MOQT_PROTOCOL_ID.removeprefix("moqt-")
    assert SPEC["moqt"]["document"] == f"draft-ietf-moq-transport-{draft_number}"


def test_object_payload_limit_matches_spec() -> None:
    assert SPEC["limits"]["maxObjectPayloadBytes"] == MAX_OBJECT_PAYLOAD_LENGTH


def test_namespace_field_limit_matches_spec() -> None:
    assert SPEC["limits"]["maxNamespaceFields"] == MAX_NAMESPACE_FIELDS
