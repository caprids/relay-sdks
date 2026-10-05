"""The validation module through its interface, and each entry point's use of it.

Portable cases come from ``spec/vectors/arguments.json`` and run in every SDK; the local tables hold
what JSON cannot carry (tuples, generators, buffers, integer bounds) and mirror
``ts/tests/validation.test.ts`` by name. Rows follow the "Arguments" table in ``spec/behavior.md``.
"""

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from pathlib import Path
from typing import Any

import pytest
from core_helpers.fake_transport import FakeTransport, create_fake_transport
from relay_helpers.relay_hub import create_relay_hub

from moq_core import MAX_OBJECT_PAYLOAD_LENGTH, KeyValuePair, MoqtSubscribeOptions
from moq_core.bytes import ByteWriter
from moq_core.control_messages import Subscribe, encode_key_value_pairs
from mux_relay import (
    CancellationToken,
    ConnectingTransport,
    RelayError,
    RelayPublisher,
    RelaySubscriber,
    connect_relay_publisher,
    connect_relay_subscriber,
)
from mux_relay._validation import (
    MAX_PROPERTY_VALUE_LENGTH,
    MAX_RELAY_OBJECT_PROPERTIES_LENGTH,
    namespace_fields,
    object_property_pairs,
    payload_bytes,
    property_pairs,
    track_name,
)

REPO = Path(__file__).resolve().parents[4]
VECTORS = json.loads((REPO / "spec" / "vectors" / "arguments.json").read_text())
PROTOCOL = json.loads((REPO / "spec" / "protocol.json").read_text())
ERROR_CLASS: dict[str, type[Exception]] = {"type": TypeError, "range": ValueError}
URL = "https://relay.example.com/moq"

Cleanup = list[Callable[[], Awaitable[None] | None]]
Case = dict[str, Any]


def accepted(cases: list[Case]) -> list[Case]:
    return [case for case in cases if "error" not in case]


def rejected(cases: list[Case]) -> list[Case]:
    return [case for case in cases if "error" in case]


def case_id(case: Case) -> str:
    return str(case["name"])


@pytest.fixture
async def cleanup() -> AsyncIterator[Cleanup]:
    steps: Cleanup = []
    yield steps

    for dispose in reversed(steps):
        result = dispose()

        if result is not None:
            await result


def completed() -> asyncio.Future[None]:
    future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    future.set_result(None)
    return future


def forbidden(*args: Any) -> Any:
    pytest.fail("an invalid argument must fail before the token provider or the transport factory")


async def connect_fake_subscriber(cleanup: Cleanup) -> tuple[FakeTransport, RelaySubscriber]:
    fake = create_fake_transport()

    def create_transport(url: str, protocols: Sequence[str]) -> ConnectingTransport:
        return ConnectingTransport(transport=fake, ready=completed())

    connecting = asyncio.ensure_future(connect_relay_subscriber(URL, create_transport=create_transport))
    fake.send_server_setup()
    connection = await connecting
    cleanup.append(connection.close)
    return fake, connection


async def connect_hub_publisher(cleanup: Cleanup) -> RelayPublisher:
    hub = create_relay_hub()
    cleanup.append(hub.destroy)
    publisher = await connect_relay_publisher(
        URL, namespace=["live", "example"], create_transport=hub.connect_publisher
    )
    cleanup.append(publisher.close)
    return publisher


def subscriber_entry_points(connection: RelaySubscriber, namespace: Any, track: Any) -> dict[str, Callable[[], object]]:
    """Every subscriber entry point that takes a namespace and a track name."""
    return {
        "subscribe": lambda: connection.subscribe(namespace, track),
        "subscribe_events": lambda: connection.subscribe_events(namespace, track),
        "track_source": lambda: connection.track_source.subscribe(
            MoqtSubscribeOptions(track_namespace=namespace, track_name=track)
        ),
    }


class TestNamespaceVectors:
    @pytest.mark.parametrize("case", accepted(VECTORS["namespaces"]), ids=case_id)
    def test_accepted_is_admitted_as_a_fresh_immutable_copy(self, case: Case) -> None:
        source = list(case["namespace"])
        fields = namespace_fields(source)
        assert fields == tuple(case["namespace"])
        assert isinstance(fields, tuple)
        source.append("later")
        assert fields == tuple(case["namespace"])

    @pytest.mark.parametrize("case", rejected(VECTORS["namespaces"]), ids=case_id)
    def test_rejected_is_the_named_error(self, case: Case) -> None:
        with pytest.raises(ERROR_CLASS[case["error"]]):
            namespace_fields(case["namespace"])

    @pytest.mark.parametrize("case", rejected(VECTORS["namespaces"]), ids=case_id)
    async def test_rejected_by_every_subscriber_entry_point_before_a_request_id(
        self, cleanup: Cleanup, case: Case
    ) -> None:
        fake, connection = await connect_fake_subscriber(cleanup)
        for call in subscriber_entry_points(connection, case["namespace"], "events").values():
            with pytest.raises(ERROR_CLASS[case["error"]]):
                call()
        connection.subscribe(["live"], "events")
        request = await fake.next_request_stream()
        assert await request.first_message == Subscribe(request_id=0, track_namespace=["live"], track_name="events")

    @pytest.mark.parametrize("case", rejected(VECTORS["namespaces"]), ids=case_id)
    async def test_rejected_by_connect_relay_publisher_before_token_or_transport(self, case: Case) -> None:
        with pytest.raises(ERROR_CLASS[case["error"]]):
            await connect_relay_publisher(
                URL, namespace=case["namespace"], get_token=forbidden, create_transport=forbidden
            )

    @pytest.mark.parametrize("case", accepted(VECTORS["namespaces"]), ids=case_id)
    async def test_accepted_is_sent_verbatim_by_subscribe(self, cleanup: Cleanup, case: Case) -> None:
        fake, connection = await connect_fake_subscriber(cleanup)
        connection.subscribe(case["namespace"], "events")
        request = await fake.next_request_stream()
        assert await request.first_message == Subscribe(
            request_id=0, track_namespace=list(case["namespace"]), track_name="events"
        )

    async def test_a_string_is_not_spread_into_characters_by_the_publisher(self) -> None:
        """Issue #65: ``"lab"`` used to become ``["l", "a", "b"]`` and reach the transport factory."""

        def create_transport(*args: Any) -> Any:
            raise RuntimeError("FACTORY_REACHED")

        namespace: Any = "lab"
        with pytest.raises(TypeError):
            await connect_relay_publisher(URL, namespace=namespace, create_transport=create_transport)

    @pytest.mark.parametrize(
        ("label", "namespace"),
        [
            ("a generator", (field for field in ["live"])),
            ("a set", {"live"}),
            ("a bytes container", b"live"),
            ("a dict", {"0": "live"}),
        ],
    )
    def test_rejects_what_json_cannot_express_as_a_type_error(self, label: str, namespace: Any) -> None:
        with pytest.raises(TypeError):
            namespace_fields(namespace)

    def test_admits_a_tuple(self) -> None:
        assert namespace_fields(("live", "room/42")) == ("live", "room/42")


class TestTrackNameVectors:
    @pytest.mark.parametrize("case", accepted(VECTORS["trackNames"]), ids=case_id)
    def test_accepted_is_admitted_unchanged(self, case: Case) -> None:
        assert track_name(case["track"]) is case["track"]

    @pytest.mark.parametrize("case", rejected(VECTORS["trackNames"]), ids=case_id)
    def test_rejected_is_the_named_error(self, case: Case) -> None:
        with pytest.raises(ERROR_CLASS[case["error"]]):
            track_name(case["track"])

    @pytest.mark.parametrize("case", rejected(VECTORS["trackNames"]), ids=case_id)
    async def test_rejected_by_every_subscriber_entry_point_before_a_request_id(
        self, cleanup: Cleanup, case: Case
    ) -> None:
        fake, connection = await connect_fake_subscriber(cleanup)
        for call in subscriber_entry_points(connection, ["live"], case["track"]).values():
            with pytest.raises(ERROR_CLASS[case["error"]]):
                call()
        connection.subscribe(["live"], "events")
        request = await fake.next_request_stream()
        assert await request.first_message == Subscribe(request_id=0, track_namespace=["live"], track_name="events")

    async def test_rejected_by_register_track_for_every_invalid_case(self, cleanup: Cleanup) -> None:
        publisher = await connect_hub_publisher(cleanup)
        for case in rejected(VECTORS["trackNames"]):
            with pytest.raises(ERROR_CLASS[case["error"]]):
                publisher.register_track(case["track"])
        assert publisher.register_track("events").closed is False


class TestArgumentsBeforeState:
    async def test_reports_an_invalid_namespace_on_a_closed_subscriber_as_a_type_error_not_closed(
        self, cleanup: Cleanup
    ) -> None:
        _, connection = await connect_fake_subscriber(cleanup)
        connection.close()
        await connection.closed
        for call in subscriber_entry_points(connection, "live", "events").values():
            with pytest.raises(TypeError):
                call()
        for call in subscriber_entry_points(connection, ["live"], "events").values():
            with pytest.raises(RelayError):
                call()

    async def test_checks_subscribe_events_arguments_and_state_even_when_the_token_is_already_cancelled(
        self, cleanup: Cleanup
    ) -> None:
        _, connection = await connect_fake_subscriber(cleanup)
        cancelled = CancellationToken()
        cancelled.cancel()
        with pytest.raises(TypeError):
            connection.subscribe_events([], "", cancellation=cancelled)
        with pytest.raises(TypeError):
            connection.subscribe_events(["live"], "", cancellation=cancelled)
        events = connection.subscribe_events(["live"], "events", cancellation=cancelled)
        events.close()
        connection.close()
        await connection.closed
        with pytest.raises(RelayError):
            connection.subscribe_events(["live"], "events", cancellation=cancelled)

    async def test_reports_an_invalid_track_name_on_a_closed_publisher_as_a_type_error_not_closed(
        self, cleanup: Cleanup
    ) -> None:
        publisher = await connect_hub_publisher(cleanup)
        await publisher.close()
        with pytest.raises(TypeError):
            publisher.register_track("")
        with pytest.raises(RelayError):
            publisher.register_track("events")

    async def test_reports_an_invalid_payload_on_a_closed_track_writer_as_a_type_error_not_closed(
        self, cleanup: Cleanup
    ) -> None:
        publisher = await connect_hub_publisher(cleanup)
        track = publisher.register_track("events")
        track.close()
        bad: Any = "bad"
        with pytest.raises(TypeError):
            await track.write(bad)
        with pytest.raises(RelayError) as failure:
            await track.write(b"\x01")
        assert failure.value.code == "CLOSED"


def maximal_values(count: int) -> list[KeyValuePair]:
    """Sixteen maximal byte values encode past the 1 MiB object bound although each is legal on its own."""
    return [KeyValuePair(2 * index + 1, bytes(MAX_PROPERTY_VALUE_LENGTH)) for index in range(count)]


def block_of(length: int) -> list[KeyValuePair]:
    """One odd pair whose encoding is exactly ``length`` bytes: its type and length take 4 bytes."""
    pairs = [KeyValuePair(1, bytes(length - 4))]
    encoded = ByteWriter()
    encode_key_value_pairs(encoded, pairs)
    assert encoded.length == length
    return pairs


def pair(kind: Any, value: Any) -> KeyValuePair:
    """A property the type checker would refuse to spell: the point is to see it rejected at runtime."""
    return KeyValuePair(kind, value)


class TestProperties:
    @pytest.mark.parametrize(
        ("label", "properties"),
        [
            ("a string container", "abc"),
            ("an object container", KeyValuePair(2, 1)),
            ("a None pair", [None]),
            ("an empty pair", [object()]),
            ("an odd type with a string value", [pair(3, "abc")]),
            ("an odd type with a list value", [pair(3, [1, 2])]),
            ("an odd type with an integer value", [KeyValuePair(3, 3)]),
            ("a negative type", [KeyValuePair(-1, 0)]),
            ("a fractional type", [pair(1.5, 0)]),
            ("a bool type", [KeyValuePair(True, b"x")]),
            ("a type above the varint bound", [KeyValuePair(2**64, 0)]),
            ("an even type with a negative value", [KeyValuePair(2, -1)]),
            ("an even type with a fractional value", [pair(2, 1.5)]),
            ("an even type with a bool value", [KeyValuePair(2, True)]),
            ("an even type with a value above the varint bound", [KeyValuePair(2, 2**64)]),
            ("an even type with a byte value", [KeyValuePair(2, b"x")]),
        ],
    )
    def test_rejects_as_a_type_error(self, label: str, properties: Any) -> None:
        with pytest.raises(TypeError):
            property_pairs(properties)

    def test_rejects_a_byte_value_longer_than_its_wire_length_as_a_value_error(self) -> None:
        with pytest.raises(ValueError, match="Property 0 value exceeds"):
            property_pairs([KeyValuePair(3, bytes(MAX_PROPERTY_VALUE_LENGTH + 1))])

    def test_rejects_a_list_whose_encoding_exceeds_the_object_bound_as_a_value_error(self) -> None:
        with pytest.raises(ValueError, match="encoded limit"):
            property_pairs(maximal_values(16))

    def test_admits_the_boundaries(self) -> None:
        assert len(property_pairs([KeyValuePair(3, bytes(MAX_PROPERTY_VALUE_LENGTH))])) == 1
        assert len(property_pairs(maximal_values(15))) == 15
        assert property_pairs([KeyValuePair(2**64 - 2, 2**64 - 1)]) == [KeyValuePair(2**64 - 2, 2**64 - 1)]

    def test_copies_the_bytes_a_view_spans_and_admits_an_absent_list_as_empty(self) -> None:
        backing = bytearray(b"abcd")
        pairs = property_pairs([pair(3, memoryview(backing)[1:3])])
        backing[:] = b"xxxx"
        assert pairs == [KeyValuePair(3, b"bc")]
        assert property_pairs(None) == []
        assert property_pairs([]) == []
        assert property_pairs(()) == []

    async def test_reports_an_oversized_property_as_an_argument_error_not_a_transport_failure(
        self, cleanup: Cleanup
    ) -> None:
        publisher = await connect_hub_publisher(cleanup)
        with pytest.raises(ValueError):
            publisher.register_track("events", track_properties=maximal_values(16))
        track = publisher.register_track("events")
        with pytest.raises(ValueError):
            await track.write(b"\x01", properties=maximal_values(16))
        with pytest.raises(ValueError):
            await track.write(b"\x01", properties=[KeyValuePair(3, bytes(MAX_PROPERTY_VALUE_LENGTH + 1))])
        assert not track.closed

    def test_object_properties_admit_exactly_the_relay_limit(self) -> None:
        assert object_property_pairs(block_of(MAX_RELAY_OBJECT_PROPERTIES_LENGTH)) == block_of(
            MAX_RELAY_OBJECT_PROPERTIES_LENGTH
        )
        assert object_property_pairs(None) == []

    def test_object_properties_past_the_relay_limit_are_a_value_error_naming_the_size(self) -> None:
        with pytest.raises(ValueError, match="encode to 65537 bytes, over the relay's 65536-byte limit"):
            object_property_pairs(block_of(MAX_RELAY_OBJECT_PROPERTIES_LENGTH + 1))
        with pytest.raises(ValueError, match="relay's 65536-byte limit"):
            object_property_pairs(maximal_values(16))


class TestPayloads:
    @pytest.mark.parametrize(
        ("label", "payload"),
        [
            ("a string", "hello"),
            ("a list of integers", [1, 2, 3]),
            ("a tuple of integers", (1, 2, 3)),
            ("None", None),
            ("a number", 7),
        ],
    )
    def test_rejects_as_a_type_error(self, label: str, payload: Any) -> None:
        with pytest.raises(TypeError, match="bytes-like"):
            payload_bytes(payload)

    def test_rejects_more_than_the_object_payload_limit_and_admits_exactly_the_limit(self) -> None:
        with pytest.raises(ValueError, match="16 MiB"):
            payload_bytes(bytes(MAX_OBJECT_PAYLOAD_LENGTH + 1))
        assert len(payload_bytes(bytes(MAX_OBJECT_PAYLOAD_LENGTH))) == MAX_OBJECT_PAYLOAD_LENGTH

    def test_copies_the_bytes_a_view_spans_for_any_buffer_including_an_empty_one(self) -> None:
        backing = bytearray(b"abc123def")
        data = payload_bytes(memoryview(backing)[3:6])
        assert data == b"123"
        backing[:] = b"x" * 9
        assert data == b"123"
        assert payload_bytes(bytearray(b"ab")) == b"ab"
        assert payload_bytes(b"") == b""


def test_relay_object_properties_limit_matches_spec() -> None:
    assert PROTOCOL["relay"]["limits"]["maxObjectPropertiesBytes"] == MAX_RELAY_OBJECT_PROPERTIES_LENGTH


def test_property_value_limit_matches_spec() -> None:
    """The core keeps its copy of this bound private; the SDK's validation module holds the one the pin checks."""
    assert PROTOCOL["limits"]["maxPropertyValueBytes"] == MAX_PROPERTY_VALUE_LENGTH
