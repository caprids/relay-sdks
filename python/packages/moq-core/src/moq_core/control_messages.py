"""MOQT control-message wire codecs (moq-transport draft-20 §10).

Every control or request-stream message is framed as::

    MOQT Control Message {
      Message Type (vi64),
      Message Length (16),
      Message Body (..),
    }

Scope follows the subscribe-only engine: the encoders cover what a subscriber sends (SETUP,
SUBSCRIBE, FETCH, TRACK_STATUS, SUBSCRIBE_NAMESPACE, REQUEST_UPDATE, GOAWAY, and REQUEST_OK /
REQUEST_ERROR responses to incoming PUBLISH) plus the publisher-side messages the publish driver and
the in-memory test peers need; the decoders cover everything either side receives. All draft-version
specifics live in this package: it is the churn-absorption layer for pre-WGLC drafts.

Where the draft text and the deployed relay (moq-relay, moq-dev/moq) disagree on a wire detail, the
codec follows the relay and says so at the site. Interop is with the relay, not the prose.

Namespace fields and track names are byte strings on the wire; this codec surfaces them as UTF-8
``str`` values.

Decoded messages are frozen dataclasses. Each carries a ``kind`` class attribute with the TypeScript
core's discriminator spelling (``"subscribe-ok"``, ``"request-error"``, ...) so tests and logs read
the same in every language.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from enum import IntEnum
from typing import ClassVar, Literal, TypeAlias

from .bytes import ByteReader, ByteWriter, utf8_decode, utf8_encode
from .errors import IncompleteInputError, MoqtProtocolError
from .varint import MAX_VARINT_VALUE, decode_varint

# ============================================================================
# Wire constants
# ============================================================================

MOQT_PROTOCOL_ID = "moqt-20"
"""ALPN / WebTransport protocol identifier for the implemented draft (§3.1: ``moqt-`` plus the draft number).

Version negotiation happens at connection time (``WT-Available-Protocols`` in WebTransport, ALPN in
native QUIC), not in SETUP.
"""


class MESSAGE_TYPE(IntEnum):
    """Control/request message type codes (§10, Table 5)."""

    SETUP = 0x2F00
    GOAWAY = 0x10
    SUBSCRIBE = 0x3
    SUBSCRIBE_OK = 0x4
    REQUEST_ERROR = 0x5
    PUBLISH_NAMESPACE = 0x6
    REQUEST_OK = 0x7
    NAMESPACE = 0x8
    PUBLISH_DONE = 0xB
    TRACK_STATUS = 0xD
    NAMESPACE_DONE = 0xE
    PUBLISH_SKIPPED = 0xF
    FETCH = 0x16
    FETCH_OK = 0x18
    PUBLISH = 0x1D
    PUBLISH_STATE_NOTIFY = 0x22
    REQUEST_UPDATE = 0x2
    SUBSCRIBE_NAMESPACE = 0x50
    SUBSCRIBE_TRACKS = 0x51


class SETUP_OPTION(IntEnum):
    """Setup Option types (§10.3.1). A namespace separate from message parameters."""

    PATH = 0x01
    AUTHORIZATION_TOKEN = 0x03
    MAX_AUTH_TOKEN_CACHE_SIZE = 0x04
    AUTHORITY = 0x05
    MAX_FILTER_RANGES = 0x06
    MOQT_IMPLEMENTATION = 0x07
    MAX_REQUEST_UPDATES = 0x08


class PARAMETER_TYPE(IntEnum):
    """Message parameter types (§10.2, §15.7)."""

    OBJECT_DELIVERY_TIMEOUT = 0x02
    AUTHORIZATION_TOKEN = 0x03
    RENDEZVOUS_TIMEOUT = 0x04
    SUBGROUP_DELIVERY_TIMEOUT = 0x06
    EXPIRES = 0x08
    LARGEST_OBJECT = 0x09
    FILL_TIMEOUT = 0x0A
    FORWARD = 0x10
    SUBSCRIBER_PRIORITY = 0x20
    LOCATION_FILTER = 0x21
    GROUP_ORDER = 0x22
    FILL_PARAMETERS = 0x23
    SUBGROUP_FILTER = 0x25
    OBJECTID_FILTER = 0x26
    PRIORITY_FILTER = 0x27
    OBJECT_PROPERTY_FILTER = 0x28
    TRACK_PROPERTY_FILTER = 0x29
    NEW_GROUP_REQUEST = 0x32
    TRACK_NAMESPACE_PREFIX = 0x34
    INCLUDE_PROPERTIES = 0x35


class TRACK_PROPERTY(IntEnum):
    """Track Property types (§10.8): the KVP block trailing SUBSCRIBE_OK / PUBLISH / FETCH_OK.

    A registry of its own, distinct from both Setup Options and message parameters.
    """

    TIMESCALE = 0x08
    """Units-per-second for the track's object TIMESTAMP extensions.

    Declaring it in SUBSCRIBE_OK is what opts the track into timestamp forwarding on moq-lite-rs
    relays; without it they parse and discard the extensions and re-stamp frames on arrival.
    """


class REQUEST_ERROR_CODE(IntEnum):
    """REQUEST_ERROR codes (§15.11.2)."""

    INTERNAL_ERROR = 0x0
    UNAUTHORIZED = 0x1
    TIMEOUT = 0x2
    NOT_SUPPORTED = 0x3
    MALFORMED_AUTH_TOKEN = 0x4
    EXPIRED_AUTH_TOKEN = 0x5
    GOING_AWAY = 0x6
    EXCESSIVE_LOAD = 0x9
    DOES_NOT_EXIST = 0x10
    INVALID_RANGE = 0x11
    MALFORMED_TRACK = 0x12
    UNINTERESTED = 0x20
    PREFIX_OVERLAP = 0x30
    NAMESPACE_TOO_LARGE = 0x31
    UNSUPPORTED_EXTENSION = 0x33
    REDIRECT = 0x34
    CONFLICTING_FILTERS = 0x35
    INVALID_FILTER = 0x36


class PUBLISH_DONE_STATUS(IntEnum):
    """PUBLISH_DONE status codes (§10.12)."""

    INTERNAL_ERROR = 0x0
    UNAUTHORIZED = 0x1
    TRACK_ENDED = 0x2
    GOING_AWAY = 0x4
    TOO_FAR_BEHIND = 0x5
    EXPIRED = 0x6
    UPDATE_FAILED = 0x8
    EXCESSIVE_LOAD = 0x9
    MALFORMED_TRACK = 0x12


class STREAM_RESET_CODE(IntEnum):
    """Stream reset / STOP_SENDING error codes (§3.3.4)."""

    INTERNAL_ERROR = 0x0
    CANCELLED = 0x1
    DELIVERY_TIMEOUT = 0x2
    SESSION_CLOSED = 0x3
    GOING_AWAY = 0x4
    TOO_FAR_BEHIND = 0x5
    UNKNOWN_OBJECT_STATUS = 0x6
    EXPIRED_AUTH_TOKEN = 0x7
    EXCESSIVE_LOAD = 0x9
    MALFORMED_TRACK = 0x12


# REQUEST_ERROR codes for which an identical retry can never succeed: the request or its credentials
# are wrong in a way that does not change between attempts. Everything else, including codes this
# table does not know, is treated as retryable, so a relay introducing a new transient code degrades
# to paced retries rather than a dead player. EXPIRED_AUTH_TOKEN is deliberately absent: it has its
# own refresh path wherever requests are retried.
_PERMANENT_REQUEST_ERROR_CODES: frozenset[int] = frozenset(
    {
        REQUEST_ERROR_CODE.UNAUTHORIZED,
        REQUEST_ERROR_CODE.NOT_SUPPORTED,
        REQUEST_ERROR_CODE.MALFORMED_AUTH_TOKEN,
        REQUEST_ERROR_CODE.INVALID_RANGE,
        # The track cannot be served as requested; an identical resubscribe re-earns the rejection.
        REQUEST_ERROR_CODE.MALFORMED_TRACK,
        REQUEST_ERROR_CODE.UNINTERESTED,
        REQUEST_ERROR_CODE.PREFIX_OVERLAP,
        REQUEST_ERROR_CODE.NAMESPACE_TOO_LARGE,
        REQUEST_ERROR_CODE.UNSUPPORTED_EXTENSION,
        # Until session migration exists, the redirect target is unreachable and re-asking the same
        # relay just re-earns the redirect.
        REQUEST_ERROR_CODE.REDIRECT,
        REQUEST_ERROR_CODE.CONFLICTING_FILTERS,
        REQUEST_ERROR_CODE.INVALID_FILTER,
    }
)


def is_retryable_request_error_code(error_code: int) -> bool:
    """Whether a failed request is worth re-issuing unchanged after a backoff.

    The transient family (``DOES_NOT_EXIST`` while a broadcast has not started, ``TIMEOUT``,
    ``INTERNAL_ERROR``, overload, ``GOING_AWAY``) reflects relay or publisher *state* that time can
    fix; the permanent family reflects the request itself and loops forever if retried.
    """
    return error_code not in _PERMANENT_REQUEST_ERROR_CODES


# PUBLISH_DONE statuses after which an identical re-subscribe can never succeed: auth-shaped ends and
# a malformed track. Every other status describes publisher/relay state a restart or live-edge rejoin
# legitimately recovers from, and unknown statuses degrade to paced retries for the same reason as
# is_retryable_request_error_code.
_PERMANENT_PUBLISH_DONE_STATUSES: frozenset[int] = frozenset(
    {PUBLISH_DONE_STATUS.UNAUTHORIZED, PUBLISH_DONE_STATUS.EXPIRED, PUBLISH_DONE_STATUS.MALFORMED_TRACK}
)


def is_retryable_publish_done_status(status_code: int) -> bool:
    """Whether a PUBLISH_DONE'd subscription is worth re-subscribing."""
    return status_code not in _PERMANENT_PUBLISH_DONE_STATUSES


MAX_CONTROL_MESSAGE_LENGTH = 0xFFFF
"""Maximum body size: the frame length field is 16 bits."""

MAX_NAMESPACE_FIELDS = 32
_MAX_FULL_TRACK_NAME_LENGTH = 4096
_MAX_REASON_PHRASE_LENGTH = 1024
_MAX_NEW_SESSION_URI_LENGTH = 8192
_MAX_KVP_VALUE_LENGTH = 0xFFFF
_MAX_LOCATION_FILTER_FIELDS = 4

# ============================================================================
# Shared structures
# ============================================================================


@dataclass(frozen=True, slots=True)
class Location:
    """A particular Object in a Group within a Track (§1.4.2)."""

    group: int
    object: int


def compare_locations(a: Location, b: Location) -> int:
    """Negative when ``a < b`` in Location order (group first, then object), zero when equal."""
    return (a.group - b.group) or (a.object - b.object)


TrackNamespace: TypeAlias = list[str]
"""Track namespace as an ordered tuple of UTF-8 fields. Byte strings on the wire."""


def _write_track_namespace(writer: ByteWriter, namespace: Sequence[str]) -> None:
    if len(namespace) > MAX_NAMESPACE_FIELDS:
        raise MoqtProtocolError(f"track namespace has more than {MAX_NAMESPACE_FIELDS} fields")

    writer.write_varint(len(namespace))

    for part in namespace:
        encoded = utf8_encode(part)
        if len(encoded) == 0:
            raise MoqtProtocolError("track namespace field must not be empty")

        writer.write_length_prefixed(encoded)


def _read_track_namespace(reader: ByteReader) -> TrackNamespace:
    field_count = reader.read_varint()

    if field_count > MAX_NAMESPACE_FIELDS:
        raise MoqtProtocolError(f"track namespace has more than {MAX_NAMESPACE_FIELDS} fields")

    namespace: TrackNamespace = []
    total_length = 0

    for _ in range(field_count):
        length = reader.read_varint()
        if length == 0:
            raise MoqtProtocolError("track namespace field must not be empty")

        total_length += length

        if total_length > _MAX_FULL_TRACK_NAME_LENGTH:
            raise MoqtProtocolError("track namespace exceeds 4096 bytes")

        namespace.append(utf8_decode(reader.read_bytes(length)))

    return namespace


def _write_reason_phrase(writer: ByteWriter, reason: str) -> None:
    encoded = utf8_encode(reason)
    if len(encoded) > _MAX_REASON_PHRASE_LENGTH:
        raise MoqtProtocolError("reason phrase exceeds 1024 bytes")

    writer.write_length_prefixed(encoded)


def _read_reason_phrase(reader: ByteReader) -> str:
    length = reader.read_varint()
    if length > _MAX_REASON_PHRASE_LENGTH:
        raise MoqtProtocolError("reason phrase exceeds 1024 bytes")

    return utf8_decode(reader.read_bytes(length))


def _write_location(writer: ByteWriter, location: Location) -> None:
    writer.write_varint(location.group)
    writer.write_varint(location.object)


def _read_location(reader: ByteReader) -> Location:
    return Location(group=reader.read_varint(), object=reader.read_varint())


def _write_location_parameter(writer: ByteWriter, location: Location) -> None:
    # LARGEST_OBJECT rides inside a length prefix. §10.2 defines a Location parameter as two bare
    # varints, but moq-relay, the peer this SDK is deployed against, applies the Key-Value-Pair
    # parity rule to the odd type 0x09 and frames it with a Length on every draft (moq-dev/moq#3255
    # records it as a spec conflict). The two forms are indistinguishable on the wire, so the codec
    # follows the relay.
    value = ByteWriter()
    _write_location(value, location)
    writer.write_length_prefixed(value.to_bytes())


def _read_location_parameter(reader: ByteReader) -> Location:
    value = ByteReader(reader.read_bytes(reader.read_varint()))
    location = _read_location(value)

    if value.remaining != 0:
        raise MoqtProtocolError("Location parameter has trailing bytes")

    return location


# ============================================================================
# Key-Value-Pairs (§1.4.3): Setup Options and Track Properties
# ============================================================================


@dataclass(frozen=True, slots=True)
class KeyValuePair:
    """A generic MOQT Key-Value-Pair.

    Even types carry a varint value, odd types a byte field. Used for Setup Options and Track/Object
    Properties (message parameters use per-type encodings instead; see :class:`MessageParameters`).
    """

    type: int
    value: int | bytes


def encode_key_value_pairs(writer: ByteWriter, pairs: Sequence[KeyValuePair]) -> None:
    """Serialize pairs in ascending type order with delta-encoded types."""
    previous_type = 0

    for pair in sorted(pairs, key=lambda p: p.type):
        writer.write_varint(pair.type - previous_type)
        previous_type = pair.type

        if pair.type % 2 == 0:
            if not isinstance(pair.value, int):
                raise MoqtProtocolError(f"even KVP type {pair.type} requires a varint value")

            writer.write_varint(pair.value)
        else:
            if isinstance(pair.value, int):
                raise MoqtProtocolError(f"odd KVP type {pair.type} requires a byte value")

            if len(pair.value) > _MAX_KVP_VALUE_LENGTH:
                raise MoqtProtocolError("KVP value exceeds 2^16-1 bytes")

            writer.write_length_prefixed(pair.value)


def decode_key_value_pairs(reader: ByteReader, end_offset: int) -> list[KeyValuePair]:
    """Parse pairs until ``reader`` reaches ``end_offset`` (KVP blocks are length-bounded)."""
    pairs: list[KeyValuePair] = []
    previous_type = 0

    while reader.offset < end_offset:
        pair_type = previous_type + reader.read_varint()
        previous_type = pair_type

        if pair_type % 2 == 0:
            pairs.append(KeyValuePair(pair_type, reader.read_varint()))
        else:
            length = reader.read_varint()
            if length > _MAX_KVP_VALUE_LENGTH:
                raise MoqtProtocolError("KVP value exceeds 2^16-1 bytes")

            pairs.append(KeyValuePair(pair_type, reader.read_bytes(length)))

    if reader.offset != end_offset:
        raise MoqtProtocolError("KVP block overran its bound")

    return pairs


# ============================================================================
# Location Filters (§5.1.2), carried in the LOCATION_FILTER parameter
# ============================================================================


@dataclass(frozen=True, slots=True)
class NoLocationFilter:
    """Zero fields. Unfiltered: the whole track for a subscription, ``{0,0}`` up to Largest Object for a fetch."""

    type: ClassVar[Literal["none"]] = "none"


@dataclass(frozen=True, slots=True)
class NextObjectFilter:
    """The two-field spelling ``{0,0}``: ``{Largest.Group, Largest.Object + 1}``, the live edge."""

    type: ClassVar[Literal["next-object"]] = "next-object"


@dataclass(frozen=True, slots=True)
class RelativeGroupFilter:
    """One field: start at ``{Largest.Group + 1 - groups_before_next, 0}``, open-ended.

    ``0`` is the next group, ``1`` the start of the current group (a decodable join), ``N`` N-1 groups
    before it.
    """

    groups_before_next: int
    type: ClassVar[Literal["relative-group"]] = "relative-group"


@dataclass(frozen=True, slots=True)
class AbsoluteLocationFilter:
    """Two to four fields: an explicit start, optionally bounded by an end group.

    The end group is a delta from the start group and is included whole unless ``end_object``
    narrows it. An open-ended absolute ``{0,0}`` is unfiltered by definition and encodes as
    :class:`NoLocationFilter`; its two-field spelling would read as :class:`NextObjectFilter`.
    """

    start: Location
    end_group_delta: int | None = None
    end_object: int | None = None
    type: ClassVar[Literal["absolute"]] = "absolute"


LocationFilter: TypeAlias = NoLocationFilter | NextObjectFilter | RelativeGroupFilter | AbsoluteLocationFilter
"""Which Objects a subscription or fetch covers; the field count selects the meaning."""


def encode_location_filter(location_filter: LocationFilter) -> bytes:
    """Serialize a filter as the LOCATION_FILTER parameter's value (without its length prefix)."""
    writer = ByteWriter()

    match location_filter:
        case NoLocationFilter():
            pass
        case NextObjectFilter():
            writer.write_varint(0)
            writer.write_varint(0)
        case RelativeGroupFilter(groups_before_next=groups_before_next):
            writer.write_varint(groups_before_next)
        case AbsoluteLocationFilter(start=start, end_group_delta=end_group_delta, end_object=end_object):
            if end_object is not None and end_group_delta is None:
                raise MoqtProtocolError("location filter end object requires an end group")

            if start.group == 0 and start.object == 0 and end_group_delta is None:
                return b""

            _write_location(writer, start)

            if end_group_delta is not None:
                writer.write_varint(end_group_delta)

            if end_object is not None:
                writer.write_varint(end_object)

    return writer.to_bytes()


def decode_location_filter(data: bytes) -> LocationFilter:
    reader = ByteReader(data)
    fields: list[int] = []

    while reader.remaining > 0:
        if len(fields) == _MAX_LOCATION_FILTER_FIELDS:
            raise MoqtProtocolError("location filter has more than four fields")

        fields.append(reader.read_varint())

    if len(fields) == 0:
        return NoLocationFilter()

    if len(fields) == 1:
        return RelativeGroupFilter(groups_before_next=fields[0])

    group, obj = fields[0], fields[1]
    if len(fields) == 2 and group == 0 and obj == 0:
        return NextObjectFilter()

    end_group_delta = fields[2] if len(fields) > 2 else None
    end_object = fields[3] if len(fields) > 3 else None

    if end_group_delta is not None and group + end_group_delta > MAX_VARINT_VALUE:
        raise MoqtProtocolError("location filter end group exceeds supported range")

    return AbsoluteLocationFilter(start=Location(group, obj), end_group_delta=end_group_delta, end_object=end_object)


# ============================================================================
# Message Parameters (§10.2)
# ============================================================================

GroupOrder: TypeAlias = Literal["ascending", "descending"]


@dataclass(frozen=True, slots=True)
class RangeFilter:
    """A range-filter parameter (§5.1.4), kept opaque: the SDK never sends or interprets them."""

    type: int
    value: bytes


@dataclass(slots=True)
class MessageParameters:
    """Decoded message parameters.

    Unlike Setup Options, message parameters use per-type encodings (uint8 / varint /
    length-prefixed), so the codec is registry-driven. Per §10.2 an unknown parameter type is a
    PROTOCOL_VIOLATION (parameters cannot be skipped; the block is bounded by count, not length).

    ``fill_parameters`` (§10.2.15) asks the publisher to deliver a fill range on a fetch stream
    alongside the subscription; its presence alone requests the fill. It is a nested parameter scope:
    only the fill-scoped subset (Table 6) is legal inside, and omitted entries inherit the
    subscription's own values.
    """

    object_delivery_timeout: int | None = None
    subgroup_delivery_timeout: int | None = None
    rendezvous_timeout: int | None = None
    fill_timeout: int | None = None
    authorization_tokens: list[bytes] | None = None
    """Serialized Token structures (§10.2.2). May repeat."""
    expires: int | None = None
    largest_object: Location | None = None
    forward: int | None = None
    """``0`` or ``1``."""
    subscriber_priority: int | None = None
    location_filter: LocationFilter | None = None
    group_order: GroupOrder | None = None
    fill_parameters: MessageParameters | None = None
    new_group_request: int | None = None
    range_filters: list[RangeFilter] | None = None
    track_namespace_prefix: TrackNamespace | None = None
    include_properties: int | None = None
    """INCLUDE_PROPERTIES (§10.2.21): ``0`` asks the responder to send empty Track Properties. Default ``1``."""


ParameterScope: TypeAlias = Literal["message", "fill"]

_FILL_SCOPE_TYPES: frozenset[int] = frozenset(
    {
        PARAMETER_TYPE.FILL_TIMEOUT,
        PARAMETER_TYPE.SUBSCRIBER_PRIORITY,
        PARAMETER_TYPE.LOCATION_FILTER,
        PARAMETER_TYPE.GROUP_ORDER,
        PARAMETER_TYPE.SUBGROUP_FILTER,
        PARAMETER_TYPE.OBJECTID_FILTER,
        PARAMETER_TYPE.PRIORITY_FILTER,
        PARAMETER_TYPE.OBJECT_PROPERTY_FILTER,
    }
)

_GROUP_ORDER_WIRE: dict[str, int] = {"ascending": 0x1, "descending": 0x2}

_RANGE_FILTER_TYPES: frozenset[int] = frozenset(
    {
        PARAMETER_TYPE.SUBGROUP_FILTER,
        PARAMETER_TYPE.OBJECTID_FILTER,
        PARAMETER_TYPE.PRIORITY_FILTER,
        PARAMETER_TYPE.OBJECT_PROPERTY_FILTER,
        PARAMETER_TYPE.TRACK_PROPERTY_FILTER,
    }
)


def _collect_parameter_entries(parameters: MessageParameters) -> list[tuple[int, Callable[[ByteWriter], None]]]:
    entries: list[tuple[int, Callable[[ByteWriter], None]]] = []

    def varint(parameter_type: int, value: int) -> None:
        entries.append((parameter_type, lambda w: w.write_varint(value)))

    def uint8(parameter_type: int, value: int) -> None:
        entries.append((parameter_type, lambda w: w.write_uint8(value)))

    def prefixed(parameter_type: int, value: bytes) -> None:
        entries.append((parameter_type, lambda w: w.write_length_prefixed(value)))

    if parameters.object_delivery_timeout is not None:
        varint(PARAMETER_TYPE.OBJECT_DELIVERY_TIMEOUT, parameters.object_delivery_timeout)

    if parameters.subgroup_delivery_timeout is not None:
        varint(PARAMETER_TYPE.SUBGROUP_DELIVERY_TIMEOUT, parameters.subgroup_delivery_timeout)

    if parameters.rendezvous_timeout is not None:
        varint(PARAMETER_TYPE.RENDEZVOUS_TIMEOUT, parameters.rendezvous_timeout)

    if parameters.fill_timeout is not None:
        varint(PARAMETER_TYPE.FILL_TIMEOUT, parameters.fill_timeout)

    for token in parameters.authorization_tokens or ():
        prefixed(PARAMETER_TYPE.AUTHORIZATION_TOKEN, token)

    if parameters.expires is not None:
        varint(PARAMETER_TYPE.EXPIRES, parameters.expires)

    if parameters.largest_object is not None:
        largest = parameters.largest_object
        entries.append((PARAMETER_TYPE.LARGEST_OBJECT, lambda w: _write_location_parameter(w, largest)))

    if parameters.forward is not None:
        uint8(PARAMETER_TYPE.FORWARD, parameters.forward)

    if parameters.subscriber_priority is not None:
        uint8(PARAMETER_TYPE.SUBSCRIBER_PRIORITY, parameters.subscriber_priority)

    if parameters.location_filter is not None:
        prefixed(PARAMETER_TYPE.LOCATION_FILTER, encode_location_filter(parameters.location_filter))

    if parameters.group_order is not None:
        uint8(PARAMETER_TYPE.GROUP_ORDER, _GROUP_ORDER_WIRE[parameters.group_order])

    if parameters.fill_parameters is not None:
        prefixed(PARAMETER_TYPE.FILL_PARAMETERS, _encode_fill_parameters(parameters.fill_parameters))

    if parameters.new_group_request is not None:
        varint(PARAMETER_TYPE.NEW_GROUP_REQUEST, parameters.new_group_request)

    for range_filter in parameters.range_filters or ():
        prefixed(range_filter.type, range_filter.value)

    if parameters.track_namespace_prefix is not None:
        prefix = parameters.track_namespace_prefix
        entries.append((PARAMETER_TYPE.TRACK_NAMESPACE_PREFIX, lambda w: _write_track_namespace(w, prefix)))

    if parameters.include_properties is not None:
        prefixed(PARAMETER_TYPE.INCLUDE_PROPERTIES, bytes((parameters.include_properties,)))

    return entries


def encode_message_parameters(
    writer: ByteWriter, parameters: MessageParameters | None = None, scope: ParameterScope = "message"
) -> None:
    """Serialize as ``Number of Parameters (vi64)`` + delta-typed parameter list.

    The ``"fill"`` scope is the nested list inside FILL_PARAMETERS, where only Table 6's types are legal.
    """
    entries = sorted(_collect_parameter_entries(parameters or MessageParameters()), key=lambda entry: entry[0])

    if scope == "fill":
        for parameter_type, _ in entries:
            if parameter_type not in _FILL_SCOPE_TYPES:
                raise MoqtProtocolError(f"parameter type {parameter_type} is not allowed inside FILL_PARAMETERS")

    writer.write_varint(len(entries))
    previous_type = 0

    for parameter_type, write in entries:
        writer.write_varint(parameter_type - previous_type)
        previous_type = parameter_type
        write(writer)


def _encode_fill_parameters(fill: MessageParameters) -> bytes:
    """FILL_PARAMETERS' value: a parameter list encoded as if for a separate message (§10.2.15)."""
    writer = ByteWriter()
    encode_message_parameters(writer, fill, "fill")
    return writer.to_bytes()


def decode_message_parameters(reader: ByteReader, scope: ParameterScope = "message") -> MessageParameters:
    count = reader.read_varint()
    parameters = MessageParameters()
    previous_type = 0

    for _ in range(count):
        parameter_type = previous_type + reader.read_varint()
        previous_type = parameter_type

        # §10.2.15: a type outside Table 6 inside FILL_PARAMETERS is a PROTOCOL_VIOLATION, the
        # nested scope included.
        if scope == "fill" and parameter_type not in _FILL_SCOPE_TYPES:
            raise MoqtProtocolError(f"parameter type {parameter_type} is not allowed inside FILL_PARAMETERS")

        if parameter_type == PARAMETER_TYPE.OBJECT_DELIVERY_TIMEOUT:
            parameters.object_delivery_timeout = reader.read_varint()
        elif parameter_type == PARAMETER_TYPE.SUBGROUP_DELIVERY_TIMEOUT:
            parameters.subgroup_delivery_timeout = reader.read_varint()
        elif parameter_type == PARAMETER_TYPE.RENDEZVOUS_TIMEOUT:
            parameters.rendezvous_timeout = reader.read_varint()
        elif parameter_type == PARAMETER_TYPE.FILL_TIMEOUT:
            parameters.fill_timeout = reader.read_varint()
        elif parameter_type == PARAMETER_TYPE.AUTHORIZATION_TOKEN:
            token = reader.read_bytes(reader.read_varint())

            if parameters.authorization_tokens is None:
                parameters.authorization_tokens = []

            parameters.authorization_tokens.append(token)
        elif parameter_type == PARAMETER_TYPE.EXPIRES:
            parameters.expires = reader.read_varint()
        elif parameter_type == PARAMETER_TYPE.LARGEST_OBJECT:
            parameters.largest_object = _read_location_parameter(reader)
        elif parameter_type == PARAMETER_TYPE.FORWARD:
            forward = reader.read_uint8()
            if forward not in (0, 1):
                raise MoqtProtocolError(f"invalid FORWARD value {forward}")

            parameters.forward = forward
        elif parameter_type == PARAMETER_TYPE.SUBSCRIBER_PRIORITY:
            parameters.subscriber_priority = reader.read_uint8()
        elif parameter_type == PARAMETER_TYPE.LOCATION_FILTER:
            parameters.location_filter = decode_location_filter(reader.read_bytes(reader.read_varint()))
        elif parameter_type == PARAMETER_TYPE.GROUP_ORDER:
            order = reader.read_uint8()
            if order not in (0x1, 0x2):
                raise MoqtProtocolError(f"invalid GROUP_ORDER value {order}")

            parameters.group_order = "ascending" if order == 0x1 else "descending"
        elif parameter_type == PARAMETER_TYPE.FILL_PARAMETERS:
            value = ByteReader(reader.read_bytes(reader.read_varint()))
            parameters.fill_parameters = decode_message_parameters(value, "fill")

            if value.remaining != 0:
                raise MoqtProtocolError("FILL_PARAMETERS has trailing bytes")
        elif parameter_type == PARAMETER_TYPE.NEW_GROUP_REQUEST:
            parameters.new_group_request = reader.read_varint()
        elif parameter_type == PARAMETER_TYPE.TRACK_NAMESPACE_PREFIX:
            parameters.track_namespace_prefix = _read_track_namespace(reader)
        elif parameter_type == PARAMETER_TYPE.INCLUDE_PROPERTIES:
            # §10.2.21 calls the value a uint8; moq-relay frames it length-prefixed by the odd-type
            # parity rule (moq-dev/moq#3255), and the codec follows the relay, as for LARGEST_OBJECT.
            value_bytes = reader.read_bytes(reader.read_varint())

            if len(value_bytes) != 1 or value_bytes[0] > 1:
                raise MoqtProtocolError(f"invalid INCLUDE_PROPERTIES value {','.join(str(b) for b in value_bytes)}")

            parameters.include_properties = value_bytes[0]
        elif parameter_type in _RANGE_FILTER_TYPES:
            value_bytes = reader.read_bytes(reader.read_varint())

            if parameters.range_filters is None:
                parameters.range_filters = []

            parameters.range_filters.append(RangeFilter(parameter_type, value_bytes))
        else:
            # §10.2: unknown parameters cannot be skipped (the block is bounded by count, and the
            # value encoding is per-type); session error.
            raise MoqtProtocolError(f"unknown message parameter type {parameter_type}")

    return parameters


# ============================================================================
# Authorization tokens (§10.2.2)
# ============================================================================


def encode_auth_token_use_value(token_type: int, value: bytes) -> bytes:
    """Serialize a Token structure with Alias Type USE_VALUE (0x3).

    The stateless form that attaches a token to a request without alias registration.
    """
    writer = ByteWriter()
    writer.write_varint(0x3)
    writer.write_varint(token_type)
    writer.write_bytes(value)
    return writer.to_bytes()


# ============================================================================
# Message framing
# ============================================================================


@dataclass(frozen=True, slots=True)
class ControlMessageFrame:
    """A decoded frame: message type + body bytes (not yet interpreted)."""

    type: int
    body: bytes


def _frame_message(message_type: int, body: ByteWriter) -> bytes:
    body_bytes = body.to_bytes()

    if len(body_bytes) > MAX_CONTROL_MESSAGE_LENGTH:
        raise MoqtProtocolError("control message exceeds 2^16-1 bytes")

    writer = ByteWriter()
    writer.write_varint(message_type)
    writer.write_uint16(len(body_bytes))
    writer.write_bytes(body_bytes)
    return writer.to_bytes()


class ControlMessageDeframer:
    """Incremental control-message deframer.

    Feed it stream chunks; it returns complete :class:`ControlMessageFrame` values as they
    materialize. Used for both the control streams and request streams (same framing).
    """

    __slots__ = ("_buffer",)

    def __init__(self) -> None:
        self._buffer = bytearray()

    def push(self, chunk: bytes) -> list[ControlMessageFrame]:
        self._buffer += chunk
        frames: list[ControlMessageFrame] = []
        offset = 0

        while True:
            result = self._try_read_frame(offset)
            if result is None:
                break

            frame, offset = result
            frames.append(frame)

        if offset > 0:
            del self._buffer[:offset]

        return frames

    @property
    def pending_bytes(self) -> int:
        """Bytes buffered but not yet forming a complete frame."""
        return len(self._buffer)

    def _try_read_frame(self, offset: int) -> tuple[ControlMessageFrame, int] | None:
        buffer = self._buffer
        if offset >= len(buffer):
            return None

        try:
            message_type, header_length = decode_varint(buffer, offset)
        except IncompleteInputError:
            # Incomplete varint: wait for more bytes.
            return None

        if offset + header_length + 2 > len(buffer):
            return None

        length = (buffer[offset + header_length] << 8) | buffer[offset + header_length + 1]
        body_start = offset + header_length + 2
        if body_start + length > len(buffer):
            return None

        return ControlMessageFrame(message_type, bytes(buffer[body_start : body_start + length])), body_start + length


# ============================================================================
# Decoded messages
# ============================================================================


@dataclass(frozen=True, slots=True, kw_only=True)
class Redirect:
    connect_uri: str
    track_namespace: TrackNamespace
    track_name: str


@dataclass(frozen=True, slots=True, kw_only=True)
class Setup:
    kind: ClassVar[Literal["setup"]] = "setup"
    options: list[KeyValuePair] = field(default_factory=list)


@dataclass(frozen=True, slots=True, kw_only=True)
class Goaway:
    kind: ClassVar[Literal["goaway"]] = "goaway"
    new_session_uri: str = ""
    timeout: int = 0


@dataclass(frozen=True, slots=True, kw_only=True)
class Subscribe:
    kind: ClassVar[Literal["subscribe"]] = "subscribe"
    request_id: int
    track_namespace: TrackNamespace
    track_name: str
    parameters: MessageParameters = field(default_factory=MessageParameters)


@dataclass(frozen=True, slots=True, kw_only=True)
class TrackStatus:
    """TRACK_STATUS (§10.15): identical wire format to SUBSCRIBE."""

    kind: ClassVar[Literal["track-status"]] = "track-status"
    request_id: int
    track_namespace: TrackNamespace
    track_name: str
    parameters: MessageParameters = field(default_factory=MessageParameters)


@dataclass(frozen=True, slots=True, kw_only=True)
class Fetch:
    """FETCH (§10.13) has the SUBSCRIBE body.

    The range is ``parameters.location_filter``, evaluated with the fetch rules of §5.1.2: relative
    starts count back from Largest Object, an omitted end stops there, and no filter means the whole
    track. Nothing selects "standalone" or "joining" any more; a join is a subscription with
    FILL_PARAMETERS.
    """

    kind: ClassVar[Literal["fetch"]] = "fetch"
    request_id: int
    track_namespace: TrackNamespace
    track_name: str
    parameters: MessageParameters = field(default_factory=MessageParameters)


@dataclass(frozen=True, slots=True, kw_only=True)
class SubscribeNamespace:
    kind: ClassVar[Literal["subscribe-namespace"]] = "subscribe-namespace"
    request_id: int
    track_namespace_prefix: TrackNamespace
    parameters: MessageParameters = field(default_factory=MessageParameters)


@dataclass(frozen=True, slots=True, kw_only=True)
class SubscribeOk:
    kind: ClassVar[Literal["subscribe-ok"]] = "subscribe-ok"
    track_alias: int
    parameters: MessageParameters = field(default_factory=MessageParameters)
    track_properties: list[KeyValuePair] = field(default_factory=list)


@dataclass(frozen=True, slots=True, kw_only=True)
class RequestOk:
    kind: ClassVar[Literal["request-ok"]] = "request-ok"
    parameters: MessageParameters = field(default_factory=MessageParameters)
    track_properties: list[KeyValuePair] = field(default_factory=list)


@dataclass(frozen=True, slots=True, kw_only=True)
class RequestError:
    kind: ClassVar[Literal["request-error"]] = "request-error"
    error_code: int
    retry_interval: int = 0
    reason: str = ""
    redirect: Redirect | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class RequestUpdate:
    kind: ClassVar[Literal["request-update"]] = "request-update"
    request_id: int
    parameters: MessageParameters = field(default_factory=MessageParameters)


@dataclass(frozen=True, slots=True, kw_only=True)
class PublishStateNotify:
    kind: ClassVar[Literal["publish-state-notify"]] = "publish-state-notify"
    parameters: MessageParameters = field(default_factory=MessageParameters)


@dataclass(frozen=True, slots=True, kw_only=True)
class PublishDone:
    kind: ClassVar[Literal["publish-done"]] = "publish-done"
    status_code: int
    stream_count: int | None
    """``None`` when the publisher could not count its streams (the 2^64-1 sentinel, §10.12)."""
    reason: str = ""


@dataclass(frozen=True, slots=True, kw_only=True)
class Publish:
    kind: ClassVar[Literal["publish"]] = "publish"
    request_id: int
    track_namespace: TrackNamespace
    track_name: str
    track_alias: int
    parameters: MessageParameters = field(default_factory=MessageParameters)
    track_properties: list[KeyValuePair] = field(default_factory=list)


@dataclass(frozen=True, slots=True, kw_only=True)
class PublishNamespace:
    kind: ClassVar[Literal["publish-namespace"]] = "publish-namespace"
    request_id: int
    track_namespace: TrackNamespace
    parameters: MessageParameters = field(default_factory=MessageParameters)


@dataclass(frozen=True, slots=True, kw_only=True)
class FetchOk:
    kind: ClassVar[Literal["fetch-ok"]] = "fetch-ok"
    end_of_track: bool
    end_location: Location
    parameters: MessageParameters = field(default_factory=MessageParameters)
    track_properties: list[KeyValuePair] = field(default_factory=list)


@dataclass(frozen=True, slots=True, kw_only=True)
class Namespace:
    kind: ClassVar[Literal["namespace"]] = "namespace"
    track_namespace_suffix: TrackNamespace


@dataclass(frozen=True, slots=True, kw_only=True)
class NamespaceDone:
    kind: ClassVar[Literal["namespace-done"]] = "namespace-done"
    track_namespace_suffix: TrackNamespace


@dataclass(frozen=True, slots=True, kw_only=True)
class PublishSkipped:
    kind: ClassVar[Literal["publish-skipped"]] = "publish-skipped"
    track_namespace_suffix: TrackNamespace
    track_name: str


ControlMessage: TypeAlias = (
    Setup
    | Goaway
    | Subscribe
    | TrackStatus
    | Fetch
    | SubscribeNamespace
    | SubscribeOk
    | RequestOk
    | RequestError
    | RequestUpdate
    | PublishStateNotify
    | PublishDone
    | Publish
    | PublishNamespace
    | FetchOk
    | Namespace
    | NamespaceDone
    | PublishSkipped
)

# ============================================================================
# Encoders (subscriber -> publisher)
# ============================================================================


def encode_setup(options: Sequence[KeyValuePair] = ()) -> bytes:
    """SETUP (§10.3): first message on the endpoint's control stream."""
    body = ByteWriter()
    encode_key_value_pairs(body, options)
    return _frame_message(MESSAGE_TYPE.SETUP, body)


def _encode_track_request(
    message_type: int,
    request_id: int,
    track_namespace: Sequence[str],
    track_name: str,
    parameters: MessageParameters | None,
) -> bytes:
    """SUBSCRIBE, TRACK_STATUS, and FETCH share one body: Request ID, Full Track Name, Parameters."""
    body = ByteWriter()
    body.write_varint(request_id)
    _write_track_namespace(body, track_namespace)
    body.write_length_prefixed(utf8_encode(track_name))
    encode_message_parameters(body, parameters)
    return _frame_message(message_type, body)


def encode_subscribe(request: Subscribe) -> bytes:
    """SUBSCRIBE (§10.7): first message on a new request stream."""
    return _encode_track_request(
        MESSAGE_TYPE.SUBSCRIBE, request.request_id, request.track_namespace, request.track_name, request.parameters
    )


def encode_track_status(request: TrackStatus) -> bytes:
    """TRACK_STATUS (§10.15): identical wire format to SUBSCRIBE."""
    return _encode_track_request(
        MESSAGE_TYPE.TRACK_STATUS, request.request_id, request.track_namespace, request.track_name, request.parameters
    )


def encode_fetch(request: Fetch) -> bytes:
    """FETCH (§10.13): first message on a new request stream."""
    return _encode_track_request(
        MESSAGE_TYPE.FETCH, request.request_id, request.track_namespace, request.track_name, request.parameters
    )


def encode_subscribe_namespace(request: SubscribeNamespace) -> bytes:
    """SUBSCRIBE_NAMESPACE (§10.19): first message on a new request stream."""
    body = ByteWriter()
    body.write_varint(request.request_id)
    _write_track_namespace(body, request.track_namespace_prefix)
    encode_message_parameters(body, request.parameters)
    return _frame_message(MESSAGE_TYPE.SUBSCRIBE_NAMESPACE, body)


def encode_request_update(request_id: int, parameters: MessageParameters | None = None) -> bytes:
    """REQUEST_UPDATE (§10.9), sent on an existing request stream.

    ``request_id`` is the update's own, freshly allocated Request ID (§10.1 counts REQUEST_UPDATE
    among the messages that consume one); the stream identifies the request being updated.
    """
    body = ByteWriter()
    body.write_varint(request_id)
    encode_message_parameters(body, parameters)
    return _frame_message(MESSAGE_TYPE.REQUEST_UPDATE, body)


def encode_goaway(timeout: int = 0) -> bytes:
    """GOAWAY (§10.4). Clients MUST send a zero-length New Session URI, so only the timeout (ms; 0 = none) is taken."""
    body = ByteWriter()
    body.write_varint(0)
    body.write_varint(timeout)
    return _frame_message(MESSAGE_TYPE.GOAWAY, body)


def encode_request_ok(parameters: MessageParameters | None = None) -> bytes:
    """REQUEST_OK (§10.5).

    The subscriber sends this to accept an incoming PUBLISH (PUBLISH_OK). Track Properties are always
    empty in that role, and since draft-20 so are the parameters: a subscriber changes subscription
    parameters with REQUEST_UPDATE, not in PUBLISH_OK (§10.11). The parameter slot stays for the
    publisher-side roles of REQUEST_OK (TRACK_STATUS_OK, REQUEST_UPDATE_OK).
    """
    body = ByteWriter()
    encode_message_parameters(body, parameters)
    return _frame_message(MESSAGE_TYPE.REQUEST_OK, body)


def encode_request_error(error_code: int, reason: str = "", retry_interval: int = 0) -> bytes:
    """REQUEST_ERROR (§10.6) without a Redirect: enough to reject incoming requests."""
    body = ByteWriter()
    body.write_varint(error_code)
    body.write_varint(retry_interval)
    _write_reason_phrase(body, reason)
    return _frame_message(MESSAGE_TYPE.REQUEST_ERROR, body)


# ============================================================================
# Encoders (publisher -> subscriber)
#
# Used by the publish session driver and by the in-memory peers in tests.
# ============================================================================


def encode_subscribe_ok(
    track_alias: int,
    parameters: MessageParameters | None = None,
    track_properties: Sequence[KeyValuePair] = (),
) -> bytes:
    """SUBSCRIBE_OK (§10.8)."""
    body = ByteWriter()
    body.write_varint(track_alias)
    encode_message_parameters(body, parameters)
    encode_key_value_pairs(body, track_properties)
    return _frame_message(MESSAGE_TYPE.SUBSCRIBE_OK, body)


def encode_publish_done(status_code: int, stream_count: int, reason: str = "") -> bytes:
    """PUBLISH_DONE (§10.12)."""
    body = ByteWriter()
    body.write_varint(status_code)
    body.write_varint(stream_count)
    _write_reason_phrase(body, reason)
    return _frame_message(MESSAGE_TYPE.PUBLISH_DONE, body)


def encode_publish_state_notify(parameters: MessageParameters | None = None) -> bytes:
    """PUBLISH_STATE_NOTIFY (§10.10): the publisher's unilateral "these subscription parameters changed"."""
    body = ByteWriter()
    encode_message_parameters(body, parameters)
    return _frame_message(MESSAGE_TYPE.PUBLISH_STATE_NOTIFY, body)


def encode_fetch_ok(
    end_of_track: bool,
    end_location: Location,
    parameters: MessageParameters | None = None,
    track_properties: Sequence[KeyValuePair] = (),
) -> bytes:
    """FETCH_OK (§10.14)."""
    body = ByteWriter()
    body.write_uint8(1 if end_of_track else 0)
    _write_location(body, end_location)
    encode_message_parameters(body, parameters)
    encode_key_value_pairs(body, track_properties)
    return _frame_message(MESSAGE_TYPE.FETCH_OK, body)


def encode_publish(request: Publish) -> bytes:
    """PUBLISH (§10.11)."""
    body = ByteWriter()
    body.write_varint(request.request_id)
    _write_track_namespace(body, request.track_namespace)
    body.write_length_prefixed(utf8_encode(request.track_name))
    body.write_varint(request.track_alias)
    encode_message_parameters(body, request.parameters)
    encode_key_value_pairs(body, request.track_properties)
    return _frame_message(MESSAGE_TYPE.PUBLISH, body)


def encode_publish_namespace(request: PublishNamespace) -> bytes:
    """PUBLISH_NAMESPACE (§10.16): a publisher announcing a namespace."""
    body = ByteWriter()
    body.write_varint(request.request_id)
    _write_track_namespace(body, request.track_namespace)
    encode_message_parameters(body, request.parameters)
    return _frame_message(MESSAGE_TYPE.PUBLISH_NAMESPACE, body)


def encode_namespace(track_namespace_suffix: Sequence[str]) -> bytes:
    """NAMESPACE (§10.19): announce one namespace on an accepted inbound SUBSCRIBE_NAMESPACE request stream.

    The body is the namespace suffix relative to that stream's subscribed prefix and nothing else:
    peers bound-check the tuple against the frame length (moq-lite-rs treats even a zero parameter
    count as a malformed entry and kills the announce stream), so no parameter block is written.
    """
    body = ByteWriter()
    _write_track_namespace(body, track_namespace_suffix)
    return _frame_message(MESSAGE_TYPE.NAMESPACE, body)


def encode_namespace_done(track_namespace_suffix: Sequence[str]) -> bytes:
    """NAMESPACE_DONE (§10.20): retract a NAMESPACE announcement (same body shape)."""
    body = ByteWriter()
    _write_track_namespace(body, track_namespace_suffix)
    return _frame_message(MESSAGE_TYPE.NAMESPACE_DONE, body)


def encode_control_message(message: ControlMessage) -> bytes:
    """Encode any decoded message back to its frame. The per-message encoders above are the primary API."""
    match message:
        case Setup(options=options):
            return encode_setup(options)
        case Goaway(new_session_uri=uri, timeout=timeout):
            if uri:
                body = ByteWriter()
                body.write_length_prefixed(utf8_encode(uri))
                body.write_varint(timeout)
                return _frame_message(MESSAGE_TYPE.GOAWAY, body)

            return encode_goaway(timeout)
        case Subscribe():
            return encode_subscribe(message)
        case TrackStatus():
            return encode_track_status(message)
        case Fetch():
            return encode_fetch(message)
        case SubscribeNamespace():
            return encode_subscribe_namespace(message)
        case SubscribeOk(track_alias=alias, parameters=parameters, track_properties=properties):
            return encode_subscribe_ok(alias, parameters, properties)
        case RequestOk(parameters=parameters, track_properties=properties):
            body = ByteWriter()
            encode_message_parameters(body, parameters)
            encode_key_value_pairs(body, properties)
            return _frame_message(MESSAGE_TYPE.REQUEST_OK, body)
        case RequestError(error_code=code, retry_interval=retry, reason=reason, redirect=redirect):
            if redirect is not None:
                body = ByteWriter()
                body.write_varint(code)
                body.write_varint(retry)
                _write_reason_phrase(body, reason)
                body.write_length_prefixed(utf8_encode(redirect.connect_uri))
                _write_track_namespace(body, redirect.track_namespace)
                body.write_length_prefixed(utf8_encode(redirect.track_name))
                return _frame_message(MESSAGE_TYPE.REQUEST_ERROR, body)

            return encode_request_error(code, reason, retry)
        case RequestUpdate(request_id=request_id, parameters=parameters):
            return encode_request_update(request_id, parameters)
        case PublishStateNotify(parameters=parameters):
            return encode_publish_state_notify(parameters)
        case PublishDone(status_code=status, stream_count=count, reason=reason):
            return encode_publish_done(status, MAX_VARINT_VALUE if count is None else count, reason)
        case Publish():
            return encode_publish(message)
        case PublishNamespace():
            return encode_publish_namespace(message)
        case FetchOk(end_of_track=end, end_location=location, parameters=parameters, track_properties=properties):
            return encode_fetch_ok(end, location, parameters, properties)
        case Namespace(track_namespace_suffix=suffix):
            return encode_namespace(suffix)
        case NamespaceDone(track_namespace_suffix=suffix):
            return encode_namespace_done(suffix)
        case PublishSkipped(track_namespace_suffix=suffix, track_name=name):
            body = ByteWriter()
            _write_track_namespace(body, suffix)
            body.write_length_prefixed(utf8_encode(name))
            return _frame_message(MESSAGE_TYPE.PUBLISH_SKIPPED, body)

    raise TypeError(f"not a control message: {message!r}")


# ============================================================================
# Decoders
# ============================================================================


def _read_redirect(reader: ByteReader) -> Redirect:
    connect_uri = utf8_decode(reader.read_bytes(reader.read_varint()))
    track_namespace = _read_track_namespace(reader)
    track_name = utf8_decode(reader.read_bytes(reader.read_varint()))
    return Redirect(connect_uri=connect_uri, track_namespace=track_namespace, track_name=track_name)


def decode_control_message(frame: ControlMessageFrame) -> ControlMessage:
    """Decode one control-message frame into a typed message.

    Raises :class:`MoqtProtocolError` for unknown message types (§10: "An endpoint that receives an
    unknown message type MUST close the session") and for malformed bodies.
    """
    reader = ByteReader(frame.body)
    end = len(frame.body)

    try:
        message = _decode_control_message_body(frame.type, reader, end)
    except IncompleteInputError:
        # Field reads past the declared frame length surface as IncompleteInputError from
        # ByteReader. Normalize them: the session's stream loops treat only MoqtProtocolError as
        # fatal, and a truncated body must terminate the session (§10) rather than vanish as a read
        # error.
        raise MoqtProtocolError(f"control message 0x{frame.type:x} body is truncated") from None

    if reader.offset != end:
        raise MoqtProtocolError(f"control message 0x{frame.type:x} body length mismatch")

    return message


def _decode_control_message_body(message_type: int, reader: ByteReader, end: int) -> ControlMessage:
    if message_type == MESSAGE_TYPE.SETUP:
        return Setup(options=decode_key_value_pairs(reader, end))

    if message_type == MESSAGE_TYPE.GOAWAY:
        uri_length = reader.read_varint()

        if uri_length > _MAX_NEW_SESSION_URI_LENGTH:
            raise MoqtProtocolError("GOAWAY new session URI exceeds 8192 bytes")

        new_session_uri = utf8_decode(reader.read_bytes(uri_length))
        return Goaway(new_session_uri=new_session_uri, timeout=reader.read_varint())

    if message_type == MESSAGE_TYPE.SUBSCRIBE_OK:
        track_alias = reader.read_varint()
        parameters = decode_message_parameters(reader)
        return SubscribeOk(
            track_alias=track_alias, parameters=parameters, track_properties=decode_key_value_pairs(reader, end)
        )

    if message_type == MESSAGE_TYPE.REQUEST_OK:
        parameters = decode_message_parameters(reader)
        return RequestOk(parameters=parameters, track_properties=decode_key_value_pairs(reader, end))

    if message_type == MESSAGE_TYPE.REQUEST_ERROR:
        error_code = reader.read_varint()
        retry_interval = reader.read_varint()
        reason = _read_reason_phrase(reader)
        redirect = _read_redirect(reader) if error_code == REQUEST_ERROR_CODE.REDIRECT else None
        return RequestError(error_code=error_code, retry_interval=retry_interval, reason=reason, redirect=redirect)

    if message_type == MESSAGE_TYPE.REQUEST_UPDATE:
        request_id = reader.read_varint()
        return RequestUpdate(request_id=request_id, parameters=decode_message_parameters(reader))

    if message_type == MESSAGE_TYPE.PUBLISH_STATE_NOTIFY:
        return PublishStateNotify(parameters=decode_message_parameters(reader))

    if message_type == MESSAGE_TYPE.PUBLISH_DONE:
        status_code = reader.read_varint()
        # A publisher that cannot count its streams MUST send 2^64-1 (§10.12): read that one value
        # as "unknown" rather than failing the session over a legitimate PUBLISH_DONE.
        stream_count = reader.read_count_varint()
        return PublishDone(status_code=status_code, stream_count=stream_count, reason=_read_reason_phrase(reader))

    if message_type == MESSAGE_TYPE.PUBLISH:
        request_id = reader.read_varint()
        track_namespace = _read_track_namespace(reader)
        track_name = utf8_decode(reader.read_bytes(reader.read_varint()))
        track_alias = reader.read_varint()
        parameters = decode_message_parameters(reader)
        return Publish(
            request_id=request_id,
            track_namespace=track_namespace,
            track_name=track_name,
            track_alias=track_alias,
            parameters=parameters,
            track_properties=decode_key_value_pairs(reader, end),
        )

    if message_type == MESSAGE_TYPE.PUBLISH_NAMESPACE:
        request_id = reader.read_varint()
        track_namespace = _read_track_namespace(reader)
        return PublishNamespace(
            request_id=request_id, track_namespace=track_namespace, parameters=decode_message_parameters(reader)
        )

    if message_type == MESSAGE_TYPE.FETCH_OK:
        end_of_track_wire = reader.read_uint8()
        if end_of_track_wire > 1:
            raise MoqtProtocolError(f"invalid FETCH_OK end-of-track value {end_of_track_wire}")

        end_location = _read_location(reader)
        parameters = decode_message_parameters(reader)
        return FetchOk(
            end_of_track=end_of_track_wire == 1,
            end_location=end_location,
            parameters=parameters,
            track_properties=decode_key_value_pairs(reader, end),
        )

    if message_type in (MESSAGE_TYPE.SUBSCRIBE, MESSAGE_TYPE.TRACK_STATUS, MESSAGE_TYPE.FETCH):
        request_id = reader.read_varint()
        track_namespace = _read_track_namespace(reader)
        track_name = utf8_decode(reader.read_bytes(reader.read_varint()))
        parameters = decode_message_parameters(reader)

        if message_type == MESSAGE_TYPE.FETCH:
            return Fetch(
                request_id=request_id, track_namespace=track_namespace, track_name=track_name, parameters=parameters
            )

        if message_type == MESSAGE_TYPE.SUBSCRIBE:
            return Subscribe(
                request_id=request_id, track_namespace=track_namespace, track_name=track_name, parameters=parameters
            )

        return TrackStatus(
            request_id=request_id, track_namespace=track_namespace, track_name=track_name, parameters=parameters
        )

    if message_type == MESSAGE_TYPE.SUBSCRIBE_NAMESPACE:
        request_id = reader.read_varint()
        track_namespace_prefix = _read_track_namespace(reader)
        return SubscribeNamespace(
            request_id=request_id,
            track_namespace_prefix=track_namespace_prefix,
            parameters=decode_message_parameters(reader),
        )

    if message_type == MESSAGE_TYPE.NAMESPACE:
        return Namespace(track_namespace_suffix=_read_track_namespace(reader))

    if message_type == MESSAGE_TYPE.NAMESPACE_DONE:
        return NamespaceDone(track_namespace_suffix=_read_track_namespace(reader))

    if message_type == MESSAGE_TYPE.PUBLISH_SKIPPED:
        track_namespace_suffix = _read_track_namespace(reader)
        track_name = utf8_decode(reader.read_bytes(reader.read_varint()))
        return PublishSkipped(track_namespace_suffix=track_namespace_suffix, track_name=track_name)

    raise MoqtProtocolError(f"unknown control message type 0x{message_type:x}")


__all__ = [
    "MAX_CONTROL_MESSAGE_LENGTH",
    "MESSAGE_TYPE",
    "MOQT_PROTOCOL_ID",
    "PARAMETER_TYPE",
    "PUBLISH_DONE_STATUS",
    "REQUEST_ERROR_CODE",
    "SETUP_OPTION",
    "STREAM_RESET_CODE",
    "TRACK_PROPERTY",
    "AbsoluteLocationFilter",
    "ControlMessage",
    "ControlMessageDeframer",
    "ControlMessageFrame",
    "Fetch",
    "FetchOk",
    "Goaway",
    "GroupOrder",
    "KeyValuePair",
    "Location",
    "LocationFilter",
    "MessageParameters",
    "Namespace",
    "NamespaceDone",
    "NextObjectFilter",
    "NoLocationFilter",
    "ParameterScope",
    "Publish",
    "PublishDone",
    "PublishNamespace",
    "PublishSkipped",
    "PublishStateNotify",
    "RangeFilter",
    "Redirect",
    "RelativeGroupFilter",
    "RequestError",
    "RequestOk",
    "RequestUpdate",
    "Setup",
    "Subscribe",
    "SubscribeNamespace",
    "SubscribeOk",
    "TrackNamespace",
    "TrackStatus",
    "compare_locations",
    "decode_control_message",
    "decode_key_value_pairs",
    "decode_location_filter",
    "decode_message_parameters",
    "encode_auth_token_use_value",
    "encode_control_message",
    "encode_fetch",
    "encode_fetch_ok",
    "encode_goaway",
    "encode_key_value_pairs",
    "encode_location_filter",
    "encode_message_parameters",
    "encode_namespace",
    "encode_namespace_done",
    "encode_publish",
    "encode_publish_done",
    "encode_publish_namespace",
    "encode_publish_state_notify",
    "encode_request_error",
    "encode_request_ok",
    "encode_request_update",
    "encode_setup",
    "encode_subscribe",
    "encode_subscribe_namespace",
    "encode_subscribe_ok",
    "encode_track_status",
    "is_retryable_publish_done_status",
    "is_retryable_request_error_code",
]
