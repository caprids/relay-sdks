"""What the SDK admits from the caller before anything reaches the core.

The wire-facing values (namespace, track name, properties, payload) are each checked in their
original container and copied once. Both roles use the same rules, so the publisher and the
subscriber cannot drift apart, and the "Arguments" table in ``spec/behavior.md`` is the list these
functions implement.

Errors keep their idiomatic types: a wrong type or shape is a ``TypeError``; a value of the right
type outside its bound is a ``ValueError``. Nothing here is a ``RelayError``: these are programming
errors, raised before any connection state is consulted.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence

from moq_core import MAX_NAMESPACE_FIELDS, MAX_OBJECT_PAYLOAD_LENGTH, MAX_TRACK_PROPERTIES_LENGTH, KeyValuePair
from moq_core.bytes import ByteWriter
from moq_core.control_messages import encode_key_value_pairs
from moq_core.varint import MAX_VARINT_VALUE

#: A key-value pair's byte value carries a 16-bit length on the wire (draft-ietf-moq-transport-20
#: §1.4.2), pinned in ``spec/protocol.json``. The core enforces the same bound while encoding.
MAX_PROPERTY_VALUE_LENGTH = 0xFFFF

#: moq-relay drops an object whose encoded property block is larger than this (``MAX_OBJECT_EXTENSIONS``
#: since 0.17.0, an implementation limit rather than a draft one), so a write refuses it up front.
#: Pinned in ``spec/protocol.json`` as ``relay.limits.maxObjectPropertiesBytes``.
MAX_RELAY_OBJECT_PROPERTIES_LENGTH = 64 * 1024

#: moq-relay refuses every subscriber to a track with more track properties than this (``MAX_PROPERTIES``
#: in 0.17.0, REQUEST_ERROR 18 ``too many``) and the publisher hears nothing, so registration refuses it.
#: Pinned in ``spec/protocol.json`` as ``relay.limits.maxTrackProperties``.
MAX_RELAY_TRACK_PROPERTIES = 64

#: Track property values draft-ietf-moq-transport-20 restricts (sections 12.4 and 12.5), pinned in
#: ``spec/protocol.json`` under ``trackProperties``. moq-relay refuses every subscriber to a track
#: that breaks them, so registration checks them.
DEFAULT_PUBLISHER_PRIORITY = 0x0E
MAX_DEFAULT_PUBLISHER_PRIORITY = 255
DEFAULT_PUBLISHER_GROUP_ORDER = 0x22
DEFAULT_PUBLISHER_GROUP_ORDERS = (1, 2)

_NOT_A_SEQUENCE_OF_FIELDS = (str, bytes, bytearray, memoryview)


def namespace_fields(namespace: object) -> tuple[str, ...]:
    """Check a namespace in its original container and return a fresh, immutable copy of its fields."""
    if isinstance(namespace, _NOT_A_SEQUENCE_OF_FIELDS) or not isinstance(namespace, Sequence):
        raise TypeError("A namespace must be a sequence of non-empty string fields")
    fields = tuple(namespace)
    if not fields or any(not isinstance(field, str) or not field for field in fields):
        raise TypeError("A namespace must be a sequence of non-empty string fields")
    if len(fields) > MAX_NAMESPACE_FIELDS:
        raise ValueError(f"A namespace must contain at most {MAX_NAMESPACE_FIELDS} fields")
    return fields


def track_name(track: object) -> str:
    """Check a track name: the string part of a full track name."""
    if not isinstance(track, str) or not track:
        raise TypeError("A track name must be a non-empty string")
    return track


def payload_bytes(payload: object) -> bytes:
    """Check a write payload and copy its bytes, before any slot or stream is taken.

    A payload is anything that exposes the buffer protocol (``bytes``, ``bytearray``,
    ``memoryview``, an array, ...). A ``str`` or a list of integers is not a byte sequence.
    """
    try:
        view = memoryview(payload)  # ty: ignore[invalid-argument-type]  # finding out is the point
    except TypeError as error:
        raise TypeError("Payload must be bytes-like (bytes, bytearray, memoryview or another buffer)") from error
    if view.nbytes > MAX_OBJECT_PAYLOAD_LENGTH:
        raise ValueError("Payload exceeds the 16 MiB object limit")
    # bytes is immutable, so it is already its own copy.
    return payload if type(payload) is bytes else view.tobytes()


def track_property_pairs(properties: object) -> list[KeyValuePair]:
    """Check track properties and copy any byte values, so later mutation cannot leak in.

    Integer types and values are bounded by what the core's varint carries, ``2**64 - 1``, and byte
    values by their 16-bit wire length. The list rides in every SUBSCRIBE_OK, so its encoding,
    measured here with the core's own encoder, must fit the room that control message leaves; the
    relay also caps how many pairs it reads and refuses the values draft-20 rules out. A track that
    breaks any of these would register and then never answer a subscription, with the publisher's
    writes timing out, so each is an argument error here instead.
    """
    pairs, length = _measured_pairs(properties)
    for index, prop in enumerate(pairs):
        priority = prop.value if prop.type == DEFAULT_PUBLISHER_PRIORITY else None
        if isinstance(priority, int) and priority > MAX_DEFAULT_PUBLISHER_PRIORITY:
            raise ValueError(
                f"Property {index} (Default Publisher Priority, 0x0E) must be at most {MAX_DEFAULT_PUBLISHER_PRIORITY}"
            )
        if prop.type == DEFAULT_PUBLISHER_GROUP_ORDER and prop.value not in DEFAULT_PUBLISHER_GROUP_ORDERS:
            raise ValueError(
                f"Property {index} (Default Publisher Group Order, 0x22) must be 1 (ascending) or 2 (descending)"
            )
    if len(pairs) > MAX_RELAY_TRACK_PROPERTIES:
        raise ValueError(
            f"Track properties hold {len(pairs)} pairs, over the relay's {MAX_RELAY_TRACK_PROPERTIES}-pair limit,"
            " so the relay would refuse every subscriber"
        )
    if length > MAX_TRACK_PROPERTIES_LENGTH:
        raise ValueError(
            f"Track properties encode to {length} bytes, over the {MAX_TRACK_PROPERTIES_LENGTH} bytes SUBSCRIBE_OK"
            " has room for, so no subscription could be answered; carry larger data in a track's objects"
        )
    return pairs


def object_property_pairs(properties: object) -> list[KeyValuePair]:
    """Check one object's properties like :func:`track_property_pairs`, against the relay's object bound.

    The relay would drop an object whose block is over its limit, silently for the subscriber, so the
    write fails here instead. The relay forwards no object property but LOC Timestamp anyway.
    """
    pairs, length = _measured_pairs(properties)
    if length > MAX_RELAY_OBJECT_PROPERTIES_LENGTH:
        raise ValueError(
            f"Object properties encode to {length} bytes, over the relay's {MAX_RELAY_OBJECT_PROPERTIES_LENGTH}-byte"
            " limit, so the relay would drop the object; it forwards only LOC Timestamp (0x10), so carry larger"
            " data in the payload"
        )
    return pairs


def _measured_pairs(properties: object) -> tuple[list[KeyValuePair], int]:
    """Check and copy a property list, returning it with its encoded length."""
    if properties is None:
        return [], 0
    if isinstance(properties, _NOT_A_SEQUENCE_OF_FIELDS) or not isinstance(properties, Iterable):
        raise TypeError("Properties must be a sequence of KeyValuePair")
    pairs: list[KeyValuePair] = []
    for index, prop in enumerate(properties):
        try:
            kind, value = prop.type, prop.value
        except AttributeError:
            raise TypeError(f"Property {index} must be a KeyValuePair") from None
        if not _is_varint(kind):
            raise TypeError(f"Property {index} type must be a non-negative integer")
        if kind % 2 == 0:
            if not _is_varint(value):
                raise TypeError(f"Property {index} value must be a non-negative integer for an even type")
            pairs.append(KeyValuePair(kind, value))
            continue
        if isinstance(value, int):
            raise TypeError(f"Property {index} value must be bytes-like for an odd type")
        try:
            view = memoryview(value)
        except TypeError as error:
            raise TypeError(f"Property {index} value must be bytes-like for an odd type") from error
        if view.nbytes > MAX_PROPERTY_VALUE_LENGTH:
            raise ValueError(f"Property {index} value exceeds the {MAX_PROPERTY_VALUE_LENGTH}-byte limit")
        pairs.append(KeyValuePair(kind, view.tobytes()))
    if not pairs:
        return pairs, 0
    encoded = ByteWriter()
    encode_key_value_pairs(encoded, pairs)
    return pairs, encoded.length


def _is_varint(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= MAX_VARINT_VALUE
