"""Relay endpoints: the Endpoint rule every connect call checks, and the room-rooted endpoint for a room
("Room-rooted endpoints" in ``spec/behavior.md``).

Pure: it builds strings and connects nothing.
"""

import re
from collections.abc import Sequence
from typing import overload
from urllib.parse import parse_qsl, urlsplit, urlunsplit

from ._validation import namespace_fields

# The characters a room field may hold: what a URL path carries unchanged in every parser and
# moq-relay reads back as the same field. The relay does not percent-decode the path, so a field that
# would need encoding cannot be rooted at, and `*` fails the connection (spec/relay-interop.md,
# "Endpoint paths").
_ROOM_FIELD = re.compile(r"[A-Za-z0-9\-._~!$&'()+,;=:@]+")

DEFAULT_ENDPOINT = "https://relay.mux.global"
"""The default Relay endpoint pinned in ``spec/protocol.json``."""


def _parse_endpoint(url: object) -> tuple[str, bool]:
    """Validate the endpoint and return it normalized, plus whether it already carries a ``jwt``.

    Not a string, or not an absolute URL (no scheme, or an ``https`` URL with no host), is a
    ``TypeError``: the wrong shape, as the TypeScript URL parser reports it. A URL that parses but
    is not one Relay accepts is a ``ValueError``: a disallowed value.
    """
    if not isinstance(url, str):
        raise TypeError("Relay endpoints must be a URL string")

    try:
        parts = urlsplit(url)
        hostname = parts.hostname
    except ValueError as error:
        raise TypeError("Relay endpoints must be a valid absolute URL") from error

    if not parts.scheme:
        raise TypeError("Relay endpoints must be a valid absolute URL")

    if parts.scheme != "https":
        raise ValueError("Relay endpoints must use HTTPS")

    if parts.fragment or "@" in parts.netloc:
        raise ValueError("Relay endpoints cannot contain fragments or userinfo")

    if not hostname:
        raise TypeError("Relay endpoints must name a host")

    jwts = [value for key, value in parse_qsl(parts.query, keep_blank_values=True) if key == "jwt"]

    # A present jwt wins over get_token, so an empty one would connect with no credential at all.
    if "" in jwts:
        raise ValueError(
            "Relay endpoint has an empty jwt query parameter; put a token in it, or remove it and supply get_token"
        )

    has_jwt = bool(jwts)
    normalized = urlunsplit((parts.scheme, parts.netloc, parts.path or "/", parts.query, ""))

    # urlunsplit drops an empty query, but `?` with nothing after it is a present query that PATH keeps (TA-6).
    if not parts.query and "?" in url:
        normalized += "?"

    return normalized, has_jwt


@overload
def room_endpoint(room: Sequence[str], /) -> str: ...
@overload
def room_endpoint(endpoint: str, room: Sequence[str], /) -> str: ...
def room_endpoint(*args: object) -> str:
    """Root an endpoint at a room.

    The result is the endpoint with each room field appended to its path, after any path it has and
    without its trailing slashes, and with its query kept, a ``jwt`` parameter included. A
    connection to it names namespaces relative to the room. Without an endpoint it roots
    ``DEFAULT_ENDPOINT``::

        room_endpoint(["acme", "rooms", "42"])
        # 'https://relay.mux.global/acme/rooms/42'
        room_endpoint("https://relay.example", ["acme", "rooms", "42"])
        # 'https://relay.example/acme/rooms/42'

    The endpoint follows the connect calls' rules and the room the namespace rules. Nothing is
    percent-encoded: a room field holds only ``A``-``Z``, ``a``-``z``, ``0``-``9`` and
    ``-._~!$&'()+,;=:@``, and is not ``.`` or ``..``. A namespace with any other field is reached
    from the bare host instead.

    :raises TypeError: an endpoint that is not an absolute URL string, or a room that is not a list
        or tuple of non-empty string fields.
    :raises ValueError: an endpoint that is not ``https``, has a fragment or userinfo, or an empty
        ``jwt``; a room of more than 32 fields; a room field with any other character, or ``.`` or
        ``..``.
    """
    # One argument is the room alone; two are the endpoint and the room, as before the default.
    if len(args) == 1:
        endpoint, room = DEFAULT_ENDPOINT, args[0]
    elif len(args) == 2:
        endpoint, room = args
    else:
        raise TypeError(f"room_endpoint takes a room, or an endpoint and a room, not {len(args)} arguments")
    normalized, _ = _parse_endpoint(endpoint)
    fields = namespace_fields(room)

    for index, field in enumerate(fields):
        if not _ROOM_FIELD.fullmatch(field):
            raise ValueError(
                f"Room field {index} ({field!r}) has a character a room-rooted endpoint cannot carry: "
                "only A-Z, a-z, 0-9 and -._~!$&'()+,;=:@ reach the relay as the same field. "
                "Reach this namespace from the bare host instead"
            )
        if field in (".", ".."):
            raise ValueError(
                f"Room field {index} is {field!r}, which a URL folds into the path; "
                "reach this namespace from the bare host instead"
            )

    parts = urlsplit(normalized)
    path = parts.path.rstrip("/") + "/" + "/".join(fields)
    rooted = urlunsplit((parts.scheme, parts.netloc, path, parts.query, ""))

    # urlunsplit drops an empty query, but `?` with nothing after it is a present query, kept as given.
    return rooted + "?" if normalized.endswith("?") and not parts.query else rooted
