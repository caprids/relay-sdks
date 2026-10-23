"""The room-rooted endpoint for a room ("Room-rooted endpoints" in ``spec/behavior.md``).

Pure: it builds a string and connects nothing.
"""

import re
from collections.abc import Sequence
from urllib.parse import urlsplit, urlunsplit

from ._validation import namespace_fields
from .connect_session import _parse_endpoint

# The characters a room field may hold: what a URL path carries unchanged in every parser and
# moq-relay reads back as the same field. The relay does not percent-decode the path, so a field that
# would need encoding cannot be rooted at, and `*` fails the connection (spec/relay-interop.md,
# "Endpoint paths").
_ROOM_FIELD = re.compile(r"[A-Za-z0-9\-._~!$&'()+,;=:@]+")


def room_endpoint(endpoint: str, room: Sequence[str]) -> str:
    """Root an endpoint at a room.

    The result is the endpoint with each room field appended to its path, after any path it has and
    without its trailing slashes, and with its query kept, a ``jwt`` parameter included. A
    connection to it names namespaces relative to the room::

        room_endpoint("https://relay.mux.global", ["acme", "rooms", "42"])
        # 'https://relay.mux.global/acme/rooms/42'

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
