"""MSF source URL composition and UTF-8 namespace escaping."""

import re
from collections.abc import Sequence
from urllib.parse import quote, urlsplit, urlunsplit

from .loc import CATALOG_TRACK


def encode_name_component(value: str) -> str:
    """MSF namespace/name string form: ``[a-zA-Z0-9_]`` literal, every other UTF-8 byte as ``.xx`` lowercase hex."""
    out: list[str] = []

    for byte in value.encode("utf-8"):
        char = chr(byte)

        if char.isascii() and (char.isalnum() or char == "_"):
            out.append(char)
        else:
            out.append(f".{byte:02x}")

    return "".join(out)


def encode_namespace_name(namespace: Sequence[str], track_name: str) -> str:
    """The ``ns1-ns2--name`` identifier that follows ``#msf:`` in a source URL (msf-01 §11.1.2)."""
    if isinstance(namespace, str):
        raise ValueError("namespace must be a sequence of fields, not a string")
    return "-".join(encode_name_component(field) for field in namespace) + "--" + encode_name_component(track_name)


def compose_player_url(
    relay_url: str, namespace: Sequence[str], *, token: str | None = None, track_name: str = CATALOG_TRACK
) -> str:
    """Compose the ``moqt://`` MSF source URL a Video.js MoQ player takes, the inverse of its parser.

    The relay origin may be written as ``https://`` or ``moqt://``; an existing path and query
    survive. The token, when given, rides the ``c4m`` fragment parameter, which the player moves onto
    the connect URL's ``jwt`` query parameter.
    """
    if not re.match(r"^(https|moqt)://", relay_url, re.IGNORECASE) or re.search(r"[\x00-\x20\x7f\\]", relay_url):
        raise ValueError("relay URL must be an absolute https:// or moqt:// URL")
    parts = urlsplit(relay_url)
    # Accessing port validates its range and syntax; urlsplit otherwise defers that check.
    _ = parts.port

    if parts.scheme not in ("https", "moqt"):
        raise ValueError("relay URL must be https:// or moqt://")

    if "#" in relay_url:
        raise ValueError("relay URL must not carry a fragment")

    if isinstance(namespace, str):
        raise ValueError("namespace must be a sequence of fields, not a string")
    fields = list(namespace)
    if not fields or any(not field for field in fields):
        raise ValueError("namespace is empty")

    if not parts.hostname or parts.username is not None or parts.password is not None:
        raise ValueError("relay URL must have a host and no credentials")
    if not track_name:
        raise ValueError("track name is empty")

    base = urlunsplit(("moqt", parts.netloc, parts.path or "/", parts.query, ""))
    fragment = f"msf:{encode_namespace_name(fields, track_name)}"

    if token:
        fragment += f"&c4m={quote(token, safe='')}"

    return f"{base}#{fragment}"
