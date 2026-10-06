"""MOQT session-termination error codes (moq-transport draft-22 §12.2) and the codec error types.

``SESSION_ERROR`` values are used as the close code when terminating the transport session and as
the ``code`` carried by :class:`MoqtProtocolError`.
"""

from __future__ import annotations

from enum import IntEnum


class SESSION_ERROR(IntEnum):
    """Session-termination codes (§12.2)."""

    NO_ERROR = 0x0
    INTERNAL_ERROR = 0x1
    UNAUTHORIZED = 0x2
    PROTOCOL_VIOLATION = 0x3
    INVALID_REQUEST_ID = 0x4
    DUPLICATE_TRACK_ALIAS = 0x5
    KEY_VALUE_FORMATTING_ERROR = 0x6
    INVALID_PATH = 0x8
    MALFORMED_PATH = 0x9
    GOAWAY_TIMEOUT = 0x10
    CONTROL_MESSAGE_TIMEOUT = 0x11
    DATA_STREAM_TIMEOUT = 0x12
    AUTH_TOKEN_CACHE_OVERFLOW = 0x13
    DUPLICATE_AUTH_TOKEN_ALIAS = 0x14
    MALFORMED_AUTH_TOKEN = 0x16
    UNKNOWN_AUTH_TOKEN_ALIAS = 0x17
    EXPIRED_AUTH_TOKEN = 0x18
    INVALID_AUTHORITY = 0x19
    MALFORMED_AUTHORITY = 0x1A
    TOO_MANY_REQUEST_UPDATES = 0x1B


class IncompleteInputError(ValueError):
    """A read ran past the end of the available bytes.

    The synchronous readers raise it when a buffer ends before a field completes, and
    :class:`~moq_core.bytes.StreamReader` raises it when a stream ends (FIN) mid-field. It is the
    Python counterpart of the ``RangeError`` the TypeScript codec throws: the deframer treats it as
    "wait for more bytes", the control-message decoder normalizes it into a protocol error, and the
    data-stream loops treat it as a truncated stream rather than a session error.
    """


class MoqtProtocolError(Exception):
    """A wire-protocol violation detected while encoding or decoding MOQT data.

    ``code`` is the session-termination code (§12.2) the session driver should close the transport
    with; it defaults to ``PROTOCOL_VIOLATION``.
    """

    def __init__(self, message: str, code: int = SESSION_ERROR.PROTOCOL_VIOLATION) -> None:
        super().__init__(message)
        self.code = code


def is_moqt_protocol_error(value: object) -> bool:
    """True when ``value`` is a :class:`MoqtProtocolError`: the session drivers close the transport on those alone."""
    return isinstance(value, MoqtProtocolError)


__all__ = ["SESSION_ERROR", "IncompleteInputError", "MoqtProtocolError", "is_moqt_protocol_error"]
