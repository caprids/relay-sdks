"""The transport layer's own exception, and how a caller's abort reason becomes an application error code."""

from __future__ import annotations

from typing import Literal

_MAX_SAFE_CODE = 2**53 - 1
"""The largest code a stream error reports; a larger one is reported as unknown (TA-22)."""

_H3_FIRST_CODE = 0x52E4A40FA8DB
"""Where draft-ietf-webtrans-http3 maps application error code 0 into the HTTP/3 error-code space."""
_H3_LAST_CODE = 0x52E5AC983162
"""Where it maps 2^32-1, the last application error code."""


class QuicTransportError(Exception):
    """A QUIC stream or session failure, classified by its shape (``spec/transport-adapter.md`` TA-22).

    ``source`` is ``"stream"`` when the failure belongs to one stream: the peer reset it (TA-19) or
    stopped it (TA-20), and ``stream_error_code`` is the peer's code, or ``None`` when it is unknown or
    above 2^53-1, never 0. A stop is a stream error raised on the send half: where it surfaces makes it a
    stop, not a subclass. ``source`` is ``"session"`` when the session is over, or the dial failed, and
    the operation failed because of it (TA-23). ``close_code`` and ``reason`` are set when the error
    reports the peer's application close (TA-24). ``source`` is ``None`` for a failure that is neither,
    which leaves the session usable: an exhausted stream limit (TA-21), or a write to a stream this end
    already closed or reset.

    Classify by those fields (:func:`moq_core.transport.is_stream_error`,
    :func:`~moq_core.transport.is_session_error`), never by this class. It is the transport layer's own
    exception, not a :class:`caprid_relay.RelayError`: a Relay operation that fails on the transport raises
    ``TRANSPORT_ERROR`` with this chained as its cause (``UNAUTHORIZED`` when the peer closed the session
    for its credentials).
    """

    def __init__(
        self,
        message: str,
        *,
        source: Literal["stream", "session"] | None = None,
        stream_error_code: int | None = None,
        close_code: int | None = None,
        reason: str | None = None,
    ) -> None:
        super().__init__(message)
        self.source = source
        self.stream_error_code = (
            stream_error_code if stream_error_code is not None and 0 <= stream_error_code <= _MAX_SAFE_CODE else None
        )
        self.close_code = close_code
        self.reason = reason


def _stream_error(action: Literal["reset", "stopped"], code: int | None) -> QuicTransportError:
    """The peer reset or stopped a stream with ``code``, ``None`` when unknown (TA-19, TA-20)."""
    detail = "" if code is None else f" (code {code})"
    return QuicTransportError(f"peer {action} the stream{detail}", source="stream", stream_error_code=code)


def _session_error(message: str) -> QuicTransportError:
    """The session is over, or the dial failed (TA-23)."""
    return QuicTransportError(message, source="session")


def _error_code_of(reason: object) -> int:
    """The code a reset or a stop carries for ``reason`` (TA-17): its ``stream_error_code``, else an integer reason.

    Either must be an integer from 0 to 2^32-1, and the code is 0 otherwise. The write slot's ``StreamReset``
    and a :class:`QuicTransportError` from the peer both carry ``stream_error_code``.
    """
    stream_error_code = getattr(reason, "stream_error_code", None)
    if (
        isinstance(stream_error_code, int)
        and not isinstance(stream_error_code, bool)
        and 0 <= stream_error_code <= 0xFFFFFFFF
    ):
        return stream_error_code

    if isinstance(reason, int) and not isinstance(reason, bool) and 0 <= reason <= 0xFFFFFFFF:
        return reason

    return 0


def _to_h3_error_code(code: int) -> int:
    """The HTTP/3 error code that carries the 32-bit application error ``code``, as draft-ietf-webtrans-http3 maps it.

    Every 31st code in the range is a reserved GREASE code point, so each 30 codes skip one.
    """
    if code < 0 or code > 0xFFFFFFFF:
        raise ValueError("application error codes are 32-bit unsigned integers")

    return _H3_FIRST_CODE + code + code // 0x1E


def _from_h3_error_code(code: int) -> int | None:
    """The application error code an HTTP/3 error ``code`` carries; ``None`` outside the range or on a GREASE slot."""
    if code < _H3_FIRST_CODE or code > _H3_LAST_CODE:
        return None

    shifted = code - _H3_FIRST_CODE

    if shifted % 0x1F == 0x1E:
        return None

    return shifted - shifted // 0x1F


def _from_native_wire_code(code: int) -> int | None:
    """The application's code for a reset or a stop that carried ``code`` on native QUIC (TA-27).

    A code in the HTTP/3 range is decoded, and is ``None`` on a GREASE slot; any other code is the peer's as it is.
    """
    if _H3_FIRST_CODE <= code <= _H3_LAST_CODE:
        return _from_h3_error_code(code)

    return code
