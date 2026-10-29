"""Shared transport and authentication lifetime for the subscribe and publish session drivers.

One deadline covers token resolution, the transport handshake, and MOQT SETUP. The optional
cancellation token governs the connection's entire lifetime, not only setup. Every connection
exposes a ``closed`` future carrying the error that ended it, if any.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import inspect
import json
import math
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Generic, Literal, NamedTuple, Protocol, TypeAlias, TypeVar
from urllib.parse import parse_qs, parse_qsl, urlencode, urlsplit, urlunsplit

from moq_core import MOQT_PROTOCOL_IDS, Closed, Goaway, KeyValuePair, MoqtProtocolId, MoqtTransport
from moq_core._tasks import call_later, observe, spawn
from moq_core.control_messages import SETUP_OPTION, resolve_moqt_protocol
from moq_core.transport import transport_ended

from .cancellation import CancellationToken
from .errors import RelayError, RelayErrorPhase, _peer_close, _refusal, _transport_error, _unauthorized
from .room_endpoint import DEFAULT_ENDPOINT, _parse_endpoint, room_endpoint


@dataclass(frozen=True, slots=True)
class RelayCredential:
    """A token together with when the relay stops honoring it.

    moq-relay ends a session at its grant's deadline, which is the token's expiry, and a live session
    cannot take a new token; supervised connections replace theirs before then.
    """

    token: str
    """The token, carried in the endpoint's ``jwt`` query parameter. An empty one sends no token."""
    expires_at: datetime
    """When the relay ends a session the token admitted: the JWT's ``exp``, or the grant's deadline if earlier.

    Must be timezone-aware.
    """

    def __post_init__(self) -> None:
        if not isinstance(self.token, str):
            raise TypeError("RelayCredential.token must be a string")

        if not isinstance(self.expires_at, datetime):
            raise TypeError("RelayCredential.expires_at must be a datetime")

        if self.expires_at.tzinfo is None or self.expires_at.utcoffset() is None:
            raise ValueError("RelayCredential.expires_at must be timezone-aware")


TokenProvider = Callable[[], str | RelayCredential | Awaitable[str | RelayCredential | None] | None]
"""Resolve a connection token, synchronously or asynchronously. ``None`` or ``""`` sends no token.

Return a :class:`RelayCredential` to say when the token expires; for a bare token the connection
reads the JWT's ``exp`` claim, without verifying it. The provider runs once per connection.
"""


def _not_json(constant: str) -> object:
    raise ValueError(f"{constant} is not JSON")


def _jwt_claims(token: str) -> dict[str, object] | None:
    """A JWT's claims, read without verifying the token.

    The relay verifies the token; the SDK only reads when the session will end and, with no endpoint,
    where to dial. Anything that is not a JWT with a JSON object payload has none. Decoding is as
    strict as the TypeScript SDK's: no characters outside base64, and no ``NaN`` or ``Infinity``.
    """
    parts = token.split(".")

    if len(parts) != 3 or not parts[1]:
        return None

    try:
        payload = base64.b64decode(parts[1] + "=" * (-len(parts[1]) % 4), altchars=b"-_", validate=True)
        claims = json.loads(payload.decode("utf-8"), parse_constant=_not_json)
    except (ValueError, binascii.Error, UnicodeDecodeError):
        return None

    return claims if isinstance(claims, dict) else None


def _claims_expiry(claims: dict[str, object] | None) -> datetime | None:
    """The numeric ``exp`` claim as an aware UTC datetime; anything else has no known expiry."""
    exp = claims.get("exp") if claims is not None else None

    if not isinstance(exp, int | float) or isinstance(exp, bool):
        return None

    # A huge integer `exp` overflows float conversion, in isfinite as in fromtimestamp: no known expiry either.
    try:
        if not math.isfinite(exp):
            return None
        return datetime.fromtimestamp(exp, UTC)
    except (OverflowError, OSError, ValueError):
        return None


def _no_root(reason: str) -> str:
    """Why a connection given no endpoint has none to dial, and what to do instead."""
    return (
        f"With no url, the endpoint is rooted at the token's root claim, but {reason}; "
        "pass url to choose the endpoint yourself"
    )


def _token_rooted_endpoint(token: str, claims: dict[str, object] | None) -> str:
    """``DEFAULT_ENDPOINT`` rooted at the token's ``root`` claim, for a connection given no endpoint.

    The root's fields, without its leading and trailing ``/``, follow ``room_endpoint``'s rule, and an
    empty root is the whole relay.
    """
    if not token:
        raise TypeError(_no_root("get_token returned no token"))

    if claims is None:
        raise TypeError(_no_root("the token is not a JWT with a JSON object payload"))

    root = claims.get("root")

    if not isinstance(root, str):
        raise TypeError(_no_root("the token has no string root claim"))

    room = root.strip("/")

    if not room:
        return _parse_endpoint(DEFAULT_ENDPOINT)[0]

    try:
        return room_endpoint(DEFAULT_ENDPOINT, room.split("/"))
    except (TypeError, ValueError) as error:
        raise type(error)(_no_root(f"its root cannot be an endpoint path: {error}")) from error


@dataclass(frozen=True, slots=True)
class _DialTarget:
    """Where a connection dials, and when its token expires."""

    endpoint: str
    """The endpoint before the provider's token was added."""
    url: str
    """The endpoint with the provider's token in ``jwt``."""
    expires_at: datetime | None


class _Issued(NamedTuple):
    """A token the provider returned, and the expiry it gave, if any."""

    token: str
    expires_at: datetime | None


def _dial_target(given: str | None, issued: _Issued | None) -> _DialTarget:
    """The endpoint as given, or with none ``DEFAULT_ENDPOINT`` rooted at the token's ``root`` claim.

    ``issued`` is the provider's token and the expiry it gave, if it ran; without it, the token is the
    ``jwt`` already on the endpoint, if any ("Token-rooted endpoint" in ``spec/behavior.md``).
    """
    token = issued.token if issued is not None else parse_qs(urlsplit(given or "").query).get("jwt", [""])[0]
    claims = _jwt_claims(token)
    endpoint = given if given is not None else _token_rooted_endpoint(token, claims)
    url = _with_token(endpoint, token) if issued is not None and token else endpoint
    expires_at = issued.expires_at if issued is not None else None
    return _DialTarget(endpoint, url, expires_at if expires_at is not None else _claims_expiry(claims))


Framing: TypeAlias = Literal["webtransport", "native-quic", "websocket"]
"""What a transport carries: a WebTransport session, MOQT directly on a QUIC connection, or QMux over a
WebSocket (TA-1). A WebSocket carries the path and token in its upgrade request, as a WebTransport session does."""


@dataclass(frozen=True, slots=True)
class ConnectingTransport:
    """What a transport factory hands back: the transport, an awaitable for its handshake, and its framing."""

    transport: MoqtTransport
    ready: Awaitable[None]
    framing: Framing = "webtransport"
    """``"native-quic"`` makes the connection send the endpoint's PATH and AUTHORITY in SETUP (TA-1, TA-6)."""


RelayTransportFactory = Callable[[str, Sequence[str]], ConnectingTransport]
"""Supply a transport for ``(url, protocols)``.

``protocols`` are the MOQT protocol IDs to offer, in preference order: ``moqt-22``, then ``moqt-20``
as a fallback. A transport reports the one the peer selected as ``protocol_id`` (TA-8).

Native QUIC on aioquic (``caprid_relay.quic.connect_quic``) by default; tests supply in-memory pairs.
"""

DEFAULT_CONNECT_TIMEOUT = 10.0
DEFAULT_REQUEST_TIMEOUT = 10.0


@dataclass(frozen=True, slots=True, kw_only=True)
class ConnectRelayOptions:
    url: str | None = None
    """Relay's ``https:`` endpoint, including any endpoint path, which SETUP carries on native QUIC.

    ``None`` dials ``DEFAULT_ENDPOINT`` rooted at the token's ``root`` claim, read without verifying the
    token: the URL ``room_endpoint`` builds from that root. ``get_token`` is then required, and a token
    that names no usable root is a ``TypeError`` or ``ValueError`` before the transport is used.
    """
    get_token: TokenProvider | None = None
    """Resolve a connection token. Relay carries it in the endpoint's ``jwt`` query parameter.

    An explicit ``jwt`` in the URL wins, and the provider is not called in that case. An empty
    ``jwt`` in the URL is a ``ValueError``, raised before the provider or the transport is used.
    """
    cancellation: CancellationToken | None = None
    """Cancelling closes the connection, including after the connect call has returned."""
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT
    """Seconds for token resolution, transport connection, and protocol SETUP together."""
    create_transport: RelayTransportFactory | None = None


class _Session(Protocol):
    @property
    def ready(self) -> asyncio.Future[None]: ...

    def destroy(self, error: BaseException | None = None) -> None: ...


T = TypeVar("T", bound=_Session)

ClosureObserver: TypeAlias = Callable[[BaseException | None], None]
"""Told the error that ended the connection, ``None`` after a clean shutdown."""


@dataclass(slots=True)
class _Connecting(Generic[T]):
    """What ``establish()`` has produced so far, so ``finish()`` can tear down exactly that."""

    transport: MoqtTransport | None = None
    session: T | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class LifetimeCallbacks:
    """The session callbacks the lifetime owns: closure and GOAWAY both end the connection."""

    on_closed: Callable[[Closed], None]
    on_goaway: Callable[[Goaway], None]


class RelaySessionLifetime(Generic[T]):
    """An established session plus the shutdown plumbing both connection kinds share."""

    __slots__ = (
        "_assert_open",
        "_close",
        "_observe_closed",
        "_transport",
        "closed",
        "endpoint",
        "expires_at",
        "session",
    )

    def __init__(
        self,
        session: T,
        closed: asyncio.Future[Closed],
        observe_closed: Callable[[ClosureObserver], Callable[[], None]],
        transport: MoqtTransport,
        assert_open: Callable[[], None],
        close: Callable[[], None],
        expires_at: datetime | None,
        endpoint: str,
    ) -> None:
        self.session = session
        self.expires_at = expires_at
        """When the relay ends the session for the credential's expiry, if known."""
        self.endpoint = endpoint
        """The endpoint the connection dialed, before the provider's token was added.

        A supervisor given no ``url`` dials it for every later connection.
        """
        self.closed = closed
        self._observe_closed = observe_closed
        self._transport = transport
        self._assert_open = assert_open
        self._close = close

    def assert_open(self) -> None:
        """Raise the error that ended the connection, if it has ended."""
        self._assert_open()

    def observe_closed(self, observer: ClosureObserver) -> Callable[[], None]:
        """Observe the closure in the turn it happens, before the session tears its requests down.

        An observer added after the end is told at once. Returns the function that removes it.
        """
        return self._observe_closed(observer)

    def transport_ended(self) -> bool:
        """Whether the transport has already ended, ahead of the session reporting the closure.

        ``False`` for a transport that cannot say; the closure then arrives in the turn the session
        reports it.
        """
        return transport_ended(self._transport)

    @property
    def protocol(self) -> MoqtProtocolId:
        """The MOQT version the transport negotiated, which selects the core's parameter codec."""
        return resolve_moqt_protocol(getattr(self._transport, "protocol_id", None))

    def close(self) -> None:
        self._close()

    async def wait_transport_closed(self) -> None:
        """The transport's own closure, which the publisher waits on before reporting itself closed."""
        await self._transport.wait_closed()


#: The largest deadline, in seconds, that the TypeScript SDK's timers can hold (2147483647 ms). Python
#: keeps the same bound so a deadline means the same thing in both SDKs.
MAX_TIMEOUT = 2_147_483.647


def validate_timeout(name: str, value: object) -> None:
    """Reject a deadline that is not a number of seconds in ``(0, MAX_TIMEOUT]``.

    A value that is not a number (including ``bool``) is a ``TypeError``; a number outside the
    bound, ``nan`` or infinity is a ``ValueError``.
    """
    if not isinstance(value, int | float) or isinstance(value, bool):
        raise TypeError(f"{name} must be a number of seconds")

    if not 0 < value <= MAX_TIMEOUT:
        raise ValueError(f"{name} must be between 0 (exclusive) and {MAX_TIMEOUT} seconds")


def _with_token(url: str, token: str) -> str:
    parts = urlsplit(url)
    query = [*parse_qsl(parts.query, keep_blank_values=True), ("jwt", token)]
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), ""))


def _endpoint_setup_options(framing: object, url: str) -> list[KeyValuePair]:
    """The setup options the endpoint URL needs in CLIENT_SETUP for a framing (TA-6).

    On native QUIC, PATH (the path, then ``?`` and the query when present, the resolved ``jwt``
    included) and AUTHORITY take the place of the CONNECT request a WebTransport session sends; on a
    WebTransport session neither may be sent (draft-22 §9.1.2, §9.1.1). ``url`` is the endpoint
    as ``_parse_endpoint`` normalized it, so its path is never empty and it has no fragment.
    """
    if framing in ("webtransport", "websocket"):
        return []

    if framing != "native-quic":
        raise RelayError(
            f"create_transport returned an unknown framing {framing!r}; "
            "expected 'webtransport', 'native-quic' or 'websocket'",
            code="TRANSPORT_ERROR",
            phase="connecting",
        )

    parts = urlsplit(url)
    # The query is everything from the first `?`, an empty one included: a path cannot contain `?`.
    query_start = url.find("?")
    path = parts.path + ("" if query_start == -1 else url[query_start:])
    authority = parts.hostname or ""

    if ":" in authority:
        authority = f"[{authority}]"

    if parts.port is not None and parts.port != 443:
        authority = f"{authority}:{parts.port}"

    return [
        KeyValuePair(SETUP_OPTION.PATH, path.encode()),
        KeyValuePair(SETUP_OPTION.AUTHORITY, authority.encode()),
    ]


def _default_transport(url: str, protocols: Sequence[str]) -> ConnectingTransport:
    from .quic import connect_quic

    return connect_quic(url, protocols)


async def open_relay_session(
    options: ConnectRelayOptions,
    create_session: Callable[[MoqtTransport, LifetimeCallbacks, Sequence[KeyValuePair]], T],
) -> RelaySessionLifetime[T]:
    """Resolve the token, connect the transport, run SETUP, and bind the session to the lifetime."""
    url, has_jwt = (None, False) if options.url is None else _parse_endpoint(options.url)
    if url is None and options.get_token is None:
        raise TypeError(_no_root("there is no get_token"))
    validate_timeout("connect_timeout", options.connect_timeout)

    cancellation = options.cancellation
    if cancellation is not None:
        cancellation.throw_if_cancelled()

    loop = asyncio.get_running_loop()
    state: _Connecting[T] = _Connecting()
    stopped = False
    established = False
    using_transport = False
    target: _DialTarget | None = None
    stop_error: BaseException | None = None
    interrupted: asyncio.Future[None] = loop.create_future()
    observe(interrupted)
    closed: asyncio.Future[Closed] = loop.create_future()
    closure_observers: list[ClosureObserver] = []
    closed_info = Closed()

    def finish(info: Closed | None = None) -> None:
        nonlocal stopped, stop_error, closed_info

        if stopped:
            return

        info = info or Closed()
        stopped = True
        closed_info = info
        stop_error = info.error if info.error is not None else RelayError("Relay connection is closed", code="CLOSED")
        timer.cancel()

        if cancellation is not None:
            cancellation.remove_callback(on_cancel)

        # Settle before synchronous user callbacks run during session teardown.
        if not closed.done():
            closed.set_result(info)

        if not interrupted.done():
            interrupted.set_exception(stop_error)

        # Subscriptions hear the closure in this turn. The session fails a pending request before it
        # reports an unexpected closure, so this is how a lost connection is kept from reading as a
        # rejection.
        observers, closure_observers[:] = list(closure_observers), []
        for observer in observers:
            observer(info.error)

        if state.session is not None:
            state.session.destroy(info.error)
        elif state.transport is not None:
            try:
                state.transport.close()
            except Exception:
                # A failed handshake may leave a transport that already refuses close().
                pass

    def closure(info: Closed, phase: RelayErrorPhase) -> Closed:
        """How the session ended, as the connection reports it.

        On native QUIC a peer's application close ends the transport cleanly whatever its code
        (TA-9), so a credentials refusal is read from the code it carried, not from an error.
        """
        peer_close = _peer_close(state.transport)

        if info.error is not None:
            return Closed(_transport_error(info.error, phase, peer_close))

        refusal = _refusal(peer_close)
        return Closed(_unauthorized(refusal, phase)) if refusal is not None else info

    def on_cancel(reason: BaseException) -> None:
        finish(Closed(reason))

    timer = call_later(
        options.connect_timeout,
        lambda: finish(Closed(RelayError("Relay connection timed out", code="CONNECTION_TIMEOUT", phase="connecting"))),
    )

    if cancellation is not None:
        cancellation.add_callback(on_cancel)

    def assert_open() -> None:
        if stopped:
            assert stop_error is not None
            raise stop_error

    def observe_closed(observer: ClosureObserver) -> Callable[[], None]:
        if stopped:
            observer(closed_info.error)
            return lambda: None

        closure_observers.append(observer)

        def remove() -> None:
            if observer in closure_observers:
                closure_observers.remove(observer)

        return remove

    async def establish() -> T:
        nonlocal using_transport, target
        issued: _Issued | None = None

        if not has_jwt and options.get_token is not None:
            resolved = options.get_token()

            if inspect.isawaitable(resolved):
                resolved = await resolved

            assert_open()

            if isinstance(resolved, RelayCredential):
                issued = _Issued(resolved.token, resolved.expires_at.astimezone(UTC))
            elif resolved is None or isinstance(resolved, str):
                issued = _Issued(resolved or "", None)
            else:
                raise TypeError("get_token must return a string, a RelayCredential or None")

        target = _dial_target(url, issued)
        connect_url = target.url
        assert_open()
        using_transport = True
        created = (options.create_transport or _default_transport)(connect_url, list(MOQT_PROTOCOL_IDS))
        transport = created.transport
        state.transport = transport
        ready = asyncio.ensure_future(created.ready)
        observe(ready)

        # A custom factory can synchronously cancel before returning its transport.
        if stopped:
            try:
                transport.close()
            except Exception:
                # The factory may have closed it while cancelling.
                pass

            assert_open()

        await ready
        assert_open()
        setup_options = _endpoint_setup_options(created.framing, connect_url)

        session = create_session(
            transport,
            LifetimeCallbacks(
                on_closed=lambda info: finish(closure(info, "connection" if established else "connecting")),
                on_goaway=lambda goaway: finish(
                    Closed(
                        RelayError(
                            "Relay requested session migration; open a new connection",
                            code="GOAWAY",
                            phase="connection" if established else "connecting",
                        )
                    )
                ),
            ),
            setup_options,
        )
        state.session = session

        await session.ready
        assert_open()
        return session

    establishing = spawn(establish())

    try:
        await asyncio.wait({establishing, interrupted}, return_when=asyncio.FIRST_COMPLETED)
        # When the lifetime ended first (deadline, cancellation), report why it ended rather than how
        # the half-built session noticed: the session's own "closed before SETUP" is a consequence.
        assert_open()
        connected = establishing.result()
        assert_open()
    except BaseException as error:
        if not isinstance(error, Exception):
            failure = error  # A caller's asyncio task cancellation takes precedence over closure.
        elif stopped:
            failure = stop_error
        else:
            failure = _transport_error(error, "connecting", _peer_close(state.transport)) if using_transport else error
        assert failure is not None
        finish(Closed(failure))
        if failure is error:
            raise
        raise failure from failure.__cause__

    timer.cancel()
    established = True
    assert state.transport is not None
    assert target is not None

    return RelaySessionLifetime(
        connected,
        closed,
        observe_closed,
        state.transport,
        assert_open,
        lambda: finish(),
        target.expires_at,
        target.endpoint,
    )


__all__ = [
    "DEFAULT_CONNECT_TIMEOUT",
    "DEFAULT_REQUEST_TIMEOUT",
    "ClosureObserver",
    "ConnectRelayOptions",
    "ConnectingTransport",
    "Framing",
    "LifetimeCallbacks",
    "RelayCredential",
    "RelaySessionLifetime",
    "RelayTransportFactory",
    "TokenProvider",
    "open_relay_session",
    "validate_timeout",
]
