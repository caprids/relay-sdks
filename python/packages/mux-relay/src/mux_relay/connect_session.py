"""Shared transport and authentication lifetime for the subscribe and publish session drivers.

One deadline covers token resolution, the transport handshake, and MOQT SETUP. The optional
cancellation token governs the connection's entire lifetime, not only setup. Every connection
exposes a ``closed`` future carrying the error that ended it, if any.
"""

from __future__ import annotations

import asyncio
import inspect
import math
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Generic, Protocol, TypeAlias, TypeVar, runtime_checkable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from moq_core import MOQT_PROTOCOL_ID, Closed, Goaway, MoqtTransport
from moq_core._tasks import call_later, observe, spawn

from .cancellation import CancellationToken
from .errors import RelayClosedError, RelayGoawayError, RelayTimeoutError, _transport_error

TokenProvider = Callable[[], "str | Awaitable[str | None] | None"]
"""Resolve a connection token, synchronously or asynchronously. ``None`` or ``""`` sends no token."""


@dataclass(frozen=True, slots=True)
class ConnectingTransport:
    """What a transport factory hands back: the transport, and an awaitable for its handshake."""

    transport: MoqtTransport
    ready: Awaitable[None]


RelayTransportFactory = Callable[[str, Sequence[str]], ConnectingTransport]
"""Supply a transport for ``(url, protocols)``.

The aioquic WebTransport client by default; tests supply in-memory pairs.
"""

DEFAULT_CONNECT_TIMEOUT = 10.0
DEFAULT_REQUEST_TIMEOUT = 10.0


@dataclass(frozen=True, slots=True, kw_only=True)
class ConnectRelayOptions:
    url: str
    """Relay's HTTPS WebTransport endpoint, including any endpoint path."""
    get_token: TokenProvider | None = None
    """Resolve a connection token. Relay carries it in the endpoint's ``jwt`` query parameter.

    An explicit ``jwt`` in the URL wins, and the provider is not called in that case.
    """
    cancellation: CancellationToken | None = None
    """Cancelling closes the connection, including after the connect call has returned."""
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT
    """Seconds for token resolution, transport connection, and protocol SETUP together."""
    request_timeout: float = DEFAULT_REQUEST_TIMEOUT
    """Seconds for an initial subscription response, not for silence on an accepted track."""
    create_transport: RelayTransportFactory | None = None


class _Session(Protocol):
    @property
    def ready(self) -> asyncio.Future[None]: ...

    def destroy(self, error: BaseException | None = None) -> None: ...


T = TypeVar("T", bound=_Session)

ClosureObserver: TypeAlias = Callable[[BaseException | None], None]
"""Told the error that ended the connection, ``None`` after a clean shutdown."""


@runtime_checkable
class _EndAware(Protocol):
    """A transport that can say it has ended before ``wait_closed`` returns.

    The native transport fails its streams the moment the QUIC connection terminates but releases
    its socket, and so returns from ``wait_closed``, turns later. Subscriptions ask this to tell a
    stream failure that the closure caused from a rejection the relay sent.
    """

    @property
    def ended(self) -> bool: ...


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

    __slots__ = ("_assert_open", "_close", "_observe_closed", "_transport", "closed", "session")

    def __init__(
        self,
        session: T,
        closed: asyncio.Future[Closed],
        observe_closed: Callable[[ClosureObserver], Callable[[], None]],
        transport: MoqtTransport,
        assert_open: Callable[[], None],
        close: Callable[[], None],
    ) -> None:
        self.session = session
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
        transport = self._transport
        return isinstance(transport, _EndAware) and transport.ended

    def close(self) -> None:
        self._close()

    async def wait_transport_closed(self) -> None:
        """The transport's own closure, which the publisher waits on before reporting itself closed."""
        await self._transport.wait_closed()


def _validate_timeout(name: str, value: float) -> None:
    if not isinstance(value, int | float) or isinstance(value, bool) or math.isnan(value) or value <= 0:
        raise ValueError(f"{name} must be a positive finite number of seconds")

    if math.isinf(value):
        raise ValueError(f"{name} must be a positive finite number of seconds")


def _parse_endpoint(url: str) -> tuple[str, bool]:
    """Validate the endpoint and return it normalized, plus whether it already carries a ``jwt``."""
    parts = urlsplit(url)

    if parts.scheme != "https":
        raise TypeError("Relay endpoints must use HTTPS")

    if parts.fragment or "@" in parts.netloc:
        raise TypeError("Relay endpoints cannot contain fragments or userinfo")

    if not parts.hostname:
        raise TypeError("Relay endpoints must name a host")

    has_jwt = any(key == "jwt" for key, _ in parse_qsl(parts.query, keep_blank_values=True))
    normalized = urlunsplit((parts.scheme, parts.netloc, parts.path or "/", parts.query, ""))
    return normalized, has_jwt


def _with_token(url: str, token: str) -> str:
    parts = urlsplit(url)
    query = [*parse_qsl(parts.query, keep_blank_values=True), ("jwt", token)]
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), ""))


def _default_transport(url: str, protocols: Sequence[str]) -> ConnectingTransport:
    from .webtransport import connect_webtransport

    return connect_webtransport(url, protocols)


async def open_relay_session(
    options: ConnectRelayOptions,
    create_session: Callable[[MoqtTransport, LifetimeCallbacks], T],
) -> RelaySessionLifetime[T]:
    """Resolve the token, connect the transport, run SETUP, and bind the session to the lifetime."""
    url, has_jwt = _parse_endpoint(options.url)
    _validate_timeout("connect_timeout", options.connect_timeout)
    _validate_timeout("request_timeout", options.request_timeout)

    cancellation = options.cancellation
    if cancellation is not None:
        cancellation.throw_if_cancelled()

    loop = asyncio.get_running_loop()
    state: _Connecting[T] = _Connecting()
    stopped = False
    established = False
    using_transport = False
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
        stop_error = info.error if info.error is not None else RelayClosedError("Relay connection is closed")
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

    def on_cancel(reason: BaseException) -> None:
        finish(Closed(reason))

    timer = call_later(
        options.connect_timeout,
        lambda: finish(Closed(RelayTimeoutError("Relay connection timed out", phase="connecting"))),
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
        nonlocal using_transport
        connect_url = url

        if not has_jwt and options.get_token is not None:
            token = options.get_token()

            if inspect.isawaitable(token):
                token = await token

            assert_open()

            if token is not None and not isinstance(token, str):
                raise TypeError("get_token must return a string or None")

            if token:
                connect_url = _with_token(connect_url, token)

        assert_open()
        using_transport = True
        created = (options.create_transport or _default_transport)(connect_url, [MOQT_PROTOCOL_ID])
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

        session = create_session(
            transport,
            LifetimeCallbacks(
                on_closed=lambda info: finish(
                    Closed(_transport_error(info.error, "connection" if established else "connecting"))
                    if info.error is not None
                    else info
                ),
                on_goaway=lambda goaway: finish(
                    Closed(
                        RelayGoawayError(
                            "Relay requested session migration; open a new connection",
                            phase="connection" if established else "connecting",
                        )
                    )
                ),
            ),
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
            failure = _transport_error(error, "connecting") if using_transport else error
        assert failure is not None
        finish(Closed(failure))
        if failure is error:
            raise
        raise failure from failure.__cause__

    timer.cancel()
    established = True
    assert state.transport is not None

    return RelaySessionLifetime(connected, closed, observe_closed, state.transport, assert_open, lambda: finish())


__all__ = [
    "DEFAULT_CONNECT_TIMEOUT",
    "DEFAULT_REQUEST_TIMEOUT",
    "ClosureObserver",
    "ConnectRelayOptions",
    "ConnectingTransport",
    "LifetimeCallbacks",
    "RelaySessionLifetime",
    "RelayTransportFactory",
    "TokenProvider",
    "open_relay_session",
]
