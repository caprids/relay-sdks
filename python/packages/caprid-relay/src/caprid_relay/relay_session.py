"""One Relay connection that publishes and subscribes, over one MOQT session (``moq_core.create_moqt_duplex_session``).

See spec/behavior.md, "One connection that publishes and subscribes". The counterpart of ``ts/src/relay-session.ts``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Sequence
from types import TracebackType

from moq_core import (
    KeyValuePair,
    MoqtDuplexSession,
    MoqtDuplexSessionConfig,
    MoqtPublishSessionConfig,
    MoqtSession,
    MoqtSessionCallbacks,
    MoqtSessionConfig,
    MoqtTransport,
    TrackNamespace,
    create_moqt_duplex_session,
)

from ._validation import namespace_fields
from ._write_slot import _observe
from .cancellation import CancellationToken
from .connect import IMPLEMENTATION_NAME, SESSION_SETUP_OPTIONS, _Subscribing
from .connect_session import (
    DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_REQUEST_TIMEOUT,
    ConnectRelayOptions,
    LifetimeCallbacks,
    RelaySessionLifetime,
    RelayTransportFactory,
    TokenProvider,
    open_relay_session,
    validate_timeout,
)
from .publish import RelayPublication, _namespace_ended, _PublishHost


class RelaySession(_Subscribing):
    """A connection that publishes and subscribes. Create one with :func:`connect_relay`.

    Everything a :class:`RelaySubscriber` does works here unchanged. :meth:`publish` adds a namespace that this
    connection announces and serves. The relay never routes a connection's own announcements back to it.
    ``namespace_events`` does not report this connection's publications, and the relay refuses a subscribe to one
    of its own tracks (request error 16 on moq-relay).
    """

    __slots__ = ("_host", "_shutdown")

    def __init__(self, lifetime: RelaySessionLifetime[MoqtSession], host: _PublishHost) -> None:
        super().__init__(lifetime)
        self._host = host
        self._shutdown: asyncio.Future[None] | None = None

    def publish(self, namespace: Sequence[str]) -> RelayPublication:
        """Announce ``namespace`` and return the publication that serves its tracks.

        The connection sends nothing for it until its first track is registered. ``namespace`` follows the
        namespace rules. A namespace published once stays taken on this connection, also after its publication
        closes, so publishing it again raises ``ValueError``. The token must grant publishing it.
        """
        # Arguments before connection state, as on the other entry points.
        return self._host.publish(namespace_fields(namespace))

    async def close(self) -> None:
        """End every publication's tracks, wait for their groups, retract the announces, and close the connection.

        It waits for the groups the relay has not acknowledged as :meth:`RelayPublisher.close` does. Every
        subscription and request ends, and this returns once the transport has closed. Idempotent. Cancelling the
        caller does not stop the shutdown.
        """
        if self._shutdown is None:
            self._shutdown = asyncio.ensure_future(self._close_after(self._host.end_all()))
            _observe(self._shutdown)

        await asyncio.shield(self._shutdown)

    async def _close_after(self, drained: Awaitable[object]) -> None:
        await drained
        self._lifetime.close()

        try:
            await self._lifetime.wait_transport_closed()
        except Exception:  # the closure is the lifetime's to report
            pass

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        await self.close()


async def connect_relay(
    url: str | None = None,
    *,
    get_token: TokenProvider | None = None,
    cancellation: CancellationToken | None = None,
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
    request_timeout: float = DEFAULT_REQUEST_TIMEOUT,
    create_transport: RelayTransportFactory | None = None,
) -> RelaySession:
    """Open one Relay connection that publishes and subscribes. The caller owns connection shutdown.

    It takes the options :func:`connect_relay_subscriber` takes, since publishing adds none. One token covers
    both directions, so it must grant every namespace the connection publishes and every one it subscribes to.
    If the relay stops soliciting a namespace, only that namespace's publication ends, with ``NAMESPACE_ENDED`` on
    its ``closed``. The connection stays open.
    """
    validate_timeout("request_timeout", request_timeout)
    options = ConnectRelayOptions(
        url=url,
        get_token=get_token,
        cancellation=cancellation,
        connect_timeout=connect_timeout,
        create_transport=create_transport,
    )
    host = _PublishHost()
    duplex: MoqtDuplexSession | None = None

    def on_namespace_ended(namespace: TrackNamespace, cause: BaseException | None) -> None:
        host.disconnect(namespace, _namespace_ended(cause))

    def create_session(
        transport: MoqtTransport, callbacks: LifetimeCallbacks, setup_options: Sequence[KeyValuePair]
    ) -> MoqtSession:
        nonlocal duplex
        duplex = create_moqt_duplex_session(
            transport,
            MoqtDuplexSessionConfig(
                subscriber=MoqtSessionConfig(
                    implementation_name=IMPLEMENTATION_NAME,
                    setup_options=[*setup_options, *SESSION_SETUP_OPTIONS],
                    request_timeout=request_timeout,
                    callbacks=MoqtSessionCallbacks(on_closed=callbacks.on_closed, on_goaway=callbacks.on_goaway),
                ),
                # A protocol failure on the publishing side, or a GOAWAY on one of the relay's requests to it, ends
                # the connection as on a publisher connection.
                publisher=MoqtPublishSessionConfig(callbacks=host.session_callbacks(callbacks, on_namespace_ended)),
            ),
        )
        # Ending the subscribe driver ends the session in order. The subscriber ends at once, and the transport
        # after the publisher's drain.
        return duplex.subscriber

    lifetime = await open_relay_session(options, create_session)
    assert duplex is not None
    host.bind(duplex.publisher, lifetime)
    return RelaySession(lifetime, host)


__all__ = ["RelaySession", "connect_relay"]
