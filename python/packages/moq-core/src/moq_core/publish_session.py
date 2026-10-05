"""Publish-capable MOQT session (moq-transport draft-20).

The publish-side sibling of the subscribe-only driver in ``session``.

Owns the protocol mechanics over an established transport: the SETUP exchange on paired
unidirectional control streams (§3.3), and the announce-and-serve ingest flow. The peer's
SUBSCRIBE_NAMESPACE solicitation is answered with REQUEST_OK and a NAMESPACE entry for the announced
namespace (§10.18-19), then the per-track SUBSCRIBEs the peer routes back are served (answered with
SUBSCRIBE_OK carrying a publisher-assigned track alias, §10.8) for as long as each subscription's
request stream stays open.

Flow choice: the publisher initiates no request streams at all. moq-relay 0.14.7 removed
PUBLISH-based ingest (every PUBLISH is answered with a request error), and it solicits namespaces
itself: one SUBSCRIBE_NAMESPACE per authorized prefix, sent immediately after SETUP. Announcing rides
the solicitation stream as NAMESPACE entries (suffix-relative, parameterless; the relay treats even
an empty parameter count as malformed); data flows only on aliases bound by our own SUBSCRIBE_OKs,
per subscription.

Teardown follows draft-20: every end of an Established subscription the publisher chooses resets
the subscription's still-open data streams, then writes PUBLISH_DONE (status, the number of data
streams opened for it, reason) and FINs the request stream (§3.3.2, §10.12). moq-relay 0.15.4 and
later require that PUBLISH_DONE, and treat a bare FIN as a failed request; 0.15.1 to 0.15.3 abort
the track on any byte after SUBSCRIBE_OK, PUBLISH_DONE included, so this core needs a relay at
0.15.4 or later for a clean end. An orderly ``close()`` holds the transport open, bounded, until
the peer closes its side of each ended request stream, which moq-relay does only after it has read
the PUBLISH_DONE and finished the track. The publisher sends no GOAWAY: 0.14.17 closes the session
on a client GOAWAY (code 17), 0.15.1 and later record it and keep the session. Aliases are the peer's
own request IDs, unique by construction: moq-relay 0.15.x closes the session (PROTOCOL_VIOLATION)
when one alias names two different tracks, and cancels the newer subscription when two live
subscriptions to one track share an alias.

Deliberately callback-shaped, mirroring the subscribe driver. Must be created inside a running
asyncio event loop.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field

from ._tasks import observe, spawn
from .bytes import StreamReader, utf8_encode
from .control_messages import (
    MAX_CONTROL_MESSAGE_LENGTH,
    MESSAGE_TYPE,
    PUBLISH_DONE_STATUS,
    REQUEST_ERROR_CODE,
    SETUP_OPTION,
    STREAM_RESET_CODE,
    ControlMessage,
    ControlMessageFrame,
    Fetch,
    Goaway,
    KeyValuePair,
    MessageParameters,
    Publish,
    PublishNamespace,
    RequestUpdate,
    Setup,
    Subscribe,
    SubscribeNamespace,
    TrackNamespace,
    TrackStatus,
    decode_control_message,
    encode_namespace,
    encode_namespace_done,
    encode_publish_done,
    encode_request_error,
    encode_request_ok,
    encode_setup,
    encode_subscribe_ok,
)
from .errors import SESSION_ERROR, MoqtProtocolError, is_moqt_protocol_error
from .object_stream import STREAM_TYPE, is_subgroup_header_type
from .session import Closed, _call, _describe
from .transport import (
    BidirectionalStream,
    MoqtTransport,
    ReceiveStream,
    SendStream,
    is_session_error,
    transport_ended,
    wait_acknowledged,
)

# =============================================================================
# Driver types
# =============================================================================


@dataclass(frozen=True, slots=True, kw_only=True)
class AnnounceEnded:
    """What ``on_announce_ended`` receives: no solicitation stream carries the announce any more.

    ``error`` is a :class:`MoqtProtocolError` whose ``__cause__`` is the stream failure, if any, that
    ended the last carrier.
    """

    error: BaseException | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class TrackBinding:
    """A track's effective alias binding changed.

    The most recent live subscription wins the binding (a replaced one ends with PUBLISH_DONE
    ``TRACK_ENDED``, a clean end for that request, covering the peer's resubscribe races), and only
    while its Forward State permits data (``forward=0`` subscribes without authorizing it;
    REQUEST_UPDATE toggles it). ``None`` means no live forwarding subscription, so no data streams
    may be opened for the track.

    The two delivery timeouts are that subscription's values in milliseconds (draft-20 §8, §10.2.3,
    §10.2.4): SUBSCRIBE's parameters as updated by every REQUEST_UPDATE since, ``None`` where the
    subscriber never sent one, and always ``None`` without a binding. A REQUEST_UPDATE that changes
    either is reported as a binding with the same alias.
    """

    track_name: str
    track_alias: int | None
    object_delivery_timeout: int | None = None
    subgroup_delivery_timeout: int | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class IncomingRequestUpdate:
    """REQUEST_UPDATE on an inbound request stream.

    ``request_id`` is the request being updated (the stream's SUBSCRIBE or SUBSCRIBE_NAMESPACE);
    ``update_request_id`` is the update's own, freshly consumed Request ID (§10.1).
    """

    request_id: int
    update_request_id: int
    parameters: MessageParameters


@dataclass(slots=True, kw_only=True)
class MoqtPublishSessionCallbacks:
    """Synchronous callbacks a publish session calls as its state changes. Every one is optional."""

    on_ready: Callable[[list[KeyValuePair]], None] | None = None
    """The server's SETUP arrived; the session is fully established."""
    on_goaway: Callable[[Goaway], None] | None = None
    """GOAWAY (control stream or a request stream): stop initiating, migrate."""
    on_announced: Callable[[TrackNamespace], None] | None = None
    """The announced namespace reached the wire on an accepted SUBSCRIBE_NAMESPACE solicitation.

    The peer can now route SUBSCRIBEs to it. NAMESPACE entries have no acknowledgement of their own;
    this is the strongest "announced" fact the transport draft offers.
    """
    on_announce_ended: Callable[[AnnounceEnded], None] | None = None
    """The solicitation stream carrying the announce ended while the session was still live.

    The peer rejected the announce (unauthorized suffix) or withdrew its namespace interest. Either
    way the ingest path is gone.
    """
    on_subscribe: Callable[[Subscribe], None] | None = None
    """The peer subscribed to a registered track (already answered SUBSCRIBE_OK)."""
    on_subscribe_end: Callable[[int], None] | None = None
    """A subscription ended (the peer's FIN or reset, or our PUBLISH_DONE); the argument is its request id."""
    on_track_binding: Callable[[TrackBinding], None] | None = None
    """A registered track's effective alias binding changed; see :class:`TrackBinding`."""
    on_request_update: Callable[[IncomingRequestUpdate], None] | None = None
    """The peer sent REQUEST_UPDATE on one of its requests; see :class:`IncomingRequestUpdate`."""
    on_closed: Callable[[Closed], None] | None = None
    """The session ended: transport closed, or a fatal protocol error."""


DEFAULT_IMPLEMENTATION_NAME = "moq-core (Python) publisher"

CLOSE_FLUSH_TIMEOUT = 0.25
"""Seconds ``close()`` waits for the PUBLISH_DONE + FIN / NAMESPACE_DONE writes to drain.

After that the transport closes out from under them.
"""

CLOSE_END_ACK_TIMEOUT = 2.0
"""Seconds ``close()`` then waits for the peer to close each ended subscription's request stream.

A write reaching the wire does not mean the peer has read it: moq-relay reads PUBLISH_DONE, waits
for Stream Count data streams (or a 1 s grace when it cannot count them), finishes the track and
only then FINs its side. A transport closed before that aborts the track, which viewers see as
PUBLISH_DONE status 0 ``internal error``. The bound covers the relay's grace plus a round trip, so
a peer that never closes its side costs at most this much.
"""


MAX_TRACK_PROPERTIES_LENGTH = MAX_CONTROL_MESSAGE_LENGTH - 9 - 1
"""The largest encoded track property block every SUBSCRIBE_OK this session sends can carry.

The properties ride in SUBSCRIBE_OK, a control message whose length field is 16 bits (§10). The
rest of its body is the track alias, a varint of up to 9 bytes (it is the peer's request ID, so
it is unknown until a subscription arrives), and the count of message parameters, of which this
session sends none (1 byte). A longer block registers but can never answer a subscription.
"""


@dataclass(slots=True, kw_only=True)
class MoqtPublishSessionConfig:
    """How :func:`create_moqt_publish_session` sets up a publish session."""

    setup_options: Sequence[KeyValuePair] = ()
    """Extra Setup Options to send (an implementation identifier is added automatically)."""
    implementation_name: str = DEFAULT_IMPLEMENTATION_NAME
    """Identifies this implementation in SETUP (§10.3.1.5)."""
    callbacks: MoqtPublishSessionCallbacks = field(default_factory=MoqtPublishSessionCallbacks)


@dataclass(frozen=True, slots=True, kw_only=True)
class RegisterTrackOptions:
    """The track :meth:`MoqtPublishSession.register_track` makes servable."""

    track_namespace: Sequence[str]
    track_name: str
    track_properties: Sequence[KeyValuePair] = ()
    """Track metadata is explicit; raw tracks have no implied timestamp unit."""


class RegisteredTrack:
    """One servable track.

    ``end()`` ends every live subscription (its data streams closed, then PUBLISH_DONE
    ``TRACK_ENDED`` and a FIN on its request stream) and refuses future SUBSCRIBEs with
    DOES_NOT_EXIST. Idempotent.
    """

    __slots__ = ("_end", "track_name")

    def __init__(self, track_name: str, end: Callable[[], None]) -> None:
        self.track_name = track_name
        self._end = end

    def end(self) -> None:
        """End every live subscription on the track and refuse later SUBSCRIBEs. Idempotent."""
        self._end()


# =============================================================================
# Internal records
# =============================================================================


@dataclass(slots=True, kw_only=True)
class _SubscriberStream:
    """One inbound subscription's stream-side plumbing."""

    request_id: int
    track_alias: int
    writer: SendStream
    reader: StreamReader
    accepted: bool = False
    """SUBSCRIBE_OK reached the wire; only accepted subscriptions may carry the binding."""
    reported: bool = False
    """``on_subscribe`` fired; ``on_subscribe_end`` pairs on this, keeping counts balanced."""
    forwarding: bool = True
    """Forward State (SUBSCRIBE ``forward``, togglable via REQUEST_UPDATE); data flows only while true."""
    object_delivery_timeout: int | None = None
    """OBJECT_DELIVERY_TIMEOUT (§10.2.4) from SUBSCRIBE, as updated by REQUEST_UPDATE; ``None`` if never sent."""
    subgroup_delivery_timeout: int | None = None
    """SUBGROUP_DELIVERY_TIMEOUT (§10.2.3) from SUBSCRIBE, as updated by REQUEST_UPDATE; ``None`` if never sent."""
    finished: bool = False
    """The subscription is ending or over; no new data stream may open on its alias."""
    writer_closed: bool = False
    """Our direction of the request stream was FINed (after PUBLISH_DONE); it is never reset afterwards."""
    stream_count: int = 0
    """Data streams opened for this subscription: PUBLISH_DONE's Stream Count (§10.12)."""
    data_streams: set[_DataStream] = field(default_factory=set)
    """Data streams opened for this subscription that the caller has not closed yet."""
    opening: set[asyncio.Future[None]] = field(default_factory=set)
    """Data-stream opens in flight; PUBLISH_DONE waits for them to settle (§10.12)."""
    request_ended: asyncio.Future[None] = field(default_factory=lambda: asyncio.get_running_loop().create_future())
    """Settles once the peer's direction of the request stream ends (FIN or reset) or the session tears it down.

    ``close()`` waits on it, bounded, before closing the transport: the peer ends its side only
    after it has consumed our PUBLISH_DONE.
    """

    def settle_request(self) -> None:
        if not self.request_ended.done():
            self.request_ended.set_result(None)

    def reset_data_streams(self) -> None:
        for data_stream in list(self.data_streams):
            data_stream.abort(_DataStreamReset())

    def cancel(self) -> None:
        self.finished = True

        # A request stream whose PUBLISH_DONE and FIN were handed over is complete: resetting it now
        # (a send stream's abort resets until the FIN is acknowledged) would destroy that end.
        if not self.writer_closed:
            self.writer.abort()

        self.reader.cancel()
        self.settle_request()


class _DataStreamReset(Exception):
    """The reason handed to a data stream the session resets when its subscription ends.

    Transports read ``stream_error_code`` from it (§3.3.4 CANCELLED).
    """

    stream_error_code = STREAM_RESET_CODE.CANCELLED

    def __init__(self) -> None:
        super().__init__("the subscription ended")


class _DataStream:
    """A data stream as handed to the caller: it leaves its subscription's open set once closed or reset."""

    __slots__ = ("_stream", "_subscriber")

    def __init__(self, stream: SendStream, subscriber: _SubscriberStream) -> None:
        self._stream = stream
        self._subscriber = subscriber

    def _settle(self) -> None:
        self._subscriber.data_streams.discard(self)

    async def write(self, data: bytes) -> None:
        await self._stream.write(data)

    async def close(self) -> None:
        # The FIN is issued now as far as PUBLISH_DONE's ordering goes: it queues behind the
        # accepted writes on this stream, and no later write can join it.
        self._settle()
        await self._stream.close()

    def abort(self, reason: object = None) -> None:
        self._settle()
        self._stream.abort(reason)

    async def wait_acknowledged(self) -> None:
        await wait_acknowledged(self._stream)


@dataclass(slots=True, kw_only=True)
class _TrackRecord:
    track_properties: list[KeyValuePair]
    key: str
    track_name: str
    subscribers: list[_SubscriberStream] = field(default_factory=list)
    done: bool = False
    pending_ends: list[asyncio.Task[None]] = field(default_factory=list)
    """Subscription ends (PUBLISH_DONE + FIN) still in flight, from any end path (pruned as they settle).

    ``_end_track`` folds them into ``done_flushed``, which ``close()`` drains. An ended subscription
    is already ``finished``, and one whose peer closed its request stream has left ``subscribers``,
    so without this list its queued end would be skipped and discarded by the transport close,
    turning a clean end into an abrupt one.
    """
    done_flushed: asyncio.Task[None] | None = None
    """Settles when the ends queued by ``end()`` land."""


@dataclass(slots=True, kw_only=True)
class _NamespaceStream:
    """One accepted inbound SUBSCRIBE_NAMESPACE: the announce carrier."""

    prefix: TrackNamespace
    writer: SendStream
    announced: list[TrackNamespace] = field(default_factory=list)
    """Suffixes announced on this stream (for NAMESPACE_DONE at close)."""


def _track_key(namespace: Sequence[str], name: str) -> str:
    return f"{'/'.join(namespace)}--{name}"


def _is_namespace_prefix(prefix: Sequence[str], namespace: Sequence[str]) -> bool:
    return len(prefix) <= len(namespace) and all(part == namespace[i] for i, part in enumerate(prefix))


async def _swallow(awaitable: Awaitable[object]) -> None:
    """Await a best-effort write, dropping its failure."""
    try:
        await awaitable
    except Exception:  # the peer is gone or the stream is already over
        pass


# =============================================================================
# Session driver
# =============================================================================


class MoqtPublishSession:
    """The publish-side session driver. Create one with :func:`create_moqt_publish_session`."""

    def __init__(self, transport: MoqtTransport, config: MoqtPublishSessionConfig | None = None) -> None:
        self._transport = transport
        self._config = config or MoqtPublishSessionConfig()
        self._callbacks = self._config.callbacks

        self._tracks: dict[str, _TrackRecord] = {}
        self._namespace_streams: list[_NamespaceStream] = []
        # Prefixes whose solicitation is mid-acceptance. Request handlers run concurrently and
        # registration happens only after the REQUEST_OK write settles, so an overlapping solicitation
        # racing that await must be refused off this reservation, not just off the registered set.
        self._pending_namespace_prefixes: list[TrackNamespace] = []
        # Every inbound request ID ever seen. Reuse closes the session with INVALID_REQUEST_ID (§10.1), and because
        # aliases ARE the peer's request IDs, a reused ID would hand two subscriptions the same alias
        # and make their subgroup streams indistinguishable: session-fatal, per the draft.
        self._peer_request_ids: set[int] = set()
        self._desired_namespace: TrackNamespace | None = None
        self._announced_fired = False
        self._received_namespace_solicitation = False

        loop = asyncio.get_running_loop()
        self.ready: asyncio.Future[None] = loop.create_future()
        """Resolves when the server's SETUP has been received; fails if the session ends first."""
        observe(self.ready)

        self._control_writer: SendStream | None = None
        self._received_server_setup = False
        self._received_control_goaway = False
        self._received_goaway = False
        """Any GOAWAY (control or request stream): the peer is migrating."""
        self._closing = False
        self._close_error: BaseException | None = None
        self._destroyed = False
        self._end_acknowledgements: set[asyncio.Task[None]] = set()
        """One task per subscription end whose PUBLISH_DONE and FIN the peer has not acknowledged yet."""

        spawn(self._start())

    @property
    def namespace_solicited(self) -> bool | None:
        """Whether a current solicitation covers our namespace; None before any is accepted."""
        namespace = self._desired_namespace
        if namespace is None or not self._received_namespace_solicitation:
            return None
        return any(_is_namespace_prefix(stream.prefix, namespace) for stream in self._namespace_streams)

    # ---------------------------------------------------------------------------
    # Session lifecycle
    # ---------------------------------------------------------------------------

    async def _start(self) -> None:
        spawn(self._accept_unidirectional_streams())
        spawn(self._accept_bidirectional_streams())
        spawn(self._watch_transport())

        try:
            control = await self._transport.create_unidirectional_stream()
            self._control_writer = control
            options = [
                KeyValuePair(SETUP_OPTION.MOQT_IMPLEMENTATION, utf8_encode(self._config.implementation_name)),
                *self._config.setup_options,
            ]
            await control.write(encode_setup(options))
        except Exception as error:  # any failure to open the control stream ends the session
            self._fatal(error)

    async def _watch_transport(self) -> None:
        try:
            await self._transport.wait_closed()
        except Exception as error:  # the transport's own failure ends the session
            self._handle_closed(error)
        else:
            self._handle_closed(None)

    def _fatal(self, error: BaseException) -> None:
        # Once an orderly close starts, late control-stream failures must not replace its
        # reason/code or interrupt the bounded FIN/retraction drain.
        if self._destroyed or self._closing:
            return

        close_code = error.code if isinstance(error, MoqtProtocolError) else SESSION_ERROR.INTERNAL_ERROR

        try:
            self._transport.close(close_code, _describe(error, "session error"))
        except Exception:  # transport already gone
            pass

        self._handle_closed(error)

    def _fatal_if_protocol_error(self, error: BaseException) -> None:
        if is_moqt_protocol_error(error):
            self._fatal(error)

    def _handle_closed(self, error: BaseException | None, *, expected: bool = False) -> None:
        if self._destroyed:
            return

        self._destroyed = True
        if error is None:
            error = self._close_error

        if not self._received_server_setup and not self.ready.done():
            self.ready.set_exception(
                error if error is not None else MoqtProtocolError("session closed before server SETUP")
            )

        for track in self._tracks.values():
            for subscriber in track.subscribers:
                subscriber.cancel()

        self._tracks.clear()
        self._namespace_streams.clear()
        self._control_writer = None

        # Mirror the subscribe driver: a transport that drops before server SETUP is a session failure
        # even when the close itself was clean.
        close_error = error
        if close_error is None and not expected and not self._received_server_setup:
            close_error = MoqtProtocolError("session closed before server SETUP")

        _call(self._callbacks.on_closed, Closed(close_error))

    def close(self, close_code: int = SESSION_ERROR.NO_ERROR, reason: str = "") -> None:
        """Orderly stop.

        End every live subscription (PUBLISH_DONE ``TRACK_ENDED``, then FIN) and retract the announce
        (NAMESPACE_DONE), give those writes a bounded window to reach the wire (closing a WebTransport
        discards queued data), then give the peer a bounded window to close each ended subscription's
        request stream, which it does once it has consumed the PUBLISH_DONE, and close the transport.
        Returns immediately; the transport closes within :data:`CLOSE_FLUSH_TIMEOUT` plus
        :data:`CLOSE_END_ACK_TIMEOUT`, and sooner against a peer that answers promptly.

        Neither bound waits for acknowledgement, so the writes can still be in flight when the
        transport closes. A caller that has ended its tracks and must not close while their data
        streams or ends are unacknowledged waits first, for as long as it chooses, on
        :meth:`wait_ends_acknowledged` and on its data streams; the relay publisher does, for up to its
        largest write deadline.
        """
        self._close(close_code, reason)

    def _close(self, close_code: int, reason: str, error: BaseException | None = None) -> None:
        if self._destroyed or self._closing:
            return

        self._closing = True
        self._close_error = error
        spawn(self._drain_and_close(close_code, reason, error))

    async def _drain_and_close(self, close_code: int, reason: str, error: BaseException | None = None) -> None:
        # Orderly close: closing the transport discards queued data, so the subscription ends
        # (PUBLISH_DONE + FIN) and NAMESPACE_DONE retractions must be given a bounded window to land
        # first. A peer should observe every track ending cleanly, not an abrupt transport end. No
        # GOAWAY is sent (see the module doc).
        #
        # One loop beat before the last-resort sweep below: track owners tearing down in the same
        # turn get to end their tracks themselves.
        await asyncio.sleep(0)

        if self._destroyed:
            return

        for track in self._tracks.values():
            self._end_track(track)

        writes: set[asyncio.Future[None]] = set()

        for track in self._tracks.values():
            if track.done_flushed is not None:
                writes.add(track.done_flushed)

            # An end started after the track's own end (none should, but a stray one must not be cut off).
            writes.update(track.pending_ends)

        for stream in self._namespace_streams:
            for suffix in stream.announced:
                writes.add(spawn(_swallow(stream.writer.write(encode_namespace_done(suffix)))))

            stream.announced = []

        if writes:
            await asyncio.wait(writes, timeout=CLOSE_FLUSH_TIMEOUT)

        if self._destroyed:
            return

        # The writes are on the wire, not yet read: a peer that has not consumed a PUBLISH_DONE when
        # the transport closes aborts the track instead of finishing it (moq-relay 0.15.4 and later
        # report that downstream as status 0). The peer closes its side of the request stream once it
        # has, so wait for that on every subscription still being served, bounded for a peer that
        # never does.
        request_ends = {
            subscriber.request_ended
            for track in self._tracks.values()
            for subscriber in track.subscribers
            if not subscriber.request_ended.done()
        }

        if request_ends:
            await asyncio.wait(request_ends, timeout=CLOSE_END_ACK_TIMEOUT)

        if self._destroyed:
            return

        try:
            self._transport.close(close_code, reason)
        except Exception:  # transport already gone
            pass

        self._handle_closed(error, expected=error is None)

    def destroy(self, error: BaseException | None = None) -> None:
        """Drain outgoing streams, preserving an optional local closure error."""
        self._close(SESSION_ERROR.NO_ERROR, "", error)

    # ---------------------------------------------------------------------------
    # Announcing and the track registry
    # ---------------------------------------------------------------------------

    def announce(self, track_namespace: Sequence[str]) -> None:
        """Announce the namespace on every matching solicitation, current and future.

        The wire write additionally waits for the first :meth:`register_track`: an announce invites
        immediate SUBSCRIBEs, and an empty registry would answer them with a terminal DOES_NOT_EXIST.
        Result via ``on_announced`` / ``on_announce_ended``.
        """
        if self._destroyed or self._closing:
            return

        self._desired_namespace = list(track_namespace)

        for stream in list(self._namespace_streams):
            self._reconcile_announce(stream)

    def _reconcile_announce(self, stream: _NamespaceStream) -> None:
        """Write the desired namespace onto a solicitation stream whose prefix covers it.

        Solicitations, ``announce()``, and the first ``register_track()`` arrive in any order (the
        relay's SUBSCRIBE_NAMESPACE races our post-SETUP setup), so all three paths funnel here. The
        announce is deferred until at least one track is registered: an announced namespace invites
        immediate SUBSCRIBEs, and answering one with DOES_NOT_EXIST is terminal on moq-lite-rs, so
        advertising an empty registry would permanently break the first viewer in the
        pre-registration window.
        """
        namespace = self._desired_namespace
        if namespace is None or len(self._tracks) == 0:
            return

        if not _is_namespace_prefix(stream.prefix, namespace):
            return

        suffix = namespace[len(stream.prefix) :]
        if any("/".join(existing) == "/".join(suffix) for existing in stream.announced):
            return

        stream.announced.append(suffix)

        async def write_entry() -> None:
            try:
                await stream.writer.write(encode_namespace(suffix))
            except Exception as error:  # the response half died alone
                # A reset the request half may never mirror: the entry never reached the peer, no
                # future write can, and the read loop may stay open forever. The carrier is dead.
                # Deregister it so the peer's replacement solicitation is a fresh start rather than a
                # PREFIX_OVERLAP against a corpse; if another path already swept it, the loss was
                # reported there.
                if stream not in self._namespace_streams:
                    return

                self._namespace_streams.remove(stream)
                stream.announced = [existing for existing in stream.announced if existing is not suffix]
                spawn(_swallow(stream.writer.close()))
                self._report_announce_loss(error)
                return

            if self._destroyed or self._announced_fired:
                return

            self._announced_fired = True
            _call(self._callbacks.on_announced, namespace)

        spawn(write_entry())

    def _report_announce_loss(self, cause: BaseException | None) -> None:
        """Fire ``on_announce_ended`` when no solicitation carries the announce anymore.

        Shared by the read-loop cleanup and the response-write failure path; ``cause`` is the stream
        failure that ended the carrier. Silent while closing, destroyed, or migrating (a GOAWAY makes
        the peer's streams ending the orderly drain), and once the transport has ended: an ended
        transport fails every stream before the session learns of it, and that is connection loss,
        which ``_handle_closed`` reports, not a withdrawal. A ``source="session"`` failure (TA-23) or the
        transport's own flag decides, not which of the two wakes first.
        """
        if self._destroyed or self._closing or self._received_goaway:
            return

        if is_session_error(cause) or transport_ended(self._transport):
            return

        if any(len(stream.announced) > 0 for stream in self._namespace_streams):
            return

        error = MoqtProtocolError("the namespace solicitation carrying the announce ended")
        error.__cause__ = cause
        _call(self._callbacks.on_announce_ended, AnnounceEnded(error=error))

    def register_track(self, options: RegisterTrackOptions) -> RegisteredTrack:
        """Register one servable track; the peer's SUBSCRIBEs are answered from this registry."""
        if self._destroyed or self._closing:
            # A closed session must not repopulate the registry: hand back an inert handle so late
            # callers still get the RegisteredTrack shape.
            return RegisteredTrack(options.track_name, lambda: None)

        track = _TrackRecord(
            track_properties=list(options.track_properties),
            key=_track_key(options.track_namespace, options.track_name),
            track_name=options.track_name,
        )

        self._tracks[track.key] = track

        # The first registration may be what the deferred announce was waiting for.
        if len(self._tracks) == 1:
            for stream in list(self._namespace_streams):
                self._reconcile_announce(stream)

        return RegisteredTrack(options.track_name, lambda: self._end_track(track))

    def _end_track(self, track: _TrackRecord) -> None:
        """End every live subscription with PUBLISH_DONE ``TRACK_ENDED`` and refuse future SUBSCRIBEs.

        The aggregated write completion lands on ``track.done_flushed`` so ``close()`` can hold the
        transport open until the PUBLISH_DONEs and FINs reach the wire.
        """
        if track.done:
            return

        track.done = True
        had_reported = False

        for subscriber in track.subscribers:
            if not subscriber.finished:
                self._end_subscription(track, subscriber, PUBLISH_DONE_STATUS.TRACK_ENDED, "track ended")

            # The local end is authoritative: report each live subscription ended NOW rather than
            # when the peer eventually closes its request direction. Otherwise the subscriber count
            # and the binding stay live (and publishers keep opening data streams) until the peer
            # notices the FIN. Clearing `reported` keeps the read loop's own end notification paired:
            # it fires on the same flag.
            if subscriber.reported:
                subscriber.reported = False
                had_reported = True

                if not self._destroyed:
                    _call(self._callbacks.on_subscribe_end, subscriber.request_id)

        if had_reported and not self._destroyed:
            _call(self._callbacks.on_track_binding, TrackBinding(track_name=track.track_name, track_alias=None))

        writes: list[asyncio.Task[None]] = list(track.pending_ends)

        async def flushed() -> None:
            if writes:
                await asyncio.wait(writes)

        track.done_flushed = spawn(flushed())

    def _end_subscription(
        self, track: _TrackRecord, subscriber: _SubscriberStream, status: int, reason: str, *, preceding: bytes = b""
    ) -> None:
        """End an Established subscription from the publisher side (§3.3.2, §10.12).

        Marks it finished at once, so no new data stream opens on its alias. Then, in order: wait for
        data-stream opens in flight, reset every data stream the caller has not closed (a caller that
        follows the binding has closed them already), write ``preceding`` (a REQUEST_ERROR answering
        a failed update) and PUBLISH_DONE with the subscription's Stream Count, and FIN. The task
        settles when the FIN lands or the stream fails, and stays in ``track.pending_ends`` until then,
        so ``close()`` drains it whichever path started it.
        """
        subscriber.finished = True

        fin_written = False

        async def end() -> None:
            nonlocal fin_written
            if subscriber.opening:
                await asyncio.wait(set(subscriber.opening))

            subscriber.reset_data_streams()
            await subscriber.writer.write(preceding + encode_publish_done(status, subscriber.stream_count, reason))
            subscriber.writer_closed = True
            await subscriber.writer.close()
            fin_written = True

        ended = spawn(_swallow(end()))
        track.pending_ends.append(ended)

        def prune(_: asyncio.Future[None]) -> None:
            if ended in track.pending_ends:
                track.pending_ends.remove(ended)

        ended.add_done_callback(prune)

        async def acknowledged() -> None:
            await ended

            if fin_written:
                await wait_acknowledged(subscriber.writer)

        acknowledging = spawn(_swallow(acknowledged()))
        self._end_acknowledgements.add(acknowledging)
        acknowledging.add_done_callback(self._end_acknowledgements.discard)

    async def wait_ends_acknowledged(self) -> None:
        """Return once the peer acknowledged every subscription end queued so far: its PUBLISH_DONE and FIN.

        An end that failed, or whose stream was reset, counts as done. On a transport that does not
        report acknowledgement, an end counts once its FIN was written. Never raises; the caller bounds
        the wait.
        """
        if self._end_acknowledgements:
            await asyncio.wait(set(self._end_acknowledgements))

    def _live_subscription(self, track_alias: int) -> _SubscriberStream | None:
        for track in self._tracks.values():
            for subscriber in track.subscribers:
                if subscriber.track_alias == track_alias and subscriber.accepted and not subscriber.finished:
                    return subscriber

        return None

    async def open_uni_stream(self, track_alias: int) -> SendStream:
        """Open a unidirectional data stream for the live subscription bound to ``track_alias``.

        The stream counts toward that subscription's PUBLISH_DONE Stream Count, and a stream still
        open when the subscription ends is reset before PUBLISH_DONE is written (§10.12). Raises
        ``RuntimeError`` once the session is closing, when no live subscription holds the alias, and
        when the subscription ends while the stream is opening (the stream is reset first).
        """
        # Refuse rather than touch the transport once the session is going away: encoders keep
        # producing frames for a beat after a peer kills the session.
        if self._destroyed or self._closing:
            raise RuntimeError("moq publish session: closed")

        subscriber = self._live_subscription(track_alias)
        if subscriber is None:
            raise RuntimeError(f"moq publish session: no live subscription for track alias {track_alias}")

        opening: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        subscriber.opening.add(opening)

        try:
            stream = await self._transport.create_unidirectional_stream()

            if subscriber.finished:
                # The subscription ended while this stream was opening. Its PUBLISH_DONE waits on
                # `opening`, so the reset lands before it and the stream is not counted.
                stream.abort(_DataStreamReset())
                raise RuntimeError(f"moq publish session: the subscription for track alias {track_alias} ended")

            data_stream = _DataStream(stream, subscriber)
            subscriber.data_streams.add(data_stream)
            subscriber.stream_count += 1
            return data_stream
        finally:
            subscriber.opening.discard(opening)
            opening.set_result(None)

    # ---------------------------------------------------------------------------
    # Incoming unidirectional streams (control, padding)
    # ---------------------------------------------------------------------------

    async def _accept_unidirectional_streams(self) -> None:
        try:
            async for stream in self._transport.incoming_unidirectional_streams():
                spawn(self._handle_unidirectional_stream(stream), on_error=self._fatal_if_protocol_error)
        except Exception:  # transport went away; `wait_closed` handles the surfacing
            pass

    async def _handle_unidirectional_stream(self, stream: ReceiveStream) -> None:
        reader = StreamReader(stream)
        stream_type = await reader.read_varint()

        if stream_type == STREAM_TYPE.SETUP:
            await self._run_control_stream(reader)
            return

        if stream_type == STREAM_TYPE.PADDING:
            reader.cancel()
            return

        reader.cancel()

        if is_subgroup_header_type(stream_type) or stream_type == STREAM_TYPE.FETCH_HEADER:
            raise MoqtProtocolError("publish session received a data stream")

        raise MoqtProtocolError(f"unknown unidirectional stream type 0x{stream_type:x}")

    async def _read_control_frame(self, reader: StreamReader, message_type: int) -> ControlMessage:
        """Read one control-message frame using the pull-based stream reader."""
        high = await reader.read_uint8()
        low = await reader.read_uint8()
        body = await reader.read_bytes((high << 8) | low)
        return decode_control_message(ControlMessageFrame(message_type, body))

    async def _run_control_stream(self, reader: StreamReader) -> None:
        # The stream-type varint (0x2F00) doubles as the first message's type.
        setup = await self._read_control_frame(reader, MESSAGE_TYPE.SETUP)
        if not isinstance(setup, Setup):
            raise MoqtProtocolError("control stream did not begin with SETUP")

        if self._received_server_setup:
            raise MoqtProtocolError("received a second control stream")

        self._received_server_setup = True
        if not self.ready.done():
            self.ready.set_result(None)
        _call(self._callbacks.on_ready, setup.options)

        while not await reader.at_end():
            message_type = await reader.read_varint()
            message = await self._read_control_frame(reader, message_type)
            if not isinstance(message, Goaway):
                raise MoqtProtocolError(f"unexpected {message.kind} on the control stream")

            if self._received_control_goaway:
                raise MoqtProtocolError("received more than one GOAWAY on the control stream")

            self._received_control_goaway = True
            self._received_goaway = True
            _call(self._callbacks.on_goaway, message)

        # A closed control stream ends the session (§3.3).
        if not self._destroyed:
            self._fatal(MoqtProtocolError("peer closed its control stream"))

    # ---------------------------------------------------------------------------
    # Incoming bidirectional streams (peer-initiated requests)
    # ---------------------------------------------------------------------------

    async def _accept_bidirectional_streams(self) -> None:
        try:
            async for stream in self._transport.incoming_bidirectional_streams():
                spawn(self._handle_incoming_request(stream), on_error=self._fatal_if_protocol_error)
        except Exception:  # transport went away
            pass

    async def _handle_incoming_request(self, stream: BidirectionalStream) -> None:
        reader = StreamReader(stream.readable)
        message_type = await reader.read_varint()
        message = await self._read_control_frame(reader, message_type)

        # Reserve the request ID for EVERY request-initiating kind: the uniqueness invariant is
        # session-wide, so an ID burned on a rejected PUBLISH or FETCH must not be reusable by a later
        # SUBSCRIBE (whose alias it would become).
        if isinstance(message, Subscribe | SubscribeNamespace | Publish | PublishNamespace | TrackStatus | Fetch):
            self._claim_peer_request_id(message.request_id)

        if isinstance(message, Subscribe):
            await self._handle_incoming_subscribe(stream, reader, message)
            return

        if isinstance(message, SubscribeNamespace):
            await self._handle_subscribe_namespace(stream, reader, message)
            return

        # Everything else: this endpoint only serves its own published tracks.
        if isinstance(message, Publish):
            error_code: int = REQUEST_ERROR_CODE.UNINTERESTED
        else:
            error_code = REQUEST_ERROR_CODE.NOT_SUPPORTED

        try:
            await stream.writable.write(encode_request_error(error_code, "publish-only endpoint"))
            await stream.writable.close()
        except Exception:  # peer cancelled; nothing to clean up
            pass

        reader.cancel()

    def _claim_peer_request_id(self, request_id: int) -> None:
        """Request IDs are consumed once per session (§10.1); a reused or wrong-parity one is session-fatal.

        Every request-initiating message consumes one, and since draft-20 gave it an id of its own,
        so does REQUEST_UPDATE. This endpoint is always the client, so the peer's ids are odd; a
        reused id or an even one closes the session with INVALID_REQUEST_ID.
        """
        if request_id % 2 != 1:
            raise MoqtProtocolError(f"peer sent even request id {request_id}", SESSION_ERROR.INVALID_REQUEST_ID)

        if request_id in self._peer_request_ids:
            raise MoqtProtocolError(f"peer reused request id {request_id}", SESSION_ERROR.INVALID_REQUEST_ID)

        self._peer_request_ids.add(request_id)

    async def _handle_subscribe_namespace(
        self, stream: BidirectionalStream, reader: StreamReader, subscribe_namespace: SubscribeNamespace
    ) -> None:
        """A peer's namespace solicitation: the announce carrier.

        Accepted with REQUEST_OK, then held open for the session's lifetime: NAMESPACE /
        NAMESPACE_DONE entries for every announced namespace under the prefix ride this stream. A
        clean requester-side FIN is half-closure, not withdrawal (§3.3.2); only a reset (or a rejected
        update, §10.9.1) ends the carrier, and its loss while the announce is live means the peer
        refused or withdrew the ingest path.
        """
        writer = stream.writable
        prefix = subscribe_namespace.track_namespace_prefix

        # §10.18: a solicitation overlapping an established one (either prefix containing the other)
        # is refused with PREFIX_OVERLAP. Announcing on both would hand the peer duplicate namespace
        # state with inconsistent withdrawals.
        overlaps = any(
            _is_namespace_prefix(existing.prefix, prefix) or _is_namespace_prefix(prefix, existing.prefix)
            for existing in self._namespace_streams
        ) or any(
            _is_namespace_prefix(pending, prefix) or _is_namespace_prefix(prefix, pending)
            for pending in self._pending_namespace_prefixes
        )

        if overlaps:
            try:
                await writer.write(
                    encode_request_error(REQUEST_ERROR_CODE.PREFIX_OVERLAP, "overlapping namespace prefix")
                )
                await writer.close()
            except Exception:  # peer cancelled
                pass

            reader.cancel()
            return

        # Reserve the prefix across the acceptance write; the registration below happens in the same
        # turn as the release, so a racing overlapping solicitation always sees one or the other.
        self._pending_namespace_prefixes.append(prefix)
        accepted = False

        try:
            await writer.write(encode_request_ok())
            accepted = True
        except Exception:  # peer cancelled before the acceptance landed
            pass
        finally:
            self._pending_namespace_prefixes.remove(prefix)

        if not accepted:
            reader.cancel()
            return

        record = _NamespaceStream(prefix=prefix, writer=writer)
        self._received_namespace_solicitation = True
        self._namespace_streams.append(record)
        self._reconcile_announce(record)

        # Hold the stream. The known peers write nothing further on it.
        failed_update = False
        cause: BaseException | None = None

        try:
            while not await reader.at_end():
                message_type = await reader.read_varint()
                message = await self._read_control_frame(reader, message_type)

                if isinstance(message, Goaway):
                    self._received_goaway = True
                    _call(self._callbacks.on_goaway, message)
                elif isinstance(message, RequestUpdate):
                    self._claim_peer_request_id(message.request_id)
                    # Namespace subscriptions may legally be updated (§10.9). v1 applies none of it (a
                    # prefix change would re-base every entry), so per §10.9.1 the update is answered
                    # with an error and the request stream closes below: never session-fatal (only the
                    # reused Request ID above is).
                    _call(
                        self._callbacks.on_request_update,
                        IncomingRequestUpdate(
                            request_id=subscribe_namespace.request_id,
                            update_request_id=message.request_id,
                            parameters=message.parameters,
                        ),
                    )
                    spawn(
                        _swallow(
                            writer.write(
                                encode_request_error(REQUEST_ERROR_CODE.NOT_SUPPORTED, "namespace update not supported")
                            )
                        )
                    )
                    failed_update = True
                    break
                else:
                    raise MoqtProtocolError(f"unexpected {message.kind} on an incoming subscribe-namespace stream")

            if not failed_update:
                # A clean requester-side FIN is NOT a cancellation (§3.3.2: the peer merely commits to
                # sending no updates): the response side keeps carrying NAMESPACE entries until a
                # reset or the transport ends, so the carrier stays registered and open.
                return
        except MoqtProtocolError:
            # A protocol violation is session-fatal; _handle_closed sweeps the carrier state with
            # everything else.
            raise
        except Exception as error:  # reset: the peer withdrew the namespace subscription
            cause = error

        # Withdrawal (reset) or a failed update: the carrier is over.
        if record in self._namespace_streams:
            self._namespace_streams.remove(record)

        spawn(_swallow(writer.close()))

        if len(record.announced) > 0:
            self._report_announce_loss(cause)

    async def _handle_incoming_subscribe(
        self, stream: BidirectionalStream, reader: StreamReader, subscribe: Subscribe
    ) -> None:
        track = self._tracks.get(_track_key(subscribe.track_namespace, subscribe.track_name))
        writer = stream.writable

        if track is None or track.done:
            try:
                await writer.write(encode_request_error(REQUEST_ERROR_CODE.DOES_NOT_EXIST, "unknown track"))
                await writer.close()
            except Exception:  # peer cancelled
                pass

            reader.cancel()
            return

        # The peer's request IDs are session-unique, so they double as the track alias: binding one
        # alias to two live request IDs is a session-fatal Duplicate on moq-lite-rs.
        track_alias = subscribe.request_id
        subscriber = _SubscriberStream(
            request_id=subscribe.request_id,
            track_alias=track_alias,
            writer=writer,
            reader=reader,
            # Forward State defaults to 1; `forward=0` subscribes without authorizing data until a
            # REQUEST_UPDATE flips it.
            forwarding=subscribe.parameters.forward != 0,
            object_delivery_timeout=subscribe.parameters.object_delivery_timeout,
            subgroup_delivery_timeout=subscribe.parameters.subgroup_delivery_timeout,
        )

        track.subscribers.append(subscriber)

        try:
            await writer.write(encode_subscribe_ok(track_alias, MessageParameters(), track.track_properties))
            subscriber.accepted = True
        except Exception:  # peer cancelled before the response landed
            track.subscribers = [s for s in track.subscribers if s is not subscriber]
            reader.cancel()
            subscriber.settle_request()
            return

        # Overlapping same-track subscriptions settle their SUBSCRIBE_OK writes independently, so a
        # newer subscription may have replaced this one (marking it finished) while this acceptance
        # was still in flight. A stale completion must not steal the binding back onto an alias whose
        # stream is already ending, and a subscription that was never live is never reported
        # (`on_subscribe_end` pairs on the same flag, keeping subscriber counts balanced).
        if not subscriber.finished:
            subscriber.reported = True
            _call(self._callbacks.on_subscribe, subscribe)

            # The newest subscription wins the binding, and every live PREDECESSOR (arrival order: a
            # successor may accept before an older backpressured write settles, and must not be swept
            # by it) ends cleanly (FIN) so the peer's bookkeeping resolves. One live subscription per
            # track is a deliberate v1 constraint: the spec permits concurrent same-track
            # subscriptions, but serving them means writing every group once per alias, and the known
            # peers (moq-lite-rs relays) hold at most one upstream subscription per track and fan out
            # on their side; the sweep also covers their resubscribe races.
            for other in track.subscribers[: track.subscribers.index(subscriber)]:
                if other.finished:
                    continue

                # No status names a replacement. TRACK_ENDED is the one the relay finishes a track
                # on (anything else aborts it), and the newer subscription carries the track on.
                self._end_subscription(
                    track, other, PUBLISH_DONE_STATUS.TRACK_ENDED, "superseded by a newer subscription"
                )

            _call(self._callbacks.on_track_binding, _binding_of(track, subscriber if subscriber.forwarding else None))

        # Keep serving the request stream: REQUEST_UPDATEs arrive here.
        try:
            while not await reader.at_end():
                message_type = await reader.read_varint()
                message = await self._read_control_frame(reader, message_type)

                if isinstance(message, RequestUpdate):
                    self._claim_peer_request_id(message.request_id)
                    parameters = message.parameters
                    unsupported = _unsupported_update_fields(parameters)

                    # Forward State toggles ride REQUEST_UPDATE; the binding must follow so data stops
                    # (or starts) with the subscription's authorization. The delivery timeouts ride it
                    # too (§8), and the publisher applies the new values to groups it opens afterwards.
                    # Newest-wins keeps this subscriber the track's only live one, so its state IS the
                    # binding.
                    rebind = False

                    if parameters.forward is not None:
                        subscriber.forwarding = parameters.forward != 0
                        rebind = True

                    if parameters.object_delivery_timeout is not None:
                        subscriber.object_delivery_timeout = parameters.object_delivery_timeout
                        rebind = True

                    if parameters.subgroup_delivery_timeout is not None:
                        subscriber.subgroup_delivery_timeout = parameters.subgroup_delivery_timeout
                        rebind = True

                    if rebind and not subscriber.finished and subscriber.accepted:
                        _call(
                            self._callbacks.on_track_binding,
                            _binding_of(track, subscriber if subscriber.forwarding else None),
                        )

                    _call(
                        self._callbacks.on_request_update,
                        IncomingRequestUpdate(
                            request_id=subscribe.request_id,
                            update_request_id=message.request_id,
                            parameters=parameters,
                        ),
                    )

                    if not subscriber.finished:
                        if unsupported:
                            # Acknowledging an update we did not apply would leave the peer serving
                            # stale expectations (a filter or range it believes is in effect). Forward
                            # State and the delivery timeouts are applied above and priority is advisory
                            # (the known relays discard ours too); anything else ends this request
                            # honestly: the update's REQUEST_ERROR, then PUBLISH_DONE UPDATE_FAILED
                            # (§10.9.1).
                            self._end_subscription(
                                track,
                                subscriber,
                                PUBLISH_DONE_STATUS.UPDATE_FAILED,
                                "unsupported subscription update",
                                preceding=encode_request_error(
                                    REQUEST_ERROR_CODE.NOT_SUPPORTED, "unsupported subscription update"
                                ),
                            )

                            if subscriber.reported:
                                subscriber.reported = False
                                _call(self._callbacks.on_subscribe_end, subscribe.request_id)
                                self._emit_binding(track)
                        else:
                            # The subscribe driver treats REQUEST_OK as the update's completion
                            # (`on_update_ok`). Strictly reactive: a peer that never sends
                            # REQUEST_UPDATE (moq-relay 0.15.8 writes nothing after SUBSCRIBE until
                            # it cancels) never sees a trailing byte.
                            spawn(_swallow(writer.write(encode_request_ok())))
                elif isinstance(message, Goaway):
                    self._received_goaway = True
                    _call(self._callbacks.on_goaway, message)
                else:
                    raise MoqtProtocolError(f"unexpected {message.kind} on an incoming subscribe stream")
            # The peer FINed its direction while the subscription was still ours to end. moq-relay
            # (0.15.8 `cancel_subscribe`) pairs that FIN with STOP_SENDING (a cancellation, so the
            # write below fails harmlessly); a peer that only half-closes still gets the PUBLISH_DONE
            # §3.3.2 requires before our FIN. No status names this, so it reports the publisher's own
            # limitation.
            if not subscriber.finished:
                self._end_subscription(
                    track, subscriber, PUBLISH_DONE_STATUS.INTERNAL_ERROR, "subscriber closed its request stream"
                )
        except MoqtProtocolError:
            raise
        except Exception:  # reset: the subscriber cancelled (§3.3.3); ordinary end-of-request
            # A cancelled subscription gets no PUBLISH_DONE, and its open data streams are reset
            # (§5.1.1).
            subscriber.reset_data_streams()
        finally:
            already_ending = subscriber.finished
            subscriber.finished = True
            track.subscribers = [s for s in track.subscribers if s is not subscriber]
            subscriber.settle_request()

            if not already_ending:
                # The subscription is over both ways, mirror of the namespace stream's cleanup: an
                # unclosed response direction would leak a half-open stream per cancelled request
                # until the transport closes. An already-aborted writer fails harmlessly. A
                # subscription this side ended closes its writer after PUBLISH_DONE instead.
                spawn(_swallow(writer.close()))

            if not self._destroyed and subscriber.reported:
                _call(self._callbacks.on_subscribe_end, subscribe.request_id)
                # Only accepted, forwarding subscriptions may carry the binding: one whose SUBSCRIBE_OK
                # is still in flight could yet fail (its removal would strand the binding on an alias
                # the peer never registered), and one with Forward State 0 forbids data. When a
                # pending one accepts, it emits its own binding.
                self._emit_binding(track)

    def _emit_binding(self, track: _TrackRecord) -> None:
        live = [s for s in track.subscribers if not s.finished and s.accepted and s.forwarding]
        _call(self._callbacks.on_track_binding, _binding_of(track, live[-1] if live else None))


def _binding_of(track: _TrackRecord, subscriber: _SubscriberStream | None) -> TrackBinding:
    """The track's binding to ``subscriber``, or no binding."""
    if subscriber is None:
        return TrackBinding(track_name=track.track_name, track_alias=None)

    return TrackBinding(
        track_name=track.track_name,
        track_alias=subscriber.track_alias,
        object_delivery_timeout=subscriber.object_delivery_timeout,
        subgroup_delivery_timeout=subscriber.subgroup_delivery_timeout,
    )


def _unsupported_update_fields(parameters: MessageParameters) -> list[str]:
    """Parameter names set on a REQUEST_UPDATE that the publish driver does not apply.

    Everything but FORWARD and the two delivery timeouts (applied) and SUBSCRIBER_PRIORITY (advisory).
    """
    ignored = {"forward", "object_delivery_timeout", "subgroup_delivery_timeout", "subscriber_priority"}
    return [
        name
        for name in MessageParameters.__dataclass_fields__
        if name not in ignored and getattr(parameters, name) is not None
    ]


def create_moqt_publish_session(
    transport: MoqtTransport, config: MoqtPublishSessionConfig | None = None
) -> MoqtPublishSession:
    """Create a publish-capable MOQT session driver over an established transport. Requires a running event loop.

    Example::

        session = create_moqt_publish_session(transport)
        await session.ready
        session.announce(["live", "abc123"])
        track = session.register_track(RegisterTrackOptions(track_namespace=["live", "abc123"], track_name="video"))
    """
    return MoqtPublishSession(transport, config)


__all__ = [
    "CLOSE_END_ACK_TIMEOUT",
    "CLOSE_FLUSH_TIMEOUT",
    "DEFAULT_IMPLEMENTATION_NAME",
    "MAX_TRACK_PROPERTIES_LENGTH",
    "AnnounceEnded",
    "IncomingRequestUpdate",
    "MoqtPublishSession",
    "MoqtPublishSessionCallbacks",
    "MoqtPublishSessionConfig",
    "RegisterTrackOptions",
    "RegisteredTrack",
    "TrackBinding",
    "create_moqt_publish_session",
]
