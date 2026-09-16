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
per subscription. Three hard-won peer constraints shape the teardown paths: a subscription ends by
FIN alone (any byte after SUBSCRIBE_OK, a PUBLISH_DONE say, makes the relay abort the track for every
viewer instead of finishing it), a client-sent GOAWAY closes the whole session (code 17), and a track
alias bound to two live request IDs closes it too (code 12), so aliases are the peer's own request
IDs, unique by construction.

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
    MESSAGE_TYPE,
    REQUEST_ERROR_CODE,
    SETUP_OPTION,
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
    encode_request_error,
    encode_request_ok,
    encode_setup,
    encode_subscribe_ok,
)
from .errors import SESSION_ERROR, MoqtProtocolError, is_moqt_protocol_error
from .object_stream import STREAM_TYPE, is_subgroup_header_type
from .session import Closed, _call, _describe
from .transport import BidirectionalStream, MoqtTransport, ReceiveStream, SendStream

# =============================================================================
# Driver types
# =============================================================================


@dataclass(frozen=True, slots=True, kw_only=True)
class AnnounceEnded:
    error: BaseException | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class TrackBinding:
    """A track's effective alias binding changed.

    The most recent live subscription wins the binding (a replaced one is FINed, a clean end for
    that request, covering the peer's resubscribe races), and only while its Forward State permits
    data (``forward=0`` subscribes without authorizing it; REQUEST_UPDATE toggles it). ``None`` means
    no live forwarding subscription, so no data streams may be opened for the track.
    """

    track_name: str
    track_alias: int | None


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
    """A subscriber's request stream ended (FIN, reset, or track done); the argument is its request id."""
    on_track_binding: Callable[[TrackBinding], None] | None = None
    on_request_update: Callable[[IncomingRequestUpdate], None] | None = None
    on_closed: Callable[[Closed], None] | None = None
    """The session ended: transport closed, or a fatal protocol error."""


DEFAULT_IMPLEMENTATION_NAME = "moq-core (Python) publisher"

CLOSE_FLUSH_TIMEOUT = 0.25
"""Seconds ``close()`` waits for the subscription FINs / NAMESPACE_DONE writes to drain.

After that the transport closes out from under them.
"""


@dataclass(slots=True, kw_only=True)
class MoqtPublishSessionConfig:
    setup_options: Sequence[KeyValuePair] = ()
    """Extra Setup Options to send (an implementation identifier is added automatically)."""
    implementation_name: str = DEFAULT_IMPLEMENTATION_NAME
    """Identifies this implementation in SETUP (§10.3.1.5)."""
    callbacks: MoqtPublishSessionCallbacks = field(default_factory=MoqtPublishSessionCallbacks)


@dataclass(frozen=True, slots=True, kw_only=True)
class RegisterTrackOptions:
    track_namespace: Sequence[str]
    track_name: str
    track_properties: Sequence[KeyValuePair] = ()
    """Track metadata is explicit; raw tracks have no implied timestamp unit."""


class RegisteredTrack:
    """One servable track.

    ``end()`` FINs every live subscription's request stream (a FIN with no trailing bytes is the
    clean track end) and refuses future SUBSCRIBEs with DOES_NOT_EXIST. Idempotent.
    """

    __slots__ = ("_end", "track_name")

    def __init__(self, track_name: str, end: Callable[[], None]) -> None:
        self.track_name = track_name
        self._end = end

    def end(self) -> None:
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
    finished: bool = False

    async def fin(self) -> None:
        await self.writer.close()

    def cancel(self) -> None:
        self.finished = True
        self.writer.abort()
        self.reader.cancel()


@dataclass(slots=True, kw_only=True)
class _TrackRecord:
    track_properties: list[KeyValuePair]
    key: str
    track_name: str
    subscribers: list[_SubscriberStream] = field(default_factory=list)
    done: bool = False
    pending_fins: list[asyncio.Task[None]] = field(default_factory=list)
    """Replacement FINs still in flight (pruned as they settle).

    ``end()`` folds them into ``done_flushed``: a swept predecessor is already ``finished``, so it
    would otherwise be skipped and its queued FIN discarded by the transport close, turning the clean
    replacement into an abrupt reset.
    """
    done_flushed: asyncio.Task[None] | None = None
    """Settles when the FINs queued by ``end()`` land."""


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
        # Every inbound request ID ever seen. Reuse is a protocol violation (§10.1), and because
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
        self._destroyed = False

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
        if self._destroyed:
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

        FIN every live subscription's stream and retract the announce (NAMESPACE_DONE), give those
        writes a bounded window to reach the wire (closing a WebTransport discards queued data), then
        close the transport. Returns immediately; the transport closes within roughly
        :data:`CLOSE_FLUSH_TIMEOUT`.
        """
        self._close(close_code, reason)

    def _close(self, close_code: int, reason: str, error: BaseException | None = None) -> None:
        if self._destroyed or self._closing:
            return

        self._closing = True
        spawn(self._drain_and_close(close_code, reason, error))

    async def _drain_and_close(self, close_code: int, reason: str, error: BaseException | None = None) -> None:
        # Orderly close: closing the transport discards queued data, so the subscription FINs and
        # NAMESPACE_DONE retractions must be given a bounded window to land first. A peer should
        # observe every track ending cleanly, not an abrupt transport end. Neither GOAWAY nor
        # PUBLISH_DONE is sent: moq-lite-rs closes the session on a client GOAWAY, and treats any
        # post-SUBSCRIBE_OK byte on a subscribe stream as an error that aborts the track.
        #
        # One loop beat before the last-resort sweep below: track owners tearing down in the same
        # turn get to end their tracks themselves.
        await asyncio.sleep(0)

        if self._destroyed:
            return

        for track in self._tracks.values():
            self._end_track(track)

        writes: set[asyncio.Future[None]] = {
            track.done_flushed for track in self._tracks.values() if track.done_flushed is not None
        }

        for stream in self._namespace_streams:
            for suffix in stream.announced:
                writes.add(spawn(_swallow(stream.writer.write(encode_namespace_done(suffix)))))

            stream.announced = []

        if writes:
            await asyncio.wait(writes, timeout=CLOSE_FLUSH_TIMEOUT)

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
            except Exception:  # the response half died alone
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
                self._report_announce_loss()
                return

            if self._destroyed or self._announced_fired:
                return

            self._announced_fired = True
            _call(self._callbacks.on_announced, namespace)

        spawn(write_entry())

    def _report_announce_loss(self) -> None:
        """Fire ``on_announce_ended`` when no solicitation carries the announce anymore.

        Shared by the read-loop cleanup and the response-write failure path. Silent while closing,
        destroyed, or migrating (a GOAWAY makes the peer's streams ending the orderly drain).
        """
        if self._destroyed or self._closing or self._received_goaway:
            return

        if any(len(stream.announced) > 0 for stream in self._namespace_streams):
            return

        _call(
            self._callbacks.on_announce_ended,
            AnnounceEnded(error=MoqtProtocolError("the namespace solicitation carrying the announce ended")),
        )

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
        """FIN every live subscription's request stream and refuse future SUBSCRIBEs.

        A FIN with no trailing bytes is the clean track end. The aggregated write completion lands on
        ``track.done_flushed`` so ``close()`` can hold the transport open until the FINs reach the
        wire.
        """
        if track.done:
            return

        track.done = True
        writes: list[asyncio.Future[None]] = list(track.pending_fins)
        had_reported = False

        for subscriber in track.subscribers:
            if not subscriber.finished:
                subscriber.finished = True
                writes.append(spawn(_swallow(subscriber.fin())))

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

        async def flushed() -> None:
            if writes:
                await asyncio.wait(writes)

        track.done_flushed = spawn(flushed())

    async def open_uni_stream(self) -> SendStream:
        """Open a unidirectional data stream (for a track publisher's subgroups)."""
        # Refuse rather than touch the transport once the session is going away: encoders keep
        # producing frames for a beat after a peer kills the session.
        if self._destroyed or self._closing:
            raise RuntimeError("moq publish session: closed")

        return await self._transport.create_unidirectional_stream()

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
        """Request IDs are consumed once per session (§10.1) and a reuse is session-fatal.

        Every request-initiating message consumes one, and since draft-20 gave it an id of its own,
        so does REQUEST_UPDATE.
        """
        if request_id in self._peer_request_ids:
            raise MoqtProtocolError(f"peer reused request id {request_id}")

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
        except Exception:  # reset: the peer withdrew the namespace subscription
            pass

        # Withdrawal (reset) or a failed update: the carrier is over.
        if record in self._namespace_streams:
            self._namespace_streams.remove(record)

        spawn(_swallow(writer.close()))

        if len(record.announced) > 0:
            self._report_announce_loss()

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
        )

        track.subscribers.append(subscriber)

        try:
            await writer.write(encode_subscribe_ok(track_alias, MessageParameters(), track.track_properties))
            subscriber.accepted = True
        except Exception:  # peer cancelled before the response landed
            track.subscribers = [s for s in track.subscribers if s is not subscriber]
            reader.cancel()
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

                other.finished = True
                fin = spawn(_swallow(other.fin()))
                track.pending_fins.append(fin)

                def prune(
                    done: asyncio.Future[None], fin: asyncio.Task[None] = fin, track: _TrackRecord = track
                ) -> None:
                    if fin in track.pending_fins:
                        track.pending_fins.remove(fin)

                fin.add_done_callback(prune)

            _call(
                self._callbacks.on_track_binding,
                TrackBinding(track_name=track.track_name, track_alias=track_alias if subscriber.forwarding else None),
            )

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
                    # (or starts) with the subscription's authorization. Newest-wins keeps this
                    # subscriber the track's only live one, so its state IS the binding.
                    if parameters.forward is not None:
                        subscriber.forwarding = parameters.forward != 0

                        if not subscriber.finished and subscriber.accepted:
                            _call(
                                self._callbacks.on_track_binding,
                                TrackBinding(
                                    track_name=track.track_name,
                                    track_alias=subscriber.track_alias if subscriber.forwarding else None,
                                ),
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
                            # State is applied above and priority is advisory (the known relays discard
                            # ours too); anything else ends this request honestly.
                            subscriber.finished = True

                            async def reject_and_close() -> None:
                                await writer.write(
                                    encode_request_error(
                                        REQUEST_ERROR_CODE.NOT_SUPPORTED, "unsupported subscription update"
                                    )
                                )
                                await writer.close()

                            spawn(_swallow(reject_and_close()))

                            if subscriber.reported:
                                subscriber.reported = False
                                _call(self._callbacks.on_subscribe_end, subscribe.request_id)
                                self._emit_binding(track)
                        else:
                            # The subscribe driver treats REQUEST_OK as the update's completion
                            # (`on_update_ok`). Strictly reactive: a peer that never sends
                            # REQUEST_UPDATE (moq-lite-rs parks on end-of-stream after SUBSCRIBE_OK)
                            # never sees a trailing byte.
                            spawn(_swallow(writer.write(encode_request_ok())))
                elif isinstance(message, Goaway):
                    self._received_goaway = True
                    _call(self._callbacks.on_goaway, message)
                else:
                    raise MoqtProtocolError(f"unexpected {message.kind} on an incoming subscribe stream")
        except MoqtProtocolError:
            raise
        except Exception:  # reset: the subscriber cancelled (§3.3.3); ordinary end-of-request
            pass
        finally:
            subscriber.finished = True
            track.subscribers = [s for s in track.subscribers if s is not subscriber]
            # The subscription is over both ways, mirror of the namespace stream's cleanup: when the
            # peer FINed first, an unclosed response direction would leak a half-open stream per
            # unsubscribe until the transport closes. Already-FINed/aborted writers fail harmlessly.
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
        binding = live[-1] if live else None
        _call(
            self._callbacks.on_track_binding,
            TrackBinding(track_name=track.track_name, track_alias=binding.track_alias if binding else None),
        )


def _unsupported_update_fields(parameters: MessageParameters) -> list[str]:
    """Parameter names set on a REQUEST_UPDATE that the publish driver does not apply.

    Everything but FORWARD (applied) and SUBSCRIBER_PRIORITY (advisory).
    """
    ignored = {"forward", "subscriber_priority"}
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
    "CLOSE_FLUSH_TIMEOUT",
    "DEFAULT_IMPLEMENTATION_NAME",
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
