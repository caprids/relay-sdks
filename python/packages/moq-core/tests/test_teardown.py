"""The teardown rule (issue #530): after a driver's ``on_closed``, none of its callbacks run, and nothing it accepted
outlives its drain. One test per path the issue lists.

The QUIC adapter's ``close()`` drains in-flight data before it ends the connection, so the transports here keep every
stream open across a driver's close: whatever a driver still reads then, it reads after ``on_closed``.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import asynccontextmanager
from typing import Any

import pytest
from core_helpers.fake_transport import ControlledDataStream, FakeTransport
from core_helpers.raw_peer import RawRequest, open_raw_request, raw_subscribe, solicit_namespace
from core_helpers.streams import StreamClosedError
from core_helpers.transport_pair import TransportPair, create_transport_pair
from core_helpers.waiting import settle, wait_for

from moq_core.bytes import ByteWriter
from moq_core.control_messages import (
    REQUEST_ERROR_CODE,
    SETUP_OPTION,
    ControlMessage,
    ControlMessageDeframer,
    KeyValuePair,
    Location,
    MessageParameters,
    Publish,
    RequestError,
    Subscribe,
    SubscribeNamespace,
    TrackStatus,
    decode_control_message,
    encode_fetch_ok,
    encode_goaway,
    encode_namespace,
    encode_publish,
    encode_request_error,
    encode_request_ok,
    encode_request_update,
    encode_setup,
    encode_subscribe,
    encode_subscribe_namespace,
    encode_track_status,
)
from moq_core.object_stream import STREAM_TYPE
from moq_core.publish_session import (
    MAX_TRACK_PROPERTIES_LENGTH,
    MoqtPublishSession,
    MoqtPublishSessionCallbacks,
    MoqtPublishSessionConfig,
    RegisterTrackOptions,
    SubscribeFailed,
    TrackBinding,
    create_moqt_publish_session,
)
from moq_core.session import (
    FetchHandle,
    FetchHandlers,
    MoqtSession,
    MoqtSessionCallbacks,
    MoqtSessionConfig,
    MoqtSubscribeNamespaceOptions,
    MoqtSubscribeOptions,
    NamespaceSubscriptionHandlers,
    PublishResponder,
    TrackStatusHandlers,
    create_moqt_session,
)
from moq_core.transport import BidirectionalStream, ReceiveStream, SendStream

NAMESPACE = ["live", "abc123"]
TRACK = MoqtSubscribeOptions(track_namespace=NAMESPACE, track_name="video")
ACTIVE_COUNT = KeyValuePair(SETUP_OPTION.ACTIVE_COUNT, 1)
UNKNOWN_MESSAGE = b"\x3f\x00\x00"
"""A control message of a type no draft defines: session-fatal wherever it arrives."""
INVALID_FETCH_FLAGS = b"\x40\x80"
"""Serialization flags 0x80, which name neither an object nor an end of range: session-fatal."""


async def stopped(write: Callable[[bytes], Awaitable[None]]) -> bool:
    """Whether the far side has stopped reading (STOP_SENDING): an empty write then fails, and sends nothing."""
    try:
        await write(b"")
    except StreamClosedError:
        return True
    return False


async def read_to_end(readable: ReceiveStream) -> bytes:
    """Every byte up to the far side's FIN; raises if it resets its direction instead."""
    data = bytearray()

    while (chunk := await readable.read()) is not None:
        data += chunk

    return bytes(data)


def decode_frames(data: bytes) -> list[ControlMessage]:
    return [decode_control_message(frame) for frame in ControlMessageDeframer().push(data)]


def fetch_header(request_id: int) -> bytes:
    writer = ByteWriter()
    writer.write_varint(STREAM_TYPE.FETCH_HEADER)
    writer.write_varint(request_id)
    return writer.to_bytes()


def fetch_entry(group: int, object_id: int) -> bytes:
    """One object carrying its group and object IDs and a priority (§11.4.1)."""
    writer = ByteWriter()
    writer.write_varint(0x1C)
    writer.write_varint(group)
    writer.write_varint(object_id)
    writer.write_uint8(128)
    writer.write_varint(1)
    writer.write_bytes(b"\x07")
    return writer.to_bytes()


# ============================================================================
# Subscribe driver
# ============================================================================


class StuckSendStream:
    """A sending direction whose writes never land, as when the peer stops reading. Records whether it was aborted."""

    def __init__(self) -> None:
        self.aborted = False
        self._blocked: asyncio.Future[None] = asyncio.get_running_loop().create_future()

    async def write(self, data: bytes) -> None:
        await self._blocked

    async def close(self) -> None:
        await self._blocked

    def abort(self, reason: object = None) -> None:
        self.aborted = True
        if not self._blocked.done():
            self._blocked.set_exception(StreamClosedError("stream was aborted", reason))


class DrainingTransport(FakeTransport):
    """A fake whose ``close()`` keeps every stream open, as the QUIC adapter's does while it drains."""

    def __init__(self) -> None:
        super().__init__()
        self.requests_opened = 0
        self.opening: asyncio.Event | None = None
        """When set, opening a request stream waits for it."""
        self.stuck_writes: list[StuckSendStream] | None = None
        """When set, each request stream opened sends through a :class:`StuckSendStream`, collected here."""

    def close(self, close_code: int = 0, reason: str = "") -> None:
        self.close_calls += 1

    async def create_bidirectional_stream(self) -> BidirectionalStream:
        if self.opening is not None:
            await self.opening.wait()

        self.requests_opened += 1
        stream = await super().create_bidirectional_stream()

        if self.stuck_writes is None:
            return stream

        stuck = StuckSendStream()
        self.stuck_writes.append(stuck)
        return BidirectionalStream(readable=stream.readable, writable=stuck)


@asynccontextmanager
async def subscribe_session(
    callbacks: MoqtSessionCallbacks | None = None,
    *,
    setup: bool = True,
    setup_options: Sequence[KeyValuePair] = (),
) -> AsyncIterator[tuple[DrainingTransport, MoqtSession, ControlledDataStream]]:
    fake = DrainingTransport()
    session = create_moqt_session(
        fake, MoqtSessionConfig(callbacks=callbacks or MoqtSessionCallbacks(), setup_options=setup_options)
    )
    control = fake.open_controlled_data_stream()

    if setup:
        await control.write(encode_setup(setup_options))
        await session.ready

    try:
        yield fake, session, control
    finally:
        fake.drop_transport()


class TestSubscribeDriverTeardown:
    async def test_answers_no_request_whose_stream_opened_across_teardown(self) -> None:
        order: list[str] = []

        callbacks = MoqtSessionCallbacks(on_closed=lambda _: order.append("closed"))

        async with subscribe_session(callbacks) as (fake, session, _):
            fake.opening = asyncio.Event()
            session.track_status(
                TRACK,
                TrackStatusHandlers(
                    on_ok=lambda _: order.append("status ok"), on_error=lambda _: order.append("status error")
                ),
            )
            session.destroy()
            fake.opening.set()

            stream = await fake.next_request_stream()
            try:
                await stream.send(encode_request_ok())
            except StreamClosedError:
                pass
            await settle()

            assert order == ["closed"]
            await wait_for(lambda: stream.client_end.done())
            assert stream.client_end.result() == "reset"

    async def test_stops_reading_an_answered_track_status_at_teardown(self) -> None:
        answers: list[object] = []

        async with subscribe_session() as (fake, session, _):
            session.track_status(TRACK, TrackStatusHandlers(on_ok=answers.append))
            stream = await fake.next_request_stream()
            await stream.send(encode_request_ok())
            await wait_for(lambda: answers)

            session.destroy()

            assert await stopped(stream.send)

    async def test_reports_no_goaway_on_a_completed_fetch_after_teardown(self) -> None:
        order: list[str] = []

        callbacks = MoqtSessionCallbacks(on_closed=lambda _: order.append("closed"))

        async with subscribe_session(callbacks) as (fake, session, _):
            fetch = session.fetch(
                TRACK,
                FetchHandlers(
                    on_ok=lambda _: order.append("ok"),
                    on_end=lambda: order.append("end"),
                    on_goaway=lambda _: order.append("goaway"),
                ),
            )
            stream = await fake.next_request_stream()
            await stream.send(encode_fetch_ok(False, Location(4, 0)))
            fake.open_data_stream(fetch_header(fetch.request_id) + fetch_entry(4, 0))
            await wait_for(lambda: order == ["ok", "end"])

            session.destroy()
            try:
                await stream.send(encode_goaway())
            except StreamClosedError:
                pass
            await settle()

            assert order == ["ok", "end", "closed"]

    @pytest.mark.parametrize(
        ("response", "peer_fins"),
        [
            pytest.param(encode_request_ok(), True, id="answered and finished"),
            # REQUEST_ERROR alone completes the request.
            pytest.param(encode_request_error(REQUEST_ERROR_CODE.DOES_NOT_EXIST, "unknown track"), False, id="refused"),
        ],
    )
    async def test_finishes_its_side_of_a_completed_fetch_request(self, response: bytes, peer_fins: bool) -> None:
        async with subscribe_session() as (fake, session, _):
            session.fetch(TRACK)
            stream = await fake.next_request_stream()
            await stream.send(response)
            if peer_fins:
                await stream.fin()

            await wait_for(stream.client_end.done)
            assert stream.client_end.result() == "fin"

    async def test_resets_a_fetch_request_whose_answered_response_the_peer_reset(self) -> None:
        async with subscribe_session() as (fake, session, _):
            session.fetch(TRACK)
            stream = await fake.next_request_stream()
            await stream.send(encode_request_ok())
            await settle()
            stream.reset(RuntimeError("reset"))

            await wait_for(stream.client_end.done)
            assert stream.client_end.result() == "reset"

    async def test_resets_a_request_the_peer_finished_without_answering(self) -> None:
        errors: list[object] = []

        async with subscribe_session() as (fake, session, _):
            session.track_status(TRACK, TrackStatusHandlers(on_error=errors.append))
            stream = await fake.next_request_stream()
            await stream.fin()

            await wait_for(lambda: errors)
            await wait_for(stream.client_end.done)
            assert stream.client_end.result() == "reset"

    async def test_resets_a_completed_request_whose_own_message_is_still_being_written(self) -> None:
        async with subscribe_session() as (fake, session, _):
            stuck: list[StuckSendStream] = []
            fake.stuck_writes = stuck
            session.track_status(TRACK)
            # The request's write never lands; the peer answers and finishes anyway.
            stream = await fake.next_request_stream()
            await stream.send(encode_request_ok())
            await stream.fin()
            await settle()

            session.destroy()

            assert stuck[0].aborted

    async def test_answers_no_incoming_request_from_a_responder_kept_past_teardown(self) -> None:
        responders: list[PublishResponder] = []
        callbacks = MoqtSessionCallbacks(on_incoming_publish=lambda _, responder: responders.append(responder))

        async with subscribe_session(callbacks) as (fake, session, _):
            response = fake.open_incoming_request(
                encode_publish(Publish(request_id=1, track_namespace=NAMESPACE, track_name="video", track_alias=9))
            )
            await wait_for(lambda: responders)

            session.destroy()
            responders[0].accept()

            with pytest.raises(StreamClosedError):
                await asyncio.wait_for(read_to_end(response), 1)

    async def test_drops_an_incoming_request_whose_accepted_response_is_still_being_written(self) -> None:
        accepted: list[object] = []

        def accept(publish: Publish, responder: PublishResponder) -> None:
            responder.accept()
            accepted.append(publish)

        async with subscribe_session(MoqtSessionCallbacks(on_incoming_publish=accept)) as (fake, session, _):
            # Nothing reads the response yet, so the REQUEST_OK write waits.
            response = fake.open_incoming_request(
                encode_publish(Publish(request_id=1, track_namespace=NAMESPACE, track_name="video", track_alias=9))
            )
            await wait_for(lambda: accepted)

            session.destroy()

            with pytest.raises(StreamClosedError):
                await asyncio.wait_for(read_to_end(response), 1)

    async def test_starts_no_request_once_it_has_ended(self) -> None:
        async with subscribe_session() as (fake, session, _):
            session.destroy()
            session.subscribe(TRACK)
            session.fetch(TRACK)
            session.track_status(TRACK)
            session.subscribe_namespace(MoqtSubscribeNamespaceOptions(track_namespace_prefix=["live"]))
            await settle()

            assert fake.requests_opened == 0

    async def test_stops_a_fetch_data_stream_at_teardown_and_reports_nothing_more_from_it(self) -> None:
        order: list[str] = []

        callbacks = MoqtSessionCallbacks(on_closed=lambda _: order.append("closed"))

        async with subscribe_session(callbacks) as (fake, session, _):
            fetch = session.fetch(
                TRACK,
                FetchHandlers(
                    on_entry=lambda _: order.append("entry"),
                    on_end=lambda: order.append("end"),
                    on_reset=lambda _: order.append("reset"),
                ),
            )
            await fake.next_request_stream()
            data = fake.open_controlled_data_stream()
            await data.write(fetch_header(fetch.request_id) + fetch_entry(4, 0))
            await wait_for(lambda: order == ["entry"])

            session.destroy()

            assert await stopped(data.write)
            await settle()
            assert order == ["entry", "closed"]

    @pytest.mark.parametrize(
        "rest",
        [pytest.param(fetch_entry(0, 1), id="a whole entry"), pytest.param(fetch_entry(0, 1)[:3], id="part of one")],
    )
    async def test_reports_nothing_more_from_a_fetch_after_a_destroy_from_on_entry(self, rest: bytes) -> None:
        order: list[str] = []
        callbacks = MoqtSessionCallbacks(on_closed=lambda _: order.append("closed"))

        async with subscribe_session(callbacks) as (fake, session, _):

            def on_entry(_: object) -> None:
                order.append("entry")
                session.destroy()

            fetch = session.fetch(
                TRACK,
                FetchHandlers(
                    on_entry=on_entry, on_end=lambda: order.append("end"), on_reset=lambda _: order.append("reset")
                ),
            )
            await fake.next_request_stream()
            fake.open_data_stream(fetch_header(fetch.request_id) + fetch_entry(4, 0) + rest)
            await settle()

            assert order == ["entry", "closed"]

    async def test_reports_no_readiness_from_a_setup_completed_after_teardown(self) -> None:
        ready: list[object] = []

        async with subscribe_session(MoqtSessionCallbacks(on_ready=ready.append), setup=False) as (_, session, control):
            setup = encode_setup()
            await control.write(setup[:3])
            await settle()

            session.destroy()
            await control.write(setup[3:])
            await settle()

            assert ready == []

    async def test_reports_no_control_stream_goaway_after_teardown(self) -> None:
        goaways: list[object] = []

        async with subscribe_session(MoqtSessionCallbacks(on_goaway=goaways.append)) as (_, session, control):
            session.destroy()
            await control.write(encode_goaway())
            await settle()

            assert goaways == []

    async def _namespace_events(self, destroy_on: str, *frames: bytes, fin: bool = False) -> list[str]:
        order: list[str] = []

        async with subscribe_session(
            MoqtSessionCallbacks(on_closed=lambda _: order.append("closed")), setup_options=[ACTIVE_COUNT]
        ) as (fake, session, _):

            def event(name: str) -> Callable[..., None]:
                def record(*_: object) -> None:
                    order.append(name)
                    if name == destroy_on:
                        session.destroy()

                return record

            session.subscribe_namespace(
                MoqtSubscribeNamespaceOptions(track_namespace_prefix=["room"]),
                NamespaceSubscriptionHandlers(
                    on_ok=event("ok"),
                    on_synced=event("synced"),
                    on_namespace=event("namespace"),
                    on_namespace_done=event("namespace done"),
                    on_end=event("end"),
                ),
            )
            stream = await fake.next_request_stream()
            await stream.send(b"".join(frames))
            if fin:
                await wait_for(lambda: "synced" in order)
                await stream.fin()
            await settle()

        return order

    async def test_reports_no_sync_after_a_destroy_from_on_ok(self) -> None:
        order = await self._namespace_events("ok", encode_request_ok(MessageParameters(active_count=0)))

        assert order == ["ok", "closed"]

    async def test_reports_no_sync_after_a_destroy_from_the_last_counted_on_namespace(self) -> None:
        order = await self._namespace_events(
            "namespace", encode_request_ok(MessageParameters(active_count=1)), encode_namespace(["alice"])
        )

        assert order == ["ok", "namespace", "closed"]

    async def test_stops_the_end_of_request_reports_after_a_destroy_from_the_first_on_namespace_done(self) -> None:
        order = await self._namespace_events(
            "namespace done",
            encode_request_ok(MessageParameters(active_count=2)),
            encode_namespace(["alice"]),
            encode_namespace(["bob"]),
            fin=True,
        )

        assert order == ["ok", "namespace", "namespace", "synced", "namespace done", "closed"]

    async def test_resets_its_side_of_an_incoming_request_that_ends_before_a_whole_message(self) -> None:
        async with subscribe_session() as (fake, _, _):
            response = fake.open_incoming_bidi(
                encode_publish(Publish(request_id=1, track_namespace=NAMESPACE, track_name="video", track_alias=9))[:2]
            )

            await wait_for(response.done)
            assert isinstance(response.exception(), StreamClosedError)

    async def test_reports_no_incoming_publish_that_arrives_after_teardown(self) -> None:
        offered: list[object] = []

        async with subscribe_session(
            MoqtSessionCallbacks(on_incoming_publish=lambda publish, _: offered.append(publish))
        ) as (fake, session, _):
            session.destroy()
            response = fake.open_incoming_bidi(
                encode_publish(Publish(request_id=1, track_namespace=NAMESPACE, track_name="video", track_alias=9))
            )
            await settle()

            assert offered == []
            await wait_for(response.done)
            assert isinstance(response.exception(), StreamClosedError)

    @pytest.mark.parametrize("arrives", ["before teardown", "after teardown"])
    async def test_drops_an_incoming_request_whose_message_is_still_arriving_at_teardown(self, arrives: str) -> None:
        async with subscribe_session() as (fake, session, _):
            if arrives == "after teardown":
                session.destroy()

            request, response = fake.open_controlled_incoming_request()
            try:
                await request.write(
                    encode_publish(Publish(request_id=1, track_namespace=NAMESPACE, track_name="video", track_alias=9))[
                        :2
                    ]
                )
            except StreamClosedError:
                pass
            await settle()

            session.destroy()

            assert await stopped(request.write)
            with pytest.raises(StreamClosedError):
                await asyncio.wait_for(read_to_end(response), 1)

    @pytest.mark.parametrize(
        ("opens", "first_bytes"),
        [
            pytest.param("before teardown", b"", id="its type still to come"),
            pytest.param("after teardown", fetch_header(7)[:-1], id="a fetch header's request id still to come"),
        ],
    )
    async def test_stops_reading_a_data_stream_still_arriving_at_teardown(self, opens: str, first_bytes: bytes) -> None:
        async with subscribe_session() as (fake, session, _):
            if opens == "after teardown":
                session.destroy()

            data = fake.open_controlled_data_stream()
            try:
                await data.write(first_bytes)
            except StreamClosedError:
                pass
            await settle()

            session.destroy()

            assert await stopped(data.write)

    async def test_drops_an_incoming_request_whose_malformed_message_ends_it(self) -> None:
        closed: list[object] = []

        async with subscribe_session(MoqtSessionCallbacks(on_closed=closed.append)) as (fake, _, _):
            request, response = fake.open_controlled_incoming_request()
            await request.write(UNKNOWN_MESSAGE)
            await wait_for(lambda: closed)

            assert await stopped(request.write)
            with pytest.raises(StreamClosedError):
                await asyncio.wait_for(read_to_end(response), 1)

    async def test_stops_reading_a_fetch_data_stream_whose_malformed_entry_ends_it(self) -> None:
        closed: list[object] = []

        async with subscribe_session(MoqtSessionCallbacks(on_closed=closed.append)) as (fake, session, _):
            fetch = session.fetch(TRACK, FetchHandlers())
            await fake.next_request_stream()
            data = fake.open_controlled_data_stream()
            await data.write(fetch_header(fetch.request_id) + INVALID_FETCH_FLAGS)
            await wait_for(lambda: closed)

            assert await stopped(data.write)

    async def test_reports_nothing_from_the_rest_of_a_read_after_a_cancel_from_on_ok(self) -> None:
        order: list[str] = []
        fetch: FetchHandle | None = None

        def on_ok(_: object) -> None:
            order.append("ok")
            assert fetch is not None
            fetch.cancel()

        async with subscribe_session() as (fake, session, _):
            fetch = session.fetch(
                TRACK,
                FetchHandlers(
                    on_ok=on_ok,
                    on_error=lambda _: order.append("error"),
                    on_goaway=lambda _: order.append("goaway"),
                ),
            )
            stream = await fake.next_request_stream()
            try:
                await stream.send(encode_fetch_ok(False, Location(4, 0)) + encode_goaway())
            except StreamClosedError:
                pass
            await settle()

            assert order == ["ok"]


# ============================================================================
# Publish driver
# ============================================================================


@asynccontextmanager
async def publish_session(
    callbacks: MoqtPublishSessionCallbacks | None = None, *, hold_open: bool = False, setup: bool = True
) -> AsyncIterator[tuple[TransportPair, MoqtPublishSession, SendStream]]:
    pair = create_transport_pair()

    if hold_open:
        setattr(pair.client, "close", lambda close_code=0, reason="": None)  # noqa: B010 - replaces a method in place

    session = create_moqt_publish_session(
        pair.client, MoqtPublishSessionConfig(callbacks=callbacks or MoqtPublishSessionCallbacks())
    )

    async def read_publisher_control() -> None:
        stream = await pair.server.incoming_unidirectional_streams().__anext__()
        try:
            while await stream.read() is not None:
                pass
        except StreamClosedError:
            pass

    reading = asyncio.get_running_loop().create_task(read_publisher_control())
    control = await pair.server.create_unidirectional_stream()

    if setup:
        await control.write(encode_setup())
        await session.ready
        session.announce(NAMESPACE)

    session.register_track(RegisterTrackOptions(track_namespace=NAMESPACE, track_name="video"))

    try:
        yield pair, session, control
    finally:
        pair.server.close()
        await reading


class Recorded:
    """Publish callbacks that log in order, with ``close_on`` closing the session from inside the named one."""

    def __init__(self, close_on: str | None = None) -> None:
        self.order: list[str] = []
        self.session: MoqtPublishSession | None = None
        self._close_on = close_on

    def event(self, name: str) -> Callable[..., None]:
        def record(*_: Any) -> None:
            self.order.append(name)
            if name == self._close_on and self.session is not None:
                self.session.destroy()

        return record


async def ended_by_the_drain(subscribe: RawRequest) -> None:
    """Wait for the drain's PUBLISH_DONE on ``subscribe``, then FIN back as moq-relay does, releasing the drain."""
    await wait_for(lambda: "publish-done" in subscribe.kinds())
    await subscribe.fin()


class TestPublishDriverTeardown:
    async def test_stops_reading_every_request_stream_it_accepted_when_it_ends(self) -> None:
        closed: list[object] = []

        callbacks = MoqtPublishSessionCallbacks(on_closed=closed.append)

        async with publish_session(callbacks, hold_open=True) as (pair, session, _):
            solicitation = await solicit_namespace(pair.server, ["live"])
            await wait_for(lambda: solicitation.kinds() == ["request-ok", "namespace"])

            session.close()
            await wait_for(lambda: closed)

            assert await solicitation.stopped()

    async def test_reports_nothing_from_a_subscriptions_buffered_frames_after_a_close_from_on_request_update(
        self,
    ) -> None:
        recorded = Recorded(close_on="update")

        async with publish_session(
            MoqtPublishSessionCallbacks(
                on_track_binding=lambda binding: recorded.order.append(f"binding {binding.track_alias}"),
                on_request_update=recorded.event("update"),
                on_goaway=recorded.event("goaway"),
                on_closed=recorded.event("closed"),
            )
        ) as (pair, session, _):
            recorded.session = session
            subscribe = await raw_subscribe(pair.server, NAMESPACE, "video", 3)
            await wait_for(lambda: len(subscribe.received) == 1)

            await subscribe.send(
                encode_request_update(5) + encode_request_update(7, MessageParameters(forward=1)) + encode_goaway()
            )
            await ended_by_the_drain(subscribe)
            await wait_for(lambda: "closed" in recorded.order)

            assert recorded.order == ["binding 3", "update", "binding None", "closed"]

    async def test_reports_nothing_from_a_solicitations_buffered_frames_after_a_close_from_on_goaway(self) -> None:
        recorded = Recorded(close_on="goaway")

        async with publish_session(
            MoqtPublishSessionCallbacks(
                on_request_update=recorded.event("update"),
                on_goaway=recorded.event("goaway"),
                on_closed=recorded.event("closed"),
            )
        ) as (pair, session, _):
            recorded.session = session
            solicitation = await solicit_namespace(pair.server, ["live"])
            await wait_for(lambda: len(solicitation.received) == 2)

            await solicitation.send(encode_goaway() + encode_goaway() + encode_request_update(3))
            await wait_for(lambda: "closed" in recorded.order)

            assert recorded.order == ["goaway", "closed"]

    async def test_binds_no_alias_after_a_close_from_on_subscribe(self) -> None:
        bindings: list[TrackBinding] = []
        holder: list[MoqtPublishSession] = []

        async with publish_session(
            MoqtPublishSessionCallbacks(on_subscribe=lambda _: holder[0].destroy(), on_track_binding=bindings.append)
        ) as (pair, session, _):
            holder.append(session)
            subscribe = await raw_subscribe(pair.server, NAMESPACE, "video", 3)
            await wait_for(lambda: "publish-done" in subscribe.kinds())

            assert [binding.track_alias for binding in bindings if binding.track_alias is not None] == []

    async def test_reports_no_update_after_a_close_from_on_track_binding(self) -> None:
        updates: list[object] = []
        close_on_binding: list[MoqtPublishSession] = []

        def on_track_binding(_: TrackBinding) -> None:
            for session in close_on_binding:
                session.destroy()

        async with publish_session(
            MoqtPublishSessionCallbacks(on_track_binding=on_track_binding, on_request_update=updates.append)
        ) as (pair, session, _):
            subscribe = await raw_subscribe(pair.server, NAMESPACE, "video", 3)
            await wait_for(lambda: len(subscribe.received) == 1)

            close_on_binding.append(session)
            await subscribe.send(encode_request_update(5, MessageParameters(forward=0)))
            await wait_for(lambda: "publish-done" in subscribe.kinds())

            assert updates == []

    async def test_binds_no_alias_after_a_close_from_on_subscribe_end(self) -> None:
        order: list[str] = []
        holder: list[MoqtPublishSession] = []

        def on_subscribe_end(request_id: int) -> None:
            order.append(f"end {request_id}")
            holder[0].destroy()

        async with publish_session(
            MoqtPublishSessionCallbacks(
                on_subscribe_end=on_subscribe_end,
                on_track_binding=lambda binding: order.append(f"binding {binding.track_alias}"),
            )
        ) as (pair, session, _):
            holder.append(session)
            first = await raw_subscribe(pair.server, NAMESPACE, "video", 3)
            await wait_for(lambda: len(first.received) == 1)
            second = await raw_subscribe(pair.server, NAMESPACE, "video", 5)
            await wait_for(lambda: len(second.received) == 1)
            assert order == ["binding 3", "binding 5"]

            # The superseded subscription's request stream ends: its end is reported, and nothing binds after it.
            await first.fin()
            await wait_for(lambda: "end 3" in order)
            await settle()

            assert order.count("binding 5") == 1

    async def test_reports_no_goaway_and_no_solicitation_update_during_the_drain(self) -> None:
        goaways: list[object] = []
        updates: list[object] = []

        async with publish_session(
            MoqtPublishSessionCallbacks(on_goaway=goaways.append, on_request_update=updates.append)
        ) as (pair, session, control):
            solicitation = await solicit_namespace(pair.server, ["live"])
            await wait_for(lambda: len(solicitation.received) == 2)
            subscribe = await raw_subscribe(pair.server, NAMESPACE, "video", 3)
            await wait_for(lambda: len(subscribe.received) == 1)

            session.close()
            # Unanswered, the drain's PUBLISH_DONE holds the drain open.
            await wait_for(lambda: "publish-done" in subscribe.kinds())
            await control.write(encode_goaway())
            await subscribe.send(encode_goaway())
            await solicitation.send(encode_request_update(5))
            await wait_for(lambda: "request-error" in solicitation.kinds())

            assert goaways == []
            assert updates == []

    async def test_reports_no_subscribe_failure_that_lands_during_the_drain(self) -> None:
        failures: list[SubscribeFailed] = []
        callbacks = MoqtPublishSessionCallbacks(on_subscribe_failed=failures.append)

        async with publish_session(callbacks) as (pair, session, _):
            # The largest alias overflows SUBSCRIBE_OK's frame once the properties fill the limit.
            session.register_track(
                RegisterTrackOptions(
                    track_namespace=NAMESPACE,
                    track_name="large",
                    track_properties=[KeyValuePair(1, bytes(MAX_TRACK_PROPERTIES_LENGTH - 3))],
                )
            )
            held = await raw_subscribe(pair.server, NAMESPACE, "video", 3)
            await wait_for(lambda: len(held.received) == 1)
            # Nothing reads the response yet, so the REQUEST_ERROR write waits across the start of the drain.
            stream = await pair.server.create_bidirectional_stream()
            await stream.writable.write(
                encode_subscribe(Subscribe(request_id=2**64 - 1, track_namespace=NAMESPACE, track_name="large"))
            )
            await settle()

            session.close()
            await wait_for(lambda: "publish-done" in held.kinds())
            refusal = await read_to_end(stream.readable)
            await settle()

            assert [message.kind for message in decode_frames(refusal)] == ["request-error"]
            assert failures == []

    async def test_reports_no_readiness_from_a_setup_that_lands_during_the_drain(self) -> None:
        ready: list[object] = []

        callbacks = MoqtPublishSessionCallbacks(on_ready=ready.append)

        async with publish_session(callbacks, setup=False) as (pair, session, control):
            subscribe = await raw_subscribe(pair.server, NAMESPACE, "video", 3)
            await wait_for(lambda: len(subscribe.received) == 1)

            session.close()
            await wait_for(lambda: "publish-done" in subscribe.kinds())
            await control.write(encode_setup())
            await settle()

            assert ready == []

    @pytest.mark.parametrize(
        "request_message",
        [
            pytest.param(
                encode_subscribe_namespace(SubscribeNamespace(request_id=5, track_namespace_prefix=["live"])),
                id="SUBSCRIBE_NAMESPACE",
            ),
            pytest.param(
                encode_subscribe(Subscribe(request_id=5, track_namespace=NAMESPACE, track_name="video")),
                id="SUBSCRIBE",
            ),
            pytest.param(
                encode_track_status(TrackStatus(request_id=5, track_namespace=NAMESPACE, track_name="video")),
                id="TRACK_STATUS",
            ),
        ],
    )
    async def test_refuses_a_request_that_arrives_during_the_drain_with_going_away(
        self, request_message: bytes
    ) -> None:
        async with publish_session() as (pair, session, _):
            subscribe = await raw_subscribe(pair.server, NAMESPACE, "video", 3)
            await wait_for(lambda: len(subscribe.received) == 1)

            session.close()
            await wait_for(lambda: "publish-done" in subscribe.kinds())
            request = await open_raw_request(pair.server, request_message)
            await wait_for(request.ended)

            assert request.kinds() == ["request-error"]
            refusal = request.received[0]
            assert isinstance(refusal, RequestError)
            assert refusal.error_code == REQUEST_ERROR_CODE.GOING_AWAY

    async def test_announces_nothing_on_a_solicitation_whose_acceptance_lands_during_the_drain(self) -> None:
        async with publish_session() as (pair, session, _):
            subscribe = await raw_subscribe(pair.server, NAMESPACE, "video", 3)
            await wait_for(lambda: len(subscribe.received) == 1)

            # Nothing reads the response yet, so the REQUEST_OK write waits across the start of the drain.
            stream = await pair.server.create_bidirectional_stream()
            await stream.writable.write(
                encode_subscribe_namespace(SubscribeNamespace(request_id=5, track_namespace_prefix=["live"]))
            )
            await settle()
            session.close()
            await wait_for(lambda: "publish-done" in subscribe.kinds())

            response = asyncio.ensure_future(read_to_end(stream.readable))
            await settle(0.1)

            assert response.done()
            assert response.result() == encode_request_ok()

    async def test_reports_no_announce_whose_namespace_lands_during_the_drain(self) -> None:
        announced: list[object] = []

        async with publish_session(MoqtPublishSessionCallbacks(on_announced=announced.append)) as (pair, session, _):
            stream = await pair.server.create_bidirectional_stream()
            await stream.writable.write(
                encode_subscribe_namespace(SubscribeNamespace(request_id=5, track_namespace_prefix=["live"]))
            )
            # Read REQUEST_OK only: the NAMESPACE write then waits on this reader across the start of the drain.
            assert await stream.readable.read() == encode_request_ok()
            await settle()

            session.close()
            await settle()
            assert await stream.readable.read() == encode_namespace(["abc123"])
            await settle()

            assert announced == []

    async def test_resets_its_side_of_an_incoming_request_that_ends_before_a_whole_message(self) -> None:
        async with publish_session() as (pair, _, _):
            stream = await pair.server.create_bidirectional_stream()
            await stream.writable.write(
                encode_subscribe(Subscribe(request_id=1, track_namespace=NAMESPACE, track_name="video"))[:2]
            )
            await stream.writable.close()
            response = asyncio.ensure_future(read_to_end(stream.readable))

            await wait_for(response.done)
            assert isinstance(response.exception(), StreamClosedError)

    async def test_reports_no_subscription_whose_acceptance_lands_as_the_close_begins(self) -> None:
        subscribes: list[object] = []

        async with publish_session(MoqtPublishSessionCallbacks(on_subscribe=subscribes.append)) as (pair, session, _):
            # Nothing reads the response yet, so the SUBSCRIBE_OK write waits.
            stream = await pair.server.create_bidirectional_stream()
            await stream.writable.write(
                encode_subscribe(Subscribe(request_id=3, track_namespace=NAMESPACE, track_name="video"))
            )
            await settle()

            session.close()
            # Landed in the same turn, before the drain's first beat ends its subscriptions.
            await stream.readable.read()
            await settle()

            assert subscribes == []

    @pytest.mark.parametrize(
        "request_message",
        [
            pytest.param(
                encode_subscribe_namespace(SubscribeNamespace(request_id=5, track_namespace_prefix=["live"])),
                id="SUBSCRIBE_NAMESPACE",
            ),
            pytest.param(
                encode_track_status(TrackStatus(request_id=5, track_namespace=NAMESPACE, track_name="video")),
                id="TRACK_STATUS",
            ),
            pytest.param(
                encode_publish(Publish(request_id=5, track_namespace=NAMESPACE, track_name="video", track_alias=9)),
                id="PUBLISH",
            ),
        ],
    )
    async def test_resets_a_response_still_being_written_when_it_ends(self, request_message: bytes) -> None:
        closed: list[object] = []

        async with publish_session(MoqtPublishSessionCallbacks(on_closed=closed.append), hold_open=True) as (
            pair,
            session,
            _,
        ):
            # Nothing reads the response, so its write waits across the close.
            stream = await pair.server.create_bidirectional_stream()
            await stream.writable.write(request_message)
            await settle()

            session.close()
            await wait_for(lambda: closed)

            with pytest.raises(StreamClosedError):
                await asyncio.wait_for(read_to_end(stream.readable), 1)

    async def test_leaves_an_announce_carrier_unreset_when_it_ends(self) -> None:
        # Under a transport whose close drains, the carrier's NAMESPACE_DONE may still be unacknowledged, and a reset
        # would discard it (TA-16).
        closed: list[object] = []

        async with publish_session(MoqtPublishSessionCallbacks(on_closed=closed.append), hold_open=True) as (
            pair,
            session,
            _,
        ):
            solicitation = await solicit_namespace(pair.server, ["live"])
            await wait_for(lambda: solicitation.kinds() == ["request-ok", "namespace"])

            session.close()
            await wait_for(lambda: closed)
            await settle()

            assert solicitation.kinds() == ["request-ok", "namespace", "namespace-done"]
            assert solicitation.failure() is None

    async def test_drops_a_request_whose_message_arrives_after_it_ends(self) -> None:
        closed: list[object] = []

        async with publish_session(MoqtPublishSessionCallbacks(on_closed=closed.append), hold_open=True) as (
            pair,
            session,
            _,
        ):
            session.close()
            await wait_for(lambda: closed)

            stream = await pair.server.create_bidirectional_stream()
            try:
                await stream.writable.write(
                    encode_subscribe(Subscribe(request_id=5, track_namespace=NAMESPACE, track_name="video"))[:2]
                )
            except StreamClosedError:
                pass
            await settle()

            assert await stopped(stream.writable.write)
            with pytest.raises(StreamClosedError):
                await asyncio.wait_for(read_to_end(stream.readable), 1)

    @pytest.mark.parametrize("opens", ["before it ends", "after it ends"])
    async def test_stops_reading_a_unidirectional_stream_whose_type_has_not_arrived_when_it_ends(
        self, opens: str
    ) -> None:
        closed: list[object] = []

        async with publish_session(MoqtPublishSessionCallbacks(on_closed=closed.append), hold_open=True) as (
            pair,
            session,
            _,
        ):
            if opens == "after it ends":
                session.close()
                await wait_for(lambda: closed)

            stream = await pair.server.create_unidirectional_stream()
            await settle()

            session.close()
            await wait_for(lambda: closed)

            assert await stopped(stream.write)

    async def test_drops_an_incoming_request_whose_malformed_message_ends_it(self) -> None:
        closed: list[object] = []

        async with publish_session(MoqtPublishSessionCallbacks(on_closed=closed.append), hold_open=True) as (
            pair,
            _,
            _,
        ):
            stream = await pair.server.create_bidirectional_stream()
            await stream.writable.write(UNKNOWN_MESSAGE)
            await wait_for(lambda: closed)

            assert await stopped(stream.writable.write)
            with pytest.raises(StreamClosedError):
                await asyncio.wait_for(read_to_end(stream.readable), 1)

    async def test_stops_reading_a_subscription_whose_malformed_follow_up_ends_it(self) -> None:
        closed: list[object] = []

        async with publish_session(MoqtPublishSessionCallbacks(on_closed=closed.append), hold_open=True) as (
            pair,
            _,
            _,
        ):
            subscribe = await raw_subscribe(pair.server, NAMESPACE, "video", 3)
            await wait_for(lambda: subscribe.kinds() == ["subscribe-ok"])

            await subscribe.send(UNKNOWN_MESSAGE)
            await wait_for(lambda: closed)

            assert await subscribe.stopped()

    async def test_resets_a_publish_done_still_unwritten_when_it_ends_after_the_requester_finished(self) -> None:
        closed: list[object] = []

        async with publish_session(MoqtPublishSessionCallbacks(on_closed=closed.append), hold_open=True) as (
            pair,
            session,
            _,
        ):
            stream = await pair.server.create_bidirectional_stream()
            await stream.writable.write(
                encode_subscribe(Subscribe(request_id=3, track_namespace=NAMESPACE, track_name="video"))
            )
            assert [message.kind for message in decode_frames(await stream.readable.read() or b"")] == ["subscribe-ok"]
            # The FIN ends the subscription, and with nothing reading the response its PUBLISH_DONE write waits.
            await stream.writable.close()
            await settle()

            session.close()
            await wait_for(lambda: closed)

            with pytest.raises(StreamClosedError):
                await asyncio.wait_for(read_to_end(stream.readable), 1)
