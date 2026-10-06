"""Subscribe-side session driver tests, ported from the Video.js MOQT session tests (Apache-2.0)."""

import asyncio
from typing import Any

import pytest
from core_helpers.fake_transport import FakeTransport, create_fake_transport, encode_subgroup
from core_helpers.waiting import settle, wait_for

from moq_core.bytes import ByteWriter, utf8_encode
from moq_core.control_messages import (
    PUBLISH_DONE_STATUS,
    REQUEST_ERROR_CODE,
    ControlMessage,
    ControlMessageDeframer,
    ControlMessageFrame,
    Fetch,
    Location,
    MessageParameters,
    Publish,
    PublishNamespace,
    RelativeGroupFilter,
    RequestError,
    RequestUpdate,
    Setup,
    Subscribe,
    SubscribeNamespace,
    SubscribeOk,
    decode_control_message,
    encode_namespace,
    encode_namespace_done,
    encode_publish,
    encode_publish_done,
    encode_publish_namespace,
    encode_publish_state_notify,
    encode_request_error,
    encode_request_ok,
    encode_setup,
    encode_subscribe_ok,
)
from moq_core.errors import SESSION_ERROR
from moq_core.object_stream import FetchObject, FetchStreamEntry, MoqtObject
from moq_core.session import (
    Closed,
    FetchHandlers,
    MoqtSession,
    MoqtSessionCallbacks,
    MoqtSessionConfig,
    MoqtSubscribeNamespaceOptions,
    MoqtSubscribeOptions,
    NamespaceSubscriptionHandlers,
    SubgroupEnd,
    SubscriptionHandlers,
    TrackStatusHandlers,
    create_moqt_session,
)
from moq_core.transport import SendStream

# The fetch range playback would ask for if it fetched: the current group from its start.
CURRENT_GROUP = RelativeGroupFilter(groups_before_next=1)


class Recorder:
    """A ``vi.fn()`` stand-in: records every call."""

    def __init__(self) -> None:
        self.calls: list[tuple[Any, ...]] = []

    def __call__(self, *args: Any) -> None:
        self.calls.append(args)

    @property
    def called(self) -> bool:
        return bool(self.calls)

    @property
    def last(self) -> Any:
        return self.calls[-1][0] if self.calls and self.calls[-1] else None


class Harness:
    def __init__(self, fake: FakeTransport, session: MoqtSession) -> None:
        self.fake = fake
        self.session = session


def create_session_harness(callbacks: MoqtSessionCallbacks | None = None, **config: Any) -> Harness:
    fake = create_fake_transport()
    settings = MoqtSessionConfig(callbacks=callbacks or MoqtSessionCallbacks(), unknown_alias_timeout=0.2, **config)
    return Harness(fake, create_moqt_session(fake, settings))


def options(namespace: list[str], track: str, parameters: MessageParameters | None = None) -> MoqtSubscribeOptions:
    return MoqtSubscribeOptions(track_namespace=namespace, track_name=track, parameters=parameters)


def fetch_stream_bytes(request_id: int, payload: bytes = b'{"version":"1"}') -> bytes:
    """A fetch data stream: FETCH_HEADER + one object at (41, 0)."""
    writer = ByteWriter()
    writer.write_varint(0x05)
    writer.write_varint(request_id)
    writer.write_varint(0x1C)  # group delta + object delta + priority present
    writer.write_varint(41)  # group (absolute)
    writer.write_varint(0)  # object (absolute)
    writer.write_uint8(128)
    writer.write_varint(len(payload))
    writer.write_bytes(payload)
    return writer.to_bytes()


def decode_chunks(chunks: list[bytes]) -> list[ControlMessage]:
    deframer = ControlMessageDeframer()
    return [decode_control_message(frame) for chunk in chunks for frame in deframer.push(chunk)]


def _publish(request_id: int) -> bytes:
    return encode_publish(Publish(request_id=request_id, track_namespace=["live"], track_name="offered", track_alias=9))


def _publish_namespace(request_id: int) -> bytes:
    return encode_publish_namespace(PublishNamespace(request_id=request_id, track_namespace=["anon"]))


async def _expect_invalid_request_id(fake: FakeTransport, reason: str) -> None:
    await wait_for(lambda: fake.close_info is not None)
    assert fake.close_info is not None
    assert (fake.close_info.close_code, fake.close_info.reason) == (SESSION_ERROR.INVALID_REQUEST_ID, reason)


class _RecordingSendStream:
    """A ``SendStream`` that records every write attempted on it, including ones that then fail."""

    def __init__(self, inner: SendStream, writes: list[bytes]) -> None:
        self._inner = inner
        self._writes = writes

    async def write(self, data: bytes) -> None:
        self._writes.append(bytes(data))
        await self._inner.write(data)

    async def close(self) -> None:
        await self._inner.close()

    def abort(self, reason: object = None) -> None:
        self._inner.abort(reason)


class _WriteRecordingTransport(FakeTransport):
    """A fake whose client-opened unidirectional streams (the control stream) record their writes."""

    def __init__(self) -> None:
        super().__init__()
        self.uni_writes: list[bytes] = []

    async def create_unidirectional_stream(self) -> SendStream:
        return _RecordingSendStream(await super().create_unidirectional_stream(), self.uni_writes)


class TestCreateMoqtSession:
    async def test_sends_setup_on_a_fresh_unidirectional_control_stream(self) -> None:
        harness = create_session_harness()
        control = await harness.fake.client_control_stream()
        stream_type = await control.read_varint()

        assert stream_type == 0x2F00
        high = await control.read_uint8()
        low = await control.read_uint8()
        body = await control.read_bytes(high * 256 + low)
        setup = decode_control_message(ControlMessageFrame(stream_type, body))

        assert isinstance(setup, Setup)
        harness.session.destroy()

    async def test_resolves_ready_and_reports_server_options_once_the_server_setup_arrives(self) -> None:
        on_ready = Recorder()
        harness = create_session_harness(MoqtSessionCallbacks(on_ready=on_ready))

        harness.fake.send_server_setup()
        await harness.session.ready
        assert on_ready.calls == [([],)]
        harness.session.destroy()

    async def test_subscribes_on_a_new_request_stream_and_routes_objects_by_track_alias(self) -> None:
        harness = create_session_harness()
        harness.fake.send_server_setup()

        on_ok = Recorder()
        objects: list[MoqtObject] = []
        subgroup_ends: list[SubgroupEnd] = []

        harness.session.subscribe(
            options(["live", "stream1"], "video"),
            SubscriptionHandlers(on_ok=on_ok, on_object=objects.append, on_subgroup_end=subgroup_ends.append),
        )

        request = await harness.fake.next_request_stream()
        message = await request.first_message

        assert message == Subscribe(request_id=0, track_namespace=["live", "stream1"], track_name="video")

        await request.send(encode_subscribe_ok(11))
        await wait_for(lambda: on_ok.called)
        assert on_ok.last.track_alias == 11

        harness.fake.open_data_stream(encode_subgroup(11, 41, utf8_encode("frame")))
        await wait_for(lambda: len(objects) == 1)
        assert (objects[0].group_id, objects[0].object_id, objects[0].subgroup_id, objects[0].status) == (
            41,
            0,
            0,
            "normal",
        )
        await wait_for(lambda: len(subgroup_ends) == 1)
        assert subgroup_ends[0] == SubgroupEnd(group_id=41, subgroup_id=0, end_of_group=True)

        harness.session.destroy()

    async def test_buffers_a_data_stream_that_arrives_before_subscribe_ok_registers_the_alias(self) -> None:
        harness = create_session_harness()
        harness.fake.send_server_setup()

        objects: list[MoqtObject] = []

        harness.session.subscribe(options(["live"], "audio"), SubscriptionHandlers(on_object=objects.append))
        request = await harness.fake.next_request_stream()
        await request.first_message

        # Objects outrace the SUBSCRIBE_OK.
        harness.fake.open_data_stream(encode_subgroup(5, 100, utf8_encode("early")))
        await settle()
        assert objects == []

        await request.send(encode_subscribe_ok(5))
        await wait_for(lambda: len(objects) == 1)
        assert objects[0].group_id == 100

        harness.session.destroy()

    async def test_drops_a_data_stream_whose_alias_never_registers(self) -> None:
        harness = create_session_harness()
        harness.fake.send_server_setup()
        # No subscription at all: the stream must be cancelled after the timeout, and the session
        # must survive.
        harness.fake.open_data_stream(encode_subgroup(99, 1, utf8_encode("orphan")))
        await asyncio.sleep(0.3)
        assert harness.fake.close_info is None
        harness.session.destroy()

    async def test_surfaces_request_error_to_the_subscription_handlers(self) -> None:
        harness = create_session_harness()
        harness.fake.send_server_setup()

        on_error = Recorder()

        harness.session.subscribe(options(["live"], "nope"), SubscriptionHandlers(on_error=on_error))
        request = await harness.fake.next_request_stream()

        await request.first_message
        await request.send(encode_request_error(REQUEST_ERROR_CODE.DOES_NOT_EXIST, "no such track"))

        await wait_for(lambda: on_error.called)
        assert on_error.last.error_code == REQUEST_ERROR_CODE.DOES_NOT_EXIST
        assert on_error.last.reason == "no such track"
        harness.session.destroy()

    # §3.3.2 leaves "was this FIN premature?" to the caller; the session is the only layer that
    # knows a SUBSCRIBE needs a SUBSCRIBE_OK. Without these the subscription sits pending forever
    # with neither response nor error.
    async def test_fails_a_subscription_when_the_peer_fins_before_subscribe_ok(self) -> None:
        harness = create_session_harness()
        harness.fake.send_server_setup()

        on_error = Recorder()

        harness.session.subscribe(options(["live"], "video"), SubscriptionHandlers(on_error=on_error))
        request = await harness.fake.next_request_stream()

        await request.first_message
        await request.fin()

        await wait_for(lambda: on_error.called)
        assert on_error.last.reason == "request stream closed before a response"
        harness.session.destroy()

    async def test_fails_a_subscription_when_the_request_stream_is_reset_before_subscribe_ok(self) -> None:
        harness = create_session_harness()
        harness.fake.send_server_setup()

        on_error = Recorder()

        harness.session.subscribe(options(["live"], "video"), SubscriptionHandlers(on_error=on_error))
        request = await harness.fake.next_request_stream()

        await request.first_message
        request.reset(RuntimeError("stream reset"))

        await wait_for(lambda: on_error.called)
        harness.session.destroy()

    async def test_does_not_fail_a_subscription_when_the_fin_follows_subscribe_ok(self) -> None:
        harness = create_session_harness()
        harness.fake.send_server_setup()

        on_ok = Recorder()
        on_error = Recorder()

        harness.session.subscribe(options(["live"], "video"), SubscriptionHandlers(on_ok=on_ok, on_error=on_error))
        request = await harness.fake.next_request_stream()

        await request.first_message
        await request.send(encode_subscribe_ok(7))
        await wait_for(lambda: on_ok.called)
        await request.fin()

        await settle()
        assert not on_error.called
        harness.session.destroy()

    async def test_fails_a_subscription_that_gets_no_response_before_the_request_timeout(self) -> None:
        fake = create_fake_transport()
        session = create_moqt_session(fake, MoqtSessionConfig(request_timeout=0.01))

        fake.send_server_setup()

        on_error = Recorder()

        session.subscribe(options(["live"], "video"), SubscriptionHandlers(on_error=on_error))
        request = await fake.next_request_stream()

        await request.first_message

        await wait_for(lambda: on_error.called)
        assert on_error.last.error_code == REQUEST_ERROR_CODE.TIMEOUT
        session.destroy()

    async def test_ignores_a_response_that_arrives_after_the_request_timeout(self) -> None:
        fake = create_fake_transport()
        session = create_moqt_session(fake, MoqtSessionConfig(request_timeout=0.01, unknown_alias_timeout=0.05))

        fake.send_server_setup()

        on_ok = Recorder()
        on_error = Recorder()
        objects: list[MoqtObject] = []

        session.subscribe(
            options(["live"], "video"), SubscriptionHandlers(on_ok=on_ok, on_error=on_error, on_object=objects.append)
        )
        request = await fake.next_request_stream()

        await request.first_message
        await wait_for(lambda: on_error.called)

        # A SUBSCRIBE_OK after the timeout must not fire on_ok after on_error, and must not resurrect
        # the alias route of the dead subscription.
        try:
            await request.send(encode_subscribe_ok(11))
        except Exception:
            pass

        fake.open_data_stream(encode_subgroup(11, 41, utf8_encode("late")))
        await asyncio.sleep(0.08)
        assert not on_ok.called
        assert objects == []
        session.destroy()

    async def test_fails_a_pending_track_status_when_the_transport_drops(self) -> None:
        harness = create_session_harness()
        harness.fake.send_server_setup()

        on_error = Recorder()

        harness.session.track_status(options(["live"], "video"), TrackStatusHandlers(on_error=on_error))
        await (await harness.fake.next_request_stream()).first_message
        harness.fake.drop_transport()

        await wait_for(lambda: on_error.called)
        assert on_error.last.reason == "session closed before the request was answered"

    async def test_fails_pending_requests_when_the_transport_drops_but_not_on_a_local_close(self) -> None:
        dropped = create_session_harness()
        dropped.fake.send_server_setup()
        on_drop_error = Recorder()

        dropped.session.subscribe(options(["live"], "video"), SubscriptionHandlers(on_error=on_drop_error))
        await (await dropped.fake.next_request_stream()).first_message
        dropped.fake.drop_transport()
        await wait_for(lambda: on_drop_error.called)

        closed = create_session_harness()
        closed.fake.send_server_setup()
        on_close_error = Recorder()

        closed.session.subscribe(options(["live"], "video"), SubscriptionHandlers(on_error=on_close_error))
        await (await closed.fake.next_request_stream()).first_message
        closed.session.close()
        await settle()
        assert not on_close_error.called

    async def test_surfaces_publish_done_and_keeps_late_subgroups_flowing_until_cancel(self) -> None:
        harness = create_session_harness()
        harness.fake.send_server_setup()

        on_done = Recorder()
        objects: list[MoqtObject] = []
        subscription = harness.session.subscribe(
            options(["live"], "video"), SubscriptionHandlers(on_done=on_done, on_object=objects.append)
        )
        request = await harness.fake.next_request_stream()

        await request.first_message
        await request.send(encode_subscribe_ok(7))
        await request.send(encode_publish_done(PUBLISH_DONE_STATUS.TRACK_ENDED, 1, "ended"))

        await wait_for(lambda: on_done.called)
        assert on_done.last.status_code == PUBLISH_DONE_STATUS.TRACK_ENDED

        # A late-opening stream still routes (PUBLISH_DONE often beats data).
        harness.fake.open_data_stream(encode_subgroup(7, 1, utf8_encode("late")))
        await wait_for(lambda: len(objects) == 1)

        subscription.cancel()
        harness.session.destroy()

    async def test_fins_its_request_stream_once_the_publishers_fin_follows_publish_done(self) -> None:
        harness = create_session_harness()
        harness.fake.send_server_setup()

        objects: list[MoqtObject] = []
        subscription = harness.session.subscribe(
            options(["live"], "video"), SubscriptionHandlers(on_object=objects.append)
        )
        request = await harness.fake.next_request_stream()

        await request.first_message
        await request.send(encode_subscribe_ok(7))
        await request.send(encode_publish_done(PUBLISH_DONE_STATUS.TRACK_ENDED, 0, "ended"))
        await settle()

        # PUBLISH_DONE alone does not complete the request (§3.3.2): the publisher's FIN does.
        assert not request.client_end.done()

        await request.fin()

        assert await asyncio.wait_for(request.client_end, 1) == "fin"

        # Late data still routes, and an update() after the FIN writes nothing.
        harness.fake.open_data_stream(encode_subgroup(7, 1, utf8_encode("late")))
        await wait_for(lambda: len(objects) == 1)
        subscription.update(MessageParameters(forward=0))
        await settle()
        assert len(request.messages) == 1

        subscription.cancel()
        harness.session.destroy()

    async def test_does_not_fin_back_for_a_publisher_fin_without_publish_done(self) -> None:
        harness = create_session_harness()
        harness.fake.send_server_setup()

        harness.session.subscribe(options(["live"], "video"))
        request = await harness.fake.next_request_stream()

        await request.first_message
        await request.send(encode_subscribe_ok(7))
        await request.fin()
        await settle()

        assert not request.client_end.done()

        harness.session.destroy()

    async def test_allocates_even_request_ids_starting_at_0(self) -> None:
        harness = create_session_harness()
        harness.fake.send_server_setup()
        first = harness.session.subscribe(options(["a"], "x"))
        second = harness.session.subscribe(options(["a"], "y"))

        assert first.request_id == 0
        assert second.request_id == 2
        harness.session.destroy()

    async def test_routes_fetch_streams_by_request_id(self) -> None:
        harness = create_session_harness()
        harness.fake.send_server_setup()

        harness.session.subscribe(options(["live"], "catalog"))
        subscribe_stream = await harness.fake.next_request_stream()
        await subscribe_stream.first_message

        entries: list[FetchStreamEntry] = []
        on_end = Recorder()

        harness.session.fetch(
            options(["live"], "catalog", MessageParameters(location_filter=CURRENT_GROUP)),
            FetchHandlers(on_entry=entries.append, on_end=on_end),
        )
        fetch_stream = await harness.fake.next_request_stream()
        fetch_message = await fetch_stream.first_message

        assert fetch_message == Fetch(
            request_id=2,
            track_namespace=["live"],
            track_name="catalog",
            parameters=MessageParameters(location_filter=CURRENT_GROUP),
        )

        harness.fake.open_data_stream(fetch_stream_bytes(2))

        await wait_for(lambda: len(entries) == 1)
        assert isinstance(entries[0], FetchObject)
        assert (entries[0].group_id, entries[0].object_id) == (41, 0)
        await wait_for(lambda: on_end.called)

        harness.session.destroy()

    async def test_tolerates_request_ok_in_place_of_fetch_ok_on_a_fetch_stream(self) -> None:
        on_closed = Recorder()
        harness = create_session_harness(MoqtSessionCallbacks(on_closed=on_closed))
        harness.fake.send_server_setup()

        harness.session.subscribe(options(["live"], "catalog"))
        await harness.fake.next_request_stream()

        entries: list[FetchStreamEntry] = []
        on_end = Recorder()

        harness.session.fetch(
            options(["live"], "catalog", MessageParameters(location_filter=CURRENT_GROUP)),
            FetchHandlers(on_entry=entries.append, on_end=on_end),
        )
        fetch_stream = await harness.fake.next_request_stream()

        await fetch_stream.first_message
        await fetch_stream.send(encode_request_ok())

        # The data stream still delivers normally: REQUEST_OK on the bidi stream has no bearing on the
        # separate unidirectional data stream.
        harness.fake.open_data_stream(fetch_stream_bytes(2))

        await wait_for(lambda: len(entries) == 1)
        await wait_for(lambda: on_end.called)
        assert not on_closed.called

        harness.session.destroy()

    # REQUEST_OK *is* the response on such a relay, so it has to disarm the response timer. If it
    # doesn't, the deadline fires on an answered fetch, deletes the record, and the fetch stream
    # handler then cancels the data stream instead of delivering it.
    async def test_settles_a_fetch_answered_with_request_ok_so_the_request_timeout_cannot_fail_it(self) -> None:
        fake = create_fake_transport()
        # 25ms: below the 30ms wait (an un-disarmed timer would still fire in time to be observed)
        # with headroom for the REQUEST_OK on slow CI.
        session = create_moqt_session(fake, MoqtSessionConfig(request_timeout=0.025, unknown_alias_timeout=0.2))

        fake.send_server_setup()

        session.subscribe(options(["live"], "catalog"))
        await fake.next_request_stream()

        on_error = Recorder()
        entries: list[FetchStreamEntry] = []

        session.fetch(
            options(["live"], "catalog", MessageParameters(location_filter=CURRENT_GROUP)),
            FetchHandlers(on_entry=entries.append, on_error=on_error),
        )
        fetch_stream = await fake.next_request_stream()

        await fetch_stream.first_message
        await fetch_stream.send(encode_request_ok())

        await asyncio.sleep(0.03)
        assert not on_error.called

        # The data stream arrives after the deadline would have passed.
        fake.open_data_stream(fetch_stream_bytes(2))

        await wait_for(lambda: len(entries) == 1)
        session.destroy()

    async def test_rejects_ready_when_the_session_closes_before_the_server_setup(self) -> None:
        harness = create_session_harness()

        harness.session.close()
        with pytest.raises(Exception, match="session closed before server SETUP"):
            await harness.session.ready

    async def test_reports_a_transport_drop_before_the_server_setup_as_an_error_close(self) -> None:
        on_closed = Recorder()
        harness = create_session_harness(MoqtSessionCallbacks(on_closed=on_closed))

        harness.fake.drop_transport()

        # Callback-only consumers must observe the failure too: a clean Closed() would read as a
        # normal session close.
        await wait_for(lambda: on_closed.called)
        closed = on_closed.last
        assert isinstance(closed, Closed)
        assert closed.error is not None
        assert str(closed.error) == "session closed before server SETUP"
        with pytest.raises(Exception, match="session closed before server SETUP"):
            await harness.session.ready

    async def test_writes_nothing_on_the_control_stream_after_setup_when_closed_no_goaway(self) -> None:
        # A GOAWAY sent as the transport closes gives the peer nothing to act on, so close() sends
        # none. The fake breaks its pipes on close, so record every write the session attempts, not
        # what arrives.
        fake = _WriteRecordingTransport()
        attempted = fake.uni_writes
        session = create_moqt_session(fake, MoqtSessionConfig())
        fake.send_server_setup()
        await session.ready
        await wait_for(lambda: len(attempted) > 0)

        session.close()
        await settle()
        assert [type(message) for message in decode_chunks(attempted)] == [Setup]
        assert fake.close_info is not None
        assert fake.close_info.close_code == 0

    async def test_reports_a_local_close_before_the_server_setup_as_a_clean_close(self) -> None:
        on_closed = Recorder()
        harness = create_session_harness(MoqtSessionCallbacks(on_closed=on_closed))

        harness.session.close()

        await wait_for(lambda: on_closed.called)
        assert on_closed.last == Closed()

    async def test_reports_a_reset_fetch_stream_as_on_reset_not_a_clean_on_end(self) -> None:
        harness = create_session_harness()
        harness.fake.send_server_setup()

        harness.session.subscribe(options(["live"], "catalog"))
        subscribe_stream = await harness.fake.next_request_stream()
        await subscribe_stream.first_message

        entries: list[FetchStreamEntry] = []
        on_end = Recorder()
        on_reset = Recorder()

        harness.session.fetch(
            options(["live"], "catalog", MessageParameters(location_filter=CURRENT_GROUP)),
            FetchHandlers(on_entry=entries.append, on_end=on_end, on_reset=on_reset),
        )
        fetch_stream = await harness.fake.next_request_stream()
        await fetch_stream.first_message

        # Fetch data stream: header + one full entry, then a reset mid-replay.
        data_stream = harness.fake.open_controlled_data_stream()

        await data_stream.write(fetch_stream_bytes(2))
        await wait_for(lambda: len(entries) == 1)
        data_stream.reset(RuntimeError("reset mid-replay"))

        await wait_for(lambda: on_reset.called)
        assert not on_end.called

        harness.session.destroy()

    async def test_treats_a_truncated_response_frame_on_a_request_stream_as_a_protocol_error(self) -> None:
        on_closed = Recorder()
        harness = create_session_harness(MoqtSessionCallbacks(on_closed=on_closed))

        harness.fake.send_server_setup()
        await harness.session.ready

        harness.session.subscribe(options(["live"], "video"))
        request = await harness.fake.next_request_stream()
        await request.first_message

        # A frame header promising a 100-byte body, then FIN with none of it.
        partial = ByteWriter()
        partial.write_varint(0x04)
        partial.write_uint16(100)
        await request.send(partial.to_bytes())
        await request.fin()

        await wait_for(lambda: on_closed.called)
        assert harness.fake.close_info is not None
        assert harness.fake.close_info.close_code == 0x3

    async def test_reports_goaway_from_the_control_stream(self) -> None:
        on_goaway = Recorder()
        harness = create_session_harness(MoqtSessionCallbacks(on_goaway=on_goaway))
        # Server control stream with SETUP then GOAWAY.
        writer = ByteWriter()
        writer.write_bytes(encode_setup([]))
        # GOAWAY with a migration URI.
        goaway_body = ByteWriter()
        uri = utf8_encode("https://relay2.example.com/moq")
        goaway_body.write_varint(len(uri))
        goaway_body.write_bytes(uri)
        goaway_body.write_varint(1000)
        goaway_frame = ByteWriter()
        goaway_frame.write_varint(0x10)
        goaway_frame.write_uint16(goaway_body.length)
        goaway_frame.write_bytes(goaway_body.to_bytes())
        writer.write_bytes(goaway_frame.to_bytes())
        harness.fake.open_data_stream(writer.to_bytes())

        await wait_for(lambda: on_goaway.called)
        assert on_goaway.last.new_session_uri == "https://relay2.example.com/moq"
        assert on_goaway.last.timeout == 1000
        harness.session.destroy()

    async def test_rejects_incoming_publish_with_uninterested_by_default(self) -> None:
        harness = create_session_harness()
        harness.fake.send_server_setup()

        # Server-initiated PUBLISH request stream.
        publish_body = ByteWriter()
        publish_body.write_varint(1)  # server request id (odd)
        publish_body.write_varint(1)  # namespace field count
        namespace_field = utf8_encode("live")
        publish_body.write_varint(len(namespace_field))
        publish_body.write_bytes(namespace_field)
        name = utf8_encode("unwanted")
        publish_body.write_varint(len(name))
        publish_body.write_bytes(name)
        publish_body.write_varint(9)  # track alias
        publish_body.write_varint(0)  # parameter count
        publish_frame = ByteWriter()
        publish_frame.write_varint(0x1D)
        publish_frame.write_uint16(publish_body.length)
        publish_frame.write_bytes(publish_body.to_bytes())

        responses = harness.fake.open_incoming_bidi(publish_frame.to_bytes())
        messages = decode_chunks(await responses)

        assert isinstance(messages[0], RequestError)
        assert messages[0].error_code == REQUEST_ERROR_CODE.UNINTERESTED

        harness.session.destroy()

    async def test_lets_the_application_accept_an_incoming_publish(self) -> None:
        seen: list[Any] = []

        def on_incoming_publish(publish: Any, responder: Any) -> None:
            seen.append(publish)
            responder.accept()
            responder.reject()  # ignored: exactly one response goes out

        harness = create_session_harness(MoqtSessionCallbacks(on_incoming_publish=on_incoming_publish))
        harness.fake.send_server_setup()

        from moq_core.control_messages import Publish, RequestOk, encode_publish

        responses = harness.fake.open_incoming_bidi(
            encode_publish(Publish(request_id=1, track_namespace=["live"], track_name="offered", track_alias=9))
        )
        messages = decode_chunks(await responses)

        assert messages == [RequestOk()]
        assert seen[0].track_name == "offered"
        harness.session.destroy()

    async def test_rejects_incoming_publish_namespace_with_not_supported_by_default(self) -> None:
        harness = create_session_harness()
        harness.fake.send_server_setup()

        # Server-initiated PUBLISH_NAMESPACE request stream: relays commonly announce active
        # namespaces unsolicited to newly connected clients.
        responses = harness.fake.open_incoming_bidi(
            encode_publish_namespace(PublishNamespace(request_id=1, track_namespace=["anon"]))
        )
        messages = decode_chunks(await responses)

        assert isinstance(messages[0], RequestError)
        assert messages[0].error_code == REQUEST_ERROR_CODE.NOT_SUPPORTED

        harness.session.destroy()

    async def test_closes_with_invalid_request_id_on_an_even_publish_id_before_offering_it(self) -> None:
        seen: list[Any] = []
        harness = create_session_harness(MoqtSessionCallbacks(on_incoming_publish=lambda p, r: seen.append(p)))
        harness.fake.send_server_setup()

        harness.fake.open_incoming_bidi(_publish(2))

        await _expect_invalid_request_id(harness.fake, "peer sent even request id 2")
        assert seen == []

    async def test_closes_with_invalid_request_id_on_a_reused_publish_id(self) -> None:
        seen: list[Any] = []

        def on_incoming_publish(publish: Any, responder: Any) -> None:
            seen.append(publish)
            responder.accept()

        harness = create_session_harness(MoqtSessionCallbacks(on_incoming_publish=on_incoming_publish))
        harness.fake.send_server_setup()

        await harness.fake.open_incoming_bidi(_publish(1))
        harness.fake.open_incoming_bidi(_publish(1))

        await _expect_invalid_request_id(harness.fake, "peer reused request id 1")
        assert len(seen) == 1

    async def test_closes_with_invalid_request_id_on_an_even_id_of_a_kind_it_rejects(self) -> None:
        harness = create_session_harness()
        harness.fake.send_server_setup()

        harness.fake.open_incoming_bidi(_publish_namespace(4))

        await _expect_invalid_request_id(harness.fake, "peer sent even request id 4")

    async def test_counts_a_rejected_request_as_consuming_its_id(self) -> None:
        harness = create_session_harness()
        harness.fake.send_server_setup()

        await harness.fake.open_incoming_bidi(_publish_namespace(1))
        harness.fake.open_incoming_bidi(_publish(1))

        await _expect_invalid_request_id(harness.fake, "peer reused request id 1")

    async def test_answers_distinct_odd_request_ids_and_stays_open(self) -> None:
        harness = create_session_harness()
        harness.fake.send_server_setup()

        await harness.fake.open_incoming_bidi(_publish(1))
        messages = decode_chunks(await harness.fake.open_incoming_bidi(_publish_namespace(3)))

        assert isinstance(messages[0], RequestError)
        assert messages[0].error_code == REQUEST_ERROR_CODE.NOT_SUPPORTED
        assert harness.fake.close_info is None
        harness.session.destroy()

    async def test_closes_the_session_when_an_incoming_request_frame_has_a_truncated_body(self) -> None:
        on_closed = Recorder()
        harness = create_session_harness(MoqtSessionCallbacks(on_closed=on_closed))
        harness.fake.send_server_setup()

        # A PUBLISH_NAMESPACE frame whose declared body length cuts the namespace off: decoding must
        # terminate the session, not vanish as an unhandled read error in the accept loop.
        frame = ByteWriter()
        frame.write_varint(0x06)  # PUBLISH_NAMESPACE
        frame.write_uint16(1)
        frame.write_varint(1)  # request id, then no namespace at all
        harness.fake.open_incoming_bidi(frame.to_bytes())

        await wait_for(lambda: on_closed.called)
        assert harness.fake.close_info is not None
        assert harness.fake.close_info.close_code == 0x3

    async def test_closes_the_session_with_protocol_violation_on_an_unknown_data_stream_type(self) -> None:
        on_closed = Recorder()
        harness = create_session_harness(MoqtSessionCallbacks(on_closed=on_closed))
        harness.fake.send_server_setup()

        writer = ByteWriter()
        writer.write_varint(0x0F)  # not a valid unidirectional stream type
        harness.fake.open_data_stream(writer.to_bytes())

        await wait_for(lambda: on_closed.called)
        assert harness.fake.close_info is not None
        assert harness.fake.close_info.close_code == 0x3

    async def test_surfaces_publish_state_notify_to_the_subscription_without_replying(self) -> None:
        harness = create_session_harness()
        harness.fake.send_server_setup()

        on_state_notify = Recorder()

        harness.session.subscribe(options(["live"], "video"), SubscriptionHandlers(on_state_notify=on_state_notify))
        request = await harness.fake.next_request_stream()

        await request.first_message
        await request.send(encode_subscribe_ok(7))
        await request.send(encode_publish_state_notify(MessageParameters(largest_object=Location(12, 3))))

        await wait_for(lambda: on_state_notify.called)
        assert on_state_notify.last == MessageParameters(largest_object=Location(12, 3))

        # §10.10: unilateral; no REQUEST_OK or REQUEST_ERROR goes back.
        await settle()
        assert len(request.messages) == 1
        harness.session.destroy()

    async def test_closes_the_session_on_publish_state_notify_over_a_fetch_stream(self) -> None:
        on_closed = Recorder()
        harness = create_session_harness(MoqtSessionCallbacks(on_closed=on_closed))
        harness.fake.send_server_setup()
        harness.session.fetch(options(["live"], "catalog"))
        request = await harness.fake.next_request_stream()

        await request.first_message
        await request.send(encode_publish_state_notify(MessageParameters()))

        await wait_for(lambda: on_closed.called)
        assert harness.fake.close_info is not None
        assert harness.fake.close_info.close_code == 0x3

    # §10.1 counts REQUEST_UPDATE among the messages that consume a Request ID; reusing the
    # subscription's would be a duplicate the peer closes on.
    async def test_gives_each_request_update_a_fresh_request_id(self) -> None:
        harness = create_session_harness()
        harness.fake.send_server_setup()

        subscription = harness.session.subscribe(options(["live"], "video"))
        request = await harness.fake.next_request_stream()

        await request.first_message
        subscription.update(MessageParameters(forward=0))
        subscription.update(MessageParameters(forward=1))

        await wait_for(lambda: len(request.messages) == 3)
        assert request.messages[1] == RequestUpdate(request_id=2, parameters=MessageParameters(forward=0))
        assert request.messages[2] == RequestUpdate(request_id=4, parameters=MessageParameters(forward=1))
        harness.session.destroy()

    async def test_reports_an_uncountable_publish_done_stream_count_as_none(self) -> None:
        on_closed = Recorder()
        harness = create_session_harness(MoqtSessionCallbacks(on_closed=on_closed))
        harness.fake.send_server_setup()

        on_done = Recorder()

        harness.session.subscribe(options(["live"], "video"), SubscriptionHandlers(on_done=on_done))
        request = await harness.fake.next_request_stream()

        await request.first_message
        await request.send(encode_subscribe_ok(7))

        # Status, the 9-byte varint 2^64-1 (§10.12's sentinel), empty reason.
        body = bytes([PUBLISH_DONE_STATUS.TRACK_ENDED, *([0xFF] * 9), 0x00])
        frame = ByteWriter()
        frame.write_varint(0x0B)
        frame.write_uint16(len(body))
        frame.write_bytes(body)
        await request.send(frame.to_bytes())

        await wait_for(lambda: on_done.called)
        assert on_done.last.status_code == PUBLISH_DONE_STATUS.TRACK_ENDED
        assert on_done.last.stream_count is None
        assert on_done.last.reason == ""
        assert not on_closed.called
        harness.session.destroy()

    async def test_answers_track_status_and_fins_the_request(self) -> None:
        harness = create_session_harness()
        harness.fake.send_server_setup()

        on_ok = Recorder()
        harness.session.track_status(options(["live"], "video"), TrackStatusHandlers(on_ok=on_ok))
        request = await harness.fake.next_request_stream()
        await request.first_message
        await request.send(encode_request_ok(MessageParameters(largest_object=Location(4, 2))))

        await wait_for(lambda: on_ok.called)
        assert on_ok.last.parameters == MessageParameters(largest_object=Location(4, 2))
        harness.session.destroy()

    async def test_duplicate_track_alias_across_subscriptions_is_session_fatal(self) -> None:
        on_closed = Recorder()
        harness = create_session_harness(MoqtSessionCallbacks(on_closed=on_closed))
        harness.fake.send_server_setup()

        harness.session.subscribe(options(["live"], "a"))
        harness.session.subscribe(options(["live"], "b"))
        first = await harness.fake.next_request_stream()
        second = await harness.fake.next_request_stream()
        await first.first_message
        await second.first_message
        await first.send(encode_subscribe_ok(5))
        await second.send(encode_subscribe_ok(5))

        await wait_for(lambda: on_closed.called)
        assert harness.fake.close_info is not None
        assert harness.fake.close_info.close_code == 0x5

    async def test_subscribe_ok_reports_the_full_message(self) -> None:
        harness = create_session_harness()
        harness.fake.send_server_setup()
        on_ok = Recorder()
        harness.session.subscribe(options(["live"], "video"), SubscriptionHandlers(on_ok=on_ok))
        request = await harness.fake.next_request_stream()
        await request.first_message
        await request.send(encode_subscribe_ok(3, MessageParameters(expires=5)))
        await wait_for(lambda: on_ok.called)
        assert on_ok.last == SubscribeOk(track_alias=3, parameters=MessageParameters(expires=5), track_properties=[])
        harness.session.destroy()


def _room() -> MoqtSubscribeNamespaceOptions:
    return MoqtSubscribeNamespaceOptions(track_namespace_prefix=["room"])


class TestSubscribeNamespace:
    async def test_sends_subscribe_namespace_and_reports_request_ok_then_namespace_and_namespace_done(self) -> None:
        harness = create_session_harness()
        harness.fake.send_server_setup()
        events: list[object] = []

        harness.session.subscribe_namespace(
            _room(),
            NamespaceSubscriptionHandlers(
                on_ok=lambda: events.append("ok"),
                on_namespace=lambda suffix: events.append(("namespace", suffix)),
                on_namespace_done=lambda suffix: events.append(("done", suffix)),
            ),
        )
        request = await harness.fake.next_request_stream()

        assert await request.first_message == SubscribeNamespace(request_id=0, track_namespace_prefix=["room"])
        await request.send(encode_request_ok())
        await request.send(encode_namespace(["alice"]))
        await request.send(encode_namespace([]))
        await request.send(encode_namespace_done(["alice"]))

        await wait_for(lambda: len(events) == 4)
        assert events == ["ok", ("namespace", ["alice"]), ("namespace", []), ("done", ["alice"])]
        harness.session.destroy()

    async def test_surfaces_request_error_and_does_not_report_an_end_after_it(self) -> None:
        harness = create_session_harness()
        harness.fake.send_server_setup()
        on_error = Recorder()
        on_end = Recorder()

        harness.session.subscribe_namespace(_room(), NamespaceSubscriptionHandlers(on_error=on_error, on_end=on_end))
        request = await harness.fake.next_request_stream()

        await request.first_message
        await request.send(encode_request_error(REQUEST_ERROR_CODE.UNAUTHORIZED, "no"))
        await request.fin()

        await wait_for(lambda: on_error.called)
        assert (on_error.last.error_code, on_error.last.reason) == (REQUEST_ERROR_CODE.UNAUTHORIZED, "no")
        assert await asyncio.wait_for(request.client_end, 1) == "fin"
        await settle()
        assert not on_end.called
        harness.session.destroy()

    async def test_fails_the_request_when_the_peer_fins_before_request_ok(self) -> None:
        harness = create_session_harness()
        harness.fake.send_server_setup()
        on_error = Recorder()

        harness.session.subscribe_namespace(_room(), NamespaceSubscriptionHandlers(on_error=on_error))
        request = await harness.fake.next_request_stream()

        await request.first_message
        await request.fin()

        await wait_for(lambda: on_error.called)
        assert on_error.last == RequestError(
            error_code=REQUEST_ERROR_CODE.INTERNAL_ERROR,
            retry_interval=0,
            reason="request stream closed before a response",
        )
        harness.session.destroy()

    async def test_reports_a_fin_after_request_ok_as_a_clean_end_and_fins_back(self) -> None:
        harness = create_session_harness()
        harness.fake.send_server_setup()
        on_end = Recorder()

        harness.session.subscribe_namespace(_room(), NamespaceSubscriptionHandlers(on_end=on_end))
        request = await harness.fake.next_request_stream()

        await request.first_message
        await request.send(encode_request_ok())
        await request.fin()

        await wait_for(lambda: on_end.calls == [(None,)])
        assert await asyncio.wait_for(request.client_end, 1) == "fin"
        harness.session.destroy()

    async def test_reports_a_reset_after_request_ok_as_an_end_with_the_error(self) -> None:
        harness = create_session_harness()
        harness.fake.send_server_setup()
        on_end = Recorder()
        on_error = Recorder()

        harness.session.subscribe_namespace(_room(), NamespaceSubscriptionHandlers(on_end=on_end, on_error=on_error))
        request = await harness.fake.next_request_stream()

        await request.first_message
        await request.send(encode_request_ok())
        request.reset(RuntimeError("gone"))

        await wait_for(lambda: on_end.called)
        assert isinstance(on_end.last, BaseException)
        assert not on_error.called
        harness.session.destroy()

    async def test_closes_the_session_on_namespace_before_request_ok(self) -> None:
        harness = create_session_harness()
        harness.fake.send_server_setup()
        harness.session.subscribe_namespace(_room())
        request = await harness.fake.next_request_stream()

        await request.first_message
        await request.send(encode_namespace(["alice"]))

        await wait_for(lambda: harness.fake.close_info is not None)
        assert harness.fake.close_info is not None
        assert harness.fake.close_info.close_code == SESSION_ERROR.PROTOCOL_VIOLATION

    @pytest.mark.parametrize(
        "second", [encode_request_ok(), encode_request_error(REQUEST_ERROR_CODE.INTERNAL_ERROR, "late")]
    )
    async def test_closes_the_session_on_a_second_response(self, second: bytes) -> None:
        harness = create_session_harness()
        harness.fake.send_server_setup()
        harness.session.subscribe_namespace(_room())
        request = await harness.fake.next_request_stream()

        await request.first_message
        await request.send(encode_request_ok())
        await request.send(second)

        await wait_for(lambda: harness.fake.close_info is not None)
        assert harness.fake.close_info is not None
        assert harness.fake.close_info.close_code == SESSION_ERROR.PROTOCOL_VIOLATION

    async def test_cancels_without_reporting_anything_further(self) -> None:
        harness = create_session_harness()
        harness.fake.send_server_setup()
        on_namespace = Recorder()
        on_end = Recorder()
        handle = harness.session.subscribe_namespace(
            _room(), NamespaceSubscriptionHandlers(on_namespace=on_namespace, on_end=on_end)
        )
        request = await harness.fake.next_request_stream()

        await request.first_message
        await request.send(encode_request_ok())
        handle.cancel()
        assert await asyncio.wait_for(request.client_end, 1) == "reset"
        try:
            await request.send(encode_namespace(["alice"]))
        except Exception:
            pass

        await settle()
        assert not on_namespace.called
        assert not on_end.called
        harness.session.destroy()

    async def test_fails_a_pending_request_when_the_transport_drops(self) -> None:
        harness = create_session_harness()
        harness.fake.send_server_setup()
        on_error = Recorder()

        harness.session.subscribe_namespace(_room(), NamespaceSubscriptionHandlers(on_error=on_error))
        request = await harness.fake.next_request_stream()

        await request.first_message
        harness.fake.drop_transport()

        await wait_for(lambda: on_error.called)
