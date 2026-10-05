"""Publish-side session driver tests, ported from the Video.js publish session tests (Apache-2.0)."""

import asyncio
from typing import Any

import pytest
from core_helpers.raw_peer import RawRequest, open_raw_request, solicit_namespace
from core_helpers.raw_peer import raw_subscribe as raw_subscribe_to
from core_helpers.streams import StreamClosedError
from core_helpers.transport_pair import (
    CloseInfo,
    PairTransport,
    TransportPair,
    create_transport_pair,
    set_request_id_parity,
    with_server_request_ids,
)
from core_helpers.waiting import settle, wait_for

from moq_core import KeyValuePair, publish_session
from moq_core.control_messages import (
    PUBLISH_DONE_STATUS,
    REQUEST_ERROR_CODE,
    ControlMessage,
    ControlMessageDeframer,
    MessageParameters,
    Namespace,
    Publish,
    PublishDone,
    RelativeGroupFilter,
    RequestError,
    Subscribe,
    SubscribeNamespace,
    SubscribeOk,
    decode_control_message,
    encode_goaway,
    encode_publish,
    encode_publish_done,
    encode_request_update,
    encode_setup,
    encode_subscribe,
    encode_subscribe_namespace,
    encode_subscribe_ok,
)
from moq_core.errors import SESSION_ERROR, MoqtProtocolError
from moq_core.object_stream import MoqtObject
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
    MoqtSession,
    MoqtSessionConfig,
    MoqtSubscribeOptions,
    Subscription,
    SubscriptionHandlers,
    create_moqt_session,
)

NAMESPACE = ["live", "abc123"]


def raw_subscribe(server: PairTransport, track_name: str, request_id: int) -> asyncio.Future[RawRequest]:
    """The relay's per-track pull under the test namespace."""
    return raw_subscribe_to(server, NAMESPACE, track_name, request_id)


def register(session: MoqtPublishSession, track_name: str) -> Any:
    return session.register_track(RegisterTrackOptions(track_namespace=NAMESPACE, track_name=track_name))


def publish_done_bytes(status: int, stream_count: int, reason: str) -> bytes:
    """PUBLISH_DONE spelled out byte by byte (§10.12), for statuses and counts under 64."""
    encoded = reason.encode()
    body = bytes([status, stream_count, len(encoded)]) + encoded
    return bytes([0x0B, 0x00, len(body)]) + body


def record_data_pipes(transport: PairTransport) -> list[Any]:
    """Collect, in order, the pipes under the data streams the publisher opens on ``transport``."""
    pipes: list[Any] = []
    create = transport.create_unidirectional_stream

    async def recording() -> Any:
        pipe = await create()
        pipes.append(pipe)
        return pipe

    setattr(transport, "create_unidirectional_stream", recording)  # noqa: B010 - replaces a method in place
    return pipes


def after_subscribe_ok(request: RawRequest, track_alias: int) -> bytes:
    """What the publisher wrote on a subscription's request stream after SUBSCRIBE_OK."""
    subscribe_ok = encode_subscribe_ok(track_alias, MessageParameters(), [])
    assert bytes(request.raw[: len(subscribe_ok)]) == subscribe_ok
    return bytes(request.raw[len(subscribe_ok) :])


class SessionOver(Exception):
    """A transport's session error (TA-22): the shape alone, with no transport behind it."""

    source = "session"


class Harness:
    def __init__(self, callbacks: MoqtPublishSessionCallbacks | None = None) -> None:
        self.pair: TransportPair = create_transport_pair()
        self.subscriber: MoqtSession = create_moqt_session(
            with_server_request_ids(self.pair.server), MoqtSessionConfig(unknown_alias_timeout=0.5)
        )
        self.session: MoqtPublishSession = create_moqt_publish_session(
            self.pair.client, MoqtPublishSessionConfig(callbacks=callbacks or MoqtPublishSessionCallbacks())
        )

    def destroy(self) -> None:
        self.session.destroy()
        self.subscriber.destroy()


class CollectedTrack:
    def __init__(self, subscriber: MoqtSession, track_name: str) -> None:
        self.objects: list[MoqtObject] = []
        self.aliases: list[int] = []
        self.subscription: Subscription = subscriber.subscribe(
            MoqtSubscribeOptions(track_namespace=NAMESPACE, track_name=track_name),
            SubscriptionHandlers(on_ok=lambda ok: self.aliases.append(ok.track_alias), on_object=self.objects.append),
        )


def decode_frames(data: bytes) -> list[ControlMessage]:
    return [decode_control_message(frame) for frame in ControlMessageDeframer().push(data)]


class TestCreateMoqtPublishSession:
    async def test_completes_setup_both_ways_against_the_existing_subscribe_driver(self) -> None:
        harness = Harness()

        assert await harness.session.ready is None
        assert await harness.subscriber.ready is None
        harness.destroy()

    async def test_accepts_a_namespace_solicitation_and_announces_the_namespace_on_it(self) -> None:
        announced: list[list[str]] = []
        harness = Harness(MoqtPublishSessionCallbacks(on_announced=announced.append))

        await harness.session.ready
        harness.session.announce(NAMESPACE)
        register(harness.session, "catalog")

        solicitation = await solicit_namespace(harness.pair.server, [])

        await wait_for(lambda: solicitation.kinds() == ["request-ok", "namespace"])
        # Empty prefix: the suffix is the full namespace.
        assert solicitation.received[1] == Namespace(track_namespace_suffix=NAMESPACE)
        assert announced == [NAMESPACE]
        harness.destroy()

    async def test_defers_the_announce_until_the_first_track_is_registered(self) -> None:
        harness = Harness()

        await harness.session.ready
        harness.session.announce(NAMESPACE)

        # Solicited and announced, but nothing to serve yet: an announce now would invite SUBSCRIBEs
        # that DOES_NOT_EXIST terminally.
        solicitation = await solicit_namespace(harness.pair.server, [])

        await wait_for(lambda: solicitation.kinds() == ["request-ok"])
        await settle()
        assert len(solicitation.received) == 1

        # The first registration releases it.
        register(harness.session, "catalog")
        await wait_for(lambda: solicitation.kinds() == ["request-ok", "namespace"])
        harness.destroy()

    async def test_announces_on_a_solicitation_that_arrived_before_announce(self) -> None:
        announced: list[list[str]] = []
        harness = Harness(MoqtPublishSessionCallbacks(on_announced=announced.append))

        await harness.session.ready

        solicitation = await solicit_namespace(harness.pair.server, [])

        await wait_for(lambda: solicitation.kinds() == ["request-ok"])

        harness.session.announce(NAMESPACE)
        register(harness.session, "catalog")
        await wait_for(lambda: solicitation.kinds() == ["request-ok", "namespace"] and announced == [NAMESPACE])
        harness.destroy()

    async def test_announces_the_suffix_relative_to_a_non_empty_solicited_prefix(self) -> None:
        harness = Harness()

        await harness.session.ready
        harness.session.announce(NAMESPACE)
        register(harness.session, "catalog")

        solicitation = await solicit_namespace(harness.pair.server, ["live"])

        await wait_for(lambda: len(solicitation.received) == 2)
        assert solicitation.received[1] == Namespace(track_namespace_suffix=["abc123"])
        harness.destroy()

    async def test_accepts_but_never_announces_on_a_solicitation_whose_prefix_does_not_cover(self) -> None:
        announced: list[list[str]] = []
        harness = Harness(MoqtPublishSessionCallbacks(on_announced=announced.append))

        await harness.session.ready
        harness.session.announce(NAMESPACE)
        register(harness.session, "catalog")

        assert harness.session.namespace_solicited is None
        solicitation = await solicit_namespace(harness.pair.server, ["other"])

        await wait_for(lambda: solicitation.kinds() == ["request-ok"])
        await settle()
        assert len(solicitation.received) == 1
        assert announced == []
        assert harness.session.namespace_solicited is False
        matching = await solicit_namespace(harness.pair.server, ["live"], 3)
        await wait_for(lambda: matching.kinds() == ["request-ok", "namespace"])
        assert harness.session.namespace_solicited is True
        matching.reset()
        await wait_for(lambda: harness.session.namespace_solicited is False)
        harness.destroy()

    async def test_reports_on_announce_ended_when_the_solicitation_carrying_the_announce_ends(self) -> None:
        endings: list[BaseException | None] = []
        harness = Harness(MoqtPublishSessionCallbacks(on_announce_ended=lambda ended: endings.append(ended.error)))

        await harness.session.ready
        harness.session.announce(NAMESPACE)
        register(harness.session, "catalog")

        solicitation = await solicit_namespace(harness.pair.server, [])

        await wait_for(lambda: len(solicitation.received) == 2)

        # The peer withdrawing the namespace subscription (a reset; a clean FIN is only half-closure)
        # takes the announce with it: the ingest path is gone.
        solicitation.reset()
        await wait_for(lambda: len(endings) == 1)
        assert isinstance(endings[0], Exception)
        harness.destroy()

    async def test_does_not_report_on_announce_ended_when_the_carrier_fails_with_a_session_error(self) -> None:
        endings: list[BaseException | None] = []
        harness = Harness(MoqtPublishSessionCallbacks(on_announce_ended=lambda ended: endings.append(ended.error)))

        await harness.session.ready
        harness.session.announce(NAMESPACE)
        register(harness.session, "catalog")

        solicitation = await solicit_namespace(harness.pair.server, [])

        await wait_for(lambda: len(solicitation.received) == 2)

        # TA-23: a failure with source "session" means the session is over, which the closure reports, not a
        # withdrawal; the shape decides even before the transport says it ended.
        solicitation.reset(SessionOver())
        await settle(0.03)
        assert endings == []
        harness.destroy()

    async def test_keeps_the_carrier_alive_across_the_requester_half_closing_its_side(self) -> None:
        endings: list[BaseException | None] = []
        harness = Harness(MoqtPublishSessionCallbacks(on_announce_ended=lambda ended: endings.append(ended.error)))

        await harness.session.ready
        harness.session.announce(NAMESPACE)
        register(harness.session, "catalog")

        solicitation = await solicit_namespace(harness.pair.server, [])

        await wait_for(lambda: solicitation.kinds() == ["request-ok", "namespace"])

        # §3.3.2: a clean requester FIN only commits to sending no updates. It is not a withdrawal,
        # and the response side keeps carrying the announce.
        await solicitation.fin()
        await settle(0.03)
        assert endings == []

        # The carrier is still the retraction path at close().
        harness.session.close()
        await wait_for(lambda: solicitation.kinds() == ["request-ok", "namespace", "namespace-done"])
        harness.subscriber.destroy()

    async def test_rejects_an_overlapping_namespace_solicitation_with_prefix_overlap(self) -> None:
        endings: list[BaseException | None] = []
        harness = Harness(MoqtPublishSessionCallbacks(on_announce_ended=lambda ended: endings.append(ended.error)))

        await harness.session.ready
        harness.session.announce(NAMESPACE)
        register(harness.session, "catalog")

        broad = await solicit_namespace(harness.pair.server, [], 1)

        await wait_for(lambda: broad.kinds() == ["request-ok", "namespace"])

        # §10.18: a second solicitation whose prefix nests with an established one would produce
        # duplicate namespace state: refused, and the established carrier is untouched.
        narrow = await solicit_namespace(harness.pair.server, ["live"], 3)

        await wait_for(lambda: narrow.kinds() == ["request-error"] and narrow.ended())
        assert isinstance(narrow.received[0], RequestError)
        assert narrow.received[0].error_code == REQUEST_ERROR_CODE.PREFIX_OVERLAP
        await settle()
        assert not broad.ended()
        assert endings == []
        harness.destroy()

    async def test_holds_the_overlap_guard_across_the_acceptance_write_of_a_racing_solicitation(self) -> None:
        endings: list[BaseException | None] = []
        harness = Harness(MoqtPublishSessionCallbacks(on_announce_ended=lambda ended: endings.append(ended.error)))

        await harness.session.ready
        harness.session.announce(NAMESPACE)
        register(harness.session, "catalog")

        # Both solicitations are in flight before either REQUEST_OK write settles: request handlers
        # run concurrently, so §10.18's single-carrier rule must hold across the acceptance await,
        # not only at the synchronous check.
        broad, narrow = await asyncio.gather(
            solicit_namespace(harness.pair.server, [], 1), solicit_namespace(harness.pair.server, ["live"], 3)
        )

        await wait_for(lambda: len(broad.received) >= 1 and len(narrow.received) >= 1)
        assert sorted([broad.received[0].kind, narrow.received[0].kind]) == ["request-error", "request-ok"]
        refused = broad if broad.received[0].kind == "request-error" else narrow
        accepted = narrow if refused is broad else broad

        assert isinstance(refused.received[0], RequestError)
        assert refused.received[0].error_code == REQUEST_ERROR_CODE.PREFIX_OVERLAP
        await wait_for(lambda: accepted.kinds() == ["request-ok", "namespace"] and refused.ended())
        assert endings == []
        harness.destroy()

    async def test_answers_inbound_subscribe_with_the_request_id_as_the_alias_and_routes_request_update(
        self,
    ) -> None:
        subscribes: list[Subscribe] = []
        updates: list[tuple[int, int]] = []
        bindings: list[tuple[str, int | None]] = []
        harness = Harness(
            MoqtPublishSessionCallbacks(
                on_subscribe=subscribes.append,
                on_request_update=lambda update: updates.append((update.request_id, update.update_request_id)),
                on_track_binding=lambda binding: bindings.append((binding.track_name, binding.track_alias)),
            )
        )

        await harness.session.ready
        register(harness.session, "video")

        video = CollectedTrack(harness.subscriber, "video")
        # The driver's even id reaches the publisher with its low bit set.
        video_request_id = video.subscription.request_id + 1

        # The peer's request ids are session-unique, so they double as the alias: two live request
        # ids sharing an alias is a session-fatal Duplicate on moq-lite-rs.
        await wait_for(
            lambda: (
                video.aliases == [video_request_id]
                and [s.track_name for s in subscribes] == ["video"]
                and bindings == [("video", video_request_id)]
            )
        )

        # The ack lands as the driver's update completion (`on_update_ok` sits in the subscribe-time
        # handlers).
        update_acks: list[int] = []
        updated = harness.subscriber.subscribe(
            MoqtSubscribeOptions(track_namespace=NAMESPACE, track_name="video"),
            SubscriptionHandlers(on_update_ok=lambda: update_acks.append(1)),
        )

        await wait_for(lambda: len(subscribes) == 2)
        updated.update(MessageParameters(subscriber_priority=9))
        # The update is attributed to the subscription it rode in on, while consuming a Request ID of
        # its own (§10.1).
        await wait_for(lambda: len(updates) == 1 and len(update_acks) == 1)
        assert updates[0][0] == updated.request_id + 1
        assert updates[0][1] != updated.request_id + 1
        harness.destroy()

    async def test_gives_the_subscribe_driver_odd_request_ids_past_the_one_byte_varint_range(self) -> None:
        closes: list[Any] = []
        harness = Harness(MoqtPublishSessionCallbacks(on_closed=closes.append))

        await harness.session.ready

        # 70 subscriptions take the driver's ids 0..138, past 128, the one-byte MOQT varint limit.
        names = [f"track-{index}" for index in range(70)]

        for name in names:
            register(harness.session, name)

        tracks = [CollectedTrack(harness.subscriber, name) for name in names]

        await wait_for(
            lambda: [track.aliases for track in tracks] == [[track.subscription.request_id + 1] for track in tracks]
        )
        assert tracks[-1].subscription.request_id == 138
        assert closes == []
        harness.destroy()

    def test_sets_only_the_request_id_bit_at_every_moqt_varint_length_boundary(self) -> None:
        for request_id in (0, 62, 64, 126, 128, 16_382, 16_384, 2**21 - 2, 2**21, 2**31, 2**32 + 2**28, 2**53 - 2):
            rewritten = decode_frames(
                set_request_id_parity(
                    encode_subscribe(Subscribe(request_id=request_id, track_namespace=NAMESPACE, track_name="video"))
                )
            )

            assert len(rewritten) == 1
            assert isinstance(rewritten[0], Subscribe)
            assert rewritten[0].request_id == request_id + 1
            assert rewritten[0].track_name == "video"

        # SUBSCRIBE_NAMESPACE's type (0x50) takes two bytes in MOQT's encoding.
        assert decode_frames(
            set_request_id_parity(
                encode_subscribe_namespace(SubscribeNamespace(request_id=64, track_namespace_prefix=NAMESPACE))
            )
        ) == [SubscribeNamespace(request_id=65, track_namespace_prefix=NAMESPACE)]

    async def test_does_not_declare_a_media_timescale_for_raw_tracks(self) -> None:
        harness = Harness()

        await harness.session.ready
        register(harness.session, "video")

        subscribe = await raw_subscribe(harness.pair.server, "video", 11)

        await wait_for(lambda: len(subscribe.received) == 1)
        # Undeclared, moq-lite-rs relays parse and DISCARD the objects' TIMESTAMP extensions and
        # re-stamp frames on arrival: viewers would sync to relay arrival time instead of capture time.
        assert subscribe.received[0] == SubscribeOk(track_alias=11, track_properties=[])
        harness.destroy()

    def test_track_property_limit_fits_a_subscribe_ok_with_the_largest_alias(self) -> None:
        # One odd pair whose type and length take 4 bytes; the alias is the peer's request ID, so
        # registration has to assume the 9-byte varint.
        def block(length: int) -> list[KeyValuePair]:
            return [KeyValuePair(1, bytes(length - 4))]

        largest_alias = 2**64 - 1
        assert (
            len(encode_subscribe_ok(largest_alias, MessageParameters(), block(MAX_TRACK_PROPERTIES_LENGTH)))
            == 3 + 0xFFFF
        )
        with pytest.raises(MoqtProtocolError, match="control message exceeds"):
            encode_subscribe_ok(largest_alias, MessageParameters(), block(MAX_TRACK_PROPERTIES_LENGTH + 1))

    async def test_answers_a_subscribe_for_a_track_whose_properties_fill_the_limit(self) -> None:
        harness = Harness()

        await harness.session.ready
        properties = [KeyValuePair(1, bytes(MAX_TRACK_PROPERTIES_LENGTH - 4))]
        harness.session.register_track(
            RegisterTrackOptions(track_namespace=NAMESPACE, track_name="video", track_properties=properties)
        )

        subscribe = await raw_subscribe(harness.pair.server, "video", 11)

        await wait_for(lambda: len(subscribe.received) == 1)
        assert subscribe.received[0] == SubscribeOk(track_alias=11, track_properties=properties)
        harness.destroy()

    async def test_refuses_a_subscribe_whose_subscribe_ok_cannot_be_encoded_with_internal_error(self) -> None:
        failures: list[SubscribeFailed] = []
        harness = Harness(MoqtPublishSessionCallbacks(on_subscribe_failed=failures.append))

        await harness.session.ready
        # One byte past the limit the SDK enforces at registration, which the core does not check: with
        # the largest alias, the 9-byte varint the limit allows for, SUBSCRIBE_OK overflows its frame.
        harness.session.register_track(
            RegisterTrackOptions(
                track_namespace=NAMESPACE,
                track_name="video",
                track_properties=[KeyValuePair(1, bytes(MAX_TRACK_PROPERTIES_LENGTH - 3))],
            )
        )
        register(harness.session, "audio")

        subscribe = await raw_subscribe(harness.pair.server, "video", 2**64 - 1)

        # A local failure is not a peer cancel: the peer gets a prompt REQUEST_ERROR, then the FIN.
        await wait_for(lambda: subscribe.ended())
        assert subscribe.failure() is None
        assert subscribe.received == [
            RequestError(error_code=REQUEST_ERROR_CODE.INTERNAL_ERROR, reason="could not encode SUBSCRIBE_OK")
        ]
        assert [(failure.request_id, failure.track_name) for failure in failures] == [(2**64 - 1, "video")]
        assert isinstance(failures[0].error, MoqtProtocolError)

        # The session stays up and keeps serving: the other track still answers SUBSCRIBE_OK.
        audio = await raw_subscribe(harness.pair.server, "audio", 11)

        await wait_for(lambda: len(audio.received) == 1)
        assert audio.received[0] == SubscribeOk(track_alias=11, track_properties=[])
        harness.destroy()

    async def test_rejects_inbound_subscribe_for_unknown_tracks_with_does_not_exist(self) -> None:
        harness = Harness()

        await harness.session.ready

        errors: list[int] = []

        harness.subscriber.subscribe(
            MoqtSubscribeOptions(track_namespace=NAMESPACE, track_name="nope"),
            SubscriptionHandlers(on_error=lambda error: errors.append(error.error_code)),
        )
        await wait_for(lambda: errors == [REQUEST_ERROR_CODE.DOES_NOT_EXIST])
        harness.destroy()

    async def test_ends_a_track_with_publish_done_track_ended_then_fin(self) -> None:
        ended_requests: list[int] = []
        bindings: list[int | None] = []
        harness = Harness(
            MoqtPublishSessionCallbacks(
                on_subscribe_end=ended_requests.append,
                on_track_binding=lambda binding: bindings.append(binding.track_alias),
            )
        )

        await harness.session.ready
        track = register(harness.session, "video")

        subscribe = await raw_subscribe(harness.pair.server, "video", 21)

        await wait_for(lambda: len(subscribe.received) == 1)

        track.end()
        # The local end is authoritative: the subscription is reported ended and the binding cleared
        # immediately, not when the peer eventually closes its request direction.
        assert ended_requests == [21]
        assert bindings[-1] is None
        await wait_for(lambda: subscribe.ended())
        # PUBLISH_DONE before the FIN (§3.3.2): TRACK_ENDED, no data streams opened, then the FIN.
        # moq-relay 0.15.4 and later treat a FIN without it as a failed request.
        assert subscribe.failure() is None
        assert after_subscribe_ok(subscribe, 21) == publish_done_bytes(0x2, 0, "track ended")
        assert after_subscribe_ok(subscribe, 21) == encode_publish_done(
            PUBLISH_DONE_STATUS.TRACK_ENDED, 0, "track ended"
        )

        # The peer closing its side afterwards must not double-report.
        await subscribe.fin()
        await settle(0.03)
        assert ended_requests == [21]
        harness.destroy()

    async def test_hands_the_binding_to_the_newest_subscription_and_fins_the_replaced_stream(self) -> None:
        bindings: list[int | None] = []
        ended_requests: list[int] = []
        harness = Harness(
            MoqtPublishSessionCallbacks(
                on_track_binding=lambda binding: bindings.append(binding.track_alias),
                on_subscribe_end=ended_requests.append,
            )
        )

        await harness.session.ready
        register(harness.session, "video")

        first = await raw_subscribe(harness.pair.server, "video", 31)

        await wait_for(lambda: len(first.received) == 1)

        second = await raw_subscribe(harness.pair.server, "video", 33)

        # The replaced subscription ends cleanly: PUBLISH_DONE TRACK_ENDED, then FIN.
        await wait_for(lambda: len(second.received) == 1 and first.ended())
        assert after_subscribe_ok(first, 31) == publish_done_bytes(0x2, 0, "superseded by a newer subscription")
        # The relay closes its own side after seeing ours; only then does the replaced subscription's
        # stream fully end.
        await first.fin()
        await wait_for(lambda: ended_requests == [31])
        # Rebinding lands on the newest alias; the replaced stream's own end re-derives the same
        # binding rather than clearing it.
        assert bindings[0] == 31
        assert bindings[-1] == 33
        assert None not in bindings
        harness.destroy()

    async def test_converges_on_the_newest_subscription_regardless_of_acceptance_order(self) -> None:
        bindings: list[int | None] = []
        harness = Harness(
            MoqtPublishSessionCallbacks(on_track_binding=lambda binding: bindings.append(binding.track_alias))
        )

        await harness.session.ready
        register(harness.session, "video")

        # Both SUBSCRIBEs land with their acceptances backpressured (unread responses); the OLDER one
        # is then released first.
        class Held:
            def __init__(self) -> None:
                self.ended = False
                self.readable: Any = None

            def drain(self) -> None:
                async def pump() -> None:
                    try:
                        while await self.readable.read() is not None:
                            pass
                    finally:
                        self.ended = True

                asyncio.get_running_loop().create_task(pump())

        async def open_held(request_id: int) -> Held:
            stream = await harness.pair.server.create_bidirectional_stream()
            await stream.writable.write(
                encode_subscribe(Subscribe(request_id=request_id, track_namespace=NAMESPACE, track_name="video"))
            )
            held = Held()
            held.readable = stream.readable
            return held

        older = await open_held(91)
        await settle()
        newer = await open_held(93)
        await settle()

        # Older accepts first: it may bind transiently, but must not sweep the newer request that
        # arrived after it.
        older.drain()
        await wait_for(lambda: bindings and bindings[-1] == 91)

        # The newer acceptance lands and takes the binding; the older is FINed as its predecessor.
        newer.drain()
        await wait_for(lambda: bindings[-1] == 93 and older.ended)
        harness.destroy()

    async def test_a_stale_acceptance_does_not_steal_the_binding_from_a_newer_subscription(self) -> None:
        bindings: list[int | None] = []
        subscribed: list[int] = []
        harness = Harness(
            MoqtPublishSessionCallbacks(
                on_track_binding=lambda binding: bindings.append(binding.track_alias),
                on_subscribe=lambda subscribe: subscribed.append(subscribe.request_id),
            )
        )

        await harness.session.ready
        register(harness.session, "video")

        # Subscription A: write the SUBSCRIBE but do NOT read the response. Over the in-memory pair
        # a write only settles once the far side reads, so A's SUBSCRIBE_OK hangs in flight.
        stream_a = await harness.pair.server.create_bidirectional_stream()
        await stream_a.writable.write(
            encode_subscribe(Subscribe(request_id=61, track_namespace=NAMESPACE, track_name="video"))
        )
        await settle()

        # Subscription B reads immediately: its acceptance settles first, replaces A, and takes the
        # binding.
        b = await raw_subscribe(harness.pair.server, "video", 63)

        await wait_for(lambda: len(b.received) == 1 and bindings[-1] == 63)

        # Now drain A: its stale acceptance completes against a subscription that was already
        # replaced. It must not rebind the track.
        async def drain_a() -> None:
            try:
                while await stream_a.readable.read() is not None:
                    pass
            except Exception:
                pass

        asyncio.get_running_loop().create_task(drain_a())
        await settle(0.05)
        assert 61 not in bindings
        assert bindings[-1] == 63
        # The swept-while-pending subscription was never live, so it is never reported: subscriber
        # counts stay balanced.
        assert subscribed == [63]
        harness.destroy()

    async def test_withholds_the_binding_while_forward_state_is_0_and_follows_request_update_toggles(self) -> None:
        bindings: list[int | None] = []
        harness = Harness(
            MoqtPublishSessionCallbacks(on_track_binding=lambda binding: bindings.append(binding.track_alias))
        )

        await harness.session.ready
        register(harness.session, "video")

        # `forward=0` subscribes without authorizing data.
        sub = await open_raw_request(
            harness.pair.server,
            encode_subscribe(
                Subscribe(
                    request_id=81,
                    track_namespace=NAMESPACE,
                    track_name="video",
                    parameters=MessageParameters(forward=0),
                )
            ),
        )

        await wait_for(lambda: sub.kinds() == ["subscribe-ok"] and bindings == [None])

        # Each REQUEST_UPDATE consumes a Request ID of its own (§10.1).
        await sub.send(encode_request_update(83, MessageParameters(forward=1)))
        await wait_for(lambda: bindings[-1] == 81)

        await sub.send(encode_request_update(85, MessageParameters(forward=0)))
        await wait_for(lambda: bindings[-1] is None)
        # Each accepted update is acknowledged: the subscribe driver treats REQUEST_OK as the update's
        # completion. (The binding update above is synchronous server-side while the ack rides the
        # transport, so this must wait too.)
        await wait_for(lambda: sub.kinds() == ["subscribe-ok", "request-ok", "request-ok"])
        harness.destroy()

    async def test_sweeps_every_live_predecessor_when_a_new_subscription_takes_the_binding(self) -> None:
        bindings: list[int | None] = []
        harness = Harness(
            MoqtPublishSessionCallbacks(on_track_binding=lambda binding: bindings.append(binding.track_alias))
        )

        await harness.session.ready
        register(harness.session, "video")

        # A: fully accepted and bound.
        a = await raw_subscribe(harness.pair.server, "video", 71)

        await wait_for(lambda: bindings and bindings[-1] == 71)

        # B: acceptance held in flight (unread response).
        stream_b = await harness.pair.server.create_bidirectional_stream()
        await stream_b.writable.write(
            encode_subscribe(Subscribe(request_id=73, track_namespace=NAMESPACE, track_name="video"))
        )
        await settle()

        # C settles first and must sweep BOTH A and B: leaving A live would let it reclaim the
        # binding when C ends.
        c = await raw_subscribe(harness.pair.server, "video", 75)

        await wait_for(lambda: len(c.received) == 1 and bindings[-1] == 75 and a.ended())

        # C ends (the relay closes both directions): the binding clears instead of falling back to
        # the superseded A.
        await c.fin()
        await wait_for(lambda: bindings[-1] is None)
        assert [alias for alias in bindings if alias == 71] == [71]
        harness.destroy()

    async def test_surfaces_the_announce_loss_when_only_the_response_half_of_the_solicitation_dies(self) -> None:
        endings: list[BaseException | None] = []
        harness = Harness(MoqtPublishSessionCallbacks(on_announce_ended=lambda ended: endings.append(ended.error)))

        await harness.session.ready

        solicitation = await solicit_namespace(harness.pair.server, [])

        await wait_for(lambda: solicitation.kinds() == ["request-ok"])

        # The peer resets only the response direction; its request half (the read loop's end signal)
        # stays open, so the entry write's failure is the only evidence the carrier is unusable.
        solicitation.abandon_reads()
        harness.session.announce(NAMESPACE)
        register(harness.session, "catalog")
        await wait_for(lambda: len(endings) == 1)
        harness.destroy()

    async def test_accepts_a_replacement_solicitation_after_the_response_half_of_the_carrier_dies(self) -> None:
        endings: list[BaseException | None] = []
        harness = Harness(MoqtPublishSessionCallbacks(on_announce_ended=lambda ended: endings.append(ended.error)))

        await harness.session.ready

        dead = await solicit_namespace(harness.pair.server, [], 1)

        await wait_for(lambda: dead.kinds() == ["request-ok"])
        dead.abandon_reads()
        harness.session.announce(NAMESPACE)
        register(harness.session, "catalog")
        await wait_for(lambda: len(endings) == 1)

        # The write failure deregistered the dead carrier along with the loss report, so the relay's
        # retry is a fresh start, not a PREFIX_OVERLAP refusal against a corpse.
        retry = await solicit_namespace(harness.pair.server, [], 3)

        await wait_for(lambda: retry.kinds() == ["request-ok", "namespace"])
        harness.destroy()

    async def test_rejects_a_namespace_request_update_and_closes_the_carrier_never_the_session(self) -> None:
        updates: list[int] = []
        closes: list[Any] = []
        endings: list[BaseException | None] = []
        harness = Harness(
            MoqtPublishSessionCallbacks(
                on_request_update=lambda update: updates.append(update.request_id),
                on_closed=closes.append,
                on_announce_ended=lambda ended: endings.append(ended.error),
            )
        )

        await harness.session.ready
        harness.session.announce(NAMESPACE)
        register(harness.session, "catalog")

        solicitation = await solicit_namespace(harness.pair.server, [])

        await wait_for(lambda: len(solicitation.received) == 2)

        # A legal §10.9 update (on a fresh Request ID) must never be session-fatal; v1 applies no
        # prefix changes, so per §10.9.1 the update is answered with an error and the request stream
        # closes, which costs the announce carrier, surfaced as announce loss.
        await solicitation.send(encode_request_update(3, MessageParameters(subscriber_priority=5)))
        await wait_for(
            lambda: (
                updates == [1]
                and solicitation.kinds() == ["request-ok", "namespace", "request-error"]
                and solicitation.ended()
                and len(endings) == 1
            )
        )
        assert closes == []
        harness.destroy()

    async def test_closes_the_session_when_the_peer_reuses_a_request_id(self) -> None:
        closes: list[Any] = []
        harness = Harness(MoqtPublishSessionCallbacks(on_closed=closes.append))

        await harness.session.ready
        register(harness.session, "video")
        register(harness.session, "audio")

        first = await raw_subscribe(harness.pair.server, "video", 101)

        await wait_for(lambda: len(first.received) == 1)

        # Aliases ARE the peer's request ids: a reused id would hand two tracks the same alias and
        # make their subgroup streams indistinguishable. Session-fatal with INVALID_REQUEST_ID (§10.1).
        await raw_subscribe(harness.pair.server, "audio", 101)
        await wait_for(lambda: len(closes) == 1)
        assert isinstance(closes[0].error, Exception)
        assert harness.pair.client._shared.close_info == CloseInfo(
            SESSION_ERROR.INVALID_REQUEST_ID, "peer reused request id 101"
        )
        harness.subscriber.destroy()

    async def test_closes_the_session_when_the_peer_sends_a_request_id_with_the_client_parity(self) -> None:
        closes: list[Any] = []
        harness = Harness(MoqtPublishSessionCallbacks(on_closed=closes.append))

        await harness.session.ready
        register(harness.session, "video")

        # The peer is the server, so its request ids are odd (§10.1).
        await raw_subscribe(harness.pair.server, "video", 100)
        await wait_for(lambda: len(closes) == 1)
        assert harness.pair.client._shared.close_info == CloseInfo(
            SESSION_ERROR.INVALID_REQUEST_ID, "peer sent even request id 100"
        )
        harness.subscriber.destroy()

    async def test_closes_the_session_when_a_request_update_reuses_a_request_id(self) -> None:
        closes: list[Any] = []
        harness = Harness(MoqtPublishSessionCallbacks(on_closed=closes.append))

        await harness.session.ready
        register(harness.session, "video")

        sub = await raw_subscribe(harness.pair.server, "video", 111)

        await wait_for(lambda: sub.kinds() == ["subscribe-ok"])

        # REQUEST_UPDATE consumes a Request ID of its own (§10.1); riding on the subscription's id is
        # the same duplicate as a repeated SUBSCRIBE.
        await sub.send(encode_request_update(111, MessageParameters(subscriber_priority=9)))
        await wait_for(lambda: len(closes) == 1)
        assert isinstance(closes[0].error, Exception)
        assert harness.pair.client._shared.close_info == CloseInfo(
            SESSION_ERROR.INVALID_REQUEST_ID, "peer reused request id 111"
        )
        harness.subscriber.destroy()

    async def test_closes_the_session_when_a_request_update_carries_a_request_id_with_the_client_parity(
        self,
    ) -> None:
        closes: list[Any] = []
        harness = Harness(MoqtPublishSessionCallbacks(on_closed=closes.append))

        await harness.session.ready
        register(harness.session, "video")

        sub = await raw_subscribe(harness.pair.server, "video", 111)

        await wait_for(lambda: sub.kinds() == ["subscribe-ok"])

        await sub.send(encode_request_update(112, MessageParameters(subscriber_priority=9)))
        await wait_for(lambda: len(closes) == 1)
        assert harness.pair.client._shared.close_info == CloseInfo(
            SESSION_ERROR.INVALID_REQUEST_ID, "peer sent even request id 112"
        )
        harness.subscriber.destroy()

    async def test_reserves_request_ids_across_every_inbound_request_kind(self) -> None:
        closes: list[Any] = []
        harness = Harness(MoqtPublishSessionCallbacks(on_closed=closes.append))

        await harness.session.ready
        register(harness.session, "video")

        # A rejected PUBLISH burns its id session-wide...
        publish = await open_raw_request(
            harness.pair.server,
            encode_publish(Publish(request_id=121, track_namespace=NAMESPACE, track_name="video", track_alias=9)),
        )

        await wait_for(lambda: publish.kinds() == ["request-error"])
        assert closes == []

        # ...so a later SUBSCRIBE cannot reuse it as an alias.
        await raw_subscribe(harness.pair.server, "video", 121)
        await wait_for(lambda: len(closes) == 1)
        assert harness.pair.client._shared.close_info is not None
        assert harness.pair.client._shared.close_info.close_code == SESSION_ERROR.INVALID_REQUEST_ID
        harness.subscriber.destroy()

    async def test_rejects_a_subscription_update_it_cannot_apply_instead_of_acknowledging_it(self) -> None:
        ended_requests: list[int] = []
        bindings: list[int | None] = []
        harness = Harness(
            MoqtPublishSessionCallbacks(
                on_subscribe_end=ended_requests.append,
                on_track_binding=lambda binding: bindings.append(binding.track_alias),
            )
        )

        await harness.session.ready
        register(harness.session, "video")

        sub = await raw_subscribe(harness.pair.server, "video", 111)

        await wait_for(lambda: bindings and bindings[-1] == 111)

        # A filter change is not applied: acknowledging it would leave the peer believing its new
        # range is in effect.
        await sub.send(
            encode_request_update(113, MessageParameters(location_filter=RelativeGroupFilter(groups_before_next=0)))
        )
        await wait_for(
            lambda: (
                sub.kinds() == ["subscribe-ok", "request-error", "publish-done"]
                and sub.ended()
                and ended_requests == [111]
                and bindings[-1] is None
            )
        )
        # A failed update terminates the subscription with PUBLISH_DONE UPDATE_FAILED (§10.9.1).
        assert sub.received[-1] == PublishDone(
            status_code=PUBLISH_DONE_STATUS.UPDATE_FAILED, stream_count=0, reason="unsupported subscription update"
        )
        harness.destroy()

    async def test_reports_the_subscriptions_delivery_timeouts_and_accepts_an_update_that_changes_them(self) -> None:
        # §8, §10.2.3, §10.2.4 (#167): the values reach the publisher on the binding, and REQUEST_UPDATE
        # carrying either is applied and acknowledged rather than refused as unsupported.
        ended_requests: list[int] = []
        bindings: list[TrackBinding] = []
        harness = Harness(
            MoqtPublishSessionCallbacks(on_subscribe_end=ended_requests.append, on_track_binding=bindings.append)
        )

        await harness.session.ready
        register(harness.session, "video")

        sub = await open_raw_request(
            harness.pair.server,
            encode_subscribe(
                Subscribe(
                    request_id=121,
                    track_namespace=NAMESPACE,
                    track_name="video",
                    parameters=MessageParameters(object_delivery_timeout=40, subgroup_delivery_timeout=300),
                )
            ),
        )

        await wait_for(lambda: sub.kinds() == ["subscribe-ok"] and len(bindings) == 1)
        assert bindings[0] == TrackBinding(
            track_name="video", track_alias=121, object_delivery_timeout=40, subgroup_delivery_timeout=300
        )

        await sub.send(encode_request_update(123, MessageParameters(subgroup_delivery_timeout=50)))
        await wait_for(lambda: sub.kinds() == ["subscribe-ok", "request-ok"])
        assert bindings[-1] == TrackBinding(
            track_name="video", track_alias=121, object_delivery_timeout=40, subgroup_delivery_timeout=50
        )

        await sub.send(encode_request_update(125, MessageParameters(object_delivery_timeout=0)))
        await wait_for(lambda: sub.kinds() == ["subscribe-ok", "request-ok", "request-ok"])
        assert bindings[-1] == TrackBinding(
            track_name="video", track_alias=121, object_delivery_timeout=0, subgroup_delivery_timeout=50
        )
        assert ended_requests == []
        assert not sub.ended()
        harness.destroy()

    async def test_a_subscription_without_delivery_timeouts_binds_with_none(self) -> None:
        bindings: list[TrackBinding] = []
        harness = Harness(MoqtPublishSessionCallbacks(on_track_binding=bindings.append))

        await harness.session.ready
        register(harness.session, "video")
        await raw_subscribe(harness.pair.server, "video", 131)

        await wait_for(lambda: len(bindings) == 1)
        assert bindings[0] == TrackBinding(track_name="video", track_alias=131)
        harness.destroy()

    async def test_teardown_does_not_reset_a_request_stream_that_already_carried_its_publish_done_and_fin(
        self,
    ) -> None:
        # A send stream's abort resets until its FIN is acknowledged (TA-16), so the session must not
        # abort a request stream it already ended: that would destroy the PUBLISH_DONE in flight.
        harness = Harness()
        aborts: list[object] = []
        incoming = harness.pair.client.incoming_bidirectional_streams

        async def spying() -> Any:
            async for stream in incoming():
                writable = stream.writable
                inner_abort = writable.abort

                def abort(reason: object = None, inner_abort: Any = inner_abort) -> None:
                    aborts.append(reason)
                    inner_abort(reason)

                setattr(writable, "abort", abort)  # noqa: B010 - replaces a method in place
                yield stream

        setattr(harness.pair.client, "incoming_bidirectional_streams", spying)  # noqa: B010

        await harness.session.ready
        track = register(harness.session, "video")
        sub = await raw_subscribe(harness.pair.server, "video", 141)
        await wait_for(lambda: sub.kinds() == ["subscribe-ok"])

        track.end()
        await wait_for(lambda: sub.kinds() == ["subscribe-ok", "publish-done"] and sub.ended())
        harness.destroy()
        await settle()

        assert aborts == []

    async def test_treats_announce_carrier_loss_after_a_goaway_as_migration_not_failure(self) -> None:
        goaways: list[int] = []
        endings: list[BaseException | None] = []
        harness = Harness(
            MoqtPublishSessionCallbacks(
                on_goaway=lambda goaway: goaways.append(goaway.timeout),
                on_announce_ended=lambda ended: endings.append(ended.error),
            )
        )

        await harness.session.ready
        harness.session.announce(NAMESPACE)
        register(harness.session, "catalog")

        solicitation = await solicit_namespace(harness.pair.server, [])

        await wait_for(lambda: len(solicitation.received) == 2)

        # The peer migrates: GOAWAY, then its streams drain. The announce ending here is the orderly
        # drain; surfacing it as a failure would override "draining" with "failed".
        await solicitation.send(encode_goaway(1500))
        await wait_for(lambda: goaways == [1500])
        solicitation.reset()
        await settle(0.03)
        assert endings == []
        harness.destroy()

    async def test_clears_the_binding_when_the_only_subscription_ends(self) -> None:
        bindings: list[int | None] = []
        harness = Harness(
            MoqtPublishSessionCallbacks(on_track_binding=lambda binding: bindings.append(binding.track_alias))
        )

        await harness.session.ready
        register(harness.session, "video")

        subscribe = await raw_subscribe(harness.pair.server, "video", 41)

        await wait_for(lambda: bindings == [41])

        await subscribe.fin()
        # The response direction closes too: a peer-initiated unsubscribe must not leak a half-open
        # stream. The subscription was Established, so PUBLISH_DONE precedes that FIN (§3.3.2).
        await wait_for(lambda: bindings == [41, None] and subscribe.ended())
        assert after_subscribe_ok(subscribe, 41) == publish_done_bytes(0x0, 0, "subscriber closed its request stream")
        harness.destroy()

    async def test_sends_no_publish_done_for_a_cancelled_subscription_and_resets_its_data_streams(self) -> None:
        bindings: list[int | None] = []
        harness = Harness(
            MoqtPublishSessionCallbacks(on_track_binding=lambda binding: bindings.append(binding.track_alias))
        )

        await harness.session.ready
        register(harness.session, "video")

        subscribe = await raw_subscribe(harness.pair.server, "video", 43)

        await wait_for(lambda: bindings == [43])
        pipes = record_data_pipes(harness.pair.client)
        await harness.session.open_uni_stream(43)

        # A reset is a cancellation (§3.3.3): no PUBLISH_DONE, and open data streams are reset (§5.1.1).
        subscribe.reset()
        await wait_for(lambda: bindings == [43, None] and subscribe.ended())
        assert after_subscribe_ok(subscribe, 43) == b""
        assert (pipes[0], "abort", b"") in harness.pair.events
        harness.destroy()

    async def test_counts_the_data_streams_and_closes_every_one_before_publish_done(self) -> None:
        harness = Harness()

        await harness.session.ready
        track = register(harness.session, "video")

        subscribe = await raw_subscribe(harness.pair.server, "video", 61)

        await wait_for(lambda: len(subscribe.received) == 1)

        pipes = record_data_pipes(harness.pair.client)
        finished = await harness.session.open_uni_stream(61)
        still_open = await harness.session.open_uni_stream(61)
        await finished.close()

        track.end()
        await wait_for(lambda: subscribe.ended())
        assert after_subscribe_ok(subscribe, 61) == publish_done_bytes(0x2, 2, "track ended")

        # In issue order: the caller's FIN, the session's reset of the stream still open, then
        # PUBLISH_DONE and the request stream's FIN (§10.12: "MUST NOT send PUBLISH_DONE until it
        # has closed all streams").
        underlying = {pipes[0]: "finished", pipes[1]: "still open"}
        request_pipe = next(pipe for pipe, kind, data in harness.pair.events if data[:1] == b"\x0b")
        steps = [
            (underlying.get(pipe, "request" if pipe is request_pipe else None), kind)
            for pipe, kind, data in harness.pair.events
            if pipe in underlying or (pipe is request_pipe and data[:1] != b"\x04")
        ]
        assert steps == [
            ("finished", "close"),
            ("still open", "abort"),
            ("request", "write"),
            ("request", "close"),
        ]

        # The stream the session reset is gone for the caller too.
        with pytest.raises(StreamClosedError):
            await still_open.write(b"late")
        harness.destroy()

    async def test_opens_data_streams_only_for_a_live_subscription(self) -> None:
        harness = Harness()

        await harness.session.ready
        track = register(harness.session, "video")

        with pytest.raises(RuntimeError, match="no live subscription"):
            await harness.session.open_uni_stream(81)

        subscribe = await raw_subscribe(harness.pair.server, "video", 81)

        await wait_for(lambda: len(subscribe.received) == 1)
        await harness.session.open_uni_stream(81)

        track.end()
        # Ended: its PUBLISH_DONE already carries the count, so no further stream may join it.
        with pytest.raises(RuntimeError, match="no live subscription"):
            await harness.session.open_uni_stream(81)

        await wait_for(lambda: subscribe.ended())
        assert after_subscribe_ok(subscribe, 81) == publish_done_bytes(0x2, 1, "track ended")
        harness.destroy()

    async def test_resets_a_data_stream_that_finishes_opening_after_its_subscription_ended(self) -> None:
        harness = Harness()

        await harness.session.ready
        track = register(harness.session, "video")

        subscribe = await raw_subscribe(harness.pair.server, "video", 91)

        await wait_for(lambda: len(subscribe.received) == 1)

        # Hold the open in flight across the end.
        release: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        create = harness.pair.client.create_unidirectional_stream

        async def held_open() -> Any:
            await release
            return await create()

        setattr(harness.pair.client, "create_unidirectional_stream", held_open)  # noqa: B010 - holds the open
        opening = asyncio.ensure_future(harness.session.open_uni_stream(91))
        await settle()

        track.end()
        await settle()
        # PUBLISH_DONE waits for the open to settle.
        assert not subscribe.ended()

        release.set_result(None)
        with pytest.raises(RuntimeError, match="ended"):
            await opening

        await wait_for(lambda: subscribe.ended())
        # The late stream was reset before PUBLISH_DONE and is not counted.
        kinds = [kind for _, kind, data in harness.pair.events if kind == "abort" or data[:1] == b"\x0b"]
        assert kinds == ["abort", "write"]
        assert after_subscribe_ok(subscribe, 91) == publish_done_bytes(0x2, 0, "track ended")
        harness.destroy()

    async def test_drains_a_peer_initiated_end_still_waiting_on_a_data_stream_open_before_closing(self) -> None:
        bindings: list[int | None] = []
        harness = Harness(
            MoqtPublishSessionCallbacks(on_track_binding=lambda binding: bindings.append(binding.track_alias))
        )

        await harness.session.ready
        register(harness.session, "video")

        subscribe = await raw_subscribe(harness.pair.server, "video", 95)

        await wait_for(lambda: bindings == [95])

        # Hold a data-stream open in flight, and mark where the transport closes in the event log.
        release: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        create = harness.pair.client.create_unidirectional_stream
        close_transport = harness.pair.client.close
        closed_at: list[int] = []

        async def held_open() -> Any:
            await release
            return await create()

        def marked_close(close_code: int = 0, reason: str = "") -> None:
            closed_at.append(len(harness.pair.events))
            close_transport(close_code, reason)

        setattr(harness.pair.client, "create_unidirectional_stream", held_open)  # noqa: B010 - holds the open
        setattr(harness.pair.client, "close", marked_close)  # noqa: B010 - marks the transport close
        opening = asyncio.ensure_future(harness.session.open_uni_stream(95))
        await settle()

        # The subscriber FINs while the open is pending: its end waits on the open, and the
        # subscription leaves the track.
        await subscribe.fin()
        await wait_for(lambda: bindings == [95, None])

        harness.session.close()
        await settle(0.01)
        # The close drain waits for that end instead of cutting it off.
        assert closed_at == []

        release.set_result(None)
        with pytest.raises(RuntimeError):
            await opening

        await wait_for(lambda: bool(closed_at))
        assert subscribe.ended()
        assert after_subscribe_ok(subscribe, 95) == publish_done_bytes(0x0, 0, "subscriber closed its request stream")

        # PUBLISH_DONE and the request stream's FIN both precede the transport close.
        request_pipe = next(pipe for pipe, kind, data in harness.pair.events if data[:1] == b"\x0b")
        steps = [
            (index, kind)
            for index, (pipe, kind, data) in enumerate(harness.pair.events)
            if pipe is request_pipe and (kind == "close" or data[:1] == b"\x0b")
        ]
        assert [kind for _, kind in steps] == ["write", "close"]
        assert all(index < closed_at[0] for index, _ in steps)
        harness.subscriber.destroy()

    async def test_the_subscriber_driver_reports_the_publish_done_of_an_ended_track(self) -> None:
        subscribes: list[Subscribe] = []
        harness = Harness(MoqtPublishSessionCallbacks(on_subscribe=subscribes.append))

        await harness.session.ready
        track = register(harness.session, "video")

        done: list[PublishDone] = []
        harness.subscriber.subscribe(
            MoqtSubscribeOptions(track_namespace=NAMESPACE, track_name="video"),
            SubscriptionHandlers(on_done=done.append),
        )
        await wait_for(lambda: len(subscribes) == 1)
        # The alias is the request ID the relay-side driver put on the wire.
        data = await harness.session.open_uni_stream(subscribes[0].request_id)
        await data.close()

        track.end()
        await wait_for(lambda: len(done) == 1)
        assert done[0] == PublishDone(status_code=PUBLISH_DONE_STATUS.TRACK_ENDED, stream_count=1, reason="track ended")
        harness.destroy()

    @pytest.mark.parametrize("with_error", [False, True])
    async def test_close_drains_the_fins_and_namespace_done_to_the_wire_without_a_goaway(
        self, with_error: bool
    ) -> None:
        pair = create_transport_pair()
        # A fully raw peer: complete SETUP and record every frame on the publisher's control stream.
        # It must carry SETUP and nothing else: the publisher sends no GOAWAY (moq-relay 0.14.17
        # closes the session on a client GOAWAY, code 17; 0.15.1 and later keep it).
        control_frames: list[ControlMessage] = []

        async def raw_server() -> None:
            control = await pair.server.create_unidirectional_stream()
            await control.write(encode_setup([]))
            publisher_control = await pair.server.incoming_unidirectional_streams().__anext__()
            deframer = ControlMessageDeframer()

            try:
                while True:
                    chunk = await publisher_control.read()
                    if chunk is None:
                        break

                    for frame in deframer.push(chunk):
                        control_frames.append(decode_control_message(frame))
            except Exception:
                pass

        asyncio.get_running_loop().create_task(raw_server())
        closed: list[Any] = []
        session = create_moqt_publish_session(
            pair.client, MoqtPublishSessionConfig(callbacks=MoqtPublishSessionCallbacks(on_closed=closed.append))
        )

        await session.ready
        session.announce(NAMESPACE)
        register(session, "catalog")
        register(session, "video")

        solicitation = await solicit_namespace(pair.server, [])
        subscribe = await raw_subscribe(pair.server, "video", 51)

        await wait_for(lambda: len(solicitation.received) == 2 and len(subscribe.received) == 1)

        error = RuntimeError("migration requested")
        if with_error:
            session.destroy(error)
        else:
            session.close()
        # The announce is retracted and the subscription ended cleanly (PUBLISH_DONE, then FIN) before
        # the transport goes away.
        await wait_for(
            lambda: solicitation.kinds() == ["request-ok", "namespace", "namespace-done"] and subscribe.ended()
        )
        assert after_subscribe_ok(subscribe, 51) == publish_done_bytes(0x2, 0, "track ended")
        assert [frame.kind for frame in control_frames] == ["setup"]
        # Like moq-relay once it has consumed the PUBLISH_DONE: FIN back, which releases the close.
        await subscribe.fin()

        await pair.client.wait_closed()
        assert pair.client._shared.close_info is not None
        assert pair.client._shared.close_info.close_code == 0
        assert pair.client._shared.close_info.reason == ""
        assert len(closed) == 1
        assert closed[0].error is (error if with_error else None)

    @pytest.mark.parametrize("with_error", [False, True])
    async def test_finishes_publisher_drain_after_goaway_and_control_eof(self, with_error: bool) -> None:
        pair = create_transport_pair()
        control = await pair.server.create_unidirectional_stream()
        error = RuntimeError("migration requested")
        closed: list[Any] = []
        session = create_moqt_publish_session(
            pair.client,
            MoqtPublishSessionConfig(
                callbacks=MoqtPublishSessionCallbacks(
                    on_closed=closed.append,
                    on_goaway=lambda _: session.destroy(error) if with_error else session.close(),
                )
            ),
        )
        await control.write(encode_setup([]))
        await session.ready
        session.announce(NAMESPACE)
        register(session, "video")
        solicitation = await solicit_namespace(pair.server, [])
        subscribe = await raw_subscribe(pair.server, "video", 51)
        await wait_for(lambda: len(solicitation.received) == 2 and len(subscribe.received) == 1)

        await control.write(encode_goaway(1500))
        await control.close()

        await wait_for(
            lambda: solicitation.kinds() == ["request-ok", "namespace", "namespace-done"] and subscribe.ended()
        )
        assert subscribe.failure() is None
        await subscribe.fin()

        await pair.client.wait_closed()
        assert pair.client._shared.close_info is not None
        assert pair.client._shared.close_info.close_code == 0
        assert pair.client._shared.close_info.reason == ""
        assert len(closed) == 1
        assert closed[0].error is (error if with_error else None)

    @pytest.mark.parametrize("peer_end", ["fin", "reset"])
    async def test_close_holds_the_transport_until_the_peer_ends_its_side_of_each_ended_request(
        self, peer_end: str
    ) -> None:
        # moq-relay reads PUBLISH_DONE, finishes the track and only then FINs its side; a transport
        # closed before that aborts the track (viewers see status 0). The PUBLISH_DONE and FIN reaching
        # the wire is not enough, so the close waits past CLOSE_FLUSH_TIMEOUT for the peer's own end.
        harness = Harness()
        await harness.session.ready
        register(harness.session, "video")
        subscribe = await raw_subscribe(harness.pair.server, "video", 21)
        await wait_for(lambda: harness.session._live_subscription(21) is not None)

        harness.session.close()
        await wait_for(lambda: subscribe.ended())
        assert after_subscribe_ok(subscribe, 21) == publish_done_bytes(0x2, 0, "track ended")

        # A peer slow to read the PUBLISH_DONE: well past the write window, the transport stays up.
        await asyncio.sleep(publish_session.CLOSE_FLUSH_TIMEOUT + 0.25)
        assert not harness.pair.client._shared.closed

        started = asyncio.get_running_loop().time()
        if peer_end == "fin":
            await subscribe.fin()
        else:
            subscribe.reset()

        await asyncio.wait_for(harness.pair.client.wait_closed(), 1)
        assert asyncio.get_running_loop().time() - started < 0.5
        assert harness.pair.client._shared.close_info == CloseInfo(0, "")
        harness.subscriber.destroy()

    async def test_close_waits_on_the_peer_only_up_to_the_end_ack_bound(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # A peer that never ends its side of the request stream must not hold the close.
        monkeypatch.setattr(publish_session, "CLOSE_END_ACK_TIMEOUT", 0.3)
        closed: list[Any] = []
        harness = Harness(MoqtPublishSessionCallbacks(on_closed=closed.append))
        await harness.session.ready
        register(harness.session, "video")
        subscribe = await raw_subscribe(harness.pair.server, "video", 21)
        await wait_for(lambda: harness.session._live_subscription(21) is not None)

        started = asyncio.get_running_loop().time()
        harness.session.close()
        await asyncio.wait_for(harness.pair.client.wait_closed(), 2)
        elapsed = asyncio.get_running_loop().time() - started

        assert 0.3 <= elapsed < 0.3 + publish_session.CLOSE_FLUSH_TIMEOUT + 0.25
        assert subscribe.ended()
        assert after_subscribe_ok(subscribe, 21) == publish_done_bytes(0x2, 0, "track ended")
        assert harness.pair.client._shared.close_info == CloseInfo(0, "")
        assert len(closed) == 1 and closed[0].error is None
        harness.subscriber.destroy()

    async def test_close_does_not_wait_on_a_request_the_peer_already_ended(self) -> None:
        # A peer that FINs as soon as it reads the PUBLISH_DONE (or ended the request earlier) owes
        # nothing more: close goes as soon as the writes land.
        harness = Harness()
        await harness.session.ready
        register(harness.session, "video")
        subscribe = await raw_subscribe(harness.pair.server, "video", 21)
        await wait_for(lambda: harness.session._live_subscription(21) is not None)
        subscribe.reset()
        await wait_for(lambda: harness.session._live_subscription(21) is None)

        started = asyncio.get_running_loop().time()
        harness.session.close()
        await asyncio.wait_for(harness.pair.client.wait_closed(), 1)
        assert asyncio.get_running_loop().time() - started < publish_session.CLOSE_FLUSH_TIMEOUT
        harness.subscriber.destroy()

    async def test_control_eof_is_still_fatal_before_local_close(self) -> None:
        pair = create_transport_pair()
        control = await pair.server.create_unidirectional_stream()
        closed: list[Any] = []
        session = create_moqt_publish_session(
            pair.client, MoqtPublishSessionConfig(callbacks=MoqtPublishSessionCallbacks(on_closed=closed.append))
        )
        await control.write(encode_setup([]))
        await session.ready
        await control.close()
        await pair.client.wait_closed()
        assert pair.client._shared.close_info is not None
        assert pair.client._shared.close_info.close_code == SESSION_ERROR.PROTOCOL_VIOLATION
        assert len(closed) == 1
        assert str(closed[0].error) == "peer closed its control stream"

    async def test_preserves_local_error_when_peer_closes_during_drain(self) -> None:
        pair = create_transport_pair()

        async def setup() -> None:
            control = await pair.server.create_unidirectional_stream()
            await control.write(encode_setup([]))

        setup_task = asyncio.create_task(setup())
        closed: list[Any] = []
        session = create_moqt_publish_session(
            pair.client, MoqtPublishSessionConfig(callbacks=MoqtPublishSessionCallbacks(on_closed=closed.append))
        )
        await session.ready
        await setup_task
        error = RuntimeError("migration requested")
        session.destroy(error)
        pair.server.close()
        await wait_for(lambda: len(closed) == 1)
        assert closed[0].error is error

    async def test_close_leaves_no_background_work_running(self) -> None:
        pair = create_transport_pair()

        async def raw_server() -> None:
            control = await pair.server.create_unidirectional_stream()
            await control.write(encode_setup([]))

        asyncio.get_running_loop().create_task(raw_server())
        session = create_moqt_publish_session(pair.client)

        await session.ready
        session.close()
        # Room for the close drain's own (self-clearing) wait.
        await asyncio.sleep(0.4)
        current = asyncio.current_task()
        leftovers = [task for task in asyncio.all_tasks() if task is not current and not task.done()]
        assert leftovers == []

    async def test_register_track_after_destroy_is_inert(self) -> None:
        harness = Harness()

        await harness.session.ready
        harness.session.destroy()

        handle = register(harness.session, "late")

        assert handle.track_name == "late"
        handle.end()  # must not raise
        harness.subscriber.destroy()

    async def test_surfaces_a_control_stream_goaway(self) -> None:
        pair = create_transport_pair()

        async def raw_server() -> None:
            control = await pair.server.create_unidirectional_stream()
            await control.write(encode_setup([]))
            await control.write(encode_goaway(1500))

        asyncio.get_running_loop().create_task(raw_server())
        goaways: list[int] = []
        session = create_moqt_publish_session(
            pair.client,
            MoqtPublishSessionConfig(
                callbacks=MoqtPublishSessionCallbacks(on_goaway=lambda g: goaways.append(g.timeout))
            ),
        )

        await session.ready
        await wait_for(lambda: goaways == [1500])
        session.destroy()

    async def test_open_uni_stream_refuses_once_closing(self) -> None:
        harness = Harness()
        await harness.session.ready
        register(harness.session, "video")
        await raw_subscribe(harness.pair.server, "video", 21)
        await wait_for(lambda: harness.session._live_subscription(21) is not None)
        stream = await harness.session.open_uni_stream(21)
        assert stream is not None
        harness.session.close()

        try:
            await harness.session.open_uni_stream(21)
        except RuntimeError as error:
            assert "closed" in str(error)
        else:
            raise AssertionError("open_uni_stream should refuse once the session is closing")

        harness.subscriber.destroy()
