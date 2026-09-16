import pytest

from moq_core.bytes import ByteReader, ByteWriter, utf8_encode
from moq_core.control_messages import (
    MESSAGE_TYPE,
    PARAMETER_TYPE,
    PUBLISH_DONE_STATUS,
    REQUEST_ERROR_CODE,
    SETUP_OPTION,
    AbsoluteLocationFilter,
    ControlMessage,
    ControlMessageDeframer,
    ControlMessageFrame,
    Fetch,
    FetchOk,
    Goaway,
    KeyValuePair,
    Location,
    LocationFilter,
    MessageParameters,
    NextObjectFilter,
    NoLocationFilter,
    PublishDone,
    PublishNamespace,
    PublishStateNotify,
    RangeFilter,
    RelativeGroupFilter,
    RequestError,
    Setup,
    Subscribe,
    SubscribeOk,
    decode_control_message,
    decode_key_value_pairs,
    decode_location_filter,
    decode_message_parameters,
    encode_control_message,
    encode_fetch,
    encode_fetch_ok,
    encode_goaway,
    encode_key_value_pairs,
    encode_location_filter,
    encode_message_parameters,
    encode_publish_done,
    encode_publish_namespace,
    encode_publish_state_notify,
    encode_request_error,
    encode_setup,
    encode_subscribe,
    encode_subscribe_ok,
    is_retryable_publish_done_status,
    is_retryable_request_error_code,
)
from moq_core.errors import MoqtProtocolError


def decode_all(data: bytes) -> list[ControlMessage]:
    return [decode_control_message(frame) for frame in ControlMessageDeframer().push(data)]


def encode_parameters(parameters: MessageParameters) -> bytes:
    writer = ByteWriter()
    encode_message_parameters(writer, parameters)
    return writer.to_bytes()


class TestControlMessageDeframer:
    def test_reassembles_messages_split_across_arbitrary_chunk_boundaries(self) -> None:
        encoded = encode_subscribe(
            Subscribe(request_id=0, track_namespace=["live", "a"], track_name="video")
        ) + encode_goaway(250)

        # Split at every possible boundary and confirm both frames come out.
        for split in range(1, len(encoded)):
            deframer = ControlMessageDeframer()
            frames = [*deframer.push(encoded[:split]), *deframer.push(encoded[split:])]

            assert len(frames) == 2
            assert frames[0].type == MESSAGE_TYPE.SUBSCRIBE
            assert frames[1].type == MESSAGE_TYPE.GOAWAY
            assert deframer.pending_bytes == 0

    def test_yields_one_frame_per_push_when_messages_arrive_whole(self) -> None:
        deframer = ControlMessageDeframer()

        assert len(deframer.push(encode_goaway(0))) == 1
        assert len(deframer.push(encode_publish_done(0x2, 5, "track ended"))) == 1


class TestEncodeSubscribe:
    def test_round_trips_through_the_decoder(self) -> None:
        parameters = MessageParameters(
            subscriber_priority=16, group_order="ascending", location_filter=NextObjectFilter(), forward=1
        )
        request = Subscribe(
            request_id=4, track_namespace=["live", "stream1"], track_name="catalog", parameters=parameters
        )

        assert decode_all(encode_subscribe(request)) == [request]

    def test_round_trips_the_canonical_current_group_join(self) -> None:
        # Next Object plus a StartGroup=1 fill.
        parameters = MessageParameters(
            location_filter=NextObjectFilter(),
            fill_parameters=MessageParameters(location_filter=RelativeGroupFilter(groups_before_next=1)),
        )
        request = Subscribe(request_id=0, track_namespace=["live"], track_name="video", parameters=parameters)

        assert decode_all(encode_subscribe(request)) == [request]

    def test_rejects_empty_namespace_fields(self) -> None:
        with pytest.raises(MoqtProtocolError):
            encode_subscribe(Subscribe(request_id=0, track_namespace=[""], track_name="t"))


class TestEncodeFetch:
    def test_round_trips_a_fetch_whose_range_rides_in_location_filter(self) -> None:
        request = Fetch(
            request_id=2,
            track_namespace=["live"],
            track_name="video",
            parameters=MessageParameters(
                location_filter=AbsoluteLocationFilter(start=Location(10, 0), end_group_delta=2)
            ),
        )

        assert decode_all(encode_fetch(request)) == [request]

    def test_round_trips_an_unfiltered_fetch(self) -> None:
        # The whole track up to Largest Object.
        request = Fetch(request_id=6, track_namespace=["live"], track_name="catalog")

        [decoded] = decode_all(encode_fetch(request))
        assert decoded == request
        assert isinstance(decoded, Fetch)
        assert decoded.parameters == MessageParameters()


class TestEncodeSubscribeOk:
    def test_round_trips_alias_parameters_and_track_properties(self) -> None:
        [message] = decode_all(
            encode_subscribe_ok(
                7, MessageParameters(expires=30_000, largest_object=Location(41, 12)), [KeyValuePair(0x02, 1)]
            )
        )

        assert message == SubscribeOk(
            track_alias=7,
            parameters=MessageParameters(expires=30_000, largest_object=Location(41, 12)),
            track_properties=[KeyValuePair(0x02, 1)],
        )


class TestEncodePublishDone:
    def test_round_trips_a_counted_stream_total(self) -> None:
        [message] = decode_all(encode_publish_done(PUBLISH_DONE_STATUS.TRACK_ENDED, 5, "ended"))

        assert message == PublishDone(status_code=PUBLISH_DONE_STATUS.TRACK_ENDED, stream_count=5, reason="ended")

    def test_reads_the_2_64_minus_1_could_not_count_sentinel_as_none(self) -> None:
        # Status, then the 9-byte varint 0xFF..FF (2^64-1), then an empty reason.
        body = bytes([PUBLISH_DONE_STATUS.GOING_AWAY, *([0xFF] * 9), 0x00])
        message = decode_control_message(ControlMessageFrame(MESSAGE_TYPE.PUBLISH_DONE, body))

        assert message == PublishDone(status_code=PUBLISH_DONE_STATUS.GOING_AWAY, stream_count=None, reason="")

    def test_reads_any_other_9_byte_count_as_a_number(self) -> None:
        # One bit short of the sentinel: a real (if absurd) count, exact in Python.
        body = bytes([PUBLISH_DONE_STATUS.GOING_AWAY, 0xFF, 0x80, *([0xFF] * 7), 0x00])
        message = decode_control_message(ControlMessageFrame(MESSAGE_TYPE.PUBLISH_DONE, body))

        assert isinstance(message, PublishDone)
        assert message.stream_count == 0x80FFFFFFFFFFFFFF

    def test_encodes_a_none_count_back_to_the_sentinel(self) -> None:
        message = PublishDone(status_code=PUBLISH_DONE_STATUS.GOING_AWAY, stream_count=None, reason="")

        assert decode_all(encode_control_message(message)) == [message]


class TestEncodePublishStateNotify:
    def test_round_trips_the_changed_parameters(self) -> None:
        parameters = MessageParameters(
            largest_object=Location(9, 4), location_filter=RelativeGroupFilter(groups_before_next=0)
        )

        assert decode_all(encode_publish_state_notify(parameters)) == [PublishStateNotify(parameters=parameters)]


class TestEncodeRequestError:
    def test_round_trips_code_retry_interval_and_reason(self) -> None:
        [message] = decode_all(encode_request_error(REQUEST_ERROR_CODE.DOES_NOT_EXIST, "no such track", 1500))

        assert message == RequestError(
            error_code=REQUEST_ERROR_CODE.DOES_NOT_EXIST, retry_interval=1500, reason="no such track"
        )


class TestEncodeSetup:
    def test_round_trips_setup_options_as_generic_key_value_pairs(self) -> None:
        [message] = decode_all(
            encode_setup(
                [
                    KeyValuePair(SETUP_OPTION.MAX_REQUEST_UPDATES, 3),
                    KeyValuePair(SETUP_OPTION.MOQT_IMPLEMENTATION, utf8_encode("spf-test")),
                ]
            )
        )

        assert isinstance(message, Setup)
        # Serialized in ascending type order regardless of input order.
        assert [option.type for option in message.options] == [
            SETUP_OPTION.MOQT_IMPLEMENTATION,
            SETUP_OPTION.MAX_REQUEST_UPDATES,
        ]
        assert message.options[1].value == 3


class TestEncodeGoaway:
    def test_encodes_a_zero_length_uri_and_round_trips_the_timeout(self) -> None:
        assert decode_all(encode_goaway(500)) == [Goaway(new_session_uri="", timeout=500)]


class TestEncodePublishNamespace:
    def test_round_trips_through_the_decoder(self) -> None:
        request = PublishNamespace(request_id=1, track_namespace=["anon"], parameters=MessageParameters(forward=1))

        assert decode_all(encode_publish_namespace(request)) == [request]


class TestDecodeControlMessage:
    def test_raises_for_unknown_message_types(self) -> None:
        with pytest.raises(MoqtProtocolError):
            decode_control_message(ControlMessageFrame(0x99, b""))

    def test_raises_when_the_body_has_trailing_bytes(self) -> None:
        # GOAWAY body: uri length 0, timeout 0, plus a stray trailing byte.
        with pytest.raises(MoqtProtocolError):
            decode_control_message(ControlMessageFrame(MESSAGE_TYPE.GOAWAY, bytes([0, 0, 0xAB])))

    def test_raises_a_protocol_error_for_a_truncated_body(self) -> None:
        # PUBLISH_NAMESPACE body cut short: request id, then no namespace. The read past the frame
        # must be normalized so malformed frames terminate the session.
        with pytest.raises(MoqtProtocolError):
            decode_control_message(ControlMessageFrame(MESSAGE_TYPE.PUBLISH_NAMESPACE, bytes([0x01])))

        # A namespace field whose declared length overruns the frame.
        body = ByteWriter()
        body.write_varint(1)  # request id
        body.write_varint(1)  # field count
        body.write_varint(10)  # field length, followed by only 2 bytes
        body.write_bytes(utf8_encode("ab"))

        with pytest.raises(MoqtProtocolError):
            decode_control_message(ControlMessageFrame(MESSAGE_TYPE.PUBLISH_NAMESPACE, body.to_bytes()))


class TestEncodeMessageParameters:
    def test_round_trips_every_supported_parameter(self) -> None:
        parameters = MessageParameters(
            object_delivery_timeout=100,
            subgroup_delivery_timeout=200,
            authorization_tokens=[utf8_encode("tok-a"), utf8_encode("tok-b")],
            expires=60_000,
            largest_object=Location(100, 3),
            forward=0,
            subscriber_priority=200,
            location_filter=AbsoluteLocationFilter(start=Location(5, 1), end_group_delta=4),
            group_order="descending",
            fill_parameters=MessageParameters(
                fill_timeout=500,
                subscriber_priority=8,
                location_filter=RelativeGroupFilter(groups_before_next=1),
                group_order="ascending",
            ),
            new_group_request=42,
            include_properties=0,
        )

        assert decode_message_parameters(ByteReader(encode_parameters(parameters))) == parameters

    def test_raises_on_an_unknown_parameter_type(self) -> None:
        # Hand-craft: 1 parameter, type 0x99 (unknown).
        writer = ByteWriter()
        writer.write_varint(1)
        writer.write_varint(0x99)

        with pytest.raises(MoqtProtocolError):
            decode_message_parameters(ByteReader(writer.to_bytes()))

    def test_frames_largest_object_as_bare_varints_with_moq_relay_0_14_17(self) -> None:
        # Upstream draft-20 vector from moq-dev/moq#3561, independent of our encoder.
        wire = bytes([0x01, 0x09, 0x80, 0xFF, 0x80, 0x80])
        parameters = MessageParameters(largest_object=Location(255, 128))
        assert encode_parameters(parameters) == wire
        assert decode_message_parameters(ByteReader(wire)) == parameters

    @pytest.mark.parametrize("wire", [b"\x01\x09", b"\x01\x09\x80", b"\x01\x09\x01", b"\x01\x09\x01\x80"])
    def test_rejects_truncated_largest_object(self, wire: bytes) -> None:
        with pytest.raises(MoqtProtocolError):
            decode_control_message(ControlMessageFrame(type=MESSAGE_TYPE.SUBSCRIBE_OK, body=b"\x07" + wire))

    @pytest.mark.parametrize(
        ("group", "object_id", "location_bytes"),
        [(0, 0, [0, 0]), (1, 2, [1, 2]), (255, 128, [0x80, 0xFF, 0x80, 0x80])],
    )
    def test_subscribe_ok_largest_object_with_following_fields(
        self, group: int, object_id: int, location_bytes: list[int]
    ) -> None:
        # Alias 7; Largest Object, delta 1 => Fill Timeout (0x0a); track property type 2, value 1.
        body = bytes([7, 2, 9, *location_bytes, 1, 5, 2, 1])
        wire = bytes([4, 0, len(body)]) + body
        parameters = MessageParameters(largest_object=Location(group, object_id), fill_timeout=5)
        properties = [KeyValuePair(2, 1)]
        assert decode_all(wire) == [SubscribeOk(track_alias=7, parameters=parameters, track_properties=properties)]
        assert encode_subscribe_ok(7, parameters, properties) == wire
        with pytest.raises(MoqtProtocolError):
            decode_control_message(ControlMessageFrame(type=MESSAGE_TYPE.SUBSCRIBE_OK, body=body + b"\x00"))

    def test_frames_include_properties_as_a_length_prefixed_byte_and_rejects_other_values(self) -> None:
        assert list(encode_parameters(MessageParameters(include_properties=1))) == [0x01, 0x35, 0x01, 0x01]

        writer = ByteWriter()
        writer.write_varint(1)
        writer.write_varint(PARAMETER_TYPE.INCLUDE_PROPERTIES)
        writer.write_varint(1)
        writer.write_uint8(2)

        with pytest.raises(MoqtProtocolError, match="INCLUDE_PROPERTIES"):
            decode_message_parameters(ByteReader(writer.to_bytes()))

    def test_encodes_fill_parameters_as_a_nested_parameter_list(self) -> None:
        encoded = encode_parameters(
            MessageParameters(
                fill_parameters=MessageParameters(location_filter=RelativeGroupFilter(groups_before_next=1))
            )
        )

        # count 1, type 0x23, length 4, then the nested list: count 1, type 0x21, length 1, StartGroup 1.
        assert list(encoded) == [0x01, 0x23, 0x04, 0x01, 0x21, 0x01, 0x01]

    def test_rejects_parameters_outside_table_6_inside_fill_parameters_on_both_sides(self) -> None:
        # Decode: FILL_PARAMETERS whose nested list carries FORWARD (0x10).
        writer = ByteWriter()
        writer.write_varint(1)
        writer.write_varint(PARAMETER_TYPE.FILL_PARAMETERS)
        writer.write_varint(3)
        writer.write_varint(1)  # nested count
        writer.write_varint(PARAMETER_TYPE.FORWARD)
        writer.write_uint8(1)

        with pytest.raises(MoqtProtocolError, match="FILL_PARAMETERS"):
            decode_message_parameters(ByteReader(writer.to_bytes()))

        # Encode: a range filter type the fill scope excludes (TRACK_PROPERTY_FILTER).
        with pytest.raises(MoqtProtocolError, match="FILL_PARAMETERS"):
            encode_parameters(
                MessageParameters(
                    fill_parameters=MessageParameters(
                        range_filters=[RangeFilter(PARAMETER_TYPE.TRACK_PROPERTY_FILTER, b"")]
                    )
                )
            )


class TestEncodeLocationFilter:
    def test_round_trips_every_filter_shape(self) -> None:
        filters: list[LocationFilter] = [
            NoLocationFilter(),
            NextObjectFilter(),
            RelativeGroupFilter(groups_before_next=0),
            RelativeGroupFilter(groups_before_next=1),
            RelativeGroupFilter(groups_before_next=5),
            AbsoluteLocationFilter(start=Location(3, 9)),
            AbsoluteLocationFilter(start=Location(3, 9), end_group_delta=0),
            AbsoluteLocationFilter(start=Location(3, 9), end_group_delta=2, end_object=7),
        ]

        for location_filter in filters:
            assert decode_location_filter(encode_location_filter(location_filter)) == location_filter

    def test_selects_the_meaning_by_field_count(self) -> None:
        assert list(encode_location_filter(NoLocationFilter())) == []
        assert list(encode_location_filter(RelativeGroupFilter(groups_before_next=1))) == [1]
        assert list(encode_location_filter(NextObjectFilter())) == [0, 0]
        assert list(encode_location_filter(AbsoluteLocationFilter(start=Location(3, 9)))) == [3, 9]
        assert list(
            encode_location_filter(AbsoluteLocationFilter(start=Location(3, 9), end_group_delta=2, end_object=7))
        ) == [3, 9, 2, 7]

    def test_normalizes_an_open_ended_absolute_zero_to_no_filter(self) -> None:
        # Rather than the Next Object spelling.
        assert list(encode_location_filter(AbsoluteLocationFilter(start=Location(0, 0)))) == []
        # Bounded, {0,0} is a real absolute start.
        assert list(encode_location_filter(AbsoluteLocationFilter(start=Location(0, 0), end_group_delta=3))) == [
            0,
            0,
            3,
        ]

    def test_rejects_an_end_object_without_an_end_group(self) -> None:
        with pytest.raises(MoqtProtocolError):
            encode_location_filter(AbsoluteLocationFilter(start=Location(1, 0), end_object=4))

    def test_rejects_more_than_four_fields(self) -> None:
        with pytest.raises(MoqtProtocolError):
            decode_location_filter(bytes([1, 2, 3, 4, 5]))

    def test_rejects_an_end_group_past_the_supported_range(self) -> None:
        writer = ByteWriter()
        writer.write_varint(2**64 - 1)
        writer.write_varint(0)
        writer.write_varint(1)

        with pytest.raises(MoqtProtocolError):
            decode_location_filter(writer.to_bytes())


class TestEncodeFetchOk:
    def test_round_trips_and_rejects_end_of_track_values_other_than_0_and_1(self) -> None:
        not_ended = encode_fetch_ok(False, Location(3, 9))
        ended = encode_fetch_ok(True, Location(3, 9))

        assert decode_all(not_ended) == [FetchOk(end_of_track=False, end_location=Location(3, 9))]
        assert decode_all(ended) == [FetchOk(end_of_track=True, end_location=Location(3, 9))]

        # The end-of-track byte is the single position where the two differ.
        offset = next(index for index, (a, b) in enumerate(zip(not_ended, ended, strict=True)) if a != b)
        invalid = bytearray(not_ended)
        invalid[offset] = 2

        with pytest.raises(MoqtProtocolError):
            decode_all(bytes(invalid))


class TestEncodeKeyValuePairs:
    def test_delta_encodes_types_and_round_trips_even_odd_values(self) -> None:
        pairs = [KeyValuePair(0x02, 7), KeyValuePair(0x07, utf8_encode("bytes")), KeyValuePair(0x08, 1_000_000)]
        writer = ByteWriter()
        encode_key_value_pairs(writer, pairs)
        encoded = writer.to_bytes()

        # First delta is the absolute type (0x02), then 5, then 1.
        assert encoded[0] == 0x02
        assert decode_key_value_pairs(ByteReader(encoded), len(encoded)) == pairs

    def test_rejects_values_of_the_wrong_shape_for_the_type_parity(self) -> None:
        with pytest.raises(MoqtProtocolError, match="varint value"):
            encode_key_value_pairs(ByteWriter(), [KeyValuePair(0x02, b"bytes")])
        with pytest.raises(MoqtProtocolError, match="byte value"):
            encode_key_value_pairs(ByteWriter(), [KeyValuePair(0x03, 7)])


class TestIsRetryableRequestErrorCode:
    def test_keeps_transient_state_shaped_failures_retryable(self) -> None:
        for code in (
            REQUEST_ERROR_CODE.DOES_NOT_EXIST,
            REQUEST_ERROR_CODE.TIMEOUT,
            REQUEST_ERROR_CODE.INTERNAL_ERROR,
            REQUEST_ERROR_CODE.GOING_AWAY,
            REQUEST_ERROR_CODE.EXCESSIVE_LOAD,
            # EXPIRED_AUTH_TOKEN has its own refresh path; the generic classifier must not preempt it.
            REQUEST_ERROR_CODE.EXPIRED_AUTH_TOKEN,
        ):
            assert is_retryable_request_error_code(code)

    def test_treats_request_shaped_rejections_as_permanent(self) -> None:
        for code in (
            REQUEST_ERROR_CODE.UNAUTHORIZED,
            REQUEST_ERROR_CODE.MALFORMED_AUTH_TOKEN,
            REQUEST_ERROR_CODE.NOT_SUPPORTED,
            REQUEST_ERROR_CODE.MALFORMED_TRACK,
            REQUEST_ERROR_CODE.INVALID_FILTER,
            REQUEST_ERROR_CODE.REDIRECT,
        ):
            assert not is_retryable_request_error_code(code)

    def test_degrades_unknown_codes_to_retryable(self) -> None:
        assert is_retryable_request_error_code(0x7FFF)


class TestIsRetryablePublishDoneStatus:
    def test_keeps_publisher_relay_life_cycle_ends_retryable(self) -> None:
        for status in (
            PUBLISH_DONE_STATUS.TRACK_ENDED,
            PUBLISH_DONE_STATUS.GOING_AWAY,
            PUBLISH_DONE_STATUS.TOO_FAR_BEHIND,
            0x7FFF,
        ):
            assert is_retryable_publish_done_status(status)

    def test_treats_auth_shaped_and_malformed_track_ends_as_permanent(self) -> None:
        for status in (
            PUBLISH_DONE_STATUS.UNAUTHORIZED,
            PUBLISH_DONE_STATUS.EXPIRED,
            PUBLISH_DONE_STATUS.MALFORMED_TRACK,
        ):
            assert not is_retryable_publish_done_status(status)


class TestEncodeControlMessage:
    def test_round_trips_every_message_kind(self) -> None:
        # The generic encoder is the inverse of the decoder for every variant, including the ones the
        # per-message helpers do not spell out (redirects, a non-empty GOAWAY URI, PUBLISH_SKIPPED).
        from moq_core.control_messages import (
            Namespace,
            NamespaceDone,
            Publish,
            PublishSkipped,
            Redirect,
            RequestOk,
            RequestUpdate,
            SubscribeNamespace,
            TrackStatus,
        )

        messages: list[ControlMessage] = [
            Setup(options=[KeyValuePair(SETUP_OPTION.MAX_REQUEST_UPDATES, 2)]),
            Goaway(new_session_uri="https://relay2.example.com/moq", timeout=1000),
            Subscribe(request_id=0, track_namespace=["a"], track_name="b"),
            TrackStatus(request_id=2, track_namespace=["a"], track_name="b"),
            Fetch(request_id=4, track_namespace=["a"], track_name="b"),
            SubscribeNamespace(request_id=1, track_namespace_prefix=["a"]),
            SubscribeOk(track_alias=3, track_properties=[KeyValuePair(0x08, 90_000)]),
            RequestOk(parameters=MessageParameters(expires=1)),
            RequestError(
                error_code=REQUEST_ERROR_CODE.REDIRECT,
                retry_interval=5,
                reason="moved",
                redirect=Redirect(connect_uri="https://other.example.com", track_namespace=["x"], track_name="y"),
            ),
            RequestUpdate(request_id=6, parameters=MessageParameters(forward=0)),
            PublishStateNotify(parameters=MessageParameters(largest_object=Location(1, 2))),
            PublishDone(status_code=PUBLISH_DONE_STATUS.TRACK_ENDED, stream_count=3, reason="done"),
            Publish(request_id=7, track_namespace=["a"], track_name="b", track_alias=9),
            PublishNamespace(request_id=9, track_namespace=["a"]),
            FetchOk(end_of_track=True, end_location=Location(5, 6)),
            Namespace(track_namespace_suffix=["s"]),
            NamespaceDone(track_namespace_suffix=["s"]),
            PublishSkipped(track_namespace_suffix=["s"], track_name="t"),
        ]

        for message in messages:
            assert decode_all(encode_control_message(message)) == [message], message.kind
