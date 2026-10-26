"""The WebTransport accepting end in ``relay_helpers.webtransport``, which the TypeScript suite's Chromium target dials.

The SDK no longer dials WebTransport (ADR 0004), so nothing here is product code: these tests keep the conformance
peer's HTTP/3 layer honest between runs of ``pnpm test:conformance``. They cover its wire codecs, its capsule
handling, and the ``H3Connection`` private members ``relay_helpers.webtransport.h3`` names, so an aioquic bump fails
here before it fails in a browser.
"""

import pytest
from aioquic.h3.connection import ErrorCode, FrameType, H3Connection, H3Stream
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.connection import QuicConnection
from aioquic.quic.events import StreamDataReceived
from relay_helpers.webtransport.h3 import forget_h3_stream, mark_webtransport_stream, peer_control_stream_id
from relay_helpers.webtransport.session import WebTransportSessionBase
from relay_helpers.webtransport.wire import (
    CAPSULE_CLOSE_WEBTRANSPORT_SESSION,
    encode_close_session_capsule,
    h3_to_webtransport_error_code,
    parse_sf_list_of_strings,
    parse_sf_string,
    sf_list_of_strings,
    webtransport_to_h3_error_code,
)

from caprid_relay.quic._errors import QuicTransportError, _session_error


class TestWire:
    def test_error_codes_round_trip_through_the_reserved_h3_range(self) -> None:
        for code in (0, 1, 0x1D, 0x1E, 0x1F, 12345, 0xFFFFFFFF):
            mapped = webtransport_to_h3_error_code(code)
            assert h3_to_webtransport_error_code(mapped) == code

        # The draft's worked example: application code 0 sits at the start of the range.
        assert webtransport_to_h3_error_code(0) == 0x52E4A40FA8DB
        assert h3_to_webtransport_error_code(0x100) is None
        # Every 31st value in the range is skipped (a GREASE code point), so it maps to nothing.
        assert h3_to_webtransport_error_code(0x52E4A40FA8DB + 0x1E) is None

    def test_structured_field_lists_of_strings(self) -> None:
        assert sf_list_of_strings(["moqt-20"]) == b'"moqt-20"'
        assert sf_list_of_strings(["a", 'b"c']) == b'"a", "b\\"c"'
        assert parse_sf_list_of_strings(b'"moqt-20", "moqt-19"') == ["moqt-20", "moqt-19"]
        assert parse_sf_string(b'"moqt-20"') == "moqt-20"
        assert parse_sf_string(b"moqt-20") == "moqt-20"

    def test_the_close_capsule_type_is_the_registered_value(self) -> None:
        assert CAPSULE_CLOSE_WEBTRANSPORT_SESSION == 0x2843


class TestSessionClose:
    async def test_transport_failures_keep_their_details(self) -> None:
        session = WebTransportSessionBase()
        error = _session_error("QUIC connection terminated: Idle timeout (code 0x1)")
        session._finish(error)

        with pytest.raises(QuicTransportError) as failure:
            await session.wait_closed()

        assert failure.value is error

    async def test_a_close_capsule_with_code_zero_ends_the_session_cleanly(self) -> None:
        session = WebTransportSessionBase()
        capsule = encode_close_session_capsule(0, "done")

        assert capsule[:2] == bytes([0x68, 0x43])  # 0x2843 as a two-byte QUIC varint
        # Split across two DATA frames: the capsule parser buffers partial input.
        session._on_connect_data(capsule[:3], False)
        assert not session._closed.done()
        session._on_connect_data(capsule[3:], False)
        assert session.close_info == (0, "done")
        await session.wait_closed()

    async def test_a_fin_without_a_capsule_ends_the_session_cleanly(self) -> None:
        session = WebTransportSessionBase()
        session._on_connect_data(b"", True)
        assert session.close_info is None
        await session.wait_closed()

    async def test_a_close_capsule_with_a_nonzero_code_fails_the_session_with_that_code(self) -> None:
        session = WebTransportSessionBase()
        # The bytes moq-relay 0.15.8 sends when it refuses a connection's credentials.
        capsule = bytes.fromhex("68431000000002756e617574686f72697a6564")
        assert capsule == encode_close_session_capsule(2, "unauthorized")

        session._on_connect_data(capsule, True)
        assert session.close_info == (2, "unauthorized")

        with pytest.raises(QuicTransportError) as failure:
            await session.wait_closed()

        assert (failure.value.source, failure.value.close_code, failure.value.reason) == ("session", 2, "unauthorized")


class TestH3Internals:
    """Pins the ``H3Connection`` private members ``relay_helpers.webtransport.h3`` names, by name and behaviour."""

    @staticmethod
    def _connection() -> QuicConnection:
        return QuicConnection(configuration=QuicConfiguration(is_client=True))

    def test_the_private_h3_members_still_exist(self) -> None:
        # `_get_or_create_stream` became a context manager in aioquic 1.3.0; under 1.2 it is a plain method.
        quic = self._connection()
        h3 = H3Connection(quic, enable_webtransport=True)
        stream_id = quic.get_next_available_stream_id(is_unidirectional=True)
        assert isinstance(h3._stream, dict)
        with h3._get_or_create_stream(stream_id) as h3_stream:
            assert isinstance(h3_stream, H3Stream)
        assert stream_id in h3._stream
        assert h3._peer_control_stream_id is None  # set once the peer's control stream type is read

    def test_mark_webtransport_stream_records_the_frame_type_and_session(self) -> None:
        quic = self._connection()
        h3 = H3Connection(quic, enable_webtransport=True)
        stream_id = quic.get_next_available_stream_id()

        mark_webtransport_stream(h3, stream_id, 4)

        assert h3._stream[stream_id].frame_type == FrameType.WEBTRANSPORT_STREAM
        assert h3._stream[stream_id].session_id == 4

    def test_forget_h3_stream_drops_the_record(self) -> None:
        quic = self._connection()
        h3 = H3Connection(quic, enable_webtransport=True)
        stream_id = quic.get_next_available_stream_id()
        mark_webtransport_stream(h3, stream_id, 4)

        forget_h3_stream(h3, stream_id)

        assert stream_id not in h3._stream
        forget_h3_stream(h3, stream_id)  # already gone: nothing to do

    def test_peer_control_stream_id_names_the_stream_once_its_type_is_read(self) -> None:
        quic = self._connection()
        h3 = H3Connection(quic, enable_webtransport=True)
        assert peer_control_stream_id(h3) is None

        h3.handle_event(StreamDataReceived(data=b"\x00", end_stream=False, stream_id=3))  # stream type CONTROL

        assert peer_control_stream_id(h3) == 3

        # What the protocol holds that stream's FIN back from: aioquic closes the connection with
        # H3_CLOSED_CRITICAL_STREAM at once and ignores every later event (#14).
        h3.handle_event(StreamDataReceived(data=b"", end_stream=True, stream_id=3))

        assert quic._close_event is not None
        assert quic._close_event.error_code == ErrorCode.H3_CLOSED_CRITICAL_STREAM
        assert h3.handle_event(StreamDataReceived(data=b"\x00\x00", end_stream=False, stream_id=0)) == []
