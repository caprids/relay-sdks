"""The aioquic WebTransport adapter's wire helpers: error codes, structured fields, capsules, the URL and the socket."""

from typing import Any

import pytest

from mux_relay.webtransport import WebTransportError
from mux_relay.webtransport._client import _open_socket, _parse_url
from mux_relay.webtransport._session import WebTransportSessionBase
from mux_relay.webtransport._wire import (
    CAPSULE_CLOSE_WEBTRANSPORT_SESSION,
    encode_close_session_capsule,
    h3_to_webtransport_error_code,
    parse_sf_list_of_strings,
    parse_sf_string,
    sf_list_of_strings,
    webtransport_to_h3_error_code,
)


class TestWireHelpers:
    async def test_transport_failures_keep_their_details(self) -> None:
        session = WebTransportSessionBase()
        error = WebTransportError("QUIC connection terminated", code=1, reason="Idle timeout")
        session._finish(error)

        with pytest.raises(WebTransportError) as failure:
            await session.wait_closed()

        assert failure.value is error
        assert str(error) == "QUIC connection terminated"
        assert error.code == 1
        assert error.reason == "Idle timeout"

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

    def test_parses_endpoints_into_connect_headers(self) -> None:
        endpoint = _parse_url("https://sjc.relay.mux.global")
        assert (endpoint.host, endpoint.port, endpoint.authority, endpoint.path) == (
            "sjc.relay.mux.global",
            443,
            "sjc.relay.mux.global",
            "/",
        )

        endpoint = _parse_url("https://127.0.0.1:4443/moq?jwt=abc")
        assert (endpoint.authority, endpoint.path) == ("127.0.0.1:4443", "/moq?jwt=abc")

        with pytest.raises(ValueError, match="HTTPS"):
            _parse_url("http://relay.example.com")

        with pytest.raises(TypeError, match="host"):
            _parse_url("https:///moq")

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

        with pytest.raises(WebTransportError) as failure:
            await session.wait_closed()

        assert failure.value.peer_closed
        assert (failure.value.code, failure.value.reason) == (2, "unauthorized")
        assert str(failure.value) == "peer closed the WebTransport session: unauthorized (code 0x2)"

    def test_the_close_capsule_type_is_the_registered_value(self) -> None:
        assert CAPSULE_CLOSE_WEBTRANSPORT_SESSION == 0x2843

    def test_socket_fallback_picks_an_ipv4_address_when_ipv6_sockets_are_unavailable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import socket as socket_module

        real_socket = socket_module.socket

        def no_ipv6(family: int = socket_module.AF_INET, *args: Any, **kwargs: Any) -> Any:
            if family == socket_module.AF_INET6:
                raise OSError("IPv6 is unavailable")

            return real_socket(family, *args, **kwargs)

        monkeypatch.setattr(socket_module, "socket", no_ipv6)
        # The resolver put an IPv6 address first; the IPv4 socket must not be pointed at it.
        sock, address = _open_socket([("2001:db8::1", 443, 0, 0), ("192.0.2.1", 443)])

        try:
            assert sock.family == socket_module.AF_INET
            assert address == ("192.0.2.1", 443)
            assert sock.getpeername() == address  # connected, so a refusal reaches the protocol
        finally:
            sock.close()

        with pytest.raises(WebTransportError, match="only to IPv6"):
            _open_socket([("2001:db8::1", 443, 0, 0)])

    def test_dual_stack_socket_maps_ipv4_addresses(self) -> None:
        try:
            sock, address = _open_socket([("192.0.2.1", 443)])
        except WebTransportError:
            pytest.skip("no IPv6 socket support on this host")

        try:
            assert address == ("::ffff:192.0.2.1", 443, 0, 0)
            assert sock.getpeername() == address
        finally:
            sock.close()
