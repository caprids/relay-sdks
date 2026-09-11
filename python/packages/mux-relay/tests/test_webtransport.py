"""The aioquic WebTransport adapter: wire helpers, and the whole SDK over a real QUIC loopback.

The loopback test is the offline counterpart of the live smoke test in ``scripts/test_relay.py``: it
runs the same assertion list from ``spec/behavior.md`` against the in-memory relay hub served over
real WebTransport sessions on 127.0.0.1, so the transport adapter is exercised end to end.
"""

import asyncio
from collections.abc import AsyncIterator, Sequence
from typing import Any

import pytest
from core_helpers.waiting import wait_for
from relay_helpers.relay_hub import RelayHub, create_relay_hub
from relay_helpers.wt_server import (
    LoopbackWebTransportServer,
    WebTransportServerSession,
    start_webtransport_server,
)

from moq_core import MoqtObject, SubscriptionHandlers
from mux_relay import Closed, ConnectingTransport, connect_relay, connect_relay_publisher
from mux_relay.webtransport import (
    CAPSULE_CLOSE_WEBTRANSPORT_SESSION,
    WebTransportError,
    WebTransportSessionBase,
    _parse_url,
    connect_webtransport,
    encode_close_session_capsule,
    h3_to_webtransport_error_code,
    parse_sf_list_of_strings,
    parse_sf_string,
    sf_list_of_strings,
    webtransport_to_h3_error_code,
)

NAMESPACE = ["loopback", "data"]


class TestWireHelpers:
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

    async def test_close_capsule_from_the_peer_ends_the_session_cleanly(self) -> None:
        session = WebTransportSessionBase()
        capsule = encode_close_session_capsule(7, "bye")

        assert capsule[:2] == bytes([0x68, 0x43])  # 0x2843 as a two-byte QUIC varint
        # Split across two DATA frames: the capsule parser buffers partial input.
        session._on_connect_data(capsule[:3], False)
        assert not session._closed.done()
        session._on_connect_data(capsule[3:], False)
        assert session.close_info == (7, "bye")
        await session.wait_closed()

    def test_the_close_capsule_type_is_the_registered_value(self) -> None:
        assert CAPSULE_CLOSE_WEBTRANSPORT_SESSION == 0x2843


@pytest.fixture
async def loopback() -> AsyncIterator[tuple[LoopbackWebTransportServer, RelayHub, list[WebTransportServerSession]]]:
    hub = create_relay_hub()
    sessions: list[WebTransportServerSession] = []

    def on_session(session: WebTransportServerSession) -> None:
        sessions.append(session)

        if session.path.startswith("/publish"):
            hub.serve_publisher(session)
        else:
            hub.serve_subscriber(session)

    server = await start_webtransport_server(on_session)

    try:
        yield server, hub, sessions
    finally:
        hub.destroy()
        server.close()
        await asyncio.sleep(0.05)


def factory_for(server: LoopbackWebTransportServer):
    def create_transport(url: str, protocols: Sequence[str]) -> ConnectingTransport:
        return connect_webtransport(url, protocols, configuration=server.client_configuration())

    return create_transport


class TestLoopback:
    async def test_negotiates_the_moqt_protocol_over_a_real_webtransport_session(
        self, loopback: tuple[LoopbackWebTransportServer, RelayHub, list[WebTransportServerSession]]
    ) -> None:
        server, _, sessions = loopback
        created = connect_webtransport(
            server.url_for("/subscribe?jwt=test"), ["moqt-20"], configuration=server.client_configuration()
        )
        await asyncio.wait_for(created.ready, 5)
        transport: Any = created.transport

        assert transport.protocol_id == "moqt-20"
        await wait_for(lambda: len(sessions) == 1)
        assert sessions[0].path == "/subscribe?jwt=test"
        assert sessions[0].protocol_id == "moqt-20"

        transport.close()
        await asyncio.wait_for(transport.wait_closed(), 5)
        # The peer observed the CLOSE_WEBTRANSPORT_SESSION capsule as a clean end.
        await asyncio.wait_for(sessions[0].wait_closed(), 5)
        assert sessions[0].close_info == (0, "")

    async def test_runs_the_smoke_test_assertion_list_over_quic(
        self, loopback: tuple[LoopbackWebTransportServer, RelayHub, list[WebTransportServerSession]]
    ) -> None:
        server, hub, _ = loopback
        create_transport = factory_for(server)

        # 1. A publisher connection and a subscriber connection both open.
        publisher = await asyncio.wait_for(
            connect_relay_publisher(server.url_for("/publish"), namespace=NAMESPACE, create_transport=create_transport),
            10,
        )
        subscriber = await asyncio.wait_for(
            connect_relay(server.url_for("/subscribe"), create_transport=create_transport), 10
        )
        closing = False
        problems: list[str] = []

        def observe(label: str, closed: asyncio.Future[Closed]) -> None:
            def done(future: asyncio.Future[Closed]) -> None:
                if not closing:
                    problems.append(f"{label} closed: {future.result().error!r}")

            closed.add_done_callback(done)

        observe("publisher", publisher.closed)
        observe("subscriber", subscriber.closed)

        try:
            events = publisher.publish("events", write_timeout=5)
            status = publisher.publish("status", write_timeout=5)
            received: dict[str, list[bytes]] = {"events": [], "status": []}
            accepted: list[str] = []

            def subscribe(track: str) -> None:
                def on_object(obj: MoqtObject) -> None:
                    if obj.status == "normal":
                        received[track].append(obj.payload)

                subscriber.subscribe(
                    NAMESPACE,
                    track,
                    SubscriptionHandlers(
                        on_ok=lambda ok: accepted.append(track),
                        on_object=on_object,
                        on_error=lambda error: problems.append(f"{track}: {error.error_code} {error.reason}"),
                        on_subgroup_reset=lambda reset: problems.append(f"{track} reset: {reset.error!r}"),
                    ),
                )

            subscribe("events")
            subscribe("status")
            # 2. Subscriptions to two tracks on the namespace are accepted.
            await wait_for(lambda: sorted(accepted) == ["events", "status"], timeout=10)

            # 3. Binary, empty, and text payloads all arrive intact.
            expected_events = [bytes([0, 255, 128, 1]), b"hello from Relay SDK", b""]

            for payload in expected_events:
                await events.write(payload)

            await status.write(bytes([42]))
            await wait_for(lambda: len(received["events"]) == 3 and len(received["status"]) == 1, timeout=10)

            # 4. Closing one track writer does not affect delivery on another track.
            events.close()
            await status.write(bytes([43]))
            await wait_for(lambda: len(received["status"]) == 2, timeout=10)

            assert sorted(received["events"]) == sorted(expected_events)
            assert received["status"] == [bytes([42]), bytes([43])]
            assert problems == []
            assert hub.subscribes.count("events") >= 1
        finally:
            closing = True
            # 5. Both connections close cleanly once the test closes them.
            await asyncio.wait_for(publisher.close(), 10)
            subscriber.close()

        assert await publisher.closed == Closed()
        assert await subscriber.closed == Closed()

    async def test_reports_a_rejected_connect(self) -> None:
        # A plain HTTPS/QUIC server that is not there: the handshake never completes.
        created = connect_webtransport("https://127.0.0.1:9/", ["moqt-20"])

        with pytest.raises(WebTransportError, match="closed before the session was established"):
            created.transport.close()
            await created.ready
