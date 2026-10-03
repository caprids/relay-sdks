"""The setup options the connection sends for each framing (spec/transport-adapter.md TA-1, TA-4, TA-6).

Driven by the language-neutral cases in ``spec/vectors/setup-options.json``, as ``ts/tests/setup-options.test.ts`` is.
"""

import asyncio
import json
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import pytest
from core_helpers.fake_transport import FakeTransport, create_fake_transport

from moq_core.bytes import StreamReader, utf8_decode
from moq_core.control_messages import SETUP_OPTION, ControlMessageFrame, KeyValuePair, Setup, decode_control_message
from mux_relay import (
    CancellationToken,
    ConnectingTransport,
    RelayError,
    connect_relay_publisher,
    connect_relay_subscriber,
)

VECTORS = json.loads(
    (Path(__file__).parents[4] / "spec" / "vectors" / "setup-options.json").read_text(encoding="utf-8")
)["cases"]

ROLES: dict[str, Callable[..., Any]] = {
    "subscriber": connect_relay_subscriber,
    "publisher": lambda url, **options: connect_relay_publisher(url, namespace=["room"], **options),
}


async def read_client_setup(reader: StreamReader) -> list[KeyValuePair]:
    """Read the SETUP message the client opens its control stream with."""
    stream_type = await reader.read_varint()
    high = await reader.read_uint8()
    low = await reader.read_uint8()
    message = decode_control_message(ControlMessageFrame(stream_type, await reader.read_bytes(high * 256 + low)))
    assert isinstance(message, Setup)
    return list(message.options)


def option_value(options: Sequence[KeyValuePair], option_type: int) -> str | None:
    matches = [option for option in options if option.type == option_type]
    assert len(matches) <= 1
    if not matches:
        return None
    value = matches[0].value
    assert isinstance(value, bytes), f"setup option {option_type} is not a byte field"
    return utf8_decode(value)


class _Factory:
    def __init__(self, fake: FakeTransport, framing: Any = None) -> None:
        self.fake = fake
        self.framing = framing
        self.urls: list[str] = []

    def __call__(self, url: str, protocols: Sequence[str]) -> ConnectingTransport:
        self.urls.append(url)
        ready: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        ready.set_result(None)

        if self.framing is None:
            return ConnectingTransport(transport=self.fake, ready=ready)

        return ConnectingTransport(transport=self.fake, ready=ready, framing=self.framing)


async def client_setup(role: str, vector: dict[str, Any]) -> tuple[str | None, str | None, str]:
    """Connect ``role`` through a fake transport; return the client's PATH and AUTHORITY and the URL the factory saw."""
    fake = create_fake_transport()
    factory = _Factory(fake, vector.get("framing"))
    owner = CancellationToken()
    token = vector.get("token")
    connecting = asyncio.ensure_future(
        ROLES[role](
            vector["url"],
            cancellation=owner,
            create_transport=factory,
            get_token=(lambda: token) if token is not None else None,
        )
    )

    try:
        options = await asyncio.wait_for(read_client_setup(await fake.client_control_stream()), 5)
    finally:
        owner.cancel()
        await asyncio.wait({connecting})

    return option_value(options, SETUP_OPTION.PATH), option_value(options, SETUP_OPTION.AUTHORITY), factory.urls[0]


@pytest.mark.parametrize("role", list(ROLES))
class TestClientSetup:
    @pytest.mark.parametrize("vector", VECTORS, ids=[vector["name"] for vector in VECTORS])
    async def test_ta6_sends_path_and_authority_only_on_native_quic(self, role: str, vector: dict[str, Any]) -> None:
        path, authority, _ = await client_setup(role, vector)

        assert (path, authority) == (vector["path"], vector["authority"])

    @pytest.mark.parametrize("framing", ["webtransport", "native-quic"])
    async def test_ta4_hands_every_framing_the_https_url_with_the_token_in_its_jwt_parameter(
        self, role: str, framing: str
    ) -> None:
        vector = {"url": "https://relay.example.com/moq", "token": "abc", "framing": framing}
        _, _, url = await client_setup(role, vector)

        assert url == "https://relay.example.com/moq?jwt=abc"

    async def test_ta1_an_unknown_framing_fails_the_connect_as_a_transport_error_and_closes_the_transport(
        self, role: str
    ) -> None:
        fake = create_fake_transport()

        with pytest.raises(RelayError) as failure:
            await ROLES[role]("https://relay.example.com/moq", create_transport=_Factory(fake, "quic"))

        assert (failure.value.code, failure.value.phase) == ("TRANSPORT_ERROR", "connecting")
        assert "unknown framing 'quic'" in str(failure.value)
        assert fake.close_calls == 1
