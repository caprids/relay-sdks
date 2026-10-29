"""A connect call given no url dials ``DEFAULT_ENDPOINT`` rooted at its token's root claim.

"Token-rooted endpoint" in ``spec/behavior.md``. The cases come from
``spec/vectors/token-rooted-endpoint.json`` and run in every SDK.
"""

import asyncio
import base64
import inspect
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from core_helpers.fake_transport import FakeTransport, create_fake_transport
from core_helpers.waiting import wait_for

from caprid_relay import (
    ConnectingTransport,
    RelayCredential,
    SupervisionBackoff,
    connect_relay_publisher,
    connect_relay_subscriber,
    connect_supervised_publisher,
    connect_supervised_subscriber,
)

REPO = Path(__file__).resolve().parents[4]
VECTORS = json.loads((REPO / "spec" / "vectors" / "token-rooted-endpoint.json").read_text())
ERROR_CLASS: dict[str, type[Exception]] = {"type": TypeError, "range": ValueError}
FAST = SupervisionBackoff(initial_delay=0, max_delay=0)
Case = dict[str, Any]


def _b64url(text: str) -> str:
    return base64.urlsafe_b64encode(text.encode()).decode().rstrip("=")


def _jwt(payload_text: str) -> str:
    return f"{_b64url(json.dumps({'alg': 'HS256', 'typ': 'JWT'}))}.{_b64url(payload_text)}.sig"


def _rooted(root: str, **claims: object) -> str:
    return _jwt(json.dumps({"root": root, **claims}))


def case_token(case: Case) -> str | None:
    if "payloadText" in case:
        return _jwt(case["payloadText"])
    if "payload" in case:
        return _jwt(json.dumps(case["payload"]))
    return case["token"]


def case_id(case: Case) -> str:
    return case["name"]


class Relay:
    """A ``create_transport`` that records each URL it dials and completes SETUP on a fresh fake transport."""

    def __init__(self) -> None:
        self.urls: list[str] = []
        self.transports: list[FakeTransport] = []

    def __call__(self, url: str, protocols: Sequence[str]) -> ConnectingTransport:
        fake = create_fake_transport()
        self.urls.append(url)
        self.transports.append(fake)
        fake.send_server_setup()
        ready: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        ready.set_result(None)
        return ConnectingTransport(transport=fake, ready=ready)


@pytest.fixture
async def cleanup() -> AsyncIterator[list[Callable[[], object]]]:
    closers: list[Callable[[], object]] = []
    yield closers
    for close in closers:
        result = close()
        if inspect.isawaitable(result):
            await result


@pytest.mark.parametrize("case", [case for case in VECTORS["cases"] if "error" not in case], ids=case_id)
async def test_vector_dials_the_expected_endpoint(case: Case, cleanup: list[Callable[[], object]]) -> None:
    token = case_token(case)
    relay = Relay()
    subscriber = await connect_relay_subscriber(get_token=lambda: token, create_transport=relay)
    cleanup.append(subscriber.close)

    assert relay.urls == [f"{case['expected']}?jwt={token}"]


@pytest.mark.parametrize("case", [case for case in VECTORS["cases"] if "error" in case], ids=case_id)
async def test_vector_is_refused_before_dialing(case: Case) -> None:
    token = case_token(case)
    relay = Relay()

    with pytest.raises(ERROR_CLASS[case["error"]], match="pass url"):
        await connect_relay_subscriber(get_token=lambda: token, create_transport=relay)
    assert relay.urls == []


TOKEN = _rooted("acme/rooms/42")
EXPECTED = f"https://relay.mux.global/acme/rooms/42?jwt={TOKEN}"


@pytest.mark.parametrize(
    "connect",
    [
        lambda relay: connect_relay_subscriber(get_token=lambda: TOKEN, create_transport=relay),
        lambda relay: connect_relay_publisher(namespace=["p"], get_token=lambda: TOKEN, create_transport=relay),
        lambda relay: connect_supervised_subscriber(get_token=lambda: TOKEN, create_transport=relay),
        lambda relay: connect_supervised_publisher(namespace=["p"], get_token=lambda: TOKEN, create_transport=relay),
    ],
    ids=[
        "connect_relay_subscriber",
        "connect_relay_publisher",
        "connect_supervised_subscriber",
        "connect_supervised_publisher",
    ],
)
async def test_every_connect_call_dials_the_hosted_relay_rooted_at_the_token(
    connect: Callable[[Relay], Awaitable[Any]], cleanup: list[Callable[[], object]]
) -> None:
    relay = Relay()
    connection = await connect(relay)
    cleanup.append(connection.close)

    assert relay.urls == [EXPECTED]


async def test_roots_the_endpoint_at_a_relay_credentials_token(cleanup: list[Callable[[], object]]) -> None:
    relay = Relay()
    expires_at = datetime(2030, 1, 1, tzinfo=UTC)

    async def get_token() -> RelayCredential:
        return RelayCredential(TOKEN, expires_at)

    subscriber = await connect_relay_subscriber(get_token=get_token, create_transport=relay)
    cleanup.append(subscriber.close)

    assert relay.urls == [EXPECTED]
    assert subscriber.expires_at == expires_at


async def test_still_reads_the_tokens_expiry(cleanup: list[Callable[[], object]]) -> None:
    subscriber = await connect_relay_subscriber(
        get_token=lambda: _rooted("acme", exp=1_900_000_000), create_transport=Relay()
    )
    cleanup.append(subscriber.close)

    assert subscriber.expires_at == datetime.fromtimestamp(1_900_000_000, UTC)


async def test_needs_get_token_and_says_so_before_anything_else_runs() -> None:
    relay = Relay()

    with pytest.raises(TypeError, match=r"no get_token.*pass url"):
        await connect_relay_subscriber(create_transport=relay)
    assert relay.urls == []


async def test_a_supervisor_whose_first_token_names_no_root_raises_without_dialing() -> None:
    relay = Relay()

    with pytest.raises(TypeError):
        await connect_supervised_subscriber(get_token=lambda: "opaque", create_transport=relay)
    with pytest.raises(ValueError):
        await connect_supervised_publisher(namespace=["p"], get_token=lambda: _rooted("a b"), create_transport=relay)
    assert relay.urls == []


def _provider() -> Callable[[], str]:
    """Hands out a token rooted at ``acme``, then one rooted elsewhere on every later call."""
    calls = 0

    def get_token() -> str:
        nonlocal calls
        calls += 1
        return FIRST if calls == 1 else MOVED

    return get_token


FIRST = _rooted("acme")
MOVED = _rooted("elsewhere")


@pytest.mark.parametrize(
    "connect",
    [
        lambda relay: connect_supervised_subscriber(get_token=_provider(), create_transport=relay, reconnect=FAST),
        lambda relay: connect_supervised_publisher(
            namespace=["p"], get_token=_provider(), create_transport=relay, reconnect=FAST
        ),
    ],
    ids=["subscriber", "publisher"],
)
async def test_a_supervisor_keeps_its_first_endpoint_across_a_reconnect_and_a_rotation(
    connect: Callable[[Relay], Awaitable[Any]], cleanup: list[Callable[[], object]]
) -> None:
    relay = Relay()
    supervised = await connect(relay)
    cleanup.append(supervised.close)

    relay.transports[0].drop_transport()
    await wait_for(lambda: len(relay.urls) == 2 and supervised.status == "live")
    supervised.rotate()
    await wait_for(lambda: len(relay.urls) == 3)

    assert relay.urls == [
        f"https://relay.mux.global/acme?jwt={FIRST}",
        f"https://relay.mux.global/acme?jwt={MOVED}",
        f"https://relay.mux.global/acme?jwt={MOVED}",
    ]


async def test_a_payload_nested_past_the_recursion_limit_has_no_claims(cleanup: list[Callable[[], object]]) -> None:
    deep = _jwt("[" * 100_000)
    with pytest.raises(TypeError, match="pass url"):
        await connect_relay_subscriber(get_token=lambda: deep, create_transport=Relay())

    relay = Relay()
    expires_at = datetime(2030, 1, 1, tzinfo=UTC)
    subscriber = await connect_relay_subscriber(
        "https://relay.example/acme",
        get_token=lambda: RelayCredential(deep, expires_at),
        create_transport=relay,
    )
    cleanup.append(subscriber.close)
    assert relay.urls == [f"https://relay.example/acme?jwt={deep}"]
    assert subscriber.expires_at == expires_at
