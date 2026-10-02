"""The transport adapter conformance suite (``spec/transport-adapter.md``, "Conformance"), ported to pytest.

It is the counterpart of ``ts/conformance/suite.js``, written once against :class:`moq_core.transport.MoqtTransport`
and run against every Python dialing end in ``relay_helpers.conformance_targets``. The accepting end is the
out-of-process ``relay_helpers/conformance_peer.py``, driven through the JSON-lines protocol the TypeScript suite
uses (``relay_helpers.conformance_client``). Each test is named after the rule it checks.

The Python seam spells some members differently, as the spec's "Python mapping" records, and the cases follow it:

- ``closed`` is ``wait_closed()``: it returns after a clean end and raises otherwise, with no code to return.
- A send stream's ``writer.closed`` is ``wait_acknowledged()``, which raises once the stream can no longer be
  acknowledged (reset, stopped, or the session ended), whether or not it was closed. TA-15's close is split: ``close()``
  returns once the FIN is handed over, and ``wait_acknowledged()`` returns on acknowledgement.
- An incoming readable is an async iterator, which ends with ``StopAsyncIteration``.
- A reason carries its code as ``WebTransportError.code``, a ``stream_error_code`` attribute or an integer (TA-17).
- Errors are classified by name (``WebTransportError``, ``StreamStopped``), not shape. The cases check that mapping,
  and the TA-19, TA-20 and TA-22 cases check the TA-22 shape on top of it: known failures until #233.

TA-6 binds the core (its SETUP tests cover it), and TA-25 and TA-26 do not apply to asyncio: those tests are skipped
with their reasons. A known failure is a strict xfail that must fail with the expected assertion, so it goes red
once the target passes or once it fails some other way.
"""

from __future__ import annotations

import asyncio
import contextlib
import math
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterator, Sequence
from dataclasses import dataclass, field
from typing import Any, TypeVar

import pytest
from aioquic.quic.configuration import QuicConfiguration
from relay_helpers.conformance_client import ConformancePeerProcess, RemotePeer
from relay_helpers.conformance_targets import (
    IDLE_TIMEOUT,
    KNOWN_FAILURE,
    TARGETS,
    ConformanceTarget,
    ContractFailure,
    ErrorShapeFailure,
)

from moq_core._tasks import observe
from moq_core.transport import AcknowledgedSendStream, MoqtTransport, ReceiveStream, SendStream
from mux_relay.webtransport import WebTransportError
from mux_relay.webtransport._errors import StreamStopped

MOQT_PROTOCOL = "moqt-20"

PROMPTLY = 2.0
"""Seconds a case may wait for something that must happen promptly."""
STAYS_PENDING = 0.3
"""Seconds a case watches for something that must not happen."""

T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])

# ── Marking cases ─────────────────────────────────────────────────────────────────────────────────────────────────


def needs(*capabilities: str) -> Callable[[F], F]:
    """The capabilities a case needs from its target; a target without one skips it with the gap's reason."""

    def mark(test: F) -> F:
        test.conformance_needs = capabilities  # ty: ignore[unresolved-attribute]
        return test

    return mark


def session(*, handshake: str | None = None, select: str | None = None, max_streams: int | None = None) -> Any:
    """How the peer answers the case's dial: the ``prepare`` options."""

    def mark(test: F) -> F:
        test.conformance_session = {  # ty: ignore[unresolved-attribute]
            "handshake": handshake,
            "select": select,
            "maxStreams": max_streams,
        }
        return test

    return mark


@contextlib.contextmanager
def should_be_met() -> Iterator[None]:
    """A SHOULD: a failure is reported as a skip naming what went wrong, not as a failed test."""
    try:
        yield
    except ContractFailure as failure:
        pytest.skip(f"SHOULD unmet: {failure}")


# ── Settling ──────────────────────────────────────────────────────────────────────────────────────────────────────


def check(condition: object, message: str) -> None:
    if not condition:
        raise ContractFailure(message)


def later(awaitable: Awaitable[T]) -> asyncio.Future[T]:
    """Start ``awaitable`` now, to assert on later; a failure nobody reads is not reported as unretrieved."""
    future = asyncio.ensure_future(_settled(awaitable))
    observe(future)
    return future


async def _settled(awaitable: Awaitable[T]) -> T:
    # A task cannot finish with StopAsyncIteration's sibling StopIteration; carry both as plain errors.
    try:
        return await awaitable
    except StopAsyncIteration as error:
        raise _Ended("the iterator ended") from error


class _Ended(Exception):
    """An incoming-stream iterator ended (``StopAsyncIteration``)."""


@dataclass
class Outcome:
    state: str
    """``fulfilled``, ``rejected`` or ``pending``."""
    value: Any = None
    error: BaseException | None = None


async def settle_within(future: asyncio.Future[Any], seconds: float) -> Outcome:
    """Where ``future`` stands after ``seconds``, leaving it running."""
    await asyncio.wait({future}, timeout=seconds)
    if not future.done():
        return Outcome("pending")
    if future.cancelled():
        return Outcome("rejected", error=asyncio.CancelledError())
    error = future.exception()
    return Outcome("rejected", error=error) if error is not None else Outcome("fulfilled", value=future.result())


def _as_future(awaitable: Awaitable[T]) -> asyncio.Future[T]:
    return awaitable if isinstance(awaitable, asyncio.Future) else later(awaitable)


async def resolves(awaitable: Awaitable[T], what: str, seconds: float = PROMPTLY) -> T:
    outcome = await settle_within(_as_future(awaitable), seconds)
    check(outcome.state != "pending", f"{what}: still pending after {seconds} s")
    check(outcome.state == "fulfilled", f"{what}: raised {outcome.error!r}")
    return outcome.value


async def rejects(awaitable: Awaitable[Any], what: str, seconds: float = PROMPTLY) -> BaseException:
    outcome = await settle_within(_as_future(awaitable), seconds)
    check(outcome.state != "pending", f"{what}: still pending after {seconds} s")
    check(outcome.state == "rejected", f"{what}: returned {outcome.value!r}, expected it to raise")
    assert outcome.error is not None
    return outcome.error


async def stays_pending(awaitable: Awaitable[Any], what: str, seconds: float = STAYS_PENDING) -> None:
    outcome = await settle_within(_as_future(awaitable), seconds)
    check(outcome.state == "pending", f"{what}: settled ({outcome.state}) within {seconds} s: {outcome}")


# ── Errors ────────────────────────────────────────────────────────────────────────────────────────────────────────


def expect_stream_failure(error: BaseException, code: int, what: str, *, stopped: bool = False) -> None:
    """The Python mapping of a stream error: a ``WebTransportError`` (``StreamStopped`` for a stop) with the code."""
    kind = StreamStopped if stopped else WebTransportError
    check(isinstance(error, kind), f"{what}: expected a {kind.__name__}, got {error!r}")
    check(getattr(error, "code", None) == code, f"{what}: expected code {code}, got {getattr(error, 'code', None)!r}")


def expect_session_failure(error: BaseException, what: str) -> None:
    """The Python mapping of a session error: a ``WebTransportError``."""
    check(isinstance(error, WebTransportError), f"{what}: expected a WebTransportError, got {error!r}")


def expect_stream_error_shape(error: BaseException, code: int | None, what: str) -> None:
    """TA-22: ``source == 'stream'`` and the peer's code as ``stream_error_code`` (``None`` where it is unknown)."""
    if getattr(error, "source", None) != "stream":
        raise ErrorShapeFailure(f"{what}: expected source 'stream', got {error!r}")
    reported = getattr(error, "stream_error_code", "missing")
    if reported not in (code, None):
        raise ErrorShapeFailure(f"{what}: expected stream_error_code {code} (or None for unknown), got {reported!r}")


def expect_session_error_shape(error: BaseException, what: str) -> None:
    """TA-22: ``source == 'session'``."""
    if getattr(error, "source", None) != "session":
        raise ErrorShapeFailure(f"{what}: expected source 'session', got {error!r}")


class CodeReason(Exception):
    """The reason the core hands ``abort()`` or ``cancel()``: the write slot's ``StreamReset`` carries the same."""

    def __init__(self, stream_error_code: object) -> None:
        super().__init__(f"conformance reset {stream_error_code!r}")
        self.stream_error_code = stream_error_code


# ── The target and the peer ───────────────────────────────────────────────────────────────────────────────────────


@dataclass
class Dialed:
    """A dial's factory result, with ``ready`` and ``wait_closed()`` already started."""

    transport: MoqtTransport
    ready: asyncio.Future[None]
    closed: asyncio.Future[None]
    framing: str | None
    url: str


@dataclass
class Ctx:
    target: ConformanceTarget
    peer: RemotePeer
    process: ConformancePeerProcess
    dialed: list[Dialed] = field(default_factory=list)

    def dial(self, path: str = "/conformance", protocols: Sequence[str] = (MOQT_PROTOCOL,)) -> Dialed:
        assert self.target.dial is not None
        url = f"https://{self.target.host}:{self.process.port}{path}"
        configuration = QuicConfiguration(is_client=True, idle_timeout=IDLE_TIMEOUT)
        configuration.load_verify_locations(cadata=self.process.certificate_pem.encode())
        result = self.target.dial(url, list(protocols), configuration)
        dialed = Dialed(
            transport=result.transport,
            ready=later(result.ready),
            closed=later(result.transport.wait_closed()),
            framing=getattr(result, "framing", None),
            url=url,
        )
        self.dialed.append(dialed)
        return dialed

    async def open(self, path: str = "/conformance", protocols: Sequence[str] = (MOQT_PROTOCOL,)) -> MoqtTransport:
        """Dial and wait for ``ready``."""
        dialed = self.dial(path, protocols)
        await resolves(dialed.ready, "ready")
        return dialed.transport

    async def hold_peer(self) -> None:
        """Pause the peer so it acknowledges nothing."""
        if "pause" in self.target.capabilities:
            await self.peer.pause()

    async def release_peer(self) -> None:
        if "pause" in self.target.capabilities:
            await self.peer.resume()


@pytest.fixture(scope="session")
def conformance_peers() -> Iterator[dict[str, ConformancePeerProcess]]:
    """One accepting end per framing, started on first use and shared by every case."""
    peers: dict[str, ConformancePeerProcess] = {}
    yield peers
    for peer in peers.values():
        peer.stop()


@pytest.fixture(params=TARGETS, ids=lambda factory: factory().name)
def target(request: pytest.FixtureRequest) -> ConformanceTarget:
    built: ConformanceTarget = request.param()
    if built.unavailable is not None:
        pytest.skip(built.unavailable)
    return built


@pytest.fixture
async def ctx(
    request: pytest.FixtureRequest, target: ConformanceTarget, conformance_peers: dict[str, ConformancePeerProcess]
) -> AsyncIterator[Ctx]:
    test = request.function
    missing = [need for need in getattr(test, "conformance_needs", ()) if need not in target.capabilities]
    if missing:
        pytest.skip("; ".join(target.gaps.get(need, f"no {need} on this target") for need in missing))
    expectation = target.expectations.get(test.__name__)
    if expectation is not None:
        request.applymarker(pytest.mark.xfail(reason=expectation.reason, raises=expectation.raises, strict=True))
        request.node.stash[KNOWN_FAILURE] = expectation

    process = conformance_peers.get(target.peer_framing)
    if process is None:
        process = conformance_peers[target.peer_framing] = ConformancePeerProcess(target.peer_framing)
    await process.call("prepare", **getattr(test, "conformance_session", {}))
    context = Ctx(target=target, peer=RemotePeer(process), process=process)
    try:
        yield context
    finally:
        await process.call("dispose")
        for dialed in context.dialed:
            dialed.transport.close()
        if context.dialed:
            # Bounded by the close drain (5 s): the peer has already closed, so it is normally at once.
            await asyncio.wait({dialed.closed for dialed in context.dialed}, timeout=6)


# ── Stream helpers ────────────────────────────────────────────────────────────────────────────────────────────────


async def next_incoming(streams: AsyncIterable[T], what: str) -> T:
    """The next stream the peer opened."""
    return await resolves(anext(aiter(streams)), what)


async def read_to_end(stream: ReceiveStream, what: str = "read") -> bytes:
    out = bytearray()
    while (chunk := await resolves(stream.read(), what)) is not None:
        out += chunk
    return bytes(out)


async def read_bytes(stream: ReceiveStream, count: int) -> bytes:
    out = bytearray()
    while len(out) < count:
        chunk = await resolves(stream.read(), "read")
        check(chunk is not None, f"the stream ended after {len(out)} of {count} bytes")
        assert chunk is not None
        out += chunk
    return bytes(out)


async def open_accepted_uni(ctx: Ctx, transport: MoqtTransport, data: bytes = b"hello") -> tuple[SendStream, int]:
    """Open a unidirectional stream, write ``data`` and have the peer accept it."""
    send = await resolves(transport.create_unidirectional_stream(), "create_unidirectional_stream")
    await resolves(send.write(data), "write")
    stream_id = await resolves(ctx.peer.accept_uni(), "the peer accepting the stream")
    return send, stream_id


async def write_until_blocked(send: SendStream, limit: int = 64 * 1024 * 1024) -> tuple[asyncio.Future[None], int]:
    """Write until a write stays pending: the sign that flow control holds the stream."""
    chunk = bytes(64 * 1024)
    written = 0
    while written < limit:
        write = later(send.write(chunk))
        written += len(chunk)
        if (await settle_within(write, 0.2)).state == "pending":
            return write, written
    raise ContractFailure(f"{limit} bytes were accepted without a write ever staying pending")


def acknowledged(send: SendStream) -> asyncio.Future[None]:
    """The Python ``writer.closed``: settles once the stream is acknowledged, raises once it cannot be."""
    check(isinstance(send, AcknowledgedSendStream), f"{send!r} cannot report acknowledgement (TA-15)")
    assert isinstance(send, AcknowledgedSendStream)
    return later(send.wait_acknowledged())


LOW_CODES = (0, 1, 7, 0x12, 255)
"""Codes a reset or stop must carry: all the MOQT codes the core sends (draft-20 §3.3.4) fit in these."""
HIGH_CODES = (256, 0x1_0000, 0xFFFF_FFFF)
"""The rest of TA-17's range."""
INVALID_CODES: tuple[object, ...] = (-1, 0x1_0000_0000, 1.5, math.nan, "7", None, True)
"""``stream_error_code`` values TA-17 maps to 0."""


async def resets_carry(ctx: Ctx, codes: Sequence[int]) -> None:
    transport = await ctx.open()
    for code in codes:
        send, stream_id = await open_accepted_uni(ctx, transport)
        send.abort(CodeReason(code))
        seen = await resolves(ctx.peer.read(stream_id), "the peer read")
        check(seen["resetCode"] == code, f"the reset code for stream_error_code {code}: got {seen['resetCode']}")


async def stops_carry(ctx: Ctx, codes: Sequence[int]) -> None:
    transport = await ctx.open()
    for code in codes:
        stream_id = await ctx.peer.open_uni()
        await ctx.peer.write(stream_id, b"before")
        receive = await next_incoming(transport.incoming_unidirectional_streams(), "the peer stream")
        await read_bytes(receive, 6)
        receive.cancel(CodeReason(code))
        seen = await resolves(ctx.peer.stopped(stream_id), "the peer seeing the stop")
        check(seen == code, f"the stop code for {code}: got {seen}")
        with contextlib.suppress(Exception):
            await ctx.peer.write(stream_id, b"after")
        check(await resolves(receive.read(), "a read after cancel") is None, "a read after cancel delivered bytes")


# ── Dialing and framing ───────────────────────────────────────────────────────────────────────────────────────────


async def test_ta_1_declares_its_framing_in_the_factory_result(ctx: Ctx) -> None:
    dialed = ctx.dial()
    check(dialed.framing in (None, "webtransport", "native-quic"), f"framing is {dialed.framing!r}")
    framing = dialed.framing or "webtransport"
    check(framing == ctx.target.framing, f"framing: expected {ctx.target.framing!r}, got {framing!r}")
    await resolves(dialed.ready, "ready")


@needs("webtransport")
async def test_ta_2_sends_the_connect_to_the_url_as_given_path_and_query_included(ctx: Ctx) -> None:
    path = "/conformance/ta-2/x%2Fy?jwt=header.payload.signature&other=1"
    await ctx.open(path)
    observed = await ctx.peer.observed()
    check(observed is not None and observed["path"] == path, f"the path the peer saw: {observed!r}")


@needs("native-quic")
async def test_ta_3_connects_to_the_host_as_the_server_name_and_sends_no_path_or_query(ctx: Ctx) -> None:
    await ctx.open("/conformance/ta-3?jwt=secret")
    observed = await ctx.peer.observed()
    assert observed is not None
    check(observed["serverName"] == ctx.target.host, f"the TLS server name: {observed!r}")
    check("path" not in observed, f"a path on the wire: {observed!r}")


async def test_ta_4_accepts_an_https_url(ctx: Ctx) -> None:
    dialed = ctx.dial()
    check(dialed.url.startswith("https://"), f"the target dialled {dialed.url}, not an https URL")
    await resolves(dialed.ready, "ready")


# ── Version negotiation ───────────────────────────────────────────────────────────────────────────────────────────


async def test_ta_5_offers_exactly_the_given_protocols_in_order(ctx: Ctx) -> None:
    protocols = [MOQT_PROTOCOL, "moqt-conformance-b", "moqt-conformance-a"]
    await ctx.open(protocols=protocols)
    observed = await ctx.peer.observed()
    check(observed is not None and observed["protocols"] == protocols, f"the protocols offered: {observed!r}")


def test_ta_6_is_the_cores_rule() -> None:
    pytest.skip("TA-6 binds the core, not the adapter: the core's SETUP tests and spec/vectors cover it")


@needs("holdHandshake")
@session(handshake="hold")
async def test_ta_7_ready_waits_for_the_peer_to_select_a_protocol(ctx: Ctx) -> None:
    dialed = ctx.dial()
    await stays_pending(dialed.ready, "ready while the handshake is held")
    await ctx.peer.release_handshake()
    await resolves(dialed.ready, "ready once the peer answered")


async def _ready_rejects_with_a_session_error(ctx: Ctx) -> None:
    dialed = ctx.dial()
    outcome = await settle_within(dialed.ready, PROMPTLY)
    protocol = getattr(dialed.transport, "protocol_id", None)
    check(outcome.state != "fulfilled", f"ready returned, with protocol {protocol!r}")
    check(outcome.state == "rejected", f"ready still pending after {PROMPTLY} s")
    assert outcome.error is not None
    expect_session_failure(outcome.error, "ready")


@needs("selectProtocol")
@session(select="none")
async def test_ta_7_ready_rejects_when_the_peer_selects_no_protocol(ctx: Ctx) -> None:
    await _ready_rejects_with_a_session_error(ctx)


@needs("selectProtocol")
@session(select="unoffered")
async def test_ta_7_ready_rejects_when_the_peer_selects_a_protocol_that_was_not_offered(ctx: Ctx) -> None:
    await _ready_rejects_with_a_session_error(ctx)


async def test_ta_8_protocol_is_the_selected_identifier_once_ready(ctx: Ctx) -> None:
    transport = await ctx.open(protocols=[MOQT_PROTOCOL, "moqt-conformance-b"])
    protocol = getattr(transport, "protocol_id", None)
    check(protocol == MOQT_PROTOCOL, f"protocol_id: expected {MOQT_PROTOCOL!r}, got {protocol!r}")


# ── Session lifetime ──────────────────────────────────────────────────────────────────────────────────────────────


async def test_ta_9_wait_closed_returns_after_a_local_close(ctx: Ctx) -> None:
    dialed = ctx.dial()
    await resolves(dialed.ready, "ready")
    dialed.transport.close(7, "conformance")
    await resolves(dialed.closed, "wait_closed", 7.0)


async def test_ta_9_wait_closed_returns_after_the_peers_application_close(ctx: Ctx) -> None:
    dialed = ctx.dial()
    await resolves(dialed.ready, "ready")
    await ctx.peer.close(42, "peer done")
    await resolves(dialed.closed, "wait_closed")


@needs("quicClose")
async def test_ta_9_wait_closed_raises_a_session_error_on_a_quic_layer_close(ctx: Ctx) -> None:
    dialed = ctx.dial()
    await resolves(dialed.ready, "ready")
    await ctx.peer.quic_close()
    expect_session_failure(await rejects(dialed.closed, "wait_closed"), "wait_closed")


@needs("connectionLoss")
async def test_ta_9_wait_closed_raises_a_session_error_on_connection_loss(ctx: Ctx) -> None:
    dialed = ctx.dial()
    await resolves(dialed.ready, "ready")
    await ctx.peer.lose_connection()
    expect_session_failure(await rejects(dialed.closed, "wait_closed", IDLE_TIMEOUT * 3), "wait_closed")


@needs("refuseHandshake")
@session(handshake="refuse")
async def test_ta_9_wait_closed_raises_a_session_error_when_the_dial_fails(ctx: Ctx) -> None:
    dialed = ctx.dial()
    await rejects(dialed.ready, "ready")
    expect_session_failure(await rejects(dialed.closed, "wait_closed"), "wait_closed")


async def test_ta_10_close_sends_its_code_and_reason_to_the_peer(ctx: Ctx) -> None:
    transport = await ctx.open()
    transport.close(9, "bye")
    seen = await resolves(ctx.peer.closed(), "the peer seeing the close", 7.0)
    check(seen == {"closeCode": 9, "reason": "bye"}, f"the close the peer saw: {seen!r}")


async def test_ta_10_close_without_a_code_sends_code_0_and_an_empty_reason(ctx: Ctx) -> None:
    dialed = ctx.dial()
    await resolves(dialed.ready, "ready")
    dialed.transport.close()
    await resolves(dialed.closed, "wait_closed", 7.0)
    seen = await resolves(ctx.peer.closed(), "the peer seeing the close")
    check(seen == {"closeCode": 0, "reason": ""}, f"the close the peer saw: {seen!r}")


async def test_ta_10_close_is_idempotent_and_does_not_raise_after_the_session_ended(ctx: Ctx) -> None:
    dialed = ctx.dial()
    await resolves(dialed.ready, "ready")
    # Code 0: a nonzero code is TA-9's known failure on the WebTransport dialing end, and would end the case here.
    await ctx.peer.close(0, "peer first")
    await resolves(dialed.closed, "wait_closed")
    dialed.transport.close(4, "late")
    dialed.transport.close()
    await resolves(dialed.transport.wait_closed(), "wait_closed after the late close calls")


@needs("holdHandshake")
@session(handshake="hold")
async def test_ta_11_close_before_ready_cancels_the_dial(ctx: Ctx) -> None:
    dialed = ctx.dial()
    await stays_pending(dialed.ready, "ready while the handshake is held")
    dialed.transport.close(1, "deadline")
    expect_session_failure(await rejects(dialed.ready, "ready"), "ready")
    expect_session_failure(await rejects(dialed.closed, "wait_closed"), "wait_closed")
    await ctx.peer.release_handshake()
    await rejects(dialed.transport.create_unidirectional_stream(), "create_unidirectional_stream after the cancel")


async def test_ta_12_creates_raise_once_the_session_has_ended(ctx: Ctx) -> None:
    dialed = ctx.dial()
    await resolves(dialed.ready, "ready")
    dialed.transport.close()
    await resolves(dialed.closed, "wait_closed", 7.0)
    await rejects(dialed.transport.create_unidirectional_stream(), "create_unidirectional_stream")
    await rejects(dialed.transport.create_bidirectional_stream(), "create_bidirectional_stream")


async def test_ta_12_pending_stream_operations_raise_a_session_error_on_the_peers_close(ctx: Ctx) -> None:
    transport = await ctx.open()
    send, _ = await open_accepted_uni(ctx, transport)
    stream_id = await ctx.peer.open_uni()
    await ctx.peer.write(stream_id, b"x")
    receive = await next_incoming(transport.incoming_unidirectional_streams(), "the peer stream")
    await read_bytes(receive, 1)
    read = later(receive.read())
    sent = acknowledged(send)
    await ctx.peer.close(5, "gone")
    expect_session_failure(await rejects(read, "the pending read"), "the pending read")
    expect_session_failure(await rejects(sent, "wait_acknowledged"), "wait_acknowledged")
    expect_session_failure(await rejects(send.write(b"late"), "a write after the close"), "a write after the close")


@needs("quicClose")
async def test_ta_12_pending_stream_operations_raise_a_session_error_on_a_quic_layer_close(ctx: Ctx) -> None:
    transport = await ctx.open()
    send, _ = await open_accepted_uni(ctx, transport)
    sent = acknowledged(send)
    await ctx.peer.quic_close()
    expect_session_failure(await rejects(sent, "wait_acknowledged"), "wait_acknowledged")


async def test_ta_12_incoming_stream_iterators_end_after_a_clean_end(ctx: Ctx) -> None:
    transport = await ctx.open()
    uni = later(anext(aiter(transport.incoming_unidirectional_streams())))
    bidi = later(anext(aiter(transport.incoming_bidirectional_streams())))
    await ctx.peer.close(0, "")
    for name, pending in (("uni", uni), ("bidi", bidi)):
        error = await rejects(pending, f"the incoming {name} iterator")
        check(isinstance(error, _Ended), f"the incoming {name} iterator raised {error!r} instead of ending")


@needs("quicClose")
async def test_ta_12_incoming_stream_iterators_raise_after_a_quic_layer_close(ctx: Ctx) -> None:
    transport = await ctx.open()
    uni = later(anext(aiter(transport.incoming_unidirectional_streams())))
    bidi = later(anext(aiter(transport.incoming_bidirectional_streams())))
    await ctx.peer.quic_close()
    for name, pending in (("uni", uni), ("bidi", bidi)):
        error = await rejects(pending, f"the incoming {name} iterator")
        check(not isinstance(error, _Ended), f"the incoming {name} iterator ended cleanly instead of raising")


# ── Streams ───────────────────────────────────────────────────────────────────────────────────────────────────────


async def test_ta_13_peer_streams_arrive_in_order_bytes_in_order_fin_after_the_bytes(ctx: Ctx) -> None:
    transport = await ctx.open()
    labels = ("first", "second", "third")
    for label in labels:
        stream_id = await ctx.peer.open_uni()
        await ctx.peer.write(stream_id, f"{label}:".encode())
        await ctx.peer.write(stream_id, b"a" * 5000)
        await ctx.peer.finish(stream_id)
    for label in labels:
        receive = await next_incoming(transport.incoming_unidirectional_streams(), f"the {label} stream")
        got = await read_to_end(receive)
        check(got == f"{label}:".encode() + b"a" * 5000, f"the {label} stream: got {got[:16]!r}…")


async def test_ta_13_bidirectional_streams_carry_bytes_and_fin_both_ways(ctx: Ctx) -> None:
    transport = await ctx.open()
    outgoing = await resolves(transport.create_bidirectional_stream(), "create_bidirectional_stream")
    await resolves(outgoing.writable.write(b"ping"), "write")
    await resolves(outgoing.writable.close(), "close")
    stream_id = await resolves(ctx.peer.accept_bi(), "the peer accepting the stream")
    seen = await resolves(ctx.peer.read(stream_id), "the peer read")
    check((seen["bytes"], seen["fin"], seen["resetCode"]) == (list(b"ping"), True, None), f"what the peer read: {seen}")
    await ctx.peer.write(stream_id, b"pong")
    await ctx.peer.finish(stream_id)
    check(await read_to_end(outgoing.readable) == b"pong", "the reply")

    peer_opened = await ctx.peer.open_bi()
    await ctx.peer.write(peer_opened, b"hello")
    await ctx.peer.finish(peer_opened)
    incoming = await next_incoming(transport.incoming_bidirectional_streams(), "the peer bidi stream")
    check(await read_to_end(incoming.readable) == b"hello", "the peer bidi stream")


async def test_ta_14_a_write_returns_before_the_peer_has_read_the_bytes(ctx: Ctx) -> None:
    transport = await ctx.open()
    send = await resolves(transport.create_unidirectional_stream(), "create_unidirectional_stream")
    await ctx.hold_peer()
    await resolves(send.write(b"accepted, not acknowledged"), "a write the peer has not read")
    await ctx.release_peer()


async def test_ta_14_a_write_stays_pending_while_flow_control_holds_the_stream(ctx: Ctx) -> None:
    transport = await ctx.open()
    send, stream_id = await open_accepted_uni(ctx, transport, b"x")
    await ctx.hold_peer()
    write, written = await write_until_blocked(send)
    await stays_pending(write, "the blocked write")
    await ctx.release_peer()
    drained = later(ctx.peer.read(stream_id, count=written + 1, discard=True))
    await resolves(write, "the blocked write once the peer reads", 20.0)
    await resolves(drained, "the peer reading everything", 20.0)


@needs("pause")
async def test_ta_15_close_is_acknowledged_only_once_the_peer_has_acknowledged_the_fin(ctx: Ctx) -> None:
    transport = await ctx.open()
    send, _ = await open_accepted_uni(ctx, transport)
    await ctx.peer.pause()
    await resolves(send.close(), "close, which returns once the FIN is handed over")
    sent = acknowledged(send)
    await stays_pending(sent, "wait_acknowledged while the peer acknowledges nothing", 1.0)
    await ctx.peer.resume()
    await resolves(sent, "wait_acknowledged once the peer resumed", 5.0)


@needs("pause")
async def test_ta_15_close_fails_when_the_peer_stops_the_stream_before_acknowledging_it(ctx: Ctx) -> None:
    transport = await ctx.open()
    send, stream_id = await open_accepted_uni(ctx, transport)
    await ctx.peer.pause()
    await resolves(send.close(), "close")
    sent = acknowledged(send)
    await stays_pending(sent, "wait_acknowledged while the peer acknowledges nothing")
    await ctx.peer.stop(stream_id, 3)
    await ctx.peer.resume()
    await rejects(sent, "wait_acknowledged", 5.0)


@needs("pause")
async def test_ta_15_close_fails_when_the_session_ends_before_the_fin_is_acknowledged(ctx: Ctx) -> None:
    transport = await ctx.open()
    send, _ = await open_accepted_uni(ctx, transport)
    await ctx.peer.pause()
    await resolves(send.close(), "close")
    sent = acknowledged(send)
    await stays_pending(sent, "wait_acknowledged while the peer acknowledges nothing")
    await ctx.peer.close(0, "")
    await rejects(sent, "wait_acknowledged", 5.0)


async def test_ta_16_abort_resets_the_stream_with_the_code(ctx: Ctx) -> None:
    transport = await ctx.open()
    send, stream_id = await open_accepted_uni(ctx, transport)
    send.abort(CodeReason(9))
    await rejects(acknowledged(send), "wait_acknowledged")
    seen = await resolves(ctx.peer.read(stream_id), "the peer read")
    check(seen["resetCode"] == 9, f"the reset code the peer saw: {seen}")


async def test_ta_16_abort_resets_at_once_while_a_write_is_blocked_on_flow_control(ctx: Ctx) -> None:
    transport = await ctx.open()
    send, stream_id = await open_accepted_uni(ctx, transport, b"x")
    await ctx.hold_peer()
    write, _ = await write_until_blocked(send)
    send.abort(CodeReason(4))
    await rejects(write, "the blocked write")
    await rejects(acknowledged(send), "wait_acknowledged")
    await ctx.release_peer()
    seen = await resolves(ctx.peer.read(stream_id, discard=True), "the peer read", 10.0)
    check(seen["resetCode"] == 4, f"the reset code the peer saw: {seen}")


@needs("pause")
async def test_ta_16_abort_resets_while_close_waits_for_acknowledgement(ctx: Ctx) -> None:
    transport = await ctx.open()
    send, stream_id = await open_accepted_uni(ctx, transport)
    await ctx.peer.pause()
    await resolves(send.close(), "close")
    sent = acknowledged(send)
    await stays_pending(sent, "wait_acknowledged while the peer acknowledges nothing")
    send.abort(CodeReason(6))
    await rejects(sent, "the pending wait_acknowledged")
    await ctx.peer.resume()
    seen = await resolves(ctx.peer.read(stream_id), "the peer read", 5.0)
    # The peer dropped the FIN: it was paused before close(). A reset sender may not send it again (RFC 9000 §3.1,
    # §3.3), so a FIN here means the adapter let it win over the abort.
    check(seen["resetCode"] == 6 and not seen["fin"], f"the peer did not see only the reset: {seen}")


async def test_ta_17_a_reset_carries_the_reasons_stream_error_code_up_to_255(ctx: Ctx) -> None:
    await resets_carry(ctx, LOW_CODES)


async def test_ta_17_a_reset_carries_the_reasons_stream_error_code_above_255(ctx: Ctx) -> None:
    await resets_carry(ctx, HIGH_CODES)


async def test_ta_17_a_reset_carries_the_code_of_a_webtransport_error_or_an_integer_reason(ctx: Ctx) -> None:
    transport = await ctx.open()
    reasons: list[tuple[object, int]] = [
        (WebTransportError("reset", code=0x12), 0x12),
        (0xFFFF_FFFF, 0xFFFF_FFFF),
        (3, 3),
    ]
    for reason, code in reasons:
        send, stream_id = await open_accepted_uni(ctx, transport)
        send.abort(reason)
        seen = await resolves(ctx.peer.read(stream_id), "the peer read")
        check(seen["resetCode"] == code, f"the reset code for the reason {reason!r}: got {seen['resetCode']}")


async def test_ta_17_a_reset_carries_0_when_the_reason_has_no_valid_code(ctx: Ctx) -> None:
    transport = await ctx.open()
    reasons: list[object] = [None, "a string", -1, 0x1_0000_0000, True, *map(CodeReason, INVALID_CODES)]
    for reason in reasons:
        send, stream_id = await open_accepted_uni(ctx, transport)
        send.abort(reason)
        seen = await resolves(ctx.peer.read(stream_id), "the peer read")
        check(seen["resetCode"] == 0, f"the reset code for the reason {reason!r}: got {seen['resetCode']}")


async def test_ta_18_cancel_sends_stop_sending_with_the_code_up_to_255_and_ends_the_stream(ctx: Ctx) -> None:
    await stops_carry(ctx, LOW_CODES)


async def test_ta_18_cancel_sends_stop_sending_with_the_code_above_255(ctx: Ctx) -> None:
    await stops_carry(ctx, HIGH_CODES)


async def test_ta_18_cancel_sends_code_0_when_the_reason_has_no_valid_code(ctx: Ctx) -> None:
    transport = await ctx.open()
    for reason in (None, CodeReason(-1), CodeReason(0x1_0000_0000)):
        stream_id = await ctx.peer.open_uni()
        await ctx.peer.write(stream_id, b"x")
        receive = await next_incoming(transport.incoming_unidirectional_streams(), "the peer stream")
        receive.cancel(reason)
        seen = await resolves(ctx.peer.stopped(stream_id), "the peer seeing the stop")
        check(seen == 0, f"the stop code for {reason!r}: got {seen}")


async def test_ta_19_a_peer_reset_errors_the_receive_stream_with_the_peers_code(ctx: Ctx) -> None:
    transport = await ctx.open()
    stream_id = await ctx.peer.open_uni()
    await ctx.peer.write(stream_id, b"delivered")
    receive = await next_incoming(transport.incoming_unidirectional_streams(), "the peer stream")
    check(await read_bytes(receive, 9) == b"delivered", "the bytes before the reset")
    await ctx.peer.reset(stream_id, 11)
    error = await rejects(receive.read(), "the read after the reset")
    expect_stream_failure(error, 11, "the read after the reset")
    expect_stream_error_shape(error, 11, "the read after the reset")


async def test_ta_19_a_peer_reset_errors_the_receive_stream_while_nothing_reads_it(ctx: Ctx) -> None:
    transport = await ctx.open()
    stream_id = await ctx.peer.open_uni()
    await ctx.peer.write(stream_id, b"unread")
    receive = await next_incoming(transport.incoming_unidirectional_streams(), "the peer stream")
    await ctx.peer.reset(stream_id, 11)
    await asyncio.sleep(STAYS_PENDING)
    # The unread bytes may be discarded (RFC 9000 §3.2) or still delivered, but the reset must follow them.
    while True:
        outcome = await settle_within(later(receive.read()), PROMPTLY)
        check(outcome.state != "pending", f"a read after the reset: still pending after {PROMPTLY} s")
        if outcome.state == "rejected":
            break
        check(outcome.value is not None, "the stream ended cleanly after the reset")
    assert outcome.error is not None
    expect_stream_failure(outcome.error, 11, "the read after the reset")
    expect_stream_error_shape(outcome.error, 11, "the read after the reset")


async def test_ta_19_a_peer_reset_errors_a_bidirectional_streams_readable(ctx: Ctx) -> None:
    transport = await ctx.open()
    stream = await resolves(transport.create_bidirectional_stream(), "create_bidirectional_stream")
    await resolves(stream.writable.write(b"open"), "write")
    stream_id = await resolves(ctx.peer.accept_bi(), "the peer accepting the stream")
    await ctx.peer.reset(stream_id, 0xFFFF_FFFF)
    error = await rejects(stream.readable.read(), "the read after the reset")
    expect_stream_failure(error, 0xFFFF_FFFF, "the read after the reset")
    expect_stream_error_shape(error, 0xFFFF_FFFF, "the read after the reset")


async def test_ta_20_a_peer_stop_fails_the_send_stream_while_nothing_is_in_flight(ctx: Ctx) -> None:
    transport = await ctx.open()
    send, stream_id = await open_accepted_uni(ctx, transport)
    sent = acknowledged(send)
    await ctx.peer.stop(stream_id, 13)
    error = await rejects(sent, "wait_acknowledged")
    expect_stream_failure(error, 13, "wait_acknowledged", stopped=True)
    late = await rejects(send.write(b"late"), "the next write")
    expect_stream_failure(late, 13, "the next write", stopped=True)
    expect_stream_error_shape(error, 13, "wait_acknowledged")
    expect_stream_error_shape(late, 13, "the next write")


async def test_ta_20_a_peer_stop_fails_a_write_blocked_on_flow_control(ctx: Ctx) -> None:
    transport = await ctx.open()
    send, stream_id = await open_accepted_uni(ctx, transport, b"x")
    await ctx.hold_peer()
    write, _ = await write_until_blocked(send)
    sent = acknowledged(send)
    await ctx.peer.stop(stream_id, 5)
    error = await rejects(write, "the blocked write")
    expect_stream_failure(error, 5, "the blocked write", stopped=True)
    closed = await rejects(sent, "wait_acknowledged")
    expect_stream_failure(closed, 5, "wait_acknowledged", stopped=True)
    await ctx.release_peer()
    expect_stream_error_shape(error, 5, "the blocked write")


async def test_ta_20_a_peer_stop_on_a_bidirectional_stream_fails_its_writer(ctx: Ctx) -> None:
    transport = await ctx.open()
    stream = await resolves(transport.create_bidirectional_stream(), "create_bidirectional_stream")
    await resolves(stream.writable.write(b"open"), "write")
    stream_id = await resolves(ctx.peer.accept_bi(), "the peer accepting the stream")
    sent = acknowledged(stream.writable)
    await ctx.peer.stop(stream_id, 2)
    error = await rejects(sent, "wait_acknowledged")
    expect_stream_failure(error, 2, "wait_acknowledged", stopped=True)
    expect_stream_error_shape(error, 2, "wait_acknowledged")


@needs("maxStreams")
@session(max_streams=4)
async def test_ta_21_creates_raise_promptly_at_the_peers_stream_limit_and_succeed_once_credit_returns(
    ctx: Ctx,
) -> None:
    transport = await ctx.open()
    for kind in ("unidirectional", "bidirectional"):

        async def create(kind: str = kind) -> SendStream:
            if kind == "unidirectional":
                return await transport.create_unidirectional_stream()
            return (await transport.create_bidirectional_stream()).writable

        opened: list[SendStream] = []
        for index in range(4):
            send = await resolves(create(), f"create {kind} stream {index + 1} of 4")
            await resolves(send.write(b"x"), f"write on {kind} stream {index + 1} of 4")
            opened.append(send)
        await rejects(create(), f"create {kind} stream 5 of 4")
        await resolves(opened[0].close(), "close")
        stream_id = await (ctx.peer.accept_uni() if kind == "unidirectional" else ctx.peer.accept_bi())
        await ctx.peer.read(stream_id)
        if kind == "bidirectional":
            await ctx.peer.finish(stream_id)
        loop = asyncio.get_running_loop()
        until = loop.time() + 5.0
        while True:
            outcome = await settle_within(later(create()), PROMPTLY)
            if outcome.state != "rejected" or loop.time() >= until:
                break
            await asyncio.sleep(0.01)
        check(outcome.state == "fulfilled", f"a {kind} create after credit returned: {outcome}")
        for send in opened[1:]:
            send.abort(CodeReason(0))


# ── Error shape ───────────────────────────────────────────────────────────────────────────────────────────────────


@needs("native-quic", "largeCodes")
async def test_ta_22_a_code_above_2_53_minus_1_is_reported_as_none(ctx: Ctx) -> None:
    transport = await ctx.open()
    stream_id = await ctx.peer.open_uni()
    await ctx.peer.write(stream_id, b"x")
    receive = await next_incoming(transport.incoming_unidirectional_streams(), "the peer stream")
    await read_bytes(receive, 1)
    await ctx.peer.reset(stream_id, "4611686018427387903")
    error = await rejects(receive.read(), "the read")
    expect_stream_error_shape(error, None, "the read after a 62-bit reset")


async def test_ta_22_a_session_error_has_source_session(ctx: Ctx) -> None:
    transport = await ctx.open()
    send, _ = await open_accepted_uni(ctx, transport)
    sent = acknowledged(send)
    await ctx.peer.close(0, "")
    error = await rejects(sent, "wait_acknowledged")
    expect_session_failure(error, "wait_acknowledged")
    expect_session_error_shape(error, "wait_acknowledged")


async def test_ta_22_stream_and_session_errors_are_exceptions(ctx: Ctx) -> None:
    with should_be_met():
        transport = await ctx.open()
        stream_id = await ctx.peer.open_uni()
        await ctx.peer.write(stream_id, b"x")
        receive = await next_incoming(transport.incoming_unidirectional_streams(), "the peer stream")
        await read_bytes(receive, 1)
        await ctx.peer.reset(stream_id, 1)
        stream_error = await rejects(receive.read(), "the read")
        check(isinstance(stream_error, Exception), f"the stream error is not an Exception: {stream_error!r}")
        send, _ = await open_accepted_uni(ctx, transport)
        sent = acknowledged(send)
        await ctx.peer.close(0, "")
        session_error = await rejects(sent, "wait_acknowledged")
        check(isinstance(session_error, Exception), f"the session error is not an Exception: {session_error!r}")


async def test_ta_23_a_stream_error_leaves_the_session_usable(ctx: Ctx) -> None:
    dialed = ctx.dial()
    await resolves(dialed.ready, "ready")
    transport = dialed.transport
    send, stream_id = await open_accepted_uni(ctx, transport)
    sent = acknowledged(send)
    await ctx.peer.stop(stream_id, 1)
    expect_stream_failure(await rejects(sent, "wait_acknowledged"), 1, "wait_acknowledged", stopped=True)
    await stays_pending(dialed.closed, "wait_closed after a stream error")
    following, following_id = await open_accepted_uni(ctx, transport, b"still open")
    await resolves(following.close(), "close")
    seen = await resolves(ctx.peer.read(following_id), "the peer read")
    check(bytes(seen["bytes"]) == b"still open", f"a stream after the error: {seen}")


async def test_ta_24_a_session_error_after_the_peers_close_carries_its_code_and_reason(ctx: Ctx) -> None:
    with should_be_met():
        transport = await ctx.open()
        send, _ = await open_accepted_uni(ctx, transport)
        sent = acknowledged(send)
        await ctx.peer.close(0x10, "unauthorized")
        error = await rejects(sent, "wait_acknowledged")
        expect_session_failure(error, "wait_acknowledged")
        carried = (getattr(error, "peer_closed", None), getattr(error, "code", None), getattr(error, "reason", None))
        check(carried == (True, 0x10, "unauthorized"), f"(peer_closed, code, reason): {carried!r}")


# ── Process lifetime ──────────────────────────────────────────────────────────────────────────────────────────────


def test_ta_25_and_ta_26_do_not_apply_to_asyncio() -> None:
    pytest.skip(
        "TA-25, TA-26: asyncio holds no process open by itself; the application's own asyncio.run decides "
        "(spec/transport-adapter.md, Python mapping)"
    )
