"""The Python dialing ends the transport adapter conformance suite runs against (``test_transport_conformance.py``).

A target is what ``ts/conformance/README.md`` calls one ("Targets"), spelled for pytest: its framing, what its peer
and runtime can do, the gaps it skips with their reasons, and the rules it is known to fail, each naming the issue
that closes the gap. ``TARGETS`` lists one factory per dialing end; a dialing end that does not exist yet would be a
target with ``unavailable`` set, so its tests are skipped with that reason rather than missing.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field

import pytest
from aioquic.quic.configuration import QuicConfiguration

from mux_relay.connect_session import ConnectingTransport
from mux_relay.quic import connect_quic
from mux_relay.webtransport import connect_webtransport

IDLE_TIMEOUT = 10.0
"""The dialer's idle timeout: how long the TA-9 connection-loss case waits. aioquic's default is 60 s."""

Dial = Callable[[str, Sequence[str], QuicConfiguration], ConnectingTransport]
"""Start a dial to ``(url, protocols)`` with a configuration that trusts the peer's certificate."""


class ErrorShapeFailure(AssertionError):
    """An error lacks the TA-22 shape (``source``, ``stream_error_code``). Python classifies by name until #233."""


class ContractFailure(AssertionError):
    """A case's assertion about the contract failed."""


@dataclass(frozen=True, slots=True)
class KnownFailure:
    """A rule the target is known not to meet. Its test is a strict xfail: it goes red once the target passes."""

    reason: str
    """The issue that closes the gap, or the Known deviation in ``spec/transport-adapter.md``."""
    match: str
    """A pattern the failure's message must contain (``re.search``): the assertion the gap trips, and no other."""
    raises: type[BaseException] = ContractFailure
    """What the failure must be. Anything else, or a message ``match`` misses (a timeout, another assertion), is a
    real failure: ``conftest.py`` turns it red."""


KNOWN_FAILURE = pytest.StashKey[KnownFailure]()
"""Where a case's known failure is kept on its test item, for ``conftest.py`` to check the failure against."""


@dataclass(frozen=True, slots=True)
class ConformanceTarget:
    name: str
    framing: str
    """What TA-1 expects the factory to declare: ``webtransport`` or ``native-quic``."""
    peer_framing: str
    """The conformance peer's ``--framing``."""
    host: str
    """The host the target dials; on native QUIC, the TLS server name TA-3 expects."""
    dial: Dial | None
    capabilities: frozenset[str]
    gaps: Mapping[str, str] = field(default_factory=dict)
    """For each capability the target lacks, the reason its cases are skipped."""
    expectations: Mapping[str, KnownFailure] = field(default_factory=dict)
    """Keyed by test function name."""
    unavailable: str | None = None
    """Why the whole target is skipped, when its dialing end does not exist yet."""


PEER_CAPABILITIES = frozenset(
    {"selectProtocol", "holdHandshake", "refuseHandshake", "quicClose", "connectionLoss", "pause", "maxStreams"}
)
"""What the conformance peer can do in either framing (``ts/conformance/README.md``, "Capabilities")."""

_SHAPE = KnownFailure(
    "#233: Python classifies transport errors by name (WebTransportError, StreamStopped), not by a source field "
    "(spec/transport-adapter.md, Python mapping, TA-19, TA-20, TA-22)",
    match=r"expected source '(stream|session)', got",
    raises=ErrorShapeFailure,
)
_PEER_CLOSE = KnownFailure(
    "#232: the WebTransport dialing end raises for a peer close with a nonzero code, which is how it reports "
    "UNAUTHORIZED; native QUIC settles cleanly (#229), and #232 retires this end "
    "(spec/transport-adapter.md, Python mapping, TA-9)",
    match=r"wait_closed: raised WebTransportError\('peer closed the WebTransport session",
)
_ITERATORS_END = KnownFailure(
    "#235: the incoming stream iterators end cleanly however the session ended "
    "(spec/transport-adapter.md, Python mapping, TA-12)",
    match=r"^the incoming uni iterator ended cleanly instead of raising$",
)
_SHAPE_CASES = dict.fromkeys(
    (
        "test_ta_19_a_peer_reset_errors_the_receive_stream_with_the_peers_code",
        "test_ta_19_a_peer_reset_errors_the_receive_stream_while_nothing_reads_it",
        "test_ta_19_a_peer_reset_errors_a_bidirectional_streams_readable",
        "test_ta_20_a_peer_stop_fails_the_send_stream_while_nothing_is_in_flight",
        "test_ta_20_a_peer_stop_fails_a_write_blocked_on_flow_control",
        "test_ta_20_a_peer_stop_on_a_bidirectional_stream_fails_its_writer",
        "test_ta_22_a_session_error_has_source_session",
    ),
    _SHAPE,
)
"""The cases that check the TA-22 shape, on every target until #233."""


def webtransport_target() -> ConformanceTarget:
    """The WebTransport dialing end, ``connect_webtransport``, against the peer's default framing. #232 retires it."""
    return ConformanceTarget(
        name="webtransport",
        framing="webtransport",
        peer_framing="webtransport",
        host="127.0.0.1",
        dial=lambda url, protocols, configuration: connect_webtransport(url, protocols, configuration=configuration),
        capabilities=PEER_CAPABILITIES | {"webtransport"},
        gaps={
            "native-quic": "a WebTransport session is not native QUIC",
            "largeCodes": "only native QUIC carries codes above 2^32-1",
        },
        expectations={
            "test_ta_7_ready_rejects_when_the_peer_selects_no_protocol": KnownFailure(
                "#232: a WebTransport response that names no protocol is accepted, with protocol_id None; native "
                "QUIC fails the dial, and #232 retires this end (spec/transport-adapter.md, Python mapping, TA-7)",
                match=r"^ready returned, with protocol None$",
            ),
            "test_ta_9_wait_closed_returns_after_the_peers_application_close": _PEER_CLOSE,
            "test_ta_11_close_before_ready_cancels_the_dial": KnownFailure(
                "#232: close() before ready ends a WebTransport session cleanly, so wait_closed() returns; native "
                "QUIC raises, and #232 retires this end (#234; spec/transport-adapter.md, Python mapping, TA-11)",
                match=r"^wait_closed: returned None, expected it to raise$",
            ),
            "test_ta_12_incoming_stream_iterators_raise_after_a_quic_layer_close": _ITERATORS_END,
            "test_ta_21_creates_raise_promptly_at_the_peers_stream_limit_and_succeed_once_credit_returns": KnownFailure(
                "#232: aioquic queues a WebTransport stream beyond the peer's limit as blocked, so the create "
                "succeeds; native QUIC raises, and #232 retires this end (#236; spec/transport-adapter.md, Python "
                "mapping, TA-21)",
                match=r"^create unidirectional stream 5 of 4: returned .*, expected it to raise$",
            ),
            **_SHAPE_CASES,
        },
    )


def native_quic_target() -> ConformanceTarget:
    """The native QUIC dialing end: ``connect_quic``, against ``--framing native-quic``."""
    return ConformanceTarget(
        name="native-quic",
        framing="native-quic",
        peer_framing="native-quic",
        host="localhost",
        dial=lambda url, protocols, configuration: connect_quic(url, protocols, configuration=configuration),
        capabilities=PEER_CAPABILITIES | {"native-quic", "largeCodes"},
        gaps={"webtransport": "native QUIC carries no WebTransport session"},
        expectations={
            "test_ta_12_incoming_stream_iterators_raise_after_a_quic_layer_close": _ITERATORS_END,
            "test_ta_22_a_code_above_2_53_minus_1_is_reported_as_none": _SHAPE,
            **_SHAPE_CASES,
        },
    )


TARGETS: tuple[Callable[[], ConformanceTarget], ...] = (webtransport_target, native_quic_target)
