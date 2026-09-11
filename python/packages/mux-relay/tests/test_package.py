import moq_core
import mux_relay


def test_reexports_the_shared_protocol_pin() -> None:
    assert mux_relay.MOQT_PROTOCOL_ID == moq_core.MOQT_PROTOCOL_ID
