"""Transport-independent MOQT track primitives.

The Python counterpart of ``ts/packages/moq-core``: protocol encoding, sessions, and stream
handling over an abstract transport, with no runtime dependencies and no knowledge of Relay.
The protocol implementation has not been ported yet. ``MOQT_PROTOCOL_ID`` is the draft pin every
SDK shares; ``spec/protocol.json`` is its source of truth and ``tests/test_spec_protocol.py``
checks the two agree.
"""

MOQT_PROTOCOL_ID = "moqt-20"

__all__ = ["MOQT_PROTOCOL_ID"]
