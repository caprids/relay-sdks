"""Relay Python SDK.

Owns the Relay product API and authentication convention, built on ``moq_core``. The planned
transport is aioquic's WebTransport support, which lets this SDK reach a relay natively with no
browser. Nothing is implemented yet beyond the package boundary.
"""

from moq_core import MOQT_PROTOCOL_ID

__all__ = ["MOQT_PROTOCOL_ID"]
