"""Package boundaries, the Python counterpart of ts/scripts/check-boundaries.mjs.

import-linter enforces the import graph in the check task; these tests pin the declared dependency
lists and prove the core imports no transport at runtime.
"""

import subprocess
import sys
import tomllib
from pathlib import Path

import moq_core
import mux_relay

PYTHON_ROOT = Path(__file__).resolve().parents[3]


def dependencies(package: str) -> list[str]:
    pyproject = tomllib.loads((PYTHON_ROOT / "packages" / package / "pyproject.toml").read_text())
    return [
        requirement.split(">")[0].split("<")[0].split("=")[0].strip()
        for requirement in pyproject["project"]["dependencies"]
    ]


def test_the_generic_core_declares_no_runtime_dependencies() -> None:
    assert dependencies("moq-core") == []


def test_the_sdk_depends_only_on_the_core_and_its_transport_library() -> None:
    assert sorted(dependencies("mux-relay")) == ["aioquic", "moq-core"]


def test_importing_the_core_loads_no_transport_or_product_module() -> None:
    script = (
        "import sys, moq_core, moq_core.session, moq_core.publish_session; "
        "loaded = sorted(m for m in sys.modules if m.split('.')[0] in {'aioquic', 'mux_relay', 'cryptography'}); "
        "assert loaded == [], loaded"
    )
    subprocess.run([sys.executable, "-c", script], check=True)


def test_the_sdk_leaves_the_protocol_pin_and_limits_to_the_core() -> None:
    # The constants stay where the protocol lives; test_exports.py pins what the SDK re-exports.
    assert isinstance(moq_core.MOQT_PROTOCOL_ID, str)
    assert isinstance(moq_core.MAX_OBJECT_PAYLOAD_LENGTH, int)
    assert not hasattr(mux_relay, "MOQT_PROTOCOL_ID")
    assert not hasattr(mux_relay, "MAX_OBJECT_PAYLOAD_LENGTH")
    assert callable(mux_relay.connect_relay_subscriber)
    assert callable(mux_relay.connect_relay_publisher)
