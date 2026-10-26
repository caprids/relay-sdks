"""Package boundaries, the Python counterpart of ts/scripts/check-boundaries.mjs.

import-linter enforces the import graph in the check task; these tests pin the declared dependency
lists and prove the core imports no transport at runtime.
"""

import ast
import subprocess
import sys
import tomllib
from pathlib import Path

import caprid_relay
import moq_core

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
    assert sorted(dependencies("caprid-relay")) == ["aioquic", "caprid-moq-core"]


def test_importing_the_core_loads_no_transport_or_product_module() -> None:
    script = (
        "import sys, moq_core, moq_core.session, moq_core.publish_session; "
        "loaded = sorted(m for m in sys.modules "
        "if m.split('.')[0] in {'aioquic', 'caprid_relay', 'moq_media', 'cryptography'}); "
        "assert loaded == [], loaded"
    )
    subprocess.run([sys.executable, "-c", script], check=True)


def test_the_sdk_imports_no_http3() -> None:
    # Native QUIC is the only dialing end (ADR 0004); HTTP/3 and WebTransport live in the test helpers (#232).
    # import-linter cannot forbid a subpackage of an external package, so every import statement is read here,
    # the ones inside functions included.
    imported: list[str] = []

    for path in sorted(Path(caprid_relay.__file__).parent.rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text(), str(path))):
            if isinstance(node, ast.Import):
                imported += [f"{path.name}: {alias.name}" for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module is not None and node.level == 0:
                imported.append(f"{path.name}: {node.module}")

    assert any(name.endswith(": aioquic.quic.connection") for name in imported)  # the scan sees aioquic at all
    assert [name for name in imported if name.split(": ")[1].startswith("aioquic.h3")] == []


def test_the_sdk_leaves_the_protocol_pin_and_limits_to_the_core() -> None:
    # The constants stay where the protocol lives; test_exports.py pins what the SDK re-exports.
    assert isinstance(moq_core.MOQT_PROTOCOL_ID, str)
    assert isinstance(moq_core.MAX_OBJECT_PAYLOAD_LENGTH, int)
    assert not hasattr(caprid_relay, "MOQT_PROTOCOL_ID")
    assert not hasattr(caprid_relay, "MAX_OBJECT_PAYLOAD_LENGTH")
    assert callable(caprid_relay.connect_relay_subscriber)
    assert callable(caprid_relay.connect_relay_publisher)


def test_the_media_package_depends_only_on_the_core() -> None:
    assert dependencies("moq-media") == ["caprid-moq-core"]


def test_the_packages_release_in_lockstep_under_one_licence() -> None:
    # Every package releases at one version and pins its siblings exactly (#216, ADR 0003); scripts/release.py
    # moves both.
    projects = [
        tomllib.loads(path.read_text())["project"]
        for path in sorted((PYTHON_ROOT / "packages").glob("*/pyproject.toml"))
    ]
    versions = {project["version"] for project in projects}
    assert len(versions) == 1, versions
    names = {project["name"] for project in projects}
    for project in projects:
        assert project["license"] == "Apache-2.0", project["name"]
        for requirement in project["dependencies"]:
            name = requirement.split("=")[0].split(">")[0].split("<")[0]
            if name in names:
                assert requirement == f"{name}=={project['version']}", (project["name"], requirement)
