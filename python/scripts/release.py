#!/usr/bin/env python3
"""Lockstep releases of the Python packages, the counterpart of ``ts/scripts/release.mjs`` (#216, ADR 0003).

Every package under ``packages/`` moves at one version, and each pins the others it depends on exactly. Nothing here
names a package: the names come from the ``pyproject.toml`` files, so a rename touches only them.

    uv run python scripts/release.py version <x.y.z>   set every version and exact pin, then relock
    uv run python scripts/release.py check <x.y.z>     fail unless every package is at <x.y.z> and CHANGELOG.md has it
    uv run python scripts/release.py notes <x.y.z>     print that version's CHANGELOG.md section
    uv run python scripts/release.py smoke <dist dir>  install the built wheels into a fresh venv and import them

Publishing is ``uv publish`` in the release workflow; it uploads ``dist/`` in one call.
"""

from __future__ import annotations

import re
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VERSION = re.compile(r"^\d+\.\d+\.\d+(?:(?:a|b|rc)\d+)?$")


def packages() -> list[tuple[Path, dict]]:
    """Each package's ``pyproject.toml`` path and its parsed ``[project]`` table."""
    paths = sorted(ROOT.glob("packages/*/pyproject.toml"))
    return [(path, tomllib.loads(path.read_text())["project"]) for path in paths]


def require_version(version: str | None) -> str:
    if version is None or not VERSION.match(version):
        raise SystemExit(f"expected a version such as 0.1.0, got {version}")
    return version


def changelog_section(version: str) -> str | None:
    lines = (ROOT / "CHANGELOG.md").read_text().splitlines()
    # A version's heading is `## <version>`, optionally followed by a date.
    starts = [index for index, line in enumerate(lines) if line.split()[:2] == ["##", version]]
    if not starts:
        return None
    start = starts[0]
    end = next((index for index in range(start + 1, len(lines)) if lines[index].startswith("## ")), len(lines))
    return "\n".join(lines[start + 1 : end]).strip()


def set_version(version: str) -> None:
    names = [project["name"] for _, project in packages()]
    for path, _ in packages():
        text = path.read_text()
        text = re.sub(r'(?m)^version = "[^"]*"$', f'version = "{version}"', text, count=1)
        for name in names:
            text = re.sub(rf'"{re.escape(name)}==[^"]*"', f'"{name}=={version}"', text)
        path.write_text(text)
    subprocess.run(["uv", "lock"], cwd=ROOT, check=True)
    print(f'every package is now {version}; add a "## {version}" section to CHANGELOG.md')


def check(version: str) -> None:
    names = {project["name"] for _, project in packages()}
    wrong = [f"{project['name']}=={project['version']}" for _, project in packages() if project["version"] != version]
    if wrong:
        raise SystemExit(f"not at {version}: {', '.join(wrong)}; run `scripts/release.py version {version}`")
    for _, project in packages():
        for requirement in project.get("dependencies", []):
            name = re.split(r"[<>=!~ ;\[]", requirement, maxsplit=1)[0]
            if name in names and requirement != f"{name}=={version}":
                raise SystemExit(f"{project['name']} must pin {name}=={version}, not {requirement}")
    if changelog_section(version) is None:
        raise SystemExit(f'CHANGELOG.md has no "## {version}" section')
    print(f"every package is at {version} with exact pins, and CHANGELOG.md has its section")


def smoke(dist: Path) -> None:
    """Installs the built wheels into a fresh venv, the way an application would, and imports every package.

    The workspace's own packages come only from ``dist`` (they are not on PyPI yet, and a stale upload must not stand
    in for them); their third-party dependencies come from PyPI. Each package must also ship ``py.typed``.
    """
    wheels = sorted(dist.glob("*.whl"))
    names = [project["name"] for _, project in packages()]
    if len(wheels) != len(names):
        raise SystemExit(f"expected one wheel per package ({', '.join(names)}) in {dist}, found {len(wheels)}")
    modules = [
        tomllib.loads(path.read_text()).get("tool", {}).get("uv", {}).get("build-backend", {}).get("module-name")
        or project["name"].replace("-", "_")
        for path, project in packages()
    ]
    with tempfile.TemporaryDirectory(prefix="relay-release-smoke-") as scratch:
        venv = Path(scratch) / "venv"
        subprocess.run(["uv", "venv", "--quiet", str(venv)], check=True)
        python = venv / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
        subprocess.run(
            ["uv", "pip", "install", "--quiet", "--python", str(python), *map(str, wheels)],
            check=True,
        )
        script = (
            "import importlib, importlib.resources, sys\n"
            f"for name in {modules!r}:\n"
            "    module = importlib.import_module(name)\n"
            "    assert importlib.resources.files(name).joinpath('py.typed').is_file(), f'{name} has no py.typed'\n"
            "    assert module.__all__, f'{name} exports nothing'\n"
            "print('imported', ', '.join(" + repr(modules) + "))\n"
        )
        subprocess.run([str(python), "-c", script], check=True, cwd=scratch)
    print(f"release smoke passed for {len(wheels)} wheels")


def main(argv: list[str]) -> None:
    command, argument = [*argv, None, None][:2]
    if command == "version":
        set_version(require_version(argument))
    elif command == "check":
        check(require_version(argument))
    elif command == "notes":
        notes = changelog_section(require_version(argument))
        if notes is None:
            raise SystemExit(f'CHANGELOG.md has no "## {argument}" section')
        print(notes)
    elif command == "smoke":
        smoke(Path(argument or "dist").resolve())
    else:
        raise SystemExit("usage: scripts/release.py version|check|notes <x.y.z> | smoke <dist dir>")


if __name__ == "__main__":
    main(sys.argv[1:])
