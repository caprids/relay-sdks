#!/usr/bin/env python3
"""Fail when a name exported through ``__all__`` has no docstring. Runs as a step of ``python:check``.

For each package below, every exported class and function needs its own docstring, and so does every
public (non-underscore) method and property defined on an exported class. A member that overrides a
documented base-class member inherits that docstring, as it does in the generated reference. A
dataclass without a docstring gets one generated from its signature; that one does not count.
Constants and type aliases are not checked.
"""

from __future__ import annotations

import dataclasses
import importlib
import inspect
import sys
from collections.abc import Iterator

PACKAGES = ("mux_relay", "mux_relay.webtransport", "moq_core", "moq_media")


def _own_doc(obj: object) -> str | None:
    doc = obj.__dict__.get("__doc__") if isinstance(obj, type) else getattr(obj, "__doc__", None)
    if not isinstance(doc, str) or not doc.strip():
        return None
    if isinstance(obj, type) and dataclasses.is_dataclass(obj):
        try:
            generated = obj.__name__ + str(inspect.signature(obj)).replace(" -> None", "")
        except (TypeError, ValueError):
            generated = None
        if doc == generated:
            return None
    return doc


def _member_doc(cls: type, name: str, member: object) -> str | None:
    target = member.fget if isinstance(member, property) else member
    if isinstance(target, (classmethod, staticmethod)):
        target = target.__func__
    doc = getattr(target, "__doc__", None)
    if isinstance(doc, str) and doc.strip():
        return doc
    # An override without its own docstring inherits the base class's, as inspect.getdoc does.
    return inspect.getdoc(getattr(cls, name, None))


def _is_checked_member(member: object) -> bool:
    return isinstance(member, (property, classmethod, staticmethod)) or inspect.isfunction(member)


def _missing(package: str) -> Iterator[str]:
    module = importlib.import_module(package)
    for name in module.__all__:
        obj = getattr(module, name)
        if not (inspect.isclass(obj) or inspect.isfunction(obj)):
            continue
        if _own_doc(obj) is None:
            yield f"{package}.{name}"
        if not inspect.isclass(obj):
            continue
        for member_name, member in vars(obj).items():
            if member_name.startswith("_") or not _is_checked_member(member):
                continue
            if _member_doc(obj, member_name, member) is None:
                yield f"{package}.{name}.{member_name}"


def main() -> int:
    missing = [qualname for package in PACKAGES for qualname in _missing(package)]
    if missing:
        print(f"{len(missing)} exported names have no docstring:", file=sys.stderr)
        for qualname in missing:
            print(f"  {qualname}", file=sys.stderr)
        return 1
    print(f"Docstrings present on every exported name in {', '.join(PACKAGES)}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
