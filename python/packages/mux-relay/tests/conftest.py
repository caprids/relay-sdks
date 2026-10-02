"""Holds a conformance case's known failure to the assertion it names (``relay_helpers.conformance_targets``).

``pytest.mark.xfail(raises=...)`` checks only the exception's type, and a case raises ``ContractFailure`` for any
broken expectation, a timeout included. A known failure also names the assertion it expects, and a failure that
does not match it is reported as a failure, so an unrelated regression cannot pass as the known gap.
"""

from __future__ import annotations

import re
from collections.abc import Generator
from typing import Any

import pytest
from relay_helpers.conformance_targets import KNOWN_FAILURE


@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[None]) -> Generator[None, Any, Any]:
    report = yield
    expectation = item.stash.get(KNOWN_FAILURE, None)
    if expectation is None or call.when != "call" or call.excinfo is None or not hasattr(report, "wasxfail"):
        return report
    message = str(call.excinfo.value)
    if not re.search(expectation.match, message):
        del report.wasxfail
        report.outcome = "failed"
        report.longrepr = (
            f"{report.longrepr}\n\nThe known failure ({expectation.reason}) expects a failure matching "
            f"{expectation.match!r}; this one is something else: {message}"
        )
    return report
