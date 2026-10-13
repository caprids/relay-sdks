"""Print one Markdown table from the device latency lab's exported JSON files.

    python summarize.py device-latency-*.json

Each row is one run, output and input, with the page's own medians over the clicks that matched. The raw
per-click values stay in the files.
"""

import json
import pathlib
import sys

COLUMNS = [
    ("Browser", None),
    ("Run", "run"),
    ("Output", "output"),
    ("Input", "input"),
    ("n", "n"),
    ("Loopback ms", "loopbackMs"),
    ("Stamp - claim ms", "stampMinusClaimMs"),
    ("outputLatency ms", "outputLatencyMs"),
    ("MSTP - claim ms", "mstpMinusClaimMs"),
    ("Input - reference ms", "primaryMinusReferenceMs"),
]


def cell(value: object, key: str | None) -> str:
    if value is None:
        return "—"
    if key and key.endswith("Ms") and isinstance(value, int | float):
        return f"{value:.1f}"
    return str(value).replace("|", "\\|")


def table(paths: list[pathlib.Path]) -> str:
    lines = [
        "| " + " | ".join(title for title, _ in COLUMNS) + " |",
        "|" + " --- |" * len(COLUMNS),
    ]
    for path in paths:
        export = json.loads(path.read_text())
        for row in export.get("summary", []):
            values = [(export.get("browser"), None)] + [(row.get(key), key) for _, key in COLUMNS[1:]]
            lines.append("| " + " | ".join(cell(v, k) for v, k in values) + " |")
    return "\n".join(lines)


def main() -> None:
    paths = [pathlib.Path(arg) for arg in sys.argv[1:]]
    if not paths:
        sys.exit(__doc__)
    print(table(paths))


if __name__ == "__main__":
    main()
