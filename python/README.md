# Relay Python SDK

The Python SDK in the Relay SDKs repository. The package layout, tooling, and boundary rules are in
place; the protocol implementation has not been ported yet.

## Layout

- `packages/moq-core` (module `moq_core`): transport-independent MOQT primitives. Zero runtime
  dependencies and no knowledge of Relay, mirroring `ts/packages/moq-core`.
- `packages/mux-relay` (module `mux_relay`): the Relay product API and authentication convention,
  depending only on `moq_core`. The planned transport is aioquic's WebTransport support, so this SDK
  runs natively with no browser.

## Development

Tool versions come from the repository's root `mise.toml`.

```sh
mise install              # once
mise run python:check     # ruff check, ruff format --check, lint-imports, pytest
```

From this directory under `mise exec`, the native commands are `uv sync`, `uv run pytest`,
`uv run ruff check .`, and `uv run lint-imports`. Run `uv lock` after changing a dependency and
commit `uv.lock`.

## Boundaries

import-linter forbids `moq_core` from importing `mux_relay`; the contract is in `pyproject.toml`.
`moq_core`'s own `pyproject.toml` declares no dependencies. When a transport is added it belongs in
`mux_relay`, behind an abstract interface declared in `moq_core`, matching the TypeScript
`createTransport` seam.

## Conformance

`packages/moq-core/tests/test_spec_protocol.py` checks `moq_core.MOQT_PROTOCOL_ID` against
`spec/protocol.json`. As the protocol layer is ported, its tests should consume `spec/vectors/`,
and the live smoke test should follow the assertion list in `spec/behavior.md`.
