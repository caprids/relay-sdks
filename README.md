# Relay SDKs

Client SDKs for Relay, Mux's Media over QUIC (MoQ) relay service, in every language we support.
Each SDK connects to a Relay endpoint, subscribes to tracks, and publishes data, with the same
protocol pin and the same observable behavior. The SDKs share no code. They share a specification.

This is an initial development workspace. Package names are provisional and publication is disabled.

| Directory | Contents | Status |
| --- | --- | --- |
| `spec/` | Protocol pins, the behavior contract every SDK implements, and language-neutral test vectors | Source of truth |
| `ts/` | TypeScript SDK `@mux/relay` and the generic `@mux/moq-core` protocol package, as a pnpm workspace | Working; live-tested against `sjc.relay.mux.global` |
| `python/` | Python SDK `mux-relay` and the generic `moq-core` package, as a uv workspace (uv, ruff, ty) | Working; feature-complete with `ts/`; live-tested against `sjc.relay.mux.global` |

Start with the [JavaScript quickstart](ts/README.md#first-round-trip) (browser WebTransport) or
[Python quickstart](python/README.md#first-round-trip) (native asyncio). Both use local packages
and include credential setup and a bounded publish/subscribe round trip.

## Development

Tool versions for every language are pinned in `mise.toml`, and the mise tasks are the entry points:

```sh
mise install              # node, pnpm, python, uv
mise run check            # every SDK
mise run ts:check
mise run python:check
mise run ts:test:relay    # opt-in live smoke test; needs RELAY_TOKEN or a hidden prompt
mise run python:test:relay
```

Each directory keeps its native tooling, and the tasks only fan out to it. `cd ts && pnpm ...` and
`cd python && uv ...` work as usual under `mise exec`. See `ts/README.md` and `python/README.md`.

## How the SDKs stay in lockstep

Alignment comes from `spec/`, not from shared version numbers:

- `spec/protocol.json` pins the MOQT draft, the default endpoint, the authentication convention,
  and the object size limit. Every SDK has a test asserting its constants match, so changing a pin
  fails every SDK until each one is updated in the same change.
- `spec/behavior.md` states the semantics an SDK must exhibit: connection deadlines and
  cancellation, what subscribing does and does not promise, the announce-and-serve publishing
  flow, package boundaries, and the assertion list for the live smoke test.
- `spec/vectors/` holds encoding vectors that every protocol layer's tests load.

## Versioning and releases

Each SDK is versioned and released independently and tagged by directory, for example `ts/v0.2.0`
and `python/v0.1.0`. Lockstep applies to the protocol pin and the behavior contract, not to version
numbers, so a fix in one language never forces an empty release in another. This table records
which SDK versions speak which draft:

| SDK | Version | MOQT draft |
| --- | --- | --- |
| `@mux/relay` (`ts/`) | unreleased | `moqt-20` |
| `mux-relay` (`python/`) | unreleased | `moqt-20` |

For the moq-relay 0.14.17 compatibility boundary and a reproducible local test, see
[relay interoperability](spec/relay-interop.md). SDK adoption and the relay upgrade must be coordinated.

## Package boundaries

The same shape applies in every language:

- The product package (`@mux/relay`, `mux-relay`) owns the Relay API and the authentication
  convention.
- The generic MoQ core (`@mux/moq-core`, `moq-core`) is protocol-only: an abstract transport, no
  runtime dependencies, no knowledge of Relay, and no player, UI, or media engine. Each language
  enforces this in its check task (`ts/scripts/check-boundaries.mjs`; the import-linter contract in
  `python/pyproject.toml`).
- The TypeScript core is an attributed snapshot of the Video.js protocol modules, intended to
  migrate back upstream rather than become a separately maintained fork. See
  `ts/packages/moq-core/UPSTREAM.json`.

## Adding a language

1. Create `<lang>/` with that ecosystem's native tooling, a product package, and a core package.
2. Add `<lang>:install`, `<lang>:check`, and `<lang>:test` tasks to `mise.toml`, and add
   `<lang>:check` to the root `check` task.
3. Add a pin test against `spec/protocol.json` and load `spec/vectors/` in the protocol tests.
4. Add an opt-in live smoke test with the assertion list in `spec/behavior.md`.
5. Add a workflow under `.github/workflows/` filtered to `<lang>/**` and `spec/**`.
6. Add the SDK to the tables above.
