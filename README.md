# Relay SDKs

Client SDKs for Relay, Mux's Media over QUIC (MoQ) relay service, in every language we support.
Each SDK connects to a Relay endpoint, subscribes to tracks, and publishes data, with the same
protocol pin and the same observable behavior. The SDKs share no code. They share a specification.

This is an initial development workspace. Package names are provisional and publication is disabled.

| Directory | Contents | Status |
| --- | --- | --- |
| `spec/` | Protocol pins, the behavior contract every SDK implements, and language-neutral test vectors | Source of truth |
| `ts/` | TypeScript SDK `@caprid/relay` and the generic `@caprid/moq-core` protocol and `@caprid/moq-media` format packages, as a pnpm workspace | Working; live-tested against `sjc.relay.mux.global` |
| `python/` | Python SDK `caprid-relay` and the generic `caprid-moq-core` and `caprid-moq-media` packages (imported as `moq_core` and `moq_media`), as a uv workspace (uv, ruff, ty) | Working; feature-complete with `ts/`; live-tested against `sjc.relay.mux.global` |
| `tools/` | Hardware measurement rigs, such as the [device latency lab](tools/device-latency/README.md) for microphone and output latency | Run by hand |

Start with the [JavaScript quickstart](ts/GUIDE.md#first-round-trip) (browser WebTransport) or
[Python quickstart](python/README.md#first-round-trip) (native asyncio). Both use local packages
and include credential setup and a bounded publish/subscribe round trip.

## Development

Tool versions for every language are pinned in `mise.toml`, and the mise tasks are the entry points:

```sh
mise install              # node, pnpm, python, uv
mise run check            # every SDK
mise run ts:check
mise run python:check
mise run ts:test:relay    # opt-in live smoke test in Node and Chromium, over QUIC and WebSocket; needs RELAY_TOKEN or a hidden prompt
mise run python:test:relay
```

Each directory keeps its native tooling, and the tasks only fan out to it. `cd ts && pnpm ...` and
`cd python && uv ...` work as usual under `mise exec`. See the
[TypeScript guide](ts/GUIDE.md) and the [Python guide](python/README.md) for each SDK's usage.

### TypeScript

Run the root tasks from anywhere in the repository, or run pnpm from `ts/` under `mise exec`:

```sh
mise run ts:install
mise run ts:check     # equivalent to: cd ts && mise exec -- pnpm check
mise run ts:lint      # Biome format and lint check only
mise run ts:fix       # apply Biome's safe formatting and lint fixes
```

Formatting and linting are configured in `ts/biome.jsonc`, and every package extends the strict
compiler options in `ts/tsconfig.base.json`. `pnpm lint` fails on warnings, so a clean check prints
nothing.

### TypeScript live Relay smoke test

This opt-in test publishes and subscribes against a real relay, running the same scenario four
times: in Node over [`@caprid/relay-quic`](ts/packages/relay-quic/README.md)'s native QUIC, in
headless Chromium over WebTransport, then in both over
[`@caprid/relay-websocket`](ts/packages/relay-websocket/README.md)'s QMux over WebSocket. It is excluded from `pnpm check` and needs a token authorized
for both operations. The token can be supplied through
hidden stdin input or the `RELAY_TOKEN` environment variable; it is never written to a file.

Set `RELAY_URL` and `RELAY_NAMESPACE` using the [quickstart prompts](ts/GUIDE.md#install-and-run) first. Your token must
authorize both operations under that namespace prefix, including children: the test appends a
random UUID. Running this command explicitly opts into contacting that endpoint.

```sh
mise exec -- pnpm --dir ts exec playwright install chromium   # once
RELAY_URL="$RELAY_URL" RELAY_NAMESPACE_PREFIX="$RELAY_NAMESPACE" mise run ts:test:relay
```

`RELAY_RUNTIME` picks the runtimes, comma-separated: `node`, `chromium`, `node-websocket` and
`chromium-websocket` (the default four), and on request `firefox` (WebTransport, Firefox 155 or
later), `webkit-websocket`, `firefox-websocket` (each needs `playwright install firefox` or
`webkit`) and `chromium-fallback`, Chromium through `withWebSocketFallback`. The task builds
`@caprid/relay-quic`'s native addon from source first, with the Rust toolchain it brings: the
workspace never installs the [prebuilt platform packages](ts/packages/relay-quic/README.md#prebuilt-binaries),
which are added only when packing.

Each runtime gets its own fresh namespace and prints its own `PASS` or `FAIL` line, tagged with
`runtime`. A final `summary` line follows. `DIVERGED` means the scenario passed on one runtime and
failed on the other, which is a bug in an adapter or in the
[transport adapter contract](spec/transport-adapter.md), unless a rerun shows a relay flake.
Any result but `PASS` exits nonzero.

The test waits for the publisher's `announced`, subscribes through a supervised subscriber's wants,
warms each track up until a marker arrives, checks binary and empty payloads plus independent track
closure, checks that a want follows a supervised publisher's newer generation, then closes every
connection. It defaults to the bare host
`https://sjc.relay.mux.global`; override that with `RELAY_URL` if needed. Set `RELAY_SUB_URL` to
another relay to put the subscribers there. Namespace fields are relative to the endpoint URL's
path: with the recommended endpoint, whose path is the token's root (as the quickstart prompts
for), `RELAY_NAMESPACE_PREFIX` leaves the root out; with the bare default it includes it. Either
way the test also dials an endpoint rooted at its own namespace and checks that the two forms reach
each other's tracks and discovery ([issue #266](https://github.com/muxinc/relay-sdks/issues/266)).

The live test passed against `sjc.relay.mux.global` on 2026-09-09: three objects on `events` and two
on `status`, with the second status object delivered after the events track closed.

### Python

Tool versions come from the repository's root `mise.toml`. The Python toolchain is the Astral stack:
uv for the workspace and lockfile, ruff for linting and formatting, ty for type checking.

```sh
mise install              # once
mise run python:check     # uv sync, ruff check, ruff format --check, lint-imports, ty check, pytest
mise run python:test      # pytest only
```

From `python/` under `mise exec`, the native commands are `uv sync`, `uv run pytest`,
`uv run ruff check .`, `uv run ruff format .`, `uv run ty check`, and `uv run lint-imports`. Run
`uv lock` after changing a dependency and commit `uv.lock`.

### Python live Relay smoke test

This opt-in test publishes and subscribes against a real relay. It is excluded from `python:check`
and needs a token authorized for both operations. The token can be supplied through a hidden prompt
or the `RELAY_TOKEN` environment variable; it is never written to a file and is redacted from output.

Set `RELAY_URL` and `RELAY_NAMESPACE` using the [quickstart prompts](python/README.md#install-and-run) first. Your token must
authorize both operations under that namespace prefix, including children: the test appends a
random UUID. Running this command explicitly opts into contacting that endpoint.

```sh
RELAY_URL="$RELAY_URL" RELAY_NAMESPACE_PREFIX="$RELAY_NAMESPACE" mise run python:test:relay
```

The test waits for the publisher's `announced`, subscribes through the examples' announce-race
retry and reports how often it retried, warms each track up until a marker arrives, checks binary,
empty, and text payloads plus independent track closure, then closes both connections. It defaults to the bare host
`https://sjc.relay.mux.global`; override that with `RELAY_URL` if needed. Set `RELAY_SUB_URL` to
another relay to put the subscribers there. Namespace fields are relative to the endpoint URL's
path: with the recommended endpoint, whose path is the token's root (as the quickstart prompts
for), `RELAY_NAMESPACE_PREFIX` leaves the root out; with the bare default it includes it. Either
way the test also dials an endpoint rooted at its own namespace and checks that the two forms reach
each other's tracks and discovery ([issue #266](https://github.com/muxinc/relay-sdks/issues/266)).

The live test passed against `sjc.relay.mux.global` on 2026-10-03 over the default native QUIC
transport: three objects on `events` and two on `status`, with the second status object delivered
after the events track closed, and both connections closed cleanly.

## How the SDKs stay in lockstep

Alignment comes from `spec/`, not from shared version numbers:

- `spec/protocol.json` pins the MOQT draft, the default endpoint, the authentication convention,
  and the object size limit. Every SDK has a test asserting its constants match, so changing a pin
  fails every SDK until each one is updated in the same change.
- `spec/behavior.md` states the semantics an SDK must exhibit: connection deadlines and
  cancellation, what subscribing does and does not promise, the announce-and-serve publishing
  flow, package boundaries, and the assertion list for the live smoke test.
- `spec/vocabulary.md` fixes the public names both SDKs use for each concept, in each language's
  casing, and records why each was chosen.
- `spec/vectors/` holds encoding vectors that every protocol layer's tests load.

## Versioning and releases

Each SDK is versioned and released independently and tagged by directory, for example `ts/v0.2.0`
and `python/v0.1.0`. Lockstep across languages applies to the protocol pin and the behavior
contract, not to version numbers, so a fix in one language never forces an empty release in
another. Within a language every package moves at one version: all the `@caprid/*` packages together,
and `caprid-relay`, `caprid-moq-core` and `caprid-moq-media` together. Everything is Apache-2.0
([LICENSE](LICENSE)). [Releasing](docs/releasing.md) has the steps, and
[ADR 0003](docs/adr/0003-hand-cut-lockstep-releases.md) the reasons. This table records which SDK
versions speak which draft:

| SDK | Version | MOQT draft |
| --- | --- | --- |
| `@caprid/relay` (`ts/`) | unreleased | `moqt-22`, falling back to `moqt-20` |
| `caprid-relay` (`python/`) | unreleased | `moqt-22`, falling back to `moqt-20` |

Publishers end a track with PUBLISH_DONE, which needs moq-relay 0.15.4 or later: on 0.15.1 to
0.15.3 every track end reaches viewers as an aborted track. For that boundary, the 0.14.17 one, and
a reproducible local test, see [relay interoperability](spec/relay-interop.md). SDK adoption and
the relay upgrade must be coordinated.

The package READMEs (`ts/README.md` for npm, `python/packages/caprid-relay/README.md` for PyPI) use
relative links while the repository is private. Those links do not resolve on a registry page, and
the linked files are not in either package, so each must become an absolute URL before the first
release:

- `ts/README.md`: `GUIDE.md`, `GUIDE.md#first-round-trip`,
  `GUIDE.md#use-the-local-packages-in-your-application`, `GUIDE.md#errors-and-recovery`, and
  `../spec/README.md`.
- `python/packages/caprid-relay/README.md`: `../../README.md`, `../../README.md#first-round-trip`,
  `../../README.md#use-the-local-packages-in-your-application`, `../../README.md#errors-and-recovery`,
  and `../../../spec/README.md`.

## Package boundaries

The same shape applies in every language:

- The optional media format packages (`@caprid/moq-media`, `moq-media`) own pure LOC/MSF and H.264
  helpers and depend only on the core. They have no transport or encoder dependencies. TypeScript
  accepts native WebCodecs chunks/configuration; start with the
  [live browser example](ts/examples/webcodecs/README.md). Mediabunny is an optional file-input adapter.
- The product package (`@caprid/relay`, `caprid-relay`) owns the Relay API and the authentication
  convention.
- The generic MoQ core (`@caprid/moq-core`, `moq-core`) is protocol-only: an abstract transport, no
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
