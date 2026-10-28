# Relay SDKs

Client SDKs for Relay, Mux's Media over QUIC (MoQ) relay service, in every language we support.
Each SDK connects to a Relay endpoint, subscribes to tracks, and publishes data, with the same
protocol pin and the same observable behavior. The SDKs share no code. They share a specification.

The packages are published as `@caprid/*` on npm and `caprid-*` on PyPI. Before 1.0, a minor version
can break the API.

| Directory | Contents | Status |
| --- | --- | --- |
| `spec/` | Protocol pins, the behavior contract every SDK implements, and language-neutral test vectors | Source of truth |
| `ts/` | TypeScript SDK `@caprid/relay`, the generic `@caprid/moq-core` protocol and `@caprid/moq-media` format packages, the optional `@caprid/moq-mediabunny` file-input adapter, and the `@caprid/relay-quic` and `@caprid/relay-websocket` transport adapters, as a pnpm workspace | Working. Live-tested against Mux's relays |
| `python/` | Python SDK `caprid-relay` and the generic `caprid-moq-core` and `caprid-moq-media` packages (imported as `caprid_relay`, `moq_core`, and `moq_media`), as a uv workspace (uv, ruff, ty) | Working. Feature-complete with `ts/`, and live-tested against Mux's relays |
| `tools/` | Hardware measurement rigs, such as the [device latency lab](tools/device-latency/README.md) for microphone and output latency | Run by hand |

Start with the [JavaScript quickstart](ts/GUIDE.md#first-round-trip) (Node; browsers use WebTransport) or
[Python quickstart](python/README.md#first-round-trip) (native asyncio). Each one installs the
published packages into a new project and runs a short publish and subscribe round trip. Each guide
then runs the repository's fuller demo from a checkout.

## Development

Tool versions for every language are pinned in `mise.toml`, and the mise tasks are the entry points:

```sh
mise install              # node, pnpm, python, uv
mise run check            # every SDK
mise run ts:check
mise run python:check
mise run ts:test:relay    # opt-in live smoke test in Node and Chromium, over QUIC and WebSocket (RELAY_TOKEN or a hidden prompt)
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
for both operations. Supply the token through hidden stdin input or the `RELAY_TOKEN` environment
variable. The test never writes it to a file.

Set `RELAY_URL` and `RELAY_NAMESPACE` using the [full demo's prompts](ts/GUIDE.md#run-the-full-demo-from-a-checkout) first. Your token must
authorize both operations under that namespace prefix, including children, because the test
appends a random UUID. Running this command opts you into contacting that endpoint.

```sh
mise exec -- pnpm --dir ts exec playwright install chromium   # once
RELAY_URL="$RELAY_URL" RELAY_NAMESPACE_PREFIX="$RELAY_NAMESPACE" mise run ts:test:relay
```

`RELAY_RUNTIME` picks the runtimes, comma-separated: `node`, `chromium`, `node-websocket` and
`chromium-websocket` (the default four). On request it also runs `firefox` (WebTransport, Firefox
155 or later), `webkit-websocket`, `firefox-websocket`, and `chromium-fallback` (Chromium through
`withWebSocketFallback`). The Firefox and WebKit runtimes need `playwright install firefox` or
`playwright install webkit`. The task first builds `@caprid/relay-quic`'s native addon from source,
with the Rust toolchain the task brings. The workspace never installs the [prebuilt platform packages](ts/packages/relay-quic/README.md#prebuilt-binaries),
which are added only when packing.

Each runtime gets its own fresh namespace and prints its own `PASS` or `FAIL` line, tagged with
`runtime`. A final `summary` line follows. `DIVERGED` means the scenario passed on one runtime and
failed on the other, which is a bug in an adapter or in the
[transport adapter contract](spec/transport-adapter.md), unless a rerun shows a relay flake.
Any result but `PASS` exits nonzero.

The test waits for the publisher's `announced`, subscribes through a supervised subscriber's wants,
warms each track up until a marker arrives, checks binary and empty payloads plus independent track
closure, checks that a want follows a supervised publisher's newer generation, then closes every
connection. It defaults to the bare host `https://relay.mux.global`. Set `RELAY_URL` to use
another endpoint. Set `RELAY_SUB_URL` to
another relay to put the subscribers there. Namespace fields are relative to the endpoint URL's
path. With the recommended endpoint, whose path is the token's root (as the full demo prompts
for), `RELAY_NAMESPACE_PREFIX` leaves the root out. With the bare default, it includes the root.
Either way, the test also dials an endpoint rooted at its own namespace and checks that the two forms reach
each other's tracks and discovery ([issue #266](https://github.com/muxinc/relay-sdks/issues/266)).

The live test passed against `sjc.relay.mux.global` on 2026-09-09: three objects on `events` and two
on `status`, with the second status object delivered after the events track closed.

### Python

Tool versions come from the repository's root `mise.toml`. The Python toolchain is the Astral stack:
uv for the workspace and lockfile, ruff for linting and formatting, and ty for type checking.

```sh
mise install              # once
mise run python:check     # uv sync, ruff check, ruff format --check, lint-imports, ty check, docstring check, pytest
mise run python:test      # pytest only
```

From `python/` under `mise exec`, the native commands are `uv sync`, `uv run pytest`,
`uv run ruff check .`, `uv run ruff format .`, `uv run ty check`, and `uv run lint-imports`. Run
`uv lock` after changing a dependency and commit `uv.lock`.

### Python live Relay smoke test

This opt-in test publishes and subscribes against a real relay. It is excluded from `python:check`
and needs a token authorized for both operations. Supply the token through a hidden prompt or the
`RELAY_TOKEN` environment variable. The test never writes it to a file and redacts it from output.

Set `RELAY_URL` and `RELAY_NAMESPACE` using the [full demo's prompts](python/README.md#run-the-full-demo-from-a-checkout) first. Your token must
authorize both operations under that namespace prefix, including children: the test appends a
random UUID. Running this command explicitly opts into contacting that endpoint.

```sh
RELAY_URL="$RELAY_URL" RELAY_NAMESPACE_PREFIX="$RELAY_NAMESPACE" mise run python:test:relay
```

The test waits for the publisher's `announced`, subscribes through a supervised subscriber's wants,
warms each track up until a marker arrives, and checks binary, empty, and text payloads plus
independent track closure. It also checks a late subscriber's LARGEST_OBJECT, namespace discovery,
a want's move to a supervised publisher's newer generation, and a switchable subscription's move,
then closes every connection. `python/scripts/test_relay.py` lists every assertion. It defaults to
the bare host `https://relay.mux.global`. Set `RELAY_URL` to use another endpoint. Set `RELAY_SUB_URL` to
another relay to put the subscribers there. Namespace fields are relative to the endpoint URL's
path. With the recommended endpoint, whose path is the token's root (as the full demo prompts
for), `RELAY_NAMESPACE_PREFIX` leaves the root out. With the bare default, it includes the root.
Either way, the test also dials an endpoint rooted at its own namespace and checks that the two forms reach
each other's tracks and discovery ([issue #266](https://github.com/muxinc/relay-sdks/issues/266)).

The live test passed against `sjc.relay.mux.global` on 2026-10-03 over the default native QUIC
transport: three objects on `events` and two on `status`, with the second status object delivered
after the events track closed, and both connections closed cleanly.

## How the SDKs stay in lockstep

Alignment comes from `spec/`, not from shared version numbers:

- `spec/protocol.json` pins the MOQT drafts, the default endpoint, the authentication convention,
  the size limits, and the media format drafts. Every SDK has a test asserting its constants match, so changing a pin
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
| `@caprid/relay` (`ts/`) | 0.1.0, 0.1.1 | `moqt-22`, falling back to `moqt-20` |
| `caprid-relay` (`python/`) | 0.1.0 | `moqt-22`, falling back to `moqt-20` |

Publishers end a track with PUBLISH_DONE, which needs moq-relay 0.15.4 or later. On 0.15.1 to
0.15.3, every track end reaches viewers as an aborted track. For that boundary, the 0.14.17 one, and
a reproducible local test, see [relay interoperability](spec/relay-interop.md). SDK adoption and
the relay upgrade must be coordinated.

Package READMEs (`ts/README.md`, `ts/packages/*/README.md`, and `python/packages/*/README.md`) link
to the rest of the repository with absolute URLs to `github.com/caprids/relay-sdks`. npm and PyPI
show these READMEs without the files around them, so a relative link would break there.

## Package boundaries

The same shape applies in every language:

- The optional media format packages (`@caprid/moq-media`, `caprid-moq-media`) own the LOC and MSF
  helpers, H.264 helpers, the broadcast clock, and the frame sequencer. They depend only on the
  core and have no transport dependency. Python's Opus encoder and decoder sit in `moq_media.opus`
  behind the optional `opus` extra ([ADR 0006](docs/adr/0006-opus-codec-behind-an-extra.md)).
  TypeScript accepts native WebCodecs chunks and configurations. Start with the
  [live browser example](ts/examples/webcodecs/README.md). `@caprid/moq-mediabunny` is an optional
  file-input adapter.
- The product package (`@caprid/relay`, `caprid-relay`) owns the Relay API and the authentication
  convention.
- The generic MoQ core (`@caprid/moq-core`, `caprid-moq-core`) is protocol-only: an abstract transport, no
  runtime dependencies, no knowledge of Relay, and no player, UI, or media engine. Each language
  enforces this in its check task: `ts/scripts/check-boundaries.mjs` in TypeScript, and the
  import-linter contracts in `python/pyproject.toml` in Python.
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
