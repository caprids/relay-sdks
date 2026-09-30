# Relay SDKs

Client SDKs for Relay, Mux's Media over QUIC (MoQ) relay service, in every language we support.
Each SDK connects to a Relay endpoint, subscribes to tracks, and publishes data, with the same
protocol pin and the same observable behavior. The SDKs share no code. They share a specification.

This is an initial development workspace. Package names are provisional and publication is disabled.

| Directory | Contents | Status |
| --- | --- | --- |
| `spec/` | Protocol pins, the behavior contract every SDK implements, and language-neutral test vectors | Source of truth |
| `ts/` | TypeScript SDK `@mux/relay` and the generic `@mux/moq-core` protocol and `@mux/moq-media` format packages, as a pnpm workspace | Working; live-tested against `sjc.relay.mux.global` |
| `python/` | Python SDK `mux-relay` and the generic `moq-core` and `moq-media` packages, as a uv workspace (uv, ruff, ty) | Working; feature-complete with `ts/`; live-tested against `sjc.relay.mux.global` |

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
mise run ts:test:relay    # opt-in live smoke test; needs RELAY_TOKEN or a hidden prompt
mise run python:test:relay
```

Each directory keeps its native tooling, and the tasks only fan out to it. `cd ts && pnpm ...` and
`cd python && uv ...` work as usual under `mise exec`. See the
[TypeScript guide](ts/GUIDE.md) and the [Python guide](python/README.md) for each SDK's usage, and
[Python development](python/README.md#development) for the native uv commands.

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

This opt-in Chromium test publishes and subscribes against a real relay. It is excluded from
`pnpm check` and needs a token authorized for both operations. The token can be supplied through
hidden stdin input or the `RELAY_TOKEN` environment variable; it is never written to a file.

Set `RELAY_URL` and `RELAY_NAMESPACE` using the [quickstart prompts](ts/GUIDE.md#install-and-run) first. Your token must
authorize both operations under that namespace prefix, including children: the test appends a
random UUID. Running this command explicitly opts into contacting that endpoint.

```sh
mise exec -- pnpm --dir ts exec playwright install chromium   # once
RELAY_URL="$RELAY_URL" RELAY_NAMESPACE_PREFIX="$RELAY_NAMESPACE" mise run ts:test:relay
```

The test waits for the publisher's `announced`, subscribes through the examples' announce-race
retry and reports how often it retried, checks binary and empty payloads plus independent track
closure, then closes both connections. It defaults to
`https://sjc.relay.mux.global`; override that with `RELAY_URL` if needed. Namespace fields sent by
the SDK are absolute: include the token's root prefix when required by the relay.

The live test passed against `sjc.relay.mux.global` on 2026-09-09: three objects on `events` and two
on `status`, with the second status object delivered after the events track closed.

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
and `python/v0.1.0`. Lockstep applies to the protocol pin and the behavior contract, not to version
numbers, so a fix in one language never forces an empty release in another. This table records
which SDK versions speak which draft:

| SDK | Version | MOQT draft |
| --- | --- | --- |
| `@mux/relay` (`ts/`) | unreleased | `moqt-20` |
| `mux-relay` (`python/`) | unreleased | `moqt-20` |

Publishers end a track with PUBLISH_DONE, which needs moq-relay 0.15.4 or later: on 0.15.1 to
0.15.3 every track end reaches viewers as an aborted track. For that boundary, the 0.14.17 one, and
a reproducible local test, see [relay interoperability](spec/relay-interop.md). SDK adoption and
the relay upgrade must be coordinated.

The package READMEs (`ts/README.md` for npm, `python/packages/mux-relay/README.md` for PyPI) use
relative links while the repository is private. Those links do not resolve on a registry page, and
the linked files are not in either package, so each must become an absolute URL before the first
release:

- `ts/README.md`: `GUIDE.md`, `GUIDE.md#first-round-trip`,
  `GUIDE.md#use-the-local-packages-in-your-application`, `GUIDE.md#errors-and-recovery`, and
  `../spec/README.md`.
- `python/packages/mux-relay/README.md`: `../../README.md`, `../../README.md#first-round-trip`,
  `../../README.md#use-the-local-packages-in-your-application`, `../../README.md#errors-and-recovery`,
  and `../../../spec/README.md`.

## Package boundaries

The same shape applies in every language:

- The optional media format packages (`@mux/moq-media`, `moq-media`) own pure LOC/MSF and H.264
  helpers and depend only on the core. They have no transport or encoder dependencies. TypeScript
  accepts native WebCodecs chunks/configuration; start with the
  [live browser example](ts/examples/webcodecs/README.md). Mediabunny is an optional file-input adapter.
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
