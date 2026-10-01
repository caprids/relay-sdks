---
status: accepted
date: 2026-10-01
---

# Releases are hand-cut per language, in lockstep, and dry-run until a variable says otherwise

Nothing in this repository was installable from a registry. We decided how the SDKs ship (#216):

- **Two release trains.** TypeScript and Python release independently, tagged by directory
  (`ts/v0.1.0`, `python/v0.1.0`). Within a train, every package moves at one version: the
  `@mux/relay`, `@mux/moq-core`, `@mux/moq-media`, `@mux/moq-mediabunny` and `@mux/relay-quic`
  packages, and relay-quic's seven platform packages, on npm; `mux-relay`, `mux-moq-core` and
  `mux-moq-media` on PyPI, which pin each other exactly. The first version is 0.1.0, and before 1.0
  a minor version can break the API.
- **Cut by hand.** A release PR runs `ts/scripts/release.mjs version` or
  `python/scripts/release.py version` and names the version in that language's `CHANGELOG.md`.
  Merging it and pushing the tag starts the release workflow, which fails unless the tag, every
  manifest and the changelog agree.
- **Gated in code.** `release-ts.yml` and `release-python.yml` dry-run on pull requests and manual
  runs. On a tag they run in the `release` environment, and they publish for real only when the
  repository variable `RELEASE_PUBLISH` is `true`. Both registries authenticate through trusted
  publishing (OIDC), so the workflows hold no registry token. (npm adds a trusted publisher only
  to a package that exists, so each npm package's very first version is published once by the scope
  owner.) Provenance (npm) and attestations (PyPI) are on whenever the repository is public; neither
  registry issues them for a private repository's builds.
- **Apache-2.0 throughout.** The generic packages are Apache-2.0 because they derive from Video.js,
  and the product packages and the Rust crate now match them, rather than being `UNLICENSED`.
- **PyPI names carry the Mux prefix.** PyPI has no scopes, so the generic packages publish as
  `mux-moq-core` and `mux-moq-media` rather than claiming the generic names `moq-core` and
  `moq-media`. Their import names stay `moq_core` and `moq_media`.

The scripts read every package name from the manifests, so the rename that is still possible before
the first publish (the npm scope among it) changes manifests and docs, not the release machinery.

## Considered options

- **release-please**, which `muxinc/elements` and `muxinc/mux-ts` use. It derives versions and
  changelogs from Conventional Commits; this repository's commits are one imperative sentence with
  the issue number, and we did not want to change that for a 0.x cadence. Worth revisiting if
  releases become frequent.
- **Changesets.** A changeset file per PR, JS-only, so Python would still need its own scheme.
- **Independent versions per package.** relay-quic and its platform packages must match exactly
  anyway, `@mux/relay` peer-depends on relay-quic, and one version per language is the simplest
  thing to support.
- **One train for both languages.** A fix in one language would force an empty release of the
  other, against the existing rule that lockstep across languages covers the protocol pin and the
  behaviour contract, not version numbers.
- **Keeping `private: true` and stripping it in CI.** pnpm and npm will not even dry-run a private
  package, so the gate would live in a sed step; the variable and the environment are a clearer
  gate.

## Consequences

- Before the first real publish, someone with access to the npm `@mux` scope and to PyPI registers
  the trusted publishers for these two workflows and the `release` environment, the package names
  are confirmed, and the repository is made public (so the README links resolve and provenance is
  issued). Those are tracked as follow-up issues.
- A release workflow rerun after a partial publish skips what the registry already has.
- The tarballs and wheels that are smoke-installed are the files that are published.
