# Releasing

The TypeScript and Python SDKs are two release trains, each released by hand at one version for
all its packages. [ADR 0003](adr/0003-hand-cut-lockstep-releases.md) records why.

| Train | Packages | Tag | Workflow |
| --- | --- | --- | --- |
| TypeScript | `@caprid/relay`, `@caprid/moq-core`, `@caprid/moq-media`, `@caprid/moq-mediabunny`, `@caprid/relay-quic` and its 7 platform packages, `@caprid/relay-websocket` | `ts/v<x.y.z>` | `release-ts.yml` |
| Python | `caprid-relay`, `caprid-moq-core`, `caprid-moq-media` | `python/v<x.y.z>` | `release-python.yml` |

## Cut a release

1. On a branch, set the version and relock:

   ```sh
   cd ts && node scripts/release.mjs version 0.2.0
   cd python && uv run python scripts/release.py version 0.2.0
   ```

2. In that language's `CHANGELOG.md`, rename `## Unreleased` to `## 0.2.0`. A date may follow
   the version. Say what changed, and what a user must do about anything that breaks.
3. Check it, then open the release PR:

   ```sh
   cd ts && node scripts/release.mjs check 0.2.0
   cd python && uv run python scripts/release.py check 0.2.0
   ```

4. After the PR merges, push `main` to `caprids/relay-sdks`, then tag the merge commit and push the
   tag there. The packages publish from that repository. `RELEASE_PUBLISH` is unset on
   `muxinc/relay-sdks`, so a tag pushed there only dry-runs.

   ```sh
   git remote add caprids git@github.com:caprids/relay-sdks.git  # once
   git push caprids main
   git tag ts/v0.2.0 && git push caprids ts/v0.2.0
   ```

The workflow checks the tag against every manifest and the changelog, and runs the language's full
check. It builds and packs everything, installs the packages into an empty project, and imports
them. Then it publishes and creates a GitHub release with the changelog section and the packages
attached.
TypeScript also rebuilds and smoke-loads relay-quic's binaries on every platform first, through
`relay-quic-binaries.yml`.

A prerelease version such as `0.2.0-rc.1` publishes to npm's `next` dist-tag, not `latest`. PyPI
leaves a prerelease such as `0.2.0rc1` out of a plain install without a tag.

If a publish fails partway, rerun the workflow. It skips the packages (npm) or files (PyPI) the
registry already has, and uploads any release assets that are missing.

## What stops a real publish

Pull requests that touch the release scripts, workflows, or manifests always dry-run. So do manual
runs, even on a tag ref. A tag push runs the `publish` job in the `release` environment, and it
publishes only when the repository variable `RELEASE_PUBLISH` is `true`. Otherwise it dry-runs and
creates no GitHub release.

A real publish from `caprids/relay-sdks` depends on this setup, which was in place for 0.1.0:

- The repository is public, so registries can issue provenance.
- On npmjs.com, each package has a trusted publisher for `caprids/relay-sdks`, workflow
  `release-ts.yml`, environment `release`.
- On PyPI, each project has a trusted publisher for `caprids/relay-sdks`, workflow
  `release-python.yml`, environment `release`.
- The `release` environment on `caprids/relay-sdks` has required reviewers and admits only `ts/v*`
  and `python/v*` tags. `RELEASE_PUBLISH` is set there.

To add a package, register its trusted publisher first. npm can add a trusted publisher only to a
package that exists, so a new npm package's first publish needs a one-off token or a placeholder
publish by the scope owner. PyPI takes a pending trusted publisher for a project that does not
exist yet.

## Try it locally

These run the same steps as the workflows, without publishing:

```sh
cd ts && pnpm build
# relay-quic's pack needs a binary per platform in packages/relay-quic/npm/<platform>/. CI builds them.
(cd packages/relay-quic && node scripts/pack.mjs ../../packed)
node scripts/release.mjs pack packed && node scripts/release.mjs smoke packed
node scripts/release.mjs publish packed --dry-run

cd python && uv build --all-packages --out-dir dist
uv run python scripts/release.py smoke dist && uv publish --dry-run --trusted-publishing never dist/*
```
