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
   cd ts && node scripts/release.mjs version 0.1.0
   cd python && uv run python scripts/release.py version 0.1.0
   ```

2. In that language's `CHANGELOG.md`, rename `## Unreleased` to `## 0.1.0` (a date may follow
   the version) and say what changed, and what a user must do about anything that breaks.
3. Check it, then open the release PR:

   ```sh
   cd ts && node scripts/release.mjs check 0.1.0
   cd python && uv run python scripts/release.py check 0.1.0
   ```

4. After the PR merges, tag the merge commit on `main` and push the tag:

   ```sh
   git tag ts/v0.1.0 && git push origin ts/v0.1.0
   ```

The workflow checks the tag against every manifest and the changelog, runs the language's full
check, builds and packs everything, installs the packages into an empty project and imports them,
then publishes, and creates a GitHub release with the changelog section and the packages attached.
TypeScript also rebuilds and smoke-loads relay-quic's binaries on every platform first, through
`relay-quic-binaries.yml`.

A prerelease version such as `0.2.0-rc.1` publishes to npm's `next` dist-tag, not `latest`; PyPI
(`0.2.0rc1`) leaves prereleases out of a plain install by itself.

If a publish fails partway, rerun the workflow. It skips the packages (npm) or files (PyPI) the
registry already has, and uploads any release assets that are missing.

## What stops a real publish

Pull requests that touch the release scripts, workflows or manifests, and manual runs (even on a
tag ref), always dry-run. A tag push runs the `publish` job in the `release` environment, and it
publishes only when the repository variable `RELEASE_PUBLISH` is `true`. Otherwise it dry-runs and
creates no GitHub release. Before the first real publish:

- the repository is public, so the package READMEs' links resolve and provenance can be issued;
- on npmjs.com, each package has a trusted publisher for `muxinc/relay-sdks`, workflow
  `release-ts.yml`, environment `release`. npm can only add one to a package that exists, so the
  first publish of each package needs a one-off token, or a placeholder publish by the scope owner;
- on PyPI, a pending trusted publisher for each project: `muxinc/relay-sdks`, workflow
  `release-python.yml`, environment `release`;
- the `release` environment has required reviewers, and only then is `RELEASE_PUBLISH` set.

## Try it locally

These run the same steps as the workflows, without publishing:

```sh
cd ts && pnpm build
# relay-quic's pack needs a binary per platform in packages/relay-quic/npm/<platform>/; CI builds them
(cd packages/relay-quic && node scripts/pack.mjs ../../packed)
node scripts/release.mjs pack packed && node scripts/release.mjs smoke packed
node scripts/release.mjs publish packed --dry-run

cd python && uv build --all-packages --out-dir dist
uv run python scripts/release.py smoke dist && uv publish --dry-run --trusted-publishing never dist/*
```
