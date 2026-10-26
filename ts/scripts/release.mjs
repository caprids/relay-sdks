/**
 * Lockstep releases of the TypeScript packages (#216, docs/adr/0003-hand-cut-lockstep-releases.md). Every package
 * this workspace publishes moves at one version, and nothing here names a package: the names come from the
 * manifests, so a rename touches only them.
 *
 *     node scripts/release.mjs version <x.y.z>    set every package's version, for the release PR
 *     node scripts/release.mjs check <x.y.z>      fail unless every package is at <x.y.z> and CHANGELOG.md has it
 *     node scripts/release.mjs notes <x.y.z>      print that version's CHANGELOG.md section
 *     node scripts/release.mjs pack <dir>         pack each package whose tarball is not in <dir> yet
 *     node scripts/release.mjs smoke <dir>        install the tarballs into an empty project, import and type-check
 *     node scripts/release.mjs publish <dir> [--dry-run] [--provenance]
 *
 * `pack` leaves `@caprid/relay-quic` and its platform packages to `packages/relay-quic/scripts/pack.mjs`, which adds the
 * optional dependencies on the platform packages and needs their binaries; it fails if their tarballs are missing.
 * `publish` publishes in dependency order (platform packages first), a release to the `latest` dist-tag and a
 * prerelease to `next`, and skips a package whose version the registry already has, so a rerun after a partial publish
 * finishes the job.
 */
import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const VERSION = /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/;

/** Every package this workspace publishes: the SDK at the root, `packages/*` and relay-quic's platform packages. */
function packages() {
  const quic = resolve(root, 'packages/relay-quic');
  const dirs = [
    root,
    ...readdirSync(resolve(root, 'packages')).map((name) => resolve(root, 'packages', name)),
    ...readdirSync(resolve(quic, 'npm')).map((suffix) => resolve(quic, 'npm', suffix)),
  ].filter((dir) => existsSync(resolve(dir, 'package.json')));
  return dirs.map((dir) => {
    const path = resolve(dir, 'package.json');
    const manifest = JSON.parse(readFileSync(path, 'utf8'));
    return { dir, path, manifest, platform: dir.startsWith(resolve(quic, 'npm')) };
  });
}

/** The tarball name both npm and pnpm give a package. */
const tarballOf = ({ name, version }) => `${name.replace(/^@/, '').replace('/', '-')}-${version}.tgz`;

/** Platform packages first, then the rest so that each comes after the packages it depends on. */
function publishOrder(all) {
  const names = new Set(all.map(({ manifest }) => manifest.name));
  const internal = ({ manifest }) =>
    Object.keys({ ...manifest.dependencies, ...manifest.peerDependencies, ...manifest.optionalDependencies }).filter(
      (name) => names.has(name),
    );
  const ordered = all.filter(({ platform }) => platform);
  const done = new Set(ordered.map(({ manifest }) => manifest.name));
  let rest = all.filter(({ platform }) => !platform);
  while (rest.length > 0) {
    const ready = rest.filter((entry) => internal(entry).every((name) => done.has(name)));
    if (ready.length === 0) throw new Error(`dependency cycle among ${rest.map(({ manifest }) => manifest.name)}`);
    for (const entry of ready) {
      ordered.push(entry);
      done.add(entry.manifest.name);
    }
    rest = rest.filter((entry) => !ready.includes(entry));
  }
  return ordered;
}

function run(command, args, options = {}) {
  return execFileSync(command, args, { encoding: 'utf8', shell: process.platform === 'win32', ...options });
}

function requireVersion(version) {
  if (!VERSION.test(version ?? '')) throw new Error(`expected a semver version such as 0.1.0, got ${version}`);
  return version;
}

function changelogSection(version) {
  const lines = readFileSync(resolve(root, 'CHANGELOG.md'), 'utf8').split('\n');
  // A version's heading is `## <version>`, optionally followed by a date.
  const start = lines.findIndex((line) => line.split(/\s+/, 2).join(' ') === `## ${version}`);
  if (start === -1) return undefined;
  const end = lines.findIndex((line, index) => index > start && line.startsWith('## '));
  return lines
    .slice(start + 1, end === -1 ? undefined : end)
    .join('\n')
    .trim();
}

const [command, argument, ...flags] = process.argv.slice(2);

if (command === 'version') {
  const version = requireVersion(argument);
  for (const { path, manifest } of packages()) {
    writeFileSync(path, `${JSON.stringify({ ...manifest, version }, null, 2)}\n`);
  }
  process.stdout.write(`every package is now ${version}; add a "## ${version}" section to CHANGELOG.md\n`);
} else if (command === 'check') {
  const version = requireVersion(argument);
  const wrong = packages().filter(({ manifest }) => manifest.version !== version);
  if (wrong.length > 0) {
    const list = wrong.map(({ manifest }) => `${manifest.name}@${manifest.version}`).join(', ');
    throw new Error(`not at ${version}: ${list}; run \`node scripts/release.mjs version ${version}\``);
  }
  if (!changelogSection(version)) throw new Error(`CHANGELOG.md has no "## ${version}" section`);
  process.stdout.write(`every package is at ${version}, and CHANGELOG.md has its section\n`);
} else if (command === 'notes') {
  const notes = changelogSection(requireVersion(argument));
  if (!notes) throw new Error(`CHANGELOG.md has no "## ${argument}" section`);
  process.stdout.write(`${notes}\n`);
} else if (command === 'pack') {
  const out = resolve(argument ?? 'packed');
  mkdirSync(out, { recursive: true });
  const quic = resolve(root, 'packages/relay-quic');
  for (const { dir, manifest } of packages()) {
    if (existsSync(join(out, tarballOf(manifest)))) continue;
    if (dir === quic || dir.startsWith(resolve(quic, 'npm'))) {
      throw new Error(`no ${tarballOf(manifest)} in ${out}: packages/relay-quic/scripts/pack.mjs packs it`);
    }
    // pnpm rewrites `workspace:*` to the exact version.
    run('pnpm', ['pack', '--pack-destination', out], { cwd: dir, stdio: ['ignore', 'ignore', 'inherit'] });
    process.stdout.write(`packed ${tarballOf(manifest)}\n`);
  }
} else if (command === 'smoke') {
  smoke(resolve(argument ?? 'packed'));
} else if (command === 'publish') {
  const dir = resolve(argument ?? 'packed');
  const dryRun = flags.includes('--dry-run');
  const provenance = flags.includes('--provenance');
  for (const { manifest } of publishOrder(packages())) {
    const spec = `${manifest.name}@${manifest.version}`;
    const tarball = join(dir, tarballOf(manifest));
    if (!existsSync(tarball)) throw new Error(`no ${tarballOf(manifest)} in ${dir}`);
    if (published(spec)) {
      process.stdout.write(`${spec} is already on the registry; skipped\n`);
      continue;
    }
    // A prerelease (0.2.0-rc.1) goes to `next`, so it never becomes what a plain install gets.
    const tag = manifest.version.includes('-') ? 'next' : 'latest';
    const args = ['publish', tarball, '--access', 'public', '--tag', tag];
    if (provenance) args.push('--provenance');
    if (dryRun) args.push('--dry-run');
    run('npm', args, { stdio: 'inherit' });
  }
} else {
  process.stderr.write('usage: node scripts/release.mjs version|check|notes <x.y.z> | pack|smoke|publish <dir>\n');
  process.exit(2);
}

function published(spec) {
  try {
    return run('npm', ['view', spec, 'version'], { stdio: ['ignore', 'pipe', 'ignore'] }).trim() !== '';
  } catch {
    return false;
  }
}

/**
 * Installs the tarballs into an empty project with npm, as an application would, every workspace name resolving to
 * its tarball, then imports each entry point in Node and type-checks a module that imports each one. This catches a
 * broken `exports` or `files`, which no check of the sources sees. relay-quic's own smoke covers its binaries.
 */
function smoke(dir) {
  const all = packages();
  const missing = all.filter(({ manifest }) => !existsSync(join(dir, tarballOf(manifest))));
  if (missing.length > 0) throw new Error(`no tarball in ${dir} for ${missing.map(({ manifest }) => manifest.name)}`);
  const file = (manifest) => `file:${join(dir, tarballOf(manifest))}`;
  const libraries = all.filter(({ platform }) => !platform);
  const sdk = all.find(({ dir: packageDir }) => packageDir === root).manifest;
  const app = mkdtempSync(join(tmpdir(), 'relay-release-smoke-'));
  // Peers outside the workspace (mediabunny) at the version the workspace builds against.
  const externalPeers = Object.fromEntries(
    libraries.flatMap(({ manifest }) =>
      Object.keys(manifest.peerDependencies ?? {})
        .filter((name) => !all.some((entry) => entry.manifest.name === name))
        .map((name) => [name, sdk.devDependencies[name] ?? manifest.devDependencies?.[name]]),
    ),
  );
  writeFileSync(
    join(app, 'package.json'),
    JSON.stringify(
      {
        private: true,
        type: 'module',
        dependencies: {
          ...Object.fromEntries(libraries.map(({ manifest }) => [manifest.name, file(manifest)])),
          ...externalPeers,
        },
        devDependencies: {
          typescript: sdk.devDependencies.typescript,
          '@types/node': sdk.devDependencies['@types/node'],
        },
        overrides: Object.fromEntries(all.map(({ manifest }) => [manifest.name, file(manifest)])),
      },
      null,
      2,
    ),
  );
  run('npm', ['install', '--no-audit', '--no-fund', '--loglevel=error'], { cwd: app, stdio: 'inherit' });

  const entries = libraries.flatMap(({ manifest }) =>
    Object.keys(manifest.exports).map((key) => (key === '.' ? manifest.name : `${manifest.name}/${key.slice(2)}`)),
  );
  const runtime = entries
    .map(
      (entry, index) =>
        `import * as m${index} from '${entry}';\n` +
        `if (Object.keys(m${index}).length === 0) throw new Error('${entry} exports nothing');`,
    )
    .join('\n');
  writeFileSync(join(app, 'smoke.mjs'), `${runtime}\nconsole.log('imported ${entries.join(', ')}');\n`);
  run(process.execPath, ['smoke.mjs'], { cwd: app, stdio: 'inherit' });

  // Types as an application resolves them: NodeNext, strict, no skipLibCheck, so a declaration that names a module
  // the package does not ship fails here.
  writeFileSync(
    join(app, 'smoke.ts'),
    `${entries.map((entry, index) => `import type * as t${index} from '${entry}';`).join('\n')}\n` +
      `export type All = [${entries.map((_, index) => `typeof t${index}`).join(', ')}];\n`,
  );
  writeFileSync(
    join(app, 'tsconfig.json'),
    JSON.stringify({
      compilerOptions: {
        module: 'nodenext',
        moduleResolution: 'nodenext',
        target: 'es2024',
        lib: ['es2024', 'dom'],
        types: ['node'],
        strict: true,
        noEmit: true,
        skipLibCheck: false,
      },
      files: ['smoke.ts'],
    }),
  );
  // Only errors in the workspace's packages or the smoke module count: a third party's declarations can clash with
  // each other (mediabunny's @types/dom-webcodecs with lib.dom), and that is not this release's to fix.
  const ours = new Set(all.map(({ manifest }) => `node_modules/${manifest.name}/`));
  let output = '';
  try {
    run(process.execPath, [join(app, 'node_modules/typescript/bin/tsc'), '-p', 'tsconfig.json', '--pretty', 'false'], {
      cwd: app,
      stdio: ['ignore', 'pipe', 'inherit'],
    });
  } catch (error) {
    output = error.stdout ?? '';
  }
  const errors = output
    .split('\n')
    .filter((line) => /error TS\d+/.test(line))
    .filter((line) => line.startsWith('smoke.ts') || [...ours].some((prefix) => line.startsWith(prefix)));
  if (errors.length > 0) throw new Error(`the packed declarations do not type-check:\n${errors.join('\n')}`);
  process.stdout.write(`release smoke passed for ${entries.length} entry points in ${relative(process.cwd(), app)}\n`);
}
