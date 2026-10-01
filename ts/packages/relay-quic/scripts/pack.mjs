/**
 * Packs @mux/relay-quic for release, with its prebuilt platform packages (#195):
 *
 *     node scripts/pack.mjs <out dir>
 *
 * Run it after `napi artifacts` has put each target's binary in `npm/<suffix>/`. It packs every platform package, then
 * the main package with an optional dependency on each one at the same version (added only for the pack, so the
 * lockfile never names packages that are not published yet), and `@mux/moq-core`, which the main package depends on
 * and which is not published either. It writes `sizes.json` and a Markdown table of each platform's packed and
 * installed size to `<out dir>`, and appends the table to the GitHub job summary when there is one.
 */
import { execFileSync } from 'node:child_process';
import { appendFileSync, existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const out = resolve(process.argv[2] ?? 'packed');
mkdirSync(out, { recursive: true });

const manifestPath = resolve(root, 'package.json');
const manifestText = readFileSync(manifestPath, 'utf8');
const manifest = JSON.parse(manifestText);

/** `<npm|pnpm> pack` in `dir`; returns what npm reports for the tarball. */
function pack(dir, client = 'npm') {
  if (client === 'pnpm') {
    // pnpm rewrites `workspace:*` to the version; its JSON report has no sizes, so npm reads the tarball afterwards.
    const { filename } = JSON.parse(run('pnpm', ['pack', '--pack-destination', out, '--json'], dir));
    return { ...inspect(resolve(out, filename.split(/[/\\]/).at(-1))) };
  }
  const [report] = JSON.parse(run('npm', ['pack', '--pack-destination', out, '--json'], dir));
  return report;
}

/** npm's report on a tarball that is already packed. */
function inspect(tarball) {
  const [report] = JSON.parse(run('npm', ['pack', '--dry-run', '--json', tarball], out));
  return { ...report, filename: tarball.split(/[/\\]/).at(-1) };
}

function run(command, args, cwd) {
  return execFileSync(command, args, { cwd, encoding: 'utf8', shell: process.platform === 'win32' });
}

const platforms = readdirSync(resolve(root, 'npm')).sort();
const missing = platforms.filter((suffix) => !existsSync(resolve(root, 'npm', suffix, `relay-quic.${suffix}.node`)));
if (missing.length > 0) throw new Error(`no binary in npm/ for ${missing.join(', ')}: run napi artifacts first`);

const packed = Object.fromEntries(platforms.map((suffix) => [suffix, pack(resolve(root, 'npm', suffix))]));

writeFileSync(
  manifestPath,
  `${JSON.stringify(
    {
      ...manifest,
      optionalDependencies: Object.fromEntries(
        platforms.map((suffix) => [`${manifest.name}-${suffix}`, manifest.version]),
      ),
    },
    null,
    2,
  )}\n`,
);
let main;
try {
  main = pack(root, 'pnpm');
} finally {
  writeFileSync(manifestPath, manifestText);
}
const core = pack(resolve(root, '../moq-core'), 'pnpm');

const kib = (bytes) => `${(bytes / 1024).toFixed(0)} KiB`;
const mib = (bytes) => `${(bytes / 1024 / 1024).toFixed(2)} MiB`;
const sizes = {
  [manifest.name]: { tarball: main.filename, packed: main.size, unpacked: main.unpackedSize },
  '@mux/moq-core': { tarball: core.filename, packed: core.size, unpacked: core.unpackedSize },
  platforms: Object.fromEntries(
    platforms.map((suffix) => {
      const report = packed[suffix];
      return [
        suffix,
        {
          tarball: report.filename,
          packed: report.size,
          unpacked: report.unpackedSize,
          // What an install of @mux/relay-quic puts on disk on this platform: it, the core and one binary.
          installed: report.unpackedSize + main.unpackedSize + core.unpackedSize,
        },
      ];
    }),
  ),
};
writeFileSync(resolve(out, 'sizes.json'), `${JSON.stringify(sizes, null, 2)}\n`);

const table = [
  `### ${manifest.name} ${manifest.version}: size per platform`,
  '',
  `The main package packs to ${kib(main.size)} (${kib(main.unpackedSize)} unpacked) and \`@mux/moq-core\` to ` +
    `${kib(core.size)} (${kib(core.unpackedSize)}). Installed is all three on disk.`,
  '',
  '| Platform package | Packed | Unpacked | Installed |',
  '| --- | --- | --- | --- |',
  ...platforms.map((suffix) => {
    const { packed: p, unpacked, installed } = sizes.platforms[suffix];
    return `| \`${manifest.name}-${suffix}\` | ${mib(p)} | ${mib(unpacked)} | ${mib(installed)} |`;
  }),
  '',
].join('\n');
writeFileSync(resolve(out, 'sizes.md'), table);
process.stdout.write(table);
if (process.env.GITHUB_STEP_SUMMARY) appendFileSync(process.env.GITHUB_STEP_SUMMARY, table);
