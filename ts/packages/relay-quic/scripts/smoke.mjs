/**
 * Smoke-tests the packed @caprid/relay-quic on the platform it runs on (#195):
 *
 *     node scripts/smoke.mjs <dir of tarballs from pack.mjs>
 *
 * It installs the tarballs into an empty directory with npm, the way an application would, then checks there that:
 *
 * - npm installed exactly the platform package for this platform (its `os`, `cpu` and `libc` fields work);
 * - the package's own loader finds that binary, and `nativeQuicTransport()` builds a client (rustls, the platform's
 *   root certificates);
 * - a dial puts a QUIC version 1 Initial on the wire (tokio, quinn) to a local UDP socket that never answers;
 * - closing before `ready` cancels the dial (TA-11), and the process then exits by itself (TA-26).
 *
 * Only Node's built-ins are used, so it runs as is in any Node 24 image.
 */
import { execFileSync } from 'node:child_process';
import { copyFileSync, mkdtempSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

if (process.argv[2] === '--probe') {
  await probe();
} else {
  install(resolve(process.argv[2] ?? 'packed'));
}

function install(tarballs) {
  const sizes = JSON.parse(readFileSync(join(tarballs, 'sizes.json'), 'utf8'));
  const app = mkdtempSync(join(tmpdir(), 'relay-quic-smoke-'));
  const file = (tarball) => `file:${join(tarballs, tarball)}`;
  const main = Object.keys(sizes).find((name) => name !== 'platforms' && name !== '@caprid/moq-core');
  // The packages are not on the registry, so every @caprid name resolves to its tarball.
  const overrides = {
    '@caprid/moq-core': file(sizes['@caprid/moq-core'].tarball),
    ...Object.fromEntries(
      Object.entries(sizes.platforms).map(([suffix, { tarball }]) => [`${main}-${suffix}`, file(tarball)]),
    ),
  };
  writeFileSync(
    join(app, 'package.json'),
    JSON.stringify({ private: true, type: 'module', dependencies: { [main]: file(sizes[main].tarball) }, overrides }),
  );
  execFileSync('npm', ['install', '--no-audit', '--no-fund', '--loglevel=error'], {
    cwd: app,
    stdio: 'inherit',
    shell: process.platform === 'win32',
  });
  copyFileSync(fileURLToPath(import.meta.url), join(app, 'smoke.mjs'));
  execFileSync(process.execPath, ['smoke.mjs', '--probe'], { cwd: app, stdio: 'inherit', timeout: 30_000 });
  process.stdout.write(`smoke passed in ${app}\n`);
}

async function probe() {
  const { createSocket } = await import('node:dgram');
  const main = '@caprid/relay-quic';

  const installed = readdirSync(join('node_modules', '@caprid')).filter((name) => name.startsWith('relay-quic-'));
  // Worked out here, not by the package, so a wrong suffix in its loader or manifests can't pass unnoticed.
  const glibc = typeof process.report.getReport().header.glibcVersionRuntime === 'string';
  const { platform, arch } = process;
  const suffix =
    platform === 'linux'
      ? `linux-${arch}-${glibc ? 'gnu' : 'musl'}`
      : platform === 'win32'
        ? `win32-${arch}-msvc`
        : `${platform}-${arch}`;
  check(
    installed.length === 1 && installed[0] === `relay-quic-${suffix}`,
    `npm installed ${installed.join(', ') || 'no platform package'}; expected only ${main}-${suffix}`,
  );

  const { nativeQuicTransport } = await import(main);
  const factory = nativeQuicTransport();

  const socket = createSocket('udp4');
  await new Promise((done) => socket.bind(0, '127.0.0.1', done));
  const initial = new Promise((done) => socket.once('message', done));
  const { transport, ready } = factory(`https://127.0.0.1:${socket.address().port}/smoke`, ['moqt-22']);
  transport.closed.catch(() => {});
  const packet = await within(initial, 5_000, 'no QUIC packet reached the local socket');
  // A long header Initial (form, fixed and type bits 1, 1, 00) of QUIC version 1 (RFC 9000 §17.2.2).
  check((packet[0] & 0xf0) === 0xc0 && packet.readUInt32BE(1) === 1, `not a QUIC v1 Initial: ${packet.subarray(0, 5)}`);
  socket.close();

  transport.close({ closeCode: 0, reason: 'smoke' });
  const settled = await within(Promise.allSettled([ready, transport.closed]), 5_000, 'the cancelled dial never ended');
  check(
    settled.every(({ status }) => status === 'rejected'),
    'closing before ready must reject ready and closed (TA-11)',
  );
  process.stdout.write(`${main}-${suffix}: loaded, dialed a QUIC v1 Initial, cancelled the dial\n`);
  // Nothing else is scheduled: the process exits here only if the adapter holds nothing (TA-26). The parent's
  // timeout fails the smoke otherwise.
}

function within(promise, ms, message) {
  let timer;
  const timeout = new Promise((_, fail) => {
    timer = setTimeout(() => fail(new Error(message)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

function check(condition, message) {
  if (!condition) throw new Error(message);
}
