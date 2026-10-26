import { execFileSync, spawn } from 'node:child_process';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { createInterface } from 'node:readline';
import { fileURLToPath } from 'node:url';

// The transport adapter conformance suite's accepting end (python/…/relay_helpers/conformance_peer.py), started for a
// launcher: the Chromium one (test-conformance.mjs) and the Node target's vitest run (conformance/relay-quic.conformance.js).
// For QMux over WebSocket it is the Node accepting end in websocket-peer.js instead, which speaks the same JSON lines.

const scripts = dirname(fileURLToPath(import.meta.url));
const python = resolve(scripts, '../../python');

/**
 * A self-signed P-256 certificate for `localhost` and 127.0.0.1, valid for 10 days, written with openssl to a fresh
 * temporary directory: `{ certificate, key }`, the two PEM files' paths. The WebSocket accepting end serves it; a Node
 * dialer trusts it through NODE_EXTRA_CA_CERTS, which Node reads only as a process starts.
 */
export function createPeerCertificate() {
  const directory = mkdtempSync(join(tmpdir(), 'relay-conformance-'));
  const certificate = join(directory, 'certificate.pem');
  const key = join(directory, 'key.pem');
  execFileSync(
    'openssl',
    [
      'req',
      '-x509',
      '-newkey',
      'ec',
      '-pkeyopt',
      'ec_paramgen_curve:P-256',
      '-nodes',
      '-days',
      '10',
      '-subj',
      '/CN=localhost',
      '-addext',
      'subjectAltName=DNS:localhost,IP:127.0.0.1',
      '-keyout',
      key,
      '-out',
      certificate,
    ],
    { stdio: 'ignore' },
  );
  return { certificate, key };
}

/** The accepting end's process for `framing`. */
function spawnPeer(framing, host) {
  if (framing === 'websocket') {
    // The certificate the launcher made before its dialers started, else a fresh one.
    const certificate = process.env.WEBSOCKET_PEER_CERTIFICATE;
    const key = process.env.WEBSOCKET_PEER_KEY;
    const files = certificate && key ? { certificate, key } : createPeerCertificate();
    return spawn(
      process.execPath,
      [resolve(scripts, 'websocket-peer.js'), files.certificate, files.key, ...(host ? [host] : [])],
      { stdio: ['pipe', 'pipe', 'inherit'] },
    );
  }
  return spawn(
    'uv',
    [
      'run',
      '--locked',
      'python',
      '-m',
      'relay_helpers.conformance_peer',
      '--framing',
      framing,
      ...(host ? ['--host', host] : []),
    ],
    {
      cwd: python,
      env: { ...process.env, PYTHONPATH: resolve(python, 'packages/caprid-relay/tests') },
      stdio: ['pipe', 'pipe', 'inherit'],
    },
  );
}

/**
 * Start the accepting end for `framing` (`'webtransport'`, `'native-quic'` or `'websocket'`), on `host` when given
 * instead of the framing's default. Resolves `{ port, certificateHash, certificatePem, call, stop }` once it listens.
 */
export async function startConformancePeer({ framing = 'webtransport', host } = {}) {
  const child = spawnPeer(framing, host);
  const pending = new Map();
  let nextId = 0;
  let listening;
  const started = new Promise((resolve, reject) => {
    listening = { resolve, reject };
  });
  child.once('exit', (code) => {
    const error = new Error(`the conformance peer exited (code ${code})`);
    listening.reject(error);
    for (const { reject } of pending.values()) reject(error);
    pending.clear();
  });
  createInterface({ input: child.stdout }).on('line', (line) => {
    const message = JSON.parse(line);
    if (message.event === 'listening') {
      listening.resolve(message);
      return;
    }
    const waiter = pending.get(message.id);
    pending.delete(message.id);
    if ('error' in message) waiter?.reject(new Error(`peer: ${message.error}`));
    else waiter?.resolve(message.result);
  });
  const { port, certificateHash, certificatePem } = await started;
  return {
    port,
    certificateHash,
    certificatePem,
    call(method, params = {}) {
      const id = nextId++;
      return new Promise((resolve, reject) => {
        pending.set(id, { resolve, reject });
        child.stdin.write(`${JSON.stringify({ id, method, params })}\n`);
      });
    },
    async stop() {
      child.stdin.end();
      await new Promise((resolve) => (child.exitCode === null ? child.once('exit', resolve) : resolve()));
    },
  };
}
