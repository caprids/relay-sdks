import { spawn } from 'node:child_process';
import { dirname, resolve } from 'node:path';
import { createInterface } from 'node:readline';
import { fileURLToPath } from 'node:url';

// The transport adapter conformance suite's accepting end (python/…/relay_helpers/conformance_peer.py), started for a
// launcher: the Chromium one (test-conformance.mjs) and the Node target's vitest run (conformance/relay-quic.conformance.js).

const python = resolve(dirname(fileURLToPath(import.meta.url)), '../../python');

/**
 * Start the accepting end for `framing` (`'webtransport'` or `'native-quic'`). Resolves
 * `{ port, certificateHash, certificatePem, call, stop }` once it listens.
 */
export async function startConformancePeer({ framing = 'webtransport' } = {}) {
  const child = spawn(
    'uv',
    ['run', '--locked', 'python', '-m', 'relay_helpers.conformance_peer', '--framing', framing],
    {
      cwd: python,
      env: { ...process.env, PYTHONPATH: resolve(python, 'packages/mux-relay/tests') },
      stdio: ['pipe', 'pipe', 'inherit'],
    },
  );
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
