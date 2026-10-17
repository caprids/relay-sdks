/**
 * The TA-25 and TA-26 checks for a Node adapter: a child Node process runs a lifetime script, which dials the peer with
 * nothing else scheduled (`relay-quic-lifetime.js` describes the kinds), and this watches how it ends.
 */
import { spawn } from 'node:child_process';

/** How long TA-25 watches an open session's process, and how long TA-26 gives a closed one to exit. */
const LIFETIME_MS = 2_500;
const EXIT_MS = 10_000;

/**
 * Run `script` with `args` and report how it ended: `{ code, signal, lines, alive }`, where `alive` says whether it was
 * still running `LIFETIME_MS` after it reported `ready`.
 */
function runLifetimeChild(script, args, kind) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [script, ...args], { stdio: ['ignore', 'pipe', 'inherit'] });
    const lines = [];
    let alive;
    let deadline;
    child.stdout.setEncoding('utf8').on('data', (text) => {
      lines.push(...text.split('\n').filter(Boolean));
      if (kind === 'keeps-alive' && lines.includes('ready') && alive === undefined) {
        setTimeout(() => {
          alive = child.exitCode === null && child.signalCode === null;
          child.kill();
        }, LIFETIME_MS);
      }
    });
    deadline = setTimeout(() => {
      child.kill();
      deadline = undefined;
    }, LIFETIME_MS + EXIT_MS);
    child.once('error', reject);
    child.once('exit', (code, signal) => {
      if (deadline) clearTimeout(deadline);
      resolve({ code, signal, lines, alive, timedOut: deadline === undefined });
    });
  });
}

/** Run the `kind` check (`keeps-alive`, `exits-after-close` or `exits-after-cancel`) with `script`; throws on failure. */
export async function checkProcessLifetime(script, args, kind) {
  const { code, signal, lines, alive, timedOut } = await runLifetimeChild(script, args, kind);
  if (kind === 'keeps-alive') {
    if (!lines.includes('ready')) throw new Error(`the child never connected (exit ${code ?? signal})`);
    if (!alive) throw new Error(`the process exited with the session open (exit ${code ?? signal})`);
    return;
  }
  if (!lines.includes('closed')) throw new Error(`the child never closed its session (exit ${code ?? signal})`);
  if (timedOut) throw new Error(`the process was still running ${EXIT_MS} ms after closed settled`);
  // 13 is Node's exit code for a top-level await that never settled: something stopped holding it too early.
  if (code !== 0) throw new Error(`the process exited with ${code ?? signal}, not 0`);
}
