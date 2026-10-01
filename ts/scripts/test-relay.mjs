import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { createInterface } from 'node:readline/promises';
import { fileURLToPath } from 'node:url';
import { startModuleServer } from './module-server.js';
import { summarizeRuns } from './relay-runs.js';

// The launchers for the live-relay scenario in relay-scenario.js. It runs once per runtime, each with a fresh
// namespace: in Node over @mux/relay-quic's native QUIC, then in a headless Chromium page over WebTransport.
// RELAY_RUNTIME=node or RELAY_RUNTIME=chromium runs only one. Each runtime's outcome is reported, redacted, and
// folded into a summary that calls out a scenario passing on one runtime and failing on the other.

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const endpoint = process.env.RELAY_URL ?? 'https://sjc.relay.mux.global';
const endpointUrl = new URL(endpoint);
assert.equal(endpointUrl.protocol, 'https:');
assert(!endpointUrl.searchParams.has('jwt'), 'Supply the token via stdin or RELAY_TOKEN, not RELAY_URL');
let token = process.env.RELAY_TOKEN;
const anonymous = process.env.RELAY_ANONYMOUS === '1';
const certificateHash = process.env.RELAY_CERTIFICATE_HASH;
if (certificateHash) assert.match(certificateHash, /^[0-9a-f]{64}$/i);
// Node can also trust a CA file; the browser cannot, so the hash wins when both are set.
const caFile = process.env.RELAY_CA_FILE;
const launchers = { node: runInNode, chromium: runInChromium };
const runtimes = process.env.RELAY_RUNTIME ? [process.env.RELAY_RUNTIME] : Object.keys(launchers);
assert(Object.hasOwn(launchers, runtimes[0]), 'RELAY_RUNTIME must be node or chromium, or unset for both');
if (!token && !anonymous) {
  const raw = process.stdin.isRaw ?? false;
  const input = createInterface({ input: process.stdin, terminal: false });
  const interrupt = (chunk) => {
    if (chunk.includes(3) || chunk.includes(4)) input.close();
  };
  if (process.stdin.isTTY) {
    process.stdin.setRawMode(true);
    process.stdin.on('data', interrupt);
    process.stdout.write('Relay token (input hidden): ');
  }
  try {
    for await (const line of input) {
      token = line.trim();
      break;
    }
  } finally {
    input.close();
    if (process.stdin.isTTY) {
      process.stdin.off('data', interrupt);
      process.stdin.setRawMode(raw);
      process.stdout.write('\n');
    }
  }
}
assert(token || anonymous, 'A Relay token is required');
const prefix = (process.env.RELAY_NAMESPACE_PREFIX ?? 'relay-sdk-smoke').split('/').filter(Boolean);
const redact = (value) => (token ? String(value).replaceAll(token, '[redacted]') : String(value));
const report = (value) => console.log(redact(typeof value === 'string' ? value : JSON.stringify(value)));

const outcomes = [];
for (const runtime of runtimes) {
  const namespace = [...prefix, randomUUID()];
  let phase;
  const reportPhase = (value) => {
    phase = value.phase;
    report({ runtime, ...value });
  };
  try {
    const result = await launchers[runtime]({ namespace, reportPhase });
    report({ runtime, result: 'PASS', ...result, connections: 'closed' });
    outcomes.push({ runtime, ok: true });
  } catch (error) {
    const message = redact(error instanceof Error ? error.message : error);
    report({ runtime, result: 'FAIL', error: message });
    outcomes.push({ runtime, ok: false, phase, error: message });
  }
}
const { summary, exitCode } = summarizeRuns(outcomes);
report({ summary });
process.exitCode = exitCode;

/** The scenario in this process, over native QUIC. */
async function runInNode({ namespace, reportPhase }) {
  const [{ nativeQuicTransport }, { runRelayScenario }] = await Promise.all([
    import('@mux/relay-quic'),
    import('./relay-scenario.js'),
  ]);
  const trust = certificateHash
    ? { serverCertificateHashes: [{ algorithm: 'sha-256', value: Buffer.from(certificateHash, 'hex') }] }
    : caFile
      ? { ca: await readFile(caFile) }
      : {};
  const createTransport = nativeQuicTransport(trust);
  return runRelayScenario({ endpoint, token, namespace, createTransport, report: reportPhase });
}

/** The scenario in a headless Chromium page, over WebTransport: the built modules are served to it on localhost. */
async function runInChromium({ namespace, reportPhase }) {
  const { chromium } = await import('playwright');
  // The built SDK, the built core, the runtime-neutral scenario, and the example policy it shares with
  // the mux-global demo, at the paths the scenario's relative imports expect.
  const modules = {
    '/sdk/': resolve(root, 'dist'),
    '/core/': resolve(root, 'packages/moq-core/dist'),
    '/scripts/': resolve(root, 'scripts'),
    '/examples/shared/': resolve(root, 'examples/shared'),
  };
  const server = await startModuleServer({
    modules,
    importMap: { '@mux/relay': '/sdk/index.js', '@mux/moq-core': '/core/index.js' },
  });
  let browser;
  try {
    browser = await chromium.launch({ headless: true });
    const page = await browser.newPage();
    await page.exposeFunction('reportPhase', reportPhase);
    await page.goto(`http://127.0.0.1:${server.address().port}/`);
    return await page.evaluate(
      async ({ endpoint, token, namespace, certificateHash }) => {
        const { runRelayScenario } = await import('/scripts/relay-scenario.js');
        if (typeof WebTransport === 'undefined') throw new Error('This browser does not provide WebTransport');
        const createTransport = certificateHash
          ? (url, protocols) => {
              const value = Uint8Array.from(certificateHash.match(/../g), (byte) => Number.parseInt(byte, 16));
              const transport = new WebTransport(url, {
                protocols,
                serverCertificateHashes: [{ algorithm: 'sha-256', value }],
              });
              return { transport, ready: transport.ready };
            }
          : undefined;
        return runRelayScenario({ endpoint, token, namespace, createTransport, report: window.reportPhase });
      },
      { endpoint, token, namespace, certificateHash },
    );
  } finally {
    await browser?.close();
    await new Promise((resolve) => server.close(resolve));
  }
}
