import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { dirname, resolve } from 'node:path';
import { createInterface } from 'node:readline/promises';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { startModuleServer } from './module-server.js';

// The Chromium launcher for the live-relay scenario in relay-scenario.js: it serves the built modules,
// runs the scenario in a Playwright page, and redacts and reports its outcome.

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const endpoint = process.env.RELAY_URL ?? 'https://sjc.relay.mux.global';
const endpointUrl = new URL(endpoint);
assert.equal(endpointUrl.protocol, 'https:');
assert(!endpointUrl.searchParams.has('jwt'), 'Supply the token via stdin or RELAY_TOKEN, not RELAY_URL');
let token = process.env.RELAY_TOKEN;
const anonymous = process.env.RELAY_ANONYMOUS === '1';
const certificateHash = process.env.RELAY_CERTIFICATE_HASH;
if (certificateHash) assert.match(certificateHash, /^[0-9a-f]{64}$/i);
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
const namespace = [
  ...(process.env.RELAY_NAMESPACE_PREFIX ?? 'relay-sdk-smoke').split('/').filter(Boolean),
  randomUUID(),
];
const redact = (value) => (token ? String(value).replaceAll(token, '[redacted]') : String(value));
const report = (value) => console.log(redact(typeof value === 'string' ? value : JSON.stringify(value)));

/**
 * The built SDK, the built core, the runtime-neutral scenario, and the example policy it shares with
 * the mux-global demo, at the paths the scenario's relative imports expect.
 */
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
  await page.exposeFunction('reportPhase', report);
  await page.goto(`http://127.0.0.1:${server.address().port}/`);
  const result = await page.evaluate(
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
  report({ result: 'PASS', ...result, connections: 'closed' });
} catch (error) {
  report({ result: 'FAIL', error: redact(error instanceof Error ? error.message : error) });
  process.exitCode = 1;
} finally {
  await browser?.close();
  await new Promise((resolve) => server.close(resolve));
}
