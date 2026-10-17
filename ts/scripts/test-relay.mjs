import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { realpathSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { dirname, resolve } from 'node:path';
import { createInterface } from 'node:readline/promises';
import { fileURLToPath } from 'node:url';
import { startModuleServer } from './module-server.js';
import { summarizeRuns } from './relay-runs.js';

// The launchers for the live-relay scenario in relay-scenario.js. It runs once per runtime, each with a fresh
// namespace: in Node over @mux/relay-quic's native QUIC, in a headless Chromium page over WebTransport, then both again
// over @mux/relay-websocket's QMux over WebSocket (#321). RELAY_RUNTIME names the runtimes to run, comma-separated:
// those four (node, chromium, node-websocket, chromium-websocket), plus webkit-websocket and firefox-websocket, and
// chromium-fallback, Chromium through withWebSocketFallback. Each runtime's outcome is reported, redacted, and folded
// into a summary that calls out a scenario passing on one runtime and failing on the other.
//
// Over WebSocket, Node trusts a local relay's certificate only through NODE_EXTRA_CA_CERTS, and the browsers ignore
// certificate errors when RELAY_CERTIFICATE_HASH or RELAY_CA_FILE says the relay is a local one.

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const readEndpoint = (name, fallback) => {
  const endpoint = process.env[name] ?? fallback;
  const url = new URL(endpoint);
  assert.equal(url.protocol, 'https:', `${name} must be an https URL`);
  assert(!url.searchParams.has('jwt'), `Supply the token via stdin or RELAY_TOKEN, not ${name}`);
  return endpoint;
};
const endpoint = readEndpoint('RELAY_URL', 'https://sjc.relay.mux.global');
// The subscribers' relay, for a cross-relay run.
const subEndpoint = readEndpoint('RELAY_SUB_URL', endpoint);
let token = process.env.RELAY_TOKEN;
const anonymous = process.env.RELAY_ANONYMOUS === '1';
const certificateHash = process.env.RELAY_CERTIFICATE_HASH;
if (certificateHash) assert.match(certificateHash, /^[0-9a-f]{64}$/i);
// Node can also trust a CA file; the browser cannot, so the hash wins when both are set.
const caFile = process.env.RELAY_CA_FILE;
const launchers = {
  node: runInNode,
  chromium: (options) => runInBrowser('chromium', 'webtransport', options),
  'node-websocket': runInNodeOverWebSocket,
  'chromium-websocket': (options) => runInBrowser('chromium', 'websocket', options),
  'webkit-websocket': (options) => runInBrowser('webkit', 'websocket', options),
  'firefox-websocket': (options) => runInBrowser('firefox', 'websocket', options),
  'chromium-fallback': (options) => runInBrowser('chromium', 'fallback', options),
};
const runtimes = process.env.RELAY_RUNTIME
  ? process.env.RELAY_RUNTIME.split(',').filter(Boolean)
  : ['node', 'chromium', 'node-websocket', 'chromium-websocket'];
for (const runtime of runtimes) {
  assert(Object.hasOwn(launchers, runtime), `RELAY_RUNTIME names ${runtime}; expected ${Object.keys(launchers)}`);
}
// A local relay's self-signed certificate, which a browser's WebSocket can only be told to ignore.
const localRelay = Boolean(certificateHash || caFile);
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
  return runRelayScenario({ endpoint, subEndpoint, token, namespace, createTransport, report: reportPhase });
}

/** The scenario in this process, over QMux over WebSocket. */
async function runInNodeOverWebSocket({ namespace, reportPhase }) {
  const [{ createWebSocketTransport }, { runRelayScenario }] = await Promise.all([
    import('@mux/relay-websocket'),
    import('./relay-scenario.js'),
  ]);
  const createTransport = createWebSocketTransport();
  return runRelayScenario({ endpoint, subEndpoint, token, namespace, createTransport, report: reportPhase });
}

/** The directory a package's entry point is in, through pnpm's symlinks. */
function packageDirectory(specifier, from) {
  return dirname(realpathSync(createRequire(from).resolve(specifier)));
}

/**
 * The scenario in a headless browser page: over WebTransport, over QMux over WebSocket, or through the fallback from
 * one to the other (`transport`). The built modules are served to it on localhost.
 */
async function runInBrowser(name, transport, { namespace, reportPhase }) {
  const playwright = await import('playwright');
  const qmux = packageDirectory('@moq/qmux', resolve(root, 'packages/relay-websocket/package.json'));
  // The built SDK, the built core, the runtime-neutral scenario, and the example policy it shares with
  // the mux-global demo, at the paths the scenario's relative imports expect; the WebSocket adapter beside them.
  const modules = {
    '/sdk/': resolve(root, 'dist'),
    '/core/': resolve(root, 'packages/moq-core/dist'),
    '/scripts/': resolve(root, 'scripts'),
    '/examples/shared/': resolve(root, 'examples/shared'),
    '/websocket/': resolve(root, 'packages/relay-websocket/dist'),
    '/qmux/': qmux,
    '/web-socket-stream/': packageDirectory('@moq/web-socket-stream', resolve(qmux, 'package.json')),
  };
  const server = await startModuleServer({
    modules,
    importMap: {
      '@mux/relay': '/sdk/index.js',
      '@mux/moq-core': '/core/index.js',
      '@mux/relay-websocket': '/websocket/index.js',
      '@moq/qmux': '/qmux/index.js',
      '@moq/web-socket-stream': '/web-socket-stream/index.js',
    },
  });
  let browser;
  try {
    browser = await playwright[name].launch({ headless: true });
    const context = await browser.newContext({ ignoreHTTPSErrors: transport !== 'webtransport' && localRelay });
    const page = await context.newPage();
    await page.exposeFunction('reportPhase', reportPhase);
    await page.goto(`http://127.0.0.1:${server.address().port}/`);
    return await page.evaluate(
      async ({ endpoint, subEndpoint, token, namespace, certificateHash, transport }) => {
        const { runRelayScenario } = await import('/scripts/relay-scenario.js');
        const report = window.reportPhase;
        if (transport !== 'webtransport') {
          const { createWebSocketTransport, withWebSocketFallback } = await import('@mux/relay-websocket');
          const createTransport =
            transport === 'websocket'
              ? createWebSocketTransport()
              : withWebSocketFallback({
                  onFallback: (reason) => void report({ phase: 'fallback', reason: String(reason) }),
                });
          return runRelayScenario({ endpoint, subEndpoint, token, namespace, createTransport, report });
        }
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
        return runRelayScenario({
          endpoint,
          subEndpoint,
          token,
          namespace,
          createTransport,
          report: window.reportPhase,
        });
      },
      { endpoint, subEndpoint, token, namespace, certificateHash, transport },
    );
  } finally {
    await browser?.close();
    await new Promise((resolve) => server.close(resolve));
  }
}
