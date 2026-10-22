import assert from 'node:assert/strict';
import { realpathSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium, firefox, webkit } from 'playwright';
import { startConformancePeer } from './conformance-peer.js';
import { startModuleServer } from './module-server.js';

// The browser launcher for the transport adapter conformance suite against @caprid/relay-websocket (#321). It starts the
// Node accepting end for QMux over WebSocket (websocket-peer.js), serves the suite, the adapter and @moq/qmux to a
// Playwright page in each browser, runs every case there against the browser's own WebSocket, and prints the per-case
// results. It exits non-zero when a case fails, or when an expected failure starts to pass.
//
//   BROWSERS=chromium,firefox,webkit   the browsers to run, all three by default
//   CONFORMANCE_GREP=<text>            only the cases whose name contains it
//
// The pages trust the peer's self-signed certificate through Playwright's ignoreHTTPSErrors.

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const grep = process.env.CONFORMANCE_GREP;
const browsers = { chromium, firefox, webkit };
const selected = (process.env.BROWSERS ?? 'chromium,firefox,webkit').split(',').filter(Boolean);

const LABELS = {
  passed: 'pass',
  failed: 'FAIL',
  skipped: 'skip',
  'expected-failure': 'known',
  'unexpected-pass': 'PROMOTE',
  unmet: 'unmet',
};

function report(result) {
  const detail = result.error ?? result.reason;
  console.log(`${LABELS[result.status].padEnd(7)} ${result.name}${detail ? `\n          ${detail}` : ''}`);
}

/** The directory a package's entry point is in, through pnpm's symlinks, as the adapter resolves it. */
function packageDirectory(specifier, from) {
  return dirname(realpathSync(createRequire(from).resolve(specifier)));
}

const adapter = resolve(root, 'packages/relay-websocket/dist');
const qmux = packageDirectory('@moq/qmux', resolve(root, 'packages/relay-websocket/package.json'));
const socketStream = packageDirectory('@moq/web-socket-stream', resolve(qmux, 'package.json'));

const peer = await startConformancePeer({ framing: 'websocket' });
const server = await startModuleServer({
  modules: {
    '/core/': resolve(root, 'packages/moq-core/dist'),
    '/websocket/': adapter,
    '/qmux/': qmux,
    '/web-socket-stream/': socketStream,
    '/conformance/': resolve(root, 'conformance'),
  },
  importMap: {
    '@caprid/moq-core': '/core/index.js',
    '@caprid/relay-websocket': '/websocket/index.js',
    '@moq/qmux': '/qmux/index.js',
    '@moq/web-socket-stream': '/web-socket-stream/index.js',
  },
});

let failed = false;
try {
  for (const name of selected) {
    const type = browsers[name];
    assert(type, `unknown browser ${name}; expected one of ${Object.keys(browsers).join(', ')}`);
    console.log(`\n── ${name} ──`);
    const browser = await type.launch({ headless: true });
    try {
      const context = await browser.newContext({ ignoreHTTPSErrors: true });
      const page = await context.newPage();
      await page.exposeFunction('conformancePeer', (method, params) => peer.call(method, params));
      await page.exposeFunction('conformanceReport', report);
      await page.goto(`http://127.0.0.1:${server.address().port}/`);
      const results = await page.evaluate(
        async ({ port, grep, name }) => {
          const { runCase, cases } = await import('/conformance/suite.js');
          const { createWebSocketTarget } = await import('/conformance/targets/websocket.js');
          const target = createWebSocketTarget({
            name: `${name} WebSocket (${navigator.userAgent})`,
            port,
            call: window.conformancePeer,
          });
          const results = [];
          for (const testCase of cases.filter((candidate) => !grep || candidate.name.includes(grep))) {
            const result = await runCase(target, testCase);
            const error = result.error && `${result.error.name ?? 'Error'}: ${result.error.message ?? result.error}`;
            const plain = { ...result, ...(error ? { error } : {}) };
            results.push(plain);
            await window.conformanceReport(plain);
          }
          return results;
        },
        { port: peer.port, grep, name },
      );
      const counts = Object.groupBy(results, (result) => result.status);
      console.log(
        `${name}: ${Object.entries(counts)
          .map(([status, list]) => `${list.length} ${status}`)
          .join(', ')}`,
      );
      assert(results.length > 0, 'no case ran');
      if (counts.failed || counts['unexpected-pass']) failed = true;
    } finally {
      await browser.close();
    }
  }
} catch (error) {
  console.log(`FAIL ${error instanceof Error ? error.message : error}`);
  failed = true;
} finally {
  await new Promise((resolve) => server.close(resolve));
  await peer.stop();
}
if (failed) process.exitCode = 1;
