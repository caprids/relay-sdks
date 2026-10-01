import assert from 'node:assert/strict';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { startConformancePeer } from './conformance-peer.js';
import { startModuleServer } from './module-server.js';

// The Chromium launcher for the transport adapter conformance suite (conformance/suite.js). It starts the scriptable
// Python accepting end, serves the suite and the built core to a Playwright page, runs every case there against the
// browser's native WebTransport, and prints the per-case results. It exits non-zero when a case fails, or when an
// expected failure starts to pass. CONFORMANCE_GREP=<text> runs only the cases whose name contains it.

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const grep = process.env.CONFORMANCE_GREP;

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

const peer = await startConformancePeer();
const server = await startModuleServer({
  modules: { '/core/': resolve(root, 'packages/moq-core/dist'), '/conformance/': resolve(root, 'conformance') },
  importMap: { '@mux/moq-core': '/core/index.js' },
});
let browser;
try {
  browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  await page.exposeFunction('conformancePeer', (method, params) => peer.call(method, params));
  await page.exposeFunction('conformanceReport', report);
  await page.goto(`http://127.0.0.1:${server.address().port}/`);
  const results = await page.evaluate(
    async ({ port, certificateHash, grep }) => {
      const { runCase, cases } = await import('/conformance/suite.js');
      const { createChromiumTarget } = await import('/conformance/targets/chromium.js');
      if (typeof WebTransport === 'undefined') throw new Error('This browser does not provide WebTransport');
      const target = createChromiumTarget({ port, certificateHash, call: window.conformancePeer });
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
    { port: peer.port, certificateHash: peer.certificateHash, grep },
  );
  const counts = Object.groupBy(results, (result) => result.status);
  console.log(
    `\n${Object.entries(counts)
      .map(([status, list]) => `${list.length} ${status}`)
      .join(', ')}`,
  );
  assert(results.length > 0, 'no case ran');
  if (counts.failed || counts['unexpected-pass']) process.exitCode = 1;
} catch (error) {
  console.log(`FAIL ${error instanceof Error ? error.message : error}`);
  process.exitCode = 1;
} finally {
  await browser?.close();
  await new Promise((resolve) => server.close(resolve));
  await peer.stop();
}
