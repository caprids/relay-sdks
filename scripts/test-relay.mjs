import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { dirname, resolve, sep } from 'node:path';
import { createInterface } from 'node:readline/promises';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const endpoint = process.env.RELAY_URL ?? 'https://sjc.relay.mux.global';
const endpointUrl = new URL(endpoint);
assert.equal(endpointUrl.protocol, 'https:');
assert(!endpointUrl.searchParams.has('jwt'), 'Supply the token via stdin or RELAY_TOKEN, not RELAY_URL');
let token = process.env.RELAY_TOKEN;
if (!token) {
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
assert(token, 'A Relay token is required');
const namespace = [
  ...(process.env.RELAY_NAMESPACE_PREFIX ?? 'relay-sdk-smoke').split('/').filter(Boolean),
  randomUUID(),
];
const redact = (value) => String(value).replaceAll(token, '[redacted]');
const report = (value) => console.log(redact(typeof value === 'string' ? value : JSON.stringify(value)));

const server = createServer(async (request, response) => {
  try {
    const path = new URL(request.url, 'http://localhost').pathname;
    if (path === '/') {
      response.setHeader('Content-Type', 'text/html');
      response.end('<!doctype html><script type="importmap">{"imports":{"@mux/moq-core":"/core/index.js"}}</script>');
      return;
    }
    const prefix = path.startsWith('/sdk/') ? '/sdk/' : path.startsWith('/core/') ? '/core/' : undefined;
    if (!prefix) {
      response.writeHead(404).end();
      return;
    }
    const base = resolve(root, prefix === '/sdk/' ? 'dist' : 'packages/moq-core/dist');
    const file = resolve(base, decodeURIComponent(path.slice(prefix.length)));
    if (!file.startsWith(base + sep) || !file.endsWith('.js')) {
      response.writeHead(403).end();
      return;
    }
    response.setHeader('Content-Type', 'text/javascript');
    response.end(await readFile(file));
  } catch {
    response.writeHead(404).end();
  }
});
await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
let browser;
try {
  browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  await page.exposeFunction('reportPhase', report);
  await page.goto(`http://127.0.0.1:${server.address().port}/`);
  const result = await page.evaluate(
    async ({ endpoint, token, namespace }) => {
      const { connectRelay, connectRelayPublisher } = await import('/sdk/index.js');
      if (typeof WebTransport === 'undefined') throw new Error('This browser does not provide WebTransport');
      let publisher;
      let subscriber;
      const lifetime = new AbortController();
      const deadline = setTimeout(() => lifetime.abort(new Error('Live relay test exceeded 45 seconds')), 45_000);
      const received = { events: [], status: [] };
      const errors = [];
      const expectedEvents = [[0, 255, 128, 1], Array.from(new TextEncoder().encode('hello from Relay SDK')), []];
      let closing = false;
      const observeClosed = (label, connection) => {
        void connection.closed.then(({ error }) => {
          if (!closing) errors.push(`${label} closed: ${String(error ?? 'peer closure')}`);
        });
      };
      const waitForCount = async (track, count) => {
        const until = performance.now() + 10_000;
        while (received[track].length < count) {
          if (errors.length) throw new Error(errors.join('; '));
          lifetime.signal.throwIfAborted();
          if (performance.now() >= until)
            throw new Error(`Timed out receiving ${track}: ${received[track].length}/${count}`);
          await new Promise((resolve) => setTimeout(resolve, 20));
        }
      };
      try {
        publisher = await connectRelayPublisher({
          url: endpoint,
          namespace,
          getToken: () => token,
          signal: lifetime.signal,
        });
        observeClosed('publisher', publisher);
        await window.reportPhase({ phase: 'publisher connected', namespace });
        const events = publisher.publish({ track: 'events', writeTimeoutMs: 20_000 });
        const status = publisher.publish({ track: 'status', writeTimeoutMs: 20_000 });
        subscriber = await connectRelay({ url: endpoint, getToken: () => token, signal: lifetime.signal });
        observeClosed('subscriber', subscriber);
        await window.reportPhase({ phase: 'subscriber connected' });
        for (const track of ['events', 'status']) {
          subscriber.subscribe(
            { namespace, track },
            {
              onOk: () => {
                void window.reportPhase({ phase: 'subscribed', track });
              },
              onObject: (object) => {
                if (object.status === 'normal') received[track].push(Array.from(object.payload));
              },
              onError: (error) => errors.push(`${track}: ${error.errorCode} ${error.reason}`),
              onSubgroupReset: ({ error }) => errors.push(`${track} reset: ${String(error)}`),
            },
          );
        }
        for (const payload of expectedEvents) await events.write(new Uint8Array(payload));
        await status.write(new Uint8Array([42]));
        await waitForCount('events', 3);
        await waitForCount('status', 1);
        await window.reportPhase({
          phase: 'received initial payloads',
          events: received.events.length,
          status: received.status.length,
        });
        events.close();
        await status.write(new Uint8Array([43]));
        await waitForCount('status', 2);
        const samePayloads = (actual, expected) =>
          JSON.stringify(actual.map(JSON.stringify).sort()) === JSON.stringify(expected.map(JSON.stringify).sort());
        if (!samePayloads(received.events, expectedEvents) || !samePayloads(received.status, [[42], [43]])) {
          throw new Error(`Payload mismatch: ${JSON.stringify(received)}`);
        }
        if (errors.length) throw new Error(errors.join('; '));
        return {
          endpoint,
          namespace,
          events: received.events.length,
          status: received.status.length,
          binaryAndEmptyPayloads: 'passed',
          independentTrackClose: 'passed',
        };
      } finally {
        closing = true;
        clearTimeout(deadline);
        try {
          await publisher?.close();
        } finally {
          subscriber?.close();
        }
      }
    },
    { endpoint, token, namespace },
  );
  report({ result: 'PASS', ...result, connections: 'closed' });
} catch (error) {
  report({ result: 'FAIL', error: redact(error instanceof Error ? error.message : error) });
  process.exitCode = 1;
} finally {
  await browser?.close();
  await new Promise((resolve) => server.close(resolve));
}
