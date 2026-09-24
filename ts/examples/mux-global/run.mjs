#!/usr/bin/env node
/**
 * Run the Relay example (demo.js) against a live Relay in headless Chromium.
 *
 * Node has no WebTransport, so this serves the built SDK and demo.js on localhost, launches
 * Chromium through Playwright, and prints the page's log entries. The SDK calls live in demo.js.
 *
 *   cd ts && pnpm build
 *   # After the README's endpoint/namespace setup; enter the token at the hidden prompt.
 *   node examples/mux-global/run.mjs --url "$RELAY_URL" --namespace "$RELAY_NAMESPACE"
 */
import { randomUUID } from 'node:crypto';
import { access, readFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { dirname, resolve, sep } from 'node:path';
import { createInterface } from 'node:readline/promises';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { chromium } from 'playwright';

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, '../..');
/** The two built packages, which must exist, and the example policies shared with the smoke test. */
const built = { '/sdk/': resolve(root, 'dist'), '/core/': resolve(root, 'packages/moq-core/dist') };
const modules = { ...built, '/shared/': resolve(here, '../shared') };
const usage = `Usage: node examples/mux-global/run.mjs [options]

  --url <https url>      Relay endpoint (default RELAY_URL or https://sjc.relay.mux.global)
  --namespace <a/b/c>    Namespace, slash-separated, including the token's root prefix
                         (default RELAY_NAMESPACE_PREFIX or relay-sdk-example, plus a random field)
  --track <name>         Track name (default ticker)
  --mode <both|publish|subscribe>
                         Publish and subscribe in one page, or run only one side (default both)
  --count <n>            Objects to publish and/or wait for (default 5)
  --interval <ms>        Delay between published objects (default 1000)
  --timeout <s>          Deadline for the whole run (default 60)

The token comes from RELAY_TOKEN or a hidden prompt; it is never written to disk.`;

function fail(message) {
  console.error(`${message}\n\n${usage}`);
  process.exit(2);
}

function positiveInteger(name, value) {
  const number = Number(value);
  if (!Number.isInteger(number) || number <= 0) fail(`--${name} must be a positive integer, got "${value}"`);
  return number;
}

let args;
try {
  ({ values: args } = parseArgs({
    options: {
      url: { type: 'string', default: process.env.RELAY_URL ?? 'https://sjc.relay.mux.global' },
      namespace: { type: 'string' },
      track: { type: 'string', default: 'ticker' },
      mode: { type: 'string', default: 'both' },
      count: { type: 'string', default: '5' },
      interval: { type: 'string', default: '1000' },
      timeout: { type: 'string', default: '60' },
      help: { type: 'boolean', short: 'h', default: false },
    },
  }));
} catch (error) {
  fail(error.message);
}
if (args.help) {
  console.log(usage);
  process.exit(0);
}

const endpoint = new URL(args.url);
if (endpoint.protocol !== 'https:') fail('--url must be an https URL');
if (endpoint.searchParams.has('jwt')) fail('Supply the token via RELAY_TOKEN or the prompt, not inside --url');
if (!['both', 'publish', 'subscribe'].includes(args.mode)) fail('--mode must be both, publish, or subscribe');
const namespace = (
  args.namespace ?? `${process.env.RELAY_NAMESPACE_PREFIX ?? 'relay-sdk-example'}/${randomUUID().slice(0, 8)}`
)
  .split('/')
  .filter(Boolean);
if (namespace.length === 0) fail('--namespace must contain at least one field');
if (!args.track) fail('--track must not be empty');
const config = {
  url: endpoint.href,
  namespace,
  track: args.track,
  mode: args.mode,
  count: positiveInteger('count', args.count),
  intervalMs: positiveInteger('interval', args.interval),
  timeoutMs: positiveInteger('timeout', args.timeout) * 1000,
};

await Promise.all(Object.values(built).map((directory) => access(resolve(directory, 'index.js')))).catch(() =>
  fail('The SDK is not built yet; run `pnpm build` first'),
);

const token = await readToken();
if (!token) fail('A Relay token is required');

const redact = (text) => text.replaceAll(token, '[redacted]');
const format = ({ event, ...fields }) =>
  [
    event.padEnd(20),
    ...Object.entries(fields)
      .filter(([, value]) => value !== undefined)
      .map(([key, value]) => `${key}=${typeof value === 'string' ? value : JSON.stringify(value)}`),
  ].join(' ');
const print = (entry) => console.log(redact(format(entry)));

const server = createServer(async (request, response) => {
  const path = new URL(request.url, 'http://localhost').pathname;
  try {
    if (path === '/') {
      response.setHeader('Content-Type', 'text/html');
      response.end(
        '<!doctype html><title>@mux/relay example</title>' +
          '<script type="importmap">{"imports":{"@mux/relay":"/sdk/index.js","@mux/moq-core":"/core/index.js"}}</script>',
      );
      return;
    }
    const file = resolveModule(path);
    if (!file) {
      response.writeHead(404).end();
      return;
    }
    response.setHeader('Content-Type', 'text/javascript');
    response.end(await readFile(file));
  } catch {
    response.writeHead(404).end();
  }
});

/** Only demo.js, the shared example policies, and built .js files under the two dist directories are served. */
function resolveModule(path) {
  if (path === '/demo.js') return resolve(here, 'demo.js');
  for (const [prefix, base] of Object.entries(modules)) {
    if (!path.startsWith(prefix)) continue;
    const file = resolve(base, decodeURIComponent(path.slice(prefix.length)));
    return file.startsWith(base + sep) && file.endsWith('.js') ? file : undefined;
  }
  return undefined;
}

async function readToken() {
  if (process.env.RELAY_TOKEN) return process.env.RELAY_TOKEN;
  const tty = process.stdin.isTTY;
  const wasRaw = process.stdin.isRaw ?? false;
  const input = createInterface({ input: process.stdin, terminal: false });
  const interrupt = (chunk) => {
    if (chunk.includes(3) || chunk.includes(4)) input.close(); // Ctrl-C / Ctrl-D
  };
  if (tty) {
    process.stdin.setRawMode(true);
    process.stdin.on('data', interrupt);
    process.stdout.write('Relay token (input hidden): ');
  }
  try {
    for await (const line of input) return line.trim();
    return undefined;
  } finally {
    input.close();
    if (tty) {
      process.stdin.off('data', interrupt);
      process.stdin.setRawMode(wasRaw);
      process.stdout.write('\n');
    }
  }
}

await new Promise((listening) => server.listen(0, '127.0.0.1', listening));
let browser;
try {
  browser = await chromium.launch({ headless: true });
  const tab = await browser.newPage();
  tab.on('pageerror', (error) => print({ event: 'page error', message: error.message }));
  tab.on('console', (message) => {
    if (message.type() === 'error' || message.type() === 'warning') {
      print({ event: `console ${message.type()}`, message: message.text() });
    }
  });
  await tab.exposeFunction('relayLog', print);
  await tab.goto(`http://127.0.0.1:${server.address().port}/`);
  print({
    event: 'starting',
    mode: config.mode,
    namespace: namespace.join('/'),
    track: config.track,
    count: config.count,
  });
  const result = await tab.evaluate(
    async (config) => {
      if (typeof WebTransport === 'undefined') throw new Error('This browser does not provide WebTransport');
      const { runDemo } = await import('/demo.js');
      return runDemo(config, (entry) => void window.relayLog(entry));
    },
    { ...config, token },
  );
  print({ event: 'finished', ...result });
} catch (error) {
  // Playwright prefixes page errors with the call site and appends the page stack; keep the message itself.
  const message = (error instanceof Error ? error.message : String(error))
    .replace(/^page\.evaluate: (Error: )?/, '')
    .split('\n')[0];
  console.error(redact(format({ event: 'failed', error: message })));
  process.exitCode = 1;
} finally {
  await browser?.close();
  await new Promise((closed) => server.close(closed));
}
