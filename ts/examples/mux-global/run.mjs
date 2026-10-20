#!/usr/bin/env node
/**
 * Run the Relay example (demo.js) against a live Relay in Node.
 *
 * Node has no WebTransport, so the connections use @mux/relay-quic, which carries MOQT over native
 * QUIC. This parses the options, reads the token, and prints demo.js's log entries. The SDK calls
 * live in demo.js.
 *
 *   cd ts && pnpm build && pnpm --filter @mux/relay-quic build:native
 *   # After the README's endpoint/namespace setup; enter the token at the hidden prompt.
 *   node examples/mux-global/run.mjs --url "$RELAY_URL" --namespace "$RELAY_NAMESPACE"
 */
import { randomUUID } from 'node:crypto';
import { access } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { createInterface } from 'node:readline/promises';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, '../..');
/** The built packages, which must exist. */
const built = [
  resolve(root, 'dist'),
  resolve(root, 'packages/moq-core/dist'),
  resolve(root, 'packages/relay-quic/dist'),
];
const usage = `Usage: node examples/mux-global/run.mjs [options]

  --url <https url>      Relay endpoint (default RELAY_URL or https://sjc.relay.mux.global)
  --room <a/b/c>         Root --url at these slash-separated fields with roomEndpoint, such as the
                         token's root on the bare host; --namespace is then relative to them
  --namespace <a/b/c>    Namespace, slash-separated, relative to the --url path (on the bare
                         host, include the token's root prefix)
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
      room: { type: 'string' },
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

await Promise.all(built.map((directory) => access(resolve(directory, 'index.js')))).catch(() =>
  fail('The SDK is not built yet; run `pnpm build` first'),
);
// Imported once the build is known to exist. The factory throws at once, naming the build command, when the
// native addon has not been built for this platform.
const { nativeQuicTransport } = await import('@mux/relay-quic');
const { runDemo } = await import('./demo.js');
const { roomEndpoint } = await import('@mux/relay');
if (args.room !== undefined) {
  try {
    config.url = roomEndpoint(config.url, args.room.split('/').filter(Boolean));
  } catch (error) {
    fail(`--room: ${error.message}`);
  }
}
let createTransport;
try {
  createTransport = nativeQuicTransport();
} catch (error) {
  fail(error.message);
}

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

try {
  print({
    event: 'starting',
    mode: config.mode,
    namespace: namespace.join('/'),
    track: config.track,
    count: config.count,
  });
  const result = await runDemo({ ...config, token, createTransport }, print);
  print({ event: 'finished', ...result });
} catch (error) {
  console.error(redact(format({ event: 'failed', error: error instanceof Error ? error.message : String(error) })));
  process.exitCode = 1;
}
