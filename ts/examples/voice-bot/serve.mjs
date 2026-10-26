import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { roomEndpoint } from '@caprid/relay';
import { startModuleServer } from '../../scripts/module-server.js';

// Serve the voice bot page on http://127.0.0.1 (a secure context, as WebTransport needs). The page talks to the bot
// in python/examples/voice_bot over the Relay at RELAY_URL, in the conversation --namespace names (default
// RELAY_NAMESPACE_PREFIX or relay-sdk-example, plus voice-bot, as the bot's default). With RELAY_TOKEN set, the page
// holds the token; the server listens on the loopback interface only, and never prints it.

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const { values } = parseArgs({
  options: { room: { type: 'string' }, namespace: { type: 'string' }, port: { type: 'string', default: '8320' } },
});
const namespace = (values.namespace ?? `${process.env.RELAY_NAMESPACE_PREFIX ?? 'relay-sdk-example'}/voice-bot`)
  .split('/')
  .filter(Boolean);
const relayUrl = process.env.RELAY_URL ?? 'https://relay.mux.global';
const config = {
  // --room roots RELAY_URL at its slash-separated fields, as the bot's --room does; --namespace is then relative to them.
  url: values.room === undefined ? relayUrl : roomEndpoint(relayUrl, values.room.split('/').filter(Boolean)),
  namespace,
  ...(process.env.RELAY_TOKEN ? { token: process.env.RELAY_TOKEN } : {}),
};
// The config goes into a script element: escape what could close it.
const json = JSON.stringify(config).replaceAll('<', '\\u003c');
const body = `
<title>Voice bot</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  body { font: 15px/1.5 system-ui, sans-serif; max-width: 40rem; margin: 2rem auto; padding: 0 1rem; }
  button { font: inherit; padding: 0.4rem 1rem; margin-right: 0.5rem; }
  input { font: inherit; width: 100%; box-sizing: border-box; }
  pre { background: #f4f4f4; padding: 0.75rem; min-height: 8rem; white-space: pre-wrap; }
</style>
<h1>Voice bot</h1>
<p>Talking to <code id="where"></code>. Start the bot with <code>mise run python:example:voice-bot</code>.</p>
<label>Relay token <input id="token" type="password" autocomplete="off"></label>
<p><button id="talk">Talk</button><button id="hang-up" disabled>Hang up</button></p>
<p id="status"></p>
<pre id="said"></pre>
<pre id="log"></pre>
<script>window.voiceBot = ${json};</script>
<script type="module" src="/examples/voice-bot/page.js"></script>`;

const server = await startModuleServer({
  modules: {
    '/sdk/': resolve(root, 'dist'),
    '/packages/': resolve(root, 'packages'),
    '/examples/': resolve(root, 'examples'),
  },
  importMap: {
    '@caprid/relay': '/sdk/index.js',
    '@caprid/relay/media': '/sdk/media.js',
    '@caprid/moq-core': '/packages/moq-core/dist/index.js',
    '@caprid/moq-media': '/packages/moq-media/dist/index.js',
  },
  transpile: true,
  body,
  port: Number(values.port),
});
console.log(`voice bot page: http://127.0.0.1:${server.address().port}/ (${namespace.join('/')} on ${config.url})`);
