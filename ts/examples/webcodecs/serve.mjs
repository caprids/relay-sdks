import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { DEFAULT_ENDPOINT, roomEndpoint } from '@caprid/relay';
import { startModuleServer } from '../../scripts/module-server.js';

// Serve the WebCodecs example page on http://127.0.0.1 (a secure context, as WebTransport needs). The page publishes a
// test pattern and the microphone, and watches the broadcast, in the same tab or another, over the Relay at RELAY_URL,
// at --namespace (default RELAY_NAMESPACE_PREFIX or relay-sdk-example, plus webcodecs). Open it with ?watch to start
// watching without a click. With RELAY_TOKEN set, the page holds the token; the server listens on the loopback
// interface only, and never prints it.

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const { values } = parseArgs({
  options: { room: { type: 'string' }, namespace: { type: 'string' }, port: { type: 'string', default: '8321' } },
});
const namespace = (values.namespace ?? `${process.env.RELAY_NAMESPACE_PREFIX ?? 'relay-sdk-example'}/webcodecs`)
  .split('/')
  .filter(Boolean);
const relayUrl = process.env.RELAY_URL ?? DEFAULT_ENDPOINT;
const config = {
  // --room roots RELAY_URL at its slash-separated fields; --namespace is then relative to them.
  url: values.room === undefined ? relayUrl : roomEndpoint(relayUrl, values.room.split('/').filter(Boolean)),
  namespace,
  ...(process.env.RELAY_TOKEN ? { token: process.env.RELAY_TOKEN } : {}),
};
// The config goes into a script element: escape what could close it.
const json = JSON.stringify(config).replaceAll('<', '\\u003c');
const body = `
<title>WebCodecs example</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  body { font: 15px/1.5 system-ui, sans-serif; max-width: 44rem; margin: 2rem auto; padding: 0 1rem; }
  button { font: inherit; padding: 0.4rem 1rem; margin-right: 0.5rem; }
  input { font: inherit; width: 100%; box-sizing: border-box; }
  canvas { width: 100%; height: auto; background: #000; display: block; }
  pre { background: #f4f4f4; padding: 0.75rem; min-height: 6rem; white-space: pre-wrap; }
</style>
<h1>WebCodecs example</h1>
<p>Broadcast <code id="where"></code>. Publish here, and watch here or in another tab
(<a id="watch-link" target="_blank">open a watcher</a>).</p>
<label>Relay token <input id="token" type="password" autocomplete="off"></label>
<h2>Publish</h2>
<p><button id="publish">Publish</button><button id="stop-publishing" disabled>Stop</button></p>
<canvas id="source" width="640" height="360"></canvas>
<h2>Watch</h2>
<p><button id="watch">Watch</button><button id="unmute" disabled>Unmute</button><button id="stop-watching" disabled>Stop</button></p>
<canvas id="watched"></canvas>
<p id="status"></p>
<pre id="log"></pre>
<script>window.webcodecs = ${json};</script>
<script type="module" src="/examples/webcodecs/page.js"></script>`;

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
console.log(
  `WebCodecs example page: http://127.0.0.1:${server.address().port}/ (${namespace.join('/')} on ${config.url})`,
);
