// Opt-in live check of the publisher's send order (spec/behavior.md, "Send order") against a real relay. Run after
// `pnpm build` and `pnpm --filter @mux/relay-quic build:native`.
//
// A headless browser page (SEND_ORDER_BROWSER: firefox, the default, or chromium) publishes one audio and one video track, registered with mediaTrackProperties, through
// frame writers: 20 ms audio frames of SEND_ORDER_AUDIO_BYTES (default 160), one group per frame, and 30 fps video of
// SEND_ORDER_DELTA_BYTES (default 3000) per delta with a SEND_ORDER_KEY_BYTES (default 40000) keyframe every second,
// about 1 Mbit/s. Every payload starts with its capture time (Date.now(), float64). Point RELAY_URL through an uplink
// impairer, so the browser's sender holds a queue. A subscriber in this process, on RELAY_SUB_URL (default RELAY_URL;
// give it a clean path), measures each audio frame's capture-to-arrival latency; both ends share the host's clock.
// SEND_ORDER=off strips the send order from every stream the page opens, which is the publisher before #336, so one
// build measures both. SEND_ORDER=invert turns the media streams' order around, video ahead of audio: a check that the
// browser honours send order at all. It prints a summary of audio latency after a warm-up, and how much video arrived.
//
// Environment: RELAY_URL, RELAY_SUB_URL, RELAY_TOKEN, RELAY_NAMESPACE_PREFIX, SEND_ORDER (on, off or invert; default on),
// SEND_ORDER_RUN_S (default 25), SEND_ORDER_WARM_UP_S (default 5), SEND_ORDER_BROWSER, SEND_ORDER_RESOLVE (an address
// the browser resolves RELAY_URL's host to, for an impairer on a local port).
//
// Firefox 155 honours send order (WebTransportSendStream.sendOrder); Chromium 153 has no WebTransportSendStream and
// drops the option, so on Chromium on, off and invert measure the same.
//
// The run behind spec/behavior.md's figures put the shared UDP impairment proxy (impair.py, stdlib Python) between
// the page and sjc.relay.mux.global, the browser resolving the relay to it, and the subscriber direct:
//
//   python3 impair.py --port 14433 --set '{"up":{"rate_kbps":800,"queue_ms":300}}' &
//   RELAY_URL=https://sjc.relay.mux.global:14433 RELAY_SUB_URL=https://sjc.relay.mux.global \
//     SEND_ORDER_RESOLVE=127.0.0.1 \
//     node scripts/probe-send-order.mjs
import { randomUUID } from 'node:crypto';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { connectSupervisedSubscriber } from '@mux/relay/node';
import { startModuleServer } from './module-server.js';

const env = process.env;
const url = env.RELAY_URL;
if (!url) throw new Error('RELAY_URL is required');
const subUrl = env.RELAY_SUB_URL ?? url;
const token = env.RELAY_TOKEN;
if (!token) throw new Error('RELAY_TOKEN is required');
const sendOrder = env.SEND_ORDER ?? 'on';
if (!['on', 'off', 'invert'].includes(sendOrder)) throw new Error('SEND_ORDER must be on, off or invert');
const runMs = Number(env.SEND_ORDER_RUN_S ?? 25) * 1000;
const warmUpMs = Number(env.SEND_ORDER_WARM_UP_S ?? 5) * 1000;
const sizes = {
  audio: Number(env.SEND_ORDER_AUDIO_BYTES ?? 160),
  delta: Number(env.SEND_ORDER_DELTA_BYTES ?? 3000),
  key: Number(env.SEND_ORDER_KEY_BYTES ?? 40000),
};
const namespace = [...(env.RELAY_NAMESPACE_PREFIX ?? 'relay-sdk-probe').split('/').filter(Boolean), randomUUID()];
const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');

const server = await startModuleServer({
  modules: { '/sdk/': resolve(root, 'dist'), '/packages/': resolve(root, 'packages') },
  importMap: {
    '@mux/relay': '/sdk/index.js',
    '@mux/relay/media': '/sdk/media.js',
    '@mux/moq-core': '/packages/moq-core/dist/index.js',
  },
});
const browserName = env.SEND_ORDER_BROWSER ?? 'firefox';
if (!['firefox', 'chromium'].includes(browserName)) throw new Error('SEND_ORDER_BROWSER must be firefox or chromium');
const resolveTo = env.SEND_ORDER_RESOLVE;
const playwright = await import('playwright');
const browser = await playwright[browserName].launch({
  headless: true,
  ...(resolveTo === undefined
    ? {}
    : browserName === 'firefox'
      ? { firefoxUserPrefs: { 'network.dns.forceResolve': resolveTo } }
      : { args: [`--host-resolver-rules=MAP ${new URL(url).hostname} ${resolveTo}`] }),
});
const browserVersion = `${browserName} ${browser.version()}`;
const arrivals = { audio: [], video: [] }; // [captureMs, arrivalMs]
const subscriber = await connectSupervisedSubscriber({ url: subUrl, getToken: () => token });
const capture = (payload) => new DataView(payload.buffer, payload.byteOffset, 8).getFloat64(0);
for (const track of ['audio', 'video']) {
  subscriber.want(
    track,
    { namespace, track },
    { onObject: (object) => arrivals[track].push([capture(object.payload), Date.now()]) },
  );
}

let page;
let outcomes;
try {
  page = await browser.newPage();
  page.on('console', (message) => console.error(`[page] ${message.text()}`));
  await page.goto(`http://127.0.0.1:${server.address().port}/`);
  outcomes = await page.evaluate(
    async ({ url, token, namespace, sendOrder, runMs, sizes }) => {
      if (sendOrder !== 'on') {
        const create = WebTransport.prototype.createUnidirectionalStream;
        WebTransport.prototype.createUnidirectionalStream = function (options) {
          const { sendOrder: order, ...rest } = options ?? {};
          // Inverted, -17 and -97 become -240 and -160: still below the control streams, video now first.
          if (sendOrder === 'invert' && order !== undefined) rest.sendOrder = -257 - order;
          return create.call(this, rest);
        };
      }
      const { connectRelayPublisher } = await import('@mux/relay');
      const { createFrameWriter, mediaTrackProperties } = await import('@mux/relay/media');
      const publisher = await connectRelayPublisher({ url, namespace, getToken: () => token });
      const audio = createFrameWriter(
        publisher.registerTrack({ track: 'audio', trackProperties: mediaTrackProperties('audio') }),
        { grouping: 'frame' },
      );
      const video = createFrameWriter(
        publisher.registerTrack({
          track: 'video',
          trackProperties: mediaTrackProperties('video', { keyframeIntervalUs: 1_000_000 }),
        }),
      );
      await publisher.announced;
      const counts = {};
      const count = (track) => (outcome) => {
        const key = `${track}:${outcome.kind}`;
        counts[key] = (counts[key] ?? 0) + 1;
      };
      const frame = (bytes, key) => {
        const payload = new Uint8Array(bytes);
        new DataView(payload.buffer).setFloat64(0, Date.now());
        return { payload, key };
      };
      const start = performance.now();
      let videoFrames = 0;
      const timers = [
        setInterval(() => void audio.write(frame(sizes.audio, true)).then(count('audio')), 20),
        setInterval(() => {
          const key = videoFrames++ % 30 === 0;
          void video.write(frame(key ? sizes.key : sizes.delta, key)).then(count('video'));
        }, 1000 / 30),
      ];
      await new Promise((resolve) => setTimeout(resolve, runMs - (performance.now() - start)));
      for (const timer of timers) clearInterval(timer);
      await new Promise((resolve) => setTimeout(resolve, 2_000));
      await Promise.all([audio.close(), video.close()]);
      await publisher.close();
      return counts;
    },
    { url, token, namespace, sendOrder, runMs, sizes },
  );
} finally {
  await browser.close();
  subscriber.close();
  server.close();
}

const measured = (track) => {
  const after = arrivals[track].filter(([captured]) => captured >= arrivals[track][0]?.[0] + warmUpMs);
  const latencies = after.map(([captured, at]) => at - captured).sort((a, b) => a - b);
  const at = (p) => (latencies.length === 0 ? null : Math.round(latencies[Math.floor((latencies.length - 1) * p)]));
  return { received: after.length, p50Ms: at(0.5), p95Ms: at(0.95), p99Ms: at(0.99), maxMs: at(1) };
};
console.log(
  JSON.stringify({
    result: 'MEASURED',
    browser: browserVersion,
    sendOrder,
    warmUpMs,
    runMs,
    sizes,
    audio: measured('audio'),
    video: measured('video'),
    outcomes,
  }),
);
