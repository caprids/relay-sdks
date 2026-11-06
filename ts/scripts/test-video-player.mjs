import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { DEFAULT_ENDPOINT } from '@caprid/relay';
import { startModuleServer } from './module-server.js';

// The live video player test (issue #478): scripts/video-player/scenario.ts publishes the WebCodecs example's canvas
// and microphone and plays the video back through createVideoPlayer in headless Chromium: on a wall-paced clock, on
// the audio listener's clock, and in a dedicated worker. VIDEO_PLAYER_RUNS lists the transports, default "hub" (the
// in-memory relay hub inside the page); "relay" is the production Relay (RELAY_URL, RELAY_TOKEN,
// RELAY_NAMESPACE_PREFIX). VIDEO_PLAYER_SECONDS sets how much of the broadcast timeline is measured.

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const endpoint = process.env.RELAY_URL ?? DEFAULT_ENDPOINT;
const token = process.env.RELAY_TOKEN;
const prefix = (process.env.RELAY_NAMESPACE_PREFIX ?? 'relay-sdk-smoke').split('/').filter(Boolean);
const seconds = Number(process.env.VIDEO_PLAYER_SECONDS ?? 8);
const runs = (process.env.VIDEO_PLAYER_RUNS ?? 'hub').split(',');
for (const run of runs) assert(['relay', 'hub'].includes(run), `Unknown transport in VIDEO_PLAYER_RUNS: ${run}`);
if (runs.includes('relay')) assert(token, 'RELAY_TOKEN is required for a relay run');
const redact = (value) => (token ? String(value).replaceAll(token, '[redacted]') : String(value));
const report = (value) => console.log(redact(typeof value === 'string' ? value : JSON.stringify(value)));

const server = await startModuleServer({
  modules: {
    '/sdk/': resolve(root, 'dist'),
    '/packages/': resolve(root, 'packages'),
    '/examples/': resolve(root, 'examples'),
    '/scripts/': resolve(root, 'scripts'),
    '/tests/helpers/': resolve(root, 'tests/helpers'),
  },
  importMap: {
    '@caprid/relay': '/sdk/index.js',
    '@caprid/relay/media': '/sdk/media.js',
    '@caprid/moq-core': '/packages/moq-core/dist/index.js',
    '@caprid/moq-media': '/packages/moq-media/dist/index.js',
  },
  transpile: true,
});
const { chromium } = await import('playwright');

// What a pass needs, for every player: at least 90% of the frames published in the window shown, no error, a first
// frame shown, the publisher's two colors on its canvas (H.264 moves a flat color by a few levels), and every frame a
// decoder output closed exactly once. The audio player's clock is driven by audio for nearly the whole window.
const TOLERANCE = 24;
const checks = (result) => {
  const failed = [];
  const check = (ok, name) => ok || failed.push(name);
  for (const [name, player] of Object.entries(result.players)) {
    check(player.shownRatio >= 0.9, `${name}: shown ${player.shownRatio} of ${result.published} published`);
    check(player.stats.errors === 0, `${name}: ${player.stats.errors} errors`);
    check(player.joinMs.firstShown !== null, `${name}: no first frame shown`);
    const picture = player.pixels.every((pixel, index) =>
      pixel.every((value, channel) => Math.abs(value - result.expectedPixels[index][channel]) <= TOLERANCE),
    );
    check(picture, `${name}: picture ${JSON.stringify(player.pixels)} is not the publisher's`);
  }
  const { worker, audio } = result.players;
  check(worker.inWorker, 'worker: did not run in a worker');
  for (const [name, counts] of [
    ['page', result.counts],
    ['worker', worker.counts],
  ]) {
    check(counts.output > 0 && counts.closed === counts.output, `${name}: ${counts.closed} of ${counts.output} closed`);
    check(counts.closedAgain === 0, `${name}: ${counts.closedAgain} frames closed twice`);
  }
  const ticks = Object.values(audio.modes).reduce((sum, count) => sum + count, 0);
  check((audio.modes.audio ?? 0) >= ticks * 0.9, `audio: clock modes ${JSON.stringify(audio.modes)}`);
  return failed;
};

let failed = false;
try {
  for (const transport of runs) {
    const namespace = [...prefix, `video-player-${randomUUID()}`];
    const browser = await chromium.launch({
      headless: true,
      args: ['--autoplay-policy=no-user-gesture-required'],
    });
    try {
      const page = await browser.newPage();
      page.on('pageerror', (error) => report({ transport, pageerror: error.message }));
      await page.exposeFunction('reportPhase', (value) => report({ transport, ...value }));
      await page.goto(`http://127.0.0.1:${server.address().port}/`);
      const result = await page.evaluate(
        async ({ transport, endpoint, token, namespace, seconds }) => {
          const { runVideoPlayerScenario } = await import('/scripts/video-player/scenario.js');
          let scenarioTransport = { kind: 'relay', endpoint, token };
          if (transport === 'hub') {
            const { createRelayHub } = await import('/tests/helpers/relay-hub.js');
            const hub = createRelayHub();
            scenarioTransport = { kind: 'hub', publisher: hub.connectPublisher, subscriber: hub.connectSubscriber };
          }
          try {
            return await runVideoPlayerScenario({
              transport: scenarioTransport,
              namespace,
              seconds,
              report: window.reportPhase,
            });
          } catch (error) {
            throw new Error(error instanceof Error ? `${error.name}: ${error.message}` : String(error));
          }
        },
        { transport, endpoint, token, namespace, seconds },
      );
      const failures = checks(result);
      failed ||= failures.length > 0;
      report({ transport, result: failures.length === 0 ? 'PASS' : 'FAIL', failures, ...result });
    } catch (error) {
      failed = true;
      report({ transport, result: 'FAIL', error: redact(error instanceof Error ? error.message : error) });
    } finally {
      await browser.close();
    }
  }
} finally {
  await new Promise((resolve) => server.close(resolve));
}
process.exitCode = failed ? 1 : 0;
