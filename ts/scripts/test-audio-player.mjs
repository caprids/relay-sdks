import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { startModuleServer } from './module-server.js';

// The live audio player test (issue #314): scripts/audio-player/scenario.ts publishes the WebCodecs example's
// microphone and plays it back through the audio-only listener example, in headless Chromium. AUDIO_PLAYER_RUNS
// lists the transports, default "hub" (the in-memory relay hub inside the page); "relay" is the production Relay
// (RELAY_URL, RELAY_TOKEN, RELAY_NAMESPACE_PREFIX). AUDIO_PLAYER_SECONDS sets the steady phase's length.

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const endpoint = process.env.RELAY_URL ?? 'https://sjc.relay.mux.global';
const token = process.env.RELAY_TOKEN;
const prefix = (process.env.RELAY_NAMESPACE_PREFIX ?? 'relay-sdk-smoke').split('/').filter(Boolean);
const seconds = Number(process.env.AUDIO_PLAYER_SECONDS ?? 8);
const runs = (process.env.AUDIO_PLAYER_RUNS ?? 'hub').split(',');
for (const run of runs) assert(['relay', 'hub'].includes(run), `Unknown transport in AUDIO_PLAYER_RUNS: ${run}`);
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
    '@mux/relay': '/sdk/index.js',
    '@mux/relay/media': '/sdk/media.js',
    '@mux/moq-core': '/packages/moq-core/dist/index.js',
    '@mux/moq-media': '/packages/moq-media/dist/index.js',
  },
  transpile: true,
});
const { chromium } = await import('playwright');

// What a pass needs: steady playback with the clock driven by audio, no underrun, the buffer near its target, and
// audio flowing again after the cut.
const verdict = ({ phases }) => {
  const steady = phases.find((phase) => phase.phase === 'steady');
  const cut = phases.find((phase) => phase.phase === 'cut');
  const ticks = Object.values(steady.modes).reduce((sum, count) => sum + count, 0);
  return (
    steady.decoded >= seconds * 45 &&
    steady.underruns === 0 &&
    (steady.modes.audio ?? 0) >= ticks * 0.95 &&
    steady.bufferedMs.p95 <= steady.jitterTargetMs * 2 + 60 &&
    cut.cutMs > 0 &&
    cut.decoded > 0 &&
    (cut.modes.audio ?? 0) > 0
  );
};

let failed = false;
try {
  for (const transport of runs) {
    const namespace = [...prefix, `audio-player-${randomUUID()}`];
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
          const { runAudioPlayerScenario } = await import('/scripts/audio-player/scenario.js');
          let scenarioTransport = { kind: 'relay', endpoint, token };
          if (transport === 'hub') {
            const { createRelayHub } = await import('/tests/helpers/relay-hub.js');
            const hub = createRelayHub();
            scenarioTransport = { kind: 'hub', publisher: hub.connectPublisher, subscriber: hub.connectSubscriber };
          }
          try {
            return await runAudioPlayerScenario({
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
      const ok = verdict(result);
      failed ||= !ok;
      report({ transport, result: ok ? 'PASS' : 'FAIL', sequencer: result.sequencer, stats: result.stats });
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
