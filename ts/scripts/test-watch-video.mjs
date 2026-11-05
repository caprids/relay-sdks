import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { DEFAULT_ENDPOINT } from '@caprid/relay';
import { startModuleServer } from './module-server.js';

// The live watchVideo test (issue #443): scripts/watch-video/scenario.ts publishes the WebCodecs example's test pattern
// and a synthetic microphone, and watches them with watchVideo and listenAudio on one clock, in the same page, in
// headless Chromium, through the Relay (RELAY_URL, RELAY_TOKEN, RELAY_NAMESPACE_PREFIX). The watcher's AudioContext
// starts suspended, is resumed, and is then muted. WATCH_VIDEO_PHASE_SECONDS sets each phase's length; WATCH_VIDEO_OUT
// writes the full result as JSON.

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const endpoint = process.env.RELAY_URL ?? DEFAULT_ENDPOINT;
assert.equal(new URL(endpoint).protocol, 'https:', 'RELAY_URL must be an https URL');
const token = process.env.RELAY_TOKEN;
assert(token, 'RELAY_TOKEN is required');
const prefix = (process.env.RELAY_NAMESPACE_PREFIX ?? 'relay-sdk-smoke').split('/').filter(Boolean);
const phaseSeconds = Number(process.env.WATCH_VIDEO_PHASE_SECONDS ?? 10);
const redact = (value) => String(value).replaceAll(token, '[redacted]');
const report = (value) => console.log(redact(typeof value === 'string' ? value : JSON.stringify(value)));

// The A/V sync test's limits (ITU-R BT.1359, EBU R37): every settled offset within ±45 ms and the mean no more than
// 5 ms early. Without audio, video keeps moving with no pause longer than 100 ms.
const LIMIT_MS = 45;
const EARLY_MEAN_LIMIT_MS = 5;
const MAX_STALL_MS = 100;
const ticks = (modes) => Object.values(modes).reduce((sum, count) => sum + count, 0);
const verdicts = {
  // No audio reaches the clock while the context is suspended: wall pacing, and video moves.
  suspended: (phase) =>
    phase.contextState === 'suspended' &&
    (phase.modes.audio ?? 0) === 0 &&
    (phase.modes.wall ?? 0) > 0 &&
    phase.shown > 0 &&
    phase.longestGapMs <= MAX_STALL_MS,
  // Audio plays and is the master; flashes and beeps line up.
  resumed: (phase) =>
    (phase.modes.audio ?? 0) >= 0.9 * ticks(phase.modes) &&
    phase.sync !== undefined &&
    phase.sync.count >= 5 &&
    phase.sync.max <= LIMIT_MS &&
    phase.sync.min >= -LIMIT_MS &&
    phase.sync.mean >= -EARLY_MEAN_LIMIT_MS,
  // Muted by gain: reports keep flowing, so audio stays the master, and video keeps moving.
  muted: (phase) =>
    (phase.modes.wall ?? 0) === 0 &&
    (phase.modes.audio ?? 0) > 0 &&
    phase.shown > 0 &&
    phase.longestGapMs <= MAX_STALL_MS,
};

const server = await startModuleServer({
  modules: {
    '/sdk/': resolve(root, 'dist'),
    '/packages/': resolve(root, 'packages'),
    '/examples/': resolve(root, 'examples'),
    '/scripts/': resolve(root, 'scripts'),
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
let failed = true;
// One browser: more than two Chromium instances freeze the host.
const browser = await chromium.launch({
  headless: true,
  args: ['--autoplay-policy=no-user-gesture-required', '--use-fake-ui-for-media-stream'],
});
try {
  const page = await browser.newPage();
  page.on('pageerror', (error) => report({ pageerror: error.message }));
  await page.exposeFunction('reportPhase', (value) => report(value));
  await page.goto(`http://127.0.0.1:${server.address().port}/`);
  const namespace = [...prefix, `watch-video-${randomUUID()}`];
  // A page that stops responding would otherwise hang the run forever.
  const budgetMs = (3 * phaseSeconds + 60) * 1000;
  let watchdog;
  const expired = new Promise((_, reject) => {
    watchdog = setTimeout(() => reject(new Error(`No result within ${budgetMs / 1000} s`)), budgetMs);
  });
  const evaluation = page.evaluate(
    async ({ endpoint, token, namespace, phaseSeconds }) => {
      const { runWatchVideoScenario } = await import('/scripts/watch-video/scenario.js');
      try {
        return await runWatchVideoScenario({ endpoint, token, namespace, phaseSeconds, report: window.reportPhase });
      } catch (error) {
        // Only the message crosses page.evaluate; keep the name, the code and the cause in it.
        const describe = (value) =>
          value instanceof Error
            ? `${value.name}${value.code ? ` ${value.code}` : ''}: ${value.message}${value.cause ? ` (cause: ${describe(value.cause)})` : ''}`
            : String(value);
        throw new Error(describe(error));
      }
    },
    { endpoint, token, namespace, phaseSeconds },
  );
  evaluation.catch(() => {});
  const result = await Promise.race([evaluation, expired]).finally(() => clearTimeout(watchdog));
  const phases = result.phases.map((phase) => ({ result: verdicts[phase.phase](phase) ? 'PASS' : 'FAIL', ...phase }));
  for (const phase of phases) report(phase);
  failed = phases.some((phase) => phase.result === 'FAIL');
  const { phases: _, ...summary } = result;
  report({ result: failed ? 'FAIL' : 'PASS', ...summary });
  if (process.env.WATCH_VIDEO_OUT)
    await writeFile(process.env.WATCH_VIDEO_OUT, redact(JSON.stringify(result, null, 2)));
} catch (error) {
  report({ result: 'FAIL', error: redact(error instanceof Error ? error.message : error) });
} finally {
  await browser.close();
  await new Promise((resolve) => server.close(resolve));
}
process.exitCode = failed ? 1 : 0;
