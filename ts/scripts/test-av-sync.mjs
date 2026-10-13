import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { startModuleServer } from './module-server.js';

// The live A/V sync test (issue #288): scripts/av-sync/scenario.ts publishes the WebCodecs example's canvas and
// microphone and plays them back in the same page, once per run, one browser at a time.
//
// AV_SYNC_RUNS lists the runs as browser:transport, default "chromium:relay,firefox:hub". `relay` is the production
// Relay (RELAY_URL, RELAY_TOKEN, RELAY_NAMESPACE_PREFIX); `hub` is the in-memory relay hub inside the page. Firefox
// (148) cannot reach the Relay: its WebTransport offers no protocols (no `protocol` attribute), and the SDK refuses a
// session that negotiates none (spec/transport-adapter.md, TA-7). AV_SYNC_REPORT picks how the player reports audio:
// `render-clock` (default) or `output-timestamp`. AV_SYNC_PHASE_SECONDS sets each phase's length; AV_SYNC_OUT writes
// the full results as JSON.
//
// Firefox's AudioContext stays suspended without an audio server; on Linux run PulseAudio (it falls back to a null
// sink), which this launcher starts when `pulseaudio` is installed.

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const endpoint = process.env.RELAY_URL ?? 'https://sjc.relay.mux.global';
assert.equal(new URL(endpoint).protocol, 'https:', 'RELAY_URL must be an https URL');
const token = process.env.RELAY_TOKEN;
const prefix = (process.env.RELAY_NAMESPACE_PREFIX ?? 'relay-sdk-smoke').split('/').filter(Boolean);
const reportMethod = process.env.AV_SYNC_REPORT ?? 'render-clock';
assert(['render-clock', 'output-timestamp'].includes(reportMethod), 'AV_SYNC_REPORT: render-clock or output-timestamp');
const phaseSeconds = Number(process.env.AV_SYNC_PHASE_SECONDS ?? 10);
// The player's audio jitter buffer. Firefox's (148, Linux) H.264 VideoEncoder holds about 41 frames, 1.4 s, even with
// latencyMode 'realtime' (VP8, VP9 and AV1 do not), so its video arrives that much after its audio. The playout clock
// keeps audio the master and drops late video; the audio path has to wait for the video instead.
const jitterMsFor = (browser) => Number(process.env.AV_SYNC_JITTER_MS ?? (browser === 'firefox' ? 1600 : 150));
const runs = (process.env.AV_SYNC_RUNS ?? 'chromium:relay,firefox:hub').split(',').map((run) => {
  const [browser, transport] = run.split(':');
  assert(['chromium', 'firefox', 'webkit'].includes(browser), `Unknown browser in AV_SYNC_RUNS: ${browser}`);
  assert(['relay', 'hub'].includes(transport), `Unknown transport in AV_SYNC_RUNS: ${transport}`);
  return { browser, transport };
});
if (runs.some(({ transport }) => transport === 'relay')) assert(token, 'RELAY_TOKEN is required for a relay run');
const redact = (value) => (token ? String(value).replaceAll(token, '[redacted]') : String(value));
const report = (value) => console.log(redact(typeof value === 'string' ? value : JSON.stringify(value)));

// The target (ITU-R BT.1359, EBU R37): audio within ±45 ms of video, and not audibly early. A settled phase passes when
// every offset is within ±45 ms and their mean is no more than 5 ms early: the renderer's timers add a few ms of
// scatter to each offset, in both directions, while a real lead would move the mean.
const LIMIT_MS = 45;
const EARLY_MEAN_LIMIT_MS = 5;
// The longest pause between two shown frames while audio is missing.
const MAX_STALL_MS = 100;

if (runs.some(({ browser }) => browser === 'firefox')) {
  try {
    execFileSync('pulseaudio', ['--check']);
  } catch {
    try {
      execFileSync('pulseaudio', ['--start', '--exit-idle-time=-1']);
    } catch {
      report('PulseAudio is not available; Firefox will keep its AudioContext suspended');
    }
  }
}

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
    '@mux/moq-core': '/packages/moq-core/dist/index.js',
    '@mux/moq-media': '/packages/moq-media/dist/index.js',
  },
  transpile: true,
});
const { chromium, firefox, webkit } = await import('playwright');
const launchers = {
  chromium: () =>
    chromium.launch({
      headless: true,
      args: ['--autoplay-policy=no-user-gesture-required', '--use-fake-ui-for-media-stream'],
    }),
  firefox: () =>
    firefox.launch({
      headless: true,
      firefoxUserPrefs: { 'media.autoplay.default': 0, 'media.autoplay.block-webaudio': false },
    }),
  webkit: () => webkit.launch({ headless: true }),
};

const results = [];
let failed = false;
try {
  for (const { browser: name, transport } of runs) {
    const runtime = `${name}:${transport}`;
    const namespace = [...prefix, `av-sync-${randomUUID()}`];
    // One browser at a time: more than two Chromium instances freeze the host.
    const browser = await launchers[name]();
    try {
      const page = await browser.newPage();
      page.on('pageerror', (error) => report({ runtime, pageerror: error.message }));
      await page.exposeFunction('reportPhase', (value) => report({ runtime, ...value }));
      await page.goto(`http://127.0.0.1:${server.address().port}/`);
      // A page that stops responding, or an AudioContext that never starts, would otherwise hang the run forever.
      const budgetMs = (6 * phaseSeconds + 60) * 1000 + jitterMsFor(name);
      let watchdog;
      const expired = new Promise((_, reject) => {
        watchdog = setTimeout(() => reject(new Error(`No result within ${budgetMs / 1000} s`)), budgetMs);
      });
      const evaluation = page.evaluate(
        async ({ transport, endpoint, token, namespace, reportMethod, phaseSeconds, jitterMs }) => {
          const { runAvSyncScenario } = await import('/scripts/av-sync/scenario.js');
          let scenarioTransport = { kind: 'relay', endpoint, token };
          if (transport === 'hub') {
            const { createRelayHub } = await import('/tests/helpers/relay-hub.js');
            const hub = createRelayHub();
            scenarioTransport = { kind: 'hub', publisher: hub.connectPublisher, subscriber: hub.connectSubscriber };
          }
          try {
            return await runAvSyncScenario({
              transport: scenarioTransport,
              namespace,
              reportMethod,
              phaseSeconds,
              jitterMs,
              report: window.reportPhase,
            });
          } catch (error) {
            // Only the message crosses page.evaluate; keep the name, the code and the cause in it.
            const describe = (value) =>
              value instanceof Error
                ? `${value.name}${value.code ? ` ${value.code}` : ''}: ${value.message}${value.cause ? ` (cause: ${describe(value.cause)})` : ''}`
                : String(value);
            throw new Error(describe(error));
          }
        },
        { transport, endpoint, token, namespace, reportMethod, phaseSeconds, jitterMs: jitterMsFor(name) },
      );
      // The browser closes in `finally`, which ends an evaluation the watchdog gave up on.
      evaluation.catch(() => {});
      const result = await Promise.race([evaluation, expired]).finally(() => clearTimeout(watchdog));
      const verdicts = result.phases.map((phase) => {
        // Without audio, video must keep moving: the clock falls back to the wall clock, with no stall.
        if (!phase.audio) {
          const ok = (phase.modes.wall ?? 0) > 0 && phase.framesShown > 0 && phase.longestGapMs <= MAX_STALL_MS;
          return { phase: phase.phase, ok };
        }
        // Net of the synthetic microphone's path to the capture input, which stands in for a real microphone's input
        // latency: no browser API reports it, so neither the recipe nor the player can correct for it (#290).
        const settled = phase.sync;
        const ok =
          settled !== undefined &&
          settled.max <= LIMIT_MS &&
          settled.min >= -LIMIT_MS &&
          settled.mean >= -EARLY_MEAN_LIMIT_MS;
        return { phase: phase.phase, ok };
      });
      const ok = verdicts.every((verdict) => verdict.ok);
      failed ||= !ok;
      const { events, ...summary } = result;
      report({ runtime, result: ok ? 'PASS' : 'FAIL', ...summary });
      results.push({ runtime, ok, ...result });
    } catch (error) {
      failed = true;
      report({ runtime, result: 'FAIL', error: redact(error instanceof Error ? error.message : error) });
      results.push({ runtime, ok: false, error: redact(error) });
    } finally {
      await browser.close();
    }
  }
} finally {
  await new Promise((resolve) => server.close(resolve));
}
if (process.env.AV_SYNC_OUT) await writeFile(process.env.AV_SYNC_OUT, redact(JSON.stringify(results, null, 2)));
process.exitCode = failed ? 1 : 0;
