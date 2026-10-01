/** How the live-relay launcher folds each runtime's outcome into one summary and exit code. */
import { expect, it } from 'vitest';
import { summarizeRuns } from '../scripts/relay-runs.js';

const pass = (runtime) => ({ runtime, ok: true });
const fail = (runtime, phase, error) => ({ runtime, ok: false, phase, error });

it('passes when every runtime passes', () => {
  expect(summarizeRuns([pass('node'), pass('chromium')])).toEqual({
    summary: { result: 'PASS', runtimes: ['node', 'chromium'] },
    exitCode: 0,
  });
});

it('passes a run of one runtime', () => {
  expect(summarizeRuns([pass('chromium')])).toEqual({
    summary: { result: 'PASS', runtimes: ['chromium'] },
    exitCode: 0,
  });
});

it('fails when every runtime fails, with each failure', () => {
  const { summary, exitCode } = summarizeRuns([
    fail('node', 'publisher connected', 'boom'),
    fail('chromium', undefined, 'no WebTransport'),
  ]);
  expect(exitCode).toBe(1);
  expect(summary).toEqual({
    result: 'FAIL',
    runtimes: ['node', 'chromium'],
    failures: [
      { runtime: 'node', after: 'publisher connected', error: 'boom' },
      { runtime: 'chromium', after: 'nothing', error: 'no WebTransport' },
    ],
  });
});

it('reports a scenario that passes on one runtime and fails on the other as a divergence', () => {
  const { summary, exitCode } = summarizeRuns([pass('node'), fail('chromium', 'tracks published', 'timed out')]);
  expect(exitCode).toBe(1);
  expect(summary).toEqual({
    result: 'DIVERGED',
    runtimes: ['node', 'chromium'],
    passed: ['node'],
    failures: [{ runtime: 'chromium', after: 'tracks published', error: 'timed out' }],
    note: expect.stringContaining('bug in an adapter or in the transport adapter contract'),
  });
});
