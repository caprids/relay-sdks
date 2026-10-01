/**
 * Fold the live-relay scenario's outcome on each runtime into the launcher's summary line and exit code.
 *
 * The scenario is the same on every runtime, so one that passes on one launcher and fails on another
 * is not a relay flake to shrug off: it is a bug in an adapter or in the transport adapter contract
 * (spec/transport-adapter.md), and the summary says so.
 *
 * @param {Array<{ runtime: string, ok: boolean, phase?: string, error?: string }>} outcomes One per runtime, in
 *   run order. A failure carries the last phase that passed before it, if any, and its redacted error.
 * @returns {{ summary: object, exitCode: number }}
 */
export function summarizeRuns(outcomes) {
  const runtimes = outcomes.map(({ runtime }) => runtime);
  const passed = outcomes.filter(({ ok }) => ok).map(({ runtime }) => runtime);
  const failures = outcomes
    .filter(({ ok }) => !ok)
    .map(({ runtime, phase, error }) => ({ runtime, after: phase ?? 'nothing', error }));
  if (failures.length === 0) return { summary: { result: 'PASS', runtimes }, exitCode: 0 };
  if (passed.length === 0) return { summary: { result: 'FAIL', runtimes, failures }, exitCode: 1 };
  return {
    summary: {
      result: 'DIVERGED',
      runtimes,
      passed,
      failures,
      note:
        'The scenario passed on one runtime and failed on another: a bug in an adapter or in the transport ' +
        'adapter contract, unless a rerun shows a relay flake.',
    },
    exitCode: 1,
  };
}
