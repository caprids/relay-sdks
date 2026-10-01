/**
 * Turn the conformance cases into vitest tests for one target. A case the target skips is a skipped test naming the
 * reason; an expected failure passes while it still fails and fails once it passes, so it gets promoted; a SHOULD that
 * is not met is skipped at run time with what went wrong.
 */
import { describe, it } from 'vitest';
import { cases, planCase, runCase } from './suite.js';

/** Register one vitest test per conformance case, run against `target`. */
export function describeConformance(target) {
  describe(`transport adapter conformance: ${target.name}`, () => {
    for (const testCase of cases) {
      const plan = planCase(target, testCase);
      if (!plan.run) {
        it.skip(`${testCase.name} (skipped: ${plan.reason})`);
        continue;
      }
      const title = plan.expectFailure ? `${testCase.name} (expected failure: ${plan.expectFailure})` : testCase.name;
      it(
        title,
        async (context) => {
          const result = await runCase(target, testCase);
          if (result.status === 'failed') throw result.error;
          if (result.status === 'unexpected-pass') {
            throw new Error(`passes now: remove its expected failure ("${result.reason}") and promote it`);
          }
          if (result.status === 'unmet') context.skip(`SHOULD not met: ${result.error?.message ?? result.error}`);
        },
        (testCase.timeoutMs ?? 10_000) + 5_000,
      );
    }
  });
}
