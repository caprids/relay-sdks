import { expect, it } from 'vitest';
import { createRenderClock } from '../examples/webcodecs/render-clock.js';
import { createOnsetDetector, matchEvents, summarize } from '../scripts/av-sync/measure.js';

it('takes the render clock from the earliest observation in its window', () => {
  const renders = createRenderClock({ windowUs: 1_000_000 });
  expect(renders.best()).toBeUndefined();
  expect(renders.renderedAtUs(0)).toBeUndefined();
  // Observations arrive 3, 1 and 7 ms after their render; the 1 ms one sets the clock.
  renders.observe(10_000, 13_000);
  renders.observe(20_000, 21_000);
  renders.observe(30_000, 37_000);
  expect(renders.best()).toEqual({ contextUs: 20_000, atUs: 21_000 });
  expect(renders.renderedAtUs(500_000)).toBe(501_000);
  // Once the best observation leaves the window, the best of the rest takes over.
  renders.observe(1_025_000, 1_029_000);
  expect(renders.best()).toEqual({ contextUs: 1_025_000, atUs: 1_029_000 });
  // A better observation replaces it at once.
  renders.observe(1_040_000, 1_040_500);
  expect(renders.renderedAtUs(0)).toBe(500);
  expect(() => createRenderClock({ windowUs: 0 })).toThrow(RangeError);
  expect(() => renders.observe(Number.NaN, 0)).toThrow(RangeError);
});

it('finds tone onsets after enough quiet, across blocks and resets', () => {
  const onsets = createOnsetDetector({ threshold: 0.1, quietUs: 200_000 });
  // At 1 kHz, one sample per millisecond; loud from `from` to `to`, quiet (but not silent) elsewhere.
  const tone = (length: number, from: number, to: number) =>
    Float32Array.from({ length }, (_, index) => (index >= from && index < to ? 0.5 : 0.01));
  expect(onsets.push(tone(1000, 250, 300), 1000, 0)).toEqual([250_000]);
  // 101 ms after the last loud sample is too soon for a new onset; 291 ms is not.
  expect(onsets.push(tone(100, 0, 10), 1000, 400_000)).toEqual([]);
  expect(onsets.push(tone(1000, 0, 10), 1000, 700_000)).toEqual([700_000]);
  // After a reset, quiet is counted from the next block.
  onsets.reset();
  expect(onsets.push(tone(1000, 100, 110), 1000, 5_000_000)).toEqual([]);
  expect(onsets.push(tone(1000, 300, 310), 1000, 6_000_000)).toEqual([6_300_000]);
});

it('pairs each emitted flash and beep with what the player showed and played', () => {
  const originUs = 1_000_000;
  const emitted = [
    { beepAtUs: 2_000_000, flashAtUs: 2_000_500 },
    // Nothing was heard for this one: it is left out.
    { beepAtUs: 3_000_000, flashAtUs: 3_000_500 },
  ];
  // The white frame was captured 20 ms after the canvas turned white and shown 100 ms after its capture.
  const flashes = [
    { timestampUs: 1_020_500, shownAtUs: 2_120_500 },
    { timestampUs: 2_020_500, shownAtUs: 3_120_500 },
  ];
  // The beep was stamped 30 ms late and was audible 135 ms after it was emitted.
  const beeps = [{ timestampUs: 1_030_000, audibleAtUs: 2_135_000 }];
  // It reached the capture input 25 ms after it was emitted.
  const arrivals = [2_025_000];
  expect(matchEvents(emitted, flashes, beeps, originUs, arrivals)).toEqual([
    {
      beepAtUs: 2_000_000,
      heardAtUs: 2_135_000,
      offsetMs: 35,
      captureMs: 30,
      playoutMs: 5,
      pathMs: 25,
      calibrationMs: 5,
      syncMs: 10,
    },
  ]);
  expect(matchEvents(emitted, flashes, beeps, originUs)).toEqual([
    { beepAtUs: 2_000_000, heardAtUs: 2_135_000, offsetMs: 35, captureMs: 30, playoutMs: 5 },
  ]);
});

it('summarizes signed offsets', () => {
  expect(summarize([])).toBeUndefined();
  expect(summarize([10, -30, 20, 5])).toEqual({ count: 4, mean: 1.3, p95: 30, worst: -30, min: -30, max: 20 });
});
