/**
 * When an `AudioContext` renders, on `performance.now()`'s clock. Every engine exposes the context's render position
 * as `currentTime` (or as `currentFrame` inside an `AudioWorklet`), and anything that observes it from the main thread
 * sees it late by however long the observation took to arrive: a message from the worklet, or the main thread's own
 * turn. The earliest observation in a short window is the best estimate of when the context rendered.
 *
 * Why not `AudioContext.getOutputTimestamp()`: Chromium returns the context time reaching the speaker and when, as the
 * Web Audio spec says. Firefox (148) returns `currentTime − outputLatency` and `performance.now() − outputLatency`,
 * which is the render pair shifted by the output delay, and it steps with each audio callback. A recipe that adds the
 * output delay to `contextTime` is right in Chromium and stamps audio early by the output delay in Firefox.
 */

/** One observation: context time `contextUs` had been rendered by `atUs` on `performance.now()`'s clock. */
export interface RenderObservation {
  /** Context time, in microseconds: `currentTime × 1e6`, or a worklet frame count over its sample rate. */
  contextUs: number;
  /** When it was observed, in `performance.now()` microseconds. */
  atUs: number;
}

/** Options for {@link createRenderClock}. */
export interface RenderClockOptions {
  /** How long an observation is kept, in µs. Default 1,000,000: drift over a second is far below a millisecond. */
  windowUs?: number;
}

/** The context's render clock against `performance.now()`, from the earliest observation in a sliding window. */
export interface RenderClock {
  /** Record an observation. Observations must arrive in `atUs` order. */
  observe(contextUs: number, atUs: number): void;
  /**
   * The observation in the window that arrived soonest after its render, as a calibration pair for
   * `SourceTimeline.calibrate(contextUs, atUs)`; `undefined` before the first observation.
   */
  best(): RenderObservation | undefined;
  /** When context time `contextUs` renders, in `performance.now()` microseconds; `undefined` before any observation. */
  renderedAtUs(contextUs: number): number | undefined;
}

/** Create a render clock for one `AudioContext`. A replaced context needs a new one: its time restarts at zero. */
export function createRenderClock({ windowUs = 1_000_000 }: RenderClockOptions = {}): RenderClock {
  if (!(windowUs > 0)) throw new RangeError('windowUs must be positive');
  // A monotonic deque: lags (atUs − contextUs) increase from front to back, so the front is the window's minimum.
  const kept: RenderObservation[] = [];
  const lag = (observation: RenderObservation) => observation.atUs - observation.contextUs;
  return {
    observe(contextUs, atUs) {
      if (!Number.isFinite(contextUs) || !Number.isFinite(atUs)) throw new RangeError('Observations must be finite');
      const observation = { contextUs, atUs };
      while (kept.length > 0 && lag(kept[kept.length - 1] as RenderObservation) >= lag(observation)) kept.pop();
      kept.push(observation);
      while ((kept[0] as RenderObservation).atUs < atUs - windowUs) kept.shift();
    },
    best() {
      const first = kept[0];
      return first && { ...first };
    },
    renderedAtUs(contextUs) {
      const first = kept[0];
      return first && contextUs + lag(first);
    },
  };
}
