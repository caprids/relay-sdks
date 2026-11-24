/** A canvas animation that makes A/V sync visible: a bar sweeping across a dark frame, and a white flash on request. */
export interface TestPattern {
  /** Turn the frame white for `durationMs` (default 150), from now: pair it with a sound to judge sync by eye and ear. */
  flash(durationMs?: number): void;
  /** Stop animating. */
  stop(): void;
}

/** Animate `canvas` every 10 ms until `stop()`. */
export function startTestPattern(canvas: HTMLCanvasElement | OffscreenCanvas): TestPattern {
  const draw = canvas.getContext('2d') as CanvasRenderingContext2D | OffscreenCanvasRenderingContext2D | null;
  if (!draw) throw new Error('The canvas has no 2D context');
  let flashUntil = 0;
  const paint = () => {
    const now = performance.now();
    const flashing = now < flashUntil;
    draw.fillStyle = flashing ? '#fff' : '#202020';
    draw.fillRect(0, 0, canvas.width, canvas.height);
    if (flashing) return;
    draw.fillStyle = '#606060';
    draw.fillRect(Math.floor(now / 5) % canvas.width, 0, 20, canvas.height);
  };
  paint();
  // A timer, not requestAnimationFrame, which an OffscreenCanvas in a worker would not have.
  const timer = setInterval(paint, 10);
  return {
    flash(durationMs = 150) {
      flashUntil = performance.now() + durationMs;
      paint();
    },
    stop() {
      clearInterval(timer);
    },
  };
}
