/**
 * The demand timeout, and the application's side of it.
 *
 * `write()` waits for relay demand before it sends anything, and its write deadline covers that wait
 * and the transport steps after it together. A `RelayError` with code `WRITE_TIMEOUT` in phase
 * `waiting-for-demand` therefore proves the attempt sent no object bytes: no subscriber had asked for
 * the track yet, and writing the same object again cannot duplicate it. A failure in a sending phase
 * (`opening-stream`, `writing`, `closing`) proves nothing of the kind: bytes may already have reached
 * the relay, so the same payload is never resent and the caller chooses what comes next
 * (spec/behavior.md, "Write phases and interruptions"). This module is that policy, used by the
 * mux-global demo, which serves it to the browser next to the SDK. It branches on the error's code
 * and phase, never on its message.
 */
import { RelayError } from '@mux/relay';

/** The phases after which a failed write may have delivered some or all of its bytes. */
export const SENDING_PHASES = Object.freeze(['opening-stream', 'writing', 'closing']);

/** Whether `error` is a write deadline that expired before the attempt sent anything. */
export function isDemandTimeout(error) {
  return error instanceof RelayError && error.code === 'WRITE_TIMEOUT' && error.phase === 'waiting-for-demand';
}

/**
 * Write `payload` once, retrying only a demand timeout until a subscriber creates demand.
 *
 * Each attempt is one `writer.write()` with the track's own write deadline; the SDK does the
 * waiting. `onRetry(attempt, error)` runs after a demand timeout, before the next attempt, and may
 * throw to stop. Any other failure rejects unchanged; when it happened in a sending phase,
 * `onUncertain(error)` runs first, because the relay may hold part or all of the object. Only
 * `signal` bounds the retries: it is passed to every write, so aborting it ends the SDK's wait at
 * once and rejects with the abort reason.
 *
 * @param {{ write(payload: ArrayBufferView, options?: { signal?: AbortSignal }): Promise<void> }} writer
 * @param {ArrayBufferView} payload
 * @param {{ signal?: AbortSignal, onRetry?: (attempt: number, error: RelayError) => void, onUncertain?: (error: RelayError) => void }} [retry]
 * @returns {Promise<void>}
 */
export async function writeWithRetry(writer, payload, { signal, onRetry, onUncertain } = {}) {
  for (let attempt = 1; ; attempt += 1) {
    signal?.throwIfAborted();
    try {
      await writer.write(payload, { signal });
      return;
    } catch (error) {
      if (!isDemandTimeout(error)) {
        if (error instanceof RelayError && SENDING_PHASES.includes(error.phase)) onUncertain?.(error);
        throw error;
      }
      onRetry?.(attempt, error);
    }
  }
}
