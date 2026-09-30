/**
 * A publisher connection's settling groups (see CONTEXT.md): groups whose operation completed once
 * the transport took their FIN, but whose FIN the relay has not acknowledged yet.
 *
 * Chromium refuses `createUnidirectionalStream()` once stream credit runs out instead of waiting
 * for it, and settling groups are what can pile up on a connection: open streams are already
 * bounded by one per track. So before an operation opens its stream it waits while the connection
 * has {@link SETTLING_GROUP_LIMIT} settling groups, first come first served; it holds nothing while
 * its stream is open, and its group joins the set once its FIN is handed over. The count can
 * briefly exceed the limit by the number of groups open at that moment.
 *
 * A settling group that fails later (STOP_SENDING, a reset, session teardown) leaves the set like
 * one that settles cleanly, and nobody hears of it: the cause surfaces as demand, a rebind, or the
 * connection's `closed`. `publisher.close()` drains the set before it closes the transport, because
 * Chromium resets every stream still awaiting its acknowledgment when the session goes.
 */

/** Settling groups per publisher connection before new streams wait. Internal, not an option. */
export const SETTLING_GROUP_LIMIT = 64;

export interface SettlingGroups {
  /** Whether a new stream may open now: the connection is below the limit. */
  hasRoom(): boolean;
  /**
   * Wait until the connection is below the limit. Rejects with the reason of `interrupted` if that
   * rejects first.
   */
  waitForRoom(interrupted: Promise<never>): Promise<void>;
  /** A group's FIN was handed over: count it until `settled` settles, either way. */
  add(settled: Promise<void>): void;
  /** Wait for every settling group, for at most `timeoutMs` or until `stop` settles. Never rejects. */
  drain(timeoutMs: number, stop: Promise<unknown>): Promise<void>;
  /** How many groups are settling now. */
  readonly size: number;
}

export function createSettlingGroups(limit = SETTLING_GROUP_LIMIT): SettlingGroups {
  const settling = new Set<Promise<void>>();
  const waiting: (() => void)[] = [];

  // Every waiter goes on, in arrival order, once the connection is below the limit again.
  const wake = () => {
    while (waiting.length > 0 && settling.size < limit) waiting.shift()?.();
  };

  return {
    hasRoom: () => settling.size < limit,
    waitForRoom(interrupted) {
      if (settling.size < limit) return Promise.resolve();
      return new Promise((resolve, reject) => {
        waiting.push(resolve);
        interrupted.catch((error: unknown) => {
          const index = waiting.indexOf(resolve);
          if (index === -1) return;
          waiting.splice(index, 1);
          reject(error);
        });
      });
    },
    add(settled) {
      const tracked = settled.then(
        () => {},
        () => {},
      );
      settling.add(tracked);
      void tracked.then(() => {
        settling.delete(tracked);
        wake();
      });
    },
    async drain(timeoutMs, stop) {
      if (settling.size === 0) return;
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        await Promise.race([
          Promise.all(settling),
          new Promise<void>((resolve) => {
            timer = setTimeout(resolve, timeoutMs);
          }),
          stop.then(
            () => {},
            () => {},
          ),
        ]);
      } finally {
        clearTimeout(timer);
      }
    },
    get size() {
      return settling.size;
    },
  };
}
