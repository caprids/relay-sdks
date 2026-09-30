/**
 * A publisher connection's settling groups (see CONTEXT.md): groups whose operation completed once
 * the transport took their FIN, but whose FIN the relay has not acknowledged yet.
 *
 * Chromium refuses `createUnidirectionalStream()` once stream credit runs out instead of waiting
 * for it, so the connection bounds its streams itself. An operation reserves a place before it
 * opens its stream and holds it while the stream is open and then while its group settles; at the
 * limit the next reservation waits, first come first served. A settling group that fails later
 * (STOP_SENDING, a reset, session teardown) frees its place like one that settles cleanly, and
 * nobody hears of it: the cause surfaces as demand, a rebind, or the connection's `closed`.
 *
 * `publisher.close()` drains the settling groups before it closes the transport, because Chromium
 * resets every stream still awaiting its acknowledgment when the session goes.
 */

/** Places per publisher connection: open streams plus settling groups. Internal, not an option. */
export const SETTLING_GROUP_LIMIT = 64;

/** One operation's place under the limit. The first of `settle` and `release` counts. */
export interface GroupReservation {
  /** The group's FIN was handed over: hold the place until `settled` settles, either way. */
  settle(settled: Promise<void>): void;
  /** The operation ended without handing over a FIN: free the place now. */
  release(): void;
}

export interface SettlingGroups {
  /** Take a place at once if the connection is below the limit. */
  tryReserve(): GroupReservation | undefined;
  /**
   * Take a place, waiting while the connection is at the limit. Rejects with the reason of
   * `interrupted` if that rejects first, and then holds no place.
   */
  reserve(interrupted: Promise<never>): Promise<GroupReservation>;
  /** Wait for every settling group, for at most `timeoutMs` or until `stop` settles. Never rejects. */
  drain(timeoutMs: number, stop: Promise<unknown>): Promise<void>;
}

export function createSettlingGroups(limit = SETTLING_GROUP_LIMIT): SettlingGroups {
  let held = 0;
  const settling = new Set<Promise<void>>();
  const waiting: ((reservation: GroupReservation) => void)[] = [];

  // A freed place goes to the longest waiter, so `held` only drops when nobody waits.
  const free = () => {
    const next = waiting.shift();
    if (next) next(reservation());
    else held--;
  };
  const reservation = (): GroupReservation => {
    let done = false;
    return {
      settle(settled) {
        if (done) return;
        done = true;
        const tracked = settled.then(
          () => {},
          () => {},
        );
        settling.add(tracked);
        void tracked.then(() => {
          settling.delete(tracked);
          free();
        });
      },
      release() {
        if (done) return;
        done = true;
        free();
      },
    };
  };

  // Below the limit nobody waits: a freed place goes to a waiter before `held` drops.
  const tryReserve = (): GroupReservation | undefined => {
    if (held >= limit) return undefined;
    held++;
    return reservation();
  };

  return {
    tryReserve,
    reserve(interrupted) {
      const now = tryReserve();
      if (now) return Promise.resolve(now);
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
  };
}
