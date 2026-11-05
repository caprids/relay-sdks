import type { MoqtObject } from '@caprid/moq-core';
import type { WantSwitchedInfo } from './supervised-subscriber.js';

/** Options for {@link createLatestValueReader}. */
export interface LatestValueReaderOptions {
  /**
   * Called synchronously with each value newer than every one judged before it. An exception from it goes to
   * `console.error` and the reader carries on.
   */
  onValue(object: MoqtObject): void;
  /**
   * The application's test that a newer value still arrived too late to act on, such as a capture time in the payload
   * against the viewer's clock. A late value is dropped, and still supersedes older ones. An exception from it goes to
   * `console.error` and counts as `false`.
   */
  late?(object: MoqtObject): boolean;
  /**
   * The first late value since the start or since a delivered value; a restart does not re-arm it. Replace the subscription at the live
   * edge, which drops what the relay still holds queued for it: a supervised subscriber's `catchUp(key)` does, and its
   * `onRestarted` restarts this reader. Not called again until a value is delivered. An
   * exception from it goes to `console.error`.
   */
  onCatchUp?(): void;
}

/** What a {@link LatestValueReader} dropped and delivered. */
export interface LatestValueStats {
  /** Values passed to `onValue`. */
  delivered: number;
  /** Objects dropped because a newer one had already been judged. */
  superseded: number;
  /** Newer values dropped because `late` returned true. */
  late: number;
  /** Times the reader signalled a catch-up. */
  catchUps: number;
}

/**
 * A subscription's or a want's handlers that deliver only the newest value of a latest-wins track, such as telemetry
 * or a control. Pass it, or spread it with handlers of your own, to `subscribe` or `want`. A want read through a
 * reader keeps `drainMs` at 0, the default: a drained old subscription's groups would arrive after the Move, numbered
 * against the old one.
 */
export interface LatestValueReader {
  /** Judges the object against the newest location so far, and delivers it if it is newer and not late. */
  onObject(object: MoqtObject): void;
  /** A want's Restart: the new subscription numbers its groups afresh, so the reader starts over. */
  onRestarted(): void;
  /** A want's Move: the same. */
  onSwitched(info: WantSwitchedInfo): void;
  /** Counters. */
  readonly stats: Readonly<LatestValueStats>;
}

/**
 * Read a latest-wins track: drop each object whose location (group, then object) is not after every one judged since
 * the start or the last restart, and each one the `late` predicate rejects.
 * See spec/behavior.md, "Latest-value reading".
 *
 * ```ts
 * const telemetry = createLatestValueReader({
 *   onValue: (object) => render(decode(object.payload)),
 *   late: (object) => Date.now() - decode(object.payload).sentAt > 2_000,
 *   onCatchUp: () => subscriber.catchUp('telemetry'),
 * });
 * subscriber.want('telemetry', { namespace, track: 'telemetry' }, { ...telemetry, onError });
 * ```
 */
export function createLatestValueReader(options: LatestValueReaderOptions): LatestValueReader {
  const stats: LatestValueStats = { delivered: 0, superseded: 0, late: 0, catchUps: 0 };
  let newest: { groupId: number; objectId: number } | undefined;
  let catchUpSignalled = false;

  // A restart does not re-arm the catch-up: a replacement late from its first value is still behind the old
  // subscription's bytes on the viewer's link, and catching up again would only repeat the gap.
  const restart = () => {
    newest = undefined;
  };
  const isLate = (object: MoqtObject) => {
    try {
      return options.late?.(object) === true;
    } catch (error) {
      // biome-ignore lint/suspicious/noConsole: the application's callback failed and has no caller to report to
      console.error(new Error('late threw; the latest-value reader delivers the value', { cause: error }));
      return false;
    }
  };

  const reader: Omit<LatestValueReader, 'stats'> = {
    onObject(object) {
      // A status object (an end of group or track) carries no value.
      if (object.status !== 'normal') return;
      if (
        newest !== undefined &&
        (object.groupId < newest.groupId || (object.groupId === newest.groupId && object.objectId <= newest.objectId))
      ) {
        stats.superseded++;
        return;
      }
      newest = { groupId: object.groupId, objectId: object.objectId };
      if (isLate(object)) {
        stats.late++;
        if (catchUpSignalled) return;
        catchUpSignalled = true;
        stats.catchUps++;
        try {
          options.onCatchUp?.();
        } catch (error) {
          // biome-ignore lint/suspicious/noConsole: the application's callback failed and has no caller to report to
          console.error(new Error('onCatchUp threw; the latest-value reader carries on', { cause: error }));
        }
        return;
      }
      catchUpSignalled = false;
      stats.delivered++;
      try {
        options.onValue(object);
      } catch (error) {
        // biome-ignore lint/suspicious/noConsole: the application's callback failed and has no caller to report to
        console.error(new Error('onValue threw; the latest-value reader carries on', { cause: error }));
      }
    },
    onRestarted: restart,
    onSwitched: restart,
  };
  // Not enumerable, so spreading the reader into handlers copies only callbacks: a want refuses any other value.
  return Object.defineProperty(reader, 'stats', { get: () => stats }) as LatestValueReader;
}
