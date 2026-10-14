import { NO_DELIVERY_TIMEOUTS } from '../../src/delivery-timeouts.js';
import type { PublishProperty } from '../../src/index.js';
import { createSettlingGroups } from '../../src/settling-groups.js';
import { createWriteSlot } from '../../src/write-slot.js';

/** One object a slot track's stream took. */
export interface RecordedObject {
  bytes: Uint8Array;
  properties: readonly PublishProperty[];
}

/** One group stream a slot track opened. */
export interface RecordedGroup {
  groupId: number;
  objects: RecordedObject[];
  finished: boolean;
  aborted: boolean;
}

/**
 * A registered track over the real write slot, with an in-memory subgroup stream: the media publishing tests drive the
 * same demand, slot and interruption behaviour the publisher connection does.
 */
export function slotTrack(options: { demand?: boolean; writeTimeoutMs?: number } = {}) {
  const groups: RecordedGroup[] = [];
  /** Object writes that wait for `release()` before the stream takes them. */
  let stalled = false;
  /** Stream opens that wait for `release()`. */
  let stalledOpen = false;
  const held: (() => void)[] = [];
  let failNext: Error | undefined;
  let alias = 1;
  const slot = createWriteSlot({
    settlingGroups: createSettlingGroups(),
    namespaceSolicited: () => true,
    assertOpen() {},
    endTrack() {},
    writeTimeoutMs: options.writeTimeoutMs ?? 10_000,
    trackTimeouts: NO_DELIVERY_TIMEOUTS,
    subscriberTimeouts: () => NO_DELIVERY_TIMEOUTS,
    async openSubgroup(_alias, groupId) {
      if (stalledOpen) await new Promise<void>((resolve) => held.push(resolve));
      const group: RecordedGroup = { groupId, objects: [], finished: false, aborted: false };
      groups.push(group);
      let rejectWrite: ((error: unknown) => void) | undefined;
      const fin = async () => {
        if (!group.aborted) group.finished = true;
      };
      return {
        groupId,
        async writeObject(object) {
          if (failNext) {
            const error = failNext;
            failNext = undefined;
            throw error;
          }
          if (stalled) {
            await new Promise<void>((resolve, reject) => {
              rejectWrite = reject;
              held.push(resolve);
            });
          }
          group.objects.push({ bytes: object.payload.slice(), properties: object.properties ?? [] });
        },
        fin,
        handOverFin: fin,
        abort(reason) {
          if (group.finished || group.aborted) return;
          group.aborted = true;
          rejectWrite?.(reason);
        },
      };
    },
  });
  if (options.demand !== false) slot.bind(alias);
  return {
    writer: slot.writer,
    groups,
    /** The relay subscribes (again: a replacement), or its subscription ends. */
    demand(value: boolean) {
      slot.bind(value ? ++alias : undefined);
    },
    /** Hold every object write until `release()`. */
    stall() {
      stalled = true;
    },
    /** Hold every stream open until `release()`. */
    stallOpen() {
      stalledOpen = true;
    },
    /** Let held writes and opens through and stop holding. */
    release() {
      stalled = false;
      stalledOpen = false;
      for (const resolve of held.splice(0)) resolve();
    },
    /** The next object write fails with this transport error. */
    failNextWrite(error: Error) {
      failNext = error;
    },
    /** The publisher connection ends. */
    disconnect(error?: unknown) {
      slot.disconnect(error);
    },
  };
}

/** Let queued promise callbacks run. */
export const settle = async (turns = 20) => {
  for (let turn = 0; turn < turns; turn++) await Promise.resolve();
};
