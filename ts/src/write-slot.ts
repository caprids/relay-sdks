/**
 * The write slot: a track's single place for a pending operation (see CONTEXT.md).
 *
 * One slot per registered track. It owns the relay's demand binding for the track, the one active
 * operation (a single write or an open group) and its phases, the write deadline, cancellation, and
 * the demand observers behind `hasDemand` and `demandChanges()`. Its public face is the
 * {@link RelayTrackWriter} the application holds; the publisher connection drives it through two
 * inbound calls, `bind()` when the relay's binding changes and `disconnect()` when the connection
 * ends.
 *
 * An operation completes once the transport has taken its group's FIN, not once the relay has
 * acknowledged it: the slot hands the FIN over, frees itself, and passes the group on to the
 * connection as a settling group (see CONTEXT.md). Nothing resets a settling group, and nothing
 * reports how it ends.
 *
 * Everything the slot borrows enters through {@link WriteSlotDependencies}: how to open a subgroup
 * stream, the connection's settling-group limit, whether the namespace is solicited, whether the
 * connection is still open, and how to end the track in the core. Tests drive a slot with an
 * in-memory subgroup writer and fake timers; the "Write phases and interruptions" table in
 * spec/behavior.md is the list they cover.
 */
import { type SubgroupWriter, streamResetReason } from '@mux/moq-core';
import { RelayError, type RelayErrorPhase, transportError } from './errors.js';
import { encodeJSON, encodeText } from './payload.js';
import type { RelayGroupWriter, RelayTrackWriter } from './publish.js';
import type { SettlingGroups } from './settling-groups.js';
import { payloadBytes, propertyPairs } from './validation.js';

/**
 * Stream reset codes a publisher sends (draft-ietf-moq-transport-20 §3.3.4, registry §15.11.4).
 * The core defines the same values but does not export them.
 */
export const RESET_CODE = {
  INTERNAL_ERROR: 0x0,
  CANCELLED: 0x1,
  DELIVERY_TIMEOUT: 0x2,
  SESSION_CLOSED: 0x3,
  GOING_AWAY: 0x4,
} as const;

export type ResetCode = (typeof RESET_CODE)[keyof typeof RESET_CODE];

/** What the slot borrows from the publisher connection and the core session. */
export interface WriteSlotDependencies {
  /** Open a subgroup stream for one group of this track. Rejects when the transport refuses. */
  openSubgroup(trackAlias: number, groupId: number): Promise<SubgroupWriter>;
  /** The connection's settling groups: an operation waits for room before opening its stream. */
  settlingGroups: SettlingGroups;
  /** Whether a current relay solicitation covers the namespace; undefined before any arrived. */
  namespaceSolicited(): boolean | undefined;
  /** Throws the connection's closing error once the connection has ended. */
  assertOpen(): void;
  /** Tell the core the track is done, so it FINs any live subscription stream. */
  endTrack(): void;
  /** Deadline for waiting for demand and for each stream step. */
  writeTimeoutMs: number;
}

export interface WriteSlot {
  /** The application's handle: the public track writer. */
  readonly writer: RelayTrackWriter;
  /** The relay's binding for this track changed: a new alias, or undefined when its subscription ended. */
  bind(trackAlias: number | undefined): void;
  /** The publisher connection ended; `error` is its failure, absent after a clean shutdown. */
  disconnect(error?: unknown): void;
}

/**
 * The reason handed to a stream reset outside a browser: transports read `streamErrorCode` from it.
 * In a browser the reason is a WebTransportError, which is how WebTransport carries the code.
 */
export class StreamReset extends Error {
  override readonly name = 'StreamReset';
  readonly streamErrorCode: ResetCode;

  constructor(code: ResetCode, cause: unknown) {
    super(cause instanceof Error ? cause.message : 'Publish operation reset', { cause });
    this.streamErrorCode = code;
  }
}

function resetReason(code: ResetCode, cause: unknown): unknown {
  const message = cause instanceof Error ? cause.message : 'Publish operation reset';
  return streamResetReason(code, message, () => new StreamReset(code, cause));
}

/** The code for a reset caused by the connection ending. */
function connectionResetCode(error: unknown): ResetCode {
  return error instanceof RelayError && error.code === 'GOAWAY' ? RESET_CODE.GOING_AWAY : RESET_CODE.SESSION_CLOSED;
}

interface Operation {
  kind: 'write' | 'group';
  alias: number | undefined;
  writer: SubgroupWriter | undefined;
  cancelled: boolean;
  failure: unknown;
  code: ResetCode;
  phase: RelayErrorPhase;
  demand: Promise<void>;
  interrupted: Promise<never>;
  wake(): void;
  cancel(error: unknown, code: ResetCode): void;
  cleanup(): void;
}

export function createWriteSlot(deps: WriteSlotDependencies): WriteSlot {
  const { openSubgroup, settlingGroups, namespaceSolicited, assertOpen, endTrack, writeTimeoutMs } = deps;
  let alias: number | undefined;
  let ended = false;
  let nextGroupId = 0;
  let active: Operation | undefined;
  const watchers = new Set<(value: boolean | undefined) => void>();

  const notify = (value: boolean | undefined) => {
    for (const watcher of watchers) watcher(value);
  };
  const end = () => {
    if (ended) return;
    ended = true;
    endTrack();
    notify(undefined);
    watchers.clear();
  };
  const close = (
    error: unknown = new RelayError('CLOSED', 'Published track is closed', active?.phase ?? 'waiting-for-demand'),
    code: ResetCode = RESET_CODE.CANCELLED,
  ): void => {
    if (ended) return;
    active?.cancel(error, code);
    end();
  };
  const assertWritable = () => {
    assertOpen();
    // Refused before an operation starts: nothing of this write was sent (spec/behavior.md).
    if (ended) throw new RelayError('CLOSED', 'Published track is closed', 'waiting-for-demand');
    if (active)
      throw new RelayError(
        'WRITE_PENDING',
        'A write or group is already pending; await it before writing another object',
        'writing',
      );
  };
  const begin = (kind: Operation['kind'], signal?: AbortSignal): Operation => {
    let wake!: () => void;
    let reject!: (error: unknown) => void;
    const demand = new Promise<void>((resolve) => {
      wake = resolve;
    });
    const interrupted = new Promise<never>((_, rejectPromise) => {
      reject = rejectPromise;
    });
    // An open group can be aborted between calls, when nobody is awaiting this promise.
    void interrupted.catch(() => {});
    const abort = () => {
      const reason = signal?.reason;
      operation.cancel(
        reason === undefined ? new RelayError('CLOSED', 'Publish operation aborted', operation.phase) : reason,
        RESET_CODE.CANCELLED,
      );
    };
    const operation: Operation = {
      kind,
      phase: 'waiting-for-demand',
      alias: undefined,
      writer: undefined,
      cancelled: false,
      failure: undefined,
      code: RESET_CODE.INTERNAL_ERROR,
      demand,
      interrupted,
      wake,
      cleanup() {
        signal?.removeEventListener('abort', abort);
      },
      cancel(error, code) {
        if (operation.cancelled) return;
        operation.cancelled = true;
        operation.failure = error;
        operation.code = code;
        operation.writer?.abort(resetReason(code, error));
        if (active === operation) active = undefined;
        operation.cleanup();
        wake();
        reject(error);
      },
    };
    active = operation;
    signal?.addEventListener('abort', abort, { once: true });
    if (signal?.aborted) abort();
    return operation;
  };
  const check = (operation: Operation) => {
    if (operation.cancelled) throw operation.failure;
    assertOpen();
  };
  const run = async <T>(operation: Operation, send: () => Promise<T>): Promise<T> => {
    const timer = setTimeout(() => {
      const uncovered = operation.phase === 'waiting-for-demand' && namespaceSolicited() === false;
      operation.cancel(
        new RelayError(
          uncovered ? 'NAMESPACE_NOT_SOLICITED' : 'WRITE_TIMEOUT',
          uncovered
            ? 'Publisher namespace is not covered by the relay namespace solicitations; check the token and namespace'
            : 'Publish write timed out waiting for relay demand or transport',
          operation.phase,
        ),
        RESET_CODE.DELIVERY_TIMEOUT,
      );
    }, writeTimeoutMs);
    try {
      check(operation);
      const result = await Promise.race([send(), operation.interrupted]);
      check(operation);
      return result;
    } catch (error) {
      let failure: unknown;
      let code: ResetCode = RESET_CODE.INTERNAL_ERROR;
      try {
        // Closure can precede the callback that cancels this track. Preserve its reason
        // even when the send continuation runs before that callback.
        check(operation);
        failure = transportError(error, operation.phase);
      } catch (interrupted) {
        failure = interrupted;
        code = connectionResetCode(interrupted);
      }
      operation.cancel(failure, code);
      throw failure;
    } finally {
      clearTimeout(timer);
    }
  };
  const openStream = async (operation: Operation) => {
    if (alias === undefined) {
      await operation.demand;
      check(operation);
      if (alias === undefined)
        throw new RelayError(
          'SUBSCRIPTION_CHANGED',
          'Relay subscription ended before the write started',
          operation.phase,
        );
    }
    check(operation);
    operation.alias = alias;
    const groupId = nextGroupId++;
    operation.phase = 'opening-stream';
    // At the settling-group limit this waits under the write deadline, which reports it as a
    // WRITE_TIMEOUT in this phase rather than letting the transport refuse the stream.
    if (!settlingGroups.hasRoom()) {
      await settlingGroups.waitForRoom(operation.interrupted);
      check(operation);
    }
    const subgroup = await openSubgroup(operation.alias, groupId);
    if (operation.cancelled) {
      // The stream opened after the failure: reset it without sending payload bytes (§11.4.3).
      subgroup.abort(resetReason(operation.code, operation.failure));
      check(operation);
    }
    operation.writer = subgroup;
    return subgroup;
  };
  const release = (operation: Operation) => {
    if (active === operation) active = undefined;
    operation.cleanup();
  };
  // The operation is complete: its FIN was handed over, so the group settles on the connection's
  // account and the slot is free.
  const completeOperation = (operation: Operation, subgroup: SubgroupWriter) => {
    settlingGroups.add(subgroup.fin());
    release(operation);
  };

  const writer: RelayTrackWriter = {
    get hasDemand() {
      return alias !== undefined && !ended;
    },
    get closed() {
      return ended;
    },
    demandChanges() {
      const queue: boolean[] = ended ? [] : [alias !== undefined];
      const waiting: ((result: IteratorResult<boolean>) => void)[] = [];
      let done = ended;
      const push = (value: boolean | undefined) => {
        if (done) return;
        if (value === undefined) {
          done = true;
          for (const resolve of waiting.splice(0)) resolve({ done: true, value: undefined });
          watchers.delete(push);
        } else {
          const resolve = waiting.shift();
          if (resolve) resolve({ done: false, value });
          else queue.push(value);
        }
      };
      if (!done) watchers.add(push);
      const iterator: AsyncIterableIterator<boolean> = {
        [Symbol.asyncIterator]() {
          return iterator;
        },
        next() {
          const value = queue.shift();
          if (value !== undefined) return Promise.resolve({ done: false, value });
          if (done) return Promise.resolve({ done: true, value: undefined });
          return new Promise((resolve) => {
            waiting.push(resolve);
          });
        },
        return() {
          queue.length = 0;
          push(undefined);
          return Promise.resolve({ done: true, value: undefined });
        },
      };
      return iterator;
    },
    close: () => close(),
    async writeText(text, writeOptions) {
      await writer.write(encodeText(text), writeOptions);
    },
    async writeJSON(value, writeOptions) {
      await writer.write(encodeJSON(value), writeOptions);
    },
    async write(payload, writeOptions = {}) {
      // Arguments before slot state: a bad payload is a programming error whatever the slot holds.
      const bytes = payloadBytes(payload);
      const properties = propertyPairs(writeOptions.properties);
      assertWritable();
      const operation = begin('write', writeOptions.signal);
      try {
        const subgroup = await run(operation, async () => {
          const opened = await openStream(operation);
          operation.phase = 'writing';
          await opened.writeObject({ objectId: 0, properties, payload: bytes });
          check(operation);
          operation.phase = 'closing';
          await opened.handOverFin();
          return opened;
        });
        completeOperation(operation, subgroup);
      } finally {
        release(operation);
      }
    },
    async openGroup(groupOptions = {}) {
      assertWritable();
      const operation = begin('group', groupOptions.signal);
      const subgroup = await run(operation, () => openStream(operation));
      let objectCount = 0;
      let pending = false;
      let complete = false;
      const assertGroup = () => {
        check(operation);
        // The FIN already went out, so nothing of the refused call is sent.
        if (complete) throw new RelayError('CLOSED', 'Group is closed', 'waiting-for-demand');
        if (pending) throw new RelayError('WRITE_PENDING', 'A group write or close is already pending', 'writing');
      };
      const groupWriter: RelayGroupWriter = {
        groupId: subgroup.groupId,
        get objectCount() {
          return objectCount;
        },
        get closed() {
          return complete || operation.cancelled;
        },
        async writeText(text, writeOptions) {
          await groupWriter.write(encodeText(text), writeOptions);
        },
        async writeJSON(value, writeOptions) {
          await groupWriter.write(encodeJSON(value), writeOptions);
        },
        async write(payload, writeOptions = {}) {
          const bytes = payloadBytes(payload);
          const properties = propertyPairs(writeOptions.properties);
          const status = writeOptions.status ?? 'normal';
          if (typeof status !== 'string') throw new TypeError('Object status must be a string');
          if (!['normal', 'end-of-group', 'end-of-track'].includes(status))
            throw new RangeError('Unknown object status');
          assertGroup();
          pending = true;
          try {
            operation.phase = 'writing';
            await run(operation, () =>
              subgroup.writeObject({ objectId: objectCount, properties, payload: bytes, status }),
            );
            objectCount++;
          } finally {
            pending = false;
          }
        },
        async close() {
          if (complete) return;
          assertGroup();
          pending = true;
          try {
            operation.phase = 'closing';
            await run(operation, () => subgroup.handOverFin());
            complete = true;
            completeOperation(operation, subgroup);
          } finally {
            pending = false;
          }
        },
        abort(reason = new RelayError('CLOSED', 'Group was aborted', operation.phase)) {
          // After a clean close the FIN already ended the stream; a later reset is ignorable (§11.4.3).
          if (!complete) operation.cancel(reason, RESET_CODE.CANCELLED);
        },
      };
      return groupWriter;
    },
  };

  return {
    writer,
    bind(nextAlias) {
      if (active?.alias !== undefined && active.alias !== nextAlias) {
        active.cancel(
          new RelayError(
            'SUBSCRIPTION_CHANGED',
            'Relay subscription changed during a write or group; objects may be incomplete',
            active.phase,
          ),
          RESET_CODE.CANCELLED,
        );
      }
      const changed = alias !== nextAlias;
      alias = nextAlias;
      if (alias !== undefined) active?.wake();
      if (changed && !ended) notify(alias !== undefined);
    },
    disconnect(error) {
      close(error, connectionResetCode(error));
    },
  };
}
