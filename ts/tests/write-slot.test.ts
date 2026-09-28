/**
 * The write slot through its own interface: the public track writer plus the two inbound calls the
 * publisher makes, driven with an in-memory subgroup writer and fake timers. Rows follow the
 * "Write phases and interruptions" table in spec/behavior.md. Relay behaviour the slot cannot see
 * (demand arriving through the real binding callback, announce-and-serve) stays in publish.test.ts.
 */
import type { SubgroupWriter } from '@mux/moq-core';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { RelayError } from '../src/errors.js';
import { createWriteSlot, RESET_CODE, StreamReset } from '../src/write-slot.js';

type ObjectInput = Parameters<SubgroupWriter['writeObject']>[0];
type Stall = 'none' | 'write' | 'second-write' | 'fin';

/** One subgroup stream as the slot sees it: records objects, FIN and abort reasons, and can stall or fail. */
class FakeSubgroup implements SubgroupWriter {
  readonly trackAlias: number;
  readonly groupId: number;
  readonly objects: ObjectInput[] = [];
  readonly aborts: unknown[] = [];
  finCalls = 0;
  readonly #stall: Stall;
  #waiting: { resolve(): void; reject(error: unknown): void } | undefined;

  constructor(trackAlias: number, groupId: number, stall: Stall) {
    this.trackAlias = trackAlias;
    this.groupId = groupId;
    this.#stall = stall;
  }

  writeObject(object: ObjectInput): Promise<void> {
    this.objects.push(object);
    return this.#settle(this.#stall === 'write' || (this.#stall === 'second-write' && this.objects.length === 2));
  }

  fin(): Promise<void> {
    this.finCalls++;
    return this.#settle(this.#stall === 'fin');
  }

  abort(reason?: unknown): void {
    this.aborts.push(reason);
    this.#waiting?.reject(reason);
    this.#waiting = undefined;
  }

  /** The transport rejects the stalled step, as a failed stream write would. */
  fail(error: unknown): void {
    this.#waiting?.reject(error);
    this.#waiting = undefined;
  }

  /** The transport accepts the stalled step. */
  release(): void {
    this.#waiting?.resolve();
    this.#waiting = undefined;
  }

  get stalled(): boolean {
    return this.#waiting !== undefined;
  }

  #settle(stall: boolean): Promise<void> {
    if (!stall) return Promise.resolve();
    return new Promise((resolve, reject) => {
      this.#waiting = { resolve, reject };
    });
  }
}

interface HarnessOptions {
  writeTimeoutMs?: number;
  solicited?: boolean | undefined;
  stall?: Stall;
  open?: 'ok' | 'hold' | 'reject';
}

function harness(options: HarnessOptions = {}) {
  const opened: FakeSubgroup[] = [];
  const holds: (() => void)[] = [];
  const openFailure = new Error('data stream failed');
  const connection: { closed: boolean; error?: unknown } = { closed: false };
  const endTrack = vi.fn();
  const slot = createWriteSlot({
    async openSubgroup(trackAlias, groupId) {
      if (options.open === 'reject') throw openFailure;
      // Only the first stream stalls, so the recovery write after a failure can complete.
      const subgroup = new FakeSubgroup(trackAlias, groupId, opened.length === 0 ? (options.stall ?? 'none') : 'none');
      opened.push(subgroup);
      if (options.open === 'hold') {
        return new Promise<FakeSubgroup>((resolve) => {
          holds.push(() => resolve(subgroup));
        });
      }
      return subgroup;
    },
    namespaceSolicited: () => options.solicited,
    assertOpen() {
      if (connection.closed)
        throw connection.error ?? new RelayError('CLOSED', 'Relay connection is closed', 'connection');
    },
    endTrack,
    writeTimeoutMs: options.writeTimeoutMs ?? 1_000,
  });
  return {
    slot,
    writer: slot.writer,
    opened,
    endTrack,
    openFailure,
    /** Let a held stream open complete, as a late transport would. */
    releaseOpen: () => holds.shift()?.(),
    /** The connection ends: the lifetime's open check fails, then the publisher tells the slot. */
    disconnect(error?: unknown) {
      connection.closed = true;
      connection.error = error;
      slot.disconnect(error);
    },
    /** The connection ends before the publisher's callback reaches the slot. */
    failConnection(error: unknown) {
      connection.closed = true;
      connection.error = error;
    },
  };
}

const bytes = (...values: number[]) => new Uint8Array(values);
const flush = () => vi.advanceTimersByTimeAsync(0);
const elapse = (ms: number) => vi.advanceTimersByTimeAsync(ms);
const resetWith = (code: number) => expect.objectContaining({ streamErrorCode: code });

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

describe('write slot: waiting for demand', () => {
  it('sends one object as its own group once demand is bound, copying the payload first', async () => {
    const { slot, writer, opened } = harness();
    const payload = bytes(1, 2);
    const writing = writer.write(payload, { properties: [{ type: 2, value: 7 }] });
    payload[0] = 9;
    expect(writer.hasDemand).toBe(false);
    await flush();
    expect(opened).toHaveLength(0);
    slot.bind(4);
    expect(writer.hasDemand).toBe(true);
    await writing;
    expect(opened).toHaveLength(1);
    expect(opened[0]).toMatchObject({ trackAlias: 4, groupId: 0, finCalls: 1, aborts: [] });
    expect(opened[0]?.objects).toEqual([{ objectId: 0, properties: [{ type: 2, value: 7 }], payload: bytes(1, 2) }]);
    await writer.write(bytes(3));
    expect(opened[1]).toMatchObject({ trackAlias: 4, groupId: 1 });
  });

  it('times out with no stream opened and no group ID consumed, then writes after demand arrives', async () => {
    const { slot, writer, opened } = harness({ writeTimeoutMs: 50 });
    const writing = writer.write(bytes(1));
    const rejected = expect(writing).rejects.toMatchObject({ code: 'WRITE_TIMEOUT', phase: 'waiting-for-demand' });
    await elapse(50);
    await rejected;
    expect(opened).toHaveLength(0);
    expect(writer.closed).toBe(false);
    slot.bind(1);
    await writer.write(bytes(2));
    expect(opened[0]?.groupId).toBe(0);
  });

  it.each([
    [false, 'NAMESPACE_NOT_SOLICITED'],
    [true, 'WRITE_TIMEOUT'],
    [undefined, 'WRITE_TIMEOUT'],
  ] as const)('with namespaceSolicited %s the deadline reports %s', async (solicited, code) => {
    const { writer } = harness({ writeTimeoutMs: 50, solicited });
    const rejected = expect(writer.write(bytes(1))).rejects.toMatchObject({ code, phase: 'waiting-for-demand' });
    await elapse(50);
    await rejected;
  });

  it('keeps waiting when the relay subscription ends before any demand was used', async () => {
    const { slot, writer, opened } = harness({ writeTimeoutMs: 50 });
    const rejected = expect(writer.write(bytes(1))).rejects.toMatchObject({ code: 'WRITE_TIMEOUT' });
    slot.bind(undefined);
    await elapse(25);
    expect(opened).toHaveLength(0);
    await elapse(25);
    await rejected;
  });

  it('rejects a second write or group with WRITE_PENDING and leaves the first unaffected', async () => {
    const { slot, writer, opened } = harness();
    const writing = writer.write(bytes(1));
    await expect(writer.write(bytes(2))).rejects.toMatchObject({ code: 'WRITE_PENDING', phase: 'writing' });
    await expect(writer.openGroup()).rejects.toMatchObject({ code: 'WRITE_PENDING', phase: 'writing' });
    slot.bind(1);
    await writing;
    expect(opened).toHaveLength(1);
  });

  it.each([
    ['a live signal', false],
    ['an already-aborted signal', true],
  ])('surfaces the caller reason unwrapped for %s', async (_, aborted) => {
    const { writer, opened } = harness();
    const owner = new AbortController();
    const reason = new Error('caller moved on');
    if (aborted) owner.abort(reason);
    const writing = writer.write(bytes(1), { signal: owner.signal });
    const rejected = expect(writing).rejects.toBe(reason);
    if (!aborted) owner.abort(reason);
    await rejected;
    expect(opened).toHaveLength(0);
    expect(writer.closed).toBe(false);
  });

  it.each([undefined, null])('preserves a %s abort reason as the contract says', async (reason) => {
    const { writer } = harness();
    const owner = new AbortController();
    // Older AbortSignal polyfills expose abort events but no reason field.
    const signal = {
      get aborted() {
        return owner.signal.aborted;
      },
      reason,
      addEventListener: owner.signal.addEventListener.bind(owner.signal),
      removeEventListener: owner.signal.removeEventListener.bind(owner.signal),
    } as AbortSignal;
    const writing = writer.write(bytes(1), { signal });
    const rejected =
      reason === undefined
        ? expect(writing).rejects.toMatchObject({ code: 'CLOSED', phase: 'waiting-for-demand' })
        : expect(writing).rejects.toBe(null);
    owner.abort();
    await rejected;
  });

  it('ends the track on close, cancelling the wait and telling the core once', async () => {
    const { writer, endTrack, opened } = harness();
    const rejected = expect(writer.write(bytes(1))).rejects.toMatchObject({
      code: 'CLOSED',
      phase: 'waiting-for-demand',
    });
    writer.close();
    writer.close();
    await rejected;
    expect(endTrack).toHaveBeenCalledOnce();
    expect(writer.closed).toBe(true);
    expect(writer.hasDemand).toBe(false);
    expect(opened).toHaveLength(0);
    await expect(writer.write(bytes(2))).rejects.toMatchObject({ code: 'CLOSED' });
  });

  it.each([
    ['its failure', new RelayError('TRANSPORT_ERROR', 'link dropped', 'connection')],
    ['a clean shutdown', undefined],
  ])('shares the connection end (%s) with the pending write and ends the track', async (_, error) => {
    const { writer, disconnect, endTrack } = harness();
    const writing = writer.write(bytes(1));
    const rejected = error
      ? expect(writing).rejects.toBe(error)
      : expect(writing).rejects.toMatchObject({ code: 'CLOSED' });
    disconnect(error);
    await rejected;
    expect(writer.closed).toBe(true);
    expect(endTrack).toHaveBeenCalledOnce();
  });
});

describe('write slot: opening the stream', () => {
  it('times out a held open, resets the late stream after its header with DELIVERY_TIMEOUT, and moves on', async () => {
    const { slot, writer, opened, releaseOpen } = harness({ writeTimeoutMs: 50, open: 'hold' });
    slot.bind(1);
    const rejected = expect(writer.write(bytes(1))).rejects.toMatchObject({
      code: 'WRITE_TIMEOUT',
      phase: 'opening-stream',
    });
    await elapse(50);
    await rejected;
    expect(writer.closed).toBe(false);
    releaseOpen();
    await flush();
    expect(opened[0]?.aborts).toEqual([resetWith(RESET_CODE.DELIVERY_TIMEOUT)]);
    expect(opened[0]?.objects).toEqual([]);
    const next = writer.write(bytes(2));
    releaseOpen();
    await next;
    expect(opened[1]).toMatchObject({ groupId: 1, finCalls: 1 });
  });

  it('reports a refused stream as TRANSPORT_ERROR with the cause and leaves the track usable', async () => {
    const { slot, writer, openFailure } = harness({ open: 'reject' });
    slot.bind(1);
    await expect(writer.write(bytes(1))).rejects.toMatchObject({
      code: 'TRANSPORT_ERROR',
      phase: 'opening-stream',
      cause: openFailure,
    });
    expect(writer.closed).toBe(false);
    expect(writer.hasDemand).toBe(true);
  });

  it('cancels a held open with the caller reason and resets the late stream with CANCELLED', async () => {
    const { slot, writer, opened, releaseOpen } = harness({ open: 'hold' });
    slot.bind(1);
    const owner = new AbortController();
    const reason = new Error('stop');
    const rejected = expect(writer.write(bytes(1), { signal: owner.signal })).rejects.toBe(reason);
    await flush();
    owner.abort(reason);
    await rejected;
    releaseOpen();
    await flush();
    expect(opened[0]?.aborts).toEqual([resetWith(RESET_CODE.CANCELLED)]);
  });

  it('fails a held open with SUBSCRIPTION_CHANGED when the relay rebinds, then serves the new alias', async () => {
    const { slot, writer, opened, releaseOpen } = harness({ open: 'hold' });
    slot.bind(1);
    const rejected = expect(writer.write(bytes(1))).rejects.toMatchObject({
      code: 'SUBSCRIPTION_CHANGED',
      phase: 'opening-stream',
    });
    await flush();
    slot.bind(2);
    await rejected;
    releaseOpen();
    await flush();
    expect(opened[0]?.aborts).toEqual([resetWith(RESET_CODE.CANCELLED)]);
    const next = writer.write(bytes(2));
    releaseOpen();
    await next;
    expect(opened[1]).toMatchObject({ trackAlias: 2, groupId: 1 });
  });

  it.each([
    ['a transport failure', new RelayError('TRANSPORT_ERROR', 'gone', 'connection'), RESET_CODE.SESSION_CLOSED],
    ['GOAWAY', new RelayError('GOAWAY', 'migrate', 'connection'), RESET_CODE.GOING_AWAY],
  ])('resets a late stream after the connection ended by %s with the matching code', async (_, error, code) => {
    const { slot, writer, opened, releaseOpen, disconnect } = harness({ open: 'hold' });
    slot.bind(1);
    const rejected = expect(writer.write(bytes(1))).rejects.toBe(error);
    await flush();
    disconnect(error);
    await rejected;
    releaseOpen();
    await flush();
    expect(opened[0]?.aborts).toEqual([resetWith(code)]);
    expect(writer.closed).toBe(true);
  });
});

describe('write slot: writing and closing a single write', () => {
  it.each([
    ['write', 'writing'],
    ['fin', 'closing'],
  ] as const)(
    'times out a stalled %s, resets with DELIVERY_TIMEOUT, and keeps the track and demand',
    async (stall, phase) => {
      const { slot, writer, opened } = harness({ writeTimeoutMs: 100, stall });
      slot.bind(1);
      const rejected = expect(writer.write(bytes(1))).rejects.toMatchObject({ code: 'WRITE_TIMEOUT', phase });
      await flush();
      expect(opened[0]?.stalled).toBe(true);
      await expect(writer.write(bytes(2))).rejects.toMatchObject({ code: 'WRITE_PENDING' });
      await elapse(100);
      await rejected;
      expect(opened[0]?.aborts).toEqual([resetWith(RESET_CODE.DELIVERY_TIMEOUT)]);
      expect(writer.closed).toBe(false);
      expect(writer.hasDemand).toBe(true);
    },
  );

  it('cancels a stalled write with the caller reason and resets with CANCELLED', async () => {
    const { slot, writer, opened } = harness({ stall: 'write' });
    slot.bind(1);
    const owner = new AbortController();
    const reason = new Error('discard stale object');
    const rejected = expect(writer.write(bytes(1), { signal: owner.signal })).rejects.toBe(reason);
    await flush();
    owner.abort(reason);
    await rejected;
    expect(opened[0]?.aborts).toEqual([resetWith(RESET_CODE.CANCELLED)]);
    await writer.write(bytes(2));
    expect(opened[1]).toMatchObject({ groupId: 1, finCalls: 1 });
  });

  it('wraps a failed stream write as TRANSPORT_ERROR with the cause and resets with INTERNAL_ERROR', async () => {
    const { slot, writer, opened } = harness({ stall: 'write' });
    slot.bind(1);
    const failure = new Error('stream write failed');
    const rejected = expect(writer.write(bytes(1))).rejects.toMatchObject({
      code: 'TRANSPORT_ERROR',
      phase: 'writing',
      cause: failure,
    });
    await flush();
    opened[0]?.fail(failure);
    await rejected;
    expect(opened[0]?.aborts).toEqual([resetWith(RESET_CODE.INTERNAL_ERROR)]);
    expect(writer.closed).toBe(false);
  });

  it.each([
    ['ends', undefined],
    ['is replaced', 2],
  ])('fails an in-flight write with SUBSCRIPTION_CHANGED when the relay subscription %s', async (_, nextAlias) => {
    const { slot, writer, opened } = harness({ stall: 'write' });
    slot.bind(1);
    const rejected = expect(writer.write(bytes(1))).rejects.toMatchObject({
      code: 'SUBSCRIPTION_CHANGED',
      phase: 'writing',
    });
    await flush();
    slot.bind(nextAlias);
    await rejected;
    expect(opened[0]?.aborts).toEqual([resetWith(RESET_CODE.CANCELLED)]);
    expect(writer.closed).toBe(false);
    expect(writer.hasDemand).toBe(nextAlias !== undefined);
  });

  it('cancels an in-flight write with CLOSED when the track closes and resets with CANCELLED', async () => {
    const { slot, writer, opened, endTrack } = harness({ stall: 'write' });
    slot.bind(1);
    const rejected = expect(writer.write(bytes(1))).rejects.toMatchObject({ code: 'CLOSED', phase: 'writing' });
    await flush();
    writer.close();
    await rejected;
    expect(opened[0]?.aborts).toEqual([resetWith(RESET_CODE.CANCELLED)]);
    expect(endTrack).toHaveBeenCalledOnce();
    expect(writer.closed).toBe(true);
  });

  it('reports the connection failure, not a wrapped transport error, when closure precedes the callback', async () => {
    const { slot, writer, opened, failConnection } = harness({ stall: 'fin' });
    slot.bind(1);
    const error = new RelayError('TRANSPORT_ERROR', 'connection dropped', 'connection');
    const rejected = expect(writer.write(bytes(1))).rejects.toBe(error);
    await flush();
    failConnection(error);
    opened[0]?.release();
    await rejected;
    expect(opened[0]?.aborts).toEqual([resetWith(RESET_CODE.SESSION_CLOSED)]);
  });

  it.each(Array.from({ length: 8 }, (_, turns) => turns))(
    'settles with the owner reason or success, never a wrapped error, when closure lands %s microtasks after FIN',
    async (turns) => {
      const { slot, writer, opened, disconnect } = harness({ stall: 'fin' });
      slot.bind(1);
      const reason = new Error('owner finished');
      const writing = writer.write(bytes(1));
      await flush();
      opened[0]?.release();
      let completed = Promise.resolve();
      for (let turn = 0; turn < turns; turn++) completed = completed.then(() => {});
      void completed.then(() => disconnect(reason));
      await writing.catch((error: unknown) => expect(error).toBe(reason));
      await completed;
      await flush();
      expect(writer.closed).toBe(true);
    },
  );
});

describe('write slot: groups', () => {
  it('holds the slot for the group, appends objects with increasing IDs, and FINs once on close', async () => {
    const { slot, writer, opened } = harness();
    slot.bind(1);
    const group = await writer.openGroup();
    expect(group).toMatchObject({ groupId: 0, objectCount: 0, closed: false });
    await expect(writer.write(bytes(1))).rejects.toMatchObject({ code: 'WRITE_PENDING' });
    await expect(writer.openGroup()).rejects.toMatchObject({ code: 'WRITE_PENDING' });
    await expect(group.write('bad' as unknown as ArrayBufferView)).rejects.toThrow(TypeError);
    await expect(group.write(bytes(1), { properties: [{ type: 3, value: 2 }] })).rejects.toThrow(TypeError);
    await expect(group.write(bytes(), { status: 'later' as never })).rejects.toThrow(RangeError);
    await expect(group.write(bytes(), { status: 'later' as never })).rejects.toThrow('Unknown object status');
    await expect(group.write(bytes(), { status: 7 as never })).rejects.toThrow(TypeError);
    await group.write(bytes(1));
    await group.write(bytes(2), { properties: [{ type: 2, value: 7 }] });
    await group.write(bytes(), { status: 'end-of-group' });
    expect(group.objectCount).toBe(3);
    expect(opened[0]?.objects.map((object) => [object.objectId, object.status])).toEqual([
      [0, 'normal'],
      [1, 'normal'],
      [2, 'end-of-group'],
    ]);
    await group.close();
    await group.close();
    expect(group.closed).toBe(true);
    expect(opened[0]?.finCalls).toBe(1);
    await expect(group.write(bytes(9))).rejects.toMatchObject({ code: 'CLOSED' });
    await writer.write(bytes(3));
    expect(opened[1]?.groupId).toBe(1);
  });

  it('waits for demand before opening a group and releases the slot after a timeout or pre-aborted signal', async () => {
    const { slot, writer, opened } = harness({ writeTimeoutMs: 50 });
    const rejected = expect(writer.openGroup()).rejects.toMatchObject({
      code: 'WRITE_TIMEOUT',
      phase: 'waiting-for-demand',
    });
    await elapse(50);
    await rejected;
    const owner = new AbortController();
    const reason = new Error('cancelled before opening');
    owner.abort(reason);
    await expect(writer.openGroup({ signal: owner.signal })).rejects.toBe(reason);
    expect(writer.closed).toBe(false);
    slot.bind(1);
    const group = await writer.openGroup();
    await group.write(bytes(1));
    await group.close();
    expect(opened).toHaveLength(1);
  });

  it.each([
    ['write', 'second-write', 'writing'],
    ['close', 'fin', 'closing'],
  ] as const)(
    'times out a stalled group %s, resets it with DELIVERY_TIMEOUT, and permits the next group',
    async (mode, stall, phase) => {
      const { slot, writer, opened } = harness({ writeTimeoutMs: 100, stall });
      slot.bind(1);
      const group = await writer.openGroup();
      await group.write(bytes(1));
      const pending = mode === 'write' ? group.write(bytes(2)) : group.close();
      const rejected = expect(pending).rejects.toMatchObject({ code: 'WRITE_TIMEOUT', phase });
      await flush();
      expect(opened[0]?.stalled).toBe(true);
      await expect(group.write(bytes(3))).rejects.toMatchObject({ code: 'WRITE_PENDING' });
      await expect(group.close()).rejects.toMatchObject({ code: 'WRITE_PENDING' });
      await elapse(100);
      await rejected;
      expect(opened[0]?.aborts).toEqual([resetWith(RESET_CODE.DELIVERY_TIMEOUT)]);
      expect(group.closed).toBe(true);
      expect(writer.closed).toBe(false);
      const next = await writer.openGroup();
      expect(next.groupId).toBe(1);
      await next.write(bytes(4));
      await next.close();
    },
  );

  it('aborts an idle group explicitly or through its signal, resetting with CANCELLED, and frees the slot', async () => {
    const { slot, writer, opened } = harness();
    slot.bind(1);
    const owner = new AbortController();
    const first = await writer.openGroup({ signal: owner.signal });
    await first.write(bytes(1));
    const reason = new Error('drop incomplete GOP');
    owner.abort(reason);
    await expect(first.write(bytes(2))).rejects.toBe(reason);
    await expect(first.close()).rejects.toBe(reason);
    expect(first.closed).toBe(true);
    expect(opened[0]?.aborts).toEqual([resetWith(RESET_CODE.CANCELLED)]);
    const second = await writer.openGroup();
    second.abort(reason);
    expect(second.closed).toBe(true);
    expect(opened[1]?.aborts).toEqual([resetWith(RESET_CODE.CANCELLED)]);
    const third = await writer.openGroup();
    third.abort();
    await expect(third.close()).rejects.toMatchObject({ code: 'CLOSED' });
    const fourth = await writer.openGroup();
    await fourth.write(bytes(3));
    await fourth.close();
    expect(fourth.groupId).toBe(3);
  });

  it('ignores abort after a clean close: no reset, closed stays true, close stays idempotent', async () => {
    const { slot, writer, opened } = harness();
    slot.bind(1);
    const group = await writer.openGroup();
    await group.write(bytes(1));
    await group.close();
    group.abort(new Error('late abort'));
    expect(group.closed).toBe(true);
    await group.close();
    expect(opened[0]?.aborts).toEqual([]);
    const next = await writer.openGroup();
    expect(next.groupId).toBe(1);
  });

  it('ends an idle group with SUBSCRIPTION_CHANGED when the relay subscription ends, keeping the track', async () => {
    const { slot, writer, opened } = harness();
    slot.bind(1);
    const group = await writer.openGroup();
    await group.write(bytes(1));
    slot.bind(undefined);
    expect(group.closed).toBe(true);
    expect(writer.closed).toBe(false);
    await expect(group.close()).rejects.toMatchObject({ code: 'SUBSCRIPTION_CHANGED', phase: 'writing' });
    expect(opened[0]?.aborts).toEqual([resetWith(RESET_CODE.CANCELLED)]);
    slot.bind(2);
    const next = await writer.openGroup();
    expect(next.groupId).toBe(1);
    expect(opened[1]?.trackAlias).toBe(2);
  });

  it('cancels an open group when the connection ends and reports the connection error on close', async () => {
    const { slot, writer, opened, disconnect } = harness();
    slot.bind(1);
    const group = await writer.openGroup();
    await group.write(bytes(1));
    const error = new RelayError('TRANSPORT_ERROR', 'gone', 'connection');
    disconnect(error);
    expect(group.closed).toBe(true);
    expect(writer.closed).toBe(true);
    await expect(group.close()).rejects.toBe(error);
    expect(opened[0]?.aborts).toEqual([resetWith(RESET_CODE.SESSION_CLOSED)]);
  });
});

describe('write slot: demand observers', () => {
  it('yields the current state first, repeats true on replacement, and ends with the track', async () => {
    const { slot, writer } = harness();
    const demand = writer.demandChanges();
    expect(await demand.next()).toEqual({ done: false, value: false });
    slot.bind(1);
    slot.bind(2);
    slot.bind(undefined);
    slot.bind(3);
    expect(await demand.next()).toEqual({ done: false, value: true });
    expect(await demand.next()).toEqual({ done: false, value: true });
    expect(await demand.next()).toEqual({ done: false, value: false });
    expect(await demand.next()).toEqual({ done: false, value: true });
    const other = writer.demandChanges();
    expect(await other.next()).toEqual({ done: false, value: true });
    const pending = other.next();
    await other.return?.();
    expect(await pending).toEqual({ done: true, value: undefined });
    writer.close();
    expect(writer.hasDemand).toBe(false);
    expect(await demand.next()).toEqual({ done: true, value: undefined });
    expect(await writer.demandChanges().next()).toEqual({ done: true, value: undefined });
  });

  it('does not notify observers after the track ended, even if the relay rebinds', async () => {
    const { slot, writer } = harness();
    const demand = writer.demandChanges();
    await demand.next();
    writer.close();
    slot.bind(1);
    expect(writer.hasDemand).toBe(false);
    expect(await demand.next()).toEqual({ done: true, value: undefined });
  });
});

describe('write slot: reset reasons', () => {
  it('carries the MOQT code on a StreamReset outside a browser, keeping the cause', () => {
    const cause = new Error('why');
    const reset = new StreamReset(RESET_CODE.DELIVERY_TIMEOUT, cause);
    expect(reset).toMatchObject({ name: 'StreamReset', streamErrorCode: 2, message: 'why', cause });
  });
});

it('resets browser streams with the legacy WebTransportError dictionary constructor', async () => {
  class LegacyWebTransportError extends Error {
    readonly streamErrorCode: number;
    constructor(init: { message: string; streamErrorCode: number }) {
      if (typeof init !== 'object') throw new TypeError('Expected WebTransportErrorInit');
      super(init.message);
      this.streamErrorCode = init.streamErrorCode;
    }
  }
  vi.stubGlobal('WebTransportError', LegacyWebTransportError);
  try {
    const { slot, writer, opened } = harness();
    slot.bind(1);
    const group = await writer.openGroup();
    expect(() => group.abort(new Error('Stopped'))).not.toThrow();
    expect(opened[0]?.aborts[0]).toBeInstanceOf(LegacyWebTransportError);
    expect(opened[0]?.aborts[0]).toMatchObject({ streamErrorCode: RESET_CODE.CANCELLED, message: 'Stopped' });
    expect(group.closed).toBe(true);
    await writer.write(bytes(2));
  } finally {
    vi.unstubAllGlobals();
  }
});
