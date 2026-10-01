/**
 * The write slot through its own interface: the public track writer plus the two inbound calls the
 * publisher makes, driven with an in-memory subgroup writer and fake timers. Rows follow the
 * "Write phases and interruptions" table in spec/behavior.md. Relay behaviour the slot cannot see
 * (demand arriving through the real binding callback, announce-and-serve) stays in publish.test.ts.
 */
import type { SubgroupWriteOptions, SubgroupWriter } from '@mux/moq-core';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { type DeliveryTimeouts, NO_DELIVERY_TIMEOUTS } from '../src/delivery-timeouts.js';
import { RelayError } from '../src/errors.js';
import type { RelayGroupWriter } from '../src/publish.js';
import { createSettlingGroups, SETTLING_GROUP_LIMIT, type SettlingGroups } from '../src/settling-groups.js';
import { createWriteSlot, RESET_CODE } from '../src/write-slot.js';

type ObjectInput = Parameters<SubgroupWriter['writeObject']>[0];
type Stall = 'none' | 'header' | 'write' | 'second-write' | 'fin';

/**
 * One subgroup stream as the slot sees it: records objects, FIN and abort reasons, and can stall or
 * fail. `fin` stalls the FIN's hand-over; how the handed-over FIN settles is separate, and with
 * `manualSettle` it waits for `acknowledge()` or `failSettle()`, as Chromium waits for the relay.
 */
class FakeSubgroup implements SubgroupWriter {
  readonly trackAlias: number;
  readonly groupId: number;
  readonly objects: ObjectInput[] = [];
  readonly aborts: unknown[] = [];
  finCalls = 0;
  readonly #stall: Stall;
  readonly #manualSettle: boolean;
  #waiting: { resolve(): void; reject(error: unknown): void } | undefined;
  readonly #settled: Promise<void>;
  #settle!: { resolve(): void; reject(error: unknown): void };

  constructor(trackAlias: number, groupId: number, stall: Stall, manualSettle = false) {
    this.trackAlias = trackAlias;
    this.groupId = groupId;
    this.#stall = stall;
    this.#manualSettle = manualSettle;
    this.#settled = new Promise((resolve, reject) => {
      this.#settle = { resolve, reject };
    });
    this.#settled.catch(() => {});
  }

  /** `header` holds the stream's header write before the object; an object refused by `beforeWrite` is not recorded. */
  async writeObject(object: ObjectInput, options: SubgroupWriteOptions = {}): Promise<void> {
    if (this.#stall === 'header' && this.objects.length === 0) await this.#stallStep(true);
    options.beforeWrite?.();
    this.objects.push(object);
    return this.#stallStep(this.#stall === 'write' || (this.#stall === 'second-write' && this.objects.length === 2));
  }

  fin(): Promise<void> {
    return this.#settled;
  }

  handOverFin(): Promise<void> {
    this.finCalls++;
    if (!this.#manualSettle) this.#settle.resolve();
    return this.#stallStep(this.#stall === 'fin');
  }

  /** The relay acknowledges the handed-over FIN. */
  acknowledge(): void {
    this.#settle.resolve();
  }

  /** The handed-over FIN fails late: STOP_SENDING, a reset, or session teardown. */
  failSettle(error: unknown): void {
    this.#settle.reject(error);
  }

  /** A reset fails a FIN still awaiting acknowledgment, as Chromium rejects its in-flight close (TA-16). */
  abort(reason?: unknown): void {
    this.aborts.push(reason);
    this.#waiting?.reject(reason);
    this.#waiting = undefined;
    this.#settle.reject(reason);
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

  #stallStep(stall: boolean): Promise<void> {
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
  /** Handed-over FINs settle only when the test says so. */
  manualSettle?: boolean;
  /** The connection's settling groups, shared when several slots stand for one connection. */
  settlingGroups?: SettlingGroups;
  /** The publisher's delivery timeouts from the track properties. */
  trackTimeouts?: Partial<DeliveryTimeouts>;
  /** Each relay subscription's delivery timeouts, by alias; the test may change them, as REQUEST_UPDATE would. */
  subscriberTimeouts?: Map<number, Partial<DeliveryTimeouts>>;
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
      const subgroup = new FakeSubgroup(
        trackAlias,
        groupId,
        opened.length === 0 ? (options.stall ?? 'none') : 'none',
        options.manualSettle,
      );
      opened.push(subgroup);
      if (options.open === 'hold') {
        return new Promise<FakeSubgroup>((resolve) => {
          holds.push(() => resolve(subgroup));
        });
      }
      return subgroup;
    },
    settlingGroups: options.settlingGroups ?? createSettlingGroups(),
    namespaceSolicited: () => options.solicited,
    assertOpen() {
      if (connection.closed)
        throw connection.error ?? new RelayError('CLOSED', 'Relay connection is closed', 'connection');
    },
    endTrack,
    writeTimeoutMs: options.writeTimeoutMs ?? 1_000,
    trackTimeouts: { ...NO_DELIVERY_TIMEOUTS, ...options.trackTimeouts },
    subscriberTimeouts: (alias) => ({ ...NO_DELIVERY_TIMEOUTS, ...options.subscriberTimeouts?.get(alias) }),
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

/**
 * A send-stream failure as Chromium reports it: a `WebTransportError` whose `source` says whether the
 * peer stopped the stream (`'stream'`, from STOP_SENDING) or the session failed (`'session'`).
 */
class FakeWebTransportError extends Error {
  override readonly name = 'WebTransportError';
  readonly source: 'stream' | 'session';
  readonly streamErrorCode: number | null;

  constructor(message: string, source: 'stream' | 'session', streamErrorCode: number | null = null) {
    super(message);
    this.source = source;
    this.streamErrorCode = streamErrorCode;
  }
}

/** The relay's STOP_SENDING on a data stream, as the rejected write or FIN carries it. */
const stopSending = () => new FakeWebTransportError('Received STOP_SENDING.', 'stream', 0);

/** The same stop when Chromium learns first that the network side closed the stream: a NetworkError DOMException. */
const remoteAbort = () =>
  Object.assign(new Error('The stream was aborted by the remote server'), { name: 'NetworkError' });

/** The same stop from a non-browser adapter: the TA-22 shape, with no `WebTransportError` name. */
const shapeOnlyStop = () =>
  Object.assign(new Error('peer stopped the stream'), { source: 'stream', streamErrorCode: 0 });

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
    const writing = writer.write(payload, { properties: [{ type: 0x7a, value: 7 }] });
    payload[0] = 9;
    expect(writer.hasDemand).toBe(false);
    await flush();
    expect(opened).toHaveLength(0);
    slot.bind(4);
    expect(writer.hasDemand).toBe(true);
    await writing;
    expect(opened).toHaveLength(1);
    expect(opened[0]).toMatchObject({ trackAlias: 4, groupId: 0, finCalls: 1, aborts: [] });
    expect(opened[0]?.objects).toEqual([{ objectId: 0, properties: [{ type: 0x7a, value: 7 }], payload: bytes(1, 2) }]);
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
    // Refused before any operation started: nothing sent, so not a sending phase.
    await expect(writer.write(bytes(2))).rejects.toMatchObject({ code: 'CLOSED', phase: 'waiting-for-demand' });
    await expect(writer.openGroup()).rejects.toMatchObject({ code: 'CLOSED', phase: 'waiting-for-demand' });
    expect(opened).toHaveLength(0);
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

  it.each([
    ['write', 'writing'],
    ['fin', 'closing'],
  ] as const)(
    'reports a relay stop of the %s as SUBSCRIPTION_CHANGED before the subscription ends, reporting nothing more',
    async (stall, phase) => {
      const { slot, writer, opened } = harness({ stall });
      slot.bind(1);
      const stop = stopSending();
      const outcome = writer.write(bytes(1)).catch((error: unknown) => error);
      await flush();
      opened[0]?.fail(stop);
      expect(await outcome).toMatchObject({ code: 'SUBSCRIPTION_CHANGED', phase, cause: stop });
      expect(() => slot.bind(undefined)).not.toThrow();
      expect(opened[0]?.aborts).toEqual([resetWith(RESET_CODE.CANCELLED)]);
      expect(writer.closed).toBe(false);
      expect(writer.hasDemand).toBe(false);
      // The subscription ended, so the next write waits for new demand.
      const next = writer.write(bytes(2));
      await flush();
      expect(opened).toHaveLength(1);
      slot.bind(2);
      await next;
      expect(opened[1]).toMatchObject({ trackAlias: 2, groupId: 1, finCalls: 1 });
    },
  );

  it.each([
    ['WebTransportError', stopSending],
    ['NetworkError', remoteAbort],
    ['shape only, TA-20 and TA-22', shapeOnlyStop],
  ])(
    'reports a relay stop (%s) as SUBSCRIPTION_CHANGED while the subscription goes on, and writes on at once',
    async (_, stopped) => {
      const { slot, writer, opened } = harness({ stall: 'write' });
      slot.bind(1);
      const stop = stopped();
      const rejected = expect(writer.write(bytes(1))).rejects.toMatchObject({
        code: 'SUBSCRIPTION_CHANGED',
        phase: 'writing',
        cause: stop,
      });
      await flush();
      opened[0]?.fail(stop);
      await rejected;
      expect(writer.hasDemand).toBe(true);
      await writer.write(bytes(2));
      expect(opened[1]).toMatchObject({ trackAlias: 1, groupId: 1, finCalls: 1 });
    },
  );

  it('keeps the binding change as the reason when the relay stop arrives after it', async () => {
    const { slot, writer, opened } = harness({ stall: 'write' });
    slot.bind(1);
    const outcome = writer.write(bytes(1)).catch((error: unknown) => error);
    await flush();
    slot.bind(undefined);
    opened[0]?.fail(stopSending());
    const error = await outcome;
    expect(error).toMatchObject({ code: 'SUBSCRIPTION_CHANGED', phase: 'writing' });
    expect(error).toHaveProperty(
      'message',
      'Relay subscription changed during a write or group; objects may be incomplete',
    );
    expect(error).not.toHaveProperty('cause');
    expect(opened[0]?.aborts).toEqual([resetWith(RESET_CODE.CANCELLED)]);
  });

  it('still reports a session failure on the stream as TRANSPORT_ERROR', async () => {
    const { slot, writer, opened } = harness({ stall: 'write' });
    slot.bind(1);
    const failure = new FakeWebTransportError('Connection lost.', 'session');
    const rejected = expect(writer.write(bytes(1))).rejects.toMatchObject({
      code: 'TRANSPORT_ERROR',
      phase: 'writing',
      cause: failure,
    });
    await flush();
    opened[0]?.fail(failure);
    await rejected;
    expect(opened[0]?.aborts).toEqual([resetWith(RESET_CODE.INTERNAL_ERROR)]);
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
    // The FIN already went out: the refused write sent nothing.
    await expect(group.write(bytes(9))).rejects.toMatchObject({ code: 'CLOSED', phase: 'waiting-for-demand' });
    expect(group.objectCount).toBe(3);
    expect(opened[0]?.objects).toHaveLength(3);
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

  it.each([
    ['write', 'second-write', 'writing'],
    ['close', 'fin', 'closing'],
  ] as const)(
    'reports a relay stop of a group %s as SUBSCRIPTION_CHANGED and permits the next group',
    async (mode, stall, phase) => {
      const { slot, writer, opened } = harness({ stall });
      slot.bind(1);
      const group = await writer.openGroup();
      await group.write(bytes(1));
      const stop = stopSending();
      const pending = mode === 'write' ? group.write(bytes(2)) : group.close();
      const rejected = expect(pending).rejects.toMatchObject({ code: 'SUBSCRIPTION_CHANGED', phase, cause: stop });
      await flush();
      opened[0]?.fail(stop);
      await rejected;
      expect(group.closed).toBe(true);
      slot.bind(undefined);
      expect(opened[0]?.aborts).toEqual([resetWith(RESET_CODE.CANCELLED)]);
      slot.bind(2);
      const next = await writer.openGroup();
      expect(next.groupId).toBe(1);
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

describe('write slot: settling groups', () => {
  it('completes a write once its FIN is handed over and accepts the next write before any acknowledgment', async () => {
    const { slot, writer, opened } = harness({ manualSettle: true });
    slot.bind(1);
    await writer.write(bytes(1));
    await writer.write(bytes(2));
    const group = await writer.openGroup();
    await group.write(bytes(3));
    await group.close();
    expect(group.closed).toBe(true);
    const next = await writer.openGroup();
    await next.close();
    expect(opened.map((subgroup) => [subgroup.groupId, subgroup.finCalls, subgroup.aborts.length])).toEqual([
      [0, 1, 0],
      [1, 1, 0],
      [2, 1, 0],
      [3, 1, 0],
    ]);
  });

  it('drops a settling group that fails late: nothing reported, the track stays usable', async () => {
    const { slot, writer, opened } = harness({ manualSettle: true });
    slot.bind(1);
    await writer.write(bytes(1));
    const group = await writer.openGroup();
    await group.close();
    opened[0]?.failSettle(stopSending());
    opened[1]?.failSettle(new Error('reset by the relay'));
    await flush();
    group.abort(new Error('late abort'));
    await writer.write(bytes(2));
    expect(writer.closed).toBe(false);
    expect(opened[2]).toMatchObject({ groupId: 2, finCalls: 1 });
    expect(opened.flatMap((subgroup) => subgroup.aborts)).toEqual([]);
  });

  it('never resets a settling group when the relay subscription changes or ends', async () => {
    const { slot, writer, opened } = harness({ manualSettle: true });
    slot.bind(1);
    const owner = new AbortController();
    await writer.write(bytes(1), { signal: owner.signal });
    slot.bind(2);
    slot.bind(undefined);
    owner.abort(new Error('caller moved on'));
    await flush();
    expect(opened[0]?.aborts).toEqual([]);
  });

  it('waits in opening-stream at the connection limit and proceeds when a group settles', async () => {
    const settlingGroups = createSettlingGroups();
    const first = harness({ manualSettle: true, settlingGroups });
    const second = harness({ manualSettle: true, settlingGroups });
    first.slot.bind(1);
    second.slot.bind(1);
    for (let i = 0; i < SETTLING_GROUP_LIMIT; i++) await first.writer.write(bytes(i % 256));
    const waiting = second.writer.write(bytes(1));
    await flush();
    expect(second.opened).toHaveLength(0);
    await expect(second.writer.write(bytes(2))).rejects.toMatchObject({ code: 'WRITE_PENDING' });
    first.opened[5]?.acknowledge();
    await waiting;
    expect(second.opened).toHaveLength(1);
  });

  it('fails WRITE_TIMEOUT in opening-stream when no group settles within the deadline', async () => {
    const { slot, writer, opened } = harness({ manualSettle: true, writeTimeoutMs: 100 });
    slot.bind(1);
    for (let i = 0; i < SETTLING_GROUP_LIMIT; i++) await writer.write(bytes(1));
    const rejected = expect(writer.write(bytes(2))).rejects.toMatchObject({
      code: 'WRITE_TIMEOUT',
      phase: 'opening-stream',
    });
    await elapse(100);
    await rejected;
    expect(opened).toHaveLength(SETTLING_GROUP_LIMIT);
    expect(writer.closed).toBe(false);
    // The timed-out wait gave up its turn: the next place goes to the next write.
    opened[0]?.acknowledge();
    await writer.write(bytes(3));
    expect(opened[SETTLING_GROUP_LIMIT]).toMatchObject({ groupId: SETTLING_GROUP_LIMIT + 1, finCalls: 1 });
  });

  it('reports a stream that failed before the FIN was handed over, rather than dropping it', async () => {
    const { slot, writer, opened } = harness({ stall: 'fin' });
    slot.bind(1);
    const group = await writer.openGroup();
    await group.write(bytes(1));
    const failure = new Error('STOP_SENDING before the FIN');
    const rejected = expect(group.close()).rejects.toMatchObject({
      code: 'TRANSPORT_ERROR',
      phase: 'closing',
      cause: failure,
    });
    await flush();
    opened[0]?.fail(failure);
    await rejected;
    expect(opened[0]?.aborts).toEqual([resetWith(RESET_CODE.INTERNAL_ERROR)]);
    expect(group.closed).toBe(true);
    expect(writer.closed).toBe(false);
  });

  it('counts settling groups only: open groups on many tracks never make another track wait', async () => {
    const settlingGroups = createSettlingGroups();
    const tracks = Array.from({ length: SETTLING_GROUP_LIMIT + 1 }, () =>
      harness({ manualSettle: true, settlingGroups }),
    );
    for (const track of tracks) track.slot.bind(1);
    const groups: RelayGroupWriter[] = [];
    for (const track of tracks) groups.push(await track.writer.openGroup());
    const extra = harness({ manualSettle: true, settlingGroups });
    extra.slot.bind(1);
    await extra.writer.write(bytes(2));
    expect(extra.opened).toHaveLength(1);
    // Closing every open group may take the count past the limit for a moment; nothing is refused.
    for (const group of groups) await group.close();
    expect(settlingGroups.size).toBe(SETTLING_GROUP_LIMIT + 2);
  });
});

describe('write slot: delivery timeouts (draft-20 §8)', () => {
  const MAX_TIMER_MS = 2_147_483_647;

  /** One track on its own connection, bound to alias 1, with FINs that wait for `acknowledge()`. */
  function settling(options: HarnessOptions = {}) {
    const settlingGroups = createSettlingGroups();
    const track = harness({ manualSettle: true, settlingGroups, ...options });
    track.slot.bind(1);
    return { ...track, settlingGroups };
  }

  it('resets a settling group with DELIVERY_TIMEOUT once its subgroup timeout runs out, reporting nothing', async () => {
    const { writer, opened, settlingGroups } = settling({ trackTimeouts: { subgroupMs: 50 } });
    await writer.write(bytes(1));
    expect(settlingGroups.size).toBe(1);
    await elapse(49);
    expect(opened[0]?.aborts).toEqual([]);
    await elapse(1);
    expect(opened[0]?.aborts).toEqual([resetWith(RESET_CODE.DELIVERY_TIMEOUT)]);
    expect(settlingGroups.size).toBe(0);
    expect(writer.closed).toBe(false);
    await writer.write(bytes(2));
    expect(opened[1]).toMatchObject({ groupId: 1, aborts: [] });
  });

  it('leaves a group the relay acknowledged in time alone', async () => {
    const { writer, opened, settlingGroups } = settling({ trackTimeouts: { subgroupMs: 50 } });
    await writer.write(bytes(1));
    opened[0]?.acknowledge();
    await elapse(100);
    expect(opened[0]?.aborts).toEqual([]);
    expect(settlingGroups.size).toBe(0);
  });

  it.each([
    [50, 30, 30],
    [30, 50, 30],
    [0, 40, 40],
    [40, 0, 40],
  ])(
    'with publisher %i ms and subscriber %i ms the subgroup timeout is %i ms',
    async (publisher, subscriber, expected) => {
      const { writer, opened } = settling({
        trackTimeouts: { subgroupMs: publisher },
        subscriberTimeouts: new Map([[1, { subgroupMs: subscriber }]]),
      });
      await writer.write(bytes(1));
      await elapse(expected - 1);
      expect(opened[0]?.aborts).toEqual([]);
      await elapse(1);
      expect(opened[0]?.aborts).toEqual([resetWith(RESET_CODE.DELIVERY_TIMEOUT)]);
    },
  );

  it('starts no timer when both sides are 0 or unset, or a property holds bytes', async () => {
    const { writer, opened } = settling({
      trackTimeouts: { objectMs: 0, subgroupMs: 0 },
      subscriberTimeouts: new Map([[1, { objectMs: 0, subgroupMs: 0 }]]),
    });
    await writer.write(bytes(1), { properties: [{ type: 0x06, value: 0 }] });
    // Odd neighbours of the timeout types carry bytes; they are sent and never read as a timeout.
    const odd = [
      { type: 0x03, value: bytes(9) },
      { type: 0x07, value: bytes(9) },
    ];
    await writer.write(bytes(2), { properties: odd });
    await elapse(60_000);
    expect(opened.map((subgroup) => subgroup.aborts)).toEqual([[], []]);
    expect(opened[1]?.objects[0]?.properties).toEqual(odd);
  });

  it('clamps a value above the timer bound instead of firing at once', async () => {
    const { writer, opened } = settling({ trackTimeouts: { subgroupMs: Number.MAX_SAFE_INTEGER } });
    await writer.write(bytes(1));
    await elapse(MAX_TIMER_MS - 1);
    expect(opened[0]?.aborts).toEqual([]);
    await elapse(1);
    expect(opened[0]?.aborts).toEqual([resetWith(RESET_CODE.DELIVERY_TIMEOUT)]);
  });

  it("takes the first object's 0x06 over the track's, and ignores it on a later object", async () => {
    const { writer, opened } = settling({ trackTimeouts: { subgroupMs: 100 } });
    await writer.write(bytes(1), { properties: [{ type: 0x06, value: 20 }] });
    const group = await writer.openGroup();
    await group.write(bytes(2), { properties: [{ type: 0x06, value: 30 }] });
    await group.write(bytes(3), { properties: [{ type: 0x06, value: 5 }] });
    await group.close();
    const plain = await writer.openGroup();
    await plain.write(bytes(4));
    await plain.write(bytes(5), { properties: [{ type: 0x06, value: 5 }] });
    await plain.close();
    await elapse(20);
    expect(opened.map((subgroup) => subgroup.aborts.length)).toEqual([1, 0, 0]);
    await elapse(10);
    expect(opened.map((subgroup) => subgroup.aborts.length)).toEqual([1, 1, 0]);
    await elapse(70);
    expect(opened.map((subgroup) => subgroup.aborts.length)).toEqual([1, 1, 1]);
  });

  it('applies a changed subscriber value only to groups opened afterwards', async () => {
    const subscriber = new Map([[1, { subgroupMs: 100 }]]);
    const { writer, opened } = settling({ subscriberTimeouts: subscriber });
    await writer.write(bytes(1));
    const open = await writer.openGroup();
    // A REQUEST_UPDATE lowers the subscription's value while one group settles and one is open.
    subscriber.set(1, { subgroupMs: 20 });
    await open.write(bytes(2));
    await open.close();
    await writer.write(bytes(3));
    await elapse(20);
    expect(opened.map((subgroup) => subgroup.aborts.length)).toEqual([0, 0, 1]);
    await elapse(80);
    expect(opened.map((subgroup) => subgroup.aborts.length)).toEqual([1, 1, 1]);
  });

  it('keeps timing a settling group through a subscription change, its end, and the track closing', async () => {
    const { slot, writer, opened } = settling({ trackTimeouts: { subgroupMs: 50 } });
    await writer.write(bytes(1));
    slot.bind(2);
    await writer.write(bytes(2));
    slot.bind(undefined);
    writer.close();
    await elapse(50);
    expect(opened.map((subgroup) => subgroup.aborts)).toEqual([
      [resetWith(RESET_CODE.DELIVERY_TIMEOUT)],
      [resetWith(RESET_CODE.DELIVERY_TIMEOUT)],
    ]);
  });

  it('stops the subgroup timers when the connection ends', async () => {
    const { writer, opened, disconnect } = settling({ trackTimeouts: { subgroupMs: 50 } });
    await writer.write(bytes(1));
    disconnect();
    await elapse(100);
    expect(opened[0]?.aborts).toEqual([]);
  });

  it('fails a write waiting for demand with DELIVERY_TIMEOUT, sending nothing and keeping the track', async () => {
    const { slot, writer, opened } = harness({ trackTimeouts: { objectMs: 50 } });
    const rejected = expect(writer.write(bytes(1))).rejects.toMatchObject({
      code: 'DELIVERY_TIMEOUT',
      phase: 'waiting-for-demand',
    });
    await elapse(50);
    await rejected;
    expect(opened).toHaveLength(0);
    expect(writer.closed).toBe(false);
    slot.bind(1);
    await writer.write(bytes(2));
    expect(opened[0]).toMatchObject({ groupId: 0, aborts: [] });
  });

  it('reports whichever of the object timeout and the write deadline expires first', async () => {
    const early = harness({ writeTimeoutMs: 100, trackTimeouts: { objectMs: 40 } });
    const late = harness({ writeTimeoutMs: 40, trackTimeouts: { objectMs: 100 } });
    const first = expect(early.writer.write(bytes(1))).rejects.toMatchObject({ code: 'DELIVERY_TIMEOUT' });
    const second = expect(late.writer.write(bytes(1))).rejects.toMatchObject({ code: 'WRITE_TIMEOUT' });
    await elapse(40);
    await first;
    await second;
  });

  it("takes the object's own 0x02 over the track's for the timeout it waits under", async () => {
    const { writer } = harness({ writeTimeoutMs: 1_000, trackTimeouts: { objectMs: 500 } });
    const rejected = expect(writer.write(bytes(1), { properties: [{ type: 0x02, value: 30 }] })).rejects.toMatchObject({
      code: 'DELIVERY_TIMEOUT',
      phase: 'waiting-for-demand',
    });
    await elapse(30);
    await rejected;
  });

  it("counts the wait for demand against the subscriber's value once the subscription arrives", async () => {
    const { slot, writer, opened } = harness({ subscriberTimeouts: new Map([[1, { objectMs: 30 }]]) });
    const writing = writer.write(bytes(1));
    const rejected = expect(writing).rejects.toMatchObject({ code: 'DELIVERY_TIMEOUT', phase: 'waiting-for-demand' });
    // Before a subscription exists only the publisher's value applies, and it has none.
    await elapse(40);
    slot.bind(1);
    await rejected;
    expect(opened).toHaveLength(0);
    await writer.write(bytes(2));
    expect(opened[0]).toMatchObject({ groupId: 0, aborts: [] });
  });

  it('counts the wait for the stream, and resets a stream that opens late with DELIVERY_TIMEOUT', async () => {
    const { slot, writer, opened, releaseOpen } = harness({ open: 'hold', trackTimeouts: { objectMs: 50 } });
    slot.bind(1);
    const rejected = expect(writer.write(bytes(1))).rejects.toMatchObject({
      code: 'DELIVERY_TIMEOUT',
      phase: 'opening-stream',
    });
    await elapse(50);
    await rejected;
    releaseOpen();
    await flush();
    expect(opened[0]?.objects).toEqual([]);
    expect(opened[0]?.aborts).toEqual([resetWith(RESET_CODE.DELIVERY_TIMEOUT)]);
  });

  it('counts the wait for settling room in opening-stream', async () => {
    const { writer, opened } = settling({ trackTimeouts: { objectMs: 50 } });
    for (let i = 0; i < SETTLING_GROUP_LIMIT; i++) await writer.write(bytes(1));
    const rejected = expect(writer.write(bytes(2))).rejects.toMatchObject({
      code: 'DELIVERY_TIMEOUT',
      phase: 'opening-stream',
    });
    await elapse(50);
    await rejected;
    expect(opened).toHaveLength(SETTLING_GROUP_LIMIT);
  });

  it('resets an open group whose object does not reach the transport in time; later writes fail, nothing reopens', async () => {
    const { slot, writer, opened } = harness({ stall: 'second-write', trackTimeouts: { objectMs: 50 } });
    slot.bind(1);
    const group = await writer.openGroup();
    await group.write(bytes(1));
    const rejected = expect(group.write(bytes(2))).rejects.toMatchObject({
      code: 'DELIVERY_TIMEOUT',
      phase: 'writing',
    });
    await elapse(50);
    await rejected;
    expect(opened[0]?.aborts).toEqual([resetWith(RESET_CODE.DELIVERY_TIMEOUT)]);
    expect(group.closed).toBe(true);
    await expect(group.write(bytes(3))).rejects.toMatchObject({ code: 'DELIVERY_TIMEOUT' });
    await expect(group.close()).rejects.toMatchObject({ code: 'DELIVERY_TIMEOUT' });
    expect(opened).toHaveLength(1);
    expect(writer.closed).toBe(false);
    const next = await writer.openGroup();
    expect(next.groupId).toBe(1);
  });

  it("times each object of a group by the first object's 0x02, from that object's own write call", async () => {
    const { slot, writer, opened } = harness({ stall: 'second-write', trackTimeouts: { objectMs: 500 } });
    slot.bind(1);
    const group = await writer.openGroup();
    await elapse(1_000);
    await group.write(bytes(1), { properties: [{ type: 0x02, value: 30 }] });
    const rejected = expect(group.write(bytes(2), { properties: [{ type: 0x02, value: 900 }] })).rejects.toMatchObject({
      code: 'DELIVERY_TIMEOUT',
      phase: 'writing',
    });
    await elapse(29);
    expect(opened[0]?.aborts).toEqual([]);
    await elapse(1);
    await rejected;
  });

  it('checks the age again once the header write settles, even when the timer fires late', async () => {
    const { slot, writer, opened } = harness({ stall: 'header', trackTimeouts: { objectMs: 20 } });
    slot.bind(1);
    const group = await writer.openGroup();
    const rejected = expect(group.write(bytes(1))).rejects.toMatchObject({
      code: 'DELIVERY_TIMEOUT',
      phase: 'writing',
    });
    await flush();
    // The event loop is late: the clock passes the timeout before the timer has run.
    const late = performance.now() + 50;
    const clock = vi.spyOn(performance, 'now').mockReturnValue(late);
    opened[0]?.release();
    await rejected;
    clock.mockRestore();
    expect(opened[0]?.objects).toEqual([]);
    expect(opened[0]?.aborts).toEqual([resetWith(RESET_CODE.DELIVERY_TIMEOUT)]);
  });

  it('fixes the subscriber value once the stream has opened, after any wait for it', async () => {
    const subscriber = new Map([[1, { subgroupMs: 0 }]]);
    const { writer, opened, releaseOpen } = settling({ open: 'hold', subscriberTimeouts: subscriber });
    const writing = writer.write(bytes(1));
    await flush();
    subscriber.set(1, { subgroupMs: 10 });
    releaseOpen();
    await writing;
    await elapse(10);
    expect(opened[0]?.aborts).toEqual([resetWith(RESET_CODE.DELIVERY_TIMEOUT)]);
  });

  it('fixes the subscriber value when the stream opens, after any wait for settling room', async () => {
    const subscriber = new Map([[1, { subgroupMs: 0 }]]);
    const { writer, opened } = settling({ subscriberTimeouts: subscriber });
    for (let i = 0; i < SETTLING_GROUP_LIMIT; i++) await writer.write(bytes(1));
    const waiting = writer.write(bytes(2));
    await flush();
    subscriber.set(1, { subgroupMs: 10 });
    opened[0]?.acknowledge();
    await waiting;
    await elapse(10);
    expect(opened[SETTLING_GROUP_LIMIT]?.aborts).toEqual([resetWith(RESET_CODE.DELIVERY_TIMEOUT)]);
    expect(opened[1]?.aborts).toEqual([]);
  });

  it('times nothing once the object reached the transport, and nothing while a group is idle', async () => {
    const { slot, writer, opened } = harness({ stall: 'fin', trackTimeouts: { objectMs: 20 } });
    slot.bind(1);
    const group = await writer.openGroup();
    await group.write(bytes(1));
    await elapse(100);
    const closing = group.close();
    await elapse(100);
    expect(opened[0]?.aborts).toEqual([]);
    opened[0]?.release();
    await closing;
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
  it('resets outside a browser with a stream error carrying the MOQT code and the cause (TA-17, TA-22)', async () => {
    const { slot, writer, opened } = harness();
    slot.bind(1);
    const group = await writer.openGroup();
    const cause = new Error('Stopped');
    group.abort(cause);
    expect(opened[0]?.aborts[0]).toBeInstanceOf(Error);
    expect(opened[0]?.aborts[0]).toMatchObject({
      source: 'stream',
      streamErrorCode: RESET_CODE.CANCELLED,
      message: 'Stopped',
      cause,
    });
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
