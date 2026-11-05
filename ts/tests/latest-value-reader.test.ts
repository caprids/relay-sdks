import type { MoqtObject } from '@caprid/moq-core';
import { afterEach, describe, expect, expectTypeOf, it, vi } from 'vitest';
import type { RelaySubscriptionHandlers, WantHandlers } from '../src/index.js';
import { createLatestValueReader, type LatestValueReaderOptions } from '../src/media.js';
import { superviseRelaySubscriber, type WantSwitchedInfo } from '../src/supervised-subscriber.js';
import { type FakeSubscriber, fakeSubscriber, until } from './helpers/supervision-fakes.js';

const object = (groupId: number, objectId = 0, status: MoqtObject['status'] = 'normal'): MoqtObject => ({
  groupId,
  objectId,
  subgroupId: 0,
  status,
  properties: [],
  payload: new Uint8Array(status === 'normal' ? [groupId, objectId] : []),
});
const at = (o: MoqtObject) => `${o.groupId}.${o.objectId}`;
const switched = {} as WantSwitchedInfo;

function setup(options: Partial<LatestValueReaderOptions> = {}) {
  const values: string[] = [];
  let catchUps = 0;
  const reader = createLatestValueReader({
    onValue: (o) => values.push(at(o)),
    onCatchUp: () => catchUps++,
    ...options,
  });
  return { reader, values, catchUps: () => catchUps };
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe('createLatestValueReader', () => {
  it("is a subscription's and a want's handlers", () => {
    const reader = createLatestValueReader({ onValue() {} });
    expectTypeOf(reader).toMatchTypeOf<RelaySubscriptionHandlers>();
    expectTypeOf(reader).toMatchTypeOf<WantHandlers>();
  });

  it('drops a group that arrives after a newer one as superseded', () => {
    const { reader, values } = setup();
    reader.onObject(object(1));
    reader.onObject(object(3));
    reader.onObject(object(2));
    reader.onObject(object(3));
    reader.onObject(object(4));
    expect(values).toEqual(['1.0', '3.0', '4.0']);
    expect(reader.stats).toEqual({ delivered: 3, superseded: 2, late: 0, catchUps: 0 });
  });

  it('orders objects within a group, then across groups', () => {
    const { reader, values } = setup();
    reader.onObject(object(5, 0));
    reader.onObject(object(5, 2));
    reader.onObject(object(5, 1));
    reader.onObject(object(6, 0));
    reader.onObject(object(5, 3));
    expect(values).toEqual(['5.0', '5.2', '6.0']);
    expect(reader.stats.superseded).toBe(2);
  });

  it('ignores status objects', () => {
    const { reader, values } = setup();
    reader.onObject(object(1));
    reader.onObject(object(1, 1, 'end-of-group'));
    reader.onObject(object(1, 2, 'end-of-track'));
    reader.onObject(object(2));
    expect(values).toEqual(['1.0', '2.0']);
    expect(reader.stats).toEqual({ delivered: 2, superseded: 0, late: 0, catchUps: 0 });
  });

  it.each([
    ['restart', (reader: ReturnType<typeof setup>['reader']) => reader.onRestarted()],
    ['switch', (reader: ReturnType<typeof setup>['reader']) => reader.onSwitched(switched)],
  ])('starts numbering over on a %s, keeping its counters', (_, restart) => {
    const { reader, values } = setup();
    reader.onObject(object(40));
    reader.onObject(object(39));
    restart(reader);
    reader.onObject(object(0));
    reader.onObject(object(1));
    expect(values).toEqual(['40.0', '0.0', '1.0']);
    expect(reader.stats).toEqual({ delivered: 3, superseded: 1, late: 0, catchUps: 0 });
  });

  it('drops late values, signals one catch-up until a value is delivered, and lets a late value supersede', () => {
    const lateGroups = new Set([2, 3, 4, 6]);
    const { reader, values, catchUps } = setup({ late: (o) => lateGroups.has(o.groupId) });
    reader.onObject(object(1));
    reader.onObject(object(2));
    reader.onObject(object(3));
    expect(catchUps()).toBe(1);
    reader.onObject(object(1, 5));
    reader.onObject(object(4));
    expect(catchUps()).toBe(1);
    reader.onObject(object(5));
    reader.onObject(object(6));
    expect(values).toEqual(['1.0', '5.0']);
    expect(catchUps()).toBe(2);
    expect(reader.stats).toEqual({ delivered: 2, superseded: 1, late: 4, catchUps: 2 });
  });

  it('does not signal a catch-up again after a restart until a value is delivered', () => {
    let lateNow = true;
    const { reader, catchUps } = setup({ late: () => lateNow });
    reader.onObject(object(7));
    reader.onObject(object(8));
    reader.onRestarted();
    reader.onObject(object(0));
    reader.onObject(object(1));
    expect(catchUps()).toBe(1);
    lateNow = false;
    reader.onObject(object(2));
    lateNow = true;
    reader.onObject(object(3));
    expect(catchUps()).toBe(2);
    expect(reader.stats).toEqual({ delivered: 1, superseded: 0, late: 5, catchUps: 2 });
  });

  it('restarts cleanly when onCatchUp restarts it synchronously', () => {
    let reader!: ReturnType<typeof createLatestValueReader>;
    let catchUps = 0;
    reader = createLatestValueReader({
      onValue() {},
      late: (o) => o.groupId >= 10,
      onCatchUp: () => {
        catchUps++;
        reader.onRestarted();
      },
    });
    reader.onObject(object(10));
    reader.onObject(object(0));
    reader.onObject(object(11));
    expect(catchUps).toBe(2);
    expect(reader.stats).toEqual({ delivered: 1, superseded: 0, late: 2, catchUps: 2 });
  });

  it('logs throwing callbacks and carries on; a throwing late predicate delivers the value', () => {
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {});
    const values: string[] = [];
    const reader = createLatestValueReader({
      onValue: (o) => {
        values.push(at(o));
        if (o.groupId === 1) throw new Error('render failed');
      },
      late: (o) => {
        if (o.groupId === 2) throw new Error('bad payload');
        return o.groupId === 3;
      },
      onCatchUp: () => {
        throw new Error('no subscriber');
      },
    });
    reader.onObject(object(1));
    reader.onObject(object(2));
    reader.onObject(object(3));
    reader.onObject(object(4));
    expect(values).toEqual(['1.0', '2.0', '4.0']);
    expect(logged).toHaveBeenCalledTimes(3);
    expect(reader.stats).toEqual({ delivered: 3, superseded: 0, late: 1, catchUps: 1 });
  });
});

describe('createLatestValueReader on a supervised want', () => {
  it('restarts with the want, so a caught-up subscription starts numbering over', async () => {
    const connections: FakeSubscriber[] = [];
    const subscriber = await superviseRelaySubscriber(
      async () => {
        const fake = fakeSubscriber();
        connections.push(fake);
        return fake;
      },
      { url: 'https://relay.example.com/moq', reconnect: { initialDelayMs: 0, maxDelayMs: 0 } },
    );
    try {
      const values: number[] = [];
      const reader = createLatestValueReader({
        onValue: (o) => values.push(o.groupId),
        late: (o) => o.groupId === 6,
        onCatchUp: () => subscriber.catchUp('telemetry'),
      });
      subscriber.want('telemetry', { namespace: ['room'], track: 'telemetry' }, { ...reader, onError() {} });
      await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
      const first = connections[0]!.subscriptions[0]!;
      first.accept();
      first.object(4);
      first.object(5);
      first.object(6);
      await until(() => connections[0]!.subscriptions.length === 2, 'resubscribe after the catch-up');
      const second = connections[0]!.subscriptions[1]!;
      second.accept();
      second.object(0);
      second.object(1);
      expect(values).toEqual([4, 5, 0, 1]);
      expect(reader.stats).toEqual({ delivered: 4, superseded: 0, late: 1, catchUps: 1 });
    } finally {
      await subscriber.close();
    }
  });
});
