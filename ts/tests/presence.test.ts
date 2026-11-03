import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { MoqtObject, SubscribeOk } from '../packages/moq-core/src/index.js';
import {
  createPresenceView,
  PRESENCE_HEARTBEAT_MS,
  type PresenceChange,
  type PresenceViewHandlers,
  publishPresence,
} from '../src/media.js';
import type { Member, MembersChange, MembersHandlers } from '../src/members.js';
import {
  type RestartReason,
  type SupervisedSubscriber,
  superviseRelaySubscriber,
  type WantHandlers,
  type WantOptions,
} from '../src/supervised-subscriber.js';
import { newGeneration } from '../src/supervision.js';
import { settle } from './helpers/slot-track.js';
import {
  type FakeSubscriber,
  type FakeTrack,
  fakePublisher,
  fakeSubscriber,
  until,
} from './helpers/supervision-fakes.js';

const ROOT = ['room', 'p'];
const WINDOW = 3 * PRESENCE_HEARTBEAT_MS;
const OLD = '0000000aaxxxx';
const G1 = '0000000bbxxxx';
const G2 = '0000000ccxxxx';
const OK = { trackAlias: 1, parameters: {}, trackProperties: [] } as unknown as SubscribeOk;
const encode = (document: unknown) => new TextEncoder().encode(JSON.stringify(document));
const memberOf = (id: string): Member => ({ id, namespace: [...ROOT, id] });

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance', 'Date'] });
});

afterEach(() => {
  vi.useRealTimers();
});

interface Opened {
  key: string;
  options: WantOptions;
  handlers: WantHandlers;
}

/** A supervised subscriber whose members view and wants the test drives by hand. */
function fakeRoom() {
  let membersHandlers: MembersHandlers | undefined;
  let present = new Map<string, Member>();
  const opened: Opened[] = [];
  const unwanted: string[] = [];
  let stop = () => {};
  const closed = new Promise<{ error?: unknown }>((resolve) => {
    stop = () => resolve({});
  });
  const room = {
    membersCalls: 0,
    membersClosed: false,
    opened,
    unwanted,
    subscriber: {
      members(_options, handlers = {}) {
        room.membersCalls += 1;
        membersHandlers = handlers;
        return {
          loaded: false,
          present,
          close() {
            room.membersClosed = true;
          },
        };
      },
      want(key, options, handlers = {}) {
        opened.push({ key, options, handlers });
      },
      unwant(key) {
        unwanted.push(key);
      },
      closed,
    } satisfies Pick<SupervisedSubscriber, 'members' | 'want' | 'unwant' | 'closed'>,
    /** The supervisor stops: it closes the members view and drops the wants without calling them, as `close()` does. */
    stop,
    change(joined: string[], left: string[] = [], initial = false) {
      present = new Map(present);
      for (const id of left) present.delete(id);
      for (const id of joined) present.set(id, memberOf(id));
      const change: MembersChange = { joined: joined.map(memberOf), left: left.map(memberOf), present, initial };
      membersHandlers?.onChanged?.(change);
    },
    load(...ids: string[]) {
      room.change(ids, [], true);
    },
    fail(error: unknown) {
      membersHandlers?.onError?.(error);
    },
    /** The newest want opened for `id`, as the relay would drive it. */
    peer(id: string, which = -1) {
      const want = opened.filter((entry) => entry.options.namespace.at(-1) === id).at(which);
      if (!want) throw new Error(`no want for ${id}`);
      const { handlers } = want;
      const at = (generation: string) => [...ROOT, id, generation];
      const bytes = (groupId: number, payload: Uint8Array) =>
        handlers.onObject?.({
          groupId,
          objectId: 0,
          subgroupId: 0,
          status: 'normal',
          properties: [],
          payload,
        } as MoqtObject);
      return {
        want,
        subscribed(generation = G1) {
          handlers.onSubscribed?.(OK, { namespace: at(generation) });
        },
        switched(from: string, to: string) {
          handlers.onSwitched?.({
            from: { namespace: at(from), track: want.options.track },
            to: { namespace: at(to), track: want.options.track },
            subscribed: OK,
          });
        },
        restarted(reason: RestartReason, generation = G1) {
          handlers.onRestarted?.({ reason, namespace: at(generation) });
        },
        bytes,
        object(groupId: number, document: unknown) {
          bytes(groupId, encode(document));
        },
      };
    },
  };
  return room;
}

function recorder<T = unknown>() {
  const changes: PresenceChange<T>[] = [];
  const errors: unknown[] = [];
  const handlers: PresenceViewHandlers<T> = {
    onChanged: (change) => changes.push(change),
    onError: (error) => errors.push(error),
  };
  return { changes, errors, handlers };
}

const KINDS = ['joined', 'left', 'updated', 'stale', 'resumed'] as const;
/** A change as the ids it names, leaving out what is empty. */
function summary(change: PresenceChange<unknown>) {
  const named: { initial?: true } & Partial<Record<(typeof KINDS)[number], string[]>> = {};
  if (change.initial) named.initial = true;
  for (const kind of KINDS) if (change[kind].length > 0) named[kind] = change[kind].map((member) => member.id);
  return named;
}

/** A view with `ids` loaded and live, each on generation G1 with a `{ name: id }` document at group 0. */
function liveRoom(...ids: string[]) {
  const room = fakeRoom();
  const record = recorder();
  const view = createPresenceView(room.subscriber, { root: ROOT }, record.handlers);
  room.load(...ids);
  for (const id of ids) {
    const peer = room.peer(id);
    peer.subscribed();
    peer.object(0, { name: id });
  }
  expect(record.changes.map(summary)).toEqual([{ initial: true, ...(ids.length > 0 ? { joined: ids } : {}) }]);
  record.changes.length = 0;
  return { room, view, ...record };
}

describe('createPresenceView', () => {
  it('reports a member only once its document arrives, with what it published', () => {
    const { room, view, changes } = liveRoom();
    room.change(['bob']);
    const bob = room.peer('bob');
    expect(bob.want.options).toEqual({
      namespace: [...ROOT, 'bob'],
      track: 'presence',
      generations: true,
      parameters: { subscriberPriority: 32 },
    });
    bob.subscribed();
    vi.advanceTimersByTime(10_000);
    expect(changes).toEqual([]);
    expect(view.present.size).toBe(0);
    bob.object(0, { name: 'Bob', host: true });
    expect(changes.map(summary)).toEqual([{ joined: ['bob'] }]);
    const entry = changes[0]!.joined[0]!;
    expect(entry).toEqual({
      id: 'bob',
      namespace: [...ROOT, 'bob'],
      document: { name: 'Bob', host: true },
      since: Date.now(),
      stale: false,
    });
    expect(view.present).toBe(changes[0]!.present);
    expect([...view.present.values()]).toEqual([entry]);
  });

  it('loads an empty root at once, with nobody', () => {
    const room = fakeRoom();
    const { changes, handlers } = recorder();
    const view = createPresenceView(room.subscriber, { root: ROOT }, handlers);
    expect(view.loaded).toBe(false);
    room.load();
    expect(changes.map(summary)).toEqual([{ initial: true }]);
    expect(view.loaded).toBe(true);
    expect(view.present.size).toBe(0);
  });

  it('loads as soon as every member present has a document, in members() order', () => {
    const room = fakeRoom();
    const { changes, handlers } = recorder();
    const view = createPresenceView(room.subscriber, { root: ROOT }, handlers);
    room.load('alice', 'bob');
    room.peer('bob').subscribed();
    room.peer('bob').object(0, { name: 'Bob' });
    expect(changes).toEqual([]);
    expect(view.loaded).toBe(false);
    room.peer('alice').subscribed();
    room.peer('alice').object(0, { name: 'Alice' });
    expect(changes.map(summary)).toEqual([{ initial: true, joined: ['alice', 'bob'] }]);
    expect([...view.present.keys()]).toEqual(['alice', 'bob']);
  });

  it('loads after 1 s without a member whose document never came, and reports it as an ordinary join later', () => {
    const room = fakeRoom();
    const { changes, handlers } = recorder();
    createPresenceView(room.subscriber, { root: ROOT }, handlers);
    room.load('alice', 'carol');
    room.peer('alice').subscribed();
    room.peer('alice').object(0, { name: 'Alice' });
    room.peer('carol').subscribed();
    vi.advanceTimersByTime(999);
    expect(changes).toEqual([]);
    vi.advanceTimersByTime(1);
    expect(changes.map(summary)).toEqual([{ initial: true, joined: ['alice'] }]);
    room.peer('carol').object(4, { name: 'Carol' });
    expect(changes.map(summary)).toEqual([{ initial: true, joined: ['alice'] }, { joined: ['carol'] }]);
  });

  it('reports a changed document once as updated, and nothing for the same JSON again', () => {
    const { room, view, changes } = liveRoom('alice');
    const alice = room.peer('alice');
    const before = view.present;
    alice.object(1, { name: 'alice' });
    expect(changes).toEqual([]);
    expect(view.present).toBe(before);
    alice.object(2, { name: 'Alice L.' });
    expect(changes.map(summary)).toEqual([{ updated: ['alice'] }]);
    expect(changes[0]!.updated[0]!.document).toEqual({ name: 'Alice L.' });
    expect(view.present.get('alice')!.document).toEqual({ name: 'Alice L.' });
    expect(view.present).not.toBe(before);
  });

  it('keeps the latest document when an older group arrives late', () => {
    const { room, view, changes } = liveRoom('alice');
    const alice = room.peer('alice');
    alice.object(5, { name: 'newest' });
    alice.object(4, { name: 'older' });
    expect(changes.map(summary)).toEqual([{ updated: ['alice'] }]);
    expect(view.present.get('alice')!.document).toEqual({ name: 'newest' });
  });

  it('calls a member stale after three heartbeats with nothing new, then left as last reported', () => {
    const { room, changes } = liveRoom('alice');
    vi.advanceTimersByTime(WINDOW - 1);
    expect(changes).toEqual([]);
    vi.advanceTimersByTime(1);
    expect(changes.map(summary)).toEqual([{ stale: ['alice'] }]);
    expect(changes[0]!.present.get('alice')!.stale).toBe(true);
    room.change([], ['alice']);
    expect(changes.map(summary)).toEqual([{ stale: ['alice'] }, { left: ['alice'] }]);
    expect(changes[1]!.left[0]!.stale).toBe(true);
    expect(changes[1]!.present.size).toBe(0);
  });

  it('counts a cache replay of the same group as nothing: no report, and no new life', () => {
    const { room, changes } = liveRoom('alice');
    vi.advanceTimersByTime(WINDOW - 1000);
    room.peer('alice').object(0, { name: 'alice' });
    vi.advanceTimersByTime(1000);
    expect(changes.map(summary)).toEqual([{ stale: ['alice'] }]);
  });

  it('keeps a member live while new groups arrive, each one starting the window again', () => {
    const { room, changes } = liveRoom('alice');
    for (let group = 1; group <= 5; group++) {
      vi.advanceTimersByTime(PRESENCE_HEARTBEAT_MS);
      room.peer('alice').object(group, { name: 'alice' });
    }
    vi.advanceTimersByTime(WINDOW - 1);
    expect(changes).toEqual([]);
  });

  it('resumes a stale member on its next new group, with updated when its document changed', () => {
    const { room, view, changes } = liveRoom('alice');
    const alice = room.peer('alice');
    vi.advanceTimersByTime(WINDOW);
    alice.object(1, { name: 'alice' });
    vi.advanceTimersByTime(WINDOW);
    alice.object(2, { name: 'back' });
    expect(changes.map(summary)).toEqual([
      { stale: ['alice'] },
      { resumed: ['alice'] },
      { stale: ['alice'] },
      { resumed: ['alice'], updated: ['alice'] },
    ]);
    expect(view.present.get('alice')).toMatchObject({ stale: false, document: { name: 'back' } });
    expect(changes[3]!.resumed[0]).toBe(changes[3]!.updated[0]);
  });

  it('keeps since across updates, staleness and rotations', () => {
    const { room, view } = liveRoom('alice');
    const since = view.present.get('alice')!.since;
    vi.advanceTimersByTime(WINDOW);
    room.peer('alice').switched(G1, G2);
    room.peer('alice').object(0, { name: 'renamed' });
    expect(view.present.get('alice')).toMatchObject({ since, stale: false, document: { name: 'renamed' } });
  });

  it("reports nothing for the publisher's rotation to a newer generation with the same document", () => {
    const { room, changes } = liveRoom('alice');
    const alice = room.peer('alice');
    alice.object(9, { name: 'alice' });
    vi.advanceTimersByTime(PRESENCE_HEARTBEAT_MS);
    alice.switched(G1, G2);
    alice.object(0, { name: 'alice' });
    vi.advanceTimersByTime(WINDOW - 1);
    expect(changes).toEqual([]);
  });

  it("reports nothing for the subscriber's own rotation, which calls nothing", () => {
    const { room, changes } = liveRoom('alice');
    vi.advanceTimersByTime(PRESENCE_HEARTBEAT_MS);
    room.peer('alice').object(1, { name: 'alice' });
    vi.advanceTimersByTime(WINDOW - 1);
    expect(changes).toEqual([]);
  });

  it("pauses the clock through the view's own outage and gives a full window from the resubscription", () => {
    const { room, changes } = liveRoom('alice');
    const alice = room.peer('alice');
    vi.advanceTimersByTime(1000);
    alice.restarted('connection');
    vi.advanceTimersByTime(60_000);
    expect(changes).toEqual([]);
    alice.subscribed();
    alice.object(0, { name: 'alice' });
    vi.advanceTimersByTime(WINDOW - 1);
    expect(changes).toEqual([]);
    vi.advanceTimersByTime(1);
    expect(changes.map(summary)).toEqual([{ stale: ['alice'] }]);
  });

  it('keeps the clock running through any other restart', () => {
    const { room, changes } = liveRoom('alice');
    vi.advanceTimersByTime(1000);
    room.peer('alice').restarted('publisher-done');
    vi.advanceTimersByTime(2000);
    room.peer('alice').subscribed();
    vi.advanceTimersByTime(WINDOW - 3000);
    expect(changes.map(summary)).toEqual([{ stale: ['alice'] }]);
  });

  it('ignores an older generation while live, and takes any other generation once stale', () => {
    const { room, view, changes } = liveRoom('alice');
    const alice = room.peer('alice');
    alice.restarted('generation');
    alice.subscribed(OLD);
    alice.object(7, { name: 'zombie' });
    expect(changes).toEqual([]);
    expect(view.present.get('alice')!.document).toEqual({ name: 'alice' });
    vi.advanceTimersByTime(WINDOW);
    // A publisher restarted on a clock behind: its generation sorts older, but the member had gone stale.
    alice.object(8, { name: 'restarted' });
    expect(changes.map(summary)).toEqual([{ stale: ['alice'] }, { resumed: ['alice'], updated: ['alice'] }]);
  });

  it('reports nothing when a member with no document yet leaves, and unwants it', () => {
    const { room, changes } = liveRoom();
    room.change(['bob']);
    const { key } = room.peer('bob').want;
    room.change([], ['bob']);
    expect(changes).toEqual([]);
    expect(room.unwanted).toEqual([key]);
  });

  it("never lets a member's old want reach it after it leaves and joins again", () => {
    const { room, changes } = liveRoom('alice');
    const old = room.peer('alice');
    room.change([], ['alice']);
    room.change(['alice']);
    const rejoined = room.peer('alice');
    expect(rejoined.want.key).not.toBe(old.want.key);
    expect(room.unwanted).toEqual([old.want.key]);
    old.subscribed(G2);
    old.object(0, { name: 'ghost' });
    expect(changes.map(summary)).toEqual([{ left: ['alice'] }]);
    rejoined.subscribed(G2);
    rejoined.object(0, { name: 'alice again' });
    expect(changes.map(summary)).toEqual([{ left: ['alice'] }, { joined: ['alice'] }]);
    expect(changes[1]!.joined[0]!.document).toEqual({ name: 'alice again' });
  });

  it('skips a document parse refuses, keeping the old one, but counts it as heard', () => {
    const room = fakeRoom();
    const { changes, handlers } = recorder<{ name: string }>();
    const parse = (value: unknown) => {
      if (typeof value !== 'object' || value === null || typeof (value as { name?: unknown }).name !== 'string')
        throw new TypeError('not a profile');
      return value as { name: string };
    };
    const view = createPresenceView(room.subscriber, { root: ROOT, parse }, handlers);
    room.load('alice');
    const alice = room.peer('alice');
    alice.subscribed();
    alice.object(0, { nickname: 'unreadable' });
    alice.bytes(1, Uint8Array.of(0xff, 0xfe));
    vi.advanceTimersByTime(1000);
    expect(changes.map(summary)).toEqual([{ initial: true }]);
    alice.object(2, { name: 'Alice' });
    vi.advanceTimersByTime(WINDOW - 1000);
    alice.object(3, { nickname: 'unreadable' });
    vi.advanceTimersByTime(WINDOW - 1);
    expect(changes.map(summary)).toEqual([{ initial: true }, { joined: ['alice'] }]);
    expect(view.present.get('alice')!.document.name).toBe('Alice');
    vi.advanceTimersByTime(1);
    expect(changes.map(summary).at(-1)).toEqual({ stale: ['alice'] });
  });

  it('delivers nothing after close() from inside parse', () => {
    const room = fakeRoom();
    const { changes, handlers } = recorder();
    const view = createPresenceView(
      room.subscriber,
      {
        root: ROOT,
        parse: (value) => {
          view.close();
          return value;
        },
      },
      handlers,
    );
    room.load('alice');
    const alice = room.peer('alice');
    alice.subscribed();
    alice.object(0, { name: 'Alice' });
    vi.advanceTimersByTime(WINDOW * 2);
    expect(changes).toEqual([]);
    expect(view.loaded).toBe(false);
    expect(view.present.size).toBe(0);
  });

  it('delivers nothing after close() from onChanged, and unwants every member and closes the members view', () => {
    const room = fakeRoom();
    const changes: PresenceChange<unknown>[] = [];
    const view = createPresenceView(
      room.subscriber,
      { root: ROOT },
      {
        onChanged: (change) => {
          changes.push(change);
          view.close();
          view.close();
        },
      },
    );
    room.load('alice', 'bob');
    room.peer('alice').subscribed();
    room.peer('alice').object(0, { name: 'alice' });
    vi.advanceTimersByTime(1000);
    expect(changes.map(summary)).toEqual([{ initial: true, joined: ['alice'] }]);
    expect(room.unwanted.sort()).toEqual(room.opened.map((want) => want.key).sort());
    expect(room.membersClosed).toBe(true);
    room.peer('bob').subscribed();
    room.peer('bob').object(0, { name: 'bob' });
    room.change(['carol']);
    vi.advanceTimersByTime(60_000);
    expect(changes).toHaveLength(1);
    expect(view.loaded).toBe(true);
    expect([...view.present.keys()]).toEqual(['alice']);
    expect(vi.getTimerCount()).toBe(0);
  });

  it('ends with its subscriber, delivering nothing after it and leaving no timer', async () => {
    const { room, view, changes, errors } = liveRoom('alice', 'bob');
    room.stop();
    await Promise.resolve();
    vi.advanceTimersByTime(60_000);
    expect(changes).toEqual([]);
    expect(errors).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
    expect([...view.present.keys()]).toEqual(['alice', 'bob']);
  });

  it("closes and reports the members view's failure, and delivers nothing after it", () => {
    const { room, view, changes, errors } = liveRoom('alice');
    const failure = new Error('token does not allow the room');
    room.fail(failure);
    expect(errors).toEqual([failure]);
    expect(room.unwanted).toEqual([room.peer('alice').want.key]);
    expect(room.membersClosed).toBe(true);
    room.peer('alice').object(1, { name: 'changed' });
    vi.advanceTimersByTime(60_000);
    expect(changes).toEqual([]);
    expect(view.present.get('alice')!.stale).toBe(false);
  });

  it('carries on when onChanged throws', () => {
    const room = fakeRoom();
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    const seen: PresenceChange<unknown>[] = [];
    createPresenceView(
      room.subscriber,
      { root: ROOT },
      {
        onChanged: (change) => {
          seen.push(change);
          throw new Error('render failed');
        },
      },
    );
    room.load();
    room.change(['alice']);
    room.peer('alice').subscribed();
    room.peer('alice').object(0, { name: 'alice' });
    expect(seen.map(summary)).toEqual([{ initial: true }, { joined: ['alice'] }]);
    expect(error).toHaveBeenCalledTimes(2);
    error.mockRestore();
  });

  it('calls a member stale after three of the heartbeats it is given', () => {
    const room = fakeRoom();
    const { changes, handlers } = recorder();
    createPresenceView(room.subscriber, { root: ROOT, heartbeatMs: 500 }, handlers);
    room.load('alice');
    room.peer('alice').subscribed();
    room.peer('alice').object(0, {});
    vi.advanceTimersByTime(1499);
    expect(changes.map(summary)).toEqual([{ initial: true, joined: ['alice'] }]);
    vi.advanceTimersByTime(1);
    expect(changes.map(summary).at(-1)).toEqual({ stale: ['alice'] });
  });

  it('checks its arguments at once, before following the root', () => {
    const room = fakeRoom();
    const view = (options: unknown, handlers?: unknown) => () =>
      createPresenceView(room.subscriber, options as never, handlers as never);
    expect(view(undefined)).toThrow(TypeError);
    expect(view({ root: ROOT, track: '' })).toThrow(TypeError);
    expect(view({ root: ROOT, heartbeatMs: '2000' })).toThrow(TypeError);
    expect(view({ root: ROOT, heartbeatMs: 99 })).toThrow(RangeError);
    expect(view({ root: ROOT, heartbeatMs: 1000.5 })).toThrow(RangeError);
    expect(view({ root: ROOT, heartbeatMs: 715_827_883 })).toThrow(RangeError);
    expect(view({ root: ROOT, parse: 'json' })).toThrow(TypeError);
    expect(view({ root: ROOT }, null)).toThrow(TypeError);
    expect(view({ root: ROOT }, { onChanged: 'render' })).toThrow(TypeError);
    expect(view({ root: ROOT }, { onChanged: async () => {} })).toThrow(TypeError);
    expect(room.membersCalls).toBe(0);
  });
});

describe('createPresenceView over a supervised subscriber', () => {
  it('reports nothing when the subscriber rotates its connection', async () => {
    vi.useRealTimers();
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
      const { changes, handlers } = recorder();
      const view = createPresenceView(subscriber, { root: ROOT }, handlers);
      const generation = newGeneration();
      const serve = async (n: number) => {
        await until(() => (connections[n]?.discoveries.length ?? 0) > 0, `discovery on connection ${n}`);
        const discovery = connections[n]!.discoveries[0]!;
        discovery.push({ type: 'announced', namespace: [...ROOT, 'alice', generation] });
        discovery.push({ type: 'loaded' });
        await until(() => (connections[n]?.subscriptions.length ?? 0) > 0, `presence subscription on connection ${n}`);
        const subscription = connections[n]!.subscriptions[0]!;
        subscription.accept();
        return {
          subscription,
          send: (groupId: number) =>
            subscription.handlers.onObject?.({
              groupId,
              objectId: 0,
              subgroupId: 0,
              status: 'normal',
              properties: [],
              payload: encode({ name: 'alice' }),
            } as MoqtObject),
        };
      };
      const old = await serve(0);
      expect(old.subscription.options).toMatchObject({ namespace: [...ROOT, 'alice', generation], track: 'presence' });
      old.send(0);
      await until(() => changes.length === 1, 'the load');
      expect(changes.map(summary)).toEqual([{ initial: true, joined: ['alice'] }]);

      subscriber.rotate();
      const replacement = await serve(1);
      old.send(1);
      replacement.send(1);
      replacement.send(2);
      old.send(3);
      await until(() => old.subscription.cancelled, 'the old subscription cancelled');
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(changes).toHaveLength(1);
      expect(view.present.get('alice')).toMatchObject({ stale: false, document: { name: 'alice' } });
    } finally {
      subscriber.close();
    }
  });

  it('reports nothing once the subscriber is closed, not even a member going stale', async () => {
    vi.useRealTimers();
    const connections: FakeSubscriber[] = [];
    const subscriber = await superviseRelaySubscriber(
      async () => {
        const fake = fakeSubscriber();
        connections.push(fake);
        return fake;
      },
      { url: 'https://relay.example.com/moq', reconnect: { initialDelayMs: 0, maxDelayMs: 0 } },
    );
    const { changes, errors, handlers } = recorder();
    createPresenceView(subscriber, { root: ROOT, heartbeatMs: 100 }, handlers);
    await until(() => (connections[0]?.discoveries.length ?? 0) > 0, 'discovery');
    const discovery = connections[0]!.discoveries[0]!;
    discovery.push({ type: 'announced', namespace: [...ROOT, 'alice', newGeneration()] });
    discovery.push({ type: 'loaded' });
    await until(() => (connections[0]?.subscriptions.length ?? 0) > 0, 'the presence subscription');
    const subscription = connections[0]!.subscriptions[0]!;
    subscription.accept();
    subscription.handlers.onObject?.({
      groupId: 0,
      objectId: 0,
      subgroupId: 0,
      status: 'normal',
      properties: [],
      payload: encode({ name: 'alice' }),
    } as MoqtObject);
    await until(() => changes.length === 1, 'the load');
    subscriber.close();
    await new Promise((resolve) => setTimeout(resolve, 500));
    expect(changes.map(summary)).toEqual([{ initial: true, joined: ['alice'] }]);
    expect(errors).toEqual([]);
  });
});

describe('publishPresence', () => {
  const sent = (track: FakeTrack) => track.writes.map((bytes) => new TextDecoder().decode(bytes));

  function start(document: unknown = { name: 'Ada', joinedAt: 1 }, options = {}) {
    const publisher = fakePublisher([...ROOT, 'alice']);
    const me = publishPresence(publisher, document, options);
    const track = publisher.tracks.get('presence')!;
    return { publisher, me, track };
  }

  it('serves the document as compact JSON on the presence track, at a priority between audio and video', async () => {
    const { me, track } = start();
    expect(track.options).toEqual({ track: 'presence', trackProperties: [{ type: 0x0e, value: 32 }] });
    await settle();
    expect(sent(track)).toEqual([]);
    track.setDemand(true);
    await settle();
    expect(sent(track)).toEqual(['{"name":"Ada","joinedAt":1}']);
    expect(me.document).toEqual({ name: 'Ada', joinedAt: 1 });
  });

  it('sends a changed document once and an equal one not at all', async () => {
    const { me, track } = start();
    track.setDemand(true);
    await settle();
    const copy = { ...(me.document as object) };
    me.update(copy);
    await settle();
    expect(sent(track)).toHaveLength(1);
    expect(me.document).toBe(copy);
    me.update({ name: 'Ada L.', joinedAt: 1 });
    await settle();
    expect(sent(track)).toEqual(['{"name":"Ada","joinedAt":1}', '{"name":"Ada L.","joinedAt":1}']);
  });

  it('resends the same bytes heartbeatMs after the last change or resend, and nothing while nobody is subscribed', async () => {
    const { me, track } = start({ name: 'Ada' }, { heartbeatMs: 1000 });
    track.setDemand(true);
    await settle();
    await vi.advanceTimersByTimeAsync(999);
    expect(sent(track)).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1);
    await settle();
    expect(sent(track)).toEqual(['{"name":"Ada"}', '{"name":"Ada"}']);
    await vi.advanceTimersByTimeAsync(500);
    me.update({ name: 'Ada L.' });
    await settle();
    await vi.advanceTimersByTimeAsync(999);
    expect(sent(track)).toHaveLength(3);
    await vi.advanceTimersByTimeAsync(1);
    await settle();
    expect(sent(track).slice(2)).toEqual(['{"name":"Ada L."}', '{"name":"Ada L."}']);
    track.setDemand(false);
    await vi.advanceTimersByTimeAsync(5000);
    await settle();
    expect(sent(track)).toHaveLength(4);
  });

  it('stops its heartbeat when the publisher closes', async () => {
    const { publisher, me } = start();
    expect(vi.getTimerCount()).toBe(1);
    publisher.end();
    await settle();
    expect(vi.getTimerCount()).toBe(0);
    me.update({ name: 'after close' });
    expect(vi.getTimerCount()).toBe(0);
  });

  it('refuses a document JSON cannot encode before registering anything, and keeps the current one on update', () => {
    const publisher = fakePublisher([...ROOT, 'alice']);
    expect(() => publishPresence(publisher, undefined)).toThrow(TypeError);
    expect(() => publishPresence(publisher, () => {})).toThrow(TypeError);
    expect(() => publishPresence(publisher, { joinedAt: 1n })).toThrow(TypeError);
    expect(() => publishPresence(publisher, {}, null as never)).toThrow(TypeError);
    expect(() => publishPresence(publisher, {}, { track: '' })).toThrow(TypeError);
    expect(() => publishPresence(publisher, {}, { heartbeatMs: 99 })).toThrow(RangeError);
    expect(publisher.tracks.size).toBe(0);
    expect(() => publishPresence(publisher, { name: 'x'.repeat(16 * 1024 * 1024) })).toThrow(RangeError);
    expect(publisher.tracks.size).toBe(0);
    const me = publishPresence(publisher, { name: 'Ada' });
    expect(() => me.update(undefined as never)).toThrow(TypeError);
    expect(() => me.update({ name: 'x'.repeat(16 * 1024 * 1024) })).toThrow(RangeError);
    expect(me.document).toEqual({ name: 'Ada' });
  });

  it('keeps its heartbeat on the current document after refusing an oversized one', async () => {
    const { me, track } = start({ name: 'Ada' }, { heartbeatMs: 1000 });
    track.setDemand(true);
    await settle();
    expect(() => me.update({ name: 'x'.repeat(16 * 1024 * 1024) })).toThrow(RangeError);
    await vi.advanceTimersByTimeAsync(1000);
    await settle();
    expect(sent(track)).toEqual(['{"name":"Ada"}', '{"name":"Ada"}']);
  });
});
