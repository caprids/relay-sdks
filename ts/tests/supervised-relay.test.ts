import { afterEach, describe, expect, it } from 'vitest';
import { RelayError } from '../src/errors.js';
import type { RelaySession } from '../src/relay-session.js';
import { superviseRelay } from '../src/supervised-relay.js';
import { deferred, type FakeSession, fakeSession, until } from './helpers/supervision-fakes.js';

const fast = { initialDelayMs: 0, maxDelayMs: 0 };
const cleanup: (() => unknown)[] = [];
afterEach(async () => {
  for (const close of cleanup.splice(0)) await close();
});

async function start(options: { expiresAt?: () => Date | undefined } = {}) {
  const connections: FakeSession[] = [];
  const statuses: [string, unknown][] = [];
  const generations: string[] = [];
  const relay = await superviseRelay(
    async () => {
      const session = fakeSession(options.expiresAt?.());
      connections.push(session);
      return session as unknown as RelaySession;
    },
    {
      url: 'https://relay.example.com/moq',
      reconnect: fast,
      onStatus: (status, error) => statuses.push([status, error]),
      onGeneration: ({ generation }) => generations.push(generation),
    },
  );
  cleanup.push(() => relay.close());
  return { relay, connections, statuses, generations };
}

const namespaces = (session: FakeSession) => [...session.publications.keys()].sort();

describe('connectSupervisedRelay', () => {
  it('serves every root under the connection generation, and subscribes on the same connection', async () => {
    const { relay, connections, generations } = await start();
    const generation = relay.generation!;
    expect(generations).toEqual([generation]);
    const me = relay.publish(['p', 'alice']);
    const host = relay.publish(['host']);
    expect(me.namespace).toEqual(['p', 'alice', generation]);
    expect(host.namespace).toEqual(['host', generation]);
    expect(namespaces(connections[0]!)).toEqual([`host/${generation}`, `p/alice/${generation}`]);

    const video = me.registerTrack({ track: 'video' });
    connections[0]!.publications.get(`p/alice/${generation}`)!.tracks.get('video')!.setDemand(true);
    await video.write(new Uint8Array([1]));
    expect(connections[0]!.publications.get(`p/alice/${generation}`)!.tracks.get('video')!.writes).toEqual([
      new Uint8Array([1]),
    ]);

    relay.want('bob', { namespace: ['p', 'bob', 'g1'], track: 'video', retry: fast });
    await until(() => connections[0]!.subscriptions.length === 1, 'subscribe on the one connection');
    expect(connections).toHaveLength(1);
  });

  it('brings every root, track and want back together as the next generation after a reconnect', async () => {
    const { relay, connections, statuses, generations } = await start();
    const me = relay.publish(['p', 'alice']);
    relay.publish(['host']);
    const writer = me.registerTrack({ track: 'video' });
    relay.want('bob', { namespace: ['p', 'bob', 'g1'], track: 'video', retry: fast });
    await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
    connections[0]!.subscriptions[0]!.accept();

    const lost = new RelayError('TRANSPORT_ERROR', 'gone', 'connection');
    connections[0]!.end(lost);
    await until(() => connections.length === 2 && relay.status === 'live', 'second connection');
    expect(statuses).toContainEqual(['reconnecting', lost]);
    const [first, second] = generations;
    expect(second! > first!).toBe(true);
    expect(namespaces(connections[1]!)).toEqual([`host/${second}`, `p/alice/${second}`]);
    await until(() => connections[1]!.subscriptions.length === 1, 'want resubscribed on the new connection');

    connections[1]!.publications.get(`p/alice/${second}`)!.tracks.get('video')!.setDemand(true);
    await writer.write(new Uint8Array([2]));
    expect(connections[1]!.publications.get(`p/alice/${second}`)!.tracks.get('video')!.writes).toEqual([
      new Uint8Array([2]),
    ]);
  });

  it('fails a write between connections with RECONNECTING', async () => {
    const gate = deferred();
    let calls = 0;
    const connections: FakeSession[] = [];
    const relay = await superviseRelay(
      async () => {
        calls += 1;
        if (calls > 1) await gate.promise;
        const session = fakeSession();
        connections.push(session);
        return session as unknown as RelaySession;
      },
      { url: 'https://relay.example.com/moq', reconnect: fast },
    );
    cleanup.push(() => relay.close());
    const writer = relay.publish(['p', 'alice']).registerTrack({ track: 'video' });
    connections[0]!.end(new RelayError('TRANSPORT_ERROR', 'gone', 'connection'));
    await until(() => relay.status === 'reconnecting', 'reconnecting');
    await expect(writer.write(new Uint8Array([1]))).rejects.toMatchObject({ code: 'RECONNECTING' });
    gate.resolve();
    await until(() => relay.status === 'live', 'live again');
  });

  it('replaces the whole connection when the relay stops soliciting one root', async () => {
    const { relay, connections, statuses, generations } = await start();
    relay.publish(['p', 'alice']);
    relay.publish(['host']);
    const first = generations[0]!;
    const withdrawn = new RelayError('NAMESPACE_ENDED', 'Relay withdrew the namespace', 'connection');
    connections[0]!.publications.get(`host/${first}`)!.end(withdrawn);
    await until(() => connections.length === 2 && relay.status === 'live', 'replacement connection');
    expect(statuses).toContainEqual(['reconnecting', withdrawn]);
    // The old connection is closed, not left serving the other root.
    let oldClosed = false;
    void connections[0]!.closed.then(() => {
      oldClosed = true;
    });
    await until(() => oldClosed, 'old connection closed');
    const second = generations[1]!;
    expect(namespaces(connections[1]!)).toEqual([`host/${second}`, `p/alice/${second}`]);
  });

  it('rotates make-before-break: the tracks and the wants move, then the old connection closes', async () => {
    const { relay, connections, generations } = await start();
    const me = relay.publish(['p', 'alice']);
    const writer = me.registerTrack({ track: 'video' });
    const objects: number[] = [];
    relay.want(
      'bob',
      { namespace: ['p', 'bob', 'g1'], track: 'video', retry: fast },
      { onObject: (object) => objects.push(object.groupId) },
    );
    await until(() => connections[0]!.subscriptions.length === 1, 'subscribe');
    connections[0]!.subscriptions[0]!.accept();
    connections[0]!.subscriptions[0]!.object(1);
    const firstTrack = connections[0]!.publications.get(`p/alice/${generations[0]}`)!.tracks.get('video')!;
    firstTrack.setDemand(true);

    relay.rotate();
    await until(() => connections.length === 2 && generations.length === 2, 'replacement up');
    const second = generations[1]!;
    expect(relay.generation).toBe(second);
    expect(me.namespace).toEqual(['p', 'alice', second]);
    // The old connection keeps serving until the track has demand on the new one.
    await until(() => connections[1]!.subscriptions.length === 1, 'want moved');
    connections[1]!.subscriptions[0]!.accept();
    connections[1]!.subscriptions[0]!.object(2);
    const nextTrack = connections[1]!.publications.get(`p/alice/${second}`)!.tracks.get('video')!;
    nextTrack.setDemand(true);
    await writer.write(new Uint8Array([9]));
    expect(nextTrack.writes).toEqual([new Uint8Array([9])]);
    await until(() => firstTrack.closed, 'old connection closed');
    expect(objects).toEqual([1, 2]);
    expect(relay.status).toBe('live');
  });

  it('publishes a root added while live, keeps roots unique, and closes one publication alone', async () => {
    const { relay, connections } = await start();
    const generation = relay.generation!;
    const me = relay.publish(['p', 'alice']);
    expect(() => relay.publish(['p', 'alice'])).toThrow(RangeError);
    const host = relay.publish(['host']);
    host.registerTrack({ track: 'chat' });
    await host.close();
    expect(connections[0]!.publications.get(`host/${generation}`)!.closeCalls).toBe(1);
    expect(() => host.registerTrack({ track: 'other' })).toThrow(RelayError);
    expect(() => relay.publish(['host'])).toThrow(RangeError);
    // The connection and the other publication carry on.
    expect(relay.status).toBe('live');
    expect(me.namespace).toEqual(['p', 'alice', generation]);
    expect(connections[0]!.publications.get(`p/alice/${generation}`)!.closeCalls).toBe(0);
  });

  it('checks a root at once, and refuses calls after close', async () => {
    const { relay, statuses } = await start();
    expect(() => relay.publish([])).toThrow(TypeError);
    expect(() => relay.publish(Array.from({ length: 32 }, (_, i) => `f${i}`))).toThrow(RangeError);
    await relay.close();
    expect(statuses.at(-1)).toEqual(['closed', undefined]);
    expect(await relay.closed).toEqual({});
    expect(() => relay.publish(['p'])).toThrow(RelayError);
    expect(() => relay.want('k', { namespace: ['p'], track: 't' })).toThrow(RelayError);
  });

  it('ends the writers and closes the connection on close', async () => {
    const { relay, connections } = await start();
    const writer = relay.publish(['p', 'alice']).registerTrack({ track: 'video' });
    await relay.close();
    expect(writer.closed).toBe(true);
    expect(connections[0]!.publications.size).toBe(1);
    expect([...connections[0]!.publications.values()][0]!.tracks.get('video')!.closed).toBe(true);
    expect(relay.generation).toBeUndefined();
  });
});

describe('connectSupervisedRelay publishing roots', () => {
  it('never reports its own roots, also from the connection a rotation is replacing', async () => {
    const { relay, connections } = await start();
    relay.publish(['room', 'p', 'alice']);
    const changes: string[][] = [];
    relay.publishingRoots(
      { prefix: ['room', 'p'] },
      { onChanged: ({ present }) => changes.push([...present.keys()].sort()) },
    );
    await until(() => connections[0]!.discoveries.length === 1, 'discovery');
    const discovery = connections[0]!.discoveries[0]!;
    // What the old connection would see during a rotation: its own next generation, announced by another session.
    discovery.push({ type: 'announced', namespace: ['room', 'p', 'alice', 'g2'] });
    discovery.push({ type: 'announced', namespace: ['room', 'p', 'bob', 'g1'] });
    discovery.push({ type: 'loaded' });
    await until(() => changes.length > 0 && changes.at(-1)!.includes('bob'), 'bob listed');
    expect(changes.flat()).not.toContain('alice');
  });
});
