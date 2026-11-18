import { describe, expect, it, vi } from 'vitest';

import type { TrackNamespace } from '../protocol/control-messages.js';
import { createMoqtPublishSession, type MoqtPublishSessionCallbacks } from '../protocol/publish-session.js';
import { createMoqtSession } from '../protocol/session.js';
import { rawSubscribe, solicitNamespace } from './helpers/raw-peer.js';
import { createTransportPair } from './helpers/transport-pair.js';

const PERSON = ['room', 'p', 'alice'];
const HOST = ['room', 'host'];

function harness(callbacks: MoqtPublishSessionCallbacks = {}) {
  const pair = createTransportPair();
  const subscriber = createMoqtSession(pair.server, { perspective: 'server' });
  const session = createMoqtPublishSession(pair.client, { callbacks });
  return {
    pair,
    session,
    done: () => {
      session.destroy();
      subscriber.destroy();
    },
  };
}

describe('a publish session announcing several namespaces', () => {
  it('announces each namespace on one solicitation and reports each once', async () => {
    const announced: TrackNamespace[] = [];
    const { pair, session, done } = harness({ onAnnounced: ({ namespace }) => announced.push(namespace) });

    await session.ready;
    session.announce(PERSON);
    session.announce(HOST);
    session.announce(PERSON);
    session.registerTrack({ trackNamespace: PERSON, trackName: 'video' });
    session.registerTrack({ trackNamespace: HOST, trackName: 'control' });

    const solicitation = await solicitNamespace(pair.server, ['room']);

    await vi.waitFor(() => expect(solicitation.received).toHaveLength(3));
    expect(solicitation.received.slice(1)).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ kind: 'namespace', trackNamespaceSuffix: ['p', 'alice'] }),
        expect.objectContaining({ kind: 'namespace', trackNamespaceSuffix: ['host'] }),
      ])
    );
    await vi.waitFor(() => expect(announced).toHaveLength(2));
    expect(announced).toEqual(expect.arrayContaining([PERSON, HOST]));
    done();
  });

  it("defers each namespace's announce until a track in it is registered", async () => {
    const { pair, session, done } = harness();

    await session.ready;
    session.announce(PERSON);
    session.announce(HOST);
    session.registerTrack({ trackNamespace: PERSON, trackName: 'video' });

    const solicitation = await solicitNamespace(pair.server, []);

    await vi.waitFor(() => expect(solicitation.received.map((m) => m.kind)).toEqual(['request-ok', 'namespace']));
    expect(solicitation.received[1]).toMatchObject({ trackNamespaceSuffix: PERSON });
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(solicitation.received).toHaveLength(2);

    session.registerTrack({ trackNamespace: HOST, trackName: 'control' });
    await vi.waitFor(() => expect(solicitation.received).toHaveLength(3));
    expect(solicitation.received[2]).toMatchObject({ kind: 'namespace', trackNamespaceSuffix: HOST });
    done();
  });

  it('withdraws one namespace with NAMESPACE_DONE and keeps the other', async () => {
    const announced: TrackNamespace[] = [];
    const endings: TrackNamespace[] = [];
    const { pair, session, done } = harness({
      onAnnounced: ({ namespace }) => announced.push(namespace),
      onAnnounceEnded: ({ namespace }) => endings.push(namespace),
    });

    await session.ready;
    session.announce(PERSON);
    session.announce(HOST);
    session.registerTrack({ trackNamespace: PERSON, trackName: 'video' });
    session.registerTrack({ trackNamespace: HOST, trackName: 'control' });
    const solicitation = await solicitNamespace(pair.server, []);
    await vi.waitFor(() => expect(solicitation.received).toHaveLength(3));

    session.withdraw(HOST);
    session.withdraw(HOST);
    session.withdraw(['room', 'never']);
    await vi.waitFor(() => expect(solicitation.received).toHaveLength(4));
    expect(solicitation.received[3]).toMatchObject({ kind: 'namespace-done', trackNamespaceSuffix: HOST });
    expect(session.isSolicited(PERSON)).toBe(true);

    // Announcing it again writes a fresh NAMESPACE and reports it again.
    session.announce(HOST);
    await vi.waitFor(() => expect(solicitation.received).toHaveLength(5));
    expect(solicitation.received[4]).toMatchObject({ kind: 'namespace', trackNamespaceSuffix: HOST });
    await vi.waitFor(() => expect(announced.filter((ns) => ns === HOST || ns.join() === HOST.join())).toHaveLength(2));
    expect(endings).toEqual([]);
    done();
  });

  it('reports the loss of each namespace a dying solicitation carried, and only those', async () => {
    const endings: TrackNamespace[] = [];
    const { pair, session, done } = harness({ onAnnounceEnded: ({ namespace }) => endings.push(namespace) });

    await session.ready;
    session.announce(PERSON);
    session.announce(HOST);
    session.registerTrack({ trackNamespace: PERSON, trackName: 'video' });
    session.registerTrack({ trackNamespace: HOST, trackName: 'control' });
    const people = await solicitNamespace(pair.server, ['room', 'p'], 1);
    const hosts = await solicitNamespace(pair.server, ['room', 'host'], 3);
    await vi.waitFor(() => {
      expect(people.received).toHaveLength(2);
      expect(hosts.received).toHaveLength(2);
    });

    void hosts.reset();
    await vi.waitFor(() => expect(endings).toEqual([HOST]));
    expect(session.isSolicited(HOST)).toBe(false);
    expect(session.isSolicited(PERSON)).toBe(true);
    expect(session.namespaceSolicited).toBe(true);
    done();
  });

  it('retracts every namespace at close', async () => {
    const { pair, session } = harness();

    await session.ready;
    session.announce(PERSON);
    session.announce(HOST);
    session.registerTrack({ trackNamespace: PERSON, trackName: 'video' });
    session.registerTrack({ trackNamespace: HOST, trackName: 'control' });
    const solicitation = await solicitNamespace(pair.server, ['room']);
    await vi.waitFor(() => expect(solicitation.received).toHaveLength(3));

    session.close();
    await vi.waitFor(() =>
      expect(
        solicitation.received.filter((m) => m.kind === 'namespace-done').map((m) => m.trackNamespaceSuffix)
      ).toEqual(expect.arrayContaining([['p', 'alice'], ['host']]))
    );
  });
  it('keeps namespaces apart whose fields joined with a slash would match', async () => {
    const bindings: { trackNamespace: TrackNamespace; trackAlias: number | undefined }[] = [];
    const { pair, session, done } = harness({
      onTrackBinding: ({ trackNamespace, trackAlias }) => bindings.push({ trackNamespace, trackAlias }),
    });

    await session.ready;
    session.announce(['a/b']);
    session.announce(['a', 'b']);
    session.registerTrack({ trackNamespace: ['a/b'], trackName: 'video' });
    session.registerTrack({ trackNamespace: ['a', 'b'], trackName: 'video' });
    await solicitNamespace(pair.server, [], 1);

    const first = await rawSubscribe(pair.server, ['a/b'], 'video', 3);
    await vi.waitFor(() => expect(first.received.map((m) => m.kind)).toEqual(['subscribe-ok']));
    expect(bindings).toEqual([{ trackNamespace: ['a/b'], trackAlias: 3 }]);
    done();
  });
});
