/**
 * `RegisteredTrack.end({ after })`: a track's last group, written while the track ends or the session closes, reaches
 * its subscribers before their PUBLISH_DONE. Ported from Video.js v10 bdd65558f, adapted to this core's
 * `openUniStream(trackAlias)` and its close drain.
 */
import { describe, expect, it, vi } from 'vitest';

import { PUBLISH_DONE_STATUS, REQUEST_ERROR_CODE } from '../protocol/control-messages.js';
import type { MoqtObject } from '../protocol/object-stream.js';
import {
  CLOSE_FLUSH_TIMEOUT_MS,
  createMoqtPublishSession,
  type IncomingSubscribe,
  type MoqtPublishSessionCallbacks,
} from '../protocol/publish-session.js';
import { createMoqtSession, type PublishDone, type RequestError } from '../protocol/session.js';
import { createSubgroupWriter } from '../protocol/subgroup-writer.js';
import { rawSubscribe } from './helpers/raw-peer.js';
import { createTransportPair } from './helpers/transport-pair.js';

const NAMESPACE = ['live', 'abc123'];

function makeHarness(callbacks: MoqtPublishSessionCallbacks = {}) {
  const pair = createTransportPair();
  const subscribes: IncomingSubscribe[] = [];
  const bindings: (number | undefined)[] = [];
  const subscriber = createMoqtSession(pair.server, { perspective: 'server', unknownAliasTimeoutMs: 500 });
  const session = createMoqtPublishSession(pair.client, {
    callbacks: {
      onSubscribe: (subscribe) => subscribes.push(subscribe),
      onTrackBinding: ({ trackAlias }) => bindings.push(trackAlias),
      ...callbacks,
    },
  });

  /** Subscribe through the subscribe driver, collecting what it reports. */
  const subscribe = (trackName: string) => {
    const objects: MoqtObject[] = [];
    const done: PublishDone[] = [];
    const errors: RequestError[] = [];

    subscriber.subscribe(
      { trackNamespace: NAMESPACE, trackName },
      {
        onObject: (object) => objects.push(object),
        onDone: (d) => done.push(d),
        onError: (error) => errors.push(error),
      }
    );
    return { objects, done, errors };
  };

  return { pair, session, subscriber, subscribes, bindings, subscribe };
}

/** Write one whole group, as a track's last group would be. */
async function writeLastGroup(stream: WritableStream<Uint8Array>, trackAlias: number, groupId: number) {
  const group = createSubgroupWriter(stream, { trackAlias, groupId });

  await group.writeObject({ objectId: 0, payload: new TextEncoder().encode('end of broadcast') });
  await group.fin();
}

function deferred() {
  let resolve!: () => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<void>((res, rej) => {
    resolve = res;
    reject = rej;
  });

  return { promise, resolve, reject };
}

describe('RegisteredTrack.end({ after })', () => {
  it('delivers a last group written inside the close drain before PUBLISH_DONE', async () => {
    const { session, subscriber, subscribes, bindings, subscribe } = makeHarness();

    await session.ready;
    const track = session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'catalog' });
    const catalog = subscribe('catalog');

    await vi.waitFor(() => expect(subscribes).toHaveLength(1));
    const alias = subscribes[0]!.requestId;
    const last = deferred();

    // The track's owner ends it with its last group still to write, then closes the session.
    track.end({ after: last.promise });
    session.close();
    await new Promise((resolve) => setTimeout(resolve, 10));

    // Still bound, and the session still opens the stream although it is closing.
    expect(bindings.at(-1)).toBe(alias);
    await writeLastGroup(await session.openUniStream(alias), alias, 7);
    expect(catalog.done).toEqual([]);

    last.resolve();
    await vi.waitFor(() => expect(catalog.done).toHaveLength(1));
    expect(catalog.objects.map((object) => [object.groupId, new TextDecoder().decode(object.payload)])).toEqual([
      [7, 'end of broadcast'],
    ]);
    expect(catalog.done[0]).toMatchObject({
      statusCode: PUBLISH_DONE_STATUS.TRACK_ENDED,
      streamCount: 1,
      reason: 'track ended',
    });
    expect(bindings.at(-1)).toBeUndefined();
    subscriber.destroy();
  });

  it('ends the subscriptions when `after` rejects, too', async () => {
    const { session, subscriber, subscribes, subscribe } = makeHarness();

    await session.ready;
    const track = session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'catalog' });
    const catalog = subscribe('catalog');

    await vi.waitFor(() => expect(subscribes).toHaveLength(1));
    const last = deferred();

    track.end({ after: last.promise });
    last.reject(new Error('the last group failed'));
    await vi.waitFor(() => expect(catalog.done).toHaveLength(1));
    expect(catalog.done[0]).toMatchObject({ statusCode: PUBLISH_DONE_STATUS.TRACK_ENDED, streamCount: 0 });
    session.destroy();
    subscriber.destroy();
  });

  it('refuses new SUBSCRIBEs at once while the end waits', async () => {
    const ended: number[] = [];
    const { pair, session, subscriber, subscribes } = makeHarness({
      onSubscribeEnd: ({ requestId }) => ended.push(requestId),
    });

    await session.ready;
    const track = session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'catalog' });
    const first = await rawSubscribe(pair.server, NAMESPACE, 'catalog', 31);

    await vi.waitFor(() => expect(subscribes).toHaveLength(1));
    const last = deferred();

    track.end({ after: last.promise });
    // Only the first call's options apply.
    track.end();

    const late = await rawSubscribe(pair.server, NAMESPACE, 'catalog', 33);

    await vi.waitFor(() => expect(late.received.map((m) => m.kind)).toEqual(['request-error']));
    expect(late.received[0]).toMatchObject({ errorCode: REQUEST_ERROR_CODE.DOES_NOT_EXIST });
    expect(first.received.map((m) => m.kind)).toEqual(['subscribe-ok']);
    expect(ended).toEqual([]);

    last.resolve();
    await vi.waitFor(() => expect(first.received.map((m) => m.kind)).toEqual(['subscribe-ok', 'publish-done']));
    expect(ended).toEqual([31]);
    session.destroy();
    subscriber.destroy();
  });

  it('close() ends the subscriptions once its flush window passes when `after` never settles', async () => {
    const { session, subscriber, subscribes, subscribe } = makeHarness();

    await session.ready;
    const track = session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'catalog' });
    const catalog = subscribe('catalog');

    await vi.waitFor(() => expect(subscribes).toHaveLength(1));
    const alias = subscribes[0]!.requestId;
    const stuck = await session.openUniStream(alias);

    track.end({ after: new Promise(() => {}) });
    session.close();

    await new Promise((resolve) => setTimeout(resolve, CLOSE_FLUSH_TIMEOUT_MS / 2));
    expect(catalog.done).toEqual([]);

    // The stream still open when the end goes out is reset first, and counted (§9.9).
    await vi.waitFor(() => expect(catalog.done).toHaveLength(1), { timeout: CLOSE_FLUSH_TIMEOUT_MS * 4 });
    expect(catalog.done[0]).toMatchObject({ statusCode: PUBLISH_DONE_STATUS.TRACK_ENDED, streamCount: 1 });
    await expect(stuck.getWriter().write(new Uint8Array([1]))).rejects.toBeDefined();
    subscriber.destroy();
  });

  it('opens no stream while closing for a track whose end does not wait', async () => {
    const { session, subscriber, subscribes, subscribe } = makeHarness();

    await session.ready;
    session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video' });
    const waiting = session.registerTrack({ trackNamespace: NAMESPACE, trackName: 'catalog' });

    subscribe('video');
    subscribe('catalog');
    await vi.waitFor(() => expect(subscribes).toHaveLength(2));
    const aliasOf = (name: string) => subscribes.find((s) => s.trackName === name)!.requestId;

    waiting.end({ after: new Promise(() => {}) });
    session.close();

    // The video subscription is still live for a beat (close() sweeps on the next macrotask), but closing has begun.
    await expect(session.openUniStream(aliasOf('video'))).rejects.toThrow('moq publish session: closed');
    await (await session.openUniStream(aliasOf('catalog'))).close();
    subscriber.destroy();
  });
});
