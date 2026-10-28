/**
 * `createMoqtSession({ perspective: 'server' })` standing in for a relay against a publish session over a loopback pair
 * (#367): the server allocates odd Request IDs from 1 (§6.4.2.1), the publish session takes each SUBSCRIBE's Request ID
 * as its Track Alias, and the subgroup streams it writes reach the subscription by that alias.
 */
import { describe, expect, it, vi } from 'vitest';

import { utf8Decode, utf8Encode } from '../protocol/bytes.js';
import { SESSION_ERROR } from '../protocol/errors.js';
import type { MoqtObject } from '../protocol/object-stream.js';
import { createMoqtPublishSession, type IncomingSubscribe } from '../protocol/publish-session.js';
import { createMoqtSession, type MoqtSessionConfig, type SubscribeOk } from '../protocol/session.js';
import { createSubgroupWriter } from '../protocol/subgroup-writer.js';
import { createTransportPair } from './helpers/transport-pair.js';

const NAMESPACE = ['live', 'abc123'];

function makeHarness(config: MoqtSessionConfig) {
  const pair = createTransportPair();
  const subscribes: IncomingSubscribe[] = [];
  const updates: { requestId: number; updateRequestId: number }[] = [];
  const publisher = createMoqtPublishSession(pair.client, {
    callbacks: {
      onSubscribe: (subscribe) => subscribes.push(subscribe),
      onRequestUpdate: ({ requestId, updateRequestId }) => updates.push({ requestId, updateRequestId }),
    },
  });
  const relay = createMoqtSession(pair.server, config);

  /** Subscribe through the server-perspective session, collecting what it reports. */
  const subscribe = (trackName: string) => {
    const oks: SubscribeOk[] = [];
    const objects: MoqtObject[] = [];
    const subscription = relay.subscribe(
      { trackNamespace: NAMESPACE, trackName },
      { onOk: (ok) => oks.push(ok), onObject: (object) => objects.push(object) }
    );

    return { subscription, oks, objects };
  };

  return { pair, publisher, relay, subscribes, updates, subscribe };
}

async function writeGroup(stream: WritableStream<Uint8Array>, trackAlias: number, groupId: number, text: string) {
  const group = createSubgroupWriter(stream, { trackAlias, groupId });

  await group.writeObject({ objectId: 0, payload: utf8Encode(text) });
  await group.fin();
}

describe("createMoqtSession({ perspective: 'server' })", () => {
  it('allocates odd request IDs from 1, which the publish session takes as Track Aliases', async () => {
    const { publisher, relay, subscribes, updates, subscribe } = makeHarness({ perspective: 'server' });

    await Promise.all([publisher.ready, relay.ready]);
    publisher.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video' });
    publisher.registerTrack({ trackNamespace: NAMESPACE, trackName: 'audio' });

    const video = subscribe('video');
    const audio = subscribe('audio');

    await vi.waitFor(() => expect(video.oks).toHaveLength(1));
    await vi.waitFor(() => expect(audio.oks).toHaveLength(1));
    expect(subscribes.map(({ requestId, trackName }) => [requestId, trackName])).toEqual([
      [1, 'video'],
      [3, 'audio'],
    ]);
    expect(video.oks[0]!.trackAlias).toBe(1);
    expect(audio.oks[0]!.trackAlias).toBe(3);

    // The next request, a REQUEST_UPDATE, takes the next odd ID.
    video.subscription.update({ forward: 1 });
    await vi.waitFor(() => expect(updates).toEqual([{ requestId: 1, updateRequestId: 5 }]));

    // Objects written under each alias reach the subscription that alias names.
    await writeGroup(await publisher.openUniStream(1), 1, 0, 'video frame');
    await writeGroup(await publisher.openUniStream(3), 3, 0, 'audio frame');
    await vi.waitFor(() => expect(video.objects).toHaveLength(1));
    await vi.waitFor(() => expect(audio.objects).toHaveLength(1));
    expect(utf8Decode(video.objects[0]!.payload)).toBe('video frame');
    expect(utf8Decode(audio.objects[0]!.payload)).toBe('audio frame');

    publisher.close();
    relay.destroy();
  });

  it("is refused with INVALID_REQUEST_ID by the publish session in the default 'client' perspective", async () => {
    const { pair, publisher, relay, subscribes, subscribe } = makeHarness({});
    const closed = pair.client.closed.then((info) => info);

    await Promise.all([publisher.ready, relay.ready]);
    publisher.registerTrack({ trackNamespace: NAMESPACE, trackName: 'video' });
    subscribe('video');

    await expect(closed).resolves.toEqual({
      closeCode: SESSION_ERROR.INVALID_REQUEST_ID,
      reason: 'peer sent even request id 0',
    });
    expect(subscribes).toEqual([]);
    relay.destroy();
  });
});
