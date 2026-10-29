import { resolveObjectURL } from 'node:buffer';
import type { MoqtObject } from '@caprid/moq-core';
import {
  buildInterrupt,
  buildMsfCatalog,
  buildTranscriptSegment,
  INTERRUPT_TRACK,
  LOC_TIMESTAMP,
  TRANSCRIPT_TRACK,
  warmAudioPlayer,
} from '@caprid/moq-media';
import type { RelaySubscriber } from '@caprid/relay';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { listenAudio } from '../examples/webcodecs/listen-audio.js';

const NAMESPACE = ['room', 'mic'];

// Consecutive 20 ms Opus packets (SILK, mono, a 300 Hz tone), as the audio player's tests use.
const OPUS_PACKETS = [
  'eNsq0lbYDhjw3GMFVER1GLNe/OddAMhCpLUNddotogMYjyI+Lqw7/cGB8SUyVzLYYg1p+OhrUr8GRx9EnlvinGV2IKO9G2mTqw==',
  'eNsq7WglA7LrXjVTAhZx0u/bcYjn2IQ+nZkkmbqtIvM/GUdF096ifK6pJfCiP31rkbzF9I+RIOc5eIcG43G2Lg+9G2mTqw==',
  'eNsqyzGGB/WI4/gZs/o8XsQJA0SUE8aydrKEZ/l2yDY60kCD8mNSJpMyMiKnR9ho9tnY2k1lWBhoNZrsbpXKJnPrRdc2kh1vW+mTpw==',
  'eNsqyzGGGB8x8vZN7XpxGSMf/LkvWKgFq+6+0RYrb/bGxwU3ZW65u2NIDiMUQ5vXkRTsudkJIZrXTjS8UxZOz//FKJXciG+9GxPpqw==',
].map((base64) => new Uint8Array(Buffer.from(base64, 'base64')));

class FakeNode {
  gain = { value: 1 };
  connect = vi.fn();
  disconnect = vi.fn();
}

class FakeWorkletNode extends FakeNode {
  static instances: FakeWorkletNode[] = [];
  port = { onmessage: null, postMessage: vi.fn(), close: vi.fn() };
  constructor() {
    super();
    FakeWorkletNode.instances.push(this);
  }
}

class FakeContext {
  static instances: FakeContext[] = [];
  /** Fails the worklet module's load, from the context's construction on. */
  static moduleFailure: Error | undefined;
  readonly sampleRate = 48_000;
  readonly baseLatency = 0;
  readonly outputLatency = 0;
  destination = new FakeNode();
  close = vi.fn(async () => {});
  audioWorklet = {
    addModule: vi.fn(async (url: string) => {
      if (FakeContext.moduleFailure) throw FakeContext.moduleFailure;
      if (!resolveObjectURL(url)) throw new Error('Not a blob URL');
    }),
  };
  constructor() {
    FakeContext.instances.push(this);
  }
  createGain() {
    return new FakeNode();
  }
}

type Handlers = NonNullable<Parameters<RelaySubscriber['subscribe']>[1]>;
type Request = { namespace: string[]; track: string };

/** A subscriber that records each subscription and lets the test deliver catalog objects. */
function fakeSubscriber() {
  const subscriptions: { request: Request; handlers: Handlers; cancel: ReturnType<typeof vi.fn> }[] = [];
  const subscriber = {
    subscribe: vi.fn((request: Request, handlers: Handlers) => {
      const subscription = { request, handlers, cancel: vi.fn() };
      subscriptions.push(subscription);
      return subscription;
    }),
  };
  return {
    subscriber: subscriber as unknown as RelaySubscriber,
    subscriptions,
    /** The catalog subscription: the first the listener makes. */
    get catalog() {
      return subscriptions[0] as (typeof subscriptions)[number];
    },
    deliverCatalog(payload: Uint8Array) {
      (subscriptions[0] as (typeof subscriptions)[number]).handlers.onObject?.({
        status: 'normal',
        payload,
      } as unknown as Parameters<NonNullable<Handlers['onObject']>>[0]);
    },
  };
}

const audioCatalog = () => buildMsfCatalog({ namespace: NAMESPACE, audio: {} });
const videoOnlyCatalog = () =>
  buildMsfCatalog({ namespace: NAMESPACE, video: { codec: 'vp8', width: 640, height: 360, framerate: 30 } });

/** Start listening and hand the catalog over once the listener has subscribed to it. */
function listen(
  fake: ReturnType<typeof fakeSubscriber>,
  payload: Uint8Array,
  options?: Parameters<typeof listenAudio>[2],
) {
  const listening = listenAudio(fake.subscriber, NAMESPACE, options);
  // A rejection that lands before the test awaits it must not count as unhandled.
  listening.catch(() => {});
  fake.deliverCatalog(payload);
  return listening;
}

beforeEach(() => {
  FakeContext.instances.length = 0;
  FakeContext.moduleFailure = undefined;
  FakeWorkletNode.instances.length = 0;
  vi.stubGlobal('AudioContext', FakeContext);
  vi.stubGlobal('AudioWorkletNode', FakeWorkletNode);
});
afterEach(() => {
  vi.unstubAllGlobals();
});

it('closes the context it created and cancels the catalog when the catalog does not parse', async () => {
  const fake = fakeSubscriber();
  await expect(listen(fake, new TextEncoder().encode('not json'))).rejects.toThrow();
  expect(FakeContext.instances).toHaveLength(1);
  expect(FakeContext.instances[0]?.close).toHaveBeenCalledTimes(1);
  expect(fake.catalog.cancel).toHaveBeenCalledTimes(1);
});

it('closes the context and cancels the catalog when the catalog lists no audio track', async () => {
  const fake = fakeSubscriber();
  await expect(listen(fake, videoOnlyCatalog())).rejects.toThrow(/lists no audio track/);
  expect(FakeContext.instances[0]?.close).toHaveBeenCalledTimes(1);
  expect(fake.catalog.cancel).toHaveBeenCalledTimes(1);
  // The audio track was subscribed with the catalog, before it arrived: it is cancelled too.
  expect(fake.subscriptions.map(({ request }) => request.track)).toEqual(['catalog', 'audio']);
  expect(fake.subscriptions[1]?.cancel).toHaveBeenCalledTimes(1);
});

it('closes the context and cancels the catalog when the audio player cannot load', async () => {
  const fake = fakeSubscriber();
  const failure = new Error('The worklet module failed to load');
  // The warm starts as soon as listenAudio does, and fails; the player loads again, fails the same, and reports it.
  FakeContext.moduleFailure = failure;
  await expect(listen(fake, audioCatalog())).rejects.toBe(failure);
  expect(FakeContext.instances[0]?.close).toHaveBeenCalledTimes(1);
  expect(fake.catalog.cancel).toHaveBeenCalledTimes(1);
  expect(fake.subscriptions[1]?.cancel).toHaveBeenCalledTimes(1);
});

it('releases the player and subscriptions when the audio subscription cannot start', async () => {
  const fake = fakeSubscriber();
  const failure = new Error('The relay is gone');
  const subscribe = fake.subscriber.subscribe as unknown as ReturnType<typeof vi.fn>;
  const original = subscribe.getMockImplementation() as (request: Request, handlers: Handlers) => unknown;
  subscribe.mockImplementation((request: Request, handlers: Handlers) => {
    if (request.track === 'audio') throw failure;
    return original(request, handlers);
  });
  await expect(listen(fake, audioCatalog())).rejects.toBe(failure);
  expect(FakeContext.instances[0]?.close).toHaveBeenCalledTimes(1);
  // The catalog was subscribed first and is still pending: the failure ends its read and cancels it.
  expect(fake.catalog.cancel).toHaveBeenCalledTimes(1);
});

it('does not close a context the caller supplied when setup fails', async () => {
  const fake = fakeSubscriber();
  const context = new FakeContext();
  await expect(listen(fake, videoOnlyCatalog(), { context: context as unknown as AudioContext })).rejects.toThrow();
  expect(fake.catalog.cancel).toHaveBeenCalledTimes(1);
  expect(context.close).not.toHaveBeenCalled();
  // Only the caller's context exists: the listener made none of its own.
  expect(FakeContext.instances).toHaveLength(1);
});

it('keeps the original error when cleanup fails too', async () => {
  const fake = fakeSubscriber();
  const listening = listen(fake, videoOnlyCatalog());
  const context = FakeContext.instances[0] as FakeContext;
  context.close.mockRejectedValue(new Error('close failed'));
  fake.catalog.cancel.mockImplementation(() => {
    throw new Error('cancel failed');
  });
  await expect(listening).rejects.toThrow(/lists no audio track/);
  expect(context.close).toHaveBeenCalledTimes(1);
});

it('starts warming the player on the context before the catalog arrives', async () => {
  const fake = fakeSubscriber();
  const listening = listenAudio(fake.subscriber, NAMESPACE);
  const context = FakeContext.instances[0] as FakeContext;
  // Nothing has arrived: the worklet module is already loading, and the audio is already subscribed.
  expect(context.audioWorklet.addModule).toHaveBeenCalledTimes(1);
  expect(fake.subscriptions.map(({ request }) => request.track)).toEqual(['catalog', 'audio']);
  fake.deliverCatalog(audioCatalog());
  const listener = await listening;
  // The player found the module loaded or loading: it did not load it again.
  expect(context.audioWorklet.addModule).toHaveBeenCalledTimes(1);
  await listener.stop();
});

it('does not load again a context the page already warmed', async () => {
  const fake = fakeSubscriber();
  const context = new FakeContext();
  await warmAudioPlayer(context as unknown as AudioContext);
  expect(context.audioWorklet.addModule).toHaveBeenCalledTimes(1);
  const listening = listenAudio(fake.subscriber, NAMESPACE, { context: context as unknown as AudioContext });
  fake.deliverCatalog(audioCatalog());
  await (await listening).stop();
  expect(context.audioWorklet.addModule).toHaveBeenCalledTimes(1);
});

it('cancels both subscriptions and closes its context when aborted before the catalog arrives', async () => {
  const fake = fakeSubscriber();
  const controller = new AbortController();
  const reason = new Error('Hung up');
  const listening = listenAudio(fake.subscriber, NAMESPACE, { signal: controller.signal });
  listening.catch(() => {});
  controller.abort(reason);
  await expect(listening).rejects.toBe(reason);
  expect(fake.subscriptions.map(({ request }) => request.track)).toEqual(['catalog', 'audio']);
  expect(fake.subscriptions[0]?.cancel).toHaveBeenCalledTimes(1);
  expect(fake.subscriptions[1]?.cancel).toHaveBeenCalledTimes(1);
  expect(FakeContext.instances[0]?.close).toHaveBeenCalledTimes(1);
});

it('holds the frames that arrive before the player exists and passes them to the input in order', async () => {
  const fake = fakeSubscriber();
  const listening = listenAudio(fake.subscriber, NAMESPACE);
  const audio = fake.subscriptions[1] as (typeof fake.subscriptions)[number];
  const object = (index: number) =>
    ({
      groupId: index,
      objectId: 0,
      subgroupId: 0,
      status: 'normal',
      properties: [{ type: LOC_TIMESTAMP, value: 1_000_000 + index * 20_000 }],
      payload: OPUS_PACKETS[index] as Uint8Array,
    }) as MoqtObject;
  // Three frames, before the catalog: no player, no input.
  for (const index of [0, 1, 2]) audio.handlers.onObject?.(object(index));
  expect(FakeWorkletNode.instances).toHaveLength(0);
  fake.deliverCatalog(audioCatalog());
  const listener = await listening;
  const phases = listener.input.joinPhases;
  expect(phases.firstPushUs).not.toBeNull();
  expect(listener.input.stats.decoded).toBe(3);
  // They reached the worklet in order, and a frame that arrives now follows them straight on.
  audio.handlers.onObject?.(object(3));
  expect(listener.input.stats.decoded).toBe(4);
  const timestamps = (FakeWorkletNode.instances[0] as FakeWorkletNode).port.postMessage.mock.calls
    .map(([message]) => message as { type: string; timestampUs?: number })
    .filter((message) => message.type === 'pcm')
    .map((message) => message.timestampUs as number);
  expect(timestamps).toHaveLength(4);
  expect(timestamps).toEqual([...timestamps].sort((a, b) => a - b));
  await listener.stop();
});

it('plays through a new context and stop() closes it and cancels both subscriptions', async () => {
  const fake = fakeSubscriber();
  const listening = listenAudio(fake.subscriber, NAMESPACE);
  fake.deliverCatalog(audioCatalog());
  const listener = await listening;
  expect(listener.context).toBe(FakeContext.instances[0]);
  expect(fake.subscriptions.map(({ request }) => request.track)).toEqual(['catalog', 'audio']);
  expect(listener.player.inputs).toHaveLength(1);
  expect(FakeContext.instances[0]?.close).not.toHaveBeenCalled();
  await listener.stop();
  expect(fake.subscriptions[0]?.cancel).toHaveBeenCalledTimes(1);
  expect(fake.subscriptions[1]?.cancel).toHaveBeenCalledTimes(1);
  expect(listener.player.inputs).toHaveLength(0);
  expect(FakeContext.instances[0]?.close).toHaveBeenCalledTimes(1);
});

it('leaves a supplied context open when stopped', async () => {
  const fake = fakeSubscriber();
  const context = new FakeContext();
  const listening = listenAudio(fake.subscriber, NAMESPACE, { context: context as unknown as AudioContext });
  fake.deliverCatalog(audioCatalog());
  const listener = await listening;
  await listener.stop();
  expect(context.close).not.toHaveBeenCalled();
});

const interruptCatalog = () => buildMsfCatalog({ namespace: NAMESPACE, audio: {}, data: [{ name: INTERRUPT_TRACK }] });

/** Deliver one object on the subscription to `track`. */
function deliver(fake: ReturnType<typeof fakeSubscriber>, track: string, payload: Uint8Array) {
  const subscription = fake.subscriptions.find(({ request }) => request.track === track);
  subscription?.handlers.onObject?.({ status: 'normal', payload } as unknown as Parameters<
    NonNullable<Handlers['onObject']>
  >[0]);
}

it('subscribes to the interrupt track only when the catalog lists it', async () => {
  const fake = fakeSubscriber();
  const listening = listenAudio(fake.subscriber, NAMESPACE);
  fake.deliverCatalog(interruptCatalog());
  const listener = await listening;
  expect(fake.subscriptions.map(({ request }) => request.track)).toEqual(['catalog', 'audio', INTERRUPT_TRACK]);
  await listener.stop();
  expect(fake.subscriptions[2]?.cancel).toHaveBeenCalledTimes(1);
});

it('cuts the audio an interrupt for the audio track names, and ignores one for another track', async () => {
  const fake = fakeSubscriber();
  const onInterrupt = vi.fn();
  const listening = listenAudio(fake.subscriber, NAMESPACE, { onInterrupt });
  fake.deliverCatalog(interruptCatalog());
  const listener = await listening;
  const cut = vi.spyOn(listener.input, 'cut');

  deliver(fake, INTERRUPT_TRACK, buildInterrupt({ track: 'other', untilUs: 1_000 }));
  deliver(fake, INTERRUPT_TRACK, buildInterrupt({ fromUs: 500, untilUs: 2_000 }));

  expect(cut).toHaveBeenCalledTimes(1);
  expect(cut).toHaveBeenCalledWith({ track: 'audio', fromUs: 500, untilUs: 2_000 });
  expect(onInterrupt).toHaveBeenCalledWith({ track: 'audio', fromUs: 500, untilUs: 2_000 });
  await listener.stop();
});

it('reports an interrupt that does not parse and cuts nothing', async () => {
  const fake = fakeSubscriber();
  const onError = vi.fn();
  const listening = listenAudio(fake.subscriber, NAMESPACE, { onError });
  fake.deliverCatalog(interruptCatalog());
  const listener = await listening;
  const cut = vi.spyOn(listener.input, 'cut');

  deliver(fake, INTERRUPT_TRACK, new TextEncoder().encode('{"track":"audio"}'));

  expect(cut).not.toHaveBeenCalled();
  expect(onError).toHaveBeenCalledTimes(1);
  await listener.stop();
});

const transcriptCatalog = () =>
  buildMsfCatalog({ namespace: NAMESPACE, audio: {}, data: [{ name: INTERRUPT_TRACK }, { name: TRANSCRIPT_TRACK }] });

it('keeps the transcript of the audio track when the catalog lists it, and stop() cancels it', async () => {
  const fake = fakeSubscriber();
  const onTranscript = vi.fn();
  const listening = listenAudio(fake.subscriber, NAMESPACE, { onTranscript });
  fake.deliverCatalog(transcriptCatalog());
  const listener = await listening;
  expect(fake.subscriptions.map(({ request }) => request.track)).toEqual([
    'catalog',
    'audio',
    INTERRUPT_TRACK,
    TRANSCRIPT_TRACK,
  ]);

  deliver(fake, TRANSCRIPT_TRACK, buildTranscriptSegment({ seq: 0, track: 'other', fromUs: 1_000, text: 'Not ours' }));
  deliver(fake, TRANSCRIPT_TRACK, buildTranscriptSegment({ seq: 0, fromUs: 1_000, text: 'Hel' }));
  deliver(
    fake,
    TRANSCRIPT_TRACK,
    buildTranscriptSegment({ seq: 0, fromUs: 1_000, untilUs: 2_000, text: 'Hello', final: true }),
  );
  deliver(fake, TRANSCRIPT_TRACK, buildTranscriptSegment({ seq: 0, fromUs: 1_000, text: 'Hel' }));

  expect(onTranscript).toHaveBeenCalledTimes(2);
  expect(listener.transcript.lines).toEqual([
    { seq: 0, track: 'audio', fromUs: 1_000, untilUs: 2_000, text: 'Hello', final: true, cut: false },
  ]);
  await listener.stop();
  expect(fake.subscriptions[3]?.cancel).toHaveBeenCalledTimes(1);
});

it('cuts the transcript at the playout position on an interrupt', async () => {
  const fake = fakeSubscriber();
  const listening = listenAudio(fake.subscriber, NAMESPACE);
  fake.deliverCatalog(transcriptCatalog());
  const listener = await listening;
  vi.spyOn(listener.clock, 'position').mockReturnValue(1_500);

  deliver(fake, TRANSCRIPT_TRACK, buildTranscriptSegment({ seq: 0, fromUs: 1_000, text: 'One' }));
  deliver(fake, TRANSCRIPT_TRACK, buildTranscriptSegment({ seq: 1, fromUs: 2_000, text: 'Two' }));
  deliver(fake, INTERRUPT_TRACK, buildInterrupt({ untilUs: 3_000 }));

  expect(listener.transcript.lines).toEqual([
    { seq: 0, track: 'audio', fromUs: 1_000, untilUs: 1_500, text: 'One', final: true, cut: true },
  ]);
  await listener.stop();
});

it('reports a transcript segment that does not parse', async () => {
  const fake = fakeSubscriber();
  const onError = vi.fn();
  const listening = listenAudio(fake.subscriber, NAMESPACE, { onError });
  fake.deliverCatalog(transcriptCatalog());
  const listener = await listening;

  deliver(fake, TRANSCRIPT_TRACK, new TextEncoder().encode('{"seq":0}'));

  expect(onError).toHaveBeenCalledTimes(1);
  expect(listener.transcript.lines).toEqual([]);
  await listener.stop();
});
