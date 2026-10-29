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
  port: {
    onmessage: ((event: { data: unknown }) => void) | null;
    postMessage: ReturnType<typeof vi.fn>;
    close: ReturnType<typeof vi.fn>;
  } = {
    onmessage: null,
    postMessage: vi.fn(),
    close: vi.fn(),
  };
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

it('fails setup when the audio subscription fails before the catalog arrives and the catalog names that track', async () => {
  const fake = fakeSubscriber();
  const onError = vi.fn();
  const failure = new Error('unroutable');
  const listening = listenAudio(fake.subscriber, NAMESPACE, { onError });
  listening.catch(() => {});
  const audio = fake.subscriptions[1] as (typeof fake.subscriptions)[number];
  expect(audio.request.track).toBe('audio');
  audio.handlers.onError?.(failure as never);
  // Judged once the catalog has named the track: a broadcast whose track was replaced has another one.
  fake.deliverCatalog(audioCatalog());
  await expect(listening).rejects.toBe(failure);
  // A setup failure is the rejection, not an onError call.
  expect(onError).not.toHaveBeenCalled();
  expect(fake.catalog.cancel).toHaveBeenCalledTimes(1);
  expect(audio.cancel).toHaveBeenCalledTimes(1);
  expect(FakeContext.instances[0]?.close).toHaveBeenCalledTimes(1);
});

it('fails setup when the audio subscription failed by the time the catalog arrives', async () => {
  const fake = fakeSubscriber();
  const failure = new Error('ended');
  const listening = listenAudio(fake.subscriber, NAMESPACE);
  listening.catch(() => {});
  const audio = fake.subscriptions[1] as (typeof fake.subscriptions)[number];
  fake.deliverCatalog(audioCatalog());
  audio.handlers.onError?.(failure as never);
  await expect(listening).rejects.toBe(failure);
  expect(fake.catalog.cancel).toHaveBeenCalledTimes(1);
  expect(audio.cancel).toHaveBeenCalledTimes(1);
});

it('reports an audio subscription failure after it started to onError', async () => {
  const fake = fakeSubscriber();
  const onError = vi.fn();
  const listener = await listen(fake, audioCatalog(), { onError });
  const failure = new Error('later');
  fake.subscriptions[1]?.handlers.onError?.(failure as never);
  expect(onError).toHaveBeenCalledWith(failure);
  await listener.stop();
});

it('cancels a catalog that came at once when the audio subscribe then throws', async () => {
  const fake = fakeSubscriber();
  const failure = new Error('The relay is gone');
  const subscribe = fake.subscriber.subscribe as unknown as ReturnType<typeof vi.fn>;
  const original = subscribe.getMockImplementation() as (request: Request, handlers: Handlers) => unknown;
  subscribe.mockImplementation((request: Request, handlers: Handlers) => {
    if (request.track === 'audio') throw failure;
    const subscription = original(request, handlers);
    // The catalog's first object arrives before subscribe() returns.
    handlers.onObject?.({ status: 'normal', payload: audioCatalog() } as never);
    return subscription;
  });
  const listening = listenAudio(fake.subscriber, NAMESPACE);
  await expect(listening).rejects.toBe(failure);
  await Promise.resolve();
  expect(fake.catalog.cancel).toHaveBeenCalledTimes(1);
  expect(FakeContext.instances[0]?.close).toHaveBeenCalledTimes(1);
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

it('subscribes to interrupts at the catalog and cuts held audio with one that comes before the player exists', async () => {
  const fake = fakeSubscriber();
  const onInterrupt = vi.fn();
  let ready: () => void = () => {};
  // The worklet module loads only when the test says so: the player cannot exist before then.
  const loaded = new Promise<void>((resolve) => {
    ready = resolve;
  });
  const context = new FakeContext();
  context.audioWorklet.addModule.mockImplementation(async () => loaded);
  const listening = listenAudio(fake.subscriber, NAMESPACE, {
    onInterrupt,
    context: context as unknown as AudioContext,
  });
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
  for (const index of [0, 1, 2]) audio.handlers.onObject?.(object(index));
  fake.deliverCatalog(interruptCatalog());
  // The interrupt track is subscribed once the catalog lists it, while the player is still being built.
  await vi.waitFor(() => expect(fake.subscriptions.map(({ request }) => request.track)).toContain(INTERRUPT_TRACK));
  expect(FakeWorkletNode.instances).toHaveLength(0);
  // The talker cut everything it had sent.
  deliver(fake, INTERRUPT_TRACK, buildInterrupt({ track: 'audio', untilUs: 1_060_000 }));
  ready();
  const listener = await listening;
  expect(onInterrupt).toHaveBeenCalledWith({ track: 'audio', untilUs: 1_060_000 });
  // The held frames were inside the cut: none of them reached the worklet.
  const pcm = (FakeWorkletNode.instances[0] as FakeWorkletNode).port.postMessage.mock.calls.filter(
    ([message]) => (message as { type: string }).type === 'pcm',
  );
  expect(pcm).toHaveLength(0);
  expect(listener.input.stats.decoded).toBe(0);
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

/** The relay ends a subscription with an error. */
const failWith = (subscription: { handlers: Handlers } | undefined, error: Error) =>
  subscription?.handlers.onError?.(error as unknown as Parameters<NonNullable<Handlers['onError']>>[0]);

const replacementCatalog = (name: string) =>
  buildMsfCatalog({ namespace: NAMESPACE, media: [{ role: 'audio', name }] });

it('follows a track replacement: cuts the old track, restarts the sequencer and plays the new track', async () => {
  const fake = fakeSubscriber();
  const onTrackReplaced = vi.fn();
  const listening = listenAudio(fake.subscriber, NAMESPACE, { onTrackReplaced });
  fake.deliverCatalog(audioCatalog());
  const listener = await listening;
  expect(listener.track).toBe('audio');
  const cut = vi.spyOn(listener.input, 'cut');
  const restart = vi.spyOn(listener.sequencer, 'restart');

  // The same catalog again replaces nothing.
  fake.deliverCatalog(audioCatalog());
  expect(cut).not.toHaveBeenCalled();
  fake.deliverCatalog(replacementCatalog('audio-2'));

  expect(fake.subscriptions.map(({ request }) => request.track)).toEqual(['catalog', 'audio', 'audio-2']);
  expect(fake.subscriptions[1]?.cancel).toHaveBeenCalledTimes(1);
  expect(cut).toHaveBeenCalledWith();
  expect(restart).toHaveBeenCalledTimes(1);
  expect(listener.track).toBe('audio-2');
  expect(onTrackReplaced).toHaveBeenCalledWith({
    from: 'audio',
    to: 'audio-2',
    phase: 'seen',
    atUs: expect.any(Number),
  });
  await listener.stop();
  expect(fake.subscriptions[2]?.cancel).toHaveBeenCalledTimes(1);
  // Nothing follows a catalog after stop().
  fake.deliverCatalog(replacementCatalog('audio-3'));
  expect(fake.subscriptions).toHaveLength(3);
});

it('applies interrupts to the track that replaced the audio track', async () => {
  const fake = fakeSubscriber();
  const listening = listenAudio(fake.subscriber, NAMESPACE);
  fake.deliverCatalog(interruptCatalog());
  const listener = await listening;
  fake.deliverCatalog(
    buildMsfCatalog({
      namespace: NAMESPACE,
      media: [{ role: 'audio', name: 'audio-2' }],
      data: [{ name: INTERRUPT_TRACK }],
    }),
  );
  const cut = vi.spyOn(listener.input, 'cut');
  deliver(fake, INTERRUPT_TRACK, buildInterrupt({ track: 'audio', untilUs: 1_000 }));
  deliver(fake, INTERRUPT_TRACK, buildInterrupt({ track: 'audio-2', untilUs: 2_000 }));
  expect(cut).toHaveBeenCalledTimes(1);
  expect(cut).toHaveBeenCalledWith({ track: 'audio-2', untilUs: 2_000 });
  await listener.stop();
});

it('joins a broadcast whose audio track was replaced already', async () => {
  const fake = fakeSubscriber();
  const listening = listenAudio(fake.subscriber, NAMESPACE);
  // The conventional name is subscribed with the catalog; this talker retired that track before the listener joined.
  const conventional = fake.subscriptions[1] as (typeof fake.subscriptions)[number];
  failWith(conventional, new Error('no such track'));
  fake.deliverCatalog(replacementCatalog('audio-3'));
  const listener = await listening;
  expect(listener.track).toBe('audio-3');
  expect(fake.subscriptions.map(({ request }) => request.track)).toEqual(['catalog', 'audio', 'audio-3']);
  // Its failure did not fail the setup, and its subscription was dropped for the catalog's track.
  expect(conventional.cancel).toHaveBeenCalledTimes(1);
  await listener.stop();
});

it('reports a later catalog that does not parse and keeps playing', async () => {
  const fake = fakeSubscriber();
  const onError = vi.fn();
  const listening = listenAudio(fake.subscriber, NAMESPACE, { onError });
  fake.deliverCatalog(audioCatalog());
  const listener = await listening;
  fake.deliverCatalog(new TextEncoder().encode('not json'));
  expect(onError).toHaveBeenCalledTimes(1);
  expect(listener.track).toBe('audio');
  await listener.stop();
});

it('follows a replacement that came while the listener was setting up', async () => {
  const fake = fakeSubscriber();
  const onTrackReplaced = vi.fn();
  const listening = listenAudio(fake.subscriber, NAMESPACE, { onTrackReplaced });
  fake.deliverCatalog(audioCatalog());
  // The player is still loading its worklet: the replacement arrives before the listener has started.
  fake.deliverCatalog(replacementCatalog('audio-2'));
  const listener = await listening;
  expect(listener.track).toBe('audio-2');
  expect(fake.subscriptions.map(({ request }) => request.track)).toEqual(['catalog', 'audio', 'audio-2']);
  expect(fake.subscriptions[1]?.cancel).toHaveBeenCalledTimes(1);
  expect(onTrackReplaced).toHaveBeenCalledWith(
    expect.objectContaining({ from: 'audio', to: 'audio-2', phase: 'seen' }),
  );
  await listener.stop();
});

it('starts a new transcript for the track that replaced the audio track', async () => {
  const fake = fakeSubscriber();
  const onTranscript = vi.fn();
  const listening = listenAudio(fake.subscriber, NAMESPACE, { onTranscript });
  const transcriptCatalog = (name: string) =>
    buildMsfCatalog({
      namespace: NAMESPACE,
      media: [{ role: 'audio', name }],
      data: [{ name: TRANSCRIPT_TRACK }],
    });
  fake.deliverCatalog(transcriptCatalog('audio'));
  const listener = await listening;
  deliver(
    fake,
    TRANSCRIPT_TRACK,
    buildTranscriptSegment({ track: 'audio', seq: 0, final: true, fromUs: 1_000, untilUs: 1_500, text: 'old reply' }),
  );
  expect(listener.transcript.lines.map((line) => line.text)).toEqual(['old reply']);
  fake.deliverCatalog(transcriptCatalog('audio-2'));
  expect(listener.transcript.lines).toEqual([]);
  // The new track's numbers start over: its segment 0 is not the old track's final.
  deliver(
    fake,
    TRANSCRIPT_TRACK,
    buildTranscriptSegment({ track: 'audio-2', seq: 0, final: true, fromUs: 2_000, untilUs: 2_500, text: 'new reply' }),
  );
  expect(listener.transcript.lines.map((line) => line.text)).toEqual(['new reply']);
  expect(onTranscript).toHaveBeenCalledTimes(2);
  await listener.stop();
});

it('keeps the old track when the replacement cannot be subscribed, and tries again on the next catalog', async () => {
  const fake = fakeSubscriber();
  const onError = vi.fn();
  const onTrackReplaced = vi.fn();
  const listening = listenAudio(fake.subscriber, NAMESPACE, { onError, onTrackReplaced });
  fake.deliverCatalog(audioCatalog());
  const listener = await listening;
  const subscribe = fake.subscriber.subscribe as unknown as ReturnType<typeof vi.fn>;
  const original = subscribe.getMockImplementation() as (request: Request, handlers: Handlers) => unknown;
  const failure = new Error('The relay is gone');
  subscribe.mockImplementationOnce(() => {
    throw failure;
  });
  const cut = vi.spyOn(listener.input, 'cut');
  fake.deliverCatalog(replacementCatalog('audio-2'));
  expect(onError).toHaveBeenCalledWith(failure);
  expect(listener.track).toBe('audio');
  expect(fake.subscriptions[1]?.cancel).not.toHaveBeenCalled();
  expect(cut).not.toHaveBeenCalled();
  expect(onTrackReplaced).not.toHaveBeenCalled();
  subscribe.mockImplementation(original);
  fake.deliverCatalog(replacementCatalog('audio-2'));
  expect(listener.track).toBe('audio-2');
  expect(fake.subscriptions.map(({ request }) => request.track)).toEqual(['catalog', 'audio', 'audio-2']);
  expect(fake.subscriptions[1]?.cancel).toHaveBeenCalledTimes(1);
  expect(cut).toHaveBeenCalledTimes(1);
  await listener.stop();
});

it('subscribes to a replacement again when the relay rejected its subscription, on the next catalog', async () => {
  const fake = fakeSubscriber();
  const onError = vi.fn();
  const onTrackReplaced = vi.fn();
  const listening = listenAudio(fake.subscriber, NAMESPACE, { onError, onTrackReplaced });
  fake.deliverCatalog(audioCatalog());
  const listener = await listening;
  const cut = vi.spyOn(listener.input, 'cut');
  const restart = vi.spyOn(listener.sequencer, 'restart');
  fake.deliverCatalog(replacementCatalog('audio-2'));
  expect(listener.track).toBe('audio-2');

  // An error of the replaced track's old subscription does not mark the current one failed.
  const stale = new Error('late error of the old track');
  failWith(fake.subscriptions[1], stale);
  expect(onError).toHaveBeenLastCalledWith(stale);
  fake.deliverCatalog(replacementCatalog('audio-2'));
  expect(fake.subscriptions).toHaveLength(3);

  // The relay rejects the replacement's subscription, after the old one was closed.
  const rejected = new Error('The relay rejected audio-2');
  failWith(fake.subscriptions[2], rejected);
  expect(onError).toHaveBeenLastCalledWith(rejected);
  expect(fake.subscriptions).toHaveLength(3);
  cut.mockClear();
  restart.mockClear();
  onTrackReplaced.mockClear();

  fake.deliverCatalog(replacementCatalog('audio-2'));
  expect(fake.subscriptions.map(({ request }) => request.track)).toEqual(['catalog', 'audio', 'audio-2', 'audio-2']);
  expect(fake.subscriptions[2]?.cancel).toHaveBeenCalledTimes(1);
  expect(cut).toHaveBeenCalledTimes(1);
  expect(restart).toHaveBeenCalledTimes(1);
  expect(listener.track).toBe('audio-2');
  // It is the same track, not a replacement.
  expect(onTrackReplaced).not.toHaveBeenCalled();

  // The new subscription is healthy: a repeated catalog changes nothing.
  fake.deliverCatalog(replacementCatalog('audio-2'));
  expect(fake.subscriptions).toHaveLength(4);

  // A failure of the newest subscription marks it failed, but nothing runs after stop().
  failWith(fake.subscriptions[3], rejected);
  await listener.stop();
  fake.deliverCatalog(replacementCatalog('audio-2'));
  expect(fake.subscriptions).toHaveLength(4);
});

it('reports an error of the catalog subscription after the listener started', async () => {
  const fake = fakeSubscriber();
  const onError = vi.fn();
  const listening = listenAudio(fake.subscriber, NAMESPACE, { onError });
  fake.deliverCatalog(audioCatalog());
  const listener = await listening;
  expect(onError).not.toHaveBeenCalled();
  const failure = new Error('The catalog subscription ended');
  failWith(fake.catalog, failure);
  expect(onError).toHaveBeenCalledTimes(1);
  expect(onError).toHaveBeenCalledWith(failure);
  await listener.stop();
});

it('reports an error of the catalog subscription that came while the listener was setting up', async () => {
  const fake = fakeSubscriber();
  const onError = vi.fn();
  const listening = listenAudio(fake.subscriber, NAMESPACE, { onError });
  fake.deliverCatalog(audioCatalog());
  const failure = new Error('The catalog subscription ended');
  failWith(fake.catalog, failure);
  failWith(fake.catalog, new Error('a second one is not kept'));
  const listener = await listening;
  expect(onError).toHaveBeenCalledTimes(1);
  expect(onError).toHaveBeenCalledWith(failure);
  await listener.stop();
});

it('reports a later catalog that does not parse while the listener was setting up', async () => {
  const fake = fakeSubscriber();
  const onError = vi.fn();
  const listening = listenAudio(fake.subscriber, NAMESPACE, { onError });
  fake.deliverCatalog(audioCatalog());
  // Still setting up (the player is not built yet): a second catalog arrives and does not parse.
  fake.deliverCatalog(new TextEncoder().encode('not json'));
  expect(onError).not.toHaveBeenCalled();
  const listener = await listening;
  expect(onError).toHaveBeenCalledTimes(1);
  expect(listener.track).toBe('audio');
  await listener.stop();
});

it('finishes a replacement when onTrackReplaced throws', async () => {
  const fake = fakeSubscriber();
  const error = vi.spyOn(console, 'error').mockImplementation(() => {});
  const listening = listenAudio(fake.subscriber, NAMESPACE, {
    onTrackReplaced: () => {
      throw new Error('the application failed');
    },
  });
  fake.deliverCatalog(audioCatalog());
  // A replacement during setup is followed once the listener starts; the callback's failure does not stop it.
  fake.deliverCatalog(replacementCatalog('audio-2'));
  const listener = await listening;
  expect(listener.track).toBe('audio-2');
  expect(fake.subscriptions[1]?.cancel).toHaveBeenCalledTimes(1);
  expect(error).toHaveBeenCalledTimes(1);
  await listener.stop();
  expect(fake.subscriptions[2]?.cancel).toHaveBeenCalledTimes(1);
});

it('keeps a report posted before the cut from re-anchoring the clock for the new track', async () => {
  FakeWorkletNode.instances.length = 0;
  const fake = fakeSubscriber();
  const onTrackReplaced = vi.fn();
  const listening = listenAudio(fake.subscriber, NAMESPACE, { onTrackReplaced });
  fake.deliverCatalog(audioCatalog());
  const listener = await listening;
  const node = FakeWorkletNode.instances[0] as FakeWorkletNode;
  // A worklet report: the old track playing, or (with the cut's id) the first quantum after the cut.
  const report = (frame: number, timestampUs: number | null, cutId: number | null) =>
    node.port.onmessage?.({
      data: {
        frame,
        rendered: frame + 128,
        timestampUs,
        playing: timestampUs !== null,
        playStarts: 1,
        heldUntilUs: null,
        dryAtUs: null,
        bufferedFrames: 0,
        targetFrames: 2880,
        underruns: 0,
        trimmedFrames: 0,
        concealedFrames: 0,
        cutFrames: 0,
        cutId,
        acceleratedFrames: 0,
        deceleratedFrames: 0,
        droppedRecoveredFrames: 0,
        droppedSynthesizedFrames: 0,
        droppedRedundantFrames: 0,
      },
    });
  report(0, 5_000_000, null);
  expect(listener.clock.mode).toBe('audio');
  fake.deliverCatalog(replacementCatalog('audio-2'));
  // The restart reset the clock; a report the worklet posted before it applied the cut leaves it so.
  expect(listener.clock.mode).toBe('idle');
  report(960, 5_020_000, null);
  expect(listener.clock.mode).toBe('idle');
  expect(onTrackReplaced.mock.calls.map(([event]) => event.phase)).toEqual(['seen']);
  // The report of the quantum after the cut: the old audio has stopped, and the new audio anchors the clock.
  report(1920, 9_000_000, 1);
  expect(onTrackReplaced.mock.calls.map(([event]) => event.phase)).toEqual(['seen', 'stopped', 'started']);
  expect(listener.clock.mode).toBe('audio');
  await listener.stop();
});

it('keeps a report posted before the cut from re-anchoring the clock when a failed subscription is retried', async () => {
  FakeWorkletNode.instances.length = 0;
  const fake = fakeSubscriber();
  const listening = listenAudio(fake.subscriber, NAMESPACE, { onError: () => {} });
  fake.deliverCatalog(audioCatalog());
  const listener = await listening;
  const node = FakeWorkletNode.instances[0] as FakeWorkletNode;
  const report = (frame: number, timestampUs: number | null, cutId: number | null) =>
    node.port.onmessage?.({
      data: {
        frame,
        rendered: frame + 128,
        timestampUs,
        playing: timestampUs !== null,
        playStarts: 1,
        heldUntilUs: null,
        dryAtUs: null,
        bufferedFrames: 0,
        targetFrames: 2880,
        underruns: 0,
        trimmedFrames: 0,
        concealedFrames: 0,
        cutFrames: 0,
        cutId,
      },
    });
  report(0, 5_000_000, null);
  expect(listener.clock.mode).toBe('audio');
  // The relay ends the audio subscription with an error; the next catalog that lists the track subscribes again.
  failWith(fake.subscriptions[1], new Error('The relay ended audio'));
  fake.deliverCatalog(audioCatalog());
  expect(fake.subscriptions.map(({ request }) => request.track)).toEqual(['catalog', 'audio', 'audio']);
  // The restart reset the clock; a report the worklet posted before it applied the cut leaves it so.
  expect(listener.clock.mode).toBe('idle');
  report(960, 5_020_000, null);
  expect(listener.clock.mode).toBe('idle');
  // The quantum after the cut: from here reports anchor the clock again.
  report(1920, 9_000_000, 1);
  expect(listener.clock.mode).toBe('audio');
  expect(listener.clock.position()).toBeGreaterThanOrEqual(9_000_000);
  await listener.stop();
});
