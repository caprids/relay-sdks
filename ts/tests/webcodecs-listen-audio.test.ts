import { resolveObjectURL } from 'node:buffer';
import { buildInterrupt, buildMsfCatalog, INTERRUPT_TRACK } from '@mux/moq-media';
import type { RelaySubscriber } from '@mux/relay';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { listenAudio } from '../examples/webcodecs/listen-audio.js';

const NAMESPACE = ['room', 'mic'];

class FakeNode {
  gain = { value: 1 };
  connect = vi.fn();
  disconnect = vi.fn();
}

class FakeWorkletNode extends FakeNode {
  port = { onmessage: null, postMessage: vi.fn(), close: vi.fn() };
}

class FakeContext {
  static instances: FakeContext[] = [];
  readonly sampleRate = 48_000;
  readonly baseLatency = 0;
  readonly outputLatency = 0;
  destination = new FakeNode();
  close = vi.fn(async () => {});
  audioWorklet = {
    addModule: vi.fn(async (url: string) => {
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
  expect(fake.subscriptions).toHaveLength(1);
});

it('closes the context and cancels the catalog when the audio player cannot load', async () => {
  const fake = fakeSubscriber();
  const failure = new Error('The worklet module failed to load');
  const listening = listen(fake, audioCatalog());
  // The context exists as soon as listenAudio has started; make its worklet fail before the player asks.
  (FakeContext.instances[0] as FakeContext).audioWorklet.addModule.mockRejectedValue(failure);
  await expect(listening).rejects.toBe(failure);
  expect(FakeContext.instances[0]?.close).toHaveBeenCalledTimes(1);
  expect(fake.catalog.cancel).toHaveBeenCalledTimes(1);
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
