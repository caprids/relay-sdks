import type { MoqtObject } from '@caprid/moq-core';
import { buildMsfCatalog, createPlayoutClock, LOC_TIMESTAMP } from '@caprid/moq-media';
import type { RelaySubscriber } from '@caprid/relay';
import type { FrameReader } from '@caprid/relay/media';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { watchVideo } from '../examples/webcodecs/watch-video.js';

const NAMESPACE = ['room', 'camera'];
const AVCC = new Uint8Array([1, 0x42, 0xe0, 0x1f, 0xff]);
const FRAME_US = 33_333;

class FakeFrame {
  static closed = 0;
  displayWidth = 640;
  displayHeight = 360;
  readonly timestamp: number;
  constructor(timestamp: number) {
    this.timestamp = timestamp;
  }
  close() {
    FakeFrame.closed++;
  }
}

interface Chunk {
  type: 'key' | 'delta';
  timestamp: number;
}

class FakeDecoder {
  static instances: FakeDecoder[] = [];
  state = 'unconfigured';
  configs: VideoDecoderConfig[] = [];
  chunks: Chunk[] = [];
  resets = 0;
  readonly init: VideoDecoderInit;
  constructor(init: VideoDecoderInit) {
    this.init = init;
    FakeDecoder.instances.push(this);
  }
  configure(config: VideoDecoderConfig) {
    this.configs.push(config);
    this.state = 'configured';
  }
  decode(chunk: Chunk) {
    this.chunks.push(chunk);
  }
  reset() {
    this.resets++;
    this.state = 'unconfigured';
  }
  close() {
    this.state = 'closed';
  }
  /** Hand a decoded frame to the watcher, as the decoder's output callback. */
  emit(timestampUs: number) {
    this.init.output(new FakeFrame(timestampUs) as unknown as VideoFrame);
  }
}

type Handlers = NonNullable<Parameters<RelaySubscriber['subscribe']>[1]>;
type Request = { namespace: string[]; track: string };

function fakeSubscriber() {
  const subscriptions: { request: Request; handlers: Handlers; cancel: ReturnType<typeof vi.fn> }[] = [];
  const subscriber = {
    subscribe: vi.fn((request: Request, handlers: Handlers) => {
      const subscription = { request, handlers, cancel: vi.fn() };
      subscriptions.push(subscription);
      return subscription;
    }),
  };
  const byTrack = (track: string) => {
    const found = subscriptions.find((subscription) => subscription.request.track === track);
    if (!found) throw new Error(`No subscription to ${track}`);
    return found;
  };
  return {
    subscriber: subscriber as unknown as RelaySubscriber,
    byTrack,
    deliverCatalog(payload = videoCatalog()) {
      byTrack('catalog').handlers.onObject?.({ status: 'normal', payload } as unknown as MoqtObject);
    },
    /** Deliver one video object; object 0 of a group is its keyframe. */
    deliverVideo(groupId: number, objectId: number) {
      byTrack('video').handlers.onObject?.({
        groupId,
        subgroupId: 0,
        objectId,
        status: 'normal',
        properties: [{ type: LOC_TIMESTAMP, value: timestampOf(groupId, objectId) }],
        payload: new Uint8Array([groupId, objectId]),
      } as unknown as MoqtObject);
    },
    endGroup(groupId: number) {
      byTrack('video').handlers.onSubgroupEnd?.({ groupId } as never);
    },
  };
}

/** Thirty frames per group, one group a second, from 10 s on the timeline. */
const timestampOf = (groupId: number, objectId: number) => 10_000_000 + groupId * 30 * FRAME_US + objectId * FRAME_US;

const videoCatalog = () =>
  buildMsfCatalog({
    namespace: NAMESPACE,
    video: { codec: 'avc1.42E01F', width: 640, height: 360, framerate: 30, initData: AVCC },
  });

function fakeCanvas() {
  const drawn: number[] = [];
  const canvas = {
    width: 300,
    height: 150,
    getContext: () => ({ drawImage: (frame: FakeFrame) => drawn.push(frame.timestamp) }),
  };
  return { canvas: canvas as unknown as HTMLCanvasElement, drawn };
}

let nowUs = 0;
const clockAtNow = () => createPlayoutClock({ now: () => nowUs });

beforeEach(() => {
  vi.useFakeTimers();
  nowUs = 0;
  FakeFrame.closed = 0;
  FakeDecoder.instances.length = 0;
  vi.stubGlobal('VideoDecoder', FakeDecoder);
  vi.stubGlobal(
    'EncodedVideoChunk',
    class {
      constructor(init: Chunk) {
        Object.assign(this, init);
      }
    },
  );
});
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

const decoder = () => FakeDecoder.instances.at(-1) as FakeDecoder;
const decoded = () => decoder().chunks.map((chunk) => [chunk.type, chunk.timestamp]);

it('configures the decoder from the catalog: codec, the avcC as description, and low latency', async () => {
  const fake = fakeSubscriber();
  const watching = watchVideo(fake.subscriber, NAMESPACE, fakeCanvas().canvas, { clock: clockAtNow() });
  fake.deliverCatalog();
  const watcher = await watching;
  expect(decoder().configs).toEqual([{ codec: 'avc1.42E01F', description: AVCC, optimizeForLatency: true }]);
  await watcher.stop();
});

it('subscribes to video with the catalog and decodes what came before it, in order, once configured', async () => {
  const fake = fakeSubscriber();
  const watching = watchVideo(fake.subscriber, NAMESPACE, fakeCanvas().canvas, { clock: clockAtNow() });
  expect(fake.byTrack('video').request).toMatchObject({ track: 'video', parameters: { subscriberPriority: 96 } });
  for (const objectId of [0, 1, 2]) fake.deliverVideo(0, objectId);
  expect(FakeDecoder.instances).toHaveLength(0);
  fake.deliverCatalog();
  const watcher = await watching;
  fake.deliverVideo(0, 3);
  expect(decoded()).toEqual([
    ['key', timestampOf(0, 0)],
    ['delta', timestampOf(0, 1)],
    ['delta', timestampOf(0, 2)],
    ['delta', timestampOf(0, 3)],
  ]);
  await watcher.stop();
});

it('decodes nothing until a keyframe after a discontinuity, and resets the decoder there', async () => {
  const fake = fakeSubscriber();
  const watching = watchVideo(fake.subscriber, NAMESPACE, fakeCanvas().canvas, { clock: clockAtNow() });
  fake.deliverCatalog();
  const watcher = await watching;
  fake.deliverVideo(0, 0);
  fake.deliverVideo(0, 1);
  // Group 0 is cut short; group 1 follows after a gap.
  fake.byTrack('video').handlers.onSubgroupReset?.({ groupId: 0 } as never);
  fake.deliverVideo(1, 0);
  fake.deliverVideo(1, 1);
  expect(decoder().resets).toBe(1);
  expect(decoder().configs).toHaveLength(2);
  expect(decoded()).toEqual([
    ['key', timestampOf(0, 0)],
    ['delta', timestampOf(0, 1)],
    ['key', timestampOf(1, 0)],
    ['delta', timestampOf(1, 1)],
  ]);
  expect(watcher.stats.discontinuities).toBe(1);
  // A restart joins the replacement subscription mid-group: its deltas wait for the next keyframe.
  (fake.byTrack('video').handlers as unknown as FrameReader).onRestarted();
  fake.deliverVideo(5, 2);
  fake.deliverVideo(5, 3);
  fake.endGroup(5);
  fake.deliverVideo(6, 0);
  expect(decoder().resets).toBe(2);
  expect(decoded().slice(4)).toEqual([['key', timestampOf(6, 0)]]);
  expect(watcher.stats.discontinuities).toBe(2);
  await watcher.stop();
});

it('draws each decoded frame when the clock says it is due, and drops frames audio has passed', async () => {
  const fake = fakeSubscriber();
  const clock = clockAtNow();
  const { canvas, drawn } = fakeCanvas();
  const watching = watchVideo(fake.subscriber, NAMESPACE, canvas, { clock });
  fake.deliverCatalog();
  const watcher = await watching;
  clock.reportAudio(timestampOf(0, 1));
  decoder().emit(timestampOf(0, 0));
  decoder().emit(timestampOf(0, 1));
  decoder().emit(timestampOf(0, 2));
  // Audio is at frame 1: frame 0 is 33 ms behind, inside the late-drop limit, so it is shown; frame 2 waits.
  expect(drawn).toEqual([timestampOf(0, 0), timestampOf(0, 1)]);
  expect(canvas.width).toBe(640);
  nowUs += FRAME_US;
  clock.reportAudio(timestampOf(0, 2));
  await vi.advanceTimersByTimeAsync(FRAME_US / 1000);
  expect(drawn).toEqual([timestampOf(0, 0), timestampOf(0, 1), timestampOf(0, 2)]);
  // Audio jumps 100 ms ahead: the next frame is too late to show.
  nowUs += 1000;
  clock.reportAudio(timestampOf(0, 6));
  decoder().emit(timestampOf(0, 3));
  expect(watcher.stats).toMatchObject({ shown: 3, late: 1, overflowed: 0 });
  expect(FakeFrame.closed).toBe(4);
  await watcher.stop();
});

it('keeps at most four decoded frames waiting and drops the oldest past that', async () => {
  const fake = fakeSubscriber();
  const clock = clockAtNow();
  const { canvas, drawn } = fakeCanvas();
  const watching = watchVideo(fake.subscriber, NAMESPACE, canvas, { clock });
  fake.deliverCatalog();
  const watcher = await watching;
  // Audio is half a second behind every frame, and no timer runs, as in a hidden tab.
  clock.reportAudio(timestampOf(0, 0) - 500_000);
  for (let objectId = 0; objectId < 10; objectId++) decoder().emit(timestampOf(0, objectId));
  expect(drawn).toEqual([]);
  expect(watcher.stats.overflowed).toBe(6);
  expect(FakeFrame.closed).toBe(6);
  // The four newest are what is left to draw.
  nowUs += 500_000;
  clock.reportAudio(timestampOf(0, 6));
  for (let step = 0; step < 4; step++) {
    await vi.advanceTimersByTimeAsync(FRAME_US / 1000);
    nowUs += FRAME_US;
  }
  expect(drawn).toEqual([6, 7, 8, 9].map((objectId) => timestampOf(0, objectId)));
  await watcher.stop();
});

it('stop() cancels the video subscription, closes waiting frames and the decoder, and leaves the clock', async () => {
  const fake = fakeSubscriber();
  const clock = clockAtNow();
  const watching = watchVideo(fake.subscriber, NAMESPACE, fakeCanvas().canvas, { clock });
  fake.deliverCatalog();
  const watcher = await watching;
  // The catalog is read once.
  expect(fake.byTrack('catalog').cancel).toHaveBeenCalledTimes(1);
  clock.reportAudio(timestampOf(0, 0) - 500_000);
  decoder().emit(timestampOf(0, 0));
  await watcher.stop();
  expect(fake.byTrack('video').cancel).toHaveBeenCalledTimes(1);
  expect(FakeFrame.closed).toBe(1);
  expect(decoder().state).toBe('closed');
  expect(clock.mode).toBe('audio');
});

it('cancels both subscriptions and rejects when aborted before the catalog arrives', async () => {
  const fake = fakeSubscriber();
  const abort = new AbortController();
  const watching = watchVideo(fake.subscriber, NAMESPACE, fakeCanvas().canvas, { signal: abort.signal });
  const reason = new Error('Gave up');
  abort.abort(reason);
  await expect(watching).rejects.toBe(reason);
  expect(fake.byTrack('catalog').cancel).toHaveBeenCalledTimes(1);
  expect(fake.byTrack('video').cancel).toHaveBeenCalledTimes(1);
  expect(FakeDecoder.instances).toHaveLength(0);
});

it('rejects when the catalog lists no video track, after cancelling both subscriptions', async () => {
  const fake = fakeSubscriber();
  const watching = watchVideo(fake.subscriber, NAMESPACE, fakeCanvas().canvas);
  watching.catch(() => {});
  fake.deliverCatalog(buildMsfCatalog({ namespace: NAMESPACE, audio: {} }));
  await expect(watching).rejects.toThrow(/lists no video track/);
  expect(fake.byTrack('catalog').cancel).toHaveBeenCalledTimes(1);
  expect(fake.byTrack('video').cancel).toHaveBeenCalledTimes(1);
});

it('reports a decoder failure to onError and decodes again from the next keyframe with a new decoder', async () => {
  const fake = fakeSubscriber();
  const onError = vi.fn();
  const watching = watchVideo(fake.subscriber, NAMESPACE, fakeCanvas().canvas, { clock: clockAtNow(), onError });
  fake.deliverCatalog();
  const watcher = await watching;
  fake.deliverVideo(0, 0);
  const failure = new Error('decode failed');
  decoder().init.error(failure as DOMException);
  expect(onError).toHaveBeenCalledWith(failure);
  fake.deliverVideo(0, 1);
  fake.endGroup(0);
  fake.deliverVideo(1, 0);
  expect(FakeDecoder.instances).toHaveLength(2);
  expect(decoded()).toEqual([['key', timestampOf(1, 0)]]);
  await watcher.stop();
});
