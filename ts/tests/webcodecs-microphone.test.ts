import { resolveObjectURL } from 'node:buffer';
import { createBroadcastClock } from '@mux/moq-media';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import {
  captureMicrophone,
  estimateCaptureLatencyUs,
  type MicrophoneBlock,
} from '../examples/webcodecs/capture-microphone.js';

class FakeNode {
  connect = vi.fn();
  disconnect = vi.fn();
  readonly stream: unknown;
  constructor(stream?: unknown) {
    this.stream = stream;
  }
}

const workletSources: string[] = [];
class FakeContext {
  destination = new FakeNode();
  audioWorklet = {
    addModule: vi.fn(async (url: string) => {
      const blob = resolveObjectURL(url);
      if (!blob) throw new Error('Not a blob URL');
      workletSources.push(await blob.text());
    }),
  };
  sources: FakeNode[] = [];
  currentTime = 0;
  readonly sampleRate: number;
  constructor(sampleRate = 48_000) {
    this.sampleRate = sampleRate;
  }
  createMediaStreamSource(stream: unknown) {
    const source = new FakeNode(stream);
    this.sources.push(source);
    return source;
  }
}

type WorkletMessage = { frame: number; samples: Float32Array<ArrayBuffer>; rendered: number };
const nodes: FakeWorkletNode[] = [];
class FakeWorkletNode extends FakeNode {
  port = {
    onmessage: null as ((event: { data: WorkletMessage }) => void) | null,
    postMessage: vi.fn(),
    close: vi.fn(),
  };
  readonly options: AudioWorkletNodeOptions;
  constructor(_context: FakeContext, _name: string, options: AudioWorkletNodeOptions) {
    super();
    this.options = options;
    nodes.push(this);
  }
  /** Deliver a block as the worklet would post it. */
  post(frame: number, rendered = frame + 960) {
    this.port.onmessage?.({ data: { frame, samples: new Float32Array(960), rendered } });
  }
}

let nowUs = 0;
// Pinned at epoch 0, so timestamps count from the origin and read as the test's own times.
const clock = () => createBroadcastClock({ now: () => nowUs, originUs: 0, originEpochUs: 0 });
const noLatency = () => 0;
const asContext = (context: FakeContext) => context as unknown as AudioContext;
const stream = (name: string) => ({ name }) as unknown as MediaStream;

beforeEach(() => {
  nowUs = 0;
  nodes.length = 0;
  workletSources.length = 0;
  vi.stubGlobal('AudioWorkletNode', FakeWorkletNode);
});
afterEach(() => {
  vi.unstubAllGlobals();
});

it('stamps each block with context time, calibrated from when blocks arrive', async () => {
  const context = new FakeContext();
  const capture = await captureMicrophone(clock(), stream('mic'), {
    context: asContext(context),
    captureLatencyUs: noLatency,
  });
  const blocks: MicrophoneBlock[] = [];
  capture.onblock = (block) => blocks.push(block);
  const node = nodes[0] as FakeWorkletNode;
  expect(node.options.processorOptions).toEqual({ blockFrames: 960 });
  expect(context.sources[0]?.connect).toHaveBeenCalledWith(node);
  // Rendered up to 20 ms of context time, observed at 50 ms: context 0 is broadcast 30 ms.
  nowUs = 50_000;
  node.post(0);
  expect(blocks[0]).toMatchObject({ contextUs: 0, sampleRate: 48_000 });
  expect(blocks[0]?.timeline.timestampUs(0)).toBe(30_000);
  // An observation that arrived 9 ms sooner after its render re-anchors the timeline.
  nowUs = 61_000;
  node.post(960);
  expect(blocks[1]?.contextUs).toBe(20_000);
  expect(blocks[1]?.timeline.timestampUs(20_000)).toBe(41_000);
  // A later, slower arrival does not move it.
  nowUs = 90_000;
  node.post(1920);
  expect(blocks[2]?.timeline.timestampUs(40_000)).toBe(61_000);
  expect(new Set(blocks.map((block) => block.timeline)).size).toBe(1);
});

it('switches microphones in the same context and replaces the context with a fresh timeline', async () => {
  const first = new FakeContext();
  const capture = await captureMicrophone(clock(), stream('laptop'), {
    context: asContext(first),
    captureLatencyUs: noLatency,
  });
  const blocks: MicrophoneBlock[] = [];
  capture.onblock = (block) => blocks.push(block);
  const [laptop] = first.sources;
  capture.useStream(stream('earbuds'));
  expect(laptop?.disconnect).toHaveBeenCalled();
  expect(first.sources[1]?.stream).toEqual({ name: 'earbuds' });
  expect(first.sources[1]?.connect).toHaveBeenCalledWith(nodes[0]);
  capture.useStream(undefined);
  expect(first.sources).toHaveLength(2);
  (nodes[0] as FakeWorkletNode).post(0);

  const second = new FakeContext(44_100);
  capture.useStream(stream('earbuds'));
  await capture.useContext(asContext(second));
  expect(capture.context).toBe(second);
  expect((nodes[0] as FakeWorkletNode).port.onmessage).toBeNull();
  // The processor returns true from process() until told to stop; a disconnected node alone keeps running.
  expect((nodes[0] as FakeWorkletNode).port.postMessage).toHaveBeenCalledWith('stop');
  expect((nodes[0] as FakeWorkletNode).port.close).toHaveBeenCalled();
  expect((nodes[0] as FakeWorkletNode).disconnect).toHaveBeenCalled();
  expect(nodes[1]?.options.processorOptions).toEqual({ blockFrames: 882 });
  expect(second.sources[0]?.stream).toEqual({ name: 'earbuds' });
  (nodes[1] as FakeWorkletNode).post(0, 882);
  expect(blocks[1]?.sampleRate).toBe(44_100);
  expect(blocks[1]?.timeline).not.toBe(blocks[0]?.timeline);

  // A second capture on the same context reuses the loaded processor: registering it twice would throw.
  await captureMicrophone(clock(), undefined, { context: asContext(second) });
  expect(second.audioWorklet.addModule).toHaveBeenCalledTimes(1);
  capture.close();
  expect((nodes[1] as FakeWorkletNode).port.onmessage).toBeNull();
  expect(() => capture.useStream(undefined)).toThrow('closed');
});

it('subtracts the capture latency of each microphone, asked again on every switch', async () => {
  const latencies: Record<string, number> = { laptop: 30_000, earbuds: 190_000 };
  const asked: (string | undefined)[] = [];
  const context = new FakeContext();
  const capture = await captureMicrophone(clock(), stream('laptop'), {
    context: asContext(context),
    captureLatencyUs: (current) => {
      const name = (current as unknown as { name: string } | undefined)?.name;
      asked.push(name);
      return name ? (latencies[name] ?? 0) : 0;
    },
  });
  const blocks: MicrophoneBlock[] = [];
  capture.onblock = (block) => blocks.push(block);
  const node = nodes[0] as FakeWorkletNode;
  // Rendered up to 20 ms of context time, observed at 50 ms: context 0 is broadcast 30 ms, less the laptop 30 ms.
  nowUs = 50_000;
  node.post(0);
  expect(blocks[0]?.timeline.timestampUs(0)).toBe(0);
  expect(blocks[0]?.captureLatencyUs).toBe(30_000);
  // Switched once the context has rendered 40 ms: a laptop block still queued keeps the laptop latency.
  context.currentTime = 0.04;
  capture.useStream(stream('earbuds'));
  expect(blocks[0]?.timeline.captureLatencyUs).toBe(190_000);
  expect(blocks[0]?.timeline.timestampUs(200_000)).toBe(40_000);
  node.post(960);
  node.post(1920);
  expect(blocks.slice(1).map((block) => block.captureLatencyUs)).toEqual([30_000, 190_000]);
  capture.useStream(undefined);
  expect(blocks[0]?.timeline.captureLatencyUs).toBe(0);
  expect(asked).toEqual(['laptop', 'earbuds', undefined]);
});

it('estimates capture latency from the microphone label', () => {
  const labelled = (label: string) => ({ getAudioTracks: () => [{ label }] }) as unknown as MediaStream;
  expect(estimateCaptureLatencyUs(labelled('MacBook Air Microphone (Built-in)'))).toBe(30_000);
  expect(estimateCaptureLatencyUs(labelled('AirPods'))).toBe(190_000);
  expect(estimateCaptureLatencyUs(labelled('Jabra Evolve2 (Bluetooth)'))).toBe(190_000);
  expect(estimateCaptureLatencyUs(labelled(''))).toBe(0);
  expect(estimateCaptureLatencyUs({ getAudioTracks: () => [] } as unknown as MediaStream)).toBe(0);
  expect(estimateCaptureLatencyUs(undefined)).toBe(0);
});

it('posts contiguous blocks only, starting over where currentFrame jumps', async () => {
  await captureMicrophone(clock(), undefined, { context: asContext(new FakeContext()) });
  let Processor: (new (options: unknown) => { process(inputs: Float32Array[][]): boolean }) | undefined;
  class Base {
    port: { postMessage: ReturnType<typeof vi.fn>; onmessage?: () => void } = { postMessage: vi.fn() };
  }
  new Function('AudioWorkletProcessor', 'registerProcessor', workletSources[0] as string)(
    Base,
    (_name: string, processor: typeof Processor) => {
      Processor = processor;
    },
  );
  if (!Processor) throw new Error('The worklet registered no processor');
  const processor = new Processor({ processorOptions: { blockFrames: 256 } }) as unknown as {
    process(inputs: Float32Array[][]): boolean;
    port: { postMessage: ReturnType<typeof vi.fn>; onmessage?: () => void };
  };
  const run = (frame: number, value?: number) => {
    vi.stubGlobal('currentFrame', frame);
    return processor.process([value === undefined ? [] : [new Float32Array(128).fill(value)]]);
  };
  // Chromium's first call reports frame 0; the next ones the real frame. The partial block is dropped.
  expect(run(0, 0.1)).toBe(true);
  run(640, 0.2);
  run(768, 0.3);
  // No microphone: silence, and the count goes on.
  run(896);
  run(1024, 0.4);
  // A skipped stretch starts a new block there.
  run(2048, 0.5);
  run(2176, 0.6);
  const posted = processor.port.postMessage.mock.calls.map(([message]) => message as WorkletMessage);
  expect(posted.map(({ frame, rendered }) => [frame, rendered])).toEqual([
    [640, 896],
    [896, 1152],
    [2048, 2304],
  ]);
  expect(posted[0]?.samples.slice(127, 129)).toEqual(Float32Array.of(0.2, 0.3));
  expect(posted[1]?.samples.slice(127, 129)).toEqual(Float32Array.of(0, 0.4));
  // Closing capture stops the processor, so the engine can drop it.
  processor.port.onmessage?.();
  expect(run(2304, 0.7)).toBe(false);
  expect(processor.port.postMessage).toHaveBeenCalledTimes(3);
});
