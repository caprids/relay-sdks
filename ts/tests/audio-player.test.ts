import { resolveObjectURL } from 'node:buffer';
import type { MoqtObject } from '@mux/moq-core';
import {
  AUDIO_ONLY_TARGET_LATENCY_US,
  type AudioInputClock,
  type AudioInputStats,
  type AudioPlayer,
  type AudioPlayerInput,
  audioDecoderConfigFromTrack,
  createAudioPlayer,
  createEchoCancellationLoopback,
  createFrameSequencer,
  createPlayoutClock,
  LOC_TIMESTAMP,
  opusPacketDurationUs,
  type PlayoutClock,
} from '@mux/moq-media';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createFrameReader } from '../src/frame-reader.js';

// A fake Web Audio graph that runs the player's real worklet source: the context renders 128-frame quanta through each
// processor, and ports deliver messages synchronously. Time on the clock's source is the context's time plus `lagUs`,
// as if every report reached the main thread that long after its render.
const RATE = 48_000;
const QUANTUM = 128;
const FRAME_US = 20_000;
const FRAME_SAMPLES = 960;

type Processor = {
  port: { onmessage: ((event: { data: unknown }) => void) | null; postMessage(message: unknown): void };
  process(inputs: Float32Array[][], outputs: Float32Array[][]): boolean;
};
type ProcessorClass = new (options: unknown) => Processor;

class FakeGain {
  gain = { value: 1 };
  targets = new Set<unknown>();
  connect(destination: unknown) {
    this.targets.add(destination);
    return destination;
  }
  disconnect = vi.fn((destination?: unknown) => {
    if (destination === undefined) this.targets.clear();
    else this.targets.delete(destination);
  });
}

let nowUs = 0;
let lagUs = 0;

class FakeContext {
  sampleRate: number;
  baseLatency = 0;
  outputLatency = 0;
  frame = 0;
  renderedMs = 0;
  processors = new Map<string, ProcessorClass>();
  nodes: FakeWorkletNode[] = [];
  audioWorklet = {
    addModule: vi.fn(async (url: string) => {
      const blob = resolveObjectURL(url);
      if (!blob) throw new Error('Not a blob URL');
      class Base {
        port = {
          onmessage: null as ((event: { data: unknown }) => void) | null,
          postMessage: (_message: unknown) => {},
        };
      }
      vi.stubGlobal('sampleRate', this.sampleRate);
      new Function('AudioWorkletProcessor', 'registerProcessor', await blob.text())(
        Base,
        (name: string, processor: ProcessorClass) => this.processors.set(name, processor),
      );
    }),
  };
  constructor(sampleRate = RATE) {
    this.sampleRate = sampleRate;
  }
  get currentTime() {
    return this.frame / this.sampleRate;
  }
  createGain() {
    return new FakeGain();
  }
  /** Render `ms` of audio; returns each live node's output, first channel. */
  render(ms: number): Map<FakeWorkletNode, number[]> {
    const outputs = new Map<FakeWorkletNode, number[]>();
    // Quanta do not divide 20 ms: render on a running total, so a frame of audio per 20 ms keeps pace.
    this.renderedMs += ms;
    const until = Math.round((this.renderedMs * this.sampleRate) / 1000);
    while (this.frame + QUANTUM <= until) {
      vi.stubGlobal('currentFrame', this.frame);
      vi.stubGlobal('sampleRate', this.sampleRate);
      // A report arrives once its quantum has rendered.
      nowUs = Math.round(((this.frame + QUANTUM) * 1e6) / this.sampleRate) + lagUs;
      for (const node of this.nodes) {
        if (!node.alive) continue;
        const channels = Array.from({ length: node.channels }, () => new Float32Array(QUANTUM));
        node.alive = node.processor.process([], [channels]);
        if (!node.alive) continue;
        const samples = outputs.get(node) ?? [];
        outputs.set(node, samples);
        samples.push(...(channels[0] as Float32Array));
      }
      this.frame += QUANTUM;
    }
    nowUs = Math.round((this.frame * 1e6) / this.sampleRate) + lagUs;
    return outputs;
  }
}

class FakeWorkletNode extends FakeGain {
  processor: Processor;
  channels: number;
  alive = true;
  port: {
    onmessage: ((event: { data: unknown }) => void) | null;
    postMessage(message: unknown): void;
    close(): void;
  };
  constructor(context: FakeContext, name: string, options: { outputChannelCount: number[] }) {
    super();
    const Processor = context.processors.get(name);
    if (!Processor) throw new Error(`No processor ${name}`);
    this.processor = new Processor(options);
    this.channels = options.outputChannelCount[0] as number;
    this.port = {
      onmessage: null,
      postMessage: (message) => this.processor.port.onmessage?.({ data: message }),
      close: vi.fn(),
    };
    this.processor.port.postMessage = (message) => this.port.onmessage?.({ data: message });
    context.nodes.push(this);
  }
}

// An Opus packet's TOC byte for one 20 ms CELT frame, then a marker the fake decoder turns into the sample value.
const OPUS_20MS = 31 << 3;
const packet = (marker: number) => new Uint8Array([OPUS_20MS, marker]);

/** Like Chromium's Opus decoder: output timestamps count on from the first input, whatever the input says. */
class FakeDecoder {
  static instances: FakeDecoder[] = [];
  static hold = false;
  /** A marker whose frame `decode()` throws on. */
  static failOn: number | undefined;
  /** Output a ramp that rises `RAMP` per sample of the timeline from `BASE_US`, instead of the marker. */
  static ramp = false;
  state = 'unconfigured';
  config: AudioDecoderConfig | undefined;
  nextUs: number | undefined;
  held: (() => void)[] = [];
  readonly init: AudioDecoderInit;
  constructor(init: AudioDecoderInit) {
    this.init = init;
    FakeDecoder.instances.push(this);
  }
  configure(config: AudioDecoderConfig) {
    this.config = config;
    this.state = 'configured';
  }
  decode(chunk: { timestamp: number; data: Uint8Array }) {
    if (this.state !== 'configured') throw new Error('Not configured');
    if (chunk.data[1] === FakeDecoder.failOn) throw new DOMException('Bad packet', 'EncodingError');
    const timestamp = this.nextUs ?? chunk.timestamp;
    this.nextUs = timestamp + FRAME_US;
    const value = (chunk.data[1] ?? 0) / 100;
    const output = () => {
      if (this.state === 'closed') return;
      this.init.output({
        timestamp,
        duration: FRAME_US,
        sampleRate: RATE,
        numberOfFrames: FRAME_SAMPLES,
        numberOfChannels: 1,
        copyTo(destination: Float32Array) {
          if (!FakeDecoder.ramp) destination.fill(value);
          else {
            const start = ((timestamp - BASE_US) * RATE) / 1e6;
            for (let index = 0; index < destination.length; index++) destination[index] = (start + index) * RAMP;
          }
        },
        close() {},
      } as unknown as AudioData);
    };
    if (FakeDecoder.hold) this.held.push(output);
    else output();
  }
  release() {
    for (const output of this.held.splice(0)) output();
  }
  /** Like a real decoder's: everything it holds is output, a task later, and then the promise resolves. */
  flush() {
    if (this.state !== 'configured') return Promise.reject(new DOMException('Not configured', 'InvalidStateError'));
    return new Promise<void>((resolve) =>
      setTimeout(() => {
        this.release();
        resolve();
      }),
    );
  }
  close() {
    this.state = 'closed';
  }
}

class FakeChunk {
  readonly type: string;
  readonly timestamp: number;
  readonly data: Uint8Array;
  constructor(init: { type: string; timestamp: number; data: Uint8Array }) {
    this.type = init.type;
    this.timestamp = init.timestamp;
    this.data = init.data;
  }
}

beforeEach(() => {
  nowUs = 0;
  lagUs = 0;
  FakeDecoder.instances = [];
  FakeDecoder.hold = false;
  FakeDecoder.failOn = undefined;
  FakeDecoder.ramp = false;
  vi.stubGlobal('AudioWorkletNode', FakeWorkletNode);
  vi.stubGlobal('AudioDecoder', FakeDecoder);
  vi.stubGlobal('EncodedAudioChunk', FakeChunk);
});
afterEach(() => {
  vi.unstubAllGlobals();
});

const BASE_US = 1_000_000_000;
const RAMP = 1e-5;
const frameAt = (index: number, discontinuity?: 'gap' | 'resync' | 'restart') => ({
  timestampUs: BASE_US + index * FRAME_US,
  payload: packet(index % 100),
  ...(discontinuity ? { discontinuity } : {}),
});

// The fake decoder's payloads are markers, not Opus: these tests decode with WebCodecs (the stand-in), so they turn
// loss concealment, which decodes Opus itself, off. The concealment tests below play real Opus packets.
async function setUp(options: { sampleRate?: number; lossConcealment?: boolean } = {}) {
  const context = new FakeContext(options.sampleRate);
  const clock = createPlayoutClock({ now: () => nowUs });
  const player = await createAudioPlayer({ context, lossConcealment: options.lossConcealment ?? false });
  return { context, clock, player };
}

/** Let a decoder replaced at a gap or a timestamp jump finish, so the next one starts. */
const settle = () => new Promise<void>((resolve) => setTimeout(resolve));
const nodeOf = (context: FakeContext, index = 0) => context.nodes[index] as FakeWorkletNode;
/** The distinct non-silent markers in an output, in order. */
const markers = (samples: number[]) => {
  const seen: number[] = [];
  for (const sample of samples) {
    const marker = Math.round(sample * 100);
    if (sample !== 0 && seen[seen.length - 1] !== marker) seen.push(marker);
  }
  return seen;
};

describe('createAudioPlayer', () => {
  it('joins: holds the jitter target, then plays in order and reports the audible timestamp', async () => {
    const { context, clock, player } = await setUp();
    expect(context.audioWorklet.addModule).toHaveBeenCalledTimes(1);
    const input = player.addInput({ clock });
    expect(nodeOf(context).targets.size).toBe(1);
    expect(player.inputs).toEqual([input]);
    // Two frames, 40 ms: below the 60 ms target, so silence.
    input.push(frameAt(1));
    input.push(frameAt(2));
    expect(markers(context.render(40).get(nodeOf(context)) ?? [])).toEqual([]);
    expect(input.stats).toMatchObject({ playing: false, bufferedUs: 40_000, jitterTargetUs: 60_000 });
    // The third reaches the target.
    input.push(frameAt(3));
    lagUs = 2_000;
    const played = context.render(60).get(nodeOf(context)) ?? [];
    expect(markers(played)).toEqual([1, 2, 3]);
    expect(input.stats.decoded).toBe(3);
    expect(input.stats.reports).toBeGreaterThan(0);
    expect(clock.mode).toBe('audio');
    // The clock's position is what is playing: the first frame started at context frame 1920 (40 ms).
    const playedUs = nowUs - lagUs - 40_000;
    const position = clock.position() as number;
    expect(Math.abs(position - (BASE_US + FRAME_US + playedUs))).toBeLessThanOrEqual(3_000);
  });

  it('reports the output delay: the clock trails the render by base plus output latency', async () => {
    const { context, clock, player } = await setUp();
    context.baseLatency = 0.01;
    context.outputLatency = 0.03;
    const input = player.addInput({ clock });
    for (let index = 1; index <= 6; index++) input.push(frameAt(index));
    context.render(60);
    const renderedUs = BASE_US + FRAME_US + (nowUs - 0);
    expect(Math.abs((clock.position() as number) - (renderedUs - 40_000))).toBeLessThanOrEqual(3_000);
  });

  it('reorders through a frame reader: a frame later than one delivered is dropped and its slot is silence', async () => {
    const { context, clock, player } = await setUp();
    const input = player.addInput({ clock });
    const sequencer = createFrameSequencer({ clock, grouping: 'frame' });
    const reader = createFrameReader(sequencer, { onFrame: input.push });
    const object = (index: number): MoqtObject => ({
      groupId: index,
      objectId: 0,
      subgroupId: 0,
      status: 'normal',
      properties: [{ type: LOC_TIMESTAMP, value: BASE_US + index * FRAME_US }],
      payload: packet(index),
    });
    for (const index of [1, 2, 4, 3, 5, 6]) reader.onObject(object(index));
    reader.close();
    expect(sequencer.stats.late).toBe(1);
    await settle();
    const played = context.render(160).get(nodeOf(context)) ?? [];
    expect(markers(played)).toEqual([1, 2, 4, 5, 6]);
    // Frame 3's 20 ms are silence, so frame 4 still starts 40 ms after frame 2.
    const start = played.findIndex((sample) => sample !== 0);
    expect(played.slice(start + 2 * FRAME_SAMPLES, start + 3 * FRAME_SAMPLES).every((sample) => sample === 0)).toBe(
      true,
    );
    expect(Math.round((played[start + 3 * FRAME_SAMPLES] as number) * 100)).toBe(4);
    expect(input.stats.concealedUs).toBe(20_000);
    expect(input.stats.decoderResets).toBe(1);
  });

  it('lets a decoder replaced at a gap output what it still holds, then decodes on in order', async () => {
    const { context, player } = await setUp();
    const input = player.addInput();
    // One poll of a frame reader: frames 1 to 3, then frame 5 after a gap, before the decoder has output anything.
    FakeDecoder.hold = true;
    for (let index = 1; index <= 3; index++) input.push(frameAt(index));
    input.push(frameAt(5, 'gap'));
    input.push(frameAt(6));
    FakeDecoder.hold = false;
    await settle();
    for (const decoder of FakeDecoder.instances) decoder.release();
    expect(input.stats).toMatchObject({ decoded: 5, decoderResets: 1 });
    expect(FakeDecoder.instances).toHaveLength(2);
    expect(markers(context.render(160).get(nodeOf(context)) ?? [])).toEqual([1, 2, 3, 5, 6]);
    expect(input.stats.concealedUs).toBe(20_000);
  });

  it('stops replaying frames that waited for a new decoder when an error callback removes the input', async () => {
    const { player } = await setUp();
    const input = player.addInput({ onError: () => input.remove() });
    FakeDecoder.hold = true;
    for (let index = 1; index <= 3; index++) input.push(frameAt(index));
    // Frames 5 to 7 wait for the next decoder, which throws on frame 5.
    FakeDecoder.failOn = 5;
    for (let index = 5; index <= 7; index++) input.push(frameAt(index, index === 5 ? 'gap' : undefined));
    await settle();
    expect(input.removed).toBe(true);
    expect(input.stats).toMatchObject({ errors: 1, decoded: 3 });
    expect(FakeDecoder.instances).toHaveLength(2);
    expect(FakeDecoder.instances.every((decoder) => decoder.state === 'closed')).toBe(true);
  });

  it('drops what a replaced decoder still holds at a resync', async () => {
    const { context, player } = await setUp();
    const input = player.addInput();
    FakeDecoder.hold = true;
    for (let index = 1; index <= 3; index++) input.push(frameAt(index));
    input.push(frameAt(5, 'gap'));
    FakeDecoder.hold = false;
    input.push(frameAt(511, 'resync'));
    input.push(frameAt(512));
    input.push(frameAt(513));
    await settle();
    for (const decoder of FakeDecoder.instances) decoder.release();
    // Frames 1 to 3 and 5 belong to the replaced timeline: only the new one plays.
    expect(markers(context.render(100).get(nodeOf(context)) ?? [])).toEqual([11, 12, 13]);
    expect(input.stats).toMatchObject({ decoded: 6, decoderResets: 1 });
  });

  it('drops a frame behind what it already decoded, and resets the decoder at a timestamp jump', async () => {
    const { player } = await setUp();
    const input = player.addInput();
    input.push(frameAt(1));
    input.push(frameAt(2));
    input.push(frameAt(1));
    expect(input.stats).toMatchObject({ decoded: 2, late: 1, decoderResets: 0 });
    // Chromium's decoder would stamp frame 5 as frame 3: a fresh decoder stamps it from its input.
    input.push(frameAt(5));
    expect(input.stats.decoderResets).toBe(1);
    await settle();
    expect(FakeDecoder.instances).toHaveLength(2);
    expect(FakeDecoder.instances[1]?.nextUs).toBe(frameAt(6).timestampUs);
  });

  it('grows the target after an underrun and shrinks it after smooth playback', async () => {
    const { context, clock, player } = await setUp();
    const input = player.addInput({ clock });
    let next = 1;
    const feed = (count: number) => {
      for (let index = 0; index < count; index++) input.push(frameAt(next++));
    };
    feed(3);
    context.render(60);
    // Nothing arrives for 80 ms: the buffer runs dry, and audio coming back that soon counts as an underrun.
    context.render(80);
    expect(input.stats.underruns).toBe(0);
    feed(4);
    context.render(20);
    expect(input.stats.underruns).toBe(1);
    expect(input.stats.jitterTargetUs).toBe(80_000);
    // Then 9 s of steady arrival, a frame per 20 ms: the target steps back down.
    for (let tick = 0; tick < 450; tick++) {
      feed(1);
      context.render(20);
    }
    expect(input.stats.underruns).toBe(1);
    expect(input.stats.jitterTargetUs).toBe(70_000);
  });

  it('does not count the talker going quiet as an underrun', async () => {
    const { context, player } = await setUp();
    const input = player.addInput();
    for (let index = 1; index <= 3; index++) input.push(frameAt(index));
    context.render(100);
    // Silent for a second, longer than any target could cover.
    context.render(1000);
    input.push(frameAt(60));
    expect(input.stats.underruns).toBe(0);
  });

  it('trims a backlog that never drains back to the target', async () => {
    const { context, player } = await setUp();
    const input = player.addInput();
    let next = 1;
    // A burst of 160 ms after a stall, then steady arrival: the held audio sits 100 ms past the 60 ms target.
    for (let index = 0; index < 8; index++) input.push(frameAt(next++));
    for (let tick = 0; tick < 60; tick++) {
      context.render(20);
      input.push(frameAt(next++));
    }
    context.render(20);
    expect(input.stats.trimmedUs).toBeGreaterThan(0);
    expect(input.stats.bufferedUs).toBeLessThanOrEqual(80_000);
  });

  it('flushes held audio at a resync and starts over at the target', async () => {
    const { context, clock, player } = await setUp();
    const input = player.addInput({ clock });
    for (let index = 1; index <= 5; index++) input.push(frameAt(index));
    context.render(40);
    input.push(frameAt(501, 'resync'));
    expect(input.stats.decoderResets).toBe(1);
    const played = context.render(40).get(nodeOf(context)) ?? [];
    // The old frames are gone, and one new frame is below the target: silence.
    expect(markers(played)).toEqual([]);
    expect(input.stats.bufferedUs).toBe(20_000);
    input.push(frameAt(502));
    input.push(frameAt(503));
    expect(markers(context.render(60).get(nodeOf(context)) ?? [])).toEqual([1, 2, 3]);
  });

  it('flushes and resets the decoder at a restart, even to an earlier timeline', async () => {
    const { context, player } = await setUp();
    const input = player.addInput();
    for (let index = 10; index <= 14; index++) input.push(frameAt(index));
    context.render(20);
    input.push(frameAt(1, 'restart'));
    expect(input.stats).toMatchObject({ late: 0, decoderResets: 1 });
    input.push(frameAt(2));
    input.push(frameAt(3));
    expect(markers(context.render(60).get(nodeOf(context)) ?? [])).toEqual([1, 2, 3]);
  });

  it('cuts a timeline range: held audio, output still decoding, and frames that arrive later', async () => {
    const { context, player } = await setUp();
    const input = player.addInput();
    for (let index = 1; index <= 4; index++) input.push(frameAt(index));
    FakeDecoder.hold = true;
    input.push(frameAt(5));
    input.push(frameAt(6));
    // Drop frames 2 to 5, as at an interruption ending at frame 6's start.
    input.cut({ fromUs: frameAt(2).timestampUs, untilUs: frameAt(6).timestampUs });
    FakeDecoder.instances[0]?.release();
    FakeDecoder.hold = false;
    input.push(frameAt(5));
    expect(input.stats.late).toBe(1);
    input.push(frameAt(7));
    expect(markers(context.render(100).get(nodeOf(context)) ?? [])).toEqual([1, 6, 7]);
    expect(input.stats.cutUs).toBe(80_000);
    // From a position on, with no end: what is held from there goes; later frames play.
    input.cut({ fromUs: frameAt(7).timestampUs });
    input.push(frameAt(8));
    expect(input.stats.late).toBe(1);
    // No range: everything held.
    input.cut();
    expect(input.stats.decoderResets).toBe(1);
  });

  it('keeps a cut through a resync, so interrupted audio arriving behind it stays dropped', async () => {
    const { context, player } = await setUp();
    const input = player.addInput();
    for (let index = 1; index <= 3; index++) input.push(frameAt(index));
    context.render(20);
    input.cut({ untilUs: frameAt(50).timestampUs });
    // A resync to a frame from before the cut's end: the sequencer delivers it, the cut still drops it.
    input.push(frameAt(10, 'resync'));
    input.push(frameAt(11));
    input.push(frameAt(50));
    input.push(frameAt(51));
    input.push(frameAt(52));
    expect(input.stats.late).toBe(2);
    await settle();
    expect(markers(context.render(100).get(nodeOf(context)) ?? [])).toEqual([50, 51, 52]);
  });

  it('plays frames that arrive later from before a cut range, and drops those inside it', async () => {
    const { context, player } = await setUp();
    const input = player.addInput();
    input.push(frameAt(1));
    // The talker keeps frames 2 and 3 (the end of a word) and drops 4 and 5, before any of them has arrived.
    input.cut({ fromUs: frameAt(4).timestampUs, untilUs: frameAt(6).timestampUs });
    for (let index = 2; index <= 7; index++) input.push(frameAt(index));
    expect(input.stats.late).toBe(2);
    // Frame 6 does not follow frame 3, so it waits for a new decoder.
    await settle();
    expect(markers(context.render(140).get(nodeOf(context)) ?? [])).toEqual([1, 2, 3, 6, 7]);
  });

  it('clears its cuts at a restart', async () => {
    const { context, player } = await setUp();
    const input = player.addInput();
    input.push(frameAt(20));
    input.cut({ untilUs: frameAt(30).timestampUs });
    input.push(frameAt(1, 'restart'));
    input.push(frameAt(2));
    input.push(frameAt(3));
    expect(input.stats.late).toBe(0);
    expect(markers(context.render(60).get(nodeOf(context)) ?? [])).toEqual([1, 2, 3]);
  });

  it('joins the audio after a cut when audio from before the cut lands first, with nothing held', async () => {
    const { context, player } = await setUp();
    const input = player.addInput();
    // Nothing is held yet: frames 1 to 3 are still decoding when frames 2 and 3 are cut.
    FakeDecoder.hold = true;
    for (let index = 1; index <= 3; index++) input.push(frameAt(index));
    input.cut({ fromUs: frameAt(2).timestampUs, untilUs: frameAt(4).timestampUs });
    FakeDecoder.instances[0]?.release();
    FakeDecoder.hold = false;
    for (let index = 4; index <= 6; index++) input.push(frameAt(index));
    expect(markers(context.render(100).get(nodeOf(context)) ?? [])).toEqual([1, 4, 5, 6]);
    expect(input.stats).toMatchObject({ concealedUs: 0, cutUs: 40_000 });
  });

  it('joins the audio after a cut when audio from before the cut lands first, with audio held', async () => {
    const { context, player } = await setUp();
    const input = player.addInput();
    for (let index = 1; index <= 3; index++) input.push(frameAt(index));
    // Frame 4 is still decoding when a cut from frame 5 to frame 7 begins after it.
    FakeDecoder.hold = true;
    for (let index = 4; index <= 6; index++) input.push(frameAt(index));
    input.cut({ fromUs: frameAt(5).timestampUs, untilUs: frameAt(7).timestampUs });
    FakeDecoder.instances[0]?.release();
    FakeDecoder.hold = false;
    input.push(frameAt(7));
    input.push(frameAt(8));
    expect(markers(context.render(160).get(nodeOf(context)) ?? [])).toEqual([1, 2, 3, 4, 7, 8]);
    expect(input.stats).toMatchObject({ concealedUs: 0, cutUs: 40_000 });
  });

  it('fills only the part of a hole outside a cut with silence', async () => {
    const { context, player } = await setUp();
    const input = player.addInput();
    input.push(frameAt(1));
    // Frames 2 and 3 are cut before they arrive, and frame 4 never comes: its 20 ms are silence.
    input.cut({ fromUs: frameAt(2).timestampUs, untilUs: frameAt(4).timestampUs });
    input.push(frameAt(5));
    input.push(frameAt(6));
    await settle();
    const played = context.render(100).get(nodeOf(context)) ?? [];
    expect(markers(played)).toEqual([1, 5, 6]);
    expect(input.stats.concealedUs).toBe(20_000);
    const start = played.findIndex((sample) => sample !== 0);
    expect(Math.round((played[start + 2 * FRAME_SAMPLES] as number) * 100)).toBe(5);
  });

  it('joins on after each of two cuts made while their audio was still decoding', async () => {
    const { context, player } = await setUp();
    const input = player.addInput();
    FakeDecoder.hold = true;
    for (let index = 1; index <= 6; index++) input.push(frameAt(index));
    input.cut({ fromUs: frameAt(2).timestampUs, untilUs: frameAt(3).timestampUs });
    input.cut({ fromUs: frameAt(5).timestampUs, untilUs: frameAt(6).timestampUs });
    FakeDecoder.instances[0]?.release();
    FakeDecoder.hold = false;
    expect(markers(context.render(100).get(nodeOf(context)) ?? [])).toEqual([1, 3, 4, 6]);
    expect(input.stats).toMatchObject({ concealedUs: 0, cutUs: 40_000 });
  });

  it('still fills a frame lost after a cut inside held audio with silence', async () => {
    const { context, player } = await setUp();
    const input = player.addInput();
    for (let index = 1; index <= 6; index++) input.push(frameAt(index));
    input.cut({ fromUs: frameAt(2).timestampUs, untilUs: frameAt(4).timestampUs });
    // Frame 7 never comes: frame 8 is not the audio after the cut, so the hole is silence.
    input.push(frameAt(8));
    await settle();
    expect(markers(context.render(160).get(nodeOf(context)) ?? [])).toEqual([1, 4, 5, 6, 8]);
    expect(input.stats).toMatchObject({ concealedUs: 20_000, cutUs: 40_000 });
  });

  it('does not count refilling after a cut that empties the buffer as an underrun', async () => {
    const { context, player } = await setUp();
    const input = player.addInput();
    for (let index = 1; index <= 4; index++) input.push(frameAt(index));
    context.render(40);
    input.cut({ untilUs: frameAt(10).timestampUs });
    context.render(100);
    for (let index = 10; index <= 13; index++) input.push(frameAt(index));
    await settle();
    expect(markers(context.render(80).get(nodeOf(context)) ?? [])).toEqual([10, 11, 12, 13]);
    expect(input.stats).toMatchObject({ underruns: 0, jitterTargetUs: 60_000 });
  });

  it('mixes two inputs into one output, each with its own gain and clock', async () => {
    const { context, player } = await setUp();
    const first = createPlayoutClock({ now: () => nowUs });
    const second = createPlayoutClock({ now: () => nowUs });
    const a = player.addInput({ clock: first, gain: 0.5 });
    const b = player.addInput({ clock: second });
    expect(nodeOf(context, 0).targets.size).toBe(1);
    const [gainA] = [...nodeOf(context, 0).targets] as FakeGain[];
    const [gainB] = [...nodeOf(context, 1).targets] as FakeGain[];
    expect(gainA?.targets.has(player.output)).toBe(true);
    expect(gainB?.targets.has(player.output)).toBe(true);
    expect(a.gain).toBe(0.5);
    b.gain = 0.8;
    expect(gainB?.gain.value).toBe(0.8);
    for (let index = 1; index <= 3; index++) {
      a.push(frameAt(index));
      b.push({ ...frameAt(index + 1000), payload: packet(50 + index) });
    }
    const outputs = context.render(80);
    expect(markers(outputs.get(nodeOf(context, 0)) ?? [])).toEqual([1, 2, 3]);
    expect(markers(outputs.get(nodeOf(context, 1)) ?? [])).toEqual([51, 52, 53]);
    expect(first.position()).toBeLessThan(BASE_US + 200_000);
    expect(second.position()).toBeGreaterThan(BASE_US + 1000 * FRAME_US);
    a.remove();
    expect(a.removed).toBe(true);
    expect(gainA?.disconnect).toHaveBeenCalled();
    expect(first.mode).toBe('wall');
    expect(player.inputs).toEqual([b]);
    player.close();
    expect(b.removed).toBe(true);
    expect(context.render(20).size).toBe(0);
  });

  it('resamples to a 44.1 kHz context and keeps the timeline', async () => {
    const { context, clock, player } = await setUp({ sampleRate: 44_100 });
    const input = player.addInput({ clock });
    for (let index = 1; index <= 5; index++) input.push(frameAt(index));
    const played = context.render(100).get(nodeOf(context)) ?? [];
    expect(markers(played)).toEqual([1, 2, 3, 4, 5]);
    // 20 ms of 48 kHz is 882 frames at 44.1 kHz.
    const start = played.findIndex((sample) => sample !== 0);
    expect(Math.round((played[start + 882 + 2] as number) * 100)).toBe(2);
    expect(input.stats.concealedUs).toBe(0);
  });

  it('caps the jitter buffer under the clock target latency less the output delay', async () => {
    const { context, player } = await setUp();
    context.outputLatency = 0.2;
    const clock: PlayoutClock = createPlayoutClock({ now: () => nowUs, targetLatencyUs: AUDIO_ONLY_TARGET_LATENCY_US });
    const input = player.addInput({ clock, jitterTargetUs: 400_000 });
    // 500 ms budget, less 200 ms output and the 40 ms margin.
    expect(input.stats.jitterTargetUs).toBe(260_000);
    // The output delay drops to 50 ms: the ceiling rises to 410 ms, but the target only grows from underruns.
    context.outputLatency = 0.05;
    context.render(40);
    expect(input.stats.jitterTargetUs).toBe(260_000);
  });

  it('rejects bad options and decodes from a fresh decoder after an error', async () => {
    const { player } = await setUp();
    expect(() => player.addInput({ minJitterTargetUs: 500_000 })).toThrow(RangeError);
    expect(() => player.addInput({ jitterTargetUs: 0 })).toThrow(RangeError);
    const onError = vi.fn();
    const input = player.addInput({ onError });
    input.push(frameAt(1));
    FakeDecoder.instances.at(-1)?.init.error(new DOMException('boom', 'EncodingError'));
    expect(onError).toHaveBeenCalledTimes(1);
    input.push(frameAt(2));
    expect(input.stats).toMatchObject({ errors: 1, decoded: 2 });
    expect(FakeDecoder.instances.at(-1)?.state).toBe('configured');
  });

  it('rejects a playback rate outside 0.5 to 2, from the options and the setter', async () => {
    const { player } = await setUp();
    for (const playbackRate of [0.49, 2.01, 0, Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(() => player.addInput({ playbackRate })).toThrow(RangeError);
    }
    const input = player.addInput({ playbackRate: 0.5 });
    expect(input.playbackRate).toBe(0.5);
    input.playbackRate = 2;
    expect(input.playbackRate).toBe(2);
    expect(() => {
      input.playbackRate = 2.5;
    }).toThrow(RangeError);
    expect(input.playbackRate).toBe(2);
    expect(player.addInput().playbackRate).toBe(1);
  });

  it('plays held audio faster at a rate above 1, keeping the jitter buffer in held audio', async () => {
    const { context, player } = await setUp();
    const reports: { timestampUs: number; atUs: number }[] = [];
    const clock: AudioInputClock = {
      targetLatencyUs: 2_000_000,
      localNow: () => nowUs,
      reportAudio: (timestampUs, atUs) => reports.push({ timestampUs, atUs }),
      stopAudio() {},
    };
    const input = player.addInput({ clock, playbackRate: 1.5 });
    for (let index = 1; index <= 8; index++) input.push(frameAt(index));
    // 160 ms held, past the 60 ms target. The latest report came after 3072 frames of output, which read 4608.
    context.render(80);
    expect(input.stats).toMatchObject({ playing: true, bufferedUs: 64_000, jitterTargetUs: 60_000, trimmedUs: 0 });
    // Each report's timestamp moves 1.5 times as far as its render time.
    const [first, second] = reports.slice(-2) as [(typeof reports)[0], (typeof reports)[0]];
    expect(Math.abs((second.timestampUs - first.timestampUs) / (second.atUs - first.atUs) - 1.5)).toBeLessThan(0.01);
  });

  it('keeps the fractional read position across quanta and rate changes, and reports the timestamp there', async () => {
    const { context, player } = await setUp();
    FakeDecoder.ramp = true;
    const reports: { timestampUs: number; atUs: number }[] = [];
    const clock: AudioInputClock = {
      targetLatencyUs: 2_000_000,
      localNow: () => nowUs,
      reportAudio: (timestampUs, atUs) => reports.push({ timestampUs, atUs }),
      stopAudio() {},
    };
    const input = player.addInput({ clock, playbackRate: 0.95 });
    for (let index = 0; index < 8; index++) input.push(frameAt(index));
    const played = [...(context.render(30).get(nodeOf(context)) ?? [])];
    input.playbackRate = 1.05;
    played.push(...(context.render(30).get(nodeOf(context)) ?? []));
    input.playbackRate = 1;
    played.push(...(context.render(30).get(nodeOf(context)) ?? []));
    // It plays from the first quantum. A linear ramp read at a rate rises by the rate per sample: any other step would
    // be a click.
    const steps = new Set<number>();
    for (let index = 1; index < played.length; index++) {
      steps.add(Math.round(((played[index] as number) - (played[index - 1] as number)) / RAMP / 1e-3) * 1e-3);
    }
    expect([...steps].map((step) => step.toFixed(2)).sort()).toEqual(['0.95', '1.00', '1.05']);
    // A report's timestamp is the read position of the sample output at its render time.
    for (const report of reports) {
      const sample = played[Math.round((report.atUs * RATE) / 1e6)] as number;
      expect(Math.abs(report.timestampUs - (BASE_US + (sample / RAMP / RATE) * 1e6))).toBeLessThanOrEqual(1);
    }
    expect(reports.length).toBeGreaterThan(2);
  });

  it('does not trim the audio a rate below 1 gains, and trims it as usual once back at 1', async () => {
    const { context, player } = await setUp();
    const input = player.addInput({ playbackRate: 0.95, jitterTargetUs: 120_000 });
    let next = 1;
    for (; next <= 6; next++) input.push(frameAt(next));
    // Steady arrival, a frame per 20 ms, for 3 s at 0.95: held audio grows 50 ms a second, across three trim windows,
    // and stays under the 300 ms hard bound.
    for (let tick = 0; tick < 150; tick++) {
      context.render(20);
      input.push(frameAt(next++));
    }
    expect(input.stats).toMatchObject({ trimmedUs: 0, underruns: 0 });
    expect(input.stats.bufferedUs).toBeGreaterThan(240_000);
    // Back at 1, held audio that never dips to the target is delay nobody needs again.
    input.playbackRate = 1;
    for (let tick = 0; tick < 100; tick++) {
      context.render(20);
      input.push(frameAt(next++));
    }
    expect(input.stats.trimmedUs).toBeGreaterThan(0);
    expect(input.stats.bufferedUs).toBeLessThanOrEqual(140_000);
  });

  it('stops the read position at the newest held sample at rate 2', async () => {
    const { context, player } = await setUp();
    const input = player.addInput({ playbackRate: 2 });
    for (let index = 1; index <= 3; index++) input.push(frameAt(index));
    // 60 ms held plays out in 30 ms, then runs dry exactly where the audio ends.
    expect(markers(context.render(100).get(nodeOf(context)) ?? [])).toEqual([1, 2, 3]);
    expect(input.stats).toMatchObject({ playing: false, bufferedUs: 0, dryAtUs: frameAt(4).timestampUs });
  });

  it('runs dry with nothing held at a fractional rate, so a gap after it is not filled with silence', async () => {
    const { context, player } = await setUp();
    const input = player.addInput({ playbackRate: 0.95 });
    for (let index = 1; index <= 3; index++) input.push(frameAt(index));
    context.render(200);
    expect(input.stats).toMatchObject({ playing: false, bufferedUs: 0, heldUntilUs: null });
    expect(Math.abs((input.stats.dryAtUs as number) - frameAt(4).timestampUs)).toBeLessThanOrEqual(21);
    // The talker went quiet for 200 ms: the audio after it starts the buffer again, with no silence in front.
    for (let index = 14; index <= 16; index++) input.push(frameAt(index));
    await settle();
    context.render(30);
    expect(input.stats).toMatchObject({ concealedUs: 0, bufferedUs: expect.any(Number), trimmedUs: 0 });
    expect(input.stats.bufferedUs).toBeLessThanOrEqual(60_000);
  });

  it('forgets where it ran dry at a flush of an empty buffer', async () => {
    const { context, player } = await setUp();
    const input = player.addInput();
    for (let index = 1; index <= 3; index++) input.push(frameAt(index));
    context.render(100);
    expect(input.stats.dryAtUs).toBe(frameAt(4).timestampUs);
    input.push(frameAt(500, 'resync'));
    context.render(30);
    expect(input.stats).toMatchObject({ dryAtUs: null, underruns: 0 });
  });

  it('reports where held audio ends, where it ran dry, and each start of playback to the clock', async () => {
    const { context, player } = await setUp();
    const reports: { timestampUs: number; stats: Pick<AudioInputStats, 'playStarts' | 'playing'> }[] = [];
    const clock: AudioInputClock = {
      targetLatencyUs: 2_000_000,
      localNow: () => nowUs,
      reportAudio: (timestampUs, _atUs, { playStarts, playing }) =>
        reports.push({ timestampUs, stats: { playStarts, playing } }),
      stopAudio() {},
    };
    const input = player.addInput({ clock });
    expect(input.stats).toMatchObject({ playStarts: 0, heldUntilUs: null, dryAtUs: null });
    for (let index = 1; index <= 3; index++) input.push(frameAt(index));
    context.render(30);
    expect(input.stats).toMatchObject({ playStarts: 1, heldUntilUs: frameAt(4).timestampUs, dryAtUs: null });
    // It runs dry at the end of frame 3, and holds that until it plays again.
    context.render(100);
    expect(input.stats).toMatchObject({ playing: false, playStarts: 1, heldUntilUs: null });
    expect(input.stats.dryAtUs).toBe(frameAt(4).timestampUs);
    const tail = reports.length;
    // Back soon enough to count as an underrun: the target grows to 80 ms.
    for (let index = 9; index <= 12; index++) input.push(frameAt(index));
    await settle();
    context.render(30);
    expect(input.stats).toMatchObject({ playing: true, playStarts: 2, dryAtUs: null });
    // Reports of the tail carry the old count, and reports of the resumed audio the new one.
    expect(reports.slice(0, tail).every((report) => report.stats.playStarts === 1)).toBe(true);
    expect(reports.slice(tail).every((report) => report.stats.playStarts === 2)).toBe(true);
    expect(reports[tail]?.timestampUs).toBeGreaterThanOrEqual(frameAt(9).timestampUs);
    // A cut of everything held is a new start too, and forgets nothing it did not hold.
    input.cut();
    for (let index = 20; index <= 23; index++) input.push(frameAt(index, index === 20 ? 'resync' : undefined));
    context.render(30);
    expect(input.stats).toMatchObject({ playStarts: 3, dryAtUs: null, heldUntilUs: frameAt(24).timestampUs });
  });

  it('loads the worklet once per context', async () => {
    const context = new FakeContext();
    const players: AudioPlayer[] = await Promise.all([
      createAudioPlayer({ context, lossConcealment: false }),
      createAudioPlayer({ context, lossConcealment: false }),
    ]);
    expect(players).toHaveLength(2);
    expect(context.audioWorklet.addModule).toHaveBeenCalledTimes(1);
  });
});

describe('audio helpers', () => {
  it('reads an Opus packet duration from its TOC byte', () => {
    expect(opusPacketDurationUs(new Uint8Array([OPUS_20MS]))).toBe(20_000);
    expect(opusPacketDurationUs(new Uint8Array([(16 << 3) | 1]))).toBe(5_000);
    expect(opusPacketDurationUs(new Uint8Array([(3 << 3) | 3, 3]))).toBe(180_000);
    expect(opusPacketDurationUs(new Uint8Array([(13 << 3) | 2]))).toBe(40_000);
    expect(opusPacketDurationUs(new Uint8Array([]))).toBeUndefined();
  });

  it('builds a decoder config from a catalog track', () => {
    const initData = new Uint8Array([1, 2]);
    const track = { namespace: ['a'], name: 'audio', packaging: 'loc', isLive: true, codec: 'opus' };
    expect(audioDecoderConfigFromTrack({ ...track, samplerate: 48_000, channelConfig: '1', initData })).toEqual({
      codec: 'opus',
      sampleRate: 48_000,
      numberOfChannels: 1,
      description: initData,
    });
    expect(audioDecoderConfigFromTrack(track)).toEqual({ codec: 'opus', sampleRate: 48_000, numberOfChannels: 2 });
    expect(() => audioDecoderConfigFromTrack({ ...track, channelConfig: '5.1' })).toThrow(/channel count/);
    const { codec: _, ...uncoded } = track;
    expect(() => audioDecoderConfigFromTrack(uncoded)).toThrow(/codec/);
  });
});

// Type-level only, never called: a Web Audio context and its nodes satisfy the structural types, and a playout clock
// or a clock that takes only a timestamp and a time is an input's clock.
async function typeCheck(context: AudioContext, input: AudioPlayerInput, clock: PlayoutClock) {
  const player = await createAudioPlayer({ context, lossConcealment: false });
  player.addInput({ clock });
  player.addInput({
    clock: {
      targetLatencyUs: 1,
      localNow: () => 0,
      reportAudio(_timestampUs: number, _atUs?: number) {},
      stopAudio() {},
    },
  });
  const gain: GainNode = player.output;
  gain.connect(context.destination);
  input.push({ timestampUs: 0, payload: new Uint8Array() });
}
void typeCheck;

describe('createEchoCancellationLoopback', () => {
  class FakePeer {
    static all: FakePeer[] = [];
    onicecandidate: ((event: { candidate: unknown }) => void) | null = null;
    ontrack: ((event: { track: unknown }) => void) | null = null;
    connectionState = 'new';
    listeners: (() => void)[] = [];
    tracks: unknown[] = [];
    local: { type: string; sdp?: string } | undefined;
    remote: { type: string; sdp?: string } | undefined;
    close = vi.fn(() => {
      this.connectionState = 'closed';
    });
    constructor() {
      FakePeer.all.push(this);
    }
    addEventListener(_type: string, listener: () => void) {
      this.listeners.push(listener);
    }
    addTrack(track: unknown) {
      this.tracks.push(track);
    }
    async addIceCandidate() {}
    async createOffer() {
      return { type: 'offer', sdp: 'a=fmtp:111 minptime=10;useinbandfec=1' };
    }
    async createAnswer() {
      return { type: 'answer', sdp: 'a=fmtp:111 minptime=10;useinbandfec=1' };
    }
    async setLocalDescription(description: { type: string; sdp?: string }) {
      this.local = description;
    }
    async setRemoteDescription(description: { type: string; sdp?: string }) {
      this.remote = description;
      if (description.type === 'offer') {
        // The receiver gets the sender's track and connects.
        const sender = FakePeer.all[0] as FakePeer;
        queueMicrotask(() => {
          this.ontrack?.({ track: sender.tracks[0] });
          this.connectionState = 'connected';
          for (const listener of this.listeners) listener();
        });
      }
    }
  }
  class FakeElement {
    autoplay = false;
    srcObject: unknown = null;
    play = vi.fn(async () => {});
    setSinkId = vi.fn(async () => {});
  }
  const elements: FakeElement[] = [];

  beforeEach(() => {
    FakePeer.all = [];
    elements.length = 0;
    vi.stubGlobal('RTCPeerConnection', FakePeer);
    vi.stubGlobal(
      'MediaStream',
      class {
        tracks: unknown[];
        constructor(tracks: unknown[]) {
          this.tracks = tracks;
        }
      },
    );
    vi.stubGlobal(
      'Audio',
      class extends FakeElement {
        constructor() {
          super();
          elements.push(this);
        }
      },
    );
  });

  it('plays the node through a local peer connection into a media element, and closes', async () => {
    const track = { kind: 'audio' };
    const destination = Object.assign(new FakeGain(), { channelCount: 1, stream: { getAudioTracks: () => [track] } });
    const context = { createMediaStreamDestination: () => destination };
    const node = new FakeGain();
    const loopback = await createEchoCancellationLoopback(context, node, { sinkId: 'speakers' });
    const [sender, receiver] = FakePeer.all as [FakePeer, FakePeer];
    expect(sender.tracks).toEqual([track]);
    expect(sender.local?.sdp).toContain('useinbandfec=1;stereo=1;sprop-stereo=1;maxaveragebitrate=510000');
    expect(receiver.local?.sdp).toContain('useinbandfec=1;stereo=1;maxaveragebitrate=510000');
    expect(destination.channelCount).toBe(2);
    expect(node.targets.has(destination)).toBe(true);
    const element = elements[0] as FakeElement;
    expect(element.srcObject).toEqual({ tracks: [track] });
    expect(element.setSinkId).toHaveBeenCalledWith('speakers');
    expect(element.play).toHaveBeenCalled();
    await loopback.setSinkId('headset');
    expect(element.setSinkId).toHaveBeenLastCalledWith('headset');
    loopback.close();
    expect(node.disconnect).toHaveBeenCalledWith(destination);
    expect(sender.close).toHaveBeenCalled();
    expect(receiver.close).toHaveBeenCalled();
    expect(element.srcObject).toBeNull();
  });

  it('rejects and closes both connections when the loopback does not connect', async () => {
    vi.useFakeTimers();
    try {
      vi.stubGlobal(
        'RTCPeerConnection',
        class extends FakePeer {
          override async setRemoteDescription(description: { type: string; sdp?: string }) {
            this.remote = description;
          }
        },
      );
      const destination = Object.assign(new FakeGain(), {
        channelCount: 1,
        stream: { getAudioTracks: () => [{ kind: 'audio' }] },
      });
      const node = new FakeGain();
      const pending = createEchoCancellationLoopback({ createMediaStreamDestination: () => destination }, node, {
        timeoutMs: 100,
      });
      const failed = expect(pending).rejects.toThrow(/received no track within 100 ms/);
      await vi.advanceTimersByTimeAsync(100);
      await failed;
      expect(FakePeer.all.every((peer) => peer.close.mock.calls.length === 1)).toBe(true);
      expect(node.targets.size).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });
});

// Six consecutive 20 ms Opus packets (SILK, 32 kbit/s, mono) of a 300 Hz tone, from libopus with in-band FEC on and
// 10% expected loss: the first four carry LBRR, the FEC copy of the frame before them; the last two do not.
const OPUS_PACKETS = [
  'eNsq0lbYDhjw3GMFVER1GLNe/OddAMhCpLUNddotogMYjyI+Lqw7/cGB8SUyVzLYYg1p+OhrUr8GRx9EnlvinGV2IKO9G2mTqw==',
  'eNsq7WglA7LrXjVTAhZx0u/bcYjn2IQ+nZkkmbqtIvM/GUdF096ifK6pJfCiP31rkbzF9I+RIOc5eIcG43G2Lg+9G2mTqw==',
  'eNsqyzGGB/WI4/gZs/o8XsQJA0SUE8aydrKEZ/l2yDY60kCD8mNSJpMyMiKnR9ho9tnY2k1lWBhoNZrsbpXKJnPrRdc2kh1vW+mTpw==',
  'eNsqyzGGGB8x8vZN7XpxGSMf/LkvWKgFq+6+0RYrb/bGxwU3ZW65u2NIDiMUQ5vXkRTsudkJIZrXTjS8UxZOz//FKJXciG+9GxPpqw==',
  'eJjU1c8luahy5ovPmDeqaS1NkregygoGINmSbt1466dHJX/UJ1ltP08T89ZDoDjK8ihpnMj3y70baZOr',
  'eJjU1c7K1paUCS3NaRLby6IL0bZ79E7/HB4vNHGMPaB+r/PXYwBIodA7cn5T+Zzjf+60Abe9G2mTqw==',
].map((base64) => new Uint8Array(Buffer.from(base64, 'base64')));
const opusAt = (index: number, discontinuity?: 'gap' | 'resync' | 'restart') => ({
  timestampUs: BASE_US + index * FRAME_US,
  payload: OPUS_PACKETS[index] as Uint8Array,
  ...(discontinuity ? { discontinuity } : {}),
});
const mono = { codec: 'opus', sampleRate: RATE, numberOfChannels: 1 };
/** RMS of a stretch of output samples. */
const rms = (samples: number[]) => Math.sqrt(samples.reduce((sum, value) => sum + value * value, 0) / samples.length);

describe('loss concealment (libopus in WebAssembly)', () => {
  it('rebuilds a lost frame from the next frame’s in-band FEC, with no silence and no decoder reset', async () => {
    const { context, clock, player } = await setUp({ lossConcealment: true });
    const input = player.addInput({ clock, decoderConfig: mono, jitterTargetUs: 40_000 });
    input.push(opusAt(0));
    input.push(opusAt(1));
    context.render(30);
    expect(input.stats.playing).toBe(true);
    // Frame 2 never comes; frame 3 carries its FEC copy.
    input.push(opusAt(3, 'gap'));
    input.push(opusAt(4));
    expect(input.stats).toMatchObject({ decoded: 4, recoveredUs: 20_000, synthesizedUs: 0, decoderResets: 0 });
    const played = context.render(80).get(nodeOf(context)) ?? [];
    // The buffer played on through the lost frame's slot: nothing silent was written for it, and it has sound.
    expect(input.stats).toMatchObject({ concealedUs: 0, underruns: 0 });
    expect(rms(played.slice(0, 2 * FRAME_SAMPLES))).toBeGreaterThan(0.01);
  });

  it('conceals a lost frame whose next frame carries no FEC, and leaves a long gap to the worklet’s silence', async () => {
    const { context, clock, player } = await setUp({ lossConcealment: true });
    const input = player.addInput({ clock, decoderConfig: mono, jitterTargetUs: 40_000 });
    input.push(opusAt(2));
    input.push(opusAt(3));
    context.render(30);
    // Frame 4 is lost and frame 5 has no LBRR: packet-loss concealment fills it.
    input.push(opusAt(5, 'gap'));
    expect(input.stats).toMatchObject({ decoded: 3, recoveredUs: 0, synthesizedUs: 20_000 });
    // 400 ms missing: 120 ms synthesized at most, the last frame from FEC; the rest is silence the worklet writes.
    input.push({ timestampUs: BASE_US + 26 * FRAME_US, payload: OPUS_PACKETS[1] as Uint8Array, discontinuity: 'gap' });
    expect(input.stats.synthesizedUs).toBe(20_000 + 120_000);
    expect(input.stats.recoveredUs).toBe(20_000);
    context.render(100);
    expect(input.stats.concealedUs).toBeGreaterThan(0);
  });

  it('fills nothing once the buffer ran dry: the worklet skips that audio rather than play it late', async () => {
    const { context, clock, player } = await setUp({ lossConcealment: true });
    const input = player.addInput({ clock, decoderConfig: mono, jitterTargetUs: 40_000 });
    input.push(opusAt(0));
    input.push(opusAt(1));
    context.render(100);
    expect(input.stats).toMatchObject({ playing: false });
    expect(input.stats.dryAtUs).not.toBeNull();
    input.push(opusAt(3, 'gap'));
    expect(input.stats).toMatchObject({ decoded: 3, recoveredUs: 0, synthesizedUs: 0 });
  });

  it('takes an empty payload as a frame not received: the next frame fills its slot, nothing errs', async () => {
    const { context, clock, player } = await setUp({ lossConcealment: true });
    const input = player.addInput({ clock, decoderConfig: mono, jitterTargetUs: 40_000 });
    input.push(opusAt(0));
    input.push(opusAt(1));
    context.render(30);
    input.push({ timestampUs: opusAt(2).timestampUs, payload: new Uint8Array(0) });
    input.push(opusAt(3, 'gap'));
    expect(input.stats).toMatchObject({ decoded: 3, errors: 0, recoveredUs: 20_000, synthesizedUs: 0 });
    context.render(60);
    expect(input.stats).toMatchObject({ concealedUs: 0, underruns: 0 });
  });

  it('fills the slot and decodes the frame that came even when that frame is damaged', async () => {
    const { context, clock, player } = await setUp({ lossConcealment: true });
    const input = player.addInput({ clock, decoderConfig: mono, jitterTargetUs: 40_000 });
    input.push(opusAt(0));
    input.push(opusAt(1));
    context.render(30);
    // Frame 2 is lost; frame 3 comes truncated to two bytes: frame 2's slot is filled (from what LBRR libopus can
    // read, or by concealment), and frame 3 is still decoded rather than dropped with it.
    const truncated = (OPUS_PACKETS[3] as Uint8Array).subarray(0, 2);
    input.push({ timestampUs: opusAt(3).timestampUs, payload: truncated, discontinuity: 'gap' });
    expect(input.stats.recoveredUs + input.stats.synthesizedUs).toBe(20_000);
    expect(input.stats).toMatchObject({ decoded: 3, errors: 0 });
    input.push(opusAt(4));
    expect(input.stats.decoded).toBe(4);
  });

  it('counts and reports a frame that fails once, after it is done, so onError may remove the input', async () => {
    const { context, clock, player } = await setUp({ lossConcealment: true });
    const errors: unknown[] = [];
    const input = player.addInput({
      clock,
      decoderConfig: mono,
      jitterTargetUs: 40_000,
      onError: (error) => {
        errors.push(error);
        input.remove();
      },
    });
    input.push(opusAt(0));
    input.push(opusAt(1));
    context.render(30);
    // Frame 2 is lost and frame 3 is not a valid packet (code 3, no frames): its slot is concealed, it fails to decode.
    const invalid = new Uint8Array([0x0b, 0x00]);
    expect(() =>
      input.push({ timestampUs: opusAt(3).timestampUs, payload: invalid, discontinuity: 'gap' }),
    ).not.toThrow();
    expect(errors).toHaveLength(1);
    expect(input.removed).toBe(true);
    expect(input.stats).toMatchObject({ decoded: 2, errors: 1, decoderResets: 1, synthesizedUs: 20_000 });
  });

  it('leaves audio cut out on purpose out: a gap over a cut is not concealed', async () => {
    const { context, clock, player } = await setUp({ lossConcealment: true });
    const input = player.addInput({ clock, decoderConfig: mono, jitterTargetUs: 40_000 });
    input.push(opusAt(0));
    input.push(opusAt(1));
    context.render(30);
    expect(input.stats.playing).toBe(true);
    // An interruption cuts frames 2 and 3 before they arrive; frame 4 comes next.
    input.cut({ fromUs: opusAt(2).timestampUs, untilUs: opusAt(4).timestampUs });
    input.push(opusAt(4, 'gap'));
    expect(input.stats).toMatchObject({ decoded: 3, recoveredUs: 0, synthesizedUs: 0, decoderResets: 0 });
    context.render(20);
  });

  it('starts afresh after a resync instead of concealing across it', async () => {
    const { context, clock, player } = await setUp({ lossConcealment: true });
    const input = player.addInput({ clock, decoderConfig: mono });
    input.push(opusAt(0));
    input.push(opusAt(3, 'resync'));
    expect(input.stats).toMatchObject({ decoded: 2, recoveredUs: 0, synthesizedUs: 0, decoderResets: 1 });
    context.render(20);
  });

  it('decodes with WebCodecs when concealment is off, or for another codec', async () => {
    const off = await setUp({ lossConcealment: false });
    off.player.addInput({ clock: off.clock, decoderConfig: mono }).push(opusAt(0));
    expect(FakeDecoder.instances).toHaveLength(1);
    const on = await setUp({ lossConcealment: true });
    on.player
      .addInput({ clock: on.clock, decoderConfig: { codec: 'flac', sampleRate: RATE, numberOfChannels: 1 } })
      .push(opusAt(0));
    expect(FakeDecoder.instances).toHaveLength(2);
  });
});
