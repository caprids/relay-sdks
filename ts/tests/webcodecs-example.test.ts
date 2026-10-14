import {
  createBroadcastClock,
  LOC_TIMESTAMP,
  LOC_VIDEO_CONFIG,
  parseMediaTimeline,
  type SourceTimeline,
} from '@mux/moq-media';
import type { PublishProperty, RelayPublisher, RelayTrackWriter } from '@mux/relay';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import type { MicrophoneCapture } from '../examples/webcodecs/capture-microphone.js';
import { publishCanvas } from '../examples/webcodecs/publish-canvas.js';
import { NO_DELIVERY_TIMEOUTS } from '../src/delivery-timeouts.js';
import { createSettlingGroups } from '../src/settling-groups.js';
import { createWriteSlot } from '../src/write-slot.js';

vi.mock('@mux/relay', async () => import('../src/index.js'));
vi.mock('@mux/relay/media', async () => import('../src/media.js'));

const description = Uint8Array.from(Buffer.from('0142c014ffe1000a6742c014da05067e784001000468ce3c80', 'hex'));
const decoderConfig = { codec: 'avc1.42001f', codedWidth: 640, codedHeight: 360, description };
const canvas = { width: 640, height: 360 } as HTMLCanvasElement;

class TestFrame {
  close = vi.fn();
  timestamp: number;
  constructor(_canvas: unknown, { timestamp }: VideoFrameInit) {
    this.timestamp = timestamp ?? 0;
    frames.push(this);
  }
}
const frames: TestFrame[] = [];
class TestEncoder {
  static instance: TestEncoder;
  static isConfigSupported = vi.fn(async () => ({ supported: true }));
  state = 'unconfigured';
  encodeQueueSize = 0;
  config?: VideoEncoderConfig;
  close = vi.fn(() => {
    this.state = 'closed';
  });
  configure(config: VideoEncoderConfig) {
    this.config = config;
    this.state = 'configured';
  }
  first = true;
  readonly callbacks: VideoEncoderInit;
  constructor(callbacks: VideoEncoderInit) {
    this.callbacks = callbacks;
    TestEncoder.instance = this;
  }
  encode(frame: TestFrame, { keyFrame }: VideoEncoderEncodeOptions) {
    const bytes = new Uint8Array([0, 0, 0, 1, keyFrame ? 0x65 : 0x41]);
    this.callbacks.output(
      {
        type: keyFrame ? 'key' : 'delta',
        timestamp: frame.timestamp,
        duration: null,
        byteLength: bytes.length,
        copyTo(destination) {
          if (!ArrayBuffer.isView(destination)) throw new Error('Expected an output view');
          new Uint8Array(destination.buffer, destination.byteOffset, destination.byteLength).set(bytes);
        },
      },
      this.first ? { decoderConfig } : undefined,
    );
    this.first = false;
  }
}

class TestAudioData {
  readonly timestamp: number;
  readonly numberOfFrames: number;
  readonly sampleRate: number;
  close = vi.fn();
  constructor(init: AudioDataInit) {
    this.timestamp = init.timestamp;
    this.numberOfFrames = init.numberOfFrames;
    this.sampleRate = init.sampleRate;
  }
}
class TestAudioEncoder {
  static instances: TestAudioEncoder[] = [];
  static isConfigSupported = vi.fn(async () => ({ supported: true }));
  /** Firefox's encoder stamps its output this much before the input it came from; Chromium's does not. */
  static lookaheadUs = 0;
  state = 'unconfigured';
  encodeQueueSize = 0;
  config?: AudioEncoderConfig;
  inputs: number[] = [];
  private firstUs: number | undefined;
  private frames = 0;
  close = vi.fn(() => {
    this.state = 'closed';
  });
  readonly callbacks: AudioEncoderInit;
  constructor(callbacks: AudioEncoderInit) {
    this.callbacks = callbacks;
    TestAudioEncoder.instances.push(this);
  }
  configure(config: AudioEncoderConfig) {
    this.config = config;
    this.state = 'configured';
  }
  encode(data: TestAudioData) {
    this.inputs.push(data.timestamp);
    // Chromium's policy: count samples from the first input, whatever later input timestamps say.
    this.firstUs ??= data.timestamp;
    const countedUs = this.firstUs + Math.round((this.frames * 1_000_000) / data.sampleRate);
    this.frames += data.numberOfFrames;
    const bytes = new Uint8Array([0xfc, this.inputs.length]);
    this.callbacks.output({
      type: 'key',
      timestamp: Math.max(0, countedUs - TestAudioEncoder.lookaheadUs),
      duration: 20_000,
      byteLength: bytes.length,
      copyTo(destination) {
        if (!ArrayBuffer.isView(destination)) throw new Error('Expected an output view');
        new Uint8Array(destination.buffer, destination.byteOffset, destination.byteLength).set(bytes);
      },
    });
  }
}

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] });
  vi.stubGlobal('VideoEncoder', TestEncoder);
  vi.stubGlobal('VideoFrame', TestFrame);
  vi.stubGlobal('AudioEncoder', TestAudioEncoder);
  vi.stubGlobal('AudioData', TestAudioData);
  TestAudioEncoder.instances.length = 0;
  TestAudioEncoder.lookaheadUs = 0;
  frames.length = 0;
  TestEncoder.isConfigSupported.mockResolvedValue({ supported: true });
});
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

function recordingPublisher(
  options: { stallVideo?: boolean; failTrack?: string; initialDemand?: boolean; stallCatalog?: boolean } = {},
) {
  type RecordedObject = { bytes: Uint8Array; properties: readonly PublishProperty[] };
  const tracks = new Map<
    string,
    {
      writer: RelayTrackWriter;
      objects: RecordedObject[][];
      demand: (value: boolean) => void;
      aborted: number;
      finished: number;
    }
  >();
  let disconnect = (_value: { error?: Error }) => {};
  const closed = new Promise<{ error?: Error }>((resolve) => {
    disconnect = resolve;
  });
  const settlingGroups = createSettlingGroups();
  const publisher: RelayPublisher = {
    announced: Promise.resolve(),
    expiresAt: undefined,
    closed,
    close: vi.fn(async () => {}),
    registerTrack({ track }) {
      const objects: RecordedObject[][] = [];
      let alias = 1;
      const slot = createWriteSlot({
        settlingGroups,
        namespaceSolicited: () => true,
        assertOpen() {},
        endTrack() {},
        writeTimeoutMs: 10000,
        trackTimeouts: NO_DELIVERY_TIMEOUTS,
        subscriberTimeouts: () => NO_DELIVERY_TIMEOUTS,
        async openSubgroup(_alias, groupId) {
          const group: RecordedObject[] = [];
          objects.push(group);
          let rejectWrite: ((error: unknown) => void) | undefined;
          let finished = false;
          const fin = async () => {
            if (!finished) {
              finished = true;
              record.finished++;
            }
          };
          return {
            groupId,
            async writeObject(object) {
              if (options.failTrack === track) throw new Error(`${track} failed`);
              if ((options.stallVideo && track === 'video') || (options.stallCatalog && track === 'catalog')) {
                await new Promise<void>((_resolve, reject) => {
                  rejectWrite = reject;
                });
              }
              group.push({ bytes: object.payload.slice(), properties: object.properties ?? [] });
            },
            fin,
            handOverFin: fin,
            abort(reason) {
              if (!finished) {
                finished = true;
                record.aborted++;
                rejectWrite?.(reason);
              }
            },
          };
        },
      });
      const record = {
        writer: slot.writer,
        objects,
        aborted: 0,
        finished: 0,
        demand(value: boolean) {
          slot.bind(value ? ++alias : undefined);
        },
      };
      if (track !== 'video' || options.initialDemand !== false) slot.bind(alias);
      tracks.set(track, record);
      return slot.writer;
    },
  };
  return { publisher, tracks, disconnect };
}

function run(options: Parameters<typeof recordingPublisher>[0] = {}, microphone?: MicrophoneCapture) {
  const recorded = recordingPublisher(options);
  const stop = new AbortController();
  const result = publishCanvas(canvas, recorded.publisher, ['demo'], {
    signal: stop.signal,
    ...(microphone ? { microphone } : { clock: testClock() }),
  });
  return { ...recorded, stop, result };
}

/** A broadcast clock pinned at epoch 0, so timestamps count from its origin and read as the test's own times. */
const testClock = () => createBroadcastClock({ originEpochUs: 0 });

/** A microphone whose blocks the test delivers, with context 0 at broadcast time 5 ms. */
function testMicrophone() {
  const clock = testClock();
  const timeline = clock.sourceTimeline();
  timeline.calibrate(0, clock.originUs + 5_000);
  const microphone: MicrophoneCapture = {
    clock,
    context: { sampleRate: 48_000 } as AudioContext,
    onblock: undefined,
    useStream() {},
    async useContext() {},
    close() {},
  };
  const deliver = (contextUs: number, on: SourceTimeline = timeline, captureLatencyUs = on.captureLatencyUs) =>
    microphone.onblock?.({
      samples: new Float32Array(960),
      sampleRate: 48_000,
      contextUs,
      timeline: on,
      captureLatencyUs,
    });
  return { clock, timeline, microphone, deliver };
}

const timestampsOf = (groups: { properties: readonly PublishProperty[] }[][]) =>
  groups.map((group) => group.map(({ properties }) => properties.find(({ type }) => type === LOC_TIMESTAMP)?.value));

it('publishes native encoder output and retained config in keyframe groups, then releases resources', async () => {
  const session = run();
  const completion = expect(session.result).rejects.toThrow('Stopped');
  await vi.advanceTimersByTimeAsync(2100);
  const video = session.tracks.get('video')!;
  expect(video.objects.length).toBe(2);
  expect(video.finished).toBe(1);
  for (const group of video.objects) {
    expect(group[0]!.bytes).toEqual(new Uint8Array([0, 0, 0, 1, 0x65]));
    expect(group[0]!.properties).toContainEqual({ type: LOC_VIDEO_CONFIG, value: description });
    for (const frame of group.slice(1)) {
      expect(frame.bytes[4]).toBe(0x41);
      expect(frame.properties.some((property) => property.type === LOC_VIDEO_CONFIG)).toBe(false);
    }
  }
  expect(video.objects[0]![1]!.properties).toContainEqual({ type: LOC_TIMESTAMP, value: 33000 });
  const catalog = session.tracks.get('catalog')!;
  const document = JSON.parse(new TextDecoder().decode(catalog.objects[0]![0]!.bytes));
  expect(document.initDataList[0].data).toBe(Buffer.from(description).toString('base64'));
  expect(document.tracks[0]).toMatchObject({ bitrate: 1_500_000, framerate: 30 });
  catalog.demand(true); // replacement subscriptions also need the catalog
  await vi.advanceTimersByTimeAsync(0);
  expect(catalog.objects).toHaveLength(2);
  session.stop.abort(new Error('Stopped'));
  await completion;
  expect([...session.tracks.values()].every(({ writer }) => writer.closed)).toBe(true);
  expect(TestEncoder.instance.close).toHaveBeenCalled();
  expect(frames.every((frame) => frame.close.mock.calls.length === 1)).toBe(true);
  expect(session.publisher.close).not.toHaveBeenCalled();
});

it('publishes a media timeline entry for each keyframe group that went out', async () => {
  const session = run();
  const completion = expect(session.result).rejects.toThrow('Stopped');
  await vi.advanceTimersByTimeAsync(2100);
  const video = session.tracks.get('video')!;
  const timeline = session.tracks.get('timeline')!;
  const catalog = JSON.parse(new TextDecoder().decode(session.tracks.get('catalog')!.objects[0]![0]!.bytes));
  expect(catalog.tracks.find((track: { name: string }) => track.name === 'timeline')).toMatchObject({
    packaging: 'mediatimeline',
    depends: ['video'],
  });
  // One independent document per entry, each in its own group.
  expect(timeline.objects.map((group) => group.length)).toEqual([1, 1]);
  const entries = parseMediaTimeline(timeline.objects[1]![0]!.bytes);
  const keyframes = timestampsOf(video.objects).map((group) => Math.floor(Number(group[0]) / 1000));
  expect(entries.map(({ group, object, mediaTimeMs }) => [group, object, mediaTimeMs])).toEqual([
    [0, 0, keyframes[0]],
    [1, 0, keyframes[1]],
  ]);
  // The broadcast timeline is Unix-epoch time (pinned at 0 here), so each record's two times agree.
  for (const { mediaTimeMs, wallclockMs } of entries) expect(wallclockMs).toBe(mediaTimeMs);
  timeline.demand(true); // a replacement subscription gets the latest document again
  await vi.advanceTimersByTimeAsync(0);
  expect(timeline.objects[2]![0]!.bytes).toEqual(timeline.objects[1]![0]!.bytes);
  session.stop.abort(new Error('Stopped'));
  await completion;
});

it('indexes no keyframe the relay had no subscription for', async () => {
  const session = run({ initialDemand: false });
  const completion = session.result.catch((error) => error);
  try {
    await vi.advanceTimersByTimeAsync(400);
    expect(session.tracks.get('timeline')!.objects).toHaveLength(0);
    session.tracks.get('video')!.demand(true);
    await vi.advanceTimersByTimeAsync(34);
    const timeline = session.tracks.get('timeline')!;
    expect(timeline.objects).toHaveLength(1);
    expect(parseMediaTimeline(timeline.objects[0]![0]!.bytes).map(({ group }) => group)).toEqual([0]);
  } finally {
    session.stop.abort(new Error('Stopped'));
    await completion;
  }
});

it('resets on a demand gap between frames and resumes with a keyframe', async () => {
  const session = run();
  const completion = expect(session.result).rejects.toThrow('Stopped');
  await vi.advanceTimersByTimeAsync(34);
  const video = session.tracks.get('video')!;
  video.demand(false);
  await vi.advanceTimersByTimeAsync(0);
  video.demand(true);
  await vi.advanceTimersByTimeAsync(34);
  expect(video.aborted).toBe(1);
  expect(video.objects).toHaveLength(2);
  expect(video.objects[1]![0]!.bytes[4]).toBe(0x65);
  session.stop.abort(new Error('Stopped'));
  await completion;
});

it.each(['video', 'catalog', 'timeline'])('keeps publishing through %s write failures', async (failTrack) => {
  const session = run({ failTrack });
  let settled = false;
  const completion = session.result.finally(() => {
    settled = true;
  });
  completion.catch(() => {});
  // A failed group is an interruption the frame writer absorbs; the services retry while demand lasts.
  await vi.advanceTimersByTimeAsync(2100);
  expect(settled).toBe(false);
  expect(TestEncoder.instance.state).toBe('configured');
  const healthy = failTrack === 'video' ? 'catalog' : 'video';
  expect(session.tracks.get(healthy)!.objects.length).toBeGreaterThan(0);
  session.stop.abort(new Error('Stopped'));
  await expect(completion).rejects.toThrow('Stopped');
  expect([...session.tracks.values()].every(({ writer }) => writer.closed)).toBe(true);
  expect(TestEncoder.instance.close).toHaveBeenCalled();
});

it.each(['video', 'catalog'])('stops and cleans up when the %s track ends between frames', async (name) => {
  const session = run();
  const completion = expect(session.result).rejects.toThrow(`The ${name} track ended`);
  await vi.advanceTimersByTimeAsync(100);
  session.tracks.get(name)!.writer.close();
  await vi.advanceTimersByTimeAsync(0);
  await completion;
  expect([...session.tracks.values()].every(({ writer }) => writer.closed)).toBe(true);
  expect(TestEncoder.instance.close).toHaveBeenCalled();
});

it('drops a burst the stalled transport cannot take, asks for a keyframe, and keeps publishing', async () => {
  const encode = TestEncoder.prototype.encode;
  let calls = 0;
  vi.spyOn(TestEncoder.prototype, 'encode').mockImplementation(function (this: TestEncoder, frame, options) {
    // The second capture comes out as a burst of ten deltas while the first group's keyframe is stalled.
    const burst = ++calls === 2 ? 10 : 1;
    for (let i = 0; i < burst; i++) encode.call(this, frame, burst > 1 ? { keyFrame: false } : options);
  });
  const keyFrames: boolean[] = [];
  const encodeSpy = vi.mocked(TestEncoder.prototype.encode);
  const session = run({ stallVideo: true });
  let settled = false;
  const completion = session.result.finally(() => {
    settled = true;
  });
  completion.catch(() => {});
  await vi.advanceTimersByTimeAsync(400);
  for (const [, options] of encodeSpy.mock.calls) keyFrames.push(Boolean(options?.keyFrame));
  expect(settled).toBe(false);
  // Eight deltas wait behind the stalled keyframe and the ninth overflows: the group and its backlog are dropped.
  const video = session.tracks.get('video')!;
  expect(video.aborted).toBe(1);
  // The writer asked for a keyframe at once, instead of waiting out the 2 s interval.
  expect(keyFrames.slice(0, 3)).toEqual([true, false, true]);
  // Capture waits while frames are waiting, so a stall costs encoded frames, not an unbounded queue of them.
  expect(frames.length).toBeLessThan(5);
  session.stop.abort(new Error('Stopped'));
  await expect(completion).rejects.toThrow('Stopped');
});

it('cancels a stalled video write and preserves the caller abort reason', async () => {
  const session = run({ stallVideo: true });
  const reason = new Error('Stop stalled write');
  const completion = expect(session.result).rejects.toBe(reason);
  await vi.advanceTimersByTimeAsync(34);
  session.stop.abort(reason);
  await completion;
});

it('stops when the encoder reports an error or the publisher disconnects', async () => {
  for (const source of ['encoder', 'publisher']) {
    const session = run();
    const reason = new DOMException(`${source} stopped`);
    const completion = expect(session.result).rejects.toBe(reason);
    await vi.advanceTimersByTimeAsync(0);
    if (source === 'encoder') TestEncoder.instance.callbacks.error(reason);
    else session.disconnect({ error: reason });
    await completion;
    expect([...session.tracks.values()].every(({ writer }) => writer.closed)).toBe(true);
  }
});

it('rejects unsupported encoding and pre-aborted calls before registering tracks', async () => {
  TestEncoder.isConfigSupported.mockResolvedValue({ supported: false });
  const session = run();
  await expect(session.result).rejects.toThrow('cannot encode');
  expect(session.tracks.size).toBe(0);
  const signal = AbortSignal.abort(new Error('Already stopped'));
  await expect(publishCanvas(canvas, session.publisher, ['demo'], { signal })).rejects.toThrow('Already stopped');
  expect(session.tracks.size).toBe(0);
});

it('stops on decoder configuration changes instead of publishing a stale catalog', async () => {
  const session = run();
  const completion = expect(session.result).rejects.toThrow('configuration changed');
  await vi.advanceTimersByTimeAsync(0);
  const output = TestEncoder.instance.callbacks.output;
  TestEncoder.instance.callbacks.output = (chunk) =>
    output(chunk, { decoderConfig: { ...decoderConfig, codedWidth: 320 } });
  await vi.advanceTimersByTimeAsync(34);
  await completion;
  expect(session.tracks.get('catalog')!.objects).toHaveLength(1);
  expect(session.tracks.get('video')!.objects[0]).toHaveLength(1);
});

it('skips capture while transport is stalled instead of filling the encoded queue', async () => {
  const session = run({ stallVideo: true });
  const completion = session.result.catch((error) => error);
  try {
    await vi.advanceTimersByTimeAsync(400);
    expect(TestEncoder.instance.state).toBe('configured');
    expect(frames.length).toBeLessThanOrEqual(2);
  } finally {
    session.stop.abort(new Error('Stopped'));
    await completion;
  }
});

it.each(['idle', 'writing'] as const)('recovers a replaced video subscription while %s', async (phase) => {
  const options = { stallVideo: phase === 'writing' };
  const session = run(options);
  const completion = session.result.catch((error) => error);
  try {
    await vi.advanceTimersByTimeAsync(34);
    const video = session.tracks.get('video')!;
    options.stallVideo = false;
    video.demand(true);
    await vi.advanceTimersByTimeAsync(100);
    expect(TestEncoder.instance.state).toBe('configured');
    expect(video.objects).toHaveLength(2);
    expect(video.objects[1]![0]!.bytes[4]).toBe(0x65);
  } finally {
    session.stop.abort(new Error('Stopped'));
    await completion;
  }
});

it('serves a replacement catalog subscription after its previous write is interrupted', async () => {
  const options = { stallCatalog: true };
  const session = run(options);
  const completion = session.result.catch((error) => error);
  try {
    await vi.advanceTimersByTimeAsync(34);
    const catalog = session.tracks.get('catalog')!;
    options.stallCatalog = false;
    catalog.demand(true);
    // The interrupted write is retried with the catalog service's one-second pause.
    await vi.advanceTimersByTimeAsync(1000);
    expect(TestEncoder.instance.state).toBe('configured');
    expect(catalog.objects[1]).toHaveLength(1);
  } finally {
    session.stop.abort(new Error('Stopped'));
    await completion;
  }
});

it('encodes only enough to obtain the catalog before video demand arrives', async () => {
  const session = run({ initialDemand: false });
  const completion = session.result.catch((error) => error);
  try {
    await vi.advanceTimersByTimeAsync(400);
    expect(frames).toHaveLength(1);
    expect(session.tracks.get('catalog')!.objects[0]).toHaveLength(1);
    session.tracks.get('video')!.demand(true);
    await vi.advanceTimersByTimeAsync(34);
    expect(session.tracks.get('video')!.objects[0]![0]!.bytes[4]).toBe(0x65);
  } finally {
    session.stop.abort(new Error('Stopped'));
    await completion;
  }
});

it.each([
  ['counts samples, as Chromium does', 0, [5_000, 25_000, 65_000, 100_000]],
  ['stamps before its input by its lookahead, as Firefox does', 6_500, [5_000, 18_500, 38_500, 100_000]],
])(
  'publishes the microphone as Opus on the broadcast timeline when the encoder %s',
  async (_, lookaheadUs, expected) => {
    TestAudioEncoder.lookaheadUs = lookaheadUs;
    const mic = testMicrophone();
    const session = run({}, mic.microphone);
    const completion = expect(session.result).rejects.toThrow('Stopped');
    await vi.advanceTimersByTimeAsync(0);
    // Three blocks, the third after a stretch the worklet never saw, then a replaced context with its own timeline.
    mic.deliver(0);
    mic.deliver(20_000);
    mic.deliver(60_000);
    const replaced = mic.clock.sourceTimeline();
    replaced.calibrate(0, mic.clock.originUs + 100_000);
    mic.deliver(0, replaced);
    await vi.advanceTimersByTimeAsync(0);
    const audio = session.tracks.get('audio')!;
    // One group per frame, each stamped where its block was captured.
    expect(audio.objects.map((group) => group.length)).toEqual([1, 1, 1, 1]);
    expect(timestampsOf(audio.objects)).toEqual(expected.map((value) => [value]));
    // The encoder sees one contiguous count; a new context gets a new encoder.
    const [first, second] = TestAudioEncoder.instances;
    expect(first?.inputs).toEqual([0, 20_000, 40_000]);
    expect(first?.close).toHaveBeenCalled();
    expect(second?.inputs).toEqual([0]);
    expect(first?.config).toMatchObject({ codec: 'opus', sampleRate: 48_000, numberOfChannels: 1, bitrate: 64_000 });
    const catalog = JSON.parse(new TextDecoder().decode(session.tracks.get('catalog')!.objects[0]![0]!.bytes));
    expect(catalog.tracks[1]).toMatchObject({
      name: 'audio',
      codec: 'opus',
      samplerate: 48_000,
      channelConfig: '1',
      bitrate: 64_000,
      renderGroup: 1,
    });
    session.stop.abort(new Error('Stopped'));
    await completion;
    expect(audio.writer.closed).toBe(true);
    expect(mic.microphone.onblock).toBeUndefined();
    expect(second?.close).toHaveBeenCalled();
  },
);

it('stamps audio with the capture latency of the microphone it came from, not the one switched to since', async () => {
  const mic = testMicrophone();
  const session = run({}, mic.microphone);
  const completion = expect(session.result).rejects.toThrow('Stopped');
  await vi.advanceTimersByTimeAsync(0);
  mic.timeline.captureLatencyUs = 30_000;
  mic.deliver(40_000);
  // Captured on the 30 ms microphone, encoded after a switch to a 190 ms one.
  mic.timeline.captureLatencyUs = 190_000;
  mic.deliver(60_000, mic.timeline, 30_000);
  await vi.advanceTimersByTimeAsync(0);
  expect(timestampsOf(session.tracks.get('audio')!.objects)).toEqual([[15_000], [35_000]]);
  session.stop.abort(new Error('Stopped'));
  await completion;
});

it('encodes every block without audio demand, and sends nothing stamped before the epoch', async () => {
  const mic = testMicrophone();
  const session = run({}, mic.microphone);
  const completion = expect(session.result).rejects.toThrow('Stopped');
  await vi.advanceTimersByTimeAsync(0);
  const audio = session.tracks.get('audio')!;
  audio.demand(false);
  mic.deliver(0);
  mic.deliver(20_000);
  audio.demand(true);
  mic.deliver(40_000);
  // A context whose time 0 came before epoch 0 on this test clock: its first block has no place on the timeline.
  const early = mic.clock.sourceTimeline();
  early.calibrate(0, mic.clock.originUs - 10_000);
  mic.deliver(0, early);
  mic.deliver(20_000, early);
  await vi.advanceTimersByTimeAsync(0);
  expect(TestAudioEncoder.instances[0]?.inputs).toEqual([0, 20_000, 40_000]);
  expect(timestampsOf(audio.objects)).toEqual([[45_000], [10_000]]);
  session.stop.abort(new Error('Stopped'));
  await completion;
});

it('stamps video from the broadcast clock and refuses a microphone on another clock', async () => {
  const mic = testMicrophone();
  vi.advanceTimersByTime(1000);
  const session = run({}, mic.microphone);
  const completion = expect(session.result).rejects.toThrow('Stopped');
  await vi.advanceTimersByTimeAsync(34);
  // The microphone's clock started a second before publishing did.
  expect(timestampsOf(session.tracks.get('video')!.objects)[0]).toEqual([1_000_000, 1_033_000]);
  session.stop.abort(new Error('Stopped'));
  await completion;
  const signal = new AbortController().signal;
  await expect(
    publishCanvas(canvas, recordingPublisher().publisher, ['demo'], {
      signal,
      microphone: mic.microphone,
      clock: createBroadcastClock(),
    }),
  ).rejects.toThrow('same broadcast clock');
});
