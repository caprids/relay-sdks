import { LOC_TIMESTAMP, LOC_VIDEO_CONFIG } from '@mux/moq-media';
import type { PublishProperty, RelayPublisher, RelayTrackWriter } from '@mux/relay';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { publishCanvas } from '../examples/webcodecs/publish-canvas.js';
import { createWriteSlot } from '../src/write-slot.js';

vi.mock('@mux/relay', async () => import('../src/index.js'));

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

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] });
  vi.stubGlobal('VideoEncoder', TestEncoder);
  vi.stubGlobal('VideoFrame', TestFrame);
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
  const publisher: RelayPublisher = {
    announced: Promise.resolve(),
    closed,
    close: vi.fn(async () => {}),
    registerTrack({ track }) {
      const objects: RecordedObject[][] = [];
      let alias = 1;
      const slot = createWriteSlot({
        namespaceSolicited: () => true,
        assertOpen() {},
        endTrack() {},
        writeTimeoutMs: 10000,
        async openSubgroup(_alias, groupId) {
          const group: RecordedObject[] = [];
          objects.push(group);
          let rejectWrite: ((error: unknown) => void) | undefined;
          let finished = false;
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
            async fin() {
              if (!finished) {
                finished = true;
                record.finished++;
              }
            },
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

function run(options: Parameters<typeof recordingPublisher>[0] = {}) {
  const recorded = recordingPublisher(options);
  const stop = new AbortController();
  const result = publishCanvas(canvas, recorded.publisher, ['demo'], { signal: stop.signal });
  return { ...recorded, stop, result };
}

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

it.each(['video', 'catalog'])('stops and cleans up on a %s write error', async (failTrack) => {
  const session = run({ failTrack });
  await expect(session.result).rejects.toThrow(`${failTrack} failed`);
  expect([...session.tracks.values()].every(({ writer }) => writer.closed)).toBe(true);
  expect(TestEncoder.instance.close).toHaveBeenCalled();
});

it('bounds a burst of encoder output and cancels a stalled write on overflow', async () => {
  const encode = TestEncoder.prototype.encode;
  vi.spyOn(TestEncoder.prototype, 'encode').mockImplementation(function (this: TestEncoder, frame, options) {
    for (let i = 0; i < 9; i++) encode.call(this, frame, options);
  });
  const session = run({ stallVideo: true });
  const completion = expect(session.result).rejects.toThrow('queue is full');
  await vi.advanceTimersByTimeAsync(400);
  await completion;
  expect(frames).toHaveLength(1);
  expect([...session.tracks.values()].every(({ writer }) => writer.closed)).toBe(true);
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
    await vi.advanceTimersByTimeAsync(34);
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
