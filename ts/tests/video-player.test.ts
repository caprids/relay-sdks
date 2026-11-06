import {
  canvasRenderer,
  createFrameSequencer,
  createPlayoutClock,
  createVideoPlayer,
  LOC_TIMESTAMP,
  type PlayoutClock,
  type SequencedFrame,
  type VideoCanvas,
  type VideoPlayer,
  type VideoPlayerClock,
  type VideoPlayerFrame,
  type VideoPlayerOptions,
  videoDecoderConfigFromTrack,
} from '@caprid/moq-media';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { connectRelayPublisher, connectRelaySubscriber } from '../src/index.js';
import { createFrameReader, createFrameWriter } from '../src/media.js';
import { createRelayHub } from './helpers/relay-hub.js';

// Fake WebCodecs: a decoder that outputs one frame per chunk, in order, when a test releases them (or on a microtask
// with `auto`), and frames that count how often they are closed, so every test checks that none leaks.
const FAIL = 0xee;
const UNSUPPORTED = 0xef;

class FakeFrame {
  static made: FakeFrame[] = [];
  closes = 0;
  readonly timestamp: number;
  readonly displayWidth: number;
  readonly displayHeight: number;
  constructor(timestamp: number, displayWidth = 320, displayHeight = 180) {
    this.timestamp = timestamp;
    this.displayWidth = displayWidth;
    this.displayHeight = displayHeight;
    FakeFrame.made.push(this);
  }
  close() {
    this.closes++;
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

/** Like WebCodecs' decoder: a key chunk is required after configure and reset; an error closes it, then calls back. */
class FakeDecoder {
  static instances: FakeDecoder[] = [];
  static auto = false;
  state: 'unconfigured' | 'configured' | 'closed' = 'unconfigured';
  config: VideoDecoderConfig | undefined;
  /** Each chunk decoded, with the config it decoded with. */
  chunks: { type: string; index: number; codec: string | undefined }[] = [];
  resets = 0;
  pending: FakeChunk[] = [];
  keyRequired = true;
  readonly init: VideoDecoderInit;
  constructor(init: VideoDecoderInit) {
    this.init = init;
    FakeDecoder.instances.push(this);
  }
  configure(config: VideoDecoderConfig) {
    if (this.state === 'closed') throw new DOMException('Closed', 'InvalidStateError');
    if (config.codec === 'invalid') throw new TypeError('Invalid codec');
    this.config = config;
    this.state = 'configured';
    this.keyRequired = true;
  }
  decode(chunk: FakeChunk) {
    if (this.state !== 'configured') throw new DOMException('Not configured', 'InvalidStateError');
    if (this.keyRequired && chunk.type !== 'key') throw new DOMException('A key frame is required', 'DataError');
    this.keyRequired = false;
    this.chunks.push({ type: chunk.type, index: indexOf(chunk.timestamp), codec: this.config?.codec });
    this.pending.push(chunk);
    if (FakeDecoder.auto) queueMicrotask(() => this.release());
  }
  release() {
    for (const chunk of this.pending.splice(0)) {
      if (this.state !== 'configured') return;
      if (chunk.data[0] === FAIL || chunk.data[0] === UNSUPPORTED) {
        this.state = 'closed';
        this.init.error(
          chunk.data[0] === FAIL
            ? new DOMException('Decoding failed', 'EncodingError')
            : new DOMException('Unsupported', 'NotSupportedError'),
        );
        return;
      }
      this.init.output(new FakeFrame(chunk.timestamp) as unknown as VideoFrame);
    }
  }
  reset() {
    if (this.state === 'closed') throw new DOMException('Closed', 'InvalidStateError');
    this.state = 'unconfigured';
    this.pending = [];
    this.resets++;
  }
  close() {
    this.state = 'closed';
    this.pending = [];
  }
}

const BASE_US = 1_000_000_000;
const FRAME_US = 40_000;
const CONFIG: VideoDecoderConfig = { codec: 'avc1.42001f' };
const indexOf = (timestampUs: number) => (timestampUs - BASE_US) / FRAME_US;
const frame = (index: number, isKey: boolean, extra: Partial<VideoPlayerFrame> = {}): VideoPlayerFrame => ({
  timestampUs: BASE_US + index * FRAME_US,
  payload: new Uint8Array([index]),
  key: isKey,
  ...extra,
});
const key = (index: number, extra?: Partial<VideoPlayerFrame>) => frame(index, true, extra);
const delta = (index: number, extra?: Partial<VideoPlayerFrame>) => frame(index, false, extra);
const nowUs = () => Date.now() * 1000;
const decoder = (index = -1) => FakeDecoder.instances.at(index) as FakeDecoder;

const players: VideoPlayer[] = [];
/** A player on a clock that reads the fake timers; `shown` records each frame rendered: its index and when. */
function setUp(options: Partial<VideoPlayerOptions> = {}) {
  const clock = createPlayoutClock({ now: nowUs });
  const shown: [index: number, atUs: number][] = [];
  const player = createVideoPlayer({
    clock,
    decoderConfig: CONFIG,
    render: (rendered) => shown.push([indexOf(rendered.timestamp), nowUs()]),
    ...options,
  });
  players.push(player);
  return { clock, player, shown };
}

beforeEach(() => {
  FakeFrame.made = [];
  FakeDecoder.instances = [];
  FakeDecoder.auto = false;
  vi.stubGlobal('VideoDecoder', FakeDecoder);
  vi.stubGlobal('EncodedVideoChunk', FakeChunk);
});
afterEach(() => {
  for (const player of players.splice(0)) player.close();
  expect(
    FakeFrame.made.map((made) => made.closes),
    'every decoded frame is closed exactly once',
  ).toEqual(FakeFrame.made.map(() => 1));
  vi.useRealTimers();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe('createVideoPlayer', () => {
  beforeEach(() => {
    vi.useFakeTimers({ now: 0 });
  });

  it('shows the first frame at once and each later one when wall pacing reaches it', () => {
    const { player, shown } = setUp();
    player.push(key(0));
    player.push(delta(1));
    player.push(delta(2));
    decoder().release();
    expect(shown).toEqual([[0, 0]]);
    expect(player.stats.queued).toBe(2);
    vi.advanceTimersByTime(40);
    expect(shown).toEqual([
      [0, 0],
      [1, 40_000],
    ]);
    vi.advanceTimersByTime(40);
    expect(shown).toEqual([
      [0, 0],
      [1, 40_000],
      [2, 80_000],
    ]);
    expect(player.stats).toMatchObject({ decoded: 3, shown: 3, dropped: 0, skipped: 0, queued: 0, lastOffsetUs: 0 });
  });

  it('follows the audio clock: drops frames more than 40 ms behind it and holds frames ahead of it', () => {
    const { clock, player, shown } = setUp();
    // Audio 10 ms past frame 3 is audible now.
    clock.reportAudio(BASE_US + 3 * FRAME_US + 10_000);
    for (let index = 0; index < 6; index++) player.push(index === 0 ? key(index) : delta(index));
    decoder().release();
    expect(shown).toEqual([[3, 0]]);
    expect(player.stats).toMatchObject({ dropped: 3, shown: 1, lastOffsetUs: -10_000 });
    vi.advanceTimersByTime(70);
    expect(shown).toEqual([
      [3, 0],
      [4, 30_000],
      [5, 70_000],
    ]);
    expect(clock.mode).toBe('audio');
    expect(player.stats.lastOffsetUs).toBe(0);
  });

  it('asks the clock again within 50 ms while a frame waits, so a frame shows soon after the clock resets', () => {
    const { clock, player, shown } = setUp();
    clock.reportAudio(BASE_US);
    player.push(key(0));
    player.push(delta(20));
    decoder().release();
    // Frame 20 is 800 ms ahead of the audio. Another track's sequencer resyncs and resets the shared clock.
    vi.advanceTimersByTime(20);
    clock.reset();
    vi.advanceTimersByTime(30);
    expect(shown).toEqual([
      [0, 0],
      [20, 50_000],
    ]);
  });

  it('shows or drops what is due at each decoded frame, so only frames ahead of the clock wait while timers stall', () => {
    const { player, shown } = setUp();
    player.push(key(0));
    player.push(delta(1));
    decoder().release();
    expect(player.stats.queued).toBe(1);
    // A hidden tab: a second passes and the timer for frame 1 does not fire, while the decoder outputs frames 2 to 30.
    vi.setSystemTime(Date.now() + 1000);
    for (let index = 2; index <= 30; index++) player.push(delta(index));
    decoder().release();
    expect(shown).toEqual([
      [0, 0],
      [24, 1_000_000],
      [25, 1_000_000],
    ]);
    expect(player.stats).toMatchObject({ dropped: 23, queued: 5 });
  });

  it('waits for a keyframe at the start, skipping the frames before it', () => {
    const { player, shown } = setUp();
    player.push(delta(0));
    player.push(delta(1));
    player.push(key(2));
    player.push(delta(3));
    expect(player.stats.skipped).toBe(2);
    expect(decoder().chunks.map(({ type, index }) => [type, index])).toEqual([
      ['key', 2],
      ['delta', 3],
    ]);
    decoder().release();
    vi.advanceTimersByTime(40);
    expect(shown.map(([index]) => index)).toEqual([2, 3]);
  });

  it('keeps the decoder and the frames waiting across a gap: they are still on the timeline', () => {
    const { player, shown } = setUp();
    player.push(key(0));
    player.push(delta(1));
    player.push(delta(2));
    decoder().release();
    player.push(key(5, { discontinuity: 'gap' }));
    decoder().release();
    vi.advanceTimersByTime(200);
    expect(shown).toEqual([
      [0, 0],
      [1, 40_000],
      [2, 80_000],
      [5, 200_000],
    ]);
    expect(FakeDecoder.instances).toHaveLength(1);
    expect(decoder().resets).toBe(0);
    expect(player.stats).toMatchObject({ decoderResets: 0, skipped: 0 });
  });

  it('skips a frame after a gap that is not a keyframe, and every frame until one, on the same decoder', () => {
    const { player, shown } = setUp();
    player.push(key(0));
    player.push(delta(1));
    player.push(delta(3, { discontinuity: 'gap' }));
    player.push(delta(4));
    player.push(key(5));
    expect(player.stats.skipped).toBe(2);
    expect(decoder().chunks.map(({ index }) => index)).toEqual([0, 1, 5]);
    decoder().release();
    vi.advanceTimersByTime(200);
    expect(shown.map(([index]) => index)).toEqual([0, 1, 5]);
    expect(FakeDecoder.instances).toHaveLength(1);
  });

  it.each(['resync', 'restart'] as const)(
    'at a %s, closes the frames waiting, drops what the decoder holds, and waits for a keyframe',
    (discontinuity) => {
      const { clock, player, shown } = setUp();
      player.push(key(0));
      player.push(delta(1));
      player.push(delta(2));
      decoder().release();
      player.push(delta(3));
      // The sequencer resets the clock and delivers the new timeline, here a minute on.
      clock.reset();
      player.push(delta(1500, { discontinuity }));
      expect(player.stats.queued).toBe(0);
      player.push(delta(1501));
      player.push(key(1502));
      decoder().release();
      vi.advanceTimersByTime(1000);
      expect(shown).toEqual([
        [0, 0],
        [1502, 0],
      ]);
      expect(decoder().resets).toBe(1);
      expect(FakeDecoder.instances).toHaveLength(1);
      expect(player.stats).toMatchObject({ decoderResets: 1, skipped: 2, decoded: 4, shown: 2, dropped: 0 });
    },
  );

  it('replaces a decoder that fails and decodes again from the next keyframe', () => {
    const onError = vi.fn();
    const { player, shown } = setUp({ onError });
    player.push(key(0));
    player.push(delta(1, { payload: new Uint8Array([FAIL]) }));
    decoder().release();
    expect(onError).toHaveBeenCalledOnce();
    expect(onError.mock.calls[0]?.[0]).toMatchObject({ name: 'EncodingError' });
    player.push(delta(2));
    player.push(key(3));
    expect(FakeDecoder.instances).toHaveLength(2);
    decoder().release();
    vi.advanceTimersByTime(200);
    expect(shown.map(([index]) => index)).toEqual([0, 3]);
    expect(player.stats).toMatchObject({ errors: 1, decoderResets: 1, skipped: 1 });
  });

  it('reports a config the decoder refuses once, and decodes once another is configured', () => {
    const onError = vi.fn();
    const { player, shown } = setUp({ decoderConfig: { codec: 'invalid' }, onError });
    player.push(key(0));
    player.push(delta(1));
    player.push(key(2));
    expect(onError).toHaveBeenCalledExactlyOnceWith(expect.any(TypeError));
    expect(FakeDecoder.instances).toHaveLength(1);
    expect(player.stats).toMatchObject({ errors: 1, skipped: 2, decoded: 0 });
    player.configure(CONFIG);
    player.push(key(3));
    decoder().release();
    expect(shown.map(([index]) => index)).toEqual([3]);
  });

  it.each([
    ['fails before its first frame', [key(0, { payload: new Uint8Array([FAIL]) })], 'EncodingError'],
    [
      'does not support the stream',
      [key(0), delta(1, { payload: new Uint8Array([UNSUPPORTED]) })],
      'NotSupportedError',
    ],
  ])('decodes nothing more when the decoder %s, until configure() gives it another config', (_, frames, name) => {
    const onError = vi.fn();
    const { player, shown } = setUp({ onError });
    for (const each of frames) player.push(each);
    decoder().release();
    // A new decoder with the same config would fail the same way, at every keyframe.
    player.push(key(5, { discontinuity: 'resync' }));
    player.push(delta(6));
    player.push(key(7));
    expect(onError).toHaveBeenCalledExactlyOnceWith(expect.objectContaining({ name }));
    expect(FakeDecoder.instances).toHaveLength(1);
    expect(player.stats).toMatchObject({ errors: 1, skipped: 3, decoderResets: 0 });
    player.configure({ codec: 'avc1.64001f' });
    player.push(delta(8));
    player.push(key(9));
    expect(FakeDecoder.instances).toHaveLength(2);
    expect(decoder().chunks).toEqual([{ type: 'key', index: 9, codec: 'avc1.64001f' }]);
    decoder().release();
    vi.advanceTimersByTime(1000);
    expect(shown.map(([index]) => index).at(-1)).toBe(9);
  });

  it('decodes with a new config from the next keyframe, and frames before it with the old one', () => {
    const { player, shown } = setUp();
    player.push(key(0));
    player.push(delta(1));
    player.configure({ codec: 'avc1.64001f' });
    player.push(delta(2));
    player.push(key(3));
    player.push(delta(4));
    expect(decoder().chunks.map(({ index, codec }) => [index, codec])).toEqual([
      [0, 'avc1.42001f'],
      [1, 'avc1.42001f'],
      [2, 'avc1.42001f'],
      [3, 'avc1.64001f'],
      [4, 'avc1.64001f'],
    ]);
    decoder().release();
    vi.advanceTimersByTime(200);
    expect(shown.map(([index]) => index)).toEqual([0, 1, 2, 3, 4]);
    expect(FakeDecoder.instances).toHaveLength(1);
    expect(player.stats).toMatchObject({ skipped: 0, decoderResets: 0 });
  });

  it('reports a render that throws, closes the frame anyway, and plays on', () => {
    const onError = vi.fn();
    const failure = new Error('sink failed');
    const rendered: number[] = [];
    const { player } = setUp({
      onError,
      render(shownFrame) {
        rendered.push(indexOf(shownFrame.timestamp));
        if (rendered.length === 2) throw failure;
      },
    });
    for (let index = 0; index < 3; index++) player.push(index === 0 ? key(index) : delta(index));
    decoder().release();
    vi.advanceTimersByTime(80);
    expect(rendered).toEqual([0, 1, 2]);
    expect(onError).toHaveBeenCalledExactlyOnceWith(failure);
    expect(player.stats).toMatchObject({ shown: 3, errors: 1 });
  });

  it('logs errors to console.error without onError', () => {
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {});
    const { player } = setUp({
      render() {
        throw new Error('sink failed');
      },
    });
    player.push(key(0));
    decoder().release();
    player.push(delta(1, { payload: new Uint8Array([FAIL]) }));
    decoder().release();
    expect(logged).toHaveBeenCalledTimes(2);
    expect(player.stats.errors).toBe(2);
  });

  it('closes the decoder and every frame waiting, then ignores pushes, configs, and late output', () => {
    const { player, shown } = setUp();
    player.push(key(0));
    player.push(delta(1));
    player.push(delta(2));
    decoder().release();
    player.push(delta(3));
    const closing = decoder();
    player.close();
    expect(player.closed).toBe(true);
    expect(closing.state).toBe('closed');
    expect(player.stats.queued).toBe(0);
    // Output a decoder had in flight when it closed is closed too.
    closing.init.output(new FakeFrame(BASE_US + 4 * FRAME_US) as unknown as VideoFrame);
    player.configure({ codec: 'avc1.64001f' });
    player.push(key(10));
    vi.advanceTimersByTime(1000);
    expect(shown).toEqual([[0, 0]]);
    expect(FakeDecoder.instances).toHaveLength(1);
    expect(closing.chunks.map(({ index }) => index)).toEqual([0, 1, 2, 3]);
  });

  it('records when it was created, first pushed to, first decoded, and first showed a frame', () => {
    const { clock, player } = setUp();
    expect(player.joinPhases).toEqual({ createdUs: 0, firstPushUs: null, firstDecodedUs: null, firstShownUs: null });
    vi.advanceTimersByTime(10);
    player.push(key(0));
    vi.advanceTimersByTime(5);
    // Audio 20 ms before the frame is audible now, so the frame waits 20 ms.
    clock.reportAudio(BASE_US - 20_000);
    decoder().release();
    player.push(delta(1));
    vi.advanceTimersByTime(20);
    expect(player.joinPhases).toEqual({
      createdUs: 0,
      firstPushUs: 10_000,
      firstDecodedUs: 15_000,
      firstShownUs: 35_000,
    });
  });
});

describe('canvasRenderer', () => {
  class FakeCanvas {
    resizes = 0;
    drawn: unknown[][] = [];
    #width = 300;
    #height = 150;
    get width() {
      return this.#width;
    }
    set width(value: number) {
      this.resizes++;
      this.#width = value;
    }
    get height() {
      return this.#height;
    }
    set height(value: number) {
      this.resizes++;
      this.#height = value;
    }
    getContext(contextId: string) {
      return contextId === '2d' ? { drawImage: (...args: unknown[]) => this.drawn.push(args) } : null;
    }
  }

  it('draws each frame over the whole canvas, sized to the frame only when its display size changes', () => {
    const canvas = new FakeCanvas();
    const render = canvasRenderer(canvas as unknown as VideoCanvas);
    const frames = [new FakeFrame(0, 640, 360), new FakeFrame(1, 640, 360), new FakeFrame(2, 1280, 720)];
    for (const each of frames) render(each as unknown as VideoFrame);
    expect(canvas.drawn).toEqual([
      [frames[0], 0, 0, 640, 360],
      [frames[1], 0, 0, 640, 360],
      [frames[2], 0, 0, 1280, 720],
    ]);
    expect(canvas.resizes).toBe(4);
    for (const each of frames) each.close();
  });

  it('throws at once for a canvas with no 2D context', () => {
    const canvas = { width: 1, height: 1, getContext: () => null };
    expect(() => canvasRenderer(canvas)).toThrow(/2D context/);
  });
});

describe('videoDecoderConfigFromTrack', () => {
  it('builds a decoder config from a catalog track', () => {
    const initData = new Uint8Array([1, 2]);
    const track = { namespace: ['a'], name: 'video', packaging: 'loc', isLive: true, codec: 'avc1.42001f' };
    expect(videoDecoderConfigFromTrack({ ...track, width: 640, height: 360, initData })).toEqual({
      codec: 'avc1.42001f',
      description: initData,
      codedWidth: 640,
      codedHeight: 360,
      optimizeForLatency: true,
    });
    // WebCodecs refuses a coded width without a coded height.
    expect(videoDecoderConfigFromTrack({ ...track, width: 640 })).toEqual({
      codec: 'avc1.42001f',
      optimizeForLatency: true,
    });
    const { codec: _, ...uncoded } = track;
    expect(() => videoDecoderConfigFromTrack(uncoded)).toThrow(/codec/);
  });
});

describe('createVideoPlayer through the relay hub', () => {
  const url = 'https://relay.example.com/moq';
  const namespace = ['testing', 'video-player'];
  const cleanup: (() => unknown)[] = [];
  afterEach(async () => {
    for (const dispose of cleanup.splice(0).reverse()) await dispose();
  });

  it('plays frames a frame writer publishes, read by a frame reader, in order on the clock', async () => {
    FakeDecoder.auto = true;
    const hub = createRelayHub();
    cleanup.push(() => hub.destroy());
    const publisher = await connectRelayPublisher({ url, namespace, createTransport: hub.connectPublisher });
    cleanup.push(() => publisher.close());
    const subscriber = await connectRelaySubscriber({ url, createTransport: hub.connectSubscriber });
    cleanup.push(() => subscriber.close());
    const track = publisher.registerTrack({ track: 'video' });
    const writer = createFrameWriter(track);

    const clock = createPlayoutClock();
    const shown: { index: number; atUs: number }[] = [];
    const player = createVideoPlayer({
      clock,
      decoderConfig: CONFIG,
      render: (rendered) => shown.push({ index: indexOf(rendered.timestamp), atUs: clock.localNow() }),
    });
    players.push(player);
    const reader = createFrameReader(createFrameSequencer({ clock }), { onFrame: player.push });
    cleanup.push(() => reader.close());
    subscriber.subscribe({ namespace, track: 'video' }, reader);
    await vi.waitFor(() => expect(track.hasDemand).toBe(true));

    for (let index = 0; index < 10; index++) {
      const outcome = await writer.write({
        payload: new Uint8Array([index]),
        key: index % 5 === 0,
        properties: [{ type: LOC_TIMESTAMP, value: BASE_US + index * FRAME_US }],
      });
      expect(outcome.kind).toBe('sent');
    }
    await vi.waitFor(() => expect(shown).toHaveLength(10), { timeout: 2000 });
    expect(shown.map(({ index }) => index)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    // Paced by the wall clock from the first frame: the ninth interval ends 360 ms after the first was shown.
    const spanUs = (shown[9]?.atUs as number) - (shown[0]?.atUs as number);
    expect(spanUs).toBeGreaterThanOrEqual(9 * FRAME_US - 2_000);
    expect(player.stats).toMatchObject({ decoded: 10, shown: 10, dropped: 0, skipped: 0, errors: 0 });
  });
});

// Type-level only, never called: a playout clock is a player's clock, a sequenced frame is a player's frame, and both
// kinds of canvas take a renderer.
function typeCheck(clock: PlayoutClock, frame: SequencedFrame, element: HTMLCanvasElement, offscreen: OffscreenCanvas) {
  const playerClock: VideoPlayerClock = clock;
  const playerFrame: VideoPlayerFrame = frame;
  return [playerClock, playerFrame, canvasRenderer(element), canvasRenderer(offscreen)];
}
void typeCheck;
