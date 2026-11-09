/**
 * The video player scenario, run inside one browser page (issue #478). The WebCodecs example publishes a canvas, a
 * picture of two colored halves and a moving square, with a synthetic microphone. Three players play the video back
 * with `createVideoPlayer` and `canvasRenderer`:
 *
 * - `wall`: on a playout clock of its own, so the wall clock paces it, onto a canvas element.
 * - `audio`: on the audio-only listener example's clock, so the audio it plays drives it, onto another canvas element.
 * - `worker`: in a dedicated module worker, the `wall` player's frames posted to it, onto an `OffscreenCanvas` the
 *   page transferred to it, on a wall-paced clock of the worker's own.
 *
 * Once all three show video, the scenario measures the frames published over the next `seconds` of the broadcast
 * timeline: how many each player showed, and the clock's offset for each. Every frame a decoder outputs is counted, as
 * is each close.
 */
import {
  CATALOG_TRACK,
  canvasRenderer,
  createBroadcastClock,
  createFrameSequencer,
  createPlayoutClock,
  createVideoPlayer,
  LOC_VIDEO_CONFIG,
  type MsfCatalogTrack,
  type PlayoutClock,
  parseMsfCatalog,
  type SequencedFrame,
  VIDEO_TRACK,
  type VideoPlayer,
  videoDecoderConfigFromTrack,
} from '@caprid/moq-media';
import {
  connectRelayPublisher,
  connectRelaySubscriber,
  connectSupervisedSubscriber,
  type RelaySubscriber,
  type RelaySubscription,
  type RelayTransportFactory,
} from '@caprid/relay';
import { createFrameReader, type FrameReader } from '@caprid/relay/media';
import { captureMicrophone } from '../../examples/webcodecs/capture-microphone.js';
import { listenAudio } from '../../examples/webcodecs/listen-audio.js';
import { publishCanvas } from '../../examples/webcodecs/publish-canvas.js';
import { countDecodedFrames, readPixel, recordEncodedTimestamps } from './instrument.js';
import type { WorkerCommand, WorkerResult } from './worker.js';

/** Where the publisher and the subscriber connect. */
export type ScenarioTransport =
  | { kind: 'relay'; endpoint: string; token: string | undefined }
  | { kind: 'hub'; publisher: RelayTransportFactory; subscriber: RelayTransportFactory };

/** Options for {@link runVideoPlayerScenario}. */
export interface ScenarioOptions {
  transport: ScenarioTransport;
  namespace: string[];
  /** Seconds of the broadcast timeline measured. Default 8. */
  seconds?: number;
  report(value: object): unknown;
}

const WIDTH = 320;
const HEIGHT = 180;
const LEFT = [230, 60, 30];
const RIGHT = [30, 90, 230];
/** Where the picture is read back: the middle of each half, clear of the moving square. */
const POINTS: [x: number, y: number][] = [
  [WIDTH / 4, HEIGHT / 3],
  [(3 * WIDTH) / 4, HEIGHT / 3],
];

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const quantile = (values: number[], q: number) => {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))];
};
const rgb = ([red, green, blue]: number[]) => `rgb(${red}, ${green}, ${blue})`;

/** The catalog's video track: the first catalog object read. */
async function readVideoTrack(subscriber: RelaySubscriber, namespace: readonly string[]) {
  let subscription: RelaySubscription | undefined;
  try {
    return await new Promise<MsfCatalogTrack>((resolve, reject) => {
      subscription = subscriber.subscribe(
        { namespace: [...namespace], track: CATALOG_TRACK },
        {
          onObject(object) {
            if (object.status !== 'normal') return;
            const { tracks } = parseMsfCatalog(object.payload, { catalogNamespace: namespace });
            const track = tracks.find((candidate) => candidate.name === VIDEO_TRACK);
            if (track) resolve(track);
            else reject(new Error('The catalog lists no video track'));
          },
          onError: reject,
        },
      );
    });
  } finally {
    subscription?.cancel();
  }
}

/** What one player showed: each frame's timestamp and the clock's offset for it. */
interface Played {
  player: VideoPlayer;
  shown: [timestampUs: number, offsetUs: number | null][];
  canvas: HTMLCanvasElement;
}

/** Run the scenario; resolves with its measurements. */
export async function runVideoPlayerScenario(options: ScenarioOptions) {
  const { transport } = options;
  const seconds = options.seconds ?? 8;
  const url = transport.kind === 'relay' ? transport.endpoint : 'https://hub.invalid/';
  const getToken = () => (transport.kind === 'relay' ? transport.token : undefined);
  const counts = countDecodedFrames();
  const published = recordEncodedTimestamps();
  const publisher = await connectRelayPublisher({
    url,
    namespace: options.namespace,
    getToken,
    ...(transport.kind === 'hub' ? { createTransport: transport.publisher } : {}),
  });
  const subscriber = await connectRelaySubscriber({
    url,
    getToken,
    ...(transport.kind === 'hub' ? { createTransport: transport.subscriber } : {}),
  });
  // listenAudio takes a supervised subscriber; the video players subscribe on the plain one.
  const audioSubscriber = await connectSupervisedSubscriber({
    url,
    getToken,
    ...(transport.kind === 'hub' ? { createTransport: transport.subscriber } : {}),
  });
  const failures: unknown[] = [];
  const stop = new AbortController();
  const tone = new AudioContext({ sampleRate: 48_000 });
  const destination = tone.createMediaStreamDestination();
  const oscillator = new OscillatorNode(tone, { frequency: 440 });
  oscillator.connect(new GainNode(tone, { gain: 0.3 })).connect(destination);
  oscillator.start();
  const captureContext = new AudioContext({ sampleRate: 48_000 });
  const broadcast = createBroadcastClock();
  const microphone = await captureMicrophone(broadcast, destination.stream, {
    context: captureContext,
    captureLatencyUs: () => 0,
  });
  const source = new OffscreenCanvas(WIDTH, HEIGHT);
  const draw = source.getContext('2d');
  if (!draw) throw new Error('No 2D canvas context');
  let tick = 0;
  const paint = () => {
    draw.fillStyle = rgb(LEFT);
    draw.fillRect(0, 0, WIDTH / 2, HEIGHT);
    draw.fillStyle = rgb(RIGHT);
    draw.fillRect(WIDTH / 2, 0, WIDTH / 2, HEIGHT);
    draw.fillStyle = 'white';
    draw.fillRect((tick++ * 4) % (WIDTH - 16), HEIGHT - 24, 16, 16);
  };
  paint();
  const painter = setInterval(paint, 1000 / 30);
  const publishing = publishCanvas(source, publisher, options.namespace, { signal: stop.signal, microphone });
  publishing.catch((error) => stop.signal.aborted || failures.push(error));
  await publisher.announced;

  const playback = new AudioContext({ sampleRate: 48_000, latencyHint: 'interactive' });
  await playback.resume();
  let listener: Awaited<ReturnType<typeof listenAudio>> | undefined;
  const readers: FrameReader[] = [];
  const subscriptions: RelaySubscription[] = [];
  const players: Played[] = [];
  const worker = new Worker(new URL('./worker.js', import.meta.url), { type: 'module' });
  worker.onerror = (event) => failures.push(new Error(`Worker: ${event.message}`));
  const post = (command: WorkerCommand, transfer: Transferable[] = []) => worker.postMessage(command, transfer);
  let posted = 0;
  try {
    for (const deadline = performance.now() + 15_000; !listener; ) {
      try {
        listener = await listenAudio(audioSubscriber, options.namespace, {
          context: playback,
          onError: (error) => failures.push(error),
        });
      } catch (error) {
        // A subscribe that reaches the relay before the announce is rejected (the announce race); retry for a while.
        if (performance.now() > deadline) throw error;
        await sleep(250);
      }
    }
    const decoderConfig = videoDecoderConfigFromTrack(await readVideoTrack(subscriber, options.namespace));

    const offscreen = document.createElement('canvas').transferControlToOffscreen();
    post({ type: 'start', canvas: offscreen, decoderConfig }, [offscreen]);
    const play = (clock: PlayoutClock, onFrame?: (frame: SequencedFrame) => void) => {
      const canvas = document.createElement('canvas');
      const drawFrame = canvasRenderer(canvas);
      const played: Played = {
        canvas,
        shown: [],
        player: createVideoPlayer({
          clock,
          decoderConfig,
          render(frame) {
            drawFrame(frame);
            played.shown.push([frame.timestamp, played.player.stats.lastOffsetUs]);
          },
          onError: (error) => failures.push(error),
        }),
      };
      players.push(played);
      const reader = createFrameReader(createFrameSequencer({ clock }), {
        onFrame(frame) {
          played.player.push(frame);
          onFrame?.(frame);
        },
      });
      readers.push(reader);
      subscriptions.push(
        subscriber.subscribe(
          { namespace: [...options.namespace], track: VIDEO_TRACK },
          { ...reader, onError: (error) => failures.push(error) },
        ),
      );
      return played;
    };
    // Whether the relay passes on the Video Config (0x0D) the publisher puts on each keyframe: it is odd-typed.
    const keyframes = { received: 0, withVideoConfig: 0 };
    const wall = play(createPlayoutClock(), ({ timestampUs, payload, key, discontinuity, properties }) => {
      if (key) {
        keyframes.received++;
        if (properties.some(({ type }) => type === LOC_VIDEO_CONFIG)) keyframes.withVideoConfig++;
      }
      posted++;
      post({ type: 'frame', frame: { timestampUs, payload, key, ...(discontinuity ? { discontinuity } : {}) } });
    });
    const audioClock = listener.clock;
    const audio = play(audioClock);

    // Measure from when every player shows video (the worker's frames are posted with the wall player's) and audio
    // drives the audio player's clock, which in headless Chromium starts about 2 s after the video.
    const joiningAt = performance.now();
    for (const deadline = joiningAt + 15_000; ; await sleep(50)) {
      if (failures.length > 0) throw failures[0];
      if (wall.shown.length > 0 && audio.shown.length > 0 && posted > 0 && audioClock.mode === 'audio') break;
      if (performance.now() > deadline) throw new Error('No video, or no audio driving the clock, within 15 s');
    }
    const fromUs = broadcast.now() + 500_000;
    const untilUs = fromUs + seconds * 1_000_000;
    await options.report({ phase: 'joined', joinMs: Math.round(performance.now() - joiningAt), fromUs, untilUs });
    const modes: Record<string, number> = {};
    const joinedAt = performance.now();
    // When the audio clock's mode changed, in ms since the join.
    const modeChanges: [ms: number, mode: string][] = [];
    const sampler = setInterval(() => {
      const { mode } = audioClock;
      modes[mode] = (modes[mode] ?? 0) + 1;
      if (modeChanges.at(-1)?.[1] !== mode) modeChanges.push([Math.round(performance.now() - joinedAt), mode]);
    }, 20);
    await sleep(seconds * 1000 + 2000);
    clearInterval(sampler);
    if (failures.length > 0) throw failures[0];

    const pixels = (canvas: HTMLCanvasElement) => POINTS.map(([x, y]) => readPixel(canvas, x, y));
    const fromWorker = new Promise<WorkerResult>((resolve) => {
      worker.onmessage = (event: MessageEvent<WorkerResult>) => resolve(event.data);
    });
    post({ type: 'stop', points: POINTS });
    const workerResult = await fromWorker;
    const expected = published.filter((timestampUs) => timestampUs >= fromUs && timestampUs < untilUs);
    const summarize = (
      shown: Played['shown'],
      player: Pick<VideoPlayer, 'stats' | 'joinPhases'>,
      picture: number[][],
      extra: object = {},
    ) => {
      const inWindow = shown.filter(([timestampUs]) => timestampUs >= fromUs && timestampUs < untilUs);
      const timestamps = new Set(inWindow.map(([timestampUs]) => timestampUs));
      const offsets = inWindow.flatMap(([, offsetUs]) => (offsetUs === null ? [] : [Math.abs(offsetUs) / 1000]));
      const { createdUs, firstPushUs, firstDecodedUs, firstShownUs } = player.joinPhases;
      const since = (us: number | null) => (us === null ? null : Math.round((us - createdUs) / 100) / 10);
      const matched = expected.filter((timestampUs) => timestamps.has(timestampUs)).length;
      return {
        shown: matched,
        shownRatio: expected.length > 0 ? Math.round((matched / expected.length) * 1000) / 1000 : 0,
        offsetMs: { p50: quantile(offsets, 0.5), p95: quantile(offsets, 0.95), max: quantile(offsets, 1) },
        joinMs: { firstPush: since(firstPushUs), firstDecoded: since(firstDecodedUs), firstShown: since(firstShownUs) },
        stats: { ...player.stats },
        pixels: picture,
        ...extra,
      };
    };
    const result = {
      published: expected.length,
      seconds,
      expectedPixels: [LEFT, RIGHT],
      players: {
        wall: summarize(wall.shown, wall.player, pixels(wall.canvas)),
        audio: summarize(audio.shown, audio.player, pixels(audio.canvas), {
          modes: { ...modes },
          modeChanges,
          audio: { ...listener.input.stats },
        }),
        worker: summarize(workerResult.shown, workerResult, workerResult.pixels, {
          inWorker: workerResult.inWorker,
          errors: workerResult.errors,
          counts: workerResult.counts,
        }),
      },
      counts: { ...counts },
      keyframes,
    };
    for (const each of players) each.player.close();
    result.counts = { ...counts };
    return result;
  } finally {
    clearInterval(painter);
    worker.terminate();
    for (const reader of readers) reader.close();
    for (const subscription of subscriptions) subscription.cancel();
    for (const each of players) each.player.close();
    stop.abort();
    await listener?.stop();
    microphone.close();
    await publishing.catch(() => {});
    for (const context of [tone, captureContext, playback]) await context.close().catch(() => {});
    audioSubscriber.close();
    await subscriber.close();
    await publisher.close();
  }
}
