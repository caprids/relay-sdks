/**
 * The video player scenario's dedicated worker: plays the frames the page posts with `createVideoPlayer` and
 * `canvasRenderer` onto the `OffscreenCanvas` the page transferred, on a playout clock of its own (wall pacing). It
 * imports the built package by path, since a module worker does not see the page's import map.
 */
import {
  canvasRenderer,
  createPlayoutClock,
  createVideoPlayer,
  type VideoPlayer,
  type VideoPlayerFrame,
  type VideoPlayerJoinPhases,
  type VideoPlayerStats,
} from '../../packages/moq-media/dist/index.js';
import { countDecodedFrames, type DecodedFrameCounts, readPixel } from './instrument.js';

/** What the page posts to the worker. */
export type WorkerCommand =
  | { type: 'start'; canvas: OffscreenCanvas; decoderConfig: VideoDecoderConfig }
  | { type: 'frame'; frame: VideoPlayerFrame }
  | { type: 'stop'; points: [x: number, y: number][] };

/** What the worker posts back when it stops. */
export interface WorkerResult {
  inWorker: boolean;
  stats: VideoPlayerStats;
  joinPhases: VideoPlayerJoinPhases;
  /** The timestamp and clock offset of each frame shown. */
  shown: [timestampUs: number, offsetUs: number | null][];
  errors: string[];
  counts: DecodedFrameCounts;
  pixels: number[][];
}

const scope = globalThis as unknown as {
  onmessage: ((event: MessageEvent<WorkerCommand>) => void) | null;
  postMessage(message: WorkerResult): void;
};
const counts = countDecodedFrames();
const shown: WorkerResult['shown'] = [];
const errors: string[] = [];
let player: VideoPlayer | undefined;
let canvas: OffscreenCanvas | undefined;

scope.onmessage = ({ data: command }) => {
  if (command.type === 'start') {
    canvas = command.canvas;
    const draw = canvasRenderer(canvas);
    player = createVideoPlayer({
      clock: createPlayoutClock(),
      decoderConfig: command.decoderConfig,
      render(frame) {
        draw(frame);
        shown.push([frame.timestamp, player?.stats.lastOffsetUs ?? null]);
      },
      onError: (error) => errors.push(String(error)),
    });
  } else if (command.type === 'frame') player?.push(command.frame);
  else {
    if (!player || !canvas) throw new Error('The worker was stopped before it started');
    const pixels = command.points.map(([x, y]) => readPixel(canvas as OffscreenCanvas, x, y));
    player.close();
    scope.postMessage({
      inWorker: typeof document === 'undefined',
      stats: { ...player.stats },
      joinPhases: { ...player.joinPhases },
      shown,
      errors,
      counts: { ...counts },
      pixels,
    });
  }
};
