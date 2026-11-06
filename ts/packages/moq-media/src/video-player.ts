/**
 * A headless video player: decodes frames with WebCodecs and hands each decoded frame to a sink when the broadcast's
 * playout clock says it is due. No DOM and no transport: it uses only `VideoDecoder`, `EncodedVideoChunk` and timers,
 * so it runs unchanged in a dedicated worker. See spec/media-layer.md, "Video player".
 */

import type { PlayoutClock } from './clock.js';
import type { FrameDiscontinuity, SequencedFrame } from './sequencer.js';

/**
 * What the player calls on its clock: a `PlayoutClock` qualifies. Share the broadcast's clock with its audio input and
 * with the frame reader's sequencer, so video follows the audio that drives it.
 */
export type VideoPlayerClock = Pick<PlayoutClock, 'due' | 'localNow'>;

/** One encoded video frame: a `SequencedFrame` from a frame reader qualifies. */
export type VideoPlayerFrame = Pick<SequencedFrame, 'timestampUs' | 'payload' | 'key'> & {
  /** Set where the frame does not follow the one before; see {@link VideoPlayer.push}. */
  discontinuity?: FrameDiscontinuity | undefined;
};

/** Options for {@link createVideoPlayer}. */
export interface VideoPlayerOptions {
  /** The broadcast's playout clock, which decides when each decoded frame is shown or dropped. */
  clock: VideoPlayerClock;
  /** How to decode. `videoDecoderConfigFromTrack` reads one from the catalog. */
  decoderConfig: VideoDecoderConfig;
  /**
   * Show a frame: called when it is due. The frame is borrowed: the player closes it once this returns, because a
   * hardware decoder has a fixed pool of frames and stalls while they are all held. A sink that keeps the frame keeps
   * `frame.clone()`. An exception goes to `onError` (or `console.error` without one); the frame is still closed and
   * playback carries on. `canvasRenderer(canvas)` draws onto a canvas.
   */
  render(frame: VideoFrame): void;
  /**
   * A decoder error, after which the player replaces the decoder and decodes again from the next keyframe, or an
   * exception from `render`. Without it, both go to `console.error`. A decoder that fails before it output any frame,
   * or with a `NotSupportedError`, fails for good: a new decoder with the same config would fail the same way, so the
   * player decodes nothing more until `configure()` gives it another config.
   */
  onError?(error: unknown): void;
}

/** The player's counters. Counters are cumulative. */
export interface VideoPlayerStats {
  /** Frames the decoder output. */
  decoded: number;
  /** Frames handed to `render`. */
  shown: number;
  /** Decoded frames the clock said to drop, as later than its late-drop limit. */
  dropped: number;
  /** Frames pushed and not decoded: while the player waited for a keyframe, or after the decoder failed for good. */
  skipped: number;
  /** Times the decoder was reset or replaced: at a `resync` or `restart`, and after an error. */
  decoderResets: number;
  /** Decoder errors and exceptions from `render`. */
  errors: number;
  /** Decoded frames waiting to be due now. */
  queued: number;
  /** The clock's offset for the last frame shown, in microseconds: its sync error, positive when early. */
  lastOffsetUs: number | null;
}

/**
 * When each step of the player's start happened, in microseconds on its clock's local time source; `null` until it
 * happens. The object is updated in place.
 */
export interface VideoPlayerJoinPhases {
  /** `createVideoPlayer` returned. */
  createdUs: number;
  /** The first frame was pushed. */
  firstPushUs: number | null;
  /** The decoder output its first frame. */
  firstDecodedUs: number | null;
  /** The first frame was handed to `render`. */
  firstShownUs: number | null;
}

/** Plays one video track: frames in, decoded frames out to `render` on the clock. */
export interface VideoPlayer {
  /**
   * Decode a frame, in decode order. A frame reader's `onFrame` can be this. The player waits for a keyframe at the
   * start, after a decoder error, and after a `resync` or `restart`, skipping the frames before it. A `resync` or
   * `restart` also resets the decoder and closes every decoded frame still waiting: they belong to the replaced
   * timeline. A `gap` keeps both, since the frames before it are still on the timeline; a frame after a gap that is not
   * a keyframe is skipped, as is every frame after it until one.
   */
  push(frame: VideoPlayerFrame): void;
  /**
   * Decode with another config from the next keyframe pushed on, as at a rendition switch. Frames pushed before that
   * keyframe still decode with the config they belong to. After the decoder failed for good, the next keyframe opens
   * a new decoder with this config.
   */
  configure(decoderConfig: VideoDecoderConfig): void;
  /** Counters, updated in place. */
  readonly stats: Readonly<VideoPlayerStats>;
  /** When each step of the start happened, for measuring a join. */
  readonly joinPhases: Readonly<VideoPlayerJoinPhases>;
  /** Stop: the decoder closes, every decoded frame still waiting is closed, and later pushes are ignored. */
  close(): void;
  /** Whether `close()` was called. */
  readonly closed: boolean;
}

/** The part of a canvas that {@link canvasRenderer} draws on: an `HTMLCanvasElement` or an `OffscreenCanvas` qualifies. */
export interface VideoCanvas {
  /** The canvas's width in pixels. */
  width: number;
  /** The canvas's height in pixels. */
  height: number;
  /** Its 2D context, or `null` when it already has a context of another kind. */
  getContext(contextId: '2d'): CanvasDrawImage | null;
}

/** Ask the clock again at least this often while a frame waits: the audio input or another track may move the clock. */
const CLOCK_RECHECK_US = 50_000;

/**
 * Where decoding stands. While `awaiting-key`, frames that are not keyframes are skipped, and the next keyframe
 * configures `decoder` with the latest config, or a new decoder when there is none: at the start, and after an error.
 * While `decoding`, every frame is decoded, and a keyframe first applies a config newer than `configured`. While
 * `failed`, every frame is skipped until `configure()`.
 */
type Decoding =
  | { state: 'awaiting-key'; decoder: VideoDecoder | undefined }
  | { state: 'decoding'; decoder: VideoDecoder; configured: VideoDecoderConfig }
  | { state: 'failed' }
  | { state: 'closed' };

const isNotSupported = (error: unknown) => error instanceof Error && error.name === 'NotSupportedError';

/**
 * Create a video player. Decoded frames wait in presentation order until the clock says the earliest is due, so a
 * decoder whose pool of frames is full resumes as they are shown: the earliest is due within the clock's
 * `maxVideoDelayUs`, or shown at once as a re-sync.
 *
 * ```ts
 * const clock = createPlayoutClock();
 * const player = createVideoPlayer({
 *   clock,
 *   decoderConfig: videoDecoderConfigFromTrack(videoTrack),
 *   render: canvasRenderer(canvas),
 * });
 * const reader = createFrameReader(createFrameSequencer({ clock }), { onFrame: player.push });
 * subscriber.subscribe({ namespace, track: 'video' }, { ...reader, onError });
 * ```
 */
export function createVideoPlayer(options: VideoPlayerOptions): VideoPlayer {
  const { clock, render, onError } = options;
  let config = options.decoderConfig;
  let decoding: Decoding = { state: 'awaiting-key', decoder: undefined };
  // Whether the current decoder has output a frame: one that fails before it does would fail again.
  let output = false;
  const queue: VideoFrame[] = [];
  let timer: ReturnType<typeof setTimeout> | undefined;
  const stats: VideoPlayerStats = {
    decoded: 0,
    shown: 0,
    dropped: 0,
    skipped: 0,
    decoderResets: 0,
    errors: 0,
    get queued() {
      return queue.length;
    },
    lastOffsetUs: null,
  };
  const joinPhases: VideoPlayerJoinPhases = {
    createdUs: clock.localNow(),
    firstPushUs: null,
    firstDecodedUs: null,
    firstShownUs: null,
  };

  const report = (error: unknown, what: string) => {
    stats.errors++;
    if (onError) onError(error);
    // biome-ignore lint/suspicious/noConsole: the application gave no handler to report to
    else console.error(new Error(what, { cause: error }));
  };
  const current = () => ('decoder' in decoding ? decoding.decoder : undefined);
  const stopTimer = () => {
    if (timer !== undefined) clearTimeout(timer);
    timer = undefined;
  };

  const pump = () => {
    timer = undefined;
    for (let head = queue[0]; head !== undefined; head = queue[0]) {
      const decision = clock.due(head.timestamp);
      if (decision.action === 'wait') {
        // Whole milliseconds, rounded up: browsers truncate a timeout, so the timer fired early, and the sub-millisecond
        // wait after it was clamped to 4 ms, showing frames about 4 ms late.
        timer = setTimeout(pump, Math.ceil(Math.min(decision.waitUs, CLOCK_RECHECK_US) / 1000));
        return;
      }
      queue.shift();
      if (decision.action === 'drop') {
        stats.dropped++;
        head.close();
        continue;
      }
      stats.shown++;
      stats.lastOffsetUs = decision.offsetUs;
      joinPhases.firstShownUs ??= clock.localNow();
      try {
        render(head);
      } catch (error) {
        report(error, 'render threw; the video player carries on');
      } finally {
        head.close();
      }
    }
  };

  const open = () => {
    output = false;
    const decoder: VideoDecoder = new VideoDecoder({
      output(frame) {
        if (current() !== decoder) {
          frame.close();
          return;
        }
        output = true;
        stats.decoded++;
        joinPhases.firstDecodedUs ??= clock.localNow();
        queue.push(frame);
        if (timer === undefined) pump();
      },
      error(error) {
        if (current() === decoder) fail(decoder, error);
      },
    });
    return decoder;
  };
  // A decoder that failed is closed (WebCodecs closes it before its error callback): the next keyframe opens another,
  // unless this one failed for good.
  const fail = (decoder: VideoDecoder, error: unknown) => {
    if (decoder.state !== 'closed') decoder.close();
    if (!output || isNotSupported(error)) {
      decoding = { state: 'failed' };
      report(error, 'The video decoder failed for good; the video player decodes nothing until configure()');
      return;
    }
    decoding = { state: 'awaiting-key', decoder: undefined };
    stats.decoderResets++;
    report(error, 'The video decoder failed; the video player decodes again from the next keyframe');
  };
  // A resync or restart: what the decoder holds and what waits to be shown belong to the replaced timeline.
  const replaceTimeline = () => {
    for (const frame of queue.splice(0)) frame.close();
    stopTimer();
    const decoder = current();
    if (decoder?.state === 'configured') {
      decoder.reset();
      stats.decoderResets++;
    }
    if (decoding.state !== 'failed') decoding = { state: 'awaiting-key', decoder };
  };

  return {
    push(frame) {
      if (decoding.state === 'closed') return;
      joinPhases.firstPushUs ??= clock.localNow();
      if (frame.discontinuity === 'resync' || frame.discontinuity === 'restart') replaceTimeline();
      else if (frame.discontinuity === 'gap' && !frame.key && decoding.state === 'decoding') {
        decoding = { state: 'awaiting-key', decoder: decoding.decoder };
      }
      if (decoding.state === 'failed' || (decoding.state === 'awaiting-key' && !frame.key)) {
        stats.skipped++;
        return;
      }
      const configure = decoding.state === 'awaiting-key' || (frame.key && decoding.configured !== config);
      const decoder = decoding.decoder ?? open();
      try {
        if (configure) {
          decoder.configure(config);
          decoding = { state: 'decoding', decoder, configured: config };
        }
        decoder.decode(
          new EncodedVideoChunk({
            type: frame.key ? 'key' : 'delta',
            timestamp: frame.timestampUs,
            data: frame.payload,
          })
        );
      } catch (error) {
        fail(decoder, error);
      }
    },
    configure(decoderConfig) {
      if (decoding.state === 'closed') return;
      config = decoderConfig;
      if (decoding.state === 'failed') decoding = { state: 'awaiting-key', decoder: undefined };
    },
    stats,
    joinPhases,
    close() {
      const decoder = current();
      decoding = { state: 'closed' };
      stopTimer();
      for (const frame of queue.splice(0)) frame.close();
      if (decoder && decoder.state !== 'closed') decoder.close();
    },
    get closed() {
      return decoding.state === 'closed';
    },
  };
}

/**
 * A `render` for {@link createVideoPlayer} that draws each frame onto a canvas, sized to the frame's display size when
 * that changes. It takes the canvas's 2D context at once, and throws when there is none. An `OffscreenCanvas` works in
 * a worker.
 */
export function canvasRenderer(canvas: VideoCanvas): (frame: VideoFrame) => void {
  const context = canvas.getContext('2d');
  if (!context) throw new Error('The canvas has no 2D context: it already has a context of another kind');
  return (frame) => {
    if (canvas.width !== frame.displayWidth) canvas.width = frame.displayWidth;
    if (canvas.height !== frame.displayHeight) canvas.height = frame.displayHeight;
    context.drawImage(frame, 0, 0, canvas.width, canvas.height);
  };
}
