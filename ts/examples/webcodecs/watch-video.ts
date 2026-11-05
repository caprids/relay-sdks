import {
  createFrameHold,
  createFrameSequencer,
  createPlayoutClock,
  type FrameSequencer,
  type PlayoutClock,
  type SequencedFrame,
  VIDEO_TRACK,
} from '@caprid/moq-media';
import type { RelaySubscriber, RelaySubscription } from '@caprid/relay';
import { createFrameReader, type FrameReader, mediaSubscribeParameters } from '@caprid/relay/media';
import { readCatalog } from './read-catalog.js';

/**
 * Decoded frames waiting for their time, at most: a hidden tab throttles timers, so the pump falls behind the decoder,
 * and a decoder whose frames the app keeps open can stall. Video waits for audio by the audio player's jitter buffer and
 * output delay, up to about 450 ms, which is 14 frames at 30 fps (4 measured with a 50 ms buffer on a clean network).
 */
const MAX_DECODED_FRAMES = 16;
/** Ask the clock again at least this often while a frame waits: an audio report can move its position meanwhile. */
const CLOCK_RECHECK_MS = 50;

/** Options for {@link watchVideo}. */
export interface WatchVideoOptions {
  /** The playout clock that paces the video; pass the same one to `listenAudio`. Default: a new one. */
  clock?: PlayoutClock;
  /** A failure after the watcher started: the video subscription ended with an error, or the decoder failed. */
  onError?(error: unknown): void;
  /** A frame was drawn: its timestamp on the broadcast timeline, and its offset from the clock (positive is early). */
  onShown?(timestampUs: number, offsetUs: number): void;
  /**
   * Aborting while the watcher starts cancels what it set up and rejects with the reason, as when the catalog is slow
   * to arrive. Once started, stop it with `stop()`.
   */
  signal?: AbortSignal;
}

/** Counters of a {@link VideoWatcher}, cumulative. */
export interface VideoWatcherStats {
  /** Frames drawn. */
  shown: number;
  /** Decoded frames dropped because audio had moved past them. */
  late: number;
  /** Decoded frames dropped because more were waiting than the bound allows. */
  overflowed: number;
  /** Frames after which the decoder was reset: a gap, a resync, or a restart. */
  discontinuities: number;
  /** The offset of the last frame drawn, in µs: positive is early. */
  lastOffsetUs: number | undefined;
}

/** A running watcher. */
export interface VideoWatcher {
  /** The clock pacing the video: the one passed in, or the watcher's own. */
  readonly clock: PlayoutClock;
  readonly sequencer: FrameSequencer;
  readonly stats: Readonly<VideoWatcherStats>;
  /** Stop drawing, cancel the subscription and close the decoder. The clock is left as it is. */
  stop(): Promise<void>;
}

/**
 * Play the video track of the broadcast at `namespace`, such as the WebCodecs example's canvas, onto `canvas`: the
 * catalog gives the decoder config (the codec, and the avcC as `initData`, which is the only place an H.264 track
 * carries it), a frame reader orders the frames, a `VideoDecoder` decodes them, and the playout clock says when each
 * one is drawn. The canvas takes the size of the frames. At a discontinuity the decoder is reset and waits for the next
 * keyframe.
 *
 * It joins quickly: it subscribes to the video track by its conventional name together with the catalog, before the
 * catalog arrives, and a frame hold keeps the frames until the decoder is configured, which saves the catalog's round
 * trip. A catalog that names another video track has it subscribed instead.
 *
 * For sound in sync, pass one clock to both this and `listenAudio`: audio is the master. Until audio reports reach the
 * clock, video plays on wall pacing, so it starts before any user gesture and keeps playing while the `AudioContext`
 * is suspended; once audio plays, video follows it.
 */
export async function watchVideo(
  subscriber: RelaySubscriber,
  namespace: readonly string[],
  canvas: HTMLCanvasElement | OffscreenCanvas,
  options: WatchVideoOptions = {},
): Promise<VideoWatcher> {
  const draw = canvas.getContext('2d') as CanvasRenderingContext2D | OffscreenCanvasRenderingContext2D | null;
  if (!draw) throw new Error('The canvas has no 2D context');
  const clock = options.clock ?? createPlayoutClock();
  // What setup has created so far, released in reverse order if a later step fails.
  const release: (() => void)[] = [];
  // The setup's own signal: the caller's abort, or a failure of a later step that must end the catalog read.
  const setup = new AbortController();
  const abortSetup = () => setup.abort(options.signal?.reason);
  options.signal?.addEventListener('abort', abortSetup, { once: true });
  // Once started, a failure goes to onError; before, it fails the setup.
  let started = false;
  let reading: ReturnType<typeof readCatalog> | undefined;
  let catalogCancelled = false;
  try {
    options.signal?.throwIfAborted();
    const sequencer = createFrameSequencer({ clock });
    const hold = createFrameHold<SequencedFrame>();
    interface Video {
      reader: FrameReader;
      subscription: RelaySubscription;
      failed: { error: unknown } | undefined;
    }
    const subscribeVideo = (name: string): Video => {
      const reader = createFrameReader(sequencer, { onFrame: hold.push });
      const video: Partial<Video> & { reader: FrameReader } = { reader, failed: undefined };
      try {
        video.subscription = subscriber.subscribe(
          { namespace: [...namespace], track: name, parameters: mediaSubscribeParameters('video') },
          {
            ...reader,
            onError(error) {
              video.failed ??= { error };
              if (started) options.onError?.(error);
            },
          },
        );
      } catch (error) {
        reader.close();
        throw error;
      }
      return video as Video;
    };
    // Subscribe to the catalog and the video track at once: the video does not wait for the catalog's round trips.
    reading = readCatalog(subscriber, namespace, setup.signal, {});
    reading.catch(() => {});
    let video = subscribeVideo(VIDEO_TRACK);
    release.push(() => video.reader.close());
    release.push(() => video.subscription.cancel());
    // Later catalogs change nothing here.
    const { tracks, subscription: catalog } = await reading;
    catalog.cancel();
    catalogCancelled = true;
    const track =
      tracks.find((candidate) => candidate.name === VIDEO_TRACK) ??
      tracks.find((candidate) => candidate.role === 'video');
    if (!track?.codec) throw new Error(`The catalog at ${namespace.join('/')} lists no ${VIDEO_TRACK} track`);
    if (track.name !== VIDEO_TRACK) {
      video.reader.close();
      video.subscription.cancel();
      hold.clear();
      sequencer.restart();
      video = subscribeVideo(track.name);
    }
    if (video.failed) throw video.failed.error;

    const config: VideoDecoderConfig = {
      codec: track.codec,
      ...(track.initData ? { description: track.initData } : {}),
      optimizeForLatency: true,
    };
    const stats: VideoWatcherStats = { shown: 0, late: 0, overflowed: 0, discontinuities: 0, lastOffsetUs: undefined };
    // Decoded frames in output order, each waiting for the clock to say it is due.
    const queue: VideoFrame[] = [];
    let timer: ReturnType<typeof setTimeout> | undefined;
    let stopped = false;
    const pump = () => {
      timer = undefined;
      for (let frame = queue[0]; frame; frame = queue[0]) {
        const decision = clock.due(frame.timestamp);
        if (decision.action === 'wait') {
          timer = setTimeout(pump, Math.min(decision.waitUs / 1000, CLOCK_RECHECK_MS));
          return;
        }
        queue.shift();
        if (decision.action === 'drop') stats.late++;
        else {
          if (canvas.width !== frame.displayWidth || canvas.height !== frame.displayHeight) {
            canvas.width = frame.displayWidth;
            canvas.height = frame.displayHeight;
          }
          draw.drawImage(frame, 0, 0);
          stats.shown++;
          stats.lastOffsetUs = decision.offsetUs;
          options.onShown?.(frame.timestamp, decision.offsetUs);
        }
        frame.close();
      }
    };
    const dropQueued = () => {
      for (const frame of queue.splice(0)) frame.close();
      clearTimeout(timer);
      timer = undefined;
    };
    // After configure and after every reset, the decoder can start only at a keyframe.
    let needsKey = true;
    const createDecoder = () => {
      const created = new VideoDecoder({
        output(frame) {
          if (stopped) {
            frame.close();
            return;
          }
          queue.push(frame);
          if (queue.length > MAX_DECODED_FRAMES) {
            queue.shift()?.close();
            stats.overflowed++;
          }
          if (timer === undefined) pump();
        },
        error(error) {
          if (stopped) return;
          options.onError?.(error);
          // A decoder that failed is closed: decode again from the next keyframe with a new one.
          decoder = createDecoder();
          needsKey = true;
        },
      });
      created.configure(config);
      return created;
    };
    let decoder = createDecoder();
    release.push(() => decoder.close());
    const decode = (frame: SequencedFrame) => {
      if (frame.discontinuity) {
        stats.discontinuities++;
        // A resync or a restart moved the timeline: what waits was decoded for the old one.
        if (frame.discontinuity !== 'gap') dropQueued();
        decoder.reset();
        decoder.configure(config);
        needsKey = true;
      }
      if (needsKey && !frame.key) return;
      needsKey = false;
      decoder.decode(
        new EncodedVideoChunk({ type: frame.key ? 'key' : 'delta', timestamp: frame.timestampUs, data: frame.payload }),
      );
    };
    started = true;
    hold.release(decode);

    return {
      clock,
      sequencer,
      stats,
      async stop() {
        stopped = true;
        video.reader.close();
        video.subscription.cancel();
        dropQueued();
        if (decoder.state !== 'closed') decoder.close();
      },
    };
  } catch (error) {
    // A failure before the catalog arrived ends its read, which cancels its own subscription. One that came already,
    // before the step that failed could take it, is cancelled here.
    setup.abort(error);
    if (!catalogCancelled)
      reading?.then(
        ({ subscription }) => subscription.cancel(),
        () => {},
      );
    for (const undo of release.reverse()) {
      try {
        undo();
      } catch {}
    }
    throw error;
  } finally {
    options.signal?.removeEventListener('abort', abortSetup);
  }
}
