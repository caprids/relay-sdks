import {
  createFrameHold,
  createFrameSequencer,
  createPlayoutClock,
  type FrameSequencer,
  type PlayoutClock,
  type SequencedFrame,
  VIDEO_TRACK,
} from '@caprid/moq-media';
import type { SupervisedSubscriber, WantHandlers } from '@caprid/relay';
import { createFrameReader, type FrameReader, mediaSubscribeParameters } from '@caprid/relay/media';
import { readCatalog } from './read-catalog.js';

/** Numbers each watcher, so the want keys of two watchers on one supervised subscriber never collide. */
let watchers = 0;

/**
 * Decoded frames waiting for their time, at most: a hidden tab throttles timers, so the pump falls behind the decoder,
 * and a decoder whose frames the app keeps open can stall. Video waits for audio by the audio player's jitter buffer and
 * output delay, up to about 450 ms, which is 14 frames at 30 fps (4 measured with a 50 ms buffer on a clean network).
 * Past the bound the oldest frame is drawn early rather than dropped: when audio lags by more, every frame would
 * otherwise be dropped before its time and the picture would freeze.
 */
const MAX_DECODED_FRAMES = 16;
/** Ask the clock again at least this often while a frame waits: an audio report can move its position meanwhile. */
const CLOCK_RECHECK_MS = 50;

/** Options for {@link watchVideo}. */
export interface WatchVideoOptions {
  /** The playout clock that paces the video; pass the same one to `listenAudio`. Default: a new one. */
  clock?: PlayoutClock;
  /**
   * `namespace` is a supervised publisher's root: follow its newest generation, as `listenAudio`'s option does.
   * Default `false`: play `namespace` itself, resubscribing when its publisher restarts there.
   */
  generations?: boolean;
  /**
   * A failure after the watcher started: the video want stopped on a failure its supervised subscriber does not retry,
   * or the decoder failed. A decoder
   * that fails before its first frame, or that does not support the codec, fails for good: the watcher decodes nothing
   * more. Otherwise a new decoder starts at the next keyframe.
   */
  onError?(error: unknown): void;
  /** A frame was drawn: its timestamp on the broadcast timeline, and its offset from the clock (positive is early). */
  onShown?(timestampUs: number, offsetUs: number): void;
  /**
   * Aborting while the watcher starts withdraws what it set up and rejects with the reason. It waits for the catalog
   * as long as it takes, since the supervised subscriber retries until the publisher is there. Once started, stop it
   * with `stop()`.
   */
  signal?: AbortSignal;
}

/** Counters of a {@link VideoWatcher}, cumulative. */
export interface VideoWatcherStats {
  /** Frames drawn. */
  shown: number;
  /** Decoded frames dropped because audio had moved past them. */
  late: number;
  /** Decoded frames drawn early because more were waiting than the bound allows. */
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
  /** Stop drawing, withdraw the want and close the decoder. The clock is left as it is. */
  stop(): Promise<void>;
}

/**
 * Play the video track of the broadcast at `namespace`, such as the WebCodecs example's canvas, onto `canvas`: the
 * catalog gives the decoder config (the codec, and the avcC as `initData`, which is the only place an H.264 track
 * carries it), a frame reader orders the frames, a `VideoDecoder` decodes them, and the playout clock says when each
 * one is drawn. The canvas takes the size of the frames. At a discontinuity the decoder is reset and waits for the next
 * keyframe.
 *
 * It joins quickly: it wants the video track by its conventional name together with the catalog, before the catalog
 * arrives, and a frame hold keeps the frames until the decoder is configured, which saves the catalog's round trip. A
 * catalog that names another video track has that one wanted instead.
 *
 * The video is a want of the supervised subscriber, so the watcher outlives the publisher: when it restarts, at
 * `namespace` or (with `generations`) as a newer generation under it, or the subscriber reconnects, the want
 * subscribes again, and the next frame is a restart, where the decoder resets and waits for a keyframe. The decoder
 * config is the first catalog's: a publisher that comes back with another codec or avcC is not followed.
 *
 * For sound in sync, pass one clock to both this and `listenAudio`: audio is the master. Until audio reports reach the
 * clock, video plays on wall pacing, so it starts before any user gesture and keeps playing while the `AudioContext`
 * is suspended; once audio plays, video follows it. Start `listenAudio` once the context runs (`whenRunning`).
 */
export async function watchVideo(
  subscriber: SupervisedSubscriber,
  namespace: readonly string[],
  canvas: HTMLCanvasElement | OffscreenCanvas,
  options: WatchVideoOptions = {},
): Promise<VideoWatcher> {
  const generations = options.generations ?? false;
  const id = `watch-video/${++watchers}`;
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
  let catalogStopped = false;
  try {
    options.signal?.throwIfAborted();
    const sequencer = createFrameSequencer({ clock });
    const hold = createFrameHold<SequencedFrame>();
    interface Video {
      key: string;
      reader: FrameReader;
      failed: { error: unknown } | undefined;
    }
    const wantVideo = (name: string): Video => {
      const reader = createFrameReader(sequencer, { onFrame: hold.push });
      const video: Video = { key: `${id}/video/${name}`, reader, failed: undefined };
      const handlers: WantHandlers = {
        ...reader,
        // A restart or a move before the decoder exists: the held frames are the old subscription's. After, the
        // reader's restart marks the next frame, and decode() resets the decoder there.
        onRestarted() {
          reader.onRestarted();
          if (!started) hold.clear();
        },
        onSwitched(info) {
          reader.onSwitched(info);
          if (!started) hold.clear();
        },
        onError(error) {
          video.failed ??= { error };
          if (started) options.onError?.(error);
        },
      };
      try {
        subscriber.want(
          video.key,
          { namespace, track: name, parameters: mediaSubscribeParameters('video'), generations },
          handlers,
        );
      } catch (error) {
        reader.close();
        throw error;
      }
      return video;
    };
    const dropVideo = (dropped: Video) => {
      dropped.reader.close();
      subscriber.unwant(dropped.key);
    };
    // Want the catalog and the video track at once: the video does not wait for the catalog's round trips.
    reading = readCatalog(subscriber, `${id}/catalog`, namespace, generations, setup.signal, {});
    reading.catch(() => {});
    let video = wantVideo(VIDEO_TRACK);
    release.push(() => dropVideo(video));
    // Later catalogs change nothing here.
    const { tracks, stop: stopCatalog } = await reading;
    stopCatalog();
    catalogStopped = true;
    options.signal?.throwIfAborted();
    const track =
      tracks.find((candidate) => candidate.name === VIDEO_TRACK) ??
      tracks.find((candidate) => candidate.role === 'video');
    if (!track?.codec) throw new Error(`The catalog at ${namespace.join('/')} lists no ${VIDEO_TRACK} track`);
    if (track.name !== VIDEO_TRACK) {
      dropVideo(video);
      hold.clear();
      sequencer.restart();
      video = wantVideo(track.name);
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
    const show = (frame: VideoFrame, offsetUs: number) => {
      try {
        if (canvas.width !== frame.displayWidth || canvas.height !== frame.displayHeight) {
          canvas.width = frame.displayWidth;
          canvas.height = frame.displayHeight;
        }
        draw.drawImage(frame, 0, 0);
        stats.shown++;
        stats.lastOffsetUs = offsetUs;
        try {
          options.onShown?.(frame.timestamp, offsetUs);
        } catch (error) {
          console.error(new Error('onShown threw; the watcher carries on', { cause: error }));
        }
      } finally {
        frame.close();
      }
    };
    const pump = () => {
      timer = undefined;
      for (let frame = queue[0]; frame; frame = queue[0]) {
        const decision = clock.due(frame.timestamp);
        if (decision.action === 'wait') {
          timer = setTimeout(pump, Math.min(decision.waitUs / 1000, CLOCK_RECHECK_MS));
          return;
        }
        queue.shift();
        if (decision.action === 'show') show(frame, decision.offsetUs);
        else {
          stats.late++;
          frame.close();
        }
      }
    };
    const dropQueued = () => {
      for (const frame of queue.splice(0)) frame.close();
      clearTimeout(timer);
      timer = undefined;
    };
    // After configure and after every reset, the decoder can start only at a keyframe.
    let needsKey = true;
    let failedForGood = false;
    const createDecoder = () => {
      let producedOutput = false;
      const created = new VideoDecoder({
        output(frame) {
          producedOutput = true;
          if (stopped) {
            frame.close();
            return;
          }
          queue.push(frame);
          if (timer === undefined) pump();
          if (queue.length > MAX_DECODED_FRAMES) {
            const oldest = queue.shift() as VideoFrame;
            stats.overflowed++;
            show(oldest, clock.due(oldest.timestamp).offsetUs);
          }
        },
        error(error) {
          if (stopped) return;
          // A new decoder with the same config would fail the same way, again and again.
          failedForGood = !producedOutput || error.name === 'NotSupportedError';
          try {
            options.onError?.(error);
          } catch (thrown) {
            console.error(new Error('onError threw; the watcher carries on', { cause: thrown }));
          }
          if (stopped || failedForGood) return;
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
      if (failedForGood) return;
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
        dropVideo(video);
        dropQueued();
        if (decoder.state !== 'closed') decoder.close();
      },
    };
  } catch (error) {
    // A failure before the catalog arrived ends its read, which withdraws its own want. One that came already, before
    // the step that failed could take it, is withdrawn here.
    setup.abort(error);
    if (!catalogStopped)
      reading?.then(
        ({ stop }) => stop(),
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
