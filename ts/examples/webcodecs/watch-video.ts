import {
  canvasRenderer,
  createFrameHold,
  createFrameSequencer,
  createPlayoutClock,
  createVideoPlayer,
  type FrameSequencer,
  type PlayoutClock,
  type SequencedFrame,
  VIDEO_TRACK,
  videoDecoderConfigFromTrack,
} from '@caprid/moq-media';
import type { SupervisedSubscriber, WantHandlers } from '@caprid/relay';
import { createFrameReader, type FrameReader, mediaSubscribeParameters } from '@caprid/relay/media';
import { readCatalog } from './read-catalog.js';

/** Numbers each watcher, so the want keys of two watchers on one supervised subscriber never collide. */
let watchers = 0;

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
  /** Frames that followed a discontinuity: a gap, a resync, or a restart. */
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
 * carries it), a frame reader orders the frames, and `createVideoPlayer` decodes them and draws each one when the
 * playout clock says it is due. The canvas takes the size of the frames.
 *
 * It joins quickly: it wants the video track by its conventional name together with the catalog, before the catalog
 * arrives, and a frame hold keeps the frames until the player exists, which saves the catalog's round trip. A catalog
 * that names another video track has that one wanted instead.
 *
 * The video is a want of the supervised subscriber, so the watcher outlives the publisher: when it restarts, at
 * `namespace` or (with `generations`) as a newer generation under it, or the subscriber reconnects, the want
 * subscribes again. The player resets at once, the decoded frames still waiting are dropped rather than drawn into
 * the gap, and it starts again at the new subscription's keyframe. The decoder config is the first catalog's: a
 * publisher that comes back with another codec or avcC is not followed.
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
  const draw = canvasRenderer(canvas);
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
    // A restart or a move of the video want, after the reader has restarted the sequencer (and with it the clock).
    // Before the player exists it drops the held frames of the old subscription; after, the decoded frames waiting
    // too, at once: on the reset clock they would look due and fill the gap with the old subscription's picture.
    let restarted = () => hold.clear();
    const wantVideo = (name: string): Video => {
      const reader = createFrameReader(sequencer, { onFrame: hold.push });
      const video: Video = { key: `${id}/video/${name}`, reader, failed: undefined };
      const handlers: WantHandlers = {
        ...reader,
        // The reader's restart also marks the next frame, where decode() counts the discontinuity.
        onRestarted() {
          reader.onRestarted();
          restarted();
        },
        onSwitched(info) {
          reader.onSwitched(info);
          restarted();
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

    let discontinuities = 0;
    const onError =
      options.onError &&
      ((error: unknown) => {
        try {
          options.onError?.(error);
        } catch (thrown) {
          console.error(new Error('onError threw; the watcher carries on', { cause: thrown }));
        }
      });
    const player = createVideoPlayer({
      clock,
      decoderConfig: videoDecoderConfigFromTrack(track),
      render(frame) {
        draw(frame);
        try {
          options.onShown?.(frame.timestamp, player.stats.lastOffsetUs ?? 0);
        } catch (error) {
          console.error(new Error('onShown threw; the watcher carries on', { cause: error }));
        }
      },
      ...(onError ? { onError } : {}),
    });
    release.push(() => player.close());
    const stats: VideoWatcherStats = {
      get shown() {
        return player.stats.shown;
      },
      get late() {
        return player.stats.dropped;
      },
      get discontinuities() {
        return discontinuities;
      },
      get lastOffsetUs() {
        return player.stats.lastOffsetUs ?? undefined;
      },
    };
    restarted = () => player.reset();
    started = true;
    hold.release((frame) => {
      if (frame.discontinuity) discontinuities++;
      player.push(frame);
    });

    return {
      clock,
      sequencer,
      stats,
      async stop() {
        dropVideo(video);
        player.close();
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
