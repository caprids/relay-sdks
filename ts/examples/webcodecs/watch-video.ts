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
import type { RelaySubscriber, RelaySubscription } from '@caprid/relay';
import { createFrameReader, type FrameReader, mediaSubscribeParameters } from '@caprid/relay/media';
import { readCatalog } from './read-catalog.js';

/** Options for {@link watchVideo}. */
export interface WatchVideoOptions {
  /** The playout clock that paces the video; pass the same one to `listenAudio`. Default: a new one. */
  clock?: PlayoutClock;
  /**
   * A failure after the watcher started: the video subscription ended with an error, or the decoder failed. A decoder
   * that fails before its first frame, or that does not support the codec, fails for good: the watcher decodes nothing
   * more. Otherwise a new decoder starts at the next keyframe.
   */
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
  /** Stop drawing, cancel the subscription and close the decoder. The clock is left as it is. */
  stop(): Promise<void>;
}

/**
 * Play the video track of the broadcast at `namespace`, such as the WebCodecs example's canvas, onto `canvas`: the
 * catalog gives the decoder config (the codec, and the avcC as `initData`, which is the only place an H.264 track
 * carries it), a frame reader orders the frames, and `createVideoPlayer` decodes them and draws each one when the
 * playout clock says it is due. The canvas takes the size of the frames.
 *
 * It joins quickly: it subscribes to the video track by its conventional name together with the catalog, before the
 * catalog arrives, and a frame hold keeps the frames until the player exists, which saves the catalog's round trip. A catalog that names another video track has it subscribed instead.
 *
 * For sound in sync, pass one clock to both this and `listenAudio`: audio is the master. Until audio reports reach the
 * clock, video plays on wall pacing, so it starts before any user gesture and keeps playing while the `AudioContext`
 * is suspended; once audio plays, video follows it. Start `listenAudio` once the context runs (`whenRunning`).
 */
export async function watchVideo(
  subscriber: RelaySubscriber,
  namespace: readonly string[],
  canvas: HTMLCanvasElement | OffscreenCanvas,
  options: WatchVideoOptions = {},
): Promise<VideoWatcher> {
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
    options.signal?.throwIfAborted();
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
        video.reader.close();
        video.subscription.cancel();
        player.close();
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
