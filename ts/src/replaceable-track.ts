import {
  createFrameWriter,
  type Frame,
  type FrameOutcome,
  type FrameWriter,
  type FrameWriterOptions,
} from './frame-writer.js';
import type { PublishProperty, RelayPublisher, RelayTrackWriter } from './publish.js';

/** Where a {@link ReplaceableTrack} lists its current track: the broadcast's catalog. */
export interface ReplaceableTrackCatalog {
  /** The service that serves the catalog: a `DocumentService` qualifies. */
  service: {
    /** Replace the catalog document, and send it while the relay is subscribed. */
    update(document: ArrayBufferView): void;
  };
  /**
   * The catalog document that lists `track` as the replaceable track, with the broadcast's other tracks: for MSF,
   * `buildMsfCatalog` with that name. Called at creation and at each replacement.
   */
  build(track: string): ArrayBufferView;
}

/** Options for {@link createReplaceableTrack}. */
export interface ReplaceableTrackOptions<F extends Frame = Frame> {
  /** The first track's name. */
  track: string;
  /**
   * The name of the track that replacement attempt number `replacement` (1 for the first) registers. Names are unique
   * per publisher connection, including after a track closes, so each must differ from every name used before; an
   * attempt that fails uses its name up, and the next asks for the next. Default `${track}-${replacement + 1}`:
   * `audio-2`, `audio-3`, and so on.
   */
  name?: (replacement: number) => string;
  /** Every track's properties, as `registerTrack` takes them. */
  trackProperties?: readonly PublishProperty[];
  /** Every track's write timeout, as `registerTrack` takes it. */
  writeTimeoutMs?: number;
  /** Every track's frame writer options. Default `{ grouping: 'frame' }`, the mapping for audio. */
  writer?: FrameWriterOptions<F>;
  /** The catalog to list the current track in. */
  catalog: ReplaceableTrackCatalog;
  /**
   * How long frames written to a replacement wait for its first relay subscription, in milliseconds. A listener
   * subscribes to a replacement only once the new catalog has reached it, about a round trip, and a frame writer
   * skips what it is handed before then: the first words of the new reply. The frames wait, in order, and go out once
   * the relay subscribes; any still waiting when this runs out go to the writer, which skips them. 0 waits for
   * nothing. Default 2000.
   */
  demandWaitMs?: number;
}

/** What {@link ReplaceableTrack.replace} did. */
export interface TrackReplacement {
  /** The track replaced, now closed. */
  from: string;
  /** The new track, listed in the catalog in its place. */
  to: string;
  /** How many times the track has been replaced, this time included: 1 for the first replacement. */
  replacements: number;
}

/**
 * A track that the publisher can replace mid-stream, for barge-in: a voice agent talked over replaces its reply
 * track, and listeners drop the old reply's audio and play the new track. Create one with
 * {@link createReplaceableTrack}.
 */
export interface ReplaceableTrack<F extends Frame = Frame> {
  /** The current track's name. */
  readonly track: string;
  /** How many times the track has been replaced: 0 while the first track plays. */
  readonly replacements: number;
  /**
   * Send one frame on the current track and say what happened, as `FrameWriter.write` does. A frame written to a
   * replacement before the relay has subscribed to it waits (`demandWaitMs`). A frame of a track that is replaced
   * before it went out is `'dropped'`.
   */
  write(frame: F): Promise<FrameOutcome>;
  /**
   * Replace the track: register the next track, list it in the catalog in the current track's place, then drop
   * what the current track has not sent and close it. Resolves once the old track is closed. Frames written from the
   * call on go to the new track.
   */
  replace(): Promise<TrackReplacement>;
  /** Drop what has not been sent and close the current track. Idempotent; `write` and `replace` reject after it. */
  close(): Promise<void>;
  /** True once {@link close} ran. */
  readonly closed: boolean;
}

interface Waiting<F> {
  frame: F;
  resolve(outcome: FrameOutcome): void;
  reject(error: unknown): void;
}

interface Incarnation<F extends Frame> {
  name: string;
  replacements: number;
  track: RelayTrackWriter;
  writer: FrameWriter<F>;
  /** Frames that wait for the relay's first subscription, in order; `undefined` once they go to the writer. */
  waiting: Waiting<F>[] | undefined;
  /** Ends the wait. */
  stopWaiting: () => void;
  /** The waiting frames being handed to the writer, one at a time. */
  pumping: boolean;
  retired: boolean;
}

const dropped: FrameOutcome = { kind: 'dropped' };

/**
 * Publish a track that can be replaced by a new one, listed in the catalog in its place (spec/media-layer.md, "Track
 * replacement"). msf-01 §5.3 makes a changed track a new name; a listener that sees the catalog list a new track in
 * place of the one it plays (`@caprid/moq-media`'s `findReplacementTrack`) drops the old track's audio and plays the
 * new one.
 *
 * ```ts
 * const catalog = createDocumentService(publisher.registerTrack({ track: CATALOG_TRACK }));
 * void catalog.serve();
 * const reply = createReplaceableTrack(publisher, {
 *   track: AUDIO_TRACK,
 *   trackProperties: locTrackProperties(),
 *   catalog: { service: catalog, build: (name) => buildMsfCatalog({ namespace, media: [{ role: 'audio', name }] }) },
 * });
 * void reply.write(frame);
 * // Talked over: the reply so far is dropped, and the next one goes out on a new track.
 * await reply.replace();
 * ```
 */
export function createReplaceableTrack<F extends Frame = Frame>(
  publisher: Pick<RelayPublisher, 'registerTrack'>,
  options: ReplaceableTrackOptions<F>,
): ReplaceableTrack<F> {
  if (typeof options !== 'object' || options === null) throw new TypeError('options must be an object');
  const { track: first, catalog } = options;
  if (typeof first !== 'string' || first.length === 0) throw new TypeError('track must be a non-empty string');
  const nameOf = options.name ?? ((replacement: number) => `${first}-${replacement + 1}`);
  if (typeof nameOf !== 'function') throw new TypeError('name must be a function');
  if (
    typeof catalog !== 'object' ||
    catalog === null ||
    typeof catalog.build !== 'function' ||
    typeof catalog.service?.update !== 'function'
  ) {
    throw new TypeError('catalog must have a service with update() and a build function');
  }
  const demandWaitMs = options.demandWaitMs ?? 2000;
  if (typeof demandWaitMs !== 'number' || !(demandWaitMs >= 0 && demandWaitMs <= 2147483647))
    throw new RangeError('demandWaitMs must be from 0 to 2147483647');
  const writerOptions: FrameWriterOptions<F> = { grouping: 'frame', ...options.writer };
  let closed = false;
  let closing: Promise<void> | undefined;
  // Old tracks still closing, each settled whatever its outcome, so close() can wait for every one.
  const retiring = new Set<Promise<void>>();
  // Names asked for so far: a failed attempt uses its name up, since the publisher may have taken it.
  let attempts = 0;

  const register = (name: string, replacements: number, wait: boolean): Incarnation<F> => {
    const track = publisher.registerTrack({
      track: name,
      ...(options.trackProperties ? { trackProperties: options.trackProperties } : {}),
      ...(options.writeTimeoutMs !== undefined ? { writeTimeoutMs: options.writeTimeoutMs } : {}),
    });
    let writer: FrameWriter<F>;
    try {
      writer = createFrameWriter(track, writerOptions);
    } catch (error) {
      track.close();
      throw error;
    }
    const current: Incarnation<F> = {
      name,
      replacements,
      track,
      writer,
      waiting: wait && demandWaitMs > 0 && !track.hasDemand ? [] : undefined,
      stopWaiting: () => {},
      pumping: false,
      retired: false,
    };
    if (current.waiting) watchDemand(current);
    return current;
  };

  // Release the waiting frames at the relay's first subscription, or when the wait runs out.
  const watchDemand = (current: Incarnation<F>) => {
    const changes = current.track.demandChanges();
    let timer: ReturnType<typeof setTimeout> | undefined;
    let done = false;
    const release = () => {
      if (done) return;
      done = true;
      if (timer !== undefined) clearTimeout(timer);
      void changes.return?.();
      void pump(current);
    };
    current.stopWaiting = () => {
      done = true;
      if (timer !== undefined) clearTimeout(timer);
      void changes.return?.();
    };
    timer = setTimeout(release, demandWaitMs);
    void (async () => {
      try {
        for await (const demand of changes) {
          if (done) return;
          if (demand) break;
        }
      } catch {
        // The track ended: the writer reports it for each frame.
      }
      release();
    })();
  };

  // Hand the waiting frames to the writer one at a time, in order, so its backlog never overflows; frames written
  // meanwhile join the end. Then later frames go straight to the writer.
  const pump = async (current: Incarnation<F>) => {
    if (current.pumping) return;
    current.pumping = true;
    for (;;) {
      const next = current.waiting?.shift();
      if (!next) break;
      if (current.retired) {
        next.resolve(dropped);
        continue;
      }
      try {
        next.resolve(await current.writer.write(next.frame));
      } catch (error) {
        next.reject(error);
      }
    }
    current.waiting = undefined;
    current.pumping = false;
  };

  const retire = async (old: Incarnation<F>) => {
    old.retired = true;
    old.stopWaiting();
    for (const waiting of old.waiting?.splice(0) ?? []) waiting.resolve(dropped);
    try {
      await old.writer.close();
    } finally {
      old.track.close();
    }
  };

  let current = register(first, 0, false);
  try {
    catalog.service.update(catalog.build(first));
  } catch (error) {
    current.track.close();
    throw error;
  }

  const closedError = () => new Error('The replaceable track is closed');

  return {
    get track() {
      return current.name;
    },
    get replacements() {
      return current.replacements;
    },
    get closed() {
      return closed;
    },
    write(frame) {
      if (closed) return Promise.reject(closedError());
      const target = current;
      if (target.waiting) {
        return new Promise<FrameOutcome>((resolve, reject) => {
          (target.waiting as Waiting<F>[]).push({ frame, resolve, reject });
        });
      }
      return target.writer.write(frame);
    },
    replace() {
      if (closed) return Promise.reject(closedError());
      const old = current;
      const replacements = old.replacements + 1;
      const name = nameOf(++attempts);
      if (typeof name !== 'string' || name.length === 0)
        return Promise.reject(new TypeError('name() must return a non-empty string'));
      let next: Incarnation<F>;
      try {
        next = register(name, replacements, true);
      } catch (error) {
        return Promise.reject(error);
      }
      try {
        catalog.service.update(catalog.build(name));
      } catch (error) {
        next.stopWaiting();
        next.track.close();
        return Promise.reject(error);
      }
      current = next;
      const done = retire(old).then(() => ({ from: old.name, to: name, replacements }));
      const settled = done.then(
        () => {},
        () => {},
      );
      retiring.add(settled);
      void settled.then(() => retiring.delete(settled));
      return done;
    },
    close() {
      // The current track retires at once, so nothing waiting on it goes out; every old track still closing is waited
      // for too. A second call shares the first's promise.
      closing ??= (async () => {
        closed = true;
        await Promise.allSettled([retire(current), ...retiring]);
      })();
      return closing;
    },
  };
}
