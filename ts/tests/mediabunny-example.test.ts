import { readFileSync } from 'node:fs';
import {
  type PublishProperty,
  RelayError,
  type RelayGroupWriter,
  type RelayPublisher,
  type RelayTrackWriter,
} from '@caprid/relay';
import { Input } from 'mediabunny';
import { afterEach, expect, it, vi } from 'vitest';
import { publishVideoFile } from '../examples/mediabunny/publish-video.js';

const fixture = new Uint8Array(readFileSync(new URL('./fixtures/mediabunny-avc.mp4', import.meta.url)));
const file = new Blob([fixture]);
afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
});

async function later(microtasks: number) {
  for (let i = 0; i < microtasks; i++) await Promise.resolve();
}

function deferred() {
  let resolve = () => {};
  let reject = (_reason: Error) => {};
  const promise = new Promise<void>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

// Use the real demuxer, but hold Blob reads after the MP4 header (or before any metadata).
function stalledFile(available: number) {
  const file = new Blob([fixture]);
  const opened = deferred();
  const readers: ReadableStreamDefaultController<Uint8Array<ArrayBuffer>>[] = [];
  vi.spyOn(file, 'slice').mockImplementation((start = 0, end = file.size) => {
    const slice = new Blob([fixture.slice(start, end)]);
    vi.spyOn(slice, 'stream').mockImplementation(
      () =>
        new ReadableStream({
          start(controller) {
            const data = fixture.slice(start, Math.min(end, available));
            if (data.length > 0) controller.enqueue(data);
            readers.push(controller);
            opened.resolve();
          },
        }),
    );
    return slice;
  });
  return {
    file,
    opened: opened.promise,
    release: () => {
      for (const reader of readers) reader.close();
    },
  };
}

function recordingPublisher(
  controls: {
    /** The error the video write with this zero-based index throws, if any. */
    videoWriteError?: (index: number) => Error | undefined;
    /** Called when the example closes the video track. */
    onVideoClose?: () => void;
    /** Awaited before every video write. */
    beforeVideoWrite?: () => Promise<void>;
    catalogWrite?: () => Promise<void>;
    videoEnd?: () => void;
    videoDemand?: () => boolean;
    onVideoWrite?: () => void;
  } = {},
) {
  const tracks = new Map<
    string,
    {
      writer: RelayTrackWriter;
      groups: { payload: ArrayBufferView; properties: readonly PublishProperty[]; timeMs: number }[][];
    }
  >();
  let videoWrites = 0;
  let closeConnection = (_error: RelayError) => {};
  const connectionClosed = new Promise<{ error: RelayError }>((resolve) => {
    closeConnection = (error) => resolve({ error });
  });
  const unused = async () => {
    throw new Error('Unused text/JSON method');
  };
  const publisher: RelayPublisher = {
    announced: Promise.resolve(),
    expiresAt: undefined,
    closed: connectionClosed,
    close: async () => {},
    registerTrack({ track }) {
      let end = () => {};
      const ended = new Promise<void>((resolve) => {
        end = resolve;
      });
      let closed = false;
      const groups: { payload: ArrayBufferView; properties: readonly PublishProperty[]; timeMs: number }[][] = [];
      const writer: RelayTrackWriter = {
        get closed() {
          return closed;
        },
        get hasDemand() {
          return !closed && (track !== 'video' || (controls.videoDemand?.() ?? true));
        },
        async *demandChanges() {
          yield true;
          await ended;
        },
        close() {
          if (track === 'video' && !closed) controls.onVideoClose?.();
          closed = true;
          end();
        },
        async openGroup(options) {
          if (!writer.hasDemand) throw new Error('Opened a group without demand');
          const frames: { payload: ArrayBufferView; properties: readonly PublishProperty[]; timeMs: number }[] = [];
          groups.push(frames);
          let groupClosed = false;
          const group: RelayGroupWriter = {
            groupId: groups.length - 1,
            get objectCount() {
              return frames.length;
            },
            get closed() {
              // As the SDK's write slot does: losing relay demand ends the open group.
              if (!writer.hasDemand) groupClosed = true;
              return groupClosed;
            },
            async write(payload, options) {
              if (track === 'video') {
                await controls.beforeVideoWrite?.();
                const error = controls.videoWriteError?.(videoWrites++);
                if (error) {
                  groupClosed = true;
                  if (error instanceof RelayError && error.code === 'CLOSED') writer.close();
                  throw error;
                }
              }
              if (closed || group.closed) throw new Error('Writer closed');
              frames.push({ payload, properties: options?.properties ?? [], timeMs: performance.now() });
              if (track === 'video') controls.onVideoWrite?.();
            },
            async close() {
              groupClosed = true;
              if (track === 'video' && groups.length === 2) controls.videoEnd?.();
            },
            abort() {
              groupClosed = true;
            },
            writeText: unused,
            writeJSON: unused,
          };
          if (options?.payload !== undefined) {
            // As the SDK does: the first object goes with the open, and a failure resets the group and returns none.
            try {
              await group.write(options.payload, options.properties ? { properties: options.properties } : {});
            } catch (error) {
              group.abort();
              throw error;
            }
          }
          return group;
        },
        async write(payload, options) {
          if (track === 'catalog' && controls.catalogWrite) {
            const signal = options?.signal;
            signal?.throwIfAborted();
            let cancel = () => {};
            const cancelled = new Promise<never>((_, reject) => {
              cancel = () => reject(signal?.reason);
            });
            signal?.addEventListener('abort', cancel, { once: true });
            try {
              await Promise.race([
                controls.catalogWrite(),
                cancelled,
                ended.then(() => {
                  throw new Error('Catalog write cancelled by track close');
                }),
              ]);
            } finally {
              signal?.removeEventListener('abort', cancel);
            }
          }
          const group = await writer.openGroup();
          await group.write(payload, options);
          await group.close();
        },
        writeText: unused,
        writeJSON: unused,
      };
      tracks.set(track, { writer, groups });
      return writer;
    },
  };
  /** The connection ends: its tracks close at once, and `closed` reports the error a little later, as in the SDK. */
  const endConnection = (error: RelayError, tracksToClose = [...tracks.keys()], reportAfterMs = 20) => {
    for (const name of tracksToClose) tracks.get(name)?.writer.close();
    setTimeout(() => closeConnection(error), reportAfterMs);
  };
  return { publisher, tracks, endConnection };
}

const goaway = () => new RelayError('CLOSED', 'Relay sent GOAWAY', 'connection');

const catalogDocuments = (tracks: ReturnType<typeof recordingPublisher>['tracks']) =>
  tracks.get('catalog')!.groups.map((group) => JSON.parse(new TextDecoder().decode(group[0]!.payload)));

it('publishes the file in keyframe groups and ends the broadcast at EOF', async () => {
  const videoClosedAtCatalogWrite: boolean[] = [];
  const { publisher, tracks } = recordingPublisher({
    catalogWrite: async () => {
      videoClosedAtCatalogWrite.push(tracks.get('video')!.writer.closed);
    },
  });
  await publishVideoFile(file, publisher, ['demo']);
  expect(tracks.get('catalog')!.groups.map((group) => group.length)).toEqual([1, 1]);
  const [catalog, end] = catalogDocuments(tracks);
  // msf-01 §5.1.3 and §11.3: the live catalog is not complete; the last one, sent after the video track ended, is.
  expect(catalog).not.toHaveProperty('isComplete');
  expect(end).toEqual({ version: 'draft-01', generatedAt: expect.any(Number), isComplete: true, tracks: [] });
  expect(videoClosedAtCatalogWrite).toEqual([false, true]);
  // Catalog objects carry no LOC properties, as in the WebCodecs and Python examples.
  expect(
    tracks
      .get('catalog')!
      .groups.flat()
      .map((object) => object.properties),
  ).toEqual([[], []]);
  expect(Number.isInteger(catalog.tracks[0].bitrate)).toBe(true);
  expect(catalog.tracks[0].bitrate).toBeGreaterThan(0);
  expect(catalog.tracks[0].framerate).toBeGreaterThan(0);
  expect(Math.round(catalog.tracks[0].framerate * 1000)).toBeCloseTo(catalog.tracks[0].framerate * 1000, 6);
  expect(tracks.get('video')!.groups.map((group) => group.length)).toEqual([2, 2]);
  for (const group of tracks.get('video')!.groups) {
    expect(group[0]!.properties.some((property) => property.type === 0x0d)).toBe(true);
    expect(group[1]!.properties.some((property) => property.type === 0x0d)).toBe(false);
  }
  expect([...tracks.values()].every(({ writer }) => writer.closed)).toBe(true);
});

it('stops when the video track ends under a write and closes both tracks', async () => {
  const { publisher, tracks, endConnection } = recordingPublisher({
    videoWriteError: () => {
      endConnection(goaway(), ['video']);
      return new RelayError('CLOSED', 'Video track ended', 'writing');
    },
  });
  await expect(publishVideoFile(file, publisher, ['demo'])).rejects.toThrow('Relay sent GOAWAY');
  expect([...tracks.values()].every(({ writer }) => writer.closed)).toBe(true);
  // A failure is not the end of the broadcast.
  expect(catalogDocuments(tracks).some((document) => 'isComplete' in document)).toBe(false);
});

it('cancels a pacing wait and closes both tracks', async () => {
  const { publisher, tracks } = recordingPublisher();
  const stop = new AbortController();
  const timer = setTimeout(() => stop.abort(new Error('Stopped')), 30);
  try {
    await expect(publishVideoFile(file, publisher, ['demo'], { signal: stop.signal })).rejects.toThrow('Stopped');
    expect([...tracks.values()].every(({ writer }) => writer.closed)).toBe(true);
  } finally {
    clearTimeout(timer);
  }
});

it('resumes at the next keyframe after a write interrupts a group', async () => {
  const { publisher, tracks } = recordingPublisher({
    videoWriteError: (index) =>
      index === 1 ? new RelayError('WRITE_TIMEOUT', 'Write timed out', 'writing') : undefined,
  });
  await publishVideoFile(file, publisher, ['demo']);
  // The delta that failed is lost with the rest of its group; the next keyframe opens a fresh one.
  expect(tracks.get('video')!.groups.map((group) => group.length)).toEqual([1, 2]);
  expect(catalogDocuments(tracks).at(-1)).toHaveProperty('isComplete', true);
});

it('retries a catalog write that fails while the track stays open', async () => {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] });
  let attempts = 0;
  const failed = deferred();
  const retried = deferred();
  const { publisher, tracks } = recordingPublisher({
    catalogWrite: async () => {
      attempts++;
      if (attempts === 1) {
        failed.resolve();
        throw new RelayError('WRITE_TIMEOUT', 'Catalog write timed out', 'writing');
      }
      retried.resolve();
    },
    // Hold the file until the catalog service's one-second retry pause is over.
    beforeVideoWrite: () => retried.promise,
  });
  const completion = expect(publishVideoFile(file, publisher, ['demo'])).resolves.toBeUndefined();
  await failed.promise;
  await vi.runAllTimersAsync();
  await completion;
  expect(attempts).toBe(3);
  const [live, end] = catalogDocuments(tracks);
  expect(live).toHaveProperty('tracks');
  expect(live).not.toHaveProperty('isComplete');
  expect(end).toHaveProperty('isComplete', true);
});

it('cancels a live catalog write still pending at EOF and sends the end-of-broadcast catalog', async () => {
  const catalog = deferred();
  const endStarted = deferred();
  let writes = 0;
  const { publisher, tracks } = recordingPublisher({
    catalogWrite: () => {
      if (++writes === 2) endStarted.resolve();
      return catalog.promise;
    },
  });
  const result = publishVideoFile(file, publisher, ['demo']);
  const completion = expect(result).resolves.toBeUndefined();
  try {
    // The live catalog write is still pending when the end-of-broadcast catalog starts.
    await endStarted.promise;
    expect(tracks.get('video')!.writer.closed).toBe(true);
  } finally {
    catalog.resolve();
    await completion;
  }
  expect(catalogDocuments(tracks)).toEqual([expect.objectContaining({ isComplete: true })]);
  expect([...tracks.values()].every(({ writer }) => writer.closed)).toBe(true);
});

it('reports a failure of the end-of-broadcast catalog', async () => {
  const end = deferred();
  const endStarted = deferred();
  let writes = 0;
  const { publisher, tracks } = recordingPublisher({
    catalogWrite: async () => {
      if (++writes < 2) return;
      endStarted.resolve();
      return end.promise;
    },
  });
  const completion = expect(publishVideoFile(file, publisher, ['demo'])).rejects.toThrow('End catalog failed');
  await endStarted.promise;
  end.reject(new Error('End catalog failed'));
  await completion;
  expect(writes).toBe(2);
  expect([...tracks.values()].every(({ writer }) => writer.closed)).toBe(true);
});

it('reports a catalog failure that lands while the last group FINs', async () => {
  const catalog = deferred();
  let writes = 0;
  const { publisher, tracks } = recordingPublisher({
    // Only the live catalog write fails; the end-of-broadcast catalog would go through.
    catalogWrite: () => (++writes === 1 ? catalog.promise : Promise.resolve()),
    // Timed so that the catalog service has rejected with the failure but its handler runs only after the example
    // stopped the service at EOF. Later, the stop cancels the write first; sooner, the failure lands before the stop.
    videoEnd: () => void later(1).then(() => catalog.reject(new Error('Catalog failed'))),
  });
  await expect(publishVideoFile(file, publisher, ['demo'])).rejects.toThrow('Catalog failed');
  expect(catalogDocuments(tracks).some((document) => 'isComplete' in document)).toBe(false);
});

it('does not end the broadcast when the video track ends under the last FIN', async () => {
  const { publisher, tracks, endConnection } = recordingPublisher({
    videoEnd: () => {
      endConnection(goaway(), ['video']);
      throw new RelayError('CLOSED', 'Video track ended', 'closing');
    },
  });
  await expect(publishVideoFile(file, publisher, ['demo'])).rejects.toThrow('Relay sent GOAWAY');
  expect(catalogDocuments(tracks).some((document) => 'isComplete' in document)).toBe(false);
});

it('does not end the broadcast when the catalog track ends while its service stops', async () => {
  // As when the caller closes the publisher at EOF: the SDK closes every track at once.
  const { publisher, tracks, endConnection } = recordingPublisher({
    onVideoClose: () => endConnection(goaway(), ['catalog']),
  });
  await expect(publishVideoFile(file, publisher, ['demo'])).rejects.toThrow('Relay sent GOAWAY');
  expect(catalogDocuments(tracks).some((document) => 'isComplete' in document)).toBe(false);
});

it('reports the connection error when the video track ends with its connection', async () => {
  let ended = false;
  const { publisher, tracks, endConnection } = recordingPublisher({
    // Between packets: the next frame finds the track closed before the connection reports why.
    onVideoWrite: () => {
      if (ended) return;
      ended = true;
      // Reported long after the next packet falls due.
      endConnection(goaway(), undefined, 1000);
    },
  });
  await expect(publishVideoFile(file, publisher, ['demo'])).rejects.toThrow('Relay sent GOAWAY');
  expect(catalogDocuments(tracks).some((document) => 'isComplete' in document)).toBe(false);
});

it('reports the connection error when the catalog track ends with its connection', async () => {
  let ended = false;
  const { publisher, tracks, endConnection } = recordingPublisher({
    // The idle catalog track closes first; the connection reports why only once its transport has shut down.
    beforeVideoWrite: async () => {
      if (ended) return;
      ended = true;
      endConnection(goaway(), ['catalog']);
    },
  });
  await expect(publishVideoFile(file, publisher, ['demo'])).rejects.toThrow('Relay sent GOAWAY');
  expect(catalogDocuments(tracks).some((document) => 'isComplete' in document)).toBe(false);
});

it.each(['metadata', 'packet'] as const)('cancels a stalled %s read by disposing the input', async (phase) => {
  const mdat = Buffer.from(fixture).indexOf('mdat') + 4;
  expect(mdat).toBeGreaterThan(4);
  const stalled = stalledFile(phase === 'metadata' ? 0 : mdat);
  const catalogSent = deferred();
  const { publisher, tracks } = recordingPublisher({ catalogWrite: async () => catalogSent.resolve() });
  const dispose = vi.spyOn(Input.prototype, 'dispose');
  const stop = new AbortController();
  const result = publishVideoFile(stalled.file, publisher, ['demo'], { signal: stop.signal });
  const completion = expect(result).rejects.toThrow('Stopped during read');
  try {
    await (phase === 'metadata' ? stalled.opened : catalogSent.promise);
    stop.abort(new Error('Stopped during read'));
    await completion;
    expect(dispose).toHaveBeenCalled();
    expect([...tracks.values()].every(({ writer }) => writer.closed)).toBe(true);
  } finally {
    stalled.release();
  }
});

const bframes = new Blob([readFileSync(new URL('./fixtures/mediabunny-avc-bframes.mp4', import.meta.url))]);

it('paces a real B-frame file in decode order while preserving reordered presentation timestamps', async () => {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] });
  const first = deferred();
  const { publisher, tracks } = recordingPublisher({ onVideoWrite: first.resolve });
  const result = publishVideoFile(bframes, publisher, ['demo']);
  const completion = expect(result).resolves.toBeUndefined();
  await first.promise;
  await vi.runAllTimersAsync();
  await completion;
  const frames = tracks.get('video')!.groups.flat();
  expect(frames.map((frame) => frame.timeMs)).toEqual([0, 100, 200, 300, 400, 500, 600, 700]);
  const timestamps = frames.map((frame) => frame.properties.find((property) => property.type === 0x10)!.value);
  expect(timestamps.some((value, index) => index > 0 && value < timestamps[index - 1]!)).toBe(true);
});

it('keeps pacing through missing demand and resumes only at a later keyframe', async () => {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] });
  const first = deferred();
  const { publisher, tracks } = recordingPublisher({
    onVideoWrite: first.resolve,
    videoDemand: () => performance.now() < 50 || performance.now() >= 350,
  });
  const completion = expect(publishVideoFile(bframes, publisher, ['demo'])).resolves.toBeUndefined();
  await first.promise;
  await vi.runAllTimersAsync();
  await completion;
  expect(tracks.get('video')!.groups.map((group) => group.length)).toEqual([1, 2]);
  expect(
    tracks
      .get('video')!
      .groups.flat()
      .map((frame) => frame.timeMs),
  ).toEqual([0, 600, 700]);
});
