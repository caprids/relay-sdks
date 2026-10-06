import { readFileSync } from 'node:fs';
import type { PublishProperty, RelayGroupWriter, RelayPublisher, RelayTrackWriter } from '@mux/relay';
import { Input } from 'mediabunny';
import { afterEach, expect, it, vi } from 'vitest';
import { publishVideoFile } from '../examples/mediabunny/publish-video.js';

const fixture = new Uint8Array(readFileSync(new URL('./fixtures/mediabunny-avc.mp4', import.meta.url)));
const file = new Blob([fixture]);
afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
});

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
    failVideo?: boolean;
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
  const unused = async () => {
    throw new Error('Unused text/JSON method');
  };
  const publisher: RelayPublisher = {
    announced: Promise.resolve(),
    closed: new Promise(() => {}),
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
          closed = true;
          end();
        },
        async openGroup() {
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
              return groupClosed;
            },
            async write(payload, options) {
              if (controls.failVideo && track === 'video') throw new Error('Video write failed');
              if (closed || groupClosed) throw new Error('Writer closed');
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
          return group;
        },
        async write(payload, options) {
          if (track === 'catalog' && controls.catalogWrite) {
            await Promise.race([
              controls.catalogWrite(),
              ended.then(() => {
                throw new Error('Catalog write cancelled by track close');
              }),
            ]);
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
  return { publisher, tracks };
}

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

it('stops on a write failure and closes both tracks', async () => {
  const { publisher, tracks } = recordingPublisher({ failVideo: true });
  await expect(publishVideoFile(file, publisher, ['demo'])).rejects.toThrow('Video write failed');
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

it('drains a catalog write that is still pending when video reaches EOF', async () => {
  const catalog = deferred();
  const videoEnd = deferred();
  const { publisher, tracks } = recordingPublisher({ catalogWrite: () => catalog.promise, videoEnd: videoEnd.resolve });
  const result = publishVideoFile(file, publisher, ['demo']);
  const completion = expect(result).resolves.toBeUndefined();
  try {
    await videoEnd.promise;
    expect(tracks.get('catalog')!.writer.closed).toBe(false);
  } finally {
    catalog.resolve();
    await completion;
  }
  expect(tracks.get('catalog')!.groups.map((group) => group.length)).toEqual([1, 1]);
  expect([...tracks.values()].every(({ writer }) => writer.closed)).toBe(true);
});

it('reports a genuine catalog failure during the EOF drain', async () => {
  const catalog = deferred();
  const videoEnd = deferred();
  const { publisher, tracks } = recordingPublisher({ catalogWrite: () => catalog.promise, videoEnd: videoEnd.resolve });
  const completion = expect(publishVideoFile(file, publisher, ['demo'])).rejects.toThrow('Catalog failed');
  await videoEnd.promise;
  catalog.reject(new Error('Catalog failed'));
  await completion;
  expect([...tracks.values()].every(({ writer }) => writer.closed)).toBe(true);
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
