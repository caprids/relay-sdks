import { readFileSync } from 'node:fs';
import type { PublishProperty, RelayGroupWriter, RelayPublisher, RelayTrackWriter } from '@mux/relay';
import { expect, it } from 'vitest';
import { publishVideoFile } from '../examples/mediabunny/publish-video.js';

const file = new Blob([readFileSync(new URL('./fixtures/mediabunny-avc.mp4', import.meta.url))]);

function recordingPublisher(failVideo = false) {
  const tracks = new Map<
    string,
    { writer: RelayTrackWriter; groups: { properties: readonly PublishProperty[] }[][] }
  >();
  const unused = async () => {
    throw new Error('Unused text/JSON method');
  };
  const publisher: RelayPublisher = {
    closed: new Promise(() => {}),
    close: async () => {},
    registerTrack({ track }) {
      let end = () => {};
      const ended = new Promise<void>((resolve) => {
        end = resolve;
      });
      let closed = false;
      const groups: { properties: readonly PublishProperty[] }[][] = [];
      const writer: RelayTrackWriter = {
        get closed() {
          return closed;
        },
        get hasDemand() {
          return !closed;
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
          const frames: { properties: readonly PublishProperty[] }[] = [];
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
            async write(_payload, options) {
              if (failVideo && track === 'video') throw new Error('Video write failed');
              if (closed || groupClosed) throw new Error('Writer closed');
              frames.push({ properties: options?.properties ?? [] });
            },
            async close() {
              groupClosed = true;
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

it('publishes the file in keyframe groups and ends the catalog service at EOF', async () => {
  const { publisher, tracks } = recordingPublisher();
  await publishVideoFile(file, publisher, ['demo']);
  expect(tracks.get('catalog')!.groups.map((group) => group.length)).toEqual([1]);
  expect(tracks.get('video')!.groups.map((group) => group.length)).toEqual([2, 2]);
  for (const group of tracks.get('video')!.groups) {
    expect(group[0]!.properties.some((property) => property.type === 0x0d)).toBe(true);
    expect(group[1]!.properties.some((property) => property.type === 0x0d)).toBe(false);
  }
  expect([...tracks.values()].every(({ writer }) => writer.closed)).toBe(true);
});

it('stops on a write failure and closes both tracks', async () => {
  const { publisher, tracks } = recordingPublisher(true);
  await expect(publishVideoFile(file, publisher, ['demo'])).rejects.toThrow('Video write failed');
  expect([...tracks.values()].every(({ writer }) => writer.closed)).toBe(true);
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
