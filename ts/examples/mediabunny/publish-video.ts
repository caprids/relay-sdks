import { buildMsfCatalog, CATALOG_TRACK, locProperties, locTrackProperties, VIDEO_TRACK } from '@mux/moq-media';
import { packetToLocFrame, videoTrackFromDecoderConfig } from '@mux/moq-mediabunny';
import type { RelayGroupWriter, RelayPublisher } from '@mux/relay';
import { BlobSource, EncodedPacketSink, Input, MP4 } from 'mediabunny';

function wait(milliseconds: number, signal: AbortSignal): Promise<void> {
  signal.throwIfAborted();
  return new Promise((resolve, reject) => {
    const abort = () => {
      clearTimeout(timer);
      reject(signal.reason);
    };
    const timer = setTimeout(
      () => {
        signal.removeEventListener('abort', abort);
        resolve();
      },
      Math.max(0, milliseconds),
    );
    signal.addEventListener('abort', abort, { once: true });
  });
}

/**
 * Publish the primary AVC video track of an MP4 once, paced in real time. No decoding or encoding.
 * The caller owns the connection. Use a fresh publisher/namespace for each run; track names cannot be reused.
 * This deliberately small stage 1 example stops on write failure and omits audio and reconnect policy.
 */
export async function publishVideoFile(
  file: Blob,
  publisher: RelayPublisher,
  namespace: readonly string[],
  options: { signal?: AbortSignal } = {},
): Promise<void> {
  const input = new Input({ source: new BlobSource(file), formats: [MP4] });
  const lifetime = new AbortController();
  const signal = options.signal ? AbortSignal.any([options.signal, lifetime.signal]) : lifetime.signal;
  void publisher.closed.then(({ error }) => lifetime.abort(error ?? new Error('Publisher closed')));
  let tasks: Promise<void>[] = [];
  let closeTracks = () => {};
  try {
    signal.throwIfAborted();
    const track = await input.getPrimaryVideoTrack();
    const config = await track?.getDecoderConfig();
    if (!track || !config) throw new Error('The MP4 must contain an AVC video track with decoder configuration');
    const video = videoTrackFromDecoderConfig(config);
    const document = buildMsfCatalog({ namespace, video });
    const catalogWriter = publisher.registerTrack({ track: CATALOG_TRACK, trackProperties: locTrackProperties() });
    closeTracks = () => catalogWriter.close();
    const videoWriter = publisher.registerTrack({ track: VIDEO_TRACK, trackProperties: locTrackProperties() });
    closeTracks = () => {
      catalogWriter.close();
      videoWriter.close();
    };
    signal.addEventListener('abort', closeTracks, { once: true });
    signal.throwIfAborted();

    const serveCatalog = async () => {
      for await (const demanded of catalogWriter.demandChanges()) {
        if (demanded) await catalogWriter.write(document, { properties: locProperties(0), signal });
      }
    };
    const sendVideo = async () => {
      let group: RelayGroupWriter | undefined;
      let startedAt: number | undefined;
      let firstTimestamp = 0;
      try {
        // Mediabunny yields decode order. Preserve it even when B-frame presentation timestamps differ.
        for await (const packet of new EncodedPacketSink(track).packets()) {
          signal.throwIfAborted();
          const frame = packetToLocFrame(packet, video.initData ? { videoConfig: video.initData } : {});
          if (startedAt !== undefined) {
            await wait(startedAt + (frame.timestampUs - firstTimestamp) / 1000 - performance.now(), signal);
          }
          if (frame.key) {
            await group?.close();
            group = await videoWriter.openGroup({ signal });
            if (startedAt === undefined) {
              startedAt = performance.now();
              firstTimestamp = frame.timestampUs;
            }
          } else if (!videoWriter.hasDemand) {
            group?.abort();
            group = undefined;
          }
          if (group) await group.write(frame.payload, { properties: frame.properties });
        }
        await group?.close();
      } finally {
        group?.abort();
        catalogWriter.close();
      }
    };
    tasks = [serveCatalog(), sendVideo()];
    await Promise.all(tasks);
  } finally {
    lifetime.abort(new Error('File publishing finished'));
    closeTracks();
    signal.removeEventListener('abort', closeTracks);
    await Promise.allSettled(tasks);
    input.dispose();
  }
}
