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
  const abort = () => {
    closeTracks();
    // Dispose before awaiting the tasks: a task may be blocked reading metadata or the next packet.
    input.dispose();
  };
  signal.addEventListener('abort', abort, { once: true });
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
    signal.throwIfAborted();

    let videoDone = false;
    let catalogWrite: Promise<void> | undefined;
    const serveCatalog = async () => {
      for await (const demanded of catalogWriter.demandChanges()) {
        if (videoDone) break;
        if (demanded) {
          catalogWrite = catalogWriter.write(document, { properties: locProperties(0), signal });
          await catalogWrite;
          catalogWrite = undefined;
        }
      }
    };
    const sendVideo = async () => {
      let group: RelayGroupWriter | undefined;
      let startedAt: number | undefined;
      let decodeTimeMs = 0;
      try {
        // Mediabunny yields decode order. Preserve it even when B-frame presentation timestamps differ.
        for await (const packet of new EncodedPacketSink(track).packets()) {
          signal.throwIfAborted();
          const frame = packetToLocFrame(packet, video.initData ? { videoConfig: video.initData } : {});
          if (startedAt !== undefined) {
            await wait(startedAt + decodeTimeMs - performance.now(), signal);
          }
          if (startedAt !== undefined && !videoWriter.hasDemand) {
            // After startup keep the clock moving without waiting for replacement demand.
            group?.abort();
            group = undefined;
          } else if (frame.key) {
            await group?.close();
            group = await videoWriter.openGroup({ signal });
            if (startedAt === undefined) {
              startedAt = performance.now();
            }
          }
          if (group) await group.write(frame.payload, { properties: frame.properties });
          // PTS can run backwards for B-frames. Pace decode order by durations; preserve PTS in LOC.
          if (startedAt !== undefined) decodeTimeMs += packet.duration * 1000;
        }
        await group?.close();
        // Stop accepting demand before draining the current write; closing a track cancels writes.
        videoDone = true;
        await catalogWrite;
        catalogWriter.close();
      } finally {
        group?.abort();
      }
    };
    tasks = [serveCatalog(), sendVideo()];
    await Promise.all(tasks);
  } catch (error) {
    // Input disposal is how reads are interrupted; preserve the caller's cancellation reason.
    signal.throwIfAborted();
    throw error;
  } finally {
    lifetime.abort(new Error('File publishing finished'));
    abort();
    signal.removeEventListener('abort', abort);
    await Promise.allSettled(tasks);
  }
}
