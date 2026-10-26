import {
  buildMsfCatalog,
  buildMsfEndCatalog,
  CATALOG_TRACK,
  type LocFrame,
  locTrackProperties,
  VIDEO_TRACK,
  videoTrackFromDecoderConfig,
} from '@caprid/moq-media';
import { packetToLocFrame } from '@caprid/moq-mediabunny';
import type { RelayPublisher, RelayTrackWriter } from '@caprid/relay';
import { createCatalogService, createFrameWriter, type FrameWriter } from '@caprid/relay/media';
import { BlobSource, EncodedPacketSink, Input, MP4 } from 'mediabunny';

function wait(milliseconds: number, signal: AbortSignal): Promise<void> {
  signal.throwIfAborted();
  if (milliseconds <= 0) return Promise.resolve();
  return new Promise((resolve, reject) => {
    const abort = () => {
      clearTimeout(timer);
      reject(signal.reason);
    };
    const timer = setTimeout(() => {
      signal.removeEventListener('abort', abort);
      resolve();
    }, milliseconds);
    signal.addEventListener('abort', abort, { once: true });
  });
}

/**
 * A track ends on its own only with its publisher connection, and `publisher.closed` then aborts the run with the
 * reason. Wait for that rather than report a less specific error first.
 */
function connectionEnded(signal: AbortSignal): Promise<never> {
  return new Promise((_, reject) => {
    const abort = () => reject(signal.reason);
    if (signal.aborted) abort();
    else signal.addEventListener('abort', abort, { once: true });
  });
}

/** Wait until the relay subscribes to `track`. */
async function demanded(track: RelayTrackWriter, signal: AbortSignal): Promise<void> {
  signal.throwIfAborted();
  if (track.hasDemand) return;
  const changes = track.demandChanges();
  const abort = () => void changes.return?.();
  signal.addEventListener('abort', abort, { once: true });
  try {
    for await (const demand of changes) if (demand) return;
  } finally {
    signal.removeEventListener('abort', abort);
  }
  return connectionEnded(signal);
}

/**
 * Publish the primary AVC video track of an MP4 once, paced in real time. No decoding or encoding.
 * A frame writer from `@caprid/relay/media` maps packets onto groups and a catalog service serves the catalog on every
 * relay subscription. The caller owns the connection. Use a fresh publisher/namespace for each run; track names cannot
 * be reused. This deliberately small example stops when a track ends and omits audio and reconnect policy.
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
  const fail = (error: unknown) => lifetime.abort(error);
  void publisher.closed.then(({ error }) => fail(error ?? new Error('Publisher closed')));
  // Stops the catalog service at EOF, before the end-of-broadcast catalog, as well as on abort.
  const serving = new AbortController();
  const fileRanOut = new Error('The file ran out');
  const tracks: RelayTrackWriter[] = [];
  let writer: FrameWriter<LocFrame> | undefined;
  let catalogServed: Promise<void> | undefined;
  const abort = () => {
    for (const track of tracks) track.close();
    // Dispose before awaiting anything: a read of metadata or the next packet may be blocked.
    input.dispose();
  };
  signal.addEventListener('abort', abort, { once: true });
  try {
    signal.throwIfAborted();
    const track = await input.getPrimaryVideoTrack();
    const config = await track?.getDecoderConfig();
    if (!track || !config) throw new Error('The MP4 must contain an AVC video track with decoder configuration');
    // The decoder config has no bitrate or framerate. These are estimates from the first 100 packets.
    const stats = await track.computePacketStats(100);
    const bitrate = Math.round(stats.averageBitrate);
    const framerate = Math.round(stats.averagePacketRate * 1000) / 1000;
    const video = {
      ...videoTrackFromDecoderConfig(config),
      ...(Number.isFinite(bitrate) && bitrate > 0 && { bitrate }),
      ...(Number.isFinite(framerate) && framerate > 0 && { framerate }),
    };
    signal.throwIfAborted();
    const catalogTrack = publisher.registerTrack({ track: CATALOG_TRACK, trackProperties: locTrackProperties() });
    tracks.push(catalogTrack);
    const videoTrack = publisher.registerTrack({ track: VIDEO_TRACK, trackProperties: locTrackProperties() });
    tracks.push(videoTrack);

    // The catalog goes out on every relay subscription; a write that fails while the track stays open is retried.
    const catalog = createCatalogService(catalogTrack, buildMsfCatalog({ namespace, video }));
    // Only stopping it at EOF is expected: a failure that lands meanwhile still fails the run.
    catalogServed = catalog.serve({ signal: AbortSignal.any([signal, serving.signal]) }).then(
      // The track ended with the connection, which reports why.
      () => {},
      (error: unknown) => {
        if (error !== fileRanOut) fail(error);
      },
    );
    // A keyframe opens each group and deltas join it. Without relay demand a frame is skipped at once, so pacing goes
    // on; publishing resumes at the next keyframe. Writes are awaited one at a time, so the backlog never fills.
    writer = createFrameWriter<LocFrame>(videoTrack);

    // Start the clock when the relay first subscribes to the video, so the first viewer sees the file from its start.
    await demanded(videoTrack, signal);
    const startedAt = performance.now();
    let decodeTimeMs = 0;
    // Mediabunny yields decode order. Preserve it even when B-frame presentation timestamps differ.
    for await (const packet of new EncodedPacketSink(track).packets()) {
      await wait(startedAt + decodeTimeMs - performance.now(), signal);
      const frame = packetToLocFrame(packet, video.initData ? { videoConfig: video.initData } : {});
      const outcome = await writer.write(frame);
      // The video track ended with the connection: that is fatal, and the connection says why.
      if (outcome.kind === 'ended') await connectionEnded(signal);
      // PTS can run backwards for B-frames. Pace decode order by durations; preserve PTS in LOC.
      decodeTimeMs += packet.duration * 1000;
    }
    signal.throwIfAborted();

    // The file ran out, so the broadcast is over (msf-01 §11.3): FIN the last group, end the video track (PUBLISH_DONE
    // Track Ended), stop serving the live catalog, then tell current viewers with the end-of-broadcast catalog.
    // Without demand nobody is listening.
    await writer.close();
    // The frame writer's close swallows a failed FIN; a track that ended under it is not the end of the broadcast.
    if (videoTrack.closed) await connectionEnded(signal);
    videoTrack.close();
    serving.abort(fileRanOut);
    await catalogServed;
    signal.throwIfAborted();
    // The stopped service no longer watches the track: without this, a track closed meanwhile would pass for no demand.
    if (catalogTrack.closed) await connectionEnded(signal);
    if (catalogTrack.hasDemand) await catalogTrack.write(buildMsfEndCatalog({ generatedAt: Date.now() }), { signal });
    catalogTrack.close();
  } catch (error) {
    // Input disposal is how reads are interrupted; preserve the caller's cancellation reason.
    signal.throwIfAborted();
    throw error;
  } finally {
    lifetime.abort(new Error('File publishing finished'));
    abort();
    signal.removeEventListener('abort', abort);
    await Promise.allSettled([writer?.close(), catalogServed]);
  }
}
