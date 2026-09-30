import {
  buildMsfCatalog,
  CATALOG_TRACK,
  chunkToLocFrame,
  locProperties,
  locTrackProperties,
  VIDEO_TRACK,
  type VideoTrackInfo,
  videoTrackFromDecoderConfig,
} from '@mux/moq-media';
import { RelayError, type RelayGroupWriter, type RelayPublisher, type RelayTrackWriter } from '@mux/relay';

function wait(milliseconds: number, signal: AbortSignal): Promise<void> {
  signal.throwIfAborted();
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
 * Publish a live canvas using WebCodecs directly: fixed-size AVC, 30 fps, a keyframe at least every 2 s.
 * The caller owns drawing/capture and the connection. Use a fresh publisher/namespace for each run.
 * Abort stops this example; encoder, configuration changes, queue overflow, and write errors are fatal.
 */
export async function publishCanvas(
  canvas: HTMLCanvasElement | OffscreenCanvas,
  publisher: RelayPublisher,
  namespace: readonly string[],
  options: { signal: AbortSignal },
): Promise<void> {
  const lifetime = new AbortController();
  const signal = AbortSignal.any([options.signal, lifetime.signal]);
  const fail = (error: unknown) => lifetime.abort(error);
  void publisher.closed.then(({ error }) => fail(error ?? new Error('Publisher closed')));
  let catalogWriter: RelayTrackWriter | undefined;
  let videoWriter: RelayTrackWriter | undefined;
  let encoder: VideoEncoder | undefined;
  let group: RelayGroupWriter | undefined;
  let video: VideoTrackInfo | undefined;
  let catalogBytes: Uint8Array | undefined;
  let catalogTask: Promise<void> | undefined;
  let demandTask: Promise<void> | undefined;
  let writes = Promise.resolve();
  let pendingChunks = 0;
  let needKeyframe = true;
  const shutdown = () => {
    if (encoder && encoder.state !== 'closed') encoder.close();
    group?.abort(signal.reason);
    catalogWriter?.close();
    videoWriter?.close();
  };
  signal.addEventListener('abort', shutdown, { once: true });

  try {
    signal.throwIfAborted();
    const config = {
      codec: 'avc1.42001f',
      width: canvas.width,
      height: canvas.height,
      bitrate: 1_500_000,
      framerate: 30,
      latencyMode: 'realtime',
      avc: { format: 'avc' },
    } as const satisfies VideoEncoderConfig;
    const { supported } = await VideoEncoder.isConfigSupported(config);
    signal.throwIfAborted();
    if (!supported) throw new Error('This browser cannot encode the canvas with the example AVC configuration');
    const catalog = publisher.registerTrack({ track: CATALOG_TRACK, trackProperties: locTrackProperties() });
    catalogWriter = catalog;
    const track = publisher.registerTrack({ track: VIDEO_TRACK, trackProperties: locTrackProperties() });
    videoWriter = track;

    const serveCatalog = async (bytes: Uint8Array) => {
      for await (const demanded of catalog.demandChanges()) {
        if (!demanded) continue;
        try {
          await catalog.write(bytes, { properties: locProperties(0), signal });
        } catch (error) {
          // The next queued true event serves the replacement subscription.
          if (catalog.closed || !(error instanceof RelayError) || error.code !== 'SUBSCRIPTION_CHANGED') throw error;
        }
      }
    };
    const watchDemand = async () => {
      let initial = true;
      for await (const demanded of track.demandChanges()) {
        // The initial snapshot needs no extra keyframe: capture already starts with one.
        if (initial) {
          initial = false;
          continue;
        }
        // Repeated true means a replacement subscription; the write slot retires its old group.
        needKeyframe = true;
        if (!demanded || group?.closed) {
          group?.abort();
          group = undefined;
        }
      }
    };
    demandTask = watchDemand().catch(fail);
    encoder = new VideoEncoder({
      error: fail,
      output(chunk, metadata) {
        if (signal.aborted) return;
        try {
          // WebCodecs need not repeat the config on later output callbacks.
          if (metadata?.decoderConfig) {
            // A decoder config carries no bitrate or framerate; take them from the encoder config.
            const nextVideo: VideoTrackInfo = {
              ...videoTrackFromDecoderConfig(metadata.decoderConfig),
              bitrate: config.bitrate,
              framerate: config.framerate,
            };
            const nextCatalogBytes = buildMsfCatalog({ namespace, video: nextVideo });
            if (
              catalogBytes &&
              (catalogBytes.length !== nextCatalogBytes.length ||
                catalogBytes.some((byte, index) => byte !== nextCatalogBytes[index]))
            ) {
              throw new Error('Decoder configuration changed; start a fresh publishing session');
            }
            video = nextVideo;
            if (!catalogBytes) {
              catalogBytes = nextCatalogBytes;
              catalogTask = serveCatalog(catalogBytes).catch(fail);
            }
          }
          if (!video?.initData) throw new Error('The first encoded chunk must include AVC decoder configuration');
          // Bound callback buffering: WebCodecs does not await an async output callback.
          if (pendingChunks >= 8) throw new Error('Encoded output queue is full; transport cannot keep up');
          const frame = chunkToLocFrame(chunk, { videoConfig: video.initData });
          pendingChunks++;
          writes = writes
            .then(async () => {
              signal.throwIfAborted();
              if (!track.hasDemand) {
                group?.abort();
                group = undefined;
                needKeyframe = true;
                return;
              }
              if (group?.closed) {
                group = undefined;
                needKeyframe = true;
              }
              if (frame.key) {
                await group?.close();
                group = await track.openGroup({ signal });
              }
              // Deltas cannot start a group after startup or a loss of demand.
              if (group) await group.write(frame.payload, { properties: frame.properties });
            })
            .catch((error: unknown) => {
              if (
                !signal.aborted &&
                !track.closed &&
                error instanceof RelayError &&
                error.code === 'SUBSCRIPTION_CHANGED'
              ) {
                group?.abort();
                group = undefined;
                needKeyframe = true;
              } else {
                fail(error);
              }
            })
            .finally(() => pendingChunks--);
        } catch (error) {
          fail(error);
        }
      },
    });
    encoder.configure(config);
    const startedAt = performance.now();
    let lastKeyframeUs = Number.NEGATIVE_INFINITY;
    while (!signal.aborted) {
      // Bootstrap the catalog, then capture only with demand and space downstream.
      // Skipping raw capture preserves every encoded delta already queued.
      if ((!video || track.hasDemand) && encoder.encodeQueueSize < 2 && pendingChunks < 2) {
        const timestamp = Math.round((performance.now() - startedAt) * 1000);
        const keyFrame = needKeyframe || timestamp - lastKeyframeUs >= 2_000_000;
        const frame = new VideoFrame(canvas, { timestamp });
        try {
          if (keyFrame) {
            needKeyframe = false;
            lastKeyframeUs = timestamp;
          }
          encoder.encode(frame, { keyFrame });
        } finally {
          frame.close();
        }
      }
      await wait(1000 / 30, signal);
    }
    signal.throwIfAborted();
  } finally {
    lifetime.abort(new Error('Canvas publishing finished'));
    shutdown();
    signal.removeEventListener('abort', shutdown);
    await Promise.allSettled([writes, catalogTask, demandTask]);
  }
}
