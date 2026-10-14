import {
  AUDIO_TRACK,
  type AudioTrackInfo,
  audioTrackFromDecoderConfig,
  type BroadcastClock,
  buildMsfCatalog,
  CATALOG_TRACK,
  chunkToLocFrame,
  createBroadcastClock,
  createMediaTimeline,
  locProperties,
  locTrackProperties,
  MEDIA_TIMELINE_TRACK,
  type SourceTimeline,
  VIDEO_TRACK,
  type VideoTrackInfo,
  videoTrackFromDecoderConfig,
} from '@mux/moq-media';
import { RelayError, type RelayGroupWriter, type RelayPublisher, type RelayTrackWriter } from '@mux/relay';
import type { MicrophoneBlock, MicrophoneCapture } from './capture-microphone.js';

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

const isSubscriptionChange = (error: unknown) => error instanceof RelayError && error.code === 'SUBSCRIPTION_CHANGED';

/** Options for {@link publishCanvas}. */
export interface PublishCanvasOptions {
  /** Aborting stops publishing. */
  signal: AbortSignal;
  /**
   * The broadcast clock every track is stamped from. Default: the microphone's, or a new one.
   */
  clock?: BroadcastClock;
  /** A microphone to publish as Opus on the same timeline, from `captureMicrophone`. Capture stays the caller's. */
  microphone?: MicrophoneCapture;
}

/**
 * Publish a live canvas using WebCodecs directly: fixed-size AVC, 30 fps, a keyframe at least every 2 s. With a
 * microphone, also mono Opus in 20 ms frames, one group per frame. Video is stamped with `clock.now()` at capture and
 * audio through the microphone's calibrated timeline, so both share one broadcast timeline. A media timeline track maps
 * each video group's keyframe to its capture time as Unix-epoch time (msf-01 §7).
 * The caller owns drawing/capture and the connection. Use a fresh publisher/namespace for each run.
 * Abort stops this example; encoder, configuration changes, queue overflow, and write errors are fatal.
 */
export async function publishCanvas(
  canvas: HTMLCanvasElement | OffscreenCanvas,
  publisher: RelayPublisher,
  namespace: readonly string[],
  options: PublishCanvasOptions,
): Promise<void> {
  const { microphone } = options;
  if (microphone && options.clock && options.clock !== microphone.clock) {
    throw new Error('The microphone must be captured on the same broadcast clock');
  }
  const clock = options.clock ?? microphone?.clock ?? createBroadcastClock();
  const lifetime = new AbortController();
  const signal = AbortSignal.any([options.signal, lifetime.signal]);
  const fail = (error: unknown) => lifetime.abort(error);
  void publisher.closed.then(({ error }) => fail(error ?? new Error('Publisher closed')));
  let catalogWriter: RelayTrackWriter | undefined;
  let videoWriter: RelayTrackWriter | undefined;
  let audioWriter: RelayTrackWriter | undefined;
  let timelineWriter: RelayTrackWriter | undefined;
  let encoder: VideoEncoder | undefined;
  let audioEncoder: AudioEncoder | undefined;
  let audioTimeline: SourceTimeline | undefined;
  let audio: AudioTrackInfo | undefined;
  let group: RelayGroupWriter | undefined;
  let video: VideoTrackInfo | undefined;
  let catalogBytes: Uint8Array | undefined;
  let catalogTask: Promise<void> | undefined;
  let demandTask: Promise<void> | undefined;
  let timelineTask: Promise<void> | undefined;
  let timelineDocument: Uint8Array | undefined;
  let writes = Promise.resolve();
  let timelineWrites = Promise.resolve();
  let audioWrites = Promise.resolve();
  let pendingChunks = 0;
  let pendingAudio = 0;
  let needKeyframe = true;
  const shutdown = () => {
    if (microphone) microphone.onblock = undefined;
    if (encoder && encoder.state !== 'closed') encoder.close();
    if (audioEncoder && audioEncoder.state !== 'closed') audioEncoder.close();
    group?.abort(signal.reason);
    catalogWriter?.close();
    videoWriter?.close();
    audioWriter?.close();
    timelineWriter?.close();
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
    const audioConfig =
      microphone &&
      ({
        codec: 'opus',
        sampleRate: microphone.context.sampleRate,
        numberOfChannels: 1,
        bitrate: 64_000,
      } satisfies AudioEncoderConfig);
    if (audioConfig) {
      const { supported } = await AudioEncoder.isConfigSupported(audioConfig);
      signal.throwIfAborted();
      if (!supported) throw new Error('This browser cannot encode the microphone with the example Opus configuration');
      // Opus needs no description, so the catalog entry is known before the first chunk.
      audio = { ...audioTrackFromDecoderConfig(audioConfig), bitrate: audioConfig.bitrate };
    }
    const catalog = publisher.registerTrack({ track: CATALOG_TRACK, trackProperties: locTrackProperties() });
    catalogWriter = catalog;
    const track = publisher.registerTrack({ track: VIDEO_TRACK, trackProperties: locTrackProperties() });
    videoWriter = track;
    const audioTrack = audio && publisher.registerTrack({ track: AUDIO_TRACK, trackProperties: locTrackProperties() });
    audioWriter = audioTrack;
    const timelineTrack = publisher.registerTrack({
      track: MEDIA_TIMELINE_TRACK,
      trackProperties: locTrackProperties(),
    });
    timelineWriter = timelineTrack;
    // One entry per video group that opened on the relay, each new document a new group (spec/media-layer.md).
    const mediaTimeline = createMediaTimeline();

    const serveCatalog = async (bytes: Uint8Array) => {
      for await (const demanded of catalog.demandChanges()) {
        if (!demanded) continue;
        try {
          await catalog.write(bytes, { properties: locProperties(0), signal });
        } catch (error) {
          // The next queued true event serves the replacement subscription.
          if (catalog.closed || !isSubscriptionChange(error)) throw error;
        }
      }
    };
    // The timeline is state-shaped like the catalog, but its document changes: send the latest on every relay
    // subscription and on every new entry while subscribed. Python's mux_relay.media.DocumentService does the same.
    const sendTimeline = () => {
      const bytes = timelineDocument;
      if (!bytes || !timelineTrack.hasDemand) return;
      timelineWrites = timelineWrites
        .then(async () => {
          signal.throwIfAborted();
          // A newer document queued its own send; this one is already out of date.
          if (bytes !== timelineDocument || !timelineTrack.hasDemand) return;
          await timelineTrack.write(bytes, { properties: locProperties(0), signal });
        })
        .catch((error: unknown) => {
          // The next queued true event serves the replacement subscription.
          if (signal.aborted || timelineTrack.closed || !isSubscriptionChange(error)) fail(error);
        });
    };
    const serveTimeline = async () => {
      for await (const demanded of timelineTrack.demandChanges()) if (demanded) sendTimeline();
    };
    timelineTask = serveTimeline().catch(fail);
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
            const nextCatalogBytes = buildMsfCatalog({
              namespace,
              video: nextVideo,
              ...(audio ? { audio } : {}),
              mediaTimeline: {},
            });
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
          const timestampUs = chunk.timestamp;
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
                // Held locally: a demand change during the write may clear `group` before this resumes.
                const opened = await track.openGroup({ signal });
                group = opened;
                await opened.write(frame.payload, { properties: frame.properties });
                // The keyframe is out: the group is a place a viewer can seek to.
                timelineDocument = mediaTimeline.add({ timestampUs, group: opened.groupId });
                sendTimeline();
                return;
              }
              // Deltas cannot start a group after startup or a loss of demand.
              if (group) await group.write(frame.payload, { properties: frame.properties });
            })
            .catch((error: unknown) => {
              if (!signal.aborted && !track.closed && isSubscriptionChange(error)) {
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
    if (microphone && audioTrack && audioConfig) {
      // Every audio frame is its own group: Opus frames decode independently, so a lost one costs only itself.
      const writeAudio = (chunk: EncodedAudioChunk, timestampUs: number) => {
        // Before the Unix epoch, as on a clock pinned near zero: LOC has no place for it. Without demand, unsent.
        if (timestampUs < 0 || !audioTrack.hasDemand) return;
        if (pendingAudio >= 25) throw new Error('Encoded audio queue is full; transport cannot keep up');
        const frame = chunkToLocFrame(chunk, { timestampOffsetUs: timestampUs - chunk.timestamp });
        pendingAudio++;
        audioWrites = audioWrites
          .then(async () => {
            signal.throwIfAborted();
            if (audioTrack.hasDemand) await audioTrack.write(frame.payload, { properties: frame.properties, signal });
          })
          .catch((error: unknown) => {
            if (signal.aborted || audioTrack.closed || !isSubscriptionChange(error)) fail(error);
          })
          .finally(() => pendingAudio--);
      };
      // Encoders do not carry input timestamps through: Chromium's counts samples from its first input and ignores
      // gaps, Firefox's subtracts its lookahead. So the encoder gets one contiguous count, and each chunk is mapped
      // back to context time through the block it started in, then onto the broadcast timeline. The block also keeps
      // the capture latency of the microphone it came from: a device switch must not restamp audio still encoding.
      let fed: { encoderUs: number; contextUs: number; captureLatencyUs: number }[] = [];
      let framesFed = 0;
      const blockOf = (encoderUs: number) => {
        while (fed.length > 1 && (fed[1] as (typeof fed)[number]).encoderUs <= encoderUs) fed.shift();
        return fed[0];
      };
      const encodeBlock = (block: MicrophoneBlock) => {
        if (signal.aborted) return;
        try {
          // A new context brings a new timeline, and its time starts over: give it a fresh encoder.
          if (block.timeline !== audioTimeline || !audioEncoder) {
            if (audioEncoder && audioEncoder.state !== 'closed') audioEncoder.close();
            const timeline = block.timeline;
            audioTimeline = timeline;
            // A closed encoder delivers no more output, so the old map can go.
            fed = [];
            framesFed = 0;
            audioEncoder = new AudioEncoder({
              error: fail,
              output(chunk) {
                if (signal.aborted) return;
                try {
                  const from = blockOf(chunk.timestamp);
                  if (from === undefined) return;
                  const contextUs = from.contextUs + (chunk.timestamp - from.encoderUs);
                  // timestampUs subtracts the latency current now; put back the difference from the block's own.
                  const latencyShiftUs = timeline.captureLatencyUs - from.captureLatencyUs;
                  writeAudio(chunk, timeline.timestampUs(contextUs) + latencyShiftUs);
                } catch (error) {
                  fail(error);
                }
              },
            });
            audioEncoder.configure({ ...audioConfig, sampleRate: block.sampleRate });
          }
          // Encode every block, demand or not, so the encoder's count runs with the context's.
          if (audioEncoder.encodeQueueSize >= 25) throw new Error('The audio encoder cannot keep up');
          const encoderUs = Math.round((framesFed * 1_000_000) / block.sampleRate);
          fed.push({ encoderUs, contextUs: block.contextUs, captureLatencyUs: block.captureLatencyUs });
          framesFed += block.samples.length;
          const data = new AudioData({
            format: 'f32-planar',
            sampleRate: block.sampleRate,
            numberOfFrames: block.samples.length,
            numberOfChannels: 1,
            timestamp: encoderUs,
            data: block.samples,
          });
          try {
            audioEncoder.encode(data);
          } finally {
            data.close();
          }
        } catch (error) {
          fail(error);
        }
      };
      microphone.onblock = encodeBlock;
    }
    let lastKeyframeUs = Number.NEGATIVE_INFINITY;
    while (!signal.aborted) {
      // Bootstrap the catalog, then capture only with demand and space downstream.
      // Skipping raw capture preserves every encoded delta already queued.
      if ((!video || track.hasDemand) && encoder.encodeQueueSize < 2 && pendingChunks < 2) {
        const timestamp = clock.now();
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
    await Promise.allSettled([writes, audioWrites, timelineWrites, catalogTask, demandTask, timelineTask]);
  }
}
