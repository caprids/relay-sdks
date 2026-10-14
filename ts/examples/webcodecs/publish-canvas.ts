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
  type LocFrame,
  locTrackProperties,
  MEDIA_TIMELINE_TRACK,
  type SourceTimeline,
  VIDEO_TRACK,
  type VideoTrackInfo,
  videoTrackFromDecoderConfig,
} from '@mux/moq-media';
import type { RelayPublisher, RelayTrackWriter } from '@mux/relay';
import {
  type CatalogService,
  createCatalogService,
  createDocumentService,
  createFrameWriter,
  type FrameOutcome,
  type FrameWriter,
} from '@mux/relay/media';
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
 * Publish a live canvas using WebCodecs directly: fixed-size AVC, 30 fps, a keyframe at least every 2 s and whenever
 * the video track's frame writer needs one. With a microphone, also mono Opus in 20 ms frames, one group per frame.
 * Video is stamped with `clock.now()` at capture and audio through the microphone's calibrated timeline, so both share
 * one broadcast timeline. A media timeline track maps each video group's keyframe to its capture time as Unix-epoch
 * time (msf-01 §7). Frame writers from `@mux/relay/media` map the encoders' output onto groups, and a catalog service
 * and a document service serve the catalog and the timeline on every relay subscription.
 * The caller owns drawing/capture and the connection. Use a fresh publisher/namespace for each run.
 * Abort stops this example; encoder errors, configuration changes, and a track that ends are fatal. A transport that
 * falls behind costs dropped frames, and a failed group costs the frames until the next keyframe.
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
  const tracks: RelayTrackWriter[] = [];
  let videoWriter: FrameWriter<LocFrame> | undefined;
  let audioWriter: FrameWriter<LocFrame> | undefined;
  let encoder: VideoEncoder | undefined;
  let audioEncoder: AudioEncoder | undefined;
  let audioTimeline: SourceTimeline | undefined;
  let audio: AudioTrackInfo | undefined;
  let video: VideoTrackInfo | undefined;
  let catalog: CatalogService | undefined;
  let catalogBytes: Uint8Array | undefined;
  const serving: Promise<void>[] = [];
  /** Set when the video writer comes to need a keyframe; the capture loop forces the next one. */
  let forceKeyframe = false;
  // A frame writer reports what became of each frame instead of raising: only a track that ended stops the example.
  const check = (outcome: FrameOutcome) => {
    if (outcome.kind === 'ended' && !signal.aborted) fail(outcome.error);
  };
  const rejected = (error: unknown) => {
    if (!signal.aborted) fail(error);
  };
  const shutdown = () => {
    if (microphone) microphone.onblock = undefined;
    if (encoder && encoder.state !== 'closed') encoder.close();
    if (audioEncoder && audioEncoder.state !== 'closed') audioEncoder.close();
    for (const track of tracks) track.close();
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
    const register = (track: string) => {
      const writer = publisher.registerTrack({ track, trackProperties: locTrackProperties() });
      tracks.push(writer);
      return writer;
    };
    const catalogTrack = register(CATALOG_TRACK);
    const track = register(VIDEO_TRACK);
    const audioTrack = audio && register(AUDIO_TRACK);
    const timelineTrack = register(MEDIA_TIMELINE_TRACK);
    // A keyframe opens each group, deltas join it. When the transport falls behind, the writer drops its backlog and
    // asks for a keyframe; so does every new relay subscription, rather than wait out the 2 s interval.
    const writer = createFrameWriter<LocFrame>(track, {
      onKeyframeNeeded: () => {
        forceKeyframe = true;
      },
    });
    videoWriter = writer;
    // The timeline is state-shaped like the catalog, but its document changes: one entry per video group that opened
    // on the relay, each new document a new group (spec/media-layer.md).
    const mediaTimeline = createMediaTimeline();
    const timeline = createDocumentService(timelineTrack);
    serving.push(timeline.serve({ signal }).catch(rejected));
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
            if (!catalog) {
              catalogBytes = nextCatalogBytes;
              catalog = createCatalogService(catalogTrack, catalogBytes);
              serving.push(catalog.serve({ signal }).catch(rejected));
            }
          }
          if (!video?.initData) throw new Error('The first encoded chunk must include AVC decoder configuration');
          const frame = chunkToLocFrame(chunk, { videoConfig: video.initData });
          // WebCodecs does not await an output callback: the writer serves frames in order and bounds the backlog.
          writer
            .write(frame)
            .then((outcome) => {
              check(outcome);
              // The keyframe is out: the group is a place a viewer can seek to.
              if (outcome.kind === 'sent' && frame.key && outcome.groupId !== undefined) {
                timeline.update(mediaTimeline.add({ timestampUs: frame.timestampUs, group: outcome.groupId }));
              }
            })
            .catch(rejected);
        } catch (error) {
          fail(error);
        }
      },
    });
    encoder.configure(config);
    if (microphone && audioTrack && audioConfig) {
      // Every audio frame is its own group: Opus frames decode independently, so a lost one costs only itself, and a
      // transport that falls behind drops the oldest frames first.
      const frames = createFrameWriter<LocFrame>(audioTrack, { grouping: 'frame' });
      audioWriter = frames;
      const writeAudio = (chunk: EncodedAudioChunk, timestampUs: number) => {
        // Before the Unix epoch, as on a clock pinned near zero: LOC has no place for it. Without demand, unsent.
        if (timestampUs < 0 || !audioTrack.hasDemand) return;
        const frame = chunkToLocFrame(chunk, { timestampOffsetUs: timestampUs - chunk.timestamp });
        frames.write(frame).then(check).catch(rejected);
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
      // Skipping raw capture preserves every encoded delta already waiting in the writer.
      if ((!video || track.hasDemand) && encoder.encodeQueueSize < 2 && writer.backlog === 0) {
        const timestamp = clock.now();
        const keyFrame = forceKeyframe || timestamp - lastKeyframeUs >= 2_000_000;
        const frame = new VideoFrame(canvas, { timestamp });
        try {
          if (keyFrame) {
            forceKeyframe = false;
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
    await Promise.allSettled([videoWriter?.close(), audioWriter?.close(), ...serving]);
  }
}
