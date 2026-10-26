import {
  AUDIO_TRACK,
  type AudioTrackInfo,
  audioTrackFromDecoderConfig,
  buildMsfCatalog,
  CATALOG_TRACK,
  chunkToLocFrame,
  type DataTrackInfo,
  type LocFrame,
  locTrackProperties,
  type SourceTimeline,
} from '@caprid/moq-media';
import type { RelayPublisher, RelayTrackWriter } from '@caprid/relay';
import { createCatalogService, createFrameWriter, type FrameOutcome, mediaTrackProperties } from '@caprid/relay/media';
import type { MicrophoneBlock, MicrophoneCapture } from './capture-microphone.js';

/** A microphone's Opus encoder configuration, and the catalog entry it gives the audio track. */
export interface MicrophoneOpus {
  config: AudioEncoderConfig;
  track: AudioTrackInfo;
}

/** Mono Opus at 64 kb/s for `microphone`. Throws when the browser cannot encode it. */
export async function microphoneOpus(microphone: MicrophoneCapture, signal: AbortSignal): Promise<MicrophoneOpus> {
  const config = {
    codec: 'opus',
    sampleRate: microphone.context.sampleRate,
    numberOfChannels: 1,
    bitrate: 64_000,
  } satisfies AudioEncoderConfig;
  const { supported } = await AudioEncoder.isConfigSupported(config);
  signal.throwIfAborted();
  if (!supported) throw new Error('This browser cannot encode the microphone with the example Opus configuration');
  // Opus needs no description, so the catalog entry is known before the first chunk.
  return { config, track: { ...audioTrackFromDecoderConfig(config), bitrate: config.bitrate } };
}

/** Options for {@link encodeMicrophone}. */
export interface EncodeMicrophoneOptions {
  /** Aborting stops encoding. */
  signal: AbortSignal;
  /** What became of each frame written. */
  onOutcome(outcome: FrameOutcome): void;
  /** An encoder failure, which stops encoding. */
  onError(error: unknown): void;
}

/** A running microphone encoder. */
export interface MicrophoneEncoding {
  /** Stop encoding and close the frame writer, waiting for frames it is still writing. */
  close(): Promise<void>;
}

/**
 * Encode `microphone`'s blocks with WebCodecs and write the chunks to `track`, one group per frame, stamped on the
 * broadcast timeline through the microphone's calibrated timeline. Takes over `microphone.onblock`.
 */
export function encodeMicrophone(
  microphone: MicrophoneCapture,
  track: RelayTrackWriter,
  config: AudioEncoderConfig,
  options: EncodeMicrophoneOptions,
): MicrophoneEncoding {
  const { signal } = options;
  // Every audio frame is its own group: Opus frames decode independently, so a lost one costs only itself, and a
  // transport that falls behind drops the oldest frames first.
  const frames = createFrameWriter<LocFrame>(track, { grouping: 'frame' });
  let encoder: AudioEncoder | undefined;
  let timeline: SourceTimeline | undefined;
  const stop = () => {
    microphone.onblock = undefined;
    if (encoder && encoder.state !== 'closed') encoder.close();
  };
  const fail = (error: unknown) => {
    stop();
    options.onError(error);
  };
  signal.addEventListener('abort', stop, { once: true });
  const writeAudio = (chunk: EncodedAudioChunk, timestampUs: number) => {
    // Before the Unix epoch, as on a clock pinned near zero: LOC has no place for it. Without demand, unsent.
    if (timestampUs < 0 || !track.hasDemand) return;
    const frame = chunkToLocFrame(chunk, { timestampOffsetUs: timestampUs - chunk.timestamp });
    frames.write(frame).then(options.onOutcome).catch(fail);
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
      if (block.timeline !== timeline || !encoder) {
        if (encoder && encoder.state !== 'closed') encoder.close();
        const blockTimeline = block.timeline;
        timeline = blockTimeline;
        // A closed encoder delivers no more output, so the old map can go.
        fed = [];
        framesFed = 0;
        encoder = new AudioEncoder({
          error: fail,
          output(chunk) {
            if (signal.aborted) return;
            try {
              const from = blockOf(chunk.timestamp);
              if (from === undefined) return;
              const contextUs = from.contextUs + (chunk.timestamp - from.encoderUs);
              // timestampUs subtracts the latency current now; put back the difference from the block's own.
              const latencyShiftUs = blockTimeline.captureLatencyUs - from.captureLatencyUs;
              writeAudio(chunk, blockTimeline.timestampUs(contextUs) + latencyShiftUs);
            } catch (error) {
              fail(error);
            }
          },
        });
        encoder.configure({ ...config, sampleRate: block.sampleRate });
      }
      // Encode every block, demand or not, so the encoder's count runs with the context's.
      if (encoder.encodeQueueSize >= 25) throw new Error('The audio encoder cannot keep up');
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
        encoder.encode(data);
      } finally {
        data.close();
      }
    } catch (error) {
      fail(error);
    }
  };
  microphone.onblock = encodeBlock;
  return {
    async close() {
      stop();
      signal.removeEventListener('abort', stop);
      await frames.close();
    },
  };
}

/** Options for {@link publishMicrophone}. */
export interface PublishMicrophoneOptions {
  /** Aborting stops publishing. */
  signal: AbortSignal;
  /** The microphone to publish, from `captureMicrophone`. Capture stays the caller's. */
  microphone: MicrophoneCapture;
  /** More tracks to list in the catalog as data tracks. The caller registers and writes them on the same publisher. */
  data?: readonly DataTrackInfo[];
}

/**
 * Publish a microphone alone, as a voice participant does: a catalog listing one audio track, and mono Opus in 20 ms
 * frames, one group per frame, stamped on the microphone's broadcast timeline. Runs until aborted; an encoder error or
 * a track that ends is fatal. The caller owns capture and the connection.
 */
export async function publishMicrophone(
  publisher: RelayPublisher,
  namespace: readonly string[],
  options: PublishMicrophoneOptions,
): Promise<void> {
  const { microphone } = options;
  const lifetime = new AbortController();
  const signal = AbortSignal.any([options.signal, lifetime.signal]);
  const fail = (error: unknown) => {
    if (!signal.aborted) lifetime.abort(error);
  };
  void publisher.closed.then(({ error }) => fail(error ?? new Error('Publisher closed')));
  const tracks: RelayTrackWriter[] = [];
  const serving: Promise<void>[] = [];
  let encoding: MicrophoneEncoding | undefined;
  try {
    const opus = await microphoneOpus(microphone, signal);
    const register = (track: string, media: ReturnType<typeof mediaTrackProperties> = []) => {
      const writer = publisher.registerTrack({ track, trackProperties: [...locTrackProperties(), ...media] });
      tracks.push(writer);
      // A track's demand changes end when the track does.
      serving.push(
        (async () => {
          for await (const _demanded of writer.demandChanges()) if (signal.aborted) return;
          fail(new Error(`The ${track} track ended`));
        })().catch(fail),
      );
      return writer;
    };
    const catalogTrack = register(CATALOG_TRACK);
    const audioTrack = register(AUDIO_TRACK, mediaTrackProperties('audio'));
    const catalog = createCatalogService(
      catalogTrack,
      buildMsfCatalog({ namespace, audio: opus.track, ...(options.data ? { data: options.data } : {}) }),
    );
    serving.push(catalog.serve({ signal }).catch(fail));
    encoding = encodeMicrophone(microphone, audioTrack, opus.config, {
      signal,
      onOutcome: (outcome) => {
        if (outcome.kind === 'ended') fail(outcome.error);
      },
      onError: fail,
    });
    await new Promise((_, reject) => {
      if (signal.aborted) reject(signal.reason);
      signal.addEventListener('abort', () => reject(signal.reason), { once: true });
    });
  } finally {
    lifetime.abort(new Error('Microphone publishing finished'));
    for (const track of tracks) track.close();
    await Promise.allSettled([encoding?.close(), ...serving]);
  }
}
