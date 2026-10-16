import {
  AUDIO_ONLY_TARGET_LATENCY_US,
  AUDIO_TRACK,
  type AudioPlayer,
  type AudioPlayerInput,
  audioDecoderConfigFromTrack,
  CATALOG_TRACK,
  createAudioPlayer,
  createEchoCancellationLoopback,
  createFrameSequencer,
  createPlayoutClock,
  type EchoCancellationLoopback,
  type FrameSequencer,
  type MsfCatalogTrack,
  type PlayoutClock,
  parseMsfCatalog,
} from '@mux/moq-media';
import type { RelaySubscriber, RelaySubscription } from '@mux/relay';
import { createFrameReader } from '@mux/relay/media';

/** Options for {@link listenAudio}. */
export interface ListenAudioOptions {
  /** The context to play through; create or resume it from a user gesture. Default: a new 48 kHz context. */
  context?: AudioContext;
  /**
   * Play through a WebRTC loopback, so Chromium's echo canceller hears this audio when the page also captures a
   * microphone. Default `false`: connect straight to the context's destination.
   */
  echoCancellation?: boolean;
  /** A failure after the listener started: the audio subscription ended with an error, or a decoder failed. */
  onError?(error: unknown): void;
}

/** A running listener. */
export interface AudioListener {
  readonly context: AudioContext;
  readonly player: AudioPlayer<GainNode>;
  readonly input: AudioPlayerInput;
  readonly clock: PlayoutClock;
  readonly sequencer: FrameSequencer;
  /** Stop playing and cancel the subscriptions. A context the listener created is closed too. */
  stop(): Promise<void>;
}

function readCatalog(subscriber: RelaySubscriber, namespace: readonly string[]) {
  return new Promise<{ tracks: MsfCatalogTrack[]; subscription: RelaySubscription }>((resolve, reject) => {
    const subscription = subscriber.subscribe(
      { namespace: [...namespace], track: CATALOG_TRACK },
      {
        onObject(object) {
          if (object.status !== 'normal') return;
          try {
            resolve({ tracks: parseMsfCatalog(object.payload, { catalogNamespace: namespace }).tracks, subscription });
          } catch (error) {
            reject(error);
          }
        },
        onError: reject,
      },
    );
  });
}

/**
 * Play the audio track of the broadcast at `namespace`, such as the WebCodecs example's microphone, with no video:
 * the catalog gives the decoder config, a frame reader orders the frames, and the audio player buffers and plays
 * them. The playout clock is created for audio only, so it does not wait 2 s for keyframes that never come.
 */
export async function listenAudio(
  subscriber: RelaySubscriber,
  namespace: readonly string[],
  options: ListenAudioOptions = {},
): Promise<AudioListener> {
  const ownsContext = !options.context;
  const context = options.context ?? new AudioContext({ sampleRate: 48_000, latencyHint: 'interactive' });
  const { tracks, subscription: catalog } = await readCatalog(subscriber, namespace);
  const track = tracks.find((candidate) => candidate.name === AUDIO_TRACK);
  if (!track) {
    catalog.cancel();
    throw new Error(`The catalog at ${namespace.join('/')} lists no ${AUDIO_TRACK} track`);
  }

  const player = await createAudioPlayer({ context });
  let loopback: EchoCancellationLoopback | undefined;
  if (options.echoCancellation) loopback = await createEchoCancellationLoopback(context, player.output);
  else player.output.connect(context.destination);

  const clock = createPlayoutClock({ targetLatencyUs: AUDIO_ONLY_TARGET_LATENCY_US });
  const input = player.addInput({
    clock,
    decoderConfig: audioDecoderConfigFromTrack(track),
    ...(options.onError ? { onError: options.onError } : {}),
  });
  const sequencer = createFrameSequencer({ clock, grouping: 'frame' });
  const reader = createFrameReader(sequencer, { onFrame: input.push });
  const audio = subscriber.subscribe(
    { namespace: [...namespace], track: AUDIO_TRACK },
    { ...reader, onError: (error) => options.onError?.(error) },
  );

  return {
    context,
    player,
    input,
    clock,
    sequencer,
    async stop() {
      reader.close();
      audio.cancel();
      catalog.cancel();
      loopback?.close();
      player.close();
      if (ownsContext) await context.close();
    },
  };
}
