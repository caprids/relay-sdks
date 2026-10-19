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
  createTranscript,
  type EchoCancellationLoopback,
  type FrameSequencer,
  INTERRUPT_TRACK,
  type Interrupt,
  type MsfCatalogTrack,
  type PlayoutClock,
  parseInterrupt,
  parseMsfCatalog,
  parseTranscriptSegment,
  TRANSCRIPT_TRACK,
  type Transcript,
  type TranscriptSegment,
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
  /**
   * A failure after the listener started: the audio, interrupt or transcript subscription ended with an error, a
   * decoder failed, or an interrupt or transcript object did not parse.
   */
  onError?(error: unknown): void;
  /** An interrupt for the audio track, called after the player and the transcript have cut. */
  onInterrupt?(interrupt: Interrupt): void;
  /** A transcript segment for the audio track that changed `transcript`. */
  onTranscript?(segment: TranscriptSegment): void;
  /**
   * Aborting while the listener starts cancels what it set up and rejects with the reason, as when the catalog is
   * slow to arrive. Once started, stop it with `stop()`.
   */
  signal?: AbortSignal;
}

/** A running listener. */
export interface AudioListener {
  readonly context: AudioContext;
  readonly player: AudioPlayer<GainNode>;
  readonly input: AudioPlayerInput;
  readonly clock: PlayoutClock;
  readonly sequencer: FrameSequencer;
  /**
   * The audio track's transcript, when the catalog lists the transcript track: `transcript.linesAt(clock.position())`
   * is what has been said so far. Interrupts cut it as they cut the player.
   */
  readonly transcript: Transcript;
  /** Stop playing and cancel the subscriptions. A context the listener created is closed too. */
  stop(): Promise<void>;
}

function readCatalog(subscriber: RelaySubscriber, namespace: readonly string[], signal: AbortSignal | undefined) {
  return new Promise<{ tracks: MsfCatalogTrack[]; subscription: RelaySubscription }>((resolve, reject) => {
    signal?.throwIfAborted();
    let subscription: RelaySubscription | undefined;
    // The first catalog object, or the first error, decides; later ones change nothing.
    let outcome: { tracks: MsfCatalogTrack[] } | { error: unknown; ended: boolean } | undefined;
    // An object may arrive before subscribe() returns, so the outcome waits until the subscription is in hand.
    const settle = () => {
      if (!outcome || !subscription) return;
      signal?.removeEventListener('abort', abort);
      if ('tracks' in outcome) resolve({ tracks: outcome.tracks, subscription });
      else {
        // An error ends the subscription; a catalog that does not parse leaves it open.
        if (!outcome.ended) subscription.cancel();
        reject(outcome.error);
      }
    };
    // Aborted before the catalog came: cancel the subscription and reject.
    const abort = () => {
      if (outcome) return;
      outcome = { error: signal?.reason, ended: false };
      settle();
    };
    signal?.addEventListener('abort', abort, { once: true });
    subscription = subscriber.subscribe(
      { namespace: [...namespace], track: CATALOG_TRACK },
      {
        onObject(object) {
          if (outcome || object.status !== 'normal') return;
          try {
            outcome = { tracks: parseMsfCatalog(object.payload, { catalogNamespace: namespace }).tracks };
          } catch (error) {
            outcome = { error, ended: false };
          }
          settle();
        },
        onError(error) {
          if (outcome) return;
          outcome = { error, ended: true };
          settle();
        },
      },
    );
    settle();
  });
}

/**
 * Play the audio track of the broadcast at `namespace`, such as the WebCodecs example's microphone, with no video:
 * the catalog gives the decoder config, a frame reader orders the frames, and the audio player buffers and plays
 * them. The playout clock is created for audio only, so it does not wait 2 s for keyframes that never come. When the
 * catalog lists the interrupt track, each interrupt for the audio track cuts the audio the player holds; when it lists
 * the transcript track, the listener keeps the audio track's transcript, which interrupts cut too.
 */
export async function listenAudio(
  subscriber: RelaySubscriber,
  namespace: readonly string[],
  options: ListenAudioOptions = {},
): Promise<AudioListener> {
  const ownsContext = !options.context;
  const context = options.context ?? new AudioContext({ sampleRate: 48_000, latencyHint: 'interactive' });
  // What setup has created so far, released in reverse order if a later step fails.
  const release: (() => void | Promise<void>)[] = [];
  if (ownsContext) release.push(() => context.close());
  try {
    const { tracks, subscription: catalog } = await readCatalog(subscriber, namespace, options.signal);
    release.push(() => catalog.cancel());
    const track = tracks.find((candidate) => candidate.name === AUDIO_TRACK);
    if (!track) throw new Error(`The catalog at ${namespace.join('/')} lists no ${AUDIO_TRACK} track`);

    const player = await createAudioPlayer({ context });
    release.push(() => player.close());
    options.signal?.throwIfAborted();
    let loopback: EchoCancellationLoopback | undefined;
    if (options.echoCancellation) {
      loopback = await createEchoCancellationLoopback(context, player.output);
      release.push(() => loopback?.close());
      options.signal?.throwIfAborted();
    } else player.output.connect(context.destination);

    const clock = createPlayoutClock({ targetLatencyUs: AUDIO_ONLY_TARGET_LATENCY_US });
    const input = player.addInput({
      clock,
      decoderConfig: audioDecoderConfigFromTrack(track),
      ...(options.onError ? { onError: options.onError } : {}),
    });
    const sequencer = createFrameSequencer({ clock, grouping: 'frame' });
    const reader = createFrameReader(sequencer, { onFrame: input.push });
    release.push(() => reader.close());
    const audio = subscriber.subscribe(
      { namespace: [...namespace], track: AUDIO_TRACK },
      { ...reader, onError: (error) => options.onError?.(error) },
    );
    release.push(() => audio.cancel());
    const listed = (name: string) => tracks.some((candidate) => candidate.name === name);
    const transcript = createTranscript();
    // A talker that can stop mid-utterance lists the interrupt track; each interrupt cuts what the player holds, and
    // the transcript of what was not heard.
    const interrupts = listed(INTERRUPT_TRACK)
      ? subscriber.subscribe(
          { namespace: [...namespace], track: INTERRUPT_TRACK },
          {
            onObject(object) {
              if (object.status !== 'normal') return;
              let interrupt: Interrupt;
              try {
                interrupt = parseInterrupt(object.payload);
              } catch (error) {
                options.onError?.(error);
                return;
              }
              if (interrupt.track !== AUDIO_TRACK) return;
              input.cut(interrupt);
              transcript.cut(interrupt, clock.position());
              options.onInterrupt?.(interrupt);
            },
            onError: (error) => options.onError?.(error),
          },
        )
      : undefined;
    if (interrupts) release.push(() => interrupts.cancel());
    const transcripts = listed(TRANSCRIPT_TRACK)
      ? subscriber.subscribe(
          { namespace: [...namespace], track: TRANSCRIPT_TRACK },
          {
            onObject(object) {
              if (object.status !== 'normal') return;
              let segment: TranscriptSegment;
              try {
                segment = parseTranscriptSegment(object.payload);
              } catch (error) {
                options.onError?.(error);
                return;
              }
              if (segment.track === AUDIO_TRACK && transcript.apply(segment)) options.onTranscript?.(segment);
            },
            onError: (error) => options.onError?.(error),
          },
        )
      : undefined;

    return {
      context,
      player,
      input,
      clock,
      sequencer,
      transcript,
      async stop() {
        reader.close();
        audio.cancel();
        interrupts?.cancel();
        transcripts?.cancel();
        catalog.cancel();
        loopback?.close();
        player.close();
        if (ownsContext) await context.close();
      },
    };
  } catch (error) {
    // A failure while cleaning up must not hide the one that stopped the setup.
    for (const undo of release.reverse()) {
      try {
        await undo();
      } catch {}
    }
    throw error;
  }
}
