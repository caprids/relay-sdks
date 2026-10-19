import { createBroadcastClock, type Interrupt, type TranscriptSegment } from '@mux/moq-media';
import type { RelayPublisher, RelaySubscriber } from '@mux/relay';
import { captureMicrophone } from '../webcodecs/capture-microphone.js';
import { type AudioListener, listenAudio } from '../webcodecs/listen-audio.js';
import { publishMicrophone } from '../webcodecs/publish-microphone.js';

/** Options for {@link talkToBot}. */
export interface TalkToBotOptions {
  /** Aborting hangs up. */
  signal: AbortSignal;
  /** The bot's namespace, where it publishes its catalog, its audio, and its interrupt and transcript tracks. */
  botNamespace: readonly string[];
  /** The namespace `publisher` was connected with, where the microphone goes. */
  userNamespace: readonly string[];
  /** The context to capture and play through: 48 kHz, created or resumed in a user gesture. */
  context: AudioContext;
  /** The microphone, from `getUserMedia` with echo cancellation on. */
  stream: MediaStream;
  /** The bot's audio is playing: its catalog arrived, and the listener subscribed. */
  onListening?(listener: AudioListener): void;
  /** The bot stopped talking because it was talked over; the player has already cut what it held. */
  onInterrupt?(interrupt: Interrupt): void;
  /** A transcript segment of the bot's audio arrived; `listener.transcript` has it. */
  onTranscript?(segment: TranscriptSegment): void;
  /** A failure that does not hang up: an attempt to reach the bot, or one of the listener's. */
  onError?(error: unknown): void;
}

/** How long one attempt to reach the bot waits for its catalog. */
const CATALOG_TIMEOUT_MS = 5000;

function wait(milliseconds: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    signal.throwIfAborted();
    const timer = setTimeout(() => {
      signal.removeEventListener('abort', abort);
      resolve();
    }, milliseconds);
    const abort = () => {
      clearTimeout(timer);
      reject(signal.reason);
    };
    signal.addEventListener('abort', abort, { once: true });
  });
}

/**
 * Talk to a voice bot such as python/examples/voice_bot: publish the microphone alone, and play the bot's audio
 * through the audio player, cutting it and its transcript on each interrupt. Playback goes through Chromium's echo
 * canceller, so the bot does not hear itself and take it for being talked over. The bot can start before or after the
 * page: until its catalog arrives, the listener retries every second, waiting at most 5 s for each attempt. Runs until
 * aborted, or until publishing fails.
 */
export async function talkToBot(
  publisher: RelayPublisher,
  subscriber: RelaySubscriber,
  options: TalkToBotOptions,
): Promise<void> {
  const { context } = options;
  // Hanging up, or publishing failing, stops both halves.
  const lifetime = new AbortController();
  const signal = AbortSignal.any([options.signal, lifetime.signal]);
  const microphone = await captureMicrophone(createBroadcastClock(), options.stream, { context });
  let listener: AudioListener | undefined;
  const publishing = publishMicrophone(publisher, options.userNamespace, { signal, microphone });
  const listening = (async () => {
    while (!listener) {
      try {
        listener = await listenAudio(subscriber, options.botNamespace, {
          context,
          echoCancellation: true,
          // A catalog subscription the relay accepts can still deliver nothing; give up on it and try again.
          signal: AbortSignal.any([signal, AbortSignal.timeout(CATALOG_TIMEOUT_MS)]),
          ...(options.onError ? { onError: options.onError } : {}),
          ...(options.onInterrupt ? { onInterrupt: options.onInterrupt } : {}),
          ...(options.onTranscript ? { onTranscript: options.onTranscript } : {}),
        });
      } catch (error) {
        // Not announced yet, or announced but not serving its catalog yet (the announce race).
        signal.throwIfAborted();
        options.onError?.(error);
        await wait(1000, signal);
      }
    }
    signal.throwIfAborted();
    options.onListening?.(listener);
  })();
  try {
    await Promise.all([publishing, listening]);
  } finally {
    lifetime.abort(new Error('Hung up'));
    await Promise.allSettled([publishing, listening]);
    await listener?.stop();
    microphone.close();
  }
}
