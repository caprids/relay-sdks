import { createBroadcastClock, type Interrupt, type TranscriptSegment } from '@caprid/moq-media';
import type { RelayPublisher, RestartReason, SupervisedSubscriber } from '@caprid/relay';
import { captureMicrophone } from '../webcodecs/capture-microphone.js';
import { type AudioListener, listenAudio } from '../webcodecs/listen-audio.js';
import { publishMicrophone } from '../webcodecs/publish-microphone.js';

/** Options for {@link talkToBot}. */
export interface TalkToBotOptions {
  /** Aborting hangs up. */
  signal: AbortSignal;
  /**
   * The bot's root namespace. Its supervised publisher serves each connection one generation field below it, with the
   * catalog, the audio, and the interrupt and transcript tracks; the newest generation is the bot, and the
   * subscriber's wants follow it.
   */
  botNamespace: readonly string[];
  /** The namespace `publisher` was connected with, where the microphone goes. */
  userNamespace: readonly string[];
  /** The context to capture and play through: 48 kHz, created or resumed in a user gesture. */
  context: AudioContext;
  /** The microphone, from `getUserMedia` with echo cancellation on. */
  stream: MediaStream;
  /**
   * The bot's audio is playing: its catalog arrived, and the listener started. Called once: the listener follows the
   * bot's newest generation, and `onRestarted` reports each time the bot comes back.
   */
  onListening?(listener: AudioListener): void;
  /**
   * The listener's audio subscription was replaced: the bot reconnected as a newer generation (`'generation'`), or the
   * subscriber reconnected (`'connection'`). The player dropped what it held of the old one, and the new one plays
   * live. `namespace` is the replaced subscription's, the old generation's.
   */
  onRestarted?(info: { reason: RestartReason; namespace: string[] }): void;
  /** The bot stopped talking because it was talked over; the player has already cut what it held. */
  onInterrupt?(interrupt: Interrupt): void;
  /** A transcript segment of the bot's audio arrived; `listener.transcript` has it. */
  onTranscript?(segment: TranscriptSegment): void;
  /** A failure that does not hang up: a failed start of the listener, or one of the listener's. */
  onError?(error: unknown): void;
}

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
 * page. The listener's wants follow the bot's newest generation under `botNamespace` (`generations: true`), so when the
 * bot reconnects as a newer generation, the same listener moves to it and `onRestarted` says so. A start that fails
 * (a catalog with no audio track, say) is reported and tried again a second later. Runs until aborted, or until
 * publishing fails.
 */
export async function talkToBot(
  publisher: RelayPublisher,
  subscriber: SupervisedSubscriber,
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
    for (;;) {
      try {
        listener = await listenAudio(subscriber, options.botNamespace, {
          context,
          echoCancellation: true,
          generations: true,
          signal,
          ...(options.onError ? { onError: options.onError } : {}),
          ...(options.onInterrupt ? { onInterrupt: options.onInterrupt } : {}),
          ...(options.onTranscript ? { onTranscript: options.onTranscript } : {}),
          ...(options.onRestarted ? { onRestarted: options.onRestarted } : {}),
        });
        break;
      } catch (error) {
        signal.throwIfAborted();
        options.onError?.(error);
        await wait(1000, signal);
      }
    }
    signal.throwIfAborted();
    options.onListening?.(listener);
    // The listener carries on by itself until hanging up.
    await new Promise((_, reject) => {
      signal.throwIfAborted();
      signal.addEventListener('abort', () => reject(signal.reason), { once: true });
    });
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
