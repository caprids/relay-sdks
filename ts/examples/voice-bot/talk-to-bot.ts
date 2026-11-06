import { createBroadcastClock, type Interrupt, type TranscriptSegment } from '@caprid/moq-media';
import type { RelayPublisher, RelaySubscriber } from '@caprid/relay';
import { captureMicrophone } from '../webcodecs/capture-microphone.js';
import { type AudioListener, listenAudio } from '../webcodecs/listen-audio.js';
import { publishMicrophone } from '../webcodecs/publish-microphone.js';

/** Options for {@link talkToBot}. */
export interface TalkToBotOptions {
  /** Aborting hangs up. */
  signal: AbortSignal;
  /**
   * The bot's root namespace. Its supervised publisher serves each connection one generation field below it, with the
   * catalog, the audio, and the interrupt and transcript tracks; the newest generation is the bot.
   */
  botNamespace: readonly string[];
  /** The namespace `publisher` was connected with, where the microphone goes. */
  userNamespace: readonly string[];
  /** The context to capture and play through: 48 kHz, created or resumed in a user gesture. */
  context: AudioContext;
  /** The microphone, from `getUserMedia` with echo cancellation on. */
  stream: MediaStream;
  /**
   * The bot's audio is playing: its catalog arrived, and the listener subscribed. Called again with a new listener each
   * time the bot reconnects as a new generation; the previous one has stopped by then.
   */
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
 * page. Namespace discovery under `botNamespace` finds the bot's newest generation, and the page listens there; when
 * the bot reconnects as a newer generation, the page stops listening to the old one and listens to the new one. Until
 * a generation's catalog arrives, the listener retries every second, waiting at most 5 s for each attempt. Runs until
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
  const root = [...options.botNamespace];
  // The generations announced one field below the root, and a wake for the loop that follows the newest.
  const generations = new Set<string>();
  let wake: (() => void) | undefined;
  const changes = () =>
    new Promise<void>((resolve, reject) => {
      signal.throwIfAborted();
      const abort = () => reject(signal.reason);
      wake = () => {
        signal.removeEventListener('abort', abort);
        resolve();
      };
      signal.addEventListener('abort', abort, { once: true });
    });
  const discovering = (async () => {
    while (!signal.aborted) {
      generations.clear();
      try {
        for await (const event of subscriber.namespaceEvents({ prefix: root, signal })) {
          // Only one field below the root is a generation; anything deeper is someone else's.
          if (event.namespace.length !== root.length + 1) continue;
          const generation = event.namespace[root.length] as string;
          if (event.type === 'announced') generations.add(generation);
          else generations.delete(generation);
          wake?.();
        }
      } catch (error) {
        if (signal.aborted) return;
        options.onError?.(error);
      }
      // The relay ended the discovery: ask again.
      await wait(1000, signal).catch(() => {});
    }
  })();
  const listening = (async () => {
    let playing: string | undefined;
    for (;;) {
      // Generation fields sort by start time, so the greatest is the bot's newest connection.
      const newest = [...generations].sort().at(-1);
      if (newest === undefined || newest === playing) {
        await changes();
        continue;
      }
      // The bot started or reconnected. The old generation sends nothing more, so stop listening to it.
      const previous = listener;
      listener = undefined;
      playing = undefined;
      await previous?.stop();
      try {
        listener = await listenAudio(subscriber, [...root, newest], {
          context,
          echoCancellation: true,
          // A catalog subscription the relay accepts can still deliver nothing; give up on it and try again.
          signal: AbortSignal.any([signal, AbortSignal.timeout(CATALOG_TIMEOUT_MS)]),
          ...(options.onError ? { onError: options.onError } : {}),
          ...(options.onInterrupt ? { onInterrupt: options.onInterrupt } : {}),
          ...(options.onTranscript ? { onTranscript: options.onTranscript } : {}),
        });
        playing = newest;
        signal.throwIfAborted();
        options.onListening?.(listener);
      } catch (error) {
        // Announced but not serving its catalog yet.
        signal.throwIfAborted();
        options.onError?.(error);
        await wait(1000, signal);
      }
    }
  })();
  try {
    await Promise.all([publishing, listening]);
  } finally {
    lifetime.abort(new Error('Hung up'));
    await Promise.allSettled([publishing, listening, discovering]);
    await listener?.stop();
    microphone.close();
  }
}
