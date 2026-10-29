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
  findReplacementTrack,
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
} from '@caprid/moq-media';
import type { RelaySubscriber, RelaySubscription } from '@caprid/relay';
import { createFrameReader, type FrameReader } from '@caprid/relay/media';

/**
 * One step of an audio track replacement (a talker talked over): `seen`, the catalog listing the new track arrived
 * and the old track's audio was cut; `stopped`, the cut is heard (the old audio has stopped at the speaker); `started`,
 * the new track's first audio is heard. `atUs` is on the playout clock's time source, `performance.now()` µs.
 */
export interface AudioTrackReplacedEvent {
  /** The track replaced. */
  from: string;
  /** The track now played. */
  to: string;
  phase: 'seen' | 'stopped' | 'started';
  atUs: number;
}

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
   * A step of a track replacement: the catalog listed a new audio track in place of the one playing
   * (`findReplacementTrack`), so the listener cut the old track's audio, held and still arriving, and plays the new one
   * from its first frame. The new track must have the old one's codec.
   */
  onTrackReplaced?(event: AudioTrackReplacedEvent): void;
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
  /** The audio track playing now: `audio`, or the track that last replaced it. */
  readonly track: string;
  /** Stop playing and cancel the subscriptions. A context the listener created is closed too. */
  stop(): Promise<void>;
}

/**
 * Catalogs after the first: the listener's handlers once it has started, and the newest one that came before. An error
 * of the catalog subscription after the first catalog, which the listener depends on for replacements, goes to
 * `onError`; before the listener has started it is kept, the first one only, in `pendingError` and reported once
 * the handlers exist, as `pending` is kept.
 */
interface LaterCatalogs {
  onCatalog?(tracks: MsfCatalogTrack[]): void;
  onError?(error: unknown): void;
  pending?: MsfCatalogTrack[];
  pendingError?: { error: unknown };
}

function readCatalog(
  subscriber: RelaySubscriber,
  namespace: readonly string[],
  signal: AbortSignal | undefined,
  later: LaterCatalogs,
) {
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
          if (object.status !== 'normal') return;
          if (outcome) {
            // A later catalog: for the listener once it has started, and kept, the newest only, until then.
            if (!('tracks' in outcome)) return;
            let tracks: MsfCatalogTrack[];
            try {
              tracks = parseMsfCatalog(object.payload, { catalogNamespace: namespace }).tracks;
            } catch (error) {
              later.onError?.(error);
              return;
            }
            if (later.onCatalog) later.onCatalog(tracks);
            else later.pending = tracks;
            return;
          }
          try {
            outcome = { tracks: parseMsfCatalog(object.payload, { catalogNamespace: namespace }).tracks };
          } catch (error) {
            outcome = { error, ended: false };
          }
          settle();
        },
        onError(error) {
          if (outcome) {
            // The catalog subscription ended after its first catalog: no replacement can follow, which is reported.
            if ('tracks' in outcome) {
              if (later.onError) later.onError(error);
              else later.pendingError ??= { error };
            }
            return;
          }
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
 * the transcript track, the listener keeps the audio track's transcript, which interrupts cut too. When a later catalog
 * lists a new audio track in place of the one playing (a talker's track replacement), the listener drops the old
 * track's audio and plays the new one.
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
  // Later catalogs, once the listener has started: a track replacement.
  const later: LaterCatalogs = {};
  try {
    const { tracks, subscription: catalog } = await readCatalog(subscriber, namespace, options.signal, later);
    release.push(() => catalog.cancel());
    // The audio track; one that joins after a replacement finds the replacement, the catalog's audio track.
    const track =
      tracks.find((candidate) => candidate.name === AUDIO_TRACK) ??
      tracks.find((candidate) => candidate.role === 'audio');
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
    // The track playing, and a replacement in progress: its new audio is `started` at the first report after the cut
    // is heard, since a report the worklet posted before it applied the cut can still arrive after `cut()`; such a report
    // is not passed to the clock either.
    let current = track;
    let replacing: { from: string; to: string; stopped: boolean } | undefined;
    const replaced = (phase: AudioTrackReplacedEvent['phase'], atUs: number) => {
      if (!replacing) return;
      // The application's callback must not leave a replacement half done.
      try {
        options.onTrackReplaced?.({ from: replacing.from, to: replacing.to, phase, atUs });
      } catch (error) {
        console.error(new Error('onTrackReplaced threw; the listener carries on', { cause: error }));
      }
    };
    const input = player.addInput({
      clock: {
        get targetLatencyUs() {
          return clock.targetLatencyUs;
        },
        localNow: () => clock.localNow(),
        reportAudio(timestampUs, atUs) {
          // Until the cut is heard, a report describes the replaced track's audio, posted before the cut reached the
          // jitter buffer: it must not re-anchor the clock the sequencer's restart reset for the new track.
          if (replacing && !replacing.stopped) return;
          clock.reportAudio(timestampUs, atUs);
          if (replacing?.stopped) {
            replaced('started', atUs);
            replacing = undefined;
          }
        },
        stopAudio: () => clock.stopAudio(),
      },
      decoderConfig: audioDecoderConfigFromTrack(track),
      ...(options.onError ? { onError: options.onError } : {}),
      onCutHeard(atUs) {
        if (!replacing || replacing.stopped) return;
        replacing.stopped = true;
        replaced('stopped', atUs);
      },
    });
    const sequencer = createFrameSequencer({ clock, grouping: 'frame' });
    interface Audio {
      reader: FrameReader;
      subscription: RelaySubscription;
      /** The relay ended this subscription with an error, a rejected replacement for one. */
      failed: boolean;
    }
    const subscribeAudio = (name: string): Audio => {
      const reader = createFrameReader(sequencer, { onFrame: input.push });
      // The error belongs to this subscription: one of an old, replaced subscription does not mark the current one.
      const subscribed: { reader: FrameReader; subscription?: RelaySubscription; failed: boolean } = {
        reader,
        failed: false,
      };
      subscribed.subscription = subscriber.subscribe(
        { namespace: [...namespace], track: name },
        {
          ...reader,
          onError: (error) => {
            subscribed.failed = true;
            options.onError?.(error);
          },
        },
      );
      return subscribed as Audio;
    };
    let audio: Audio = subscribeAudio(track.name);
    release.push(() => audio.reader.close());
    release.push(() => audio.subscription.cancel());
    const listed = (name: string) => tracks.some((candidate) => candidate.name === name);
    // A track replacement starts a new transcript: its sequence numbers and cuts belong to one run of one audio track.
    let transcript = createTranscript();
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
              if (interrupt.track !== current.name) return;
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
              if (segment.track === current.name && transcript.apply(segment)) options.onTranscript?.(segment);
            },
            onError: (error) => options.onError?.(error),
          },
        )
      : undefined;
    if (transcripts) release.push(() => transcripts.cancel());

    // A track replacement: stop the old track's subscription and drop what the sequencer and the player hold of it, then
    // play the new track from its first frame, on a clock the sequencer's restart has reset. The new track is subscribed
    // first: if that fails, nothing changes, the old track plays on, and the next catalog that lists the replacement
    // tries again. A relay that rejects the subscription later, through its `onError`, leaves the new track current
    // and silent: the next catalog that still lists it subscribes to it again, the same steps without a replacement.
    let stopped = false;
    later.onError = (error) => {
      if (!stopped) options.onError?.(error);
    };
    later.onCatalog = (tracks) => {
      if (stopped) return;
      const replacement = findReplacementTrack(current, tracks);
      const next =
        replacement ?? (audio.failed ? tracks.find((candidate) => candidate.name === current.name) : undefined);
      if (!next) return;
      let subscribed: Audio;
      try {
        subscribed = subscribeAudio(next.name);
      } catch (error) {
        options.onError?.(error);
        return;
      }
      // Objects arrive asynchronously, so none of the new track's reaches the sequencer before its restart.
      audio.reader.close();
      audio.subscription.cancel();
      sequencer.restart();
      input.cut();
      audio = subscribed;
      if (!replacement) return;
      replacing = { from: current.name, to: next.name, stopped: false };
      current = next;
      transcript = createTranscript();
      replaced('seen', clock.localNow());
    };
    // A replacement and a catalog error that came while the listener was setting up.
    const { pending, pendingError } = later;
    delete later.pending;
    delete later.pendingError;
    if (pendingError) later.onError(pendingError.error);
    if (pending) later.onCatalog(pending);

    return {
      context,
      player,
      input,
      clock,
      sequencer,
      get transcript() {
        return transcript;
      },
      get track() {
        return current.name;
      },
      async stop() {
        stopped = true;
        audio.reader.close();
        audio.subscription.cancel();
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
