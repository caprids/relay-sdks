import {
  AUDIO_ONLY_TARGET_LATENCY_US,
  AUDIO_TRACK,
  type AudioPlayer,
  type AudioPlayerInput,
  audioDecoderConfigFromTrack,
  createAudioPlayer,
  createEchoCancellationLoopback,
  createFrameHold,
  createFrameSequencer,
  createPlayoutClock,
  createTranscript,
  type EchoCancellationLoopback,
  type FrameSequencer,
  findReplacementTrack,
  INTERRUPT_TRACK,
  type Interrupt,
  type PlayoutClock,
  parseInterrupt,
  parseTranscriptSegment,
  type SequencedFrame,
  TRANSCRIPT_TRACK,
  type Transcript,
  type TranscriptSegment,
  warmAudioPlayer,
} from '@caprid/moq-media';
import type { RestartReason, SupervisedSubscriber, WantHandlers } from '@caprid/relay';
import { createFrameReader, type FrameReader, mediaSubscribeParameters } from '@caprid/relay/media';
import { type LaterCatalogs, readCatalog } from './read-catalog.js';

/** Numbers each listener, so the want keys of two listeners on one supervised subscriber never collide. */
let listeners = 0;
/** How many of the latest interrupts a restart applies again. */
const MAX_RECENT_INTERRUPTS = 8;

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
   * The playout clock the audio reports to. Pass a video watcher's clock to play audio and video in sync; that clock's
   * target latency then bounds the jitter buffer, and audio up to `AUDIO_ONLY_TARGET_LATENCY_US` behind its position
   * still plays, since video may be pacing it. Default: a clock for audio only, with `AUDIO_ONLY_TARGET_LATENCY_US`.
   */
  clock?: PlayoutClock;
  /**
   * Play through a WebRTC loopback, so Chromium's echo canceller hears this audio when the page also captures a
   * microphone. Default `false`: connect straight to the context's destination.
   */
  echoCancellation?: boolean;
  /**
   * `namespace` is a supervised publisher's root, such as a voice bot's: follow its newest generation, so a talker
   * that restarts as a new generation is played again within a round trip of its announcement. Default `false`: play
   * `namespace` itself, resubscribing when its publisher restarts there.
   */
  generations?: boolean;
  /**
   * A failure after the listener started that its supervised subscriber does not retry (the audio, interrupt or
   * transcript want stopped), a decoder failed, or an interrupt or transcript object did not parse.
   */
  onError?(error: unknown): void;
  /**
   * The audio subscription was replaced after the relay had accepted it: the talker restarted (`'generation'`, a newer
   * generation took over; `'publisher-done'`, its publisher went away), the subscriber reconnected (`'connection'`),
   * or the relay ended it. The player has dropped the audio it held, and the next audio is the new subscription's,
   * live. `namespace` is the one the replaced subscription was on: the old generation's, when following one.
   */
  onRestarted?(info: { reason: RestartReason; namespace: string[] }): void;
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
   * Aborting while the listener starts withdraws what it set up and rejects with the reason. It waits for the
   * catalog as long as it takes, since the supervised subscriber retries until the talker is there. Once started,
   * stop it with `stop()`.
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
  /** Stop playing and withdraw the wants. A context the listener created is closed too. */
  stop(): Promise<void>;
}

/**
 * Resolve once `context` is running, as after a user gesture resumes it. Start `listenAudio` then: the player takes
 * every frame that arrives, and a suspended context plays none of them, so a listener started earlier holds all the
 * audio since it started and spends seconds trimming it once the context resumes, with the clock (and any video on
 * it) following that backlog. Rejects if the context closes first.
 */
export function whenRunning(context: BaseAudioContext): Promise<void> {
  return new Promise((resolve, reject) => {
    const check = () => {
      if (context.state !== 'running' && context.state !== 'closed') return;
      context.removeEventListener('statechange', check);
      if (context.state === 'running') resolve();
      else reject(new Error('The AudioContext closed before it ran'));
    };
    context.addEventListener('statechange', check);
    check();
  });
}

/**
 * Play the audio track of the broadcast at `namespace`, such as the WebCodecs example's microphone: the catalog gives
 * the decoder config, a frame reader orders the frames, and the audio player buffers and plays them. With no video,
 * the playout clock is created for audio only, so it does not wait 2 s for keyframes that never come; with
 * `watchVideo`, pass its clock as `clock`. When the catalog lists the interrupt track, each interrupt for the audio
 * track cuts the audio the player holds; when it lists the transcript track, the listener keeps the audio track's
 * transcript, which interrupts cut too. When a later catalog lists a new audio track in place of the one playing (a
 * talker's track replacement), the listener drops the old track's audio and plays the new one.
 *
 * Every track is a want of the supervised subscriber, so the listener outlives the talker. It waits for a talker that
 * has not announced yet. When the talker restarts, at `namespace` or (with `generations`) as a newer generation under
 * it, or the subscriber reconnects, the want subscribes again: the player drops what it held of the old subscription,
 * the new one plays live, and `onRestarted` says so.
 *
 * It joins quickly. It starts `warmAudioPlayer(context)` at once, so the worklet module and libopus load while the
 * catalog is on its way (create the context in the user's gesture, as the click that connects, so the warm happens
 * there; a context the page already warmed is not loaded again). It wants the audio track by its conventional name
 * together with the catalog, before the catalog arrives, and a frame hold keeps the frames until the player's input
 * exists. When the catalog names the track, it adds the input and passes the held frames to it, in order; a catalog
 * that names another audio track (the listener joined after a replacement) has that one wanted instead. The
 * interrupt track is wanted when the catalog lists it, before the player exists, and interrupts that come meanwhile
 * cut the held audio before it is released.
 */
export async function listenAudio(
  subscriber: SupervisedSubscriber,
  namespace: readonly string[],
  options: ListenAudioOptions = {},
): Promise<AudioListener> {
  const generations = options.generations ?? false;
  const id = `listen-audio/${++listeners}`;
  const ownsContext = !options.context;
  const context = options.context ?? new AudioContext({ sampleRate: 48_000, latencyHint: 'interactive' });
  // What setup has created so far, released in reverse order if a later step fails.
  const release: (() => void | Promise<void>)[] = [];
  if (ownsContext) release.push(() => context.close());
  // Load the worklet module and libopus while the catalog is on its way. A failure is not lost: createAudioPlayer
  // loads again and reports it.
  const warm = warmAudioPlayer(context).catch(() => {});
  // The setup's own signal: the caller's abort, or a failure of a later step that must end the catalog read.
  const setup = new AbortController();
  const abortSetup = () => setup.abort(options.signal?.reason);
  options.signal?.addEventListener('abort', abortSetup, { once: true });
  // Later catalogs, once the listener has started: a track replacement.
  const later: LaterCatalogs = {};
  // Once started, an audio want's failure goes to onError; before, it fails the setup (below).
  let started = false;
  // Whether the catalog's want is in `release` yet: a catalog that came before a later step failed is withdrawn here
  // otherwise.
  let catalogReleased = false;
  let reading: ReturnType<typeof readCatalog> | undefined;
  try {
    options.signal?.throwIfAborted();
    const clock = options.clock ?? createPlayoutClock({ targetLatencyUs: AUDIO_ONLY_TARGET_LATENCY_US });
    // A shared clock may be paced by video before any audio plays, and audio that arrives later than the video it goes
    // with would be dropped as late against that position, so it never plays and never takes the clock over. Keep
    // what the jitter buffer could still hold.
    const sequencer = createFrameSequencer({
      clock,
      grouping: 'frame',
      ...(options.clock ? { lateDropUs: AUDIO_ONLY_TARGET_LATENCY_US } : {}),
    });
    const hold = createFrameHold<SequencedFrame>();
    interface Audio {
      key: string;
      reader: FrameReader;
      /** The want stopped on a failure the subscriber does not retry: a rejected replacement, say. */
      failed: { error: unknown } | undefined;
    }
    // A restart or a move of the audio want, after the reader has restarted the sequencer. Before the player exists it
    // drops the held frames of the replaced subscription; once the input exists it cuts what the player holds.
    let restarted = (_reason: RestartReason, _from: string[]) => hold.clear();
    // Want one audio track, its frames to `onFrame`. A failure the subscriber does not retry is kept on the want:
    // before the listener has started it fails the setup once the catalog has named the track (the want by the
    // conventional name can fail so only because a replacement retired that track); after, it goes to onError, and
    // the next catalog that still lists the track wants it again. Each track has a key of its own, so a replacement is
    // a new want and the old track's is withdrawn at once.
    const audioKey = (name: string) => `${id}/audio/${name}`;
    const wantAudio = (name: string, onFrame: (frame: SequencedFrame) => void): Audio => {
      const reader = createFrameReader(sequencer, { onFrame });
      const wanted: Audio = { key: audioKey(name), reader, failed: undefined };
      const handlers: WantHandlers = {
        ...reader,
        // A talker restart or a reconnect: the reader restarts the sequencer (the clock with it), and its onSubscribed
        // takes the new subscription's track properties; the listener drops what it holds of the old subscription.
        onRestarted(info) {
          reader.onRestarted();
          restarted(info.reason, info.namespace);
        },
        // A move to a newer generation: the same, with the new subscription's track properties from the reader.
        onSwitched(info) {
          reader.onSwitched(info);
          restarted('generation', info.from.namespace);
        },
        onError(error) {
          wanted.failed ??= { error };
          if (started) options.onError?.(error);
        },
      };
      try {
        subscriber.want(
          wanted.key,
          { namespace, track: name, parameters: mediaSubscribeParameters('audio'), generations },
          handlers,
        );
      } catch (error) {
        reader.close();
        throw error;
      }
      return wanted;
    };
    const dropAudio = (dropped: Audio) => {
      dropped.reader.close();
      subscriber.unwant(dropped.key);
    };
    // Want the catalog and the audio track at once: the audio does not wait for the catalog's round trips.
    reading = readCatalog(subscriber, `${id}/catalog`, namespace, generations, setup.signal, later);
    // A failure below ends the read through the setup signal; the rejection is awaited later.
    reading.catch(() => {});
    let audio = wantAudio(AUDIO_TRACK, hold.push);
    release.push(() => dropAudio(audio));
    const { tracks, stop: stopCatalog } = await reading;
    release.push(stopCatalog);
    catalogReleased = true;
    // The audio track; one that joins after a replacement finds the replacement, the catalog's audio track.
    const track =
      tracks.find((candidate) => candidate.name === AUDIO_TRACK) ??
      tracks.find((candidate) => candidate.role === 'audio');
    if (!track) throw new Error(`The catalog at ${namespace.join('/')} lists no ${AUDIO_TRACK} track`);
    if (track.name !== AUDIO_TRACK) {
      // The track wanted by its conventional name was replaced before this listener joined: play the catalog's.
      dropAudio(audio);
      hold.clear();
      sequencer.restart();
      audio = wantAudio(track.name, hold.push);
    }
    const throwIfAudioFailed = () => {
      if (audio.failed) throw audio.failed.error;
    };
    throwIfAudioFailed();
    // The track playing, and a replacement in progress: its new audio is `started` at the first report after the cut
    // is heard, since a report the worklet posted before it applied the cut can still arrive after `cut()`; such a report
    // is not passed to the clock either.
    let current = track;
    let replacing: { from: string; to: string; stopped: boolean } | undefined;
    // From a restart's cut (a replacement, a want's restart or move, or a retried want) until the cut is heard: reports
    // describe audio from before the restart and are not passed to the clock the restart reset.
    let cutUnheard = false;
    const replaced = (phase: AudioTrackReplacedEvent['phase'], atUs: number) => {
      if (!replacing) return;
      // The application's callback must not leave a replacement half done.
      try {
        options.onTrackReplaced?.({ from: replacing.from, to: replacing.to, phase, atUs });
      } catch (error) {
        console.error(new Error('onTrackReplaced threw; the listener carries on', { cause: error }));
      }
    };
    const listed = (name: string) => tracks.some((candidate) => candidate.name === name);
    // A track replacement starts a new transcript, and so does a restarted talker: its sequence numbers and cuts
    // belong to one run of one audio track.
    let transcript = createTranscript();
    // A talker that can stop mid-utterance lists the interrupt track; each interrupt cuts what the player holds, and
    // the transcript of what was not heard. It is wanted as soon as the catalog lists it, before the player exists:
    // audio is already arriving into the hold, and an interrupt sent meanwhile must cut it. Interrupts that come before
    // the input exists wait, and are applied before the held audio is released. One sent before this subscription is
    // handed over as the track's latest object (a relay cache, on every restart too), and drops only older audio.
    let cutTarget: AudioPlayerInput | undefined;
    const waitingInterrupts: Interrupt[] = [];
    // The latest interrupts applied. The audio, interrupt and transcript wants restart and move each on its own, so a
    // restarted talker's interrupt can come before its first audio frame, whose restart mark makes the player forget
    // the cuts it holds for arriving audio, or before the transcript want moves and starts a new transcript. Both take
    // these again then. Interrupts share the broadcast timeline across restarts, so an older one cuts nothing new.
    const recentInterrupts: Interrupt[] = [];
    const applyInterrupt = (interrupt: Interrupt) => {
      if (!cutTarget) {
        waitingInterrupts.push(interrupt);
        return;
      }
      cutTarget.cut(interrupt);
      transcript.cut(interrupt, clock.position());
      recentInterrupts.push(interrupt);
      if (recentInterrupts.length > MAX_RECENT_INTERRUPTS) recentInterrupts.shift();
      options.onInterrupt?.(interrupt);
    };
    const recentFor = () => recentInterrupts.filter((interrupt) => interrupt.track === current.name);
    /** A new transcript, cut where the latest interrupts for the playing track cut the last one. */
    const newTranscript = () => {
      const fresh = createTranscript();
      for (const interrupt of recentFor()) fresh.cut(interrupt, clock.position());
      return fresh;
    };
    const interruptKey = `${id}/interrupt`;
    const interrupts = listed(INTERRUPT_TRACK);
    if (interrupts) {
      subscriber.want(
        interruptKey,
        { namespace, track: INTERRUPT_TRACK, generations },
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
            applyInterrupt(interrupt);
          },
          onError: (error) => options.onError?.(error),
        },
      );
      release.push(() => subscriber.unwant(interruptKey));
    }

    await warm;
    const player = await createAudioPlayer({ context });
    release.push(() => player.close());
    options.signal?.throwIfAborted();
    throwIfAudioFailed();
    let loopback: EchoCancellationLoopback | undefined;
    if (options.echoCancellation) {
      loopback = await createEchoCancellationLoopback(context, player.output);
      release.push(() => loopback?.close());
      options.signal?.throwIfAborted();
      throwIfAudioFailed();
    } else player.output.connect(context.destination);

    const input = player.addInput({
      clock: {
        get targetLatencyUs() {
          return clock.targetLatencyUs;
        },
        localNow: () => clock.localNow(),
        reportAudio(timestampUs, atUs) {
          // Until the cut is heard, a report describes the audio from before the restart, posted before the cut reached
          // the jitter buffer: it must not re-anchor the clock the sequencer's restart reset for the new subscription.
          if (cutUnheard) return;
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
        cutUnheard = false;
        if (!replacing || replacing.stopped) return;
        replacing.stopped = true;
        replaced('stopped', atUs);
      },
    });
    cutTarget = input;
    // Every frame goes to the input through here: the first of a new subscription carries a restart mark, which drops
    // the input's cuts for arriving audio, so the latest interrupts cut again.
    const pushAudio = (frame: SequencedFrame) => {
      input.push(frame);
      if (frame.discontinuity === 'restart') for (const interrupt of recentFor()) input.cut(interrupt);
    };
    // Interrupts that came while the player was built cut first, then what arrived meanwhile plays, in order, then
    // every frame as it comes.
    for (const interrupt of waitingInterrupts.splice(0)) applyInterrupt(interrupt);
    hold.release(pushAudio);
    // Each run of the talker numbers its segments anew: a transcript want that moves to another generation, or whose
    // publisher went away, starts a new transcript.
    const transcriptKey = `${id}/transcript`;
    const transcripts = listed(TRANSCRIPT_TRACK);
    let transcriptAt: string | undefined;
    const transcriptOn = (at: readonly string[]) => {
      const key = JSON.stringify(at);
      if (transcriptAt !== undefined && transcriptAt !== key) transcript = newTranscript();
      transcriptAt = key;
    };
    if (transcripts) {
      subscriber.want(
        transcriptKey,
        { namespace, track: TRANSCRIPT_TRACK, generations },
        {
          onSubscribed: (_, context) => transcriptOn(context.namespace),
          onSwitched: (info) => transcriptOn(info.to.namespace),
          onRestarted(info) {
            if (info.reason === 'publisher-done') transcript = newTranscript();
          },
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
      );
      release.push(() => subscriber.unwant(transcriptKey));
    }
    throwIfAudioFailed();

    // A track replacement: withdraw the old track's want and drop what the sequencer and the player hold of it, then
    // play the new track from its first frame, on a clock the sequencer's restart has reset. The new track is wanted
    // first: if that throws, nothing changes, the old track plays on, and the next catalog that lists the replacement
    // tries again. A want that stops later, through its `onError`, leaves the new track current and silent: the next
    // catalog that still lists it wants it again, the same steps without a replacement. A restarted talker's catalog
    // comes from its new generation's namespace: the playing track is looked for there, and when that catalog lists
    // another audio track instead, as a talker that restarts after a replacement does, that one plays.
    let stopped = false;
    // A restart or a move of the audio want from now on: the reader has restarted the sequencer, so drop what the player
    // holds of the old subscription, and tell the application.
    restarted = (reason, from) => {
      if (stopped) return;
      input.cut();
      cutUnheard = true;
      try {
        options.onRestarted?.({ reason, namespace: from });
      } catch (error) {
        console.error(new Error('onRestarted threw; the listener carries on', { cause: error }));
      }
    };
    later.onError = (error) => {
      if (!stopped) options.onError?.(error);
    };
    later.onCatalog = (tracks, at) => {
      if (stopped) return;
      const replacement = findReplacementTrack({ ...current, namespace: at }, tracks);
      const next =
        replacement ?? (audio.failed ? tracks.find((candidate) => candidate.name === current.name) : undefined);
      if (!next) return;
      // Wanting again a track whose want stopped reuses its key: the old reader goes first, and the supervisor has
      // forgotten that want already.
      const again = audio.key === audioKey(next.name);
      if (again) dropAudio(audio);
      let wanted: Audio;
      try {
        wanted = wantAudio(next.name, pushAudio);
      } catch (error) {
        options.onError?.(error);
        return;
      }
      // Objects arrive asynchronously, so none of the new track's reaches the sequencer before its restart.
      if (!again) dropAudio(audio);
      sequencer.restart();
      input.cut();
      cutUnheard = true;
      audio = wanted;
      if (!replacement) return;
      replacing = { from: current.name, to: next.name, stopped: false };
      current = next;
      transcript = createTranscript();
      replaced('seen', clock.localNow());
    };
    started = true;
    // A replacement and a catalog error that came while the listener was setting up.
    const { pending, pendingError } = later;
    delete later.pending;
    delete later.pendingError;
    if (pendingError) later.onError(pendingError.error);
    if (pending) later.onCatalog(pending.tracks, pending.namespace);

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
        dropAudio(audio);
        if (interrupts) subscriber.unwant(interruptKey);
        if (transcripts) subscriber.unwant(transcriptKey);
        stopCatalog();
        loopback?.close();
        player.close();
        if (ownsContext) await context.close();
      },
    };
  } catch (error) {
    // A failure before the catalog arrived ends its read too: it withdraws its own want. One that came already, before
    // the step that failed could take it, is withdrawn here.
    setup.abort(error);
    if (!catalogReleased)
      reading?.then(
        ({ stop }) => stop(),
        () => {},
      );
    // A failure while cleaning up must not hide the one that stopped the setup.
    for (const undo of release.reverse()) {
      try {
        await undo();
      } catch {}
    }
    throw error;
  } finally {
    // Once started, the listener stops with stop(), not with the signal.
    options.signal?.removeEventListener('abort', abortSetup);
  }
}
