/**
 * The live watchVideo scenario, run inside one browser page: the WebCodecs example publishes its test pattern and a
 * synthetic microphone, and the example's watcher plays them back as a page would, `watchVideo` on a canvas and
 * `listenAudio` on one shared playout clock. Every 600 ms the pattern flashes white and the microphone beeps at the
 * same moment. The watcher's `AudioContext` starts suspended, as one created without a user gesture does; the
 * scenario then resumes it, and then mutes the audio with the input's gain.
 *
 * A flash is timed when watchVideo draws its first white frame (read back from the canvas). A beep is timed at the
 * audio player's output: a worklet tap finds its onset, and the render clock plus `baseLatency + outputLatency` give
 * when it is audible, as the player's own clock reports count it.
 */
import { createBroadcastClock, createPlayoutClock, createRenderClock } from '@caprid/moq-media';
import { connectRelayPublisher, connectSupervisedSubscriber, type SupervisedSubscriber } from '@caprid/relay';
import { captureMicrophone, type MicrophoneBlock } from '../../examples/webcodecs/capture-microphone.js';
import { type AudioListener, listenAudio, whenRunning } from '../../examples/webcodecs/listen-audio.js';
import { publishCanvas } from '../../examples/webcodecs/publish-canvas.js';
import { startTestPattern } from '../../examples/webcodecs/test-pattern.js';
import { type VideoWatcher, watchVideo } from '../../examples/webcodecs/watch-video.js';
import {
  createOnsetDetector,
  type EmittedEvent,
  type HeardBeep,
  type MatchedEvent,
  matchEvents,
  type OffsetSummary,
  type ShownFlash,
  summarize,
} from '../av-sync/measure.js';
import { syntheticMicrophone } from '../av-sync/scenario.js';

/** Options for {@link runWatchVideoScenario}. */
export interface ScenarioOptions {
  endpoint: string;
  token: string | undefined;
  namespace: string[];
  /** Seconds per phase. Default 10. */
  phaseSeconds?: number;
  /** Progress lines for the launcher. */
  report(value: object): unknown;
}

/** One phase's result. */
export interface PhaseResult {
  phase: string;
  contextState: AudioContextState;
  /** The playout clock's mode, sampled every 20 ms. */
  modes: Record<string, number>;
  /** The watcher's counters over the phase. */
  shown: number;
  late: number;
  overflowed: number;
  discontinuities: number;
  /** The longest pause between two drawn frames, in ms. */
  longestGapMs: number;
  /** The audio input's counters over the phase, and its buffer at the end. */
  audio: { decoded: number; late: number; underruns: number; trimmedMs: number; bufferedMs: number };
  /** A/V offsets net of the synthetic microphone's path, 2 s clear of every switch, in ms: positive is audio late. */
  sync: OffsetSummary | undefined;
  /** The same for the events in the first 2 s after the phase began. */
  transient: number[];
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const BRIGHT = 160;

/** The worklet that hands the player's output to the main thread, about 21 ms at a time. */
const TAP_SOURCE = `
registerProcessor('output-tap', class extends AudioWorkletProcessor {
  constructor() { super(); this.samples = new Float32Array(1024); this.filled = 0; this.first = 0; }
  process(inputs) {
    const channel = inputs[0] && inputs[0][0];
    if (!channel) return true;
    if (this.filled === 0) this.first = currentFrame;
    this.samples.set(channel, this.filled);
    this.filled += channel.length;
    if (this.filled === this.samples.length) {
      this.port.postMessage({ first: this.first, samples: this.samples });
      this.samples = new Float32Array(1024);
      this.filled = 0;
    }
    return true;
  }
});`;

/** How long a start may wait for the publisher: the supervised subscriber's wants retry the announce race themselves. */
const startDeadline = () => AbortSignal.timeout(15_000);

/** Run the scenario; resolves with the measurements, rejects on the first failure. */
export async function runWatchVideoScenario(options: ScenarioOptions) {
  const phaseMs = (options.phaseSeconds ?? 10) * 1000;
  const failures: unknown[] = [];
  const fail = (error: unknown) => failures.push(error);
  const check = () => {
    if (failures.length > 0) throw failures[0];
  };
  const stop = new AbortController();
  const closers: (() => unknown)[] = [];
  let subscriber: SupervisedSubscriber | undefined;
  try {
    const getToken = () => options.token;
    const publisher = await connectRelayPublisher({ url: options.endpoint, namespace: options.namespace, getToken });
    closers.push(() => publisher.close());
    subscriber = await connectSupervisedSubscriber({ url: options.endpoint, getToken });

    // Publish: the test pattern and a synthetic microphone on one broadcast clock.
    const broadcast = createBroadcastClock();
    const beeper = await syntheticMicrophone(48_000);
    closers.push(() => beeper.close());
    const captureContext = new AudioContext({ sampleRate: 48_000 });
    await captureContext.resume();
    closers.push(() => captureContext.close());
    const microphone = await captureMicrophone(broadcast, beeper.stream, {
      context: captureContext,
      captureLatencyUs: () => 0,
    });
    closers.push(() => microphone.close());
    const source = new OffscreenCanvas(640, 360);
    const pattern = startTestPattern(source);
    closers.push(() => pattern.stop());
    const publishing = publishCanvas(source, publisher, options.namespace, { signal: stop.signal, microphone });
    publishing.catch((error) => stop.signal.aborted || fail(error));
    // When each beep reached the capture input: the path a real microphone's input latency stands for, which neither
    // the publisher nor the watcher can see, so it is taken out of the offsets (as the A/V sync test does).
    const arrivals: number[] = [];
    const captureRenders = new WeakMap<object, ReturnType<typeof createRenderClock>>();
    const captureOnsets = createOnsetDetector();
    while (!microphone.onblock) {
      check();
      await sleep(10);
    }
    const encode = microphone.onblock;
    microphone.onblock = (block: MicrophoneBlock) => {
      let renders = captureRenders.get(block.timeline);
      if (!renders) {
        renders = createRenderClock();
        captureRenders.set(block.timeline, renders);
      }
      renders.observe(block.contextUs + (block.samples.length * 1e6) / block.sampleRate, performance.now() * 1000);
      for (const onsetUs of captureOnsets.push(block.samples, block.sampleRate, block.contextUs)) {
        const atUs = renders.renderedAtUs(onsetUs);
        if (atUs !== undefined) arrivals.push(atUs);
      }
      encode(block);
    };
    await publisher.announced;
    await options.report({ phase: 'publishing' });

    // Watch, as the example page does without a gesture: one clock, and a context that starts suspended.
    const clock = createPlayoutClock();
    const context = new AudioContext({ sampleRate: 48_000, latencyHint: 'interactive' });
    await context.suspend();
    closers.push(() => context.close());
    const canvas = document.createElement('canvas');
    document.body.append(canvas);
    const probe = new OffscreenCanvas(8, 8).getContext('2d', { willReadFrequently: true });
    if (!probe) throw new Error('No 2D context for the brightness probe');
    const flashes: ShownFlash[] = [];
    let wasBright = false;
    let lastShownUs: number | undefined;
    let longestGapUs = 0;
    const onShown = (timestampUs: number) => {
      const shownAtUs = performance.now() * 1000;
      if (lastShownUs !== undefined) longestGapUs = Math.max(longestGapUs, shownAtUs - lastShownUs);
      lastShownUs = shownAtUs;
      probe.drawImage(canvas, 0, 0, 8, 8);
      const pixels = probe.getImageData(0, 0, 8, 8).data;
      let sum = 0;
      for (let index = 0; index < pixels.length; index += 4) sum += pixels[index + 1] as number;
      const bright = sum / (pixels.length / 4) > BRIGHT;
      if (bright && !wasBright) flashes.push({ timestampUs, shownAtUs });
      wasBright = bright;
    };
    const watcher = await watchVideo(subscriber, options.namespace, canvas, {
      clock,
      onShown,
      onError: fail,
      signal: startDeadline(),
    });
    closers.push(() => watcher.stop());
    // As the example page does: audio starts once the context runs.
    let listener: AudioListener | undefined;
    const listening = whenRunning(context).then(() =>
      listenAudio(subscriber as SupervisedSubscriber, options.namespace, {
        context,
        clock,
        onError: fail,
        signal: startDeadline(),
      }),
    );
    listening.then((started) => {
      listener = started;
      closers.push(() => started.stop());
    }, fail);
    await options.report({ phase: 'watching', contextState: context.state });

    // Beeps at the player's output.
    const beeps: HeardBeep[] = [];
    const tapUrl = URL.createObjectURL(new Blob([TAP_SOURCE], { type: 'text/javascript' }));
    await context.audioWorklet.addModule(tapUrl);
    const tap = new AudioWorkletNode(context, 'output-tap');
    tap.connect(new GainNode(context, { gain: 0 })).connect(context.destination);
    const outputRenders = createRenderClock();
    const outputOnsets = createOnsetDetector();
    tap.port.onmessage = ({ data }: MessageEvent<{ first: number; samples: Float32Array }>) => {
      const nowUs = performance.now() * 1000;
      const startUs = (data.first / context.sampleRate) * 1e6;
      outputRenders.observe(startUs + (data.samples.length / context.sampleRate) * 1e6, nowUs);
      for (const onsetUs of outputOnsets.push(data.samples, context.sampleRate, startUs)) {
        const renderedAtUs = outputRenders.renderedAtUs(onsetUs) as number;
        const audibleAtUs = renderedAtUs + (context.baseLatency + (context.outputLatency || 0)) * 1e6;
        // The tap cannot see the beep's timestamp; the clock's position when it is audible is near enough to pair it
        // with its flash (within the matching window), and plays no part in the offset.
        const position = clock.position();
        if (position === undefined) continue;
        beeps.push({ timestampUs: Math.round(position + (audibleAtUs - nowUs)), audibleAtUs });
      }
    };

    const emitted: EmittedEvent[] = [];
    let emitting = true;
    const emitter = (async () => {
      while (emitting) {
        const atMs = performance.now() + 100;
        beeper.beep(atMs * 1000);
        await sleep(atMs - performance.now());
        pattern.flash();
        emitted.push({ beepAtUs: atMs * 1000, flashAtUs: performance.now() * 1000 });
        await sleep(500);
      }
    })();

    let modes: Record<string, number> = {};
    let resumedAt: number | undefined;
    let audioAfterResumeMs: number | undefined;
    const ticker = setInterval(() => {
      modes[clock.mode] = (modes[clock.mode] ?? 0) + 1;
      if (resumedAt !== undefined && audioAfterResumeMs === undefined && clock.mode === 'audio') {
        audioAfterResumeMs = Math.round(performance.now() - resumedAt);
      }
    }, 20);
    closers.push(() => clearInterval(ticker));
    const NO_AUDIO = { decoded: 0, late: 0, underruns: 0, trimmedUs: 0, bufferedUs: 0 };
    type AudioCounters = typeof NO_AUDIO;
    const audioStats = (): AudioCounters => {
      const stats = listener?.input.stats ?? NO_AUDIO;
      return {
        decoded: stats.decoded,
        late: stats.late,
        underruns: stats.underruns,
        trimmedUs: stats.trimmedUs,
        bufferedUs: stats.bufferedUs,
      };
    };
    interface Mark {
      phase: string;
      startUs: number;
      watcher: VideoWatcher['stats'];
      audio: AudioCounters;
    }
    const marks: Mark[] = [];
    const ends: { modes: Record<string, number>; longestGapUs: number; contextState: AudioContextState }[] = [];
    // Close the phase running, make the change, and start the next one.
    const begin = async (phase: string, change?: () => Promise<void>) => {
      check();
      if (marks.length > 0) {
        // A freeze still open at the phase's end counts too: no frame has closed it yet.
        const openGapUs = lastShownUs === undefined ? 0 : performance.now() * 1000 - lastShownUs;
        ends.push({ modes, longestGapUs: Math.max(longestGapUs, openGapUs), contextState: context.state });
      }
      await change?.();
      modes = {};
      longestGapUs = 0;
      marks.push({ phase, startUs: performance.now() * 1000, watcher: { ...watcher.stats }, audio: audioStats() });
      await options.report({ phase, contextState: context.state, clockMode: clock.mode, watcher: watcher.stats });
    };
    const hold = async (ms: number) => {
      for (const until = performance.now() + ms; performance.now() < until; ) {
        check();
        await sleep(100);
      }
    };

    await hold(2000);
    await begin('suspended');
    await hold(phaseMs);
    await begin('resumed', async () => {
      resumedAt = performance.now();
      await context.resume();
      (await listening).player.output.connect(tap);
    });
    await hold(phaseMs + 2000);
    await begin('muted', async () => {
      (await listening).input.gain = 0;
    });
    await hold(phaseMs);
    await begin('end');
    emitting = false;
    await emitter;
    await sleep(1000);
    check();

    const events = matchEvents(emitted, flashes, beeps, broadcast.originUs - broadcast.originEpochUs, arrivals);
    const switches = marks.map((mark) => mark.startUs);
    const within = (atUs: number, startUs: number) => atUs >= startUs && atUs < startUs + 2_000_000;
    const clear = (event: MatchedEvent) =>
      !switches.some((startUs) => within(event.beepAtUs, startUs) || within(event.heardAtUs, startUs));
    const syncOf = (list: MatchedEvent[]) =>
      list.flatMap((event) => (event.syncMs === undefined ? [] : [event.syncMs]));
    const phases: PhaseResult[] = [];
    for (let index = 0; index + 1 < marks.length; index++) {
      const mark = marks[index] as Mark;
      const next = marks[index + 1] as Mark;
      const end = ends[index] as (typeof ends)[number];
      const inPhase = events.filter((event) => event.beepAtUs >= mark.startUs && event.beepAtUs < next.startUs);
      phases.push({
        phase: mark.phase,
        contextState: end.contextState,
        modes: end.modes,
        shown: next.watcher.shown - mark.watcher.shown,
        late: next.watcher.late - mark.watcher.late,
        overflowed: next.watcher.overflowed - mark.watcher.overflowed,
        discontinuities: next.watcher.discontinuities - mark.watcher.discontinuities,
        longestGapMs: Math.round(end.longestGapUs / 1000),
        audio: {
          decoded: next.audio.decoded - mark.audio.decoded,
          late: next.audio.late - mark.audio.late,
          underruns: next.audio.underruns - mark.audio.underruns,
          trimmedMs: Math.round((next.audio.trimmedUs - mark.audio.trimmedUs) / 1000),
          bufferedMs: Math.round(next.audio.bufferedUs / 1000),
        },
        sync: summarize(syncOf(inPhase.filter(clear))),
        transient: syncOf(events.filter((event) => within(event.beepAtUs, mark.startUs))),
      });
    }
    return {
      userAgent: navigator.userAgent,
      latencies: { baseLatency: context.baseLatency, outputLatency: context.outputLatency },
      audioAfterResumeMs,
      emitted: emitted.length,
      matched: events.length,
      sequencer: { ...watcher.sequencer.stats },
      phases,
    };
  } finally {
    stop.abort(new Error('Scenario finished'));
    for (const close of closers.reverse()) await Promise.resolve(close()).catch(() => {});
    subscriber?.close();
  }
}
