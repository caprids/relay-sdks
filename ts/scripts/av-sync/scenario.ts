/**
 * The live A/V sync scenario, run inside one browser page: the WebCodecs example publishes a canvas and a microphone,
 * and the test player subscribes to them. Every 600 ms the canvas flashes white and the microphone beeps at the same
 * moment. The player reports when it showed each flash frame and when each beep reached the output; the offset between
 * the two delays is the A/V offset. Publisher and player share the page, so they share `performance.now()`'s clock.
 *
 * The microphones are synthetic: an `AudioContext` per device whose oscillator is gated on schedule into a
 * `MediaStreamAudioDestinationNode`, which the example captures like any microphone. That makes the beep's emission
 * time known, and lets the scenario switch devices: a second microphone (at another sample rate where the browser
 * allows it), a replaced capture context, a 200 ms step in the player's output delay, and a 3 s audio gap.
 */
import { createBroadcastClock } from '@mux/moq-media';
import {
  connectRelayPublisher,
  connectRelaySubscriber,
  type RelayPublisher,
  type RelaySubscriber,
  type RelayTransportFactory,
} from '@mux/relay';
import { captureMicrophone, type MicrophoneBlock } from '../../examples/webcodecs/capture-microphone.js';
import { publishCanvas } from '../../examples/webcodecs/publish-canvas.js';
import { createRenderClock } from '../../examples/webcodecs/render-clock.js';
import {
  createOnsetDetector,
  type EmittedEvent,
  type HeardBeep,
  type MatchedEvent,
  matchEvents,
  type OffsetSummary,
  type ShownFlash,
  summarize,
} from './measure.js';
import { type PlayerStats, type ReportMethod, startPlayer } from './player.js';

/** Where the publisher and the player connect. */
export type ScenarioTransport =
  | { kind: 'relay'; endpoint: string; token: string | undefined }
  | { kind: 'hub'; publisher: RelayTransportFactory; subscriber: RelayTransportFactory };

/** Options for {@link runAvSyncScenario}. */
export interface ScenarioOptions {
  transport: ScenarioTransport;
  namespace: string[];
  reportMethod: ReportMethod;
  /** Seconds per phase. Default 10. */
  phaseSeconds?: number;
  /** The player's audio jitter buffer, in ms. Default 150. */
  jitterMs?: number;
  /** Progress lines for the launcher. */
  report(value: object): unknown;
}

/** One phase's result. */
export interface PhaseResult {
  phase: string;
  /** Whether audio reached the player in this phase; without it, the clock paces video by the wall clock. */
  audio: boolean;
  /** What the phase changed, and anything the browser refused. */
  note?: string;
  /** The phase's events 2 s clear of every switch, both when emitted and when played. */
  settled: OffsetSummary | undefined;
  /** The events emitted or played in the first 2 s after the change, in order: the transient. */
  transient: number[];
  /** The settled offset net of the path to the capture input: what the recipe and the player answer for. */
  sync: OffsetSummary | undefined;
  /** `sync`'s transient. */
  syncTransient: number[];
  /** The settled offset's parts: path to the capture input, the recipe's stamping error, and the player's. */
  path: OffsetSummary | undefined;
  calibration: OffsetSummary | undefined;
  playout: OffsetSummary | undefined;
  /** Player counters over the phase. */
  framesShown: number;
  framesDropped: number;
  longestGapMs: number;
  audioUnderruns: number;
  modes: Record<string, number>;
}

/** The scenario's result. */
export interface ScenarioResult {
  userAgent: string;
  reportMethod: ReportMethod;
  sampleRates: { firstMicrophone: number; secondMicrophone: number; capture: number; player: number };
  latencies: { capture: object; player: object };
  emitted: number;
  matched: number;
  phases: PhaseResult[];
  /**
   * How far the spec's getOutputTimestamp() calibration pair is from the render clock on the capture context, in ms:
   * positive stamps audio late, negative early. See render-clock.ts.
   */
  formulaVsRenderMs: { p5: number; median: number; p95: number };
  /** Per beep, at the publisher: the recipe's stamp minus the tap's arrival time, in ms. */
  stampVsArrivalMs: number[];
  events: MatchedEvent[];
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** A synthetic microphone: a gated 1 kHz tone, with emission times known on `performance.now()`'s clock. */
async function syntheticMicrophone(sampleRate: number) {
  const context = new AudioContext({ sampleRate });
  await context.resume();
  const destination = context.createMediaStreamDestination();
  const oscillator = new OscillatorNode(context, { frequency: 1000 });
  const gate = new GainNode(context, { gain: 0 });
  oscillator.connect(gate).connect(destination);
  oscillator.start();
  const renders = createRenderClock();
  const observe = () => renders.observe(context.currentTime * 1e6, performance.now() * 1000);
  const ticker = setInterval(observe, 5);
  return {
    context,
    stream: destination.stream,
    /** Beep for 60 ms starting at `atUs` on `performance.now()`'s clock. */
    beep(atUs: number) {
      observe();
      const offsetUs = renders.renderedAtUs(0) as number;
      const startS = (atUs - offsetUs) / 1e6;
      gate.gain.setValueAtTime(0.5, startS);
      gate.gain.setValueAtTime(0, startS + 0.06);
    },
    async close() {
      clearInterval(ticker);
      await context.close();
    },
  };
}

/** Run the scenario; resolves with the measurements, rejects on the first failure. */
export async function runAvSyncScenario(options: ScenarioOptions): Promise<ScenarioResult> {
  const phaseMs = (options.phaseSeconds ?? 10) * 1000;
  const failures: unknown[] = [];
  const fail = (error: unknown) => failures.push(error);
  const check = () => {
    if (failures.length > 0) throw failures[0];
  };
  const stop = new AbortController();
  let publisher: RelayPublisher | undefined;
  let subscriber: RelaySubscriber | undefined;
  let player: Awaited<ReturnType<typeof startPlayer>> | undefined;
  const closers: (() => unknown)[] = [];
  try {
    const { transport } = options;
    const url = transport.kind === 'relay' ? transport.endpoint : 'https://hub.invalid/';
    const getToken = () => (transport.kind === 'relay' ? transport.token : undefined);
    publisher = await connectRelayPublisher({
      url,
      namespace: options.namespace,
      getToken,
      ...(transport.kind === 'hub' ? { createTransport: transport.publisher } : {}),
    });
    subscriber = await connectRelaySubscriber({
      url,
      getToken,
      ...(transport.kind === 'hub' ? { createTransport: transport.subscriber } : {}),
    });
    await options.report({ phase: 'connected' });

    const clock = createBroadcastClock();
    const first = await syntheticMicrophone(48_000);
    closers.push(() => first.close());
    let second: Awaited<ReturnType<typeof syntheticMicrophone>>;
    let secondNote: string | undefined;
    const captureContext = new AudioContext({ sampleRate: 48_000 });
    await captureContext.resume();
    closers.push(() => captureContext.state !== 'closed' && captureContext.close());
    // Firefox refuses a stream from a context at another rate: "Connecting AudioNodes from AudioContexts with
    // different sample-rate is currently not supported". Real microphones at other rates are resampled there.
    second = await syntheticMicrophone(16_000);
    try {
      captureContext.createMediaStreamSource(second.stream).disconnect();
    } catch (error) {
      secondNote = `16 kHz synthetic microphone refused (${String(error)}); the second microphone runs at 48 kHz`;
      await second.close();
      second = await syntheticMicrophone(48_000);
    }
    closers.push(() => second.close());
    const microphone = await captureMicrophone(clock, first.stream, {
      context: captureContext,
      // Synthetic: the beep reaches the worklet with no device in between, and the tap below measures the path.
      captureLatencyUs: () => 0,
    });
    closers.push(() => microphone.close());
    // A tap on the microphone's own blocks times when each beep reached the capture input: the block's arrival on the
    // main thread, min-filtered as render-clock.ts does. That splits the path delay (a real microphone's input
    // latency) from the recipe's error. A second MediaStreamAudioSourceNode on the same track would not do: in
    // Chromium each one buffers on its own. The same clock checks the spec's getOutputTimestamp() calibration pair.
    const arrivals: number[] = [];
    const blockRenders = new WeakMap<object, ReturnType<typeof createRenderClock>>();
    const formulaVsRenderMs: number[] = [];
    const stampVsArrivalMs: number[] = [];
    const tapOnsets = createOnsetDetector();
    let tapTimeline: object | undefined;
    const tapBlock = (block: MicrophoneBlock) => {
      const nowUs = performance.now() * 1000;
      if (block.timeline !== tapTimeline) {
        tapTimeline = block.timeline;
        tapOnsets.reset();
        blockRenders.set(block.timeline, createRenderClock());
      }
      const renders = blockRenders.get(block.timeline) as ReturnType<typeof createRenderClock>;
      renders.observe(block.contextUs + (block.samples.length * 1e6) / block.sampleRate, nowUs);
      for (const onsetUs of tapOnsets.push(block.samples, block.sampleRate, block.contextUs)) {
        const atUs = renders.renderedAtUs(onsetUs);
        if (atUs === undefined) continue;
        arrivals.push(atUs);
        stampVsArrivalMs.push(Math.round((block.timeline.timestampUs(onsetUs) - clock.fromSourceUs(atUs)) / 100) / 10);
      }
      const { context } = microphone;
      const { contextTime = 0, performanceTime = 0 } = context.getOutputTimestamp();
      // spec/media-layer.md: calibrate((contextTime + baseLatency + outputLatency) × 1e6, performanceTime × 1000).
      const renderedAtUs = renders.renderedAtUs((contextTime + context.baseLatency + context.outputLatency) * 1e6);
      // Positive: the formula says the context rendered later than it did, so audio would be stamped late.
      if (contextTime > 0 && renderedAtUs !== undefined) {
        formulaVsRenderMs.push((performanceTime * 1000 - renderedAtUs) / 1000);
      }
    };

    // Draw: dark frames with a moving bar; white while a flash lasts.
    const canvas = new OffscreenCanvas(640, 360);
    const draw = canvas.getContext('2d');
    if (!draw) throw new Error('No 2D canvas context');
    let flashUntil = 0;
    const paint = () => {
      const now = performance.now();
      draw.fillStyle = now < flashUntil ? '#fff' : '#202020';
      draw.fillRect(0, 0, canvas.width, canvas.height);
      if (now >= flashUntil) {
        draw.fillStyle = '#606060';
        draw.fillRect(Math.floor(now / 5) % canvas.width, 0, 20, canvas.height);
      }
    };
    paint();
    const painter = setInterval(paint, 10);
    closers.push(() => clearInterval(painter));

    const publishing = publishCanvas(canvas, publisher, options.namespace, { signal: stop.signal, microphone });
    void publishing.catch((error) => {
      if (!stop.signal.aborted) fail(error);
    });
    // publishCanvas installs its block consumer once its encoders are set up; run the tap in front of it.
    while (!microphone.onblock) {
      check();
      await sleep(10);
    }
    const encode = microphone.onblock;
    microphone.onblock = (block) => {
      tapBlock(block);
      encode(block);
    };
    await publisher.announced;
    await options.report({ phase: 'publishing' });

    const flashes: ShownFlash[] = [];
    const beeps: HeardBeep[] = [];
    player = await startPlayer({
      subscriber,
      namespace: options.namespace,
      reportMethod: options.reportMethod,
      ...(options.jitterMs === undefined ? {} : { jitterMs: options.jitterMs }),
      onFlash: (flash) => flashes.push(flash),
      onBeep: (beep) => beeps.push(beep),
      onError: fail,
    });
    await options.report({ phase: 'playing' });

    // Emit a flash and a beep together every 600 ms, scheduled 100 ms ahead.
    const emitted: EmittedEvent[] = [];
    let microphoneNow = first;
    let emitting = true;
    const emitter = (async () => {
      while (emitting) {
        const atMs = performance.now() + 100;
        microphoneNow.beep(atMs * 1000);
        await sleep(atMs - performance.now());
        flashUntil = performance.now() + 150;
        paint();
        emitted.push({ beepAtUs: atMs * 1000, flashAtUs: performance.now() * 1000 });
        await sleep(500);
      }
    })();

    const phases: { phase: string; startUs: number; note?: string; audio: boolean; stats: PlayerStats }[] = [];
    const playing = player;
    // A snapshot closes the previous phase's counters; the gap and the modes then restart for the next one.
    const snapshot = (): PlayerStats => {
      const longestGapMs = playing.takeLongestGapMs();
      const stats = { ...playing.stats, longestGapMs, modes: { ...playing.stats.modes } };
      playing.stats.modes = {};
      return stats;
    };
    const begin = async (phase: string, note?: string, audio = true) => {
      check();
      const stats = snapshot();
      phases.push({ phase, startUs: performance.now() * 1000, ...(note ? { note } : {}), audio, stats });
      const { framesShown, framesDropped, lastVideoOffsetMs, audioFrames, reports } = stats;
      await options.report({ phase, framesShown, framesDropped, lastVideoOffsetMs, audioFrames, reports });
    };
    const hold = async (ms: number) => {
      const until = performance.now() + ms;
      while (performance.now() < until) {
        check();
        await sleep(100);
      }
    };
    // Settle the jitter buffer and the calibration before measuring.
    await hold(3000 + (options.jitterMs ?? 0));
    await begin('baseline');
    await hold(phaseMs);
    microphone.useStream(second.stream);
    microphoneNow = second;
    await begin('second microphone', `${second.context.sampleRate} Hz${secondNote ? `; ${secondNote}` : ''}`);
    await hold(phaseMs);
    const replacement = new AudioContext({ sampleRate: 48_000 });
    await replacement.resume();
    closers.push(() => replacement.state !== 'closed' && replacement.close());
    await microphone.useContext(replacement);
    await captureContext.close();
    await begin('replaced capture context');
    await hold(phaseMs);
    player.setOutputDelay(200);
    await begin('output delay +200 ms');
    await hold(phaseMs);
    player.setAudioPaused(true);
    await begin('audio gap 3 s', undefined, false);
    await hold(3000);
    player.setAudioPaused(false);
    await begin('audio resumed');
    await hold(phaseMs);
    phases.push({ phase: 'end', startUs: performance.now() * 1000, audio: true, stats: snapshot() });
    emitting = false;
    await emitter;
    await sleep(1000);
    check();

    const events = matchEvents(emitted, flashes, beeps, clock.originUs - clock.originEpochUs, arrivals);
    const sorted = [...formulaVsRenderMs].sort((a, b) => a - b);
    const percentile = (fraction: number) =>
      Math.round((sorted[Math.min(sorted.length - 1, Math.floor(fraction * sorted.length))] ?? Number.NaN) * 10) / 10;
    const results: PhaseResult[] = [];
    // A switch on the publisher shows in events emitted after it; one on the player, in events played after it, which
    // the jitter buffer puts up to its depth later. Settled events are 2 s clear of every switch on both counts.
    const switches = phases.map((phase) => phase.startUs);
    const within = (atUs: number, startUs: number) => atUs >= startUs && atUs < startUs + 2_000_000;
    const clear = (event: MatchedEvent) =>
      !switches.some((startUs) => within(event.beepAtUs, startUs) || within(event.heardAtUs, startUs));
    for (let index = 0; index + 1 < phases.length; index++) {
      const phase = phases[index] as (typeof phases)[number];
      const next = phases[index + 1] as (typeof phases)[number];
      const inPhase = events.filter((event) => event.beepAtUs >= phase.startUs && event.beepAtUs < next.startUs);
      const settled = inPhase.filter(clear);
      const transient = events.filter(
        (event) => within(event.beepAtUs, phase.startUs) || within(event.heardAtUs, phase.startUs),
      );
      results.push({
        phase: phase.phase,
        audio: phase.audio,
        ...(phase.note ? { note: phase.note } : {}),
        settled: summarize(settled.map((event) => event.offsetMs)),
        transient: transient.map((event) => event.offsetMs),
        sync: summarize(settled.flatMap((event) => (event.syncMs === undefined ? [] : [event.syncMs]))),
        syncTransient: transient.flatMap((event) => (event.syncMs === undefined ? [] : [event.syncMs])),
        path: summarize(settled.flatMap((event) => (event.pathMs === undefined ? [] : [event.pathMs]))),
        calibration: summarize(
          settled.flatMap((event) => (event.calibrationMs === undefined ? [] : [event.calibrationMs])),
        ),
        playout: summarize(settled.map((event) => event.playoutMs)),
        framesShown: next.stats.framesShown - phase.stats.framesShown,
        framesDropped: next.stats.framesDropped - phase.stats.framesDropped,
        longestGapMs: next.stats.longestGapMs,
        audioUnderruns: next.stats.audioUnderruns - phase.stats.audioUnderruns,
        modes: next.stats.modes,
      });
    }
    return {
      userAgent: navigator.userAgent,
      reportMethod: options.reportMethod,
      sampleRates: {
        firstMicrophone: first.context.sampleRate,
        secondMicrophone: second.context.sampleRate,
        capture: replacement.sampleRate,
        player: player.context.sampleRate,
      },
      latencies: {
        capture: { baseLatency: replacement.baseLatency, outputLatency: replacement.outputLatency },
        player: { baseLatency: player.context.baseLatency, outputLatency: player.context.outputLatency },
      },
      emitted: emitted.length,
      matched: events.length,
      phases: results,
      formulaVsRenderMs: { p5: percentile(0.05), median: percentile(0.5), p95: percentile(0.95) },
      stampVsArrivalMs,
      events,
    };
  } finally {
    stop.abort(new Error('Scenario finished'));
    await player?.stop().catch(() => {});
    for (const close of closers.reverse()) await Promise.resolve(close()).catch(() => {});
    subscriber?.close();
    await publisher?.close().catch(() => {});
  }
}
