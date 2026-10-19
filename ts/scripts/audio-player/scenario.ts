/**
 * The live audio player scenario, run inside one browser page: the WebCodecs example publishes a synthetic microphone
 * (a steady 440 Hz tone) with its canvas, and the example's audio-only listener plays it back through the audio
 * player. Publisher and listener share the page, so they share `performance.now()`'s clock, and the playout delay is
 * the broadcast clock's now less the playout clock's position: capture to audible, less any microphone latency. The
 * publisher also lists the interrupt track, and the cut phase sends an interrupt the listener applies. Between steady
 * playback and the cut, the input's playback rate is nudged as the Video.js renderer's latency controller does: 0.95
 * for a second, then 1.05 for a second (issue #329).
 */
import { buildInterrupt, createBroadcastClock, INTERRUPT_TRACK, type Interrupt } from '@mux/moq-media';
import { connectRelayPublisher, connectRelaySubscriber, type RelayTransportFactory } from '@mux/relay';
import { captureMicrophone } from '../../examples/webcodecs/capture-microphone.js';
import { listenAudio } from '../../examples/webcodecs/listen-audio.js';
import { publishCanvas } from '../../examples/webcodecs/publish-canvas.js';

/** Where the publisher and the listener connect. */
export type ScenarioTransport =
  | { kind: 'relay'; endpoint: string; token: string | undefined }
  | { kind: 'hub'; publisher: RelayTransportFactory; subscriber: RelayTransportFactory };

/** Options for {@link runAudioPlayerScenario}. */
export interface ScenarioOptions {
  transport: ScenarioTransport;
  namespace: string[];
  /** Seconds of steady playback. Default 8. */
  seconds?: number;
  report(value: object): unknown;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const quantile = (values: number[], q: number) => {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))];
};

/** Run the scenario; resolves with its measurements. */
export async function runAudioPlayerScenario(options: ScenarioOptions) {
  const { transport } = options;
  const url = transport.kind === 'relay' ? transport.endpoint : 'https://hub.invalid/';
  const getToken = () => (transport.kind === 'relay' ? transport.token : undefined);
  const publisher = await connectRelayPublisher({
    url,
    namespace: options.namespace,
    getToken,
    ...(transport.kind === 'hub' ? { createTransport: transport.publisher } : {}),
  });
  const subscriber = await connectRelaySubscriber({
    url,
    getToken,
    ...(transport.kind === 'hub' ? { createTransport: transport.subscriber } : {}),
  });
  const failures: unknown[] = [];
  const stop = new AbortController();
  const tone = new AudioContext({ sampleRate: 48_000 });
  const destination = tone.createMediaStreamDestination();
  const oscillator = new OscillatorNode(tone, { frequency: 440 });
  oscillator.connect(new GainNode(tone, { gain: 0.3 })).connect(destination);
  oscillator.start();
  const captureContext = new AudioContext({ sampleRate: 48_000 });
  const broadcast = createBroadcastClock();
  const microphone = await captureMicrophone(broadcast, destination.stream, {
    context: captureContext,
    captureLatencyUs: () => 0,
  });
  const canvas = new OffscreenCanvas(320, 180);
  const draw = canvas.getContext('2d');
  if (!draw) throw new Error('No 2D canvas context');
  draw.fillRect(0, 0, 320, 180);
  const interrupts = publisher.registerTrack({ track: INTERRUPT_TRACK });
  const publishing = publishCanvas(canvas, publisher, options.namespace, {
    signal: stop.signal,
    microphone,
    data: [{ name: INTERRUPT_TRACK }],
  });
  publishing.catch((error) => stop.signal.aborted || failures.push(error));
  await publisher.announced;

  // The listener plays through its own context, as a page would; 48 kHz so no resampling is involved.
  const playback = new AudioContext({ sampleRate: 48_000, latencyHint: 'interactive' });
  await playback.resume();
  let listener: Awaited<ReturnType<typeof listenAudio>> | undefined;
  const applied: { interrupt: Interrupt; at: number }[] = [];
  for (const deadline = performance.now() + 15_000; !listener; ) {
    try {
      listener = await listenAudio(subscriber, options.namespace, {
        context: playback,
        onError: (error) => failures.push(error),
        onInterrupt: (interrupt) => applied.push({ interrupt, at: performance.now() }),
      });
    } catch (error) {
      // A subscribe that reaches the relay before the announce is rejected (the announce race); retry for a while.
      if (performance.now() > deadline) throw error;
      await sleep(250);
    }
  }
  const { input, clock, sequencer } = listener;
  // The largest step between two output samples: the tone's own is about 0.017, and a click is a larger one.
  const analyser = new AnalyserNode(playback, { fftSize: 2048 });
  listener.player.output.connect(analyser);
  const recent = new Float32Array(analyser.fftSize);
  const sample = (phase: string, durationMs: number) => {
    const delays: number[] = [];
    const depths: number[] = [];
    const modes: Record<string, number> = {};
    const before = { ...input.stats };
    let maxStep = 0;
    // The smallest advance of the clock's position between two ticks: negative is a step back.
    let minAdvanceMs = Number.POSITIVE_INFINITY;
    let lastPosition: number | undefined;
    return new Promise<object>((resolve) => {
      const started = performance.now();
      const ticker = setInterval(() => {
        modes[clock.mode] = (modes[clock.mode] ?? 0) + 1;
        const position = clock.position();
        if (position !== undefined && clock.mode === 'audio') delays.push((broadcast.now() - position) / 1000);
        if (position !== undefined && lastPosition !== undefined) {
          minAdvanceMs = Math.min(minAdvanceMs, (position - lastPosition) / 1000);
        }
        lastPosition = position;
        depths.push(input.stats.bufferedUs / 1000);
        // The analyser holds the last 43 ms, so ticks every 20 ms see every sample.
        if (input.stats.playing) {
          analyser.getFloatTimeDomainData(recent);
          for (let index = 1; index < recent.length; index++) {
            maxStep = Math.max(maxStep, Math.abs((recent[index] as number) - (recent[index - 1] as number)));
          }
        }
        if (performance.now() - started < durationMs) return;
        clearInterval(ticker);
        const after = input.stats;
        resolve({
          phase,
          decoded: after.decoded - before.decoded,
          underruns: after.underruns - before.underruns,
          trimmedMs: (after.trimmedUs - before.trimmedUs) / 1000,
          concealedMs: (after.concealedUs - before.concealedUs) / 1000,
          cutMs: (after.cutUs - before.cutUs) / 1000,
          jitterTargetMs: after.jitterTargetUs / 1000,
          bufferedMs: { p50: quantile(depths, 0.5), p95: quantile(depths, 0.95) },
          playoutDelayMs:
            delays.length > 0
              ? { p5: quantile(delays, 0.05), p50: quantile(delays, 0.5), p95: quantile(delays, 0.95) }
              : undefined,
          delayFirstLastMs: delays.length > 0 ? [delays[0], delays.at(-1)] : undefined,
          maxStep: Math.round(maxStep * 1e4) / 1e4,
          minAdvanceMs: Number.isFinite(minAdvanceMs) ? Math.round(minAdvanceMs * 10) / 10 : undefined,
          playbackRate: input.playbackRate,
          outputLatencyMs: Math.round((playback.baseLatency + playback.outputLatency) * 1e4) / 10,
          modes,
        });
      }, 20);
    });
  };

  const phases: object[] = [];
  try {
    // Joining: until the first report, the clock is idle.
    phases.push(await sample('join', 2000));
    await options.report(phases.at(-1) as object);
    phases.push(await sample('steady', (options.seconds ?? 8) * 1000));
    await options.report(phases.at(-1) as object);
    // Gain 50 ms of latency, then shed it, as the Video.js renderer's ±5 % nudges do.
    for (const [phase, rate] of [
      ['slow', 0.95],
      ['fast', 1.05],
    ] as const) {
      input.playbackRate = rate;
      phases.push(await sample(phase, 1000));
      await options.report(phases.at(-1) as object);
    }
    input.playbackRate = 1;
    // An interruption over the wire: drop everything held and whatever arrives for the next 300 ms of the timeline.
    const sentAt = performance.now();
    const untilUs = broadcast.now() + 300_000;
    await interrupts.write(buildInterrupt({ untilUs }));
    const cut = await sample('cut', 2000);
    const received = applied.find((entry) => entry.interrupt.untilUs === untilUs);
    if (!received) throw new Error('The listener applied no interrupt within 2 s');
    phases.push({ ...cut, interruptMs: Math.round((received.at - sentAt) * 10) / 10 });
    await options.report(phases.at(-1) as object);
    if (failures.length > 0) throw failures[0];
    return { phases, sequencer: { ...sequencer.stats }, stats: { ...input.stats } };
  } finally {
    analyser.disconnect();
    stop.abort();
    await listener.stop();
    microphone.close();
    await publishing.catch(() => {});
    for (const context of [tone, captureContext, playback]) await context.close().catch(() => {});
    await subscriber.close();
    await publisher.close();
  }
}
