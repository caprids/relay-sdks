/**
 * The A/V sync test's player: subscribes to one broadcast's catalog, video and audio, reads both through frame readers
 * (decode order, late frames dropped against the playout clock's target latency), decodes both with WebCodecs, plays
 * audio through an AudioContext with a fixed jitter buffer, and schedules video with the playout clock. It
 * reports each flash frame it shows and each beep it plays, with times on `performance.now()`'s clock.
 *
 * It renders nothing: "shown" is the moment the renderer would draw. Audio really plays, through whatever output the
 * browser has; its audible time is the render clock plus the context's reported `baseLatency + outputLatency`.
 */
import {
  AUDIO_TRACK,
  CATALOG_TRACK,
  createFrameSequencer,
  createPlayoutClock,
  type MsfCatalogTrack,
  type PlayoutClock,
  type PlayoutClockOptions,
  parseMsfCatalog,
  type SequencedFrame,
  VIDEO_TRACK,
} from '@mux/moq-media';
import type { RelaySubscriber, RelaySubscription } from '@mux/relay';
import { createFrameReader, type FrameReader } from '@mux/relay/media';
import { createRenderClock, type RenderClock } from '../../examples/webcodecs/render-clock.js';
import { createOnsetDetector, type HeardBeep, type ShownFlash } from './measure.js';

/** How the player tells the playout clock which audio is audible now. */
export type ReportMethod = 'output-timestamp' | 'render-clock';

/** Options for {@link startPlayer}. */
export interface PlayerOptions {
  /** The subscriber connection. */
  subscriber: RelaySubscriber;
  /** The broadcast's namespace. */
  namespace: readonly string[];
  /**
   * `output-timestamp`: `getOutputTimestamp()` read fresh for every report, as spec/media-layer.md describes.
   * `render-clock`: the render clock plus `baseLatency + outputLatency`, which means the same thing in every engine.
   */
  reportMethod: ReportMethod;
  /** Audio jitter-buffer depth in ms. Default 150. */
  jitterMs?: number;
  /** Playout clock options. */
  clock?: PlayoutClockOptions;
  /** A flash's first frame was shown. */
  onFlash(flash: ShownFlash): void;
  /** A beep's onset reached the output. */
  onBeep(beep: HeardBeep): void;
  /** A failure the player cannot continue from. */
  onError(error: unknown): void;
}

/** Counters the test reports. */
export interface PlayerStats {
  framesShown: number;
  framesDropped: number;
  /** The largest gap between two shown frames, in ms, and when it ended. */
  longestGapMs: number;
  audioUnderruns: number;
  audioFrames: number;
  reports: number;
  /** The playout clock's mode, sampled at every report tick. */
  modes: Record<string, number>;
  /** The latest video decision's offset, in ms: positive is early. */
  lastVideoOffsetMs: number | undefined;
  /** Video frames that reset the decoder: a gap, a resync, or a restart. */
  discontinuities: number;
}

/** A running player. */
export interface Player {
  readonly clock: PlayoutClock;
  readonly context: AudioContext;
  readonly stats: PlayerStats;
  /** Simulate an output device change: the audio path's delay steps to `delayMs`, and the player knows it. */
  setOutputDelay(delayMs: number): void;
  /** Ignore incoming audio objects, as if the publisher went quiet; reports stop once the buffer drains. */
  setAudioPaused(paused: boolean): void;
  /** How much audio is scheduled ahead of the context's current time, in ms: the jitter buffer plus any backlog. */
  audioAheadMs(): number;
  /**
   * The longest gap between shown frames since the last call, counting a gap still open now, in ms; restarts the
   * count. A stall that outlasts a phase is counted in the phase it began in, and again in the next.
   */
  takeLongestGapMs(): number;
  stop(): Promise<void>;
}

const BRIGHT = 160;
/** How far beyond the jitter buffer's depth audio may be scheduled before the player re-anchors, in seconds. */
const MAX_EXTRA_AHEAD_S = 0.1;

/** Start playing the broadcast at `namespace`. Resolves once the catalog has been read and both tracks subscribed. */
export async function startPlayer(options: PlayerOptions): Promise<Player> {
  const jitterS = (options.jitterMs ?? 150) / 1000;
  const context = new AudioContext({ sampleRate: 48_000 });
  await context.resume();
  const delay = context.createDelay(1);
  delay.connect(context.destination);
  let extraDelayUs = 0;
  const clock = createPlayoutClock(options.clock);
  const renders: RenderClock = createRenderClock();
  const stats: PlayerStats = {
    framesShown: 0,
    framesDropped: 0,
    longestGapMs: 0,
    audioUnderruns: 0,
    audioFrames: 0,
    reports: 0,
    modes: {},
    lastVideoOffsetMs: undefined,
    discontinuities: 0,
  };
  const subscriptions: RelaySubscription[] = [];
  let stopped = false;
  const fail = (error: unknown) => {
    if (!stopped) options.onError(error);
  };

  // A subscribe that reaches the relay before the announce is rejected (the announce race); retry for a while.
  const readCatalog = () =>
    new Promise<MsfCatalogTrack[]>((resolve, reject) => {
      const subscription = options.subscriber.subscribe(
        { namespace: [...options.namespace], track: CATALOG_TRACK },
        {
          onObject(object) {
            if (object.status !== 'normal') return;
            try {
              resolve(parseMsfCatalog(object.payload, { catalogNamespace: options.namespace }).tracks);
            } catch (error) {
              reject(error);
            }
          },
          onError(error) {
            subscription.cancel();
            reject(error);
          },
        },
      );
      subscriptions.push(subscription);
    });
  let catalog: MsfCatalogTrack[] | undefined;
  for (const deadline = performance.now() + 15_000; !catalog; ) {
    try {
      catalog = await readCatalog();
    } catch (error) {
      if (performance.now() > deadline) throw error;
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
  }
  const videoTrack = catalog.find((track) => track.name === VIDEO_TRACK);
  const audioTrack = catalog.find((track) => track.name === AUDIO_TRACK);
  if (!videoTrack?.codec || !audioTrack?.codec) throw new Error('The catalog must list video and audio');

  // --- Video: decode, measure brightness, schedule with the playout clock.
  const probe = new OffscreenCanvas(8, 8).getContext('2d', { willReadFrequently: true });
  if (!probe) throw new Error('No 2D context for the brightness probe');
  const queue: { frame: VideoFrame; timestampUs: number; bright: boolean }[] = [];
  let wasBright = false;
  let lastShownUs: number | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const pump = () => {
    timer = undefined;
    while (queue.length > 0) {
      const head = queue[0] as (typeof queue)[number];
      const decision = clock.due(head.timestampUs);
      stats.lastVideoOffsetMs = Math.round(decision.offsetUs / 1000);
      if (decision.action === 'wait') {
        timer = setTimeout(pump, decision.waitUs / 1000);
        return;
      }
      queue.shift();
      head.frame.close();
      if (decision.action === 'drop') {
        stats.framesDropped++;
        continue;
      }
      const shownAtUs = performance.now() * 1000;
      if (lastShownUs !== undefined) {
        stats.longestGapMs = Math.max(stats.longestGapMs, Math.round((shownAtUs - lastShownUs) / 1000));
      }
      lastShownUs = shownAtUs;
      stats.framesShown++;
      if (head.bright && !wasBright) options.onFlash({ timestampUs: head.timestampUs, shownAtUs });
      wasBright = head.bright;
    }
  };
  const videoDecoder = new VideoDecoder({
    output(frame) {
      probe.drawImage(frame, 0, 0, 8, 8);
      const pixels = probe.getImageData(0, 0, 8, 8).data;
      let sum = 0;
      for (let index = 0; index < pixels.length; index += 4) sum += pixels[index + 1] as number;
      queue.push({ frame, timestampUs: frame.timestamp, bright: sum / (pixels.length / 4) > BRIGHT });
      if (timer === undefined) pump();
    },
    error: fail,
  });
  const videoConfig: VideoDecoderConfig = {
    codec: videoTrack.codec,
    ...(videoTrack.initData ? { description: videoTrack.initData } : {}),
    optimizeForLatency: true,
  };
  videoDecoder.configure(videoConfig);

  // --- Audio: decode, find beeps, schedule into the jitter buffer.
  const onsets = createOnsetDetector();
  let anchor: { mediaUs: number; contextS: number } | undefined;
  let scheduledFromS = Number.POSITIVE_INFINITY;
  let scheduledUntilS = 0;
  const pendingBeeps: { timestampUs: number; contextS: number }[] = [];
  let audioPaused = false;
  let reanchorAudio = false;
  // Scheduled audio not yet played: a resync stops it, so the backlog it was anchored on does not keep playing.
  let scheduled: { source: AudioBufferSourceNode; endS: number }[] = [];
  const contextTimeOf = (mediaUs: number) => (anchor ? anchor.contextS + (mediaUs - anchor.mediaUs) / 1e6 : 0);
  const audioDecoder = new AudioDecoder({
    output(data) {
      try {
        const samples = new Float32Array(data.numberOfFrames);
        data.copyTo(samples, { planeIndex: 0, format: 'f32-planar' });
        const { sampleRate, timestamp } = data;
        data.close();
        stats.audioFrames++;
        let startS = contextTimeOf(timestamp);
        if (!anchor || reanchorAudio || startS > context.currentTime + jitterS + MAX_EXTRA_AHEAD_S) {
          // First audio, a resync or restart, or a schedule that has drifted ahead of the jitter buffer's depth (the
          // context's clock stalled): (re)start the jitter buffer at this frame, so its depth stays what it was set to.
          reanchorAudio = false;
          for (const { source } of scheduled) source.stop();
          scheduled = [];
          pendingBeeps.length = 0;
          scheduledUntilS = 0;
          anchor = { mediaUs: timestamp, contextS: context.currentTime + jitterS };
          scheduledFromS = anchor.contextS;
          startS = anchor.contextS;
        } else if (startS < context.currentTime + 0.005) {
          // Its slot has passed. Re-anchoring on it would add its lateness to every later frame (and, since audio
          // drives the playout clock, to the clock too); drop it and keep the schedule.
          stats.audioUnderruns++;
          return;
        }
        const buffer = new AudioBuffer({ length: samples.length, sampleRate, numberOfChannels: 1 });
        buffer.copyToChannel(samples, 0);
        const source = new AudioBufferSourceNode(context, { buffer });
        source.connect(delay);
        source.start(startS);
        const endS = startS + samples.length / sampleRate;
        scheduled = scheduled.filter((entry) => entry.endS > context.currentTime);
        scheduled.push({ source, endS });
        scheduledUntilS = Math.max(scheduledUntilS, startS + samples.length / sampleRate);
        for (const onsetUs of onsets.push(samples, sampleRate, timestamp)) {
          pendingBeeps.push({ timestampUs: onsetUs, contextS: contextTimeOf(onsetUs) });
        }
      } catch (error) {
        fail(error);
      }
    },
    error: fail,
  });
  const audioConfig: AudioDecoderConfig = {
    codec: audioTrack.codec,
    sampleRate: audioTrack.samplerate ?? 48_000,
    numberOfChannels: Number(audioTrack.channelConfig ?? 2),
    ...(audioTrack.initData ? { description: audioTrack.initData } : {}),
  };
  audioDecoder.configure(audioConfig);
  // Decoders count samples from their first input: after a gap, Chromium's keeps counting from before it. Reset the
  // decoder at a discontinuity so its output timestamps start again from the input's.
  let nextAudioUs: number | undefined;

  // --- Reports: the playout clock learns which audio is audible, about every 20 ms.
  const latencyUs = () => (context.baseLatency + (context.outputLatency || 0)) * 1e6 + extraDelayUs;
  const tick = () => {
    const nowUs = performance.now() * 1000;
    renders.observe(context.currentTime * 1e6, nowUs);
    const renderOffsetUs = renders.renderedAtUs(0) ?? 0;
    // Beeps that have rendered: their audible time, from the render clock and the reported latencies.
    while (
      pendingBeeps.length > 0 &&
      (pendingBeeps[0] as (typeof pendingBeeps)[number]).contextS < context.currentTime
    ) {
      const beep = pendingBeeps.shift() as (typeof pendingBeeps)[number];
      options.onBeep({
        timestampUs: beep.timestampUs,
        audibleAtUs: beep.contextS * 1e6 + renderOffsetUs + latencyUs(),
      });
    }
    let audibleS: number;
    let atUs: number;
    if (options.reportMethod === 'output-timestamp') {
      const { contextTime = 0, performanceTime = 0 } = context.getOutputTimestamp();
      // The delay node is inside the graph, where getOutputTimestamp() cannot see it.
      audibleS = contextTime - extraDelayUs / 1e6;
      atUs = performanceTime * 1000;
    } else {
      audibleS = (nowUs - renderOffsetUs - latencyUs()) / 1e6;
      atUs = nowUs;
    }
    if (anchor && audibleS >= scheduledFromS && audibleS <= scheduledUntilS) {
      clock.reportAudio(anchor.mediaUs + (audibleS - anchor.contextS) * 1e6, atUs);
      stats.reports++;
    }
    stats.modes[clock.mode] = (stats.modes[clock.mode] ?? 0) + 1;
  };
  const ticker = setInterval(() => {
    try {
      tick();
    } catch (error) {
      fail(error);
    }
  }, 20);

  // Each track is read through a frame reader: in decode order, with what is too late against the clock's target
  // latency dropped. A discontinuity is where a decoder resets; the next video frame is a keyframe.
  const readers: FrameReader[] = [];
  for (const [track, grouping, onFrame] of [
    [
      VIDEO_TRACK,
      'keyframe',
      (frame: SequencedFrame) => {
        if (frame.discontinuity === 'resync' || frame.discontinuity === 'restart') {
          // The shared clock was reset: decoded frames waiting for their time, and audio scheduled on the old anchor,
          // belong to the old clock. Audio re-anchors at its next frame, which its own reader may not mark.
          for (const { frame: waiting } of queue.splice(0)) waiting.close();
          if (timer !== undefined) clearTimeout(timer);
          timer = undefined;
          reanchorAudio = true;
        }
        if (frame.discontinuity) {
          stats.discontinuities++;
          videoDecoder.reset();
          videoDecoder.configure(videoConfig);
        }
        videoDecoder.decode(
          new EncodedVideoChunk({
            type: frame.key ? 'key' : 'delta',
            timestamp: frame.timestampUs,
            data: frame.payload,
          }),
        );
      },
    ],
    [
      AUDIO_TRACK,
      'frame',
      (frame: SequencedFrame) => {
        if (audioPaused) return;
        if (frame.discontinuity === 'resync' || frame.discontinuity === 'restart') reanchorAudio = true;
        if (nextAudioUs !== undefined && Math.abs(frame.timestampUs - nextAudioUs) > 5_000) {
          audioDecoder.reset();
          audioDecoder.configure(audioConfig);
        }
        // One Opus frame per object; 20 ms is the example's frame length.
        nextAudioUs = frame.timestampUs + 20_000;
        audioDecoder.decode(new EncodedAudioChunk({ type: 'key', timestamp: frame.timestampUs, data: frame.payload }));
      },
    ],
  ] as const) {
    const reader = createFrameReader(createFrameSequencer({ clock, grouping }), {
      onFrame(frame) {
        if (stopped) return;
        try {
          onFrame(frame);
        } catch (error) {
          fail(error);
        }
      },
    });
    readers.push(reader);
    subscriptions.push(
      options.subscriber.subscribe({ namespace: [...options.namespace], track }, { ...reader, onError: fail }),
    );
  }

  return {
    clock,
    context,
    stats,
    setOutputDelay(delayMs) {
      delay.delayTime.setValueAtTime(delayMs / 1000, context.currentTime);
      extraDelayUs = delayMs * 1000;
    },
    setAudioPaused(paused) {
      audioPaused = paused;
    },
    audioAheadMs() {
      return Math.round(Math.max(0, scheduledUntilS - context.currentTime) * 1000);
    },
    takeLongestGapMs() {
      const openGapMs = lastShownUs === undefined ? 0 : Math.round((performance.now() * 1000 - lastShownUs) / 1000);
      const longest = Math.max(stats.longestGapMs, openGapMs);
      stats.longestGapMs = 0;
      return longest;
    },
    async stop() {
      stopped = true;
      for (const reader of readers) reader.close();
      clearInterval(ticker);
      if (timer !== undefined) clearTimeout(timer);
      for (const subscription of subscriptions) subscription.cancel();
      for (const { frame } of queue.splice(0)) frame.close();
      if (videoDecoder.state !== 'closed') videoDecoder.close();
      if (audioDecoder.state !== 'closed') audioDecoder.close();
      await context.close();
    },
  };
}
