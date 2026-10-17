/**
 * A headless audio player: decodes each input's frames with WebCodecs, holds them in an adaptive jitter buffer in an
 * `AudioWorklet`, mixes every input into one output node, and reports what is audible to each input's playout clock.
 * No UI and no transport: frames in, a node and clock reports out. See spec/media-layer.md, "Audio player".
 */

import {
  AUDIO_PLAYER_PROCESSOR,
  AUDIO_PLAYER_WORKLET_SOURCE,
  type AudioPlayerProcessorCommand,
  type AudioPlayerProcessorOptions,
  type AudioPlayerProcessorReport,
} from './audio-worklet.js';
import type { PlayoutClock } from './clock.js';
import { MICROSECONDS_PER_SECOND } from './loc.js';
import { createRenderClock, type RenderClock } from './render-clock.js';
import type { FrameDiscontinuity, SequencedFrame } from './sequencer.js';

/** The part of an `AudioNode` the player uses. A Web Audio node qualifies. */
export interface AudioPlayerNode {
  /** Send this node's output to `destination`. */
  connect(destination: AudioPlayerNode): unknown;
  /** Disconnect from `destination`, or from everything. */
  disconnect(destination?: AudioPlayerNode): void;
}

/** The part of a `GainNode` the player uses. */
export interface AudioPlayerGainNode extends AudioPlayerNode {
  /** The gain applied to what passes through. */
  readonly gain: {
    /** The multiplier: 1 passes audio unchanged. */
    value: number;
  };
}

/** The part of an `AudioContext` the player uses. A Web Audio `AudioContext` qualifies. */
export interface AudioPlayerContext {
  /** Frames per second the context renders at; decoded audio at another rate is resampled to it. */
  readonly sampleRate: number;
  /** Seconds between the graph and the audio subsystem. */
  readonly baseLatency?: number;
  /** Seconds from the audio subsystem to the speaker; changes with the device. */
  readonly outputLatency?: number;
  /** Where the jitter buffer's worklet module is loaded. */
  readonly audioWorklet: {
    /** Load a worklet module from a URL. */
    addModule(url: string): Promise<void>;
  };
  /** Create a gain node: one per input, and the output. */
  createGain(): AudioPlayerGainNode;
}

/** Options for {@link createAudioPlayer}. */
export interface AudioPlayerOptions<Context extends AudioPlayerContext> {
  /** The one context every input plays through. Create or resume it from a user gesture. */
  context: Context;
}

/** One encoded audio frame: a `SequencedFrame` from a frame reader qualifies. */
export type AudioPlayerFrame = Pick<SequencedFrame, 'timestampUs' | 'payload'> & {
  /** Set where the frame does not follow the one before: a decoder resets, and a `resync` or `restart` flushes. */
  discontinuity?: FrameDiscontinuity | undefined;
};

/** Options for {@link AudioPlayer.addInput}. Durations are microseconds. */
export interface AudioInputOptions {
  /**
   * The broadcast's playout clock. The input reports the audible timestamp to it about every 20 ms while it plays,
   * and its target latency caps the jitter buffer. Without one, nothing is reported.
   */
  clock?: PlayoutClock;
  /** How to decode. Default: Opus, 48 kHz, two channels; `audioDecoderConfigFromTrack` reads one from the catalog. */
  decoderConfig?: AudioDecoderConfig;
  /** The input's gain. Default 1. */
  gain?: number;
  /** How much audio to hold before playing, at the start and after each underrun. Default 60,000. */
  jitterTargetUs?: number;
  /** The least the target shrinks to while playback is smooth. Default 40,000. */
  minJitterTargetUs?: number;
  /**
   * The most the target grows to after underruns. Default 400,000, and never more than the clock's target latency
   * less the output delay and 40 ms: a frame further ahead of the clock than its target latency makes the frame
   * sequencer resync.
   */
  maxJitterTargetUs?: number;
  /** A decoder error. The input decodes again from the next frame, with a new decoder. */
  onError?(error: unknown): void;
}

/** Which audio {@link AudioPlayerInput.cut} drops, by timeline timestamp in microseconds. */
export interface AudioCutRange {
  /** Drop from this timestamp on. Default: from the oldest audio held. */
  fromUs?: number;
  /**
   * Drop before this timestamp. Default: through the newest audio held. Frames that arrive later with an earlier
   * timestamp are dropped too.
   */
  untilUs?: number;
}

/** An input's counters and jitter-buffer state. Counters are cumulative; durations are microseconds. */
export interface AudioInputStats {
  /** Frames handed to the decoder. */
  decoded: number;
  /** Frames dropped on arrival: behind audio already decoded, or inside a cut. */
  late: number;
  /** Times the decoder was reset or replaced: at a discontinuity, a timestamp jump, or after an error. */
  decoderResets: number;
  /** Decoder errors. */
  errors: number;
  /** Times the buffer ran dry while playing and audio came back within the jitter-buffer ceiling. */
  underruns: number;
  /** Whether the buffer is playing, rather than filling to its target. */
  playing: boolean;
  /** The audio held now. */
  bufferedUs: number;
  /** The jitter-buffer target now. */
  jitterTargetUs: number;
  /** Audio dropped to shed latency: held past what the target needed, or past the hard bound. */
  trimmedUs: number;
  /** Silence played in place of frames that never came. */
  concealedUs: number;
  /** Audio dropped by `cut()`. */
  cutUs: number;
  /** Clock reports made. */
  reports: number;
}

/** One input of an {@link AudioPlayer}: one track, usually one speaker. */
export interface AudioPlayerInput {
  /**
   * Decode and buffer a frame, in decode order. A frame reader's `onFrame` can be this. A `resync` or `restart`
   * discontinuity also flushes what is held: it belongs to the replaced timeline.
   */
  push(frame: AudioPlayerFrame): void;
  /**
   * Drop audio already received in a timeline range, held or still decoding, as at a talker's interruption. No range
   * drops everything held, as a flush does.
   */
  cut(range?: AudioCutRange): void;
  /** The input's gain. */
  gain: number;
  /** Counters, refreshed with each report from the worklet. */
  readonly stats: Readonly<AudioInputStats>;
  /** Stop: the decoder closes, the worklet stops, and the clock goes to wall pacing. */
  remove(): void;
  /** Whether `remove()` or the player's `close()` was called. */
  readonly removed: boolean;
}

/** Plays any number of inputs into one output node. */
export interface AudioPlayer<Output extends AudioPlayerGainNode = AudioPlayerGainNode> {
  /** The context every input plays through. */
  readonly context: AudioPlayerContext;
  /**
   * The mix of every input. Connect it to the context's destination, or route it through
   * `createEchoCancellationLoopback` so Chromium's echo canceller hears it.
   */
  readonly output: Output;
  /**
   * Delay after the output that the context does not report, in microseconds, such as an echo-cancellation loopback.
   * Added to every input's audible time. Default 0.
   */
  outputDelayUs: number;
  /** Add an input. */
  addInput(options?: AudioInputOptions): AudioPlayerInput;
  /** The inputs not yet removed. */
  readonly inputs: readonly AudioPlayerInput[];
  /** Remove every input and disconnect the output. The context stays the caller's to close. */
  close(): void;
}

const DEFAULT_DECODER_CONFIG: AudioDecoderConfig = { codec: 'opus', sampleRate: 48_000, numberOfChannels: 2 };
/** Decoded timestamps closer than this to the expected one are contiguous. */
const TIMESTAMP_TOLERANCE_US = 1_000;
/** Room kept under the clock's target latency for the frame in flight and the decoder. */
const BUDGET_MARGIN_US = 40_000;

interface WorkletPort {
  onmessage: ((event: { data: AudioPlayerProcessorReport }) => void) | null;
  postMessage(message: AudioPlayerProcessorCommand, transfer?: Transferable[]): void;
  close(): void;
}
interface WorkletNode extends AudioPlayerNode {
  readonly port: WorkletPort;
}
type WorkletNodeConstructor = new (
  context: AudioPlayerContext,
  name: string,
  options: {
    numberOfInputs: number;
    numberOfOutputs: number;
    outputChannelCount: number[];
    processorOptions: AudioPlayerProcessorOptions;
  }
) => WorkletNode;

// A processor name registers once per context: a second module defining it would throw.
const loaded = new WeakMap<AudioPlayerContext, Promise<void>>();
function loadWorklet(context: AudioPlayerContext): Promise<void> {
  let loading = loaded.get(context);
  if (!loading) {
    const url = URL.createObjectURL(new Blob([AUDIO_PLAYER_WORKLET_SOURCE], { type: 'text/javascript' }));
    loading = context.audioWorklet
      .addModule(url)
      .catch((error: unknown) => {
        loaded.delete(context);
        throw error;
      })
      .finally(() => URL.revokeObjectURL(url));
    loaded.set(context, loading);
  }
  return loading;
}

function duration(value: number | undefined, fallback: number, name: string): number {
  const result = value ?? fallback;
  if (!Number.isFinite(result) || result <= 0) throw new RangeError(`${name} must be positive`);
  return result;
}

/**
 * How long an Opus packet plays, in microseconds, from its TOC byte (RFC 6716 §3.1); `undefined` when it cannot be
 * read.
 */
export function opusPacketDurationUs(packet: Uint8Array): number | undefined {
  const toc = packet[0];
  if (toc === undefined) return undefined;
  const config = toc >> 3;
  // SILK: 10, 20, 40, 60 ms; hybrid: 10, 20 ms; CELT: 2.5, 5, 10, 20 ms.
  const frameUs =
    config < 12
      ? ([10_000, 20_000, 40_000, 60_000][config % 4] as number)
      : config < 16
        ? ([10_000, 20_000][config % 2] as number)
        : ([2_500, 5_000, 10_000, 20_000][config % 4] as number);
  const code = toc & 3;
  if (code === 0) return frameUs;
  if (code !== 3) return 2 * frameUs;
  const count = packet[1];
  return count === undefined ? undefined : (count & 0x3f) * frameUs;
}

/** A linear resampler, for a context whose rate is not the decoder's. Keeps its phase across frames. */
function createResampler(channels: number) {
  // The input position of the next output sample, relative to the next frame's first sample: −1 ≤ phase < 0 means
  // between the previous frame's last sample and this one's first.
  let phase = 0;
  let previous: number[] | undefined;
  return {
    reset() {
      phase = 0;
      previous = undefined;
    },
    /** Resample one frame's planes; also returns the input offset of the first output sample, in input samples. */
    resample(planes: Float32Array[], fromRate: number, toRate: number): { planes: Float32Array[]; offset: number } {
      const length = planes[0]?.length ?? 0;
      if (fromRate === toRate || length === 0) return { planes, offset: 0 };
      const step = fromRate / toRate;
      if (previous === undefined) phase = 0;
      const offset = phase;
      const count = Math.max(0, Math.ceil((length - 1 - phase) / step));
      const out = Array.from({ length: channels }, () => new Float32Array(count));
      for (let channel = 0; channel < channels; channel++) {
        const input = planes[channel] ?? (planes[0] as Float32Array);
        const output = out[channel] as Float32Array;
        const before = previous?.[channel] ?? (input[0] as number);
        for (let index = 0; index < count; index++) {
          const at = phase + index * step;
          const whole = Math.floor(at);
          const fraction = at - whole;
          const a = whole < 0 ? before : (input[whole] as number);
          const b = input[whole + 1] as number;
          output[index] = a + (b - a) * fraction;
        }
      }
      phase = phase + count * step - length;
      previous = planes.map((plane) => plane[length - 1] as number);
      return { planes: out, offset };
    },
  };
}

/**
 * Create an audio player on `context`: it loads the jitter buffer's worklet once per context, so it resolves after
 * that. Connect `player.output` yourself.
 *
 * ```ts
 * const context = new AudioContext({ sampleRate: 48_000, latencyHint: 'interactive' });
 * const player = await createAudioPlayer({ context });
 * player.output.connect(context.destination);
 * const clock = createPlayoutClock({ targetLatencyUs: AUDIO_ONLY_TARGET_LATENCY_US });
 * const input = player.addInput({ clock, decoderConfig: audioDecoderConfigFromTrack(audioTrack) });
 * const reader = createFrameReader(createFrameSequencer({ clock, grouping: 'frame' }), { onFrame: input.push });
 * subscriber.subscribe({ namespace, track: 'audio' }, { ...reader, onError });
 * ```
 */
export async function createAudioPlayer<Context extends AudioPlayerContext>(
  options: AudioPlayerOptions<Context>
): Promise<AudioPlayer<ReturnType<Context['createGain']>>> {
  const { context } = options;
  await loadWorklet(context);
  const output = context.createGain() as ReturnType<Context['createGain']>;
  const inputs: AudioPlayerInput[] = [];
  let outputDelayUs = 0;
  let closed = false;

  const player: AudioPlayer<ReturnType<Context['createGain']>> = {
    context,
    output,
    get outputDelayUs() {
      return outputDelayUs;
    },
    set outputDelayUs(value: number) {
      if (!Number.isFinite(value) || value < 0) throw new RangeError('outputDelayUs must not be negative');
      outputDelayUs = value;
    },
    get inputs() {
      return [...inputs];
    },
    addInput(inputOptions = {}) {
      if (closed) throw new Error('The audio player is closed');
      const input = createInput(context, inputOptions, {
        connect: (node) => node.connect(output),
        outputDelayUs: () => outputDelayUs,
        removed: (removed) => inputs.splice(inputs.indexOf(removed), 1),
      });
      inputs.push(input);
      return input;
    },
    close() {
      closed = true;
      for (const input of [...inputs]) input.remove();
      output.disconnect();
    },
  };
  return player;
}

interface InputHost {
  connect(node: AudioPlayerNode): void;
  outputDelayUs(): number;
  removed(input: AudioPlayerInput): void;
}

function createInput(context: AudioPlayerContext, options: AudioInputOptions, host: InputHost): AudioPlayerInput {
  const { clock } = options;
  const decoderConfig = options.decoderConfig ?? DEFAULT_DECODER_CONFIG;
  const channels = Math.max(1, Math.min(2, decoderConfig.numberOfChannels));
  const startTargetUs = duration(options.jitterTargetUs, 60_000, 'jitterTargetUs');
  const minTargetUs = duration(options.minJitterTargetUs, 40_000, 'minJitterTargetUs');
  const maxTargetUs = duration(options.maxJitterTargetUs, 400_000, 'maxJitterTargetUs');
  if (minTargetUs > maxTargetUs) throw new RangeError('minJitterTargetUs must not exceed maxJitterTargetUs');
  const { sampleRate } = context;
  const toFrames = (us: number) => Math.round((us * sampleRate) / MICROSECONDS_PER_SECOND);
  const toUs = (frames: number) => Math.round((frames * MICROSECONDS_PER_SECOND) / sampleRate);
  const outputLatencyUs = () =>
    ((context.baseLatency ?? 0) + (context.outputLatency ?? 0)) * MICROSECONDS_PER_SECOND + host.outputDelayUs();
  // The jitter buffer may hold no more than the clock's target latency allows, less what follows it.
  const ceilingUs = () => {
    const budgetUs = clock ? clock.targetLatencyUs - outputLatencyUs() - BUDGET_MARGIN_US : maxTargetUs;
    return Math.max(minTargetUs, Math.min(maxTargetUs, budgetUs));
  };

  const Node = (globalThis as unknown as { AudioWorkletNode: WorkletNodeConstructor }).AudioWorkletNode;
  let ceiling = toFrames(ceilingUs());
  const node = new Node(context, AUDIO_PLAYER_PROCESSOR, {
    numberOfInputs: 0,
    numberOfOutputs: 1,
    outputChannelCount: [channels],
    processorOptions: {
      channels,
      targetFrames: toFrames(startTargetUs),
      minTargetFrames: toFrames(minTargetUs),
      maxTargetFrames: ceiling,
    },
  });
  const gain = context.createGain();
  gain.gain.value = options.gain ?? 1;
  node.connect(gain);
  host.connect(gain);

  const stats: AudioInputStats = {
    decoded: 0,
    late: 0,
    decoderResets: 0,
    errors: 0,
    underruns: 0,
    playing: false,
    bufferedUs: 0,
    jitterTargetUs: Math.min(startTargetUs, toUs(ceiling)),
    trimmedUs: 0,
    concealedUs: 0,
    cutUs: 0,
    reports: 0,
  };
  const renders: RenderClock = createRenderClock();
  const localNow = () => (clock ? clock.localNow() : performance.now() * 1000);
  const resampler = createResampler(channels);
  let removed = false;
  let decoder: AudioDecoder | undefined;
  // A decoder replaced at a gap or a timestamp jump. It outputs what it was given before the next decoder starts, so
  // audio decoded but not yet output still plays, in order; frames that arrive meanwhile wait.
  let retiring: AudioDecoder | undefined;
  let waiting: { timestampUs: number; payload: Uint8Array; jumps: boolean }[] = [];
  // Bumped wherever output still in a decoder belongs to audio that was dropped.
  let epoch = 0;
  // Where the next frame should start, and the end of the newest frame decoded: anything earlier arrived late.
  let expectedUs: number | undefined;
  let lastDurationUs: number | undefined;
  // Frames arriving with timestamps before this are inside a cut.
  let cutUntilUs: number | undefined;
  // Cuts that output still in the decoder may fall in: frames up to `throughUs` had been sent to it.
  let pendingCuts: { fromUs: number; untilUs: number; throughUs: number }[] = [];
  let lastSentUs: number | undefined;
  let cutOnArrivalUs = 0;
  let workletCutUs = 0;

  const post = (command: AudioPlayerProcessorCommand, transfer?: Transferable[]) => {
    if (transfer) node.port.postMessage(command, transfer);
    else node.port.postMessage(command);
  };

  const closeDecoders = () => {
    for (const each of [decoder, retiring]) if (each && each.state !== 'closed') each.close();
    decoder = undefined;
    retiring = undefined;
    waiting = [];
  };
  // Drop what the decoders still hold: it belongs to audio that was dropped, or to a decoder that failed.
  const resetDecoder = () => {
    if (decoder) stats.decoderResets++;
    closeDecoders();
    epoch++;
    pendingCuts = [];
    resampler.reset();
  };
  const openDecoder = () => {
    const opened = epoch;
    const created = new AudioDecoder({
      output(data) {
        try {
          if (removed || opened !== epoch) return;
          if (data.duration > 0) lastDurationUs = data.duration;
          const { timestamp } = data;
          pendingCuts = pendingCuts.filter((cut) => timestamp <= cut.throughUs);
          if (pendingCuts.some((cut) => timestamp >= cut.fromUs && timestamp < cut.untilUs)) {
            cutOnArrivalUs += data.duration;
            stats.cutUs = workletCutUs + cutOnArrivalUs;
            return;
          }
          const planes: Float32Array[] = [];
          const decodedChannels = Math.max(1, data.numberOfChannels);
          for (let channel = 0; channel < Math.min(channels, decodedChannels); channel++) {
            const plane = new Float32Array(data.numberOfFrames);
            data.copyTo(plane, { planeIndex: channel, format: 'f32-planar' });
            planes.push(plane);
          }
          const { planes: resampled, offset } = resampler.resample(planes, data.sampleRate, sampleRate);
          const timestampUs = data.timestamp + Math.round((offset * MICROSECONDS_PER_SECOND) / data.sampleRate);
          if (resampled[0]?.length) {
            post(
              { type: 'pcm', timestampUs, planes: resampled },
              resampled.map((plane) => plane.buffer as ArrayBuffer)
            );
          }
        } finally {
          data.close();
        }
      },
      error(error) {
        if (opened !== epoch || removed) return;
        stats.errors++;
        // A retiring decoder that fails ends its flush, and the next decoder starts.
        if (created === decoder) resetDecoder();
        options.onError?.(error);
      },
    });
    created.configure(decoderConfig);
    decoder = created;
    return created;
  };
  // Replace the decoder, as decoders count samples from their first input; let it finish what it was given first.
  const retireDecoder = (retired: AudioDecoder) => {
    stats.decoderResets++;
    decoder = undefined;
    retiring = retired;
    const opened = epoch;
    const drained = () => {
      if (retired.state !== 'closed') retired.close();
      if (removed || opened !== epoch || retiring !== retired) return;
      retiring = undefined;
      resampler.reset();
      for (const frame of waiting.splice(0)) send(frame);
    };
    try {
      retired.flush().then(drained, drained);
    } catch {
      drained();
    }
  };
  // Decode a frame, unless a retiring decoder is still finishing: then it waits for the next decoder.
  const send = (frame: { timestampUs: number; payload: Uint8Array; jumps: boolean }) => {
    if (frame.jumps && decoder) retireDecoder(decoder);
    if (retiring) {
      waiting.push({ ...frame, jumps: frame.jumps && waiting.length > 0 });
      return;
    }
    try {
      (decoder ?? openDecoder()).decode(
        new EncodedAudioChunk({ type: 'key', timestamp: frame.timestampUs, data: frame.payload })
      );
      stats.decoded++;
    } catch (error) {
      stats.errors++;
      resetDecoder();
      options.onError?.(error);
    }
  };

  const flush = () => {
    resetDecoder();
    expectedUs = undefined;
    post({ type: 'flush' });
  };

  node.port.onmessage = ({ data: report }) => {
    if (removed) return;
    const nowUs = localNow();
    renders.observe((report.rendered * MICROSECONDS_PER_SECOND) / sampleRate, nowUs);
    stats.playing = report.playing;
    stats.bufferedUs = toUs(report.bufferedFrames);
    stats.jitterTargetUs = toUs(report.targetFrames);
    stats.underruns = report.underruns;
    stats.trimmedUs = toUs(report.trimmedFrames);
    stats.concealedUs = toUs(report.concealedFrames);
    workletCutUs = toUs(report.cutFrames);
    stats.cutUs = workletCutUs + cutOnArrivalUs;
    const next = toFrames(ceilingUs());
    if (Math.abs(next - ceiling) >= toFrames(5_000)) {
      ceiling = next;
      post({ type: 'ceiling', maxTargetFrames: ceiling });
    }
    if (clock && report.timestampUs !== null) {
      // When that sample renders, on the clock's own time source, and then reaches the speaker.
      const renderedAtUs = renders.renderedAtUs((report.frame * MICROSECONDS_PER_SECOND) / sampleRate);
      if (renderedAtUs !== undefined) {
        clock.reportAudio(report.timestampUs, renderedAtUs + outputLatencyUs());
        stats.reports++;
      }
    }
  };

  const input: AudioPlayerInput = {
    push(frame) {
      if (removed) return;
      const { timestampUs, payload, discontinuity } = frame;
      if (discontinuity === 'resync' || discontinuity === 'restart') {
        flush();
        cutUntilUs = undefined;
      }
      const behind = expectedUs !== undefined && timestampUs < expectedUs - TIMESTAMP_TOLERANCE_US;
      if (behind || (cutUntilUs !== undefined && timestampUs < cutUntilUs)) {
        stats.late++;
        return;
      }
      // Decoders count samples from their first input: a new one starts wherever this frame does not follow the last.
      const jumps =
        discontinuity !== undefined ||
        (expectedUs !== undefined && Math.abs(timestampUs - expectedUs) > TIMESTAMP_TOLERANCE_US);
      const frameUs = (decoderConfig.codec === 'opus' ? opusPacketDurationUs(payload) : undefined) ?? lastDurationUs;
      expectedUs = frameUs === undefined ? undefined : timestampUs + frameUs;
      lastSentUs = timestampUs;
      send({ timestampUs, payload, jumps });
    },
    cut(range = {}) {
      if (removed) return;
      const { fromUs, untilUs } = range;
      if (fromUs === undefined && untilUs === undefined) {
        flush();
        return;
      }
      if (untilUs !== undefined) cutUntilUs = Math.max(cutUntilUs ?? untilUs, untilUs);
      // Output still in the decoder reaches the worklet after the cut does: it is checked against the cut on its way.
      if ((decoder || retiring) && lastSentUs !== undefined) {
        pendingCuts.push({
          fromUs: fromUs ?? Number.NEGATIVE_INFINITY,
          untilUs: untilUs ?? Number.POSITIVE_INFINITY,
          throughUs: lastSentUs,
        });
      }
      post({ type: 'cut', fromUs: fromUs ?? null, untilUs: untilUs ?? null });
    },
    get gain() {
      return gain.gain.value;
    },
    set gain(value: number) {
      if (!Number.isFinite(value)) throw new RangeError('gain must be finite');
      gain.gain.value = value;
    },
    get stats() {
      return stats;
    },
    get removed() {
      return removed;
    },
    remove() {
      if (removed) return;
      removed = true;
      closeDecoders();
      post({ type: 'stop' });
      node.port.onmessage = null;
      node.port.close();
      node.disconnect();
      gain.disconnect();
      clock?.stopAudio();
      host.removed(input);
    },
  };
  // A frame reader's onFrame can be the method itself.
  input.push = input.push.bind(input);
  return input;
}
