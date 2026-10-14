import type { BroadcastClock, SourceTimeline } from '@mux/moq-media';
import { createRenderClock, type RenderClock } from './render-clock.js';

const PROCESSOR = 'relay-sdk-microphone-capture';

// Runs on the audio rendering thread. It cuts the (down-mixed, mono) input into fixed blocks and posts each with the
// context frame of its first sample, plus the context frame rendered so far: the main thread times that arrival. With
// no microphone connected the input has no channels, and the block is silence, so the count never stalls. Any message
// from the main thread stops it: process() always returns true otherwise, which would keep it running. A block
// holds only contiguous frames: where currentFrame is not the frame expected next, the partial block is dropped and a
// new one starts. Chromium (147) reports currentFrame 0 on a new node's first process() call, then the real frame.
const WORKLET_SOURCE = `
class MicrophoneCapture extends AudioWorkletProcessor {
  constructor(options) {
    super();
    this.size = options.processorOptions.blockFrames;
    this.block = new Float32Array(this.size);
    this.filled = 0;
    this.first = 0;
    this.next = -1;
    this.stopped = false;
    this.port.onmessage = () => {
      this.stopped = true;
    };
  }
  process(inputs) {
    // Returning false lets the engine drop a processor whose capture has closed.
    if (this.stopped) return false;
    const channel = inputs[0] && inputs[0][0];
    const frames = channel ? channel.length : 128;
    if (currentFrame !== this.next) this.filled = 0;
    this.next = currentFrame + frames;
    for (let offset = 0; offset < frames; ) {
      if (this.filled === 0) this.first = currentFrame + offset;
      const count = Math.min(frames - offset, this.size - this.filled);
      if (channel) this.block.set(channel.subarray(offset, offset + count), this.filled);
      else this.block.fill(0, this.filled, this.filled + count);
      offset += count;
      this.filled += count;
      if (this.filled === this.size) {
        const samples = this.block;
        this.port.postMessage({ frame: this.first, samples, rendered: currentFrame + frames }, [samples.buffer]);
        this.block = new Float32Array(this.size);
        this.filled = 0;
      }
    }
    return true;
  }
}
registerProcessor('${PROCESSOR}', MicrophoneCapture);
`;

// A processor name registers once per context: a second module defining it would throw.
const loaded = new WeakMap<BaseAudioContext, Promise<void>>();
function loadWorklet(context: BaseAudioContext): Promise<void> {
  let loading = loaded.get(context);
  if (!loading) {
    const url = URL.createObjectURL(new Blob([WORKLET_SOURCE], { type: 'text/javascript' }));
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

/** One block of microphone samples, mono, timed on one `AudioContext`'s clock. */
export interface MicrophoneBlock {
  /** The samples. */
  samples: Float32Array<ArrayBuffer>;
  /** The context's sample rate. */
  sampleRate: number;
  /** The context time of the first sample, in whole microseconds: the encoder's input timestamp. */
  contextUs: number;
  /** Maps this context's time onto the broadcast timeline. Each context has its own, so it changes with the context. */
  timeline: SourceTimeline;
  /**
   * The capture latency of the microphone these samples came from, by the context frame each microphone was connected
   * at. The timeline's value moves on at a device switch, while blocks captured before it may still be in flight.
   */
  captureLatencyUs: number;
}

/** A microphone routed through an `AudioContext` and an `AudioWorklet`, timed on a broadcast clock. */
export interface MicrophoneCapture {
  /** The broadcast clock the timelines map onto. Stamp every other track of the broadcast from it too. */
  readonly clock: BroadcastClock;
  /** The context capture runs on now. */
  readonly context: AudioContext;
  /**
   * Capture another microphone in the same context, or none (silence). The context resamples whatever the device's
   * rate, and its count runs on through the switch.
   */
  useStream(stream: MediaStream | undefined): void;
  /**
   * Move capture to another context, as when the application rebuilds its audio graph. The new context's time starts
   * over, so it gets a timeline of its own. The old context stays the caller's to close.
   */
  useContext(context: AudioContext): Promise<void>;
  /** Receives every block. `publishCanvas` sets it. */
  onblock: ((block: MicrophoneBlock) => void) | undefined;
  /** Stop capturing. The contexts stay the caller's to close. */
  close(): void;
}

/**
 * A rough capture latency for the microphone in `stream`, in microseconds, from its label: 190 ms for a Bluetooth
 * headset, 30 ms for any other device, and 0 without a labelled audio track (silence, or a synthetic source). The
 * figures are the macOS measurements in spec/media-layer.md, "Capture latency"; no browser API reports the real value.
 */
export function estimateCaptureLatencyUs(stream: MediaStream | undefined): number {
  const label = stream?.getAudioTracks()[0]?.label ?? '';
  if (!label) return 0;
  return /airpods|bluetooth/i.test(label) ? 190_000 : 30_000;
}

/** Options for {@link captureMicrophone}. */
export interface CaptureMicrophoneOptions {
  /** The context to capture in. Default: a new 48 kHz context, the rate Opus encodes at. */
  context?: AudioContext;
  /** Block length in milliseconds; each block is one encoder input. Default 20, one Opus frame. */
  blockMs?: number;
  /**
   * The capture latency of the microphone in a stream, in integer microseconds, subtracted from every timestamp.
   * Asked again whenever the stream changes. Default {@link estimateCaptureLatencyUs}.
   */
  captureLatencyUs?: (stream: MediaStream | undefined) => number;
}

interface Leg {
  context: AudioContext;
  node: AudioWorkletNode;
  source: MediaStreamAudioSourceNode | undefined;
  renders: RenderClock;
  timeline: SourceTimeline;
  /** Each microphone's capture latency, from the context frame it was connected at; oldest first. */
  latencies: { fromFrame: number; latencyUs: number }[];
}

/**
 * Capture `stream` through an `AudioContext`: the recipe in spec/media-layer.md, "Browser capture", which works in
 * every engine because it needs no `MediaStreamTrackProcessor`. Each block's samples are stamped with context time
 * and mapped onto `clock`'s timeline through a source timeline, calibrated on every block from the render clock: when
 * the worklet saw fresh input, on the broadcast clock's own source.
 *
 * The context must be running: create or resume it from a user gesture.
 */
export async function captureMicrophone(
  clock: BroadcastClock,
  stream: MediaStream | undefined,
  options: CaptureMicrophoneOptions = {},
): Promise<MicrophoneCapture> {
  const blockMs = options.blockMs ?? 20;
  const latencyOf = options.captureLatencyUs ?? estimateCaptureLatencyUs;
  // The broadcast clock's own source, so calibration pairs and timestamps share one time base.
  const sourceNowUs = () => clock.now() - clock.originEpochUs + clock.originUs;
  let current = stream;
  let closed = false;
  let leg: Leg | undefined;

  const connect = (target: Leg) => {
    const latencyUs = latencyOf(current);
    target.timeline.captureLatencyUs = latencyUs;
    // Blocks rendered before this frame came from the previous microphone, and some may still be queued on the port.
    const { currentTime = 0, sampleRate } = target.context;
    // currentTime is a whole frame count over the rate; rounding undoes the float error (0.28 × 48000 = 13440.000…2).
    target.latencies.push({ fromFrame: Math.round(currentTime * sampleRate), latencyUs });
    target.source?.disconnect();
    target.source = current ? target.context.createMediaStreamSource(current) : undefined;
    target.source?.connect(target.node);
  };
  const open = async (context: AudioContext): Promise<Leg> => {
    await loadWorklet(context);
    const blockFrames = Math.round((context.sampleRate * blockMs) / 1000);
    const node = new AudioWorkletNode(context, PROCESSOR, {
      numberOfInputs: 1,
      numberOfOutputs: 1,
      outputChannelCount: [1],
      channelCount: 1,
      channelCountMode: 'explicit',
      channelInterpretation: 'speakers',
      processorOptions: { blockFrames },
    });
    const opened: Leg = {
      context,
      node,
      source: undefined,
      renders: createRenderClock(),
      timeline: clock.sourceTimeline(),
      latencies: [],
    };
    node.port.onmessage = ({
      data,
    }: MessageEvent<{ frame: number; samples: Float32Array<ArrayBuffer>; rendered: number }>) => {
      const { sampleRate } = context;
      opened.renders.observe((data.rendered * 1_000_000) / sampleRate, sourceNowUs());
      const best = opened.renders.best();
      // Re-anchors only when the best pair disagrees with the anchor by more than the tolerance.
      if (best) opened.timeline.calibrate(best.contextUs, best.atUs);
      const { latencies } = opened;
      while (latencies.length > 1 && (latencies[1] as (typeof latencies)[number]).fromFrame <= data.frame) {
        latencies.shift();
      }
      capture.onblock?.({
        samples: data.samples,
        sampleRate,
        contextUs: Math.round((data.frame * 1_000_000) / sampleRate),
        timeline: opened.timeline,
        captureLatencyUs: latencies[0]?.latencyUs ?? opened.timeline.captureLatencyUs,
      });
    };
    // Outputs are silent; connecting one keeps every engine pulling the node.
    node.connect(context.destination);
    return opened;
  };
  const close = (target: Leg) => {
    target.node.port.onmessage = null;
    target.node.port.postMessage('stop');
    target.node.port.close();
    target.source?.disconnect();
    target.node.disconnect();
  };

  const capture: MicrophoneCapture = {
    clock,
    get context() {
      return (leg as Leg).context;
    },
    onblock: undefined,
    useStream(next) {
      if (closed) throw new Error('Microphone capture is closed');
      current = next;
      if (leg) connect(leg);
    },
    async useContext(context) {
      if (closed) throw new Error('Microphone capture is closed');
      const next = await open(context);
      if (closed) {
        close(next);
        return;
      }
      try {
        connect(next);
      } catch (error) {
        close(next);
        throw error;
      }
      const previous = leg;
      leg = next;
      if (previous) close(previous);
    },
    close() {
      closed = true;
      if (leg) close(leg);
    },
  };
  await capture.useContext(options.context ?? new AudioContext({ sampleRate: 48_000 }));
  return capture;
}
