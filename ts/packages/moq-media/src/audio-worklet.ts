/**
 * The audio player's jitter buffer: an `AudioWorkletProcessor` holding one input's decoded audio in a ring. It is
 * written as a function so it is type-checked, and shipped as that function's source: an `AudioWorklet` loads a
 * module by URL, and a package cannot name a file URL every bundler resolves. So the function references nothing
 * outside itself, not even a constant; the tests run the same source.
 */

/** The processor name `AUDIO_PLAYER_WORKLET_SOURCE` registers. */
export const AUDIO_PLAYER_PROCESSOR = 'caprid-audio-player-input';

/** The processor's options, in frames of the context's sample rate. */
export interface AudioPlayerProcessorOptions {
  /** Output channels. */
  channels: number;
  /** The starting jitter-buffer target, and its floor and ceiling. */
  targetFrames: number;
  minTargetFrames: number;
  maxTargetFrames: number;
  /** Held frames read per output frame. */
  playbackRate: number;
}

/** What the main thread sends. Timestamps are on the broadcast timeline, in microseconds. */
export type AudioPlayerProcessorCommand =
  | { type: 'pcm'; timestampUs: number; planes: Float32Array[] }
  /** Drop everything held. `id`, when set, is reported back as `cutId` at once (the input's `cut()`). */
  | { type: 'flush'; id?: number }
  /** Drop held audio in a range; `id` as for `flush`. */
  | { type: 'cut'; fromUs: number | null; untilUs: number | null; id?: number }
  | { type: 'ceiling'; maxTargetFrames: number }
  | { type: 'rate'; playbackRate: number }
  | { type: 'stop' };

/** What the processor reports, about every 20 ms. Counters are cumulative, in frames. */
export interface AudioPlayerProcessorReport {
  /** The context frame of the first sample output in the reporting quantum. */
  frame: number;
  /** The context frames rendered when the report was posted. */
  rendered: number;
  /** The timeline timestamp of the read position output at `frame`, or `null` when it was silence. */
  timestampUs: number | null;
  playing: boolean;
  /** Times playback started: at the start, after running dry, and after a flush or a cut that emptied the buffer. */
  playStarts: number;
  /** The timeline timestamp just past the newest held sample, or `null` when nothing is held. */
  heldUntilUs: number | null;
  /** The timeline timestamp of the read position where the buffer last ran dry while playing, until it plays again. */
  dryAtUs: number | null;
  bufferedFrames: number;
  targetFrames: number;
  underruns: number;
  trimmedFrames: number;
  concealedFrames: number;
  cutFrames: number;
  /**
   * The `id` of the latest `cut` or `flush` command that carried one, or `null` before any. The quantum after such a
   * command is reported at once, so `frame` is where the cut is first heard.
   */
  cutId: number | null;
}

declare const sampleRate: number;
declare const currentFrame: number;
declare class AudioWorkletProcessor {
  readonly port: MessagePort;
}
declare function registerProcessor(name: string, processor: unknown): void;

function audioPlayerWorklet(name: string): void {
  /** A run of samples from one decoded frame: sample `start` (a count of samples ever written) has this timestamp. */
  interface Segment {
    start: number;
    timestampUs: number;
  }

  const ms = (value: number) => Math.round((value * sampleRate) / 1000);
  /** At most this much audio is held: past it, everything held is dropped. */
  const CAPACITY = ms(4000);
  /** Timestamps closer than this to the expected one are contiguous. */
  const TOLERANCE_US = 1000;
  /** A gap inside held audio up to this long is filled with silence; a longer one is joined without it. */
  const MAX_CONCEAL = ms(1000);
  /** After an underrun the target grows this much; after `SHRINK_AFTER` with none it shrinks by `SHRINK_STEP`. */
  const GROW_STEP = ms(40);
  const SHRINK_STEP = ms(10);
  const SHRINK_AFTER = ms(2000);
  /** Each window, held audio that never dipped to the target is trimmed back to it, when the excess is this much. */
  const TRIM_WINDOW = ms(1000);
  const MIN_TRIM = ms(5);
  /** The hard bound on held audio: twice the target plus this, and never past the ceiling. */
  const HARD_HEADROOM = ms(60);
  const REPORT_EVERY = ms(20);

  class AudioPlayerInput extends AudioWorkletProcessor {
    readonly ring: Float32Array[];
    /**
     * Samples ever written and read: the ring holds `written − read` of them. The read position is `read + phase`:
     * at a playback rate other than 1, output falls between held samples, and `phase` (0 ≤ phase < 1) carries the
     * fraction across quanta and rate changes.
     */
    written = 0;
    read = 0;
    phase = 0;
    rate: number;
    segments: Segment[] = [];
    playing = false;
    target: number;
    readonly minTarget: number;
    maxTarget: number;
    /** Frames of silence output since the buffer ran dry while playing; `undefined` when it has not. */
    dry: number | undefined;
    /** Where it ran dry, until playback starts again. */
    dryAtUs: number | null = null;
    playStarts = 0;
    smooth = 0;
    window = 0;
    lowWater = Number.POSITIVE_INFINITY;
    sinceReport = 0;
    /** The latest cut's `id`, and whether the next quantum reports at once because one just came. */
    cutId: number | null = null;
    reportNow = false;
    stopped = false;
    underruns = 0;
    trimmed = 0;
    concealed = 0;
    cut = 0;
    /**
     * Cuts that no audio from their start on has arrived after yet, disjoint and in order. The hole before that audio
     * is the range cut out where it falls in a cut, and joins without silence there. Audio still decoding from before
     * a cut's start can land first, and leaves it pending.
     */
    pendingCuts: { fromUs: number; untilUs: number }[] = [];

    constructor(options: { processorOptions: AudioPlayerProcessorOptions }) {
      super();
      const { channels, targetFrames, minTargetFrames, maxTargetFrames, playbackRate } = options.processorOptions;
      this.rate = playbackRate;
      this.ring = Array.from({ length: channels }, () => new Float32Array(CAPACITY));
      this.minTarget = minTargetFrames;
      this.maxTarget = Math.max(minTargetFrames, maxTargetFrames);
      this.target = Math.min(this.maxTarget, Math.max(minTargetFrames, targetFrames));
      this.port.onmessage = (event: MessageEvent<AudioPlayerProcessorCommand>) => this.command(event.data);
    }

    get buffered() {
      return this.written - this.read;
    }

    command(command: AudioPlayerProcessorCommand) {
      if (command.type === 'pcm') this.push(command.timestampUs, command.planes);
      else if (command.type === 'flush' || command.type === 'cut') {
        if (command.type === 'flush') this.flush();
        else this.cutRange(command.fromUs, command.untilUs);
        if (command.id !== undefined) {
          this.cutId = command.id;
          this.reportNow = true;
        }
      } else if (command.type === 'ceiling') {
        this.maxTarget = Math.max(this.minTarget, command.maxTargetFrames);
        this.target = Math.min(this.target, this.maxTarget);
      } else if (command.type === 'rate') this.rate = command.playbackRate;
      else this.stopped = true;
    }

    /**
     * The timestamp of sample `index`, held or the next to be written, plus `fraction` of a sample; silence written for
     * a gap continues its run.
     */
    timestampAt(index: number, fraction = 0) {
      let segment = this.segments[0] as Segment;
      for (const candidate of this.segments) {
        if (candidate.start > index) break;
        segment = candidate;
      }
      return segment.timestampUs + Math.round(((index + fraction - segment.start) * 1_000_000) / sampleRate);
    }

    /** The index of the first held sample whose timestamp is at or after `timestampUs`; `written` when none is. */
    indexOf(timestampUs: number) {
      for (let at = 0; at < this.segments.length; at++) {
        const segment = this.segments[at] as Segment;
        const end = (this.segments[at + 1] as Segment | undefined)?.start ?? this.written;
        const offset = Math.max(0, Math.ceil(((timestampUs - segment.timestampUs) * sampleRate) / 1_000_000));
        const index = Math.max(this.read, segment.start + offset);
        if (index < end) return index;
      }
      return this.written;
    }

    write(source: (channel: number) => Float32Array | undefined, frames: number) {
      for (let channel = 0; channel < this.ring.length; channel++) {
        const ring = this.ring[channel] as Float32Array;
        const plane = source(channel);
        for (let offset = 0; offset < frames; offset++) {
          ring[(this.written + offset) % CAPACITY] = plane ? (plane[offset] ?? 0) : 0;
        }
      }
      this.written += frames;
    }

    push(timestampUs: number, planes: Float32Array[]) {
      const frames = planes[0]?.length ?? 0;
      if (frames === 0) return;
      if (this.dry !== undefined) {
        // Audio came back after the buffer ran dry. Soon enough, and a deeper buffer would have covered it; later, it
        // was the sender's silence, which no buffer covers.
        if (this.dry <= this.maxTarget) {
          this.underruns++;
          this.smooth = 0;
          this.target = Math.min(this.maxTarget, this.target + GROW_STEP);
        }
        this.dry = undefined;
      }
      const expectedUs = this.buffered > 0 ? this.timestampAt(this.written) : undefined;
      // The cuts this audio is past: the part of the hole before it that they cover was cut out, not lost.
      let cutOutUs = 0;
      this.pendingCuts = this.pendingCuts.filter((cut) => {
        if (cut.fromUs > timestampUs) return true;
        if (expectedUs !== undefined) {
          cutOutUs += Math.max(0, Math.min(timestampUs, cut.untilUs) - Math.max(expectedUs, cut.fromUs));
        }
        return false;
      });
      if (expectedUs !== undefined) {
        // A frame that never came, inside held audio: silence keeps the rest on time.
        const lostUs = timestampUs - expectedUs - cutOutUs;
        const gap = Math.round((lostUs * sampleRate) / 1_000_000);
        if (lostUs > TOLERANCE_US && gap <= MAX_CONCEAL && this.buffered + gap + frames <= CAPACITY) {
          this.write(() => undefined, gap);
          this.concealed += gap;
        }
      }
      if (this.buffered + frames > CAPACITY) this.dropTo(this.written, true);
      this.segments.push({ start: this.written, timestampUs });
      this.write((channel) => planes[channel] ?? planes[0], frames);
    }

    /** Drop everything held, and any cut still pending: what comes next starts a new timeline. */
    flush() {
      this.pendingCuts = [];
      this.dropTo(this.written, false);
      // Already empty, as after running dry: still forget the replaced timeline's dry point.
      this.emptied();
    }

    /** Drop held samples before `index`: as trimmed (latency shed) or as a flush. */
    dropTo(index: number, trim: boolean) {
      const count = index - this.read;
      if (count <= 0) return;
      if (trim) this.trimmed += count;
      this.read = index;
      this.prune();
      if (this.buffered === 0) this.emptied();
    }

    /** Nothing is held because it was dropped, not played: fill to the target again, and count no underrun. */
    emptied() {
      this.playing = false;
      this.dry = undefined;
      this.dryAtUs = null;
      this.phase = 0;
      this.segments = [];
    }

    prune() {
      while (this.segments.length > 1 && (this.segments[1] as Segment).start <= this.read) this.segments.shift();
    }

    /** Remove held samples whose timestamps fall in [fromUs, untilUs). */
    cutRange(fromUs: number | null, untilUs: number | null) {
      // Audio still decoding lands after the cut, held audio or not: never fill the range with silence. Overlapping
      // cuts merge, so no part of a hole counts twice.
      const cuts = [
        ...this.pendingCuts,
        { fromUs: fromUs ?? Number.NEGATIVE_INFINITY, untilUs: untilUs ?? Number.POSITIVE_INFINITY },
      ].sort((a, b) => (a.fromUs < b.fromUs ? -1 : a.fromUs > b.fromUs ? 1 : 0));
      this.pendingCuts = [];
      for (const cut of cuts) {
        const last = this.pendingCuts[this.pendingCuts.length - 1];
        if (last && cut.fromUs <= last.untilUs) last.untilUs = Math.max(last.untilUs, cut.untilUs);
        else this.pendingCuts.push({ ...cut });
      }
      if (this.buffered === 0) return;

      const start = fromUs === null ? this.read : this.indexOf(fromUs);
      const end = untilUs === null ? this.written : this.indexOf(untilUs);
      if (end <= start) return;
      const count = end - start;
      this.cut += count;
      const endTimestampUs = end < this.written ? this.timestampAt(end) : undefined;
      // Move what follows the cut back over it.
      for (let channel = 0; channel < this.ring.length; channel++) {
        const ring = this.ring[channel] as Float32Array;
        for (let index = end; index < this.written; index++) {
          ring[(index - count) % CAPACITY] = ring[index % CAPACITY] as number;
        }
      }
      const kept = this.segments.filter((segment) => segment.start < start);
      if (endTimestampUs !== undefined) kept.push({ start, timestampUs: endTimestampUs });
      for (const segment of this.segments) {
        if (segment.start > end) kept.push({ start: segment.start - count, timestampUs: segment.timestampUs });
      }
      this.segments = kept;
      this.written -= count;
      if (this.buffered === 0) this.emptied();
    }

    process(_inputs: Float32Array[][], outputs: Float32Array[][]): boolean {
      if (this.stopped) return false;
      const output = outputs[0] ?? [];
      const frames = output[0]?.length ?? 128;
      if (!this.playing && this.buffered >= this.target) {
        this.playing = true;
        this.playStarts++;
        this.dryAtUs = null;
        this.lowWater = this.buffered;
        this.window = 0;
      }
      let timestampUs: number | null = null;
      if (this.playing) {
        const hard = Math.max(this.target, Math.min(2 * this.target + HARD_HEADROOM, this.maxTarget));
        if (this.buffered > hard) this.dropTo(this.written - hard, true);
      }
      if (this.playing) {
        // Output `offset` reads position `phase + offset × rate` past `read`, between two held samples: as many as
        // stay at or before the newest one.
        const { rate, phase } = this;
        const last = this.buffered - 1 - phase;
        const available = last < 0 ? 0 : Math.min(frames, Math.floor(last / rate) + 1);
        timestampUs = this.timestampAt(this.read, phase);
        for (let channel = 0; channel < output.length; channel++) {
          const ring = this.ring[Math.min(channel, this.ring.length - 1)] as Float32Array;
          const out = output[channel] as Float32Array;
          if (rate === 1 && phase === 0) {
            for (let offset = 0; offset < available; offset++)
              out[offset] = ring[(this.read + offset) % CAPACITY] as number;
          } else {
            for (let offset = 0; offset < available; offset++) {
              const at = phase + offset * rate;
              const whole = Math.floor(at);
              const fraction = at - whole;
              const a = ring[(this.read + whole) % CAPACITY] as number;
              const b = fraction === 0 ? a : (ring[(this.read + whole + 1) % CAPACITY] as number);
              out[offset] = a + (b - a) * fraction;
            }
          }
          out.fill(0, available);
        }
        // Rate 2 can step one sample past the newest: the position stops there.
        const advance = phase + available * rate;
        const whole = Math.min(Math.floor(advance), this.buffered);
        this.read += whole;
        this.phase = whole === Math.floor(advance) ? advance - whole : 0;
        if (this.buffered === 0) this.phase = 0;
        this.prune();
        if (available < frames) {
          // Ran dry: wait for the target again. Whether it counts as an underrun is known when audio comes back. Off rate
          // 1, less than a sample can be left, past the read position: it was played, as at rate 1.
          this.playing = false;
          this.dry = frames - available;
          this.read = this.written;
          this.phase = 0;
          this.dryAtUs = this.timestampAt(this.read);
          this.segments = [];
        } else {
          this.lowWater = Math.min(this.lowWater, this.buffered);
          this.smooth += frames;
          this.window += frames;
          if (this.window >= TRIM_WINDOW) {
            // Held audio that never dipped to the target over a whole window is delay nobody needed.
            // Not below rate 1, which gains that delay on purpose: only the hard bound limits it then.
            const excess = this.lowWater - this.target;
            if (excess >= MIN_TRIM && this.rate >= 1) this.dropTo(this.read + excess, true);
            this.window = 0;
            this.lowWater = this.buffered;
          }
          if (this.smooth >= SHRINK_AFTER) {
            this.target = Math.max(this.minTarget, this.target - SHRINK_STEP);
            this.smooth = 0;
          }
        }
      } else {
        for (const out of output) out.fill(0);
        if (this.dry !== undefined) this.dry += frames;
      }
      this.sinceReport += frames;
      if (this.sinceReport >= REPORT_EVERY || this.reportNow) {
        this.sinceReport = 0;
        this.reportNow = false;
        const report: AudioPlayerProcessorReport = {
          frame: currentFrame,
          rendered: currentFrame + frames,
          timestampUs,
          playing: this.playing,
          playStarts: this.playStarts,
          heldUntilUs: this.buffered > 0 ? this.timestampAt(this.written) : null,
          dryAtUs: this.dryAtUs,
          bufferedFrames: this.buffered,
          targetFrames: this.target,
          underruns: this.underruns,
          trimmedFrames: this.trimmed,
          concealedFrames: this.concealed,
          cutFrames: this.cut,
          cutId: this.cutId,
        };
        this.port.postMessage(report);
      }
      return true;
    }
  }

  registerProcessor(name, AudioPlayerInput);
}

/** The worklet module's source. */
export const AUDIO_PLAYER_WORKLET_SOURCE = `(${audioPlayerWorklet.toString()})(${JSON.stringify(AUDIO_PLAYER_PROCESSOR)});`;
