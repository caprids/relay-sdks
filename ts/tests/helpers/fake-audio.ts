/**
 * Stand-ins for the Web Audio objects the audio player and the listener example build on, for tests in Node: a context
 * whose worklet module load checks for a blob URL, and a worklet node whose port records what the player posts.
 */
import { resolveObjectURL } from 'node:buffer';
import { type Mock, vi } from 'vitest';

// Consecutive 20 ms Opus packets (SILK, mono, a 300 Hz tone), as the audio player's tests use.
export const OPUS_PACKETS = [
  'eNsq0lbYDhjw3GMFVER1GLNe/OddAMhCpLUNddotogMYjyI+Lqw7/cGB8SUyVzLYYg1p+OhrUr8GRx9EnlvinGV2IKO9G2mTqw==',
  'eNsq7WglA7LrXjVTAhZx0u/bcYjn2IQ+nZkkmbqtIvM/GUdF096ifK6pJfCiP31rkbzF9I+RIOc5eIcG43G2Lg+9G2mTqw==',
  'eNsqyzGGB/WI4/gZs/o8XsQJA0SUE8aydrKEZ/l2yDY60kCD8mNSJpMyMiKnR9ho9tnY2k1lWBhoNZrsbpXKJnPrRdc2kh1vW+mTpw==',
  'eNsqyzGGGB8x8vZN7XpxGSMf/LkvWKgFq+6+0RYrb/bGxwU3ZW65u2NIDiMUQ5vXkRTsudkJIZrXTjS8UxZOz//FKJXciG+9GxPpqw==',
].map((base64) => new Uint8Array(Buffer.from(base64, 'base64')));

export class FakeNode {
  gain = { value: 1 };
  connect: Mock = vi.fn();
  disconnect: Mock = vi.fn();
}

export class FakeWorkletNode extends FakeNode {
  static instances: FakeWorkletNode[] = [];
  port: {
    onmessage: ((event: { data: unknown }) => void) | null;
    postMessage: ReturnType<typeof vi.fn>;
    close: ReturnType<typeof vi.fn>;
  } = {
    onmessage: null,
    postMessage: vi.fn(),
    close: vi.fn(),
  };
  constructor() {
    super();
    FakeWorkletNode.instances.push(this);
  }
}

export class FakeContext {
  static instances: FakeContext[] = [];
  /** Fails the worklet module's load, from the context's construction on. */
  static moduleFailure: Error | undefined;
  readonly sampleRate = 48_000;
  readonly baseLatency = 0;
  readonly outputLatency = 0;
  destination = new FakeNode();
  close: Mock<() => Promise<void>> = vi.fn(async () => {});
  audioWorklet: { addModule: Mock<(url: string) => Promise<void>> } = {
    addModule: vi.fn(async (url: string) => {
      if (FakeContext.moduleFailure) throw FakeContext.moduleFailure;
      if (!resolveObjectURL(url)) throw new Error('Not a blob URL');
    }),
  };
  constructor() {
    FakeContext.instances.push(this);
  }
  createGain() {
    return new FakeNode();
  }
}

/** Install the fakes as the globals the player uses, each test starting with none created. */
export function stubAudioGlobals(): void {
  FakeContext.instances.length = 0;
  FakeContext.moduleFailure = undefined;
  FakeWorkletNode.instances.length = 0;
  vi.stubGlobal('AudioContext', FakeContext);
  vi.stubGlobal('AudioWorkletNode', FakeWorkletNode);
}
