import { createFrameHold } from '@caprid/moq-media';
import { describe, expect, it } from 'vitest';

describe('createFrameHold', () => {
  it('keeps frames in order until released, then passes them and every later one straight on', () => {
    const hold = createFrameHold<number>();
    const seen: number[] = [];
    hold.push(1);
    hold.push(2);
    expect(hold.stats).toEqual({ held: 2, maxHeld: 2, released: 0 });
    expect(hold.isReleased).toBe(false);
    hold.release((frame) => seen.push(frame));
    expect(seen).toEqual([1, 2]);
    expect(hold.isReleased).toBe(true);
    // The method works detached, as a frame reader's onFrame.
    const { push } = hold;
    push(3);
    expect(seen).toEqual([1, 2, 3]);
    expect(hold.stats).toEqual({ held: 0, maxHeld: 2, released: 2 });
  });

  it('puts a frame pushed while the kept ones are passed on behind them', () => {
    const hold = createFrameHold<number>();
    const seen: number[] = [];
    hold.push(1);
    hold.push(2);
    hold.release((frame) => {
      seen.push(frame);
      if (frame === 1) hold.push(3);
    });
    hold.push(4);
    expect(seen).toEqual([1, 2, 3, 4]);
  });

  it('drops what it keeps at clear, releases once only, and passes nothing when nothing was kept', () => {
    const hold = createFrameHold<string>();
    hold.push('a');
    hold.clear();
    expect(hold.stats.held).toBe(0);
    hold.push('b');
    const seen: string[] = [];
    hold.release((frame) => seen.push(frame));
    expect(seen).toEqual(['b']);
    expect(() => hold.release(() => {})).toThrow(/already released/);
    hold.clear();
    hold.push('c');
    expect(seen).toEqual(['b', 'c']);
    const empty = createFrameHold<string>();
    empty.release((frame) => seen.push(frame));
    expect(seen).toEqual(['b', 'c']);
  });

  it('passes on every kept frame when the consumer throws on one, then throws the first error', () => {
    const hold = createFrameHold<number>();
    const seen: number[] = [];
    for (const frame of [1, 2, 3]) hold.push(frame);
    expect(() =>
      hold.release((frame) => {
        seen.push(frame);
        if (frame < 3) throw new Error(`frame ${frame}`);
      }),
    ).toThrow('frame 1');
    expect(seen).toEqual([1, 2, 3]);
    expect(hold.stats).toEqual({ held: 0, maxHeld: 3, released: 3 });
    expect(hold.isReleased).toBe(true);
    hold.push(4);
    expect(seen).toEqual([1, 2, 3, 4]);
  });
});
