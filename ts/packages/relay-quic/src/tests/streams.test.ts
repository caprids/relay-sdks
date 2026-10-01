import { describe, expect, it } from 'vitest';

import type { RecvStream, SendStream } from '../addon.js';
import { recvReadable, sendWritable } from '../streams.js';

/** A send half that records what each write was handed, and never resolves its stop watch. */
function fakeSend(): { send: SendStream; written: Uint8Array[] } {
  const written: Uint8Array[] = [];
  const send = {
    write: async (data: Uint8Array) => {
      written.push(data);
      return null;
    },
    finish: async () => null,
    reset: async () => {},
    stopped: () => new Promise(() => {}),
  } as unknown as SendStream;
  return { send, written };
}

describe('sendWritable', () => {
  it('hands the addon a copy of each chunk, which the caller may then detach', async () => {
    const { send, written } = fakeSend();
    const writer = sendWritable(send).getWriter();
    const chunk = new Uint8Array([1, 2, 3]);
    await writer.write(chunk);
    const original = chunk.buffer;
    structuredClone(original, { transfer: [original] });
    expect(chunk.byteLength).toBe(0);
    expect(written[0]?.buffer === original).toBe(false);
    expect([...(written[0] ?? [])]).toEqual([1, 2, 3]);
  });

  it('copies a Node Buffer too, whose slice() would share its memory', async () => {
    const { send, written } = fakeSend();
    const writer = sendWritable(send).getWriter();
    const chunk = Buffer.from([4, 5, 6]);
    await writer.write(chunk);
    chunk.fill(0);
    expect([...(written[0] ?? [])]).toEqual([4, 5, 6]);
  });
});

describe('recvReadable (TA-19)', () => {
  it('errors with the peer reset while nothing reads, without waiting for a pull', async () => {
    let reset: (failure: unknown) => void = () => {};
    const recv = {
      read: async () => ({ chunk: Buffer.from('x'), fin: false }),
      watch: () =>
        new Promise((resolve) => {
          reset = resolve;
        }),
      stop: async () => {},
    } as unknown as RecvStream;
    const reader = recvReadable(recv).getReader();
    expect((await reader.read()).value).toEqual(new Uint8Array([120]));
    // Nothing reads now; the watch the read left behind carries the reset.
    reset({ source: 'stream', kind: 'reset', code: '9', reason: '' });
    await expect(reader.closed).rejects.toMatchObject({ source: 'stream', streamErrorCode: 9 });
  });
});
