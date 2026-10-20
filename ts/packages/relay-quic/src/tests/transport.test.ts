import { describe, expect, it, vi } from 'vitest';

import type { Dial, SendStream, Session } from '../addon.js';
import { NativeQuicTransport } from '../transport.js';

/** A transport over a dial that succeeds with a session whose `openUni` records the priority it was handed. */
function transport(): { transport: NativeQuicTransport; openUni: ReturnType<typeof vi.fn> } {
  const send = {
    write: async () => null,
    finish: async () => null,
    reset: async () => {},
    stopped: () => new Promise(() => {}),
  } as unknown as SendStream;
  const openUni = vi.fn(() => send);
  const pending = () => new Promise<null>(() => {});
  const session = { protocol: 'moqt-22', openUni, acceptUni: pending, acceptBi: pending, closed: pending };
  const dial = { ready: async () => session as unknown as Session, cancel: () => {} } as unknown as Dial;
  return { transport: new NativeQuicTransport(dial), openUni };
}

describe('NativeQuicTransport', () => {
  it("passes a stream's send order to the addon as quinn's stream priority (TA-28)", async () => {
    const { transport: quic, openUni } = transport();
    await quic.ready;
    await quic.createUnidirectionalStream();
    await quic.createUnidirectionalStream({});
    await quic.createUnidirectionalStream({ sendOrder: -17 });
    await quic.createUnidirectionalStream({ sendOrder: 0 });
    expect(openUni.mock.calls).toEqual([[undefined], [undefined], [-17], [0]]);
  });

  it('clamps a send order to the 32-bit priority range and drops one that is not an integer', async () => {
    const { transport: quic, openUni } = transport();
    await quic.ready;
    for (const sendOrder of [2 ** 40, -(2 ** 40), 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
      await quic.createUnidirectionalStream({ sendOrder });
    }
    expect(openUni.mock.calls).toEqual([[2 ** 31 - 1], [-(2 ** 31)], [undefined], [undefined], [undefined]]);
  });
});
