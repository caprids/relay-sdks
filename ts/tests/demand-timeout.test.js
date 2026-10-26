/** `writeWithRetry` over a fake track writer: what it retries, what it never resends, what it reports. */
import { expect, it, vi } from 'vitest';
import { isDemandTimeout, writeWithRetry } from '../examples/shared/demand-timeout.js';
import { RelayError } from '../src/index.js';
import { createWriteSlot } from '../src/write-slot.js';

// The policy imports the SDK by its package name, as the demo does; everything is real.
vi.mock('@caprid/relay', () => vi.importActual('../src/index.js'));

const payload = new TextEncoder().encode('{"seq":1}');

const timeout = (phase) => new RelayError('WRITE_TIMEOUT', 'Publish write timed out', phase);

/**
 * Enough of a track writer: each `write()` takes the next scripted step. A step is an error to
 * throw before sending, `{ sent: error }` to take the bytes and then fail, or `'wait'` to wait for
 * demand until the write's signal aborts. With no step left, the write succeeds.
 */
class FakeTrackWriter {
  constructor(...steps) {
    this.steps = steps;
    this.attempts = 0;
    this.writes = [];
  }

  async write(bytes, { signal } = {}) {
    this.attempts += 1;
    const step = this.steps.shift();
    if (step === 'wait') {
      await new Promise((_, reject) => signal?.addEventListener('abort', () => reject(signal.reason), { once: true }));
    }
    if (step instanceof Error) throw step;
    this.writes.push(bytes);
    if (step?.sent) throw step.sent;
  }
}

function recorder() {
  const retries = [];
  const uncertain = [];
  return {
    retries,
    uncertain,
    onRetry: (attempt, error) => retries.push([attempt, error]),
    onUncertain: (error) => uncertain.push(error),
  };
}

it('retries demand timeouts until the write goes through', async () => {
  const first = timeout('waiting-for-demand');
  const second = timeout('waiting-for-demand');
  const writer = new FakeTrackWriter(first, second);
  const seen = recorder();

  await writeWithRetry(writer, payload, seen);

  expect(writer.attempts).toBe(3);
  expect(writer.writes).toEqual([payload]);
  expect(seen.retries).toEqual([
    [1, first],
    [2, second],
  ]);
  expect(seen.uncertain).toEqual([]);
});

it.each(['opening-stream', 'writing', 'closing'])('never resends after a timeout in phase %s', async (phase) => {
  const failure = timeout(phase);
  const writer = new FakeTrackWriter(failure);
  const seen = recorder();

  await expect(writeWithRetry(writer, payload, seen)).rejects.toBe(failure);

  expect(writer.attempts).toBe(1);
  expect(seen.retries).toEqual([]);
  expect(seen.uncertain).toEqual([failure]);
});

it('attempts a write that timed out after partial progress once and rejects with its error', async () => {
  const failure = timeout('closing');
  const writer = new FakeTrackWriter({ sent: failure });
  const seen = recorder();

  await expect(writeWithRetry(writer, payload, seen)).rejects.toBe(failure);

  expect(writer.writes).toEqual([payload]); // The relay holds the bytes; they are not sent again.
  expect(writer.attempts).toBe(1);
  expect(seen.uncertain).toEqual([failure]);
  expect(seen.retries).toEqual([]);
});

it.each([
  new RelayError('CLOSED', 'Published track is closed', 'waiting-for-demand'),
  new RelayError('NAMESPACE_NOT_SOLICITED', 'not covered', 'waiting-for-demand'),
])('rejects other failures before sending without retrying ($code)', async (failure) => {
  const writer = new FakeTrackWriter(failure);
  const seen = recorder();

  await expect(writeWithRetry(writer, payload, seen)).rejects.toBe(failure);

  expect(writer.attempts).toBe(1);
  expect(seen.retries).toEqual([]);
  expect(seen.uncertain).toEqual([]);
});

it('rejects a write on an ended track without retrying or reporting uncertain delivery', async () => {
  // The SDK's own track writer, not a fake: a refusal that sent nothing must not look like a sending phase.
  const openSubgroup = vi.fn();
  const { writer } = createWriteSlot({
    openSubgroup,
    namespaceSolicited: () => undefined,
    assertOpen() {},
    endTrack() {},
    writeTimeoutMs: 1_000,
  });
  writer.close();
  const seen = recorder();

  await expect(writeWithRetry(writer, payload, seen)).rejects.toMatchObject({
    code: 'CLOSED',
    phase: 'waiting-for-demand',
  });

  expect(openSubgroup).not.toHaveBeenCalled();
  expect(seen.retries).toEqual([]);
  expect(seen.uncertain).toEqual([]);
});

it('branches on code and phase, never on the message', async () => {
  // The demand-timeout message, but a transport phase: bytes may have left.
  const failure = new RelayError('WRITE_TIMEOUT', 'Publish write timed out waiting for relay demand', 'writing');
  const writer = new FakeTrackWriter(failure);

  await expect(writeWithRetry(writer, payload)).rejects.toBe(failure);

  expect(writer.attempts).toBe(1);
  expect(isDemandTimeout(new Error('timed out waiting for relay demand'))).toBe(false);
  expect(isDemandTimeout(new RelayError('WRITE_TIMEOUT', 'anything', 'waiting-for-demand'))).toBe(true);
});

it('ends a wait promptly when the signal aborts, and writes no further', async () => {
  const writer = new FakeTrackWriter(timeout('waiting-for-demand'), 'wait');
  const seen = recorder();
  const reason = new Error('Run exceeded 20 ms');

  await expect(writeWithRetry(writer, payload, { ...seen, signal: AbortSignal.timeout(20) })).rejects.toThrow();
  await expect(writeWithRetry(new FakeTrackWriter(), payload, { signal: AbortSignal.abort(reason) })).rejects.toBe(
    reason,
  );

  expect(writer.attempts).toBe(2);
  expect(writer.writes).toEqual([]);
  expect(seen.retries).toHaveLength(1);
});

it('stops retrying when onRetry throws', async () => {
  const writer = new FakeTrackWriter(timeout('waiting-for-demand'));
  const stop = new Error('subscribe rejected');

  await expect(
    writeWithRetry(writer, payload, {
      onRetry: () => {
        throw stop;
      },
    }),
  ).rejects.toBe(stop);

  expect(writer.attempts).toBe(1);
});
