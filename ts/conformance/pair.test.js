/** The conformance suite against the in-memory transport pair (`spec/transport-adapter.md`, "Test doubles"). */
import { expect, it } from 'vitest';
import {
  createTransportPair,
  createTransportPairFactory,
} from '../packages/moq-core/src/tests/helpers/transport-pair.js';
import { createPairTarget } from './targets/pair.js';
import { describeConformance } from './vitest.js';

describeConformance(createPairTarget());

it('the pair still settles closed when an incoming stream queue was cancelled first', async () => {
  const pair = createTransportPair();
  await pair.client.incomingUnidirectionalStreams.cancel();
  pair.client.close({ closeCode: 1, reason: 'after cancel' });
  await expect(pair.server.closed).resolves.toEqual({ closeCode: 1, reason: 'after cancel' });
});

it('the pair fails a write released by a read in the same turn the session ends', async () => {
  const pair = createTransportPair({ bufferBytes: 0 });
  const writer = (await pair.client.createUnidirectionalStream()).getWriter();
  const reader = (await pair.server.incomingUnidirectionalStreams.getReader().read()).value.getReader();
  const write = writer.write(new Uint8Array([1]));
  await new Promise((resolve) => setTimeout(resolve, 0));
  void reader.read().catch(() => {});
  pair.fail('lost');
  await expect(write).rejects.toMatchObject({ source: 'session' });
});

it('the pair rejects ready when the session ends while the dial is held', async () => {
  const factory = createTransportPairFactory({ handshake: 'hold' });
  const { ready } = factory.createTransport('https://pair.invalid/', ['moqt-20']);
  (await factory.dialled).pair.fail('lost');
  await expect(ready).rejects.toMatchObject({ source: 'session' });
  factory.releaseHandshake();
  await expect(ready).rejects.toMatchObject({ source: 'session' });
});

it('the pair rejects ready when the session ends in the same turn the held handshake is released', async () => {
  const factory = createTransportPairFactory({ handshake: 'hold' });
  const { ready } = factory.createTransport('https://pair.invalid/', ['moqt-20']);
  (await factory.dialled).pair.fail('lost');
  factory.releaseHandshake();
  await expect(ready).rejects.toMatchObject({ source: 'session' });
});
