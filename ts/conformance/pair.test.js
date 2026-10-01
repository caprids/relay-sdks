/** The conformance suite against the in-memory transport pair (`spec/transport-adapter.md`, "Test doubles"). */
import { expect, it } from 'vitest';
import { createTransportPair } from '../packages/moq-core/src/tests/helpers/transport-pair.js';
import { createPairTarget } from './targets/pair.js';
import { describeConformance } from './vitest.js';

describeConformance(createPairTarget());

it('the pair still settles closed when an incoming stream queue was cancelled first', async () => {
  const pair = createTransportPair();
  await pair.client.incomingUnidirectionalStreams.cancel();
  pair.client.close({ closeCode: 1, reason: 'after cancel' });
  await expect(pair.server.closed).resolves.toEqual({ closeCode: 1, reason: 'after cancel' });
});
