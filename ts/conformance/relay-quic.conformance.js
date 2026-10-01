/**
 * The conformance suite against `@mux/relay-quic`, the Node transport adapter over native QUIC, with the Python
 * accepting end as its peer (`--framing native-quic`). Not part of `pnpm test`: it needs the built addon and the Python
 * workspace, so `pnpm test:conformance:node` runs it, with `vitest.relay-quic.config.js`.
 */
import { nativeQuicTransport } from '@mux/relay-quic';
import { afterAll, describe, expect, it } from 'vitest';
import { startConformancePeer } from '../scripts/conformance-peer.js';
import { createRelayQuicTarget } from './targets/relay-quic.js';
import { describeConformance } from './vitest.js';

const peer = await startConformancePeer({ framing: 'native-quic' });
afterAll(() => peer.stop());

const target = createRelayQuicTarget(peer);
describeConformance(target);

it('TA-11 a dial cancelled before ready leaves nothing that keeps the process alive', async () => {
  await peer.call('prepare', { handshake: 'hold' });
  await target.checkProcessLifetime('exits-after-cancel');
}, 20_000);

describe('@mux/relay-quic trust', () => {
  it('trusts a ca anchor added to the platform roots', async () => {
    await peer.call('prepare');
    const { transport, ready } = nativeQuicTransport({ ca: peer.certificatePem })(`https://localhost:${peer.port}/`, [
      'moqt-20',
    ]);
    transport.closed.catch(() => {});
    await ready;
    transport.close();
    await expect(transport.closed).resolves.toEqual({ closeCode: 0, reason: '' });
  });

  it('fails the dial with a session error when the certificate is not trusted', async () => {
    await peer.call('prepare');
    const { transport, ready } = nativeQuicTransport()(`https://localhost:${peer.port}/`, ['moqt-20']);
    transport.closed.catch(() => {});
    await expect(ready).rejects.toMatchObject({ source: 'session' });
    await expect(transport.closed).rejects.toMatchObject({ source: 'session' });
  });

  it('throws at once for a ca with no certificate or a hash of the wrong length', () => {
    expect(() => nativeQuicTransport({ ca: 'not a certificate' })).toThrow(/ca/);
    expect(() =>
      nativeQuicTransport({ serverCertificateHashes: [{ algorithm: 'sha-256', value: new Uint8Array(16) }] }),
    ).toThrow(/32-byte/);
  });
});
