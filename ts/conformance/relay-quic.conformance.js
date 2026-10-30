/**
 * The conformance suite against `@caprid/relay-quic`, the Node transport adapter over native QUIC, with the Python
 * accepting end as its peer (`--framing native-quic`). Not part of `pnpm test`: it needs the built addon and the Python
 * workspace, so `pnpm test:conformance:node` runs it, with `vitest.relay-quic.config.js`.
 */
import { mkdtempSync, readdirSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { nativeQuicTransport } from '@caprid/relay-quic';
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

it('tries the next address when the first one the host resolves to does not answer', async () => {
  // Listening on 127.0.0.1 alone: where `localhost` resolves to ::1 first, only the fallback reaches it.
  const ipv4 = await startConformancePeer({ framing: 'native-quic', host: '127.0.0.1' });
  try {
    await ipv4.call('prepare');
    const value = Uint8Array.from(ipv4.certificateHash.match(/../g), (byte) => Number.parseInt(byte, 16));
    const factory = nativeQuicTransport({ serverCertificateHashes: [{ algorithm: 'sha-256', value }] });
    const { transport, ready } = factory(`https://localhost:${ipv4.port}/`, ['moqt-22']);
    transport.closed.catch(() => {});
    await ready;
    transport.close();
    await transport.closed;
  } finally {
    await ipv4.stop();
  }
}, 15_000);

it('TA-7 ready rejects when the offer is empty and the peer selects no protocol', async () => {
  // With nothing offered, TLS itself does not require a selection; the adapter does.
  await peer.call('prepare', { select: 'none' });
  const { transport, ready } = target.dialFactory(`https://localhost:${peer.port}/`, []);
  transport.closed.catch(() => {});
  await expect(ready).rejects.toMatchObject({ source: 'session' });
});

it('keeps reading a stream whose bytes trickle in, as the read and the reset watch hand over between them', async () => {
  await peer.call('prepare');
  const { transport, ready } = target.dialFactory(`https://localhost:${peer.port}/`, ['moqt-22']);
  transport.closed.catch(() => {});
  await ready;
  const id = await peer.call('openUni');
  const incoming = transport.incomingUnidirectionalStreams.getReader();
  let received = 0;
  const chunks = 200;
  const reading = (async () => {
    const reader = (await incoming.read()).value.getReader();
    while (true) {
      const { done, value } = await reader.read();
      if (done) return;
      received += value.byteLength;
    }
  })();
  for (let i = 0; i < chunks; i++) await peer.call('write', { stream: id, data: [i % 256] });
  await peer.call('finish', { stream: id });
  await reading;
  expect(received).toBe(chunks);
  transport.close();
  await transport.closed;
}, 20_000);

describe('@caprid/relay-quic incoming stream credit', () => {
  const hash = () => Uint8Array.from(peer.certificateHash.match(/../g), (byte) => Number.parseInt(byte, 16));

  /** A dial to the peer through a factory made with `options`, once ready. */
  async function dial(options = {}) {
    const factory = nativeQuicTransport({
      ...options,
      serverCertificateHashes: [{ algorithm: 'sha-256', value: hash() }],
    });
    const { transport, ready } = factory(`https://localhost:${peer.port}/`, ['moqt-22']);
    transport.closed.catch(() => {});
    await ready;
    return transport;
  }

  it("lets the relay have 1000 unidirectional streams open at once by default, not quinn's 100", async () => {
    await peer.call('prepare');
    const transport = await dial();
    // quinn's bidirectional default stays: the relay opens one of those per request, not one per object.
    expect(await peer.call('dialerStreamCredit')).toEqual({ uni: 1000, bidi: 100 });
    transport.close();
    await transport.closed;
  });

  it('grants the anticipatedConcurrentIncomingUnidirectionalStreams it is given', async () => {
    await peer.call('prepare');
    const transport = await dial({ anticipatedConcurrentIncomingUnidirectionalStreams: 64 });
    expect(await peer.call('dialerStreamCredit')).toMatchObject({ uni: 64 });
    transport.close();
    await transport.closed;
  });

  it('receives more than 100 unidirectional streams the peer keeps open at once', async () => {
    // A stream per object, none finished: with quinn's default of 100 the peer would hold the rest back for credit.
    await peer.call('prepare');
    const transport = await dial();
    const count = 150;
    await peer.call('openUnis', { count, data: [7] });
    const incoming = transport.incomingUnidirectionalStreams.getReader();
    const readers = [];
    for (let i = 0; i < count; i++) {
      const reader = (await incoming.read()).value.getReader();
      expect((await reader.read()).value).toEqual(Uint8Array.of(7));
      readers.push(reader);
    }
    expect(readers).toHaveLength(count);
    transport.close();
    await transport.closed;
  }, 15_000);
});

describe('@caprid/relay-quic qlog', () => {
  it('writes one qlog file per connection into qlogDir', async () => {
    const dir = join(mkdtempSync(join(tmpdir(), 'relay-quic-qlog-')), 'traces');
    try {
      await peer.call('prepare');
      const value = Uint8Array.from(peer.certificateHash.match(/../g), (byte) => Number.parseInt(byte, 16));
      const factory = nativeQuicTransport({ qlogDir: dir, serverCertificateHashes: [{ algorithm: 'sha-256', value }] });
      // One address, so one connection attempt: `localhost` could take a second one under load.
      const { transport, ready } = factory(`https://127.0.0.1:${peer.port}/`, ['moqt-22']);
      transport.closed.catch(() => {});
      await ready;
      transport.close();
      await transport.closed;
      const files = readdirSync(dir);
      expect(files).toHaveLength(1);
      expect(files[0]).toMatch(/^relay-quic-\d+-\d+-\d+\.sqlog$/);
      // JSON text sequences (RFC 7464): each record starts with a record separator.
      const records = readFileSync(join(dir, files[0]), 'utf8')
        .split('\x1e')
        .filter((record) => record.trim());
      expect(JSON.parse(records[0])).toHaveProperty('qlog_version');
      expect(records.length).toBeGreaterThan(1);
    } finally {
      rmSync(dirname(dir), { recursive: true, force: true });
    }
  });
});

describe('@caprid/relay-quic trust', () => {
  it('trusts a ca anchor added to the platform roots', async () => {
    await peer.call('prepare');
    const { transport, ready } = nativeQuicTransport({ ca: peer.certificatePem })(`https://localhost:${peer.port}/`, [
      'moqt-22',
    ]);
    transport.closed.catch(() => {});
    await ready;
    transport.close();
    await expect(transport.closed).resolves.toEqual({ closeCode: 0, reason: '' });
  });

  it('fails the dial with a session error when the certificate is not trusted', async () => {
    await peer.call('prepare');
    const { transport, ready } = nativeQuicTransport()(`https://localhost:${peer.port}/`, ['moqt-22']);
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

describe('@caprid/relay/node', () => {
  // The peer accepts the QUIC connection but speaks no MOQT, so a connect that gets as far as SETUP times out: the
  // entry point's default adapter dialled, with the trust it was given. Untrusted, the handshake itself fails.
  it('dials through @caprid/relay-quic by default, with the ca it is given', async () => {
    await peer.call('prepare');
    const { connectRelaySubscriber } = await import('@caprid/relay/node');
    const url = `https://localhost:${peer.port}/`;
    await expect(
      connectRelaySubscriber({ url, ca: peer.certificatePem, connectTimeoutMs: 1_000 }),
    ).rejects.toMatchObject({ code: 'CONNECTION_TIMEOUT' });

    await peer.call('prepare');
    await expect(connectRelaySubscriber({ url, connectTimeoutMs: 5_000 })).rejects.toMatchObject({
      code: 'TRANSPORT_ERROR',
      phase: 'connecting',
    });
  }, 15_000);
});
