/**
 * The child process behind the `@mux/relay-quic` target's process lifetime checks:
 *
 *     node relay-quic-lifetime.js <origin> <certificate hash hex> <kind>
 *
 * `kind` is `keeps-alive`, `exits-after-close` or `exits-after-cancel`. It dials the conformance peer with nothing else
 * scheduled. With `keeps-alive` it prints `ready` once the session is up and does nothing more, so only the adapter
 * keeps it running (TA-25). With `exits-after-close` it then closes the session, prints `closed` once `closed` has
 * settled, and must exit by itself (TA-26). With `exits-after-cancel` it closes the transport before `ready`, against a
 * peer that holds the handshake, and prints `closed` once both have rejected: the cancelled dial (TA-11) must not hold
 * the process either. It reads no incoming stream, unlike the SDK, so only the adapter holds the process.
 */
import { nativeQuicTransport } from '@mux/relay-quic';

const [origin, certificateHash, kind] = process.argv.slice(2);
const value = Uint8Array.from(certificateHash.match(/../g), (byte) => Number.parseInt(byte, 16));
const factory = nativeQuicTransport({ serverCertificateHashes: [{ algorithm: 'sha-256', value }] });
const { transport, ready } = factory(`${origin}/conformance`, ['moqt-20']);
transport.closed.catch(() => {});

if (kind === 'exits-after-cancel') {
  // Long enough for the handshake to be under way, which the peer holds.
  await new Promise((resolve) => setTimeout(resolve, 300));
  transport.close({ closeCode: 0, reason: 'cancelled' });
  const settled = await Promise.allSettled([ready, transport.closed]);
  process.stdout.write('closed\n');
  if (settled.some(({ status }) => status !== 'rejected')) process.exitCode = 1;
} else {
  await ready;
  process.stdout.write('ready\n');
}
if (kind === 'exits-after-close') {
  transport.close({ closeCode: 0, reason: 'lifetime check' });
  await transport.closed;
  process.stdout.write('closed\n');
}
