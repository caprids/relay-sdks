/**
 * The child process behind the `@caprid/relay-websocket` target's process lifetime checks in Node:
 *
 *     node websocket-lifetime.js <origin> <kind>
 *
 * The same checks as `relay-quic-lifetime.js`, which describes `kind`, over QMux over WebSocket. The peer's certificate
 * is trusted through NODE_EXTRA_CA_CERTS, which the child inherits.
 */
import { createWebSocketTransport } from '@caprid/relay-websocket';

const [origin, kind] = process.argv.slice(2);
const { transport, ready } = createWebSocketTransport()(`${origin}/conformance`, ['moqt-22']);
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
