/**
 * The conformance suite against `@caprid/relay-websocket` in Node, with the Node accepting end (`scripts/websocket-peer.js`)
 * as its peer. Not part of `pnpm test`: Node must trust the peer's certificate from the start, so
 * `pnpm test:conformance:websocket` runs it with `vitest.websocket.config.js`, which arranges that.
 */
import { fileURLToPath } from 'node:url';
import { createWebSocketTransport, withWebSocketFallback } from '@caprid/relay-websocket';
import { afterAll, describe, expect, it } from 'vitest';
import { startConformancePeer } from '../scripts/conformance-peer.js';
import { checkProcessLifetime } from './targets/lifetime.js';
import { createWebSocketTarget } from './targets/websocket.js';
import { describeConformance } from './vitest.js';

const peer = await startConformancePeer({ framing: 'websocket' });
afterAll(() => peer.stop());

const lifetimeScript = fileURLToPath(new URL('./targets/websocket-lifetime.js', import.meta.url));
const browserTarget = createWebSocketTarget({
  name: '@caprid/relay-websocket in Node',
  port: peer.port,
  call: peer.call,
});
const target = {
  ...browserTarget,
  capabilities: [...browserTarget.capabilities, 'process'],
  checkProcessLifetime: (kind) => checkProcessLifetime(lifetimeScript, [browserTarget.origin, kind], kind),
};
describeConformance(target);

it('TA-11 a dial cancelled before ready leaves nothing that keeps the process alive', async () => {
  await peer.call('prepare', { handshake: 'hold' });
  await target.checkProcessLifetime('exits-after-cancel');
}, 20_000);

it('rejects a URL that is not https', () => {
  expect(() => createWebSocketTransport()(`ftp://localhost:${peer.port}/`, ['moqt-22'])).toThrow(TypeError);
});

describe('withWebSocketFallback', () => {
  it('dials WebSocket at once where the runtime has no WebTransport, and says so', async () => {
    await peer.call('prepare');
    const reasons = [];
    const factory = withWebSocketFallback({ onFallback: (reason) => reasons.push(reason) });
    const connection = factory(`https://localhost:${peer.port}/fallback?jwt=t`, ['moqt-22']);
    connection.transport.closed.catch(() => {});
    await connection.ready;
    expect(connection.framing).toBe('websocket');
    expect(connection.transport.protocol).toBe('moqt-22');
    expect(reasons).toEqual([undefined]);
    expect(await peer.call('observedDial')).toMatchObject({ path: '/fallback?jwt=t' });

    // The winner's streams, both ways.
    const writer = (await connection.transport.createUnidirectionalStream()).getWriter();
    await writer.write(new TextEncoder().encode('hi'));
    await writer.close();
    const id = await peer.call('acceptUni');
    expect(await peer.call('read', { stream: id })).toMatchObject({ bytes: [104, 105], fin: true });

    connection.transport.close({ closeCode: 3, reason: 'done' });
    await expect(connection.transport.closed).resolves.toEqual({ closeCode: 3, reason: 'done' });
  });

  it('cancels the dial when closed before ready', async () => {
    await peer.call('prepare', { handshake: 'hold' });
    const connection = withWebSocketFallback()(`https://localhost:${peer.port}/`, ['moqt-22']);
    connection.transport.closed.catch(() => {});
    await new Promise((resolve) => setTimeout(resolve, 100));
    connection.transport.close();
    await expect(connection.ready).rejects.toMatchObject({ source: 'session' });
    await expect(connection.transport.closed).rejects.toMatchObject({ source: 'session' });
    await peer.call('releaseHandshake');
  });

  it('rejects a WebSocket delay a timer cannot hold, and the handshake timeout it replaced', () => {
    expect(() => withWebSocketFallback({ webSocketDelayMs: -1 })).toThrow(RangeError);
    expect(() => withWebSocketFallback({ webSocketDelayMs: '250' })).toThrow(TypeError);
    expect(() => withWebSocketFallback({ handshakeTimeoutMs: 4000 })).toThrow(TypeError);
  });
});
