/**
 * `@mux/relay-websocket` against an in-process QMux-over-WebSocket accepting end (`@moq/qmux` over `ws`), on plain
 * `ws://` so no certificate is involved. The conformance suite covers the contract rule by rule
 * (`conformance/websocket.conformance.js`); these cover the factories, the URL mapping and the fallback policy.
 */
import type { AddressInfo } from 'node:net';
import Session, { StreamError } from '@moq/qmux';
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import { type WebSocket, WebSocketServer } from 'ws';
import { resetCode, resetReason, WebSocketSessionError } from '../errors.js';
import { assertHandshakeTimeout, toWebSocketUrl } from '../fallback.js';
import { createWebSocketTransport, withWebSocketFallback } from '../index.js';

interface Accepted {
  session: Session;
  path: string;
  offered: string[];
}

let server: WebSocketServer;
let origin: string;
let accepted: Accepted[] = [];
let select: (offered: Set<string>) => string | false = (offered) => offered.values().next().value ?? false;

beforeAll(async () => {
  server = new WebSocketServer({ host: '127.0.0.1', port: 0, handleProtocols: (offered) => select(offered) });
  server.on('connection', (socket: WebSocket, request) => {
    const session = Session.accept(socket as unknown as Parameters<typeof Session.accept>[0], {
      protocol: socket.protocol,
    });
    session.closed.catch(() => {});
    accepted.push({
      session,
      path: request.url ?? '',
      offered: String(request.headers['sec-websocket-protocol'] ?? '')
        .split(',')
        .map((value) => value.trim())
        .filter(Boolean),
    });
  });
  await new Promise<void>((resolve) => server.once('listening', resolve));
  origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});

afterEach(() => {
  for (const { session } of accepted) session.close();
  accepted = [];
  select = (offered) => offered.values().next().value ?? false;
  vi.unstubAllGlobals();
});

afterAll(() => new Promise<void>((resolve) => server.close(() => resolve())));

async function nextAccepted(): Promise<Accepted> {
  await vi.waitFor(() => expect(accepted.length).toBeGreaterThan(0));
  return accepted[accepted.length - 1] as Accepted;
}

describe('toWebSocketUrl', () => {
  it('maps https to wss and http to ws, keeping the path and query exactly', () => {
    expect(toWebSocketUrl('https://relay.example.com/moq?jwt=a.b.c&x=%2F')).toBe(
      'wss://relay.example.com/moq?jwt=a.b.c&x=%2F',
    );
    expect(toWebSocketUrl('http://127.0.0.1:4443/?')).toBe('ws://127.0.0.1:4443/?');
    expect(toWebSocketUrl('wss://relay.example.com/')).toBe('wss://relay.example.com/');
  });

  it('refuses any other scheme', () => {
    expect(() => toWebSocketUrl('moqt://relay.example.com/')).toThrow(TypeError);
  });
});

describe('resetCode (TA-17)', () => {
  it("takes the reason's integer streamErrorCode from 0 to 2^32-1, and 0 for anything else", () => {
    expect(resetCode({ streamErrorCode: 7 })).toBe(7);
    expect(resetCode({ streamErrorCode: 0xffff_ffff })).toBe(0xffff_ffff);
    for (const code of [-1, 1.5, 0x1_0000_0000, Number.NaN, '7', null]) {
      expect(resetCode({ streamErrorCode: code })).toBe(0);
    }
    expect(resetCode(undefined)).toBe(0);
    expect(resetCode('reset')).toBe(0);
    // @moq/qmux would wrap -1 to 2^32-1; the reason it is handed carries the checked code.
    expect(resetReason({ streamErrorCode: -1 })).toEqual({ streamErrorCode: 0 });
  });
});

describe('createWebSocketTransport', () => {
  it('offers each protocol on qmux-01, dials the URL as given and declares its framing', async () => {
    const { transport, ready, framing } = createWebSocketTransport()(`${origin}/moq?jwt=token`, ['moqt-22', 'moqt-20']);
    expect(framing).toBe('websocket');
    expect(transport.protocol).toBe('');
    await ready;
    expect(transport.protocol).toBe('moqt-22');
    const peer = await nextAccepted();
    expect(peer.path).toBe('/moq?jwt=token');
    expect(peer.offered).toEqual(['qmux-01.moqt-22', 'qmux-01.moqt-20']);
    transport.close({ closeCode: 4, reason: 'done' });
    await expect(transport.closed).resolves.toEqual({ closeCode: 4, reason: 'done' });
    await expect(peer.session.closed).resolves.toEqual({ closeCode: 4, reason: 'done' });
  });

  it('carries streams both ways and reports a peer reset as a stream error with its code', async () => {
    const { transport, ready } = createWebSocketTransport()(`${origin}/`, ['moqt-22']);
    await ready;
    const peer = await nextAccepted();

    const writer = (await transport.createUnidirectionalStream()).getWriter();
    await writer.write(Uint8Array.of(1, 2, 3));
    await writer.close();
    const incoming = (await peer.session.incomingUnidirectionalStreams.getReader().read()).value;
    const read = await incoming?.getReader().read();
    expect([...(read?.value ?? [])]).toEqual([1, 2, 3]);

    const sent = (await peer.session.createUnidirectionalStream()).getWriter();
    await sent.write(Uint8Array.of(9));
    const reader = (await transport.incomingUnidirectionalStreams.getReader().read()).value?.getReader();
    expect([...((await reader?.read())?.value ?? [])]).toEqual([9]);
    await sent.abort(new StreamError(11));
    await expect(reader?.read()).rejects.toMatchObject({ source: 'stream', streamErrorCode: 11 });
    transport.close();
  });

  it('closes with a reason too long for one QMux record, cut short', async () => {
    const { transport, ready } = createWebSocketTransport()(`${origin}/`, ['moqt-22']);
    await ready;
    const peer = await nextAccepted();
    transport.close({ closeCode: 3, reason: 'é'.repeat(20_000) });
    const closed = (await transport.closed) as { closeCode: number; reason: string };
    expect(closed.closeCode).toBe(3);
    expect(new TextEncoder().encode(closed.reason).byteLength).toBeLessThanOrEqual(1024);
    expect(closed.reason).toBe('é'.repeat(512));
    await expect(peer.session.closed).resolves.toEqual(closed);
  });

  it('errors a receive stream holding an unread chunk as soon as the session ends', async () => {
    const { transport, ready } = createWebSocketTransport()(`${origin}/`, ['moqt-22']);
    await ready;
    const peer = await nextAccepted();
    const sent = (await peer.session.createUnidirectionalStream()).getWriter();
    await sent.write(Uint8Array.of(1));
    const reader = (await transport.incomingUnidirectionalStreams.getReader().read()).value?.getReader();
    // Let the adapter's read-ahead take the chunk, then end the session without reading it.
    await new Promise((resolve) => setTimeout(resolve, 50));
    peer.session.close({ closeCode: 8, reason: 'gone' });
    await expect(reader?.closed).rejects.toMatchObject({ source: 'session', closeCode: 8, reason: 'gone' });
  });

  it('fails ready with a session error when the peer selects no protocol', async () => {
    select = () => false;
    const { transport, ready } = createWebSocketTransport()(`${origin}/`, ['moqt-22']);
    await expect(ready).rejects.toBeInstanceOf(WebSocketSessionError);
    await expect(transport.closed).rejects.toMatchObject({ source: 'session' });
  });

  it('fails ready with a session error when nothing listens', async () => {
    const { transport, ready } = createWebSocketTransport()('http://127.0.0.1:1/', ['moqt-22']);
    await expect(ready).rejects.toMatchObject({ source: 'session' });
    await expect(transport.closed).rejects.toMatchObject({ source: 'session' });
  });
});

/** A stand-in for the runtime's WebTransport whose handshake does what `handshake` says. */
function fakeWebTransport(handshake: 'fail' | 'hang' | 'succeed') {
  const dialled: string[] = [];
  class FakeWebTransport {
    // On the prototype, as in a runtime whose WebTransport negotiates protocols.
    get protocol() {
      return handshake === 'succeed' ? 'moqt-22' : '';
    }
    readonly ready: Promise<void>;
    readonly closed: Promise<{ closeCode: number; reason: string }>;
    readonly incomingUnidirectionalStreams = new ReadableStream();
    readonly incomingBidirectionalStreams = new ReadableStream();
    #reject!: (error: unknown) => void;
    #resolveClosed!: (info: { closeCode: number; reason: string }) => void;
    constructor(url: string) {
      dialled.push(url);
      this.ready = new Promise<void>((resolve, reject) => {
        this.#reject = reject;
        if (handshake === 'succeed') resolve();
        if (handshake === 'fail') reject(new Error('Opening handshake failed.'));
      });
      this.ready.catch(() => {});
      this.closed = new Promise((resolve) => {
        this.#resolveClosed = resolve;
      });
    }
    close(info: { closeCode?: number; reason?: string } = {}) {
      this.#reject(new Error('closed'));
      this.#resolveClosed({ closeCode: info.closeCode ?? 0, reason: info.reason ?? '' });
    }
    createUnidirectionalStream() {
      return Promise.reject(new Error('not used'));
    }
    createBidirectionalStream() {
      return Promise.reject(new Error('not used'));
    }
  }
  vi.stubGlobal('WebTransport', FakeWebTransport);
  return dialled;
}

describe('withWebSocketFallback', () => {
  it('falls back to WebSocket when the runtime refuses to construct the WebTransport', async () => {
    class RefusingWebTransport {
      get protocol() {
        return '';
      }
      constructor() {
        throw new Error('refused by policy');
      }
    }
    vi.stubGlobal('WebTransport', RefusingWebTransport);
    const onFallback = vi.fn();
    const connection = withWebSocketFallback({ onFallback })(`${origin}/`, ['moqt-22']);
    await connection.ready;
    expect(connection.framing).toBe('websocket');
    expect(String(onFallback.mock.calls[0]?.[0])).toMatch(/refused by policy/);
    connection.transport.close();
  });

  it('takes a cancel of an incoming readable before the dial is won, and the session still works', async () => {
    fakeWebTransport('fail');
    const connection = withWebSocketFallback()(`${origin}/`, ['moqt-22']);
    await connection.transport.incomingUnidirectionalStreams.cancel('not reading');
    await connection.ready;
    const peer = await nextAccepted();
    // The peer's streams go to a cancelled queue; the dialer's own still flow.
    const sent = (await peer.session.createUnidirectionalStream()).getWriter();
    await sent.write(Uint8Array.of(1));
    const writer = (await connection.transport.createUnidirectionalStream()).getWriter();
    await writer.write(Uint8Array.of(2));
    await writer.close();
    const incoming = (await peer.session.incomingUnidirectionalStreams.getReader().read()).value;
    expect([...((await incoming?.getReader().read())?.value ?? [])]).toEqual([2]);
    connection.transport.close();
  });

  it('keeps a WebTransport session that comes up, and never dials WebSocket', async () => {
    const dialled = fakeWebTransport('succeed');
    const onFallback = vi.fn();
    const connection = withWebSocketFallback({ onFallback })(`${origin}/moq`, ['moqt-22']);
    await connection.ready;
    expect(connection.framing).toBe('webtransport');
    expect(connection.transport.protocol).toBe('moqt-22');
    expect(dialled).toEqual([`${origin}/moq`]);
    expect(onFallback).not.toHaveBeenCalled();
    expect(accepted).toEqual([]);
    connection.transport.close({ closeCode: 1, reason: 'bye' });
    await expect(connection.transport.closed).resolves.toEqual({ closeCode: 1, reason: 'bye' });
  });

  it('falls back to WebSocket when the WebTransport handshake fails', async () => {
    fakeWebTransport('fail');
    const onFallback = vi.fn();
    const connection = withWebSocketFallback({ onFallback })(`${origin}/moq?jwt=t`, ['moqt-22']);
    expect(connection.framing).toBe('webtransport');
    await connection.ready;
    expect(connection.framing).toBe('websocket');
    expect(connection.transport.protocol).toBe('moqt-22');
    expect(onFallback).toHaveBeenCalledOnce();
    expect(String(onFallback.mock.calls[0]?.[0])).toMatch(/Opening handshake failed/);
    expect((await nextAccepted()).path).toBe('/moq?jwt=t');

    // The winner's incoming streams come through the fallback transport.
    const peer = await nextAccepted();
    const sent = (await peer.session.createUnidirectionalStream()).getWriter();
    await sent.write(Uint8Array.of(5));
    const reader = (await connection.transport.incomingUnidirectionalStreams.getReader().read()).value?.getReader();
    expect([...((await reader?.read())?.value ?? [])]).toEqual([5]);
    connection.transport.close();
    await expect(connection.transport.closed).resolves.toEqual({ closeCode: 0, reason: '' });
  });

  it('falls back to WebSocket once the WebTransport handshake outlasts the timeout', async () => {
    fakeWebTransport('hang');
    const onFallback = vi.fn();
    const started = performance.now();
    const connection = withWebSocketFallback({ handshakeTimeoutMs: 200, onFallback })(`${origin}/`, ['moqt-22']);
    await connection.ready;
    expect(performance.now() - started).toBeGreaterThanOrEqual(190);
    expect(connection.framing).toBe('websocket');
    expect(String(onFallback.mock.calls[0]?.[0])).toMatch(/longer than 200 ms/);
    connection.transport.close();
  });

  it('cancels a dial closed during the WebTransport handshake without dialing WebSocket', async () => {
    fakeWebTransport('hang');
    const onFallback = vi.fn();
    const connection = withWebSocketFallback({ onFallback })(`${origin}/`, ['moqt-22']);
    connection.transport.close();
    await expect(connection.ready).rejects.toMatchObject({ source: 'session' });
    await expect(connection.transport.closed).rejects.toMatchObject({ source: 'session' });
    expect(onFallback).not.toHaveBeenCalled();
    expect(accepted).toEqual([]);
  });

  it('ends its incoming readables with a failed dial', async () => {
    vi.stubGlobal('WebTransport', undefined);
    const connection = withWebSocketFallback()('http://127.0.0.1:1/', ['moqt-22']);
    await expect(connection.ready).rejects.toMatchObject({ source: 'session' });
    await expect(connection.transport.incomingUnidirectionalStreams.getReader().read()).rejects.toMatchObject({
      source: 'session',
    });
  });

  it('checks the handshake timeout when the factory is made', () => {
    expect(assertHandshakeTimeout(undefined)).toBe(4_000);
    expect(() => withWebSocketFallback({ handshakeTimeoutMs: 0 })).toThrow(RangeError);
    expect(() => withWebSocketFallback({ handshakeTimeoutMs: 2_147_483_648 })).toThrow(RangeError);
    expect(() => withWebSocketFallback({ handshakeTimeoutMs: '4000' as unknown as number })).toThrow(TypeError);
  });
});
