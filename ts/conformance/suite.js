/**
 * The transport adapter conformance suite (`spec/transport-adapter.md`, "Conformance"), runtime-neutral: it uses only
 * APIs that browsers and Node both provide, and gets everything target-specific from the target it is handed. Each
 * case is named after the rule it checks. A runner per runtime turns the cases into that runtime's tests:
 * `pair.test.js` for the in-memory pair and `relay-quic.conformance.js` for `@caprid/relay-quic` under vitest, and
 * `scripts/test-conformance.mjs` for Chromium. `README.md` describes the target and peer interfaces.
 */

/** The MOQT protocol identifier the cases offer by default. */
export const MOQT_PROTOCOL = 'moqt-22';

/** The QMux wire format a WebSocket adapter offers each MOQT protocol on (TA-5): `qmux-01.moqt-22`. */
const QMUX_WIRE_FORMAT = 'qmux-01';

/** How long a case may wait for something that must happen promptly. */
const PROMPTLY_MS = 2_000;
/** How long a case watches for something that must not happen. */
const STAYS_PENDING_MS = 300;

/** A case's assertion failed. */
export class ConformanceFailure extends Error {
  name = 'ConformanceFailure';
}

function check(condition, message) {
  if (!condition) throw new ConformanceFailure(message);
}

function describe(value) {
  if (value instanceof Error) return `${value.name}: ${value.message} ${JSON.stringify(shapeOf(value))}`;
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function shapeOf(error) {
  const { source, streamErrorCode, closeCode, reason } = error;
  return { source, streamErrorCode, closeCode, reason };
}

/** Structural equality: arrays in order, objects by their own keys in any order, values as `Object.is`. */
function deepEqual(a, b) {
  if (Object.is(a, b)) return true;
  if (typeof a !== 'object' || typeof b !== 'object' || a === null || b === null) return false;
  if (Array.isArray(a) !== Array.isArray(b)) return false;
  const keys = Object.keys(a);
  if (keys.length !== Object.keys(b).length) return false;
  return keys.every((key) => Object.hasOwn(b, key) && deepEqual(a[key], b[key]));
}

function equal(actual, expected, what) {
  check(deepEqual(actual, expected), `${what}: expected ${describe(expected)}, got ${describe(actual)}`);
}

/** `promise`, with its rejection marked handled: for an operation a case starts now and asserts on later. */
function later(promise) {
  promise.catch(() => {});
  return promise;
}

const PENDING = Symbol('pending');

/** Where `promise` stands after `ms`: `{ state: 'fulfilled', value }`, `{ state: 'rejected', reason }` or pending. */
async function settleWithin(promise, ms) {
  let timer;
  const deadline = new Promise((resolve) => {
    timer = setTimeout(() => resolve(PENDING), ms);
  });
  try {
    const value = await Promise.race([promise, deadline]);
    return value === PENDING ? { state: 'pending' } : { state: 'fulfilled', value };
  } catch (reason) {
    return { state: 'rejected', reason };
  } finally {
    clearTimeout(timer);
  }
}

async function resolves(promise, what, ms = PROMPTLY_MS) {
  const outcome = await settleWithin(promise, ms);
  check(outcome.state !== 'pending', `${what}: still pending after ${ms} ms`);
  check(outcome.state === 'fulfilled', `${what}: rejected with ${describe(outcome.reason)}`);
  return outcome.value;
}

async function rejects(promise, what, ms = PROMPTLY_MS) {
  const outcome = await settleWithin(promise, ms);
  check(outcome.state !== 'pending', `${what}: still pending after ${ms} ms`);
  check(outcome.state === 'rejected', `${what}: resolved with ${describe(outcome.value)}, expected a rejection`);
  return outcome.reason;
}

async function staysPending(promise, what, ms = STAYS_PENDING_MS) {
  const outcome = await settleWithin(promise, ms);
  check(
    outcome.state === 'pending',
    `${what}: settled (${outcome.state}) within ${ms} ms: ${describe(outcome.value ?? outcome.reason)}`,
  );
}

/**
 * TA-22: a stream error has `source: 'stream'` and the peer's code, or `null` where the adapter cannot know it (TA-22
 * allows that; it is never 0 for unknown). The codes the dialer sends are checked exactly, at the peer.
 */
function expectStreamError(error, code, what) {
  check(typeof error === 'object' && error !== null, `${what}: expected a stream error object, got ${describe(error)}`);
  check(error.source === 'stream', `${what}: expected source 'stream', got ${describe(error)}`);
  check(
    error.streamErrorCode === code || error.streamErrorCode === null,
    `${what}: expected streamErrorCode ${code} (or null for unknown), got ${describe(error)}`,
  );
}

/** TA-27: a stream error whose `streamErrorCode` is exactly `code`, where `null` is no code at all. */
function expectExactStreamError(error, code, what) {
  check(typeof error === 'object' && error !== null, `${what}: expected a stream error object, got ${describe(error)}`);
  check(error.source === 'stream', `${what}: expected source 'stream', got ${describe(error)}`);
  check(error.streamErrorCode === code, `${what}: expected streamErrorCode ${code}, got ${describe(error)}`);
}

/** TA-22: a session error has `source: 'session'`. */
function expectSessionError(error, what) {
  check(
    typeof error === 'object' && error !== null,
    `${what}: expected a session error object, got ${describe(error)}`,
  );
  check(error.source === 'session', `${what}: expected source 'session', got ${describe(error)}`);
}

const encoder = new TextEncoder();
const text = (value) => encoder.encode(value);
const decoder = new TextDecoder();
const asText = (bytes) => decoder.decode(bytes);

function concat(chunks) {
  const out = new Uint8Array(chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0));
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return out;
}

/** Read the dialer's `reader` until FIN. */
async function readToEnd(reader) {
  const chunks = [];
  while (true) {
    const { done, value } = await reader.read();
    if (done) return concat(chunks);
    chunks.push(value);
  }
}

/** Read at least `count` bytes from the dialer's `reader`. */
async function readBytes(reader, count) {
  const chunks = [];
  let total = 0;
  while (total < count) {
    const { done, value } = await reader.read();
    check(!done, `the stream ended after ${total} of ${count} bytes`);
    chunks.push(value);
    total += value.byteLength;
  }
  return concat(chunks);
}

/** Dial and wait for `ready`. */
async function open(ctx, options) {
  const result = ctx.dial(options);
  await resolves(result.ready, 'ready');
  return result.transport;
}

/** The next stream the peer opened, from the dialer's incoming readable. */
async function nextIncoming(readable, what) {
  const reader = readable.getReader();
  try {
    const { done, value } = await resolves(reader.read(), what);
    check(!done, `${what}: the incoming readable ended`);
    return value;
  } finally {
    reader.releaseLock();
  }
}

/** Open a uni stream, write `bytes` and have the peer accept it. */
async function openAcceptedUni(ctx, transport, bytes = text('hello')) {
  const writer = (await resolves(transport.createUnidirectionalStream(), 'createUnidirectionalStream')).getWriter();
  await resolves(writer.write(bytes), 'write');
  const id = await resolves(ctx.peer.acceptUni(), 'the peer accepting the stream');
  return { writer, id };
}

/** Write chunks until one stays pending, the sign that flow control or the send buffer holds the stream. */
async function writeUntilBlocked(writer, limitBytes = 64 * 1024 * 1024) {
  const chunk = new Uint8Array(64 * 1024);
  let written = 0;
  while (written < limitBytes) {
    const write = writer.write(chunk);
    written += chunk.byteLength;
    if ((await settleWithin(write, 200)).state === 'pending') return { write, written };
  }
  throw new ConformanceFailure(`${limitBytes} bytes were accepted without a write ever staying pending`);
}

/** Pause the peer when the target can, so it acknowledges nothing; otherwise rely on it not reading. */
async function holdPeer(ctx) {
  if (ctx.target.capabilities.includes('pause')) await ctx.peer.pause();
}

async function releasePeer(ctx) {
  if (ctx.target.capabilities.includes('pause')) await ctx.peer.resume();
}

/** Codes a reset or stop must carry through, up to 255: all the MOQT codes the core sends (draft-22 §12.5). */
const LOW_CODES = [0, 1, 7, 0x12, 255];
/** The rest of TA-17's range, which some browsers cannot send (Known deviations). */
const HIGH_CODES = [256, 0x1_0000, 0xffff_ffff];
/** `streamErrorCode` values TA-17 maps to 0. */
const INVALID_CODES = [-1, 0x1_0000_0000, 1.5, Number.NaN, '7', null];

/** Where the HTTP/3 encoding puts code 0 (TA-27). */
const H3_FIRST_CODE = 0x52e4_a40f_a8dbn;
/** `code` in the HTTP/3 encoding, as a decimal string: one GREASE slot is skipped per 30 codes (TA-27). */
const h3Code = (code) => String(H3_FIRST_CODE + BigInt(code) + BigInt(Math.floor(code / 0x1e)));
/** Codes on both sides of the first GREASE slot, and the range's ends. */
const TA_27_CODES = [0, 7, 0x1d, 0x1e, 0xffff_ffff];
/** `[wire code, the code the dialer reports]`: encoded codes, a GREASE slot (no code), and a code outside the encoding. */
const TA_27_PEER_CODES = [
  ...TA_27_CODES.map((code) => [h3Code(code), code]),
  [String(H3_FIRST_CODE + 0x1en), null],
  ['11', 11],
];

/** TA-17: reset a fresh stream with each of `codes`; the peer must see each one. */
async function resetsCarry(ctx, codes) {
  const transport = await open(ctx);
  for (const code of codes) {
    const { writer, id } = await openAcceptedUni(ctx, transport);
    await writer.abort(ctx.reason(code));
    equal((await ctx.peer.read(id)).resetCode, code, `the reset code for streamErrorCode ${code}`);
  }
}

/** TA-18: stop a fresh peer stream with each of `codes`; the peer must see each one, and the readable ends. */
async function stopsCarry(ctx, codes) {
  const transport = await open(ctx);
  for (const code of codes) {
    const id = await ctx.peer.openUni();
    await ctx.peer.write(id, text('before'));
    const reader = (await nextIncoming(transport.incomingUnidirectionalStreams, 'the peer stream')).getReader();
    await readBytes(reader, 6);
    await reader.cancel(ctx.reason(code));
    equal(await resolves(ctx.peer.stopped(id), 'the peer seeing the stop'), code, `the stop code for ${code}`);
    await ctx.peer.write(id, text('after')).catch(() => {});
    equal((await reader.read()).done, true, 'a read after cancel');
  }
}

/**
 * The cases. Each has the `rule` it checks, a `level` (`MUST` or `SHOULD`), a `name` that starts with the rule, the
 * capabilities it `needs` from the target, optional `session` options for the peer, and `run(ctx)`.
 */
export const cases = [
  // ── Dialing and framing ──────────────────────────────────────────────────────────────────────────────────
  {
    rule: 'TA-1',
    name: 'TA-1 declares its framing in the factory result',
    async run(ctx) {
      const result = ctx.dial();
      const framing = result.framing ?? 'webtransport';
      check(
        result.framing === undefined || ['webtransport', 'native-quic', 'websocket'].includes(result.framing),
        `framing must be 'webtransport', 'native-quic' or 'websocket', got ${describe(result.framing)}`,
      );
      equal(framing, ctx.target.framing, 'framing');
      await resolves(result.ready, 'ready');
    },
  },
  {
    rule: 'TA-2',
    name: 'TA-2 sends the CONNECT to the URL as given, path and query included',
    needs: ['webtransport'],
    async run(ctx) {
      const path = '/conformance/ta-2/x%2Fy?jwt=header.payload.signature&other=1';
      await open(ctx, { path });
      equal((await ctx.peer.observed()).path, path, 'the path the peer saw');
    },
  },
  {
    rule: 'TA-2',
    name: 'TA-2 sends the WebSocket upgrade to the URL as given, path and query included',
    needs: ['websocket'],
    async run(ctx) {
      const path = '/conformance/ta-2/x%2Fy?jwt=header.payload.signature&other=1';
      await open(ctx, { path });
      equal((await ctx.peer.observed()).path, path, 'the path the peer saw');
    },
  },
  {
    rule: 'TA-3',
    name: 'TA-3 connects to the host with it as the server name and sends no path or query',
    needs: ['native-quic'],
    async run(ctx) {
      await open(ctx, { path: '/conformance/ta-3?jwt=secret' });
      const observed = await ctx.peer.observed();
      equal(observed.serverName, ctx.target.host, 'the TLS server name');
      equal(observed.path, undefined, 'a path on the wire');
    },
  },
  {
    rule: 'TA-4',
    name: 'TA-4 accepts an https URL',
    async run(ctx) {
      const result = ctx.dial();
      check(result.url.startsWith('https://'), `the target dialled ${result.url}, not an https URL`);
      await resolves(result.ready, 'ready');
    },
  },

  // ── Version negotiation ──────────────────────────────────────────────────────────────────────────────────
  {
    rule: 'TA-5',
    name: 'TA-5 offers exactly the given protocols, in order',
    async run(ctx) {
      const protocols = [MOQT_PROTOCOL, 'moqt-conformance-b', 'moqt-conformance-a'];
      await open(ctx, { protocols });
      // Over WebSocket each protocol is a subprotocol on the QMux wire format that carries it.
      const offered =
        ctx.target.framing === 'websocket' ? protocols.map((protocol) => `${QMUX_WIRE_FORMAT}.${protocol}`) : protocols;
      equal((await ctx.peer.observed()).protocols, offered, 'the protocols the peer saw offered');
    },
  },
  {
    rule: 'TA-7',
    name: 'TA-7 ready waits for the peer to select a protocol',
    needs: ['holdHandshake'],
    session: { handshake: 'hold' },
    async run(ctx) {
      const { ready } = ctx.dial();
      await staysPending(ready, 'ready while the handshake is held');
      await ctx.peer.releaseHandshake();
      await resolves(ready, 'ready once the peer answered');
    },
  },
  {
    rule: 'TA-7',
    name: 'TA-7 ready rejects with a session error when the peer selects no protocol',
    needs: ['selectProtocol'],
    session: { select: 'none' },
    async run(ctx) {
      const { ready, transport } = ctx.dial();
      const outcome = await settleWithin(ready, PROMPTLY_MS);
      check(outcome.state !== 'fulfilled', `ready resolved, with protocol ${describe(transport.protocol)}`);
      check(outcome.state === 'rejected', `ready still pending after ${PROMPTLY_MS} ms`);
      expectSessionError(outcome.reason, 'ready');
    },
  },
  {
    rule: 'TA-7',
    name: 'TA-7 ready rejects with a session error when the peer selects a protocol that was not offered',
    needs: ['selectProtocol'],
    session: { select: 'unoffered' },
    async run(ctx) {
      const { ready, transport } = ctx.dial();
      const outcome = await settleWithin(ready, PROMPTLY_MS);
      check(outcome.state !== 'fulfilled', `ready resolved, with protocol ${describe(transport.protocol)}`);
      check(outcome.state === 'rejected', `ready still pending after ${PROMPTLY_MS} ms`);
      expectSessionError(outcome.reason, 'ready');
    },
  },
  {
    rule: 'TA-8',
    name: 'TA-8 protocol is the selected identifier once ready',
    async run(ctx) {
      const transport = await open(ctx, { protocols: [MOQT_PROTOCOL, 'moqt-conformance-b'] });
      equal(transport.protocol, MOQT_PROTOCOL, 'transport.protocol');
    },
  },

  // ── Session lifetime ─────────────────────────────────────────────────────────────────────────────────────
  {
    rule: 'TA-9',
    name: 'TA-9 closed resolves with the code and reason of a local close',
    async run(ctx) {
      const transport = await open(ctx);
      transport.close({ closeCode: 7, reason: 'conformance' });
      equal(await resolves(transport.closed, 'closed'), { closeCode: 7, reason: 'conformance' }, 'closed');
    },
  },
  {
    rule: 'TA-9',
    name: "TA-9 closed resolves with the code and reason of the peer's application close",
    async run(ctx) {
      const transport = await open(ctx);
      await ctx.peer.close(42, 'peer done');
      equal(await resolves(transport.closed, 'closed'), { closeCode: 42, reason: 'peer done' }, 'closed');
    },
  },
  {
    rule: 'TA-9',
    name: 'TA-9 closed rejects with a session error on a QUIC-layer close',
    needs: ['quicClose'],
    async run(ctx) {
      const transport = await open(ctx);
      await ctx.peer.quicClose();
      expectSessionError(await rejects(transport.closed, 'closed'), 'closed');
    },
  },
  {
    rule: 'TA-9',
    name: 'TA-9 closed rejects with a session error on connection loss',
    needs: ['connectionLoss'],
    timeoutMs: 45_000,
    async run(ctx) {
      const transport = await open(ctx);
      await ctx.peer.loseConnection();
      expectSessionError(await rejects(transport.closed, 'closed', 40_000), 'closed');
    },
  },
  {
    rule: 'TA-9',
    name: 'TA-9 closed rejects with a session error when the dial fails',
    needs: ['refuseHandshake'],
    session: { handshake: 'refuse' },
    async run(ctx) {
      const { ready, transport } = ctx.dial();
      await rejects(ready, 'ready');
      expectSessionError(await rejects(transport.closed, 'closed'), 'closed');
    },
  },
  {
    rule: 'TA-10',
    name: 'TA-10 close sends its code and reason to the peer',
    async run(ctx) {
      const transport = await open(ctx);
      transport.close({ closeCode: 9, reason: 'bye' });
      equal(await resolves(ctx.peer.closed(), 'the peer seeing the close'), { closeCode: 9, reason: 'bye' }, 'peer');
    },
  },
  {
    rule: 'TA-10',
    name: 'TA-10 close without a code sends code 0 and an empty reason',
    async run(ctx) {
      const transport = await open(ctx);
      transport.close();
      equal(await resolves(transport.closed, 'closed'), { closeCode: 0, reason: '' }, 'closed');
      equal(await resolves(ctx.peer.closed(), 'the peer seeing the close'), { closeCode: 0, reason: '' }, 'peer');
    },
  },
  {
    rule: 'TA-10',
    name: 'TA-10 close is idempotent and does not throw after the session ended',
    async run(ctx) {
      const transport = await open(ctx);
      await ctx.peer.close(3, 'peer first');
      await resolves(transport.closed, 'closed');
      transport.close({ closeCode: 4, reason: 'late' });
      transport.close();
      equal(await transport.closed, { closeCode: 3, reason: 'peer first' }, 'closed after the late close calls');
    },
  },
  {
    rule: 'TA-11',
    name: 'TA-11 close before ready cancels the dial',
    needs: ['holdHandshake'],
    session: { handshake: 'hold' },
    async run(ctx) {
      const { ready, transport } = ctx.dial();
      await staysPending(ready, 'ready while the handshake is held');
      transport.close({ closeCode: 1, reason: 'deadline' });
      expectSessionError(await rejects(ready, 'ready'), 'ready');
      expectSessionError(await rejects(transport.closed, 'closed'), 'closed');
      await ctx.peer.releaseHandshake();
      await rejects(transport.createUnidirectionalStream(), 'createUnidirectionalStream after the cancelled dial');
    },
  },
  {
    rule: 'TA-12',
    name: 'TA-12 creates reject once the session has ended',
    async run(ctx) {
      const transport = await open(ctx);
      transport.close();
      await resolves(transport.closed, 'closed');
      await rejects(transport.createUnidirectionalStream(), 'createUnidirectionalStream');
      await rejects(transport.createBidirectionalStream(), 'createBidirectionalStream');
    },
  },
  {
    rule: 'TA-12',
    name: "TA-12 pending stream operations reject with a session error on the peer's close",
    async run(ctx) {
      const transport = await open(ctx);
      const { writer } = await openAcceptedUni(ctx, transport);
      const id = await ctx.peer.openUni();
      await ctx.peer.write(id, text('x'));
      const reader = (await nextIncoming(transport.incomingUnidirectionalStreams, 'the peer stream')).getReader();
      await readBytes(reader, 1);
      const read = later(reader.read());
      await ctx.peer.close(5, 'gone');
      expectSessionError(await rejects(read, 'the pending read'), 'the pending read');
      expectSessionError(await rejects(writer.closed, 'writer.closed'), 'writer.closed');
    },
  },
  {
    rule: 'TA-12',
    name: 'TA-12 pending stream operations reject with a session error on a QUIC-layer close',
    needs: ['quicClose'],
    async run(ctx) {
      const transport = await open(ctx);
      const { writer } = await openAcceptedUni(ctx, transport);
      await ctx.peer.quicClose();
      expectSessionError(await rejects(writer.closed, 'writer.closed'), 'writer.closed');
    },
  },
  {
    rule: 'TA-12',
    name: 'TA-12 incoming stream readables close after a clean end',
    async run(ctx) {
      const transport = await open(ctx);
      const uni = transport.incomingUnidirectionalStreams.getReader();
      const bidi = transport.incomingBidirectionalStreams.getReader();
      await ctx.peer.close(0, '');
      equal((await resolves(uni.read(), 'incoming uni read')).done, true, 'incoming uni done');
      equal((await resolves(bidi.read(), 'incoming bidi read')).done, true, 'incoming bidi done');
    },
  },
  {
    rule: 'TA-12',
    name: 'TA-12 incoming stream readables close after a clean end with no read pending',
    async run(ctx) {
      const transport = await open(ctx);
      const uni = transport.incomingUnidirectionalStreams.getReader();
      const bidi = transport.incomingBidirectionalStreams.getReader();
      await ctx.peer.close(0, '');
      await resolves(uni.closed, 'incoming uni closed');
      await resolves(bidi.closed, 'incoming bidi closed');
    },
  },
  {
    rule: 'TA-12',
    name: 'TA-12 incoming stream readables error after a QUIC-layer close with no read pending',
    needs: ['quicClose'],
    async run(ctx) {
      const transport = await open(ctx);
      const uni = transport.incomingUnidirectionalStreams.getReader();
      const bidi = transport.incomingBidirectionalStreams.getReader();
      await ctx.peer.quicClose();
      await rejects(uni.closed, 'incoming uni closed');
      await rejects(bidi.closed, 'incoming bidi closed');
    },
  },
  {
    rule: 'TA-12',
    name: 'TA-12 incoming stream readables error after a QUIC-layer close',
    needs: ['quicClose'],
    async run(ctx) {
      const transport = await open(ctx);
      const uni = transport.incomingUnidirectionalStreams.getReader();
      const bidi = transport.incomingBidirectionalStreams.getReader();
      await ctx.peer.quicClose();
      await rejects(uni.read(), 'incoming uni read');
      await rejects(bidi.read(), 'incoming bidi read');
    },
  },

  // ── Streams ──────────────────────────────────────────────────────────────────────────────────────────────
  {
    rule: 'TA-13',
    name: 'TA-13 peer streams arrive in order, bytes in order, FIN after the bytes',
    async run(ctx) {
      const transport = await open(ctx);
      const ids = [];
      for (const label of ['first', 'second', 'third']) {
        const id = await ctx.peer.openUni();
        ids.push(id);
        await ctx.peer.write(id, text(`${label}:`));
        await ctx.peer.write(id, text('a'.repeat(5000)));
        await ctx.peer.finish(id);
      }
      for (const label of ['first', 'second', 'third']) {
        const stream = await nextIncoming(transport.incomingUnidirectionalStreams, `the ${label} stream`);
        equal(asText(await readToEnd(stream.getReader())), `${label}:${'a'.repeat(5000)}`, `the ${label} stream`);
      }
    },
  },
  {
    rule: 'TA-13',
    name: 'TA-13 bidirectional streams carry bytes and FIN both ways',
    async run(ctx) {
      const transport = await open(ctx);
      const outgoing = await resolves(transport.createBidirectionalStream(), 'createBidirectionalStream');
      const writer = outgoing.writable.getWriter();
      await writer.write(text('ping'));
      await writer.close();
      const id = await ctx.peer.acceptBi();
      const { bytes, fin, resetCode } = await ctx.peer.read(id);
      equal({ bytes, fin, resetCode }, { bytes: [...text('ping')], fin: true, resetCode: null }, 'what the peer read');
      await ctx.peer.write(id, text('pong'));
      await ctx.peer.finish(id);
      equal(asText(await readToEnd(outgoing.readable.getReader())), 'pong', 'the reply');

      const peerOpened = await ctx.peer.openBi();
      await ctx.peer.write(peerOpened, text('hello'));
      await ctx.peer.finish(peerOpened);
      const incoming = await nextIncoming(transport.incomingBidirectionalStreams, 'the peer bidi stream');
      equal(asText(await readToEnd(incoming.readable.getReader())), 'hello', 'the peer bidi stream');
    },
  },
  {
    rule: 'TA-14',
    name: 'TA-14 a write resolves before the peer has read the bytes',
    async run(ctx) {
      const transport = await open(ctx);
      const writer = (await transport.createUnidirectionalStream()).getWriter();
      await holdPeer(ctx);
      await resolves(writer.write(text('accepted, not acknowledged')), 'a write the peer has not read');
      await releasePeer(ctx);
    },
  },
  {
    rule: 'TA-14',
    name: 'TA-14 a write stays pending while flow control holds the stream',
    timeoutMs: 30_000,
    async run(ctx) {
      const transport = await open(ctx);
      const { writer, id } = await openAcceptedUni(ctx, transport, text('x'));
      await holdPeer(ctx);
      const { write, written } = await writeUntilBlocked(writer);
      await staysPending(write, 'the blocked write');
      await releasePeer(ctx);
      const drained = later(ctx.peer.read(id, { bytes: written + 1, discard: true }));
      await resolves(write, 'the blocked write once the peer reads', 20_000);
      await resolves(drained, 'the peer reading everything', 20_000);
    },
  },
  {
    rule: 'TA-15',
    name: 'TA-15 close resolves only once the peer has acknowledged the FIN',
    needs: ['pause'],
    async run(ctx) {
      const transport = await open(ctx);
      const { writer } = await openAcceptedUni(ctx, transport);
      await ctx.peer.pause();
      const close = writer.close();
      await staysPending(close, 'close while the peer acknowledges nothing', 1_000);
      await ctx.peer.resume();
      await resolves(close, 'close once the peer resumed', 5_000);
    },
  },
  {
    rule: 'TA-15',
    name: 'TA-15 close rejects when the peer stops the stream before acknowledging it',
    needs: ['pause'],
    async run(ctx) {
      const transport = await open(ctx);
      const { writer, id } = await openAcceptedUni(ctx, transport);
      await ctx.peer.pause();
      const close = writer.close();
      await staysPending(close, 'close while the peer acknowledges nothing');
      await ctx.peer.stop(id, 3);
      await ctx.peer.resume();
      await rejects(close, 'close', 5_000);
    },
  },
  {
    rule: 'TA-15',
    name: 'TA-15 close rejects when the session ends before the FIN is acknowledged',
    needs: ['pause'],
    async run(ctx) {
      const transport = await open(ctx);
      const { writer } = await openAcceptedUni(ctx, transport);
      await ctx.peer.pause();
      const close = writer.close();
      await staysPending(close, 'close while the peer acknowledges nothing');
      await ctx.peer.close(0, '');
      await rejects(close, 'close', 5_000);
    },
  },
  {
    rule: 'TA-16',
    name: 'TA-16 abort resets the stream with the code',
    async run(ctx) {
      const transport = await open(ctx);
      const { writer, id } = await openAcceptedUni(ctx, transport);
      await writer.abort(ctx.reason(9));
      await rejects(writer.closed, 'writer.closed');
      equal((await ctx.peer.read(id)).resetCode, 9, 'the reset code the peer saw');
    },
  },
  {
    rule: 'TA-16',
    name: 'TA-16 abort resets at once while a write is blocked on flow control',
    timeoutMs: 30_000,
    async run(ctx) {
      const transport = await open(ctx);
      const { writer, id } = await openAcceptedUni(ctx, transport, text('x'));
      await holdPeer(ctx);
      const { write } = await writeUntilBlocked(writer);
      const abort = later(writer.abort(ctx.reason(4)));
      await rejects(write, 'the blocked write');
      await rejects(writer.closed, 'writer.closed');
      // abort() itself rejects when it interrupts a pending step (WHATWG streams); TA-16 asks only that it settle.
      check((await settleWithin(abort, PROMPTLY_MS)).state !== 'pending', 'abort still pending');
      await releasePeer(ctx);
      equal((await resolves(ctx.peer.read(id), 'the peer read', 10_000)).resetCode, 4, 'the reset code the peer saw');
    },
  },
  {
    rule: 'TA-16',
    name: 'TA-16 abort resets while close waits for acknowledgement',
    needs: ['pause'],
    async run(ctx) {
      const transport = await open(ctx);
      const { writer, id } = await openAcceptedUni(ctx, transport);
      await ctx.peer.pause();
      const close = writer.close();
      await staysPending(close, 'close while the peer acknowledges nothing');
      const abort = later(writer.abort(ctx.reason(6)));
      await rejects(close, 'the pending close');
      await rejects(writer.closed, 'writer.closed');
      // abort() itself rejects when it interrupts a pending step (WHATWG streams); TA-16 asks only that it settle.
      check((await settleWithin(abort, PROMPTLY_MS)).state !== 'pending', 'abort still pending');
      await ctx.peer.resume();
      const seen = await resolves(ctx.peer.read(id), 'the peer read', 5_000);
      // The peer dropped the FIN: it was paused before close(). A reset sender may not send it again (RFC 9000 §3.1,
      // §3.3), so a FIN here means the adapter let it win over the abort.
      check(seen.resetCode === 6 && !seen.fin, `the peer did not see only the reset: ${describe(seen)}`);
    },
  },
  {
    rule: 'TA-17',
    name: "TA-17 a reset carries the reason's streamErrorCode, up to 255",
    run: (ctx) => resetsCarry(ctx, LOW_CODES),
  },
  {
    rule: 'TA-17',
    name: "TA-17 a reset carries the reason's streamErrorCode, above 255",
    run: (ctx) => resetsCarry(ctx, HIGH_CODES),
  },
  {
    rule: 'TA-17',
    name: 'TA-17 a reset carries 0 when the reason has no valid code',
    async run(ctx) {
      const transport = await open(ctx);
      const reasons = [undefined, 'a string', ...INVALID_CODES.map((code) => ({ streamErrorCode: code }))];
      for (const reason of reasons) {
        const { writer, id } = await openAcceptedUni(ctx, transport);
        await writer.abort(reason);
        equal((await ctx.peer.read(id)).resetCode, 0, `the reset code for the reason ${describe(reason)}`);
      }
    },
  },
  {
    rule: 'TA-18',
    name: 'TA-18 cancel sends STOP_SENDING with the code, up to 255, and ends the readable',
    run: (ctx) => stopsCarry(ctx, LOW_CODES),
  },
  {
    rule: 'TA-18',
    name: 'TA-18 cancel sends STOP_SENDING with the code, above 255',
    run: (ctx) => stopsCarry(ctx, HIGH_CODES),
  },
  {
    rule: 'TA-18',
    name: 'TA-18 cancel sends code 0 when the reason has no valid code',
    async run(ctx) {
      const transport = await open(ctx);
      for (const reason of [undefined, { streamErrorCode: -1 }, { streamErrorCode: 0x1_0000_0000 }]) {
        const id = await ctx.peer.openUni();
        await ctx.peer.write(id, text('x'));
        const reader = (await nextIncoming(transport.incomingUnidirectionalStreams, 'the peer stream')).getReader();
        await reader.cancel(reason);
        equal(
          await resolves(ctx.peer.stopped(id), 'the peer seeing the stop'),
          0,
          `the stop code for ${describe(reason)}`,
        );
      }
    },
  },
  {
    rule: 'TA-19',
    name: "TA-19 a peer reset errors the receive stream with the peer's code",
    async run(ctx) {
      const transport = await open(ctx);
      const id = await ctx.peer.openUni();
      await ctx.peer.write(id, text('delivered'));
      const reader = (await nextIncoming(transport.incomingUnidirectionalStreams, 'the peer stream')).getReader();
      equal(asText(await readBytes(reader, 9)), 'delivered', 'the bytes before the reset');
      await ctx.peer.reset(id, 11);
      expectStreamError(await rejects(reader.read(), 'the read after the reset'), 11, 'the read after the reset');
    },
  },
  {
    rule: 'TA-19',
    name: "TA-19 a peer reset errors the receive stream with the peer's code while nothing reads it",
    async run(ctx) {
      const transport = await open(ctx);
      const id = await ctx.peer.openUni();
      await ctx.peer.write(id, text('unread'));
      const reader = (await nextIncoming(transport.incomingUnidirectionalStreams, 'the peer stream')).getReader();
      await ctx.peer.reset(id, 11);
      // The unread bytes may be discarded (RFC 9000 §3.2), but the reset must land without a read to carry it.
      expectStreamError(await rejects(reader.closed, 'reader.closed'), 11, 'reader.closed after the reset');
    },
  },
  {
    rule: 'TA-19',
    name: "TA-19 a peer reset errors a bidirectional stream's readable with the peer's code",
    async run(ctx) {
      const transport = await open(ctx);
      const stream = await transport.createBidirectionalStream();
      const writer = stream.writable.getWriter();
      await writer.write(text('open'));
      const id = await ctx.peer.acceptBi();
      await ctx.peer.reset(id, 0xffff_ffff);
      const reader = stream.readable.getReader();
      expectStreamError(await rejects(reader.read(), 'the read'), 0xffff_ffff, 'the read after the reset');
    },
  },
  {
    rule: 'TA-20',
    name: "TA-20 a peer stop rejects writer.closed with the peer's code while nothing is in flight",
    async run(ctx) {
      const transport = await open(ctx);
      const { writer, id } = await openAcceptedUni(ctx, transport);
      await ctx.peer.stop(id, 13);
      expectStreamError(await rejects(writer.closed, 'writer.closed'), 13, 'writer.closed');
      expectStreamError(await rejects(writer.write(text('late')), 'the next write'), 13, 'the next write');
    },
  },
  {
    rule: 'TA-20',
    name: "TA-20 a peer stop rejects a write blocked on flow control with the peer's code",
    timeoutMs: 30_000,
    async run(ctx) {
      const transport = await open(ctx);
      const { writer, id } = await openAcceptedUni(ctx, transport, text('x'));
      await holdPeer(ctx);
      const { write } = await writeUntilBlocked(writer);
      await ctx.peer.stop(id, 5);
      expectStreamError(await rejects(write, 'the blocked write'), 5, 'the blocked write');
      expectStreamError(await rejects(writer.closed, 'writer.closed'), 5, 'writer.closed');
      await releasePeer(ctx);
    },
  },
  {
    rule: 'TA-20',
    name: "TA-20 a peer stop on a bidirectional stream rejects its writer with the peer's code",
    async run(ctx) {
      const transport = await open(ctx);
      const stream = await transport.createBidirectionalStream();
      const writer = stream.writable.getWriter();
      await writer.write(text('open'));
      const id = await ctx.peer.acceptBi();
      await ctx.peer.stop(id, 2);
      expectStreamError(await rejects(writer.closed, 'writer.closed'), 2, 'writer.closed');
    },
  },
  {
    rule: 'TA-21',
    name: "TA-21 creates reject promptly at the peer's stream limit and succeed once credit returns",
    needs: ['maxStreams'],
    session: { maxStreams: 4 },
    async run(ctx) {
      const transport = await open(ctx);
      for (const kind of ['Unidirectional', 'Bidirectional']) {
        const create = () => transport[`create${kind}Stream`]();
        const opened = [];
        for (let i = 0; i < 4; i++) {
          const stream = await resolves(create(), `create ${kind} stream ${i + 1} of 4`);
          const writer = (stream.writable ?? stream).getWriter();
          await writer.write(text('x'));
          opened.push(writer);
        }
        await rejects(create(), `create ${kind} stream 5 of 4`);
        await opened[0].close();
        const id = await (kind === 'Unidirectional' ? ctx.peer.acceptUni() : ctx.peer.acceptBi());
        await ctx.peer.read(id);
        if (kind === 'Bidirectional') await ctx.peer.finish(id);
        const until = performance.now() + 5_000;
        let outcome;
        while (true) {
          outcome = await settleWithin(create(), PROMPTLY_MS);
          if (outcome.state !== 'rejected' || performance.now() >= until) break;
          // A rejection settles at once: yield to timers and I/O, or the MAX_STREAMS update never lands.
          await new Promise((resolve) => setTimeout(resolve, 10));
        }
        check(outcome.state === 'fulfilled', `a ${kind} create after credit returned: ${describe(outcome)}`);
        for (const writer of opened.slice(1)) await writer.abort(ctx.reason(0)).catch(() => {});
      }
    },
  },

  {
    rule: 'TA-28',
    name: 'TA-28 a send stream opened with a send order carries bytes and FIN',
    async run(ctx) {
      const transport = await open(ctx);
      for (const sendOrder of [-17, 0, 5, -(2 ** 31), 2 ** 31 - 1]) {
        const stream = await resolves(
          transport.createUnidirectionalStream({ sendOrder }),
          `createUnidirectionalStream with sendOrder ${sendOrder}`,
        );
        const writer = stream.getWriter();
        await writer.write(text(`order ${sendOrder}`));
        await writer.close();
        const id = await resolves(ctx.peer.acceptUni(), 'the peer accepting the stream');
        const { bytes, fin, resetCode } = await ctx.peer.read(id);
        equal(
          { bytes, fin, resetCode },
          { bytes: [...text(`order ${sendOrder}`)], fin: true, resetCode: null },
          `what the peer read with sendOrder ${sendOrder}`,
        );
      }
    },
  },
  {
    rule: 'TA-27',
    name: 'TA-27 a reset goes out in the HTTP/3 encoding',
    needs: ['native-quic', 'wireCodes'],
    async run(ctx) {
      const transport = await open(ctx);
      for (const code of TA_27_CODES) {
        const { writer, id } = await openAcceptedUni(ctx, transport);
        await writer.abort(ctx.reason(code));
        await ctx.peer.read(id);
        equal(await ctx.peer.wireCode(id), h3Code(code), `the wire code for ${code}`);
      }
    },
  },
  {
    rule: 'TA-27',
    name: 'TA-27 a stop goes out in the HTTP/3 encoding',
    needs: ['native-quic', 'wireCodes'],
    async run(ctx) {
      const transport = await open(ctx);
      for (const code of TA_27_CODES) {
        const id = await ctx.peer.openUni();
        await ctx.peer.write(id, text('x'));
        const reader = (await nextIncoming(transport.incomingUnidirectionalStreams, 'the peer stream')).getReader();
        await readBytes(reader, 1);
        await reader.cancel(ctx.reason(code));
        await resolves(ctx.peer.stopped(id), 'the peer seeing the stop');
        equal(await ctx.peer.wireCode(id), h3Code(code), `the wire code for ${code}`);
      }
    },
  },
  {
    rule: 'TA-27',
    name: 'TA-27 a peer reset in the HTTP/3 encoding is decoded',
    needs: ['native-quic', 'wireCodes'],
    async run(ctx) {
      const transport = await open(ctx);
      for (const [wire, code] of TA_27_PEER_CODES) {
        const id = await ctx.peer.openUni();
        await ctx.peer.write(id, text('x'));
        const reader = (await nextIncoming(transport.incomingUnidirectionalStreams, 'the peer stream')).getReader();
        await readBytes(reader, 1);
        await ctx.peer.reset(id, wire, { wire: true });
        expectExactStreamError(await rejects(reader.read(), 'the read'), code, `the read after wire code ${wire}`);
      }
    },
  },
  {
    rule: 'TA-27',
    name: 'TA-27 a peer stop in the HTTP/3 encoding is decoded',
    needs: ['native-quic', 'wireCodes'],
    async run(ctx) {
      const transport = await open(ctx);
      for (const [wire, code] of TA_27_PEER_CODES) {
        const { writer, id } = await openAcceptedUni(ctx, transport);
        await ctx.peer.stop(id, wire, { wire: true });
        expectExactStreamError(
          await rejects(writer.closed, 'writer.closed'),
          code,
          `writer.closed after wire code ${wire}`,
        );
      }
    },
  },

  // ── Error shape ──────────────────────────────────────────────────────────────────────────────────────────
  {
    rule: 'TA-22',
    name: 'TA-22 a code above 2^53-1 is reported as null',
    needs: ['native-quic', 'largeCodes', 'wireCodes'],
    async run(ctx) {
      const transport = await open(ctx);
      const id = await ctx.peer.openUni();
      await ctx.peer.write(id, text('x'));
      const reader = (await nextIncoming(transport.incomingUnidirectionalStreams, 'the peer stream')).getReader();
      await readBytes(reader, 1);
      await ctx.peer.reset(id, '4611686018427387903', { wire: true });
      expectStreamError(await rejects(reader.read(), 'the read'), null, 'the read after a 62-bit reset');
    },
  },
  {
    rule: 'TA-22',
    level: 'SHOULD',
    name: 'TA-22 stream and session errors are Error instances',
    async run(ctx) {
      const transport = await open(ctx);
      const id = await ctx.peer.openUni();
      await ctx.peer.write(id, text('x'));
      const reader = (await nextIncoming(transport.incomingUnidirectionalStreams, 'the peer stream')).getReader();
      await readBytes(reader, 1);
      await ctx.peer.reset(id, 1);
      const streamError = await rejects(reader.read(), 'the read');
      check(streamError instanceof Error, `the stream error is not an Error: ${describe(streamError)}`);
      const { writer } = await openAcceptedUni(ctx, transport);
      await ctx.peer.close(0, '');
      const sessionError = await rejects(writer.closed, 'writer.closed');
      check(sessionError instanceof Error, `the session error is not an Error: ${describe(sessionError)}`);
    },
  },
  {
    rule: 'TA-23',
    name: 'TA-23 a stream error leaves the session usable',
    async run(ctx) {
      const transport = await open(ctx);
      const { writer, id } = await openAcceptedUni(ctx, transport);
      await ctx.peer.stop(id, 1);
      expectStreamError(await rejects(writer.closed, 'writer.closed'), 1, 'writer.closed');
      await staysPending(transport.closed, 'closed after a stream error');
      const next = await openAcceptedUni(ctx, transport, text('still open'));
      await next.writer.close();
      equal(asText(new Uint8Array((await ctx.peer.read(next.id)).bytes)), 'still open', 'a stream after the error');
    },
  },
  {
    rule: 'TA-24',
    level: 'SHOULD',
    name: "TA-24 a session error after the peer's close carries its code and reason",
    async run(ctx) {
      const transport = await open(ctx);
      const { writer } = await openAcceptedUni(ctx, transport);
      await ctx.peer.close(0x10, 'unauthorized');
      const error = await rejects(writer.closed, 'writer.closed');
      expectSessionError(error, 'writer.closed');
      equal(
        { closeCode: error.closeCode, reason: error.reason },
        { closeCode: 0x10, reason: 'unauthorized' },
        'the error',
      );
    },
  },

  // ── Process lifetime ─────────────────────────────────────────────────────────────────────────────────────
  {
    rule: 'TA-25',
    name: 'TA-25 an open session keeps the process alive',
    needs: ['process'],
    timeoutMs: 30_000,
    run: (ctx) => ctx.target.checkProcessLifetime('keeps-alive'),
  },
  {
    rule: 'TA-26',
    name: 'TA-26 the process exits by itself once closed has settled',
    needs: ['process'],
    timeoutMs: 30_000,
    run: (ctx) => ctx.target.checkProcessLifetime('exits-after-close'),
  },
];

/** The reason the core would hand `abort()` or `cancel()` for `code`, unless the target builds its own. */
function defaultReason(code) {
  return Object.assign(new Error(`conformance reset ${code}`), { streamErrorCode: code });
}

/**
 * Whether and how `target` runs `testCase`: `{ run: false, reason }` when it lacks a capability or skips the rule,
 * `{ run: true, expectFailure }` when the rule is a known deviation or ahead of the code, else `{ run: true }`.
 */
export function planCase(target, testCase) {
  const missing = (testCase.needs ?? []).filter((need) => !target.capabilities.includes(need));
  if (missing.length > 0) {
    return { run: false, reason: missing.map((need) => target.gaps?.[need] ?? `no ${need} on this target`).join('; ') };
  }
  const expectation = target.expectations?.[testCase.name];
  if (expectation?.skip) return { run: false, reason: expectation.skip };
  if (expectation?.fails) return { run: true, expectFailure: expectation.fails };
  return { run: true };
}

/**
 * Run one case against `target` and report how it went: `passed`, `failed`, `skipped`, `expected-failure`,
 * `unexpected-pass` (an expected failure that now passes, to be promoted) or `unmet` (a SHOULD that failed, reported
 * but not a failure).
 */
export async function runCase(target, testCase) {
  const level = testCase.level ?? 'MUST';
  const result = { name: testCase.name, rule: testCase.rule, level };
  const plan = planCase(target, testCase);
  if (!plan.run) return { ...result, status: 'skipped', reason: plan.reason };

  const timeoutMs = testCase.timeoutMs ?? 10_000;
  // Tracked apart from `error`: a case may fail with any value, `undefined` included.
  let failed = false;
  let error;
  const session = await target.session(testCase.session ?? {});
  try {
    // A case that ends the session without reading `closed` must not leave its rejection unhandled.
    const dial = (options) => {
      const result = session.dial(options);
      result.ready.catch(() => {});
      result.transport.closed.catch(() => {});
      return result;
    };
    const ctx = { ...session, dial, target, reason: target.resetReason ?? defaultReason };
    const outcome = await settleWithin(testCase.run(ctx), timeoutMs);
    failed = outcome.state !== 'fulfilled';
    if (outcome.state === 'pending') error = new ConformanceFailure(`timed out after ${timeoutMs} ms`);
    if (outcome.state === 'rejected') error = outcome.reason;
  } finally {
    await session.dispose();
  }

  if (plan.expectFailure) {
    return failed
      ? { ...result, status: 'expected-failure', reason: plan.expectFailure, error }
      : { ...result, status: 'unexpected-pass', reason: plan.expectFailure };
  }
  if (!failed) return { ...result, status: 'passed' };
  return { ...result, status: level === 'SHOULD' ? 'unmet' : 'failed', error };
}

/** Run every case against `target`, one at a time, calling `report` with each result as it lands. */
export async function runSuite(target, { report = () => {} } = {}) {
  const results = [];
  for (const testCase of cases) {
    const result = await runCase(target, testCase);
    results.push(result);
    await report(result);
  }
  return results;
}
