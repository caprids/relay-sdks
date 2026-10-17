/**
 * The transport adapter conformance suite's accepting end for QMux over WebSocket (#321), the counterpart of the Python
 * accepting end (`python/…/relay_helpers/conformance_peer.py`) for the WebSocket targets. It speaks the same JSON lines
 * on stdin and stdout (`conformance/README.md`, "The Python accepting end"), so `scripts/conformance-peer.js` starts it
 * and `conformance/targets/remote-peer.js` drives it. QMux comes from `@moq/qmux`'s accepting role; the WebSocket server
 * from `ws`, over TLS.
 *
 *     node scripts/websocket-peer.js <certificate.pem> <key.pem> [host]
 *
 * It prints `{"event": "listening", "port", "certificateHash", "certificatePem"}` once it listens.
 */
import { createHash, X509Certificate } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { createServer } from 'node:https';
import { createInterface } from 'node:readline';
import Session, { StreamError } from '@moq/qmux';
import { WebSocketServer } from 'ws';

// `::` takes IPv4 too, so a dialer that resolves `localhost` to either address reaches it.
const [certificateFile, keyFile, host = '::'] = process.argv.slice(2);
const certificatePem = readFileSync(certificateFile, 'utf8');
const certificateHash = createHash('sha256').update(new X509Certificate(certificatePem).raw).digest('hex');

/** The QMux wire format the SDK offers its MOQT protocols on. */
const QMUX_PREFIX = 'qmux-01.';

/** What `prepare` set up for the next connection. */
let plan = { handshake: 'accept', select: 'first', maxStreams: undefined };
/** The current connection, once one was accepted. */
let current;
/** Releases every upgrade the peer holds: a browser may open more than one connection for a dial. */
const held = [];
let observed;
/** Settles with the next accepted connection. */
let nextConnection = Promise.withResolvers();

const streams = new Map();
let nextStream = 0;

const server = createServer({ cert: certificatePem, key: readFileSync(keyFile) });
const sockets = new WebSocketServer({
  noServer: true,
  handleProtocols(protocols) {
    if (plan.select === 'none') return false;
    if (plan.select === 'unoffered') return `${QMUX_PREFIX}moqt-unoffered`;
    return protocols.values().next().value ?? false;
  },
});

server.on('upgrade', async (request, socket, head) => {
  const offered = (request.headers['sec-websocket-protocol'] ?? '')
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);
  observed = { path: request.url, protocols: offered };
  if (plan.handshake === 'refuse') {
    socket.end('HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n');
    return;
  }
  if (plan.handshake === 'hold') {
    await new Promise((resolve) => {
      held.push(resolve);
    });
    if (socket.destroyed) return;
  }
  sockets.handleUpgrade(request, socket, head, (ws) => {
    const config =
      plan.maxStreams === undefined
        ? undefined
        : { maxStreamsUni: BigInt(plan.maxStreams), maxStreamsBidi: BigInt(plan.maxStreams) };
    const session = Session.accept(ws, { protocol: ws.protocol, ...(config ? { config } : {}) });
    session.closed.catch(() => {});
    current = {
      ws,
      session,
      uni: session.incomingUnidirectionalStreams.getReader(),
      bidi: session.incomingBidirectionalStreams.getReader(),
    };
    nextConnection.resolve(current);
  });
});

function forget() {
  for (const stream of streams.values()) {
    stream.reader?.cancel().catch(() => {});
    stream.writer?.abort().catch(() => {});
  }
  streams.clear();
  if (current) {
    current.ws.terminate();
    current = undefined;
  }
  for (const release of held.splice(0)) release();
  observed = undefined;
  nextConnection = Promise.withResolvers();
}

function track(entry) {
  const id = nextStream++;
  streams.set(id, entry);
  return id;
}

function stream(id) {
  const entry = streams.get(id);
  if (!entry) throw new Error(`no stream ${id}`);
  return entry;
}

const connection = () => current ?? nextConnection.promise;

/** Read `stream` until its FIN, a reset, or `count` bytes. */
async function read({ stream: id, count, discard }) {
  const { reader } = stream(id);
  const chunks = [];
  let byteLength = 0;
  let fin = false;
  let resetCode = null;
  while (count === undefined || byteLength < count) {
    try {
      const { done, value } = await reader.read();
      if (done) {
        fin = true;
        break;
      }
      byteLength += value.byteLength;
      if (!discard) chunks.push(...value);
    } catch (error) {
      resetCode = error?.source === 'stream' ? error.streamErrorCode : null;
      break;
    }
  }
  return { bytes: chunks, byteLength, fin, resetCode };
}

/** A QMux CONNECTION_CLOSE (0x1c) with INTERNAL_ERROR, frame type 0 and no reason: the transport-layer close. */
const TRANSPORT_CLOSE = Uint8Array.of(0x1c, 0x01, 0x00, 0x00);

const methods = {
  prepare({ handshake = 'accept', select = 'first', maxStreams } = {}) {
    forget();
    plan = { handshake, select, maxStreams };
    return null;
  },
  dispose() {
    forget();
    return null;
  },
  async acceptUni() {
    const { uni } = await connection();
    const { value } = await uni.read();
    return track({ reader: value.getReader() });
  },
  async acceptBi() {
    const { bidi } = await connection();
    const { value } = await bidi.read();
    return track({ reader: value.readable.getReader(), writer: value.writable.getWriter() });
  },
  async openUni() {
    const { session } = await connection();
    return track({ writer: (await session.createUnidirectionalStream()).getWriter() });
  },
  async openBi() {
    const { session } = await connection();
    const opened = await session.createBidirectionalStream();
    return track({ reader: opened.readable.getReader(), writer: opened.writable.getWriter() });
  },
  async write({ stream: id, data }) {
    await stream(id).writer.write(Uint8Array.from(data));
    return null;
  },
  async finish({ stream: id }) {
    await stream(id).writer.close();
    return null;
  },
  async reset({ stream: id, code }) {
    await stream(id)
      .writer.abort(new StreamError(code))
      .catch(() => {});
    return null;
  },
  async stop({ stream: id, code }) {
    await stream(id)
      .reader.cancel(new StreamError(code))
      .catch(() => {});
    return null;
  },
  read,
  async stopped({ stream: id }) {
    try {
      await stream(id).writer.closed;
      return null;
    } catch (error) {
      return error?.source === 'stream' ? error.streamErrorCode : null;
    }
  },
  async close({ code, reason }) {
    (await connection()).session.close({ closeCode: code, reason });
    return null;
  },
  async closed() {
    const info = await (await connection()).session.closed;
    return { closeCode: info.closeCode, reason: info.reason };
  },
  async quicClose() {
    const { ws } = await connection();
    ws.send(TRANSPORT_CLOSE);
    ws.close();
    return null;
  },
  async loseConnection() {
    // The TCP connection goes without a WebSocket close frame or a QMux close.
    (await connection()).ws._socket.destroy();
    return null;
  },
  async pause() {
    (await connection()).ws._socket.pause();
    return null;
  },
  async resume() {
    (await connection()).ws._socket.resume();
    return null;
  },
  releaseHandshake() {
    // An upgrade that arrives after the release (a browser may open a second connection) is not held either.
    plan = { ...plan, handshake: 'accept' };
    for (const release of held.splice(0)) release();
    return null;
  },
  observedDial() {
    return observed ?? null;
  },
};

server.listen(0, host, () => {
  const { port } = server.address();
  process.stdout.write(`${JSON.stringify({ event: 'listening', port, certificateHash, certificatePem })}\n`);
});

createInterface({ input: process.stdin })
  .on('line', async (line) => {
    const { id, method, params } = JSON.parse(line);
    try {
      const handler = methods[method];
      if (!handler) throw new Error(`unknown method ${method}`);
      const result = await handler(params ?? {});
      process.stdout.write(`${JSON.stringify({ id, result: result ?? null })}\n`);
    } catch (error) {
      process.stdout.write(
        `${JSON.stringify({ id, error: error instanceof Error ? error.message : String(error) })}\n`,
      );
    }
  })
  .on('close', () => {
    forget();
    sockets.close();
    server.close();
    server.closeAllConnections();
  });
