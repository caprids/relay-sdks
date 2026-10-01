/**
 * The in-memory transport pair as a conformance target. The dialer is the pair's client end behind
 * `createTransportPairFactory`; the peer is its server end, driven in-process.
 */
import { createTransportPairFactory } from '../../packages/moq-core/src/tests/helpers/transport-pair.js';

const ORIGIN = 'https://pair.conformance.invalid';

/** The reason a peer step hands the pair's `abort()` or `cancel()` for `code`. */
const peerReason = (code) => Object.assign(new Error(`peer ${code}`), { streamErrorCode: code });

/** Drive the server end of the pair `factory` dials as the accepting end. */
function createPairPeer(factory) {
  const streams = new Map();
  let nextId = 0;
  let incomingUni;
  let incomingBidi;
  const server = async () => (await factory.dialled).pair.server;
  const add = (stream) => {
    const id = nextId++;
    streams.set(id, stream);
    return id;
  };
  const writerOf = (id) => {
    const stream = streams.get(id);
    stream.writer ??= stream.writable.getWriter();
    return stream.writer;
  };
  const readerOf = (id) => {
    const stream = streams.get(id);
    stream.reader ??= stream.readable.getReader();
    return stream.reader;
  };
  const accept = async (kind) => {
    const transport = await server();
    if (kind === 'uni') incomingUni ??= transport.incomingUnidirectionalStreams.getReader();
    else incomingBidi ??= transport.incomingBidirectionalStreams.getReader();
    const { done, value } = await (kind === 'uni' ? incomingUni : incomingBidi).read();
    if (done) throw new Error('the session ended before a stream arrived');
    return add(kind === 'uni' ? { readable: value } : value);
  };

  return {
    acceptUni: () => accept('uni'),
    acceptBi: () => accept('bidi'),
    async openUni() {
      return add({ writable: await (await server()).createUnidirectionalStream() });
    },
    async openBi() {
      return add(await (await server()).createBidirectionalStream());
    },
    write: (id, bytes) => writerOf(id).write(bytes),
    finish: (id) => writerOf(id).close(),
    async reset(id, code) {
      await writerOf(id).abort(peerReason(code));
    },
    async stop(id, code) {
      await readerOf(id).cancel(peerReason(code));
    },
    async read(id, { bytes, discard = false } = {}) {
      const reader = readerOf(id);
      const out = [];
      let byteLength = 0;
      const result = (fin, resetCode) => ({ bytes: out, byteLength, fin, resetCode });
      try {
        while (bytes === undefined || byteLength < bytes) {
          const { done, value } = await reader.read();
          if (done) return result(true, null);
          byteLength += value.byteLength;
          if (!discard) out.push(...value);
        }
        return result(false, null);
      } catch (error) {
        if (error?.source !== 'stream') throw error;
        return result(false, error.streamErrorCode);
      }
    },
    async stopped(id) {
      try {
        await writerOf(id).closed;
      } catch (error) {
        if (error?.source === 'stream') return error.streamErrorCode;
        throw error;
      }
      throw new Error('the stream closed without a stop');
    },
    async close(closeCode, reason) {
      (await server()).close({ closeCode, reason });
    },
    async closed() {
      return (await server()).closed;
    },
    async quicClose() {
      (await factory.dialled).pair.fail('The peer closed the connection with a transport error.');
    },
    async loseConnection() {
      (await factory.dialled).pair.fail('The connection was lost.');
    },
    async releaseHandshake() {
      factory.releaseHandshake();
    },
    async observed() {
      const { url, protocols } = await factory.dialled;
      const parsed = new URL(url);
      return { path: parsed.pathname + parsed.search, protocols };
    },
  };
}

/** The in-memory pair, with its gaps from the contract's "Test doubles" section as the reasons its cases skip. */
export function createPairTarget() {
  return {
    name: 'in-memory pair',
    framing: 'webtransport',
    capabilities: ['webtransport', 'selectProtocol', 'holdHandshake', 'refuseHandshake', 'quicClose', 'connectionLoss'],
    gaps: {
      pause:
        'the pair has no acknowledgement to withhold: close resolves once the FIN is in the peer buffer (Test doubles, TA-15)',
      maxStreams: 'the pair has no stream credit (Test doubles, TA-21)',
      'native-quic': 'the pair stands in for a WebTransport session',
      largeCodes: 'only native QUIC carries codes above 2^32-1',
      process: 'an in-process target has no process lifetime of its own',
    },
    expectations: {},
    resetReason: (code) => Object.assign(new Error(`conformance reset ${code}`), { streamErrorCode: code }),
    async session({ handshake, select } = {}) {
      const factory = createTransportPairFactory({
        ...(handshake === undefined ? {} : { handshake }),
        ...(select === undefined ? {} : { select }),
      });
      let dialled;
      return {
        peer: createPairPeer(factory),
        dial({ path = '/conformance', protocols = ['moqt-20'] } = {}) {
          const url = ORIGIN + path;
          dialled = factory.createTransport(url, protocols);
          return { ...dialled, url };
        },
        async dispose() {
          dialled?.transport.close();
        },
      };
    },
  };
}
