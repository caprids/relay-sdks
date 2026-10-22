/**
 * `@caprid/relay-websocket` as a conformance target: QMux over WebSocket (#321). Runtime-neutral, so the same target runs
 * in Node (`../websocket.conformance.js`) and in a browser page (`../../scripts/test-conformance-websocket.mjs`). Its
 * peer is the Node accepting end, `scripts/websocket-peer.js`, reached through `call(method, params)`.
 */
import { createWebSocketTransport } from '@caprid/relay-websocket';
import { createRemotePeer } from './remote-peer.js';

/** QMux over a WebSocket rides on TCP, which acknowledges nothing an application can see (Known deviations). */
const acknowledgement = {
  fails:
    'Known deviations: QMux over WebSocket has no acknowledgement; close resolves once the FIN is handed to the socket (TA-15)',
};

/** Rules this adapter does not meet, each with its Known deviation in `spec/transport-adapter.md`. */
const expectations = {
  'TA-15 close resolves only once the peer has acknowledged the FIN': acknowledgement,
  'TA-15 close rejects when the peer stops the stream before acknowledging it': acknowledgement,
  'TA-15 close rejects when the session ends before the FIN is acknowledged': acknowledgement,
  'TA-16 abort resets while close waits for acknowledgement': acknowledgement,
  "TA-19 a peer reset errors the receive stream with the peer's code while nothing reads it": {
    fails:
      'Known deviations: @moq/qmux reports a peer reset only to a read; behind an unread chunk it lands at the next read (TA-19)',
  },
  "TA-21 creates reject promptly at the peer's stream limit and succeed once credit returns": {
    fails: 'Known deviations: @moq/qmux waits for stream credit rather than rejecting (TA-21)',
  },
};

/**
 * @param {object} options
 * @param {string} options.name Shown in the test names.
 * @param {number} options.port The accepting end's TCP port.
 * @param {string} [options.host] The host to dial, which the peer's certificate names: `localhost` by default.
 * @param {(method: string, params?: object) => Promise<unknown>} options.call Sends one request to the accepting end.
 */
export function createWebSocketTarget({ name, port, host = 'localhost', call }) {
  const origin = `https://${host}:${port}`;
  const peer = createRemotePeer(call);
  const factory = createWebSocketTransport();

  return {
    name,
    framing: 'websocket',
    capabilities: [
      'websocket',
      'selectProtocol',
      'holdHandshake',
      'refuseHandshake',
      'quicClose',
      'connectionLoss',
      'pause',
      'maxStreams',
    ],
    gaps: {
      webtransport: 'a WebSocket carries no WebTransport session',
      'native-quic': 'a WebSocket carries no native QUIC',
      largeCodes: 'only native QUIC carries codes above 2^32-1',
      wireCodes: "TA-27 is native QUIC's: QMux carries stream codes as they are",
      process: 'a browser has no process to keep alive (TA-25, TA-26 do not apply)',
    },
    expectations,
    /** The factory the target dials with, for checks outside the suite. */
    dialFactory: factory,
    origin,
    async session({ handshake, select, maxStreams } = {}) {
      await call('prepare', { handshake, select, maxStreams });
      return {
        peer,
        dial({ path = '/conformance', protocols = ['moqt-22'] } = {}) {
          const url = origin + path;
          return { ...factory(url, protocols), url };
        },
        dispose: () => call('dispose'),
      };
    },
  };
}
