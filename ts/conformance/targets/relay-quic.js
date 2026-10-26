/**
 * `@caprid/relay-quic` as a conformance target: the Node transport adapter over native QUIC. It runs under vitest
 * (`../relay-quic.conformance.js`); its peer is the Python accepting end with `--framing native-quic`, reached through
 * `call(method, params)`. TA-25 and TA-26 run the adapter in a child Node process (`relay-quic-lifetime.js`).
 */
import { fileURLToPath } from 'node:url';
import { nativeQuicTransport } from '@caprid/relay-quic';
import { checkProcessLifetime } from './lifetime.js';
import { createRemotePeer } from './remote-peer.js';

/** The host the target dials: a DNS name, so TLS carries it as SNI and the peer can report it (TA-3). */
const HOST = 'localhost';

const lifetimeScript = fileURLToPath(new URL('./relay-quic-lifetime.js', import.meta.url));

/** Rules this adapter does not meet, each with its reason. */
const expectations = {};

/**
 * @param {object} options
 * @param {number} options.port The accepting end's UDP port.
 * @param {string} options.certificateHash The SHA-256 of its certificate, in hex.
 * @param {(method: string, params?: object) => Promise<unknown>} options.call Sends one request to the accepting end.
 */
export function createRelayQuicTarget({ port, certificateHash, call }) {
  const value = Uint8Array.from(certificateHash.match(/../g), (byte) => Number.parseInt(byte, 16));
  const origin = `https://${HOST}:${port}`;
  const peer = createRemotePeer(call);
  const factory = nativeQuicTransport({ serverCertificateHashes: [{ algorithm: 'sha-256', value }] });

  return {
    name: '@caprid/relay-quic',
    framing: 'native-quic',
    host: HOST,
    capabilities: [
      'native-quic',
      'selectProtocol',
      'holdHandshake',
      'refuseHandshake',
      'quicClose',
      'connectionLoss',
      'pause',
      'maxStreams',
      'largeCodes',
      'wireCodes',
      'process',
    ],
    gaps: { webtransport: 'native QUIC carries no WebTransport session' },
    /** The factory the target dials with, for checks outside the suite. */
    dialFactory: factory,
    expectations,
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
    checkProcessLifetime: (kind) => checkProcessLifetime(lifetimeScript, [origin, certificateHash, kind], kind),
  };
}
