/**
 * The browser's native `WebTransport` as a conformance target. It runs in a Chromium page (`scripts/test-conformance.mjs`
 * loads it); its peer is the scriptable Python accepting end, reached through `call(method, params)`, which the launcher
 * exposes to the page.
 */
import { streamResetReason } from '@mux/moq-core';
import { createRemotePeer } from './remote-peer.js';

/** Rules Chromium is known not to meet, each with its Known deviation in `spec/transport-adapter.md`. */
const protocolDeviation = {
  fails:
    'Known deviations: Chromium resolves ready without a selected protocol (TA-7); the core checks protocol (#203)',
};
const codeDeviation = { fails: 'Known deviations: Chromium sends stream error codes as 8 bits (TA-17, TA-18)' };
const expectations = {
  'TA-7 ready rejects with a session error when the peer selects no protocol': protocolDeviation,
  'TA-7 ready rejects with a session error when the peer selects a protocol that was not offered': protocolDeviation,
  "TA-17 a reset carries the reason's streamErrorCode, above 255": codeDeviation,
  'TA-18 cancel sends STOP_SENDING with the code, above 255': codeDeviation,
  "TA-24 a session error after the peer's close carries its code and reason": {
    fails: 'Known deviations: Browser session close codes (TA-24, #14)',
  },
};

/**
 * @param {object} options
 * @param {number} options.port The accepting end's UDP port on 127.0.0.1.
 * @param {string} options.certificateHash The SHA-256 of its certificate, in hex.
 * @param {(method: string, params?: object) => Promise<unknown>} options.call Sends one request to the accepting end.
 */
export function createChromiumTarget({ port, certificateHash, call }) {
  const value = Uint8Array.from(certificateHash.match(/../g), (byte) => Number.parseInt(byte, 16));
  const origin = `https://127.0.0.1:${port}`;
  const peer = createRemotePeer(call);

  return {
    name: 'Chromium WebTransport',
    framing: 'webtransport',
    capabilities: [
      'webtransport',
      'selectProtocol',
      'holdHandshake',
      'refuseHandshake',
      'quicClose',
      'connectionLoss',
      'pause',
      'maxStreams',
    ],
    gaps: {
      'native-quic': 'a browser speaks WebTransport only',
      largeCodes: 'only native QUIC carries codes above 2^32-1',
      process: 'a browser has no process to keep alive (TA-25, TA-26 do not apply)',
    },
    expectations,
    // The native WebTransport reads a code only from a native WebTransportError (TA-17, Known deviations).
    resetReason: (code) => streamResetReason(code, `conformance reset ${code}`),
    async session({ handshake, select, maxStreams } = {}) {
      await call('prepare', { handshake, select, maxStreams });
      return {
        peer,
        dial({ path = '/conformance', protocols = ['moqt-20'] } = {}) {
          const url = origin + path;
          const transport = new WebTransport(url, {
            protocols,
            serverCertificateHashes: [{ algorithm: 'sha-256', value }],
          });
          return { transport, ready: transport.ready, url };
        },
        dispose: () => call('dispose'),
      };
    },
  };
}
