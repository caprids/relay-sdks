/**
 * `@mux/relay-quic` as a conformance target: the Node transport adapter over native QUIC. It runs under vitest
 * (`../relay-quic.conformance.js`); its peer is the Python accepting end with `--framing native-quic`, reached through
 * `call(method, params)`. TA-25 and TA-26 run the adapter in a child Node process (`relay-quic-lifetime.js`).
 */
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { nativeQuicTransport } from '@mux/relay-quic';
import { createRemotePeer } from './remote-peer.js';

/** The host the target dials: a DNS name, so TLS carries it as SNI and the peer can report it (TA-3). */
const HOST = 'localhost';

/** How long TA-25 watches an open session's process, and how long TA-26 gives a closed one to exit. */
const LIFETIME_MS = 2_500;
const EXIT_MS = 10_000;

const lifetimeScript = fileURLToPath(new URL('./relay-quic-lifetime.js', import.meta.url));

/** Rules this adapter does not meet, each with its reason. */
const expectations = {};

/**
 * Run the lifetime child against the peer and report how it ended: `{ code, signal, lines, alive }`, where `alive`
 * says whether it was still running `LIFETIME_MS` after it reported `ready`.
 */
function runLifetimeChild({ origin, certificateHash, kind }) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [lifetimeScript, origin, certificateHash, kind], {
      stdio: ['ignore', 'pipe', 'inherit'],
    });
    const lines = [];
    let alive;
    let deadline;
    child.stdout.setEncoding('utf8').on('data', (text) => {
      lines.push(...text.split('\n').filter(Boolean));
      if (kind === 'keeps-alive' && lines.includes('ready') && alive === undefined) {
        setTimeout(() => {
          alive = child.exitCode === null && child.signalCode === null;
          child.kill();
        }, LIFETIME_MS);
      }
    });
    deadline = setTimeout(() => {
      child.kill();
      deadline = undefined;
    }, LIFETIME_MS + EXIT_MS);
    child.once('error', reject);
    child.once('exit', (code, signal) => {
      if (deadline) clearTimeout(deadline);
      resolve({ code, signal, lines, alive, timedOut: deadline === undefined });
    });
  });
}

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
    name: '@mux/relay-quic',
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
        dial({ path = '/conformance', protocols = ['moqt-20'] } = {}) {
          const url = origin + path;
          return { ...factory(url, protocols), url };
        },
        dispose: () => call('dispose'),
      };
    },
    async checkProcessLifetime(kind) {
      const { code, signal, lines, alive, timedOut } = await runLifetimeChild({ origin, certificateHash, kind });
      if (kind === 'keeps-alive') {
        if (!lines.includes('ready')) throw new Error(`the child never connected (exit ${code ?? signal})`);
        if (!alive) throw new Error(`the process exited with the session open (exit ${code ?? signal})`);
        return;
      }
      if (!lines.includes('closed')) throw new Error(`the child never closed its session (exit ${code ?? signal})`);
      if (timedOut) throw new Error(`the process was still running ${EXIT_MS} ms after closed settled`);
      // 13 is Node's exit code for a top-level await that never settled: something stopped holding it too early.
      if (code !== 0) throw new Error(`the process exited with ${code ?? signal}, not 0`);
    },
  };
}
