/**
 * `@caprid/relay-quic`: the Relay SDK's transport adapter for Node. It speaks MOQT directly on a QUIC connection (native
 * QUIC, with the MOQT ALPNs it is given: `moqt-22`, then `moqt-20`), over quinn through a napi-rs addon, and follows the transport adapter contract
 * (`spec/transport-adapter.md`). Pass {@link nativeQuicTransport}'s factory as `createTransport`.
 *
 * @packageDocumentation
 */
import type { MoqtTransport } from '@caprid/moq-core';
import { loadAddon } from './addon.js';
import { NativeQuicTransport } from './transport.js';

/** The environment variable that turns qlog on when {@link NativeQuicTransportOptions.qlogDir} is absent. */
const QLOG_DIR_ENV = 'RELAY_QUIC_QLOG_DIR';

/** A certificate hash the server's end-entity certificate must match, in the W3C WebTransport shape. */
export interface CertificateHash {
  /** The hash algorithm; only `'sha-256'` is supported. */
  algorithm: 'sha-256';
  /** The SHA-256 of the certificate's DER encoding. */
  value: BufferSource;
}

/**
 * What {@link nativeQuicTransport}'s dials trust, how many streams they let the relay open, and where they write qlog.
 * With neither trust option, a dial trusts the platform's roots.
 */
export interface NativeQuicTransportOptions {
  /**
   * Extra trust anchors, PEM encoded, added to the platform's roots: one string or buffer may hold several
   * certificates.
   */
  ca?: string | Uint8Array | readonly (string | Uint8Array)[];
  /**
   * Accept exactly the end-entity certificates with these hashes instead of validating a chain, as W3C WebTransport's
   * `serverCertificateHashes` does. The TLS handshake signature is still verified; the 14-day validity limit browsers
   * apply is not.
   */
  serverCertificateHashes?: readonly CertificateHash[];
  /**
   * How many unidirectional streams the relay may have open to this connection at once: the MAX_STREAMS credit the
   * connection grants (RFC 9000 §4.6), an integer from 1 to 65535; 1000 when absent.
   *
   * The relay delivers each group of a track on a stream of its own, so a voice track at 50 frames a second takes 50
   * streams a second, and the credit for each closed stream returns over this end's uplink. 1000 is a 20 s stall of
   * that uplink on one such track; quinn's own default, 100, starved a subscriber whose upload queued its uplink for
   * 2 s. quinn sets aside an entry per allowed stream up front, which is the cost of a higher value. Unlike
   * WebTransport's option of the same name, which is a hint the browser may round up, this is the exact limit.
   */
  anticipatedConcurrentIncomingUnidirectionalStreams?: number;
  /**
   * A directory to write a qlog trace of every connection into, one `relay-quic-<ms>-<pid>-<n>.sqlog` file (JSON text
   * sequences, qlog 0.3) per connection attempt, created if it does not exist. Absent, the `RELAY_QUIC_QLOG_DIR`
   * environment variable is read when the factory is made; an empty string turns qlog off. Off by default. A trace
   * holds each packet's number, type and size, the packets lost, and the RTT and congestion window as they change;
   * quinn 0.11 logs no frames, so it holds no stream data and no token. A file that cannot be created is skipped, and
   * the dial goes on.
   */
  qlogDir?: string;
}

/** The transport a {@link NativeQuicConnection} carries: a `MoqtTransport` that reports the selected protocol. */
export interface NativeQuicMoqtTransport extends MoqtTransport {
  /** The ALPN identifier the peer selected; empty until `ready` resolves. */
  readonly protocol: string;
}

/** What one call of a {@link NativeQuicTransportFactory} returns: the shape the SDK's `createTransport` expects. */
export interface NativeQuicConnection {
  /** The transport the MOQT session runs on. */
  transport: NativeQuicMoqtTransport;
  /** Resolves once the QUIC handshake completed and the peer selected an offered protocol. */
  ready: Promise<void>;
  /** Always `'native-quic'`: the core sends the URL's path, query and authority in SETUP. */
  framing: 'native-quic';
}

/**
 * Dials `url`'s host and port (443 by default) over native QUIC, offering `protocols` as the ALPN list. The path and
 * query are not used here: the core carries them in SETUP.
 */
export type NativeQuicTransportFactory = (url: string, protocols: string[]) => NativeQuicConnection;

/** PEM text from a string or bytes. */
function pem(value: string | Uint8Array): string {
  return typeof value === 'string' ? value : new TextDecoder().decode(value);
}

function bytesOf(value: BufferSource): Uint8Array {
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
}

/**
 * A transport factory for the SDK's `createTransport` option that connects over native QUIC from Node:
 *
 * ```ts
 * import { connectRelayPublisher } from '@caprid/relay';
 * import { nativeQuicTransport } from '@caprid/relay-quic';
 *
 * const publisher = await connectRelayPublisher({ url, getToken, createTransport: nativeQuicTransport() });
 * ```
 *
 * Throws at once when `anticipatedConcurrentIncomingUnidirectionalStreams` is not an integer from 1 to 65535,
 * `qlogDir` is not a string or cannot be created, the native addon is missing for this platform, a `ca` entry holds no
 * PEM certificate, or a certificate hash is not a 32-byte SHA-256.
 */
export function nativeQuicTransport(options: NativeQuicTransportOptions = {}): NativeQuicTransportFactory {
  const streams = options.anticipatedConcurrentIncomingUnidirectionalStreams;
  if (streams !== undefined && !(Number.isInteger(streams) && streams >= 1 && streams <= 0xffff)) {
    throw new RangeError(
      `anticipatedConcurrentIncomingUnidirectionalStreams must be an integer from 1 to 65535, not ${streams}`,
    );
  }
  const qlogDir = options.qlogDir ?? process.env[QLOG_DIR_ENV];
  if (qlogDir !== undefined && typeof qlogDir !== 'string') throw new TypeError('qlogDir must be a string');
  const { Client } = loadAddon();
  const ca = options.ca === undefined ? [] : Array.isArray(options.ca) ? options.ca : [options.ca];
  const client = new Client({
    ca: (ca as readonly (string | Uint8Array)[]).map(pem),
    serverCertificateHashes: (options.serverCertificateHashes ?? []).map((hash) => {
      if (hash.algorithm !== 'sha-256') throw new TypeError(`unsupported certificate hash algorithm ${hash.algorithm}`);
      return bytesOf(hash.value);
    }),
    ...(streams === undefined ? {} : { maxConcurrentUniStreams: streams }),
    ...(qlogDir ? { qlogDir } : {}),
  });

  return (url, protocols) => {
    // Only the host and port are read (TA-3): the path and query, token included, travel in SETUP.
    const { hostname, port } = new URL(url);
    const host = hostname.startsWith('[') ? hostname.slice(1, -1) : hostname;
    const transport = new NativeQuicTransport(client.dial(host, port === '' ? 443 : Number(port), host, protocols));
    return { transport, ready: transport.ready, framing: 'native-quic' };
  };
}
