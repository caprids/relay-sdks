import { isStreamError, SESSION_ERROR } from '@mux/moq-core';

/** Stable categories for SDK lifecycle failures; messages are diagnostic text only. */
export type RelayErrorCode =
  | 'CONNECTION_TIMEOUT'
  | 'WRITE_TIMEOUT'
  /**
   * An object outlived its delivery timeout (draft-ietf-moq-transport-20 §8) before it reached the transport: the
   * smaller non-zero of the publisher's OBJECT_DELIVERY_TIMEOUT (`0x02`) and the relay subscription's. The data is
   * stale; do not retry it as it is.
   */
  | 'DELIVERY_TIMEOUT'
  | 'CLOSED'
  | 'WRITE_PENDING'
  | 'NAMESPACE_NOT_SOLICITED'
  | 'NAMESPACE_ENDED'
  | 'SUBSCRIPTION_CHANGED'
  | 'SUBSCRIPTION_REQUEST_FAILED'
  | 'SUBSCRIPTION_OVERFLOW'
  | 'UPDATE_QUEUE_FULL'
  | 'GOAWAY'
  | 'TRANSPORT_ERROR'
  | 'TRANSPORT_UNAVAILABLE'
  /**
   * The relay refused the connection's credentials, in phase `connecting`. Over native QUIC (`@mux/relay/node`) a
   * refusal always reports this. Browsers do not expose moq-relay 0.15.8's CLOSE_WEBTRANSPORT_SESSION code, so there
   * a refusal arrives as `TRANSPORT_ERROR` until the relay lets the capsule through (muxinc/relay-sdks#14).
   */
  | 'UNAUTHORIZED';

/** Where the failure originated, not a delivery acknowledgment or a safe-retry flag. */
export type RelayErrorPhase =
  | 'connecting'
  | 'connection'
  | 'subscription'
  | 'waiting-for-demand'
  | 'opening-stream'
  | 'writing'
  | 'closing';

/** Caller cancellation reasons and argument errors are not converted to RelayError. */
export class RelayError extends Error {
  /** `'RelayError'`, or the subclass name for the three subscription errors. */
  override readonly name: string = 'RelayError';
  /** The stable failure category; branch on this, not on `message` or the class. */
  readonly code: RelayErrorCode;
  /** The step that was running when the failure happened. */
  readonly phase: RelayErrorPhase;

  /**
   * @param code The stable failure category.
   * @param message Diagnostic text; not a compatibility contract.
   * @param phase The step that was running when the failure happened.
   * @param options `cause` carries the wrapped transport or session failure.
   */
  constructor(code: RelayErrorCode, message: string, phase: RelayErrorPhase, options?: ErrorOptions) {
    super(message, options);
    this.code = code;
    this.phase = phase;
  }
}

/**
 * Whether a send-stream failure is the peer's STOP_SENDING: a stream error (TA-20, TA-22), whatever its class or name.
 * Chromium sometimes reports the stop instead as a `NetworkError` DOMException saying the remote side aborted the
 * stream, with no `source` and no code, which is accepted too (spec/transport-adapter.md, Known deviations). A session
 * failure has `source` `'session'`.
 */
export function isStreamStop(error: unknown): boolean {
  if (isStreamError(error)) return true;
  return (
    error instanceof Error &&
    error.name === 'NetworkError' &&
    error.message === 'The stream was aborted by the remote server'
  );
}

/**
 * MOQT session termination codes (draft-20 §3.5) that mean the relay refused the connection's credentials. moq-relay
 * sends `UNAUTHORIZED` (0x2) for every refusal: no token, a bad signature, a malformed or expired token, a path outside
 * the token's root. The core never closes with any of them itself.
 */
const CREDENTIAL_REFUSALS: ReadonlySet<number> = new Set([
  SESSION_ERROR.UNAUTHORIZED,
  SESSION_ERROR.MALFORMED_AUTH_TOKEN,
  SESSION_ERROR.EXPIRED_AUTH_TOKEN,
]);

/**
 * The peer's close, when it closed the session because of the credentials: `closed` resolving with the code (TA-9), or
 * a session error carrying it (TA-24).
 */
function credentialRefusal(value: unknown): { closeCode: number; reason: string } | undefined {
  if (typeof value !== 'object' || value === null) return undefined;
  const { source, closeCode, reason } = value as { source?: unknown; closeCode?: unknown; reason?: unknown };
  // A `closed` resolution has no source; an error that has one must be a session error (TA-22).
  if (source !== undefined && source !== 'session') return undefined;
  if (typeof closeCode !== 'number' || !CREDENTIAL_REFUSALS.has(closeCode)) return undefined;
  return { closeCode, reason: typeof reason === 'string' ? reason : '' };
}

/**
 * Wrap a failure while connecting: a credentials refusal is `UNAUTHORIZED`, anything else a transport error.
 * `peerClose` is what the transport's `closed` resolved with, if it has; the session reports a clean close before
 * server SETUP without its code. Code 2 after SETUP is not a refusal, so the connected phase never comes here.
 */
export function connectingError(error: unknown, peerClose?: unknown): RelayError {
  if (error instanceof RelayError) return error;
  const refusal = credentialRefusal(error) ?? credentialRefusal(peerClose);
  if (refusal === undefined) return transportError(error, 'connecting');
  return new RelayError(
    'UNAUTHORIZED',
    `Relay refused the connection's credentials: ${refusal.reason || 'no reason given'} (session closed with code 0x${refusal.closeCode.toString(16)}); check the token and that it grants this endpoint's path`,
    'connecting',
    { cause: error },
  );
}

/** Wrap only transport/session failures, preserving their original cause. */
export function transportError(error: unknown, phase: RelayErrorPhase): RelayError {
  if (error instanceof RelayError) return error;
  return new RelayError('TRANSPORT_ERROR', error instanceof Error ? error.message : 'Relay transport failed', phase, {
    cause: error,
  });
}
