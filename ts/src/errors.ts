/** Stable categories for SDK lifecycle failures; messages are diagnostic text only. */
export type RelayErrorCode =
  | 'CONNECTION_TIMEOUT'
  | 'WRITE_TIMEOUT'
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
   * The relay refused the connection's credentials. Shared with the Python SDK, which reports it today. TypeScript
   * does not produce it yet: browsers do not expose moq-relay's CLOSE_WEBTRANSPORT_SESSION code, so a refusal
   * arrives as `TRANSPORT_ERROR` until the relay lets the capsule through (muxinc/relay-sdks#14).
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
 * Whether a send-stream failure is the peer's STOP_SENDING. Chromium reports it in one of two shapes, depending on
 * which of its own notifications lands first: a `WebTransportError` whose `source` is `'stream'` ("Received
 * STOP_SENDING."), or a `NetworkError` DOMException saying the remote side aborted the stream. A session failure is a
 * `WebTransportError` whose `source` is `'session'`.
 */
export function isStreamStop(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  if (error.name === 'WebTransportError') return (error as Error & { source?: unknown }).source === 'stream';
  return error.name === 'NetworkError' && error.message === 'The stream was aborted by the remote server';
}

/** Wrap only transport/session failures, preserving their original cause. */
export function transportError(error: unknown, phase: RelayErrorPhase): RelayError {
  if (error instanceof RelayError) return error;
  return new RelayError('TRANSPORT_ERROR', error instanceof Error ? error.message : 'Relay transport failed', phase, {
    cause: error,
  });
}
