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
  override readonly name: string = 'RelayError';
  readonly code: RelayErrorCode;
  readonly phase: RelayErrorPhase;

  constructor(code: RelayErrorCode, message: string, phase: RelayErrorPhase, options?: ErrorOptions) {
    super(message, options);
    this.code = code;
    this.phase = phase;
  }
}

/** Wrap only transport/session failures, preserving their original cause. */
export function transportError(error: unknown, phase: RelayErrorPhase): RelayError {
  if (error instanceof RelayError) return error;
  return new RelayError('TRANSPORT_ERROR', error instanceof Error ? error.message : 'Relay transport failed', phase, {
    cause: error,
  });
}
