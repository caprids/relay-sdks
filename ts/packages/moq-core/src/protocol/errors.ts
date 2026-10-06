/**
 * MOQT session-termination error codes (moq-transport draft-22 §12.2). Used as the close code when terminating the
 * transport session and as the `code` carried by `MoqtProtocolError`.
 */
export const SESSION_ERROR = {
  /** The session closed without error. */
  NO_ERROR: 0x0,
  /** An implementation-specific error occurred. */
  INTERNAL_ERROR: 0x1,
  /** The client is not authorized to establish a session. */
  UNAUTHORIZED: 0x2,
  /** The peer violated the protocol. */
  PROTOCOL_VIOLATION: 0x3,
  /** The peer reused a request ID or used one with the wrong parity. */
  INVALID_REQUEST_ID: 0x4,
  /** The peer used a Track Alias already in use. */
  DUPLICATE_TRACK_ALIAS: 0x5,
  /** A key-value pair was malformed. */
  KEY_VALUE_FORMATTING_ERROR: 0x6,
  /** The PATH setup option was not allowed or names an unsupported path. */
  INVALID_PATH: 0x8,
  /** The PATH setup option could not be parsed. */
  MALFORMED_PATH: 0x9,
  /** The peer took too long to close the session after GOAWAY. */
  GOAWAY_TIMEOUT: 0x10,
  /** The peer took too long to respond to a control message. */
  CONTROL_MESSAGE_TIMEOUT: 0x11,
  /** The peer took too long to send data expected on an open data stream. */
  DATA_STREAM_TIMEOUT: 0x12,
  /** Registered authorization tokens exceeded the negotiated cache size. */
  AUTH_TOKEN_CACHE_OVERFLOW: 0x13,
  /** The peer registered an authorization token alias already in use. */
  DUPLICATE_AUTH_TOKEN_ALIAS: 0x14,
  /** An authorization token could not be parsed. */
  MALFORMED_AUTH_TOKEN: 0x16,
  /** The peer referenced an authorization token alias that is not registered. */
  UNKNOWN_AUTH_TOKEN_ALIAS: 0x17,
  /** An authorization token has expired. */
  EXPIRED_AUTH_TOKEN: 0x18,
  /** The AUTHORITY setup option names an authority this endpoint does not serve. */
  INVALID_AUTHORITY: 0x19,
  /** The AUTHORITY setup option could not be parsed. */
  MALFORMED_AUTHORITY: 0x1a,
  /** The peer sent more REQUEST_UPDATE messages than allowed. */
  TOO_MANY_REQUEST_UPDATES: 0x1b,
} as const;

/** One of the {@link SESSION_ERROR} codes. */
export type SessionErrorCode = (typeof SESSION_ERROR)[keyof typeof SESSION_ERROR];

const MOQT_PROTOCOL_ERROR_SYMBOL = Symbol.for('@videojs/spf/moqt-protocol-error');

/**
 * A wire-protocol violation detected while encoding or decoding MOQT data. `code` is the session-termination code
 * (§12.2) the session driver should close the transport with — defaults to `PROTOCOL_VIOLATION`.
 */
export class MoqtProtocolError extends Error {
  /** Brand that {@link isMoqtProtocolError} checks, so the test works across copies of this package. */
  [MOQT_PROTOCOL_ERROR_SYMBOL] = true as const;
  /** Session-termination code to close the transport with. */
  readonly code: SessionErrorCode;

  constructor(message: string, code: SessionErrorCode = SESSION_ERROR.PROTOCOL_VIOLATION) {
    super(message);
    this.name = 'MoqtProtocolError';
    this.code = code;
  }
}

/** Whether `value` is a {@link MoqtProtocolError}, including one thrown by another copy of this package. */
export function isMoqtProtocolError(value: unknown): value is MoqtProtocolError {
  return typeof value === 'object' && value !== null && MOQT_PROTOCOL_ERROR_SYMBOL in value;
}
