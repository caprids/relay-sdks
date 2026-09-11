/**
 * MOQT session-termination error codes (moq-transport draft-20 §3.5). Used as the close code when terminating the
 * transport session and as the `code` carried by `MoqtProtocolError`.
 */
export const SESSION_ERROR = {
  NO_ERROR: 0x0,
  INTERNAL_ERROR: 0x1,
  UNAUTHORIZED: 0x2,
  PROTOCOL_VIOLATION: 0x3,
  INVALID_REQUEST_ID: 0x4,
  DUPLICATE_TRACK_ALIAS: 0x5,
  KEY_VALUE_FORMATTING_ERROR: 0x6,
  INVALID_PATH: 0x8,
  MALFORMED_PATH: 0x9,
  GOAWAY_TIMEOUT: 0x10,
  CONTROL_MESSAGE_TIMEOUT: 0x11,
  DATA_STREAM_TIMEOUT: 0x12,
  AUTH_TOKEN_CACHE_OVERFLOW: 0x13,
  DUPLICATE_AUTH_TOKEN_ALIAS: 0x14,
  MALFORMED_AUTH_TOKEN: 0x16,
  UNKNOWN_AUTH_TOKEN_ALIAS: 0x17,
  EXPIRED_AUTH_TOKEN: 0x18,
  INVALID_AUTHORITY: 0x19,
  MALFORMED_AUTHORITY: 0x1a,
  TOO_MANY_REQUEST_UPDATES: 0x1b,
} as const;

export type SessionErrorCode = (typeof SESSION_ERROR)[keyof typeof SESSION_ERROR];

const MOQT_PROTOCOL_ERROR_SYMBOL = Symbol.for('@videojs/spf/moqt-protocol-error');

/**
 * A wire-protocol violation detected while encoding or decoding MOQT data. `code` is the session-termination code
 * (§3.5) the session driver should close the transport with — defaults to `PROTOCOL_VIOLATION`.
 */
export class MoqtProtocolError extends Error {
  [MOQT_PROTOCOL_ERROR_SYMBOL] = true as const;
  readonly code: SessionErrorCode;

  constructor(message: string, code: SessionErrorCode = SESSION_ERROR.PROTOCOL_VIOLATION) {
    super(message);
    this.name = 'MoqtProtocolError';
    this.code = code;
  }
}

export function isMoqtProtocolError(value: unknown): value is MoqtProtocolError {
  return typeof value === 'object' && value !== null && MOQT_PROTOCOL_ERROR_SYMBOL in value;
}
