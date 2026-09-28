/**
 * The reason to hand a stream's `abort()` so the reset carries `code` (§3.3.4).
 *
 * A browser's WebTransport takes the reset code only from a native `WebTransportError`'s `streamErrorCode`; any other
 * reason resets with code 0. So where `WebTransportError` exists, the reason is one. Elsewhere (Node transports, test
 * fakes) the transport reads `streamErrorCode` from whatever it is handed, and the reason is `fallback()`, or an Error
 * carrying the code.
 */
export function streamResetReason(code: number, message: string, fallback?: () => unknown): unknown {
  if (typeof WebTransportError !== 'undefined') {
    try {
      return new WebTransportError(message, { source: 'stream', streamErrorCode: code });
    } catch (error) {
      if (!(error instanceof TypeError)) throw error;
      // Chromium still implements the earlier dictionary constructor; current DOM types use two arguments.
      return Reflect.construct(WebTransportError, [{ message, streamErrorCode: code }]);
    }
  }

  return fallback ? fallback() : Object.assign(new Error(message), { streamErrorCode: code });
}
