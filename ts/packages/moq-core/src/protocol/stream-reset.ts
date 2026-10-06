import { MoqtStreamError } from './transport-error.js';

/**
 * The reason to hand a stream's `abort()` so the reset carries `code` (§12.5, spec/transport-adapter.md TA-17).
 *
 * A browser's WebTransport takes the reset code only from a native `WebTransportError`'s `streamErrorCode`; any other
 * reason resets with code 0. So where `WebTransportError` exists, the reason is one. Elsewhere (native QUIC adapters,
 * test fakes) the transport reads `streamErrorCode` from whatever it is handed, and the reason is a
 * `MoqtStreamError` with the same shape (TA-22), keeping `cause`.
 */
export function streamResetReason(code: number, message: string, options?: ErrorOptions): unknown {
  if (typeof WebTransportError !== 'undefined') {
    try {
      return new WebTransportError(message, { source: 'stream', streamErrorCode: code });
    } catch (error) {
      if (!(error instanceof TypeError)) throw error;
      // Chromium still implements the earlier dictionary constructor; current DOM types use two arguments.
      return Reflect.construct(WebTransportError, [{ message, streamErrorCode: code }]);
    }
  }

  return new MoqtStreamError(message, code, options);
}
