/**
 * Transport errors by shape (spec/transport-adapter.md TA-22, TA-23). An adapter's stream or session failure is an
 * object with `source: 'stream' | 'session'`, and a stream failure carries the peer's `streamErrorCode`. The class and
 * `name` are not part of the contract: a browser's `WebTransportError` has this shape, and so does an error from any
 * other adapter.
 */

/** Whether `error` is a stream failure (TA-22): the peer reset the stream (TA-19) or stopped it (TA-20). */
export function isStreamError(error: unknown): boolean {
  return sourceOf(error) === 'stream';
}

/** Whether `error` is a session failure (TA-22): the session is over, and this operation failed because of it (TA-23). */
export function isSessionError(error: unknown): boolean {
  return sourceOf(error) === 'session';
}

function sourceOf(error: unknown): unknown {
  return typeof error === 'object' && error !== null ? (error as { source?: unknown }).source : undefined;
}

/**
 * A stream error the SDK raises or hands a transport as a reset or stop reason outside a browser: the TA-22 shape,
 * with `source` `'stream'` and the code in `streamErrorCode`.
 */
export class MoqtStreamError extends Error {
  override readonly name = 'MoqtStreamError';
  /** Always `'stream'`: the failure belongs to one stream. */
  readonly source = 'stream';
  /** The stream error code (draft-20 §3.3.4), or `null` when it is not known. */
  readonly streamErrorCode: number | null;

  /**
   * @param message Diagnostic text; not a compatibility contract.
   * @param streamErrorCode The stream error code, or `null` when it is not known.
   * @param options `cause` carries the failure that led to the reset.
   */
  constructor(message: string, streamErrorCode: number | null, options?: ErrorOptions) {
    super(message, options);
    this.streamErrorCode = streamErrorCode;
  }
}
